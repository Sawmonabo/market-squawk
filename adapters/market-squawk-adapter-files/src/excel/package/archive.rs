//! Bounded ZIP admission and retained package-part loading.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read as _};
use std::path::Component;

use zip::ZipArchive;

use crate::{FileAdapterError, ParseBudget, ParserLimit};

const EOCD_SIGNATURE: u32 = 0x0605_4b50;
const ZIP64_EOCD_SIGNATURE: u32 = 0x0606_4b50;
const ZIP64_LOCATOR_SIGNATURE: u32 = 0x0706_4b50;
const CENTRAL_FILE_SIGNATURE: u32 = 0x0201_4b50;
const LOCAL_FILE_SIGNATURE: u32 = 0x0403_4b50;
const EOCD_MINIMUM_BYTES: usize = 22;
const ZIP64_LOCATOR_BYTES: usize = 20;
const ZIP64_EOCD_MINIMUM_BYTES: usize = 56;
const CENTRAL_FILE_FIXED_BYTES: usize = 46;
const LOCAL_FILE_FIXED_BYTES: usize = 30;
const MAX_ZIP_COMMENT_BYTES: usize = u16::MAX as usize;
const CONSERVATIVE_METADATA_BYTES_PER_ENTRY: usize = 1_024;
const CONSERVATIVE_METADATA_DIRECTORY_MULTIPLIER: usize = 2;
const CONSERVATIVE_PAYLOAD_ALLOCATION_MULTIPLIER: usize = 2;

pub(super) fn read_package(
    bytes: &[u8],
    budget: &mut ParseBudget<'_>,
) -> Result<BTreeMap<String, Vec<u8>>, FileAdapterError> {
    let preflight = preflight_archive(bytes, budget)?;
    let mut archive =
        ZipArchive::new(Cursor::new(bytes)).map_err(|_| FileAdapterError::UnsafeArchive)?;
    if archive.offset() != 0 || archive.len() != preflight.entries {
        return Err(FileAdapterError::UnsafeArchive);
    }
    if archive
        .has_overlapping_files()
        .map_err(|_| FileAdapterError::UnsafeArchive)?
    {
        return Err(FileAdapterError::UnsafeArchive);
    }

    let mut names = BTreeSet::new();
    let mut declared_total = 0_u64;
    for index in 0..archive.len() {
        budget.checkpoint()?;
        let file = archive
            .by_index(index)
            .map_err(|_| FileAdapterError::UnsafeArchive)?;
        let name = validate_part_name(&file, budget)?;
        budget.ensure_dynamic_bytes(name.len())?;
        let lower = name.to_ascii_lowercase();
        budget.string_allocation(&lower)?;
        reject_active_part(&lower)?;
        if names.contains(&lower) {
            return Err(FileAdapterError::UnsafeArchive);
        }
        budget.set_entry::<String>()?;
        let _ = names.insert(lower);
        declared_total =
            declared_total
                .checked_add(file.size())
                .ok_or(FileAdapterError::LimitExceeded(
                    ParserLimit::DecompressedBytes,
                ))?;
        if declared_total > budget.limits.input.max_decompressed_bytes {
            return Err(FileAdapterError::LimitExceeded(
                ParserLimit::DecompressedBytes,
            ));
        }
        validate_compression_ratio(file.size(), file.compressed_size(), budget)?;
    }

    let mut parts = BTreeMap::new();
    for index in 0..archive.len() {
        budget.checkpoint()?;
        let mut file = archive
            .by_index(index)
            .map_err(|_| FileAdapterError::UnsafeArchive)?;
        if file.is_dir() {
            continue;
        }
        let name = budget.owned_text(file.name())?;
        let declared = file.size();
        let probe_capacity = declared
            .checked_add(1)
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        let admitted_allocation = probe_capacity
            .checked_mul(CONSERVATIVE_PAYLOAD_ALLOCATION_MULTIPLIER)
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        budget.allocation_bytes(admitted_allocation)?;
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(probe_capacity)
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;

        let remaining = budget.remaining_decompressed()?;
        let maximum =
            declared
                .min(remaining)
                .checked_add(1)
                .ok_or(FileAdapterError::LimitExceeded(
                    ParserLimit::DecompressedBytes,
                ))?;
        file.by_ref()
            .take(maximum)
            .read_to_end(&mut payload)
            .map_err(|_| FileAdapterError::UnsafeArchive)?;
        let actual = u64::try_from(payload.len())
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecompressedBytes))?;
        budget.decompressed(actual)?;
        if actual != declared {
            return Err(FileAdapterError::UnsafeArchive);
        }
        budget.map_entry::<String, Vec<u8>>()?;
        if parts.insert(name, payload).is_some() {
            return Err(FileAdapterError::UnsafeArchive);
        }
    }
    Ok(parts)
}

pub(super) fn required_part<'a>(
    parts: &'a BTreeMap<String, Vec<u8>>,
    name: &str,
) -> Result<&'a [u8], FileAdapterError> {
    parts
        .get(name)
        .map(Vec::as_slice)
        .ok_or(FileAdapterError::UnsafeSpreadsheet)
}

#[derive(Clone, Copy, Debug)]
struct ArchivePreflight {
    entries: usize,
}

#[derive(Clone, Copy, Debug)]
struct CentralDirectory {
    offset: usize,
    size: usize,
    entries: usize,
}

fn preflight_archive(
    bytes: &[u8],
    budget: &mut ParseBudget<'_>,
) -> Result<ArchivePreflight, FileAdapterError> {
    let directory = central_directory(bytes)?;
    if directory.entries > budget.limits.input.max_archive_entries {
        return Err(FileAdapterError::LimitExceeded(ParserLimit::ArchiveEntries));
    }
    validate_central_directory(bytes, directory, budget)?;
    let directory_charge = directory
        .size
        .checked_mul(CONSERVATIVE_METADATA_DIRECTORY_MULTIPLIER)
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
    let entry_charge = directory
        .entries
        .checked_mul(CONSERVATIVE_METADATA_BYTES_PER_ENTRY)
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
    let metadata_charge = directory_charge
        .checked_add(entry_charge)
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
    budget.allocation_bytes(metadata_charge)?;
    Ok(ArchivePreflight {
        entries: directory.entries,
    })
}

fn central_directory(bytes: &[u8]) -> Result<CentralDirectory, FileAdapterError> {
    let eocd = find_eocd(bytes)?;
    let disk = read_u16(bytes, eocd + 4)?;
    let directory_disk = read_u16(bytes, eocd + 6)?;
    let entries_on_disk = read_u16(bytes, eocd + 8)?;
    let entries = read_u16(bytes, eocd + 10)?;
    let size = read_u32(bytes, eocd + 12)?;
    let offset = read_u32(bytes, eocd + 16)?;
    let requires_zip64 = disk == u16::MAX
        || directory_disk == u16::MAX
        || entries_on_disk == u16::MAX
        || entries == u16::MAX
        || size == u32::MAX
        || offset == u32::MAX;
    if requires_zip64 {
        zip64_central_directory(
            bytes,
            eocd,
            disk,
            directory_disk,
            entries_on_disk,
            entries,
            size,
            offset,
        )
    } else {
        if disk != 0 || directory_disk != 0 || entries_on_disk != entries {
            return Err(FileAdapterError::UnsafeArchive);
        }
        let directory = CentralDirectory {
            offset: usize::try_from(offset).map_err(|_| FileAdapterError::UnsafeArchive)?,
            size: usize::try_from(size).map_err(|_| FileAdapterError::UnsafeArchive)?,
            entries: usize::from(entries),
        };
        require_directory_end(directory, eocd)?;
        Ok(directory)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the eight EOCD fields are cross-validated against one ZIP64 authority record"
)]
fn zip64_central_directory(
    bytes: &[u8],
    eocd: usize,
    disk: u16,
    directory_disk: u16,
    entries_on_disk: u16,
    entries: u16,
    size: u32,
    offset: u32,
) -> Result<CentralDirectory, FileAdapterError> {
    let locator = eocd
        .checked_sub(ZIP64_LOCATOR_BYTES)
        .ok_or(FileAdapterError::UnsafeArchive)?;
    if read_u32(bytes, locator)? != ZIP64_LOCATOR_SIGNATURE
        || read_u32(bytes, locator + 4)? != 0
        || read_u32(bytes, locator + 16)? != 1
    {
        return Err(FileAdapterError::UnsafeArchive);
    }
    let zip64_offset = usize::try_from(read_u64(bytes, locator + 8)?)
        .map_err(|_| FileAdapterError::UnsafeArchive)?;
    if read_u32(bytes, zip64_offset)? != ZIP64_EOCD_SIGNATURE {
        return Err(FileAdapterError::UnsafeArchive);
    }
    let record_size = usize::try_from(read_u64(bytes, zip64_offset + 4)?)
        .map_err(|_| FileAdapterError::UnsafeArchive)?;
    let record_end = zip64_offset
        .checked_add(12)
        .and_then(|value| value.checked_add(record_size))
        .ok_or(FileAdapterError::UnsafeArchive)?;
    if record_size < ZIP64_EOCD_MINIMUM_BYTES - 12 || record_end != locator {
        return Err(FileAdapterError::UnsafeArchive);
    }
    let zip64_disk = read_u32(bytes, zip64_offset + 16)?;
    let zip64_directory_disk = read_u32(bytes, zip64_offset + 20)?;
    let zip64_entries_on_disk = read_u64(bytes, zip64_offset + 24)?;
    let zip64_entries = read_u64(bytes, zip64_offset + 32)?;
    let zip64_size = read_u64(bytes, zip64_offset + 40)?;
    let zip64_directory_offset = read_u64(bytes, zip64_offset + 48)?;
    if zip64_disk != 0
        || zip64_directory_disk != 0
        || zip64_entries_on_disk != zip64_entries
        || disk != u16::MAX && u32::from(disk) != zip64_disk
        || directory_disk != u16::MAX && u32::from(directory_disk) != zip64_directory_disk
        || entries_on_disk != u16::MAX && u64::from(entries_on_disk) != zip64_entries_on_disk
        || entries != u16::MAX && u64::from(entries) != zip64_entries
        || size != u32::MAX && u64::from(size) != zip64_size
        || offset != u32::MAX && u64::from(offset) != zip64_directory_offset
    {
        return Err(FileAdapterError::UnsafeArchive);
    }
    let directory = CentralDirectory {
        offset: usize::try_from(zip64_directory_offset)
            .map_err(|_| FileAdapterError::UnsafeArchive)?,
        size: usize::try_from(zip64_size).map_err(|_| FileAdapterError::UnsafeArchive)?,
        entries: usize::try_from(zip64_entries)
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::ArchiveEntries))?,
    };
    require_directory_end(directory, zip64_offset)?;
    Ok(directory)
}

fn require_directory_end(
    directory: CentralDirectory,
    expected_end: usize,
) -> Result<(), FileAdapterError> {
    if directory
        .offset
        .checked_add(directory.size)
        .is_some_and(|end| end == expected_end)
    {
        Ok(())
    } else {
        Err(FileAdapterError::UnsafeArchive)
    }
}

fn find_eocd(bytes: &[u8]) -> Result<usize, FileAdapterError> {
    let last = bytes
        .len()
        .checked_sub(EOCD_MINIMUM_BYTES)
        .ok_or(FileAdapterError::UnsafeArchive)?;
    let first = bytes
        .len()
        .saturating_sub(EOCD_MINIMUM_BYTES + MAX_ZIP_COMMENT_BYTES);
    let mut found = None;
    for offset in (first..=last).rev() {
        if read_u32(bytes, offset).ok() != Some(EOCD_SIGNATURE) {
            continue;
        }
        let comment = usize::from(read_u16(bytes, offset + 20)?);
        if offset
            .checked_add(EOCD_MINIMUM_BYTES)
            .and_then(|value| value.checked_add(comment))
            != Some(bytes.len())
        {
            continue;
        }
        if found.replace(offset).is_some() {
            return Err(FileAdapterError::UnsafeArchive);
        }
    }
    found.ok_or(FileAdapterError::UnsafeArchive)
}

fn validate_central_directory(
    bytes: &[u8],
    directory: CentralDirectory,
    budget: &ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    let end = directory
        .offset
        .checked_add(directory.size)
        .ok_or(FileAdapterError::UnsafeArchive)?;
    let mut cursor = directory.offset;
    let mut declared_total = 0_u64;
    for _ in 0..directory.entries {
        if read_u32(bytes, cursor)? != CENTRAL_FILE_SIGNATURE {
            return Err(FileAdapterError::UnsafeArchive);
        }
        let flags = read_u16(bytes, cursor + 8)?;
        let method = read_u16(bytes, cursor + 10)?;
        if flags & 1 != 0 || !matches!(method, 0 | 8) {
            return Err(FileAdapterError::UnsafeArchive);
        }
        let compressed32 = read_u32(bytes, cursor + 20)?;
        let uncompressed32 = read_u32(bytes, cursor + 24)?;
        let name_length = usize::from(read_u16(bytes, cursor + 28)?);
        let extra_length = usize::from(read_u16(bytes, cursor + 30)?);
        let comment_length = usize::from(read_u16(bytes, cursor + 32)?);
        let disk_start = read_u16(bytes, cursor + 34)?;
        let local_offset32 = read_u32(bytes, cursor + 42)?;
        let name_start = cursor
            .checked_add(CENTRAL_FILE_FIXED_BYTES)
            .ok_or(FileAdapterError::UnsafeArchive)?;
        let extra_start = name_start
            .checked_add(name_length)
            .ok_or(FileAdapterError::UnsafeArchive)?;
        let extra_end = extra_start
            .checked_add(extra_length)
            .ok_or(FileAdapterError::UnsafeArchive)?;
        let entry_end = extra_end
            .checked_add(comment_length)
            .ok_or(FileAdapterError::UnsafeArchive)?;
        if name_length == 0 || entry_end > end {
            return Err(FileAdapterError::UnsafeArchive);
        }
        let name = bytes
            .get(name_start..extra_start)
            .ok_or(FileAdapterError::UnsafeArchive)?;
        let extra = bytes
            .get(extra_start..extra_end)
            .ok_or(FileAdapterError::UnsafeArchive)?;
        let resolved = resolve_zip64_entry(
            compressed32,
            uncompressed32,
            local_offset32,
            disk_start,
            extra,
        )?;
        if resolved.disk_start != 0 || resolved.local_offset >= directory.offset {
            return Err(FileAdapterError::UnsafeArchive);
        }
        validate_local_header(bytes, directory.offset, name, flags, method, resolved)?;
        validate_declared_ratio(resolved.uncompressed, resolved.compressed, budget)?;
        declared_total = declared_total.checked_add(resolved.uncompressed).ok_or(
            FileAdapterError::LimitExceeded(ParserLimit::DecompressedBytes),
        )?;
        if declared_total > budget.limits.input.max_decompressed_bytes {
            return Err(FileAdapterError::LimitExceeded(
                ParserLimit::DecompressedBytes,
            ));
        }
        cursor = entry_end;
    }
    if cursor != end {
        return Err(FileAdapterError::UnsafeArchive);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct ResolvedEntry {
    compressed: u64,
    uncompressed: u64,
    local_offset: usize,
    disk_start: u32,
}

fn resolve_zip64_entry(
    compressed32: u32,
    uncompressed32: u32,
    local_offset32: u32,
    disk_start16: u16,
    extra: &[u8],
) -> Result<ResolvedEntry, FileAdapterError> {
    let needs_uncompressed = uncompressed32 == u32::MAX;
    let needs_compressed = compressed32 == u32::MAX;
    let needs_offset = local_offset32 == u32::MAX;
    let needs_disk = disk_start16 == u16::MAX;
    let mut zip64 = None;
    let mut cursor = 0_usize;
    while cursor < extra.len() {
        let header = read_u16(extra, cursor)?;
        let length = usize::from(read_u16(extra, cursor + 2)?);
        let value_start = cursor
            .checked_add(4)
            .ok_or(FileAdapterError::UnsafeArchive)?;
        let value_end = value_start
            .checked_add(length)
            .ok_or(FileAdapterError::UnsafeArchive)?;
        let value = extra
            .get(value_start..value_end)
            .ok_or(FileAdapterError::UnsafeArchive)?;
        if header == 0x0001 && zip64.replace(value).is_some() {
            return Err(FileAdapterError::UnsafeArchive);
        }
        cursor = value_end;
    }
    if cursor != extra.len() {
        return Err(FileAdapterError::UnsafeArchive);
    }
    let mut values = zip64.unwrap_or_default();
    let uncompressed =
        take_zip64_u64(&mut values, needs_uncompressed)?.unwrap_or(u64::from(uncompressed32));
    let compressed =
        take_zip64_u64(&mut values, needs_compressed)?.unwrap_or(u64::from(compressed32));
    let local_offset =
        take_zip64_u64(&mut values, needs_offset)?.unwrap_or(u64::from(local_offset32));
    let disk_start = take_zip64_u32(&mut values, needs_disk)?.unwrap_or(u32::from(disk_start16));
    if (needs_uncompressed || needs_compressed || needs_offset || needs_disk) && zip64.is_none() {
        return Err(FileAdapterError::UnsafeArchive);
    }
    Ok(ResolvedEntry {
        compressed,
        uncompressed,
        local_offset: usize::try_from(local_offset).map_err(|_| FileAdapterError::UnsafeArchive)?,
        disk_start,
    })
}

fn take_zip64_u64(values: &mut &[u8], required: bool) -> Result<Option<u64>, FileAdapterError> {
    if !required {
        return Ok(None);
    }
    let value = read_u64(values, 0)?;
    *values = values.get(8..).ok_or(FileAdapterError::UnsafeArchive)?;
    Ok(Some(value))
}

fn take_zip64_u32(values: &mut &[u8], required: bool) -> Result<Option<u32>, FileAdapterError> {
    if !required {
        return Ok(None);
    }
    let value = read_u32(values, 0)?;
    *values = values.get(4..).ok_or(FileAdapterError::UnsafeArchive)?;
    Ok(Some(value))
}

fn validate_local_header(
    bytes: &[u8],
    directory_offset: usize,
    central_name: &[u8],
    central_flags: u16,
    central_method: u16,
    entry: ResolvedEntry,
) -> Result<(), FileAdapterError> {
    if read_u32(bytes, entry.local_offset)? != LOCAL_FILE_SIGNATURE
        || read_u16(bytes, entry.local_offset + 6)? != central_flags
        || read_u16(bytes, entry.local_offset + 8)? != central_method
    {
        return Err(FileAdapterError::UnsafeArchive);
    }
    let name_length = usize::from(read_u16(bytes, entry.local_offset + 26)?);
    let extra_length = usize::from(read_u16(bytes, entry.local_offset + 28)?);
    let name_start = entry
        .local_offset
        .checked_add(LOCAL_FILE_FIXED_BYTES)
        .ok_or(FileAdapterError::UnsafeArchive)?;
    let name_end = name_start
        .checked_add(name_length)
        .ok_or(FileAdapterError::UnsafeArchive)?;
    let data_start = name_end
        .checked_add(extra_length)
        .ok_or(FileAdapterError::UnsafeArchive)?;
    let data_end = data_start
        .checked_add(
            usize::try_from(entry.compressed).map_err(|_| FileAdapterError::UnsafeArchive)?,
        )
        .ok_or(FileAdapterError::UnsafeArchive)?;
    if bytes.get(name_start..name_end) != Some(central_name) || data_end > directory_offset {
        return Err(FileAdapterError::UnsafeArchive);
    }
    Ok(())
}

fn validate_declared_ratio(
    uncompressed: u64,
    compressed: u64,
    budget: &ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    if uncompressed == 0 {
        return Ok(());
    }
    let permitted = compressed
        .checked_mul(budget.limits.input.max_compression_ratio)
        .ok_or(FileAdapterError::LimitExceeded(
            ParserLimit::CompressionRatio,
        ))?;
    if compressed == 0 || uncompressed > permitted {
        Err(FileAdapterError::LimitExceeded(
            ParserLimit::CompressionRatio,
        ))
    } else {
        Ok(())
    }
}

fn validate_part_name<R: std::io::Read>(
    file: &zip::read::ZipFile<'_, R>,
    budget: &mut ParseBudget<'_>,
) -> Result<String, FileAdapterError> {
    let name = file.name();
    let path = file
        .enclosed_name()
        .ok_or(FileAdapterError::UnsafeArchive)?;
    if name.contains(['\\', ':', '\0'])
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
        || file.encrypted()
        || file.is_symlink()
    {
        return Err(FileAdapterError::UnsafeArchive);
    }
    budget.owned_text(name)
}

fn validate_compression_ratio(
    uncompressed: u64,
    compressed: u64,
    budget: &ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    validate_declared_ratio(uncompressed, compressed, budget)
}

fn reject_active_part(name: &str) -> Result<(), FileAdapterError> {
    let disallowed = name.ends_with(".bin")
        || name.contains("vbaproject")
        || name.starts_with("xl/externallinks/")
        || name.starts_with("xl/embeddings/")
        || name.starts_with("xl/activex/")
        || name.starts_with("xl/ctrlprops/")
        || name.starts_with("xl/macrosheets/")
        || name.starts_with("xl/dialogsheets/")
        || name == "xl/connections.xml"
        || name.starts_with("customui/");
    if disallowed {
        Err(FileAdapterError::UnsafeSpreadsheet)
    } else {
        Ok(())
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, FileAdapterError> {
    Ok(u16::from_le_bytes(read_array(bytes, offset)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, FileAdapterError> {
    Ok(u32::from_le_bytes(read_array(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, FileAdapterError> {
    Ok(u64::from_le_bytes(read_array(bytes, offset)?))
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], FileAdapterError> {
    let end = offset
        .checked_add(N)
        .ok_or(FileAdapterError::UnsafeArchive)?;
    bytes
        .get(offset..end)
        .ok_or(FileAdapterError::UnsafeArchive)?
        .try_into()
        .map_err(|_| FileAdapterError::UnsafeArchive)
}
