//! CRC-framed current and legacy raw-capture journals.
//!
//! `MEJ1` and `MSJ1` preserve source-faithful diagnostic bytes, raw connection identity, and
//! receive metadata. They do not persist the out-of-band capture authority identity or live
//! authority state. Journal replay is therefore permanently execution-ineligible and cannot
//! reconstruct current capture, registry, sequence, checksum, freshness, or venue authority.

use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    num::TryFromIntError,
    path::{Path, PathBuf},
};

use crc32fast::Hasher;
#[cfg(not(windows))]
use fs2::FileExt as _;
use thiserror::Error;

use crate::{RawCaptureRecord, RawCaptureRecordError, raw_record::MAX_SERIALIZED_RECORD_BYTES};

const CURRENT_MAGIC: &[u8; 4] = b"MSJ1";
const MAX_RECORD_BYTES: usize = MAX_SERIALIZED_RECORD_BYTES;
const DEFAULT_MAX_RECORDS: usize = 1_000_000;
const DEFAULT_MAX_AGGREGATE_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_JOURNAL_BUFFER_CAPACITY_BYTES: usize = 64 * 1024;
const DEFAULT_JOURNAL_RETAINED_BYTE_CEILING: usize = 1024 * 1024;

/// Separate fixed-storage limits for one journal sink.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalSinkLimits {
    buffer_capacity: NonZeroUsize,
    retained_byte_ceiling: NonZeroUsize,
}

impl JournalSinkLimits {
    /// Constructs explicit nonzero journal buffer and fixed retained-byte limits.
    pub const fn new(buffer_capacity: NonZeroUsize, retained_byte_ceiling: NonZeroUsize) -> Self {
        Self {
            buffer_capacity,
            retained_byte_ceiling,
        }
    }

    /// Returns the requested bounded `BufWriter` capacity.
    pub const fn buffer_capacity(self) -> usize {
        self.buffer_capacity.get()
    }

    /// Returns the separate journal-sink fixed Rust-graph ceiling.
    pub const fn retained_byte_ceiling(self) -> usize {
        self.retained_byte_ceiling.get()
    }

    pub(crate) fn standard() -> Self {
        let buffer_capacity = match NonZeroUsize::new(DEFAULT_JOURNAL_BUFFER_CAPACITY_BYTES) {
            Some(value) => value,
            None => NonZeroUsize::MIN,
        };
        let retained_byte_ceiling = match NonZeroUsize::new(DEFAULT_JOURNAL_RETAINED_BYTE_CEILING) {
            Some(value) => value,
            None => NonZeroUsize::MIN,
        };
        Self::new(buffer_capacity, retained_byte_ceiling)
    }
}

/// Failure to construct a separately bounded journal sink.
#[derive(Debug, Error)]
pub enum JournalSinkConstructionError {
    /// The requested or observed fixed Rust graph exceeds the configured ceiling.
    #[error("journal sink fixed storage requires {required} bytes but limit is {limit} bytes")]
    FixedStorageBudgetExceeded {
        /// Exact lower-bound or observed bytes required.
        required: usize,
        /// Configured sink-owned fixed byte ceiling.
        limit: usize,
    },
    /// Fixed-storage arithmetic overflowed.
    #[error("journal sink fixed-storage arithmetic overflowed")]
    ArithmeticOverflow,
    /// Journal path, locking, validation, or header initialization failed.
    #[error(transparent)]
    Journal(#[from] JournalError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JournalFormat {
    LegacyMej1,
    MarketSquawkMsj1,
}

/// Explicit authority limitation of every committed `MEJ1`/`MSJ1` replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalReplayAuthority {
    /// The format does not contain the out-of-band evidence needed for current live authority.
    UnavailableByFormat,
}

impl TryFrom<[u8; 4]> for JournalFormat {
    type Error = JournalError;

    fn try_from(value: [u8; 4]) -> Result<Self, Self::Error> {
        match &value {
            b"MEJ1" => Ok(Self::LegacyMej1),
            b"MSJ1" => Ok(Self::MarketSquawkMsj1),
            _ => Err(JournalError::UnsupportedMagic(value)),
        }
    }
}

/// Checked journal read/write failure.
#[derive(Debug, Error)]
pub enum JournalError {
    /// Header magic is neither the committed legacy nor current format.
    #[error("unsupported journal magic: {0:?}")]
    UnsupportedMagic([u8; 4]),
    /// Writers create and append only `.msj` files.
    #[error("new and writable journals must use the .msj extension")]
    InvalidWriterExtension,
    /// Source-derived journal filenames must be one validated portable component.
    #[error("journal source filename is invalid")]
    InvalidSourceFilename,
    /// A committed legacy journal is read-only.
    #[error("legacy journal is read-only; migrate to an MSJ1 journal before appending")]
    LegacyFormatReadOnly,
    /// Writable journal endpoints may not be symbolic links.
    #[error("journal endpoint must not be a symbolic link")]
    SymlinkNotAllowed,
    /// The platform has no implemented journal directory-durability contract.
    #[error("journal directory durability is unsupported on this platform")]
    DirectoryDurabilityUnsupported,
    /// A second writer already owns the exclusive file lock.
    #[error("journal {path} already has an active writer")]
    AlreadyLocked {
        /// Locked journal path.
        path: PathBuf,
    },
    /// Bounded collection exceeded its record count.
    #[error("journal collection exceeds record limit {limit}")]
    RecordLimitExceeded {
        /// Configured record limit.
        limit: usize,
    },
    /// Bounded collection exceeded aggregate framed bytes.
    #[error("journal collection exceeds byte limit {limit}")]
    AggregateLimitExceeded {
        /// Configured aggregate byte limit.
        limit: u64,
    },
    /// Filesystem operation failed with path-safe context.
    #[error("{context}: {source}")]
    Io {
        /// Non-secret operation context.
        context: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// CRC, length, or framing is invalid.
    #[error("{0}")]
    InvalidRecord(String),
    /// JSON payload does not match the committed raw-envelope wire.
    #[error("invalid journal payload: {0}")]
    Json(#[from] serde_json::Error),
    /// An in-memory record violated the committed raw-envelope invariants.
    #[error("invalid raw capture record: {0}")]
    InvalidRawRecord(#[from] RawCaptureRecordError),
    /// Record length cannot be represented by the committed frame.
    #[error("journal record length overflow: {0}")]
    LengthOverflow(#[from] TryFromIntError),
    /// Serialized record exceeds the committed per-record bound.
    #[error("journal record is {bytes} bytes; maximum is {max}")]
    RecordTooLarge {
        /// Serialized byte count.
        bytes: usize,
        /// Maximum serialized byte count.
        max: usize,
    },
}

impl JournalError {
    pub(crate) fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }
}

/// Single-owner writer for the current `MSJ1/.msj` format.
#[derive(Debug)]
pub struct JournalWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    fixed_retained_bytes: usize,
    retained_byte_ceiling: usize,
}

#[derive(Debug)]
struct CountingCrcWriter {
    bytes: usize,
    attempted_bytes: usize,
    maximum: usize,
    hasher: Hasher,
}

impl CountingCrcWriter {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: 0,
            attempted_bytes: 0,
            maximum,
            hasher: Hasher::new(),
        }
    }

    fn finish(self) -> (usize, u32) {
        (self.bytes, self.hasher.finalize())
    }
}

impl Write for CountingCrcWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| std::io::Error::other("serialized journal record length overflowed"))?;
        self.attempted_bytes = next;
        if next > self.maximum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "serialized journal record exceeds the committed bound",
            ));
        }
        self.hasher.update(buffer);
        self.bytes = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct BoundedCrcForwardWriter<'a, W> {
    inner: &'a mut W,
    bytes: usize,
    expected_bytes: usize,
    hasher: Hasher,
}

impl<'a, W> BoundedCrcForwardWriter<'a, W> {
    fn new(inner: &'a mut W, expected_bytes: usize) -> Self {
        Self {
            inner,
            bytes: 0,
            expected_bytes,
            hasher: Hasher::new(),
        }
    }

    fn finish(self) -> (usize, u32) {
        (self.bytes, self.hasher.finalize())
    }
}

impl<W: Write> Write for BoundedCrcForwardWriter<'_, W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| std::io::Error::other("journal second-pass length overflowed"))?;
        if next > self.expected_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "journal second pass exceeded the counted length",
            ));
        }
        let written = self.inner.write(buffer)?;
        self.hasher.update(&buffer[..written]);
        self.bytes = self
            .bytes
            .checked_add(written)
            .ok_or_else(|| std::io::Error::other("journal second-pass length overflowed"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Platform-specific directory-entry durability retained across journal creation.
#[derive(Debug)]
pub(crate) struct ParentDirectorySync {
    #[cfg(unix)]
    directory: File,
}

impl ParentDirectorySync {
    #[cfg(unix)]
    pub(crate) const fn required(directory: File) -> Self {
        Self { directory }
    }

    #[cfg(windows)]
    pub(crate) const fn file_sync_is_authoritative() -> Self {
        Self {}
    }

    fn synchronize(&self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            self.directory.sync_all()
        }
        #[cfg(windows)]
        {
            Ok(())
        }
        #[cfg(not(any(unix, windows)))]
        {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "journal directory durability is unsupported",
            ))
        }
    }
}

impl JournalWriter {
    pub(crate) fn validate_limits_for_path(
        path: &PathBuf,
        limits: JournalSinkLimits,
    ) -> Result<(), JournalSinkConstructionError> {
        let required = std::mem::size_of::<Self>()
            .checked_add(path.capacity())
            .and_then(|bytes| bytes.checked_add(limits.buffer_capacity()))
            .ok_or(JournalSinkConstructionError::ArithmeticOverflow)?;
        if required > limits.retained_byte_ceiling() {
            return Err(JournalSinkConstructionError::FixedStorageBudgetExceeded {
                required,
                limit: limits.retained_byte_ceiling(),
            });
        }
        Ok(())
    }

    pub(crate) fn from_open_file(
        path: PathBuf,
        file: File,
        parent_directory: ParentDirectorySync,
        limits: JournalSinkLimits,
    ) -> Result<Self, JournalSinkConstructionError> {
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("msj") {
            return Err(JournalError::InvalidWriterExtension.into());
        }
        #[cfg(not(windows))]
        if let Err(source) = file.try_lock_exclusive() {
            if is_lock_contended(&source) {
                return Err(JournalError::AlreadyLocked { path }.into());
            }
            return Err(JournalError::io("failed to lock journal", source).into());
        }
        let is_new = file
            .metadata()
            .map_err(|source| JournalError::io("failed to inspect journal metadata", source))?
            .len()
            == 0;
        if !is_new && validate_existing_file(&file)? == JournalFormat::LegacyMej1 {
            return Err(JournalError::LegacyFormatReadOnly.into());
        }

        let mut writer = BufWriter::with_capacity(limits.buffer_capacity(), file);
        if is_new {
            writer
                .write_all(CURRENT_MAGIC)
                .map_err(|source| JournalError::io("failed to write journal header", source))?;
            writer
                .flush()
                .map_err(|source| JournalError::io("failed to flush journal header", source))?;
            writer.get_ref().sync_all().map_err(|source| {
                JournalError::io("failed to synchronize journal header", source)
            })?;
            parent_directory.synchronize().map_err(|source| {
                JournalError::io("failed to synchronize journal directory handle", source)
            })?;
        }
        let fixed_retained_bytes = std::mem::size_of::<Self>()
            .checked_add(path.capacity())
            .and_then(|bytes| bytes.checked_add(writer.capacity()))
            .ok_or(JournalSinkConstructionError::ArithmeticOverflow)?;
        if fixed_retained_bytes > limits.retained_byte_ceiling() {
            return Err(JournalSinkConstructionError::FixedStorageBudgetExceeded {
                required: fixed_retained_bytes,
                limit: limits.retained_byte_ceiling(),
            });
        }
        Ok(Self {
            path,
            writer,
            fixed_retained_bytes,
            retained_byte_ceiling: limits.retained_byte_ceiling(),
        })
    }

    /// Appends one CRC-framed compatibility record to the buffer.
    pub fn append(&mut self, record: &RawCaptureRecord) -> Result<(), JournalError> {
        let mut first_pass = CountingCrcWriter::new(MAX_RECORD_BYTES);
        if let Err(error) = serde_json::to_writer(&mut first_pass, record) {
            if first_pass.attempted_bytes > MAX_RECORD_BYTES {
                return Err(JournalError::RecordTooLarge {
                    bytes: first_pass.attempted_bytes,
                    max: MAX_RECORD_BYTES,
                });
            }
            return Err(JournalError::Json(error));
        }
        let (payload_bytes, crc) = first_pass.finish();
        if payload_bytes > MAX_RECORD_BYTES {
            return Err(JournalError::RecordTooLarge {
                bytes: payload_bytes,
                max: MAX_RECORD_BYTES,
            });
        }
        let length = u32::try_from(payload_bytes)?;
        self.writer
            .write_all(&length.to_le_bytes())
            .map_err(|source| JournalError::io("failed to write journal length", source))?;
        self.writer
            .write_all(&crc.to_le_bytes())
            .map_err(|source| JournalError::io("failed to write journal checksum", source))?;
        let mut second_pass = BoundedCrcForwardWriter::new(&mut self.writer, payload_bytes);
        if let Err(error) = serde_json::to_writer(&mut second_pass, record) {
            if error.is_io() {
                let kind = error.io_error_kind().unwrap_or(std::io::ErrorKind::Other);
                return Err(JournalError::io(
                    "failed to write journal payload",
                    std::io::Error::new(kind, error),
                ));
            }
            return Err(JournalError::Json(error));
        }
        let (written_bytes, written_crc) = second_pass.finish();
        if written_bytes != payload_bytes || written_crc != crc {
            return Err(JournalError::InvalidRecord(
                "journal serialization passes produced different bytes".to_owned(),
            ));
        }
        Ok(())
    }

    /// Flushes buffered frames and synchronizes file data.
    pub fn flush(&mut self) -> Result<(), JournalError> {
        self.writer
            .flush()
            .map_err(|source| JournalError::io("failed to flush journal", source))?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|source| JournalError::io("failed to synchronize journal", source))
    }

    /// Returns the owned journal path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the exact observed fixed Rust graph owned by this sink.
    pub const fn fixed_retained_bytes(&self) -> usize {
        self.fixed_retained_bytes
    }

    /// Returns the journal sink's independent fixed retained-byte ceiling.
    pub const fn retained_byte_ceiling(&self) -> usize {
        self.retained_byte_ceiling
    }

    /// Returns the observed never-growing `BufWriter` byte capacity.
    pub fn buffer_capacity(&self) -> usize {
        self.writer.capacity()
    }
}

#[cfg(not(windows))]
fn is_lock_contended(source: &std::io::Error) -> bool {
    let expected = fs2::lock_contended_error();
    match (source.raw_os_error(), expected.raw_os_error()) {
        (Some(actual), Some(expected)) => actual == expected,
        _ => source.kind() == expected.kind(),
    }
}

/// Streaming reader for committed `MSJ1` and legacy `MEJ1` records.
#[derive(Debug)]
pub struct JournalReader<R = File> {
    reader: BufReader<R>,
    offset: u64,
    format: Option<JournalFormat>,
}

impl JournalReader<File> {
    /// Opens a journal and validates its header.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JournalError> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|source| {
            JournalError::io(format!("failed to open journal {}", path.display()), source)
        })?;
        let mut reader = Self::new(file);
        reader.ensure_format()?;
        Ok(reader)
    }
}

impl<R: Read> JournalReader<R> {
    /// Wraps a readable journal stream.
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            offset: 4,
            format: None,
        }
    }

    /// Returns the permanent execution-authority limitation of this diagnostic format.
    pub const fn replay_authority(&self) -> JournalReplayAuthority {
        JournalReplayAuthority::UnavailableByFormat
    }

    fn ensure_format(&mut self) -> Result<JournalFormat, JournalError> {
        if let Some(format) = self.format {
            return Ok(format);
        }
        let mut magic = [0_u8; 4];
        self.reader
            .read_exact(&mut magic)
            .map_err(|source| JournalError::io("truncated journal header", source))?;
        let format = JournalFormat::try_from(magic)?;
        self.format = Some(format);
        Ok(format)
    }

    /// Reads and validates the next record without collecting the complete journal.
    pub fn next_record(&mut self) -> Result<Option<RawCaptureRecord>, JournalError> {
        self.next_record_bounded(u64::MAX)
    }

    fn next_record_bounded(
        &mut self,
        max_framed_bytes: u64,
    ) -> Result<Option<RawCaptureRecord>, JournalError> {
        self.ensure_format()?;
        let mut length_bytes = [0_u8; 4];
        let first = self
            .reader
            .read(&mut length_bytes[..1])
            .map_err(|source| JournalError::io("failed to read journal record length", source))?;
        if first == 0 {
            return Ok(None);
        }
        self.reader
            .read_exact(&mut length_bytes[1..])
            .map_err(|source| JournalError::io("truncated record length", source))?;
        let mut crc_bytes = [0_u8; 4];
        self.reader
            .read_exact(&mut crc_bytes)
            .map_err(|source| JournalError::io("truncated record checksum", source))?;
        let length = u32::from_le_bytes(length_bytes) as usize;
        if length > MAX_RECORD_BYTES {
            return Err(JournalError::InvalidRecord(format!(
                "journal record at offset {} is too large: {length}",
                self.offset
            )));
        }
        let framed_bytes = 8_u64.checked_add(u64::try_from(length)?).ok_or_else(|| {
            JournalError::InvalidRecord("journal frame length overflow".to_owned())
        })?;
        if framed_bytes > max_framed_bytes {
            return Err(JournalError::AggregateLimitExceeded {
                limit: max_framed_bytes,
            });
        }
        let mut payload = vec![0_u8; length];
        self.reader
            .read_exact(&mut payload)
            .map_err(|source| JournalError::io("truncated record payload", source))?;
        let expected_crc = u32::from_le_bytes(crc_bytes);
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let actual_crc = hasher.finalize();
        if actual_crc != expected_crc {
            return Err(JournalError::InvalidRecord(format!(
                "journal checksum mismatch at offset {}: expected={expected_crc}, actual={actual_crc}",
                self.offset
            )));
        }
        self.offset = self
            .offset
            .checked_add(framed_bytes)
            .ok_or_else(|| JournalError::InvalidRecord("journal offset overflow".to_owned()))?;
        Ok(Some(serde_json::from_slice(&payload)?))
    }

    /// Collects using conservative default record and byte limits.
    pub fn read_all(self) -> Result<Vec<RawCaptureRecord>, JournalError> {
        self.read_all_bounded(DEFAULT_MAX_RECORDS, DEFAULT_MAX_AGGREGATE_BYTES)
    }

    /// Collects under explicit record-count and aggregate framed-byte limits.
    pub fn read_all_bounded(
        mut self,
        max_records: usize,
        max_aggregate_bytes: u64,
    ) -> Result<Vec<RawCaptureRecord>, JournalError> {
        let mut records = Vec::new();
        loop {
            let has_record = !self
                .reader
                .fill_buf()
                .map_err(|source| JournalError::io("failed to inspect journal stream", source))?
                .is_empty();
            if !has_record {
                return Ok(records);
            }
            if records.len() >= max_records {
                return Err(JournalError::RecordLimitExceeded { limit: max_records });
            }
            let consumed = self.offset.saturating_sub(4);
            let remaining = max_aggregate_bytes.saturating_sub(consumed);
            let record = self.next_record_bounded(remaining)?.ok_or_else(|| {
                JournalError::InvalidRecord("journal stream changed while reading".to_owned())
            })?;
            records.push(record);
        }
    }
}

fn validate_existing_file(file: &File) -> Result<JournalFormat, JournalError> {
    let mut file = file
        .try_clone()
        .map_err(|source| JournalError::io("failed to clone journal handle", source))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| JournalError::io("failed to seek journal handle", source))?;
    let mut reader = JournalReader::new(file);
    validate_existing_reader(&mut reader)
}

fn validate_existing_reader<R: Read>(
    reader: &mut JournalReader<R>,
) -> Result<JournalFormat, JournalError> {
    let format = reader.ensure_format()?;
    let mut records = 0_usize;
    loop {
        let has_record = !reader
            .reader
            .fill_buf()
            .map_err(|source| JournalError::io("failed to inspect journal stream", source))?
            .is_empty();
        if !has_record {
            break;
        }
        let _record = reader.next_record()?.ok_or_else(|| {
            JournalError::InvalidRecord("journal stream changed while validating".to_owned())
        })?;
        records = records.checked_add(1).ok_or_else(|| {
            JournalError::InvalidRecord("journal record count overflow".to_owned())
        })?;
    }
    Ok(format)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{
        DEFAULT_MAX_AGGREGATE_BYTES, JournalFormat, JournalReader, validate_existing_reader,
    };

    fn valid_frame() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let payload = br#"{"event_id":"00000000-0000-0000-0000-000000000001","source":"fixture","connection_id":"00000000-0000-0000-0000-000000000002","source_sequence":1,"exchange_at":null,"received_at":"2026-07-15T20:00:01Z","payload":[]}"#;
        let mut frame = Vec::new();
        frame.extend_from_slice(&u32::try_from(payload.len())?.to_le_bytes());
        frame.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
        frame.extend_from_slice(payload);
        Ok(frame)
    }

    #[test]
    fn writer_startup_validation_streams_past_the_collection_byte_ceiling()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut reader = JournalReader::new(Cursor::new(valid_frame()?));
        reader.format = Some(JournalFormat::MarketSquawkMsj1);
        reader.offset = DEFAULT_MAX_AGGREGATE_BYTES
            .checked_add(1)
            .ok_or("invalid fixed test offset")?;

        let format = validate_existing_reader(&mut reader)?;

        assert_eq!(format, JournalFormat::MarketSquawkMsj1);
        assert!(reader.offset > DEFAULT_MAX_AGGREGATE_BYTES);
        Ok(())
    }

    #[test]
    fn writer_startup_validation_still_rejects_a_torn_tail()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut frame = valid_frame()?;
        let _truncated_byte = frame.pop().ok_or("fixture frame was empty")?;
        let mut reader = JournalReader::new(Cursor::new(frame));
        reader.format = Some(JournalFormat::MarketSquawkMsj1);

        assert!(validate_existing_reader(&mut reader).is_err());
        Ok(())
    }
}
