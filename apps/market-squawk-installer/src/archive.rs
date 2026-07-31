//! Bounded ZIP admission, extraction, and installed-tree verification.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use zip::{CompressionMethod, ZipArchive};

use crate::manifest::{
    ArtifactIdentity, ComponentIdentity, ComponentRole, MAXIMUM_ARCHIVE_ENTRIES,
    MAXIMUM_EXPANDED_BYTES, TargetRelease,
};

const HASH_BUFFER_BYTES: usize = 64 * 1024;
const MAXIMUM_TREE_DEPTH: usize = 64;
const UNIX_FILE_TYPE_MASK: u32 = 0o170_000;
const UNIX_REGULAR_FILE: u32 = 0o100_000;

/// Exact identity retained for one installed component.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentReceipt {
    pub(crate) path: Box<str>,
    pub(crate) role: ComponentRole,
    pub(crate) size: u64,
    pub(crate) sha256: Box<str>,
    pub(crate) executable: bool,
}

impl From<&ComponentIdentity> for ComponentReceipt {
    fn from(component: &ComponentIdentity) -> Self {
        Self {
            path: component.path.clone(),
            role: component.role,
            size: component.size,
            sha256: component.sha256.clone(),
            executable: component.executable,
        }
    }
}

/// Verifies the outer archive before any entry is admitted.
pub(crate) fn verify_bundle(
    bundle: &Path,
    expected: &ArtifactIdentity,
) -> Result<(), ArchiveError> {
    let metadata = fs::symlink_metadata(bundle)
        .map_err(|source| ArchiveError::io("inspect release archive", source))?;
    if !metadata.file_type().is_file() || metadata.len() != expected.size {
        return Err(ArchiveError::ArchiveIdentity);
    }
    let observed = sha256_file(bundle, expected.size)?;
    if observed != expected.sha256.as_ref() {
        return Err(ArchiveError::ArchiveIdentity);
    }
    Ok(())
}

/// Extracts exactly the selected manifest component set into a new controlled directory.
pub(crate) fn extract_bundle(
    bundle: &Path,
    release: &TargetRelease,
    destination: &Path,
) -> Result<Vec<ComponentReceipt>, ArchiveError> {
    let file =
        File::open(bundle).map_err(|source| ArchiveError::io("open release archive", source))?;
    let mut archive = ZipArchive::new(file).map_err(ArchiveError::Zip)?;
    let indexes = inspect_archive(&mut archive, &release.components)?;

    for component in &release.components {
        let index = indexes
            .get(component.path.as_ref())
            .copied()
            .ok_or(ArchiveError::EntrySet)?;
        let mut entry = archive.by_index(index).map_err(ArchiveError::Zip)?;
        let relative = Path::new(component.path.as_ref());
        create_directory_chain(destination, relative.parent())?;
        let output = destination.join(relative);
        let mut file = open_new_file(&output, component.executable)?;
        let observed = copy_and_hash(&mut entry, &mut file, component.size)?;
        file.sync_all()
            .map_err(|source| ArchiveError::io("sync extracted component", source))?;
        if observed != component.sha256.as_ref() {
            return Err(ArchiveError::ComponentIdentity {
                path: component.path.clone(),
            });
        }
        set_component_permissions(&output, component.executable)?;
    }

    seal_and_sync_directories(destination)?;
    Ok(release
        .components
        .iter()
        .map(ComponentReceipt::from)
        .collect())
}

/// Re-verifies an installed immutable version against its closed component receipts.
pub(crate) fn verify_installed_tree(
    root: &Path,
    receipts: &[ComponentReceipt],
) -> Result<(), ArchiveError> {
    let metadata = fs::symlink_metadata(root)
        .map_err(|source| ArchiveError::io("inspect installed version", source))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(ArchiveError::UnsafeDestination);
    }

    let observed_paths = collect_regular_files(root)?;
    let expected_paths: Vec<&str> = receipts
        .iter()
        .map(|receipt| receipt.path.as_ref())
        .collect();
    if observed_paths.len() != expected_paths.len()
        || observed_paths
            .iter()
            .map(String::as_str)
            .ne(expected_paths.iter().copied())
    {
        return Err(ArchiveError::EntrySet);
    }

    for receipt in receipts {
        let path = root.join(receipt.path.as_ref());
        verify_component(&path, receipt)?;
    }
    Ok(())
}

pub(crate) fn seal_tree_root(root: &Path) -> Result<(), ArchiveError> {
    sync_directory(root)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        fs::set_permissions(root, fs::Permissions::from_mode(0o500))
            .map_err(|source| ArchiveError::io("seal installed version root", source))?;
    }
    Ok(())
}

pub(crate) fn sha256_file(path: &Path, maximum: u64) -> Result<String, ArchiveError> {
    let mut file =
        File::open(path).map_err(|source| ArchiveError::io("open file for hashing", source))?;
    let mut digest = Sha256::new();
    let mut observed = 0_u64;
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| ArchiveError::io("read file for hashing", source))?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(u64::try_from(read).map_err(|_| ArchiveError::SizeOverflow)?)
            .ok_or(ArchiveError::SizeOverflow)?;
        if observed > maximum {
            return Err(ArchiveError::SizeLimit);
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn inspect_archive(
    archive: &mut ZipArchive<File>,
    components: &[ComponentIdentity],
) -> Result<BTreeMap<Box<str>, usize>, ArchiveError> {
    if archive.len() != components.len() || archive.len() > MAXIMUM_ARCHIVE_ENTRIES {
        return Err(ArchiveError::EntrySet);
    }
    let expected: BTreeMap<&str, &ComponentIdentity> = components
        .iter()
        .map(|component| (component.path.as_ref(), component))
        .collect();
    let mut indexes = BTreeMap::new();
    let mut portable_paths = BTreeSet::new();
    let mut expanded = 0_u64;

    for index in 0..archive.len() {
        let entry = archive.by_index(index).map_err(ArchiveError::Zip)?;
        let raw_name =
            std::str::from_utf8(entry.name_raw()).map_err(|_| ArchiveError::EntryPath)?;
        let enclosed = entry.enclosed_name().ok_or(ArchiveError::EntryPath)?;
        if enclosed != Path::new(raw_name)
            || entry.encrypted()
            || !entry.is_file()
            || !matches!(
                entry.compression(),
                CompressionMethod::Stored | CompressionMethod::Deflated
            )
        {
            return Err(ArchiveError::UnsafeEntry);
        }
        if let Some(mode) = entry.unix_mode() {
            let file_type = mode & UNIX_FILE_TYPE_MASK;
            if (file_type != 0 && file_type != UNIX_REGULAR_FILE) || mode & 0o022 != 0 {
                return Err(ArchiveError::UnsafeEntry);
            }
        }

        let component = expected.get(raw_name).ok_or(ArchiveError::EntrySet)?;
        if entry.size() != component.size {
            return Err(ArchiveError::ComponentIdentity {
                path: component.path.clone(),
            });
        }
        if component.executable && entry.unix_mode().is_some_and(|mode| mode & 0o100 == 0) {
            return Err(ArchiveError::UnsafeEntry);
        }
        expanded = expanded
            .checked_add(entry.size())
            .ok_or(ArchiveError::SizeOverflow)?;
        if expanded > MAXIMUM_EXPANDED_BYTES
            || !portable_paths.insert(raw_name.to_ascii_lowercase())
            || indexes.insert(raw_name.into(), index).is_some()
        {
            return Err(ArchiveError::EntrySet);
        }
    }
    Ok(indexes)
}

fn copy_and_hash<R: std::io::Read>(
    input: &mut R,
    output: &mut File,
    expected_size: u64,
) -> Result<String, ArchiveError> {
    let mut digest = Sha256::new();
    let mut remaining = expected_size;
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    while remaining > 0 {
        let limit = usize::try_from(remaining.min(HASH_BUFFER_BYTES as u64))
            .map_err(|_| ArchiveError::SizeOverflow)?;
        let read = input
            .read(&mut buffer[..limit])
            .map_err(|source| ArchiveError::io("read release component", source))?;
        if read == 0 {
            return Err(ArchiveError::ComponentSize);
        }
        output
            .write_all(&buffer[..read])
            .map_err(|source| ArchiveError::io("write release component", source))?;
        digest.update(&buffer[..read]);
        remaining = remaining
            .checked_sub(u64::try_from(read).map_err(|_| ArchiveError::SizeOverflow)?)
            .ok_or(ArchiveError::SizeOverflow)?;
    }
    let mut extra = [0_u8; 1];
    if input
        .read(&mut extra)
        .map_err(|source| ArchiveError::io("check release component boundary", source))?
        != 0
    {
        return Err(ArchiveError::ComponentSize);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn create_directory_chain(root: &Path, relative: Option<&Path>) -> Result<(), ArchiveError> {
    let Some(relative) = relative else {
        return Ok(());
    };
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => return Err(ArchiveError::UnsafeDestination),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)
                    .map_err(|source| ArchiveError::io("create extraction directory", source))?;
                set_private_directory_permissions(&current)?;
            }
            Err(source) => {
                return Err(ArchiveError::io("inspect extraction directory", source));
            }
        }
    }
    Ok(())
}

fn open_new_file(path: &Path, executable: bool) -> Result<File, ArchiveError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    configure_no_follow(&mut options, executable);
    options
        .open(path)
        .map_err(|source| ArchiveError::io("create extracted component", source))
}

#[cfg(unix)]
fn configure_no_follow(options: &mut OpenOptions, executable: bool) {
    use std::os::unix::fs::OpenOptionsExt as _;

    options
        .mode(if executable { 0o700 } else { 0o600 })
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
}

#[cfg(windows)]
fn configure_no_follow(options: &mut OpenOptions, _executable: bool) {
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

pub(crate) fn set_component_permissions(path: &Path, executable: bool) -> Result<(), ArchiveError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        fs::set_permissions(
            path,
            fs::Permissions::from_mode(if executable { 0o500 } else { 0o400 }),
        )
        .map_err(|source| ArchiveError::io("seal extracted component", source))?;
    }
    #[cfg(windows)]
    {
        let mut permissions = fs::metadata(path)
            .map_err(|source| ArchiveError::io("inspect extracted component", source))?
            .permissions();
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions)
            .map_err(|source| ArchiveError::io("seal extracted component", source))?;
    }
    Ok(())
}

fn set_private_directory_permissions(path: &Path) -> Result<(), ArchiveError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|source| ArchiveError::io("secure extraction directory", source))?;
    }
    #[cfg(windows)]
    {
        let _ = path;
    }
    Ok(())
}

fn seal_and_sync_directories(root: &Path) -> Result<(), ArchiveError> {
    let mut directories = collect_directories(root)?;
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_directory(&directory)?;
        if directory == root {
            continue;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            fs::set_permissions(&directory, fs::Permissions::from_mode(0o500))
                .map_err(|source| ArchiveError::io("seal extraction directory", source))?;
        }
    }
    Ok(())
}

fn collect_directories(root: &Path) -> Result<Vec<PathBuf>, ArchiveError> {
    let mut found = vec![root.to_path_buf()];
    let mut cursor = 0;
    while cursor < found.len() {
        if found[cursor]
            .strip_prefix(root)
            .map_or(0, |path| path.components().count())
            > MAXIMUM_TREE_DEPTH
        {
            return Err(ArchiveError::EntrySet);
        }
        for entry in fs::read_dir(&found[cursor])
            .map_err(|source| ArchiveError::io("read installed directory", source))?
        {
            let entry = entry.map_err(|source| ArchiveError::io("read installed entry", source))?;
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|source| ArchiveError::io("inspect installed entry", source))?;
            if metadata.file_type().is_symlink() {
                return Err(ArchiveError::UnsafeDestination);
            }
            if metadata.is_dir() {
                found.push(entry.path());
            }
        }
        cursor += 1;
    }
    Ok(found)
}

fn collect_regular_files(root: &Path) -> Result<Vec<String>, ArchiveError> {
    let directories = collect_directories(root)?;
    let mut files = Vec::new();
    for directory in directories {
        for entry in fs::read_dir(&directory)
            .map_err(|source| ArchiveError::io("read installed directory", source))?
        {
            let entry = entry.map_err(|source| ArchiveError::io("read installed entry", source))?;
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|source| ArchiveError::io("inspect installed entry", source))?;
            if metadata.is_dir() {
                continue;
            }
            if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                return Err(ArchiveError::UnsafeDestination);
            }
            let relative = entry
                .path()
                .strip_prefix(root)
                .map_err(|_| ArchiveError::UnsafeDestination)?
                .components()
                .map(|component| {
                    component
                        .as_os_str()
                        .to_str()
                        .ok_or(ArchiveError::EntryPath)
                })
                .collect::<Result<Vec<_>, _>>()?
                .join("/");
            files.push(relative);
            if files.len() > MAXIMUM_ARCHIVE_ENTRIES {
                return Err(ArchiveError::EntrySet);
            }
        }
    }
    files.sort();
    Ok(files)
}

pub(crate) fn verify_component(
    path: &Path,
    receipt: &ComponentReceipt,
) -> Result<(), ArchiveError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| ArchiveError::io("inspect installed component", source))?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() != receipt.size
        || !component_permissions_match(&metadata, receipt.executable)
        || sha256_file(path, receipt.size)? != receipt.sha256.as_ref()
    {
        return Err(ArchiveError::ComponentIdentity {
            path: receipt.path.clone(),
        });
    }
    Ok(())
}

#[cfg(unix)]
fn component_permissions_match(metadata: &fs::Metadata, executable: bool) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    let expected = if executable { 0o500 } else { 0o400 };
    metadata.permissions().mode() & 0o7777 == expected
}

#[cfg(windows)]
const fn component_permissions_match(_metadata: &fs::Metadata, _executable: bool) -> bool {
    true
}

pub(crate) fn sync_directory(path: &Path) -> Result<(), ArchiveError> {
    #[cfg(unix)]
    {
        let directory = File::open(path)
            .map_err(|source| ArchiveError::io("open directory for synchronization", source))?;
        directory
            .sync_all()
            .map_err(|source| ArchiveError::io("synchronize directory", source))
    }
    #[cfg(windows)]
    {
        // Windows permits directory handles but does not support flushing one through
        // `FlushFileBuffers`: that call requires file write access, which directory handles
        // cannot request. Component files are flushed before same-volume activation, and the
        // retained archive remains the recovery authority if selector publication is interrupted.
        let metadata = fs::symlink_metadata(path).map_err(|source| {
            ArchiveError::io("inspect directory after synchronization", source)
        })?;
        if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
            Ok(())
        } else {
            Err(ArchiveError::UnsafeDestination)
        }
    }
}

/// Release archive or installed-tree validation failure.
#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("release archive size or SHA-256 identity does not match the manifest")]
    ArchiveIdentity,
    #[error("release archive contains an unsafe or unsupported entry")]
    UnsafeEntry,
    #[error("release archive contains an unsafe or noncanonical entry path")]
    EntryPath,
    #[error("release archive entries do not exactly match the manifest")]
    EntrySet,
    #[error("release component has the wrong expanded size")]
    ComponentSize,
    #[error("release component identity does not match the manifest: {path}")]
    ComponentIdentity { path: Box<str> },
    #[error("release archive or installed tree exceeds its fixed size bound")]
    SizeLimit,
    #[error("release archive size arithmetic overflowed")]
    SizeOverflow,
    #[error("release extraction destination is unsafe")]
    UnsafeDestination,
    #[error("release ZIP structure is malformed or unsupported")]
    Zip(#[source] zip::result::ZipError),
    #[error("installer filesystem operation failed during {operation}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
}

impl ArchiveError {
    fn io(operation: &'static str, source: std::io::Error) -> Self {
        Self::Io { operation, source }
    }
}
