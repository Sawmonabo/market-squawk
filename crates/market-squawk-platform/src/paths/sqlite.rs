//! Shared capability and file-identity primitives for fixed local SQLite databases.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};

use super::PathError;

pub(super) trait PreparedSqliteLocation {
    fn display_root(&self) -> &Path;
    fn directory(&self) -> &Arc<Dir>;
}

pub(super) fn validate_for_open(location: &impl PreparedSqliteLocation) -> Result<(), PathError> {
    use cap_fs_ext::MetadataExt as _;

    let expected = location
        .directory()
        .dir_metadata()
        .map_err(|source| PathError::io("failed to inspect prepared SQLite root", source))?;
    let reopened = open_prepared_root(location.display_root())?;
    let actual = reopened
        .dir_metadata()
        .map_err(|source| PathError::io("failed to reinspect prepared SQLite root", source))?;
    if !expected.is_dir()
        || !actual.is_dir()
        || (expected.dev(), expected.ino()) != (actual.dev(), actual.ino())
    {
        return Err(PathError::PreparedRootChanged);
    }
    Ok(())
}

pub(super) fn open_private_file(
    location: &impl PreparedSqliteLocation,
    name: &str,
    write: bool,
    create: bool,
    context: &'static str,
) -> Result<File, PathError> {
    validate_for_open(location)?;
    let mut options = OpenOptions::new();
    options.read(true).write(write).create(create);
    options.follow(FollowSymlinks::No);
    configure_private_file_creation(&mut options);
    let file = location
        .directory()
        .open_with(name, &options)
        .map_err(|source| PathError::io(context, source))?
        .into_std();
    validate_private_file_identity(location, name, &file)?;
    Ok(file)
}

pub(super) fn acquire_writer_file(
    location: &impl PreparedSqliteLocation,
    name: &str,
    create: bool,
    contended: fn() -> PathError,
    open_context: &'static str,
    lock_context: &'static str,
) -> Result<File, PathError> {
    validate_for_open(location)?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(create);
    options.follow(FollowSymlinks::No);
    configure_private_lock_creation(&mut options);
    let file = location
        .directory()
        .open_with(name, &options)
        .map_err(|source| PathError::io(open_context, source))?
        .into_std();
    validate_private_file_identity(location, name, &file)?;
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Err(contended()),
        Err(std::fs::TryLockError::Error(source)) => {
            return Err(PathError::io(lock_context, source));
        }
    }
    validate_private_file_identity(location, name, &file)?;
    Ok(file)
}

pub(super) fn validate_optional_sqlite_sidecar(
    location: &impl PreparedSqliteLocation,
    name: &str,
    require_empty: bool,
) -> Result<(), PathError> {
    let named = match location.directory().symlink_metadata(name) {
        Ok(named) => named,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(PathError::io("failed to inspect SQLite sidecar", source));
        }
    };
    validate_private_file_metadata(&named)?;
    if require_empty && named.len() != 0 {
        return Err(PathError::PreparedRootChanged);
    }
    let opened = open_private_file(
        location,
        name,
        false,
        false,
        "failed to open SQLite sidecar",
    )?;
    if require_empty
        && opened
            .metadata()
            .map_err(|source| PathError::io("failed to reinspect SQLite sidecar", source))?
            .len()
            != 0
    {
        return Err(PathError::PreparedRootChanged);
    }
    Ok(())
}

pub(super) fn validate_private_file_identity(
    location: &impl PreparedSqliteLocation,
    name: &str,
    file: &File,
) -> Result<(), PathError> {
    validate_private_file_identity_with_links(location, name, file, 1)
}

pub(super) fn validate_private_file_identity_with_links(
    location: &impl PreparedSqliteLocation,
    name: &str,
    file: &File,
    expected_links: u64,
) -> Result<(), PathError> {
    use cap_fs_ext::MetadataExt as _;

    validate_for_open(location)?;
    let opened = cap_std::fs::File::from_std(
        file.try_clone()
            .map_err(|source| PathError::io("failed to clone SQLite control file", source))?,
    )
    .metadata()
    .map_err(|source| PathError::io("failed to inspect opened SQLite control file", source))?;
    let named = location
        .directory()
        .symlink_metadata(name)
        .map_err(|source| PathError::io("failed to inspect named SQLite control file", source))?;
    if !opened.is_file()
        || !named.is_file()
        || (opened.dev(), opened.ino()) != (named.dev(), named.ino())
    {
        return Err(PathError::PreparedRootChanged);
    }
    validate_file_link_count(&opened, expected_links)?;
    validate_private_file_metadata_with_links(&named, expected_links)
}

fn validate_file_link_count(
    metadata: &cap_std::fs::Metadata,
    expected_links: u64,
) -> Result<(), PathError> {
    use cap_fs_ext::MetadataExt as _;

    if metadata.nlink() != expected_links {
        return Err(PathError::PreparedRootChanged);
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn configure_private_lock_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(unix)]
pub(super) fn configure_private_file_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(windows)]
pub(super) fn configure_private_file_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
pub(super) fn configure_private_file_creation(_options: &mut OpenOptions) {}

#[cfg(unix)]
pub(super) fn validate_private_file_metadata(
    metadata: &cap_std::fs::Metadata,
) -> Result<(), PathError> {
    validate_private_file_metadata_with_links(metadata, 1)
}

#[cfg(unix)]
fn validate_private_file_metadata_with_links(
    metadata: &cap_std::fs::Metadata,
    expected_links: u64,
) -> Result<(), PathError> {
    use cap_std::fs::PermissionsExt as _;

    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(PathError::PreparedRootChanged);
    }
    validate_file_link_count(metadata, expected_links)
}

#[cfg(windows)]
pub(super) fn validate_private_file_metadata(
    metadata: &cap_std::fs::Metadata,
) -> Result<(), PathError> {
    validate_private_file_metadata_with_links(metadata, 1)
}

#[cfg(windows)]
fn validate_private_file_metadata_with_links(
    metadata: &cap_std::fs::Metadata,
    expected_links: u64,
) -> Result<(), PathError> {
    use cap_std::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(PathError::PreparedRootChanged);
    }
    validate_file_link_count(metadata, expected_links)
}

#[cfg(not(any(unix, windows)))]
pub(super) fn validate_private_file_metadata(
    _metadata: &cap_std::fs::Metadata,
) -> Result<(), PathError> {
    Err(PathError::PreparedRootChanged)
}

#[cfg(not(any(unix, windows)))]
fn validate_private_file_metadata_with_links(
    _metadata: &cap_std::fs::Metadata,
    _expected_links: u64,
) -> Result<(), PathError> {
    Err(PathError::PreparedRootChanged)
}

#[cfg(windows)]
pub(super) fn configure_private_lock_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
pub(super) fn configure_private_lock_creation(_options: &mut OpenOptions) {}

#[cfg(unix)]
pub(super) fn open_prepared_root(root: &Path) -> Result<Dir, PathError> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(root)
        .map_err(|source| PathError::io("failed to open prepared root", source))?;
    Ok(Dir::from_std_file(file))
}

#[cfg(windows)]
pub(super) fn open_prepared_root(root: &Path) -> Result<Dir, PathError> {
    use std::os::windows::fs::{MetadataExt as _, OpenOptionsExt as _};

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS)
        .open(root)
        .map_err(|source| PathError::io("failed to open prepared root", source))?;
    let metadata = file
        .metadata()
        .map_err(|source| PathError::io("failed to inspect prepared root handle", source))?;
    if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(PathError::PreparedRootChanged);
    }
    Ok(Dir::from_std_file(file))
}

#[cfg(not(any(unix, windows)))]
pub(super) fn open_prepared_root(_root: &Path) -> Result<Dir, PathError> {
    Err(PathError::PreparedRootChanged)
}
