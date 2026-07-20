//! Prepared SQLite catalog placement and root-identity validation.

use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use fs2::FileExt as _;

use super::PathError;

const WRITER_LOCK_FILE: &str = ".catalog.writer.lock";
const CATALOG_FILE: &str = "catalog.sqlite3";

/// Lifetime guard for one capability-relative cross-process catalog writer lock.
pub struct CatalogWriterGuard {
    _file: File,
}

/// Retained handle proving the fixed catalog path names a private capability-opened file.
pub struct CatalogFileGuard {
    file: File,
    location: CatalogLocation,
}

impl CatalogFileGuard {
    /// Clones the retained exact file capability for bounded content verification.
    pub fn try_clone_file(&self) -> Result<File, PathError> {
        self.file
            .try_clone()
            .map_err(|source| PathError::io("failed to clone opened catalog", source))
    }

    /// Revalidates the retained handle against the capability-relative catalog name.
    pub fn validate_identity(&self) -> Result<(), PathError> {
        use cap_fs_ext::MetadataExt as _;

        self.location.validate_for_open()?;
        let opened = self
            .file
            .metadata()
            .map_err(|source| PathError::io("failed to inspect opened catalog", source))?;
        let named = self
            .location
            .root_capability
            .symlink_metadata(CATALOG_FILE)
            .map_err(|source| PathError::io("failed to inspect named catalog", source))?;
        if !named.is_file() || (opened.dev(), opened.ino()) != (named.dev(), named.ino()) {
            return Err(PathError::PreparedRootChanged);
        }
        validate_private_catalog_metadata(&named)?;
        Ok(())
    }
}

impl fmt::Debug for CatalogFileGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CatalogFileGuard([PRIVATE FILE CAPABILITY])")
    }
}

impl fmt::Debug for CatalogWriterGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CatalogWriterGuard([LOCKED CAPABILITY])")
    }
}

/// Prepared placement capability for the local SQLite control-plane catalog.
#[derive(Clone)]
pub struct CatalogLocation {
    path: PathBuf,
    root: PathBuf,
    root_capability: Arc<Dir>,
}

impl CatalogLocation {
    pub(super) fn from_prepared(root: PathBuf, root_capability: Arc<Dir>) -> Self {
        Self {
            path: root.join("catalog.sqlite3"),
            root,
            root_capability,
        }
    }

    /// Returns the canonical catalog path bound to the retained prepared-root capability.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Opens or creates the fixed catalog capability-relative, without following links.
    ///
    /// Unix files must be owner-only. Windows opens retain no-delete sharing and reject reparse
    /// points; POSIX permission claims are intentionally not made there.
    pub fn prepare_catalog_file(&self) -> Result<CatalogFileGuard, PathError> {
        self.validate_for_open()?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        options.follow(FollowSymlinks::No);
        configure_private_catalog_creation(&mut options);
        let file = self
            .root_capability
            .open_with(CATALOG_FILE, &options)
            .map_err(|source| PathError::io("failed to open private catalog", source))?;
        let named = self
            .root_capability
            .symlink_metadata(CATALOG_FILE)
            .map_err(|source| PathError::io("failed to inspect private catalog", source))?;
        if !named.is_file() {
            return Err(PathError::PreparedRootChanged);
        }
        validate_private_catalog_metadata(&named)?;
        let guard = CatalogFileGuard {
            file: file.into_std(),
            location: self.clone(),
        };
        guard.validate_identity()?;
        Ok(guard)
    }

    /// Opens the existing fixed catalog capability-relative and without following links.
    pub fn open_catalog_file(&self) -> Result<CatalogFileGuard, PathError> {
        self.validate_for_open()?;
        let mut options = OpenOptions::new();
        options.read(true);
        options.follow(FollowSymlinks::No);
        configure_private_catalog_creation(&mut options);
        let file = self
            .root_capability
            .open_with(CATALOG_FILE, &options)
            .map_err(|source| PathError::io("failed to open existing catalog", source))?;
        let named = self
            .root_capability
            .symlink_metadata(CATALOG_FILE)
            .map_err(|source| PathError::io("failed to inspect existing catalog", source))?;
        if !named.is_file() {
            return Err(PathError::PreparedRootChanged);
        }
        validate_private_catalog_metadata(&named)?;
        let guard = CatalogFileGuard {
            file: file.into_std(),
            location: self.clone(),
        };
        guard.validate_identity()?;
        Ok(guard)
    }

    /// Verifies that the path-based SQLite VFS boundary still names the retained prepared root.
    ///
    /// Callers must invoke this immediately before and after opening the SQLite path. The retained
    /// directory is the identity authority; this check rejects ambient rename or substitution.
    pub fn validate_for_open(&self) -> Result<(), PathError> {
        use cap_fs_ext::MetadataExt as _;

        let expected = self
            .root_capability
            .dir_metadata()
            .map_err(|source| PathError::io("failed to inspect prepared root", source))?;
        let reopened = open_prepared_root(&self.root)?;
        let actual = reopened
            .dir_metadata()
            .map_err(|source| PathError::io("failed to reinspect prepared root", source))?;
        if (expected.dev(), expected.ino()) != (actual.dev(), actual.ino()) {
            return Err(PathError::PreparedRootChanged);
        }
        Ok(())
    }

    /// Acquires the no-follow cross-process writer lock relative to the prepared root capability.
    pub fn acquire_writer(&self) -> Result<CatalogWriterGuard, PathError> {
        self.validate_for_open()?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        options.follow(FollowSymlinks::No);
        configure_private_lock_creation(&mut options);
        let file = self
            .root_capability
            .open_with(WRITER_LOCK_FILE, &options)
            .map_err(|source| PathError::io("failed to open catalog writer lock", source))?;
        let metadata = file
            .metadata()
            .map_err(|source| PathError::io("failed to inspect catalog writer lock", source))?;
        if !metadata.is_file() {
            return Err(PathError::PreparedRootChanged);
        }
        let file = file.into_std();
        file.try_lock_exclusive().map_err(|source| {
            if source.kind() == std::io::ErrorKind::WouldBlock {
                PathError::CatalogAlreadyLocked
            } else {
                PathError::io("failed to acquire catalog writer lock", source)
            }
        })?;
        self.validate_for_open()?;
        Ok(CatalogWriterGuard { _file: file })
    }
}

#[cfg(unix)]
fn configure_private_lock_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(unix)]
fn configure_private_catalog_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(windows)]
fn configure_private_catalog_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_private_catalog_creation(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn validate_private_catalog_metadata(metadata: &cap_std::fs::Metadata) -> Result<(), PathError> {
    use cap_std::fs::PermissionsExt as _;

    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(PathError::PreparedRootChanged);
    }
    Ok(())
}

#[cfg(windows)]
fn validate_private_catalog_metadata(metadata: &cap_std::fs::Metadata) -> Result<(), PathError> {
    use cap_fs_ext::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(PathError::PreparedRootChanged);
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn validate_private_catalog_metadata(_metadata: &cap_std::fs::Metadata) -> Result<(), PathError> {
    Err(PathError::PreparedRootChanged)
}

#[cfg(windows)]
fn configure_private_lock_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_private_lock_creation(_options: &mut OpenOptions) {}

impl fmt::Debug for CatalogLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CatalogLocation([PREPARED LOCAL CAPABILITY])")
    }
}

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
