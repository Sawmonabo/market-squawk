//! Prepared SQLite catalog placement and root-identity validation.

use std::fmt;
use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use fs2::FileExt as _;
use sha2::{Digest as _, Sha256};

use super::PathError;

mod restore;

pub use restore::{CatalogRestoreStage, CatalogRestoreTarget, InstalledCatalogFile};

const WRITER_LOCK_FILE: &str = ".catalog.writer.lock";
const CATALOG_FILE: &str = "catalog.sqlite3";
const CATALOG_WAL_FILE: &str = "catalog.sqlite3-wal";
const CATALOG_SHM_FILE: &str = "catalog.sqlite3-shm";

/// Lifetime guard for one private, unique-link, capability-relative catalog writer lock.
pub struct CatalogWriterGuard {
    _file: File,
}

/// Retained handle proving the fixed catalog path names one private, unique-link file.
pub struct CatalogFileGuard {
    file: File,
    location: CatalogLocation,
}

/// Retained main-file and SQLite-sidecar observation bracketing one restore proof transaction.
pub struct CatalogRestoreScanGuard {
    main: RestoreFileObservation,
    wal: Option<RestoreFileObservation>,
    shm: Option<RestoreFileObservation>,
    location: CatalogLocation,
    max_main_bytes: u64,
    max_sidecar_bytes: u64,
}

struct RestoreFileObservation {
    file: File,
    name: &'static str,
    byte_length: u64,
    sha256: [u8; 32],
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
        validate_private_file_identity(&self.location, CATALOG_FILE, &self.file)
    }

    /// Rejects a durable SQLite WAL payload and unsafe WAL/shared-memory sidecar identities.
    ///
    /// Restore authority calls this only after an exclusive `TRUNCATE` checkpoint. The shared
    /// memory file may remain while the connection is open, but both sidecars must be private,
    /// unique-link regular files and the WAL must contain no bytes that could alter the retained
    /// main-file state.
    pub fn validate_checkpointed_sidecars(&self) -> Result<(), PathError> {
        self.validate_identity()?;
        validate_optional_sqlite_sidecar(&self.location, CATALOG_WAL_FILE, true)?;
        validate_optional_sqlite_sidecar(&self.location, CATALOG_SHM_FILE, false)?;
        self.validate_identity()
    }

    /// Retains exact main, WAL, and shared-memory identities and content around one logical scan.
    ///
    /// The caller must begin its SQLite read transaction before acquiring this guard and must
    /// revalidate the guard before ending that transaction. Sidecars are bounded before hashing.
    pub fn retain_restore_scan_state(
        &self,
        max_main_bytes: u64,
        max_sidecar_bytes: u64,
    ) -> Result<CatalogRestoreScanGuard, PathError> {
        if max_main_bytes == 0 || max_sidecar_bytes == 0 {
            return Err(PathError::PreparedRootChanged);
        }
        self.validate_identity()?;
        let main = RestoreFileObservation::capture(
            &self.location,
            CATALOG_FILE,
            self.try_clone_file()?,
            Some(max_main_bytes),
        )?;
        let wal =
            capture_optional_restore_file(&self.location, CATALOG_WAL_FILE, max_sidecar_bytes)?;
        let shm =
            capture_optional_restore_file(&self.location, CATALOG_SHM_FILE, max_sidecar_bytes)?;
        self.validate_identity()?;
        Ok(CatalogRestoreScanGuard {
            main,
            wal,
            shm,
            location: self.location.clone(),
            max_main_bytes,
            max_sidecar_bytes,
        })
    }
}

impl CatalogRestoreScanGuard {
    /// Proves durable main/WAL content plus every retained sidecar identity and size.
    ///
    /// SQLite may update shared-memory read marks while establishing a read snapshot, so callers
    /// use this narrower check across snapshot acquisition and a full [`Self::revalidate`] guard
    /// around the subsequent logical scan.
    pub fn revalidate_durable(&self) -> Result<(), PathError> {
        self.main
            .revalidate(&self.location, Some(self.max_main_bytes))?;
        revalidate_optional_restore_file(
            &self.location,
            CATALOG_WAL_FILE,
            self.wal.as_ref(),
            self.max_sidecar_bytes,
        )?;
        revalidate_optional_restore_file_identity(
            &self.location,
            CATALOG_SHM_FILE,
            self.shm.as_ref(),
            self.max_sidecar_bytes,
        )
    }

    /// Proves the retained names, file identities, lengths, and bytes did not change during scan.
    pub fn revalidate(&self) -> Result<(), PathError> {
        self.main
            .revalidate(&self.location, Some(self.max_main_bytes))?;
        revalidate_optional_restore_file(
            &self.location,
            CATALOG_WAL_FILE,
            self.wal.as_ref(),
            self.max_sidecar_bytes,
        )?;
        revalidate_optional_restore_file(
            &self.location,
            CATALOG_SHM_FILE,
            self.shm.as_ref(),
            self.max_sidecar_bytes,
        )
    }
}

impl RestoreFileObservation {
    fn capture(
        location: &CatalogLocation,
        name: &'static str,
        file: File,
        maximum_bytes: Option<u64>,
    ) -> Result<Self, PathError> {
        validate_private_file_identity(location, name, &file)?;
        let byte_length = file
            .metadata()
            .map_err(|source| PathError::io("failed to inspect SQLite restore file", source))?
            .len();
        if maximum_bytes.is_some_and(|maximum| byte_length > maximum) {
            return Err(PathError::PreparedRootChanged);
        }
        let sha256 = restore_file_sha256(&file)?;
        validate_private_file_identity(location, name, &file)?;
        Ok(Self {
            file,
            name,
            byte_length,
            sha256,
        })
    }

    fn revalidate(
        &self,
        location: &CatalogLocation,
        maximum_bytes: Option<u64>,
    ) -> Result<(), PathError> {
        validate_private_file_identity(location, self.name, &self.file)?;
        let byte_length = self
            .file
            .metadata()
            .map_err(|source| PathError::io("failed to reinspect SQLite restore file", source))?
            .len();
        if byte_length != self.byte_length
            || maximum_bytes.is_some_and(|maximum| byte_length > maximum)
            || restore_file_sha256(&self.file)? != self.sha256
        {
            return Err(PathError::PreparedRootChanged);
        }
        validate_private_file_identity(location, self.name, &self.file)
    }
}

fn capture_optional_restore_file(
    location: &CatalogLocation,
    name: &'static str,
    maximum_bytes: u64,
) -> Result<Option<RestoreFileObservation>, PathError> {
    match open_optional_restore_file(location, name) {
        Ok(Some(file)) => {
            RestoreFileObservation::capture(location, name, file, Some(maximum_bytes)).map(Some)
        }
        Ok(None) => Ok(None),
        Err(error) => Err(error),
    }
}

fn revalidate_optional_restore_file(
    location: &CatalogLocation,
    name: &'static str,
    expected: Option<&RestoreFileObservation>,
    maximum_bytes: u64,
) -> Result<(), PathError> {
    match (expected, open_optional_restore_file(location, name)?) {
        (None, None) => Ok(()),
        (Some(expected), Some(_)) => expected.revalidate(location, Some(maximum_bytes)),
        (None, Some(_)) | (Some(_), None) => Err(PathError::PreparedRootChanged),
    }
}

fn revalidate_optional_restore_file_identity(
    location: &CatalogLocation,
    name: &'static str,
    expected: Option<&RestoreFileObservation>,
    maximum_bytes: u64,
) -> Result<(), PathError> {
    match (expected, open_optional_restore_file(location, name)?) {
        (None, None) => Ok(()),
        (Some(expected), Some(_)) => {
            validate_private_file_identity(location, name, &expected.file)?;
            let byte_length = expected.file.metadata().map_err(|source| {
                PathError::io("failed to reinspect SQLite restore file", source)
            })?;
            if byte_length.len() > maximum_bytes {
                return Err(PathError::PreparedRootChanged);
            }
            validate_private_file_identity(location, name, &expected.file)
        }
        (None, Some(_)) | (Some(_), None) => Err(PathError::PreparedRootChanged),
    }
}

fn open_optional_restore_file(
    location: &CatalogLocation,
    name: &'static str,
) -> Result<Option<File>, PathError> {
    match location.root_capability.symlink_metadata(name) {
        Ok(metadata) => validate_private_file_metadata(&metadata)?,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(PathError::io(
                "failed to inspect SQLite restore sidecar",
                source,
            ));
        }
    }
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    configure_private_catalog_creation(&mut options);
    let file = location
        .root_capability
        .open_with(name, &options)
        .map_err(|source| PathError::io("failed to open SQLite restore sidecar", source))?
        .into_std();
    validate_private_file_identity(location, name, &file)?;
    Ok(Some(file))
}

fn restore_file_sha256(file: &File) -> Result<[u8; 32], PathError> {
    let mut file = file
        .try_clone()
        .map_err(|source| PathError::io("failed to clone SQLite restore file", source))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| PathError::io("failed to seek SQLite restore file", source))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| PathError::io("failed to hash SQLite restore file", source))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.finalize().into())
}

fn validate_optional_sqlite_sidecar(
    location: &CatalogLocation,
    name: &str,
    require_empty: bool,
) -> Result<(), PathError> {
    let named = match location.root_capability.symlink_metadata(name) {
        Ok(named) => named,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(PathError::io(
                "failed to inspect SQLite catalog sidecar",
                source,
            ));
        }
    };
    validate_private_file_metadata(&named)?;
    if require_empty && named.len() != 0 {
        return Err(PathError::PreparedRootChanged);
    }
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    configure_private_catalog_creation(&mut options);
    let opened = location
        .root_capability
        .open_with(name, &options)
        .map_err(|source| PathError::io("failed to open SQLite catalog sidecar", source))?
        .into_std();
    validate_private_file_identity(location, name, &opened)?;
    if require_empty
        && opened
            .metadata()
            .map_err(|source| PathError::io("failed to reinspect SQLite catalog sidecar", source))?
            .len()
            != 0
    {
        return Err(PathError::PreparedRootChanged);
    }
    Ok(())
}

impl fmt::Debug for CatalogFileGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CatalogFileGuard([PRIVATE FILE CAPABILITY])")
    }
}

impl fmt::Debug for CatalogRestoreScanGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CatalogRestoreScanGuard([RETAINED SQLITE FILE SET])")
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
    /// Catalogs must have exactly one hard link. Unix files must also be owner-only. Windows opens
    /// retain no-delete sharing and reject reparse points; POSIX permission claims are not made.
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
        validate_private_file_metadata(&named)?;
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
        validate_private_file_metadata(&named)?;
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
        let file = file.into_std();
        validate_private_file_identity(self, WRITER_LOCK_FILE, &file)?;
        file.try_lock_exclusive().map_err(|source| {
            if source.kind() == std::io::ErrorKind::WouldBlock {
                PathError::CatalogAlreadyLocked
            } else {
                PathError::io("failed to acquire catalog writer lock", source)
            }
        })?;
        validate_private_file_identity(self, WRITER_LOCK_FILE, &file)?;
        Ok(CatalogWriterGuard { _file: file })
    }
}

fn validate_private_file_identity(
    location: &CatalogLocation,
    name: &str,
    file: &File,
) -> Result<(), PathError> {
    validate_private_file_identity_with_links(location, name, file, 1)
}

fn validate_private_file_identity_with_links(
    location: &CatalogLocation,
    name: &str,
    file: &File,
    expected_links: u64,
) -> Result<(), PathError> {
    use cap_fs_ext::MetadataExt as _;

    location.validate_for_open()?;
    let opened = cap_std::fs::File::from_std(
        file.try_clone()
            .map_err(|source| PathError::io("failed to clone catalog control file", source))?,
    )
    .metadata()
    .map_err(|source| PathError::io("failed to inspect opened catalog control file", source))?;
    let named = location
        .root_capability
        .symlink_metadata(name)
        .map_err(|source| PathError::io("failed to inspect named catalog control file", source))?;
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
fn validate_private_file_metadata(metadata: &cap_std::fs::Metadata) -> Result<(), PathError> {
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
fn validate_private_file_metadata(metadata: &cap_std::fs::Metadata) -> Result<(), PathError> {
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
fn validate_private_file_metadata(_metadata: &cap_std::fs::Metadata) -> Result<(), PathError> {
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
