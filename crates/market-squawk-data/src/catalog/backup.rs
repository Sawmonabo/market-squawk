//! Exact-receipt SQLite backup preparation, publication, and verification.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;

use market_squawk_platform::CatalogLocation;
use rusqlite::limits::Limit;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::storage::{verify_integrity, verify_migration_identities};
use super::types::MAX_SQLITE_RECORD_BYTES;
use super::{Catalog, CatalogError, map_catalog_location_error};

const BACKUP_HASH_BUFFER_BYTES: usize = 64 * 1024;

mod restore;

pub(crate) use restore::{InstalledBackupCatalog, InstalledCatalogState, VerifiedBackupCatalog};

/// Exact, path-free identity for one verified SQLite backup payload.
#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackupReceipt {
    version: u16,
    byte_length: u64,
    sha256: [u8; 32],
}

impl BackupReceipt {
    /// Current durable receipt schema.
    pub const VERSION: u16 = 1;

    /// Reconstructs a persisted receipt after validating its bounded fields.
    pub fn try_from_parts(
        version: u16,
        byte_length: u64,
        sha256: [u8; 32],
    ) -> Result<Self, CatalogError> {
        let receipt = Self {
            version,
            byte_length,
            sha256,
        };
        receipt.validate()?;
        Ok(receipt)
    }

    /// Returns the durable receipt schema version.
    pub const fn version(&self) -> u16 {
        self.version
    }

    /// Returns the exact backup byte length.
    pub const fn byte_length(&self) -> u64 {
        self.byte_length
    }

    /// Returns the SHA-256 digest of the exact backup bytes.
    pub const fn sha256(&self) -> [u8; 32] {
        self.sha256
    }

    fn validate(&self) -> Result<(), CatalogError> {
        if self.version == Self::VERSION && self.byte_length > 0 {
            Ok(())
        } else {
            Err(CatalogError::BackupReceiptMismatch)
        }
    }
}

impl fmt::Debug for BackupReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackupReceipt")
            .field("version", &self.version)
            .field("byte_length", &self.byte_length)
            .field("sha256", &self.sha256)
            .finish()
    }
}

impl Catalog {
    /// Creates a new consistent SQLite backup without overwriting an existing path.
    ///
    /// The returned path-free receipt binds the exact byte length and SHA-256 digest. Windows
    /// publication is durable and no-clobber but does not claim atomic visibility.
    pub fn backup_to(&self, destination: &CatalogLocation) -> Result<BackupReceipt, CatalogError> {
        let _destination_writer = destination
            .acquire_writer()
            .map_err(map_catalog_location_error)?;
        destination
            .validate_for_open()
            .map_err(|_| CatalogError::UnsafePath)?;
        let mut target = PreparedBackup::new(destination.path())?;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let mut destination_connection = Connection::open_with_flags(&target.temporary, flags)?;
        destination
            .validate_for_open()
            .map_err(|_| CatalogError::UnsafePath)?;
        let backup = rusqlite::backup::Backup::new(&self.connection, &mut destination_connection)?;
        backup.run_to_completion(128, Duration::from_millis(10), None)?;
        drop(backup);
        verify_integrity(&destination_connection)?;
        destination_connection.close().map_err(|(_, error)| error)?;
        target.synchronize_temporary()?;
        target.publish(destination)?;
        target.receipt()
    }

    /// Verifies an existing backup against its exact receipt and compiled catalog identity.
    pub fn verify_backup(
        location: &CatalogLocation,
        receipt: &BackupReceipt,
    ) -> Result<(), CatalogError> {
        receipt.validate()?;
        let _writer = location
            .acquire_writer()
            .map_err(map_catalog_location_error)?;
        location
            .validate_for_open()
            .map_err(|_| CatalogError::UnsafePath)?;
        let guard = location
            .open_catalog_file()
            .map_err(map_catalog_location_error)?;
        let file = guard.try_clone_file().map_err(map_catalog_location_error)?;
        let identity = backup_file_identity(&file)?;
        verify_named_backup(location.path(), &identity, receipt)?;
        guard
            .validate_identity()
            .map_err(map_catalog_location_error)?;
        location
            .validate_for_open()
            .map_err(|_| CatalogError::UnsafePath)
    }
}

struct PreparedBackup {
    destination: PathBuf,
    temporary: PathBuf,
    parent: PathBuf,
    temporary_file: Option<File>,
    identity: BackupFileIdentity,
    receipt: Option<BackupReceipt>,
    published: bool,
}

impl PreparedBackup {
    fn new(destination: &Path) -> Result<Self, CatalogError> {
        let parent = destination.parent().ok_or(CatalogError::UnsafePath)?;
        let parent = parent.canonicalize()?;
        let file_name = destination.file_name().ok_or(CatalogError::UnsafePath)?;
        let destination = parent.join(file_name);
        match fs::symlink_metadata(&destination) {
            Ok(_) => return Err(CatalogError::BackupAlreadyExists),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        for _attempt in 0..8 {
            let temporary = parent.join(format!(
                ".market-squawk-backup-{}.tmp",
                uuid::Uuid::new_v4()
            ));
            let mut options = OpenOptions::new();
            options.read(true).write(true).create_new(true);
            configure_private_backup_creation(&mut options);
            match options.open(&temporary) {
                Ok(temporary_file) => {
                    if !temporary_file.metadata()?.is_file() {
                        return Err(CatalogError::UnsafePath);
                    }
                    let identity = backup_file_identity(&temporary_file)?;
                    return Ok(Self {
                        destination,
                        temporary,
                        parent,
                        temporary_file: Some(temporary_file),
                        identity,
                        receipt: None,
                        published: false,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(CatalogError::BackupTemporaryUnavailable)
    }

    fn synchronize_temporary(&mut self) -> Result<(), CatalogError> {
        let file = self
            .temporary_file
            .as_ref()
            .ok_or(CatalogError::UnsafePath)?;
        file.sync_all()?;
        if !path_has_backup_identity(&self.identity, &self.temporary)? {
            return Err(CatalogError::UnsafePath);
        }
        self.receipt = Some(receipt_for_file(file)?);
        #[cfg(windows)]
        drop(self.temporary_file.take());
        Ok(())
    }

    fn receipt(&self) -> Result<BackupReceipt, CatalogError> {
        self.receipt.ok_or(CatalogError::BackupReceiptMismatch)
    }

    fn publish(&mut self, location: &CatalogLocation) -> Result<(), CatalogError> {
        let receipt = self.receipt()?;
        location
            .validate_for_open()
            .map_err(|_| CatalogError::UnsafePath)?;
        match publish_backup_no_replace(&self.temporary, &self.destination) {
            Ok(()) => self.published = true,
            Err(error) => return self.reconcile_publication_error(location, &receipt, error),
        }
        synchronize_backup_publication(&self.parent).map_err(|_| backup_indeterminate(receipt))?;
        finalize_backup_temporary(self, receipt)?;
        verify_published_backup(location, &self.identity, &receipt)
            .map_err(|_| backup_indeterminate(receipt))?;
        Ok(())
    }

    #[cfg(windows)]
    fn reconcile_publication_error(
        &mut self,
        location: &CatalogLocation,
        receipt: &BackupReceipt,
        _error: std::io::Error,
    ) -> Result<(), CatalogError> {
        // Preserve a surviving prepared source on every ambiguous outcome. Never remove or alter
        // a destination that failed exact identity, receipt, or SQLite verification.
        self.published = true;
        let _source_is_exact =
            verify_named_backup(&self.temporary, &self.identity, receipt).is_ok();
        let _destination_is_exact =
            verify_published_backup(location, &self.identity, receipt).is_ok();
        let _location_is_safe = location.validate_for_open().is_ok();
        Err(backup_indeterminate(*receipt))
    }

    #[cfg(not(windows))]
    fn reconcile_publication_error(
        &mut self,
        _location: &CatalogLocation,
        receipt: &BackupReceipt,
        error: std::io::Error,
    ) -> Result<(), CatalogError> {
        if matches!(
            path_has_backup_identity(&self.identity, &self.destination),
            Ok(true)
        ) {
            self.published = true;
            return Err(backup_indeterminate(*receipt));
        }
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            Err(CatalogError::BackupAlreadyExists)
        } else {
            Err(error.into())
        }
    }
}

fn backup_indeterminate(receipt: BackupReceipt) -> CatalogError {
    CatalogError::BackupPublicationIndeterminate { receipt }
}

fn backup_cleanup_pending(receipt: BackupReceipt) -> CatalogError {
    CatalogError::BackupPublishedWithCleanupPending { receipt }
}

pub(super) fn receipt_for_file(file: &File) -> Result<BackupReceipt, CatalogError> {
    let expected_length = file.metadata()?.len();
    let mut reader = file.try_clone()?;
    reader.seek(SeekFrom::Start(0))?;
    let mut hasher = Sha256::new();
    let mut observed_length = 0_u64;
    let mut buffer = [0_u8; BACKUP_HASH_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        observed_length = observed_length
            .checked_add(u64::try_from(read).map_err(|_| CatalogError::BackupReceiptMismatch)?)
            .ok_or(CatalogError::BackupReceiptMismatch)?;
        hasher.update(&buffer[..read]);
    }
    if observed_length != expected_length || reader.metadata()?.len() != expected_length {
        return Err(CatalogError::BackupReceiptMismatch);
    }
    BackupReceipt::try_from_parts(
        BackupReceipt::VERSION,
        observed_length,
        hasher.finalize().into(),
    )
}

fn verify_named_backup(
    path: &Path,
    identity: &BackupFileIdentity,
    receipt: &BackupReceipt,
) -> Result<(), CatalogError> {
    receipt.validate()?;
    if !path_has_backup_identity(identity, path)? {
        return Err(CatalogError::BackupReceiptMismatch);
    }
    let mut options = OpenOptions::new();
    options.read(true);
    configure_backup_read(&mut options);
    let file = options.open(path)?;
    if backup_file_identity(&file)? != *identity || receipt_for_file(&file)? != *receipt {
        return Err(CatalogError::BackupReceiptMismatch);
    }
    verify_sqlite_backup(path)?;
    if receipt_for_file(&file)? != *receipt || !path_has_backup_identity(identity, path)? {
        return Err(CatalogError::BackupReceiptMismatch);
    }
    Ok(())
}

fn verify_published_backup(
    location: &CatalogLocation,
    identity: &BackupFileIdentity,
    receipt: &BackupReceipt,
) -> Result<(), CatalogError> {
    location
        .validate_for_open()
        .map_err(|_| CatalogError::UnsafePath)?;
    let guard = location
        .open_catalog_file()
        .map_err(map_catalog_location_error)?;
    verify_named_backup(location.path(), identity, receipt)?;
    guard
        .validate_identity()
        .map_err(map_catalog_location_error)?;
    location
        .validate_for_open()
        .map_err(|_| CatalogError::UnsafePath)
}

fn verify_sqlite_backup(path: &Path) -> Result<(), CatalogError> {
    let connection = open_immutable_backup(path)?;
    let sqlite_length_limit =
        i32::try_from(MAX_SQLITE_RECORD_BYTES).map_err(|_| CatalogError::InvalidConfiguration)?;
    connection.set_limit(Limit::SQLITE_LIMIT_LENGTH, sqlite_length_limit)?;
    connection.pragma_update(None, "trusted_schema", "OFF")?;
    verify_migration_identities(&connection)?;
    verify_integrity(&connection)?;
    connection.close().map_err(|(_, error)| error.into())
}

/// Opens receipt-retained backup bytes without contending with their whole-file advisory lease.
///
/// SQLite documents that `immutable=1` forces read-only access and skips its own file locking:
/// <https://www.sqlite.org/uri.html#recognized_query_parameters>. Callers must retain and
/// revalidate the exact file capability and receipt before and after using this connection.
pub(super) fn open_immutable_backup(path: &Path) -> Result<Connection, CatalogError> {
    let uri = immutable_backup_uri(path)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE
        | OpenFlags::SQLITE_OPEN_URI;
    Connection::open_with_flags(uri, flags).map_err(Into::into)
}

fn immutable_backup_uri(path: &Path) -> Result<String, CatalogError> {
    if !path.is_absolute() {
        return Err(CatalogError::UnsafePath);
    }
    let mut uri = String::from("file:");
    append_platform_uri_path(&mut uri, path)?;
    uri.push_str("?immutable=1&mode=ro&cache=private");
    Ok(uri)
}

#[cfg(unix)]
fn append_platform_uri_path(uri: &mut String, path: &Path) -> Result<(), CatalogError> {
    use std::os::unix::ffi::OsStrExt as _;

    append_percent_encoded_path(uri, path.as_os_str().as_bytes());
    Ok(())
}

#[cfg(windows)]
fn append_platform_uri_path(uri: &mut String, path: &Path) -> Result<(), CatalogError> {
    use std::path::{Component, Prefix};

    let mut components = path.components();
    let drive = match components.next() {
        // `fs::canonicalize` produces `VerbatimDisk` for local Windows paths.
        Some(Component::Prefix(prefix)) => match prefix.kind() {
            Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) if drive.is_ascii_alphabetic() => {
                drive
            }
            _ => return Err(CatalogError::UnsafePath),
        },
        _ => return Err(CatalogError::UnsafePath),
    };
    if !matches!(components.next(), Some(Component::RootDir)) {
        return Err(CatalogError::UnsafePath);
    }

    uri.push('/');
    uri.push(char::from(drive));
    uri.push_str(":/");
    let mut first = true;
    for component in components {
        let Component::Normal(component) = component else {
            return Err(CatalogError::UnsafePath);
        };
        let component = component.to_str().ok_or(CatalogError::UnsafePath)?;
        if !first {
            uri.push('/');
        }
        append_percent_encoded_path(uri, component.as_bytes());
        first = false;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn append_platform_uri_path(_uri: &mut String, _path: &Path) -> Result<(), CatalogError> {
    Err(CatalogError::UnsafePath)
}

fn append_percent_encoded_path(uri: &mut String, path: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for &byte in path {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'/' | b':') {
            uri.push(char::from(byte));
        } else {
            uri.push('%');
            uri.push(char::from(HEX[usize::from(byte >> 4)]));
            uri.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
}

#[cfg(unix)]
fn configure_backup_read(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt as _;

    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
}

#[cfg(windows)]
fn configure_backup_read(options: &mut OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_backup_read(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn configure_private_backup_creation(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(windows)]
fn configure_private_backup_creation(options: &mut OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_private_backup_creation(_options: &mut OpenOptions) {}

#[cfg(any(unix, windows))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BackupFileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(any(unix, windows))]
fn backup_file_identity(opened: &File) -> Result<BackupFileIdentity, std::io::Error> {
    use cap_fs_ext::MetadataExt as _;

    let opened = cap_std::fs::File::from_std(opened.try_clone()?).metadata()?;
    if !safe_backup_metadata(&opened) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "backup file identity is unsafe",
        ));
    }
    Ok(BackupFileIdentity {
        device: opened.dev(),
        inode: opened.ino(),
    })
}

#[cfg(any(unix, windows))]
fn path_has_backup_identity(
    identity: &BackupFileIdentity,
    path: &Path,
) -> Result<bool, std::io::Error> {
    use cap_fs_ext::MetadataExt as _;

    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "backup path has no parent directory",
        )
    })?;
    let name = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "backup path has no file name",
        )
    })?;
    let directory = cap_std::fs::Dir::open_ambient_dir(parent, cap_std::ambient_authority())?;
    let named = directory.symlink_metadata(name)?;
    Ok(safe_backup_metadata(&named)
        && identity.device == named.dev()
        && identity.inode == named.ino())
}

#[cfg(unix)]
fn safe_backup_metadata(metadata: &cap_std::fs::Metadata) -> bool {
    metadata.is_file()
}

#[cfg(windows)]
fn safe_backup_metadata(metadata: &cap_std::fs::Metadata) -> bool {
    use cap_std::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.is_file() && metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0
}

#[cfg(not(any(unix, windows)))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BackupFileIdentity;

#[cfg(not(any(unix, windows)))]
fn backup_file_identity(_opened: &File) -> Result<BackupFileIdentity, std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "backup file identity is unsupported",
    ))
}

#[cfg(not(any(unix, windows)))]
fn path_has_backup_identity(
    _identity: &BackupFileIdentity,
    _path: &Path,
) -> Result<bool, std::io::Error> {
    Ok(false)
}

impl Drop for PreparedBackup {
    fn drop(&mut self) {
        drop(self.temporary_file.take());
        if !self.published
            && matches!(
                path_has_backup_identity(&self.identity, &self.temporary),
                Ok(true)
            )
        {
            let _ignored = fs::remove_file(&self.temporary);
        }
    }
}

#[cfg(unix)]
fn publish_backup_no_replace(source: &Path, destination: &Path) -> Result<(), std::io::Error> {
    fs::hard_link(source, destination)
}

#[cfg(windows)]
fn publish_backup_no_replace(source: &Path, destination: &Path) -> Result<(), std::io::Error> {
    // This requests MoveFileExW WRITE_THROUGH without replacement or cross-volume copying. It is
    // a durable no-clobber primitive, not a claim that Windows guarantees atomic visibility.
    atomicwrites::move_atomic(source, destination)
}

#[cfg(not(any(unix, windows)))]
fn publish_backup_no_replace(_source: &Path, _destination: &Path) -> Result<(), std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "backup publication is unsupported",
    ))
}

#[cfg(unix)]
fn synchronize_backup_publication(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options.read(true);
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    options.open(path)?.sync_all()
}

#[cfg(windows)]
fn synchronize_backup_publication(_path: &Path) -> Result<(), std::io::Error> {
    // The publication primitive already requested WRITE_THROUGH. Windows has no corresponding
    // portable directory-fsync contract; exact file identity and SQLite integrity are checked
    // immediately after this step and again whenever the catalog is opened.
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn synchronize_backup_publication(_path: &Path) -> Result<(), std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "backup directory durability is unsupported",
    ))
}

#[cfg(unix)]
fn finalize_backup_temporary(
    target: &mut PreparedBackup,
    receipt: BackupReceipt,
) -> Result<(), CatalogError> {
    if !path_has_backup_identity(&target.identity, &target.temporary)
        .map_err(|_| backup_cleanup_pending(receipt))?
    {
        return Err(backup_cleanup_pending(receipt));
    }
    fs::remove_file(&target.temporary).map_err(|_| backup_cleanup_pending(receipt))?;
    synchronize_backup_publication(&target.parent).map_err(|_| backup_cleanup_pending(receipt))
}

#[cfg(windows)]
fn finalize_backup_temporary(
    target: &mut PreparedBackup,
    receipt: BackupReceipt,
) -> Result<(), CatalogError> {
    match fs::symlink_metadata(&target.temporary) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => Err(backup_cleanup_pending(receipt)),
    }
}

#[cfg(not(any(unix, windows)))]
fn finalize_backup_temporary(
    _target: &mut PreparedBackup,
    _receipt: BackupReceipt,
) -> Result<(), CatalogError> {
    Err(CatalogError::BackupDurabilityUnsupported)
}
