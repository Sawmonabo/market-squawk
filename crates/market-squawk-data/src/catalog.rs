//! Durable single-writer SQLite catalog lifecycle and recovery.

mod publication;
mod query_artifacts;
mod records;
mod runs;
mod storage;
mod types;

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;

use market_squawk_platform::{CatalogLocation, PathError};
use rusqlite::limits::Limit;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use self::storage::{
    apply_migrations, initialize_catalog_identity, pragma_bool, prepare_local_path,
    verify_integrity, verify_migration_identities,
};
pub use self::types::{
    ArtifactRecord, AuditEvent, Catalog, CatalogConfig, CatalogError, CatalogHealth, CatalogLimit,
    CatalogResultLimits, ContractCompletion, DatasetManifestRecord, IngestReservation,
    IngestRunRecord, IngestRunState, ReferenceBundle, SourceCursor,
};
use self::types::{MAX_SQLITE_RECORD_BYTES, WriterPermit};
pub use publication::PublishedIngest;
pub(crate) use query_artifacts::QueryArtifactPublisher;
pub use query_artifacts::{
    QueryArtifactReservation, QueryArtifactReservationInput, QueryArtifactResult,
};
pub use runs::{CatalogAuthority, ResumedIngest};

const BACKUP_HASH_BUFFER_BYTES: usize = 64 * 1024;

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
    /// Opens, hardens, migrates, and verifies a local SQLite catalog.
    pub(super) fn open(config: CatalogConfig) -> Result<Self, CatalogError> {
        let cross_process_writer = config
            .location
            .acquire_writer()
            .map_err(map_catalog_location_error)?;
        let catalog_file = config
            .location
            .prepare_catalog_file()
            .map_err(map_catalog_location_error)?;
        config
            .location
            .validate_for_open()
            .map_err(|_| CatalogError::UnsafePath)?;
        let path = prepare_local_path(config.location.path())?;
        let writer_permit = WriterPermit::acquire(path.clone())?;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let mut connection = Connection::open_with_flags(&path, flags)?;
        let sqlite_length_limit = i32::try_from(config.result_bytes.max_record_bytes())
            .map_err(|_| CatalogError::InvalidConfiguration)?;
        connection.set_limit(Limit::SQLITE_LIMIT_LENGTH, sqlite_length_limit)?;
        catalog_file
            .validate_identity()
            .map_err(map_catalog_location_error)?;
        initialize_catalog_identity(&connection)?;
        config
            .location
            .validate_for_open()
            .map_err(|_| CatalogError::UnsafePath)?;
        connection.busy_timeout(config.busy_timeout)?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "trusted_schema", "OFF")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "wal_autocheckpoint", 1_000_i64)?;
        let journal_mode: String =
            connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(CatalogError::UnsafeJournalMode);
        }
        apply_migrations(&mut connection)?;
        verify_integrity(&connection)?;
        let artifact_root_binding = exact_catalog_file_binding(
            &catalog_file
                .try_clone_file()
                .map_err(map_catalog_location_error)?,
            &path,
        )?;
        Ok(Self {
            connection,
            _catalog_file: catalog_file,
            _cross_process_writer: cross_process_writer,
            _writer_permit: writer_permit,
            busy_timeout: config.busy_timeout,
            max_result_rows: config.max_result_rows,
            result_bytes: config.result_bytes,
            catalog_id: uuid::Uuid::new_v4(),
            catalog_path: path,
            artifact_root_binding,
        })
    }

    /// Returns defensive connection state and migration count.
    pub fn health(&self) -> Result<CatalogHealth, CatalogError> {
        let journal_mode = self
            .connection
            .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))?
            .to_ascii_lowercase();
        let foreign_keys = pragma_bool(&self.connection, "PRAGMA foreign_keys")?;
        let trusted_schema = pragma_bool(&self.connection, "PRAGMA trusted_schema")?;
        let synchronous = self
            .connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        let count: i64 =
            self.connection
                .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                    row.get(0)
                })?;
        let applied_migrations = u32::try_from(count).map_err(|_| CatalogError::CorruptCatalog)?;
        Ok(CatalogHealth {
            journal_mode,
            foreign_keys,
            trusted_schema,
            synchronous,
            busy_timeout: self.busy_timeout,
            applied_migrations,
        })
    }

    /// Runs SQLite integrity and foreign-key checks.
    pub fn integrity_check(&self) -> Result<(), CatalogError> {
        verify_integrity(&self.connection)
    }

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

#[cfg(unix)]
fn exact_catalog_file_binding(file: &File, path: &Path) -> Result<[u8; 32], CatalogError> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = file.metadata()?;
    Ok(hash_catalog_file_identity(
        path,
        metadata.dev(),
        metadata.ino(),
    ))
}

#[cfg(windows)]
fn exact_catalog_file_binding(file: &File, path: &Path) -> Result<[u8; 32], CatalogError> {
    use std::os::windows::fs::MetadataExt as _;

    let metadata = file.metadata()?;
    let volume = metadata
        .volume_serial_number()
        .ok_or(CatalogError::UnsafePath)?;
    let index = metadata.file_index().ok_or(CatalogError::UnsafePath)?;
    Ok(hash_catalog_file_identity(path, u64::from(volume), index))
}

#[cfg(not(any(unix, windows)))]
fn exact_catalog_file_binding(_file: &File, _path: &Path) -> Result<[u8; 32], CatalogError> {
    Err(CatalogError::UnsafePath)
}

fn hash_catalog_file_identity(path: &Path, device: u64, inode: u64) -> [u8; 32] {
    let path = path.as_os_str().as_encoded_bytes();
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/catalog-artifact-root-binding/v1");
    digest.update(u64::try_from(path.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(path);
    digest.update(device.to_be_bytes());
    digest.update(inode.to_be_bytes());
    digest.finalize().into()
}

fn map_catalog_location_error(error: PathError) -> CatalogError {
    if matches!(error, PathError::CatalogAlreadyLocked) {
        CatalogError::WriterAlreadyOpen
    } else {
        CatalogError::UnsafePath
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

fn receipt_for_file(file: &File) -> Result<BackupReceipt, CatalogError> {
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
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    let connection = Connection::open_with_flags(path, flags)?;
    let sqlite_length_limit =
        i32::try_from(MAX_SQLITE_RECORD_BYTES).map_err(|_| CatalogError::InvalidConfiguration)?;
    connection.set_limit(Limit::SQLITE_LIMIT_LENGTH, sqlite_length_limit)?;
    connection.pragma_update(None, "trusted_schema", "OFF")?;
    verify_migration_identities(&connection)?;
    verify_integrity(&connection)?;
    connection.close().map_err(|(_, error)| error.into())
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

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BackupFileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
fn backup_file_identity(opened: &File) -> Result<BackupFileIdentity, std::io::Error> {
    use std::os::unix::fs::MetadataExt as _;

    let opened = opened.metadata()?;
    Ok(BackupFileIdentity {
        device: opened.dev(),
        inode: opened.ino(),
    })
}

#[cfg(unix)]
fn path_has_backup_identity(
    identity: &BackupFileIdentity,
    path: &Path,
) -> Result<bool, std::io::Error> {
    use std::os::unix::fs::MetadataExt as _;

    let named = fs::symlink_metadata(path)?;
    Ok(named.is_file() && identity.device == named.dev() && identity.inode == named.ino())
}

#[cfg(windows)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BackupFileIdentity {
    volume: u32,
    index: u64,
}

#[cfg(windows)]
fn backup_file_identity(opened: &File) -> Result<BackupFileIdentity, std::io::Error> {
    use std::os::windows::fs::MetadataExt as _;

    let opened = opened.metadata()?;
    let volume = opened.volume_serial_number().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "missing backup volume identity",
        )
    })?;
    let index = opened.file_index().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "missing backup file identity",
        )
    })?;
    Ok(BackupFileIdentity { volume, index })
}

#[cfg(windows)]
fn path_has_backup_identity(
    identity: &BackupFileIdentity,
    path: &Path,
) -> Result<bool, std::io::Error> {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    let named = fs::symlink_metadata(path)?;
    Ok(named.is_file()
        && named.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0
        && Some(identity.volume) == named.volume_serial_number()
        && Some(identity.index) == named.file_index())
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
