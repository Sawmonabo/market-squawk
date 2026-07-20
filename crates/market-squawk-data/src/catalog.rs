//! Durable single-writer SQLite catalog lifecycle and recovery.

mod authority;
mod backup;
mod evidence;
mod publication;
mod query_artifacts;
mod records;
mod runs;
mod storage;
mod types;

use market_squawk_platform::{CatalogFileGuard, CatalogWriterGuard, PathError};
use rusqlite::limits::Limit;
use rusqlite::{Connection, OpenFlags};

use self::authority::exact_catalog_file_binding;
pub use self::backup::BackupReceipt;
pub(crate) use self::backup::{InstalledBackupCatalog, VerifiedBackupCatalog};
use self::storage::{
    apply_migrations, initialize_catalog_identity, pragma_bool, prepare_local_path,
    verify_integrity,
};
use self::types::WriterPermit;
pub use self::types::{
    ArtifactRecord, AuditEvent, Catalog, CatalogConfig, CatalogError, CatalogHealth, CatalogLimit,
    CatalogResultLimits, ContractCompletion, DatasetManifestRecord, IngestReservation,
    IngestRunRecord, IngestRunState, ReferenceBundle, SourceCursor,
};
pub use publication::PublishedIngest;
pub(crate) use query_artifacts::QueryArtifactPublisher;
pub use query_artifacts::{
    QueryArtifactReservation, QueryArtifactReservationInput, QueryArtifactResult,
};
pub use runs::{CatalogAuthority, ResumedIngest};

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
        Self::open_with_capabilities(config, cross_process_writer, catalog_file)
    }

    pub(super) fn open_installed(
        config: CatalogConfig,
        installed: InstalledBackupCatalog,
    ) -> Result<Self, CatalogError> {
        let (installed, location, _receipt) = installed.into_parts();
        if config.location.path() != location.path() {
            return Err(CatalogError::UnsafePath);
        }
        config
            .location
            .validate_for_open()
            .map_err(map_catalog_location_error)?;
        location
            .validate_for_open()
            .map_err(map_catalog_location_error)?;
        let (catalog_file, cross_process_writer) = installed.into_parts();
        Self::open_with_capabilities(config, cross_process_writer, catalog_file)
    }

    fn open_with_capabilities(
        config: CatalogConfig,
        cross_process_writer: CatalogWriterGuard,
        catalog_file: CatalogFileGuard,
    ) -> Result<Self, CatalogError> {
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
        let artifact_root_binding = exact_catalog_file_binding(
            &catalog_file
                .try_clone_file()
                .map_err(map_catalog_location_error)?,
            &path,
        )?;
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
        apply_migrations(&mut connection, artifact_root_binding)?;
        verify_integrity(&connection)?;
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
}

pub(super) fn map_catalog_location_error(error: PathError) -> CatalogError {
    if matches!(error, PathError::CatalogAlreadyLocked) {
        CatalogError::WriterAlreadyOpen
    } else {
        CatalogError::UnsafePath
    }
}
