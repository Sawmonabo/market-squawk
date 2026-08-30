//! Durable single-writer SQLite catalog lifecycle and recovery.

mod authority;
mod backup;
mod company_identity;
mod company_security;
mod diagnostics;
mod evidence;
mod fair_value;
mod listing_reference;
mod market_data_instruments;
mod migration_preflight;
mod observed_revisions;
mod official_options_reference;
mod onboarding;
mod provider_capture;
mod provider_event;
mod provider_logical;
mod provider_option;
mod publication;
mod query_artifacts;
mod records;
mod restore_logical;
mod runs;
mod search;
mod sec_fund_job;
mod storage;
mod types;

use market_squawk_platform::{CatalogFileGuard, CatalogWriterGuard, PathError};
use rusqlite::limits::Limit;
use rusqlite::{Connection, OpenFlags};

pub(crate) use self::authority::exact_catalog_file_binding;
pub use self::backup::BackupReceipt;
pub(crate) use self::backup::{
    InstalledBackupCatalog, InstalledCatalogState, VerifiedBackupCatalog,
};
pub use self::company_identity::{
    CompanyIdentityExactRecord, CompanyIdentityMatchKind, CompanyIdentityMatchReason,
    CompanyIdentitySearchMatch, CompanyIdentitySearchPage,
};
pub use self::company_security::{
    CompanySecurityIdentityCatalogError, CompanySecurityIdentityDisposition,
    CompanySecurityIdentityExclusion, CompanySecurityIdentityExclusionReason,
    CompanySecurityIdentityQuery, CompanySecurityIdentityReadCapability,
    CompanySecurityIdentityRecord, CompanySecurityIdentitySelection,
    CompanySecurityIdentitySelectionReceipt, CompanySecurityLinkPublicationCapability,
    CompanySecurityLinkPublicationDisposition, CompanySecurityLinkPublicationReceipt,
    CompanySecuritySelectionReceiptEntry, MAX_COMPANY_SECURITY_SELECTION_ROWS,
    SecFundamentalIdentityAvailability, SecFundamentalIdentityQuery,
    SecFundamentalIdentitySelection,
};
pub use self::diagnostics::{CatalogDiagnosticSnapshot, ProviderOnboardingDiagnostic};
pub use self::fair_value::{
    FairValueCatalogAuditEvent, FairValueCatalogCommit, FairValueCatalogLink,
    FairValueCatalogOperation, FairValueCatalogPosition, FairValueCatalogRecord,
    FairValueCatalogSnapshot, FairValueCatalogSnapshotLimits, FairValueCommitDisposition,
    FairValueLinkRelation, FairValueOperationKind, FairValueRecordKind,
};
pub use self::listing_reference::{
    ListingReferenceDirectoryPresence, ListingReferenceError, ListingReferenceExchangeCode,
    ListingReferenceFileEvidence, ListingReferenceFileKind, ListingReferenceFinancialStatus,
    ListingReferenceGenerationInput, ListingReferenceGenerationReceipt,
    ListingReferenceGenerationSelection, ListingReferenceMarketCategory, ListingReferenceMatchKind,
    ListingReferenceMembershipCursor, ListingReferenceMembershipPage,
    ListingReferenceMembershipPageState, ListingReferenceMembershipSelectionReceipt,
    ListingReferencePublicationCapability, ListingReferencePublicationDisposition,
    ListingReferencePublicationReceipt, ListingReferenceReadCapability, ListingReferenceRecord,
    ListingReferenceRecordInput, ListingReferenceRightsState, ListingReferenceSearchMatch,
    ListingReferenceSearchPage, ListingReferenceSourceFileInput,
    MAX_LISTING_REFERENCE_MEMBERSHIP_PAGE_ROWS, MAX_LISTING_REFERENCE_RECORDS,
    MAX_LISTING_REFERENCE_SEARCH_ROWS,
};
pub use self::market_data_instruments::{
    MAX_MARKET_DATA_INSTRUMENT_POPULATION_ROWS, MAX_MARKET_DATA_INSTRUMENT_SEARCH_ROWS,
    MAX_MARKET_DATA_INSTRUMENT_SYNC_ROWS, MarketDataInstrumentCatalogError,
    MarketDataInstrumentMatchKind, MarketDataInstrumentPopulationDisposition,
    MarketDataInstrumentPopulationExclusion, MarketDataInstrumentPopulationExclusionReason,
    MarketDataInstrumentPopulationQuery, MarketDataInstrumentPopulationSelection,
    MarketDataInstrumentReadCapability, MarketDataInstrumentRecord,
    MarketDataInstrumentSearchMatch, MarketDataInstrumentSearchPage,
    MarketDataInstrumentSynchronization, MarketDataInstrumentSynchronizationCapability,
    MarketDataInstrumentSynchronizationReceipt,
};
pub use self::official_options_reference::{
    MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_ASSERTIONS,
    MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_RESOLUTIONS,
    MAX_OFFICIAL_OPTIONS_REFERENCE_CANONICAL_CANDIDATES, MAX_OFFICIAL_OPTIONS_REFERENCE_CONFLICTS,
    MAX_OFFICIAL_OPTIONS_REFERENCE_EXACT_ROWS, MAX_OFFICIAL_OPTIONS_REFERENCE_OBJECTS,
    MAX_OFFICIAL_OPTIONS_REFERENCE_RECORDS, MAX_OFFICIAL_OPTIONS_REFERENCE_SEARCH_ROWS,
    MAX_OFFICIAL_OPTIONS_REFERENCE_STRICT_ROWS, OfficialOptionsReferenceAliasAssertionSetBuilder,
    OfficialOptionsReferenceAliasAssertionSetEvidence, OfficialOptionsReferenceAliasKey,
    OfficialOptionsReferenceAliasResolutionInput, OfficialOptionsReferenceAliasResolutionState,
    OfficialOptionsReferenceAmbiguity, OfficialOptionsReferenceCanonicalCandidate,
    OfficialOptionsReferenceCanonicalMatchKind, OfficialOptionsReferenceCanonicalResolution,
    OfficialOptionsReferenceCboeSeries, OfficialOptionsReferenceConflict,
    OfficialOptionsReferenceConflictInput, OfficialOptionsReferenceConflictKind,
    OfficialOptionsReferenceConflictSetDigestBuilder, OfficialOptionsReferenceConflictSetEvidence,
    OfficialOptionsReferenceError, OfficialOptionsReferenceExactIdentity,
    OfficialOptionsReferenceGenerationHeader, OfficialOptionsReferenceGenerationReceipt,
    OfficialOptionsReferenceGenerationSelection, OfficialOptionsReferenceIdentityQuery,
    OfficialOptionsReferenceIdentityResolution, OfficialOptionsReferenceObjectEvidence,
    OfficialOptionsReferenceObjectInput, OfficialOptionsReferenceObjectInputFields,
    OfficialOptionsReferenceOccExchangeListingEvidence, OfficialOptionsReferenceOccPositionLimit,
    OfficialOptionsReferenceOccProduct, OfficialOptionsReferenceOccProductType,
    OfficialOptionsReferenceProvider, OfficialOptionsReferencePublicationCapability,
    OfficialOptionsReferencePublicationDisposition, OfficialOptionsReferencePublicationReceipt,
    OfficialOptionsReferenceReadCapability, OfficialOptionsReferenceRecord,
    OfficialOptionsReferenceRecordInput, OfficialOptionsReferenceRecordSetDigestBuilder,
    OfficialOptionsReferenceRecordSetEvidence, OfficialOptionsReferenceRecordValue,
    OfficialOptionsReferenceResolutionSetDigestBuilder,
    OfficialOptionsReferenceResolutionSetEvidence, OfficialOptionsReferenceSearchPage,
    OfficialOptionsReferenceSourceAuthority, OfficialOptionsReferenceSourceEvidence,
    OfficialOptionsReferenceSurface,
};
pub use self::onboarding::{
    OnboardingAppendOutcome, OnboardingReservation, OnboardingReservationRequest,
    ResumedProviderOnboarding,
};
pub(crate) use self::storage::trusted_catalog_now;
use self::storage::{
    apply_migrations, initialize_catalog_identity, pragma_bool, prepare_local_path,
};
pub(crate) use self::storage::{verify_integrity, verify_migration_identities};
use self::types::WriterPermit;
pub use self::types::{
    ArtifactRecord, AuditEvent, Catalog, CatalogConfig, CatalogError, CatalogHealth, CatalogLimit,
    CatalogResultLimits, ContractCompletion, DatasetManifestRecord, IngestReservation,
    IngestRunRecord, IngestRunState, PinnedInstrumentDefinitions, ReferenceBundle, SourceCursor,
};
pub(crate) use observed_revisions::CatalogObservedRevisionAuthority;
pub use observed_revisions::StoredObservedRevision;
pub(crate) use provider_capture::{
    MAX_PROVIDER_CAPTURE_PHYSICAL_BYTES, MAX_PROVIDER_CAPTURE_PHYSICAL_CLAIMS,
    PROVIDER_CAPTURE_RECOVERY_ENTRY_BUDGET, PreparedProviderCaptureBinding,
    load_provider_capture_for_run, retain_prepared_provider_capture_binding,
};
pub use provider_capture::{
    PersistedProviderCaptureBindingEvidence, PersistedProviderCaptureBindingRow,
    PersistedProviderCapturePhysicalClaim, PersistedProviderNativeLineageSchema,
};
pub use provider_event::{
    PersistedProviderEventBindingEvidence, PersistedProviderEventBindingRow,
    PersistedProviderEventNativeLineage, PersistedProviderPublicationEvidence,
    PersistedProviderResponseMarketEventBindingEvidence,
    PersistedProviderResponseMarketEventBindingRow,
};
pub(crate) use provider_event::{
    PreparedProviderPublicationBinding, retain_prepared_provider_publication_binding,
};
pub(crate) use provider_logical::retain_sealed_provider_logical_publication_binding;
pub use provider_logical::{
    PersistedProviderLogicalGenerationBinding, PersistedProviderLogicalObjectClaim,
    PersistedProviderLogicalPartitionClaim, PersistedProviderLogicalPublicationBinding,
};
pub use provider_option::{
    PersistedProviderOptionMarketBindingEvidence, PersistedProviderOptionMarketBindingRow,
    PersistedProviderOptionMarketNativeLineage,
};
pub(crate) use provider_option::{
    PreparedProviderOptionMarketBinding, retain_prepared_provider_option_market_binding,
};
pub use publication::PublishedIngest;
pub(crate) use publication::{PublicationSourceEvidence, publish_artifact_manifest_in_transaction};
#[cfg(test)]
pub(crate) use query_artifacts::QueryArtifactBindCheckpoint;
pub(crate) use query_artifacts::QueryArtifactPublisher;
pub use query_artifacts::{
    QueryArtifactReservation, QueryArtifactReservationInput, QueryArtifactResult,
};
pub(crate) use restore_logical::RestoreCatalogBaseline;
pub(crate) use runs::complete_ingest_in_transaction;
pub use runs::{CatalogAuthority, ResumedIngest};
pub use search::{InstrumentSearchMatch, InstrumentSearchPage};
pub use sec_fund_job::{
    MAX_SEC_FUND_POINT_IN_TIME_CANDIDATES, MAX_SEC_FUND_POINT_IN_TIME_RETAINED_BYTES,
    SecFundJobCatalogCapability, SecFundJobCatalogError, SecFundJobCommit, SecFundJobCoordinate,
    SecFundJobDurablePublication, SecFundJobFamily, SecFundJobPointInTimeSelection,
    SecFundJobRecovery, SecFundPointInTimeReadOutcome, SecFundPointInTimeReadRequest,
};

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
        Self::open_with_capabilities(config, cross_process_writer, catalog_file, true)
    }

    pub(super) fn open_installed(
        config: CatalogConfig,
        installed: InstalledBackupCatalog,
    ) -> Result<(Self, InstalledCatalogState), CatalogError> {
        let (installed, location, _receipt, state) = installed.into_parts();
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
        Self::open_with_capabilities(config, cross_process_writer, catalog_file, false)
            .map(|catalog| (catalog, state))
    }

    fn open_with_capabilities(
        config: CatalogConfig,
        cross_process_writer: CatalogWriterGuard,
        catalog_file: CatalogFileGuard,
        initialize: bool,
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
        connection.busy_timeout(config.busy_timeout)?;
        connection.pragma_update(None, "trusted_schema", "OFF")?;
        catalog_file
            .validate_identity()
            .map_err(map_catalog_location_error)?;
        let artifact_root_binding = exact_catalog_file_binding(
            &catalog_file
                .try_clone_file()
                .map_err(map_catalog_location_error)?,
            &path,
        )?;
        if initialize {
            initialize_catalog_identity(&connection)?;
        } else {
            verify_migration_identities(&connection)?;
            verify_integrity(&connection)?;
        }
        config
            .location
            .validate_for_open()
            .map_err(|_| CatalogError::UnsafePath)?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "wal_autocheckpoint", 1_000_i64)?;
        let journal_mode: String = if initialize {
            connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?
        } else {
            connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?
        };
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(CatalogError::UnsafeJournalMode);
        }
        if initialize {
            apply_migrations(&mut connection, artifact_root_binding)?;
        } else {
            verify_migration_identities(&connection)?;
        }
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

    pub(crate) fn checkpoint_restore_state(&self) -> Result<BackupReceipt, CatalogError> {
        let (busy, log, checkpointed): (i64, i64, i64) =
            self.connection
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })?;
        if busy != 0 || log != 0 || checkpointed != 0 {
            return Err(CatalogError::BackupRestoreConflict);
        }
        self._catalog_file
            .validate_checkpointed_sidecars()
            .map_err(map_catalog_location_error)?;
        let file = self
            ._catalog_file
            .try_clone_file()
            .map_err(map_catalog_location_error)?;
        self._catalog_file
            .validate_identity()
            .map_err(map_catalog_location_error)?;
        self.integrity_check()?;
        self._catalog_file
            .validate_checkpointed_sidecars()
            .map_err(map_catalog_location_error)?;
        self::backup::receipt_for_file(&file)
    }

    pub(crate) fn acquire_restore_exclusive_locking(&self) -> Result<(), CatalogError> {
        // SQLite retains locks after a transaction in exclusive locking mode. Restore releases
        // this mode only after the exact Bound state and artifact root are activated. See:
        // https://www.sqlite.org/pragma.html#pragma_locking_mode
        let mode: String =
            self.connection
                .query_row("PRAGMA main.locking_mode=EXCLUSIVE", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("exclusive") {
            return Err(CatalogError::BackupRestoreConflict);
        }
        self.connection.execute_batch("BEGIN EXCLUSIVE; COMMIT;")?;
        let retained: String =
            self.connection
                .query_row("PRAGMA main.locking_mode", [], |row| row.get(0))?;
        if !retained.eq_ignore_ascii_case("exclusive") {
            return Err(CatalogError::BackupRestoreConflict);
        }
        Ok(())
    }

    pub(crate) fn release_restore_exclusive_locking(&self) -> Result<(), CatalogError> {
        let mode: String =
            self.connection
                .query_row("PRAGMA main.locking_mode=NORMAL", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("normal") {
            return Err(CatalogError::BackupRestoreConflict);
        }
        self.connection
            .query_row("SELECT rootpage FROM sqlite_schema LIMIT 1", [], |_| Ok(()))?;
        let released: String =
            self.connection
                .query_row("PRAGMA main.locking_mode", [], |row| row.get(0))?;
        if !released.eq_ignore_ascii_case("normal") {
            return Err(CatalogError::BackupRestoreConflict);
        }
        Ok(())
    }

    pub(crate) fn revalidate_restore_state(
        &self,
        expected: BackupReceipt,
    ) -> Result<(), CatalogError> {
        self._catalog_file
            .validate_checkpointed_sidecars()
            .map_err(map_catalog_location_error)?;
        let file = self
            ._catalog_file
            .try_clone_file()
            .map_err(map_catalog_location_error)?;
        if self::backup::receipt_for_file(&file)? != expected {
            return Err(CatalogError::BackupRestoreConflict);
        }
        self.integrity_check()?;
        self._catalog_file
            .validate_checkpointed_sidecars()
            .map_err(map_catalog_location_error)
    }
}

pub(super) fn map_catalog_location_error(error: PathError) -> CatalogError {
    if matches!(error, PathError::CatalogAlreadyLocked) {
        CatalogError::WriterAlreadyOpen
    } else {
        CatalogError::UnsafePath
    }
}
