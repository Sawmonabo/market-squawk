//! Independent, endpoint-bound read transactions over the live WAL catalog.

use std::fmt;
use std::time::{Duration, Instant};

use market_squawk_domain::{EvidenceDigest, MarketEvent};
use market_squawk_platform::{CatalogFileGuard, CatalogLocation};
use market_squawk_sources::ProviderIdentitySelectionEvidence;
use rusqlite::limits::Limit;
use rusqlite::{Connection, OpenFlags};
use tokio_util::sync::CancellationToken;

use super::market_data_instruments::{
    MarketDataInstrumentCatalogError, MarketDataInstrumentRecord,
    verify_provider_identity_evidence_with_limits,
};
use super::provider_event::{
    PersistedProviderPublicationEvidence, ProviderMarketEventSelectionCandidate,
    load_provider_publication_evidence, provider_market_event_selection_for_publication,
    validate_provider_market_event_metadata,
};
use super::storage::CATALOG_APPLICATION_ID;
use super::{
    Catalog, CatalogError, CatalogResultLimits, exact_catalog_file_binding,
    map_catalog_location_error, prepare_local_path, verify_migration_identities,
};

const SQLITE_PROGRESS_OPERATIONS: i32 = 1_000;

/// A private read connection; it carries no writer or publication authority.
pub(crate) struct CatalogReadSnapshot {
    connection: Connection,
    catalog_file: CatalogFileGuard,
    location: CatalogLocation,
    result_limits: CatalogResultLimits,
    deadline: Instant,
    cancellation: CancellationToken,
}

impl fmt::Debug for CatalogReadSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogReadSnapshot")
            .finish_non_exhaustive()
    }
}

impl Catalog {
    pub(crate) const fn read_result_limits(&self) -> CatalogResultLimits {
        self.result_bytes
    }
}

impl CatalogReadSnapshot {
    pub(crate) fn open(
        location: &CatalogLocation,
        expected_binding: [u8; 32],
        result_limits: CatalogResultLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, CatalogError> {
        check_control(deadline, cancellation)?;
        location
            .validate_for_open()
            .map_err(map_catalog_location_error)?;
        let catalog_file = location
            .open_catalog_file()
            .map_err(map_catalog_location_error)?;
        let path = prepare_local_path(location.path())?;
        let binding = exact_catalog_file_binding(
            &catalog_file
                .try_clone_file()
                .map_err(map_catalog_location_error)?,
            &path,
        )?;
        if binding != expected_binding {
            return Err(CatalogError::UnsafePath);
        }
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let connection = Connection::open_with_flags(path, flags)?;
        connection.set_limit(
            Limit::SQLITE_LIMIT_LENGTH,
            i32::try_from(result_limits.max_record_bytes())
                .map_err(|_| CatalogError::InvalidConfiguration)?,
        )?;
        // Busy waits do not run SQLite progress callbacks. Ordinary WAL readers need no
        // writer admission; actual SQLite lock conflicts remain bounded failures.
        connection.busy_timeout(Duration::ZERO)?;
        let token = cancellation.clone();
        connection.progress_handler(
            SQLITE_PROGRESS_OPERATIONS,
            Some(move || token.is_cancelled() || Instant::now() >= deadline),
        )?;
        let reader = Self {
            connection,
            catalog_file,
            location: location.clone(),
            result_limits,
            deadline,
            cancellation: cancellation.clone(),
        };
        let initialized = (|| {
            reader.connection.pragma_update(None, "query_only", true)?;
            reader
                .connection
                .pragma_update(None, "trusted_schema", "OFF")?;
            reader
                .connection
                .pragma_update(None, "foreign_keys", "ON")?;
            reader.validate_endpoint()?;
            let application_id: i64 =
                reader
                    .connection
                    .query_row("PRAGMA application_id", [], |row| row.get(0))?;
            if application_id != CATALOG_APPLICATION_ID {
                return Err(CatalogError::ForeignCatalog);
            }
            verify_migration_identities(&reader.connection)?;
            let journal_mode: String =
                reader
                    .connection
                    .query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
            if !journal_mode.eq_ignore_ascii_case("wal") {
                return Err(CatalogError::UnsafeJournalMode);
            }
            reader.validate_endpoint()
        })();
        reader.check_control()?;
        initialized?;
        Ok(reader)
    }

    /// One SQLite transaction covers every catalog fact used by the caller. The owned
    /// connection and transaction remain in the existing supervised worker through file reads.
    pub(crate) fn read<T, E>(&self, operation: impl FnOnce(&Self) -> Result<T, E>) -> Result<T, E>
    where
        E: From<CatalogError>,
    {
        self.check_control()?;
        self.validate_endpoint()?;
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(CatalogError::from)?;
        let result = operation(self);
        self.check_control()?;
        self.validate_endpoint()?;
        let value = result?;
        transaction.commit().map_err(CatalogError::from)?;
        self.check_control()?;
        Ok(value)
    }

    pub(crate) const fn connection(&self) -> &Connection {
        &self.connection
    }

    pub(crate) fn publication_evidence(
        &self,
        digest: EvidenceDigest,
    ) -> Result<Option<PersistedProviderPublicationEvidence>, CatalogError> {
        load_provider_publication_evidence(&self.connection, digest)
    }

    pub(crate) fn publication_coordinates(
        &self,
        digest: EvidenceDigest,
    ) -> Result<Vec<ProviderMarketEventSelectionCandidate>, CatalogError> {
        provider_market_event_selection_for_publication(&self.connection, digest)
    }

    pub(crate) fn validate_event_metadata(
        &self,
        events: &[MarketEvent],
        evidence: &PersistedProviderPublicationEvidence,
    ) -> Result<(), CatalogError> {
        validate_provider_market_event_metadata(&self.connection, events, evidence)
    }

    pub(crate) fn verify_identity_evidence(
        &self,
        evidence: &ProviderIdentitySelectionEvidence,
    ) -> Result<MarketDataInstrumentRecord, MarketDataInstrumentCatalogError> {
        verify_provider_identity_evidence_with_limits(
            &self.connection,
            evidence,
            self.result_limits,
        )
    }

    fn check_control(&self) -> Result<(), CatalogError> {
        check_control(self.deadline, &self.cancellation)
    }

    fn validate_endpoint(&self) -> Result<(), CatalogError> {
        self.catalog_file
            .validate_identity()
            .map_err(map_catalog_location_error)?;
        self.location
            .validate_for_open()
            .map_err(map_catalog_location_error)
    }
}

fn check_control(deadline: Instant, cancellation: &CancellationToken) -> Result<(), CatalogError> {
    if cancellation.is_cancelled() {
        return Err(CatalogError::MarketRecoveryReadCancelled);
    }
    if Instant::now() >= deadline {
        return Err(CatalogError::MarketRecoveryReadDeadlineExceeded);
    }
    Ok(())
}
