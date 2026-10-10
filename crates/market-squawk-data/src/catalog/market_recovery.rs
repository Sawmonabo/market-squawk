//! Bounded discovery of retained market publications and their exact source revisions.

use std::time::Instant;

use market_squawk_domain::{
    InstrumentId, LiveEventClass, MetadataRevision, SourceId, Timestamp, VenueId,
};
use market_squawk_sources::SourceMetadata;
use rusqlite::params;
use tokio_util::sync::CancellationToken;

use super::storage::{ResultBudget, sha256};
use super::{Catalog, CatalogError};
use crate::{DatasetId, DatasetSchemaRegistry};

const MAX_DURABLE_ROUTES: usize = 256;
const MAX_EVENT_KINDS: usize = 8;
const SQLITE_PROGRESS_OPERATIONS: i32 = 1_000;

/// A retained route and stable keyset cursor for the point-in-time event selector.
///
/// Discovery does not select an event or grant permission to use its source data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderMarketEventDurableRoute {
    dataset: DatasetId,
    source_surface: SourceId,
    instrument_id: InstrumentId,
    venue_id: VenueId,
}

impl ProviderMarketEventDurableRoute {
    /// Returns the immutable publication's analytical dataset.
    pub const fn dataset(&self) -> &DatasetId {
        &self.dataset
    }

    /// Returns the exact retained source surface.
    pub const fn source_surface(&self) -> &SourceId {
        &self.source_surface
    }

    /// Returns the requested canonical instrument.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact event venue.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the exact instrument, event classes, original clocks and work bounds stay explicit"
)]
pub(super) fn load_provider_market_event_durable_routes(
    connection: &rusqlite::Connection,
    result_limits: super::CatalogResultLimits,
    instrument_id: InstrumentId,
    event_kinds: &[LiveEventClass],
    as_of_cutoff: Timestamp,
    knowledge_cutoff: Timestamp,
    after: Option<&ProviderMarketEventDurableRoute>,
    maximum_routes: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<ProviderMarketEventDurableRoute>, CatalogError> {
    check_read(deadline, cancellation)?;
    if event_kinds.is_empty()
        || event_kinds.len() > MAX_EVENT_KINDS
        || maximum_routes == 0
        || maximum_routes > MAX_DURABLE_ROUTES
        || after.is_some_and(|cursor| cursor.instrument_id != instrument_id)
        || event_kinds
            .iter()
            .enumerate()
            .any(|(index, kind)| event_kinds[..index].contains(kind))
    {
        return Err(CatalogError::InvalidRecord);
    }
    let mut kinds = [None; MAX_EVENT_KINDS];
    for (index, kind) in event_kinds.iter().enumerate() {
        kinds[index] = Some(crate::provider_event_selection::event_kind_name(*kind));
    }
    let schema = DatasetSchemaRegistry::local()
        .canonical_market_events()
        .map_err(|_| CatalogError::InvalidRecord)?;
    check_read(deadline, cancellation)?;
    // Discover each route once, then establish that it has at least one complete publication.
    // Joining completeness before DISTINCT repeats publication row counts for every event.
    // EXISTS retains the same admission predicates and stops at the first qualifying event.
    // Start its exact commit/run lookups from matching indexed events, not every source run.
    let mut statement = connection.prepare(
        "SELECT route.dataset_id, route.source_id, route.venue_id
         FROM (
           SELECT DISTINCT dataset_id, source_id, venue_id
           FROM provider_market_event_selection_index
           WHERE instrument_id=?1
             AND source_timestamp_ns IS NOT NULL AND source_timestamp_ns<=?2
             AND available_at_ns<=?3 AND ingested_at_ns<=?3
             AND event_kind IN (?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
             AND (?16 IS NULL OR (dataset_id COLLATE BINARY,
                 source_id COLLATE BINARY, venue_id COLLATE BINARY)>(?16,?17,?18))
         ) AS route
         WHERE EXISTS (
           SELECT 1 FROM provider_market_event_selection_index AS indexed
           CROSS JOIN market_event_complete_commits AS committed
             ON committed.dataset_id=indexed.dataset_id
            AND committed.commit_sequence=indexed.commit_sequence
            AND committed.publication_digest=indexed.publication_digest
            AND committed.publication_kind=indexed.publication_kind
           CROSS JOIN ingest_runs AS run ON run.run_id=committed.run_id
            AND run.source_id=indexed.source_id AND run.state='succeeded'
            AND run.completed_at_ns=committed.available_at_ns
           WHERE indexed.dataset_id=route.dataset_id
             AND indexed.source_id=route.source_id AND indexed.venue_id=route.venue_id
             AND indexed.instrument_id=?1
             AND indexed.source_timestamp_ns IS NOT NULL AND indexed.source_timestamp_ns<=?2
             AND indexed.available_at_ns<=?3 AND indexed.ingested_at_ns<=?3
             AND committed.available_at_ns<=?3
             AND committed.schema_name=?4 AND committed.schema_version=?5
             AND committed.schema_fingerprint=?6
             AND indexed.event_kind IN (?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
         )
         ORDER BY route.dataset_id COLLATE BINARY,
                  route.source_id COLLATE BINARY, route.venue_id COLLATE BINARY
         LIMIT ?15",
    )?;
    let mut rows = statement.query(params![
        instrument_id.as_uuid().as_bytes().as_slice(),
        as_of_cutoff.unix_nanos(),
        knowledge_cutoff.unix_nanos(),
        schema.name(),
        i64::from(schema.version().get()),
        schema.fingerprint().as_slice(),
        kinds[0],
        kinds[1],
        kinds[2],
        kinds[3],
        kinds[4],
        kinds[5],
        kinds[6],
        kinds[7],
        i64::try_from(maximum_routes).map_err(|_| CatalogError::InvalidLimit)?,
        after.map(|route| route.dataset.as_str()),
        after.map(|route| route.source_surface.as_str()),
        after.map(|route| route.venue_id.as_str()),
    ])?;
    let mut budget = ResultBudget::new(result_limits);
    let mut routes = Vec::new();
    routes
        .try_reserve_exact(maximum_routes)
        .map_err(|_| CatalogError::Allocation)?;
    while let Some(row) = rows.next()? {
        check_read(deadline, cancellation)?;
        let dataset = row
            .get_ref(0)?
            .as_str()
            .map_err(|_| CatalogError::CorruptCatalog)?;
        let source = row
            .get_ref(1)?
            .as_str()
            .map_err(|_| CatalogError::CorruptCatalog)?;
        let venue = row
            .get_ref(2)?
            .as_str()
            .map_err(|_| CatalogError::CorruptCatalog)?;
        budget.charge([dataset.len(), source.len(), venue.len(), 16])?;
        routes.push(ProviderMarketEventDurableRoute {
            dataset: DatasetId::try_from(dataset).map_err(|_| CatalogError::CorruptCatalog)?,
            source_surface: SourceId::try_from(source).map_err(|_| CatalogError::CorruptCatalog)?,
            instrument_id,
            venue_id: VenueId::try_from(venue).map_err(|_| CatalogError::CorruptCatalog)?,
        });
    }
    Ok(routes)
}

pub(super) fn load_retained_source_metadata(
    connection: &rusqlite::Connection,
    result_limits: super::CatalogResultLimits,
    source_id: &SourceId,
    metadata_revision: &MetadataRevision,
    knowledge_cutoff: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<SourceMetadata>, CatalogError> {
    check_read(deadline, cancellation)?;
    let mut statement = connection.prepare(
        "SELECT revision_digest, metadata_json
                 FROM source_revisions
                 WHERE source_id=?1 AND registered_at_ns<=?3
                   AND json_extract(metadata_json, '$.revision_evidence.metadata_revision')=?2
                 ORDER BY revision_digest LIMIT 2",
    )?;
    let mut rows = statement.query(params![
        source_id.as_str(),
        metadata_revision.as_source_identifier().as_str(),
        knowledge_cutoff.unix_nanos(),
    ])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let digest = row
        .get_ref(0)?
        .as_blob()
        .map_err(|_| CatalogError::CorruptCatalog)?;
    let json = row
        .get_ref(1)?
        .as_str()
        .map_err(|_| CatalogError::CorruptCatalog)?;
    ResultBudget::new(result_limits).charge([digest.len(), json.len()])?;
    if digest != sha256(json.as_bytes()) {
        return Err(CatalogError::CorruptCatalog);
    }
    let source: SourceMetadata =
        serde_json::from_str(json).map_err(|_| CatalogError::CorruptCatalog)?;
    if source.source_id() != source_id
        || source.revision() != metadata_revision
        || rows.next()?.is_some()
    {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(Some(source))
}

impl Catalog {
    pub(crate) fn market_recovery_read<T>(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
        operation: impl FnOnce() -> Result<T, CatalogError>,
    ) -> Result<T, CatalogError> {
        check_read(deadline, cancellation)?;
        // Busy waiting is outside SQLite's progress handler. This read scope cannot wait
        // on another connection while its cancellation or deadline goes unobserved.
        self.connection.busy_timeout(std::time::Duration::ZERO)?;
        let token = cancellation.clone();
        let install = self.connection.progress_handler(
            SQLITE_PROGRESS_OPERATIONS,
            Some(move || token.is_cancelled() || Instant::now() >= deadline),
        );
        let result = install
            .map_err(CatalogError::from)
            .and_then(|()| operation());
        let progress_cleanup = self.connection.progress_handler::<fn() -> bool>(0, None);
        let busy_cleanup = self.connection.busy_timeout(self.busy_timeout);
        check_read(deadline, cancellation)?;
        progress_cleanup?;
        busy_cleanup?;
        result
    }
}

pub(super) fn check_read(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CatalogError> {
    if cancellation.is_cancelled() {
        Err(CatalogError::MarketRecoveryReadCancelled)
    } else if Instant::now() >= deadline {
        Err(CatalogError::MarketRecoveryReadDeadlineExceeded)
    } else {
        Ok(())
    }
}
