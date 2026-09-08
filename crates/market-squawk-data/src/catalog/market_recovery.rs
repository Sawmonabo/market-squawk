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

/// A retained route to reopen through the existing point-in-time event selector.
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

impl Catalog {
    #[allow(
        clippy::too_many_arguments,
        reason = "the exact instrument, event classes, original clocks and work bounds stay explicit"
    )]
    pub(crate) fn provider_market_event_durable_routes(
        &self,
        instrument_id: InstrumentId,
        event_kinds: &[LiveEventClass],
        as_of_cutoff: Timestamp,
        knowledge_cutoff: Timestamp,
        maximum_routes: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<ProviderMarketEventDurableRoute>, CatalogError> {
        check_read(deadline, cancellation)?;
        if event_kinds.is_empty()
            || event_kinds.len() > MAX_EVENT_KINDS
            || maximum_routes == 0
            || maximum_routes > MAX_DURABLE_ROUTES
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
        self.market_recovery_read(deadline, cancellation, || {
            let mut statement = self.connection.prepare(
                "SELECT DISTINCT generation.dataset_id, indexed.source_id, indexed.venue_id
                 FROM provider_market_event_selection_index AS indexed
                 JOIN analytical_generation_provider_publication_bindings AS publication
                   ON publication.publication_digest=indexed.publication_digest
                  AND publication.publication_kind=indexed.publication_kind
                  AND publication.source_id=indexed.source_id
                 JOIN analytical_generations AS generation
                   ON generation.generation_sequence=publication.generation_sequence
                 JOIN analytical_generation_source_inputs AS source_input
                   ON source_input.generation_sequence=generation.generation_sequence
                  AND source_input.run_id=publication.run_id
                  AND source_input.source_id=indexed.source_id
                 WHERE indexed.instrument_id=?1
                   AND indexed.source_timestamp_ns IS NOT NULL
                   AND indexed.source_timestamp_ns<=?2
                   AND indexed.available_at_ns<=?3 AND indexed.ingested_at_ns<=?3
                   AND generation.created_at_ns<=?3 AND generation.generation_kind='ingest'
                   AND generation.schema_name=?4 AND generation.schema_version=?5
                   AND generation.schema_fingerprint=?6
                   AND indexed.event_kind IN (?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                 ORDER BY generation.dataset_id COLLATE BINARY,
                          indexed.source_id COLLATE BINARY, indexed.venue_id COLLATE BINARY
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
                i64::try_from(maximum_routes + 1).map_err(|_| CatalogError::InvalidLimit)?,
            ])?;
            let mut budget = ResultBudget::new(self.result_bytes);
            let mut routes = Vec::new();
            routes
                .try_reserve_exact(maximum_routes)
                .map_err(|_| CatalogError::Allocation)?;
            while let Some(row) = rows.next()? {
                check_read(deadline, cancellation)?;
                if routes.len() == maximum_routes {
                    return Err(CatalogError::ResultRowLimitExceeded);
                }
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
                    dataset: DatasetId::try_from(dataset)
                        .map_err(|_| CatalogError::CorruptCatalog)?,
                    source_surface: SourceId::try_from(source)
                        .map_err(|_| CatalogError::CorruptCatalog)?,
                    instrument_id,
                    venue_id: VenueId::try_from(venue).map_err(|_| CatalogError::CorruptCatalog)?,
                });
            }
            Ok(routes)
        })
    }

    pub(crate) fn retained_source_metadata(
        &self,
        source_id: &SourceId,
        metadata_revision: &MetadataRevision,
        knowledge_cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<SourceMetadata>, CatalogError> {
        self.market_recovery_read(deadline, cancellation, || {
            let mut statement = self.connection.prepare(
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
            ResultBudget::new(self.result_bytes).charge([digest.len(), json.len()])?;
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
        })
    }

    fn market_recovery_read<T>(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
        operation: impl FnOnce() -> Result<T, CatalogError>,
    ) -> Result<T, CatalogError> {
        check_read(deadline, cancellation)?;
        let token = cancellation.clone();
        self.connection.progress_handler(
            SQLITE_PROGRESS_OPERATIONS,
            Some(move || token.is_cancelled() || Instant::now() >= deadline),
        )?;
        let result = operation();
        self.connection.progress_handler::<fn() -> bool>(0, None)?;
        check_read(deadline, cancellation)?;
        result
    }
}

fn check_read(deadline: Instant, cancellation: &CancellationToken) -> Result<(), CatalogError> {
    if cancellation.is_cancelled() {
        Err(CatalogError::MarketRecoveryReadCancelled)
    } else if Instant::now() >= deadline {
        Err(CatalogError::MarketRecoveryReadDeadlineExceeded)
    } else {
        Ok(())
    }
}
