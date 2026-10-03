//! The existing sealed event ingest boundary, with SQLite canonical-row durability.

use super::*;

mod archive;

/// Scheduling bounds for one archive turn; none is an ingestion or lifetime admission limit.
#[derive(Clone, Copy, Debug)]
pub struct MarketEventArchiveLimits {
    target_bytes: usize,
    maximum_publications: usize,
    working_bytes: usize,
}

impl Default for MarketEventArchiveLimits {
    fn default() -> Self {
        Self {
            target_bytes: 8 * 1024 * 1024,
            maximum_publications: 4096,
            working_bytes: MAX_EVENT_PUBLICATION_READ_BYTES,
        }
    }
}

impl MarketEventArchiveLimits {
    /// Sets a soft whole-publication target and bounded descriptor/writer work for a turn.
    pub fn try_new(
        target_bytes: usize,
        maximum_publications: usize,
        working_bytes: usize,
    ) -> Result<Self, IngestError> {
        if target_bytes == 0
            || maximum_publications == 0
            || working_bytes == 0
            || i64::try_from(maximum_publications).is_err()
            || maximum_publications
                .checked_mul(std::mem::size_of::<crate::MarketEventCommitRef>())
                .is_none_or(|bytes| bytes > working_bytes)
        {
            return Err(IngestError::InvalidDataset);
        }
        Ok(Self {
            target_bytes,
            maximum_publications,
            working_bytes,
        })
    }
}

/// One fair keyset visit; no retained process-local cursor is needed for durable progress.
#[derive(Clone, Debug)]
pub struct MarketEventArchiveTurn {
    next_dataset: Option<DatasetId>,
    archived_publications: u64,
    archived_rows: u64,
}

impl MarketEventArchiveTurn {
    /// Returns the next keyset cursor; None marks the end of the dataset pass.
    pub fn next_dataset(&self) -> Option<&DatasetId> {
        self.next_dataset.as_ref()
    }
    /// Returns the number of whole publications durably moved this turn.
    pub const fn archived_publications(&self) -> u64 {
        self.archived_publications
    }
    /// Returns the number of canonical rows reclaimed from active storage.
    pub const fn archived_rows(&self) -> u64 {
        self.archived_rows
    }
    /// Reports whether this turn published an archive.
    pub const fn worked(&self) -> bool {
        self.archived_publications != 0
    }
}

impl AnalyticalDataService {
    /// Atomically commits canonical events, sealed custody and the logical publication horizon.
    pub async fn ingest_provider_market_events(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        binding: SealedProviderPublicationBinding,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Result<crate::MarketEventCommitRef, IngestError> {
        precommit_authority.validate_precommit()?;
        let payload_digest = provider_market_event_publication_digest(&binding)?;
        let source_id = provider_market_event_source_id(&binding)?.clone();
        let converted = ProviderMarketEventArrowBatch::try_from_publication(&binding)?;
        let prepared = PreparedProviderPublicationBinding::try_from_live(&binding)?;
        if prepared.publication_digest() != payload_digest {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        let publication_authority = ProviderEventIdentityPrecommitAuthority {
            inner: precommit_authority.as_ref(),
            binding: &prepared,
            cancellation: &cancellation,
        };
        let _operation = self
            .operation_gate
            .acquire(&cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        let authority = self.lock_authority()?;
        let run = self.validate_run(&authority, &reservation, payload_digest, Some(&source_id))?;
        if run.state() == IngestRunState::Failed {
            return Err(IngestError::TerminalRun);
        }
        self.validate_provider_event_binding(&authority, &reservation, &prepared)?;
        // Successful retries validate retained evidence rather than demanding today's source identity.
        if run.state() != IngestRunState::Succeeded {
            publication_authority.validate_catalog_precommit(&authority)?;
        }
        authority
            .catalog()
            .commit_market_event_publication(
                &reservation,
                &analytical_dataset,
                &prepared,
                &converted,
                &self.objects,
                &cancellation,
            )
            .map_err(map_market_recovery_catalog_error)
    }
}
