//! The existing sealed event ingest boundary, with SQLite canonical-row durability.

use super::*;

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
                &cancellation,
            )
            .map_err(map_market_recovery_catalog_error)
    }
}
