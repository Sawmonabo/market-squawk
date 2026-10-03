//! Sealing, normalized publication and exact original reread for maintained Tiingo action queries.
use super::*;
use crate::provider_activation::tiingo::TiingoCurrentActionAcquisition;
use market_squawk_adapter_tiingo::TiingoPreparedCorporateActionsPublication;
use market_squawk_data::{
    CorporateActionQueryIdentitySelection, CurrentOrdinaryActionSourceRead,
    MarketDataProviderIdentitySelection,
};
use market_squawk_domain::{CorporateActionEventInstrumentIdentity, InstrumentId};
use market_squawk_services::RequestContext;
use market_squawk_sources::DiscoveryRequest;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use uuid::Uuid;
pub(crate) struct PublishedCurrentOrdinaryActions {
    pub(crate) manifest: DatasetManifestRef,
    pub(crate) binding: EvidenceDigest,
    pub(crate) instrument: InstrumentId,
}
impl ProductionResearchIngestCoordinator {
    pub(crate) async fn publish_current_ordinary_actions(
        &self,
        acquired: TiingoCurrentActionAcquisition,
        query: CorporateActionQueryIdentitySelection,
        events: Vec<(
            CorporateActionEventInstrumentIdentity,
            MarketDataProviderIdentitySelection,
        )>,
        context: &RequestContext,
    ) -> Result<PublishedCurrentOrdinaryActions, ServiceError> {
        check(context)?;
        let [selected] = query.retained() else {
            return Err(ServiceError::InvalidResult);
        };
        let instrument = selected.instrument_id;
        let TiingoCurrentActionAcquisition {
            captured,
            publication,
        } = acquired;
        publication
            .validate_precommit()
            .map_err(|_| controlled(context, ServiceError::Internal))?;
        let material = captured
            .capture_material(Uuid::new_v4(), Uuid::new_v4())
            .map_err(|_| ServiceError::InvalidResult)?;
        let (expected, seal) = material.into_whole_seal_parts();
        let sealed = self
            .research
            .seal_provider_capture(seal, publication.cancellation(), context.deadline())
            .await
            .map_err(crate::application::research::corporate_actions::map_research_error)?;
        let token = expected
            .try_rejoin(sealed)
            .and_then(|value| value.try_into_whole())
            .map_err(|_| ServiceError::InvalidResult)?;
        let prepared = TiingoPreparedCorporateActionsPublication::try_new(
            captured,
            token,
            publication.source().clone(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let metadata = prepared.metadata().clone();
        let dataset = DatasetId::try_from(prepared.dataset().as_str())
            .map_err(|_| ServiceError::InvalidResult)?;
        let ingested_at = now()?;
        let remaining = context
            .deadline()
            .checked_duration_since(Instant::now())
            .ok_or(ServiceError::DeadlineExceeded)?;
        let deadline = ingested_at
            .checked_add_nanos(
                i64::try_from(remaining.as_nanos()).map_err(|_| ServiceError::InvalidRequest)?,
            )
            .map_err(|_| ServiceError::InvalidRequest)?;
        let discovery =
            DiscoveryRequest::try_new(prepared.dataset().clone(), None, NonZeroU16::MIN, deadline)
                .map_err(|_| ServiceError::InvalidResult)?;
        let object = prepared
            .source_object(&discovery)
            .map_err(|_| ServiceError::InvalidResult)?;
        let extraction = ExtractionRequest::try_new(
            object,
            NonZeroU32::new(8193).ok_or(ServiceError::Internal)?,
            NonZeroU64::new(64 * 1024 * 1024).ok_or(ServiceError::Internal)?,
            deadline,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let (retained, selections): (Vec<_>, Vec<_>) = events.into_iter().unzip();
        let binding = prepared
            .try_into_binding(&extraction, query.retained(), &retained, ingested_at)
            .map_err(|_| ServiceError::InvalidResult)?;
        let digest = binding.evidence_digest().evidence();
        let guard = query
            .current_ordinary_publication_authority(
                self.research.market_data_instruments().clone(),
                &binding,
                publication.precommit_authority(),
                context.deadline(),
                publication.cancellation().clone(),
            )
            .map_err(map_current_query_error)?
            .with_event_identities(selections)
            .map_err(map_current_query_error)?;
        let rights = publication.rights().decision(
            extraction_provider_payload_digest(binding.batch()),
            ingested_at,
        )?;
        let revisions = TiingoPreparedCorporateActionsPublication::revision_plan(binding.batch())
            .map_err(|_| ServiceError::InvalidResult)?;
        let request = ResearchIngestRequest::with_provider_publication(
            metadata, rights, dataset, binding, revisions,
        )
        .map_err(|_| ServiceError::InvalidResult)?
        .with_precommit_authority(Arc::new(guard));
        let committed = self
            .research
            .ingest(request, publication.cancellation().clone())
            .await
            .map_err(crate::application::research::corporate_actions::map_research_error)?;
        check(context)?;
        Ok(PublishedCurrentOrdinaryActions {
            manifest: committed.manifest().clone(),
            binding: digest,
            instrument,
        })
    }
    pub(crate) async fn read_current_ordinary_actions(
        &self,
        published: PublishedCurrentOrdinaryActions,
        cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<CurrentOrdinaryActionSourceRead, ServiceError> {
        let generation = self
            .research
            .read_provider_capture_generation(
                published.manifest,
                context.deadline(),
                context.cancellation(),
                |generation, _, _, _, _| Ok(generation.clone()),
            )
            .await
            .map_err(crate::application::research::corporate_actions::map_research_error)?;
        let read = self
            .research
            .analytical_reader()
            .read_current_ordinary_source(
                &generation,
                &self.research.market_data_instruments(),
                published.instrument,
                cutoff,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(|error| match error {
                market_squawk_data::CorporateActionSourceReadError::ResourceBound => ServiceError::ResourceExhausted,
                market_squawk_data::CorporateActionSourceReadError::Interrupted => controlled(context, ServiceError::Internal),
                _ => ServiceError::InvalidResult,
            })?;
        if read.binding_digest() != published.binding {
            return Err(ServiceError::InvalidResult);
        }
        Ok(read)
    }
}
fn now() -> Result<Timestamp, ServiceError> {
    crate::application::market_calendar::MarketCalendarClock::now(
        &crate::application::market_calendar::SystemMarketCalendarClock,
    )
    .map_err(|_| ServiceError::Unavailable)
}
fn check(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn controlled(context: &RequestContext, error: ServiceError) -> ServiceError {
    check(context).err().unwrap_or(error)
}

fn map_current_query_error(error: market_squawk_data::CorporateActionQueryIdentityError) -> ServiceError {
    use market_squawk_data::CorporateActionQueryIdentityError as E;
    match error {
        E::MissingIdentity => ServiceError::Unavailable,
        E::Mismatch => ServiceError::InvalidResult,
        E::ResourceBound => ServiceError::ResourceExhausted,
        E::Catalog(error) => crate::application::research::map_market_definition_read_error(error),
    }
}
