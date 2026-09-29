//! Existing source acquisition, immutable publication and exact native EOD action reopen.

mod recovery;
mod replay;
pub(crate) use replay::TiingoCompletedEodHistoryReference;

use super::*;
use crate::application::ResearchProviderPublicationOperation;
use crate::provider_activation::tiingo::{TiingoEodHistoryOperation, TiingoHistoryMetadataInput};
use market_squawk_adapter_tiingo::{
    TiingoCompletedEodHistoryCandidate, TiingoEodExpectedSessionAuthority,
    TiingoEodExpectedSessionEvidence, TiingoEodHistoryActionProjection, TiingoEodMappingInput,
    TiingoHistoryPlan, TiingoHttpSource, TiingoPreparedEodHistoryCapture, TiingoSealedHistoryPage,
    map_eod_page_candidate,
};
use market_squawk_data::{CatalogAuthority, CompleteMarketBarHistoryOutput};
use market_squawk_domain::{EffectiveInterval, ExactPayloadEvidence, ResearchObservation};
use market_squawk_sources::{
    AvailabilityEvidence, DiscoveryRequest, ProviderCapturePageReceipt,
    ProviderCaptureSemanticBinding, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    SealedProviderCaptureSetReceipt, SourceObject, SourceObjectCaptureIdentity,
};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

/// Exact existing canonical generation containing the complete source date-window graph.
#[derive(Clone, Debug)]
pub(crate) struct TiingoEodHistoryPublicationReceipt {
    restart: TiingoLatestRestartBinding,
}
impl TiingoEodHistoryPublicationReceipt {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.restart.manifest
    }
    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.restart.binding_digest
    }
}

struct HistoryPrecommit {
    inner: Arc<dyn IngestPrecommitAuthority>,
    calendar: Arc<dyn IngestPrecommitAuthority>,
}
impl std::fmt::Debug for HistoryPrecommit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TiingoHistoryPrecommit")
            .field("calendar", &"[ORIGINAL EXPECTED CALENDAR GUARD]")
            .finish_non_exhaustive()
    }
}
impl IngestPrecommitAuthority for HistoryPrecommit {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.inner.validate_precommit()?;
        self.calendar.validate_precommit()?;
        Ok(())
    }
    fn validate_catalog_precommit(&self, catalog: &CatalogAuthority) -> Result<(), IngestError> {
        self.inner.validate_catalog_precommit(catalog)?;
        self.calendar.validate_catalog_precommit(catalog)?;
        Ok(())
    }
}

impl ProductionResearchIngestCoordinator {
    /// Fetches, seals and checkpoints each actual source window before dispatching its successor.
    /// No source body survives in a parallel store and no provider paging token is invented.
    pub(crate) async fn acquire_and_publish_tiingo_eod_history(
        &self,
        source: Arc<TiingoHttpSource>,
        operation: TiingoEodHistoryOperation,
        publication: ResearchProviderPublicationOperation,
        source_deadline: Timestamp,
        seal_deadline: Instant,
    ) -> Result<TiingoEodHistoryPublicationReceipt, TiingoHistoryApplicationError> {
        if operation.plan.pages().is_empty()
            || operation.plan.pages().len() >= market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES
        {
            return Err(TiingoHistoryApplicationError::Admission);
        }
        let original_lease = Arc::new(
            self.research
                .analytical()
                .acquire_provider_capture_original_lease(seal_deadline, publication.cancellation())
                .await?,
        );
        let mut checkpoint = operation.checkpoint;
        let cancellation = publication.cancellation().clone();
        let connection_id = uuid::Uuid::new_v4();
        publication
            .validate_precommit()
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let metadata = operation.captured_metadata.decoded().clone();
        let expected_count = u16::try_from(operation.plan.pages().len() + 1)
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let original_metadata = match operation.captured_metadata {
            TiingoHistoryMetadataInput::Fresh(captured) => {
                if checkpoint.next_page_index() != 0 {
                    return Err(TiingoHistoryApplicationError::OriginalContinuationRequired);
                }
                self.retain_tiingo_original(
                    captured.capture_material(uuid::Uuid::new_v4(), connection_id)?,
                    &operation.original_context,
                    0,
                    expected_count,
                    &operation.analytical_dataset,
                    metadata.evidence().decoded_at(),
                    &publication,
                    Arc::clone(&original_lease),
                    seal_deadline,
                )
                .await?
            }
            TiingoHistoryMetadataInput::Original { original, .. } => original,
        };
        if original_metadata.session() != operation.original_context.session
            || original_metadata.expected_count() != expected_count
            || original_metadata.dataset() != &operation.analytical_dataset
        {
            return Err(TiingoHistoryApplicationError::Admission);
        }
        let metadata_token = self
            .reseal_tiingo_original(
                original_metadata,
                Arc::clone(&original_lease),
                seal_deadline,
                &cancellation,
            )
            .await?;
        // A completed seal may precede its checkpoint by one page; no larger gap is valid.
        let invalid_tail = checkpoint
            .next_page_index()
            .checked_add(2)
            .and_then(|n| u16::try_from(n).ok())
            .ok_or(TiingoHistoryApplicationError::Admission)?;
        if self
            .tiingo_original(
                operation.original_context.session,
                invalid_tail,
                seal_deadline,
                &cancellation,
            )
            .await?
            .is_some()
        {
            return Err(TiingoHistoryApplicationError::Admission);
        }
        let mut tokens = Vec::new();
        let mut responses = Vec::new();
        let mut sealed_pages = Vec::new();
        let mut pages = Vec::new();
        tokens
            .try_reserve_exact(operation.plan.pages().len() + 1)
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        responses
            .try_reserve_exact(operation.plan.pages().len())
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        sealed_pages
            .try_reserve_exact(operation.plan.pages().len())
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        pages
            .try_reserve_exact(operation.plan.pages().len())
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let mut retained_body_bytes = metadata_token
            .persisted_receipt()
            .capture()
            .total_body_bytes();
        let mut retained_rows = 0usize;
        tokens.push(metadata_token);
        for index in 0..operation.plan.pages().len() {
            publication
                .validate_precommit()
                .map_err(|_| TiingoHistoryApplicationError::Admission)?;
            let ordinal =
                u16::try_from(index + 1).map_err(|_| TiingoHistoryApplicationError::Admission)?;
            let original = match self
                .tiingo_original(
                    operation.original_context.session,
                    ordinal,
                    seal_deadline,
                    &cancellation,
                )
                .await?
            {
                Some(original) => original,
                None => {
                    if index != checkpoint.next_page_index() as usize {
                        return Err(TiingoHistoryApplicationError::OriginalContinuationRequired);
                    }
                    let captured = source
                        .fetch_history_page(
                            &operation.plan,
                            &checkpoint,
                            source_deadline,
                            &cancellation,
                        )
                        .await?;
                    let decoded_at = captured.decoded().evidence().decoded_at();
                    self.retain_tiingo_original(
                        captured.capture_material(uuid::Uuid::new_v4(), connection_id)?,
                        &operation.original_context,
                        ordinal,
                        expected_count,
                        &operation.analytical_dataset,
                        decoded_at,
                        &publication,
                        Arc::clone(&original_lease),
                        seal_deadline,
                    )
                    .await?
                }
            };
            retained_body_bytes = retained_body_bytes
                .checked_add(original.capture().total_body_bytes())
                .filter(|bytes| *bytes <= 64 * 1024 * 1024)
                .ok_or(TiingoHistoryApplicationError::Admission)?;
            let response = self
                .decode_tiingo_original_page(
                    &original,
                    operation.plan.pages()[index].clone(),
                    &operation.original_context,
                    seal_deadline,
                    &cancellation,
                )
                .await?;
            retained_rows = retained_rows
                .checked_add(response.rows().len())
                .filter(|rows| {
                    *rows <= market_squawk_sources::MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS
                })
                .ok_or(TiingoHistoryApplicationError::Admission)?;
            let token = self
                .reseal_tiingo_original(
                    original,
                    Arc::clone(&original_lease),
                    seal_deadline,
                    &cancellation,
                )
                .await?;
            let sealed_page = TiingoSealedHistoryPage::try_new(
                &operation.plan.pages()[index],
                &response,
                token.persisted_receipt(),
            )?;
            let page = map_eod_page_candidate(TiingoEodMappingInput {
                response: &response,
                metadata: &metadata,
                sealed_capture: token.persisted_receipt(),
                sealed_metadata_capture: tokens[0].persisted_receipt(),
                instrument: &operation.instrument,
                contract: &operation.contract,
                ingested_at: response.evidence().decoded_at(),
            })?;
            if index + 1 == checkpoint.next_page_index() as usize
                && checkpoint.predecessor_page_identity() != Some(sealed_page.page_identity())
            {
                return Err(TiingoHistoryApplicationError::Admission);
            }
            if index == checkpoint.next_page_index() as usize {
                publication
                    .validate_precommit()
                    .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                checkpoint =
                    source.checkpoint_history_page(&operation.plan, &checkpoint, &sealed_page)?;
            }
            tokens.push(token);
            responses.push(response);
            sealed_pages.push(sealed_page);
            pages.push(page);
        }
        let complete =
            source.complete_history_capture(operation.plan, sealed_pages, &checkpoint)?;
        let history = TiingoCompletedEodHistoryCandidate::try_new(
            complete,
            pages,
            &operation.instrument,
            operation.expected_session_authority.as_ref(),
        )?
        .into_pending_publication();
        let expected = history.expected_session_evidence().clone();
        let prepared = TiingoPreparedEodHistoryCapture::try_new(
            history,
            metadata,
            responses,
            tokens,
            operation
                .instrument
                .instrument_definition()
                .payload_evidence()
                .content_digest(),
            operation.admitted_plan_digest,
            operation.cash_unit.as_ref(),
            operation.expected_session_authority.as_ref(),
        )?;
        let graph = prepared.capture();
        let observed_at = graph
            .pages()
            .last()
            .ok_or(TiingoHistoryApplicationError::Admission)?
            .received_at();
        let discovery = DiscoveryRequest::try_new(
            graph.dataset().clone(),
            None,
            NonZeroU16::MIN,
            source_deadline,
        )?;
        let object = SourceObject::try_new_with_capture_identity(
            graph.source_id().clone(),
            graph.metadata_revision().clone(),
            &discovery,
            SourceIdentifier::try_from("tiingo-complete-eod-history")
                .map_err(|_| TiingoHistoryApplicationError::Admission)?,
            SourceIdentifier::try_from("application-json")
                .map_err(|_| TiingoHistoryApplicationError::Admission)?,
            ExactPayloadEvidence::from_content_digest(graph.content_digest()),
            SourceObjectCaptureIdentity::try_from_capture(graph)?,
            EffectiveInterval::new(graph.pages()[0].received_at(), None)
                .map_err(|_| TiingoHistoryApplicationError::Admission)?,
            None,
            AvailabilityEvidence::LocalFirstObserved { observed_at },
            Some(graph.total_body_bytes()),
        )?;
        let request = ExtractionRequest::try_new(
            object,
            NonZeroU32::new(40_000).ok_or(TiingoHistoryApplicationError::Admission)?,
            NonZeroU64::new(64 * 1024 * 1024).ok_or(TiingoHistoryApplicationError::Admission)?,
            source_deadline,
        )?;
        let sealed = prepared.try_into_publication(request)?;
        let (revisions, binding) = sealed.into_parts();
        let closure = TiingoLatestApplicationClosure::try_new(
            Arc::clone(&self.research),
            publication.source().clone(),
            publication.rights().clone(),
            publication.source_registered_at(),
        )?;
        drop(original_lease);
        let calendar = operation
            .expected_session_authority
            .acquire_publication_authority(&expected)
            .await
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let precommit = Arc::new(HistoryPrecommit {
            inner: publication.precommit_authority(),
            calendar,
        });
        let result = closure
            .publish_binding(
                binding,
                revisions,
                ProviderNativeLineageImplementation::TiingoEodMarketBarV1,
                operation.analytical_dataset,
                observed_at,
                precommit,
                cancellation,
            )
            .await?;
        Ok(TiingoEodHistoryPublicationReceipt {
            restart: result.restart_binding(),
        })
    }
}

/// Opaque same-publication bar/action evidence consumed by the action ledger.
#[derive(Debug)]
pub(crate) struct TiingoCompletedEodActionRead {
    reference: TiingoCompletedEodHistoryReference,
    source: Arc<market_squawk_data::RetainedTiingoEodActionHistory>,
}
impl TiingoCompletedEodActionRead {
    pub(crate) fn history(&self) -> &CompleteMarketBarHistoryOutput {
        self.source.history()
    }
    pub(crate) fn binding(&self) -> &PersistedProviderCaptureBindingEvidence {
        self.source.binding()
    }
    pub(crate) fn actions(&self) -> &TiingoEodHistoryActionProjection {
        self.source.actions()
    }
    pub(crate) fn knowledge_cutoff(&self) -> Timestamp {
        self.source.knowledge_cutoff()
    }
    /// Shared genuine data-owned source read, never reconstructed from app JSON or normalized rows.
    pub(crate) const fn source_history(
        &self,
    ) -> &Arc<market_squawk_data::RetainedTiingoEodActionHistory> {
        &self.source
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TiingoHistoryApplicationError {
    #[error("Tiingo complete history is outside the exact admitted source or resource scope")]
    Admission,
    #[error("Tiingo history requires its original retained partial acquisition continuation")]
    OriginalContinuationRequired,
    #[error(transparent)]
    Latest(#[from] TiingoLatestApplicationError),
    #[error(transparent)]
    Source(#[from] market_squawk_adapter_tiingo::TiingoHttpSourceError),
    #[error(transparent)]
    CaptureMaterial(#[from] market_squawk_adapter_tiingo::TiingoCaptureMaterialError),
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
    #[error(transparent)]
    Research(#[from] ResearchServiceError),
    #[error(transparent)]
    Eod(#[from] market_squawk_adapter_tiingo::TiingoEodMapError),
    #[error(transparent)]
    Evidence(#[from] market_squawk_adapter_tiingo::TiingoHistoryEvidenceError),
    #[error(transparent)]
    Publication(#[from] market_squawk_adapter_tiingo::TiingoEodHistoryPublicationError),
    #[error(transparent)]
    Extraction(#[from] market_squawk_sources::ExtractionError),
    #[error(transparent)]
    Calendar(#[from] crate::application::market_calendar::CompletedMarketSessionError),
    #[error(transparent)]
    Read(#[from] market_squawk_data::AnalyticalReadError),
    #[error(transparent)]
    Ingest(#[from] market_squawk_data::IngestError),
}
