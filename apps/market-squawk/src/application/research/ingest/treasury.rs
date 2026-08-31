//! Application-owned U.S. Treasury acquisition, raw sealing, and exact Macro reads.
//!
//! Treasury Fiscal Data and Treasury daily-rate XML are independent products. Both already cross
//! the registered source, shared provider-rate, strict parser, canonical Macro, and exact raw
//! capture boundaries. Adapter-authored native lineage and exact canonical-row-to-raw-page maps
//! are carried without reinterpretation through sealing, immutable publication, catalog restart,
//! and point-in-time reads.
//!
//! Exact-manifest reads are complete and usable for a Treasury generation once the shared
//! publication hook supplies a verified binding receipt. Restart always reopens the exact
//! manifest and raw/native binding before executing the fixed latest-known Macro selector.

use std::{num::NonZeroU16, sync::Arc, time::Instant};

use market_squawk_adapter_treasury::{
    TreasuryAllHistoryAcquisitionCompletion, TreasuryAllHistoryCanonicalPage, TreasurySource,
    TreasurySourceError, TreasurySurface,
};
use market_squawk_data::{
    AnalyticalGeneration, AnalyticalMacroLatestKnownOutput, AnalyticalMacroLatestKnownRequest,
    AnalyticalMacroSeriesAllowlist, AnalyticalReadError, DatasetId, DatasetManifestRef,
    IngestError, IngestIdentity, IngestPrecommitAuthority, PersistedProviderCaptureBindingEvidence,
    PinnedDataset, ProviderMacroPlanChunkInput, ProviderMacroPlanManifestSelector,
    ProviderMacroPlanPublicationInput, ProviderMacroPlanSemantics, ProviderMacroPlanSessionInput,
    ProviderMacroPlanSessionReceipt, ProviderMacroPlanStagedPage, ProviderMacroPlanTerminal,
    QueryLimits, RightsDecisionInput, SourceOperation, StagedProviderMacroPlanRestartEvidence,
};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, ResearchObservation, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_services::{RequestContext, ServiceError};
use market_squawk_sources::{
    DiscoveryRequest, ExtractionBatch, ExtractionRevisionPlan, ProviderCaptureError,
    ProviderCaptureTerminalDisposition, ProviderNativeLineageImplementation,
    ProviderWholeCaptureToken, SealedProviderCaptureBinding, SourceClass, SourceMetadata,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    DomainLifecycle, ProductionResearchIngestCoordinator, ResearchIngestCompositionError,
    ResearchProviderRuntimeGeneration, TreasuryAllHistoryOperationAuthority, ensure_operation_live,
    operation_deadline,
};
use crate::{ResearchService, ResearchServiceError};

/// Fixed typed operation for Fiscal Data Average Interest Rates V2 latest-known reads.
pub(crate) const TREASURY_FISCAL_DATA_LATEST_KNOWN_OPERATION: &str =
    "Macro.GetTreasuryFiscalDataLatestKnown";

/// Fixed typed operation for Treasury daily-rate XML latest-known reads.
pub(crate) const TREASURY_DAILY_RATES_LATEST_KNOWN_OPERATION: &str =
    "Macro.GetTreasuryDailyRatesLatestKnown";

const MAX_DISCOVERY_RECEIPT_BYTES: usize = 512;
const MAX_TREASURY_LATEST_KNOWN_SERIES: usize = 32;
const TREASURY_PROVIDER: &str = "us-treasury";
const TREASURY_NATIVE_IMPLEMENTATION: &str = "us_treasury_macro_v1";
const TREASURY_MACRO_SEMANTICS_SCHEMA: &str = "us-treasury-macro-native-semantics-v1";
const TREASURY_ALL_HISTORY_PLAN_SCHEMA: &[u8] = b"market-squawk/treasury-all-history-plan/v1\0";
const TREASURY_ALL_HISTORY_PAGE_SCHEMA: &[u8] = b"market-squawk/treasury-all-history-page/v1\0";
const TREASURY_ALL_HISTORY_INGEST_SCHEMA: &[u8] = b"market-squawk/treasury-all-history-ingest/v1\0";
const TREASURY_FISCAL_SOURCE_ID: &str = "treasury-treasury.fiscal-data";
const TREASURY_DAILY_SOURCE_ID: &str = "treasury-treasury.daily-rates-xml";
const FISCAL_PROVIDER_DATASET_PREFIX: &str = "treasury:fiscal-data:average-interest-rates-v2:";
const FISCAL_ANALYTICAL_DATASET_PREFIX: &str = "treasury.fiscal-data.average-interest-rates-v2.";
const FISCAL_SERIES_PREFIX: &str = "treasury:average-interest-rate:v2:";
const DAILY_PROVIDER_DATASET_PREFIXES: [&str; 5] = [
    "treasury:daily-par-yield-curve:",
    "treasury:daily-bill-rates:",
    "treasury:daily-long-term-rates:",
    "treasury:daily-real-par-yield-curve:",
    "treasury:daily-real-long-term-rates:",
];
const DAILY_ANALYTICAL_DATASET_PREFIXES: [&str; 5] = [
    "treasury.daily-par-yield-curve.",
    "treasury.daily-bill-rates.",
    "treasury.daily-long-term-rates.",
    "treasury.daily-real-par-yield-curve.",
    "treasury.daily-real-long-term-rates.",
];

/// Exact selected discovery object consumed by one Treasury registered acquisition.
#[derive(Debug)]
pub(crate) struct TreasurySelectedObjectRequest {
    surface: TreasurySurface,
    provider_dataset: SourceIdentifier,
    object_id: SourceIdentifier,
    discovery_receipt: String,
}

impl TreasurySelectedObjectRequest {
    /// Binds one Fiscal Data object to the exact built-in Fiscal Data runtime slot.
    pub(crate) fn fiscal_data(
        provider_dataset: SourceIdentifier,
        object_id: SourceIdentifier,
        discovery_receipt: String,
    ) -> Result<Self, TreasuryApplicationError> {
        Self::try_new(
            TreasurySurface::FiscalData,
            provider_dataset,
            object_id,
            discovery_receipt,
        )
    }

    fn try_new(
        surface: TreasurySurface,
        provider_dataset: SourceIdentifier,
        object_id: SourceIdentifier,
        discovery_receipt: String,
    ) -> Result<Self, TreasuryApplicationError> {
        if !surface_accepts_provider_dataset(surface, &provider_dataset)
            || object_id.as_str().is_empty()
            || discovery_receipt.is_empty()
            || discovery_receipt.len() > MAX_DISCOVERY_RECEIPT_BYTES
        {
            return Err(TreasuryApplicationError::InvalidSelection);
        }
        Ok(Self {
            surface,
            provider_dataset,
            object_id,
            discovery_receipt,
        })
    }

    /// Returns the independently configured Treasury product surface.
    pub(crate) const fn surface(&self) -> TreasurySurface {
        self.surface
    }

    /// Returns the exact provider dataset selected during discovery.
    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the exact provider object selected during discovery.
    pub(crate) const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }
}

/// Application-owned registered acquisition and sole raw-store sealing boundary.
pub(crate) struct TreasuryApplicationClosure {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    research: Arc<ResearchService>,
}

impl std::fmt::Debug for TreasuryApplicationClosure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TreasuryApplicationClosure")
            .field("coordinator", &"[REGISTERED SOURCE AUTHORITY]")
            .field("research", &"[APPLICATION-OWNED RESEARCH AUTHORITY]")
            .finish()
    }
}

impl TreasuryApplicationClosure {
    /// Binds the coordinator and raw/analytical authority only when they share one application.
    pub(crate) fn try_new(
        coordinator: Arc<ProductionResearchIngestCoordinator>,
        research: Arc<ResearchService>,
    ) -> Result<Self, TreasuryApplicationError> {
        if !Arc::ptr_eq(&coordinator.research, &research) {
            return Err(TreasuryApplicationError::AuthorityInvalid);
        }
        Ok(Self {
            coordinator,
            research,
        })
    }

    /// Proves that one publication receipt belongs to this exact application research store.
    ///
    /// Operation composition calls this before exposing a ready state. A receipt minted by a
    /// different store, or one whose immutable manifest/raw/native binding no longer reopens,
    /// therefore cannot install a read route that is guaranteed to fail later.
    pub(crate) fn verify_publication_receipt(
        &self,
        receipt: &TreasuryMacroPublicationReceipt,
    ) -> Result<(), TreasuryApplicationError> {
        if receipt.manifest() != receipt.restart_selector().manifest() {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        receipt
            .restart_selector()
            .verify(self.research.as_ref())
            .map(|_pinned| ())
    }

    /// Reopens the latest exact generation for one configured Treasury dataset without provider
    /// reacquisition. Absence is distinct from corrupt or cross-bound durable evidence.
    pub(crate) fn reopen_latest_published(
        &self,
        surface: TreasurySurface,
        provider_dataset: &SourceIdentifier,
        generation: &ResearchProviderRuntimeGeneration,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<TreasuryMacroPublicationReceipt>, TreasuryApplicationError> {
        let analytical_dataset = treasury_analytical_dataset(surface, provider_dataset)?;
        let Some(published_generation) = self.research.analytical_reader().latest(
            &analytical_dataset,
            deadline,
            cancellation,
        )?
        else {
            return Ok(None);
        };
        if surface == TreasurySurface::DailyRatesXml && provider_dataset.as_str().ends_with(":all")
        {
            let manifest = published_generation.manifest().clone();
            let restart = TreasuryMacroRestartSelector::all_history(manifest.clone())?;
            restart.verify_for_runtime_generation(self.research.as_ref(), generation)?;
            return Ok(Some(TreasuryMacroPublicationReceipt { manifest, restart }));
        }
        let receipt = Self::reopen_generation(
            self.research.as_ref(),
            surface,
            provider_dataset,
            published_generation,
        )?;
        receipt
            .restart_selector()
            .verify_for_runtime_generation(self.research.as_ref(), generation)?;
        Ok(Some(receipt))
    }

    /// Resumes and publishes one exact daily-rate all-history family as one atomic generation.
    pub(crate) async fn publish_daily_rates_all_history(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        provider_dataset: &SourceIdentifier,
        context: &RequestContext,
    ) -> Result<Option<TreasuryMacroPublicationReceipt>, TreasuryApplicationError> {
        if !provider_dataset.as_str().ends_with(":all")
            || !surface_accepts_provider_dataset(TreasurySurface::DailyRatesXml, provider_dataset)
        {
            return Err(TreasuryApplicationError::InvalidSelection);
        }
        let operation = self
            .coordinator
            .acquire_treasury_all_history_operation(generation, provider_dataset, context)
            .await?;
        let source = operation.source()?;
        let common = operation.common();
        let initial = source.start_all_history_backfill(provider_dataset)?;
        let initial_checkpoint = initial.checkpoint().to_json()?.into_boxed_slice();
        let descriptor = source
            .activation_intent()
            .catalog()
            .dataset(provider_dataset)
            .ok_or(TreasuryApplicationError::InvalidSelection)?;
        let analytical_dataset = DatasetId::try_from(descriptor.analytical_dataset().as_str())
            .map_err(|_error| TreasuryApplicationError::InvalidSelection)?;
        let source_generation_digest = generation.generation_digest()?;
        let plan_identity = treasury_all_history_plan_identity(
            &analytical_dataset,
            provider_dataset,
            source_generation_digest,
            descriptor.query_digest(),
            source.activation_intent().intent_digest(),
        )?;
        let input = ProviderMacroPlanSessionInput::try_new(
            analytical_dataset.clone(),
            generation.metadata().source_id().clone(),
            generation.metadata().revision().clone(),
            provider_dataset.clone(),
            source_generation_digest,
            plan_identity,
            initial_checkpoint,
        )?;
        let session = self
            .research
            .analytical()
            .begin_staged_provider_macro_plan(input)?;
        common.ensure_live()?;
        let store = self.research.provider_capture_store();
        let (completed, observed_at) = if session.is_complete() {
            let backfill = source.restore_all_history_backfill(session.checkpoint(), &store)?;
            let completion = backfill.acquisition_completion()?;
            validate_all_history_completion(
                &completion,
                &analytical_dataset,
                provider_dataset,
                &session,
            )?;
            let observed_at = all_history_terminal_received_at(&completion)?;
            let completed = self
                .research
                .analytical()
                .recover_completed_provider_macro_plan(session.session_id())?;
            (completed, observed_at)
        } else {
            let mut backfill = source.restore_all_history_backfill(session.checkpoint(), &store)?;
            let discovery = DiscoveryRequest::try_new(
                provider_dataset.clone(),
                None,
                NonZeroU16::new(1).ok_or(TreasuryApplicationError::InvalidAcquisition)?,
                common.provider_deadline()?,
            )
            .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?;
            let fetched = source
                .fetch_next_all_history_page(
                    &backfill,
                    common.extraction(),
                    discovery,
                    common.cancellation().clone(),
                )
                .await
                .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?;
            let terminal = fetched.terminal();
            let (canonical, capture, admission) = fetched.into_parts();
            let (expectation, seal_request) = capture.into_whole_seal_parts();
            let sealed = self
                .research
                .seal_provider_capture(
                    seal_request,
                    common.cancellation(),
                    common.operation_deadline(),
                )
                .await?;
            let token = expectation.try_rejoin(sealed)?.try_into_whole()?;
            let persisted = token.persisted_receipt().clone();
            backfill.accept_sealed_page(admission, persisted)?;
            let successor_checkpoint = backfill.checkpoint().to_json()?.into_boxed_slice();
            if terminal {
                if canonical.is_some() {
                    return Err(TreasuryApplicationError::InvalidAcquisition);
                }
                let completion = backfill.acquisition_completion()?;
                validate_all_history_completion(
                    &completion,
                    &analytical_dataset,
                    provider_dataset,
                    &session,
                )?;
                let observed_at = all_history_terminal_received_at(&completion)?;
                let terminal = ProviderMacroPlanTerminal::try_new(
                    completion.completion_digest(),
                    token,
                    successor_checkpoint,
                )?;
                let completed = self
                    .research
                    .analytical()
                    .complete_staged_provider_macro_plan(
                        &session,
                        terminal,
                        common.cancellation().clone(),
                    )
                    .await?;
                (completed, observed_at)
            } else {
                let canonical = canonical.ok_or(TreasuryApplicationError::InvalidAcquisition)?;
                let staged = treasury_all_history_staged_page(
                    source,
                    provider_dataset,
                    session.data_page_count(),
                    canonical,
                    token,
                    successor_checkpoint,
                )?;
                self.research
                    .analytical()
                    .stage_provider_macro_plan_page(&session, staged, common.cancellation().clone())
                    .await?;
                return Ok(None);
            }
        };
        common.ensure_live()?;
        if completed.session().analytical_dataset() != &analytical_dataset
            || completed.session().provider_dataset() != provider_dataset
            || completed.session().source_generation_digest() != source_generation_digest
            || completed.session().plan_identity() != plan_identity
        {
            return Err(TreasuryApplicationError::InvalidAcquisition);
        }
        let rights = common.rights_decision(completed.publication_digest(), observed_at)?;
        let identity = IngestIdentity::try_new(
            generation.metadata().source_id().clone(),
            completed.publication_digest(),
            SourceOperation::Persist,
            treasury_all_history_ingest_identity(
                &analytical_dataset,
                provider_dataset,
                source_generation_digest,
                completed.publication_digest(),
            )?,
        )
        .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?;
        let reservation = self
            .research
            .analytical()
            .reserve_source_ingest(
                generation.metadata(),
                observed_at,
                rights,
                &identity,
                common.cancellation(),
            )
            .await?;
        let committed = self
            .research
            .analytical()
            .publish_staged_provider_macro_plan(
                reservation,
                completed,
                common.cancellation().clone(),
                common.publication_authority(),
            )
            .await?;
        let restart = TreasuryMacroRestartSelector::all_history(committed.manifest().clone())?;
        restart.verify_for_runtime_generation(self.research.as_ref(), generation)?;
        Ok(Some(TreasuryMacroPublicationReceipt {
            manifest: committed.manifest().clone(),
            restart,
        }))
    }

    fn reopen_generation(
        research: &ResearchService,
        surface: TreasurySurface,
        provider_dataset: &SourceIdentifier,
        generation: AnalyticalGeneration,
    ) -> Result<TreasuryMacroPublicationReceipt, TreasuryApplicationError> {
        let analytical_dataset = treasury_analytical_dataset(surface, provider_dataset)?;
        let expected_source = treasury_source_id(surface)?;
        let manifest = generation.manifest().clone();
        if generation.source_id() != &expected_source
            || manifest.dataset_id() != &analytical_dataset
            || !surface_accepts_provider_dataset(surface, provider_dataset)
        {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        let store = research.provider_capture_store();
        let owned = research
            .analytical()
            .generation_owned_provider_capture_evidence(&manifest, store.as_ref())?;
        let [object] = owned.objects() else {
            return Err(TreasuryApplicationError::RestartInvalid);
        };
        let [input] = object.inputs() else {
            return Err(TreasuryApplicationError::RestartInvalid);
        };
        let evidence = input.binding();
        if owned.pinned().manifest() != &manifest
            || owned.source_id() != &expected_source
            || object.publication_ordinal() != 0
            || object.generation_object_ordinal().checked_add(1)
                != Some(owned.pinned().objects().len())
            || input.input_ordinal() != 0
            || input.object_input_ordinal() != 0
            || object.object().object().row_count()
                != u64::try_from(evidence.record_count()).unwrap_or(u64::MAX)
        {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        validate_restored_treasury_evidence(surface, provider_dataset, &expected_source, evidence)?;
        let row_capture_page_ordinals = evidence
            .rows()
            .iter()
            .map(|row| row.capture_page_ordinal())
            .collect::<Vec<_>>();
        let published_series = TreasuryPublishedSeriesInventory::try_from_evidence(
            surface,
            provider_dataset,
            evidence,
        )?;
        let restart = TreasurySingleMacroRestartSelector::try_new(
            surface,
            manifest.clone(),
            evidence.binding_digest(),
            expected_source,
            provider_dataset.clone(),
            evidence.record_count(),
            evidence
                .capture()
                .metadata_revision()
                .as_source_identifier()
                .clone(),
            evidence.extraction_content_identity(),
            evidence.sealed_capture_receipt_digest(),
            evidence.native_lineage().version(),
            evidence.native_lineage().fingerprint(),
            evidence.native_lineage().batch_digest(),
            row_capture_page_ordinals,
            published_series,
        )?;
        restart.verify(research)?;
        Ok(TreasuryMacroPublicationReceipt {
            manifest,
            restart: TreasuryMacroRestartSelector::Single(restart),
        })
    }

    /// Runs one registered, receipt-selected, bounded Treasury acquisition and seals all exact
    /// provider responses through the sole application raw store.
    ///
    /// The retained runtime publication lease prevents source replacement between extraction and
    /// the later atomic canonical/native/raw commit. Dropping the result releases that lease and
    /// permits startup recovery to quarantine the intentionally unreferenced physical segment.
    pub(crate) async fn acquire_and_seal(
        &self,
        request: TreasurySelectedObjectRequest,
        context: &RequestContext,
        seal_deadline: Instant,
    ) -> Result<TreasurySealedPublicationHandoff, TreasuryApplicationError> {
        let _call = DomainLifecycle::enter(&self.coordinator.lifecycle, context)?;
        let operation_deadline =
            operation_deadline(context, self.coordinator.limits.operation_duration)?;
        let seal_deadline = seal_deadline.min(operation_deadline);
        let operation = self.coordinator.lifecycle.shutdown_token().child_token();
        let profile = SourceIdentifier::try_from(request.surface.profile_id())
            .map_err(|_error| TreasuryApplicationError::InvalidSelection)?;
        let extracted = self
            .coordinator
            .extract_selected(
                &request.discovery_receipt,
                &profile,
                &request.provider_dataset,
                &request.object_id,
                context,
                &operation,
                operation_deadline,
            )
            .await?;
        let super::AuthorizedExtraction {
            metadata,
            publication,
            company_identity,
            revisions,
            analytical_dataset,
            payload_digest,
            rights,
            admission,
        } = extracted;
        let super::ManagedPendingProviderPublication {
            batch,
            capture_material: capture,
            provider_native,
        } = publication
            .into_pending_provider()
            .ok_or(TreasuryApplicationError::InvalidAcquisition)?;
        let revisions = revisions.ok_or(TreasuryApplicationError::InvalidAcquisition)?;
        validate_extraction(
            request.surface,
            &request.provider_dataset,
            &metadata,
            &batch,
            &revisions,
            &analytical_dataset,
            payload_digest,
            &rights,
            company_identity.is_none(),
        )?;
        let super::ManagedProviderNativePublication {
            native_lineage,
            row_capture_page_ordinals,
        } = provider_native.ok_or(TreasuryApplicationError::InvalidAcquisition)?;
        if native_lineage.schema().implementation()
            != ProviderNativeLineageImplementation::UsTreasuryMacroV1
            || native_lineage.validate(&batch).is_err()
            || row_capture_page_ordinals.len() != batch.records().len()
        {
            return Err(TreasuryApplicationError::InvalidAcquisition);
        }

        let publication_lease: Arc<dyn IngestPrecommitAuthority> =
            Arc::new(admission.acquire_publication_lease().await?);
        publication_lease.validate_precommit()?;
        let (expectation, seal_request) = capture.into_whole_seal_parts();
        let sealed = self
            .research
            .seal_provider_capture(seal_request, &operation, seal_deadline)
            .await?;
        let token = expectation.try_rejoin(sealed)?.try_into_whole()?;
        let binding = SealedProviderCaptureBinding::try_whole(
            token,
            batch,
            native_lineage,
            row_capture_page_ordinals,
        )?;
        publication_lease.validate_precommit()?;
        ensure_operation_live(operation_deadline, &operation)?;

        let handoff = TreasurySealedPublicationHandoff {
            surface: request.surface,
            source: metadata,
            rights,
            analytical_dataset,
            provider_dataset: request.provider_dataset,
            revisions,
            payload_digest,
            binding,
            publication_lease,
        };
        handoff.validate()?;
        Ok(handoff)
    }

    /// Atomically publishes one sealed Treasury macro plan and proves catalog restart
    /// reconstruction.
    pub(crate) async fn publish(
        &self,
        handoff: TreasurySealedPublicationHandoff,
        context: &RequestContext,
    ) -> Result<TreasuryMacroPublicationReceipt, TreasuryApplicationError> {
        let _call = DomainLifecycle::enter(&self.coordinator.lifecycle, context)?;
        let deadline = operation_deadline(context, self.coordinator.limits.operation_duration)?;
        let operation = self.coordinator.lifecycle.shutdown_token().child_token();
        handoff.validate()?;
        let TreasurySealedPublicationHandoff {
            surface,
            source,
            mut rights,
            analytical_dataset,
            provider_dataset,
            revisions,
            payload_digest,
            binding,
            publication_lease,
        } = handoff;
        publication_lease.validate_precommit()?;
        let binding_digest = binding.evidence_digest().evidence();
        let record_count = binding.record_count();
        let native_schema_version = binding.native_lineage().schema().version();
        let native_schema_fingerprint = binding.native_lineage().schema().fingerprint();
        let native_batch_digest = binding.native_lineage().batch_digest();
        let extraction_content_identity = binding.content_identity().digest();
        let sealed_capture_receipt_digest = binding.sealed_capture_receipt_digest();
        let metadata_revision = binding
            .capture_evidence()
            .metadata_revision()
            .as_source_identifier()
            .clone();
        let published_series =
            TreasuryPublishedSeriesInventory::try_from_binding(surface, &binding)?;
        let mut row_capture_page_ordinals = Vec::new();
        row_capture_page_ordinals
            .try_reserve_exact(binding.row_frames().len())
            .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?;
        for (ordinal, frame) in binding.row_frames().iter().enumerate() {
            if frame.canonical_row_ordinal() != u32::try_from(ordinal).unwrap_or(u32::MAX) {
                return Err(TreasuryApplicationError::InvalidAcquisition);
            }
            row_capture_page_ordinals.push(frame.capture_page_ordinal());
        }
        let source_id = source.source_id().clone();
        let source_generation_digest = source
            .revision_evidence()
            .payload_evidence()
            .content_digest();
        let native_sidecar = binding
            .native_lineage()
            .batch_sidecar()
            .ok_or(TreasuryApplicationError::InvalidAcquisition)?;
        let candidate_digest = treasury_macro_candidate_digest(
            surface,
            &provider_dataset,
            payload_digest,
            binding_digest,
            native_sidecar.semantic_payload_digest(),
        )?;
        let semantics = ProviderMacroPlanSemantics::try_new(
            SourceIdentifier::try_from(TREASURY_MACRO_SEMANTICS_SCHEMA)
                .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?,
            native_schema_fingerprint,
            native_sidecar.semantic_payload_digest(),
            native_sidecar
                .semantic_payload()
                .to_vec()
                .into_boxed_slice(),
        )?;
        let completion_digest = treasury_macro_completion_digest(
            surface,
            &analytical_dataset,
            &provider_dataset,
            source_generation_digest,
            candidate_digest,
            binding_digest,
            record_count,
        )?;
        let expected_rows = u64::try_from(record_count)
            .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?;
        let chunk = ProviderMacroPlanChunkInput::try_new(
            0,
            1,
            candidate_digest,
            source_generation_digest,
            semantics,
            binding,
            revisions,
        )?;
        let input = ProviderMacroPlanPublicationInput::try_new(
            analytical_dataset.clone(),
            completion_digest,
            expected_rows,
            vec![chunk],
        )?;
        if input.source_id() != &source_id
            || input.provider_dataset() != &provider_dataset
            || input.source_generation_digest() != source_generation_digest
            || input.total_chunks() != 1
            || input.total_rows() != expected_rows
        {
            return Err(TreasuryApplicationError::InvalidAcquisition);
        }
        let publication_digest = input.publication_digest();
        rights.payload_digest = publication_digest;
        let identity = IngestIdentity::try_new(
            source_id.clone(),
            publication_digest,
            SourceOperation::Persist,
            treasury_macro_ingest_identity(
                surface,
                &analytical_dataset,
                &provider_dataset,
                source_generation_digest,
                publication_digest,
            )?,
        )
        .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?;
        let registered_at = rights.retrieved_at;
        let reservation = self.research.analytical().reserve_source_ingest(
            &source,
            registered_at,
            rights,
            &identity,
            &operation,
        );
        tokio::pin!(reservation);
        let reservation = tokio::select! {
            biased;
            () = context.cancellation().cancelled() => {
                operation.cancel();
                return Err(ServiceError::Cancelled.into());
            }
            () = tokio::time::sleep_until(deadline.into()) => {
                operation.cancel();
                return Err(ServiceError::DeadlineExceeded.into());
            }
            () = operation.cancelled() => return Err(ServiceError::Unavailable.into()),
            result = reservation.as_mut() => result?,
        };
        publication_lease.validate_precommit()?;
        let pending = self
            .research
            .analytical()
            .prepare_provider_macro_plan_publication(reservation, input)?;
        let publication = pending.commit(
            self.research.analytical(),
            operation.clone(),
            Arc::clone(&publication_lease),
        );
        tokio::pin!(publication);
        let committed = tokio::select! {
            biased;
            () = context.cancellation().cancelled() => {
                operation.cancel();
                return Err(ServiceError::Cancelled.into());
            }
            () = tokio::time::sleep_until(deadline.into()) => {
                operation.cancel();
                return Err(ServiceError::DeadlineExceeded.into());
            }
            () = operation.cancelled() => return Err(ServiceError::Unavailable.into()),
            result = publication.as_mut() => result?,
        };
        publication_lease.validate_precommit()?;
        let generic_restart = committed.restart_selector();
        let reopened = self
            .research
            .analytical()
            .verify_provider_macro_plan_restart(&generic_restart)?;
        if reopened.manifest() != committed.manifest()
            || generic_restart.total_chunks() != 1
            || generic_restart.total_rows() != expected_rows
        {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        let restart = TreasurySingleMacroRestartSelector::try_new(
            surface,
            committed.manifest().clone(),
            binding_digest,
            source_id,
            provider_dataset,
            record_count,
            metadata_revision,
            extraction_content_identity,
            sealed_capture_receipt_digest,
            native_schema_version,
            native_schema_fingerprint,
            native_batch_digest,
            row_capture_page_ordinals,
            published_series,
        )?;
        restart.verify(self.research.as_ref())?;
        Ok(TreasuryMacroPublicationReceipt {
            manifest: committed.manifest().clone(),
            restart: TreasuryMacroRestartSelector::Single(restart),
        })
    }

    /// Reopens exact raw/native evidence and a fixed latest-known Fiscal Data snapshot.
    pub(crate) async fn read_fiscal_data_latest_known(
        &self,
        request: TreasuryFiscalDataLatestKnownRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<TreasuryFiscalDataLatestKnownReceipt, TreasuryApplicationError> {
        let common = self
            .read_latest_known(request.common, limits, deadline, cancellation)
            .await?;
        Ok(TreasuryFiscalDataLatestKnownReceipt { common })
    }

    /// Reopens exact raw/native evidence and a fixed latest-known daily-rate snapshot.
    pub(crate) async fn read_daily_rates_latest_known(
        &self,
        request: TreasuryDailyRatesLatestKnownRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<TreasuryDailyRatesLatestKnownReceipt, TreasuryApplicationError> {
        let common = self
            .read_latest_known(request.common, limits, deadline, cancellation)
            .await?;
        Ok(TreasuryDailyRatesLatestKnownReceipt { common })
    }

    async fn read_latest_known(
        &self,
        request: TreasuryLatestKnownRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<TreasuryLatestKnownReceipt, TreasuryApplicationError> {
        let TreasuryLatestKnownRequest {
            restart,
            analytical,
        } = request;
        let expected_source = restart.source_id()?;
        if analytical.manifest() != restart.manifest() || analytical.source_id() != &expected_source
        {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        let pinned = restart.verify(self.research.as_ref())?;
        let output = self
            .research
            .analytical_reader()
            .read_macro_latest_known_snapshot(analytical, limits, deadline, cancellation)
            .await?;
        if output.source_id() != &expected_source
            || output.output().manifest() != restart.manifest()
        {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        Ok(TreasuryLatestKnownReceipt {
            restart,
            pinned,
            output,
        })
    }
}

/// Exact sealed canonical/native/raw handoff awaiting one atomic immutable publication.
#[derive(Debug)]
pub(crate) struct TreasurySealedPublicationHandoff {
    surface: TreasurySurface,
    source: SourceMetadata,
    rights: RightsDecisionInput,
    analytical_dataset: DatasetId,
    provider_dataset: SourceIdentifier,
    revisions: ExtractionRevisionPlan,
    payload_digest: EvidenceDigest,
    binding: SealedProviderCaptureBinding,
    publication_lease: Arc<dyn IngestPrecommitAuthority>,
}

impl TreasurySealedPublicationHandoff {
    fn validate(&self) -> Result<(), TreasuryApplicationError> {
        self.binding.validate()?;
        let capture = self.binding.capture_evidence();
        let batch = self.binding.batch();
        if capture.source_id() != self.source.source_id()
            || capture.metadata_revision() != self.source.revision()
            || capture.dataset() != &self.provider_dataset
            || batch.request().object().dataset() != &self.provider_dataset
            || &self.rights.source_id != self.source.source_id()
            || self.rights.payload_digest != self.payload_digest
            || self.revisions.len() != batch.records().len()
            || !self.revisions.native_lineage_required()
            || self.binding.native_lineage().schema().implementation()
                != ProviderNativeLineageImplementation::UsTreasuryMacroV1
        {
            return Err(TreasuryApplicationError::InvalidAcquisition);
        }
        self.publication_lease.validate_precommit()?;
        Ok(())
    }

    /// Returns the independently configured Treasury product.
    pub(crate) const fn surface(&self) -> TreasurySurface {
        self.surface
    }

    /// Returns the exact provider selector whose bytes were sealed.
    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the storage-safe immutable dataset target.
    pub(crate) const fn analytical_dataset(&self) -> &DatasetId {
        &self.analytical_dataset
    }

    /// Returns the exact canonical Macro row count awaiting atomic publication.
    pub(crate) fn record_count(&self) -> usize {
        self.binding.record_count()
    }
}

/// Immutable Treasury manifest plus exact catalog-reconstructible raw/native binding.
#[derive(Debug)]
pub(crate) struct TreasuryMacroPublicationReceipt {
    manifest: DatasetManifestRef,
    restart: TreasuryMacroRestartSelector,
}

impl TreasuryMacroPublicationReceipt {
    /// Returns the immutable generation selected by publication or restart reopening.
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the exact selector that reopens the generation and binding after restart.
    pub(crate) const fn restart_selector(&self) -> &TreasuryMacroRestartSelector {
        &self.restart
    }
}

/// Exact immutable Treasury generation and raw/native binding needed after restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TreasuryMacroRestartSelector {
    Single(TreasurySingleMacroRestartSelector),
    AllHistory(ProviderMacroPlanManifestSelector),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TreasurySingleMacroRestartSelector {
    surface: TreasurySurface,
    manifest: DatasetManifestRef,
    binding_digest: EvidenceDigest,
    source_id: SourceId,
    provider_dataset: SourceIdentifier,
    expected_input_records: usize,
    metadata_revision: SourceIdentifier,
    extraction_content_identity: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    native_schema_version: u16,
    native_schema_fingerprint: EvidenceDigest,
    native_batch_digest: EvidenceDigest,
    row_capture_page_ordinals: Box<[u16]>,
    published_series: AnalyticalMacroSeriesAllowlist,
    published_series_binding_digest: EvidenceDigest,
}

struct TreasuryPublishedSeriesInventory {
    series: AnalyticalMacroSeriesAllowlist,
    canonical_row_digests: Box<[EvidenceDigest]>,
}

impl TreasuryPublishedSeriesInventory {
    fn try_from_binding(
        surface: TreasurySurface,
        binding: &SealedProviderCaptureBinding,
    ) -> Result<Self, TreasuryApplicationError> {
        let records = binding.batch().records();
        if records.is_empty() || records.len() != binding.record_count() {
            return Err(TreasuryApplicationError::InvalidAcquisition);
        }
        let mut series = Vec::new();
        let mut canonical_row_digests = Vec::new();
        canonical_row_digests
            .try_reserve_exact(records.len())
            .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?;
        for record in records {
            let ResearchObservation::Macro(observation) = serde_json::from_slice(record.payload())
                .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?
            else {
                return Err(TreasuryApplicationError::InvalidAcquisition);
            };
            if !surface_accepts_series(
                surface,
                binding.batch().request().object().dataset(),
                observation.series(),
            ) {
                return Err(TreasuryApplicationError::InvalidAcquisition);
            }
            if !series.contains(observation.series()) {
                if series.len() == MAX_TREASURY_LATEST_KNOWN_SERIES {
                    return Err(TreasuryApplicationError::InvalidAcquisition);
                }
                series.push(observation.series().clone());
            }
            let canonical_digest = record.evidence().content_digest();
            if canonical_digest.algorithm() != DigestAlgorithm::Sha256
                || canonical_digest.bytes() == [0; 32]
            {
                return Err(TreasuryApplicationError::InvalidAcquisition);
            }
            canonical_row_digests.push(canonical_digest);
        }
        let series = AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(series)
            .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?;
        Ok(Self {
            series,
            canonical_row_digests: canonical_row_digests.into_boxed_slice(),
        })
    }

    fn try_from_evidence(
        surface: TreasurySurface,
        provider_dataset: &SourceIdentifier,
        evidence: &PersistedProviderCaptureBindingEvidence,
    ) -> Result<Self, TreasuryApplicationError> {
        if evidence.rows().is_empty() || evidence.rows().len() != evidence.record_count() {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        let mut series = Vec::new();
        let mut canonical_row_digests = Vec::new();
        series
            .try_reserve_exact(MAX_TREASURY_LATEST_KNOWN_SERIES)
            .map_err(|_error| TreasuryApplicationError::RestartInvalid)?;
        canonical_row_digests
            .try_reserve_exact(evidence.rows().len())
            .map_err(|_error| TreasuryApplicationError::RestartInvalid)?;
        for row in evidence.rows() {
            let restored: TreasuryPersistedNativeRowV1 =
                serde_json::from_slice(row.native_semantic_payload())
                    .map_err(|_error| TreasuryApplicationError::RestartInvalid)?;
            if !surface_accepts_series(surface, provider_dataset, &restored.canonical_series) {
                return Err(TreasuryApplicationError::RestartInvalid);
            }
            if !series.contains(&restored.canonical_series) {
                if series.len() == MAX_TREASURY_LATEST_KNOWN_SERIES {
                    return Err(TreasuryApplicationError::RestartInvalid);
                }
                series.push(restored.canonical_series);
            }
            let canonical_digest = row.canonical_row_digest();
            if canonical_digest.algorithm() != DigestAlgorithm::Sha256
                || canonical_digest.bytes() == [0; 32]
            {
                return Err(TreasuryApplicationError::RestartInvalid);
            }
            canonical_row_digests.push(canonical_digest);
        }
        let series = AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(series)
            .map_err(|_error| TreasuryApplicationError::RestartInvalid)?;
        Ok(Self {
            series,
            canonical_row_digests: canonical_row_digests.into_boxed_slice(),
        })
    }
}

#[derive(Deserialize)]
struct TreasuryPersistedNativeRowV1 {
    canonical_series: SourceIdentifier,
}

impl TreasuryMacroRestartSelector {
    fn all_history(manifest: DatasetManifestRef) -> Result<Self, TreasuryApplicationError> {
        if !manifest.dataset_id().as_str().ends_with(".all")
            || !surface_accepts_analytical_dataset(
                TreasurySurface::DailyRatesXml,
                manifest.dataset_id(),
            )
        {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        Ok(Self::AllHistory(ProviderMacroPlanManifestSelector::new(
            manifest,
        )))
    }

    /// Returns the independently configured Treasury product.
    pub(crate) const fn surface(&self) -> TreasurySurface {
        match self {
            Self::Single(selector) => selector.surface,
            Self::AllHistory(_) => TreasurySurface::DailyRatesXml,
        }
    }

    /// Returns the exact immutable generation and sole all-history restart coordinate.
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        match self {
            Self::Single(selector) => &selector.manifest,
            Self::AllHistory(selector) => selector.manifest(),
        }
    }

    /// Returns the bounded exact canonical series inventory when it is carried by the legacy
    /// single-response Fiscal publication.
    pub(crate) const fn single_published_series(&self) -> Option<&AnalyticalMacroSeriesAllowlist> {
        match self {
            Self::Single(selector) => Some(&selector.published_series),
            Self::AllHistory(_) => None,
        }
    }

    pub(crate) const fn is_all_history(&self) -> bool {
        matches!(self, Self::AllHistory(_))
    }

    fn source_id(&self) -> Result<SourceId, TreasuryApplicationError> {
        match self {
            Self::Single(selector) => Ok(selector.source_id.clone()),
            Self::AllHistory(_) => treasury_source_id(TreasurySurface::DailyRatesXml),
        }
    }

    fn verify(
        &self,
        research: &ResearchService,
    ) -> Result<PinnedDataset, TreasuryApplicationError> {
        match self {
            Self::Single(selector) => selector.verify(research).map(|(pinned, _evidence)| pinned),
            Self::AllHistory(selector) => {
                let evidence = research
                    .analytical()
                    .verify_staged_provider_macro_plan_restart(selector)?;
                validate_all_history_restart(selector, &evidence)?;
                Ok(evidence.pinned().clone())
            }
        }
    }

    fn verify_for_runtime_generation(
        &self,
        research: &ResearchService,
        generation: &ResearchProviderRuntimeGeneration,
    ) -> Result<PinnedDataset, TreasuryApplicationError> {
        let expected_source = self.source_id()?;
        if generation.profile().as_str() != self.surface().profile_id()
            || generation.metadata().source_id() != &expected_source
        {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        match self {
            Self::Single(selector) => {
                if generation.metadata().revision().as_source_identifier()
                    != &selector.metadata_revision
                {
                    return Err(TreasuryApplicationError::RestartInvalid);
                }
                selector.verify(research).map(|(pinned, _evidence)| pinned)
            }
            Self::AllHistory(selector) => {
                let evidence = research
                    .analytical()
                    .verify_staged_provider_macro_plan_restart(selector)?;
                let session = evidence.completed().session();
                if session.source_id() != generation.metadata().source_id()
                    || session.metadata_revision() != generation.metadata().revision()
                    || session.source_generation_digest() != generation.generation_digest()?
                {
                    return Err(TreasuryApplicationError::RestartInvalid);
                }
                validate_all_history_restart(selector, &evidence)?;
                Ok(evidence.pinned().clone())
            }
        }
    }
}

impl TreasurySingleMacroRestartSelector {
    /// Binds one future Treasury publication receipt to exact durable read coordinates.
    #[allow(
        clippy::too_many_arguments,
        reason = "surface, immutable generation, raw binding, source, and native schema are independent restart coordinates"
    )]
    fn try_new(
        surface: TreasurySurface,
        manifest: DatasetManifestRef,
        binding_digest: EvidenceDigest,
        source_id: SourceId,
        provider_dataset: SourceIdentifier,
        expected_input_records: usize,
        metadata_revision: SourceIdentifier,
        extraction_content_identity: EvidenceDigest,
        sealed_capture_receipt_digest: EvidenceDigest,
        native_schema_version: u16,
        native_schema_fingerprint: EvidenceDigest,
        native_batch_digest: EvidenceDigest,
        row_capture_page_ordinals: Vec<u16>,
        published_series: TreasuryPublishedSeriesInventory,
    ) -> Result<Self, TreasuryApplicationError> {
        if !surface_accepts_provider_dataset(surface, &provider_dataset)
            || binding_digest.bytes() == [0; 32]
            || extraction_content_identity.bytes() == [0; 32]
            || sealed_capture_receipt_digest.bytes() == [0; 32]
            || native_schema_fingerprint.bytes() == [0; 32]
            || native_batch_digest.bytes() == [0; 32]
            || expected_input_records == 0
            || row_capture_page_ordinals.len() != expected_input_records
            || (surface == TreasurySurface::DailyRatesXml
                && row_capture_page_ordinals
                    .iter()
                    .any(|ordinal| *ordinal != 0))
            || native_schema_version == 0
            || !surface_accepts_analytical_dataset(surface, manifest.dataset_id())
            || published_series.canonical_row_digests.len() != expected_input_records
            || published_series
                .series
                .series()
                .iter()
                .any(|series| !surface_accepts_series(surface, &provider_dataset, series))
        {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        let published_series_binding_digest = published_series_binding_digest(
            surface,
            &manifest,
            &source_id,
            &provider_dataset,
            expected_input_records,
            extraction_content_identity,
            native_batch_digest,
            &published_series.series,
            published_series.canonical_row_digests.iter().copied(),
        )?;
        Ok(Self {
            surface,
            manifest,
            binding_digest,
            source_id,
            provider_dataset,
            expected_input_records,
            metadata_revision,
            extraction_content_identity,
            sealed_capture_receipt_digest,
            native_schema_version,
            native_schema_fingerprint,
            native_batch_digest,
            row_capture_page_ordinals: row_capture_page_ordinals.into_boxed_slice(),
            published_series: published_series.series,
            published_series_binding_digest,
        })
    }

    /// Returns the independent Treasury product retained by this selector.
    pub(crate) const fn surface(&self) -> TreasurySurface {
        self.surface
    }

    /// Returns the exact immutable generation.
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the sole source-rights owner.
    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the bounded exact canonical series inventory derived before publication.
    pub(crate) const fn published_series(&self) -> &AnalyticalMacroSeriesAllowlist {
        &self.published_series
    }

    fn verify(
        &self,
        research: &ResearchService,
    ) -> Result<(PinnedDataset, PersistedProviderCaptureBindingEvidence), TreasuryApplicationError>
    {
        let store = research.provider_capture_store();
        let owned = research
            .analytical()
            .generation_owned_provider_capture_evidence(&self.manifest, store.as_ref())?;
        let [object] = owned.objects() else {
            return Err(TreasuryApplicationError::RestartInvalid);
        };
        let [input] = object.inputs() else {
            return Err(TreasuryApplicationError::RestartInvalid);
        };
        let evidence = input.binding();
        if owned.pinned().manifest() != &self.manifest
            || owned.source_id() != &self.source_id
            || object.publication_ordinal() != 0
            || object.generation_object_ordinal().checked_add(1)
                != Some(owned.pinned().objects().len())
            || input.input_ordinal() != 0
            || input.object_input_ordinal() != 0
            || object.object().object().row_count()
                != u64::try_from(self.expected_input_records).unwrap_or(u64::MAX)
        {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        validate_restored_treasury_evidence(
            self.surface,
            &self.provider_dataset,
            &self.source_id,
            evidence,
        )?;
        if evidence.binding_digest() != self.binding_digest
            || evidence.capture().source_id() != &self.source_id
            || evidence.capture().dataset() != &self.provider_dataset
            || evidence
                .capture()
                .metadata_revision()
                .as_source_identifier()
                != &self.metadata_revision
            || evidence.extraction_content_identity() != self.extraction_content_identity
            || evidence.sealed_capture_receipt_digest() != self.sealed_capture_receipt_digest
            || evidence.record_count() != self.expected_input_records
            || evidence.record_count() != evidence.rows().len()
            || evidence.native_lineage().implementation() != TREASURY_NATIVE_IMPLEMENTATION
            || evidence.native_lineage().version() != self.native_schema_version
            || evidence.native_lineage().fingerprint() != self.native_schema_fingerprint
            || evidence.native_lineage().batch_digest() != self.native_batch_digest
            || evidence.scope() != "whole"
            || evidence.layout() != "whole_single_segment"
            || evidence.component_ordinal().is_some()
            || evidence.rows().len() != self.row_capture_page_ordinals.len()
            || evidence
                .rows()
                .iter()
                .zip(self.row_capture_page_ordinals.iter())
                .enumerate()
                .any(|(ordinal, (row, expected_page))| {
                    row.canonical_row_ordinal() != u32::try_from(ordinal).unwrap_or(u32::MAX)
                        || row.capture_page_ordinal() != *expected_page
                })
            || published_series_binding_digest(
                self.surface,
                &self.manifest,
                &self.source_id,
                &self.provider_dataset,
                self.expected_input_records,
                self.extraction_content_identity,
                self.native_batch_digest,
                &self.published_series,
                evidence.rows().iter().map(|row| row.canonical_row_digest()),
            )? != self.published_series_binding_digest
        {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        Ok((owned.pinned().clone(), evidence.clone()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TreasuryLatestKnownRequest {
    restart: TreasuryMacroRestartSelector,
    analytical: AnalyticalMacroLatestKnownRequest,
}

impl TreasuryLatestKnownRequest {
    fn try_new(
        restart: TreasuryMacroRestartSelector,
        series_allowlist: AnalyticalMacroSeriesAllowlist,
        knowledge_cutoff: Timestamp,
        effective_date_cutoff: CalendarDate,
    ) -> Result<Self, TreasuryApplicationError> {
        let analytical = AnalyticalMacroLatestKnownRequest::try_new(
            restart.manifest().clone(),
            restart.source_id()?,
            knowledge_cutoff,
            effective_date_cutoff,
            series_allowlist,
        )?;
        Ok(Self {
            restart,
            analytical,
        })
    }
}

/// Exact generation-bound Fiscal Data latest-known request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TreasuryFiscalDataLatestKnownRequest {
    common: TreasuryLatestKnownRequest,
}

impl TreasuryFiscalDataLatestKnownRequest {
    /// Pins the selected Average Interest Rates series and independent PIT cutoffs.
    pub(crate) fn try_new(
        restart: TreasuryMacroRestartSelector,
        series_allowlist: AnalyticalMacroSeriesAllowlist,
        knowledge_cutoff: Timestamp,
        effective_date_cutoff: CalendarDate,
    ) -> Result<Self, TreasuryApplicationError> {
        if restart.surface() != TreasurySurface::FiscalData {
            return Err(TreasuryApplicationError::SurfaceMismatch);
        }
        TreasuryLatestKnownRequest::try_new(
            restart,
            series_allowlist,
            knowledge_cutoff,
            effective_date_cutoff,
        )
        .map(|common| Self { common })
    }

    /// Returns the fixed application operation identity.
    pub(crate) const fn operation_identity(&self) -> &'static str {
        TREASURY_FISCAL_DATA_LATEST_KNOWN_OPERATION
    }

    /// Returns the minimum candidate row envelope including saturation sentinel.
    pub(crate) fn required_query_rows(&self) -> u64 {
        self.common.analytical.required_query_rows()
    }
}

/// Exact generation-bound Treasury daily-rate latest-known request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TreasuryDailyRatesLatestKnownRequest {
    common: TreasuryLatestKnownRequest,
}

impl TreasuryDailyRatesLatestKnownRequest {
    /// Pins the selected daily-rate series and independent PIT cutoffs.
    pub(crate) fn try_new(
        restart: TreasuryMacroRestartSelector,
        series_allowlist: AnalyticalMacroSeriesAllowlist,
        knowledge_cutoff: Timestamp,
        effective_date_cutoff: CalendarDate,
    ) -> Result<Self, TreasuryApplicationError> {
        if restart.surface() != TreasurySurface::DailyRatesXml {
            return Err(TreasuryApplicationError::SurfaceMismatch);
        }
        TreasuryLatestKnownRequest::try_new(
            restart,
            series_allowlist,
            knowledge_cutoff,
            effective_date_cutoff,
        )
        .map(|common| Self { common })
    }

    /// Returns the fixed application operation identity.
    pub(crate) const fn operation_identity(&self) -> &'static str {
        TREASURY_DAILY_RATES_LATEST_KNOWN_OPERATION
    }

    /// Returns the minimum candidate row envelope including saturation sentinel.
    pub(crate) fn required_query_rows(&self) -> u64 {
        self.common.analytical.required_query_rows()
    }
}

#[derive(Debug)]
struct TreasuryLatestKnownReceipt {
    restart: TreasuryMacroRestartSelector,
    pinned: PinnedDataset,
    output: AnalyticalMacroLatestKnownOutput,
}

/// Exact raw/native and typed Fiscal Data evidence reopened after restart.
#[derive(Debug)]
pub(crate) struct TreasuryFiscalDataLatestKnownReceipt {
    common: TreasuryLatestKnownReceipt,
}

impl TreasuryFiscalDataLatestKnownReceipt {
    /// Returns the fixed operation that produced this receipt.
    pub(crate) const fn operation_identity(&self) -> &'static str {
        TREASURY_FISCAL_DATA_LATEST_KNOWN_OPERATION
    }

    /// Returns the exact immutable generation reopened for the query.
    pub(crate) const fn pinned(&self) -> &PinnedDataset {
        &self.common.pinned
    }

    /// Returns the typed latest-known Macro selection and selection digest.
    pub(crate) const fn output(&self) -> &AnalyticalMacroLatestKnownOutput {
        &self.common.output
    }

    /// Returns the exact restart selector revalidated before the read.
    pub(crate) const fn restart_selector(&self) -> &TreasuryMacroRestartSelector {
        &self.common.restart
    }
}

/// Exact raw/native and typed daily-rate evidence reopened after restart.
#[derive(Debug)]
pub(crate) struct TreasuryDailyRatesLatestKnownReceipt {
    common: TreasuryLatestKnownReceipt,
}

impl TreasuryDailyRatesLatestKnownReceipt {
    /// Returns the fixed operation that produced this receipt.
    pub(crate) const fn operation_identity(&self) -> &'static str {
        TREASURY_DAILY_RATES_LATEST_KNOWN_OPERATION
    }

    /// Returns the exact immutable generation reopened for the query.
    pub(crate) const fn pinned(&self) -> &PinnedDataset {
        &self.common.pinned
    }

    /// Returns the typed latest-known Macro selection and selection digest.
    pub(crate) const fn output(&self) -> &AnalyticalMacroLatestKnownOutput {
        &self.common.output
    }

    /// Returns the exact restart selector revalidated before the read.
    pub(crate) const fn restart_selector(&self) -> &TreasuryMacroRestartSelector {
        &self.common.restart
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "each acquired authority coordinate is independent"
)]
fn validate_extraction(
    surface: TreasurySurface,
    provider_dataset: &SourceIdentifier,
    metadata: &SourceMetadata,
    batch: &ExtractionBatch,
    revisions: &ExtractionRevisionPlan,
    analytical_dataset: &DatasetId,
    payload_digest: EvidenceDigest,
    rights: &RightsDecisionInput,
    company_identity_absent: bool,
) -> Result<(), TreasuryApplicationError> {
    let object = batch.request().object();
    if metadata.source_class() != SourceClass::OfficialAgency
        || metadata.provider().as_str() != TREASURY_PROVIDER
        || object.source_id() != metadata.source_id()
        || object.metadata_revision() != metadata.revision()
        || object.dataset() != provider_dataset
        || !surface_accepts_provider_dataset(surface, provider_dataset)
        || !surface_accepts_analytical_dataset(surface, analytical_dataset)
        || batch.records().is_empty()
        || revisions.len() != batch.records().len()
        || !revisions.is_locally_observed()
        || !revisions.native_lineage_required()
        || payload_digest != rights.payload_digest
        || &rights.source_id != metadata.source_id()
        || !metadata.is_effective_at(rights.retrieved_at)
        || !company_identity_absent
    {
        return Err(TreasuryApplicationError::InvalidAcquisition);
    }
    for record in batch.records() {
        let clock_shape_valid = match surface {
            TreasurySurface::FiscalData => record.published_time().is_none(),
            TreasurySurface::DailyRatesXml => record.published_time().is_some(),
        };
        let canonical_macro = serde_json::from_slice(record.payload()).is_ok_and(|value| {
            matches!(value, market_squawk_domain::ResearchObservation::Macro(_))
        });
        if record.source_id() != metadata.source_id()
            || record.metadata_revision() != metadata.revision()
            || record.dataset() != provider_dataset
            || record.available_at().is_none()
            || !clock_shape_valid
            || !canonical_macro
        {
            return Err(TreasuryApplicationError::InvalidAcquisition);
        }
    }
    Ok(())
}

fn surface_accepts_provider_dataset(surface: TreasurySurface, dataset: &SourceIdentifier) -> bool {
    match surface {
        TreasurySurface::FiscalData => dataset.as_str().starts_with(FISCAL_PROVIDER_DATASET_PREFIX),
        TreasurySurface::DailyRatesXml => DAILY_PROVIDER_DATASET_PREFIXES
            .iter()
            .any(|prefix| dataset.as_str().starts_with(prefix)),
    }
}

fn treasury_analytical_dataset(
    surface: TreasurySurface,
    provider_dataset: &SourceIdentifier,
) -> Result<DatasetId, TreasuryApplicationError> {
    let analytical = match surface {
        TreasurySurface::FiscalData => provider_dataset
            .as_str()
            .strip_prefix(FISCAL_PROVIDER_DATASET_PREFIX)
            .filter(|suffix| !suffix.is_empty())
            .map(|suffix| format!("{FISCAL_ANALYTICAL_DATASET_PREFIX}{suffix}")),
        TreasurySurface::DailyRatesXml => DAILY_PROVIDER_DATASET_PREFIXES
            .iter()
            .zip(DAILY_ANALYTICAL_DATASET_PREFIXES)
            .find_map(|(provider_prefix, analytical_prefix)| {
                provider_dataset
                    .as_str()
                    .strip_prefix(provider_prefix)
                    .filter(|suffix| !suffix.is_empty())
                    .map(|suffix| format!("{analytical_prefix}{suffix}"))
            }),
    }
    .ok_or(TreasuryApplicationError::RestartInvalid)?;
    DatasetId::try_from(analytical.as_str())
        .map_err(|_error| TreasuryApplicationError::RestartInvalid)
}

fn treasury_source_id(surface: TreasurySurface) -> Result<SourceId, TreasuryApplicationError> {
    SourceId::try_from(match surface {
        TreasurySurface::FiscalData => TREASURY_FISCAL_SOURCE_ID,
        TreasurySurface::DailyRatesXml => TREASURY_DAILY_SOURCE_ID,
    })
    .map_err(|_error| TreasuryApplicationError::RestartInvalid)
}

fn validate_restored_treasury_evidence(
    surface: TreasurySurface,
    provider_dataset: &SourceIdentifier,
    expected_source: &SourceId,
    evidence: &PersistedProviderCaptureBindingEvidence,
) -> Result<(), TreasuryApplicationError> {
    let capture = evidence.capture();
    let expected_terminal = match surface {
        TreasurySurface::FiscalData => ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
        TreasurySurface::DailyRatesXml => ProviderCaptureTerminalDisposition::StandaloneResponse,
    };
    if evidence.binding_digest().bytes() == [0; 32]
        || capture.source_id() != expected_source
        || capture.dataset() != provider_dataset
        || capture.terminal() != expected_terminal
        || capture.semantic_binding().is_some()
        || capture.pages().is_empty()
        || (surface == TreasurySurface::DailyRatesXml && capture.pages().len() != 1)
        || evidence.scope() != "whole"
        || evidence.layout() != "whole_single_segment"
        || evidence.component_ordinal().is_some()
        || evidence.record_count() == 0
        || evidence.record_count() != evidence.rows().len()
        || evidence.native_lineage().implementation() != TREASURY_NATIVE_IMPLEMENTATION
        || evidence.native_lineage().version() == 0
        || evidence.native_lineage().fingerprint().bytes() == [0; 32]
        || evidence.native_lineage().batch_digest().bytes() == [0; 32]
        || evidence.native_lineage().row_count() != evidence.record_count()
        || evidence.rows().iter().enumerate().any(|(ordinal, row)| {
            row.canonical_row_ordinal() != u32::try_from(ordinal).unwrap_or(u32::MAX)
                || usize::from(row.capture_page_ordinal()) >= capture.pages().len()
                || (surface == TreasurySurface::DailyRatesXml && row.capture_page_ordinal() != 0)
        })
    {
        return Err(TreasuryApplicationError::RestartInvalid);
    }
    Ok(())
}

fn validate_all_history_restart(
    selector: &ProviderMacroPlanManifestSelector,
    evidence: &StagedProviderMacroPlanRestartEvidence,
) -> Result<(), TreasuryApplicationError> {
    let session = evidence.completed().session();
    let provider_dataset = session.provider_dataset();
    let expected_source = treasury_source_id(TreasurySurface::DailyRatesXml)?;
    let expected_analytical =
        treasury_analytical_dataset(TreasurySurface::DailyRatesXml, provider_dataset)?;
    if selector.manifest() != evidence.pinned().manifest()
        || selector.manifest().dataset_id() != &expected_analytical
        || session.analytical_dataset() != &expected_analytical
        || session.source_id() != &expected_source
        || !provider_dataset.as_str().ends_with(":all")
        || !session.is_complete()
        || session.data_page_count() == 0
        || session.analytical_row_count() == 0
        || session
            .response_count()
            .checked_sub(session.data_page_count())
            != Some(1)
        || session.source_generation_digest().bytes() == [0; 32]
        || session.plan_identity().bytes() == [0; 32]
        || evidence.completed().publication_digest().bytes() == [0; 32]
        || evidence.catalog_receipt_digest().bytes() == [0; 32]
    {
        return Err(TreasuryApplicationError::RestartInvalid);
    }
    Ok(())
}

fn surface_accepts_analytical_dataset(surface: TreasurySurface, dataset: &DatasetId) -> bool {
    match surface {
        TreasurySurface::FiscalData => dataset
            .as_str()
            .starts_with(FISCAL_ANALYTICAL_DATASET_PREFIX),
        TreasurySurface::DailyRatesXml => DAILY_ANALYTICAL_DATASET_PREFIXES
            .iter()
            .any(|prefix| dataset.as_str().starts_with(prefix)),
    }
}

fn treasury_macro_candidate_digest(
    surface: TreasurySurface,
    provider_dataset: &SourceIdentifier,
    payload_digest: EvidenceDigest,
    binding_digest: EvidenceDigest,
    native_semantics_digest: EvidenceDigest,
) -> Result<EvidenceDigest, TreasuryApplicationError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/treasury-macro-candidate/v1\0");
    hash_treasury_component(&mut digest, surface.profile_id().as_bytes())?;
    hash_treasury_component(&mut digest, provider_dataset.as_str().as_bytes())?;
    digest.update(payload_digest.bytes());
    digest.update(binding_digest.bytes());
    digest.update(native_semantics_digest.bytes());
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn treasury_all_history_staged_page(
    source: &TreasurySource,
    provider_dataset: &SourceIdentifier,
    page_ordinal: u16,
    canonical: TreasuryAllHistoryCanonicalPage,
    token: ProviderWholeCaptureToken,
    successor_checkpoint: Box<[u8]>,
) -> Result<ProviderMacroPlanStagedPage, TreasuryApplicationError> {
    if canonical.batch().request().object().dataset() != provider_dataset
        || canonical.content_identity().record_count() == 0
    {
        return Err(TreasuryApplicationError::InvalidAcquisition);
    }
    let revisions = source.revision_plan(canonical.batch())?;
    let content_identity = canonical.content_identity().digest();
    let (batch, native_lineage, row_capture_page_ordinals) = canonical.into_publication_parts();
    let native_schema_fingerprint = native_lineage.schema().fingerprint();
    let native_sidecar = native_lineage
        .batch_sidecar()
        .ok_or(TreasuryApplicationError::InvalidAcquisition)?;
    let semantic_digest = native_sidecar.semantic_payload_digest();
    let semantic_payload = native_sidecar
        .semantic_payload()
        .to_vec()
        .into_boxed_slice();
    let binding = SealedProviderCaptureBinding::try_whole(
        token,
        batch,
        native_lineage,
        row_capture_page_ordinals,
    )?;
    let binding_digest = binding.evidence_digest().evidence();
    let candidate_digest = treasury_all_history_page_identity(
        provider_dataset,
        page_ordinal,
        content_identity,
        binding_digest,
        semantic_digest,
    )?;
    let semantics = ProviderMacroPlanSemantics::try_new(
        SourceIdentifier::try_from(TREASURY_MACRO_SEMANTICS_SCHEMA)
            .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?,
        native_schema_fingerprint,
        semantic_digest,
        semantic_payload,
    )?;
    ProviderMacroPlanStagedPage::try_new(
        candidate_digest,
        semantics,
        binding,
        revisions,
        successor_checkpoint,
    )
    .map_err(Into::into)
}

fn validate_all_history_completion(
    completion: &TreasuryAllHistoryAcquisitionCompletion,
    analytical_dataset: &DatasetId,
    provider_dataset: &SourceIdentifier,
    session: &ProviderMacroPlanSessionReceipt,
) -> Result<(), TreasuryApplicationError> {
    let data_page_count = u16::try_from(completion.data_page_count())
        .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?;
    let response_count = u16::try_from(completion.response_count())
        .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?;
    let expected_response_count = if session.is_complete() {
        session.response_count()
    } else {
        session
            .response_count()
            .checked_add(1)
            .ok_or(TreasuryApplicationError::InvalidAcquisition)?
    };
    if completion.descriptor().analytical_dataset().as_str() != analytical_dataset.as_str()
        || completion.descriptor().provider_dataset() != provider_dataset
        || completion.source_id() != session.source_id()
        || completion.metadata_revision() != session.metadata_revision()
        || completion.provider_snapshot_isolation_claimed()
        || data_page_count == 0
        || data_page_count != session.data_page_count()
        || response_count != expected_response_count
        || response_count.checked_sub(data_page_count) != Some(1)
        || completion.canonical_points() != session.analytical_row_count()
        || completion
            .observed_numeric_points()
            .checked_add(completion.explicit_missing_points())
            != Some(completion.canonical_points())
        || completion.raw_body_bytes() == 0
        || completion.payload_digests().len() != usize::from(response_count)
        || completion.canonical_content_digests().count() != usize::from(data_page_count)
        || completion.native_lineage_batch_digests().count() != usize::from(data_page_count)
        || completion.sealed_pages().len() != usize::from(response_count)
        || completion.terminal_source_object().is_none()
        || completion.completion_digest().bytes() == [0; 32]
    {
        return Err(TreasuryApplicationError::InvalidAcquisition);
    }
    Ok(())
}

fn all_history_terminal_received_at(
    completion: &TreasuryAllHistoryAcquisitionCompletion,
) -> Result<Timestamp, TreasuryApplicationError> {
    completion
        .receive_window()
        .map(|(_first, terminal)| terminal)
        .ok_or(TreasuryApplicationError::InvalidAcquisition)
}

fn treasury_all_history_plan_identity(
    analytical_dataset: &DatasetId,
    provider_dataset: &SourceIdentifier,
    source_generation_digest: EvidenceDigest,
    query_digest: EvidenceDigest,
    activation_intent_digest: EvidenceDigest,
) -> Result<EvidenceDigest, TreasuryApplicationError> {
    let mut digest = Sha256::new();
    digest.update(TREASURY_ALL_HISTORY_PLAN_SCHEMA);
    hash_treasury_component(&mut digest, analytical_dataset.as_str().as_bytes())?;
    hash_treasury_component(&mut digest, provider_dataset.as_str().as_bytes())?;
    digest.update(source_generation_digest.bytes());
    digest.update(query_digest.bytes());
    digest.update(activation_intent_digest.bytes());
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn treasury_all_history_page_identity(
    provider_dataset: &SourceIdentifier,
    page_ordinal: u16,
    content_identity: EvidenceDigest,
    binding_digest: EvidenceDigest,
    semantic_digest: EvidenceDigest,
) -> Result<EvidenceDigest, TreasuryApplicationError> {
    let mut digest = Sha256::new();
    digest.update(TREASURY_ALL_HISTORY_PAGE_SCHEMA);
    hash_treasury_component(&mut digest, provider_dataset.as_str().as_bytes())?;
    digest.update(page_ordinal.to_be_bytes());
    digest.update(content_identity.bytes());
    digest.update(binding_digest.bytes());
    digest.update(semantic_digest.bytes());
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn treasury_all_history_ingest_identity(
    analytical_dataset: &DatasetId,
    provider_dataset: &SourceIdentifier,
    source_generation_digest: EvidenceDigest,
    publication_digest: EvidenceDigest,
) -> Result<String, TreasuryApplicationError> {
    let mut digest = Sha256::new();
    digest.update(TREASURY_ALL_HISTORY_INGEST_SCHEMA);
    hash_treasury_component(&mut digest, analytical_dataset.as_str().as_bytes())?;
    hash_treasury_component(&mut digest, provider_dataset.as_str().as_bytes())?;
    digest.update(source_generation_digest.bytes());
    digest.update(publication_digest.bytes());
    Ok(format!("treasury-all-history-v1-{:x}", digest.finalize()))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the complete Treasury plan identity keeps every independent publication coordinate explicit"
)]
fn treasury_macro_completion_digest(
    surface: TreasurySurface,
    analytical_dataset: &DatasetId,
    provider_dataset: &SourceIdentifier,
    source_generation_digest: EvidenceDigest,
    candidate_digest: EvidenceDigest,
    binding_digest: EvidenceDigest,
    record_count: usize,
) -> Result<EvidenceDigest, TreasuryApplicationError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/treasury-macro-completion/v1\0");
    hash_treasury_component(&mut digest, surface.profile_id().as_bytes())?;
    hash_treasury_component(&mut digest, analytical_dataset.as_str().as_bytes())?;
    hash_treasury_component(&mut digest, provider_dataset.as_str().as_bytes())?;
    digest.update(source_generation_digest.bytes());
    digest.update(candidate_digest.bytes());
    digest.update(binding_digest.bytes());
    digest.update(
        u64::try_from(record_count)
            .map_err(|_error| TreasuryApplicationError::InvalidAcquisition)?
            .to_be_bytes(),
    );
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn treasury_macro_ingest_identity(
    surface: TreasurySurface,
    analytical_dataset: &DatasetId,
    provider_dataset: &SourceIdentifier,
    source_generation_digest: EvidenceDigest,
    publication_digest: EvidenceDigest,
) -> Result<String, TreasuryApplicationError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/treasury-macro-ingest/v1\0");
    hash_treasury_component(&mut digest, surface.profile_id().as_bytes())?;
    hash_treasury_component(&mut digest, analytical_dataset.as_str().as_bytes())?;
    hash_treasury_component(&mut digest, provider_dataset.as_str().as_bytes())?;
    digest.update(source_generation_digest.bytes());
    digest.update(publication_digest.bytes());
    Ok(format!("treasury-macro-v1-{:x}", digest.finalize()))
}

fn surface_accepts_series(
    surface: TreasurySurface,
    provider_dataset: &SourceIdentifier,
    series: &SourceIdentifier,
) -> bool {
    match surface {
        TreasurySurface::FiscalData => {
            provider_dataset
                .as_str()
                .starts_with(FISCAL_PROVIDER_DATASET_PREFIX)
                && series.as_str().starts_with(FISCAL_SERIES_PREFIX)
                && series.as_str().len() > FISCAL_SERIES_PREFIX.len()
        }
        TreasurySurface::DailyRatesXml => DAILY_PROVIDER_DATASET_PREFIXES
            .iter()
            .find(|prefix| provider_dataset.as_str().starts_with(**prefix))
            .is_some_and(|prefix| {
                series.as_str().starts_with(*prefix) && series.as_str().len() > prefix.len()
            }),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "series authority is bound to independent manifest, canonical, raw, and native coordinates"
)]
fn published_series_binding_digest(
    surface: TreasurySurface,
    manifest: &DatasetManifestRef,
    source_id: &SourceId,
    provider_dataset: &SourceIdentifier,
    expected_input_records: usize,
    extraction_content_identity: EvidenceDigest,
    native_batch_digest: EvidenceDigest,
    published_series: &AnalyticalMacroSeriesAllowlist,
    canonical_row_digests: impl ExactSizeIterator<Item = EvidenceDigest>,
) -> Result<EvidenceDigest, TreasuryApplicationError> {
    if canonical_row_digests.len() != expected_input_records
        || published_series.series().is_empty()
        || published_series.series().len() > MAX_TREASURY_LATEST_KNOWN_SERIES
        || extraction_content_identity.algorithm() != DigestAlgorithm::Sha256
        || extraction_content_identity.bytes() == [0; 32]
        || native_batch_digest.algorithm() != DigestAlgorithm::Sha256
        || native_batch_digest.bytes() == [0; 32]
    {
        return Err(TreasuryApplicationError::RestartInvalid);
    }
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/treasury-published-series-binding/v1\0");
    hash_treasury_component(&mut digest, surface.profile_id().as_bytes())?;
    hash_treasury_component(&mut digest, manifest.dataset_id().as_str().as_bytes())?;
    digest.update(manifest.manifest_version().to_be_bytes());
    hash_treasury_component(&mut digest, manifest.schema().name().as_bytes())?;
    digest.update(manifest.schema().version().get().to_be_bytes());
    digest.update(manifest.schema().fingerprint());
    digest.update(manifest.content_hash().bytes());
    hash_treasury_component(&mut digest, source_id.as_str().as_bytes())?;
    hash_treasury_component(&mut digest, provider_dataset.as_str().as_bytes())?;
    digest.update(
        u64::try_from(expected_input_records)
            .map_err(|_error| TreasuryApplicationError::RestartInvalid)?
            .to_be_bytes(),
    );
    digest.update(extraction_content_identity.bytes());
    digest.update(native_batch_digest.bytes());
    digest.update(
        u64::try_from(published_series.series().len())
            .map_err(|_error| TreasuryApplicationError::RestartInvalid)?
            .to_be_bytes(),
    );
    for series in published_series.series() {
        if !surface_accepts_series(surface, provider_dataset, series) {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        hash_treasury_component(&mut digest, series.as_str().as_bytes())?;
    }
    for canonical_row_digest in canonical_row_digests {
        if canonical_row_digest.algorithm() != DigestAlgorithm::Sha256
            || canonical_row_digest.bytes() == [0; 32]
        {
            return Err(TreasuryApplicationError::RestartInvalid);
        }
        digest.update(canonical_row_digest.bytes());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_treasury_component(
    digest: &mut Sha256,
    value: &[u8],
) -> Result<(), TreasuryApplicationError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_error| TreasuryApplicationError::RestartInvalid)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}

/// Failure before raw sealing or during exact-manifest Treasury PIT replay.
#[derive(Debug, Error)]
pub(crate) enum TreasuryApplicationError {
    /// Surface, dataset, object, or discovery-receipt coordinates are inconsistent.
    #[error("Treasury selected object is invalid")]
    InvalidSelection,
    /// The coordinator and research store do not belong to one application composition.
    #[error("Treasury application authority is invalid")]
    AuthorityInvalid,
    /// Registered extraction returned mismatched canonical, rights, clock, or raw evidence.
    #[error("Treasury registered acquisition evidence is invalid")]
    InvalidAcquisition,
    /// A Fiscal Data request was crossed with daily rates, or vice versa.
    #[error("Treasury product surface does not match the requested operation")]
    SurfaceMismatch,
    /// Exact manifest, source, raw binding, native schema, or read evidence changed after restart.
    #[error("Treasury exact restart evidence is invalid")]
    RestartInvalid,
    /// Registered acquisition, selection, cancellation, or deadline authority rejected the call.
    #[error("Treasury registered acquisition failed")]
    Service(#[from] ServiceError),
    /// Runtime replacement or publication authority became unavailable.
    #[error("Treasury runtime publication authority failed")]
    Composition(#[from] ResearchIngestCompositionError),
    /// The application-owned raw store or research composition rejected the operation.
    #[error("Treasury application research operation failed")]
    Research(#[from] ResearchServiceError),
    /// Exact raw sealing and its one-use expectation did not rejoin.
    #[error("Treasury raw-capture sealing evidence failed")]
    Capture(#[from] ProviderCaptureError),
    /// The typed Treasury all-history state machine rejected a page or checkpoint.
    #[error("Treasury all-history acquisition state is invalid")]
    AllHistory(#[from] TreasurySourceError),
    /// Durable manifest or provider-binding verification failed.
    #[error("Treasury immutable generation verification failed")]
    Ingest(#[from] IngestError),
    /// The fixed latest-known Macro selector rejected the generation or its bounds.
    #[error("Treasury latest-known Macro read failed")]
    AnalyticalRead(#[from] AnalyticalReadError),
}
