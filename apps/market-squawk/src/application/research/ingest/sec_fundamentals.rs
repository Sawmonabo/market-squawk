//! Seal-first SEC submissions, Company Facts, and filing-XBRL publication with exact restart reads.
//!
//! The SEC adapter authors the canonical-row-to-captured-page map while it still owns the parsed
//! request graph. This application leaf seals the exact graph in the sole raw store, consumes the
//! one-use whole-capture token into common publication authority, and retains the immutable
//! manifest plus source/native/raw/company evidence required for restart. Filing XBRL enters the
//! same path only through the adapter's opaque accession/document/taxonomy capture graph.

use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Instant;

use market_squawk_adapter_sec::{
    SecClientError, SecEdgarSource, SecExtractionResult, SecFilingXbrlCaptureHandoff,
    SecResearchDataset, SecResearchDatasetKind,
};
use market_squawk_data::{
    AnalyticalObservationOutput, AnalyticalObservationReadRequest, AnalyticalObservationTemplate,
    AnalyticalReadError, CommittedDataset, CompanySecurityIdentityReadCapability, DatasetId,
    DatasetManifestRef, IngestError, IngestPrecommitAuthority, ObservationKnowledgeRange,
    PersistedProviderCaptureBindingEvidence, QueryLimits, SecFundamentalIdentityAvailability,
    SecFundamentalIdentityQuery, SecFundamentalIdentitySelection,
    extraction_provider_payload_digest,
};
use market_squawk_domain::{
    CompanyIdentityObservation, CompanyIdentitySurface, DigestAlgorithm, EvidenceDigest,
    SourceIdentifier, Timestamp,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    ExtractionAuthority, ExtractionBatch, ExtractionError, ExtractionRequest,
    ExtractionRevisionPlan, ExtractionSourceError, ProviderCaptureError, ProviderCaptureMaterial,
    ProviderNativeLineageBatch, ProviderNativeLineageImplementation, ProviderWholeCaptureToken,
    SealedProviderCaptureBinding, SealedProviderCaptureSetReceipt, SourceMetadata,
    SourceMetadataProvider, SourceObjectCaptureIdentity,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::ResearchRightsAuthority;
use super::provider_runtime::SecLiveFundCoordinatorSeal;
use crate::{ResearchIngestRequest, ResearchService, ResearchServiceError};

/// Fixed typed operation for exact-manifest SEC filing candidate reads.
pub(crate) const SEC_SUBMISSIONS_EXACT_GENERATION_OPERATION: &str =
    "Research.GetSecFilingsExactGeneration";

/// Fixed typed operation for exact-manifest SEC Company Facts candidate reads.
pub(crate) const SEC_COMPANY_FACTS_EXACT_GENERATION_OPERATION: &str =
    "Research.GetSecCompanyFactsExactGeneration";

/// Fixed typed operation for exact-manifest filing-XBRL candidate reads.
pub(crate) const SEC_FILING_XBRL_EXACT_GENERATION_OPERATION: &str =
    "Research.GetSecFilingXbrlExactGeneration";

/// The SEC research family carried by one exact sealed publication handoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SecFundamentalsFamily {
    /// Complete submissions, including every provider-declared historical companion.
    Submissions,
    /// One complete Company Facts representation.
    CompanyFacts,
    /// One accession/document/taxonomy-bound filing XBRL extraction.
    FilingXbrl,
}

impl From<SecResearchDatasetKind> for SecFundamentalsFamily {
    fn from(value: SecResearchDatasetKind) -> Self {
        match value {
            SecResearchDatasetKind::Submissions => Self::Submissions,
            SecResearchDatasetKind::CompanyFacts => Self::CompanyFacts,
            SecResearchDatasetKind::FilingXbrl => Self::FilingXbrl,
        }
    }
}

/// Application-owned bridge into the sole physical provider-response store.
#[derive(Debug)]
struct SecFundamentalsApplicationBridge {
    research: Arc<ResearchService>,
}

/// Smallest SEC fundamentals closure that can be composed only from the coordinator-owned source
/// registration. It owns no registry and accepts no caller-substitutable source or extraction
/// authority after construction.
#[derive(Debug)]
pub(crate) struct SecFundamentalsCoordinatorClosure {
    source: Arc<SecEdgarSource>,
    extraction: ExtractionAuthority,
    rights: ResearchRightsAuthority,
    bridge: SecFundamentalsApplicationBridge,
}

impl SecFundamentalsCoordinatorClosure {
    /// The coordinator runtime is the only module able to mint `seal`; its factory must obtain the
    /// concrete source, extraction authority, and rights from one locked registered generation.
    pub(super) fn from_coordinator(
        _seal: SecLiveFundCoordinatorSeal,
        source: Arc<SecEdgarSource>,
        extraction: ExtractionAuthority,
        rights: ResearchRightsAuthority,
        research: Arc<ResearchService>,
    ) -> Result<Self, SecFundamentalsApplicationError> {
        if extraction.metadata() != source.metadata()
            || rights.source_id() != source.metadata().source_id()
        {
            return Err(SecFundamentalsApplicationError::InvalidAuthority);
        }
        extraction.validate_current()?;
        Ok(Self {
            source,
            extraction,
            rights,
            bridge: SecFundamentalsApplicationBridge::new(research),
        })
    }

    pub(crate) async fn extract_and_seal_selected(
        &self,
        request: ExtractionRequest,
        capture_material: ProviderCaptureMaterial,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SecFundamentalsSealedHandoff, SecFundamentalsApplicationError> {
        self.extraction.validate_current()?;
        self.bridge
            .extract_and_seal_selected(
                self.source.as_ref(),
                self.extraction.clone(),
                request,
                capture_material,
                cancellation,
                deadline,
            )
            .await
    }

    /// Consumes the adapter's opaque accession/document/taxonomy graph on the bounded blocking
    /// executor and seals the exact same raw graph before publication can proceed.
    pub(crate) async fn extract_and_seal_filing_xbrl(
        &self,
        handoff: SecFilingXbrlCaptureHandoff,
        max_records: NonZeroU32,
        max_bytes: NonZeroU64,
        wall_deadline: Timestamp,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SecFundamentalsSealedHandoff, SecFundamentalsApplicationError> {
        self.extraction.validate_current()?;
        if cancellation.is_cancelled() {
            return Err(SecFundamentalsApplicationError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(SecFundamentalsApplicationError::DeadlineExceeded);
        }
        let source = Arc::clone(&self.source);
        let authority = self.extraction.clone();
        let worker_cancellation = cancellation.child_token();
        let worker_token = worker_cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            handoff.extract(
                authority,
                max_records,
                max_bytes,
                wall_deadline,
                worker_token,
            )
        });
        tokio::pin!(worker);
        let deadline_wait = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
        tokio::pin!(deadline_wait);
        let extracted = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                worker_cancellation.cancel();
                let _ = worker.as_mut().await;
                return Err(SecFundamentalsApplicationError::Cancelled);
            }
            () = deadline_wait.as_mut() => {
                worker_cancellation.cancel();
                let _ = worker.as_mut().await;
                return Err(SecFundamentalsApplicationError::DeadlineExceeded);
            }
            result = worker.as_mut() => {
                result.map_err(|_| SecFundamentalsApplicationError::BlockingWorkerFailed)??
            }
        };
        let (extracted, capture_material) = extracted;
        self.bridge
            .seal_extracted(
                source.as_ref(),
                extracted,
                capture_material,
                cancellation,
                deadline,
            )
            .await
    }

    pub(crate) async fn publish(
        &self,
        handoff: SecFundamentalsSealedHandoff,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<SecFundamentalsPublicationReceipt, SecFundamentalsApplicationError> {
        self.extraction.validate_current()?;
        self.bridge
            .publish(handoff, &self.rights, precommit_authority, cancellation)
            .await
    }

    /// Reports the common exact-manifest SEC reader available after immutable publication.
    ///
    /// The caller must construct its read from the family-specific publication receipt; this
    /// state does not grant a latest-generation or identity-inferred read.
    pub(crate) const fn point_in_time_state(&self) -> SecFundamentalsPointInTimeState {
        SecFundamentalsPointInTimeState::AvailableAfterPublication
    }

    pub(crate) const fn filing_xbrl_state(&self) -> SecFilingXbrlApplicationState {
        SecFilingXbrlApplicationState::AvailableAfterCaptureAdmission
    }
}

impl SecFundamentalsApplicationBridge {
    const fn new(research: Arc<ResearchService>) -> Self {
        Self { research }
    }

    /// Extracts one already selected SEC object, seals its complete exact response graph, and
    /// returns the exclusive fail-closed canonical publication handoff.
    ///
    /// The caller remains responsible for registry selection, rights admission, and retaining its
    /// provider-generation precommit lease. This method does not refetch, reconstruct, or weaken
    /// those authorities.
    async fn extract_and_seal_selected(
        &self,
        source: &SecEdgarSource,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        capture_material: ProviderCaptureMaterial,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SecFundamentalsSealedHandoff, SecFundamentalsApplicationError> {
        if cancellation.is_cancelled() {
            return Err(SecFundamentalsApplicationError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(SecFundamentalsApplicationError::DeadlineExceeded);
        }
        let object = request.object();
        let capture = capture_material.receipt();
        let capture_identity = SourceObjectCaptureIdentity::try_from_capture(capture)?;
        if object.source_id() != source.metadata().source_id()
            || object.metadata_revision() != source.metadata().revision()
            || capture.source_id() != object.source_id()
            || capture.metadata_revision() != object.metadata_revision()
            || capture.dataset() != object.dataset()
            || capture_identity != object.capture_identity()
        {
            return Err(SecFundamentalsApplicationError::InvalidSelection);
        }

        let extracted = source
            .extract_with_company_identity(authority, request, cancellation.clone())
            .await?;
        self.seal_extracted(source, extracted, capture_material, cancellation, deadline)
            .await
    }

    /// Seals an already extracted SEC result without exposing the physical store to the adapter.
    ///
    /// This split also accepts the filing-XBRL result emitted by the adapter's opaque capture graph.
    async fn seal_extracted(
        &self,
        source: &SecEdgarSource,
        extracted: SecExtractionResult,
        capture_material: ProviderCaptureMaterial,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SecFundamentalsSealedHandoff, SecFundamentalsApplicationError> {
        if cancellation.is_cancelled() {
            return Err(SecFundamentalsApplicationError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(SecFundamentalsApplicationError::DeadlineExceeded);
        }

        let (batch, company_identity, native_lineage, row_capture_page_ordinals) =
            extracted.into_parts();
        let capture = capture_material.receipt();
        let capture_identity = SourceObjectCaptureIdentity::try_from_capture(capture)?;
        if batch.request().object().source_id() != source.metadata().source_id()
            || batch.request().object().metadata_revision() != source.metadata().revision()
            || capture.source_id() != batch.request().object().source_id()
            || capture.metadata_revision() != batch.request().object().metadata_revision()
            || capture.dataset() != batch.request().object().dataset()
            || capture_identity != batch.request().object().capture_identity()
        {
            return Err(SecFundamentalsApplicationError::InvalidSelection);
        }
        let batch = batch.try_bind_provider_capture(capture)?;
        let revisions = source.revision_plan(&batch)?;
        let coordinates = SecFundamentalsCoordinates::try_from_extraction(
            &batch,
            company_identity.as_ref(),
            &native_lineage,
        )?;

        let (expectation, seal_request) = capture_material.into_whole_seal_parts();
        let sealed = self
            .research
            .seal_provider_capture(seal_request, &cancellation, deadline)
            .await?;
        let token = expectation.try_rejoin(sealed)?.try_into_whole()?;
        if cancellation.is_cancelled() {
            return Err(SecFundamentalsApplicationError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(SecFundamentalsApplicationError::DeadlineExceeded);
        }

        Ok(SecFundamentalsSealedHandoff {
            source: source.metadata().clone(),
            coordinates,
            batch,
            company_identity,
            native_lineage,
            revisions,
            row_capture_page_ordinals,
            token,
        })
    }

    /// Consumes one sealed SEC handoff into immutable canonical publication authority.
    ///
    /// The provider retrieval time is derived from the complete sealed capture rather than a
    /// caller clock. The caller supplies only its previously admitted persistence-rights and
    /// provider-generation precommit authorities.
    async fn publish(
        &self,
        handoff: SecFundamentalsSealedHandoff,
        rights: &ResearchRightsAuthority,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<SecFundamentalsPublicationReceipt, SecFundamentalsApplicationError> {
        if cancellation.is_cancelled() {
            return Err(SecFundamentalsApplicationError::Cancelled);
        }
        precommit_authority.validate_precommit()?;
        let SecFundamentalsSealedHandoff {
            source,
            coordinates,
            batch,
            company_identity,
            native_lineage,
            revisions,
            row_capture_page_ordinals,
            token,
        } = handoff;
        if rights.source_id() != source.source_id()
            || batch.records().is_empty()
            || revisions.len() != batch.records().len()
            || !revisions.native_lineage_required()
            || row_capture_page_ordinals.len() != batch.records().len()
            || revisions.is_locally_observed()
                != (coordinates.family == SecFundamentalsFamily::FilingXbrl)
        {
            return Err(SecFundamentalsApplicationError::InvalidAuthority);
        }
        let company_identity =
            company_identity.ok_or(SecFundamentalsApplicationError::InvalidCompanyIdentity)?;
        validate_company_identity(
            coordinates.family,
            coordinates.cik.as_str(),
            &batch,
            Some(&company_identity),
        )?;

        let binding = SealedProviderCaptureBinding::try_whole(
            token,
            batch,
            native_lineage,
            row_capture_page_ordinals,
        )?;
        binding.validate()?;
        if binding.native_lineage().schema().implementation()
            != ProviderNativeLineageImplementation::SecEdgarV1
        {
            return Err(SecFundamentalsApplicationError::InvalidNativeLineage);
        }
        let retrieved_at = binding
            .capture_evidence()
            .pages()
            .iter()
            .map(|page| page.received_at())
            .max()
            .ok_or(SecFundamentalsApplicationError::InvalidSelection)?;
        if !source.is_effective_at(retrieved_at) {
            return Err(SecFundamentalsApplicationError::InvalidAuthority);
        }
        let payload_digest = extraction_provider_payload_digest(binding.batch());
        let rights = rights.decision(payload_digest, retrieved_at)?;
        let company_observation_digest = company_identity_digest(&company_identity)?;
        let retained_identity = Arc::new(company_identity.clone());
        let restart = SecFundamentalsRestartBinding::from_live_binding(
            &source,
            &coordinates,
            retained_identity,
            company_observation_digest,
            &binding,
        )?;
        let ingest = ResearchIngestRequest::with_provider_publication(
            source,
            rights,
            coordinates.analytical_dataset,
            binding,
            revisions,
        )?
        .with_company_identity(company_identity)?
        .with_precommit_authority(precommit_authority);
        let committed = self.research.ingest(ingest, cancellation).await?;
        let restart = restart.with_manifest(committed.manifest().clone())?;
        Ok(match restart.family {
            SecFundamentalsFamily::Submissions => {
                SecFundamentalsPublicationReceipt::Submissions(SecSubmissionsPublicationReceipt {
                    committed,
                    restart: SecSubmissionsRestartSelector { binding: restart },
                })
            }
            SecFundamentalsFamily::CompanyFacts => {
                SecFundamentalsPublicationReceipt::CompanyFacts(SecCompanyFactsPublicationReceipt {
                    committed,
                    restart: SecCompanyFactsRestartSelector { binding: restart },
                })
            }
            SecFundamentalsFamily::FilingXbrl => {
                SecFundamentalsPublicationReceipt::FilingXbrl(SecFilingXbrlPublicationReceipt {
                    committed,
                    restart: SecFilingXbrlRestartSelector { binding: restart },
                })
            }
        })
    }
}

#[derive(Debug)]
struct SecFundamentalsCoordinates {
    family: SecFundamentalsFamily,
    cik: SourceIdentifier,
    provider_dataset: SourceIdentifier,
    analytical_dataset: DatasetId,
}

impl SecFundamentalsCoordinates {
    fn try_from_extraction(
        batch: &ExtractionBatch,
        company_identity: Option<&CompanyIdentityObservation>,
        native_lineage: &ProviderNativeLineageBatch,
    ) -> Result<Self, SecFundamentalsApplicationError> {
        let provider_dataset = batch.request().object().dataset().clone();
        native_lineage
            .validate(batch)
            .map_err(|_| SecFundamentalsApplicationError::InvalidNativeLineage)?;
        if native_lineage.schema().implementation()
            != ProviderNativeLineageImplementation::SecEdgarV1
        {
            return Err(SecFundamentalsApplicationError::InvalidNativeLineage);
        }
        let selection = SecResearchDataset::try_from_identifier(&provider_dataset)?;
        let family = selection.kind().into();
        if selection.dataset() != &provider_dataset {
            return Err(SecFundamentalsApplicationError::InvalidSelection);
        }
        validate_company_identity(family, selection.cik(), batch, company_identity)?;
        let analytical_dataset = selection.analytical_dataset_identifier()?;
        Ok(Self {
            family,
            cik: SourceIdentifier::try_from(selection.cik())
                .map_err(|_| SecFundamentalsApplicationError::InvalidSelection)?,
            provider_dataset,
            analytical_dataset: DatasetId::try_from(analytical_dataset.as_str())
                .map_err(|_| SecFundamentalsApplicationError::InvalidSelection)?,
        })
    }
}

fn validate_company_identity(
    family: SecFundamentalsFamily,
    cik: &str,
    batch: &ExtractionBatch,
    company_identity: Option<&CompanyIdentityObservation>,
) -> Result<(), SecFundamentalsApplicationError> {
    let expected_surface = match family {
        SecFundamentalsFamily::Submissions => Some(CompanyIdentitySurface::SecSubmissions),
        SecFundamentalsFamily::CompanyFacts => Some(CompanyIdentitySurface::SecCompanyFacts),
        SecFundamentalsFamily::FilingXbrl => Some(CompanyIdentitySurface::SecSubmissions),
    };
    match (expected_surface, company_identity) {
        (Some(expected_surface), Some(identity))
            if identity.source_id() == batch.request().object().source_id()
                && identity.provider_company_id().as_str() == cik
                && identity.surface() == expected_surface
                && identity.parent_ingest_payload_evidence().content_digest()
                    == extraction_provider_payload_digest(batch) =>
        {
            Ok(())
        }
        (None, None) => Ok(()),
        _ => Err(SecFundamentalsApplicationError::InvalidCompanyIdentity),
    }
}

fn company_identity_digest(
    observation: &CompanyIdentityObservation,
) -> Result<EvidenceDigest, SecFundamentalsApplicationError> {
    let canonical = serde_json::to_vec(observation)?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(canonical).into(),
    ))
}

/// Exclusive exact-response/canonical handoff awaiting generic immutable publication.
///
/// This value is intentionally non-cloneable and non-serializable. Its token can authorize at
/// most one `SealedProviderCaptureBinding`; the adapter-authored row map cannot be replaced by an
/// application inference.
#[derive(Debug)]
pub(crate) struct SecFundamentalsSealedHandoff {
    source: SourceMetadata,
    coordinates: SecFundamentalsCoordinates,
    batch: ExtractionBatch,
    company_identity: Option<CompanyIdentityObservation>,
    native_lineage: ProviderNativeLineageBatch,
    revisions: ExtractionRevisionPlan,
    row_capture_page_ordinals: Vec<u16>,
    token: ProviderWholeCaptureToken,
}

impl SecFundamentalsSealedHandoff {
    pub(crate) const fn family(&self) -> SecFundamentalsFamily {
        self.coordinates.family
    }

    pub(crate) const fn cik(&self) -> &SourceIdentifier {
        &self.coordinates.cik
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.coordinates.provider_dataset
    }

    pub(crate) const fn analytical_dataset(&self) -> &DatasetId {
        &self.coordinates.analytical_dataset
    }

    pub(crate) const fn source(&self) -> &SourceMetadata {
        &self.source
    }

    pub(crate) const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }

    pub(crate) const fn company_identity(&self) -> Option<&CompanyIdentityObservation> {
        self.company_identity.as_ref()
    }

    pub(crate) const fn native_lineage(&self) -> &ProviderNativeLineageBatch {
        &self.native_lineage
    }

    pub(crate) const fn revisions(&self) -> &ExtractionRevisionPlan {
        &self.revisions
    }

    pub(crate) fn row_capture_page_ordinals(&self) -> &[u16] {
        &self.row_capture_page_ordinals
    }

    pub(crate) fn sealed_capture(&self) -> &SealedProviderCaptureSetReceipt {
        self.token.persisted_receipt()
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        SourceMetadata,
        DatasetId,
        ExtractionBatch,
        Option<CompanyIdentityObservation>,
        ProviderNativeLineageBatch,
        ExtractionRevisionPlan,
        Vec<u16>,
        ProviderWholeCaptureToken,
    ) {
        (
            self.source,
            self.coordinates.analytical_dataset,
            self.batch,
            self.company_identity,
            self.native_lineage,
            self.revisions,
            self.row_capture_page_ordinals,
            self.token,
        )
    }
}

/// Closed SEC publication result with family-specific read authority.
#[derive(Debug)]
pub(crate) enum SecFundamentalsPublicationReceipt {
    Submissions(SecSubmissionsPublicationReceipt),
    CompanyFacts(SecCompanyFactsPublicationReceipt),
    FilingXbrl(SecFilingXbrlPublicationReceipt),
}

/// Immutable complete-submissions generation and its exact restart selector.
#[derive(Debug)]
pub(crate) struct SecSubmissionsPublicationReceipt {
    committed: CommittedDataset,
    restart: SecSubmissionsRestartSelector,
}

impl SecSubmissionsPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &SecSubmissionsRestartSelector {
        &self.restart
    }

    pub(crate) const fn cik(&self) -> &SourceIdentifier {
        self.restart.binding.cik()
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        self.restart.binding.provider_dataset()
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        self.restart.manifest()
    }

    pub(crate) const fn provider_binding_digest(&self) -> EvidenceDigest {
        self.restart.binding_digest()
    }

    pub(crate) const fn company_observation_digest(&self) -> EvidenceDigest {
        self.restart.company_observation_digest()
    }

    pub(crate) fn company_identity(&self) -> &CompanyIdentityObservation {
        self.restart.binding.company_identity()
    }
}

/// Immutable Company Facts generation and its exact restart selector.
#[derive(Debug)]
pub(crate) struct SecCompanyFactsPublicationReceipt {
    committed: CommittedDataset,
    restart: SecCompanyFactsRestartSelector,
}

impl SecCompanyFactsPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &SecCompanyFactsRestartSelector {
        &self.restart
    }

    pub(crate) const fn cik(&self) -> &SourceIdentifier {
        self.restart.binding.cik()
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        self.restart.binding.provider_dataset()
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        self.restart.manifest()
    }

    pub(crate) const fn provider_binding_digest(&self) -> EvidenceDigest {
        self.restart.binding_digest()
    }

    pub(crate) const fn company_observation_digest(&self) -> EvidenceDigest {
        self.restart.company_observation_digest()
    }

    pub(crate) fn company_identity(&self) -> &CompanyIdentityObservation {
        self.restart.binding.company_identity()
    }
}

/// Immutable accession/document/taxonomy-bound XBRL generation and exact restart selector.
#[derive(Debug)]
pub(crate) struct SecFilingXbrlPublicationReceipt {
    committed: CommittedDataset,
    restart: SecFilingXbrlRestartSelector,
}

impl SecFilingXbrlPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &SecFilingXbrlRestartSelector {
        &self.restart
    }

    pub(crate) const fn cik(&self) -> &SourceIdentifier {
        self.restart.binding.cik()
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        self.restart.binding.provider_dataset()
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        self.restart.manifest()
    }

    pub(crate) const fn provider_binding_digest(&self) -> EvidenceDigest {
        self.restart.binding_digest()
    }

    pub(crate) const fn company_observation_digest(&self) -> EvidenceDigest {
        self.restart.company_observation_digest()
    }

    pub(crate) fn company_identity(&self) -> &CompanyIdentityObservation {
        self.restart.binding.company_identity()
    }
}

#[derive(Clone, Debug)]
struct SecPendingRestartBinding {
    source: SourceMetadata,
    family: SecFundamentalsFamily,
    cik: SourceIdentifier,
    provider_dataset: SourceIdentifier,
    analytical_dataset: DatasetId,
    company_identity: Arc<CompanyIdentityObservation>,
    company_observation_digest: EvidenceDigest,
    binding_digest: EvidenceDigest,
    extraction_content_identity: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    expected_record_count: usize,
    native_schema_version: u16,
    native_schema_fingerprint: EvidenceDigest,
    native_batch_digest: EvidenceDigest,
    row_capture_page_ordinals: Box<[u16]>,
}

impl SecPendingRestartBinding {
    fn with_manifest(
        self,
        manifest: DatasetManifestRef,
    ) -> Result<SecFundamentalsRestartBinding, SecFundamentalsApplicationError> {
        if manifest.dataset_id() != &self.analytical_dataset {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        Ok(SecFundamentalsRestartBinding {
            source: self.source,
            family: self.family,
            cik: self.cik,
            provider_dataset: self.provider_dataset,
            company_identity: self.company_identity,
            company_observation_digest: self.company_observation_digest,
            manifest,
            binding_digest: self.binding_digest,
            extraction_content_identity: self.extraction_content_identity,
            sealed_capture_receipt_digest: self.sealed_capture_receipt_digest,
            expected_record_count: self.expected_record_count,
            native_schema_version: self.native_schema_version,
            native_schema_fingerprint: self.native_schema_fingerprint,
            native_batch_digest: self.native_batch_digest,
            row_capture_page_ordinals: self.row_capture_page_ordinals,
        })
    }
}

/// Common exact immutable SEC generation and raw/native/company restart coordinates.
#[derive(Clone, Debug)]
pub(crate) struct SecFundamentalsRestartBinding {
    source: SourceMetadata,
    family: SecFundamentalsFamily,
    cik: SourceIdentifier,
    provider_dataset: SourceIdentifier,
    company_identity: Arc<CompanyIdentityObservation>,
    company_observation_digest: EvidenceDigest,
    manifest: DatasetManifestRef,
    binding_digest: EvidenceDigest,
    extraction_content_identity: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    expected_record_count: usize,
    native_schema_version: u16,
    native_schema_fingerprint: EvidenceDigest,
    native_batch_digest: EvidenceDigest,
    row_capture_page_ordinals: Box<[u16]>,
}

impl SecFundamentalsRestartBinding {
    /// Reconstructs one restart selector solely from values retained in durable publication
    /// coordinates. This grants read authority only after the exact catalog, raw-store, native,
    /// identity, and family validations in [`Self::reopen`] succeed.
    #[allow(
        clippy::too_many_arguments,
        reason = "every independently durable SEC publication coordinate is required"
    )]
    pub(crate) fn try_from_durable_coordinates(
        source: SourceMetadata,
        family: SecFundamentalsFamily,
        cik: SourceIdentifier,
        provider_dataset: SourceIdentifier,
        company_identity: CompanyIdentityObservation,
        company_observation_digest: EvidenceDigest,
        manifest: DatasetManifestRef,
        binding_digest: EvidenceDigest,
        extraction_content_identity: EvidenceDigest,
        sealed_capture_receipt_digest: EvidenceDigest,
        expected_record_count: usize,
        native_schema_version: u16,
        native_schema_fingerprint: EvidenceDigest,
        native_batch_digest: EvidenceDigest,
        row_capture_page_ordinals: Vec<u16>,
    ) -> Result<Self, SecFundamentalsApplicationError> {
        if expected_record_count == 0
            || row_capture_page_ordinals.len() != expected_record_count
            || native_schema_version == 0
            || [
                company_observation_digest,
                binding_digest,
                extraction_content_identity,
                sealed_capture_receipt_digest,
                native_schema_fingerprint,
                native_batch_digest,
            ]
            .into_iter()
            .any(|digest| {
                digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32]
            })
            || company_identity_digest(&company_identity)? != company_observation_digest
            || company_identity.source_id() != source.source_id()
            || company_identity.provider_company_id() != &cik
            || expected_company_surface(family) != Some(company_identity.surface())
        {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        let provider_selection = SecResearchDataset::try_from_identifier(&provider_dataset)?;
        let expected_analytical =
            DatasetId::try_from(provider_selection.analytical_dataset_identifier()?.as_str())
                .map_err(|_| SecFundamentalsApplicationError::RestartInvalid)?;
        if SecFundamentalsFamily::from(provider_selection.kind()) != family
            || provider_selection.cik() != cik.as_str()
            || manifest.dataset_id() != &expected_analytical
        {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        Ok(Self {
            source,
            family,
            cik,
            provider_dataset,
            company_identity: Arc::new(company_identity),
            company_observation_digest,
            manifest,
            binding_digest,
            extraction_content_identity,
            sealed_capture_receipt_digest,
            expected_record_count,
            native_schema_version,
            native_schema_fingerprint,
            native_batch_digest,
            row_capture_page_ordinals: row_capture_page_ordinals.into_boxed_slice(),
        })
    }

    fn from_live_binding(
        source: &SourceMetadata,
        coordinates: &SecFundamentalsCoordinates,
        company_identity: Arc<CompanyIdentityObservation>,
        company_observation_digest: EvidenceDigest,
        binding: &SealedProviderCaptureBinding,
    ) -> Result<SecPendingRestartBinding, SecFundamentalsApplicationError> {
        binding.validate()?;
        let capture = binding.capture_evidence();
        let native = binding.native_lineage();
        if binding.record_count() == 0
            || binding.record_count() != binding.row_frames().len()
            || capture.source_id() != source.source_id()
            || capture.metadata_revision() != source.revision()
            || capture.dataset() != &coordinates.provider_dataset
            || native.schema().implementation() != ProviderNativeLineageImplementation::SecEdgarV1
        {
            return Err(SecFundamentalsApplicationError::InvalidAuthority);
        }
        let mut row_capture_page_ordinals = Vec::new();
        row_capture_page_ordinals
            .try_reserve_exact(binding.record_count())
            .map_err(|_| SecFundamentalsApplicationError::AllocationFailed)?;
        for (ordinal, frame) in binding.row_frames().iter().enumerate() {
            if frame.canonical_row_ordinal() != u32::try_from(ordinal).unwrap_or(u32::MAX) {
                return Err(SecFundamentalsApplicationError::InvalidAuthority);
            }
            row_capture_page_ordinals.push(frame.capture_page_ordinal());
        }
        Ok(SecPendingRestartBinding {
            source: source.clone(),
            family: coordinates.family,
            cik: coordinates.cik.clone(),
            provider_dataset: coordinates.provider_dataset.clone(),
            analytical_dataset: coordinates.analytical_dataset.clone(),
            company_identity,
            company_observation_digest,
            binding_digest: binding.evidence_digest().evidence(),
            extraction_content_identity: binding.content_identity().digest(),
            sealed_capture_receipt_digest: binding.sealed_capture_receipt_digest(),
            expected_record_count: binding.record_count(),
            native_schema_version: native.schema().version(),
            native_schema_fingerprint: native.schema().fingerprint(),
            native_batch_digest: native.batch_digest(),
            row_capture_page_ordinals: row_capture_page_ordinals.into_boxed_slice(),
        })
    }

    const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    const fn company_observation_digest(&self) -> EvidenceDigest {
        self.company_observation_digest
    }

    const fn cik(&self) -> &SourceIdentifier {
        &self.cik
    }

    const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    fn company_identity(&self) -> &CompanyIdentityObservation {
        self.company_identity.as_ref()
    }

    fn verify_persisted_binding(
        &self,
        research: &ResearchService,
    ) -> Result<PersistedProviderCaptureBindingEvidence, SecFundamentalsApplicationError> {
        let store = research.provider_capture_store();
        let evidence = research.analytical().provider_capture_binding_evidence(
            &self.manifest,
            self.binding_digest,
            store.as_ref(),
        )?;
        if evidence.binding_digest() != self.binding_digest
            || evidence.capture().source_id() != self.source.source_id()
            || evidence.capture().metadata_revision() != self.source.revision()
            || evidence.capture().dataset() != &self.provider_dataset
            || evidence.sealed_capture_receipt_digest() != self.sealed_capture_receipt_digest
            || evidence.extraction_content_identity() != self.extraction_content_identity
            || evidence.record_count() != self.expected_record_count
            || evidence.native_lineage().implementation() != "sec_edgar_v1"
            || evidence.native_lineage().version() != self.native_schema_version
            || evidence.native_lineage().fingerprint() != self.native_schema_fingerprint
            || evidence.native_lineage().batch_digest() != self.native_batch_digest
            || evidence.scope() != "whole"
            || evidence.layout() != "whole_single_segment"
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
        {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        Ok(evidence)
    }

    fn verify_company_identity(
        &self,
        identity_reader: &CompanySecurityIdentityReadCapability,
        effective_at: Timestamp,
        knowledge_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SecCompanyIdentityRestartEvidence, SecFundamentalsApplicationError> {
        let surface = expected_company_surface(self.family)
            .ok_or(SecFundamentalsApplicationError::RestartInvalid)?;
        if company_identity_digest(self.company_identity.as_ref())?
            != self.company_observation_digest
        {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        let query = SecFundamentalIdentityQuery::try_new(
            self.source.source_id().clone(),
            self.cik.clone(),
            surface,
            self.company_observation_digest,
            effective_at,
            knowledge_at,
        )?;
        let selection =
            identity_reader.sec_fundamental_identity_as_of(&query, deadline, cancellation)?;
        if selection.company_observation_digest() != self.company_observation_digest
            || selection.query_digest().bytes() == [0; 32]
            || selection.receipt_digest().bytes() == [0; 32]
            || (selection.availability() == SecFundamentalIdentityAvailability::Available)
                != (selection.instrument_id().is_some()
                    && selection.market_instrument_revision_digest().is_some()
                    && selection.relationship().is_some())
        {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        Ok(SecCompanyIdentityRestartEvidence {
            observation: Arc::clone(&self.company_identity),
            selection,
        })
    }

    async fn reopen(
        &self,
        template: AnalyticalObservationTemplate,
        knowledge_range: Option<ObservationKnowledgeRange>,
        research: &ResearchService,
        identity_reader: &CompanySecurityIdentityReadCapability,
        identity_effective_at: Timestamp,
        knowledge_at: Timestamp,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecFundamentalsRestartReceipt, SecFundamentalsApplicationError> {
        if cancellation.is_cancelled() {
            return Err(SecFundamentalsApplicationError::Cancelled);
        }
        if Instant::now() >= deadline || template != self.family.observation_template() {
            return Err(if Instant::now() >= deadline {
                SecFundamentalsApplicationError::DeadlineExceeded
            } else {
                SecFundamentalsApplicationError::RestartInvalid
            });
        }
        let evidence = self.verify_persisted_binding(research)?;
        let generation =
            research
                .analytical_reader()
                .exact(&self.manifest, deadline, &cancellation)?;
        if generation.source_id() != self.source.source_id()
            || generation.manifest() != &self.manifest
        {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        let company_identity = self.verify_company_identity(
            identity_reader,
            identity_effective_at,
            knowledge_at,
            deadline,
            &cancellation,
        )?;
        let request = AnalyticalObservationReadRequest::try_new(
            self.manifest.clone(),
            template,
            Vec::new(),
            knowledge_range,
        )?;
        let observations = research
            .analytical_reader()
            .read_observations(request, limits, deadline, cancellation)
            .await?;
        if observations.source_id() != self.source.source_id()
            || observations.request().manifest() != &self.manifest
            || observations.request().template() != template
            || !observations.request().instrument_ids().is_empty()
            || observations.request().knowledge_range() != knowledge_range
            || observations.output().manifest() != &self.manifest
        {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        Ok(SecFundamentalsRestartReceipt {
            evidence,
            company_identity,
            observations,
        })
    }
}

impl SecFundamentalsFamily {
    const fn observation_template(self) -> AnalyticalObservationTemplate {
        match self {
            Self::Submissions => AnalyticalObservationTemplate::Filing,
            Self::CompanyFacts | Self::FilingXbrl => AnalyticalObservationTemplate::Fundamental,
        }
    }
}

fn expected_company_surface(family: SecFundamentalsFamily) -> Option<CompanyIdentitySurface> {
    match family {
        SecFundamentalsFamily::Submissions => Some(CompanyIdentitySurface::SecSubmissions),
        SecFundamentalsFamily::CompanyFacts => Some(CompanyIdentitySurface::SecCompanyFacts),
        SecFundamentalsFamily::FilingXbrl => Some(CompanyIdentitySurface::SecSubmissions),
    }
}

/// Exact immutable complete-submissions selector; Company Facts cannot be substituted.
#[derive(Clone, Debug)]
pub(crate) struct SecSubmissionsRestartSelector {
    binding: SecFundamentalsRestartBinding,
}

impl SecSubmissionsRestartSelector {
    pub(crate) fn try_from_durable_coordinates(
        binding: SecFundamentalsRestartBinding,
    ) -> Result<Self, SecFundamentalsApplicationError> {
        if binding.family != SecFundamentalsFamily::Submissions {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        Ok(Self { binding })
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        self.binding.manifest()
    }

    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding.binding_digest()
    }

    pub(crate) const fn company_observation_digest(&self) -> EvidenceDigest {
        self.binding.company_observation_digest()
    }

    pub(crate) const fn cik(&self) -> &SourceIdentifier {
        self.binding.cik()
    }

    /// Reopens the exact filings generation, raw graph, identity selection, and bounded candidates.
    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        identity_reader: &CompanySecurityIdentityReadCapability,
        identity_effective_at: Timestamp,
        knowledge_at: Timestamp,
        knowledge_range: Option<ObservationKnowledgeRange>,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecSubmissionsRestartReceipt, SecFundamentalsApplicationError> {
        if self.binding.family != SecFundamentalsFamily::Submissions {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        self.binding
            .reopen(
                AnalyticalObservationTemplate::Filing,
                knowledge_range,
                research,
                identity_reader,
                identity_effective_at,
                knowledge_at,
                limits,
                deadline,
                cancellation,
            )
            .await
            .map(|receipt| SecSubmissionsRestartReceipt { receipt })
    }
}

/// Exact immutable Company Facts selector; submissions cannot be substituted.
#[derive(Clone, Debug)]
pub(crate) struct SecCompanyFactsRestartSelector {
    binding: SecFundamentalsRestartBinding,
}

impl SecCompanyFactsRestartSelector {
    pub(crate) fn try_from_durable_coordinates(
        binding: SecFundamentalsRestartBinding,
    ) -> Result<Self, SecFundamentalsApplicationError> {
        if binding.family != SecFundamentalsFamily::CompanyFacts {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        Ok(Self { binding })
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        self.binding.manifest()
    }

    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding.binding_digest()
    }

    pub(crate) const fn company_observation_digest(&self) -> EvidenceDigest {
        self.binding.company_observation_digest()
    }

    pub(crate) const fn cik(&self) -> &SourceIdentifier {
        self.binding.cik()
    }

    /// Reopens exact Company Facts, raw body, identity selection, and bounded candidates.
    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        identity_reader: &CompanySecurityIdentityReadCapability,
        identity_effective_at: Timestamp,
        knowledge_at: Timestamp,
        knowledge_range: Option<ObservationKnowledgeRange>,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecCompanyFactsRestartReceipt, SecFundamentalsApplicationError> {
        if self.binding.family != SecFundamentalsFamily::CompanyFacts {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        self.binding
            .reopen(
                AnalyticalObservationTemplate::Fundamental,
                knowledge_range,
                research,
                identity_reader,
                identity_effective_at,
                knowledge_at,
                limits,
                deadline,
                cancellation,
            )
            .await
            .map(|receipt| SecCompanyFactsRestartReceipt { receipt })
    }
}

/// Exact immutable filing-XBRL selector; Company Facts cannot be substituted.
#[derive(Clone, Debug)]
pub(crate) struct SecFilingXbrlRestartSelector {
    binding: SecFundamentalsRestartBinding,
}

impl SecFilingXbrlRestartSelector {
    pub(crate) fn try_from_durable_coordinates(
        binding: SecFundamentalsRestartBinding,
    ) -> Result<Self, SecFundamentalsApplicationError> {
        if binding.family != SecFundamentalsFamily::FilingXbrl {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        Ok(Self { binding })
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        self.binding.manifest()
    }

    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding.binding_digest()
    }

    pub(crate) const fn company_observation_digest(&self) -> EvidenceDigest {
        self.binding.company_observation_digest()
    }

    pub(crate) const fn cik(&self) -> &SourceIdentifier {
        self.binding.cik()
    }

    /// Reopens exact filing-XBRL numeric facts, raw graph, identity, and bounded candidates.
    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        identity_reader: &CompanySecurityIdentityReadCapability,
        identity_effective_at: Timestamp,
        knowledge_at: Timestamp,
        knowledge_range: Option<ObservationKnowledgeRange>,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecFilingXbrlRestartReceipt, SecFundamentalsApplicationError> {
        if self.binding.family != SecFundamentalsFamily::FilingXbrl {
            return Err(SecFundamentalsApplicationError::RestartInvalid);
        }
        self.binding
            .reopen(
                AnalyticalObservationTemplate::Fundamental,
                knowledge_range,
                research,
                identity_reader,
                identity_effective_at,
                knowledge_at,
                limits,
                deadline,
                cancellation,
            )
            .await
            .map(|receipt| SecFilingXbrlRestartReceipt { receipt })
    }
}

#[derive(Debug)]
struct SecFundamentalsRestartReceipt {
    evidence: PersistedProviderCaptureBindingEvidence,
    company_identity: SecCompanyIdentityRestartEvidence,
    observations: AnalyticalObservationOutput,
}

/// Exact persisted company observation recovered beside one SEC generation.
#[derive(Debug)]
pub(crate) struct SecCompanyIdentityRestartEvidence {
    observation: Arc<CompanyIdentityObservation>,
    selection: SecFundamentalIdentitySelection,
}

impl SecCompanyIdentityRestartEvidence {
    pub(crate) fn observation(&self) -> &CompanyIdentityObservation {
        self.observation.as_ref()
    }

    pub(crate) const fn observation_digest(&self) -> EvidenceDigest {
        self.selection.company_observation_digest()
    }

    pub(crate) const fn identity_selection(&self) -> &SecFundamentalIdentitySelection {
        &self.selection
    }

    pub(crate) const fn availability(&self) -> SecFundamentalIdentityAvailability {
        self.selection.availability()
    }
}

/// Exact raw/native/company and filing candidates reopened after restart.
#[derive(Debug)]
pub(crate) struct SecSubmissionsRestartReceipt {
    receipt: SecFundamentalsRestartReceipt,
}

impl SecSubmissionsRestartReceipt {
    pub(crate) const fn provider_evidence(&self) -> &PersistedProviderCaptureBindingEvidence {
        &self.receipt.evidence
    }

    pub(crate) const fn company_identity(&self) -> &SecCompanyIdentityRestartEvidence {
        &self.receipt.company_identity
    }

    pub(crate) const fn observations(&self) -> &AnalyticalObservationOutput {
        &self.receipt.observations
    }
}

/// Exact raw/native/company and fundamental candidates reopened after restart.
#[derive(Debug)]
pub(crate) struct SecCompanyFactsRestartReceipt {
    receipt: SecFundamentalsRestartReceipt,
}

impl SecCompanyFactsRestartReceipt {
    pub(crate) const fn provider_evidence(&self) -> &PersistedProviderCaptureBindingEvidence {
        &self.receipt.evidence
    }

    pub(crate) const fn company_identity(&self) -> &SecCompanyIdentityRestartEvidence {
        &self.receipt.company_identity
    }

    pub(crate) const fn observations(&self) -> &AnalyticalObservationOutput {
        &self.receipt.observations
    }
}

/// Exact raw/native/company and filing-XBRL candidates reopened after restart.
#[derive(Debug)]
pub(crate) struct SecFilingXbrlRestartReceipt {
    receipt: SecFundamentalsRestartReceipt,
}

impl SecFilingXbrlRestartReceipt {
    pub(crate) const fn provider_evidence(&self) -> &PersistedProviderCaptureBindingEvidence {
        &self.receipt.evidence
    }

    pub(crate) const fn company_identity(&self) -> &SecCompanyIdentityRestartEvidence {
        &self.receipt.company_identity
    }

    pub(crate) const fn observations(&self) -> &AnalyticalObservationOutput {
        &self.receipt.observations
    }
}

/// Filing-XBRL application capability at this adapter head.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SecFilingXbrlApplicationState {
    /// The adapter/application opaque capture admission and immutable publication path are wired.
    AvailableAfterCaptureAdmission,
}

/// Current SEC canonical point-in-time application capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SecFundamentalsPointInTimeState {
    /// The common reader requires an exact family, immutable manifest, provider binding, company
    /// observation, and four-clock cutoff supplied from the publication receipt.
    AvailableAfterPublication,
}

/// Fail-closed SEC fundamentals application-composition failure.
#[derive(Debug, Error)]
pub(crate) enum SecFundamentalsApplicationError {
    #[error("SEC fundamentals selection does not match the configured source and capture")]
    InvalidSelection,
    #[error("SEC fundamentals publication authority does not match the exact sealed handoff")]
    InvalidAuthority,
    #[error("SEC fundamentals company identity does not match the canonical extraction")]
    InvalidCompanyIdentity,
    #[error("SEC fundamentals native lineage does not match the selected family")]
    InvalidNativeLineage,
    #[error("SEC fundamentals exact restart evidence did not reproduce its immutable generation")]
    RestartInvalid,
    #[error("SEC fundamentals bounded allocation failed")]
    AllocationFailed,
    #[error("SEC fundamentals operation was cancelled")]
    Cancelled,
    #[error("SEC fundamentals operation deadline was exceeded")]
    DeadlineExceeded,
    #[error("SEC filing-XBRL blocking worker failed")]
    BlockingWorkerFailed,
    #[error(transparent)]
    Sec(#[from] SecClientError),
    #[error(transparent)]
    Extraction(#[from] ExtractionError),
    #[error(transparent)]
    ExtractionAuthority(#[from] market_squawk_sources::ExtractionAuthorityError),
    #[error(transparent)]
    ExtractionSource(#[from] ExtractionSourceError),
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Rights(#[from] ServiceError),
    #[error(transparent)]
    Research(#[from] ResearchServiceError),
    #[error(transparent)]
    AnalyticalRead(#[from] AnalyticalReadError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    CompanyIdentity(#[from] market_squawk_data::CompanySecurityIdentityCatalogError),
}
