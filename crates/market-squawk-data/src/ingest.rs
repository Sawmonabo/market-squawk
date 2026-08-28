//! Rights-bound analytical ingestion, immutable generation commit, and compaction.

use std::fmt;
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use market_squawk_domain::{
    CompanyIdentityObservation, DigestAlgorithm, EvidenceDigest, MetadataRevision,
    ResearchObservation, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{
    ResearchObjectControl, ResearchObjectControlError, ResearchObjectControlPoint,
    SealedResearchJournalStoreError, SealedResearchRawClaim, SealedResearchRecoveryAdmission,
};
use market_squawk_sources::{
    ExtractionBatch, ExtractionContentIdentity, ExtractionError, ExtractionRevisionPlan,
    MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, MAX_PROVIDER_CAPTURE_PAGES, ObservedRevisionAuthority,
    ObservedRevisionError, OptionMarketBatchKind, ProviderCaptureError,
    ProviderCaptureTerminalDisposition, ProviderNativeLineageImplementation,
    SealedProviderCaptureBinding, SealedProviderOptionMarketBinding,
    SealedProviderPublicationBinding, SourceClass, SourceMetadata, SourceObjectCaptureIdentity,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::analytical_backup::AnalyticalOperationGate;
use crate::authority_transition::{AuthorityTransitionError, AuthorityTransitionService};
use crate::blocking_supervisor::{BlockingIoAdmissionError, BlockingIoSupervisor};
use crate::catalog::CatalogObservedRevisionAuthority;
#[cfg(test)]
use crate::catalog::QueryArtifactBindCheckpoint;
use crate::catalog::QueryArtifactPublisher;
use crate::catalog::{
    MAX_PROVIDER_CAPTURE_PHYSICAL_BYTES, MAX_PROVIDER_CAPTURE_PHYSICAL_CLAIMS,
    PROVIDER_CAPTURE_RECOVERY_ENTRY_BUDGET,
};
use crate::catalog::{
    PreparedProviderCaptureBinding, PreparedProviderOptionMarketBinding,
    PreparedProviderPublicationBinding, PublicationSourceEvidence,
};
use crate::manifest::MarketBarHistoryPublicationCandidate;
use crate::parquet_store::MAX_SCAN_OBJECTS;
use crate::parquet_store::{ArtifactRootIdentity, QueryArtifactWriterAdmission};
use crate::query::QueryArtifactMemoryLease;
use crate::{
    AnalyticalManifestCatalog, ArrowConversionError, ArtifactRecord, CatalogAuthority,
    CatalogError, ContractCompletion, DatasetArrowBatch, DatasetId, DatasetManifestRecord,
    DatasetManifestRef, DatasetSchemaRef, GenerationKind, IngestIdentity, IngestReservation,
    IngestRunState, ListingReferenceError, ListingReferencePublicationCapability,
    ListingReferenceReadCapability, ManifestCatalogError, ManifestObject, ManifestPlan,
    ManifestPlanError, ObjectStoreConfig, OrphanRecoveryReport, ParquetObjectStore,
    ParquetStoreError, PinnedDataset, PinnedQueryOutput, ProviderMarketEventArrowBatch,
    ProviderOptionMarketArrowBatch, PublishedObject, QueryArtifactReservation,
    QueryArtifactReservationInput, QueryError, QueryLimits, QueryRequest, ResearchArrowBatch,
    ResearchQueryEngine, RightsDecisionInput, Sha256Digest, SourceOperation,
};

const ORPHAN_RECOVERY_DEADLINE: Duration = Duration::from_secs(30);
const REVISION_ASSIGNMENT_DEADLINE: Duration = Duration::from_secs(30);
const MAX_EVENT_PUBLICATION_READ_BYTES: usize = 128 * 1024 * 1024;
const MAX_OPTION_PUBLICATION_READ_BYTES: usize = 192 * 1024 * 1024;
const PROVIDER_MACRO_PLAN_PUBLICATION_DOMAIN: &[u8] =
    b"market-squawk/provider-macro-plan-publication/v1";

struct ProviderCaptureRecoveryControl<'a> {
    cancellation: &'a CancellationToken,
}

impl ResearchObjectControl for ProviderCaptureRecoveryControl<'_> {
    fn checkpoint(
        &self,
        _point: ResearchObjectControlPoint,
    ) -> Result<(), ResearchObjectControlError> {
        if self.cancellation.is_cancelled() {
            return Err(ResearchObjectControlError::Cancelled);
        }
        Ok(())
    }
}

/// Exact immutable generation returned after successful reconciliation or commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedDataset {
    pinned: PinnedDataset,
}

/// Closed durable kind of one generation-bound provider market-event publication.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProviderMarketEventPublicationKind {
    /// Canonical market events decoded from one sealed response capture.
    ResponseMarketEvent,
    /// Canonical market events decoded from one sealed live-event microbatch.
    EventMicrobatch,
    /// One sealed response snapshot followed by one sealed live-event microbatch.
    CompositeResponseEvent,
}

impl ProviderMarketEventPublicationKind {
    /// Returns the exact durable catalog and Arrow-metadata tag.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResponseMarketEvent => "response_market_event",
            Self::EventMicrobatch => "event_microbatch",
            Self::CompositeResponseEvent => "composite_response_event",
        }
    }

    fn from_catalog(value: &str) -> Result<Self, IngestError> {
        match value {
            "response_market_event" => Ok(Self::ResponseMarketEvent),
            "event_microbatch" => Ok(Self::EventMicrobatch),
            "composite_response_event" => Ok(Self::CompositeResponseEvent),
            _ => Err(IngestError::ProviderCaptureRequired),
        }
    }
}

/// Exact generation-owned selector required to reopen one provider market-event publication.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProviderMarketEventPublicationSelector {
    publication_digest: EvidenceDigest,
    publication_kind: ProviderMarketEventPublicationKind,
}

impl ProviderMarketEventPublicationSelector {
    /// Returns the exact kind-qualified publication digest.
    pub const fn publication_digest(self) -> EvidenceDigest {
        self.publication_digest
    }

    /// Returns the closed durable publication kind.
    pub const fn publication_kind(self) -> ProviderMarketEventPublicationKind {
        self.publication_kind
    }
}

/// Exact generation-owned selector required to reopen one option-market publication.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProviderOptionMarketPublicationSelector {
    publication_digest: EvidenceDigest,
    publication_kind: OptionMarketBatchKind,
}

impl ProviderOptionMarketPublicationSelector {
    pub const fn publication_digest(self) -> EvidenceDigest {
        self.publication_digest
    }

    pub const fn publication_kind(self) -> OptionMarketBatchKind {
        self.publication_kind
    }
}

/// Exclusive provider publication request consumed by one atomic ingest transition.
#[derive(Debug)]
pub struct ProviderPublicationInput {
    sealed_capture: SealedProviderCaptureBinding,
    revisions: ExtractionRevisionPlan,
    company_identity: Option<CompanyIdentityObservation>,
    precommit_authority: Option<Arc<dyn IngestPrecommitAuthority>>,
}

impl ProviderPublicationInput {
    /// Binds an exact revision plan to the sole non-cloneable sealed publication authority.
    pub fn try_new(
        sealed_capture: SealedProviderCaptureBinding,
        revisions: ExtractionRevisionPlan,
    ) -> Result<Self, IngestError> {
        sealed_capture.validate()?;
        if revisions.len() != sealed_capture.record_count() {
            return Err(IngestError::RevisionEvidenceMismatch);
        }
        Ok(Self {
            sealed_capture,
            revisions,
            company_identity: None,
            precommit_authority: None,
        })
    }

    /// Attaches source-authored company identity to the same provider publication transition.
    pub fn with_company_identity(mut self, identity: CompanyIdentityObservation) -> Self {
        self.company_identity = Some(identity);
        self
    }

    /// Retains process-local caller authority through the final controlled commit.
    pub fn with_precommit_authority(
        mut self,
        authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Self {
        self.precommit_authority = Some(authority);
        self
    }
}

/// Opaque provider semantics retained beside one canonical macro chunk.
///
/// The common data layer binds the adapter-authored semantic identity and exact bounded payload
/// without interpreting provider-specific fields. Provider-native row semantics remain in the
/// sealed capture binding; this companion retains response-wide semantics that cannot be reduced
/// to one canonical row.
#[derive(Debug, Eq, PartialEq)]
pub struct ProviderMacroPlanSemantics {
    schema: SourceIdentifier,
    schema_requirement_digest: EvidenceDigest,
    semantic_digest: EvidenceDigest,
    payload: Box<[u8]>,
    payload_content_digest: EvidenceDigest,
}

impl ProviderMacroPlanSemantics {
    /// Binds an exact bounded provider semantic document to its adapter-authored identities.
    pub fn try_new(
        schema: SourceIdentifier,
        schema_requirement_digest: EvidenceDigest,
        semantic_digest: EvidenceDigest,
        payload: Box<[u8]>,
    ) -> Result<Self, IngestError> {
        require_provider_macro_digest(schema_requirement_digest)?;
        require_provider_macro_digest(semantic_digest)?;
        let payload_bytes =
            u64::try_from(payload.len()).map_err(|_| IngestError::InvalidProviderMacroPlan)?;
        if payload.is_empty() || payload_bytes > MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let payload_content_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(payload.as_ref()).into(),
        );
        Ok(Self {
            schema,
            schema_requirement_digest,
            semantic_digest,
            payload,
            payload_content_digest,
        })
    }

    /// Returns the exact provider semantics schema.
    pub const fn schema(&self) -> &SourceIdentifier {
        &self.schema
    }

    /// Returns the adapter-authored provider semantics identity.
    pub const fn semantic_digest(&self) -> EvidenceDigest {
        self.semantic_digest
    }

    /// Returns the SHA-256 identity of the exact serialized companion payload.
    pub const fn payload_content_digest(&self) -> EvidenceDigest {
        self.payload_content_digest
    }
}

/// One non-cloneable, ordered macro-plan chunk awaiting an all-or-nothing shared commit.
#[derive(Debug)]
pub struct ProviderMacroPlanChunkInput {
    chunk_index: u16,
    total_chunks: u16,
    candidate_digest: EvidenceDigest,
    source_generation_digest: EvidenceDigest,
    semantics: ProviderMacroPlanSemantics,
    sealed_capture: SealedProviderCaptureBinding,
    revisions: ExtractionRevisionPlan,
}

impl ProviderMacroPlanChunkInput {
    /// Consumes one sealed canonical/native/raw chunk and its exact revision authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "the ordered provider chunk keeps every independently verified identity explicit"
    )]
    pub fn try_new(
        chunk_index: u16,
        total_chunks: u16,
        candidate_digest: EvidenceDigest,
        source_generation_digest: EvidenceDigest,
        semantics: ProviderMacroPlanSemantics,
        sealed_capture: SealedProviderCaptureBinding,
        revisions: ExtractionRevisionPlan,
    ) -> Result<Self, IngestError> {
        require_provider_macro_digest(candidate_digest)?;
        require_provider_macro_digest(source_generation_digest)?;
        sealed_capture.validate()?;
        let native_implementation = sealed_capture.native_lineage().schema().implementation();
        if total_chunks == 0
            || chunk_index >= total_chunks
            || sealed_capture.record_count() == 0
            || revisions.len() != sealed_capture.record_count()
            || !revisions.native_lineage_required()
            || !revisions.is_locally_observed()
            || !matches!(
                native_implementation,
                ProviderNativeLineageImplementation::BlsTimeseriesV1
                    | ProviderNativeLineageImplementation::BeaRegionalV1
            )
        {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        validate_provider_macro_chunk_rows(&sealed_capture, &revisions)?;
        Ok(Self {
            chunk_index,
            total_chunks,
            candidate_digest,
            source_generation_digest,
            semantics,
            sealed_capture,
            revisions,
        })
    }

    /// Returns this chunk's contiguous position in the provider plan.
    pub const fn chunk_index(&self) -> u16 {
        self.chunk_index
    }

    /// Returns the exact canonical row count in this chunk.
    pub fn row_count(&self) -> usize {
        self.sealed_capture.record_count()
    }

    /// Returns the exact sealed canonical/native/raw binding.
    pub const fn sealed_capture(&self) -> &SealedProviderCaptureBinding {
        &self.sealed_capture
    }
}

/// Complete ordered provider macro plan admitted for one atomic publication transaction.
#[derive(Debug)]
pub struct ProviderMacroPlanPublicationInput {
    analytical_dataset: DatasetId,
    completion_digest: EvidenceDigest,
    publication_digest: EvidenceDigest,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    provider_dataset: SourceIdentifier,
    request_set_identity: EvidenceDigest,
    source_generation_digest: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    total_rows: u64,
    chunks: Box<[ProviderMacroPlanChunkInput]>,
}

impl ProviderMacroPlanPublicationInput {
    /// Consumes every exact chunk only after proving complete contiguous request-graph closure.
    pub fn try_new(
        analytical_dataset: DatasetId,
        completion_digest: EvidenceDigest,
        expected_total_rows: u64,
        chunks: Vec<ProviderMacroPlanChunkInput>,
    ) -> Result<Self, IngestError> {
        require_provider_macro_digest(completion_digest)?;
        if chunks.is_empty() || chunks.len() > MAX_PROVIDER_CAPTURE_PAGES {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let total_chunks =
            u16::try_from(chunks.len()).map_err(|_| IngestError::InvalidProviderMacroPlan)?;
        let first = chunks
            .first()
            .ok_or(IngestError::InvalidProviderMacroPlan)?;
        let root_capture = first.sealed_capture.capture_evidence();
        let source_id = root_capture.source_id().clone();
        let metadata_revision = root_capture.metadata_revision().clone();
        let provider_dataset = root_capture.dataset().clone();
        let request_set_identity = root_capture.request_set_identity();
        let source_generation_digest = first.source_generation_digest;
        let sealed_capture_receipt_digest = first.sealed_capture.sealed_capture_receipt_digest();
        let semantics_schema = first.semantics.schema.clone();
        let schema_requirement_digest = first.semantics.schema_requirement_digest;
        let native_implementation = first
            .sealed_capture
            .native_lineage()
            .schema()
            .implementation();

        validate_provider_macro_capture_shape(root_capture, total_chunks, native_implementation)?;
        let mut total_rows = 0_u64;
        let mut total_semantics_bytes = 0_u64;
        for (expected_index, chunk) in chunks.iter().enumerate() {
            let expected_index =
                u16::try_from(expected_index).map_err(|_| IngestError::InvalidProviderMacroPlan)?;
            chunk.sealed_capture.validate()?;
            let capture = chunk.sealed_capture.capture_evidence();
            validate_provider_macro_capture_shape(capture, total_chunks, native_implementation)?;
            if chunk.chunk_index != expected_index
                || chunk.total_chunks != total_chunks
                || capture != root_capture
                || chunk.source_generation_digest != source_generation_digest
                || chunk.sealed_capture.sealed_capture_receipt_digest()
                    != sealed_capture_receipt_digest
                || chunk.semantics.schema != semantics_schema
                || chunk.semantics.schema_requirement_digest != schema_requirement_digest
                || chunk
                    .sealed_capture
                    .native_lineage()
                    .schema()
                    .implementation()
                    != native_implementation
                || chunks[..expected_index as usize]
                    .iter()
                    .any(|prior| prior.candidate_digest == chunk.candidate_digest)
            {
                return Err(IngestError::InvalidProviderMacroPlan);
            }
            validate_provider_macro_component(chunk, total_chunks, native_implementation)?;
            total_rows = total_rows
                .checked_add(
                    u64::try_from(chunk.sealed_capture.record_count())
                        .map_err(|_| IngestError::InvalidProviderMacroPlan)?,
                )
                .ok_or(IngestError::InvalidProviderMacroPlan)?;
            total_semantics_bytes = total_semantics_bytes
                .checked_add(
                    u64::try_from(chunk.semantics.payload.len())
                        .map_err(|_| IngestError::InvalidProviderMacroPlan)?,
                )
                .ok_or(IngestError::InvalidProviderMacroPlan)?;
        }
        if total_rows == 0
            || total_rows != expected_total_rows
            || total_semantics_bytes > MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES
        {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let publication_digest = provider_macro_plan_publication_digest(
            &analytical_dataset,
            completion_digest,
            total_rows,
            &chunks,
        )?;
        Ok(Self {
            analytical_dataset,
            completion_digest,
            publication_digest,
            source_id,
            metadata_revision,
            provider_dataset,
            request_set_identity,
            source_generation_digest,
            sealed_capture_receipt_digest,
            total_rows,
            chunks: chunks.into_boxed_slice(),
        })
    }

    /// Returns the Task 3 persist-reservation payload for this exact complete plan.
    pub const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    /// Returns the adapter-authored complete-plan identity bound into the publication digest.
    pub const fn completion_digest(&self) -> EvidenceDigest {
        self.completion_digest
    }

    /// Returns the exact number of deterministic chunks that must commit together.
    pub fn total_chunks(&self) -> u16 {
        u16::try_from(self.chunks.len()).unwrap_or(u16::MAX)
    }

    /// Returns the checked total canonical row count.
    pub const fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Returns the sole source-rights namespace shared by every chunk.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source metadata revision shared by the complete capture graph.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the exact provider dataset addressed by the request graph.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the complete provider request-set identity.
    pub const fn request_set_identity(&self) -> EvidenceDigest {
        self.request_set_identity
    }

    /// Returns the source/configuration/credential generation shared by every chunk.
    pub const fn source_generation_digest(&self) -> EvidenceDigest {
        self.source_generation_digest
    }

    /// Returns the physical receipt identity shared by every component binding.
    pub const fn sealed_capture_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_capture_receipt_digest
    }
}

/// Validated non-cloneable publication state ready for one multi-input catalog transaction.
#[derive(Debug)]
pub struct PendingProviderMacroPlanPublication {
    reservation: IngestReservation,
    input: ProviderMacroPlanPublicationInput,
    prepared_captures: Box<[PreparedProviderCaptureBinding]>,
}

impl PendingProviderMacroPlanPublication {
    /// Returns the exact persist reservation retained for the atomic transaction.
    pub const fn reservation(&self) -> &IngestReservation {
        &self.reservation
    }

    /// Returns how many existing prepared capture projections await ordered retention.
    pub fn prepared_capture_count(&self) -> usize {
        self.prepared_captures.len()
    }

    /// Consumes the complete validated plan into one immutable generation or commits nothing.
    pub async fn commit(
        self,
        service: &AnalyticalDataService,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Result<ProviderMacroPlanPublicationReceipt, IngestError> {
        service
            .commit_prepared_provider_macro_plan(self, cancellation, precommit_authority)
            .await
    }
}

/// Durable result shape the catalog transaction must mint after all-or-nothing publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderMacroPlanPublicationReceipt {
    manifest: DatasetManifestRef,
    completion_digest: EvidenceDigest,
    publication_digest: EvidenceDigest,
    catalog_receipt_digest: EvidenceDigest,
    source_id: SourceId,
    request_set_identity: EvidenceDigest,
    source_generation_digest: EvidenceDigest,
    total_chunks: u16,
    total_rows: u64,
}

impl ProviderMacroPlanPublicationReceipt {
    /// Returns the exact immutable generation created by the atomic transaction.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the adapter-authored complete-plan identity bound to the manifest.
    pub const fn completion_digest(&self) -> EvidenceDigest {
        self.completion_digest
    }

    /// Returns the code-owned identity of every validated publication input.
    pub const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    /// Returns the durable catalog receipt binding the whole plan to this generation.
    pub const fn catalog_receipt_digest(&self) -> EvidenceDigest {
        self.catalog_receipt_digest
    }

    /// Returns the exact committed chunk count.
    pub const fn total_chunks(&self) -> u16 {
        self.total_chunks
    }

    /// Returns the exact committed canonical row count.
    pub const fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Returns an exact-manifest restart selector retaining plan closure and cardinality.
    pub fn restart_selector(&self) -> ProviderMacroPlanRestartSelector {
        ProviderMacroPlanRestartSelector {
            manifest: self.manifest.clone(),
            completion_digest: self.completion_digest,
            publication_digest: self.publication_digest,
            catalog_receipt_digest: self.catalog_receipt_digest,
            source_id: self.source_id.clone(),
            request_set_identity: self.request_set_identity,
            source_generation_digest: self.source_generation_digest,
            total_chunks: self.total_chunks,
            total_rows: self.total_rows,
        }
    }
}

/// Exact immutable selector required to verify a complete macro plan after restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderMacroPlanRestartSelector {
    manifest: DatasetManifestRef,
    completion_digest: EvidenceDigest,
    publication_digest: EvidenceDigest,
    catalog_receipt_digest: EvidenceDigest,
    source_id: SourceId,
    request_set_identity: EvidenceDigest,
    source_generation_digest: EvidenceDigest,
    total_chunks: u16,
    total_rows: u64,
}

impl ProviderMacroPlanRestartSelector {
    /// Returns the exact immutable manifest; no latest-generation substitution is permitted.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the complete-plan identity that durable catalog evidence must reproduce.
    pub const fn completion_digest(&self) -> EvidenceDigest {
        self.completion_digest
    }

    /// Returns the complete input identity that durable catalog evidence must reproduce.
    pub const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    /// Returns the exact immutable catalog receipt that must revalidate after restart.
    pub const fn catalog_receipt_digest(&self) -> EvidenceDigest {
        self.catalog_receipt_digest
    }

    /// Returns the exact source-rights owner.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the complete provider request-set identity bound into the publication digest.
    pub const fn request_set_identity(&self) -> EvidenceDigest {
        self.request_set_identity
    }

    /// Returns the exact provider activation/source generation bound into the publication digest.
    pub const fn source_generation_digest(&self) -> EvidenceDigest {
        self.source_generation_digest
    }

    /// Returns the exact expected chunk count.
    pub const fn total_chunks(&self) -> u16 {
        self.total_chunks
    }

    /// Returns the exact expected canonical row count.
    pub const fn total_rows(&self) -> u64 {
        self.total_rows
    }
}

impl CommittedDataset {
    /// Returns the exact immutable generation pin.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        self.pinned.manifest()
    }

    /// Returns the complete pinned object set.
    pub const fn pinned(&self) -> &PinnedDataset {
        &self.pinned
    }

    fn new(pinned: PinnedDataset) -> Self {
        Self { pinned }
    }
}

/// Exact compaction request identity that callers must bind into the Task 3 reservation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionRequest {
    source: DatasetManifestRef,
    payload_digest: EvidenceDigest,
}

impl CompactionRequest {
    /// Constructs a request bound to one exact immutable source generation.
    pub fn new(source: DatasetManifestRef) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/analytical-compaction/v2");
        digest.update((source.dataset_id().as_str().len() as u64).to_be_bytes());
        digest.update(source.dataset_id().as_str().as_bytes());
        digest.update(source.manifest_version().to_be_bytes());
        digest.update((source.schema().name().len() as u64).to_be_bytes());
        digest.update(source.schema().name().as_bytes());
        digest.update(source.schema_version().get().to_be_bytes());
        digest.update(source.schema().fingerprint());
        digest.update(source.content_hash().bytes());
        Self {
            source,
            payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
        }
    }

    /// Returns the immutable source generation.
    pub const fn source(&self) -> &DatasetManifestRef {
        &self.source
    }

    /// Returns the digest required by [`crate::IngestIdentity`].
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }
}

/// Returns the exact canonical digest a Task 3 ingest reservation must carry for this batch.
pub fn extraction_batch_digest(batch: &ExtractionBatch) -> Result<EvidenceDigest, IngestError> {
    ExtractionContentIdentity::try_from_batch(batch)
        .map(ExtractionContentIdentity::digest)
        .map_err(IngestError::ContentIdentity)
}

/// Returns the exact provider payload digest that owns one normalized extraction.
///
/// This identity deliberately comes from the discovered source object rather than normalized
/// record bytes. Normalized rows retain receive, ingestion, and locally observed availability
/// times, which are truthful attempt provenance but must not turn a retry of the same immutable
/// provider object into a second ingest. The source-object contract already binds the exact
/// provider bytes and extraction rejects a refetch whose bytes do not match that evidence.
pub fn extraction_provider_payload_digest(batch: &ExtractionBatch) -> EvidenceDigest {
    batch.request().object().evidence().content_digest()
}

/// Returns the exact digest a persist reservation must carry for a typed event publication.
pub fn provider_market_event_publication_digest(
    binding: &SealedProviderPublicationBinding,
) -> Result<EvidenceDigest, IngestError> {
    if matches!(binding, SealedProviderPublicationBinding::ResponseSet(_)) {
        return Err(IngestError::ProviderCaptureRequired);
    }
    Ok(binding.evidence_digest().evidence())
}

/// Returns the exact digest a persist reservation must carry for a sealed option batch.
pub fn provider_option_market_publication_digest(
    binding: &SealedProviderOptionMarketBinding,
) -> Result<EvidenceDigest, IngestError> {
    binding.validate()?;
    Ok(binding.evidence_digest().evidence())
}

fn validate_provider_macro_chunk_rows(
    sealed_capture: &SealedProviderCaptureBinding,
    revisions: &ExtractionRevisionPlan,
) -> Result<(), IngestError> {
    let observations =
        ResearchArrowBatch::validated_extraction_observations(sealed_capture.batch())?;
    if observations
        .iter()
        .any(|observation| !matches!(observation, ResearchObservation::Macro(_)))
    {
        return Err(IngestError::InvalidProviderMacroPlan);
    }
    revisions
        .clone()
        .into_observed_batch_with_native_lineage(
            sealed_capture
                .batch()
                .request()
                .object()
                .source_id()
                .clone(),
            sealed_capture.batch(),
            &observations,
            sealed_capture.native_lineage(),
        )
        .map_err(map_revision_error)?;
    Ok(())
}

fn validate_provider_macro_capture_shape(
    capture: &market_squawk_sources::ProviderCaptureSetReceipt,
    total_chunks: u16,
    native_implementation: ProviderNativeLineageImplementation,
) -> Result<(), IngestError> {
    match native_implementation {
        ProviderNativeLineageImplementation::BlsTimeseriesV1 => {
            validate_bls_provider_macro_capture_shape(capture, total_chunks)
        }
        ProviderNativeLineageImplementation::BeaRegionalV1 => {
            validate_bea_provider_macro_capture_shape(capture, total_chunks)
        }
        _ => Err(IngestError::InvalidProviderMacroPlan),
    }
}

fn validate_bls_provider_macro_capture_shape(
    capture: &market_squawk_sources::ProviderCaptureSetReceipt,
    total_chunks: u16,
) -> Result<(), IngestError> {
    let expected = usize::from(total_chunks);
    let valid =
        if total_chunks == 1 {
            capture.terminal() == ProviderCaptureTerminalDisposition::StandaloneResponse
                && capture.pages().len() == 1
                && capture.request_graph_components().is_empty()
                && capture.semantic_binding().is_none()
        } else {
            capture.terminal() == ProviderCaptureTerminalDisposition::CompleteRequestGraph
                && capture.pages().len() == expected
                && capture.request_graph_components().len() == expected
                && capture.semantic_binding().is_none()
                && capture.request_graph_components().iter().enumerate().all(
                    |(index, component)| {
                        usize::from(component.ordinal()) == index
                            && usize::from(component.first_page_ordinal()) == index
                            && component.page_count().get() == 1
                            && component.terminal()
                                == ProviderCaptureTerminalDisposition::StandaloneResponse
                    },
                )
        };
    if valid {
        Ok(())
    } else {
        Err(IngestError::InvalidProviderMacroPlan)
    }
}

fn validate_bea_provider_macro_capture_shape(
    capture: &market_squawk_sources::ProviderCaptureSetReceipt,
    total_chunks: u16,
) -> Result<(), IngestError> {
    let components = capture.request_graph_components();
    let valid = total_chunks == 1
        && capture.terminal() == ProviderCaptureTerminalDisposition::CompleteRequestGraph
        && capture.semantic_binding().is_none()
        && components.len() >= 2
        && capture.pages().len() == components.len()
        && components.iter().enumerate().all(|(index, component)| {
            usize::from(component.ordinal()) == index
                && usize::from(component.first_page_ordinal()) == index
                && component.page_count().get() == 1
                && component.terminal() == ProviderCaptureTerminalDisposition::StandaloneResponse
        });
    if valid {
        Ok(())
    } else {
        Err(IngestError::InvalidProviderMacroPlan)
    }
}

fn validate_provider_macro_component(
    chunk: &ProviderMacroPlanChunkInput,
    total_chunks: u16,
    native_implementation: ProviderNativeLineageImplementation,
) -> Result<(), IngestError> {
    match native_implementation {
        ProviderNativeLineageImplementation::BlsTimeseriesV1 => {
            validate_bls_provider_macro_component(chunk, total_chunks)
        }
        ProviderNativeLineageImplementation::BeaRegionalV1 => {
            validate_bea_provider_macro_component(chunk, total_chunks)
        }
        _ => Err(IngestError::InvalidProviderMacroPlan),
    }
}

fn validate_bls_provider_macro_component(
    chunk: &ProviderMacroPlanChunkInput,
    total_chunks: u16,
) -> Result<(), IngestError> {
    let expected_page = chunk.chunk_index;
    let capture = chunk.sealed_capture.capture_evidence();
    let exact_scope = if total_chunks == 1 {
        chunk.sealed_capture.component_ordinal().is_none()
    } else {
        let component = capture
            .request_graph_components()
            .get(usize::from(chunk.chunk_index))
            .ok_or(IngestError::InvalidProviderMacroPlan)?;
        chunk.sealed_capture.component_ordinal() == Some(chunk.chunk_index)
            && component.dataset() == capture.dataset()
            && component.ordinal() == chunk.chunk_index
    };
    if !exact_scope
        || chunk.sealed_capture.persisted_segment_receipt(0).is_none()
        || chunk.sealed_capture.persisted_segment_receipt(1).is_some()
        || chunk.sealed_capture.row_frames().iter().any(|frame| {
            frame.capture_page_ordinal() != expected_page
                || frame.segment_ordinal() != 0
                || frame.physical_frame_ordinal() != u32::from(expected_page)
        })
    {
        return Err(IngestError::InvalidProviderMacroPlan);
    }
    Ok(())
}

fn validate_bea_provider_macro_component(
    chunk: &ProviderMacroPlanChunkInput,
    total_chunks: u16,
) -> Result<(), IngestError> {
    let capture = chunk.sealed_capture.capture_evidence();
    let components = capture.request_graph_components();
    let data_component = components
        .last()
        .ok_or(IngestError::InvalidProviderMacroPlan)?;
    let data_page_ordinal = data_component.first_page_ordinal();
    let data_page = capture
        .pages()
        .get(usize::from(data_page_ordinal))
        .ok_or(IngestError::InvalidProviderMacroPlan)?;
    let expected_data_component_ordinal = u16::try_from(components.len().saturating_sub(1))
        .map_err(|_| IngestError::InvalidProviderMacroPlan)?;
    if total_chunks != 1
        || chunk.chunk_index != 0
        || chunk.total_chunks != 1
        || chunk.sealed_capture.component_ordinal().is_some()
        || data_component.ordinal() != expected_data_component_ordinal
        || data_component.page_count().get() != 1
        || data_component.content_digest()
            != chunk
                .sealed_capture
                .batch()
                .request()
                .object()
                .evidence()
                .content_digest()
        || chunk.sealed_capture.persisted_segment_receipt(0).is_none()
        || chunk.sealed_capture.persisted_segment_receipt(1).is_some()
        || chunk.sealed_capture.row_frames().iter().any(|frame| {
            frame.capture_page_ordinal() != data_page_ordinal
                || frame.segment_ordinal() != 0
                || frame.physical_frame_ordinal() != u32::from(data_page_ordinal)
                || frame.page_body_digest() != data_page.body_digest()
        })
    {
        return Err(IngestError::InvalidProviderMacroPlan);
    }
    Ok(())
}

fn provider_macro_plan_publication_digest(
    analytical_dataset: &DatasetId,
    completion_digest: EvidenceDigest,
    total_rows: u64,
    chunks: &[ProviderMacroPlanChunkInput],
) -> Result<EvidenceDigest, IngestError> {
    let first = chunks
        .first()
        .ok_or(IngestError::InvalidProviderMacroPlan)?;
    let capture = first.sealed_capture.capture_evidence();
    let mut digest = Sha256::new();
    digest.update(PROVIDER_MACRO_PLAN_PUBLICATION_DOMAIN);
    provider_macro_hash_text(&mut digest, analytical_dataset.as_str())?;
    provider_macro_hash_evidence(&mut digest, completion_digest);
    provider_macro_hash_text(&mut digest, capture.source_id().as_str())?;
    provider_macro_hash_text(
        &mut digest,
        capture.metadata_revision().as_source_identifier().as_str(),
    )?;
    provider_macro_hash_text(&mut digest, capture.dataset().as_str())?;
    provider_macro_hash_evidence(&mut digest, capture.request_set_identity());
    provider_macro_hash_evidence(&mut digest, capture.content_digest());
    provider_macro_hash_evidence(&mut digest, capture.observation_digest());
    provider_macro_hash_evidence(
        &mut digest,
        first.sealed_capture.sealed_capture_receipt_digest(),
    );
    digest.update(
        u16::try_from(chunks.len())
            .map_err(|_| IngestError::InvalidProviderMacroPlan)?
            .to_be_bytes(),
    );
    digest.update(total_rows.to_be_bytes());
    for chunk in chunks {
        digest.update(chunk.chunk_index.to_be_bytes());
        digest.update(chunk.total_chunks.to_be_bytes());
        digest.update(
            u64::try_from(chunk.sealed_capture.record_count())
                .map_err(|_| IngestError::InvalidProviderMacroPlan)?
                .to_be_bytes(),
        );
        provider_macro_hash_evidence(&mut digest, chunk.candidate_digest);
        provider_macro_hash_evidence(&mut digest, chunk.source_generation_digest);
        provider_macro_hash_evidence(
            &mut digest,
            chunk.sealed_capture.evidence_digest().evidence(),
        );
        provider_macro_hash_evidence(
            &mut digest,
            chunk.sealed_capture.content_identity().digest(),
        );
        provider_macro_hash_evidence(
            &mut digest,
            chunk.sealed_capture.native_lineage().batch_digest(),
        );
        provider_macro_hash_text(&mut digest, chunk.semantics.schema.as_str())?;
        provider_macro_hash_evidence(&mut digest, chunk.semantics.schema_requirement_digest);
        provider_macro_hash_evidence(&mut digest, chunk.semantics.semantic_digest);
        provider_macro_hash_evidence(&mut digest, chunk.semantics.payload_content_digest);
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn require_provider_macro_digest(digest: EvidenceDigest) -> Result<(), IngestError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
        Err(IngestError::InvalidProviderMacroPlan)
    } else {
        Ok(())
    }
}

fn provider_macro_hash_text(digest: &mut Sha256, value: &str) -> Result<(), IngestError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| IngestError::InvalidProviderMacroPlan)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

fn provider_macro_hash_evidence(digest: &mut Sha256, value: EvidenceDigest) {
    digest.update(match value.algorithm() {
        DigestAlgorithm::Sha256 => [1],
        DigestAlgorithm::Blake3 => [2],
    });
    digest.update(value.bytes());
}

fn provider_market_event_source_id(
    binding: &SealedProviderPublicationBinding,
) -> Result<&market_squawk_domain::SourceId, IngestError> {
    match binding {
        SealedProviderPublicationBinding::ResponseSet(_) => {
            Err(IngestError::ProviderCaptureRequired)
        }
        SealedProviderPublicationBinding::ResponseMarketEvent(response) => {
            Ok(response.capture_evidence().source_id())
        }
        SealedProviderPublicationBinding::EventMicrobatch(event) => {
            Ok(event.capture_evidence().source_id())
        }
        SealedProviderPublicationBinding::CompositeResponseEvent(composite) => {
            Ok(composite.response().capture_evidence().source_id())
        }
    }
}

/// Process-local authority that must remain live through the durable ingest commit boundary.
pub trait IngestPrecommitAuthority: fmt::Debug + Send + Sync {
    /// Revalidates the exact caller authority immediately before catalog and manifest commit.
    fn validate_precommit(&self) -> Result<(), IngestError>;
}

/// Rights-bound research ingestion service.
#[allow(
    async_fn_in_trait,
    reason = "the canonical local service contract intentionally retains native async cancellation"
)]
pub trait ResearchIngestService {
    /// Converts, publishes, and commits one request-bound extraction batch.
    async fn ingest(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError>;

    /// Assigns durable revisions for a local/imported batch before publication.
    ///
    /// Provider captures must use [`AnalyticalDataService::ingest_provider_publication`].
    async fn ingest_with_revision_plan(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        revisions: ExtractionRevisionPlan,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError>;
}

/// Local composition of Task 3 authority, immutable generations, and controlled Parquet objects.
#[derive(Debug)]
pub struct AnalyticalDataService {
    authority: Arc<Mutex<CatalogAuthority>>,
    catalog_id: uuid::Uuid,
    manifests: Arc<AnalyticalManifestCatalog>,
    objects: Arc<ParquetObjectStore>,
    operation_gate: AnalyticalOperationGate,
}

/// Least-authority factory for one exact listing-reference source.
#[derive(Clone)]
pub struct ListingReferenceAdmissionCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
    dataset: SourceIdentifier,
    source: SourceMetadata,
    registered_at: Timestamp,
}

impl fmt::Debug for ListingReferenceAdmissionCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListingReferenceAdmissionCapability")
            .field("dataset", &self.dataset)
            .field("source_id", self.source.source_id())
            .field(
                "authority",
                &"[SEALED LISTING-REFERENCE ADMISSION AUTHORITY]",
            )
            .finish()
    }
}

impl ListingReferenceAdmissionCapability {
    /// Returns the exact dataset namespace bound to this capability.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the exact source namespace bound to this capability.
    pub const fn source_id(&self) -> &market_squawk_domain::SourceId {
        self.source.source_id()
    }

    /// Returns a source-bound reader that cannot register rights or publish a generation.
    pub fn reader(&self) -> ListingReferenceReadCapability {
        ListingReferenceReadCapability::new(
            Arc::clone(&self.authority),
            self.dataset.clone(),
            self.source.source_id().clone(),
        )
    }

    /// Registers the exact source revision and binds a payload-specific rights decision into one
    /// publication capability.
    pub fn admit(
        &self,
        rights: RightsDecisionInput,
    ) -> Result<ListingReferencePublicationCapability, IngestError> {
        if self.source.source_id() != &rights.source_id {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        let source_id = self.source.source_id().clone();
        let grant = {
            let authority = self
                .authority
                .lock()
                .map_err(|_| IngestError::AuthorityLockPoisoned)?;
            if authority
                .source(&source_id)?
                .as_ref()
                .is_none_or(|registered| registered != &self.source)
            {
                authority.register_source(&self.source, self.registered_at)?;
            }
            authority.admit_source_rights(rights)?
        };
        ListingReferencePublicationCapability::try_new(
            Arc::clone(&self.authority),
            self.dataset.clone(),
            source_id,
            grant,
        )
        .map_err(Into::into)
    }
}

/// Complete immutable operation input for one bounded pinned query with durable overflow.
///
/// Fields stay private so callers cannot detach the query, owner, expiry, cancellation, or
/// absolute deadline from the immutable generation they authorize.
pub struct PinnedArtifactQueryRequest {
    pinned: PinnedDataset,
    table_name: String,
    query: QueryRequest,
    limits: QueryLimits,
    owner: SourceIdentifier,
    artifact_ttl: Duration,
    cancellation: CancellationToken,
    operation_deadline: tokio::time::Instant,
}

impl PinnedArtifactQueryRequest {
    /// Binds every execution and publication input under one absolute operation deadline.
    pub fn try_new(
        pinned: PinnedDataset,
        table_name: impl Into<String>,
        query: QueryRequest,
        limits: QueryLimits,
        owner: SourceIdentifier,
        artifact_ttl: Duration,
        cancellation: CancellationToken,
    ) -> Result<Self, QueryError> {
        let operation_deadline = tokio::time::Instant::now()
            .checked_add(limits.deadline())
            .ok_or(QueryError::InvalidLimits)?;
        Ok(Self {
            pinned,
            table_name: table_name.into(),
            query,
            limits,
            owner,
            artifact_ttl,
            cancellation,
            operation_deadline,
        })
    }
}

impl fmt::Debug for PinnedArtifactQueryRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PinnedArtifactQueryRequest")
            .field("pinned", &"[IMMUTABLE PIN]")
            .field("table_name", &self.table_name)
            .field("query", &"[VALIDATED QUERY]")
            .field("limits", &self.limits)
            .field("owner", &"[PUBLICATION OWNER]")
            .field("artifact_ttl", &self.artifact_ttl)
            .field("cancellation", &"[CANCELLATION CAPABILITY]")
            .field("operation_deadline", &self.operation_deadline)
            .finish()
    }
}

/// Non-separable query-result publication authority for one catalog and artifact root.
pub struct QueryArtifactPublication {
    objects: Arc<ParquetObjectStore>,
    publisher: QueryArtifactPublisher,
    catalog_id: uuid::Uuid,
    root_identity: ArtifactRootIdentity,
    operation_gate: AnalyticalOperationGate,
    #[cfg(test)]
    bind_barrier: Mutex<Option<QueryArtifactBindWorkerBarrier>>,
    #[cfg(test)]
    writer_barrier: Mutex<Option<QueryArtifactWriterWorkerBarrier>>,
}

#[cfg(test)]
#[derive(Debug)]
struct QueryArtifactBindWorkerBarrier {
    checkpoint: QueryArtifactBindCheckpoint,
    entered_sender: std::sync::mpsc::SyncSender<()>,
    release_receiver: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct QueryArtifactBindTestBarrier {
    entered_receiver: Option<std::sync::mpsc::Receiver<()>>,
    release_sender: std::sync::mpsc::SyncSender<()>,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct QueryArtifactWriterWorkerBarrier {
    entered_sender: std::sync::mpsc::SyncSender<()>,
    release_receiver: std::sync::mpsc::Receiver<()>,
    memory_retained: Arc<AtomicBool>,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct QueryArtifactWriterTestBarrier {
    entered_receiver: Option<std::sync::mpsc::Receiver<()>>,
    release_sender: std::sync::mpsc::SyncSender<()>,
    memory_retained: Arc<AtomicBool>,
}

impl fmt::Debug for QueryArtifactPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryArtifactPublication")
            .field("objects", &"[SEALED ROOT CAPABILITY]")
            .field("publisher", &self.publisher)
            .field("catalog", &"[SEALED CATALOG SESSION]")
            .finish()
    }
}

impl QueryArtifactPublication {
    pub(crate) fn root_identity(&self) -> &ArtifactRootIdentity {
        &self.root_identity
    }

    pub(crate) fn writer_admission(
        &self,
        batch: &RecordBatch,
    ) -> Result<QueryArtifactWriterAdmission, ParquetStoreError> {
        self.objects.query_artifact_writer_admission(batch)
    }

    /// Reads one exact query object only while its durable ownership receipt remains live.
    ///
    /// All four independently retained identities—object, catalog artifact, durable ownership,
    /// and this sealed root/catalog publication capability—must agree before bytes are returned.
    pub async fn read_verified_bytes(
        &self,
        object: &PublishedObject,
        artifact: &ArtifactRecord,
        ownership: &crate::QueryArtifactResult,
        maximum_bytes: usize,
        deadline: tokio::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, QueryError> {
        let now = system_timestamp().map_err(|_| QueryError::ArtifactAuthorityRequired)?;
        if ownership.artifact_id() != artifact.artifact_id()
            || ownership.expires_at() <= now
            || artifact.relative_reference() != object.relative_reference()
            || artifact.content_digest().algorithm() != DigestAlgorithm::Sha256
            || artifact.content_digest().bytes() != object.content_hash().bytes()
            || artifact.size_bytes() != object.size_bytes()
            || artifact.created_at() < object.created_at()
            || usize::try_from(object.size_bytes())
                .ok()
                .is_none_or(|size| size > maximum_bytes)
        {
            return Err(QueryError::Artifact(
                ParquetStoreError::ObjectMetadataMismatch,
            ));
        }
        self.objects
            .read_published_bytes_async(object, maximum_bytes, deadline, cancellation)
            .await
            .map_err(map_query_store_error)
    }

    #[cfg(test)]
    pub(crate) fn install_test_bind_barrier(
        &self,
        checkpoint: QueryArtifactBindCheckpoint,
    ) -> Result<QueryArtifactBindTestBarrier, &'static str> {
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        *self
            .bind_barrier
            .lock()
            .map_err(|_| "test query-bind barrier mutex was poisoned")? =
            Some(QueryArtifactBindWorkerBarrier {
                checkpoint,
                entered_sender,
                release_receiver,
            });
        Ok(QueryArtifactBindTestBarrier {
            entered_receiver: Some(entered_receiver),
            release_sender,
        })
    }

    #[cfg(test)]
    pub(crate) fn install_test_writer_barrier(
        &self,
    ) -> Result<QueryArtifactWriterTestBarrier, &'static str> {
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        let memory_retained = Arc::new(AtomicBool::new(false));
        *self
            .writer_barrier
            .lock()
            .map_err(|_| "test query-writer barrier mutex was poisoned")? =
            Some(QueryArtifactWriterWorkerBarrier {
                entered_sender,
                release_receiver,
                memory_retained: Arc::clone(&memory_retained),
            });
        Ok(QueryArtifactWriterTestBarrier {
            entered_receiver: Some(entered_receiver),
            release_sender,
            memory_retained,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_writer_memory_witness(&self) -> Option<Arc<AtomicBool>> {
        self.writer_barrier.lock().ok().and_then(|barrier| {
            barrier
                .as_ref()
                .map(|barrier| Arc::clone(&barrier.memory_retained))
        })
    }

    #[cfg(test)]
    fn take_test_writer_barrier(&self) -> Option<QueryArtifactWriterWorkerBarrier> {
        self.writer_barrier
            .lock()
            .ok()
            .and_then(|mut barrier| barrier.take())
    }

    #[cfg(test)]
    fn wait_at_test_bind_barrier(&self, checkpoint: QueryArtifactBindCheckpoint) {
        let barrier = self.bind_barrier.lock().ok().and_then(|mut barrier| {
            if barrier
                .as_ref()
                .is_some_and(|barrier| barrier.checkpoint == checkpoint)
            {
                barrier.take()
            } else {
                None
            }
        });
        if let Some(barrier) = barrier {
            let _ignored = barrier.entered_sender.send(());
            let _ignored = barrier.release_receiver.recv();
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "publication ownership, admission, deadline, and durability capabilities stay explicit"
    )]
    pub(crate) async fn publish_and_bind(
        &self,
        batch: RecordBatch,
        cancellation: &CancellationToken,
        reservation: &QueryArtifactReservation,
        writer_admission: QueryArtifactWriterAdmission,
        memory_lease: QueryArtifactMemoryLease,
        supervisor: &BlockingIoSupervisor,
        deadline: tokio::time::Instant,
        #[cfg(test)] bind_precommit_deadline: Option<tokio::time::Instant>,
        durable_bound: &AtomicBool,
    ) -> Result<(PublishedObject, ArtifactRecord, crate::QueryArtifactResult), crate::QueryError>
    {
        let _operation = self
            .operation_gate
            .acquire(cancellation)
            .await
            .ok_or(crate::QueryError::Cancelled)?;
        if reservation.catalog_id() != self.catalog_id {
            return Err(crate::QueryError::Catalog(
                CatalogError::InvalidReservationCapability,
            ));
        }
        let lease = self
            .objects
            .begin_publication(cancellation)
            .await
            .map_err(map_query_store_error)?;
        let object = self
            .objects
            .publish_query_artifact_under_lease(
                batch,
                cancellation,
                &lease,
                writer_admission,
                memory_lease,
                supervisor,
                #[cfg(test)]
                self.take_test_writer_barrier(),
            )
            .await
            .map_err(map_query_store_error)?;
        if !self
            .objects
            .verify(&object)
            .map_err(map_query_store_error)?
        {
            return Err(crate::QueryError::Artifact(
                ParquetStoreError::ObjectMetadataMismatch,
            ));
        }
        // Content-addressed publication may return an older, already verified immutable object.
        // The catalog timestamp records this reservation's publication, while the object retains
        // its original filesystem creation time.
        let created_at = object.created_at().max(reservation.requested_at());
        let artifact = ArtifactRecord::try_new(
            object.relative_reference(),
            object.content_hash().evidence(),
            object.size_bytes(),
            created_at,
        )
        .map_err(crate::QueryError::Catalog)?;
        let mut checkpoint = |checkpoint| {
            #[cfg(test)]
            self.wait_at_test_bind_barrier(checkpoint);
            #[cfg(not(test))]
            let _ = checkpoint;
        };
        let ownership = self
            .publisher
            .bind(
                reservation,
                &artifact,
                cancellation,
                deadline,
                #[cfg(test)]
                bind_precommit_deadline,
                durable_bound,
                &mut checkpoint,
            )
            .map_err(map_query_catalog_error)?;
        Ok((object, artifact, ownership))
    }
}

fn map_query_store_error(error: ParquetStoreError) -> crate::QueryError {
    match error {
        ParquetStoreError::Cancelled => crate::QueryError::Cancelled,
        ParquetStoreError::ReadDeadlineExceeded => crate::QueryError::DeadlineExceeded,
        ParquetStoreError::BlockingTaskLimitExceeded => {
            crate::QueryError::BlockingTaskLimitExceeded
        }
        error => crate::QueryError::Artifact(error),
    }
}

fn map_query_catalog_error(error: CatalogError) -> crate::QueryError {
    match error {
        CatalogError::QueryArtifactCancelled => crate::QueryError::Cancelled,
        CatalogError::QueryArtifactDeadlineExceeded => crate::QueryError::DeadlineExceeded,
        error => crate::QueryError::Catalog(error),
    }
}

fn map_query_reservation_error(error: IngestError) -> QueryError {
    match error {
        IngestError::Cancelled => QueryError::Cancelled,
        IngestError::DeadlineExceeded => QueryError::DeadlineExceeded,
        IngestError::Catalog(error) => QueryError::Catalog(error),
        IngestError::Parquet(error) => map_query_store_error(error),
        _ => QueryError::ArtifactAuthorityRequired,
    }
}

fn system_timestamp() -> Result<Timestamp, ()> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ())?;
    let nanos = i64::try_from(elapsed.as_nanos()).map_err(|_| ())?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn map_recovery_store_error(error: ParquetStoreError) -> IngestError {
    match error {
        ParquetStoreError::Cancelled => IngestError::Cancelled,
        ParquetStoreError::RecoveryDeadlineExceeded => IngestError::DeadlineExceeded,
        error => IngestError::Parquet(error),
    }
}

fn map_recovery_manifest_error(error: ManifestCatalogError) -> IngestError {
    match error {
        ManifestCatalogError::Cancelled => IngestError::Cancelled,
        ManifestCatalogError::DeadlineExceeded => IngestError::DeadlineExceeded,
        error => IngestError::Manifest(error),
    }
}

#[cfg(test)]
impl QueryArtifactBindTestBarrier {
    pub(crate) async fn wait_until_entered(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let receiver = self
            .entered_receiver
            .take()
            .ok_or("test query-bind barrier was already entered")?;
        tokio::task::spawn_blocking(move || receiver.recv()).await??;
        Ok(())
    }

    pub(crate) fn release(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.release_sender.send(())?;
        Ok(())
    }
}

#[cfg(test)]
impl QueryArtifactWriterWorkerBarrier {
    pub(crate) fn wait(self) {
        let _ignored = self.entered_sender.send(());
        let _ignored = self.release_receiver.recv();
    }
}

#[cfg(test)]
impl QueryArtifactWriterTestBarrier {
    pub(crate) async fn wait_until_entered(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let receiver = self
            .entered_receiver
            .take()
            .ok_or("test query-writer barrier was already entered")?;
        tokio::task::spawn_blocking(move || receiver.recv()).await??;
        Ok(())
    }

    pub(crate) fn memory_retained(&self) -> bool {
        self.memory_retained.load(Ordering::Acquire)
    }

    pub(crate) fn release(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.release_sender.send(())?;
        Ok(())
    }
}

impl AnalyticalDataService {
    /// Explicitly prepares, durably binds, and activates a fresh analytical catalog/root pair.
    pub fn initialize(
        authority: CatalogAuthority,
        manifests: AnalyticalManifestCatalog,
        artifact_root: market_squawk_platform::ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<Self, IngestError> {
        if authority.artifact_root_binding() != manifests.catalog_binding() {
            return Err(IngestError::CatalogCompositionMismatch);
        }
        let (authority, objects) =
            AuthorityTransitionService::initialize(authority, artifact_root, object_config)
                .map_err(map_authority_transition_error)?;
        Ok(Self::from_active_parts(authority, manifests, objects))
    }

    /// Initializes one analytical service with a restricted provider-onboarding facade.
    ///
    /// Generic transitions cannot carry runtime evidence. Non-Alpaca digest evidence and typed
    /// Alpaca provider observations are admitted only through their dedicated safe methods.
    pub fn initialize_with_provider_onboarding(
        authority: CatalogAuthority,
        manifests: AnalyticalManifestCatalog,
        artifact_root: market_squawk_platform::ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<(Self, crate::OnboardingCatalogCapability), IngestError> {
        let service = Self::initialize(authority, manifests, artifact_root, object_config)?;
        let capability = crate::OnboardingCatalogCapability::new(Arc::clone(&service.authority));
        Ok((service, capability))
    }

    /// Initializes one analytical service while consuming the exclusive pre-service catalog
    /// authority to issue the sole closed feature-dataset production publisher.
    ///
    /// The publisher is returned separately and cannot be recovered from the service or its public
    /// dataset builder. The catalog's process-local single-writer guard makes possession of the
    /// consumed [`CatalogAuthority`] the one-time composition capability.
    pub fn initialize_with_feature_dataset_production(
        authority: CatalogAuthority,
        manifests: AnalyticalManifestCatalog,
        artifact_root: market_squawk_platform::ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<crate::FeatureDatasetProductionComposition, IngestError> {
        Self::initialize(authority, manifests, artifact_root, object_config)
            .map(crate::FeatureDatasetProductionComposition::new)
    }

    /// Explicitly verifies and migrates an exact version-3/version-4 catalog and v1 root pair.
    pub fn migrate_legacy(
        authority: CatalogAuthority,
        manifests: AnalyticalManifestCatalog,
        artifact_root: market_squawk_platform::ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<Self, IngestError> {
        if authority.artifact_root_binding() != manifests.catalog_binding() {
            return Err(IngestError::CatalogCompositionMismatch);
        }
        let (authority, objects) =
            AuthorityTransitionService::migrate_legacy(authority, artifact_root, object_config)
                .map_err(map_authority_transition_error)?;
        Ok(Self::from_active_parts(authority, manifests, objects))
    }

    /// Opens immutable storage from controlled capabilities and a validated generation catalog.
    pub fn open(
        authority: CatalogAuthority,
        manifests: AnalyticalManifestCatalog,
        artifact_root: market_squawk_platform::ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<Self, IngestError> {
        if authority.artifact_root_binding() != manifests.catalog_binding() {
            return Err(IngestError::CatalogCompositionMismatch);
        }
        let (authority, objects) =
            AuthorityTransitionService::open_bound(authority, artifact_root, object_config)
                .map_err(map_authority_transition_error)?;
        Ok(Self::from_active_parts(authority, manifests, objects))
    }

    /// Opens one analytical service with a restricted provider-onboarding facade.
    ///
    /// Generic transitions cannot carry runtime evidence. Non-Alpaca digest evidence and typed
    /// Alpaca provider observations are admitted only through their dedicated safe methods.
    pub fn open_with_provider_onboarding(
        authority: CatalogAuthority,
        manifests: AnalyticalManifestCatalog,
        artifact_root: market_squawk_platform::ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<(Self, crate::OnboardingCatalogCapability), IngestError> {
        let service = Self::open(authority, manifests, artifact_root, object_config)?;
        let capability = crate::OnboardingCatalogCapability::new(Arc::clone(&service.authority));
        Ok((service, capability))
    }

    /// Opens one bound analytical service and transfers the sole session-bound feature-dataset
    /// publisher to root composition.
    ///
    /// No publisher getter exists after this one-time authority-consuming operation.
    pub fn open_with_feature_dataset_production(
        authority: CatalogAuthority,
        manifests: AnalyticalManifestCatalog,
        artifact_root: market_squawk_platform::ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<crate::FeatureDatasetProductionComposition, IngestError> {
        Self::open(authority, manifests, artifact_root, object_config)
            .map(crate::FeatureDatasetProductionComposition::new)
    }

    pub(crate) fn from_active_parts(
        authority: CatalogAuthority,
        manifests: AnalyticalManifestCatalog,
        objects: ParquetObjectStore,
    ) -> Self {
        let catalog_id = authority.session_id();
        Self {
            authority: Arc::new(Mutex::new(authority)),
            catalog_id,
            manifests: Arc::new(manifests),
            objects: Arc::new(objects),
            operation_gate: AnalyticalOperationGate::default(),
        }
    }

    pub(crate) const fn catalog_session_id(&self) -> uuid::Uuid {
        self.catalog_id
    }

    /// Returns the controlled object capability for manifest-pinned query construction.
    pub fn object_store(&self) -> Arc<ParquetObjectStore> {
        Arc::clone(&self.objects)
    }

    /// Returns a cloneable immutable manifest and fixed-template observation read capability.
    pub fn analytical_reader(&self) -> crate::AnalyticalReadCapability {
        crate::AnalyticalReadCapability::new(Arc::clone(&self.manifests), Arc::clone(&self.objects))
    }

    /// Executes one exact pinned query, reserving durable artifact authority only after the first
    /// bounded execution proves that the result crossed the inline threshold.
    ///
    /// The retry uses the same parsed request, manifest, complete execution limits, and immutable
    /// object graph. This avoids leaving unused durable reservations for ordinary inline results.
    pub async fn query_pinned_with_artifact_publication(
        &self,
        operation: PinnedArtifactQueryRequest,
    ) -> Result<PinnedQueryOutput, QueryError> {
        let PinnedArtifactQueryRequest {
            pinned,
            table_name,
            query,
            limits,
            owner,
            artifact_ttl,
            cancellation,
            operation_deadline,
        } = operation;
        let retry_query = query.retry_without_artifact()?;
        let engine = tokio::time::timeout_at(
            operation_deadline,
            ResearchQueryEngine::from_pinned_dataset(
                pinned,
                table_name,
                Arc::clone(&self.objects),
                cancellation.clone(),
            ),
        )
        .await
        .map_err(|_| QueryError::DeadlineExceeded)??;
        let limits = remaining_query_limits(limits, operation_deadline)?;
        match engine
            .query_pinned(query, limits, cancellation.clone())
            .await
        {
            Ok(output) => Ok(output),
            Err(QueryError::ArtifactStoreRequired) => {
                let engine = engine.with_artifact_publication(self.query_artifact_publication())?;
                let limits = remaining_query_limits(limits, operation_deadline)?;
                let expires_at = system_timestamp()
                    .ok()
                    .zip(i64::try_from(artifact_ttl.as_nanos()).ok())
                    .and_then(|(now, ttl)| now.checked_add_nanos(ttl).ok())
                    .ok_or(QueryError::ArtifactAuthorityRequired)?;
                let reservation_input = QueryArtifactReservationInput::try_new(
                    owner,
                    retry_query.artifact_identity(&limits),
                    limits.max_bytes(),
                    expires_at,
                )
                .map_err(QueryError::Catalog)?;
                let reservation = tokio::time::timeout_at(
                    operation_deadline,
                    self.reserve_query_artifact(reservation_input, &cancellation),
                )
                .await
                .map_err(|_| QueryError::DeadlineExceeded)?
                .map_err(map_query_reservation_error)?;
                let query = retry_query.with_artifact_reservation(reservation);
                engine.query_pinned(query, limits, cancellation).await
            }
            Err(error) => Err(error),
        }
    }

    /// Returns fair-value persistence authority over this service's sole catalog writer.
    pub fn fair_value_catalog(&self) -> crate::FairValueCatalogCapability {
        crate::FairValueCatalogCapability::new(Arc::clone(&self.authority))
    }

    /// Returns bounded point-in-time definition reads over this service's sole catalog session.
    pub fn instrument_definitions(&self) -> crate::InstrumentDefinitionReadCapability {
        crate::InstrumentDefinitionReadCapability::new(Arc::clone(&self.authority))
    }

    /// Returns bounded current reads over repository-owned, non-execution market-data definitions.
    pub fn market_data_instruments(&self) -> crate::MarketDataInstrumentReadCapability {
        crate::MarketDataInstrumentReadCapability::new(Arc::clone(&self.authority))
    }

    /// Returns the sole atomic publication authority for receipt-bound market-data definitions.
    pub fn market_data_instrument_synchronization(
        &self,
    ) -> crate::MarketDataInstrumentSynchronizationCapability {
        crate::MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&self.authority))
    }

    /// Returns bounded company-identity reads over this service's sole catalog session.
    pub fn company_identities(&self) -> crate::CompanyIdentityReadCapability {
        crate::CompanyIdentityReadCapability::new(Arc::clone(&self.authority))
    }

    /// Returns the sole narrow publisher for evidence-authorized company/security links.
    ///
    /// The capability publishes only a fully constructed domain link and owns no review,
    /// preview, confirmation, or consumer-workflow policy.
    pub fn company_security_link_publication(
        &self,
    ) -> crate::CompanySecurityLinkPublicationCapability {
        crate::CompanySecurityLinkPublicationCapability::new(Arc::clone(&self.authority))
    }

    /// Returns bounded canonical-instrument publication authority over the sole catalog writer.
    pub fn instrument_catalog(&self) -> crate::InstrumentCatalogCapability {
        crate::InstrumentCatalogCapability::new(Arc::clone(&self.authority))
    }

    /// Returns a sealed admission factory for one exact listing-reference source.
    pub fn listing_reference_admission(
        &self,
        source: SourceMetadata,
        registered_at: Timestamp,
        dataset: SourceIdentifier,
    ) -> ListingReferenceAdmissionCapability {
        ListingReferenceAdmissionCapability {
            authority: Arc::clone(&self.authority),
            dataset,
            source,
            registered_at,
        }
    }

    /// Returns the rights-bound point-in-time dataset builder for this exact catalog/root pair.
    pub fn dataset_builder(&self) -> crate::DatasetBuilderService<'_> {
        crate::DatasetBuilderService::new(
            self,
            Arc::clone(&self.authority),
            self.operation_gate.clone(),
        )
    }

    /// Returns object-safe observed-revision authority over this exact shared catalog writer.
    pub fn observed_revision_authority(&self) -> Arc<dyn ObservedRevisionAuthority> {
        Arc::new(CatalogObservedRevisionAuthority::new(Arc::clone(
            &self.authority,
        )))
    }

    /// Registers a source and atomically admits and reserves one exact persist operation through
    /// this service's sole catalog authority.
    pub async fn reserve_source_ingest(
        &self,
        source: &SourceMetadata,
        registered_at: Timestamp,
        rights: RightsDecisionInput,
        identity: &IngestIdentity,
        cancellation: &CancellationToken,
    ) -> Result<IngestReservation, IngestError> {
        if source.source_id() != identity.source_id()
            || rights.source_id != *identity.source_id()
            || rights.payload_digest != identity.payload_digest()
            || identity.operation() != SourceOperation::Persist
        {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        let _operation = self
            .operation_gate
            .acquire(cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        let authority = self.lock_authority()?;
        if authority
            .source(source.source_id())?
            .as_ref()
            .is_none_or(|registered| registered != source)
        {
            authority.register_source(source, registered_at)?;
        }
        let grant = authority.admit_source_rights(rights)?;
        authority
            .reserve_ingest(identity, &grant)
            .map_err(Into::into)
    }

    /// Reconciles the sealed raw-object store against the complete immutable catalog receipt set.
    ///
    /// This is the startup boundary for quarantining incomplete stages and unreferenced final
    /// objects. Every catalog claim is decoded and cross-checked against its publication and
    /// physical-unit rows, then the sealed store verifies every retained object before moving
    /// anything.
    pub async fn recover_provider_capture_store(
        &self,
        store: Arc<market_squawk_platform::SealedResearchJournalStore>,
        cancellation: &CancellationToken,
    ) -> Result<market_squawk_platform::SealedResearchJournalRecoveryReport, IngestError> {
        let _operation = self
            .operation_gate
            .acquire(cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        let supervisor = BlockingIoSupervisor::new(cancellation.clone());
        let authority = Arc::clone(&self.authority);
        let worker_cancellation = cancellation.clone();
        let worker = supervisor
            .spawn_blocking(move || {
                recover_provider_capture_store_blocking(authority, store, &worker_cancellation)
            })
            .map_err(map_provider_recovery_admission_error)?;
        worker
            .await
            .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)?
    }

    /// Lists every bounded provider-binding digest retained by one exact generation.
    pub fn provider_capture_binding_digests(
        &self,
        manifest: &DatasetManifestRef,
    ) -> Result<Vec<EvidenceDigest>, IngestError> {
        self.manifests
            .provider_capture_binding_digests(manifest)
            .map_err(IngestError::Manifest)
    }

    /// Reopens and verifies one explicitly selected historical provider binding.
    pub fn provider_capture_binding_evidence(
        &self,
        manifest: &DatasetManifestRef,
        binding_digest: EvidenceDigest,
        store: &market_squawk_platform::SealedResearchJournalStore,
    ) -> Result<crate::PersistedProviderCaptureBindingEvidence, IngestError> {
        let binding_digests = self.manifests.provider_capture_binding_digests(manifest)?;
        if !binding_digests.contains(&binding_digest) {
            return Err(IngestError::ProviderCaptureRequired);
        }
        let evidence = self
            .lock_authority()?
            .provider_capture_binding_evidence(binding_digest)?
            .ok_or(IngestError::ProviderCaptureRequired)?;
        evidence.verify_integrity()?;
        for physical in evidence.physical_claims() {
            let verified = store.open_verified_claim(physical.claim())?;
            if verified.receipt().claim() != physical.claim() {
                return Err(IngestError::ProviderCaptureRequired);
            }
            drop(verified);
        }
        Ok(evidence)
    }

    /// Lists every bounded, kind-qualified market-event publication retained by one generation.
    pub fn provider_market_event_publications(
        &self,
        manifest: &DatasetManifestRef,
    ) -> Result<Vec<ProviderMarketEventPublicationSelector>, IngestError> {
        let retained = self.manifests.provider_publication_bindings(manifest)?;
        let mut selectors = Vec::new();
        selectors
            .try_reserve_exact(retained.len())
            .map_err(|_| IngestError::ProviderCaptureRequired)?;
        for (publication_digest, publication_kind) in retained {
            if matches!(
                publication_kind.as_str(),
                "option_snapshots" | "option_expirations"
            ) {
                continue;
            }
            if selectors
                .iter()
                .any(|selector: &ProviderMarketEventPublicationSelector| {
                    selector.publication_digest == publication_digest
                })
            {
                return Err(IngestError::ProviderCaptureRequired);
            }
            selectors.push(ProviderMarketEventPublicationSelector {
                publication_digest,
                publication_kind: ProviderMarketEventPublicationKind::from_catalog(
                    &publication_kind,
                )?,
            });
        }
        Ok(selectors)
    }

    /// Lists every bounded option-market publication retained by one exact generation.
    pub fn provider_option_market_publications(
        &self,
        manifest: &DatasetManifestRef,
    ) -> Result<Vec<ProviderOptionMarketPublicationSelector>, IngestError> {
        let retained = self.manifests.provider_publication_bindings(manifest)?;
        let mut selectors = Vec::new();
        selectors
            .try_reserve_exact(retained.len())
            .map_err(|_| IngestError::ProviderCaptureRequired)?;
        for (publication_digest, publication_kind) in retained {
            let publication_kind = match publication_kind.as_str() {
                "option_snapshots" => OptionMarketBatchKind::Snapshots,
                "option_expirations" => OptionMarketBatchKind::Expirations,
                "response_market_event" | "event_microbatch" | "composite_response_event" => {
                    continue;
                }
                _ => return Err(IngestError::ProviderCaptureRequired),
            };
            selectors.push(ProviderOptionMarketPublicationSelector {
                publication_digest,
                publication_kind,
            });
        }
        Ok(selectors)
    }

    /// Reopens and verifies one generation-bound typed event publication's raw evidence.
    pub fn provider_market_event_publication_evidence(
        &self,
        manifest: &DatasetManifestRef,
        selector: ProviderMarketEventPublicationSelector,
        store: &market_squawk_platform::SealedResearchJournalStore,
    ) -> Result<crate::PersistedProviderPublicationEvidence, IngestError> {
        if !self
            .provider_market_event_publications(manifest)?
            .contains(&selector)
        {
            return Err(IngestError::ProviderCaptureRequired);
        }
        let evidence = self
            .lock_authority()?
            .provider_publication_evidence(selector.publication_digest)?
            .ok_or(IngestError::ProviderCaptureRequired)?;
        evidence.verify_integrity()?;
        if evidence.publication_digest() != selector.publication_digest
            || evidence.publication_kind() != selector.publication_kind.as_str()
        {
            return Err(IngestError::ProviderCaptureRequired);
        }
        if let Some(response) = evidence.response() {
            let verified = store.open_verified_claim(response.physical_claim())?;
            if verified.receipt().claim() != response.physical_claim() {
                return Err(IngestError::ProviderCaptureRequired);
            }
        }
        if let Some(event) = evidence.event() {
            let verified = store.open_verified_claim(event.physical_claim())?;
            if verified.receipt().claim() != event.physical_claim() {
                return Err(IngestError::ProviderCaptureRequired);
            }
        }
        Ok(evidence)
    }

    /// Reopens one generation-bound typed event publication and verifies raw claim plus Parquet.
    pub async fn read_provider_market_event_publication(
        &self,
        manifest: &DatasetManifestRef,
        selector: ProviderMarketEventPublicationSelector,
        store: &market_squawk_platform::SealedResearchJournalStore,
        cancellation: CancellationToken,
    ) -> Result<ProviderMarketEventArrowBatch, IngestError> {
        let evidence =
            self.provider_market_event_publication_evidence(manifest, selector, store)?;
        let pinned = self.manifests.pinned(manifest)?;
        let batches = self
            .objects
            .read_pinned_async(&pinned, &cancellation)
            .await?;
        Self::provider_market_event_batch_from_pinned(&batches, selector, &evidence)
    }

    fn provider_market_event_batch_from_pinned(
        batches: &[RecordBatch],
        selector: ProviderMarketEventPublicationSelector,
        evidence: &crate::PersistedProviderPublicationEvidence,
    ) -> Result<ProviderMarketEventArrowBatch, IngestError> {
        let expected_hex = crate::schema::encode_hex(selector.publication_digest.bytes());
        let expected_kind = selector.publication_kind.as_str();
        let selected = batches
            .iter()
            .filter(|batch| {
                let schema = batch.schema();
                schema
                    .metadata()
                    .get(crate::schema::PROVIDER_PUBLICATION_DIGEST_KEY)
                    == Some(&expected_hex)
                    && schema
                        .metadata()
                        .get(crate::schema::PROVIDER_PUBLICATION_KIND_KEY)
                        .is_some_and(|kind| kind == expected_kind)
            })
            .cloned()
            .collect::<Vec<_>>();
        let schema = selected
            .first()
            .map(RecordBatch::schema)
            .ok_or(IngestError::ProviderCaptureRequired)?;
        let batch = concat_batches(&schema, &selected).map_err(ArrowConversionError::Arrow)?;
        ProviderMarketEventArrowBatch::try_from_record_batch_with_publication_evidence(
            batch,
            &evidence,
            MAX_EVENT_PUBLICATION_READ_BYTES,
        )
        .map_err(IngestError::Arrow)
    }

    /// Selects and reopens every newest coherent market-event tie at exact PIT cutoffs.
    ///
    /// The data layer retains source surfaces separately; provider ranking remains an application
    /// concern. Each selected publication is reopened once from the exact resolved manifest, then
    /// its catalog coordinate, canonical Parquet row, and persisted raw evidence are reconciled.
    pub async fn read_provider_market_event_point_in_time(
        &self,
        request: &crate::ProviderMarketEventPointInTimeRequest,
        store: &market_squawk_platform::SealedResearchJournalStore,
        cancellation: CancellationToken,
    ) -> Result<Option<crate::ProviderMarketEventPointInTimeSelection>, IngestError> {
        let Some(plan) = self
            .manifests
            .select_provider_market_event_candidates(request)?
        else {
            return Ok(None);
        };
        if plan.candidates.is_empty() {
            return crate::ProviderMarketEventPointInTimeSelection::try_from_reconstructed(
                request.clone(),
                plan,
                Vec::new(),
            )
            .map(Some)
            .map_err(Into::into);
        }
        let pinned = self.manifests.pinned(&plan.manifest)?;
        let batches = self
            .objects
            .read_pinned_async(&pinned, &cancellation)
            .await?;

        let mut reopened: Vec<(
            ProviderMarketEventPublicationSelector,
            Arc<crate::PersistedProviderPublicationEvidence>,
            ProviderMarketEventArrowBatch,
        )> = Vec::new();
        reopened
            .try_reserve_exact(plan.candidates.len())
            .map_err(|_| crate::ProviderMarketEventSelectionError::Allocation)?;
        for planned in &plan.candidates {
            if cancellation.is_cancelled() {
                return Err(IngestError::Cancelled);
            }
            let selector = ProviderMarketEventPublicationSelector {
                publication_digest: planned.publication.digest(),
                publication_kind: planned.publication.kind(),
            };
            if reopened
                .iter()
                .any(|(retained, _, _)| *retained == selector)
            {
                continue;
            }
            let evidence = Arc::new(self.provider_market_event_publication_evidence(
                &plan.manifest,
                selector,
                store,
            )?);
            let batch =
                Self::provider_market_event_batch_from_pinned(&batches, selector, &evidence)?;
            reopened.push((selector, evidence, batch));
        }

        let mut reconstructed = Vec::new();
        reconstructed
            .try_reserve_exact(plan.candidates.len())
            .map_err(|_| crate::ProviderMarketEventSelectionError::Allocation)?;
        let authority = self.lock_authority()?;
        for planned in &plan.candidates {
            if cancellation.is_cancelled() {
                return Err(IngestError::Cancelled);
            }
            let selector = ProviderMarketEventPublicationSelector {
                publication_digest: planned.publication.digest(),
                publication_kind: planned.publication.kind(),
            };
            let (_, evidence, batch) = reopened
                .iter()
                .find(|(retained, _, _)| *retained == selector)
                .ok_or(crate::ProviderMarketEventSelectionError::EvidenceMismatch)?;
            reconstructed.push(
                crate::ProviderMarketEventSelectedCandidate::try_from_reopened_publication(
                    request,
                    planned,
                    authority.catalog(),
                    batch,
                    Arc::clone(evidence),
                )?,
            );
        }
        drop(authority);
        crate::ProviderMarketEventPointInTimeSelection::try_from_reconstructed(
            request.clone(),
            plan,
            reconstructed,
        )
        .map(Some)
        .map_err(Into::into)
    }

    /// Replays one prior selection against its exact manifest and verifies the full receipt.
    pub async fn verify_provider_market_event_point_in_time_restart(
        &self,
        original: &crate::ProviderMarketEventPointInTimeSelection,
        store: &market_squawk_platform::SealedResearchJournalStore,
        cancellation: CancellationToken,
    ) -> Result<crate::ProviderMarketEventPointInTimeSelection, IngestError> {
        let request = original.exact_restart_request()?;
        let replay = self
            .read_provider_market_event_point_in_time(&request, store, cancellation)
            .await?
            .ok_or(crate::ProviderMarketEventSelectionError::RestartMismatch)?;
        original.verify_restart_replay(&replay)?;
        Ok(replay)
    }

    /// Returns sealed backup authority for this exact active catalog and artifact root.
    pub fn backup_service(&self) -> crate::AnalyticalBackupService {
        crate::AnalyticalBackupService::new(
            self.operation_gate.clone(),
            Arc::clone(&self.authority),
            Arc::clone(&self.objects),
        )
    }

    /// Reopens and verifies one generation-bound option publication's raw evidence.
    pub fn provider_option_market_publication_evidence(
        &self,
        manifest: &DatasetManifestRef,
        selector: ProviderOptionMarketPublicationSelector,
        store: &market_squawk_platform::SealedResearchJournalStore,
    ) -> Result<crate::PersistedProviderOptionMarketBindingEvidence, IngestError> {
        if !self
            .provider_option_market_publications(manifest)?
            .contains(&selector)
        {
            return Err(IngestError::ProviderCaptureRequired);
        }
        let evidence = self
            .lock_authority()?
            .provider_option_market_binding_evidence(selector.publication_digest)?
            .ok_or(IngestError::ProviderCaptureRequired)?;
        evidence.verify_integrity()?;
        if evidence.binding_digest() != selector.publication_digest
            || evidence.publication_kind() != selector.publication_kind
        {
            return Err(IngestError::ProviderCaptureRequired);
        }
        let verified = store.open_verified_claim(evidence.physical_claim())?;
        if verified.receipt().claim() != evidence.physical_claim() {
            return Err(IngestError::ProviderCaptureRequired);
        }
        drop(verified);
        Ok(evidence)
    }

    /// Reopens one exact option publication and verifies its raw claim plus pinned Parquet.
    pub async fn read_provider_option_market_publication(
        &self,
        manifest: &DatasetManifestRef,
        selector: ProviderOptionMarketPublicationSelector,
        store: &market_squawk_platform::SealedResearchJournalStore,
        cancellation: CancellationToken,
    ) -> Result<ProviderOptionMarketArrowBatch, IngestError> {
        let evidence =
            self.provider_option_market_publication_evidence(manifest, selector, store)?;
        let pinned = self.manifests.pinned(manifest)?;
        let batches = self
            .objects
            .read_pinned_async(&pinned, &cancellation)
            .await?;
        let expected_hex = crate::schema::encode_hex(selector.publication_digest.bytes());
        let expected_kind = match selector.publication_kind {
            OptionMarketBatchKind::Snapshots => "option_snapshots",
            OptionMarketBatchKind::Expirations => "option_expirations",
        };
        let selected = batches
            .into_iter()
            .filter(|batch| {
                let schema = batch.schema();
                schema
                    .metadata()
                    .get(crate::schema::PROVIDER_PUBLICATION_DIGEST_KEY)
                    == Some(&expected_hex)
                    && schema
                        .metadata()
                        .get(crate::schema::PROVIDER_PUBLICATION_KIND_KEY)
                        .is_some_and(|kind| kind == expected_kind)
            })
            .collect::<Vec<_>>();
        let schema = selected
            .first()
            .map(RecordBatch::schema)
            .ok_or(IngestError::ProviderCaptureRequired)?;
        let batch = concat_batches(&schema, &selected).map_err(ArrowConversionError::Arrow)?;
        ProviderOptionMarketArrowBatch::try_from_record_batch_with_publication_evidence(
            batch,
            &evidence,
            MAX_OPTION_PUBLICATION_READ_BYTES,
        )
        .map_err(IngestError::Arrow)
    }

    /// Selects and reopens the uniquely latest coherent option batch at one knowledge cutoff.
    pub async fn read_provider_option_market_point_in_time(
        &self,
        request: &crate::OptionMarketPointInTimeRequest,
        store: &market_squawk_platform::SealedResearchJournalStore,
        cancellation: CancellationToken,
    ) -> Result<Option<crate::OptionMarketPointInTimeSelection>, IngestError> {
        let Some((manifest, publication_digest, publication_kind)) = self
            .manifests
            .select_provider_option_market_publication(request)?
        else {
            return Ok(None);
        };
        let publication_kind = match publication_kind.as_str() {
            "option_snapshots" => OptionMarketBatchKind::Snapshots,
            "option_expirations" => OptionMarketBatchKind::Expirations,
            _ => return Err(IngestError::ProviderCaptureRequired),
        };
        let selector = ProviderOptionMarketPublicationSelector {
            publication_digest,
            publication_kind,
        };
        let batch = self
            .read_provider_option_market_publication(&manifest, selector, store, cancellation)
            .await?;
        let retained_filter = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(serde_json::to_vec(batch.scope().filter())?).into(),
        );
        if retained_filter != request.filter_digest() {
            return Err(IngestError::ProviderCaptureRequired);
        }
        crate::OptionMarketPointInTimeSelection::try_new(request, manifest, batch)
            .map(Some)
            .map_err(IngestError::Arrow)
    }

    /// Persists query-result ownership and expiry before artifact publication begins.
    pub async fn reserve_query_artifact(
        &self,
        input: QueryArtifactReservationInput,
        cancellation: &CancellationToken,
    ) -> Result<QueryArtifactReservation, IngestError> {
        let _operation = self
            .operation_gate
            .acquire(cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        Ok(self.lock_authority()?.reserve_query_artifact(input)?)
    }

    /// Issues one sealed publisher that cannot be paired with another root or catalog writer.
    pub fn query_artifact_publication(&self) -> Arc<QueryArtifactPublication> {
        Arc::new(QueryArtifactPublication {
            objects: Arc::clone(&self.objects),
            publisher: QueryArtifactPublisher::new(Arc::clone(&self.authority)),
            catalog_id: self.catalog_id,
            root_identity: self.objects.authority_identity().clone(),
            operation_gate: self.operation_gate.clone(),
            #[cfg(test)]
            bind_barrier: Mutex::new(None),
            #[cfg(test)]
            writer_barrier: Mutex::new(None),
        })
    }

    /// Resolves one exact immutable generation.
    pub fn pinned(&self, manifest: &DatasetManifestRef) -> Result<PinnedDataset, IngestError> {
        Ok(self.manifests.pinned(manifest)?)
    }

    /// Resolves a unique derived generation by its complete immutable build identity.
    pub(crate) fn matching_derived_build(
        &self,
        dataset_id: &DatasetId,
        build_spec_digest: crate::DatasetBuildSpecDigest,
    ) -> Result<Option<PinnedDataset>, IngestError> {
        self.manifests
            .matching_derived_build(dataset_id, build_spec_digest)
            .map_err(Into::into)
    }

    /// Rewrites the current pinned generation into one immutable object without changing rows.
    pub async fn compact(
        &self,
        reservation: IngestReservation,
        request: CompactionRequest,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError> {
        let pinned = self.manifests.pinned(request.source())?;
        let source_id = self.manifests.source_id(request.source())?;
        {
            let authority = self.lock_authority()?;
            let run = self.validate_run(
                &authority,
                &reservation,
                request.payload_digest(),
                Some(&source_id),
            )?;
            if run.state() == IngestRunState::Failed {
                return Err(IngestError::TerminalRun);
            }
        }
        let batches = self
            .objects
            .read_pinned_async(&pinned, &cancellation)
            .await?;
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        let dataset_name = SourceIdentifier::try_from(request.source().dataset_id().as_str())
            .map_err(|_| IngestError::InvalidDataset)?;
        let compacted = ResearchArrowBatch::try_from_compaction_batches(
            dataset_name.clone(),
            request.payload_digest(),
            batches,
        )?;
        let schema = compacted.schema_ref().clone();
        let compacted = DatasetArrowBatch::from(compacted);
        let _operation = self
            .operation_gate
            .acquire(&cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        {
            let authority = self.lock_authority()?;
            let run = self.validate_run(
                &authority,
                &reservation,
                request.payload_digest(),
                Some(&source_id),
            )?;
            if run.state() == IngestRunState::Failed {
                return Err(IngestError::TerminalRun);
            }
        }
        let publication = self.objects.begin_publication(&cancellation).await?;
        let published = self
            .objects
            .publish_dataset_under_lease(&compacted, &cancellation, &publication)
            .await?;
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        let object = ManifestObject::try_new(
            published.content_hash(),
            published.row_count(),
            published.size_bytes(),
            pinned.plan().lineage_digest(),
        )?;

        let authority = self.lock_authority()?;
        let run = self.validate_run(
            &authority,
            &reservation,
            request.payload_digest(),
            Some(&source_id),
        )?;
        if let Some(committed) = self.reconcile_existing(
            &authority,
            &reservation,
            run.state(),
            request.source().dataset_id(),
            &schema,
            &object,
            None,
            None,
        )? {
            return Ok(committed);
        }
        let plan = self
            .manifests
            .preview_compaction(request.source(), object)?;
        self.commit_plan(
            &authority,
            &reservation,
            &run,
            dataset_name,
            schema,
            plan,
            published,
            GenerationKind::Compaction,
            None,
            None,
            None,
            PublicationSourceEvidence::NoNewRawInput,
        )
    }

    /// Quarantines only content-addressed objects absent from every retained generation.
    ///
    /// # Errors
    ///
    /// Returns [`IngestError::Cancelled`] when cancellation wins admission or is observed during
    /// bounded recovery. Returns [`IngestError::DeadlineExceeded`] when recovery exceeds its fixed
    /// elapsed-time ceiling. Catalog, object-store, or authority failures retain their typed
    /// [`IngestError`] variants.
    pub async fn recover_orphans(
        &self,
        now: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<OrphanRecoveryReport, IngestError> {
        let deadline = Instant::now()
            .checked_add(ORPHAN_RECOVERY_DEADLINE)
            .ok_or(IngestError::DeadlineExceeded)?;
        let _operation = self
            .operation_gate
            .acquire(&cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        let mut recovery = self
            .objects
            .begin_recovery(now, &cancellation, deadline)
            .await
            .map_err(map_recovery_store_error)?;
        let _authority = self.lock_authority()?;
        let referenced = self
            .manifests
            .referenced_candidates(
                recovery
                    .candidates()
                    .iter()
                    .map(PublishedObject::content_hash),
                now,
                MAX_SCAN_OBJECTS,
                deadline,
                &cancellation,
            )
            .map_err(map_recovery_manifest_error)?;
        for (index, is_referenced) in referenced.into_iter().enumerate() {
            if is_referenced {
                continue;
            }
            recovery
                .quarantine_candidate(index)
                .map_err(map_recovery_store_error)?;
        }
        recovery.finish().map_err(map_recovery_store_error)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "ingestion retains revision, identity, cancellation, and precommit authority explicitly"
    )]
    async fn ingest_batch(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: &ExtractionBatch,
        revision_plan: Option<ExtractionRevisionPlan>,
        provider_binding: Option<&PreparedProviderCaptureBinding>,
        provider_native_lineage: Option<&market_squawk_sources::ProviderNativeLineageBatch>,
        company_identity: Option<CompanyIdentityObservation>,
        cancellation: CancellationToken,
        precommit_authority: Option<Arc<dyn IngestPrecommitAuthority>>,
    ) -> Result<CommittedDataset, IngestError> {
        let payload_digest = extraction_provider_payload_digest(batch);
        let source_id = batch.request().object().source_id().clone();
        let paged_capture = matches!(
            batch.request().object().capture_identity(),
            SourceObjectCaptureIdentity::Paged { .. }
        );
        if paged_capture != provider_binding.is_some() {
            return Err(IngestError::ProviderCaptureRequired);
        }
        if company_identity.as_ref().is_some_and(|identity| {
            identity.source_id() != &source_id
                || identity.parent_ingest_payload_evidence().content_digest() != payload_digest
        }) {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        let dataset_name = SourceIdentifier::try_from(analytical_dataset.as_str())
            .map_err(|_| IngestError::InvalidDataset)?;
        let observations = ResearchArrowBatch::validated_extraction_observations(batch)?;
        let market_bar_history = MarketBarHistoryPublicationCandidate::try_from_batch(
            batch,
            &observations,
            provider_binding,
        )?;
        {
            let authority = self.lock_authority()?;
            let run =
                self.validate_run(&authority, &reservation, payload_digest, Some(&source_id))?;
            if run.state() == IngestRunState::Failed {
                return Err(IngestError::TerminalRun);
            }
            if provider_binding.is_some() && run.state() == IngestRunState::Succeeded {
                return self.reconcile_succeeded_provider_run(
                    &authority,
                    &reservation,
                    &analytical_dataset,
                    company_identity.as_ref(),
                );
            }
            self.validate_provider_binding(&authority, &reservation, provider_binding)?;
            if provider_binding.is_none() {
                let source = authority
                    .source(&source_id)?
                    .ok_or(IngestError::UnknownSource)?;
                if !matches!(
                    source.source_class(),
                    SourceClass::LocalFile | SourceClass::PortfolioExport
                ) {
                    return Err(IngestError::ProviderCaptureRequired);
                }
            }
            if let Some(committed) = self.reconcile_committed_run(
                &authority,
                &reservation,
                run.state(),
                &analytical_dataset,
                company_identity.as_ref(),
                market_bar_history.as_ref(),
            )? {
                return Ok(committed);
            }
        }
        let revision_plan = match revision_plan {
            Some(plan) => plan,
            None => {
                let authority = self.lock_authority()?;
                let source = authority
                    .source(&source_id)?
                    .ok_or(IngestError::UnknownSource)?;
                if !matches!(
                    source.source_class(),
                    SourceClass::LocalFile | SourceClass::PortfolioExport
                ) {
                    return Err(IngestError::RevisionEvidenceRequired);
                }
                ExtractionRevisionPlan::locally_observed(observations.len())
                    .map_err(map_revision_error)?
            }
        };
        if revision_plan.len() != observations.len() {
            return Err(IngestError::RevisionEvidenceMismatch);
        }
        let observed_batch = match (provider_binding, provider_native_lineage) {
            (Some(_), Some(native_lineage)) => revision_plan
                .into_observed_batch_with_native_lineage(
                    source_id.clone(),
                    batch,
                    &observations,
                    native_lineage,
                )
                .map_err(map_revision_error)?,
            (None, None) => revision_plan
                .into_observed_batch(source_id.clone(), &observations)
                .map_err(map_revision_error)?,
            _ => return Err(IngestError::ProviderCaptureRequired),
        };
        let deadline = Instant::now()
            .checked_add(REVISION_ASSIGNMENT_DEADLINE)
            .ok_or(IngestError::DeadlineExceeded)?;
        let assignments = self
            .observed_revision_authority()
            .assign(observed_batch, deadline, cancellation.clone())
            .await
            .map_err(map_revision_error)?;
        let converted = match provider_binding {
            Some(binding) => ResearchArrowBatch::try_from_extraction_batch_with_assigned_revisions_and_provider_binding(
                batch,
                assignments.as_slice(),
                binding,
            )?,
            None => ResearchArrowBatch::try_from_extraction_batch_with_assigned_revisions(
                batch,
                assignments.as_slice(),
            )?,
        };
        let schema = converted.schema_ref().clone();
        let lineage = converted.lineage_digest()?;
        let converted = DatasetArrowBatch::from(converted);
        self.manifests
            .validate_append_schema(&analytical_dataset, &schema)?;
        let _operation = self
            .operation_gate
            .acquire(&cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        {
            let authority = self.lock_authority()?;
            let run =
                self.validate_run(&authority, &reservation, payload_digest, Some(&source_id))?;
            self.validate_provider_binding(&authority, &reservation, provider_binding)?;
            if run.state() == IngestRunState::Failed {
                return Err(IngestError::TerminalRun);
            }
        }
        let publication = self.objects.begin_publication(&cancellation).await?;
        let published = self
            .objects
            .publish_dataset_under_lease(&converted, &cancellation, &publication)
            .await?;
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        let object = ManifestObject::try_new(
            published.content_hash(),
            published.row_count(),
            published.size_bytes(),
            Sha256Digest::new(lineage.bytes()),
        )?;

        let authority = self.lock_authority()?;
        let run = self.validate_run(&authority, &reservation, payload_digest, Some(&source_id))?;
        self.validate_provider_binding(&authority, &reservation, provider_binding)?;
        if let Some(committed) = self.reconcile_existing(
            &authority,
            &reservation,
            run.state(),
            &analytical_dataset,
            &schema,
            &object,
            company_identity.as_ref(),
            market_bar_history.as_ref(),
        )? {
            return Ok(committed);
        }
        let plan = self
            .manifests
            .preview_append(analytical_dataset, &schema, object)?;
        self.commit_plan(
            &authority,
            &reservation,
            &run,
            dataset_name,
            schema,
            plan,
            published,
            GenerationKind::Ingest,
            precommit_authority.as_deref(),
            company_identity.as_ref(),
            market_bar_history.as_ref(),
            match provider_binding {
                Some(binding) => PublicationSourceEvidence::Provider(binding),
                None => PublicationSourceEvidence::NoNewRawInput,
            },
        )
    }

    /// Consumes one exclusive provider binding through canonical publication and catalog commit.
    pub async fn ingest_provider_publication(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        input: ProviderPublicationInput,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError> {
        let ProviderPublicationInput {
            sealed_capture,
            revisions,
            company_identity,
            precommit_authority,
        } = input;
        sealed_capture.validate()?;
        let prepared = PreparedProviderCaptureBinding::try_from_live(&sealed_capture)?;
        self.ingest_batch(
            reservation,
            analytical_dataset,
            sealed_capture.batch(),
            Some(revisions),
            Some(&prepared),
            Some(sealed_capture.native_lineage()),
            company_identity,
            cancellation,
            precommit_authority,
        )
        .await
    }

    /// Consumes and validates every input needed by one atomic provider macro-plan publication.
    pub fn prepare_provider_macro_plan_publication(
        &self,
        reservation: IngestReservation,
        input: ProviderMacroPlanPublicationInput,
    ) -> Result<PendingProviderMacroPlanPublication, IngestError> {
        let schema = crate::DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(ArrowConversionError::from)?;
        self.manifests
            .validate_append_schema(&input.analytical_dataset, &schema)?;
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(input.chunks.len())
            .map_err(|_| IngestError::InvalidProviderMacroPlan)?;
        for chunk in &input.chunks {
            prepared.push(PreparedProviderCaptureBinding::try_from_live(
                &chunk.sealed_capture,
            )?);
        }
        {
            let authority = self.lock_authority()?;
            let run = self.validate_run(
                &authority,
                &reservation,
                input.publication_digest,
                Some(&input.source_id),
            )?;
            if run.state() != IngestRunState::Reserved {
                return Err(IngestError::TerminalRun);
            }
        }
        Ok(PendingProviderMacroPlanPublication {
            reservation,
            input,
            prepared_captures: prepared.into_boxed_slice(),
        })
    }

    async fn commit_prepared_provider_macro_plan(
        &self,
        pending: PendingProviderMacroPlanPublication,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Result<ProviderMacroPlanPublicationReceipt, IngestError> {
        precommit_authority.validate_precommit()?;
        let PendingProviderMacroPlanPublication {
            reservation,
            input,
            prepared_captures,
        } = pending;
        let ProviderMacroPlanPublicationInput {
            analytical_dataset,
            completion_digest,
            publication_digest,
            source_id,
            metadata_revision: _,
            provider_dataset,
            request_set_identity,
            source_generation_digest,
            sealed_capture_receipt_digest: _,
            total_rows,
            chunks,
        } = input;
        if chunks.len() != prepared_captures.len() {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let total_chunks =
            u16::try_from(chunks.len()).map_err(|_| IngestError::InvalidProviderMacroPlan)?;
        let revision_deadline = Instant::now()
            .checked_add(REVISION_ASSIGNMENT_DEADLINE)
            .ok_or(IngestError::DeadlineExceeded)?;
        let revision_authority = self.observed_revision_authority();
        let mut canonical_batches = Vec::new();
        canonical_batches
            .try_reserve_exact(chunks.len())
            .map_err(|_| IngestError::InvalidProviderMacroPlan)?;
        for (chunk, prepared) in chunks.into_vec().into_iter().zip(prepared_captures.iter()) {
            if cancellation.is_cancelled() {
                return Err(IngestError::Cancelled);
            }
            let ProviderMacroPlanChunkInput {
                chunk_index: _,
                total_chunks: _,
                candidate_digest: _,
                source_generation_digest: _,
                semantics: _,
                sealed_capture,
                revisions,
            } = chunk;
            let observations =
                ResearchArrowBatch::validated_extraction_observations(sealed_capture.batch())?;
            let observed = revisions
                .into_observed_batch_with_native_lineage(
                    source_id.clone(),
                    sealed_capture.batch(),
                    &observations,
                    sealed_capture.native_lineage(),
                )
                .map_err(map_revision_error)?;
            let assignments = revision_authority
                .assign(observed, revision_deadline, cancellation.clone())
                .await
                .map_err(map_revision_error)?;
            let converted = ResearchArrowBatch::try_from_extraction_batch_with_assigned_revisions_and_provider_binding(
                sealed_capture.batch(),
                assignments.as_slice(),
                prepared,
            )?;
            canonical_batches.push(converted.record_batch().clone());
        }
        let converted = ResearchArrowBatch::try_from_compaction_batches(
            provider_dataset,
            publication_digest,
            canonical_batches,
        )?;
        if u64::try_from(converted.record_batch().num_rows()) != Ok(total_rows) {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let schema = converted.schema_ref().clone();
        let lineage = converted.lineage_digest()?;
        let converted = DatasetArrowBatch::from(converted);
        self.manifests
            .validate_append_schema(&analytical_dataset, &schema)?;
        let dataset_name = SourceIdentifier::try_from(analytical_dataset.as_str())
            .map_err(|_| IngestError::InvalidDataset)?;
        let _operation = self
            .operation_gate
            .acquire(&cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        {
            let authority = self.lock_authority()?;
            let run = self.validate_run(
                &authority,
                &reservation,
                publication_digest,
                Some(&source_id),
            )?;
            if run.state() != IngestRunState::Reserved {
                return Err(IngestError::TerminalRun);
            }
        }
        let publication = self.objects.begin_publication(&cancellation).await?;
        let published = self
            .objects
            .publish_dataset_under_lease(&converted, &cancellation, &publication)
            .await?;
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        let object = ManifestObject::try_new(
            published.content_hash(),
            published.row_count(),
            published.size_bytes(),
            Sha256Digest::new(lineage.bytes()),
        )?;
        if object.row_count() != total_rows {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let plan = self
            .manifests
            .preview_append(analytical_dataset, &schema, object)?;
        let authority = self.lock_authority()?;
        let run = self.validate_run(
            &authority,
            &reservation,
            publication_digest,
            Some(&source_id),
        )?;
        if run.state() != IngestRunState::Reserved {
            return Err(IngestError::TerminalRun);
        }
        precommit_authority.validate_precommit()?;
        let created_at = published.created_at().max(reservation.requested_at());
        let artifact = ArtifactRecord::try_new(
            published.relative_reference(),
            published.content_hash().evidence(),
            published.size_bytes(),
            created_at,
        )?;
        let anchor = DatasetManifestRecord::try_new(
            dataset_name,
            schema.version(),
            artifact.artifact_id(),
            plan.content_hash().evidence(),
            created_at,
        );
        let (manifest, catalog_receipt_digest) = self
            .manifests
            .commit_provider_macro_plan_publication(
                self.catalog_id,
                authority.result_limits(),
                &reservation,
                &plan,
                &artifact,
                &anchor,
                &schema,
                &run,
                &prepared_captures,
                completion_digest,
                publication_digest,
                total_rows,
            )
            .map_err(|error| match error {
                ManifestCatalogError::CatalogAuthority(error) => IngestError::Catalog(error),
                error => IngestError::Manifest(error),
            })?;
        drop(authority);
        let receipt = ProviderMacroPlanPublicationReceipt {
            manifest,
            completion_digest,
            publication_digest,
            catalog_receipt_digest,
            source_id,
            request_set_identity,
            source_generation_digest,
            total_chunks,
            total_rows,
        };
        self.verify_provider_macro_plan_restart(&receipt.restart_selector())?;
        Ok(receipt)
    }

    /// Reopens only the exact generation and whole-plan receipt supplied by the selector.
    pub fn verify_provider_macro_plan_restart(
        &self,
        selector: &ProviderMacroPlanRestartSelector,
    ) -> Result<PinnedDataset, IngestError> {
        if self.manifests.source_id(selector.manifest())? != *selector.source_id() {
            return Err(IngestError::ReplayConflict);
        }
        self.manifests
            .verify_provider_macro_plan_publication(
                selector.manifest(),
                selector.completion_digest(),
                selector.publication_digest(),
                selector.total_chunks(),
                selector.total_rows(),
                selector.catalog_receipt_digest(),
            )
            .map_err(Into::into)
    }

    /// Atomically publishes typed canonical events, their sealed raw evidence, and one generation.
    pub async fn ingest_provider_market_events(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        binding: SealedProviderPublicationBinding,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Result<CommittedDataset, IngestError> {
        precommit_authority.validate_precommit()?;
        let payload_digest = provider_market_event_publication_digest(&binding)?;
        let source_id = provider_market_event_source_id(&binding)?.clone();
        let converted = ProviderMarketEventArrowBatch::try_from_publication(&binding)?;
        let prepared = PreparedProviderPublicationBinding::try_from_live(&binding)?;
        if prepared.publication_digest() != payload_digest {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        let schema = converted.schema_ref().clone();
        let lineage = converted.lineage_digest()?;
        let converted = converted.dataset_batch();
        self.manifests
            .validate_append_schema(&analytical_dataset, &schema)?;
        let _operation = self
            .operation_gate
            .acquire(&cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        {
            let authority = self.lock_authority()?;
            let run =
                self.validate_run(&authority, &reservation, payload_digest, Some(&source_id))?;
            if run.state() == IngestRunState::Failed {
                return Err(IngestError::TerminalRun);
            }
            if run.state() == IngestRunState::Succeeded {
                return self.reconcile_succeeded_provider_event_run(
                    &authority,
                    &reservation,
                    &analytical_dataset,
                    &prepared,
                );
            }
            self.validate_provider_event_binding(&authority, &reservation, &prepared)?;
        }
        let publication = self.objects.begin_publication(&cancellation).await?;
        let published = self
            .objects
            .publish_dataset_under_lease(converted, &cancellation, &publication)
            .await?;
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        let object = ManifestObject::try_new(
            published.content_hash(),
            published.row_count(),
            published.size_bytes(),
            Sha256Digest::new(lineage.bytes()),
        )?;
        let authority = self.lock_authority()?;
        let run = self.validate_run(&authority, &reservation, payload_digest, Some(&source_id))?;
        self.validate_provider_event_binding(&authority, &reservation, &prepared)?;
        if run.state() != IngestRunState::Reserved {
            return Err(IngestError::TerminalRun);
        }
        let plan = self
            .manifests
            .preview_append(analytical_dataset.clone(), &schema, object)?;
        self.commit_plan(
            &authority,
            &reservation,
            &run,
            SourceIdentifier::try_from(analytical_dataset.as_str())
                .map_err(|_| IngestError::InvalidDataset)?,
            schema,
            plan,
            published,
            GenerationKind::Ingest,
            Some(precommit_authority.as_ref()),
            None,
            None,
            PublicationSourceEvidence::ProviderEvent(&prepared),
        )
    }

    /// Atomically publishes one coherent option batch, its sealed raw evidence, and generation.
    pub async fn ingest_provider_option_market(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        binding: SealedProviderOptionMarketBinding,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Result<CommittedDataset, IngestError> {
        precommit_authority.validate_precommit()?;
        let payload_digest = provider_option_market_publication_digest(&binding)?;
        let source_id = binding.batch().scope().source_id().clone();
        let converted = ProviderOptionMarketArrowBatch::try_from_publication(&binding)?;
        let prepared = PreparedProviderOptionMarketBinding::try_from_live(&binding)?;
        if prepared.publication_digest() != payload_digest {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        let schema = converted.schema_ref().clone();
        let lineage = converted.lineage_digest()?;
        let converted = converted.dataset_batch();
        self.manifests
            .validate_append_schema(&analytical_dataset, &schema)?;
        let _operation = self
            .operation_gate
            .acquire(&cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        {
            let authority = self.lock_authority()?;
            let run =
                self.validate_run(&authority, &reservation, payload_digest, Some(&source_id))?;
            if run.state() == IngestRunState::Failed {
                return Err(IngestError::TerminalRun);
            }
            if run.state() == IngestRunState::Succeeded {
                return self.reconcile_succeeded_provider_option_run(
                    &authority,
                    &reservation,
                    &analytical_dataset,
                    &prepared,
                );
            }
            self.validate_provider_option_binding(&authority, &reservation, &prepared)?;
        }
        let publication = self.objects.begin_publication(&cancellation).await?;
        let published = self
            .objects
            .publish_dataset_under_lease(converted, &cancellation, &publication)
            .await?;
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        let object = ManifestObject::try_new(
            published.content_hash(),
            published.row_count(),
            published.size_bytes(),
            Sha256Digest::new(lineage.bytes()),
        )?;
        let authority = self.lock_authority()?;
        let run = self.validate_run(&authority, &reservation, payload_digest, Some(&source_id))?;
        self.validate_provider_option_binding(&authority, &reservation, &prepared)?;
        if run.state() != IngestRunState::Reserved {
            return Err(IngestError::TerminalRun);
        }
        let plan = self
            .manifests
            .preview_append(analytical_dataset.clone(), &schema, object)?;
        self.commit_plan(
            &authority,
            &reservation,
            &run,
            SourceIdentifier::try_from(analytical_dataset.as_str())
                .map_err(|_| IngestError::InvalidDataset)?,
            schema,
            plan,
            published,
            GenerationKind::Ingest,
            Some(precommit_authority.as_ref()),
            None,
            None,
            PublicationSourceEvidence::ProviderOptionMarket(&prepared),
        )
    }

    /// Ingests a locally observed batch while retaining exact caller authority through commit.
    pub async fn ingest_with_precommit_authority(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Result<CommittedDataset, IngestError> {
        self.ingest_batch(
            reservation,
            analytical_dataset,
            &batch,
            None,
            None,
            None,
            None,
            cancellation,
            Some(precommit_authority),
        )
        .await
    }

    /// Ingests explicit local/imported revisions while retaining caller authority through commit.
    ///
    /// Provider captures must use [`Self::ingest_provider_publication`].
    pub async fn ingest_with_revision_plan_and_precommit_authority(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        revisions: ExtractionRevisionPlan,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Result<CommittedDataset, IngestError> {
        self.ingest_batch(
            reservation,
            analytical_dataset,
            &batch,
            Some(revisions),
            None,
            None,
            None,
            cancellation,
            Some(precommit_authority),
        )
        .await
    }

    /// Ingests provider revisions and atomically publishes source-authored company identity.
    pub async fn ingest_with_revision_plan_and_company_identity(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        revisions: ExtractionRevisionPlan,
        company_identity: CompanyIdentityObservation,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError> {
        self.ingest_batch(
            reservation,
            analytical_dataset,
            &batch,
            Some(revisions),
            None,
            None,
            Some(company_identity),
            cancellation,
            None,
        )
        .await
    }

    /// Ingests provider revisions and atomically publishes source-authored company identity.
    #[allow(
        clippy::too_many_arguments,
        reason = "provider revision, identity, cancellation, and publication authority stay explicit"
    )]
    pub async fn ingest_with_revision_plan_company_identity_and_precommit_authority(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        revisions: ExtractionRevisionPlan,
        company_identity: CompanyIdentityObservation,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Result<CommittedDataset, IngestError> {
        self.ingest_batch(
            reservation,
            analytical_dataset,
            &batch,
            Some(revisions),
            None,
            None,
            Some(company_identity),
            cancellation,
            Some(precommit_authority),
        )
        .await
    }

    fn validate_run(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        payload_digest: EvidenceDigest,
        source_id: Option<&market_squawk_domain::SourceId>,
    ) -> Result<crate::IngestRunRecord, IngestError> {
        authority.validate_ingest_reservation(reservation)?;
        let run = authority
            .ingest_run(reservation.run_id())?
            .ok_or(IngestError::UnknownReservation)?;
        if run.payload_digest() != payload_digest
            || source_id.is_some_and(|source| run.source_id() != source)
        {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        if run.operation() != SourceOperation::Persist {
            return Err(IngestError::PersistRightsRequired);
        }
        Ok(run)
    }

    fn validate_provider_binding(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        input: Option<&PreparedProviderCaptureBinding>,
    ) -> Result<(), IngestError> {
        let retained = authority.provider_capture_for_run(reservation.run_id())?;
        match (input, retained) {
            (None, None) => Ok(()),
            (Some(_), None) => Ok(()),
            (Some(input), Some(retained)) if input.evidence == retained => Ok(()),
            (None, Some(_)) | (Some(_), Some(_)) => Err(IngestError::ProviderCaptureRequired),
        }
    }

    fn validate_provider_event_binding(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        input: &PreparedProviderPublicationBinding,
    ) -> Result<(), IngestError> {
        match authority.provider_publication_for_run(reservation.run_id())? {
            None => Ok(()),
            Some(retained) if input.matches_persisted(&retained) => Ok(()),
            Some(_) => Err(IngestError::ProviderCaptureRequired),
        }
    }

    fn reconcile_succeeded_provider_event_run(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        dataset_id: &DatasetId,
        input: &PreparedProviderPublicationBinding,
    ) -> Result<CommittedDataset, IngestError> {
        let existing = self
            .manifests
            .for_run(reservation.run_id())?
            .ok_or(IngestError::IncompleteSuccessfulRun)?;
        let retained = authority
            .provider_publication_for_run(reservation.run_id())?
            .ok_or(IngestError::IncompleteSuccessfulRun)?;
        let generation = self
            .manifests
            .provider_publication_bindings(existing.manifest())?;
        if existing.manifest().dataset_id() != dataset_id
            || !input.matches_persisted(&retained)
            || !generation.iter().any(|(digest, kind)| {
                *digest == retained.publication_digest() && kind == retained.publication_kind()
            })
        {
            return Err(IngestError::ReplayConflict);
        }
        Ok(CommittedDataset::new(existing))
    }

    fn validate_provider_option_binding(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        input: &PreparedProviderOptionMarketBinding,
    ) -> Result<(), IngestError> {
        match authority.provider_option_market_for_run(reservation.run_id())? {
            None => Ok(()),
            Some(retained) if input.matches_persisted(&retained) => Ok(()),
            Some(_) => Err(IngestError::ProviderCaptureRequired),
        }
    }

    fn reconcile_succeeded_provider_option_run(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        dataset_id: &DatasetId,
        input: &PreparedProviderOptionMarketBinding,
    ) -> Result<CommittedDataset, IngestError> {
        let existing = self
            .manifests
            .for_run(reservation.run_id())?
            .ok_or(IngestError::IncompleteSuccessfulRun)?;
        let retained = authority
            .provider_option_market_for_run(reservation.run_id())?
            .ok_or(IngestError::IncompleteSuccessfulRun)?;
        let generation = self
            .manifests
            .provider_publication_bindings(existing.manifest())?;
        if existing.manifest().dataset_id() != dataset_id
            || !input.matches_persisted(&retained)
            || !generation.iter().any(|(digest, kind)| {
                *digest == retained.binding_digest() && kind == retained.publication_kind_name()
            })
        {
            return Err(IngestError::ReplayConflict);
        }
        Ok(CommittedDataset::new(existing))
    }

    fn reconcile_committed_run(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        state: IngestRunState,
        dataset_id: &DatasetId,
        company_identity: Option<&CompanyIdentityObservation>,
        market_bar_history: Option<&MarketBarHistoryPublicationCandidate>,
    ) -> Result<Option<CommittedDataset>, IngestError> {
        let Some(existing) = self.manifests.for_run(reservation.run_id())? else {
            return match state {
                IngestRunState::Reserved => Ok(None),
                IngestRunState::Succeeded => Err(IngestError::IncompleteSuccessfulRun),
                IngestRunState::Failed => Err(IngestError::TerminalRun),
            };
        };
        if existing.manifest().dataset_id() != dataset_id
            || !self
                .manifests
                .market_bar_history_candidate_matches(existing.manifest(), market_bar_history)?
        {
            return Err(IngestError::ReplayConflict);
        }
        match state {
            IngestRunState::Reserved => {
                authority.complete_ingest_with_company_identity(
                    reservation,
                    ContractCompletion::Succeeded,
                    company_identity,
                )?;
            }
            IngestRunState::Succeeded => {
                if let Some(company_identity) = company_identity {
                    authority.reconcile_company_identity(reservation, company_identity)?;
                }
            }
            IngestRunState::Failed => return Err(IngestError::TerminalRun),
        }
        Ok(Some(CommittedDataset::new(existing)))
    }

    fn reconcile_succeeded_provider_run(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        dataset_id: &DatasetId,
        company_identity: Option<&CompanyIdentityObservation>,
    ) -> Result<CommittedDataset, IngestError> {
        let existing = self
            .manifests
            .for_run(reservation.run_id())?
            .ok_or(IngestError::IncompleteSuccessfulRun)?;
        if existing.manifest().dataset_id() != dataset_id {
            return Err(IngestError::ReplayConflict);
        }
        let retained = authority
            .provider_capture_for_run(reservation.run_id())?
            .ok_or(IngestError::IncompleteSuccessfulRun)?;
        let generation_bindings = self
            .manifests
            .provider_capture_binding_digests(existing.manifest())?;
        if !generation_bindings.contains(&retained.binding_digest()) {
            return Err(IngestError::ReplayConflict);
        }
        authority.validate_provider_company_identity_replay(reservation, company_identity)?;
        Ok(CommittedDataset::new(existing))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "recovery compares the exact run, generation, schema, object, and identity evidence"
    )]
    fn reconcile_existing(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        state: IngestRunState,
        dataset_id: &DatasetId,
        schema: &DatasetSchemaRef,
        object: &ManifestObject,
        company_identity: Option<&CompanyIdentityObservation>,
        market_bar_history: Option<&MarketBarHistoryPublicationCandidate>,
    ) -> Result<Option<CommittedDataset>, IngestError> {
        let Some(existing) = self.manifests.for_run(reservation.run_id())? else {
            return match state {
                IngestRunState::Reserved => Ok(None),
                IngestRunState::Succeeded => Err(IngestError::IncompleteSuccessfulRun),
                IngestRunState::Failed => Err(IngestError::TerminalRun),
            };
        };
        if existing.manifest().dataset_id() != dataset_id
            || existing.manifest().schema() != schema
            || existing.plan().objects().last() != Some(object)
            || !self
                .manifests
                .market_bar_history_candidate_matches(existing.manifest(), market_bar_history)?
        {
            return Err(IngestError::ReplayConflict);
        }
        match state {
            IngestRunState::Reserved => {
                authority.complete_ingest_with_company_identity(
                    reservation,
                    ContractCompletion::Succeeded,
                    company_identity,
                )?;
            }
            IngestRunState::Succeeded => {
                if let Some(company_identity) = company_identity {
                    authority.reconcile_company_identity(reservation, company_identity)?;
                }
            }
            IngestRunState::Failed => return Err(IngestError::TerminalRun),
        }
        Ok(Some(CommittedDataset::new(existing)))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "publication keeps each independently verified authority input explicit"
    )]
    fn commit_plan(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        run: &crate::IngestRunRecord,
        dataset_name: SourceIdentifier,
        schema: DatasetSchemaRef,
        plan: ManifestPlan,
        published: PublishedObject,
        kind: GenerationKind,
        precommit_authority: Option<&dyn IngestPrecommitAuthority>,
        company_identity: Option<&CompanyIdentityObservation>,
        market_bar_history: Option<&MarketBarHistoryPublicationCandidate>,
        source_evidence: PublicationSourceEvidence<'_>,
    ) -> Result<CommittedDataset, IngestError> {
        if run.state() != IngestRunState::Reserved {
            return Err(IngestError::TerminalRun);
        }
        if let Some(precommit_authority) = precommit_authority {
            precommit_authority.validate_precommit()?;
        }
        let created_at = published.created_at().max(reservation.requested_at());
        let artifact = ArtifactRecord::try_new(
            published.relative_reference(),
            published.content_hash().evidence(),
            published.size_bytes(),
            created_at,
        )?;
        let anchor = DatasetManifestRecord::try_new(
            dataset_name,
            schema.version(),
            artifact.artifact_id(),
            plan.content_hash().evidence(),
            created_at,
        );
        if matches!(
            source_evidence,
            PublicationSourceEvidence::Provider(_)
                | PublicationSourceEvidence::ProviderEvent(_)
                | PublicationSourceEvidence::ProviderOptionMarket(_)
        ) {
            if kind != GenerationKind::Ingest {
                return Err(IngestError::ProviderCaptureRequired);
            }
            let manifest = self
                .manifests
                .commit_provider_ingest_publication(
                    self.catalog_id,
                    authority.result_limits(),
                    reservation,
                    &plan,
                    &artifact,
                    &anchor,
                    &schema,
                    run,
                    source_evidence,
                    company_identity,
                    market_bar_history,
                )
                .map_err(|error| match error {
                    ManifestCatalogError::CatalogAuthority(error) => IngestError::Catalog(error),
                    error => IngestError::Manifest(error),
                })?;
            return Ok(CommittedDataset::new(self.manifests.pinned(&manifest)?));
        }
        let publication = authority.publish_artifact_manifest_with_source_evidence(
            reservation,
            &artifact,
            &anchor,
            source_evidence,
        )?;
        let manifest = self.manifests.commit_generation(
            &plan,
            publication.artifact(),
            publication.manifest(),
            &schema,
            kind,
            match kind {
                GenerationKind::Ingest => Some(run),
                GenerationKind::Compaction | GenerationKind::Derived => None,
            },
            market_bar_history,
        )?;
        authority.complete_ingest_with_company_identity(
            reservation,
            ContractCompletion::Succeeded,
            company_identity,
        )?;
        Ok(CommittedDataset::new(self.manifests.pinned(&manifest)?))
    }

    fn lock_authority(&self) -> Result<MutexGuard<'_, CatalogAuthority>, IngestError> {
        self.authority
            .lock()
            .map_err(|_| IngestError::AuthorityLockPoisoned)
    }
}

fn remaining_query_limits(
    limits: QueryLimits,
    deadline: tokio::time::Instant,
) -> Result<QueryLimits, QueryError> {
    limits.with_operation_deadline(deadline)
}

fn map_root_authority_catalog_error(error: CatalogError) -> IngestError {
    if matches!(error, CatalogError::ArtifactRootAuthorityMismatch) {
        IngestError::Parquet(ParquetStoreError::RootCatalogMismatch)
    } else {
        IngestError::Catalog(error)
    }
}

fn map_authority_transition_error(error: AuthorityTransitionError) -> IngestError {
    match error {
        AuthorityTransitionError::Catalog(error) => map_root_authority_catalog_error(error),
        AuthorityTransitionError::Root(error) => IngestError::Parquet(error),
        AuthorityTransitionError::Restore(_) => IngestError::AuthorityTransitionRejected,
        AuthorityTransitionError::InvalidIdentity
        | AuthorityTransitionError::TransitionConflict
        | AuthorityTransitionError::LegacyEvidenceMismatch
        | AuthorityTransitionError::NotBound => IngestError::AuthorityTransitionRejected,
    }
}

fn map_revision_error(error: ObservedRevisionError) -> IngestError {
    match error {
        ObservedRevisionError::Cancelled => IngestError::Cancelled,
        ObservedRevisionError::DeadlineExceeded => IngestError::DeadlineExceeded,
        error => IngestError::RevisionAuthority(error),
    }
}

impl ResearchIngestService for AnalyticalDataService {
    async fn ingest(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError> {
        self.ingest_batch(
            reservation,
            analytical_dataset,
            &batch,
            None,
            None,
            None,
            None,
            cancellation,
            None,
        )
        .await
    }

    async fn ingest_with_revision_plan(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        revisions: ExtractionRevisionPlan,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError> {
        self.ingest_batch(
            reservation,
            analytical_dataset,
            &batch,
            Some(revisions),
            None,
            None,
            None,
            cancellation,
            None,
        )
        .await
    }
}

/// Analytical ingestion, publication, reconciliation, or compaction failure.
#[derive(Debug, Error)]
pub enum IngestError {
    /// Exact process-local publication authority was revoked before durable commit.
    #[error("research publication authority was revoked before durable commit")]
    PublicationAuthorityRevoked,
    /// The explicit analytical catalog/root authority transition was rejected.
    #[error("analytical artifact-root authority transition was rejected")]
    AuthorityTransitionRejected,
    /// Canonical observation conversion failed.
    #[error("research observation conversion failed")]
    Arrow(#[from] ArrowConversionError),
    /// Parquet publication, verification, or recovery failed.
    #[error("analytical Parquet object operation failed")]
    Parquet(#[from] ParquetStoreError),
    /// Immutable manifest planning failed.
    #[error("analytical manifest plan is invalid")]
    Plan(#[from] ManifestPlanError),
    /// Immutable manifest persistence failed.
    #[error("analytical manifest catalog operation failed")]
    Manifest(#[from] ManifestCatalogError),
    /// Task 3 rejected a reservation, publication, or transition.
    #[error("analytical catalog authority rejected the operation")]
    Catalog(#[from] CatalogError),
    /// Reference-source registration, publication authority, or bounded read setup failed.
    #[error("listing-reference authority rejected the operation")]
    ListingReference(#[from] ListingReferenceError),
    /// Catalog and manifest capabilities do not identify the same prepared catalog path.
    #[error("analytical service capabilities name different catalogs")]
    CatalogCompositionMismatch,
    /// Exact extraction-batch serialization failed.
    #[error("extraction batch identity serialization failed")]
    Serialization(#[from] serde_json::Error),
    /// Canonical extraction content identity could not be constructed.
    #[error("extraction batch semantic identity construction failed")]
    ContentIdentity(#[source] ExtractionError),
    /// A paged provider extraction omitted or mismatched its exact retained capture authority.
    #[error("paged provider extraction requires its exact verified retained capture")]
    ProviderCaptureRequired,
    /// A complete ordered macro plan failed canonical/native/raw closure validation.
    #[error("provider macro publication plan is invalid")]
    InvalidProviderMacroPlan,
    /// Provider page/frame and sealed-segment receipts could not be bound exactly.
    #[error("provider capture receipt is invalid")]
    ProviderCapture(#[from] ProviderCaptureError),
    /// A sealed provider raw object could not be reopened and verified.
    #[error("sealed provider raw object could not be verified")]
    SealedProviderCapture(#[from] market_squawk_platform::SealedResearchJournalStoreError),
    /// Provider market-event point-in-time selection or restart verification failed.
    #[error("provider market-event point-in-time selection failed")]
    ProviderMarketEventSelection(#[from] crate::ProviderMarketEventSelectionError),
    /// Source-specific revision evidence or durable assignment failed.
    #[error("observed revision assignment failed")]
    RevisionAuthority(#[source] ObservedRevisionError),
    /// A non-local source omitted mandatory provider version and ordering evidence.
    #[error("provider extraction requires explicit revision evidence")]
    RevisionEvidenceRequired,
    /// Revision evidence did not align one-for-one with normalized extraction records.
    #[error("revision evidence does not match the extraction batch")]
    RevisionEvidenceMismatch,
    /// The extraction source was not registered in the retained catalog.
    #[error("analytical ingest source is unknown")]
    UnknownSource,
    /// The reservation does not exist in this authority.
    #[error("analytical ingest reservation is unknown")]
    UnknownReservation,
    /// The reservation's source or exact payload does not match the requested operation.
    #[error("analytical ingest reservation payload does not match")]
    ReservationPayloadMismatch,
    /// Persist rights are mandatory for immutable analytical publication.
    #[error("analytical publication requires admitted persist rights")]
    PersistRightsRequired,
    /// The reservation was already completed unsuccessfully or cannot transition again.
    #[error("analytical ingest run is already terminal")]
    TerminalRun,
    /// A successful run has no complete analytical generation.
    #[error("successful analytical ingest has no immutable generation")]
    IncompleteSuccessfulRun,
    /// A replay differs from the generation already anchored to its run.
    #[error("analytical ingest replay conflicts with its immutable generation")]
    ReplayConflict,
    /// Dataset identity is not valid for immutable storage.
    #[error("analytical dataset identity is invalid")]
    InvalidDataset,
    /// Cancellation was observed before commit.
    #[error("analytical operation was cancelled")]
    Cancelled,
    /// Recovery exceeded its fixed elapsed-time deadline.
    #[error("analytical operation deadline exceeded")]
    DeadlineExceeded,
    /// The process-owned Task 3 authority lock was poisoned.
    #[error("analytical catalog authority is unavailable")]
    AuthorityLockPoisoned,
    /// The bounded blocking worker required for provider recovery was unavailable.
    #[error("provider-capture recovery worker is unavailable")]
    ProviderCaptureRecoveryWorkerUnavailable,
}

fn recover_provider_capture_store_blocking(
    authority: Arc<Mutex<CatalogAuthority>>,
    store: Arc<market_squawk_platform::SealedResearchJournalStore>,
    cancellation: &CancellationToken,
) -> Result<market_squawk_platform::SealedResearchJournalRecoveryReport, IngestError> {
    let control = ProviderCaptureRecoveryControl { cancellation };
    let admission = SealedResearchRecoveryAdmission::try_new(
        MAX_PROVIDER_CAPTURE_PHYSICAL_CLAIMS,
        PROVIDER_CAPTURE_RECOVERY_ENTRY_BUDGET,
    )
    .map_err(map_provider_recovery_store_error)?;
    let mut recovery = store
        .begin_recovery(admission, &control)
        .map_err(map_provider_recovery_store_error)?;
    let authority = lock_provider_recovery_authority(&authority, &control)?;
    let mut after = None;
    let mut observed = 0usize;
    let mut observed_bytes = 0u64;
    loop {
        control
            .checkpoint(ResearchObjectControlPoint::BeforeRecoveryClaim {
                observed_claims: observed,
            })
            .map_err(|error| {
                map_provider_recovery_store_error(SealedResearchJournalStoreError::ObjectControl(
                    error,
                ))
            })?;
        let page = authority.authoritative_provider_raw_claim_page(after)?;
        if page.is_empty() {
            break;
        }
        for (digest, claim) in page {
            if digest.algorithm() != DigestAlgorithm::Sha256
                || after.is_some_and(|prior| digest.bytes() <= prior.bytes())
            {
                return Err(IngestError::Catalog(CatalogError::CorruptCatalog));
            }
            observed = observed
                .checked_add(1)
                .ok_or(IngestError::ProviderCaptureRequired)?;
            if observed > MAX_PROVIDER_CAPTURE_PHYSICAL_CLAIMS {
                return Err(IngestError::ProviderCaptureRequired);
            }
            observed_bytes = observed_bytes
                .checked_add(match &claim {
                    SealedResearchRawClaim::JournalSegment(claim) => claim.size_bytes(),
                    SealedResearchRawClaim::LogicalObject(claim) => claim.size_bytes(),
                })
                .ok_or(IngestError::ProviderCaptureRequired)?;
            if observed_bytes > MAX_PROVIDER_CAPTURE_PHYSICAL_BYTES {
                return Err(IngestError::Catalog(CatalogError::CorruptCatalog));
            }
            recovery
                .observe_claim(&claim)
                .map_err(map_provider_recovery_store_error)?;
            after = Some(digest);
        }
    }
    drop(authority);
    recovery.finish().map_err(map_provider_recovery_store_error)
}

fn lock_provider_recovery_authority<'a>(
    authority: &'a Mutex<CatalogAuthority>,
    control: &ProviderCaptureRecoveryControl<'_>,
) -> Result<MutexGuard<'a, CatalogAuthority>, IngestError> {
    let mut blocked_attempts = 0usize;
    loop {
        if control.cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        match authority.try_lock() {
            Ok(authority) => return Ok(authority),
            Err(TryLockError::Poisoned(_)) => return Err(IngestError::AuthorityLockPoisoned),
            Err(TryLockError::WouldBlock) => {
                blocked_attempts = blocked_attempts
                    .checked_add(1)
                    .ok_or(IngestError::ProviderCaptureRecoveryWorkerUnavailable)?;
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

fn map_provider_recovery_admission_error(error: BlockingIoAdmissionError) -> IngestError {
    match error {
        BlockingIoAdmissionError::Cancelled => IngestError::Cancelled,
        BlockingIoAdmissionError::Saturated | BlockingIoAdmissionError::ReaperUnavailable => {
            IngestError::ProviderCaptureRecoveryWorkerUnavailable
        }
    }
}

fn map_provider_recovery_store_error(error: SealedResearchJournalStoreError) -> IngestError {
    match error {
        SealedResearchJournalStoreError::ObjectControl(ResearchObjectControlError::Cancelled) => {
            IngestError::Cancelled
        }
        SealedResearchJournalStoreError::ObjectControl(
            ResearchObjectControlError::DeadlineExceeded,
        ) => IngestError::DeadlineExceeded,
        error => IngestError::SealedProviderCapture(error),
    }
}

#[cfg(test)]
mod tests;
