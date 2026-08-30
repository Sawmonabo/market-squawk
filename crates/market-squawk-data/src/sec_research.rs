//! Exact-origin SEC research reads and generation-completion-aware point-in-time selection.

use std::fmt;
use std::mem::size_of;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use market_squawk_domain::{
    CommonEquitySuitability, CompanyIdentitySurface, DigestAlgorithm, EvidenceDigest, InstrumentId,
    ResearchContext, ResearchObservation, ResearchTemporalCoordinate, SourceId, Timestamp,
};
use market_squawk_platform::{
    ResearchObjectControl, ResearchObjectControlError, ResearchObjectControlPoint,
    SealedResearchJournalStore, SealedResearchJournalStoreError,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::arrow_convert::{ProviderCaptureRowCoordinate, ResearchLineageDigestAccumulator};
use crate::catalog::{
    CompanyIdentityExactRecord, CompanySecurityIdentityDisposition,
    CompanySecurityIdentityReadCapability, CompanySecurityIdentitySelection,
};
use crate::{
    AnalyticalManifestCatalog, ArrowConversionError, CatalogAuthority, CatalogError,
    DatasetManifestRef, ManifestCatalogError, ParquetObjectStore, ParquetStoreError,
    PointInTimeCandidate, PointInTimeError, PointInTimeExclusionReason,
    PointInTimeExclusionReasons, PointInTimeLimits, PointInTimePolicy, PointInTimeRequest,
    PointInTimeRevisionMode, PointInTimeRevisionState, PointInTimeService, Sha256Digest,
};

const SEC_SOURCE_ID: &str = "sec-edgar";
const SEC_NATIVE_LINEAGE_IMPLEMENTATION: &str = "sec_edgar_v1";
const SEC_WHOLE_CAPTURE_SCOPE: &str = "whole";
const SEC_WHOLE_CAPTURE_LAYOUT: &str = "whole_single_segment";
const SEC_RESEARCH_REQUEST_DOMAIN: &[u8] = b"market-squawk/sec-research-read-request/v1\0";
const SEC_RESEARCH_ORIGIN_DOMAIN: &[u8] = b"market-squawk/sec-research-origin/v1\0";
const SEC_RESEARCH_SELECTION_DOMAIN: &[u8] = b"market-squawk/sec-research-selection/v1\0";
const SEC_RESEARCH_RESULT_DOMAIN: &[u8] = b"market-squawk/sec-research-result/v1\0";

/// Fixed process ceiling for one exact SEC Parquet object read.
pub const MAX_SEC_RESEARCH_OBJECT_BYTES: usize = 512 * 1024 * 1024;

/// Closed SEC research family retained by one exact provider publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecResearchFamily {
    /// SEC submissions normalized as filing observations.
    Submissions,
    /// SEC Company Facts normalized as fundamental observations.
    CompanyFacts,
    /// One accession/document/taxonomy-bound filing XBRL generation.
    FilingXbrl,
}

impl SecResearchFamily {
    const fn company_surface(self) -> CompanyIdentitySurface {
        match self {
            Self::Submissions => CompanyIdentitySurface::SecSubmissions,
            Self::CompanyFacts => CompanyIdentitySurface::SecCompanyFacts,
            Self::FilingXbrl => CompanyIdentitySurface::SecSubmissions,
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::Submissions => 1,
            Self::CompanyFacts => 2,
            Self::FilingXbrl => 3,
        }
    }

    const fn accepts(self, observation: &ResearchObservation) -> bool {
        matches!(
            (self, observation),
            (Self::Submissions, ResearchObservation::Filing(_))
                | (Self::CompanyFacts, ResearchObservation::Fundamental(_))
                | (Self::FilingXbrl, ResearchObservation::Fundamental(_))
        )
    }
}

/// Complete bounded request for one exact SEC generation and provider binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecResearchReadRequest {
    manifest: DatasetManifestRef,
    family: SecResearchFamily,
    provider_binding_digest: EvidenceDigest,
    company_observation_digest: EvidenceDigest,
    knowledge_at: Timestamp,
    effective_cutoff: ResearchTemporalCoordinate,
    revision_mode: PointInTimeRevisionMode,
    point_in_time_limits: PointInTimeLimits,
    maximum_object_bytes: usize,
    request_digest: EvidenceDigest,
}

/// Caller-safe SEC research request rooted only in canonical identity, family, and PIT policy.
///
/// Immutable manifests, provider bindings, and company-observation digests are intentionally
/// absent. The data authority derives those coordinates from already admitted catalog evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecResearchIdentityReadRequest {
    instrument_id: InstrumentId,
    family: SecResearchFamily,
    knowledge_at: Timestamp,
    effective_cutoff: ResearchTemporalCoordinate,
    revision_mode: PointInTimeRevisionMode,
    point_in_time_limits: PointInTimeLimits,
    maximum_object_bytes: usize,
}

impl SecResearchIdentityReadRequest {
    /// Constructs one bounded canonical-identity request without forgeable storage coordinates.
    pub fn try_new(
        instrument_id: InstrumentId,
        family: SecResearchFamily,
        knowledge_at: Timestamp,
        effective_cutoff: ResearchTemporalCoordinate,
        revision_mode: PointInTimeRevisionMode,
        point_in_time_limits: PointInTimeLimits,
        maximum_object_bytes: usize,
    ) -> Result<Self, SecResearchReadError> {
        if maximum_object_bytes == 0 || maximum_object_bytes > MAX_SEC_RESEARCH_OBJECT_BYTES {
            return Err(SecResearchReadError::InvalidRequest);
        }
        Ok(Self {
            instrument_id,
            family,
            knowledge_at,
            effective_cutoff,
            revision_mode,
            point_in_time_limits,
            maximum_object_bytes,
        })
    }

    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub const fn family(&self) -> SecResearchFamily {
        self.family
    }
    pub const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }
    pub const fn effective_cutoff(&self) -> &ResearchTemporalCoordinate {
        &self.effective_cutoff
    }
    pub const fn revision_mode(&self) -> PointInTimeRevisionMode {
        self.revision_mode
    }
    pub const fn point_in_time_limits(&self) -> PointInTimeLimits {
        self.point_in_time_limits
    }
    pub const fn maximum_object_bytes(&self) -> usize {
        self.maximum_object_bytes
    }
}

/// Truthful canonical-identity resolution before an exact SEC generation is opened.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecResearchIdentityOutcome {
    /// No admitted issuer/security relationship is usable at the cutoff.
    Missing,
    /// More than one issuer relationship or an ambiguous company parent remains possible.
    Ambiguous,
    /// The retained relationship names a superseded company or market-definition parent.
    Stale,
    /// The latest applicable relationship explicitly revoked the mapping.
    Revoked,
    /// One exact retained generation was resolved and fully selected.
    Exact(SecResearchSelection),
}

/// Complete canonical-identity selection with the relationship evidence considered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecResearchIdentitySelection {
    request: SecResearchIdentityReadRequest,
    identity: CompanySecurityIdentitySelection,
    outcome: SecResearchIdentityOutcome,
}

impl SecResearchIdentitySelection {
    pub const fn request(&self) -> &SecResearchIdentityReadRequest {
        &self.request
    }
    pub const fn identity(&self) -> &CompanySecurityIdentitySelection {
        &self.identity
    }
    pub const fn outcome(&self) -> &SecResearchIdentityOutcome {
        &self.outcome
    }
}

impl SecResearchReadRequest {
    /// Constructs a request that names every immutable authority and every execution bound.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact manifest, provider/company generations, clocks, policy, and bounds remain explicit"
    )]
    pub fn try_new(
        manifest: DatasetManifestRef,
        family: SecResearchFamily,
        provider_binding_digest: EvidenceDigest,
        company_observation_digest: EvidenceDigest,
        knowledge_at: Timestamp,
        effective_cutoff: ResearchTemporalCoordinate,
        revision_mode: PointInTimeRevisionMode,
        point_in_time_limits: PointInTimeLimits,
        maximum_object_bytes: usize,
    ) -> Result<Self, SecResearchReadError> {
        if !valid_sha256(provider_binding_digest)
            || !valid_sha256(company_observation_digest)
            || maximum_object_bytes == 0
            || maximum_object_bytes > MAX_SEC_RESEARCH_OBJECT_BYTES
        {
            return Err(SecResearchReadError::InvalidRequest);
        }
        let mut request = Self {
            manifest,
            family,
            provider_binding_digest,
            company_observation_digest,
            knowledge_at,
            effective_cutoff,
            revision_mode,
            point_in_time_limits,
            maximum_object_bytes,
            request_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        request.request_digest = request_digest(&request)?;
        Ok(request)
    }

    /// Returns the exact immutable analytical generation.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    /// Returns the closed SEC observation family.
    pub const fn family(&self) -> SecResearchFamily {
        self.family
    }
    /// Returns the exact provider-capture binding selected from the generation.
    pub const fn provider_binding_digest(&self) -> EvidenceDigest {
        self.provider_binding_digest
    }
    /// Returns the exact retained company-observation digest.
    pub const fn company_observation_digest(&self) -> EvidenceDigest {
        self.company_observation_digest
    }
    /// Returns the latest knowledge admitted by the selection.
    pub const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }
    /// Returns the precision-preserving economic cutoff.
    pub const fn effective_cutoff(&self) -> &ResearchTemporalCoordinate {
        &self.effective_cutoff
    }
    /// Returns the explicit revision-history policy.
    pub const fn revision_mode(&self) -> PointInTimeRevisionMode {
        self.revision_mode
    }
    /// Returns the complete point-in-time selector bounds.
    pub const fn point_in_time_limits(&self) -> PointInTimeLimits {
        self.point_in_time_limits
    }
    /// Returns the exact retained-memory bound for the origin object read.
    pub const fn maximum_object_bytes(&self) -> usize {
        self.maximum_object_bytes
    }
    /// Returns the digest binding the immutable inputs, clocks, policy, and bounds.
    pub const fn request_digest(&self) -> EvidenceDigest {
        self.request_digest
    }
}

/// Exact durable run/artifact/object coordinates reconstructed after restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecResearchOrigin {
    manifest: DatasetManifestRef,
    source_id: SourceId,
    run_id: Uuid,
    control_manifest_id: Uuid,
    artifact_id: Uuid,
    object_ordinal: usize,
    relative_reference: Box<str>,
    object_content_digest: EvidenceDigest,
    object_lineage_digest: EvidenceDigest,
    object_row_count: u64,
    object_size_bytes: u64,
    generation_completed_at: Timestamp,
    origin_digest: EvidenceDigest,
}

impl SecResearchOrigin {
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    pub const fn run_id(&self) -> Uuid {
        self.run_id
    }
    pub const fn control_manifest_id(&self) -> Uuid {
        self.control_manifest_id
    }
    pub const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }
    pub const fn object_ordinal(&self) -> usize {
        self.object_ordinal
    }
    pub fn relative_reference(&self) -> &str {
        &self.relative_reference
    }
    pub const fn object_content_digest(&self) -> EvidenceDigest {
        self.object_content_digest
    }
    pub const fn object_lineage_digest(&self) -> EvidenceDigest {
        self.object_lineage_digest
    }
    pub const fn object_row_count(&self) -> u64 {
        self.object_row_count
    }
    pub const fn object_size_bytes(&self) -> u64 {
        self.object_size_bytes
    }
    pub const fn generation_completed_at(&self) -> Timestamp {
        self.generation_completed_at
    }
    pub const fn origin_digest(&self) -> EvidenceDigest {
        self.origin_digest
    }
}

/// Exact canonical and provider-native identity of one decoded object row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecResearchRowIdentity {
    row_ordinal: u32,
    canonical_row_digest: EvidenceDigest,
    observation_digest: EvidenceDigest,
}

impl SecResearchRowIdentity {
    pub const fn row_ordinal(self) -> u32 {
        self.row_ordinal
    }
    pub const fn canonical_row_digest(self) -> EvidenceDigest {
        self.canonical_row_digest
    }
    pub const fn observation_digest(self) -> EvidenceDigest {
        self.observation_digest
    }
}

/// Canonical identities emitted by the common point-in-time selector.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecResearchPointInTimeIdentities {
    family_identity: Sha256Digest,
    payload_identity: Sha256Digest,
    provenance_identity: Sha256Digest,
    evidence_identity: Sha256Digest,
    revision_state: PointInTimeRevisionState,
}

impl SecResearchPointInTimeIdentities {
    pub const fn family_identity(self) -> Sha256Digest {
        self.family_identity
    }
    pub const fn payload_identity(self) -> Sha256Digest {
        self.payload_identity
    }
    pub const fn provenance_identity(self) -> Sha256Digest {
        self.provenance_identity
    }
    pub const fn evidence_identity(self) -> Sha256Digest {
        self.evidence_identity
    }
    pub const fn revision_state(self) -> PointInTimeRevisionState {
        self.revision_state
    }
}

/// Knowledge-clock exclusions enforced before the generic point-in-time kernel.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SecResearchKnowledgeExclusions {
    available_after_cutoff: bool,
    received_after_cutoff: bool,
    ingested_after_cutoff: bool,
    generation_completed_after_cutoff: bool,
    company_identity_not_knowable: bool,
}

impl SecResearchKnowledgeExclusions {
    pub const fn available_after_cutoff(self) -> bool {
        self.available_after_cutoff
    }
    pub const fn received_after_cutoff(self) -> bool {
        self.received_after_cutoff
    }
    pub const fn ingested_after_cutoff(self) -> bool {
        self.ingested_after_cutoff
    }
    pub const fn generation_completed_after_cutoff(self) -> bool {
        self.generation_completed_after_cutoff
    }
    pub const fn company_identity_not_knowable(self) -> bool {
        self.company_identity_not_knowable
    }
    pub const fn is_empty(self) -> bool {
        !self.available_after_cutoff
            && !self.received_after_cutoff
            && !self.ingested_after_cutoff
            && !self.generation_completed_after_cutoff
            && !self.company_identity_not_knowable
    }
}

/// One selected typed observation and all canonical selector identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecResearchSelectedRow {
    row: SecResearchRowIdentity,
    point_in_time: SecResearchPointInTimeIdentities,
}

impl SecResearchSelectedRow {
    pub const fn row(&self) -> SecResearchRowIdentity {
        self.row
    }
    pub const fn point_in_time(&self) -> SecResearchPointInTimeIdentities {
        self.point_in_time
    }
}

/// One completely receipted exclusion from the exact decoded object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecResearchExcludedRow {
    row: SecResearchRowIdentity,
    knowledge: SecResearchKnowledgeExclusions,
    point_in_time_reasons: Option<PointInTimeExclusionReasons>,
    point_in_time: Option<SecResearchPointInTimeIdentities>,
}

impl SecResearchExcludedRow {
    pub const fn row(&self) -> SecResearchRowIdentity {
        self.row
    }
    pub const fn knowledge(&self) -> SecResearchKnowledgeExclusions {
        self.knowledge
    }
    pub const fn point_in_time_reasons(&self) -> Option<PointInTimeExclusionReasons> {
        self.point_in_time_reasons
    }
    pub const fn point_in_time(&self) -> Option<SecResearchPointInTimeIdentities> {
        self.point_in_time
    }
}

/// One divergent same-family/same-revision conflict group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecResearchConflict {
    family_identity: Sha256Digest,
    revision: market_squawk_domain::RevisionNumber,
    rows: Box<[(SecResearchRowIdentity, SecResearchPointInTimeIdentities)]>,
}

impl SecResearchConflict {
    pub const fn family_identity(&self) -> Sha256Digest {
        self.family_identity
    }
    pub const fn revision(&self) -> market_squawk_domain::RevisionNumber {
        self.revision
    }
    pub fn rows(&self) -> &[(SecResearchRowIdentity, SecResearchPointInTimeIdentities)] {
        &self.rows
    }
}

/// Closed outcome of the exact SEC point-in-time selector.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecResearchDisposition {
    Selected,
    Unavailable,
    Conflict,
}

/// Digests binding the request, origin, exact evidence, and materialized selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecResearchSelectionReceipt {
    request_digest: EvidenceDigest,
    origin_digest: EvidenceDigest,
    provider_binding_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    row_mapping_digest: EvidenceDigest,
    company_observation_digest: EvidenceDigest,
    point_in_time_content_identity: Option<Sha256Digest>,
    point_in_time_audit_identity: Sha256Digest,
    selection_digest: EvidenceDigest,
    result_digest: EvidenceDigest,
}

impl SecResearchSelectionReceipt {
    pub const fn request_digest(self) -> EvidenceDigest {
        self.request_digest
    }
    pub const fn origin_digest(self) -> EvidenceDigest {
        self.origin_digest
    }
    pub const fn provider_binding_digest(self) -> EvidenceDigest {
        self.provider_binding_digest
    }
    pub const fn capture_observation_digest(self) -> EvidenceDigest {
        self.capture_observation_digest
    }
    pub const fn row_mapping_digest(self) -> EvidenceDigest {
        self.row_mapping_digest
    }
    pub const fn company_observation_digest(self) -> EvidenceDigest {
        self.company_observation_digest
    }
    pub const fn point_in_time_content_identity(self) -> Option<Sha256Digest> {
        self.point_in_time_content_identity
    }
    pub const fn point_in_time_audit_identity(self) -> Sha256Digest {
        self.point_in_time_audit_identity
    }
    pub const fn selection_digest(self) -> EvidenceDigest {
        self.selection_digest
    }
    pub const fn result_digest(self) -> EvidenceDigest {
        self.result_digest
    }
}

/// Exact-origin decoded SEC rows plus their fail-closed PIT result and restart receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecResearchSelection {
    request: SecResearchReadRequest,
    origin: SecResearchOrigin,
    company_identity: CompanyIdentityExactRecord,
    decoded_rows: Box<[ResearchObservation]>,
    disposition: SecResearchDisposition,
    selected: Box<[SecResearchSelectedRow]>,
    exclusions: Box<[SecResearchExcludedRow]>,
    conflicts: Box<[SecResearchConflict]>,
    receipt: SecResearchSelectionReceipt,
}

impl SecResearchSelection {
    pub const fn request(&self) -> &SecResearchReadRequest {
        &self.request
    }
    pub const fn origin(&self) -> &SecResearchOrigin {
        &self.origin
    }
    pub const fn company_identity(&self) -> &CompanyIdentityExactRecord {
        &self.company_identity
    }
    pub fn decoded_rows(&self) -> &[ResearchObservation] {
        &self.decoded_rows
    }
    pub const fn disposition(&self) -> SecResearchDisposition {
        self.disposition
    }
    pub fn selected(&self) -> &[SecResearchSelectedRow] {
        &self.selected
    }
    pub fn exclusions(&self) -> &[SecResearchExcludedRow] {
        &self.exclusions
    }
    pub fn conflicts(&self) -> &[SecResearchConflict] {
        &self.conflicts
    }
    pub const fn receipt(&self) -> SecResearchSelectionReceipt {
        self.receipt
    }
}

/// Cloneable least-authority exact SEC research reader.
#[derive(Clone)]
pub struct SecResearchReadCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
    manifests: Arc<AnalyticalManifestCatalog>,
    objects: Arc<ParquetObjectStore>,
}

impl fmt::Debug for SecResearchReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecResearchReadCapability")
            .field("authority", &"[SEALED SEC READ AUTHORITY]")
            .field("manifests", &"[IMMUTABLE MANIFEST AUTHORITY]")
            .field("objects", &"[CONTROLLED OBJECT AUTHORITY]")
            .finish()
    }
}

struct SecResearchOperationControl<'operation> {
    deadline: Instant,
    cancellation: &'operation CancellationToken,
}

impl ResearchObjectControl for SecResearchOperationControl<'_> {
    fn checkpoint(
        &self,
        _point: ResearchObjectControlPoint,
    ) -> Result<(), ResearchObjectControlError> {
        if self.cancellation.is_cancelled() {
            Err(ResearchObjectControlError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(ResearchObjectControlError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}

impl SecResearchReadCapability {
    pub(crate) fn new(
        authority: Arc<Mutex<CatalogAuthority>>,
        manifests: Arc<AnalyticalManifestCatalog>,
        objects: Arc<ParquetObjectStore>,
    ) -> Self {
        Self {
            authority,
            manifests,
            objects,
        }
    }

    /// Resolves canonical security identity to one exact SEC generation, then performs the read.
    ///
    /// Missing, ambiguous, stale, and revoked issuer mappings remain explicit outcomes. Only an
    /// exact direct common-equity relationship can supply the privately derived company, provider
    /// binding, and immutable manifest coordinates consumed by [`Self::select`].
    pub async fn select_by_identity(
        &self,
        request: SecResearchIdentityReadRequest,
        raw_store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecResearchIdentitySelection, SecResearchReadError> {
        check_operation(deadline, &cancellation)?;
        let source_id =
            SourceId::try_from(SEC_SOURCE_ID).map_err(|_| SecResearchReadError::InvalidRequest)?;
        let identity = CompanySecurityIdentityReadCapability::new(Arc::clone(&self.authority))
            .instrument_company_as_of(
                request.instrument_id(),
                &source_id,
                request.family().company_surface(),
                request.knowledge_at(),
                CommonEquitySuitability::SuitableIssuerCommonEquity,
                deadline,
                &cancellation,
            )?;
        let closed = match identity.disposition() {
            CompanySecurityIdentityDisposition::Unavailable => {
                Some(SecResearchIdentityOutcome::Missing)
            }
            CompanySecurityIdentityDisposition::Conflict => {
                Some(SecResearchIdentityOutcome::Ambiguous)
            }
            CompanySecurityIdentityDisposition::Stale => Some(SecResearchIdentityOutcome::Stale),
            CompanySecurityIdentityDisposition::Revoked => {
                Some(SecResearchIdentityOutcome::Revoked)
            }
            CompanySecurityIdentityDisposition::Complete => None,
        };
        if let Some(outcome) = closed {
            return Ok(SecResearchIdentitySelection {
                request,
                identity,
                outcome,
            });
        }

        let [relationship] = identity.candidates() else {
            return Err(SecResearchReadError::OriginMismatch);
        };
        let link = relationship.link();
        if link.instrument_id() != request.instrument_id()
            || link.company_source_id() != &source_id
            || link.company_surface() != request.family().company_surface()
            || link.common_equity_suitability()
                != CommonEquitySuitability::SuitableIssuerCommonEquity
        {
            return Err(SecResearchReadError::OriginMismatch);
        }
        let company = self
            .authority
            .try_lock()
            .map_err(|_| SecResearchReadError::AuthorityUnavailable)?
            .catalog()
            .exact_company_identity_by_digest(
                link.company_observation_digest(),
                deadline,
                &cancellation,
            )?
            .ok_or(SecResearchReadError::OriginMismatch)?;
        if company.observation().source_id() != &source_id
            || company.observation().surface() != request.family().company_surface()
            || company.observation().provider_company_id() != link.provider_company_id()
            || company.observation_digest() != link.company_observation_digest()
        {
            return Err(SecResearchReadError::OriginMismatch);
        }
        let provider_binding_digest = company
            .provider_binding_digest()
            .ok_or(SecResearchReadError::ProviderBindingMismatch)?;
        let pinned = self
            .manifests
            .for_run(company.run_id())?
            .ok_or(SecResearchReadError::OriginMismatch)?;
        if pinned.plan().content_hash().evidence() != company.manifest_content_digest()
            || pinned
                .objects()
                .iter()
                .filter(|object| object.artifact_id() == company.artifact_id())
                .count()
                != 1
        {
            return Err(SecResearchReadError::OriginMismatch);
        }
        let exact_request = SecResearchReadRequest::try_new(
            pinned.manifest().clone(),
            request.family(),
            provider_binding_digest,
            company.observation_digest(),
            request.knowledge_at(),
            request.effective_cutoff().clone(),
            request.revision_mode(),
            request.point_in_time_limits(),
            request.maximum_object_bytes(),
        )?;
        let selected = self
            .select(exact_request, raw_store, deadline, cancellation)
            .await?;
        if selected.company_identity() != &company {
            return Err(SecResearchReadError::OriginMismatch);
        }
        Ok(SecResearchIdentitySelection {
            request,
            identity,
            outcome: SecResearchIdentityOutcome::Exact(selected),
        })
    }

    /// Re-resolves canonical identity and requires the same closed outcome and exact evidence.
    pub async fn verify_identity_restart(
        &self,
        expected: &SecResearchIdentitySelection,
        raw_store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecResearchIdentitySelection, SecResearchReadError> {
        let replay = self
            .select_by_identity(
                expected.request().clone(),
                raw_store,
                deadline,
                cancellation,
            )
            .await?;
        if replay != *expected {
            return Err(SecResearchReadError::RestartMismatch);
        }
        Ok(replay)
    }

    /// Reconstructs and selects one exact SEC generation from durable authorities only.
    pub async fn select(
        &self,
        request: SecResearchReadRequest,
        raw_store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecResearchSelection, SecResearchReadError> {
        check_operation(deadline, &cancellation)?;
        let (pinned, source_id, python_export) =
            self.manifests
                .read_exact(request.manifest(), deadline, &cancellation)?;
        if python_export.is_some()
            || source_id.as_str() != SEC_SOURCE_ID
            || pinned.manifest() != request.manifest()
        {
            return Err(SecResearchReadError::OriginMismatch);
        }

        let company_identity = self
            .authority
            .try_lock()
            .map_err(|_| SecResearchReadError::AuthorityUnavailable)?
            .catalog()
            .exact_company_identity_by_digest(
                request.company_observation_digest(),
                deadline,
                &cancellation,
            )?
            .ok_or(SecResearchReadError::OriginMismatch)?;
        if company_identity.observation().source_id() != &source_id
            || company_identity.observation().surface() != request.family().company_surface()
            || !valid_sec_cik(
                company_identity
                    .observation()
                    .provider_company_id()
                    .as_str(),
            )
            || company_identity.provider_binding_digest() != Some(request.provider_binding_digest())
            || company_identity.manifest_content_digest() != pinned.plan().content_hash().evidence()
        {
            return Err(SecResearchReadError::OriginMismatch);
        }

        let retained_bindings = self
            .manifests
            .provider_capture_binding_digests(request.manifest())?;
        if !retained_bindings.contains(&request.provider_binding_digest()) {
            return Err(SecResearchReadError::ProviderBindingMismatch);
        }
        let binding = self
            .authority
            .try_lock()
            .map_err(|_| SecResearchReadError::AuthorityUnavailable)?
            .provider_capture_binding_evidence(request.provider_binding_digest())?
            .ok_or(SecResearchReadError::ProviderBindingMismatch)?;
        binding.verify_integrity()?;
        if binding.binding_digest() != request.provider_binding_digest()
            || binding.capture().source_id() != &source_id
            || binding.native_lineage().implementation() != SEC_NATIVE_LINEAGE_IMPLEMENTATION
            || binding.scope() != SEC_WHOLE_CAPTURE_SCOPE
            || binding.layout() != SEC_WHOLE_CAPTURE_LAYOUT
        {
            return Err(SecResearchReadError::ProviderBindingMismatch);
        }
        let operation_control = SecResearchOperationControl {
            deadline,
            cancellation: &cancellation,
        };
        for physical in binding.physical_claims() {
            check_operation(deadline, &cancellation)?;
            if usize::try_from(physical.claim().size_bytes())
                .ok()
                .is_none_or(|bytes| bytes > request.maximum_object_bytes())
            {
                return Err(SecResearchReadError::ObjectBudgetExceeded);
            }
            let verified = raw_store
                .open_verified_claim_with_control(physical.claim(), &operation_control)
                .map_err(map_raw_store_error)?;
            if verified.receipt().claim() != physical.claim() {
                return Err(SecResearchReadError::ProviderBindingMismatch);
            }
            drop(verified);
            check_operation(deadline, &cancellation)?;
        }

        let object_ordinal = exact_origin_object_ordinal(&pinned, &company_identity)?;
        let pinned_object = pinned
            .objects()
            .get(object_ordinal)
            .ok_or(SecResearchReadError::OriginMismatch)?;
        if usize::try_from(pinned_object.object().row_count()).ok() != Some(binding.record_count())
            || binding.record_count() > request.point_in_time_limits().max_candidates()
        {
            return Err(SecResearchReadError::OriginMismatch);
        }
        let batches = read_pinned_object_before_deadline(
            &self.objects,
            &pinned,
            company_identity.artifact_id(),
            object_ordinal,
            request.point_in_time_limits().max_candidates(),
            request.maximum_object_bytes(),
            deadline,
            &cancellation,
        )
        .await?;
        check_operation(deadline, &cancellation)?;
        let (observations, coordinates, decoded_retained_bytes, observation_dynamic_bytes, lineage) =
            decode_exact_object_batches(
                batches,
                binding.record_count(),
                pinned.manifest().schema(),
                request.maximum_object_bytes(),
                deadline,
                &cancellation,
                &operation_control,
            )?;
        if lineage.bytes() != pinned_object.object().lineage_digest().bytes() {
            return Err(SecResearchReadError::OriginMismatch);
        }
        validate_rows(
            request.family(),
            &source_id,
            &coordinates,
            &observations,
            &binding,
            deadline,
            &cancellation,
        )?;

        let mut origin = SecResearchOrigin {
            manifest: request.manifest().clone(),
            source_id,
            run_id: company_identity.run_id(),
            control_manifest_id: company_identity.manifest_id(),
            artifact_id: company_identity.artifact_id(),
            object_ordinal,
            relative_reference: company_identity
                .artifact_relative_reference()
                .to_owned()
                .into_boxed_str(),
            object_content_digest: pinned_object.object().content_hash().evidence(),
            object_lineage_digest: pinned_object.object().lineage_digest().evidence(),
            object_row_count: pinned_object.object().row_count(),
            object_size_bytes: pinned_object.object().size_bytes(),
            generation_completed_at: company_identity.completed_at(),
            origin_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        origin.origin_digest = origin_digest(&origin);

        materialize_selection(
            request,
            origin,
            company_identity,
            binding.capture().observation_digest(),
            binding.row_mapping_digest(),
            observations,
            coordinates,
            decoded_retained_bytes,
            observation_dynamic_bytes,
            deadline,
            &cancellation,
        )
        .await
    }

    /// Reopens a prior result and requires byte-identical typed evidence after a fresh process.
    pub async fn verify_restart(
        &self,
        expected: &SecResearchSelection,
        raw_store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecResearchSelection, SecResearchReadError> {
        let replay = self
            .select(
                expected.request().clone(),
                raw_store,
                deadline,
                cancellation,
            )
            .await?;
        if replay != *expected {
            return Err(SecResearchReadError::RestartMismatch);
        }
        Ok(replay)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the exact pinned object coordinates, bounds, and operation controls remain explicit"
)]
async fn read_pinned_object_before_deadline(
    objects: &ParquetObjectStore,
    pinned: &crate::PinnedDataset,
    artifact_id: Uuid,
    object_ordinal: usize,
    maximum_rows: usize,
    maximum_bytes: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<RecordBatch>, SecResearchReadError> {
    check_operation(deadline, cancellation)?;
    let operation_cancellation = cancellation.child_token();
    let read_cancellation = operation_cancellation.clone();
    let read = objects.read_pinned_object_bounded_async(
        pinned,
        artifact_id,
        object_ordinal,
        maximum_rows,
        maximum_bytes,
        &read_cancellation,
    );
    tokio::pin!(read);
    let deadline_wait = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(deadline_wait);
    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            operation_cancellation.cancel();
            let _drained = read.as_mut().await;
            return Err(SecResearchReadError::Cancelled);
        }
        _ = deadline_wait.as_mut() => {
            operation_cancellation.cancel();
            let _drained = read.as_mut().await;
            return Err(SecResearchReadError::DeadlineExceeded);
        }
        result = read.as_mut() => result,
    };
    check_operation(deadline, cancellation)?;
    Ok(result?)
}

fn decode_exact_object_batches(
    batches: Vec<RecordBatch>,
    expected_rows: usize,
    expected_schema: &crate::DatasetSchemaRef,
    maximum_bytes: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
    control: &dyn ResearchObjectControl,
) -> Result<
    (
        Vec<ResearchObservation>,
        Vec<ProviderCaptureRowCoordinate>,
        usize,
        usize,
        EvidenceDigest,
    ),
    SecResearchReadError,
> {
    let mut observed_rows = 0_usize;
    let mut remaining_arrow_bytes = 0_usize;
    for batch in &batches {
        check_operation(deadline, cancellation)?;
        observed_rows = observed_rows
            .checked_add(batch.num_rows())
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
        remaining_arrow_bytes = remaining_arrow_bytes
            .checked_add(batch.get_array_memory_size())
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
    }
    if batches.is_empty() || observed_rows != expected_rows {
        return Err(SecResearchReadError::OriginMismatch);
    }
    let batch_slots = batches
        .capacity()
        .checked_mul(size_of::<RecordBatch>())
        .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
    ensure_object_budget(maximum_bytes, remaining_arrow_bytes, batch_slots)?;

    let mut observations = Vec::new();
    observations
        .try_reserve_exact(expected_rows)
        .map_err(|_| SecResearchReadError::ObjectBudgetExceeded)?;
    let mut coordinates = Vec::new();
    coordinates
        .try_reserve_exact(expected_rows)
        .map_err(|_| SecResearchReadError::ObjectBudgetExceeded)?;
    let aggregate_slots = observations
        .capacity()
        .checked_mul(size_of::<ResearchObservation>())
        .and_then(|bytes| {
            bytes.checked_add(
                coordinates
                    .capacity()
                    .checked_mul(size_of::<ProviderCaptureRowCoordinate>())?,
            )
        })
        .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
    ensure_object_budget(
        maximum_bytes,
        remaining_arrow_bytes,
        checked_object_bytes(batch_slots, aggregate_slots)?,
    )?;

    let mut observation_dynamic_bytes = 0_usize;
    let mut lineage = ResearchLineageDigestAccumulator::new();
    for batch in batches {
        check_operation(deadline, cancellation)?;
        let batch_bytes = batch.get_array_memory_size();
        let base_bytes = checked_object_bytes(
            checked_object_bytes(remaining_arrow_bytes, batch_slots)?,
            checked_object_bytes(aggregate_slots, observation_dynamic_bytes)?,
        )?;
        let available = maximum_bytes
            .checked_sub(base_bytes)
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
        let decoded = crate::ResearchArrowBatch::decode_provider_capture_record_batch_bounded(
            batch,
            available,
            &mut lineage,
            control,
        )
        .map_err(map_arrow_error)?;
        if &decoded.schema_ref != expected_schema {
            return Err(SecResearchReadError::OriginMismatch);
        }
        let decoded_slots = decoded
            .observations
            .len()
            .checked_mul(size_of::<ResearchObservation>())
            .and_then(|bytes| {
                bytes.checked_add(
                    decoded
                        .coordinates
                        .len()
                        .checked_mul(size_of::<ProviderCaptureRowCoordinate>())?,
                )
            })
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
        observation_dynamic_bytes = observation_dynamic_bytes
            .checked_add(
                decoded
                    .retained_bytes
                    .checked_sub(decoded_slots)
                    .ok_or(SecResearchReadError::ObjectBudgetExceeded)?,
            )
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
        observations.extend(decoded.observations);
        coordinates.extend(decoded.coordinates);
        remaining_arrow_bytes = remaining_arrow_bytes
            .checked_sub(batch_bytes)
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
        check_operation(deadline, cancellation)?;
    }
    if observations.len() != expected_rows || coordinates.len() != expected_rows {
        return Err(SecResearchReadError::OriginMismatch);
    }
    let decoded_retained_bytes = aggregate_slots
        .checked_add(observation_dynamic_bytes)
        .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
    if decoded_retained_bytes > maximum_bytes {
        return Err(SecResearchReadError::ObjectBudgetExceeded);
    }
    Ok((
        observations,
        coordinates,
        decoded_retained_bytes,
        observation_dynamic_bytes,
        lineage.finish(),
    ))
}

fn checked_object_bytes(left: usize, right: usize) -> Result<usize, SecResearchReadError> {
    left.checked_add(right)
        .ok_or(SecResearchReadError::ObjectBudgetExceeded)
}

struct SecResearchAggregateBudget {
    maximum: usize,
    retained: usize,
}

impl SecResearchAggregateBudget {
    fn new(maximum: usize, retained: usize) -> Result<Self, SecResearchReadError> {
        if retained > maximum {
            return Err(SecResearchReadError::ObjectBudgetExceeded);
        }
        Ok(Self { maximum, retained })
    }

    fn charge(&mut self, bytes: usize) -> Result<(), SecResearchReadError> {
        self.retained = self
            .retained
            .checked_add(bytes)
            .filter(|retained| *retained <= self.maximum)
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
        Ok(())
    }

    fn release(&mut self, bytes: usize) -> Result<(), SecResearchReadError> {
        self.retained = self
            .retained
            .checked_sub(bytes)
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
        Ok(())
    }

    fn reserve_work(&mut self, bytes: usize) -> Result<(), SecResearchReadError> {
        self.charge(bytes)
    }

    fn reserve_exact<T>(&mut self, count: usize) -> Result<Vec<T>, SecResearchReadError> {
        let requested = count
            .checked_mul(size_of::<T>())
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
        self.charge(requested)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|_| SecResearchReadError::ObjectBudgetExceeded)?;
        let actual = values
            .capacity()
            .checked_mul(size_of::<T>())
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
        if actual > requested {
            self.charge(actual - requested)?;
        }
        Ok(values)
    }

    fn remaining(&self) -> Result<usize, SecResearchReadError> {
        self.maximum
            .checked_sub(self.retained)
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)
    }
}

fn bounded_point_in_time_limits(
    request: &SecResearchReadRequest,
    aggregate: &SecResearchAggregateBudget,
) -> Result<PointInTimeLimits, SecResearchReadError> {
    let requested = request.point_in_time_limits();
    let retained = requested.max_retained_bytes().min(aggregate.remaining()?);
    PointInTimeLimits::try_new(
        requested.max_candidates(),
        requested.max_families(),
        requested.max_conflicts(),
        requested.max_result_rows(),
        retained,
    )
    .map_err(|_| SecResearchReadError::ObjectBudgetExceeded)
}

fn ensure_object_budget(
    maximum: usize,
    left: usize,
    right: usize,
) -> Result<(), SecResearchReadError> {
    if checked_object_bytes(left, right)? > maximum {
        Err(SecResearchReadError::ObjectBudgetExceeded)
    } else {
        Ok(())
    }
}

fn map_raw_store_error(error: SealedResearchJournalStoreError) -> SecResearchReadError {
    match error {
        SealedResearchJournalStoreError::ObjectControl(ResearchObjectControlError::Cancelled) => {
            SecResearchReadError::Cancelled
        }
        SealedResearchJournalStoreError::ObjectControl(
            ResearchObjectControlError::DeadlineExceeded,
        ) => SecResearchReadError::DeadlineExceeded,
        SealedResearchJournalStoreError::ObjectControl(ResearchObjectControlError::Unavailable) => {
            SecResearchReadError::AuthorityUnavailable
        }
        other => SecResearchReadError::RawStore(other),
    }
}

fn map_arrow_error(error: ArrowConversionError) -> SecResearchReadError {
    match error {
        ArrowConversionError::ObjectControl(ResearchObjectControlError::Cancelled) => {
            SecResearchReadError::Cancelled
        }
        ArrowConversionError::ObjectControl(ResearchObjectControlError::DeadlineExceeded) => {
            SecResearchReadError::DeadlineExceeded
        }
        ArrowConversionError::ObjectControl(ResearchObjectControlError::Unavailable) => {
            SecResearchReadError::AuthorityUnavailable
        }
        other => SecResearchReadError::Arrow(other),
    }
}

#[derive(Debug, Error)]
pub enum SecResearchReadError {
    #[error("SEC research request is invalid")]
    InvalidRequest,
    #[error("SEC research read was cancelled")]
    Cancelled,
    #[error("SEC research read deadline elapsed")]
    DeadlineExceeded,
    #[error("SEC research read authority is busy or unavailable")]
    AuthorityUnavailable,
    #[error("SEC research origin does not match the exact durable generation")]
    OriginMismatch,
    #[error("SEC research provider binding does not match exact raw/canonical evidence")]
    ProviderBindingMismatch,
    #[error("SEC research object exceeded the aggregate retained-memory/work ceiling")]
    ObjectBudgetExceeded,
    #[error("SEC point-in-time selection exceeded a bound or failed canonical validation")]
    PointInTimeSelection,
    #[error("SEC restart did not reproduce the exact typed result")]
    RestartMismatch,
    #[error("SEC request/result digest encoding failed")]
    DigestEncoding,
    #[error(transparent)]
    Manifest(#[from] ManifestCatalogError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    CompanySecurity(#[from] crate::CompanySecurityIdentityCatalogError),
    #[error(transparent)]
    Parquet(#[from] ParquetStoreError),
    #[error(transparent)]
    Arrow(#[from] ArrowConversionError),
    #[error(transparent)]
    RawStore(#[from] SealedResearchJournalStoreError),
}

fn exact_origin_object_ordinal(
    pinned: &crate::PinnedDataset,
    company: &CompanyIdentityExactRecord,
) -> Result<usize, SecResearchReadError> {
    let mut matches = pinned.objects().iter().enumerate().filter(|(_, object)| {
        object.artifact_id() == company.artifact_id()
            && object.relative_reference() == company.artifact_relative_reference()
            && object.object().content_hash().evidence() == company.artifact_content_digest()
            && object.object().size_bytes() == company.artifact_size_bytes()
    });
    let (ordinal, _) = matches.next().ok_or(SecResearchReadError::OriginMismatch)?;
    if matches.next().is_some() {
        return Err(SecResearchReadError::OriginMismatch);
    }
    Ok(ordinal)
}

fn validate_rows(
    family: SecResearchFamily,
    source_id: &SourceId,
    coordinates: &[ProviderCaptureRowCoordinate],
    observations: &[ResearchObservation],
    binding: &crate::PersistedProviderCaptureBindingEvidence,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), SecResearchReadError> {
    if coordinates.len() != observations.len() || coordinates.len() != binding.rows().len() {
        return Err(SecResearchReadError::ProviderBindingMismatch);
    }
    for (ordinal, ((coordinate, observation), retained)) in coordinates
        .iter()
        .zip(observations)
        .zip(binding.rows())
        .enumerate()
    {
        if ordinal % 64 == 0 {
            check_operation(deadline, cancellation)?;
        }
        let expected_ordinal =
            u32::try_from(ordinal).map_err(|_| SecResearchReadError::OriginMismatch)?;
        if !family.accepts(observation)
            || observation_context(observation).provenance().source_id() != source_id
            || coordinate.binding_digest != binding.binding_digest()
            || coordinate.capture_observation_digest != binding.capture().observation_digest()
            || coordinate.canonical_row_ordinal != expected_ordinal
            || coordinate.canonical_row_ordinal != retained.canonical_row_ordinal()
            || coordinate.canonical_row_digest != retained.canonical_row_digest()
            || coordinate.native_semantic_digest != retained.native_semantic_digest()
            || coordinate.capture_page_ordinal != retained.capture_page_ordinal()
            || coordinate.segment_ordinal != retained.segment_ordinal()
            || coordinate.physical_frame_ordinal != retained.physical_frame_ordinal()
            || coordinate.page_body_digest != retained.page_body_digest()
        {
            return Err(SecResearchReadError::ProviderBindingMismatch);
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "all exact origin, company, binding, row, clock, and operation authorities remain explicit"
)]
async fn materialize_selection(
    request: SecResearchReadRequest,
    origin: SecResearchOrigin,
    company_identity: CompanyIdentityExactRecord,
    capture_observation_digest: EvidenceDigest,
    row_mapping_digest: EvidenceDigest,
    observations: Vec<ResearchObservation>,
    coordinates: Vec<ProviderCaptureRowCoordinate>,
    decoded_retained_bytes: usize,
    observation_dynamic_bytes: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<SecResearchSelection, SecResearchReadError> {
    check_operation(deadline, cancellation)?;
    let company_knowable = company_identity
        .observation()
        .availability()
        .conservative_available_at()
        .is_some_and(|available| available <= request.knowledge_at())
        && company_identity.observation().received_at() <= request.knowledge_at()
        && company_identity.observation().ingested_at() <= request.knowledge_at()
        && company_identity.completed_at() <= request.knowledge_at();

    let coordinate_retained_bytes = coordinates
        .capacity()
        .checked_mul(size_of::<ProviderCaptureRowCoordinate>())
        .ok_or(SecResearchReadError::ObjectBudgetExceeded)?;
    let mut aggregate =
        SecResearchAggregateBudget::new(request.maximum_object_bytes(), decoded_retained_bytes)?;
    // Candidates clone the decoded observation payloads. Charge the complete dynamic estimate
    // before cloning so the operation cannot transiently exceed the request's aggregate ceiling.
    aggregate.charge(observation_dynamic_bytes)?;
    aggregate.reserve_work(
        request
            .manifest()
            .dataset_id()
            .as_str()
            .len()
            .checked_add(request.manifest().schema().name().len())
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_mul(observations.len()))
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?,
    )?;

    let mut row_identities =
        aggregate.reserve_exact::<SecResearchRowIdentity>(observations.len())?;
    let mut candidates = aggregate.reserve_exact::<PointInTimeCandidate>(observations.len())?;
    let mut candidate_ordinals = aggregate.reserve_exact::<usize>(observations.len())?;
    let mut exclusions = aggregate.reserve_exact::<SecResearchExcludedRow>(observations.len())?;

    for (ordinal, (observation, coordinate)) in observations.iter().zip(&coordinates).enumerate() {
        if ordinal % 64 == 0 {
            check_operation(deadline, cancellation)?;
        }
        let row = SecResearchRowIdentity {
            row_ordinal: coordinate.canonical_row_ordinal,
            canonical_row_digest: coordinate.canonical_row_digest,
            observation_digest: coordinate.observation_digest,
        };
        row_identities.push(row);
        let provenance = observation_context(observation).provenance();
        let knowledge = SecResearchKnowledgeExclusions {
            available_after_cutoff: provenance
                .availability()
                .conservative_available_at()
                .is_some_and(|available| available > request.knowledge_at()),
            received_after_cutoff: provenance.received_at() > request.knowledge_at(),
            ingested_after_cutoff: provenance.ingested_at() > request.knowledge_at(),
            generation_completed_after_cutoff: origin.generation_completed_at()
                > request.knowledge_at(),
            company_identity_not_knowable: !company_knowable,
        };
        if knowledge.is_empty() {
            candidates.push(PointInTimeCandidate::new(
                observation.clone(),
                request.manifest().clone(),
            ));
            candidate_ordinals.push(ordinal);
        } else {
            exclusions.push(SecResearchExcludedRow {
                row,
                knowledge,
                point_in_time_reasons: None,
                point_in_time: None,
            });
        }
    }
    drop(coordinates);
    aggregate.release(coordinate_retained_bytes)?;

    let mut selected = aggregate.reserve_exact::<SecResearchSelectedRow>(
        request.point_in_time_limits().max_result_rows(),
    )?;
    let mut conflicts = aggregate
        .reserve_exact::<SecResearchConflict>(request.point_in_time_limits().max_conflicts())?;
    aggregate.reserve_work(
        observations
            .len()
            .checked_mul(size_of::<(
                SecResearchRowIdentity,
                SecResearchPointInTimeIdentities,
            )>())
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?,
    )?;

    let policy = PointInTimePolicy::try_new(
        NonZeroU32::new(1).ok_or(SecResearchReadError::InvalidRequest)?,
        request.revision_mode(),
    )
    .map_err(|_| SecResearchReadError::PointInTimeSelection)?;
    let pit_request = PointInTimeRequest::try_new(
        policy,
        request.knowledge_at(),
        None,
        request.effective_cutoff().clone(),
        None,
        bounded_point_in_time_limits(&request, &aggregate)?,
    )
    .map_err(|_| SecResearchReadError::PointInTimeSelection)?;
    let outcome = PointInTimeService::new()
        .select(&pit_request, &candidates, cancellation, deadline)
        .await;

    let (disposition, point_in_time_content_identity, point_in_time_audit_identity) = match outcome
    {
        Ok(selection) => {
            for record in selection.records() {
                check_operation(deadline, cancellation)?;
                let original =
                    original_row_ordinal(record.candidate(), &candidates, &candidate_ordinals)?;
                selected.push(SecResearchSelectedRow {
                    row: *row_identities
                        .get(original)
                        .ok_or(SecResearchReadError::PointInTimeSelection)?,
                    point_in_time: point_in_time_identities(record),
                });
            }
            for exclusion in selection.exclusions() {
                check_operation(deadline, cancellation)?;
                append_pit_exclusion(
                    &mut exclusions,
                    exclusion.record(),
                    exclusion.reasons(),
                    &candidates,
                    &candidate_ordinals,
                    &row_identities,
                )?;
            }
            (
                if selected.is_empty() {
                    SecResearchDisposition::Unavailable
                } else {
                    SecResearchDisposition::Selected
                },
                Some(selection.content_identity()),
                selection.audit_identity(),
            )
        }
        Err(PointInTimeError::RevisionConflicts { report }) => {
            for exclusion in report.exclusions() {
                check_operation(deadline, cancellation)?;
                append_pit_exclusion(
                    &mut exclusions,
                    exclusion.record(),
                    exclusion.reasons(),
                    &candidates,
                    &candidate_ordinals,
                    &row_identities,
                )?;
            }
            for conflict in report.conflicts() {
                let mut rows = Vec::new();
                rows.try_reserve_exact(conflict.records().len())
                    .map_err(|_| SecResearchReadError::PointInTimeSelection)?;
                for record in conflict.records() {
                    check_operation(deadline, cancellation)?;
                    let original =
                        original_row_ordinal(record.candidate(), &candidates, &candidate_ordinals)?;
                    rows.push((
                        *row_identities
                            .get(original)
                            .ok_or(SecResearchReadError::PointInTimeSelection)?,
                        point_in_time_identities(record),
                    ));
                }
                conflicts.push(SecResearchConflict {
                    family_identity: conflict.family_identity(),
                    revision: conflict.revision(),
                    rows: rows.into_boxed_slice(),
                });
            }
            (
                SecResearchDisposition::Conflict,
                None,
                report.audit_identity(),
            )
        }
        Err(PointInTimeError::Cancelled) => return Err(SecResearchReadError::Cancelled),
        Err(PointInTimeError::DeadlineExceeded) => {
            return Err(SecResearchReadError::DeadlineExceeded);
        }
        Err(_) => return Err(SecResearchReadError::PointInTimeSelection),
    };

    selected.sort_by_key(|row| row.row.row_ordinal);
    exclusions.sort_by_key(|row| row.row.row_ordinal);
    conflicts.sort_by(|left, right| {
        left.family_identity
            .bytes()
            .cmp(&right.family_identity.bytes())
            .then_with(|| left.revision.get().cmp(&right.revision.get()))
    });
    let selection_digest = selection_digest(
        disposition,
        point_in_time_content_identity,
        point_in_time_audit_identity,
        &selected,
        &exclusions,
        &conflicts,
        deadline,
        cancellation,
    )?;
    aggregate.reserve_work(
        observations
            .len()
            .checked_mul(size_of::<ResearchObservation>())
            .and_then(|bytes| {
                bytes.checked_add(
                    selected
                        .len()
                        .checked_mul(size_of::<SecResearchSelectedRow>())?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    exclusions
                        .len()
                        .checked_mul(size_of::<SecResearchExcludedRow>())?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    conflicts
                        .len()
                        .checked_mul(size_of::<SecResearchConflict>())?,
                )
            })
            .ok_or(SecResearchReadError::ObjectBudgetExceeded)?,
    )?;
    let result_digest = result_digest(
        request.request_digest(),
        origin.origin_digest(),
        request.provider_binding_digest(),
        capture_observation_digest,
        row_mapping_digest,
        company_identity.observation_digest(),
        selection_digest,
    );
    let receipt = SecResearchSelectionReceipt {
        request_digest: request.request_digest(),
        origin_digest: origin.origin_digest(),
        provider_binding_digest: request.provider_binding_digest(),
        capture_observation_digest,
        row_mapping_digest,
        company_observation_digest: company_identity.observation_digest(),
        point_in_time_content_identity,
        point_in_time_audit_identity,
        selection_digest,
        result_digest,
    };
    Ok(SecResearchSelection {
        request,
        origin,
        company_identity,
        decoded_rows: observations.into_boxed_slice(),
        disposition,
        selected: selected.into_boxed_slice(),
        exclusions: exclusions.into_boxed_slice(),
        conflicts: conflicts.into_boxed_slice(),
        receipt,
    })
}

fn original_row_ordinal(
    candidate: &PointInTimeCandidate,
    candidates: &[PointInTimeCandidate],
    ordinals: &[usize],
) -> Result<usize, SecResearchReadError> {
    candidates
        .iter()
        .position(|retained| std::ptr::eq(retained, candidate))
        .and_then(|position| ordinals.get(position).copied())
        .ok_or(SecResearchReadError::PointInTimeSelection)
}

fn append_pit_exclusion(
    exclusions: &mut Vec<SecResearchExcludedRow>,
    record: crate::PointInTimeRecord<'_>,
    reasons: PointInTimeExclusionReasons,
    candidates: &[PointInTimeCandidate],
    candidate_ordinals: &[usize],
    row_identities: &[SecResearchRowIdentity],
) -> Result<(), SecResearchReadError> {
    let original = original_row_ordinal(record.candidate(), candidates, candidate_ordinals)?;
    exclusions.push(SecResearchExcludedRow {
        row: *row_identities
            .get(original)
            .ok_or(SecResearchReadError::PointInTimeSelection)?,
        knowledge: SecResearchKnowledgeExclusions::default(),
        point_in_time_reasons: Some(reasons),
        point_in_time: Some(point_in_time_identities(&record)),
    });
    Ok(())
}

fn point_in_time_identities(
    record: &crate::PointInTimeRecord<'_>,
) -> SecResearchPointInTimeIdentities {
    SecResearchPointInTimeIdentities {
        family_identity: record.family_identity(),
        payload_identity: record.payload_identity(),
        provenance_identity: record.provenance_identity(),
        evidence_identity: record.evidence_identity(),
        revision_state: record.revision_state(),
    }
}

fn request_digest(
    request: &SecResearchReadRequest,
) -> Result<EvidenceDigest, SecResearchReadError> {
    let mut hash = Sha256::new();
    hash.update(SEC_RESEARCH_REQUEST_DOMAIN);
    hash_text(&mut hash, request.manifest.dataset_id().as_str());
    hash.update(request.manifest.manifest_version().to_be_bytes());
    hash_text(&mut hash, request.manifest.schema().name());
    hash.update(request.manifest.schema().version().get().to_be_bytes());
    hash.update(request.manifest.schema().fingerprint());
    hash.update(request.manifest.content_hash().bytes());
    hash.update([request.family.tag()]);
    hash_evidence(&mut hash, request.provider_binding_digest);
    hash_evidence(&mut hash, request.company_observation_digest);
    hash.update(request.knowledge_at.unix_nanos().to_be_bytes());
    let effective = serde_json::to_vec(&request.effective_cutoff)
        .map_err(|_| SecResearchReadError::DigestEncoding)?;
    hash_bytes(&mut hash, &effective);
    hash.update([match request.revision_mode {
        PointInTimeRevisionMode::LatestKnown => 1,
        PointInTimeRevisionMode::AllKnown => 2,
    }]);
    for bound in [
        request.point_in_time_limits.max_candidates(),
        request.point_in_time_limits.max_families(),
        request.point_in_time_limits.max_conflicts(),
        request.point_in_time_limits.max_result_rows(),
        request.point_in_time_limits.max_retained_bytes(),
        request.maximum_object_bytes,
    ] {
        hash.update(
            u64::try_from(bound)
                .map_err(|_| SecResearchReadError::InvalidRequest)?
                .to_be_bytes(),
        );
    }
    Ok(evidence_digest(hash.finalize().into()))
}

fn origin_digest(origin: &SecResearchOrigin) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(SEC_RESEARCH_ORIGIN_DOMAIN);
    hash_text(&mut hash, origin.manifest.dataset_id().as_str());
    hash.update(origin.manifest.manifest_version().to_be_bytes());
    hash.update(origin.manifest.content_hash().bytes());
    hash_text(&mut hash, origin.source_id.as_str());
    hash.update(origin.run_id.as_bytes());
    hash.update(origin.control_manifest_id.as_bytes());
    hash.update(origin.artifact_id.as_bytes());
    hash.update((origin.object_ordinal as u64).to_be_bytes());
    hash_text(&mut hash, &origin.relative_reference);
    hash_evidence(&mut hash, origin.object_content_digest);
    hash_evidence(&mut hash, origin.object_lineage_digest);
    hash.update(origin.object_row_count.to_be_bytes());
    hash.update(origin.object_size_bytes.to_be_bytes());
    hash.update(origin.generation_completed_at.unix_nanos().to_be_bytes());
    evidence_digest(hash.finalize().into())
}

fn selection_digest(
    disposition: SecResearchDisposition,
    content_identity: Option<Sha256Digest>,
    audit_identity: Sha256Digest,
    selected: &[SecResearchSelectedRow],
    exclusions: &[SecResearchExcludedRow],
    conflicts: &[SecResearchConflict],
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<EvidenceDigest, SecResearchReadError> {
    let mut hash = Sha256::new();
    hash.update(SEC_RESEARCH_SELECTION_DOMAIN);
    hash.update([match disposition {
        SecResearchDisposition::Selected => 1,
        SecResearchDisposition::Unavailable => 2,
        SecResearchDisposition::Conflict => 3,
    }]);
    match content_identity {
        Some(value) => {
            hash.update([1]);
            hash.update(value.bytes());
        }
        None => hash.update([0]),
    }
    hash.update(audit_identity.bytes());
    hash.update((selected.len() as u64).to_be_bytes());
    for (ordinal, row) in selected.iter().enumerate() {
        if ordinal % 64 == 0 {
            check_operation(deadline, cancellation)?;
        }
        hash_row_identity(&mut hash, row.row);
        hash_pit_identities(&mut hash, row.point_in_time);
    }
    hash.update((exclusions.len() as u64).to_be_bytes());
    for (ordinal, exclusion) in exclusions.iter().enumerate() {
        if ordinal % 64 == 0 {
            check_operation(deadline, cancellation)?;
        }
        hash_row_identity(&mut hash, exclusion.row);
        for excluded in [
            exclusion.knowledge.available_after_cutoff,
            exclusion.knowledge.received_after_cutoff,
            exclusion.knowledge.ingested_after_cutoff,
            exclusion.knowledge.generation_completed_after_cutoff,
            exclusion.knowledge.company_identity_not_knowable,
        ] {
            hash.update([u8::from(excluded)]);
        }
        match exclusion.point_in_time_reasons {
            Some(reasons) => {
                hash.update([1]);
                hash_exclusion_reasons(&mut hash, reasons);
            }
            None => hash.update([0]),
        }
        match exclusion.point_in_time {
            Some(identities) => {
                hash.update([1]);
                hash_pit_identities(&mut hash, identities);
            }
            None => hash.update([0]),
        }
    }
    hash.update((conflicts.len() as u64).to_be_bytes());
    for (ordinal, conflict) in conflicts.iter().enumerate() {
        if ordinal % 64 == 0 {
            check_operation(deadline, cancellation)?;
        }
        hash.update(conflict.family_identity.bytes());
        hash.update(conflict.revision.get().to_be_bytes());
        hash.update((conflict.rows.len() as u64).to_be_bytes());
        for (row_ordinal, (row, identities)) in conflict.rows.iter().enumerate() {
            if row_ordinal % 64 == 0 {
                check_operation(deadline, cancellation)?;
            }
            hash_row_identity(&mut hash, *row);
            hash_pit_identities(&mut hash, *identities);
        }
    }
    check_operation(deadline, cancellation)?;
    Ok(evidence_digest(hash.finalize().into()))
}

fn result_digest(
    request_digest: EvidenceDigest,
    origin_digest: EvidenceDigest,
    binding_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    row_mapping_digest: EvidenceDigest,
    company_observation_digest: EvidenceDigest,
    selection_digest: EvidenceDigest,
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(SEC_RESEARCH_RESULT_DOMAIN);
    for digest in [
        request_digest,
        origin_digest,
        binding_digest,
        capture_observation_digest,
        row_mapping_digest,
        company_observation_digest,
        selection_digest,
    ] {
        hash_evidence(&mut hash, digest);
    }
    evidence_digest(hash.finalize().into())
}

fn hash_row_identity(hash: &mut Sha256, row: SecResearchRowIdentity) {
    hash.update(row.row_ordinal.to_be_bytes());
    hash_evidence(hash, row.canonical_row_digest);
    hash_evidence(hash, row.observation_digest);
}

fn hash_pit_identities(hash: &mut Sha256, identities: SecResearchPointInTimeIdentities) {
    for digest in [
        identities.family_identity,
        identities.payload_identity,
        identities.provenance_identity,
        identities.evidence_identity,
    ] {
        hash.update(digest.bytes());
    }
    hash.update([match identities.revision_state {
        PointInTimeRevisionState::Current => 1,
        PointInTimeRevisionState::Superseded => 2,
        PointInTimeRevisionState::SupersessionIncomparable => 3,
    }]);
}

fn hash_exclusion_reasons(hash: &mut Sha256, reasons: PointInTimeExclusionReasons) {
    for reason in [
        PointInTimeExclusionReason::AvailabilityAfterAsOf,
        PointInTimeExclusionReason::InferredAvailability,
        PointInTimeExclusionReason::UnknownAvailability,
        PointInTimeExclusionReason::PublicationAfterAsOf,
        PointInTimeExclusionReason::PublicationAfterCutoff,
        PointInTimeExclusionReason::PublicationIncomparable,
        PointInTimeExclusionReason::EffectiveAfterCutoff,
        PointInTimeExclusionReason::EffectiveNotAfterCutoff,
        PointInTimeExclusionReason::EffectiveAfterLabelCutoff,
        PointInTimeExclusionReason::EffectiveIncomparable,
        PointInTimeExclusionReason::SupersededByKnowledgeTime,
        PointInTimeExclusionReason::SupersessionIncomparable,
        PointInTimeExclusionReason::LowerRevision,
        PointInTimeExclusionReason::DuplicateRevision,
    ] {
        hash.update([u8::from(reasons.contains(reason))]);
    }
}

fn hash_evidence(hash: &mut Sha256, digest: EvidenceDigest) {
    hash.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hash.update(digest.bytes());
}

fn hash_text(hash: &mut Sha256, value: &str) {
    hash_bytes(hash, value.as_bytes());
}

fn hash_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

const fn evidence_digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

fn valid_sha256(digest: EvidenceDigest) -> bool {
    digest.algorithm() == DigestAlgorithm::Sha256 && digest.bytes() != [0; 32]
}

fn valid_sec_cik(value: &str) -> bool {
    value.len() == 10
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.bytes().any(|byte| byte != b'0')
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), SecResearchReadError> {
    if cancellation.is_cancelled() {
        Err(SecResearchReadError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(SecResearchReadError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

const fn observation_context(observation: &ResearchObservation) -> &ResearchContext {
    match observation {
        ResearchObservation::Filing(value) => value.context(),
        ResearchObservation::Fundamental(value) => value.context(),
        ResearchObservation::Macro(value) => value.context(),
        ResearchObservation::MarketBar(value) => value.context(),
        ResearchObservation::FundNav(value) => value.context(),
        ResearchObservation::PortfolioPosition(value) => value.context(),
        ResearchObservation::Transaction(value) => value.context(),
        ResearchObservation::CorporateAction(value) => value.context(),
        ResearchObservation::UniverseMembership(value) => value.context(),
        ResearchObservation::AlternativeData(value) => value.context(),
    }
}
