//! Closed application contracts for one operator-reviewed company/security resolution.

use market_squawk_domain::{
    CommonEquitySuitability, CompanyIdentitySurface, CompanySecurityKind,
    CompanySecurityRelationshipKind, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, Figi, IdentifierEntitlement, IdentifierRightsPolicyReference,
    InstrumentId, SourceId, SourceIdentifier, Timestamp,
};
use serde::Serialize;
use uuid::Uuid;

use super::CompanySecurityResolutionError;

/// Current application grammar. V1 has no compatibility or legacy decoding branch.
pub const COMPANY_SECURITY_RESOLUTION_FORMAT_VERSION: u16 = 1;
/// Maximum exact security candidates retained by one operator preview.
pub const MAXIMUM_REVIEWED_SECURITY_CANDIDATES: usize = 32;
/// Maximum encoded immutable preview retained in process memory.
pub const MAXIMUM_COMPANY_SECURITY_PREVIEW_BYTES: usize = 128 * 1024;

/// One-process opaque identity for an immutable, one-use preview.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct CompanySecurityResolutionPreviewId(Uuid);

impl CompanySecurityResolutionPreviewId {
    pub(super) fn new(value: Uuid) -> Result<Self, CompanySecurityResolutionError> {
        if value.is_nil() {
            Err(CompanySecurityResolutionError::InvalidRequest)
        } else {
            Ok(Self(value))
        }
    }

    /// Admits a non-nil identity already decoded by a trusted transport boundary.
    pub fn try_from_uuid(value: Uuid) -> Result<Self, CompanySecurityResolutionError> {
        Self::new(value)
    }

    /// Returns the underlying UUID for a closed Desktop DTO projection.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// Operator decision for one exact FIGI candidate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "disposition", rename_all = "snake_case")]
pub enum ReviewedSecurityCandidateDisposition {
    /// The sole security the operator resolved to the exact company parent.
    Selected,
    /// A reviewed alternative that was explicitly rejected.
    Excluded {
        /// Bounded reason identity; free-form ticker/name evidence is deliberately absent.
        reason: SourceIdentifier,
    },
}

impl ReviewedSecurityCandidateDisposition {
    /// Returns the bounded exclusion reason when this candidate was rejected.
    pub const fn exclusion_reason(&self) -> Option<&SourceIdentifier> {
        match self {
            Self::Selected => None,
            Self::Excluded { reason } => Some(reason),
        }
    }

    pub(super) const fn is_selected(&self) -> bool {
        matches!(self, Self::Selected)
    }
}

/// One exact FIGI considered by the operator.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewedSecurityCandidate {
    permanent_figi: Figi,
    disposition: ReviewedSecurityCandidateDisposition,
}

impl ReviewedSecurityCandidate {
    /// Constructs one exact candidate; ticker, name, and exchange cannot identify it.
    pub const fn new(
        permanent_figi: Figi,
        disposition: ReviewedSecurityCandidateDisposition,
    ) -> Self {
        Self {
            permanent_figi,
            disposition,
        }
    }

    /// Returns the permanent FIGI used for the exact catalog read.
    pub const fn permanent_figi(&self) -> &Figi {
        &self.permanent_figi
    }

    /// Returns the explicit selected or excluded decision.
    pub const fn disposition(&self) -> &ReviewedSecurityCandidateDisposition {
        &self.disposition
    }
}

/// Current or revoked status of the operator's evidence-rights review.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "disposition", rename_all = "snake_case")]
pub enum OperatorEvidenceRightsDisposition {
    /// The versioned policy admits the exact relationship-evidence bundle until its expiry.
    Admitted,
    /// The reviewed policy or entitlement was explicitly revoked.
    Revoked { revoked_at: Timestamp },
}

/// Exact evidence and a time-bounded operator-reviewed relationship-rights decision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewedRelationshipEvidence {
    evidence: ExactPayloadEvidence,
    evidence_available_at: Timestamp,
    reviewed_at: Timestamp,
    rights: IdentifierRightsPolicyReference,
    rights_revision_digest: EvidenceDigest,
    rights_valid_until: Timestamp,
    rights_disposition: OperatorEvidenceRightsDisposition,
}

impl ReviewedRelationshipEvidence {
    /// Constructs the exact evidence bundle reviewed by the named operator.
    #[allow(
        clippy::too_many_arguments,
        reason = "every rights coordinate is independent authority"
    )]
    pub fn try_new(
        evidence: ExactPayloadEvidence,
        evidence_available_at: Timestamp,
        reviewed_at: Timestamp,
        rights: IdentifierRightsPolicyReference,
        rights_revision_digest: EvidenceDigest,
        rights_valid_until: Timestamp,
        rights_disposition: OperatorEvidenceRightsDisposition,
    ) -> Result<Self, CompanySecurityResolutionError> {
        if evidence.content_digest().bytes() == [0; 32]
            || rights_revision_digest.algorithm() != DigestAlgorithm::Sha256
            || rights_revision_digest.bytes() == [0; 32]
            || evidence_available_at > reviewed_at
            || rights_valid_until <= reviewed_at
            || rights.entitlement() == IdentifierEntitlement::UnknownOrRestricted
            || matches!(
                rights_disposition,
                OperatorEvidenceRightsDisposition::Revoked { revoked_at }
                    if revoked_at < reviewed_at
            )
        {
            return Err(CompanySecurityResolutionError::InvalidRequest);
        }
        Ok(Self {
            evidence,
            evidence_available_at,
            reviewed_at,
            rights,
            rights_revision_digest,
            rights_valid_until,
            rights_disposition,
        })
    }

    /// Returns the exact evidence bundle reviewed for this relationship only.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }

    /// Returns when the complete evidence bundle was conservatively available.
    pub const fn evidence_available_at(&self) -> Timestamp {
        self.evidence_available_at
    }

    /// Returns when the operator completed the rights review.
    pub const fn reviewed_at(&self) -> Timestamp {
        self.reviewed_at
    }

    /// Returns the policy reference governing relationship evidence only.
    pub const fn rights(&self) -> &IdentifierRightsPolicyReference {
        &self.rights
    }

    /// Returns the exact SHA-256 revision of the reviewed rights policy.
    pub const fn rights_revision_digest(&self) -> EvidenceDigest {
        self.rights_revision_digest
    }

    /// Returns the exclusive expiry of this operator-reviewed rights decision.
    pub const fn rights_valid_until(&self) -> Timestamp {
        self.rights_valid_until
    }

    /// Returns the closed admitted or revoked review outcome.
    pub const fn rights_disposition(&self) -> OperatorEvidenceRightsDisposition {
        self.rights_disposition
    }

    pub(super) fn ensure_current(
        &self,
        now: Timestamp,
    ) -> Result<(), CompanySecurityResolutionError> {
        if self.evidence_available_at > now || self.reviewed_at > now {
            return Err(CompanySecurityResolutionError::InvalidRequest);
        }
        if self.rights_valid_until <= now {
            return Err(CompanySecurityResolutionError::StaleRights);
        }
        if matches!(
            self.rights_disposition,
            OperatorEvidenceRightsDisposition::Revoked { .. }
        ) {
            return Err(CompanySecurityResolutionError::RevokedRights);
        }
        Ok(())
    }
}

/// Mutation the operator is explicitly previewing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum CompanySecurityResolutionAction {
    /// Establish or explicitly supersede an admitted relationship.
    Authorize,
    /// Revoke the exact current relationship.
    Revoke { reason: SourceIdentifier },
}

/// Exact current relationship state the preview request expects.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ExpectedCompanySecurityRelationshipState {
    /// No event currently exists for the exact company/security key.
    Absent,
    /// The request explicitly acknowledges the exact current event.
    Current { link_digest: EvidenceDigest },
}

/// Complete bounded request for an operator-resolution preview.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompanySecurityResolutionPreviewRequest {
    company_source_id: SourceId,
    provider_company_id: SourceIdentifier,
    company_surface: CompanyIdentitySurface,
    reviewed_candidates: Box<[ReviewedSecurityCandidate]>,
    operator_id: SourceIdentifier,
    security_kind: CompanySecurityKind,
    relationship_kind: CompanySecurityRelationshipKind,
    common_equity_suitability: CommonEquitySuitability,
    reviewed_evidence: ReviewedRelationshipEvidence,
    action: CompanySecurityResolutionAction,
    expected_relationship_state: ExpectedCompanySecurityRelationshipState,
    effective_end: Option<Timestamp>,
}

impl CompanySecurityResolutionPreviewRequest {
    /// Constructs one resolution request with exactly one selected FIGI candidate.
    #[allow(
        clippy::too_many_arguments,
        reason = "the authority request binds independent facts"
    )]
    pub fn try_new(
        company_source_id: SourceId,
        provider_company_id: SourceIdentifier,
        company_surface: CompanyIdentitySurface,
        reviewed_candidates: Vec<ReviewedSecurityCandidate>,
        operator_id: SourceIdentifier,
        security_kind: CompanySecurityKind,
        relationship_kind: CompanySecurityRelationshipKind,
        common_equity_suitability: CommonEquitySuitability,
        reviewed_evidence: ReviewedRelationshipEvidence,
        action: CompanySecurityResolutionAction,
        expected_relationship_state: ExpectedCompanySecurityRelationshipState,
        effective_end: Option<Timestamp>,
    ) -> Result<Self, CompanySecurityResolutionError> {
        if reviewed_candidates.is_empty()
            || reviewed_candidates.len() > MAXIMUM_REVIEWED_SECURITY_CANDIDATES
            || reviewed_candidates
                .iter()
                .filter(|candidate| candidate.disposition.is_selected())
                .count()
                != 1
        {
            return Err(CompanySecurityResolutionError::AmbiguousCandidates);
        }
        for (index, candidate) in reviewed_candidates.iter().enumerate() {
            if reviewed_candidates
                .iter()
                .skip(index + 1)
                .any(|other| other.permanent_figi() == candidate.permanent_figi())
            {
                return Err(CompanySecurityResolutionError::AmbiguousCandidates);
            }
        }
        let suitable =
            common_equity_suitability == CommonEquitySuitability::SuitableIssuerCommonEquity;
        let direct_common = security_kind == CompanySecurityKind::CommonEquity
            && relationship_kind == CompanySecurityRelationshipKind::Issuer;
        if suitable != direct_common
            || matches!(&action, CompanySecurityResolutionAction::Revoke { .. })
                && matches!(
                    expected_relationship_state,
                    ExpectedCompanySecurityRelationshipState::Absent
                )
        {
            return Err(CompanySecurityResolutionError::InvalidRequest);
        }
        if let ExpectedCompanySecurityRelationshipState::Current { link_digest } =
            expected_relationship_state
        {
            validate_sha256(link_digest)?;
        }
        Ok(Self {
            company_source_id,
            provider_company_id,
            company_surface,
            reviewed_candidates: reviewed_candidates.into_boxed_slice(),
            operator_id,
            security_kind,
            relationship_kind,
            common_equity_suitability,
            reviewed_evidence,
            action,
            expected_relationship_state,
            effective_end,
        })
    }

    pub(super) const fn company_source_id(&self) -> &SourceId {
        &self.company_source_id
    }
    pub(super) const fn provider_company_id(&self) -> &SourceIdentifier {
        &self.provider_company_id
    }
    pub(super) const fn company_surface(&self) -> CompanyIdentitySurface {
        self.company_surface
    }
    pub(super) fn reviewed_candidates(&self) -> &[ReviewedSecurityCandidate] {
        &self.reviewed_candidates
    }
    pub(super) const fn operator_id(&self) -> &SourceIdentifier {
        &self.operator_id
    }
    pub(super) const fn security_kind(&self) -> CompanySecurityKind {
        self.security_kind
    }
    pub(super) const fn relationship_kind(&self) -> CompanySecurityRelationshipKind {
        self.relationship_kind
    }
    pub(super) const fn common_equity_suitability(&self) -> CommonEquitySuitability {
        self.common_equity_suitability
    }
    pub(super) const fn reviewed_evidence(&self) -> &ReviewedRelationshipEvidence {
        &self.reviewed_evidence
    }
    pub(super) const fn action(&self) -> &CompanySecurityResolutionAction {
        &self.action
    }
    pub(super) const fn expected_relationship_state(
        &self,
    ) -> ExpectedCompanySecurityRelationshipState {
        self.expected_relationship_state
    }
    pub(super) const fn effective_end(&self) -> Option<Timestamp> {
        self.effective_end
    }
}

/// Exact current company parent bound into a preview.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompanyIdentityParentSnapshot {
    company_source_id: SourceId,
    provider_company_id: SourceIdentifier,
    company_surface: CompanyIdentitySurface,
    company_observation_digest: EvidenceDigest,
    conformed_name_for_display: String,
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    completed_at: Timestamp,
}

impl CompanyIdentityParentSnapshot {
    #[allow(
        clippy::too_many_arguments,
        reason = "the snapshot binds every exact parent coordinate"
    )]
    pub(super) fn new(
        company_source_id: SourceId,
        provider_company_id: SourceIdentifier,
        company_surface: CompanyIdentitySurface,
        company_observation_digest: EvidenceDigest,
        conformed_name_for_display: String,
        received_at: Timestamp,
        available_at: Timestamp,
        ingested_at: Timestamp,
        completed_at: Timestamp,
    ) -> Self {
        Self {
            company_source_id,
            provider_company_id,
            company_surface,
            company_observation_digest,
            conformed_name_for_display,
            received_at,
            available_at,
            ingested_at,
            completed_at,
        }
    }

    pub const fn company_source_id(&self) -> &SourceId {
        &self.company_source_id
    }
    pub const fn provider_company_id(&self) -> &SourceIdentifier {
        &self.provider_company_id
    }
    pub const fn company_surface(&self) -> CompanyIdentitySurface {
        self.company_surface
    }
    pub const fn company_observation_digest(&self) -> EvidenceDigest {
        self.company_observation_digest
    }
    /// Display-only provider name; it never participates in parent selection.
    pub fn conformed_name_for_display(&self) -> &str {
        &self.conformed_name_for_display
    }
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }
    pub const fn completed_at(&self) -> Timestamp {
        self.completed_at
    }
}

/// Exact catalog-backed candidate retained in operator order.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompanySecurityCandidateSnapshot {
    ordinal: u16,
    permanent_figi: Figi,
    instrument_id: InstrumentId,
    market_revision_digest: EvidenceDigest,
    market_revision_sequence: u32,
    market_published_at: Timestamp,
    market_effective_interval: EffectiveInterval,
    disposition: ReviewedSecurityCandidateDisposition,
}

impl CompanySecurityCandidateSnapshot {
    #[allow(
        clippy::too_many_arguments,
        reason = "the snapshot binds every exact parent coordinate"
    )]
    pub(super) const fn new(
        ordinal: u16,
        permanent_figi: Figi,
        instrument_id: InstrumentId,
        market_revision_digest: EvidenceDigest,
        market_revision_sequence: u32,
        market_published_at: Timestamp,
        market_effective_interval: EffectiveInterval,
        disposition: ReviewedSecurityCandidateDisposition,
    ) -> Self {
        Self {
            ordinal,
            permanent_figi,
            instrument_id,
            market_revision_digest,
            market_revision_sequence,
            market_published_at,
            market_effective_interval,
            disposition,
        }
    }

    pub const fn ordinal(&self) -> u16 {
        self.ordinal
    }
    pub const fn permanent_figi(&self) -> &Figi {
        &self.permanent_figi
    }
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub const fn market_revision_digest(&self) -> EvidenceDigest {
        self.market_revision_digest
    }
    pub const fn market_revision_sequence(&self) -> u32 {
        self.market_revision_sequence
    }
    pub const fn market_published_at(&self) -> Timestamp {
        self.market_published_at
    }
    pub const fn market_effective_interval(&self) -> EffectiveInterval {
        self.market_effective_interval
    }
    pub const fn disposition(&self) -> &ReviewedSecurityCandidateDisposition {
        &self.disposition
    }
}

/// The sole V1 effective-start grammar: authorization time at confirmation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompanySecurityEffectiveStartPolicy {
    AuthorizationTime,
}

/// Immutable preview shown to an operator before publication authority exists.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompanySecurityResolutionPreview {
    format_version: u16,
    preview_id: CompanySecurityResolutionPreviewId,
    preview_revision: u64,
    company_parent: CompanyIdentityParentSnapshot,
    candidates: Box<[CompanySecurityCandidateSnapshot]>,
    selected_candidate_ordinal: u16,
    operator_id: SourceIdentifier,
    security_kind: CompanySecurityKind,
    relationship_kind: CompanySecurityRelationshipKind,
    common_equity_suitability: CommonEquitySuitability,
    reviewed_evidence: ReviewedRelationshipEvidence,
    action: CompanySecurityResolutionAction,
    expected_relationship_state: ExpectedCompanySecurityRelationshipState,
    relationship_selection_receipt_digest: EvidenceDigest,
    relationship_selection_knowledge_at: Timestamp,
    effective_start_policy: CompanySecurityEffectiveStartPolicy,
    effective_end: Option<Timestamp>,
    issued_at: Timestamp,
    expires_at: Timestamp,
    preview_digest: EvidenceDigest,
}

impl CompanySecurityResolutionPreview {
    #[allow(
        clippy::too_many_arguments,
        reason = "the preview binds every independently reviewed fact"
    )]
    pub(super) fn new(
        preview_id: CompanySecurityResolutionPreviewId,
        preview_revision: u64,
        company_parent: CompanyIdentityParentSnapshot,
        candidates: Box<[CompanySecurityCandidateSnapshot]>,
        selected_candidate_ordinal: u16,
        operator_id: SourceIdentifier,
        security_kind: CompanySecurityKind,
        relationship_kind: CompanySecurityRelationshipKind,
        common_equity_suitability: CommonEquitySuitability,
        reviewed_evidence: ReviewedRelationshipEvidence,
        action: CompanySecurityResolutionAction,
        expected_relationship_state: ExpectedCompanySecurityRelationshipState,
        relationship_selection_receipt_digest: EvidenceDigest,
        relationship_selection_knowledge_at: Timestamp,
        effective_end: Option<Timestamp>,
        issued_at: Timestamp,
        expires_at: Timestamp,
    ) -> Self {
        Self {
            format_version: COMPANY_SECURITY_RESOLUTION_FORMAT_VERSION,
            preview_id,
            preview_revision,
            company_parent,
            candidates,
            selected_candidate_ordinal,
            operator_id,
            security_kind,
            relationship_kind,
            common_equity_suitability,
            reviewed_evidence,
            action,
            expected_relationship_state,
            relationship_selection_receipt_digest,
            relationship_selection_knowledge_at,
            effective_start_policy: CompanySecurityEffectiveStartPolicy::AuthorizationTime,
            effective_end,
            issued_at,
            expires_at,
            preview_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        }
    }

    pub(super) fn install_digest(&mut self, digest: EvidenceDigest) {
        self.preview_digest = digest;
    }

    pub const fn format_version(&self) -> u16 {
        self.format_version
    }
    pub const fn preview_id(&self) -> CompanySecurityResolutionPreviewId {
        self.preview_id
    }
    pub const fn preview_revision(&self) -> u64 {
        self.preview_revision
    }
    pub const fn company_parent(&self) -> &CompanyIdentityParentSnapshot {
        &self.company_parent
    }
    pub fn candidates(&self) -> &[CompanySecurityCandidateSnapshot] {
        &self.candidates
    }
    pub const fn selected_candidate_ordinal(&self) -> u16 {
        self.selected_candidate_ordinal
    }
    pub const fn operator_id(&self) -> &SourceIdentifier {
        &self.operator_id
    }
    pub const fn security_kind(&self) -> CompanySecurityKind {
        self.security_kind
    }
    pub const fn relationship_kind(&self) -> CompanySecurityRelationshipKind {
        self.relationship_kind
    }
    pub const fn common_equity_suitability(&self) -> CommonEquitySuitability {
        self.common_equity_suitability
    }
    pub const fn reviewed_evidence(&self) -> &ReviewedRelationshipEvidence {
        &self.reviewed_evidence
    }
    pub const fn action(&self) -> &CompanySecurityResolutionAction {
        &self.action
    }
    pub const fn expected_relationship_state(&self) -> ExpectedCompanySecurityRelationshipState {
        self.expected_relationship_state
    }
    pub const fn relationship_selection_receipt_digest(&self) -> EvidenceDigest {
        self.relationship_selection_receipt_digest
    }
    pub const fn relationship_selection_knowledge_at(&self) -> Timestamp {
        self.relationship_selection_knowledge_at
    }
    pub const fn effective_start_policy(&self) -> CompanySecurityEffectiveStartPolicy {
        self.effective_start_policy
    }
    pub const fn effective_end(&self) -> Option<Timestamp> {
        self.effective_end
    }
    pub const fn issued_at(&self) -> Timestamp {
        self.issued_at
    }
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
    pub const fn preview_digest(&self) -> EvidenceDigest {
        self.preview_digest
    }

    pub(super) fn selected_candidate(&self) -> &CompanySecurityCandidateSnapshot {
        &self.candidates[usize::from(self.selected_candidate_ordinal)]
    }
}

/// Explicit one-use confirmation of an exact preview revision and digest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompanySecurityResolutionConfirmation {
    preview_id: CompanySecurityResolutionPreviewId,
    preview_revision: u64,
    preview_digest: EvidenceDigest,
}

impl CompanySecurityResolutionConfirmation {
    /// The act of constructing and submitting this value is the explicit confirmation.
    pub fn try_new(
        preview_id: CompanySecurityResolutionPreviewId,
        preview_revision: u64,
        preview_digest: EvidenceDigest,
    ) -> Result<Self, CompanySecurityResolutionError> {
        if preview_revision == 0 {
            return Err(CompanySecurityResolutionError::InvalidConfirmation);
        }
        validate_sha256(preview_digest)
            .map_err(|_| CompanySecurityResolutionError::InvalidConfirmation)?;
        Ok(Self {
            preview_id,
            preview_revision,
            preview_digest,
        })
    }

    pub(super) const fn preview_id(&self) -> CompanySecurityResolutionPreviewId {
        self.preview_id
    }
    pub(super) const fn preview_revision(&self) -> u64 {
        self.preview_revision
    }
    pub(super) const fn preview_digest(&self) -> EvidenceDigest {
        self.preview_digest
    }
}

pub(super) fn validate_sha256(
    digest: EvidenceDigest,
) -> Result<(), CompanySecurityResolutionError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
        Err(CompanySecurityResolutionError::InvalidRequest)
    } else {
        Ok(())
    }
}
