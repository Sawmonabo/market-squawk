//! Manual canonical SHA-256 encoding for immutable operator-resolution previews.

use market_squawk_domain::{
    CommonEquitySuitability, CompanySecurityKind, CompanySecurityRelationshipKind, DigestAlgorithm,
    EvidenceDigest, ExactPayloadEvidence, IdentifierEntitlement, RevisionBoundPayloadEvidence,
    Timestamp,
};
use sha2::{Digest as _, Sha256};

use super::CompanySecurityResolutionError;
use super::model::{
    CompanySecurityEffectiveStartPolicy, CompanySecurityResolutionAction,
    CompanySecurityResolutionPreview, ExpectedCompanySecurityRelationshipState,
    OperatorEvidenceRightsDisposition, ReviewedSecurityCandidateDisposition,
};

const PREVIEW_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/application/company-security-resolution-preview/v2\0";

pub(super) fn preview_digest(
    preview: &CompanySecurityResolutionPreview,
) -> Result<EvidenceDigest, CompanySecurityResolutionError> {
    let mut digest = Sha256::new();
    digest.update(PREVIEW_DIGEST_DOMAIN);
    digest.update(preview.format_version().to_be_bytes());
    digest.update(preview.preview_id().as_uuid().as_bytes());
    digest.update(preview.preview_revision().to_be_bytes());

    let company = preview.company_parent();
    hash_text(&mut digest, company.company_source_id().as_str())?;
    hash_text(&mut digest, company.provider_company_id().as_str())?;
    hash_text(&mut digest, company.company_surface().database_name())?;
    hash_evidence_digest(&mut digest, company.company_observation_digest());
    hash_text(&mut digest, company.conformed_name_for_display())?;
    for value in [
        company.received_at(),
        company.available_at(),
        company.ingested_at(),
        company.completed_at(),
    ] {
        hash_timestamp(&mut digest, value);
    }

    hash_count(&mut digest, preview.candidates().len())?;
    for candidate in preview.candidates() {
        digest.update(candidate.ordinal().to_be_bytes());
        digest.update(candidate.instrument_id().as_uuid().as_bytes());
        hash_evidence_digest(&mut digest, candidate.market_revision_digest());
        hash_revision_bound_evidence(&mut digest, candidate.market_reference_evidence())?;
        digest.update(candidate.market_revision_sequence().to_be_bytes());
        hash_timestamp(&mut digest, candidate.market_published_at());
        hash_timestamp(
            &mut digest,
            candidate.market_effective_interval().starts_at(),
        );
        hash_optional_timestamp(&mut digest, candidate.market_effective_interval().ends_at());
        match candidate.disposition() {
            ReviewedSecurityCandidateDisposition::Selected => digest.update([1]),
            ReviewedSecurityCandidateDisposition::Excluded { reason } => {
                digest.update([2]);
                hash_text(&mut digest, reason.as_str())?;
            }
        }
    }
    digest.update(preview.selected_candidate_ordinal().to_be_bytes());
    hash_text(&mut digest, preview.operator_id().as_str())?;
    digest.update([security_kind_tag(preview.security_kind())]);
    digest.update([relationship_kind_tag(preview.relationship_kind())]);
    digest.update([suitability_tag(preview.common_equity_suitability())]);

    let reviewed = preview.reviewed_evidence();
    hash_exact_payload_evidence(&mut digest, reviewed.evidence())?;
    hash_timestamp(&mut digest, reviewed.evidence_available_at());
    hash_timestamp(&mut digest, reviewed.reviewed_at());
    hash_text(&mut digest, reviewed.rights().policy_id().as_str())?;
    digest.update([entitlement_tag(reviewed.rights().entitlement())]);
    hash_text(&mut digest, reviewed.rights().terms_reference().as_str())?;
    hash_evidence_digest(&mut digest, reviewed.rights_revision_digest());
    hash_timestamp(&mut digest, reviewed.rights_valid_until());
    match reviewed.rights_disposition() {
        OperatorEvidenceRightsDisposition::Admitted => digest.update([1]),
        OperatorEvidenceRightsDisposition::Revoked { revoked_at } => {
            digest.update([2]);
            hash_timestamp(&mut digest, revoked_at);
        }
    }

    match preview.action() {
        CompanySecurityResolutionAction::Authorize => digest.update([1]),
        CompanySecurityResolutionAction::Revoke { reason } => {
            digest.update([2]);
            hash_text(&mut digest, reason.as_str())?;
        }
    }
    match preview.expected_relationship_state() {
        ExpectedCompanySecurityRelationshipState::Absent => digest.update([1]),
        ExpectedCompanySecurityRelationshipState::Current { link_digest } => {
            digest.update([2]);
            hash_evidence_digest(&mut digest, link_digest);
        }
    }
    hash_evidence_digest(&mut digest, preview.relationship_selection_receipt_digest());
    hash_timestamp(&mut digest, preview.relationship_selection_knowledge_at());
    digest.update([match preview.effective_start_policy() {
        CompanySecurityEffectiveStartPolicy::AuthorizationTime => 1,
    }]);
    hash_optional_timestamp(&mut digest, preview.effective_end());
    hash_timestamp(&mut digest, preview.issued_at());
    hash_timestamp(&mut digest, preview.expires_at());
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_revision_bound_evidence(
    digest: &mut Sha256,
    evidence: &RevisionBoundPayloadEvidence,
) -> Result<(), CompanySecurityResolutionError> {
    hash_text(
        digest,
        evidence.metadata_revision().as_source_identifier().as_str(),
    )?;
    hash_exact_payload_evidence(digest, evidence.payload_evidence())
}

fn hash_exact_payload_evidence(
    digest: &mut Sha256,
    evidence: &ExactPayloadEvidence,
) -> Result<(), CompanySecurityResolutionError> {
    hash_evidence_digest(digest, evidence.content_digest());
    match evidence.version_pinned_locator() {
        Some(locator) => {
            digest.update([1]);
            hash_text(digest, locator.reference().as_str())?;
            hash_text(digest, locator.version().as_str())
        }
        None => {
            digest.update([0]);
            Ok(())
        }
    }
}

fn hash_evidence_digest(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(evidence.bytes());
}

fn hash_timestamp(digest: &mut Sha256, value: Timestamp) {
    digest.update(value.unix_nanos().to_be_bytes());
}

fn hash_optional_timestamp(digest: &mut Sha256, value: Option<Timestamp>) {
    match value {
        Some(value) => {
            digest.update([1]);
            hash_timestamp(digest, value);
        }
        None => digest.update([0]),
    }
}

fn hash_text(digest: &mut Sha256, value: &str) -> Result<(), CompanySecurityResolutionError> {
    hash_count(digest, value.len())?;
    digest.update(value.as_bytes());
    Ok(())
}

fn hash_count(digest: &mut Sha256, value: usize) -> Result<(), CompanySecurityResolutionError> {
    let value =
        u64::try_from(value).map_err(|_| CompanySecurityResolutionError::CapacityExceeded)?;
    digest.update(value.to_be_bytes());
    Ok(())
}

const fn security_kind_tag(value: CompanySecurityKind) -> u8 {
    match value {
        CompanySecurityKind::CommonEquity => 1,
        CompanySecurityKind::PreferredEquity => 2,
        CompanySecurityKind::DepositaryReceipt => 3,
        CompanySecurityKind::Debt => 4,
        CompanySecurityKind::FundInterest => 5,
        CompanySecurityKind::Other => 6,
    }
}

const fn relationship_kind_tag(value: CompanySecurityRelationshipKind) -> u8 {
    match value {
        CompanySecurityRelationshipKind::Issuer => 1,
        CompanySecurityRelationshipKind::Guarantor => 2,
        CompanySecurityRelationshipKind::DepositaryUnderlying => 3,
        CompanySecurityRelationshipKind::FundSponsor => 4,
        CompanySecurityRelationshipKind::Other => 5,
    }
}

const fn suitability_tag(value: CommonEquitySuitability) -> u8 {
    match value {
        CommonEquitySuitability::SuitableIssuerCommonEquity => 1,
        CommonEquitySuitability::NotSuitable => 2,
    }
}

const fn entitlement_tag(value: IdentifierEntitlement) -> u8 {
    match value {
        IdentifierEntitlement::UnknownOrRestricted => 1,
        IdentifierEntitlement::PublicDomain => 2,
        IdentifierEntitlement::UserOwned => 3,
        IdentifierEntitlement::LicensedInternalUse => 4,
        IdentifierEntitlement::LicensedRedistribution => 5,
    }
}
