//! Evidence-authorized company-to-security identity relationships.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    CompanyIdentitySurface, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, Figi, IdentifierEntitlement, IdentifierRightsPolicyReference,
    InstrumentId, SchemaVersion, SchemaVersionError, SourceId, SourceIdentifier, Timestamp,
};

/// The security form attached to a company identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompanySecurityKind {
    /// Ordinary voting or non-voting common equity.
    CommonEquity,
    /// Preferred equity.
    PreferredEquity,
    /// A depositary receipt representing another security.
    DepositaryReceipt,
    /// Debt issued or guaranteed by the company.
    Debt,
    /// A fund or pooled-investment interest.
    FundInterest,
    /// Another security form retained without common-equity semantics.
    Other,
}

/// The company's legal/economic relationship to the security.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompanySecurityRelationshipKind {
    /// The company is the security's issuer.
    Issuer,
    /// The company guarantees the security but is not its issuer.
    Guarantor,
    /// The company is the underlying issuer represented by a depositary security.
    DepositaryUnderlying,
    /// The company sponsors or manages a pooled investment.
    FundSponsor,
    /// Another explicitly evidenced relationship.
    Other,
}

/// Whether the relationship is suitable for direct common-equity company analysis.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommonEquitySuitability {
    /// Direct issuer common equity suitable for company-level equity analysis.
    SuitableIssuerCommonEquity,
    /// Not a direct issuer common-equity relationship.
    NotSuitable,
}

/// The only authorities permitted to resolve a company-to-security relationship.
///
/// Ticker, name, exchange, and display-text associations are deliberately absent. They cannot be
/// deserialized or promoted into relationship authority.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompanySecurityResolutionBasis {
    /// A responsible source directly published the company/security crosswalk.
    DirectAuthoritativeCrosswalk {
        /// Source namespace responsible for the crosswalk.
        authority_source_id: SourceId,
        /// Exact source revision or record identity.
        authority_revision: SourceIdentifier,
        /// Exact payload containing the direct crosswalk assertion.
        evidence: ExactPayloadEvidence,
    },
    /// A human operator explicitly approved an evidence-backed resolution.
    OperatorAuthorizedResolution {
        /// Immutable local authorization receipt identity.
        receipt_id: SourceIdentifier,
        /// Bounded operator identity recorded by the authorization workflow.
        operator_id: SourceIdentifier,
        /// Exact evidence bundle reviewed by the operator.
        evidence: ExactPayloadEvidence,
        /// Time the operator issued the resolution receipt.
        authorized_at: Timestamp,
    },
}

impl CompanySecurityResolutionBasis {
    /// Returns the exact evidence establishing the resolution.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        match self {
            Self::DirectAuthoritativeCrosswalk { evidence, .. }
            | Self::OperatorAuthorizedResolution { evidence, .. } => evidence,
        }
    }
}

/// Immutable relationship event transition.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompanySecurityLinkTransition {
    /// First authorized event for the exact company/security key.
    Initial,
    /// Explicit successor to the current event.
    Supersedes {
        /// Exact SHA-256 digest of the current event being replaced.
        previous_link_digest: EvidenceDigest,
    },
    /// Explicit revocation of the current relationship event.
    Revokes {
        /// Exact SHA-256 digest of the current event being revoked.
        previous_link_digest: EvidenceDigest,
        /// Bounded revocation reason identity.
        reason: SourceIdentifier,
    },
}

impl CompanySecurityLinkTransition {
    /// Returns the exact predecessor when this is not an initial event.
    pub const fn previous_link_digest(&self) -> Option<EvidenceDigest> {
        match self {
            Self::Initial => None,
            Self::Supersedes {
                previous_link_digest,
            }
            | Self::Revokes {
                previous_link_digest,
                ..
            } => Some(*previous_link_digest),
        }
    }

    /// Returns whether this event revokes rather than establishes the relationship.
    pub const fn is_revocation(&self) -> bool {
        matches!(self, Self::Revokes { .. })
    }
}

/// Complete checked input for one immutable company/security relationship event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompanySecurityIdentityLinkInput {
    /// Current domain schema.
    pub schema_version: SchemaVersion,
    /// Exact company-observation source namespace.
    pub company_source_id: SourceId,
    /// Exact provider company identity within that source.
    pub provider_company_id: SourceIdentifier,
    /// Exact source surface that authored the company observation.
    pub company_surface: CompanyIdentitySurface,
    /// SHA-256 digest of the exact canonical company observation revision.
    pub company_observation_digest: EvidenceDigest,
    /// Stable FIGI-derived internal instrument identity.
    pub instrument_id: InstrumentId,
    /// Permanent assigned FIGI bound to the market-definition revision.
    pub permanent_figi: Figi,
    /// SHA-256 digest of the exact market-instrument definition revision.
    pub market_instrument_revision_digest: EvidenceDigest,
    /// Security form.
    pub security_kind: CompanySecurityKind,
    /// Company/security relationship.
    pub relationship_kind: CompanySecurityRelationshipKind,
    /// Explicit suitability for direct common-equity analysis.
    pub common_equity_suitability: CommonEquitySuitability,
    /// Direct source authority or an explicit operator authorization receipt.
    pub resolution_basis: CompanySecurityResolutionBasis,
    /// Rights decision for retaining and using only the relationship evidence.
    pub relationship_evidence_rights: IdentifierRightsPolicyReference,
    /// Half-open interval during which the relationship assertion applies.
    pub effective_interval: EffectiveInterval,
    /// Conservative point-in-time availability of the resolution evidence.
    pub available_at: Timestamp,
    /// Local ingestion time for the checked relationship event.
    pub ingested_at: Timestamp,
    /// Initial, explicit successor, or explicit revocation transition.
    pub transition: CompanySecurityLinkTransition,
}

/// Immutable, evidence-authorized company/security relationship event.
///
/// This grants only the identity relationship. It grants no market-data, filing, fundamental,
/// model, redistribution, or execution right; each downstream source remains independently
/// admitted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompanySecurityIdentityLink {
    schema_version: SchemaVersion,
    company_source_id: SourceId,
    provider_company_id: SourceIdentifier,
    company_surface: CompanyIdentitySurface,
    company_observation_digest: EvidenceDigest,
    instrument_id: InstrumentId,
    permanent_figi: Figi,
    market_instrument_revision_digest: EvidenceDigest,
    security_kind: CompanySecurityKind,
    relationship_kind: CompanySecurityRelationshipKind,
    common_equity_suitability: CommonEquitySuitability,
    resolution_basis: CompanySecurityResolutionBasis,
    relationship_evidence_rights: IdentifierRightsPolicyReference,
    effective_interval: EffectiveInterval,
    available_at: Timestamp,
    ingested_at: Timestamp,
    transition: CompanySecurityLinkTransition,
}

impl CompanySecurityIdentityLink {
    /// Constructs one checked relationship event.
    pub fn try_new(
        input: CompanySecurityIdentityLinkInput,
    ) -> Result<Self, CompanySecurityIdentityError> {
        input.schema_version.ensure_supported()?;
        if digest_is_empty(input.company_observation_digest)
            || digest_is_empty(input.market_instrument_revision_digest)
            || digest_is_empty(input.resolution_basis.evidence().content_digest())
            || input
                .transition
                .previous_link_digest()
                .is_some_and(digest_is_empty)
        {
            return Err(CompanySecurityIdentityError::EmptyEvidenceDigest);
        }
        if input.company_observation_digest.algorithm() != DigestAlgorithm::Sha256
            || input.market_instrument_revision_digest.algorithm() != DigestAlgorithm::Sha256
            || input
                .transition
                .previous_link_digest()
                .is_some_and(|digest| digest.algorithm() != DigestAlgorithm::Sha256)
        {
            return Err(CompanySecurityIdentityError::InvalidParentDigestAlgorithm);
        }
        if input.available_at > input.ingested_at {
            return Err(CompanySecurityIdentityError::InvalidTime);
        }
        if input.relationship_evidence_rights.entitlement()
            == IdentifierEntitlement::UnknownOrRestricted
        {
            return Err(CompanySecurityIdentityError::RelationshipEvidenceRightsNotAdmitted);
        }
        let suitable =
            input.common_equity_suitability == CommonEquitySuitability::SuitableIssuerCommonEquity;
        let direct_common = input.security_kind == CompanySecurityKind::CommonEquity
            && input.relationship_kind == CompanySecurityRelationshipKind::Issuer;
        if suitable != direct_common {
            return Err(CompanySecurityIdentityError::InvalidCommonEquitySuitability);
        }
        if let CompanySecurityResolutionBasis::OperatorAuthorizedResolution {
            authorized_at, ..
        } = &input.resolution_basis
        {
            if *authorized_at > input.available_at {
                return Err(CompanySecurityIdentityError::InvalidTime);
            }
        }
        Ok(Self {
            schema_version: input.schema_version,
            company_source_id: input.company_source_id,
            provider_company_id: input.provider_company_id,
            company_surface: input.company_surface,
            company_observation_digest: input.company_observation_digest,
            instrument_id: input.instrument_id,
            permanent_figi: input.permanent_figi,
            market_instrument_revision_digest: input.market_instrument_revision_digest,
            security_kind: input.security_kind,
            relationship_kind: input.relationship_kind,
            common_equity_suitability: input.common_equity_suitability,
            resolution_basis: input.resolution_basis,
            relationship_evidence_rights: input.relationship_evidence_rights,
            effective_interval: input.effective_interval,
            available_at: input.available_at,
            ingested_at: input.ingested_at,
            transition: input.transition,
        })
    }

    /// Returns the domain schema.
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }
    /// Returns the exact company source.
    pub const fn company_source_id(&self) -> &SourceId {
        &self.company_source_id
    }
    /// Returns the exact provider company identity.
    pub const fn provider_company_id(&self) -> &SourceIdentifier {
        &self.provider_company_id
    }
    /// Returns the source surface for the company observation.
    pub const fn company_surface(&self) -> CompanyIdentitySurface {
        self.company_surface
    }
    /// Returns the exact canonical company observation digest.
    pub const fn company_observation_digest(&self) -> EvidenceDigest {
        self.company_observation_digest
    }
    /// Returns the stable FIGI-derived instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Returns the permanent FIGI.
    pub const fn permanent_figi(&self) -> &Figi {
        &self.permanent_figi
    }
    /// Returns the exact market-definition revision digest.
    pub const fn market_instrument_revision_digest(&self) -> EvidenceDigest {
        self.market_instrument_revision_digest
    }
    /// Returns the security form.
    pub const fn security_kind(&self) -> CompanySecurityKind {
        self.security_kind
    }
    /// Returns the company/security relationship.
    pub const fn relationship_kind(&self) -> CompanySecurityRelationshipKind {
        self.relationship_kind
    }
    /// Returns direct common-equity suitability.
    pub const fn common_equity_suitability(&self) -> CommonEquitySuitability {
        self.common_equity_suitability
    }
    /// Returns direct crosswalk or operator authorization authority.
    pub const fn resolution_basis(&self) -> &CompanySecurityResolutionBasis {
        &self.resolution_basis
    }
    /// Returns rights applying only to the relationship evidence.
    pub const fn relationship_evidence_rights(&self) -> &IdentifierRightsPolicyReference {
        &self.relationship_evidence_rights
    }
    /// Returns the relationship's half-open effective interval.
    pub const fn effective_interval(&self) -> EffectiveInterval {
        self.effective_interval
    }
    /// Returns conservative evidence availability.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }
    /// Returns local ingestion time.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }
    /// Returns the immutable lifecycle transition.
    pub const fn transition(&self) -> &CompanySecurityLinkTransition {
        &self.transition
    }
}

impl<'de> Deserialize<'de> for CompanySecurityIdentityLink {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = CompanySecurityIdentityLinkInput::deserialize(deserializer)?;
        Self::try_new(input).map_err(serde::de::Error::custom)
    }
}

/// Company/security relationship construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompanySecurityIdentityError {
    /// A required authority or parent digest was all zeroes.
    EmptyEvidenceDigest,
    /// Availability, authorization, and ingestion times were inconsistent.
    InvalidTime,
    /// Parent and predecessor identities were not canonical SHA-256 digests.
    InvalidParentDigestAlgorithm,
    /// Common-equity suitability did not match a direct issuer common-equity relationship.
    InvalidCommonEquitySuitability,
    /// Relationship evidence rights remain unknown or restricted.
    RelationshipEvidenceRightsNotAdmitted,
    /// The domain schema is unsupported.
    Schema(SchemaVersionError),
}

impl fmt::Display for CompanySecurityIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyEvidenceDigest => {
                formatter.write_str("company/security evidence digest is empty")
            }
            Self::InvalidTime => {
                formatter.write_str("company/security time coordinates are inconsistent")
            }
            Self::InvalidParentDigestAlgorithm => {
                formatter.write_str("company/security parent identities must use canonical SHA-256")
            }
            Self::InvalidCommonEquitySuitability => formatter
                .write_str("common-equity suitability does not match the security relationship"),
            Self::RelationshipEvidenceRightsNotAdmitted => formatter
                .write_str("company/security relationship evidence rights are not admitted"),
            Self::Schema(error) => {
                write!(formatter, "company/security schema is unsupported: {error}")
            }
        }
    }
}

impl std::error::Error for CompanySecurityIdentityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Schema(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SchemaVersionError> for CompanySecurityIdentityError {
    fn from(value: SchemaVersionError) -> Self {
        Self::Schema(value)
    }
}

fn digest_is_empty(digest: EvidenceDigest) -> bool {
    digest.bytes() == [0; 32]
}
