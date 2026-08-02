//! Canonical selected-authority evidence and decision contracts.

use std::cmp::Ordering;

use market_squawk_domain::{EvidenceDigest, Timestamp};

use super::canonical;
use super::graph::{ResearchUseGraph, ResearchUseSourceInput, compare_sources};
use super::model::{
    ResearchUse, ResearchUseDecisionDigest, ResearchUseError, ResearchUseGraphDigest,
    ResearchUseLimits,
};

/// Exact selected source and research-grant evidence for one allowed contribution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResearchUseAuthorityEvidence {
    pub(super) source: ResearchUseSourceInput,
    pub(super) rights_fingerprint: [u8; 32],
    pub(super) rights_basis_digest: [u8; 32],
    pub(super) authorization_evidence: EvidenceDigest,
    pub(super) rights_expires_at: Option<Timestamp>,
    pub(super) research_grant_id: [u8; 32],
    pub(super) grant_evidence: EvidenceDigest,
    pub(super) grant_expires_at: Option<Timestamp>,
    pub(super) revocation_frontier: u64,
}

impl ResearchUseAuthorityEvidence {
    /// Constructs complete selected authority evidence without inferring any downstream use.
    #[allow(
        clippy::too_many_arguments,
        reason = "all retained authority identities are mandatory"
    )]
    pub fn try_new(
        source: ResearchUseSourceInput,
        rights_fingerprint: [u8; 32],
        rights_basis_digest: [u8; 32],
        authorization_evidence: EvidenceDigest,
        rights_expires_at: Option<Timestamp>,
        research_grant_id: [u8; 32],
        grant_evidence: EvidenceDigest,
        grant_expires_at: Option<Timestamp>,
        revocation_frontier: u64,
    ) -> Result<Self, ResearchUseError> {
        if rights_fingerprint != source.rights_id()
            || rights_basis_digest == [0; 32]
            || research_grant_id == [0; 32]
        {
            return Err(ResearchUseError::InvalidAuthorityEvidence);
        }
        Ok(Self {
            source,
            rights_fingerprint,
            rights_basis_digest,
            authorization_evidence,
            rights_expires_at,
            research_grant_id,
            grant_evidence,
            grant_expires_at,
            revocation_frontier,
        })
    }

    pub(crate) const fn rights_expires_at(&self) -> Option<Timestamp> {
        self.rights_expires_at
    }

    pub(crate) const fn research_grant_id(&self) -> [u8; 32] {
        self.research_grant_id
    }

    pub(crate) const fn grant_expires_at(&self) -> Option<Timestamp> {
        self.grant_expires_at
    }

    pub(crate) const fn revocation_frontier(&self) -> u64 {
        self.revocation_frontier
    }
}

/// Closed fail-closed denial reason retained with a decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResearchUseDenialReason {
    /// At least one direct source had no grant for the requested use.
    MissingGrant,
    /// A required source-rights or research grant had expired.
    Expired,
    /// A required research grant had been revoked.
    Revoked,
    /// Retained lineage or source authority was malformed or inconsistent.
    CorruptAuthority,
    /// Traversal exceeded a caller or process limit.
    LimitExceeded,
    /// The caller cancelled traversal.
    Cancelled,
    /// Traversal exceeded its bounded deadline.
    DeadlineExceeded,
}

impl ResearchUseDenialReason {
    pub(super) const fn tag(self) -> u8 {
        match self {
            Self::MissingGrant => 1,
            Self::Expired => 2,
            Self::Revoked => 3,
            Self::CorruptAuthority => 4,
            Self::LimitExceeded => 5,
            Self::Cancelled => 6,
            Self::DeadlineExceeded => 7,
        }
    }
}

/// Outcome retained by one research-use decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResearchUseDecisionOutcome {
    /// Every transitive direct source independently admitted the requested use.
    Allowed,
    /// The whole request was denied with one closed reason.
    Denied(ResearchUseDenialReason),
}

/// Canonical evidence for one bounded research-use decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResearchUseDecisionInput {
    pub(super) graph_digest: ResearchUseGraphDigest,
    pub(super) requested_use: ResearchUse,
    pub(super) policy_version: u32,
    pub(super) evaluated_at: Timestamp,
    pub(super) expires_at: Option<Timestamp>,
    pub(super) outcome: ResearchUseDecisionOutcome,
    pub(super) authorities: Box<[ResearchUseAuthorityEvidence]>,
    pub(super) limits: ResearchUseLimits,
    digest: ResearchUseDecisionDigest,
}

impl ResearchUseDecisionInput {
    /// Validates, canonicalizes, and hashes exact selected transitive authority evidence.
    #[allow(
        clippy::too_many_arguments,
        reason = "decision evidence is intentionally explicit"
    )]
    pub fn try_new(
        graph: &ResearchUseGraph,
        requested_use: ResearchUse,
        policy_version: u32,
        evaluated_at: Timestamp,
        expires_at: Option<Timestamp>,
        outcome: ResearchUseDecisionOutcome,
        mut authorities: Vec<ResearchUseAuthorityEvidence>,
    ) -> Result<Self, ResearchUseError> {
        let limits = graph.limits();
        if policy_version != 1 || authorities.len() > limits.max_sources() {
            return Err(ResearchUseError::InvalidDecision);
        }
        let graph_digest = graph.digest();
        match outcome {
            ResearchUseDecisionOutcome::Allowed => {
                let expiry = expires_at.ok_or(ResearchUseError::InvalidDecision)?;
                let lifetime_nanos = i64::try_from(limits.permit_lifetime().as_nanos())
                    .map_err(|_| ResearchUseError::CanonicalEncodingOverflow)?;
                let maximum_expiry = evaluated_at
                    .checked_add_nanos(lifetime_nanos)
                    .map_err(|_| ResearchUseError::InvalidDecision)?;
                if authorities.is_empty()
                    || authorities.len() != graph.sources().len()
                    || expiry <= evaluated_at
                    || expiry > maximum_expiry
                    || authorities.iter().any(|authority| {
                        authority
                            .rights_expires_at
                            .is_some_and(|value| value <= evaluated_at || expiry > value)
                            || authority
                                .grant_expires_at
                                .is_some_and(|value| value <= evaluated_at || expiry > value)
                    })
                {
                    return Err(ResearchUseError::InvalidDecision);
                }
            }
            ResearchUseDecisionOutcome::Denied(_) if expires_at.is_some() => {
                return Err(ResearchUseError::InvalidDecision);
            }
            ResearchUseDecisionOutcome::Denied(_) => {}
        }
        authorities.sort_unstable_by(compare_authorities);
        if authorities.windows(2).any(|pair| {
            pair[0].source.generation_sequence() == pair[1].source.generation_sequence()
        }) {
            return Err(ResearchUseError::DuplicateAuthorityEvidence);
        }
        if authorities.iter().any(|authority| {
            graph
                .sources()
                .binary_search_by(|source| compare_sources(source, &authority.source))
                .is_err()
        }) {
            return Err(ResearchUseError::InvalidDecision);
        }
        let mut decision = Self {
            graph_digest,
            requested_use,
            policy_version,
            evaluated_at,
            expires_at,
            outcome,
            authorities: authorities.into_boxed_slice(),
            limits,
            digest: ResearchUseDecisionDigest::from_canonical([0; 32]),
        };
        decision.digest = canonical::decision_digest(&decision)?;
        Ok(decision)
    }

    /// Returns the exact canonical decision identity.
    pub const fn digest(&self) -> ResearchUseDecisionDigest {
        self.digest
    }

    /// Returns the exact graph identity evaluated by this decision.
    pub(crate) const fn graph_digest(&self) -> ResearchUseGraphDigest {
        self.graph_digest
    }

    /// Returns the independently evaluated downstream use.
    pub(crate) const fn requested_use(&self) -> ResearchUse {
        self.requested_use
    }

    /// Returns exclusive permit expiry for an allowed decision.
    pub(crate) const fn expires_at(&self) -> Option<Timestamp> {
        self.expires_at
    }

    /// Returns the closed decision outcome.
    pub(crate) const fn outcome(&self) -> ResearchUseDecisionOutcome {
        self.outcome
    }

    pub(crate) const fn policy_version(&self) -> u32 {
        self.policy_version
    }

    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    pub(crate) const fn limits(&self) -> ResearchUseLimits {
        self.limits
    }
}

fn compare_authorities(
    left: &ResearchUseAuthorityEvidence,
    right: &ResearchUseAuthorityEvidence,
) -> Ordering {
    compare_sources(&left.source, &right.source)
        .then_with(|| left.research_grant_id.cmp(&right.research_grant_id))
}
