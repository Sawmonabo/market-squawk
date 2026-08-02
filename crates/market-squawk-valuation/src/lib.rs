#![forbid(unsafe_code)]
//! Deterministic ASC 820 and IFRS 13 fair-value classification and approval.

use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// Invalid canonical text representation of a fair-value content identity.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("fair-value identity must be exactly 64 lowercase hexadecimal characters")]
pub struct FairValueIdParseError;

macro_rules! digest_id {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(pub(crate) [u8; 32]);

        impl $name {
            /// Returns the exact SHA-256 content identity.
            pub const fn bytes(self) -> [u8; 32] {
                self.0
            }
        }

        impl ::std::fmt::Debug for $name {
            fn fmt(
                &self,
                formatter: &mut ::std::fmt::Formatter<'_>,
            ) -> ::std::fmt::Result {
                formatter.write_str(concat!(stringify!($name), "([SHA-256])"))
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(
                &self,
                formatter: &mut ::std::fmt::Formatter<'_>,
            ) -> ::std::fmt::Result {
                crate::format_digest_id(self.0, formatter)
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = crate::FairValueIdParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                crate::parse_digest_id(value).map(Self)
            }
        }
    };
}

/// Typed construction, classification, workflow, and bounded-service failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum FairValueError {
    /// A bounded actor identity is empty, too long, or contains forbidden characters.
    #[error("fair-value actor identity is invalid")]
    InvalidActorId,
    /// A bounded justification or revocation reason is invalid.
    #[error("fair-value explanatory text is invalid")]
    InvalidText,
    /// Currency amount and declared decimal scale are inconsistent.
    #[error("fair-value amount or scale is invalid")]
    InvalidAmount,
    /// Source, availability, ingestion, preparation, or measurement times are inconsistent.
    #[error("fair-value evidence or workflow time ordering is invalid")]
    InvalidTime,
    /// A source payload digest uses a reserved all-zero identity.
    #[error("fair-value evidence digest is invalid")]
    InvalidEvidenceDigest,
    /// Instrument identity and relationship fields contradict one another.
    #[error("fair-value instrument relationship is invalid")]
    InvalidInstrumentRelationship,
    /// A producer receipt is missing, incompatible, or fails its authority contract.
    #[error("fair-value producer evidence is invalid")]
    InvalidProducerEvidence,
    /// The selected producer object does not contain an instrument-scoped value.
    #[error("fair-value producer evidence has no selected instrument")]
    MissingProducerInstrument,
    /// A comparable, proxy, adjusted, or unobservable input-use assessment is inconsistent.
    #[error("fair-value input-use assessment is invalid")]
    InvalidInputAssessment,
    /// A reporting-entity market-access assessment is incomplete or does not match the market.
    #[error("fair-value market-access assessment is invalid")]
    InvalidMarketAccessAssessment,
    /// A measurement is empty, inconsistent, or not in canonical form.
    #[error("fair-value measurement is invalid")]
    InvalidMeasurement,
    /// The code-owned ruleset parameters are invalid.
    #[error("fair-value classification ruleset is invalid")]
    InvalidRuleset,
    /// A collection exceeds a caller-selected bound.
    #[error("fair-value {resource} count {observed} exceeds limit {limit}")]
    LimitExceeded {
        /// Bounded resource family.
        resource: &'static str,
        /// Submitted or retained count.
        observed: usize,
        /// Caller-selected bound.
        limit: usize,
    },
    /// Retained service memory would exceed the caller-selected bound.
    #[error("fair-value retained bytes {observed} exceed limit {limit}")]
    RetainedBytesExceeded {
        /// Bytes that would be retained.
        observed: usize,
        /// Caller-selected byte bound.
        limit: usize,
    },
    /// Checked size or sequence arithmetic overflowed.
    #[error("fair-value checked arithmetic failed")]
    Arithmetic,
    /// A measurement contains the same immutable input more than once.
    #[error("fair-value measurement contains a duplicate input")]
    DuplicateInput,
    /// A requested immutable measurement is not retained.
    #[error("fair-value measurement was not found")]
    MeasurementNotFound,
    /// A requested immutable classification decision is not retained.
    #[error("fair-value decision was not found")]
    DecisionNotFound,
    /// A requested immutable approval is not retained.
    #[error("fair-value approval was not found")]
    ApprovalNotFound,
    /// An override is redundant, unclassified, expired, or otherwise invalid.
    #[error("fair-value override is invalid")]
    InvalidOverride,
    /// The same actor attempted to prepare and approve a governed decision.
    #[error("fair-value preparer and approver must be different actors")]
    SeparationOfDuties,
    /// Approval start/expiry does not fit the decision and override lifetime.
    #[error("fair-value approval window is invalid")]
    InvalidApprovalWindow,
    /// An immutable approval already has an immutable revocation record.
    #[error("fair-value approval is already revoked")]
    AlreadyRevoked,
    /// A revocation precedes the approval it revokes.
    #[error("fair-value revocation time is invalid")]
    InvalidRevocationTime,
    /// A requested result bound is zero or above the configured service maximum.
    #[error("fair-value query limit {requested} exceeds limit {limit}")]
    QueryLimitExceeded {
        /// Requested rows.
        requested: usize,
        /// Configured maximum rows.
        limit: usize,
    },
    /// The local catalog rejected an otherwise validated fair-value operation.
    #[error("fair-value catalog persistence failed")]
    Persistence,
    /// Durable fair-value records failed canonical decode or semantic recomputation.
    #[error("durable fair-value state is corrupt or incomplete")]
    CorruptPersistence,
}

mod access;
mod approval;
mod assessment;
mod evidence;
mod measurement;
mod persistence;
mod rules;
mod service;

pub use access::{ApprovedMarketAccess, MarketAccessAssessmentId};
pub use approval::{
    ApprovalRevocation, ApprovalRevocationId, ApprovalStatus, OverrideId, OverrideProposal,
    ValuationApproval, ValuationApprovalId, ValuationOverride,
};
pub use assessment::{InputUseAssessment, InputUseAssessmentHash};
pub use evidence::{
    EvidenceOrigin, EvidenceVerification, FairValueEvidence, FairValueEvidenceHash,
};
pub use measurement::{
    ActorId, CommittedMarketInputRequest, InputId, InputInstrumentRelation, InputObservability,
    InputSignificance, MarketAccess, MarketActivity, MarketActivityPolicy,
    MarketActivityPolicyHash, MarketPriceSelection, MeasurementId, PriceAdjustment,
    ValuationAmount, ValuationInput, ValuationMeasurement, ValuationMeasurementSpec,
    ValuationMethod,
};
pub use rules::{
    ClassificationDecision, ClassificationRuleset, DecisionBasis, DecisionId, DecisionReason,
    DecisionReasonCode, Predicate, PredicateResult, RulesetHash,
};
pub use service::{
    AuditEventId, AuditEventKind, FairValueAuditEvent, FairValueLimitInput, FairValueLimits,
    FairValueService,
};

pub(crate) struct CanonicalHasher(Sha256);

impl CanonicalHasher {
    pub(crate) fn new(domain: &[u8]) -> Self {
        let mut hash = Sha256::new();
        hash.update((domain.len() as u64).to_be_bytes());
        hash.update(domain);
        Self(hash)
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.0.update([value]);
    }

    pub(crate) fn u32(&mut self, value: u32) {
        self.0.update(value.to_be_bytes());
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.0.update(value.to_be_bytes());
    }

    pub(crate) fn i64(&mut self, value: i64) {
        self.0.update(value.to_be_bytes());
    }

    pub(crate) fn bytes(&mut self, value: &[u8]) {
        self.0.update((value.len() as u64).to_be_bytes());
        self.0.update(value);
    }

    pub(crate) fn fixed(&mut self, value: [u8; 32]) {
        self.0.update(value);
    }

    pub(crate) fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

pub(crate) fn checked_add(left: usize, right: usize) -> Result<usize, FairValueError> {
    left.checked_add(right).ok_or(FairValueError::Arithmetic)
}

fn format_digest_id(digest: [u8; 32], formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    for byte in digest {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

fn parse_digest_id(value: &str) -> Result<[u8; 32], FairValueIdParseError> {
    if value.len() != 64 {
        return Err(FairValueIdParseError);
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_lower_hex(pair[0])?;
        let low = decode_lower_hex(pair[1])?;
        digest[index] = (high << 4) | low;
    }
    Ok(digest)
}

const fn decode_lower_hex(value: u8) -> Result<u8, FairValueIdParseError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(FairValueIdParseError),
    }
}
