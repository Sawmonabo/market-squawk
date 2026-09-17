//! Stable identities shared by immutable decision records.

use std::fmt;

use market_squawk_domain::EvidenceDigest;

/// Maximum UTF-8 bytes retained by a decision identity.
pub const MAX_DECISION_ID_BYTES: usize = 128;

/// A decision value failed invariant validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionContractError {
    /// A stable identity was empty, oversized, or not canonical lowercase ASCII.
    InvalidIdentifier,
    /// The all-zero digest sentinel was supplied as content identity.
    ReservedDigest,
    /// A screen run had no feature binding or exceeded its feature bound.
    InvalidScreenFeatureCount,
    /// A screen run contained more than one semantic binding for the same feature key.
    DuplicateScreenFeature,
    /// A later decision record preceded evidence it claims to consume.
    InvalidTimeOrder,
    /// Exact financial values did not use one currency.
    CurrencyMismatch,
    /// A lower price exceeded an upper price or target cases were not ordered.
    InvalidPriceOrder,
    /// A target-activation review occurred at or after target expiry.
    ExpiredActivation,
    /// A configured count, statistical limit, or retained value exceeded its closed bound.
    InvalidBound,
    /// A screen selected a feature absent from the code-owned point-in-time registry.
    UnknownScreenFeature,
    /// Screen predicates, ranking, universe, or point-in-time semantics were inconsistent.
    InvalidScreen,
    /// Candidate observations, constraints, evidence, or score inputs were inconsistent.
    InvalidCandidate,
    /// A bounded decision narrative was empty, oversized, or contained a control character.
    InvalidText,
    /// Target governance did not match its financial, temporal, evidence, or supersession core.
    InvalidTargetGovernance,
}

impl fmt::Display for DecisionContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentifier => formatter.write_str("decision identity is invalid"),
            Self::ReservedDigest => formatter.write_str("decision content digest is reserved"),
            Self::InvalidScreenFeatureCount => {
                formatter.write_str("screen feature-binding count is invalid")
            }
            Self::DuplicateScreenFeature => {
                formatter.write_str("screen contains a duplicate feature binding")
            }
            Self::InvalidTimeOrder => formatter.write_str("decision time ordering is invalid"),
            Self::CurrencyMismatch => {
                formatter.write_str("decision financial values use different currencies")
            }
            Self::InvalidPriceOrder => formatter.write_str("decision prices are not ordered"),
            Self::ExpiredActivation => formatter.write_str("an expired target cannot be activated"),
            Self::InvalidBound => formatter.write_str("decision resource bound is invalid"),
            Self::UnknownScreenFeature => {
                formatter.write_str("screen feature is not code-owned point-in-time semantics")
            }
            Self::InvalidScreen => formatter.write_str("saved screen semantics are invalid"),
            Self::InvalidCandidate => formatter.write_str("candidate evidence is invalid"),
            Self::InvalidText => formatter.write_str("decision narrative is invalid"),
            Self::InvalidTargetGovernance => {
                formatter.write_str("target governance is inconsistent")
            }
        }
    }
}

impl std::error::Error for DecisionContractError {}

macro_rules! decision_id {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Box<str>);

        impl $name {
            /// Constructs a bounded canonical identity.
            ///
            /// Identities begin with a lowercase ASCII letter and otherwise contain lowercase
            /// ASCII letters, digits, `.`, `_`, or `-`.
            ///
            /// # Errors
            ///
            /// Returns [`DecisionContractError::InvalidIdentifier`] for noncanonical input.
            pub fn try_new(value: impl AsRef<str>) -> Result<Self, DecisionContractError> {
                let value = value.as_ref();
                validate_identifier(value)?;
                Ok(Self(value.into()))
            }

            /// Returns the canonical identity without allocation.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

decision_id!(
    /// Stable series identity of a saved screen; a revision selects immutable semantics.
    ScreenId
);
decision_id!(
    /// Stable identity of one point-in-time screen execution.
    ScreenRunId
);
decision_id!(
    /// Stable identity of one candidate selected by a screen run.
    CandidateId
);
decision_id!(
    /// Stable identity of one immutable evidence dossier.
    DossierId
);
decision_id!(
    /// Stable series identity of an investment target set.
    InvestmentTargetSetId
);
decision_id!(
    /// Stable identity of one review appended to a target revision.
    TargetReviewId
);
decision_id!(
    /// Stable identity of one invalidation appended to a target revision.
    TargetInvalidationId
);
decision_id!(
    /// Bounded human or system actor identity used in decision review evidence.
    DecisionActorId
);

/// Nonzero algorithm-qualified identity of exact decision content.
///
/// This validates representation only. A digest does not grant dataset, model, portfolio,
/// valuation, review, or execution authority.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DecisionContentDigest(EvidenceDigest);

impl DecisionContentDigest {
    /// Constructs a nonzero content identity from an already computed digest.
    ///
    /// # Errors
    ///
    /// Returns [`DecisionContractError::ReservedDigest`] for the all-zero sentinel.
    pub fn try_new(digest: EvidenceDigest) -> Result<Self, DecisionContractError> {
        if digest.bytes() == [0; 32] {
            Err(DecisionContractError::ReservedDigest)
        } else {
            Ok(Self(digest))
        }
    }

    /// Returns the algorithm-qualified source digest.
    #[must_use]
    pub const fn evidence_digest(self) -> EvidenceDigest {
        self.0
    }
}

fn validate_identifier(value: &str) -> Result<(), DecisionContractError> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || bytes.len() > MAX_DECISION_ID_BYTES
        || !bytes[0].is_ascii_lowercase()
        || !bytes.iter().skip(1).all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        Err(DecisionContractError::InvalidIdentifier)
    } else {
        Ok(())
    }
}
