//! Deterministic, evidence-bound investment research proposals.
//!
//! This module deliberately stops at immutable research output. It owns neither order intent nor
//! execution authority, and none of its constructors accept a caller-selected action, price
//! ladder, or confidence value.

use std::fmt;

use market_squawk_modeling::ForecastVintageId;

mod authority;
mod digest;
mod evidence;
mod output;
mod policy;

pub use authority::InvestmentProposalAuthority;
pub use evidence::{
    ChronologicalOutOfSampleEvidence, CostAdjustedPitBacktestEvidence, FinancialModelEvidence,
    FinancialModelValueRange, ForecastCalibrationSummary, ForecastPriceRanges,
    HarmonicPatternEvidenceReceipt, InvestmentAnalysisEvidence, InvestmentAnalysisEvidenceInput,
    LiquidityEvidence, MarketReferenceAdjustmentBasis, MarketReferenceEvidence,
    MarketReferencePriceKind, PortfolioPositionState, PortfolioRiskEvidence, PriceForecastEvidence,
    ProposalEvidenceWindow, ValuationEvidence,
};
pub use output::{
    GeneratedInvestmentProposal, GeneratedPriceLadder, InvestmentProposalDecision,
    NoActionInvestmentProposal, NoActionReason, ProposalExecutionEligibility, ProposalInvalidator,
    ProposalUnavailableReason, RecommendationAction, UnavailableInvestmentAnalysis,
};
pub use policy::{
    ActionSpecificCostAvailability, ProposalTimeBenchmarkAvailability, RecommendationConfidence,
    RecommendationConfidenceComponent, RecommendationConfidenceComponentKind,
    RecommendationConfidenceMeaning, RecommendationPolicy,
};

/// Parts per million used by forecast calibration and policy-weighted reliability contracts.
pub const CONFIDENCE_PARTS_PER_MILLION: u32 = 1_000_000;
/// Closed number of components in the V1 policy-weighted reliability calculation.
pub const RECOMMENDATION_CONFIDENCE_COMPONENT_COUNT: usize = 6;
/// Closed number of typed invalidators retained on one generated no-action result.
pub const MAX_PROPOSAL_INVALIDATORS: usize = 1;
/// Number of fixed assumptions carried by the V1 policy.
pub const RECOMMENDATION_ASSUMPTION_COUNT: usize = 3;
/// Number of fixed invalidation conditions carried by the V1 policy.
pub const RECOMMENDATION_INVALIDATION_COUNT: usize = 3;
/// Number of fixed limitations carried by the V1 policy.
pub const RECOMMENDATION_LIMITATION_COUNT: usize = 3;

/// A proposal contract or deterministic derivation failed validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvestmentProposalError {
    /// A required digest used the all-zero reserved sentinel.
    ReservedIdentity,
    /// Evidence publication, observation, expiry, horizon, or proposal times were inconsistent.
    InvalidTimeOrder,
    /// A price was nonpositive, mixed-currency, or not strictly ordered where required.
    InvalidPrice,
    /// A non-price evidence metric violated its declared sign or structural semantics.
    InvalidEvidenceMetric,
    /// A fair-value selection was incomplete, detached, or inconsistent with its exact receipt.
    InvalidValuationSelection,
    /// A parts-per-million quantity exceeded one million.
    InvalidPartsPerMillion,
    /// Code-owned policy semantics or narratives violated their closed invariants.
    InvalidPolicy,
    /// Exact checked arithmetic or timestamp derivation overflowed.
    ArithmeticOverflow,
    /// Recovered policy content does not match the code-owned version and digest.
    PolicyIdentityMismatch,
    /// Recovered output does not reproduce the persisted analysis/proposal identity.
    ProposalIdentityMismatch,
    /// A deterministic recovery expected a different output family.
    ProposalKindMismatch,
}

impl fmt::Display for InvestmentProposalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReservedIdentity => formatter.write_str("proposal identity is reserved"),
            Self::InvalidTimeOrder => formatter.write_str("proposal time ordering is invalid"),
            Self::InvalidPrice => formatter.write_str("proposal prices are invalid"),
            Self::InvalidEvidenceMetric => {
                formatter.write_str("proposal evidence metric is invalid")
            }
            Self::InvalidValuationSelection => {
                formatter.write_str("fair-value selection evidence is invalid")
            }
            Self::InvalidPartsPerMillion => {
                formatter.write_str("proposal parts-per-million value is invalid")
            }
            Self::InvalidPolicy => formatter.write_str("recommendation policy is invalid"),
            Self::ArithmeticOverflow => formatter.write_str("proposal arithmetic overflowed"),
            Self::PolicyIdentityMismatch => {
                formatter.write_str("recommendation policy identity does not match")
            }
            Self::ProposalIdentityMismatch => {
                formatter.write_str("investment proposal identity does not reproduce")
            }
            Self::ProposalKindMismatch => {
                formatter.write_str("recovered investment proposal has a different kind")
            }
        }
    }
}

impl std::error::Error for InvestmentProposalError {}

macro_rules! digest_identity {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 32]);

        impl $name {
            /// Reconstructs a nonzero fixed-width identity from persisted bytes.
            ///
            /// # Errors
            ///
            /// Rejects the all-zero sentinel.
            pub fn try_from_bytes(
                bytes: [u8; 32],
            ) -> Result<Self, InvestmentProposalError> {
                if bytes == [0; 32] {
                    Err(InvestmentProposalError::ReservedIdentity)
                } else {
                    Ok(Self(bytes))
                }
            }

            /// Returns the complete stable identity bytes.
            #[must_use]
            pub const fn bytes(self) -> [u8; 32] {
                self.0
            }
        }
    };
}

digest_identity!(
    /// Commitment to every code-owned recommendation-policy semantic.
    RecommendationPolicyDigest
);
digest_identity!(
    /// Commitment to the complete admitted-or-unavailable analysis evidence envelope.
    RecommendationEvidenceDigest
);
digest_identity!(
    /// Stable identity of one instrument/account/as-of analysis under one exact policy.
    InvestmentAnalysisId
);
digest_identity!(
    /// Stable identity of one reproducible generated or no-action proposal.
    InvestmentProposalId
);
digest_identity!(
    /// Commitment to the exact deterministic derivation and its output.
    RecommendationDerivationDigest
);
digest_identity!(
    /// Stable content address of one forecast vintage used by the proposal policy.
    ProposalForecastVintageId
);

impl ProposalForecastVintageId {
    /// Converts the modeling authority's content-addressed vintage identity.
    ///
    /// # Errors
    ///
    /// Rejects its reserved all-zero sentinel.
    pub fn try_from_forecast_vintage(
        vintage: ForecastVintageId,
    ) -> Result<Self, InvestmentProposalError> {
        Self::try_from_bytes(vintage.bytes())
    }
}

/// One mandatory evidence surface consumed by the recommendation authority.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RecommendationEvidenceKind {
    /// Current market reference price.
    Market,
    /// Calibrated price forecast.
    PriceForecast,
    /// Independently governed valuation measurement.
    Valuation,
    /// Evidence-closed financial model with exact inputs, assumptions, scenarios, and sensitivity.
    FinancialModel,
    /// Cost-adjusted, point-in-time backtest result.
    Backtest,
    /// Chronological independent out-of-sample study.
    OutOfSample,
    /// Optional causal harmonic-pattern evidence.
    HarmonicPattern,
    /// Current market-liquidity assessment.
    Liquidity,
    /// Current account and portfolio-risk assessment.
    PortfolioRisk,
}
