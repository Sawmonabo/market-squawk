//! Exact, immutable investment outcome and sizing projections.
//!
//! These sidecars are deterministic research calculations bound to an already generated
//! investment proposal. They do not mutate a proposal, select an order quantity, or grant order
//! or execution authority.

use std::fmt;

mod digest;
mod outcome;
mod sizing;

pub use outcome::{
    AfterTaxPnlAvailability, BenchmarkReturnAvailability, ExactFinancialRatio,
    ExactFinancialRatioRange, ExactPositionScale, ExpectedGrossPricePnlAvailability,
    ExpectedReturnAvailability, GrossMarkRelativeRange, GrossPricePnlAvailability,
    InvestmentOutcomeProjection, MarkToZoneDistance, NetPnlAvailability, SignedMoneyRange,
};
pub use sizing::{
    CandidatePortfolioSizingState, CandidateSizingConstraints, CapacityRange,
    FeasibleLotRangeAvailability, FeasibleNotionalRangeAvailability, InvestmentSizingInputs,
    InvestmentSizingProjection, LotRange, NonnegativeMoneyRange, PreferredWeightRoundingRemainder,
    SizingCapacityAvailability, SizingCapacityEvidence, SizingConstraintCap, SizingConstraintKind,
    SizingUnavailableReason,
};

use super::{InvestmentProposalId, RecommendationDerivationDigest};

/// Canonical schema version committed by every outcome-projection digest.
pub const INVESTMENT_OUTCOME_PROJECTION_SCHEMA_VERSION: u16 = 2;
/// Canonical schema version committed by every sizing-projection digest.
pub const INVESTMENT_SIZING_PROJECTION_SCHEMA_VERSION: u16 = 1;

/// Stable binding to the generated proposal and exact derivation being projected.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct InvestmentProjectionBinding {
    proposal_id: InvestmentProposalId,
    derivation_digest: RecommendationDerivationDigest,
}

impl InvestmentProjectionBinding {
    pub(super) const fn new(
        proposal_id: InvestmentProposalId,
        derivation_digest: RecommendationDerivationDigest,
    ) -> Self {
        Self {
            proposal_id,
            derivation_digest,
        }
    }

    /// Returns the generated proposal identity.
    #[must_use]
    pub const fn proposal_id(self) -> InvestmentProposalId {
        self.proposal_id
    }

    /// Returns the exact proposal-derivation commitment.
    #[must_use]
    pub const fn derivation_digest(self) -> RecommendationDerivationDigest {
        self.derivation_digest
    }
}

/// Closed marker proving that a projection carries no mutation or execution authority.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum InvestmentProjectionAuthority {
    /// Analysis only: no proposal mutation, order creation, or execution authority.
    AnalysisOnlyNoMutationNoExecution,
}

/// Versioned SHA-256 identity of one exact projection input and result.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InvestmentProjectionDigest([u8; 32]);

impl InvestmentProjectionDigest {
    pub(super) const fn from_sha256(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the complete SHA-256 result identity.
    #[must_use]
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

/// A projection contract or checked calculation failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvestmentProjectionError {
    /// A mandatory generated-proposal evidence surface was unexpectedly absent.
    MissingProposalEvidence,
    /// Execution terms or capacity evidence named a different instrument.
    InstrumentMismatch,
    /// Money, execution terms, or capacity evidence used a different currency.
    CurrencyMismatch,
    /// Execution terms did not settle in the proposal currency.
    SettlementCurrencyMismatch,
    /// The selected sizing mark was not the proposal's exact evidence-bound mark.
    SelectedMarkMismatch,
    /// A proposal or capacity price was not an exact execution-tick multiple.
    PriceNotOnExecutionTick,
    /// An exact financial value violated its required sign or ordering invariant.
    InvalidFinancialValue,
    /// Portfolio state did not match the proposal account or exact portfolio revision.
    PortfolioStateMismatch,
    /// A sizing constraint was out of range or internally inconsistent.
    InvalidSizingConstraint,
    /// Capacity evidence was detached from the exact sizing context.
    CapacityBindingMismatch,
    /// Evidence publication, observation, evaluation, or expiry times were inconsistent.
    InvalidTimeOrder,
    /// Checked integer or exact-decimal arithmetic overflowed.
    ArithmeticOverflow,
}

impl fmt::Display for InvestmentProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingProposalEvidence => {
                formatter.write_str("mandatory proposal evidence is missing")
            }
            Self::InstrumentMismatch => formatter.write_str("projection instrument mismatch"),
            Self::CurrencyMismatch => formatter.write_str("projection currency mismatch"),
            Self::SettlementCurrencyMismatch => {
                formatter.write_str("execution terms do not settle in the proposal currency")
            }
            Self::SelectedMarkMismatch => {
                formatter.write_str("selected mark does not match the proposal mark")
            }
            Self::PriceNotOnExecutionTick => {
                formatter.write_str("projection price is not on the execution tick")
            }
            Self::InvalidFinancialValue => {
                formatter.write_str("projection financial value is invalid")
            }
            Self::PortfolioStateMismatch => {
                formatter.write_str("portfolio state does not match proposal evidence")
            }
            Self::InvalidSizingConstraint => formatter.write_str("sizing constraint is invalid"),
            Self::CapacityBindingMismatch => {
                formatter.write_str("capacity evidence does not match the sizing context")
            }
            Self::InvalidTimeOrder => formatter.write_str("projection time ordering is invalid"),
            Self::ArithmeticOverflow => formatter.write_str("projection arithmetic overflowed"),
        }
    }
}

impl std::error::Error for InvestmentProjectionError {}
