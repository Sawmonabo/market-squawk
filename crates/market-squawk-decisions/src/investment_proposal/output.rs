//! Immutable generated, no-action, and unavailable recommendation payloads.

use std::num::NonZeroU32;

use market_squawk_domain::{AccountId, Currency, DataQuality, InstrumentId, Money, Timestamp};

use crate::{DecisionText, TargetPriceCases, TargetPriceRange};

use super::evidence::InvestmentAnalysisEvidence;
use super::policy::{
    ActionSpecificCostAvailability, ProposalTimeBenchmarkAvailability, RecommendationConfidence,
    RecommendationPolicy,
};
use super::{
    InvestmentAnalysisId, InvestmentProposalError, InvestmentProposalId, MAX_PROPOSAL_INVALIDATORS,
    RECOMMENDATION_ASSUMPTION_COUNT, RECOMMENDATION_INVALIDATION_COUNT,
    RECOMMENDATION_LIMITATION_COUNT, RecommendationDerivationDigest, RecommendationEvidenceDigest,
    RecommendationEvidenceKind, RecommendationPolicyDigest,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RecommendationAction {
    /// Consider establishing a position while the mark is above invalidation and no higher than
    /// the universal entry ceiling; valid only when evidence proves no current position.
    Buy,
    /// Consider increasing an existing position above invalidation and no higher than the
    /// universal add ceiling when portfolio policy permits it.
    Add,
    /// Retain an existing position when no permitted add, trim, or exit trigger is active.
    Hold,
    /// Consider reducing an existing position at or above the universal trim floor when portfolio
    /// policy permits it.
    Trim,
    /// Consider exiting an existing position at or below the downside-invalidation ceiling when
    /// portfolio policy permits it.
    Sell,
}

/// Closed marker proving this contract cannot be interpreted as execution authority.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProposalExecutionEligibility {
    /// Research-only evidence requiring a separate review/governance bridge.
    ResearchOnlyExecutionIneligible,
}

/// Exact universal long-investment scenario and trigger zones.
///
/// These ranges do not describe executable prices. The exit zone is a downside-invalidation zone,
/// not an overvaluation sell target. V1 action selection uses the exact mark and the zone boundaries
/// exposed by [`GeneratedInvestmentProposal`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GeneratedPriceLadder {
    pub(super) cases: TargetPriceCases,
    pub(super) downside_range: TargetPriceRange,
    pub(super) base_range: TargetPriceRange,
    pub(super) upside_range: TargetPriceRange,
    pub(super) entry_range: TargetPriceRange,
    pub(super) add_range: TargetPriceRange,
    pub(super) add_case: Money,
    pub(super) trim_range: TargetPriceRange,
    pub(super) exit_range: TargetPriceRange,
}

impl GeneratedPriceLadder {
    pub(super) fn try_new(
        cases: TargetPriceCases,
        downside_range: TargetPriceRange,
        base_range: TargetPriceRange,
        upside_range: TargetPriceRange,
        entry_range: TargetPriceRange,
        add_range: TargetPriceRange,
        add_case: Money,
        trim_range: TargetPriceRange,
        exit_range: TargetPriceRange,
    ) -> Result<Self, InvestmentProposalError> {
        let currency = cases.downside().currency();
        if [
            cases.base().currency(),
            cases.upside().currency(),
            downside_range.lower().currency(),
            downside_range.upper().currency(),
            base_range.lower().currency(),
            base_range.upper().currency(),
            upside_range.lower().currency(),
            upside_range.upper().currency(),
            entry_range.lower().currency(),
            entry_range.upper().currency(),
            add_range.lower().currency(),
            add_range.upper().currency(),
            add_case.currency(),
            trim_range.lower().currency(),
            trim_range.upper().currency(),
            exit_range.lower().currency(),
            exit_range.upper().currency(),
        ]
        .into_iter()
        .any(|candidate| candidate != currency)
            || !(downside_range.lower().amount()..=downside_range.upper().amount())
                .contains(&cases.downside().amount())
            || !(base_range.lower().amount()..=base_range.upper().amount())
                .contains(&cases.base().amount())
            || !(upside_range.lower().amount()..=upside_range.upper().amount())
                .contains(&cases.upside().amount())
            || downside_range.lower().amount() >= downside_range.upper().amount()
            || downside_range.upper().amount() >= exit_range.lower().amount()
            || exit_range.lower().amount() >= exit_range.upper().amount()
            || exit_range.upper().amount() >= add_range.lower().amount()
            || add_range.lower().amount() >= add_range.upper().amount()
            || add_range.upper().amount() >= entry_range.lower().amount()
            || entry_range.lower().amount() >= entry_range.upper().amount()
            || !(add_range.lower().amount()..=add_range.upper().amount())
                .contains(&add_case.amount())
            || entry_range.upper().amount() >= base_range.lower().amount()
            || base_range.lower().amount() >= base_range.upper().amount()
            || base_range.upper().amount() >= trim_range.lower().amount()
            || trim_range.lower().amount() >= trim_range.upper().amount()
            || trim_range.upper().amount() >= upside_range.lower().amount()
            || upside_range.lower().amount() >= upside_range.upper().amount()
        {
            return Err(InvestmentProposalError::InvalidPrice);
        }
        Ok(Self {
            cases,
            downside_range,
            base_range,
            upside_range,
            entry_range,
            add_range,
            add_case,
            trim_range,
            exit_range,
        })
    }

    /// Returns generated downside, base, and upside cases.
    #[must_use]
    pub const fn cases(self) -> TargetPriceCases {
        self.cases
    }

    /// Returns the calibrated downside price range.
    #[must_use]
    pub const fn downside_range(self) -> TargetPriceRange {
        self.downside_range
    }

    /// Returns the valuation-adjusted base price range.
    #[must_use]
    pub const fn base_range(self) -> TargetPriceRange {
        self.base_range
    }

    /// Returns the calibrated upside price range.
    #[must_use]
    pub const fn upside_range(self) -> TargetPriceRange {
        self.upside_range
    }

    /// Returns the universal entry zone whose upper bound is the no-position buy ceiling.
    #[must_use]
    pub const fn entry_range(self) -> TargetPriceRange {
        self.entry_range
    }

    /// Returns the universal attractive/add zone whose upper bound is the add ceiling.
    #[must_use]
    pub const fn add_range(self) -> TargetPriceRange {
        self.add_range
    }

    /// Returns the exact generated bridge point within the add range.
    #[must_use]
    pub const fn add_case(self) -> Money {
        self.add_case
    }

    /// Returns the universal trim zone whose lower bound is the trim trigger floor.
    #[must_use]
    pub const fn trim_range(self) -> TargetPriceRange {
        self.trim_range
    }

    /// Returns the downside-invalidation zone whose upper bound is the exit trigger ceiling.
    #[must_use]
    pub const fn exit_range(self) -> TargetPriceRange {
        self.exit_range
    }
}

/// Why mandatory evidence could not be admitted for policy evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProposalUnavailableReason {
    /// A configured mandatory producer supplied no evidence.
    MissingEvidence(RecommendationEvidenceKind),
    /// Evidence belongs to a different stable instrument.
    InstrumentMismatch {
        /// Mismatched evidence surface.
        evidence: RecommendationEvidenceKind,
        /// Instrument selected for analysis.
        expected: InstrumentId,
        /// Instrument named by the evidence.
        actual: InstrumentId,
    },
    /// Evidence uses a different denomination.
    CurrencyMismatch {
        /// Mismatched evidence surface.
        evidence: RecommendationEvidenceKind,
        /// Currency selected for analysis.
        expected: Currency,
        /// Currency named by the evidence.
        actual: Currency,
    },
    /// Portfolio evidence belongs to a different account.
    AccountMismatch {
        /// Account selected for analysis.
        expected: AccountId,
        /// Account named by the risk evidence.
        actual: AccountId,
    },
    /// Evidence was not knowable by the point-in-time analysis cutoff.
    NotAvailableAtCutoff(RecommendationEvidenceKind),
    /// Evidence had already reached its exclusive expiry.
    ExpiredEvidence(RecommendationEvidenceKind),
    /// Evidence exceeded the code-owned freshness budget.
    StaleEvidence(RecommendationEvidenceKind),
    /// Quality is modeled, estimated, stale, quarantined, or otherwise not admitted for this use.
    RejectedQuality {
        /// Rejected evidence surface.
        evidence: RecommendationEvidenceKind,
        /// Exact retained quality classification.
        quality: DataQuality,
    },
    /// Forecast target time did not match the code-owned horizon exactly.
    ForecastHorizonMismatch {
        /// Policy-required target time.
        expected: Timestamp,
        /// Forecast target time.
        actual: Timestamp,
    },
    /// Valuation evidence did not measure the exact policy horizon.
    ValuationHorizonMismatch {
        /// Policy-required valuation horizon.
        expected: Timestamp,
        /// Valuation measurement horizon.
        actual: Timestamp,
    },
    /// Financial-model evidence did not measure the exact policy horizon.
    FinancialModelHorizonMismatch {
        expected: Timestamp,
        actual: Timestamp,
    },
    /// Backtest outcomes evaluated a different horizon from the recommendation policy.
    BacktestHorizonMismatch {
        /// Policy-required horizon in nanoseconds.
        expected_nanos: i64,
        /// Evaluated backtest outcome horizon in nanoseconds.
        actual_nanos: i64,
    },
    /// Independent out-of-sample outcomes evaluated a different policy horizon.
    OutOfSampleHorizonMismatch {
        expected_nanos: i64,
        actual_nanos: i64,
    },
    /// The financial-model central value does not match the independently governed valuation.
    FinancialModelValuationMismatch,
    /// The chronological OOS receipt does not bind the admitted backtest study identities.
    OutOfSampleBacktestMismatch,
    /// Forecast calibration did not contain enough completed outcomes.
    InsufficientForecastOutcomes {
        /// Policy minimum.
        required: NonZeroU32,
        /// Exact evidence count.
        actual: NonZeroU32,
    },
    /// Forecast intervals declared a nominal coverage outside the policy-supported calibration.
    UnsupportedForecastCoverage {
        /// Minimum supported nominal coverage.
        minimum_ppm: u32,
        /// Maximum supported nominal coverage.
        maximum_ppm: u32,
        /// Exact declared nominal coverage.
        actual_ppm: u32,
    },
    /// PIT backtest observation count was below the policy minimum.
    InsufficientBacktestObservations {
        /// Policy minimum.
        required: NonZeroU32,
        /// Exact evidence count.
        actual: NonZeroU32,
    },
    /// PIT backtest trial count was below the policy minimum.
    InsufficientBacktestTrials {
        /// Policy minimum.
        required: NonZeroU32,
        /// Exact evidence count.
        actual: NonZeroU32,
    },
    /// The portfolio revision token used the reserved all-zero sentinel.
    ReservedPortfolioRevision,
}

/// Policy conclusion when complete, structurally sound evidence does not authorize a signal.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NoActionReason {
    /// Forecast and valuation imply opposing actionable directions.
    ConflictingForecastAndValuation,
    /// A valid backtest failed cost-adjusted performance, stability, or drawdown policy.
    BacktestBelowPolicy,
    /// Complete chronological OOS evidence failed sample, fold, or completion-coverage policy.
    OutOfSampleBelowPolicy,
    /// Complete liquidity evidence failed spread or capacity policy.
    LiquidityBelowPolicy,
    /// Complete portfolio evidence failed account-specific risk capacity policy.
    PortfolioRiskBelowPolicy,
    /// Policy-weighted evidence reliability fell below the fixed policy floor.
    ConfidenceBelowPolicy,
    /// Evidence direction is not actionable for the proven current position state.
    PositionStateNotActionable,
    /// Exact policy rounding could not preserve a truthful strictly ordered price ladder.
    GeneratedPriceOrderCollapsed,
}

/// Machine-readable condition explaining why a no-action proposal must remain inactive.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProposalInvalidator {
    /// Forecast and valuation are directionally opposed.
    ForecastValuationConflict,
    /// Backtest performance, stability, or drawdown is outside policy.
    BacktestPolicyBreach,
    /// Independent chronological OOS evidence is outside policy.
    OutOfSamplePolicyBreach,
    /// Liquidity spread or capacity is outside policy.
    LiquidityPolicyBreach,
    /// Portfolio risk capacity is outside policy.
    PortfolioRiskPolicyBreach,
    /// Policy-weighted evidence reliability is outside policy.
    ConfidencePolicyBreach,
    /// Position state or portfolio permission is incompatible with the directional signal.
    PositionStateIncompatible,
    /// Exact rounding collapsed one or more strict price boundaries.
    GeneratedPriceOrderCollapsed,
}

/// Persistable deterministic generated proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedInvestmentProposal {
    pub(super) analysis_id: InvestmentAnalysisId,
    pub(super) proposal_id: InvestmentProposalId,
    pub(super) policy: RecommendationPolicy,
    pub(super) evidence: InvestmentAnalysisEvidence,
    pub(super) evidence_digest: RecommendationEvidenceDigest,
    pub(super) derivation_digest: RecommendationDerivationDigest,
    pub(super) action: RecommendationAction,
    pub(super) price_ladder: GeneratedPriceLadder,
    pub(super) confidence: RecommendationConfidence,
    pub(super) horizon_at: Timestamp,
    pub(super) expires_at: Timestamp,
    pub(super) execution_eligibility: ProposalExecutionEligibility,
}

impl GeneratedInvestmentProposal {
    /// Returns the stable analysis identity.
    #[must_use]
    pub const fn analysis_id(&self) -> InvestmentAnalysisId {
        self.analysis_id
    }

    /// Returns the stable content-derived proposal identity.
    #[must_use]
    pub const fn proposal_id(&self) -> InvestmentProposalId {
        self.proposal_id
    }

    /// Returns the exact code-owned recommendation policy.
    #[must_use]
    pub const fn policy(&self) -> &RecommendationPolicy {
        &self.policy
    }

    /// Returns the complete immutable admitted evidence envelope.
    #[must_use]
    pub const fn evidence(&self) -> &InvestmentAnalysisEvidence {
        &self.evidence
    }

    /// Returns the commitment to every supplied evidence field and identity.
    #[must_use]
    pub const fn evidence_digest(&self) -> RecommendationEvidenceDigest {
        self.evidence_digest
    }

    /// Returns the commitment to the exact deterministic calculation and result.
    #[must_use]
    pub const fn derivation_digest(&self) -> RecommendationDerivationDigest {
        self.derivation_digest
    }

    /// Returns the generated position-aware research recommendation.
    #[must_use]
    pub const fn action(&self) -> RecommendationAction {
        self.action
    }

    /// Returns all generated exact price cases and ranges.
    #[must_use]
    pub const fn price_ladder(&self) -> GeneratedPriceLadder {
        self.price_ladder
    }

    /// Returns the versioned code-owned action/zone table used for this proposal.
    #[must_use]
    pub const fn action_zone_semantics_version(&self) -> NonZeroU32 {
        self.policy.action_zone_semantics_version()
    }

    /// Returns the named universal reference zone for this generated action.
    ///
    /// Buy uses the entry zone, Add the add zone, Trim the trim zone, and Sell the
    /// downside-invalidation/exit zone. This named band is not, by itself, the action predicate:
    /// callers must present the exact inclusive/exclusive trigger bounds exposed below. Hold is the
    /// residual state and has no active reference zone.
    #[must_use]
    pub const fn action_trigger_reference_zone(&self) -> Option<TargetPriceRange> {
        match self.action {
            RecommendationAction::Buy => Some(self.price_ladder.entry_range),
            RecommendationAction::Add => Some(self.price_ladder.add_range),
            RecommendationAction::Trim => Some(self.price_ladder.trim_range),
            RecommendationAction::Sell => Some(self.price_ladder.exit_range),
            RecommendationAction::Hold => None,
        }
    }

    /// Returns the exclusive downside-invalidation floor for Buy and Add.
    ///
    /// A mark at or below this value is never a Buy or Add under V1.
    #[must_use]
    pub const fn action_trigger_floor_exclusive(&self) -> Option<Money> {
        match self.action {
            RecommendationAction::Buy | RecommendationAction::Add => {
                Some(self.price_ladder.exit_range.upper())
            }
            RecommendationAction::Hold
            | RecommendationAction::Trim
            | RecommendationAction::Sell => None,
        }
    }

    /// Returns the inclusive trigger floor for an action with no upper trigger bound.
    ///
    /// V1 exposes this only for Trim, using the universal trim-zone lower bound.
    #[must_use]
    pub const fn action_trigger_floor_inclusive(&self) -> Option<Money> {
        match self.action {
            RecommendationAction::Trim => Some(self.price_ladder.trim_range.lower()),
            RecommendationAction::Buy
            | RecommendationAction::Add
            | RecommendationAction::Hold
            | RecommendationAction::Sell => None,
        }
    }

    /// Returns the inclusive trigger ceiling for Buy, Add, or Sell.
    ///
    /// Buy and Add use their named zone's upper bound. Sell uses the exit/invalidation zone's upper
    /// bound and remains valid below it. Hold and Trim have no upper trigger bound.
    #[must_use]
    pub const fn action_trigger_ceiling_inclusive(&self) -> Option<Money> {
        match self.action {
            RecommendationAction::Buy => Some(self.price_ladder.entry_range.upper()),
            RecommendationAction::Add => Some(self.price_ladder.add_range.upper()),
            RecommendationAction::Sell => Some(self.price_ladder.exit_range.upper()),
            RecommendationAction::Hold | RecommendationAction::Trim => None,
        }
    }

    /// Returns policy-weighted evidence reliability, not probability of profit.
    #[must_use]
    pub const fn confidence(&self) -> RecommendationConfidence {
        self.confidence
    }

    /// Returns the exact policy horizon.
    #[must_use]
    pub const fn horizon_at(&self) -> Timestamp {
        self.horizon_at
    }

    /// Returns the exclusive proposal expiry.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the research-only execution-ineligible marker.
    #[must_use]
    pub const fn execution_eligibility(&self) -> ProposalExecutionEligibility {
        self.execution_eligibility
    }

    /// Returns the explicit proposal-time outcome-benchmark availability.
    #[must_use]
    pub const fn proposal_time_benchmark_availability(&self) -> ProposalTimeBenchmarkAvailability {
        self.policy.proposal_time_benchmark_availability()
    }

    /// Returns the explicit action-specific forward-cost availability.
    #[must_use]
    pub const fn action_specific_cost_availability(&self) -> ActionSpecificCostAvailability {
        self.policy.action_specific_cost_availability()
    }

    /// Returns fixed evidence-bound policy assumptions.
    #[must_use]
    pub fn assumptions(&self) -> &[DecisionText; RECOMMENDATION_ASSUMPTION_COUNT] {
        self.policy.assumptions()
    }

    /// Returns fixed policy invalidation conditions.
    #[must_use]
    pub fn invalidation_conditions(&self) -> &[DecisionText; RECOMMENDATION_INVALIDATION_COUNT] {
        self.policy.invalidation_conditions()
    }

    /// Returns explicit research and no-guarantee limitations.
    #[must_use]
    pub fn limitations(&self) -> &[DecisionText; RECOMMENDATION_LIMITATION_COUNT] {
        self.policy.limitations()
    }
}

/// Persistable evidence-retaining no-action proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NoActionInvestmentProposal {
    pub(super) analysis_id: InvestmentAnalysisId,
    pub(super) proposal_id: InvestmentProposalId,
    pub(super) policy: RecommendationPolicy,
    pub(super) evidence: InvestmentAnalysisEvidence,
    pub(super) evidence_digest: RecommendationEvidenceDigest,
    pub(super) derivation_digest: RecommendationDerivationDigest,
    pub(super) reason: NoActionReason,
    pub(super) invalidators: [ProposalInvalidator; MAX_PROPOSAL_INVALIDATORS],
    pub(super) confidence: RecommendationConfidence,
    pub(super) horizon_at: Timestamp,
    pub(super) expires_at: Timestamp,
    pub(super) execution_eligibility: ProposalExecutionEligibility,
}

impl NoActionInvestmentProposal {
    /// Returns the stable analysis identity.
    #[must_use]
    pub const fn analysis_id(&self) -> InvestmentAnalysisId {
        self.analysis_id
    }

    /// Returns the stable content-derived proposal identity.
    #[must_use]
    pub const fn proposal_id(&self) -> InvestmentProposalId {
        self.proposal_id
    }

    /// Returns the exact code-owned recommendation policy.
    #[must_use]
    pub const fn policy(&self) -> &RecommendationPolicy {
        &self.policy
    }

    /// Returns the complete immutable admitted evidence envelope.
    #[must_use]
    pub const fn evidence(&self) -> &InvestmentAnalysisEvidence {
        &self.evidence
    }

    /// Returns the commitment to every supplied evidence field and identity.
    #[must_use]
    pub const fn evidence_digest(&self) -> RecommendationEvidenceDigest {
        self.evidence_digest
    }

    /// Returns the commitment to the exact deterministic calculation and result.
    #[must_use]
    pub const fn derivation_digest(&self) -> RecommendationDerivationDigest {
        self.derivation_digest
    }

    /// Returns the typed policy reason for abstention.
    #[must_use]
    pub const fn reason(&self) -> NoActionReason {
        self.reason
    }

    /// Returns bounded machine-readable invalidators.
    #[must_use]
    pub fn invalidators(&self) -> &[ProposalInvalidator] {
        &self.invalidators
    }

    /// Returns policy-weighted evidence reliability, not probability of profit.
    #[must_use]
    pub const fn confidence(&self) -> RecommendationConfidence {
        self.confidence
    }

    /// Returns the exact policy horizon.
    #[must_use]
    pub const fn horizon_at(&self) -> Timestamp {
        self.horizon_at
    }

    /// Returns the exclusive proposal expiry.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the research-only execution-ineligible marker.
    #[must_use]
    pub const fn execution_eligibility(&self) -> ProposalExecutionEligibility {
        self.execution_eligibility
    }

    /// Returns the explicit proposal-time outcome-benchmark availability.
    #[must_use]
    pub const fn proposal_time_benchmark_availability(&self) -> ProposalTimeBenchmarkAvailability {
        self.policy.proposal_time_benchmark_availability()
    }

    /// Returns the explicit action-specific forward-cost availability.
    #[must_use]
    pub const fn action_specific_cost_availability(&self) -> ActionSpecificCostAvailability {
        self.policy.action_specific_cost_availability()
    }

    /// Returns fixed evidence-bound policy assumptions.
    #[must_use]
    pub fn assumptions(&self) -> &[DecisionText; RECOMMENDATION_ASSUMPTION_COUNT] {
        self.policy.assumptions()
    }

    /// Returns fixed policy invalidation conditions.
    #[must_use]
    pub fn invalidation_conditions(&self) -> &[DecisionText; RECOMMENDATION_INVALIDATION_COUNT] {
        self.policy.invalidation_conditions()
    }

    /// Returns explicit research and no-guarantee limitations.
    #[must_use]
    pub fn limitations(&self) -> &[DecisionText; RECOMMENDATION_LIMITATION_COUNT] {
        self.policy.limitations()
    }
}

/// Persistable failed-closed analysis when mandatory authority is missing or inadmissible.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnavailableInvestmentAnalysis {
    pub(super) analysis_id: InvestmentAnalysisId,
    pub(super) policy: RecommendationPolicy,
    pub(super) evidence: InvestmentAnalysisEvidence,
    pub(super) evidence_digest: RecommendationEvidenceDigest,
    pub(super) reason: ProposalUnavailableReason,
    pub(super) horizon_at: Timestamp,
    pub(super) expires_at: Timestamp,
    pub(super) execution_eligibility: ProposalExecutionEligibility,
}

impl UnavailableInvestmentAnalysis {
    /// Returns the stable failed-closed analysis identity.
    #[must_use]
    pub const fn analysis_id(&self) -> InvestmentAnalysisId {
        self.analysis_id
    }

    /// Returns the exact code-owned recommendation policy.
    #[must_use]
    pub const fn policy(&self) -> &RecommendationPolicy {
        &self.policy
    }

    /// Returns the complete supplied evidence envelope, including absent authorities.
    #[must_use]
    pub const fn evidence(&self) -> &InvestmentAnalysisEvidence {
        &self.evidence
    }

    /// Returns the commitment to every supplied or absent evidence field.
    #[must_use]
    pub const fn evidence_digest(&self) -> RecommendationEvidenceDigest {
        self.evidence_digest
    }

    /// Returns the typed unavailable reason.
    #[must_use]
    pub const fn reason(&self) -> ProposalUnavailableReason {
        self.reason
    }

    /// Returns the exact policy horizon expected from a configured producer.
    #[must_use]
    pub const fn horizon_at(&self) -> Timestamp {
        self.horizon_at
    }

    /// Returns the exclusive lifetime of this unavailable analysis result.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the research-only execution-ineligible marker.
    #[must_use]
    pub const fn execution_eligibility(&self) -> ProposalExecutionEligibility {
        self.execution_eligibility
    }

    /// Returns explicit limitations explaining this is neither a guarantee nor execution input.
    #[must_use]
    pub fn limitations(&self) -> &[DecisionText; RECOMMENDATION_LIMITATION_COUNT] {
        self.policy.limitations()
    }
}

/// Closed result of the one pure recommendation authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvestmentProposalDecision {
    /// Complete admitted evidence produced a position-aware recommendation and price ladder.
    Generated(GeneratedInvestmentProposal),
    /// Complete admitted evidence required a safe, evidence-retaining abstention.
    NoAction(NoActionInvestmentProposal),
    /// Mandatory authority was missing, stale, corrupt, or bound to another subject.
    Unavailable(UnavailableInvestmentAnalysis),
}

impl InvestmentProposalDecision {
    /// Returns the stable analysis identity shared by all output families.
    #[must_use]
    pub const fn analysis_id(&self) -> InvestmentAnalysisId {
        match self {
            Self::Generated(value) => value.analysis_id,
            Self::NoAction(value) => value.analysis_id,
            Self::Unavailable(value) => value.analysis_id,
        }
    }

    /// Returns the exact code-owned recommendation policy used for this result.
    #[must_use]
    pub const fn policy(&self) -> &RecommendationPolicy {
        match self {
            Self::Generated(value) => &value.policy,
            Self::NoAction(value) => &value.policy,
            Self::Unavailable(value) => &value.policy,
        }
    }

    /// Returns the complete immutable admitted-or-unavailable evidence envelope.
    #[must_use]
    pub const fn evidence(&self) -> &InvestmentAnalysisEvidence {
        match self {
            Self::Generated(value) => &value.evidence,
            Self::NoAction(value) => &value.evidence,
            Self::Unavailable(value) => &value.evidence,
        }
    }

    /// Returns the stable proposal identity for generated and no-action results.
    #[must_use]
    pub const fn proposal_id(&self) -> Option<InvestmentProposalId> {
        match self {
            Self::Generated(value) => Some(value.proposal_id),
            Self::NoAction(value) => Some(value.proposal_id),
            Self::Unavailable(_) => None,
        }
    }

    /// Returns the exact derivation commitment for generated and no-action results.
    #[must_use]
    pub const fn derivation_digest(&self) -> Option<RecommendationDerivationDigest> {
        match self {
            Self::Generated(value) => Some(value.derivation_digest),
            Self::NoAction(value) => Some(value.derivation_digest),
            Self::Unavailable(_) => None,
        }
    }

    /// Returns the exact policy digest used for this result.
    #[must_use]
    pub const fn policy_digest(&self) -> RecommendationPolicyDigest {
        match self {
            Self::Generated(value) => value.policy.digest,
            Self::NoAction(value) => value.policy.digest,
            Self::Unavailable(value) => value.policy.digest,
        }
    }

    /// Returns the complete evidence digest used for this result.
    #[must_use]
    pub const fn evidence_digest(&self) -> RecommendationEvidenceDigest {
        match self {
            Self::Generated(value) => value.evidence_digest,
            Self::NoAction(value) => value.evidence_digest,
            Self::Unavailable(value) => value.evidence_digest,
        }
    }

    /// Returns the exact code-owned analysis horizon.
    #[must_use]
    pub const fn horizon_at(&self) -> Timestamp {
        match self {
            Self::Generated(value) => value.horizon_at,
            Self::NoAction(value) => value.horizon_at,
            Self::Unavailable(value) => value.horizon_at,
        }
    }

    /// Returns the exclusive lifetime of this exact result.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        match self {
            Self::Generated(value) => value.expires_at,
            Self::NoAction(value) => value.expires_at,
            Self::Unavailable(value) => value.expires_at,
        }
    }

    /// Returns the research-only execution-ineligible marker.
    #[must_use]
    pub const fn execution_eligibility(&self) -> ProposalExecutionEligibility {
        ProposalExecutionEligibility::ResearchOnlyExecutionIneligible
    }
}
