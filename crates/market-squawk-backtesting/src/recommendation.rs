//! Pure point-in-time recommendation-outcome evaluation with no execution authority.

mod round_trip;

pub use round_trip::{
    AllOriginRoundTripDispositionV1, AllOriginRoundTripEvaluationV1, AllOriginRoundTripEvaluatorV1,
    AllOriginRoundTripPolicyV1, AllOriginRoundTripResultV1, AllOriginRoundTripUnavailableV1,
};

use std::collections::BTreeSet;

use market_squawk_data::{CorporateActionAdjustment, CorporateActionPlan, Sha256Digest};
use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, Currency, DataQuality, Denomination,
    HistoricalStudyBasis, HistoricalStudyLimitation, InstrumentExecutionTerms, InstrumentId, Money,
    OrderId, OrderReasonCode, OrderSide, OrderType, PriceTicks, QuantityLots, SourceIdentifier,
    StrategyId, TimeInForce, Timestamp,
};
use market_squawk_execution::{OrderIntent, OrderIntentInput};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::dataset::{
    BacktestDataset, BacktestExecutionBasis, BacktestObservation, BacktestStudyQualification,
    HistoricalUniverseStatus,
};
use crate::engine::RecommendationAccounting;
use crate::fills::{
    RESEARCH_EXECUTION_POLICY_VERSION, ResearchExecutionAssumptions,
    ResearchExecutionAssumptionsInput, ResearchFill, ResearchFillSimulator,
    ResearchLiquidityPriority,
};

/// Exact strict recommendation-outcome V1 target: 365 elapsed days after its financial origin.
pub const RECOMMENDATION_TARGET_HORIZON_NANOS_V1: i64 = 365 * 24 * 60 * 60 * 1_000_000_000;
/// Exact number of independent OOS folds in the strict recommendation materialization.
pub const RECOMMENDATION_OOS_FOLD_COUNT_V1: usize = 3;
/// Exact elapsed two-year coordinate of each independent recommendation OOS fold.
pub const RECOMMENDATION_OOS_FOLD_HORIZON_NANOS_V1: i64 =
    2 * RECOMMENDATION_TARGET_HORIZON_NANOS_V1;
/// Exact elapsed six-year coordinate covered by the three strict OOS folds.
pub const RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1: i64 =
    3 * RECOMMENDATION_OOS_FOLD_HORIZON_NANOS_V1;
/// Exact public name of the already fee/slippage-adjusted signal-return metric.
pub const COST_ADJUSTED_TOTAL_RETURN_METRIC: &str = "cost-adjusted-total-return";
/// Exact public name of peak-to-trough drawdown over the retained marked-equity path.
pub const MAXIMUM_DRAWDOWN_METRIC: &str = "maximum-drawdown";
/// Exact public name of the positive independent-fold fraction.
pub const POSITIVE_FOLD_STABILITY_METRIC: &str = "positive-fold-stability";

const HARD_MAX_RECOMMENDATION_FOLDS: usize = 1_024;
const HARD_MAX_RECOMMENDATION_SIGNALS: usize = 1_000_000;
const HARD_MAX_EQUITY_POINTS_PER_OUTCOME: usize = 65_536;
const HARD_MAX_TOTAL_EQUITY_POINTS: usize = 1_000_000;
const HARD_MAX_OBSERVATION_VISITS: usize = 100_000_000;
const HARD_MAX_EXECUTION_LAG_NANOS: i64 = 30 * 24 * 60 * 60 * 1_000_000_000;
const DAILY_BAR_ASSUMED_FULL_SPREAD_BASIS_POINTS: i32 = 20;

/// Returns the existing code-owned conservative research cost and fill profile.
///
/// The profile is deterministic research evidence only. It is intentionally not a caller-tunable
/// execution or profit assumption at the strict recommendation materialization boundary.
pub fn recommendation_conservative_execution_assumptions_v1()
-> Result<ResearchExecutionAssumptions, RecommendationBacktestError> {
    ResearchExecutionAssumptions::try_new(ResearchExecutionAssumptionsInput {
        version: RESEARCH_EXECUTION_POLICY_VERSION,
        fee_basis_points: BasisPoints::new(10),
        slippage_basis_points: BasisPoints::new(15),
        maximum_random_slippage_basis_points: BasisPoints::new(5),
        maximum_participation_basis_points: BasisPoints::new(500),
        liquidity_priority: ResearchLiquidityPriority::SignalTimeThenOrderId,
        latency_nanos: 5_000_000,
        allow_partial_fills: true,
        fee_decimal_scale: 8,
    })
    .map_err(|_| RecommendationBacktestError::InvalidPolicy)
}

/// Exact benchmark instrument selected by approved policy evidence, never by symbol inference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationBenchmarkPolicyV1 {
    instrument_id: InstrumentId,
    approval_digest: Sha256Digest,
}

impl RecommendationBenchmarkPolicyV1 {
    /// Binds one exact benchmark instrument to nonzero approved policy evidence.
    pub fn try_new(
        instrument_id: InstrumentId,
        approval_digest: Sha256Digest,
    ) -> Result<Self, RecommendationBacktestError> {
        require_digest(approval_digest)?;
        Ok(Self {
            instrument_id,
            approval_digest,
        })
    }

    /// Returns the exact internal benchmark instrument identity.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the approved benchmark-policy evidence identity.
    #[must_use]
    pub const fn approval_digest(self) -> Sha256Digest {
        self.approval_digest
    }
}

/// Untrusted complete input for strict recommendation-outcome V1 evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationBacktestPolicyV1Input {
    pub study_qualification: BacktestStudyQualification,
    pub subject_instrument_id: InstrumentId,
    pub benchmark: RecommendationBenchmarkPolicyV1,
    pub accompanying_benchmark: RecommendationBenchmarkPolicyV1,
    pub raw_price_evidence_digest: Sha256Digest,
    pub corporate_action_content_digest: Sha256Digest,
    pub corporate_action_audit_digest: Sha256Digest,
    pub corporate_action_coverage_starts_at: Timestamp,
    pub execution_basis: BacktestExecutionBasis,
    pub reporting_currency: Currency,
    pub subject_quantity: QuantityLots,
    pub benchmark_quantity: QuantityLots,
    pub maximum_entry_lag_nanos: i64,
    pub maximum_exit_lag_nanos: i64,
    pub execution_assumptions: ResearchExecutionAssumptions,
    pub seed: u64,
}

/// Immutable current recommendation-outcome policy with an exact 365-day coordinate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationBacktestPolicyV1 {
    study_qualification: BacktestStudyQualification,
    subject_instrument_id: InstrumentId,
    benchmark: RecommendationBenchmarkPolicyV1,
    accompanying_benchmark: RecommendationBenchmarkPolicyV1,
    raw_price_evidence_digest: Sha256Digest,
    corporate_action_content_digest: Sha256Digest,
    corporate_action_audit_digest: Sha256Digest,
    corporate_action_coverage_starts_at: Timestamp,
    execution_basis: BacktestExecutionBasis,
    reporting_currency: Currency,
    subject_quantity: QuantityLots,
    benchmark_quantity: QuantityLots,
    maximum_entry_lag_nanos: i64,
    maximum_exit_lag_nanos: i64,
    execution_assumptions: ResearchExecutionAssumptions,
    seed: u64,
    digest: Sha256Digest,
}

impl RecommendationBacktestPolicyV1 {
    /// Validates a strict V1 policy and binds all economic assumptions into one digest.
    pub fn try_new(
        input: RecommendationBacktestPolicyV1Input,
    ) -> Result<Self, RecommendationBacktestError> {
        input
            .study_qualification
            .validate()
            .map_err(|_| RecommendationBacktestError::InvalidPolicy)?;
        require_digest(input.raw_price_evidence_digest)?;
        require_digest(input.corporate_action_content_digest)?;
        require_digest(input.corporate_action_audit_digest)?;
        if input.subject_instrument_id == input.benchmark.instrument_id
            || input.subject_instrument_id == input.accompanying_benchmark.instrument_id
            || input.benchmark.instrument_id == input.accompanying_benchmark.instrument_id
            || input.subject_quantity.get() <= 0
            || input.benchmark_quantity.get() <= 0
            || !(input.execution_assumptions.latency_nanos()..=HARD_MAX_EXECUTION_LAG_NANOS)
                .contains(&input.maximum_entry_lag_nanos)
            || !(input.execution_assumptions.latency_nanos()..=HARD_MAX_EXECUTION_LAG_NANOS)
                .contains(&input.maximum_exit_lag_nanos)
            || input.maximum_entry_lag_nanos >= RECOMMENDATION_TARGET_HORIZON_NANOS_V1
            || 5_000_i32
                .checked_add(input.execution_assumptions.slippage_basis_points().get())
                .and_then(|value| {
                    value.checked_add(
                        input
                            .execution_assumptions
                            .maximum_random_slippage_basis_points()
                            .get(),
                    )
                })
                .is_none_or(|maximum| maximum > 10_000)
        {
            return Err(RecommendationBacktestError::InvalidPolicy);
        }
        let mut value = Self {
            study_qualification: input.study_qualification,
            subject_instrument_id: input.subject_instrument_id,
            benchmark: input.benchmark,
            accompanying_benchmark: input.accompanying_benchmark,
            raw_price_evidence_digest: input.raw_price_evidence_digest,
            corporate_action_content_digest: input.corporate_action_content_digest,
            corporate_action_audit_digest: input.corporate_action_audit_digest,
            corporate_action_coverage_starts_at: input.corporate_action_coverage_starts_at,
            execution_basis: input.execution_basis,
            reporting_currency: input.reporting_currency,
            subject_quantity: input.subject_quantity,
            benchmark_quantity: input.benchmark_quantity,
            maximum_entry_lag_nanos: input.maximum_entry_lag_nanos,
            maximum_exit_lag_nanos: input.maximum_exit_lag_nanos,
            execution_assumptions: input.execution_assumptions,
            seed: input.seed,
            digest: Sha256Digest::new([0; 32]),
        };
        value.digest = policy_digest(&value);
        Ok(value)
    }

    /// Returns the exact policy identity.
    #[must_use]
    pub const fn digest(self) -> Sha256Digest {
        self.digest
    }

    pub const fn study_qualification(self) -> BacktestStudyQualification {
        self.study_qualification
    }

    /// Returns the exact recommendation subject.
    #[must_use]
    pub const fn subject_instrument_id(self) -> InstrumentId {
        self.subject_instrument_id
    }

    /// Returns the exact approved benchmark binding.
    #[must_use]
    pub const fn benchmark(self) -> RecommendationBenchmarkPolicyV1 {
        self.benchmark
    }

    /// Returns the comparison selected alongside the primary before the study ran.
    #[must_use]
    pub const fn accompanying_benchmark(self) -> RecommendationBenchmarkPolicyV1 {
        self.accompanying_benchmark
    }

    /// Exact raw-price generation evidence, qualified by the application producer for all three
    /// selected instruments. This digest alone is provenance and does not confer authority.
    #[must_use]
    pub const fn raw_price_evidence_digest(self) -> Sha256Digest {
        self.raw_price_evidence_digest
    }

    /// Exact complete corporate-action content selected for subject and both comparisons.
    #[must_use]
    pub const fn corporate_action_content_digest(self) -> Sha256Digest {
        self.corporate_action_content_digest
    }

    /// Exact admission audit for the retained corporate-action population.
    #[must_use]
    pub const fn corporate_action_audit_digest(self) -> Sha256Digest {
        self.corporate_action_audit_digest
    }

    /// Inclusive start of the application's selected complete corporate-action coverage.
    #[must_use]
    pub const fn corporate_action_coverage_starts_at(self) -> Timestamp {
        self.corporate_action_coverage_starts_at
    }

    /// Returns the predeclared observed-quote or completed-daily-bar execution basis.
    #[must_use]
    pub const fn execution_basis(self) -> BacktestExecutionBasis {
        self.execution_basis
    }

    /// Daily bars do not contain observed bid/ask spreads. V1 explicitly assumes a 20 bps full
    /// spread, charged as 10 bps adverse half-spread on each leg before slippage and fees.
    #[must_use]
    pub const fn daily_bar_assumed_spread_basis_points(self) -> Option<BasisPoints> {
        match self.execution_basis {
            BacktestExecutionBasis::ObservedQuoteDepth => None,
            BacktestExecutionBasis::CompletedDailyBar => {
                Some(BasisPoints::new(DAILY_BAR_ASSUMED_FULL_SPREAD_BASIS_POINTS))
            }
        }
    }

    /// Returns the exact reporting currency required from both instruments.
    #[must_use]
    pub const fn reporting_currency(self) -> Currency {
        self.reporting_currency
    }

    /// Returns the complete research execution-policy identity.
    #[must_use]
    pub const fn execution_assumption_digest(self) -> Sha256Digest {
        self.execution_assumptions.digest()
    }

    /// Returns the exact subject lot quantity evaluated at every entry signal.
    #[must_use]
    pub const fn subject_quantity(self) -> QuantityLots {
        self.subject_quantity
    }

    /// Returns the exact benchmark lot quantity evaluated at matching coordinates.
    #[must_use]
    pub const fn benchmark_quantity(self) -> QuantityLots {
        self.benchmark_quantity
    }

    /// Returns the complete deterministic fill assumptions.
    #[must_use]
    pub const fn execution_assumptions(self) -> ResearchExecutionAssumptions {
        self.execution_assumptions
    }

    /// Returns the maximum PIT entry-search lag.
    #[must_use]
    pub const fn maximum_entry_lag_nanos(self) -> i64 {
        self.maximum_entry_lag_nanos
    }

    /// Returns the maximum target-exit search lag.
    #[must_use]
    pub const fn maximum_exit_lag_nanos(self) -> i64 {
        self.maximum_exit_lag_nanos
    }

    /// Returns the strict recommendation-outcome V1 elapsed target horizon.
    #[must_use]
    pub const fn target_horizon_nanos(self) -> i64 {
        RECOMMENDATION_TARGET_HORIZON_NANOS_V1
    }

    /// Returns the deterministic simulation seed.
    #[must_use]
    pub const fn seed(self) -> u64 {
        self.seed
    }
}

/// Caller-selected limits below fixed process hard ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationBacktestLimitsInput {
    pub max_folds: usize,
    pub max_signals: usize,
    pub max_equity_points_per_outcome: usize,
    pub max_total_equity_points: usize,
    pub max_observation_visits: usize,
}

/// Validated recommendation-kernel resource ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationBacktestLimits {
    max_folds: usize,
    max_signals: usize,
    max_equity_points_per_outcome: usize,
    max_total_equity_points: usize,
    max_observation_visits: usize,
}

impl RecommendationBacktestLimits {
    /// Rejects zero, undersized, or process-unbounded resource limits.
    pub fn try_new(
        input: RecommendationBacktestLimitsInput,
    ) -> Result<Self, RecommendationBacktestError> {
        if !(3..=HARD_MAX_RECOMMENDATION_FOLDS).contains(&input.max_folds)
            || input.max_signals == 0
            || input.max_signals > HARD_MAX_RECOMMENDATION_SIGNALS
            || !(2..=HARD_MAX_EQUITY_POINTS_PER_OUTCOME)
                .contains(&input.max_equity_points_per_outcome)
            || !(2..=HARD_MAX_TOTAL_EQUITY_POINTS).contains(&input.max_total_equity_points)
            || input.max_equity_points_per_outcome > input.max_total_equity_points
            || input.max_observation_visits == 0
            || input.max_observation_visits > HARD_MAX_OBSERVATION_VISITS
        {
            return Err(RecommendationBacktestError::InvalidLimits);
        }
        Ok(Self {
            max_folds: input.max_folds,
            max_signals: input.max_signals,
            max_equity_points_per_outcome: input.max_equity_points_per_outcome,
            max_total_equity_points: input.max_total_equity_points,
            max_observation_visits: input.max_observation_visits,
        })
    }

    /// Returns the caller-authorized OOS-fold ceiling.
    #[must_use]
    pub const fn max_folds(self) -> usize {
        self.max_folds
    }

    /// Returns the caller-authorized signal ceiling.
    #[must_use]
    pub const fn max_signals(self) -> usize {
        self.max_signals
    }

    /// Returns the per-outcome retained equity-path ceiling.
    #[must_use]
    pub const fn max_equity_points_per_outcome(self) -> usize {
        self.max_equity_points_per_outcome
    }

    /// Returns the complete evaluation retained-equity-point ceiling.
    #[must_use]
    pub const fn max_total_equity_points(self) -> usize {
        self.max_total_equity_points
    }

    /// Returns the complete deterministic observation-scan ceiling.
    #[must_use]
    pub const fn max_observation_visits(self) -> usize {
        self.max_observation_visits
    }
}

/// One nonempty half-open independent out-of-sample fold interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendationOosFoldV1 {
    fold_id: SourceIdentifier,
    starts_at: Timestamp,
    ends_at: Timestamp,
}

impl RecommendationOosFoldV1 {
    /// Constructs a nonempty fold interval `[starts_at, ends_at)`.
    pub fn try_new(
        fold_id: SourceIdentifier,
        starts_at: Timestamp,
        ends_at: Timestamp,
    ) -> Result<Self, RecommendationBacktestError> {
        if starts_at >= ends_at {
            return Err(RecommendationBacktestError::InvalidSignalPlan);
        }
        Ok(Self {
            fold_id,
            starts_at,
            ends_at,
        })
    }

    /// Returns the stable fold identity.
    #[must_use]
    pub const fn fold_id(&self) -> &SourceIdentifier {
        &self.fold_id
    }

    /// Returns the inclusive fold start.
    #[must_use]
    pub const fn starts_at(&self) -> Timestamp {
        self.starts_at
    }

    /// Returns the exclusive fold end.
    #[must_use]
    pub const fn ends_at(&self) -> Timestamp {
        self.ends_at
    }
}

/// Closed reason why the pre-authorized producer emitted no recommendation outcome.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RecommendationSignalUnavailableReasonV1 {
    InsufficientPointInTimeEvidence,
    ConflictingEvidence,
    UnsupportedInstrument,
    PolicyRejected,
    ReportingCurrencyMismatch,
    ExecutionTermsChanged,
}

/// Closed reason why a signal has no observable 365-day outcome.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RecommendationSignalCensorReasonV1 {
    TargetAfterSimulationCutoff,
    OutsideAuthorizedDataset,
}

/// Pre-authorized signal disposition. It carries evidence, not execution authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RecommendationSignalInstructionV1 {
    Entry,
    NoAction,
    Unavailable(RecommendationSignalUnavailableReasonV1),
    Censored(RecommendationSignalCensorReasonV1),
}

/// One exact PIT signal coordinate admitted to the outcome evaluator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendationSignalV1 {
    signal_id: SourceIdentifier,
    fold_index: usize,
    signal_at: Timestamp,
    available_at: Timestamp,
    source_selection_as_of: Timestamp,
    study_qualification: BacktestStudyQualification,
    target_origin: Timestamp,
    target_at: Timestamp,
    evidence_digest: Sha256Digest,
    instruction: RecommendationSignalInstructionV1,
}

impl RecommendationSignalV1 {
    /// Constructs one qualified signal with original source availability kept independently of
    /// its historical or simulated decision. Calculation/publication clocks remain actual.
    #[allow(
        clippy::too_many_arguments,
        reason = "financial origin, target and decision clocks stay independent"
    )]
    pub fn try_new(
        signal_id: SourceIdentifier,
        fold_index: usize,
        signal_at: Timestamp,
        available_at: Timestamp,
        source_selection_as_of: Timestamp,
        study_qualification: BacktestStudyQualification,
        target_origin: Timestamp,
        target_at: Timestamp,
        evidence_digest: Sha256Digest,
        instruction: RecommendationSignalInstructionV1,
    ) -> Result<Self, RecommendationBacktestError> {
        require_digest(evidence_digest)?;
        if !study_qualification.admits_clocks(
            target_origin,
            available_at,
            source_selection_as_of,
            signal_at,
        ) || target_at <= signal_at
            || target_origin
                .checked_add_nanos(RECOMMENDATION_TARGET_HORIZON_NANOS_V1)
                .map_err(|_| RecommendationBacktestError::InvalidSignalPlan)?
                != target_at
        {
            return Err(RecommendationBacktestError::InvalidSignalPlan);
        }
        Ok(Self {
            signal_id,
            fold_index,
            signal_at,
            available_at,
            source_selection_as_of,
            study_qualification,
            target_origin,
            target_at,
            evidence_digest,
            instruction,
        })
    }

    /// Returns the exact predeclared signal identity.
    #[must_use]
    pub const fn signal_id(&self) -> &SourceIdentifier {
        &self.signal_id
    }

    /// Returns the independent OOS fold index.
    #[must_use]
    pub const fn fold_index(&self) -> usize {
        self.fold_index
    }

    /// Returns the exact recommendation decision coordinate.
    #[must_use]
    pub const fn signal_at(&self) -> Timestamp {
        self.signal_at
    }

    /// Returns the original source availability. A retrospective decision can precede this
    /// actual clock; it never becomes a fabricated historical availability timestamp.
    #[must_use]
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    pub const fn source_selection_as_of(&self) -> Timestamp {
        self.source_selection_as_of
    }
    pub const fn study_qualification(&self) -> BacktestStudyQualification {
        self.study_qualification
    }

    /// Exact financial reference origin, independent of source availability and decision time.
    #[must_use]
    pub const fn target_origin(&self) -> Timestamp {
        self.target_origin
    }

    /// Original predeclared forecast target. Execution never recomputes this from decision time.
    #[must_use]
    pub const fn target_at(&self) -> Timestamp {
        self.target_at
    }

    /// Returns the exact signal-evidence identity.
    #[must_use]
    pub const fn evidence_digest(&self) -> Sha256Digest {
        self.evidence_digest
    }

    /// Returns the pre-authorized signal instruction.
    #[must_use]
    pub const fn instruction(&self) -> RecommendationSignalInstructionV1 {
        self.instruction
    }
}

/// Completeness of the predeclared signal population.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationSignalPlanCompletenessV1 {
    Complete,
    Truncated { total_signal_count: usize },
}

/// Strict current signal plan whose external digest is provenance, never an authority grant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendationSignalPlanV1 {
    study_qualification: BacktestStudyQualification,
    preauthorized_signal_plan_digest: Sha256Digest,
    completeness: RecommendationSignalPlanCompletenessV1,
    folds: Box<[RecommendationOosFoldV1]>,
    signals: Box<[RecommendationSignalV1]>,
    digest: Sha256Digest,
}

impl RecommendationSignalPlanV1 {
    /// Canonicalizes and content-identifies at least three non-overlapping OOS folds.
    ///
    /// The supplied pre-authorization digest remains opaque provenance. This pure constructor does
    /// not authenticate it or confer proposal, risk, order, or dispatch authority.
    pub fn try_new(
        preauthorized_signal_plan_digest: Sha256Digest,
        completeness: RecommendationSignalPlanCompletenessV1,
        folds: Vec<RecommendationOosFoldV1>,
        mut signals: Vec<RecommendationSignalV1>,
    ) -> Result<Self, RecommendationBacktestError> {
        require_digest(preauthorized_signal_plan_digest)?;
        if !(3..=HARD_MAX_RECOMMENDATION_FOLDS).contains(&folds.len())
            || signals.is_empty()
            || signals.len() > HARD_MAX_RECOMMENDATION_SIGNALS
            || folds
                .windows(2)
                .any(|pair| pair[0].ends_at > pair[1].starts_at)
        {
            return Err(RecommendationBacktestError::InvalidSignalPlan);
        }
        let study_qualification = signals[0].study_qualification;
        if signals
            .iter()
            .any(|signal| signal.study_qualification != study_qualification)
        {
            return Err(RecommendationBacktestError::InvalidSignalPlan);
        }
        let mut fold_ids = BTreeSet::new();
        if folds
            .iter()
            .any(|fold| !fold_ids.insert(fold.fold_id.clone()))
        {
            return Err(RecommendationBacktestError::InvalidSignalPlan);
        }
        if matches!(
            completeness,
            RecommendationSignalPlanCompletenessV1::Truncated { total_signal_count }
                if total_signal_count <= signals.len()
        ) {
            return Err(RecommendationBacktestError::InvalidSignalPlan);
        }
        signals.sort_unstable_by(|left, right| {
            left.fold_index
                .cmp(&right.fold_index)
                .then_with(|| left.signal_at.cmp(&right.signal_at))
                .then_with(|| left.signal_id.cmp(&right.signal_id))
        });
        let mut signal_ids = BTreeSet::new();
        let mut fold_signal_counts = vec![0_usize; folds.len()];
        for signal in &signals {
            let fold = folds
                .get(signal.fold_index)
                .ok_or(RecommendationBacktestError::InvalidSignalPlan)?;
            if signal.signal_at < fold.starts_at
                || signal.signal_at >= fold.ends_at
                || !signal_ids.insert(signal.signal_id.clone())
            {
                return Err(RecommendationBacktestError::InvalidSignalPlan);
            }
            fold_signal_counts[signal.fold_index] = fold_signal_counts[signal.fold_index]
                .checked_add(1)
                .ok_or(RecommendationBacktestError::LimitExceeded)?;
        }
        if fold_signal_counts.contains(&0) {
            return Err(RecommendationBacktestError::InvalidSignalPlan);
        }
        let mut value = Self {
            study_qualification,
            preauthorized_signal_plan_digest,
            completeness,
            folds: folds.into_boxed_slice(),
            signals: signals.into_boxed_slice(),
            digest: Sha256Digest::new([0; 32]),
        };
        value.digest = signal_plan_digest(&value)?;
        Ok(value)
    }

    /// Returns the canonical plan-content identity.
    #[must_use]
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }

    /// Returns the opaque pre-authorized signal-plan evidence identity.
    #[must_use]
    pub const fn preauthorized_signal_plan_digest(&self) -> Sha256Digest {
        self.preauthorized_signal_plan_digest
    }

    /// Returns explicit complete/truncated population state.
    #[must_use]
    pub const fn completeness(&self) -> RecommendationSignalPlanCompletenessV1 {
        self.completeness
    }

    /// Returns the exact canonical independent OOS folds.
    #[must_use]
    pub fn folds(&self) -> &[RecommendationOosFoldV1] {
        &self.folds
    }

    /// Returns every retained predeclared signal in canonical order.
    #[must_use]
    pub fn signals(&self) -> &[RecommendationSignalV1] {
        &self.signals
    }
}

/// One materializer-bound research instruction over exact subject and benchmark PIT rows.
#[derive(Clone, Debug, Eq, PartialEq)]
struct BoundRecommendationSignalV1 {
    signal_id: SourceIdentifier,
    fold_index: usize,
    signal_at: Timestamp,
    available_at: Timestamp,
    source_selection_as_of: Timestamp,
    study_qualification: BacktestStudyQualification,
    target_origin: Timestamp,
    target_at: Timestamp,
    subject_lineage_digest: Sha256Digest,
    benchmark_lineage_digest: Sha256Digest,
    instruction_evidence_digest: Sha256Digest,
    instruction: RecommendationSignalInstructionV1,
}

/// Immutable observation in a coordinate-fenced PIT information set.
#[derive(Clone, Copy, Debug)]
pub struct RecommendationSignalObservationV1<'dataset> {
    coordinate: Option<&'dataset market_squawk_data::OwnedFeatureDatasetInputCoordinate>,
    observation: &'dataset BacktestObservation,
}

impl<'dataset> RecommendationSignalObservationV1<'dataset> {
    /// Exact contemporaneous tick, lot and currency terms of this observation.
    #[must_use]
    pub const fn execution_terms(self) -> InstrumentExecutionTerms {
        self.observation.execution_terms
    }

    /// Exact stable instrument identity.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.observation.instrument_id()
    }

    /// Exact issuer decision coordinate.
    #[must_use]
    pub const fn decision_at(self) -> Timestamp {
        self.observation.decision_at()
    }

    /// Exact positive mid price, or explicit absence, at the decision coordinate.
    #[must_use]
    pub const fn mid_price(self) -> Option<PriceTicks> {
        self.observation.mid_price
    }

    /// Exact admitted completed-close reference in current decision units, or observed quote value.
    pub fn market_reference(self) -> Option<Money> {
        self.observation.market_reference.or_else(|| {
            self.observation.mid_price.and_then(|price| {
                price
                    .checked_to_decimal(self.observation.execution_terms.price_tick())
                    .ok()
                    .map(|amount| {
                        Money::new(amount, self.observation.execution_terms.quote_currency())
                    })
            })
        })
    }
    /// Original sealed source epoch for this coordinate only; no outcome or future panel is exposed.
    pub fn input_epoch(self) -> Option<&'dataset market_squawk_data::FeatureDatasetInputEpoch> {
        self.coordinate.map(|coordinate| coordinate.epoch())
    }
    /// Sealed data-owned epoch and native model rows for this coordinate only.
    pub fn input_coordinate(
        self,
    ) -> Option<market_squawk_data::FeatureDatasetInputCoordinate<'dataset>> {
        self.coordinate.map(|coordinate| coordinate.coordinate())
    }
    /// Canonically admitted native model rows for this coordinate, never a future panel.
    pub fn input_rows(self) -> &'dataset [market_squawk_data::ForecastFeatureRow] {
        self.coordinate.map_or(&[], |coordinate| coordinate.rows())
    }
    pub const fn source_selection_as_of(self) -> Timestamp {
        self.observation.source_selection_as_of
    }
    pub const fn available_at(self) -> Timestamp {
        self.observation.available_at()
    }

    /// Historical universe status under the retained study basis.
    #[must_use]
    pub const fn universe(self) -> HistoricalUniverseStatus {
        self.observation.universe
    }

    /// Complete canonically ordered research-feature population.
    #[must_use]
    pub fn features(self) -> &'dataset [crate::dataset::ResearchFeatureValue] {
        self.observation.features.as_ref()
    }
}

/// One paired PIT coordinate available no later than the current signal coordinate.
#[derive(Clone, Copy, Debug)]
pub struct RecommendationSignalInformationCoordinateV1<'dataset> {
    study_qualification: BacktestStudyQualification,
    subject: RecommendationSignalObservationV1<'dataset>,
    benchmark: RecommendationSignalObservationV1<'dataset>,
    target_origin: Timestamp,
    target_at: Timestamp,
}

impl<'dataset> RecommendationSignalInformationCoordinateV1<'dataset> {
    pub const fn study_qualification(self) -> BacktestStudyQualification {
        self.study_qualification
    }
    pub const fn source_selection_as_of(self) -> Timestamp {
        self.subject.source_selection_as_of()
    }

    /// Exact shared subject/benchmark decision coordinate.
    #[must_use]
    pub const fn signal_at(self) -> Timestamp {
        self.subject.decision_at()
    }

    /// Exact financial reference time authenticated by the current source epoch.
    #[must_use]
    pub const fn target_origin(self) -> Timestamp {
        self.target_origin
    }

    /// Fixed 365-day forecast target retained by the source epoch.
    #[must_use]
    pub const fn target_at(self) -> Timestamp {
        self.target_at
    }

    /// Subject observation with private lineage withheld from the issuer.
    #[must_use]
    pub const fn subject(self) -> RecommendationSignalObservationV1<'dataset> {
        self.subject
    }

    /// Benchmark observation from the same pinned dataset and decision coordinate.
    #[must_use]
    pub const fn benchmark(self) -> RecommendationSignalObservationV1<'dataset> {
        self.benchmark
    }
}

/// Coordinate-local PIT information available when issuing one exact signal.
///
/// No prior or future coordinate values, labels, realized execution stream or study outcomes
/// are exposed. The sealed current-coordinate source view retains the actual publication
/// identity, source clocks and declared study basis needed by the model and valuation producers.
#[derive(Clone, Copy, Debug)]
pub struct RecommendationSignalInformationSetV1<'dataset> {
    current: RecommendationSignalInformationCoordinateV1<'dataset>,
}

impl<'dataset> RecommendationSignalInformationSetV1<'dataset> {
    pub const fn study_qualification(self) -> BacktestStudyQualification {
        self.current.study_qualification()
    }
    pub const fn basis(self) -> HistoricalStudyBasis {
        self.study_qualification().basis()
    }
    pub const fn snapshot_as_of(self) -> Timestamp {
        self.study_qualification().snapshot_as_of()
    }
    pub const fn source_snapshot_digest(self) -> Sha256Digest {
        self.study_qualification().source_snapshot_digest()
    }
    pub const fn limitations(self) -> &'static [HistoricalStudyLimitation] {
        self.study_qualification().limitations()
    }
    pub const fn source_selection_as_of(self) -> Timestamp {
        self.current.source_selection_as_of()
    }

    /// Exact decision coordinate being issued.
    #[must_use]
    pub const fn signal_at(self) -> Timestamp {
        self.current.signal_at()
    }

    #[must_use]
    pub const fn target_origin(self) -> Timestamp {
        self.current.target_origin()
    }

    #[must_use]
    pub const fn target_at(self) -> Timestamp {
        self.current.target_at()
    }

    /// Exact subject/benchmark pair at this coordinate.
    #[must_use]
    pub const fn current(self) -> RecommendationSignalInformationCoordinateV1<'dataset> {
        self.current
    }
}

/// One coordinate-local issuer result without caller-controlled time, lineage, or plan authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendationSignalIssuanceV1 {
    signal_id: SourceIdentifier,
    study_qualification: BacktestStudyQualification,
    source_selection_as_of: Timestamp,
    target_origin: Timestamp,
    target_at: Timestamp,
    instruction_evidence_digest: Sha256Digest,
    instruction: RecommendationSignalInstructionV1,
    instruction_evidence_payload: Option<Box<str>>,
}

impl RecommendationSignalIssuanceV1 {
    /// Constructs Entry, NoAction, or typed Unavailable evidence for one inspected coordinate.
    pub fn try_new(
        signal_id: SourceIdentifier,
        study_qualification: BacktestStudyQualification,
        source_selection_as_of: Timestamp,
        target_origin: Timestamp,
        target_at: Timestamp,
        instruction_evidence_digest: Sha256Digest,
        instruction: RecommendationSignalInstructionV1,
    ) -> Result<Self, RecommendationSignalPlanMaterializationErrorV1> {
        if study_qualification.validate().is_err()
            || source_selection_as_of > study_qualification.snapshot_as_of()
            || instruction_evidence_digest.bytes() == [0; 32]
            || target_origin
                .checked_add_nanos(RECOMMENDATION_TARGET_HORIZON_NANOS_V1)
                .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::InvalidInstruction)?
                != target_at
            || matches!(instruction, RecommendationSignalInstructionV1::Censored(_))
        {
            return Err(RecommendationSignalPlanMaterializationErrorV1::InvalidInstruction);
        }
        Ok(Self {
            signal_id,
            study_qualification,
            source_selection_as_of,
            target_origin,
            target_at,
            instruction_evidence_digest,
            instruction,
            instruction_evidence_payload: None,
        })
    }

    /// Attaches the original bounded receipt after matching its existing evidence commitment.
    pub fn with_instruction_evidence_payload(
        mut self,
        payload: Box<str>,
    ) -> Result<Self, RecommendationSignalPlanMaterializationErrorV1> {
        validate_instruction_payload(self.instruction_evidence_digest, &payload)?;
        self.instruction_evidence_payload = Some(payload);
        Ok(self)
    }

    /// Exact code-owned issuer signal identity.
    #[must_use]
    pub const fn signal_id(&self) -> &SourceIdentifier {
        &self.signal_id
    }

    #[must_use]
    pub const fn target_origin(&self) -> Timestamp {
        self.target_origin
    }

    #[must_use]
    pub const fn target_at(&self) -> Timestamp {
        self.target_at
    }

    /// Immutable evidence for the issuer-derived instruction.
    #[must_use]
    pub const fn instruction_evidence_digest(&self) -> Sha256Digest {
        self.instruction_evidence_digest
    }

    /// Exact issuer-supplied economic instruction; the materializer does not derive it.
    #[must_use]
    pub const fn instruction(&self) -> RecommendationSignalInstructionV1 {
        self.instruction
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RetainedInstructionEvidenceV1 {
    digest: [u8; 32],
    payload: Box<str>,
}

fn validate_instruction_payload(
    digest: Sha256Digest,
    payload: &str,
) -> Result<(), RecommendationSignalPlanMaterializationErrorV1> {
    if payload.is_empty() || payload.len() > 16_384 {
        return Err(RecommendationSignalPlanMaterializationErrorV1::LimitExceeded);
    }
    if Sha256Digest::new(Sha256::digest(payload.as_bytes()).into()) != digest {
        return Err(RecommendationSignalPlanMaterializationErrorV1::InstructionEvidenceMismatch);
    }
    Ok(())
}

/// Explicit semantic identity for a research signal issuer.
///
/// This identity is provenance only. Constructing it never grants installed application or product
/// evidence authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendationSignalIssuerIdentityV1 {
    producer: SourceIdentifier,
    semantic_revision: SourceIdentifier,
    bindings_digest: Sha256Digest,
    digest: Sha256Digest,
}

impl RecommendationSignalIssuerIdentityV1 {
    /// Constructs a content-derived research issuer identity.
    pub fn try_new(
        producer: SourceIdentifier,
        semantic_revision: SourceIdentifier,
        bindings_digest: Sha256Digest,
    ) -> Result<Self, RecommendationSignalPlanMaterializationErrorV1> {
        if bindings_digest.bytes() == [0; 32] {
            return Err(RecommendationSignalPlanMaterializationErrorV1::InvalidInstruction);
        }
        let mut value = Self {
            producer,
            semantic_revision,
            bindings_digest,
            digest: Sha256Digest::new([0; 32]),
        };
        value.digest = recommendation_signal_issuer_identity_digest(&value);
        Ok(value)
    }

    /// Stable issuer producer name.
    #[must_use]
    pub const fn producer(&self) -> &SourceIdentifier {
        &self.producer
    }

    /// Exact code-owned semantic revision name.
    #[must_use]
    pub const fn semantic_revision(&self) -> &SourceIdentifier {
        &self.semantic_revision
    }

    /// Immutable producer input/profile binding identity.
    #[must_use]
    pub const fn bindings_digest(&self) -> Sha256Digest {
        self.bindings_digest
    }

    /// Content-derived identity of producer, semantic revision, and bindings.
    #[must_use]
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }
}

/// Dataset- and policy-bound strict signal plan plus its materialization identity.
///
/// This wrapper is required by [`RecommendationBacktestKernelV1::run_materialized_study`]. Its
/// private fields prevent a caller from substituting a plan produced against another dataset or
/// policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedRecommendationSignalPlanV1 {
    dataset_identity: Sha256Digest,
    dataset_manifest_content: Sha256Digest,
    object_graph_digest: Sha256Digest,
    point_in_time_content: Sha256Digest,
    point_in_time_audit: Sha256Digest,
    policy_digest: Sha256Digest,
    issuer_identity: RecommendationSignalIssuerIdentityV1,
    evaluation_starts_at: Timestamp,
    evaluation_ends_at: Timestamp,
    paired_observation_count: usize,
    limits: RecommendationBacktestLimits,
    signal_plan: RecommendationSignalPlanV1,
    instruction_evidence: Box<[Option<RetainedInstructionEvidenceV1>]>,
    digest: Sha256Digest,
}

impl MaterializedRecommendationSignalPlanV1 {
    /// Exact pinned dataset identity used during materialization.
    #[must_use]
    pub const fn dataset_identity(&self) -> Sha256Digest {
        self.dataset_identity
    }

    /// Exact immutable dataset-manifest content identity.
    #[must_use]
    pub const fn dataset_manifest_content(&self) -> Sha256Digest {
        self.dataset_manifest_content
    }

    /// Exact pinned object-graph identity.
    #[must_use]
    pub const fn object_graph_digest(&self) -> Sha256Digest {
        self.object_graph_digest
    }

    /// Exact point-in-time content identity.
    #[must_use]
    pub const fn point_in_time_content(&self) -> Sha256Digest {
        self.point_in_time_content
    }

    /// Exact point-in-time audit identity.
    #[must_use]
    pub const fn point_in_time_audit(&self) -> Sha256Digest {
        self.point_in_time_audit
    }

    /// Exact strict recommendation policy identity.
    #[must_use]
    pub const fn policy_digest(&self) -> Sha256Digest {
        self.policy_digest
    }

    /// Exact research issuer semantic identity bound during sequential materialization.
    #[must_use]
    pub const fn issuer_identity(&self) -> &RecommendationSignalIssuerIdentityV1 {
        &self.issuer_identity
    }

    /// Inclusive exact evaluation start.
    #[must_use]
    pub const fn evaluation_starts_at(&self) -> Timestamp {
        self.evaluation_starts_at
    }

    /// Exclusive exact evaluation end and required simulation cutoff.
    #[must_use]
    pub const fn evaluation_ends_at(&self) -> Timestamp {
        self.evaluation_ends_at
    }

    /// Number of exact subject/benchmark PIT coordinate pairs validated in one bounded scan.
    #[must_use]
    pub const fn paired_observation_count(&self) -> usize {
        self.paired_observation_count
    }

    /// Exact bounded-work ceilings applied during materialization and required during evaluation.
    #[must_use]
    pub const fn limits(&self) -> RecommendationBacktestLimits {
        self.limits
    }

    /// Complete canonical signal plan admitted by the materializer.
    #[must_use]
    pub const fn signal_plan(&self) -> &RecommendationSignalPlanV1 {
        &self.signal_plan
    }

    /// Complete materialization receipt identity.
    #[must_use]
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }

    /// Consumes the receipt and returns the complete signal plan for controlled persistence.
    #[must_use]
    pub fn into_signal_plan(self) -> RecommendationSignalPlanV1 {
        self.signal_plan
    }

    /// Encodes the complete immutable signal population for bounded controlled persistence.
    /// This research receipt grants no application recommendation authority by itself.
    pub fn encode_persisted(
        &self,
        maximum_bytes: usize,
    ) -> Result<Vec<u8>, RecommendationSignalPlanMaterializationErrorV1> {
        // Every encoded signal exceeds this lower bound before its identifier and timestamps.
        // Reject a population that cannot fit before allocating a second owned representation.
        if maximum_bytes == 0
            || self
                .signal_plan
                .signals
                .len()
                .checked_mul(128)
                .is_none_or(|minimum| minimum > maximum_bytes)
        {
            return Err(RecommendationSignalPlanMaterializationErrorV1::LimitExceeded);
        }
        let minimum_payload_bytes = self
            .instruction_evidence
            .iter()
            .try_fold(0_usize, |total, item| {
                total.checked_add(item.as_ref().map_or(0, |value| value.payload.len()))
            })
            .ok_or(RecommendationSignalPlanMaterializationErrorV1::LimitExceeded)?;
        if minimum_payload_bytes > maximum_bytes {
            return Err(RecommendationSignalPlanMaterializationErrorV1::LimitExceeded);
        }
        let wire = PersistedRecommendationMaterializationV1::from_materialized(self);
        let mut writer = BoundedMaterializationWriter {
            bytes: Vec::new(),
            maximum: maximum_bytes,
        };
        serde_json::to_writer(&mut writer, &wire)
            .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::LimitExceeded)?;
        Ok(writer.bytes)
    }

    /// Restores a canonical research receipt against freshly pinned exact input authority.
    /// Callers must authenticate their retained record separately; a digest is provenance only.
    pub fn restore_persisted(
        bytes: &[u8],
        maximum_bytes: usize,
        dataset: &BacktestDataset,
        policy: RecommendationBacktestPolicyV1,
        limits: RecommendationBacktestLimits,
    ) -> Result<Self, RecommendationSignalPlanMaterializationErrorV1> {
        use RecommendationSignalPlanMaterializationErrorV1 as Error;
        if maximum_bytes == 0 || bytes.len() > maximum_bytes {
            return Err(Error::LimitExceeded);
        }
        let wire: PersistedRecommendationMaterializationV1 =
            serde_json::from_slice(bytes).map_err(|_| Error::MaterializationDrift)?;
        validate_recommendation_materialization_policy(policy)?;
        if wire.schema_version != 1
            || wire.signals.len() > limits.max_signals()
            || wire.folds.len() != RECOMMENDATION_OOS_FOLD_COUNT_V1
            || wire.paired_observation_count != wire.signals.len()
            || wire.instruction_evidence.len() != wire.signals.len()
            || wire.limits != persisted_limits(limits)
            || wire
                .evaluation_starts_at
                .checked_add_nanos(RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1)
                .map_err(|_| Error::MaterializationDrift)?
                != wire.evaluation_ends_at
        {
            return Err(Error::MaterializationDrift);
        }
        let issuer_identity = RecommendationSignalIssuerIdentityV1::try_new(
            wire.producer,
            wire.semantic_revision,
            Sha256Digest::new(wire.bindings_digest),
        )?;
        let folds = wire
            .folds
            .into_iter()
            .map(|(id, start, end)| {
                RecommendationOosFoldV1::try_new(id, start, end)
                    .map_err(|_| Error::MaterializationDrift)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let signals = wire
            .signals
            .into_iter()
            .map(|signal| {
                if matches!(
                    signal.instruction,
                    RecommendationSignalInstructionV1::Censored(_)
                ) {
                    return Err(Error::MaterializationDrift);
                }
                RecommendationSignalV1::try_new(
                    signal.id,
                    signal.fold_index,
                    signal.signal_at,
                    signal.available_at,
                    signal.source_selection_as_of,
                    signal.study_qualification,
                    signal.target_origin,
                    signal.target_at,
                    Sha256Digest::new(signal.evidence_digest),
                    signal.instruction,
                )
                .map_err(|_| Error::MaterializationDrift)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let signal_plan = RecommendationSignalPlanV1::try_new(
            Sha256Digest::new(wire.preauthorized_signal_plan_digest),
            RecommendationSignalPlanCompletenessV1::Complete,
            folds,
            signals,
        )
        .map_err(|_| Error::MaterializationDrift)?;
        let materialized = Self {
            dataset_identity: Sha256Digest::new(wire.dataset_identity),
            dataset_manifest_content: Sha256Digest::new(wire.dataset_manifest_content),
            object_graph_digest: Sha256Digest::new(wire.object_graph_digest),
            point_in_time_content: Sha256Digest::new(wire.point_in_time_content),
            point_in_time_audit: Sha256Digest::new(wire.point_in_time_audit),
            policy_digest: Sha256Digest::new(wire.policy_digest),
            issuer_identity,
            evaluation_starts_at: wire.evaluation_starts_at,
            evaluation_ends_at: wire.evaluation_ends_at,
            paired_observation_count: wire.paired_observation_count,
            limits,
            signal_plan,
            instruction_evidence: wire.instruction_evidence.into_boxed_slice(),
            digest: Sha256Digest::new(wire.digest),
        };
        materialized.validate_against(dataset, policy, limits)?;
        if materialized.encode_persisted(maximum_bytes)? != bytes {
            return Err(Error::MaterializationDrift);
        }
        Ok(materialized)
    }

    fn validate_against(
        &self,
        dataset: &BacktestDataset,
        policy: RecommendationBacktestPolicyV1,
        limits: RecommendationBacktestLimits,
    ) -> Result<(), RecommendationSignalPlanMaterializationErrorV1> {
        if dataset.study_qualification != Some(policy.study_qualification)
            || self.signal_plan.study_qualification != policy.study_qualification
            || self.dataset_identity != dataset.identity()
            || self.dataset_manifest_content != dataset.manifest.content_hash()
            || self.object_graph_digest != dataset.object_graph_digest()
            || self.point_in_time_content != dataset.point_in_time_content
            || self.point_in_time_audit != dataset.point_in_time_audit
            || self.policy_digest != policy.digest()
            || self.issuer_identity.digest
                != recommendation_signal_issuer_identity_digest(&self.issuer_identity)
            || self.limits != limits
            || self.signal_plan.completeness() != RecommendationSignalPlanCompletenessV1::Complete
            || materialized_signal_plan_digest(self)? != self.digest
        {
            return Err(RecommendationSignalPlanMaterializationErrorV1::MaterializationDrift);
        }
        for signal in self.signal_plan.signals() {
            let mut available_at = Timestamp::from_unix_nanos(i64::MIN);
            let mut selection = None;
            for instrument in [
                policy.subject_instrument_id(),
                policy.benchmark().instrument_id(),
            ] {
                let observation = dataset
                    .observations
                    .find(signal.signal_at(), instrument)
                    .map_err(|_| {
                        RecommendationSignalPlanMaterializationErrorV1::MaterializationDrift
                    })?;
                if materialization_target(&observation, policy.execution_basis)?
                    != (signal.target_origin(), signal.target_at())
                {
                    return Err(
                        RecommendationSignalPlanMaterializationErrorV1::MaterializationDrift,
                    );
                }
                if selection
                    .replace(observation.source_selection_as_of)
                    .is_some_and(|prior| prior != observation.source_selection_as_of)
                {
                    return Err(
                        RecommendationSignalPlanMaterializationErrorV1::MaterializationDrift,
                    );
                }
                available_at = available_at.max(observation.available_at());
            }
            if available_at != signal.available_at()
                || selection != Some(signal.source_selection_as_of)
            {
                return Err(RecommendationSignalPlanMaterializationErrorV1::MaterializationDrift);
            }
        }
        Ok(())
    }
}

/// Refuses before an encoded receipt exceeds its admitted byte budget.
struct BoundedMaterializationWriter {
    bytes: Vec<u8>,
    maximum: usize,
}
impl std::io::Write for BoundedMaterializationWriter {
    fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(input.len())
            .is_none_or(|len| len > self.maximum)
        {
            return Err(std::io::Error::other("materialization byte limit exceeded"));
        }
        self.bytes
            .try_reserve(input.len())
            .map_err(|_| std::io::Error::other("materialization capacity exceeded"))?;
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedRecommendationMaterializationV1 {
    schema_version: u16,
    dataset_identity: [u8; 32],
    dataset_manifest_content: [u8; 32],
    object_graph_digest: [u8; 32],
    point_in_time_content: [u8; 32],
    point_in_time_audit: [u8; 32],
    policy_digest: [u8; 32],
    producer: SourceIdentifier,
    semantic_revision: SourceIdentifier,
    bindings_digest: [u8; 32],
    evaluation_starts_at: Timestamp,
    evaluation_ends_at: Timestamp,
    paired_observation_count: usize,
    limits: [usize; 5],
    preauthorized_signal_plan_digest: [u8; 32],
    folds: Vec<(SourceIdentifier, Timestamp, Timestamp)>,
    signals: Vec<PersistedRecommendationSignalV1>,
    instruction_evidence: Vec<Option<RetainedInstructionEvidenceV1>>,
    digest: [u8; 32],
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedRecommendationSignalV1 {
    id: SourceIdentifier,
    fold_index: usize,
    signal_at: Timestamp,
    available_at: Timestamp,
    source_selection_as_of: Timestamp,
    study_qualification: BacktestStudyQualification,
    target_origin: Timestamp,
    target_at: Timestamp,
    evidence_digest: [u8; 32],
    instruction: RecommendationSignalInstructionV1,
}

const fn persisted_limits(limits: RecommendationBacktestLimits) -> [usize; 5] {
    [
        limits.max_folds(),
        limits.max_signals(),
        limits.max_equity_points_per_outcome(),
        limits.max_total_equity_points(),
        limits.max_observation_visits(),
    ]
}

impl PersistedRecommendationMaterializationV1 {
    fn from_materialized(value: &MaterializedRecommendationSignalPlanV1) -> Self {
        Self {
            schema_version: 1,
            dataset_identity: value.dataset_identity.bytes(),
            dataset_manifest_content: value.dataset_manifest_content.bytes(),
            object_graph_digest: value.object_graph_digest.bytes(),
            point_in_time_content: value.point_in_time_content.bytes(),
            point_in_time_audit: value.point_in_time_audit.bytes(),
            policy_digest: value.policy_digest.bytes(),
            producer: value.issuer_identity.producer.clone(),
            semantic_revision: value.issuer_identity.semantic_revision.clone(),
            bindings_digest: value.issuer_identity.bindings_digest.bytes(),
            evaluation_starts_at: value.evaluation_starts_at,
            evaluation_ends_at: value.evaluation_ends_at,
            paired_observation_count: value.paired_observation_count,
            limits: persisted_limits(value.limits),
            preauthorized_signal_plan_digest: value
                .signal_plan
                .preauthorized_signal_plan_digest
                .bytes(),
            folds: value
                .signal_plan
                .folds
                .iter()
                .map(|fold| (fold.fold_id.clone(), fold.starts_at, fold.ends_at))
                .collect(),
            signals: value
                .signal_plan
                .signals
                .iter()
                .map(|signal| PersistedRecommendationSignalV1 {
                    id: signal.signal_id.clone(),
                    fold_index: signal.fold_index,
                    signal_at: signal.signal_at,
                    available_at: signal.available_at,
                    source_selection_as_of: signal.source_selection_as_of,
                    study_qualification: signal.study_qualification,
                    target_origin: signal.target_origin,
                    target_at: signal.target_at,
                    evidence_digest: signal.evidence_digest.bytes(),
                    instruction: signal.instruction,
                })
                .collect(),
            instruction_evidence: value.instruction_evidence.to_vec(),
            digest: value.digest.bytes(),
        }
    }
}

/// Pure bounded resolver for the strict three-fold recommendation signal population.
#[derive(Clone, Copy, Debug, Default)]
pub struct RecommendationSignalPlanMaterializerV1;

impl RecommendationSignalPlanMaterializerV1 {
    /// Returns the exact predeclared three two-year OOS intervals used by materialization.
    pub fn oos_folds(
        evaluation_starts_at: Timestamp,
    ) -> Result<Vec<RecommendationOosFoldV1>, RecommendationSignalPlanMaterializationErrorV1> {
        recommendation_oos_folds(evaluation_starts_at)
    }

    /// Sequentially issues and binds every instruction against its exact PIT information set.
    ///
    /// The callback runs once per coordinate in ascending time order. It sees only observations
    /// available no later than the current coordinate, and no future panel cardinality or fold
    /// bounds. Instructions are bound immediately to private lineage and cannot be rebound across
    /// datasets. This pure generic path produces research materialization, never installed product
    /// authority or execution authority.
    pub fn materialize_sequentially<I>(
        dataset: &BacktestDataset,
        policy: RecommendationBacktestPolicyV1,
        evaluation_starts_at: Timestamp,
        issuer_identity: RecommendationSignalIssuerIdentityV1,
        limits: RecommendationBacktestLimits,
        mut issue: I,
    ) -> Result<
        MaterializedRecommendationSignalPlanV1,
        RecommendationSignalPlanMaterializationErrorV1,
    >
    where
        I: for<'information> FnMut(
            &RecommendationSignalInformationSetV1<'information>,
        ) -> Result<
            RecommendationSignalIssuanceV1,
            RecommendationSignalPlanMaterializationErrorV1,
        >,
    {
        validate_recommendation_materialization_policy(policy)?;
        if issuer_identity.digest != recommendation_signal_issuer_identity_digest(&issuer_identity)
        {
            return Err(RecommendationSignalPlanMaterializationErrorV1::InvalidInstruction);
        }
        if dataset.study_qualification != Some(policy.study_qualification) {
            return Err(RecommendationSignalPlanMaterializationErrorV1::DatasetScopeMismatch);
        }
        let evaluation_ends_at = evaluation_starts_at
            .checked_add_nanos(RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1)
            .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::InvalidEvaluationWindow)?;
        let paired_capacity =
            dataset
                .observations
                .iter()
                .try_fold(0usize, |count, observation| {
                    let observation = observation.map_err(|_| {
                        RecommendationSignalPlanMaterializationErrorV1::MaterializationDrift
                    })?;
                    Ok::<_, RecommendationSignalPlanMaterializationErrorV1>(
                        count
                            + usize::from(
                                observation.instrument_id() == policy.subject_instrument_id()
                                    && observation.decision_at() >= evaluation_starts_at
                                    && observation.decision_at() < evaluation_ends_at,
                            ),
                    )
                })?;
        if dataset.observations.len() > limits.max_observation_visits
            || paired_capacity > limits.max_signals
        {
            return Err(RecommendationSignalPlanMaterializationErrorV1::LimitExceeded);
        }
        let folds = recommendation_oos_folds(evaluation_starts_at)?;
        let evaluation_ends_at = folds
            .last()
            .map(RecommendationOosFoldV1::ends_at)
            .ok_or(RecommendationSignalPlanMaterializationErrorV1::InvalidEvaluationWindow)?;
        let mut bound = Vec::new();
        bound
            .try_reserve_exact(paired_capacity)
            .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::LimitExceeded)?;
        let mut instruction_evidence = Vec::new();
        instruction_evidence
            .try_reserve_exact(paired_capacity)
            .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::LimitExceeded)?;
        let mut signal_ids = BTreeSet::new();
        let mut fold_pair_counts = [0_usize; RECOMMENDATION_OOS_FOLD_COUNT_V1];
        let mut fold_first = [None; RECOMMENDATION_OOS_FOLD_COUNT_V1];
        let mut fold_last = [None; RECOMMENDATION_OOS_FOLD_COUNT_V1];
        let mut previous_decision_at = None;
        let mut subject_terms = None;
        let mut benchmark_terms = None;
        for coordinate in dataset.observations.panels() {
            let coordinate = coordinate.map_err(|_| {
                RecommendationSignalPlanMaterializationErrorV1::MaterializationDrift
            })?;
            if coordinate.first().is_none_or(|observation| {
                observation.decision_at() < evaluation_starts_at
                    || observation.decision_at() >= evaluation_ends_at
            }) {
                continue;
            }
            let mut subject = None;
            let mut benchmark = None;
            let mut accompanying = None;
            for observation in &coordinate {
                let slot = if observation.instrument_id() == policy.subject_instrument_id() {
                    &mut subject
                } else if observation.instrument_id() == policy.benchmark().instrument_id() {
                    &mut benchmark
                } else if observation.instrument_id()
                    == policy.accompanying_benchmark().instrument_id()
                {
                    &mut accompanying
                } else {
                    return Err(
                        RecommendationSignalPlanMaterializationErrorV1::DatasetScopeMismatch,
                    );
                };
                if slot.replace(observation).is_some() {
                    return Err(
                        RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel,
                    );
                }
            }
            // The accompanying comparison cannot change the primary signal population. Its own
            // observations remain in the exact pinned dataset for independent outcome evaluation.
            if subject.is_none() && benchmark.is_none() {
                continue;
            }
            let subject = subject.ok_or(
                RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel,
            )?;
            let benchmark = benchmark.ok_or(
                RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel,
            )?;
            validate_materialization_observation(subject, policy)?;
            validate_materialization_observation(benchmark, policy)?;
            retain_stable_execution_terms(&mut subject_terms, subject.execution_terms)?;
            retain_stable_execution_terms(&mut benchmark_terms, benchmark.execution_terms)?;
            let decision_at = subject.decision_at();
            if previous_decision_at.is_some_and(|previous: Timestamp| {
                decision_at
                    .unix_nanos()
                    .checked_sub(previous.unix_nanos())
                    .is_none_or(|gap| {
                        gap <= 0
                            || gap
                                > policy
                                    .maximum_entry_lag_nanos()
                                    .min(policy.maximum_exit_lag_nanos())
                    })
            }) {
                return Err(
                    RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel,
                );
            }
            previous_decision_at = Some(decision_at);
            let fold_index = recommendation_fold_index(&folds, decision_at)
                .ok_or(RecommendationSignalPlanMaterializationErrorV1::DatasetScopeMismatch)?;
            fold_pair_counts[fold_index] = fold_pair_counts[fold_index]
                .checked_add(1)
                .ok_or(RecommendationSignalPlanMaterializationErrorV1::LimitExceeded)?;
            let _ = fold_first[fold_index].get_or_insert(decision_at);
            fold_last[fold_index] = Some(decision_at);
            let (target_origin, target_at) =
                materialization_target(subject, policy.execution_basis)?;
            let subject_coordinate = subject
                .input_coordinate
                .as_ref()
                .map(|handle| handle.load())
                .transpose()
                .map_err(|_| {
                    RecommendationSignalPlanMaterializationErrorV1::MaterializationDrift
                })?;
            let benchmark_coordinate = benchmark
                .input_coordinate
                .as_ref()
                .map(|handle| handle.load())
                .transpose()
                .map_err(|_| {
                    RecommendationSignalPlanMaterializationErrorV1::MaterializationDrift
                })?;
            let coordinate = RecommendationSignalInformationCoordinateV1 {
                study_qualification: policy.study_qualification,
                subject: RecommendationSignalObservationV1 {
                    observation: subject,
                    coordinate: subject_coordinate.as_ref(),
                },
                benchmark: RecommendationSignalObservationV1 {
                    observation: benchmark,
                    coordinate: benchmark_coordinate.as_ref(),
                },
                target_origin,
                target_at,
            };
            if materialization_target(benchmark, policy.execution_basis)?
                != (coordinate.target_origin, coordinate.target_at)
            {
                return Err(
                    RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel,
                );
            }
            if subject.source_selection_as_of != benchmark.source_selection_as_of {
                return Err(
                    RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel,
                );
            }
            let information = RecommendationSignalInformationSetV1 {
                current: coordinate,
            };
            let issued = issue(&information)?;
            if issued.study_qualification != information.study_qualification()
                || issued.source_selection_as_of != information.source_selection_as_of()
                || issued.target_origin != information.target_origin()
                || issued.target_at != information.target_at()
                || !signal_ids.insert(issued.signal_id.clone())
            {
                return Err(RecommendationSignalPlanMaterializationErrorV1::InvalidInstruction);
            }
            if issued.instruction == RecommendationSignalInstructionV1::Entry {
                if !materialization_entry_evidence(subject, policy.execution_basis)
                    || !materialization_entry_evidence(benchmark, policy.execution_basis)
                {
                    return Err(
                        RecommendationSignalPlanMaterializationErrorV1::InstructionEvidenceMismatch,
                    );
                }
            }
            let available_at = subject.available_at().max(benchmark.available_at());
            if !policy.study_qualification.admits_clocks(
                target_origin,
                available_at,
                subject.source_selection_as_of,
                decision_at,
            ) || [
                subject.lineage_digest,
                benchmark.lineage_digest,
                issued.instruction_evidence_digest,
            ]
            .into_iter()
            .any(|digest| digest.bytes() == [0; 32])
            {
                return Err(RecommendationSignalPlanMaterializationErrorV1::InvalidInstruction);
            }
            instruction_evidence.push(issued.instruction_evidence_payload.map(|payload| {
                RetainedInstructionEvidenceV1 {
                    digest: issued.instruction_evidence_digest.bytes(),
                    payload,
                }
            }));
            bound.push(BoundRecommendationSignalV1 {
                signal_id: issued.signal_id,
                fold_index,
                signal_at: decision_at,
                available_at,
                source_selection_as_of: subject.source_selection_as_of,
                study_qualification: policy.study_qualification,
                target_origin: issued.target_origin,
                target_at: issued.target_at,
                subject_lineage_digest: subject.lineage_digest,
                benchmark_lineage_digest: benchmark.lineage_digest,
                instruction_evidence_digest: issued.instruction_evidence_digest,
                instruction: issued.instruction,
            });
        }
        if fold_pair_counts.contains(&0) {
            return Err(RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel);
        }
        for (fold_index, fold) in folds.iter().enumerate() {
            let first = fold_first[fold_index].ok_or(
                RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel,
            )?;
            let last = fold_last[fold_index].ok_or(
                RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel,
            )?;
            let maximum_first = fold
                .starts_at()
                .checked_add_nanos(policy.maximum_entry_lag_nanos())
                .map_err(|_| {
                    RecommendationSignalPlanMaterializationErrorV1::InvalidEvaluationWindow
                })?;
            let minimum_last = fold
                .ends_at()
                .checked_sub_nanos(policy.maximum_exit_lag_nanos())
                .map_err(|_| {
                    RecommendationSignalPlanMaterializationErrorV1::InvalidEvaluationWindow
                })?;
            if first > maximum_first || last < minimum_last {
                return Err(
                    RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel,
                );
            }
        }
        let signal_plan_digest = sequential_signal_plan_digest(
            dataset,
            policy,
            &issuer_identity,
            evaluation_starts_at,
            evaluation_ends_at,
            limits,
            &bound,
        )?;
        let mut materialized_signals = Vec::new();
        materialized_signals
            .try_reserve_exact(bound.len())
            .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::LimitExceeded)?;
        for instruction in &bound {
            let evidence_digest = materialized_signal_evidence_digest(
                dataset.identity(),
                policy.digest(),
                signal_plan_digest,
                instruction,
            )?;
            materialized_signals.push(
                RecommendationSignalV1::try_new(
                    instruction.signal_id.clone(),
                    instruction.fold_index,
                    instruction.signal_at,
                    instruction.available_at,
                    instruction.source_selection_as_of,
                    instruction.study_qualification,
                    instruction.target_origin,
                    instruction.target_at,
                    evidence_digest,
                    instruction.instruction,
                )
                .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::InvalidInstruction)?,
            );
        }
        let signal_plan = RecommendationSignalPlanV1::try_new(
            signal_plan_digest,
            RecommendationSignalPlanCompletenessV1::Complete,
            folds,
            materialized_signals,
        )
        .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::InvalidInstruction)?;
        let mut materialized = MaterializedRecommendationSignalPlanV1 {
            dataset_identity: dataset.identity(),
            dataset_manifest_content: dataset.manifest.content_hash(),
            object_graph_digest: dataset.object_graph_digest(),
            point_in_time_content: dataset.point_in_time_content,
            point_in_time_audit: dataset.point_in_time_audit,
            policy_digest: policy.digest(),
            issuer_identity,
            evaluation_starts_at,
            evaluation_ends_at,
            paired_observation_count: paired_capacity,
            limits,
            signal_plan,
            instruction_evidence: instruction_evidence.into_boxed_slice(),
            digest: Sha256Digest::new([0; 32]),
        };
        materialized.digest = materialized_signal_plan_digest(&materialized)?;
        Ok(materialized)
    }
}

fn validate_recommendation_materialization_policy(
    policy: RecommendationBacktestPolicyV1,
) -> Result<(), RecommendationSignalPlanMaterializationErrorV1> {
    let conservative = recommendation_conservative_execution_assumptions_v1()
        .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::PolicyMismatch)?;
    let one_lot = QuantityLots::new(1)
        .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::PolicyMismatch)?;
    if policy.subject_quantity() != one_lot
        || policy.benchmark_quantity() != one_lot
        || policy.execution_assumptions() != conservative
    {
        return Err(RecommendationSignalPlanMaterializationErrorV1::PolicyMismatch);
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "every dataset, policy, interval, limit, issuer, and instruction field is independent"
)]
fn sequential_signal_plan_digest(
    dataset: &BacktestDataset,
    policy: RecommendationBacktestPolicyV1,
    issuer_identity: &RecommendationSignalIssuerIdentityV1,
    evaluation_starts_at: Timestamp,
    evaluation_ends_at: Timestamp,
    limits: RecommendationBacktestLimits,
    instructions: &[BoundRecommendationSignalV1],
) -> Result<Sha256Digest, RecommendationSignalPlanMaterializationErrorV1> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/sequential-recommendation-signal-plan/v1\0");
    hash.update(dataset.identity().bytes());
    hash.update(dataset.manifest.content_hash().bytes());
    hash.update(dataset.object_graph_digest().bytes());
    hash.update(dataset.point_in_time_content.bytes());
    hash.update(dataset.point_in_time_audit.bytes());
    hash.update(policy.digest().bytes());
    hash.update(issuer_identity.digest.bytes());
    hash.update(evaluation_starts_at.unix_nanos().to_be_bytes());
    hash.update(evaluation_ends_at.unix_nanos().to_be_bytes());
    for limit in [
        limits.max_folds,
        limits.max_signals,
        limits.max_equity_points_per_outcome,
        limits.max_total_equity_points,
        limits.max_observation_visits,
    ] {
        hash.update(
            u64::try_from(limit)
                .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::LimitExceeded)?
                .to_be_bytes(),
        );
    }
    hash.update(
        u64::try_from(instructions.len())
            .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::LimitExceeded)?
            .to_be_bytes(),
    );
    for instruction in instructions {
        update_text(&mut hash, instruction.signal_id.as_str());
        hash.update(instruction.signal_at.unix_nanos().to_be_bytes());
        hash.update(instruction.available_at.unix_nanos().to_be_bytes());
        hash.update(
            instruction
                .source_selection_as_of
                .unix_nanos()
                .to_be_bytes(),
        );
        instruction.study_qualification.hash_into(&mut hash);
        hash.update(instruction.target_origin.unix_nanos().to_be_bytes());
        hash.update(instruction.target_at.unix_nanos().to_be_bytes());
        hash.update(instruction.subject_lineage_digest.bytes());
        hash.update(instruction.benchmark_lineage_digest.bytes());
        hash.update(instruction.instruction_evidence_digest.bytes());
        update_instruction(&mut hash, instruction.instruction);
    }
    Ok(Sha256Digest::new(hash.finalize().into()))
}

fn recommendation_signal_issuer_identity_digest(
    value: &RecommendationSignalIssuerIdentityV1,
) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/recommendation-signal-issuer-identity/v1\0");
    update_text(&mut hash, value.producer.as_str());
    update_text(&mut hash, value.semantic_revision.as_str());
    hash.update(value.bindings_digest.bytes());
    Sha256Digest::new(hash.finalize().into())
}

fn recommendation_oos_folds(
    starts_at: Timestamp,
) -> Result<Vec<RecommendationOosFoldV1>, RecommendationSignalPlanMaterializationErrorV1> {
    let mut folds = Vec::new();
    folds
        .try_reserve_exact(RECOMMENDATION_OOS_FOLD_COUNT_V1)
        .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::LimitExceeded)?;
    let mut fold_starts_at = starts_at;
    for fold_number in 1..=RECOMMENDATION_OOS_FOLD_COUNT_V1 {
        let ends_at = fold_starts_at
            .checked_add_nanos(RECOMMENDATION_OOS_FOLD_HORIZON_NANOS_V1)
            .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::InvalidEvaluationWindow)?;
        folds.push(
            RecommendationOosFoldV1::try_new(
                SourceIdentifier::try_from(format!("recommendation-oos-fold-{fold_number}-v1"))
                    .map_err(|_| {
                        RecommendationSignalPlanMaterializationErrorV1::InvalidEvaluationWindow
                    })?,
                fold_starts_at,
                ends_at,
            )
            .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::InvalidEvaluationWindow)?,
        );
        fold_starts_at = ends_at;
    }
    Ok(folds)
}

fn recommendation_fold_index(folds: &[RecommendationOosFoldV1], at: Timestamp) -> Option<usize> {
    folds
        .iter()
        .position(|fold| at >= fold.starts_at() && at < fold.ends_at())
}

fn validate_materialization_observation(
    observation: &BacktestObservation,
    policy: RecommendationBacktestPolicyV1,
) -> Result<(), RecommendationSignalPlanMaterializationErrorV1> {
    let reporting_currency = policy.reporting_currency();
    let (origin, _) = materialization_target(&observation, policy.execution_basis)?;
    if observation.execution_terms.quote_currency() != reporting_currency
        || observation.execution_terms.settlement_denomination()
            != Denomination::Currency(reporting_currency)
        || observation.event_at() > observation.available_at()
        || !policy.study_qualification.admits_clocks(
            origin,
            observation.available_at(),
            observation.source_selection_as_of,
            observation.decision_at(),
        )
        || observation.lineage_digest.bytes() == [0; 32]
    {
        return Err(RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel);
    }
    Ok(())
}

fn materialization_target(
    observation: &BacktestObservation,
    execution_basis: BacktestExecutionBasis,
) -> Result<(Timestamp, Timestamp), RecommendationSignalPlanMaterializationErrorV1> {
    use RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel;
    let (origin, target) = match observation.financial_target {
        Some(coordinates) => coordinates,
        None if execution_basis == BacktestExecutionBasis::ObservedQuoteDepth => {
            // An observed quote's financial origin is its genuine event instant. Daily close
            // studies require the separate sealed origin/target and cannot use this branch.
            let origin = observation.event_at();
            (
                origin,
                origin
                    .checked_add_nanos(RECOMMENDATION_TARGET_HORIZON_NANOS_V1)
                    .map_err(|_| IncompletePointInTimePanel)?,
            )
        }
        None => return Err(IncompletePointInTimePanel),
    };
    if origin < observation.event_at()
        || origin > observation.available_at()
        || target <= observation.decision_at()
        || origin
            .checked_add_nanos(RECOMMENDATION_TARGET_HORIZON_NANOS_V1)
            .map_err(|_| IncompletePointInTimePanel)?
            != target
    {
        return Err(IncompletePointInTimePanel);
    }
    Ok((origin, target))
}

fn retain_stable_execution_terms(
    retained: &mut Option<InstrumentExecutionTerms>,
    current: InstrumentExecutionTerms,
) -> Result<(), RecommendationSignalPlanMaterializationErrorV1> {
    match retained {
        Some(expected) if *expected != current => {
            Err(RecommendationSignalPlanMaterializationErrorV1::IncompletePointInTimePanel)
        }
        Some(_) => Ok(()),
        None => {
            *retained = Some(current);
            Ok(())
        }
    }
}

fn materialization_entry_evidence(
    observation: &BacktestObservation,
    execution_basis: BacktestExecutionBasis,
) -> bool {
    observation.universe == HistoricalUniverseStatus::Eligible
        && (observation.mid_price.is_some() || observation.market_reference.is_some())
        && (execution_basis == BacktestExecutionBasis::CompletedDailyBar
            || observation.executable_depth.get() > 0)
}

fn materialized_signal_evidence_digest(
    dataset_identity: Sha256Digest,
    policy_digest: Sha256Digest,
    preauthorized_signal_plan_digest: Sha256Digest,
    signal: &BoundRecommendationSignalV1,
) -> Result<Sha256Digest, RecommendationSignalPlanMaterializationErrorV1> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/materialized-recommendation-signal/v1\0");
    hash.update(dataset_identity.bytes());
    hash.update(policy_digest.bytes());
    hash.update(preauthorized_signal_plan_digest.bytes());
    update_text(&mut hash, signal.signal_id.as_str());
    hash.update(signal.signal_at.unix_nanos().to_be_bytes());
    hash.update(signal.available_at.unix_nanos().to_be_bytes());
    hash.update(signal.source_selection_as_of.unix_nanos().to_be_bytes());
    signal.study_qualification.hash_into(&mut hash);
    hash.update(signal.target_origin.unix_nanos().to_be_bytes());
    hash.update(signal.target_at.unix_nanos().to_be_bytes());
    hash.update(signal.subject_lineage_digest.bytes());
    hash.update(signal.benchmark_lineage_digest.bytes());
    hash.update(signal.instruction_evidence_digest.bytes());
    update_instruction(&mut hash, signal.instruction);
    Ok(Sha256Digest::new(hash.finalize().into()))
}

fn materialized_signal_plan_digest(
    value: &MaterializedRecommendationSignalPlanV1,
) -> Result<Sha256Digest, RecommendationSignalPlanMaterializationErrorV1> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/materialized-recommendation-signal-plan/v1\0");
    hash.update(value.dataset_identity.bytes());
    hash.update(value.dataset_manifest_content.bytes());
    hash.update(value.object_graph_digest.bytes());
    hash.update(value.point_in_time_content.bytes());
    hash.update(value.point_in_time_audit.bytes());
    hash.update(value.policy_digest.bytes());
    hash.update(value.issuer_identity.digest.bytes());
    hash.update(value.evaluation_starts_at.unix_nanos().to_be_bytes());
    hash.update(value.evaluation_ends_at.unix_nanos().to_be_bytes());
    hash.update(
        u64::try_from(value.paired_observation_count)
            .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::LimitExceeded)?
            .to_be_bytes(),
    );
    for limit in [
        value.limits.max_folds,
        value.limits.max_signals,
        value.limits.max_equity_points_per_outcome,
        value.limits.max_total_equity_points,
        value.limits.max_observation_visits,
    ] {
        hash.update(
            u64::try_from(limit)
                .map_err(|_| RecommendationSignalPlanMaterializationErrorV1::LimitExceeded)?
                .to_be_bytes(),
        );
    }
    hash.update(value.signal_plan.digest().bytes());
    if value.instruction_evidence.len() != value.signal_plan.signals.len() {
        return Err(RecommendationSignalPlanMaterializationErrorV1::MaterializationDrift);
    }
    for receipt in &value.instruction_evidence {
        match receipt {
            None => hash.update([0]),
            Some(receipt) => {
                validate_instruction_payload(Sha256Digest::new(receipt.digest), &receipt.payload)?;
                hash.update([1]);
                hash.update(receipt.digest);
                update_text(&mut hash, &receipt.payload);
            }
        }
    }
    Ok(Sha256Digest::new(hash.finalize().into()))
}

/// Typed refusal from strict signal-plan materialization.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RecommendationSignalPlanMaterializationErrorV1 {
    #[error("recommendation signal issuer became unavailable during sequential materialization")]
    IssuerUnavailable,
    #[error("recommendation materialization policy is not the strict one-lot conservative profile")]
    PolicyMismatch,
    #[error("recommendation materialization evaluation window is invalid")]
    InvalidEvaluationWindow,
    #[error("recommendation materialization dataset contains instruments or times outside scope")]
    DatasetScopeMismatch,
    #[error("recommendation materialization lacks a complete paired point-in-time panel")]
    IncompletePointInTimePanel,
    #[error("recommendation materialization instruction is invalid")]
    InvalidInstruction,
    #[error("recommendation materialization instruction evidence does not match pinned rows")]
    InstructionEvidenceMismatch,
    #[error("recommendation materialization resource limit exceeded")]
    LimitExceeded,
    #[error("recommendation materialization receipt drifted from its dataset, policy, or plan")]
    MaterializationDrift,
}

/// Exact evaluation, publication, availability, and expiry times.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationBacktestPublicationV1 {
    simulation_cutoff: Timestamp,
    evaluated_at: Timestamp,
    published_at: Timestamp,
    available_at: Timestamp,
    expires_at: Timestamp,
    digest: Sha256Digest,
}

impl RecommendationBacktestPublicationV1 {
    /// Requires an ordered, nonempty evidence publication window.
    pub fn try_new(
        simulation_cutoff: Timestamp,
        evaluated_at: Timestamp,
        published_at: Timestamp,
        available_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, RecommendationBacktestError> {
        if simulation_cutoff > evaluated_at
            || evaluated_at > published_at
            || published_at > available_at
            || available_at >= expires_at
        {
            return Err(RecommendationBacktestError::InvalidPublication);
        }
        let mut value = Self {
            simulation_cutoff,
            evaluated_at,
            published_at,
            available_at,
            expires_at,
            digest: Sha256Digest::new([0; 32]),
        };
        value.digest = publication_digest(value);
        Ok(value)
    }

    /// Returns the latest admitted PIT decision coordinate.
    #[must_use]
    pub const fn simulation_cutoff(self) -> Timestamp {
        self.simulation_cutoff
    }

    /// Returns when this backtest evidence was evaluated.
    #[must_use]
    pub const fn evaluated_at(self) -> Timestamp {
        self.evaluated_at
    }

    /// Returns its declared publication time.
    #[must_use]
    pub const fn published_at(self) -> Timestamp {
        self.published_at
    }

    /// Returns when the evidence became available to downstream decisions.
    #[must_use]
    pub const fn available_at(self) -> Timestamp {
        self.available_at
    }

    /// Returns the exclusive revalidation deadline.
    #[must_use]
    pub const fn expires_at(self) -> Timestamp {
        self.expires_at
    }

    /// Returns the exact timing-record identity.
    #[must_use]
    pub const fn digest(self) -> Sha256Digest {
        self.digest
    }
}

/// One exact retained mark used by the peak-to-trough drawdown calculation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationEquityPointV1 {
    marked_at: Timestamp,
    available_at: Timestamp,
    lineage_digest: Sha256Digest,
    equity: Decimal,
}

impl RecommendationEquityPointV1 {
    /// Returns the exact mark coordinate.
    #[must_use]
    pub const fn marked_at(self) -> Timestamp {
        self.marked_at
    }

    /// Returns exact marked equity in the policy reporting currency.
    #[must_use]
    pub const fn equity(self) -> Decimal {
        self.equity
    }

    /// Returns when the source observation became PIT-available.
    #[must_use]
    pub const fn available_at(self) -> Timestamp {
        self.available_at
    }

    /// Returns the exact source observation lineage identity.
    #[must_use]
    pub const fn lineage_digest(self) -> Sha256Digest {
        self.lineage_digest
    }
}

/// Fully filled, fully costed entry-to-365-day-exit result for one instrument.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendationRoundTripOutcomeV1 {
    execution_terms: InstrumentExecutionTerms,
    entry_fill: ResearchFill,
    exit_fill: ResearchFill,
    entry_cost: Decimal,
    exit_proceeds: Decimal,
    unpaid_entitlement_value: Decimal,
    cost_adjusted_total_return: Decimal,
    maximum_drawdown: Decimal,
    equity_path: Box<[RecommendationEquityPointV1]>,
    digest: Sha256Digest,
}

impl RecommendationRoundTripOutcomeV1 {
    /// Original canonical tick, lot, currency and multiplier used by both simulated fills.
    #[must_use]
    pub const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.execution_terms
    }

    /// Returns the exact instrument evaluated.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.execution_terms.instrument_id()
    }

    /// Returns the fully costed entry fill.
    #[must_use]
    pub const fn entry_fill(&self) -> &ResearchFill {
        &self.entry_fill
    }

    /// Returns the fully costed target-horizon exit fill.
    #[must_use]
    pub const fn exit_fill(&self) -> &ResearchFill {
        &self.exit_fill
    }

    /// Returns exact entry cash consumed, including fees.
    #[must_use]
    pub const fn entry_cost(&self) -> Decimal {
        self.entry_cost
    }

    /// Returns exact exit cash received, net of fees.
    #[must_use]
    pub const fn exit_proceeds(&self) -> Decimal {
        self.exit_proceeds
    }

    /// Returns distributions still owed at the terminal mark, separately from spendable cash.
    #[must_use]
    pub const fn unpaid_entitlement_value(&self) -> Decimal {
        self.unpaid_entitlement_value
    }

    /// Returns the exact fee/slippage-adjusted total-wealth return, including unpaid entitlements.
    #[must_use]
    pub const fn cost_adjusted_total_return(&self) -> Decimal {
        self.cost_adjusted_total_return
    }

    /// Returns exact peak-to-trough drawdown over the retained equity path.
    #[must_use]
    pub const fn maximum_drawdown(&self) -> Decimal {
        self.maximum_drawdown
    }

    /// Returns every retained mark used to compute drawdown.
    #[must_use]
    pub fn equity_path(&self) -> &[RecommendationEquityPointV1] {
        &self.equity_path
    }

    /// Returns the complete outcome content identity.
    #[must_use]
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }
}

/// Closed execution-evidence gap for an entry or target-horizon exit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationExecutionGapV1 {
    NoEligibleObservation,
    InsufficientLiquidity,
}

/// Exact reason the approved benchmark could not complete at the same coordinates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationBenchmarkGapV1 {
    EntryUnfilled(RecommendationExecutionGapV1),
    ExitUnfilled(RecommendationExecutionGapV1),
    Unavailable(RecommendationSignalUnavailableReasonV1),
}

/// One retained disposition for every predeclared signal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecommendationSignalDispositionV1 {
    Completed {
        subject: Box<RecommendationRoundTripOutcomeV1>,
        benchmark: Box<RecommendationRoundTripOutcomeV1>,
    },
    NoAction,
    Unavailable(RecommendationSignalUnavailableReasonV1),
    Censored(RecommendationSignalCensorReasonV1),
    EntryUnfilled {
        gap: RecommendationExecutionGapV1,
        partial_fill: Option<ResearchFill>,
    },
    ExitUnfilled {
        entry_fill: ResearchFill,
        gap: RecommendationExecutionGapV1,
        partial_fill: Option<ResearchFill>,
    },
    BenchmarkUnavailable {
        subject: Box<RecommendationRoundTripOutcomeV1>,
        gap: RecommendationBenchmarkGapV1,
    },
}

/// Exact signal scope plus its governed research-only outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendationSignalResultV1 {
    signal_id: SourceIdentifier,
    fold_index: usize,
    signal_at: Timestamp,
    signal_available_at: Timestamp,
    source_selection_as_of: Timestamp,
    study_qualification: BacktestStudyQualification,
    target_origin: Timestamp,
    target_at: Timestamp,
    signal_evidence_digest: Sha256Digest,
    disposition: RecommendationSignalDispositionV1,
    accompanying_benchmark:
        Option<Result<Box<RecommendationRoundTripOutcomeV1>, RecommendationBenchmarkGapV1>>,
    digest: Sha256Digest,
}

impl RecommendationSignalResultV1 {
    pub const fn basis(&self) -> HistoricalStudyBasis {
        self.study_qualification.basis()
    }
    pub const fn limitations(&self) -> &'static [HistoricalStudyLimitation] {
        self.study_qualification.limitations()
    }
    pub const fn snapshot_as_of(&self) -> Timestamp {
        self.study_qualification.snapshot_as_of()
    }
    pub const fn source_snapshot_digest(&self) -> Sha256Digest {
        self.study_qualification.source_snapshot_digest()
    }
    pub const fn source_selection_as_of(&self) -> Timestamp {
        self.source_selection_as_of
    }

    /// Returns the original financial reference time of the 365-day forecast.
    #[must_use]
    pub const fn target_origin(&self) -> Timestamp {
        self.target_origin
    }

    /// Returns the source signal identity.
    #[must_use]
    pub const fn signal_id(&self) -> &SourceIdentifier {
        &self.signal_id
    }

    /// Returns the independent OOS fold index.
    #[must_use]
    pub const fn fold_index(&self) -> usize {
        self.fold_index
    }

    /// Returns the exact 365-day target coordinate.
    #[must_use]
    pub const fn target_at(&self) -> Timestamp {
        self.target_at
    }

    /// Returns the exact signal coordinate.
    #[must_use]
    pub const fn signal_at(&self) -> Timestamp {
        self.signal_at
    }

    /// Returns when the signal's historical source information became PIT-available.
    #[must_use]
    pub const fn signal_available_at(&self) -> Timestamp {
        self.signal_available_at
    }

    /// Returns the exact source signal-evidence identity.
    #[must_use]
    pub const fn signal_evidence_digest(&self) -> Sha256Digest {
        self.signal_evidence_digest
    }

    /// Returns the retained signal disposition.
    #[must_use]
    pub const fn disposition(&self) -> &RecommendationSignalDispositionV1 {
        &self.disposition
    }

    /// Returns the accompanying comparison for a matured subject outcome. Absence means that
    /// the subject itself did not complete; an error retains the comparison's exact evidence gap.
    #[must_use]
    pub fn accompanying_benchmark(
        &self,
    ) -> Option<Result<&RecommendationRoundTripOutcomeV1, RecommendationBenchmarkGapV1>> {
        self.accompanying_benchmark
            .as_ref()
            .map(|value| match value {
                Ok(outcome) => Ok(outcome.as_ref()),
                Err(gap) => Err(*gap),
            })
    }

    /// Returns the complete result content identity.
    #[must_use]
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }
}

/// Availability of exact benchmark aggregate metrics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationBenchmarkAggregateV1 {
    Available {
        mean_cost_adjusted_total_return: Decimal,
        mean_excess_return: Decimal,
    },
    Unavailable,
}

/// Complete exact aggregate across completed observations and independent OOS folds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationAggregateV1 {
    observation_count: usize,
    trial_count: usize,
    cost_adjusted_total_return: Decimal,
    worst_maximum_drawdown: Decimal,
    positive_fold_count: usize,
    positive_fold_stability: Decimal,
    positive_fold_stability_ppm: u32,
    benchmark: RecommendationBenchmarkAggregateV1,
    accompanying_benchmark: RecommendationBenchmarkAggregateV1,
    digest: Sha256Digest,
}

impl RecommendationAggregateV1 {
    /// Returns the number of fully completed subject signal outcomes.
    #[must_use]
    pub const fn observation_count(self) -> usize {
        self.observation_count
    }

    /// Returns the number of independent OOS folds treated as trials.
    #[must_use]
    pub const fn trial_count(self) -> usize {
        self.trial_count
    }

    /// Returns the exact arithmetic mean of cost-adjusted completed signal returns.
    #[must_use]
    pub const fn cost_adjusted_total_return(self) -> Decimal {
        self.cost_adjusted_total_return
    }

    /// Returns the worst exact completed-signal drawdown.
    #[must_use]
    pub const fn worst_maximum_drawdown(self) -> Decimal {
        self.worst_maximum_drawdown
    }

    /// Returns the explicitly named positive-fold fraction.
    #[must_use]
    pub const fn positive_fold_stability(self) -> Decimal {
        self.positive_fold_stability
    }

    /// Returns the exact count of independent folds with positive mean signal return.
    #[must_use]
    pub const fn positive_fold_count(self) -> usize {
        self.positive_fold_count
    }

    /// Returns code-defined positive-fold stability in exact parts per million.
    #[must_use]
    pub const fn positive_fold_stability_ppm(self) -> u32 {
        self.positive_fold_stability_ppm
    }

    /// Returns exact benchmark aggregates or an explicit unavailable state.
    #[must_use]
    pub const fn benchmark(self) -> RecommendationBenchmarkAggregateV1 {
        self.benchmark
    }

    /// Returns the accompanying comparison over exactly the same completed subject population.
    #[must_use]
    pub const fn accompanying_benchmark(self) -> RecommendationBenchmarkAggregateV1 {
        self.accompanying_benchmark
    }

    /// Returns the complete aggregate identity.
    #[must_use]
    pub const fn digest(self) -> Sha256Digest {
        self.digest
    }
}

/// Why complete recommendation-backtest aggregates are not usable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationAggregateUnavailableV1 {
    TruncatedSignalPopulation,
    IncompleteDeclaredEntry { result_index: usize },
    MissingCompletedObservationInFold { fold_index: usize },
}

/// Complete exact aggregates or a typed refusal to treat partial evidence as complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationAggregateEvidenceV1 {
    Available(RecommendationAggregateV1),
    Unavailable(RecommendationAggregateUnavailableV1),
}

/// Complete immutable research study result. It cannot place, approve, or support product orders.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendationBacktestStudyV1 {
    dataset_identity: Sha256Digest,
    dataset_manifest_content: Sha256Digest,
    object_graph_digest: Sha256Digest,
    point_in_time_content: Sha256Digest,
    point_in_time_audit: Sha256Digest,
    policy: RecommendationBacktestPolicyV1,
    signal_plan_digest: Sha256Digest,
    preauthorized_signal_plan_digest: Sha256Digest,
    completeness: RecommendationSignalPlanCompletenessV1,
    publication: RecommendationBacktestPublicationV1,
    limits: RecommendationBacktestLimits,
    folds: Box<[RecommendationOosFoldV1]>,
    results: Box<[RecommendationSignalResultV1]>,
    aggregate: RecommendationAggregateEvidenceV1,
    digest: Sha256Digest,
}

impl RecommendationBacktestStudyV1 {
    pub const fn basis(&self) -> HistoricalStudyBasis {
        self.policy.study_qualification.basis()
    }
    pub const fn snapshot_as_of(&self) -> Timestamp {
        self.policy.study_qualification.snapshot_as_of()
    }
    pub const fn source_snapshot_digest(&self) -> Sha256Digest {
        self.policy.study_qualification.source_snapshot_digest()
    }
    pub const fn limitations(&self) -> &'static [HistoricalStudyLimitation] {
        self.policy.study_qualification.limitations()
    }

    /// Binds actual completion/publication clocks after evaluation without changing its cutoff.
    pub fn with_publication(
        mut self,
        publication: RecommendationBacktestPublicationV1,
    ) -> Result<Self, RecommendationBacktestError> {
        if publication.simulation_cutoff() != self.publication.simulation_cutoff()
            || publication.evaluated_at() < self.publication.evaluated_at()
        {
            return Err(RecommendationBacktestError::InvalidPublication);
        }
        self.publication = publication;
        self.digest = evidence_digest(&self)?;
        Ok(self)
    }

    /// Returns the exact PIT dataset identity.
    #[must_use]
    pub const fn dataset_identity(&self) -> Sha256Digest {
        self.dataset_identity
    }

    /// Returns the exact immutable dataset-manifest content identity.
    #[must_use]
    pub const fn dataset_manifest_content(&self) -> Sha256Digest {
        self.dataset_manifest_content
    }

    /// Returns the exact pinned object-graph identity.
    #[must_use]
    pub const fn object_graph_digest(&self) -> Sha256Digest {
        self.object_graph_digest
    }

    /// Returns the exact PIT content identity.
    #[must_use]
    pub const fn point_in_time_content(&self) -> Sha256Digest {
        self.point_in_time_content
    }

    /// Returns the exact PIT audit identity.
    #[must_use]
    pub const fn point_in_time_audit(&self) -> Sha256Digest {
        self.point_in_time_audit
    }

    /// Returns the exact policy identity.
    #[must_use]
    pub const fn policy_digest(&self) -> Sha256Digest {
        self.policy.digest
    }

    /// Returns the complete strict current policy.
    #[must_use]
    pub const fn policy(&self) -> RecommendationBacktestPolicyV1 {
        self.policy
    }

    /// Returns the exact canonical signal-plan identity.
    #[must_use]
    pub const fn signal_plan_digest(&self) -> Sha256Digest {
        self.signal_plan_digest
    }

    /// Returns the opaque pre-authorized signal-plan evidence identity.
    #[must_use]
    pub const fn preauthorized_signal_plan_digest(&self) -> Sha256Digest {
        self.preauthorized_signal_plan_digest
    }

    /// Returns explicit complete/truncated population state.
    #[must_use]
    pub const fn completeness(&self) -> RecommendationSignalPlanCompletenessV1 {
        self.completeness
    }

    /// Returns exact evaluation/publication timing.
    #[must_use]
    pub const fn publication(&self) -> RecommendationBacktestPublicationV1 {
        self.publication
    }

    /// Returns the exact resource ceilings applied to this evaluation.
    #[must_use]
    pub const fn limits(&self) -> RecommendationBacktestLimits {
        self.limits
    }

    /// Returns the exact independent OOS fold definitions.
    #[must_use]
    pub fn folds(&self) -> &[RecommendationOosFoldV1] {
        &self.folds
    }

    /// Returns one result for every retained predeclared signal.
    #[must_use]
    pub fn results(&self) -> &[RecommendationSignalResultV1] {
        &self.results
    }

    /// Returns exact aggregates or a typed incomplete-evidence refusal.
    #[must_use]
    pub const fn aggregate(&self) -> RecommendationAggregateEvidenceV1 {
        self.aggregate
    }

    /// Returns the complete current evidence identity.
    #[must_use]
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }
}

/// Pure strict recommendation-outcome V1 evaluator.
#[derive(Clone, Copy, Debug, Default)]
pub struct RecommendationBacktestKernelV1;

impl RecommendationBacktestKernelV1 {
    /// Evaluates one dataset- and policy-bound materialization as research study evidence.
    pub fn run_materialized_study(
        dataset: &BacktestDataset,
        policy: RecommendationBacktestPolicyV1,
        corporate_actions: &CorporateActionPlan,
        materialized: &MaterializedRecommendationSignalPlanV1,
        publication: RecommendationBacktestPublicationV1,
        limits: RecommendationBacktestLimits,
        cancellation: &CancellationToken,
    ) -> Result<RecommendationBacktestStudyV1, RecommendationMaterializedBacktestErrorV1> {
        materialized.validate_against(dataset, policy, limits)?;
        if publication.simulation_cutoff() != materialized.evaluation_ends_at() {
            return Err(
                RecommendationSignalPlanMaterializationErrorV1::MaterializationDrift.into(),
            );
        }
        Self::run_study(
            dataset,
            policy,
            corporate_actions,
            materialized.signal_plan(),
            publication,
            limits,
            cancellation,
        )
        .map_err(Into::into)
    }

    /// Evaluates every predeclared signal with the existing deterministic fill simulator.
    pub fn run_study(
        dataset: &BacktestDataset,
        policy: RecommendationBacktestPolicyV1,
        corporate_actions: &CorporateActionPlan,
        signal_plan: &RecommendationSignalPlanV1,
        publication: RecommendationBacktestPublicationV1,
        limits: RecommendationBacktestLimits,
        cancellation: &CancellationToken,
    ) -> Result<RecommendationBacktestStudyV1, RecommendationBacktestError> {
        if dataset.study_qualification != Some(policy.study_qualification)
            || signal_plan.study_qualification != policy.study_qualification
            || policy.study_qualification.snapshot_as_of() > publication.evaluated_at()
            || dataset.execution_basis() != policy.execution_basis
            || dataset.daily_history.as_ref().is_some_and(|history| {
                history.digest != policy.raw_price_evidence_digest
                    || history.available_at > publication.evaluated_at()
                    || history.nominal_sources.iter().any(|source| {
                        corporate_actions.source_admission().is_none_or(|coverage| {
                            coverage.knowledge_cutoff() != source.knowledge_cutoff
                                || !coverage.instruments().contains(&source.instrument)
                                || !coverage
                                    .history_input_manifests()
                                    .contains(&source.manifest)
                                || coverage
                                    .application_starts_at(source.instrument)
                                    .is_none_or(|starts_at| {
                                        starts_at > policy.corporate_action_coverage_starts_at
                                    })
                        })
                    })
            })
            || corporate_actions.policy().adjustment() != CorporateActionAdjustment::TotalReturn
            || corporate_actions.content_hash() != policy.corporate_action_content_digest
            || corporate_actions.audit_hash() != policy.corporate_action_audit_digest
            || !corporate_actions.conflicts().is_empty()
            || !corporate_actions.exclusions().is_empty()
            || corporate_actions.knowledge_cutoff() > publication.evaluated_at()
            || corporate_actions.valuation_cutoff() != publication.simulation_cutoff()
            || signal_plan
                .signals()
                .iter()
                .any(|signal| signal.signal_at() < policy.corporate_action_coverage_starts_at)
        {
            return Err(RecommendationBacktestError::InvalidDataset);
        }
        if signal_plan.folds.len() > limits.max_folds
            || signal_plan.signals.len() > limits.max_signals
        {
            return Err(RecommendationBacktestError::LimitExceeded);
        }
        let mut total_equity_points = 0_usize;
        let mut observation_visits = 0_usize;
        let mut results = Vec::new();
        results
            .try_reserve_exact(signal_plan.signals.len())
            .map_err(|_| RecommendationBacktestError::LimitExceeded)?;
        for signal in &signal_plan.signals {
            if cancellation.is_cancelled() {
                return Err(RecommendationBacktestError::Cancelled);
            }
            if signal.signal_at > publication.simulation_cutoff
                || signal.available_at > policy.study_qualification.snapshot_as_of()
            {
                return Err(RecommendationBacktestError::InvalidSignalPlan);
            }
            let target_at = signal.target_at();
            let outcome_window_end = target_at
                .checked_add_nanos(policy.maximum_exit_lag_nanos)
                .map_err(|_| RecommendationBacktestError::InvalidSignalPlan)?;
            let fold_ends_at = signal_plan
                .folds
                .get(signal.fold_index)
                .ok_or(RecommendationBacktestError::InvalidSignalPlan)?
                .ends_at;
            let disposition = match signal.instruction {
                RecommendationSignalInstructionV1::NoAction => {
                    RecommendationSignalDispositionV1::NoAction
                }
                RecommendationSignalInstructionV1::Unavailable(reason) => {
                    RecommendationSignalDispositionV1::Unavailable(reason)
                }
                RecommendationSignalInstructionV1::Censored(reason) => {
                    RecommendationSignalDispositionV1::Censored(reason)
                }
                RecommendationSignalInstructionV1::Entry
                    if target_at > publication.simulation_cutoff =>
                {
                    RecommendationSignalDispositionV1::Censored(
                        RecommendationSignalCensorReasonV1::TargetAfterSimulationCutoff,
                    )
                }
                // The issuer sees only its contemporaneous information set, never the study's
                // eventual fold boundary. Preserve its Entry and explicitly censor its outcome
                // when the complete exit window would cross into a different independent fold.
                RecommendationSignalInstructionV1::Entry if outcome_window_end >= fold_ends_at => {
                    RecommendationSignalDispositionV1::Censored(
                        RecommendationSignalCensorReasonV1::OutsideAuthorizedDataset,
                    )
                }
                RecommendationSignalInstructionV1::Entry => {
                    match simulate_round_trip(
                        dataset,
                        policy.into(),
                        corporate_actions,
                        cancellation,
                        policy.subject_instrument_id,
                        policy.subject_quantity,
                        RoundTripExecutionOrigin::recommendation(policy, signal, b"subject"),
                        target_at,
                        publication.simulation_cutoff,
                        limits,
                        &mut total_equity_points,
                        &mut observation_visits,
                    )? {
                        RoundTripSimulation::EntryUnfilled { gap, partial_fill } => {
                            RecommendationSignalDispositionV1::EntryUnfilled { gap, partial_fill }
                        }
                        RoundTripSimulation::ExitUnfilled {
                            entry_fill,
                            gap,
                            partial_fill,
                        } => RecommendationSignalDispositionV1::ExitUnfilled {
                            entry_fill,
                            gap,
                            partial_fill,
                        },
                        RoundTripSimulation::Unavailable(reason) => {
                            RecommendationSignalDispositionV1::Unavailable(reason)
                        }
                        RoundTripSimulation::Completed(subject) => {
                            match simulate_round_trip(
                                dataset,
                                policy.into(),
                                corporate_actions,
                                cancellation,
                                policy.benchmark.instrument_id,
                                policy.benchmark_quantity,
                                RoundTripExecutionOrigin::recommendation(
                                    policy,
                                    signal,
                                    b"benchmark",
                                ),
                                target_at,
                                publication.simulation_cutoff,
                                limits,
                                &mut total_equity_points,
                                &mut observation_visits,
                            )? {
                                RoundTripSimulation::Completed(benchmark) => {
                                    if matching_execution_dates(&subject, &benchmark) {
                                        RecommendationSignalDispositionV1::Completed {
                                            subject,
                                            benchmark,
                                        }
                                    } else {
                                        RecommendationSignalDispositionV1::BenchmarkUnavailable {
                                            subject,
                                            gap: RecommendationBenchmarkGapV1::Unavailable(RecommendationSignalUnavailableReasonV1::InsufficientPointInTimeEvidence),
                                        }
                                    }
                                }
                                RoundTripSimulation::EntryUnfilled { gap, .. } => {
                                    RecommendationSignalDispositionV1::BenchmarkUnavailable {
                                        subject,
                                        gap: RecommendationBenchmarkGapV1::EntryUnfilled(gap),
                                    }
                                }
                                RoundTripSimulation::ExitUnfilled { gap, .. } => {
                                    RecommendationSignalDispositionV1::BenchmarkUnavailable {
                                        subject,
                                        gap: RecommendationBenchmarkGapV1::ExitUnfilled(gap),
                                    }
                                }
                                RoundTripSimulation::Unavailable(reason) => {
                                    RecommendationSignalDispositionV1::BenchmarkUnavailable {
                                        subject,
                                        gap: RecommendationBenchmarkGapV1::Unavailable(reason),
                                    }
                                }
                            }
                        }
                    }
                }
            };
            let accompanying_benchmark = if matches!(
                disposition,
                RecommendationSignalDispositionV1::Completed { .. }
                    | RecommendationSignalDispositionV1::BenchmarkUnavailable { .. }
            ) {
                Some(
                    match simulate_round_trip(
                        dataset,
                        policy.into(),
                        corporate_actions,
                        cancellation,
                        policy.accompanying_benchmark.instrument_id,
                        policy.benchmark_quantity,
                        RoundTripExecutionOrigin::recommendation(
                            policy,
                            signal,
                            b"accompanying-benchmark",
                        ),
                        target_at,
                        publication.simulation_cutoff,
                        limits,
                        &mut total_equity_points,
                        &mut observation_visits,
                    )? {
                        RoundTripSimulation::Completed(outcome) => {
                            let subject = match &disposition {
                                RecommendationSignalDispositionV1::Completed {
                                    subject, ..
                                }
                                | RecommendationSignalDispositionV1::BenchmarkUnavailable {
                                    subject,
                                    ..
                                } => subject,
                                _ => return Err(RecommendationBacktestError::InvalidDataset),
                            };
                            if matching_execution_dates(subject, &outcome) {
                                Ok(outcome)
                            } else {
                                Err(RecommendationBenchmarkGapV1::Unavailable(RecommendationSignalUnavailableReasonV1::InsufficientPointInTimeEvidence))
                            }
                        }
                        RoundTripSimulation::EntryUnfilled { gap, .. } => {
                            Err(RecommendationBenchmarkGapV1::EntryUnfilled(gap))
                        }
                        RoundTripSimulation::ExitUnfilled { gap, .. } => {
                            Err(RecommendationBenchmarkGapV1::ExitUnfilled(gap))
                        }
                        RoundTripSimulation::Unavailable(reason) => {
                            Err(RecommendationBenchmarkGapV1::Unavailable(reason))
                        }
                    },
                )
            } else {
                None
            };
            let mut result = RecommendationSignalResultV1 {
                signal_id: signal.signal_id.clone(),
                fold_index: signal.fold_index,
                signal_at: signal.signal_at,
                signal_available_at: signal.available_at,
                source_selection_as_of: signal.source_selection_as_of,
                study_qualification: signal.study_qualification,
                target_origin: signal.target_origin,
                target_at,
                signal_evidence_digest: signal.evidence_digest,
                disposition,
                accompanying_benchmark,
                digest: Sha256Digest::new([0; 32]),
            };
            result.digest = signal_result_digest(&result)?;
            results.push(result);
        }
        let aggregate = aggregate_evidence(signal_plan, &results)?;
        let mut evidence = RecommendationBacktestStudyV1 {
            dataset_identity: dataset.identity(),
            dataset_manifest_content: dataset.manifest.content_hash(),
            object_graph_digest: dataset.object_graph_digest(),
            point_in_time_content: dataset.point_in_time_content,
            point_in_time_audit: dataset.point_in_time_audit,
            policy,
            signal_plan_digest: signal_plan.digest,
            preauthorized_signal_plan_digest: signal_plan.preauthorized_signal_plan_digest,
            completeness: signal_plan.completeness,
            publication,
            limits,
            folds: signal_plan.folds.clone(),
            results: results.into_boxed_slice(),
            aggregate,
            digest: Sha256Digest::new([0; 32]),
        };
        evidence.digest = evidence_digest(&evidence)?;
        Ok(evidence)
    }
}

/// Strict materialization or research-outcome evaluation failure.
#[derive(Debug, Error)]
pub enum RecommendationMaterializedBacktestErrorV1 {
    #[error(transparent)]
    SignalPlan(#[from] RecommendationSignalPlanMaterializationErrorV1),
    #[error(transparent)]
    Backtest(#[from] RecommendationBacktestError),
}

#[derive(Clone, Copy)]
struct RoundTripExecutionPolicy {
    digest: Sha256Digest,
    reason_code: &'static str,
    reporting_currency: Currency,
    maximum_entry_lag_nanos: i64,
    maximum_exit_lag_nanos: i64,
    execution_assumptions: ResearchExecutionAssumptions,
    seed: u64,
}

impl From<RecommendationBacktestPolicyV1> for RoundTripExecutionPolicy {
    fn from(policy: RecommendationBacktestPolicyV1) -> Self {
        Self {
            digest: policy.digest,
            reason_code: "recommendation-backtest-v1",
            reporting_currency: policy.reporting_currency,
            maximum_entry_lag_nanos: policy.maximum_entry_lag_nanos,
            maximum_exit_lag_nanos: policy.maximum_exit_lag_nanos,
            execution_assumptions: policy.execution_assumptions,
            seed: policy.seed,
        }
    }
}

#[derive(Clone, Copy)]
struct RoundTripExecutionOrigin {
    decision_at: Timestamp,
    entry_identity: [u8; 32],
    exit_identity: [u8; 32],
}

impl RoundTripExecutionOrigin {
    fn recommendation(
        policy: RecommendationBacktestPolicyV1,
        signal: &RecommendationSignalV1,
        role: &[u8],
    ) -> Self {
        Self {
            decision_at: signal.signal_at,
            entry_identity: execution_identity_digest(policy.digest, signal, role, b"entry"),
            exit_identity: execution_identity_digest(policy.digest, signal, role, b"exit"),
        }
    }
}

enum RoundTripSimulation {
    Completed(Box<RecommendationRoundTripOutcomeV1>),
    EntryUnfilled {
        gap: RecommendationExecutionGapV1,
        partial_fill: Option<ResearchFill>,
    },
    ExitUnfilled {
        entry_fill: ResearchFill,
        gap: RecommendationExecutionGapV1,
        partial_fill: Option<ResearchFill>,
    },
    Unavailable(RecommendationSignalUnavailableReasonV1),
}

fn matching_execution_dates(
    left: &RecommendationRoundTripOutcomeV1,
    right: &RecommendationRoundTripOutcomeV1,
) -> bool {
    left.entry_fill.executed_at() == right.entry_fill.executed_at()
        && left.exit_fill.executed_at() == right.exit_fill.executed_at()
}

/// Realized execution observations are projected independently of the coordinate-local alpha
/// facade. A daily bar's close and volume cannot enter the earlier signal information set.
#[derive(Clone, Copy)]
struct ExecutionObservation {
    execution_terms: InstrumentExecutionTerms,
    decision_at: Timestamp,
    available_at: Timestamp,
    reference_price: Option<Money>,
    spread_basis_points: BasisPoints,
    liquidity: ExecutionLiquidity,
    bar_starts_at: Option<Timestamp>,
    universe: HistoricalUniverseStatus,
    lineage_digest: Sha256Digest,
}

#[derive(Clone, Copy)]
enum ExecutionLiquidity {
    QuoteDepth(QuantityLots),
    DailyTradedVolume(Decimal),
}

impl ExecutionObservation {
    const fn instrument_id(self) -> InstrumentId {
        self.execution_terms.instrument_id()
    }

    const fn decision_at(self) -> Timestamp {
        self.decision_at
    }

    const fn available_at(self) -> Timestamp {
        self.available_at
    }

    fn has_liquidity(self) -> bool {
        match self.liquidity {
            ExecutionLiquidity::QuoteDepth(depth) => depth.get() > 0,
            ExecutionLiquidity::DailyTradedVolume(volume) => volume > Decimal::ZERO,
        }
    }
}

fn execution_observations_from(
    dataset: &BacktestDataset,
    starts_at: Timestamp,
) -> impl Iterator<Item = Result<ExecutionObservation, RecommendationBacktestError>> + '_ {
    let quotes = dataset
        .observations
        .from(starts_at)
        .take(if dataset.daily_history.is_none() {
            usize::MAX
        } else {
            0
        })
        .map(|observation| {
            let observation =
                observation.map_err(|_| RecommendationBacktestError::InvalidDataset)?;
            Ok(ExecutionObservation {
                execution_terms: observation.execution_terms,
                decision_at: observation.decision_at(),
                available_at: observation.available_at(),
                reference_price: observation
                    .mid_price
                    .and_then(|price| {
                        price
                            .checked_to_decimal(observation.execution_terms.price_tick())
                            .ok()
                    })
                    .map(|amount| Money::new(amount, observation.execution_terms.quote_currency())),
                spread_basis_points: observation.spread_basis_points,
                liquidity: ExecutionLiquidity::QuoteDepth(observation.executable_depth),
                bar_starts_at: None,
                universe: observation.universe,
                lineage_digest: observation.lineage_digest,
            })
        });
    let bars = dataset
        .daily_history
        .as_ref()
        .into_iter()
        .flat_map(move |history| history.bars.from(starts_at))
        .map(|bar| {
            let bar = bar.map_err(|_| RecommendationBacktestError::InvalidDataset)?;
            Ok(ExecutionObservation {
                execution_terms: bar.execution_terms,
                decision_at: bar.ends_at,
                available_at: bar.available_at,
                reference_price: Some(bar.close),
                spread_basis_points: BasisPoints::new(DAILY_BAR_ASSUMED_FULL_SPREAD_BASIS_POINTS),
                liquidity: ExecutionLiquidity::DailyTradedVolume(bar.traded_volume),
                bar_starts_at: Some(bar.starts_at),
                universe: HistoricalUniverseStatus::Eligible,
                lineage_digest: bar.lineage_digest,
            })
        });
    quotes.chain(bars)
}

#[allow(
    clippy::too_many_arguments,
    reason = "every outcome coordinate and resource authority is explicit"
)]
fn simulate_round_trip(
    dataset: &BacktestDataset,
    policy: RoundTripExecutionPolicy,
    corporate_actions: &CorporateActionPlan,
    cancellation: &CancellationToken,
    instrument_id: InstrumentId,
    quantity: QuantityLots,
    origin: RoundTripExecutionOrigin,
    target_at: Timestamp,
    simulation_cutoff: Timestamp,
    limits: RecommendationBacktestLimits,
    total_equity_points: &mut usize,
    observation_visits: &mut usize,
) -> Result<RoundTripSimulation, RecommendationBacktestError> {
    let entry_start = origin
        .decision_at
        .checked_add_nanos(policy.execution_assumptions.latency_nanos())
        .map_err(|_| RecommendationBacktestError::Arithmetic)?;
    let entry_end = origin
        .decision_at
        .checked_add_nanos(policy.maximum_entry_lag_nanos)
        .map_err(|_| RecommendationBacktestError::Arithmetic)?
        .min(
            target_at
                .checked_sub_nanos(1)
                .map_err(|_| RecommendationBacktestError::Arithmetic)?,
        );
    let entry_observation = match first_eligible_observation(
        dataset,
        instrument_id,
        entry_start,
        entry_end,
        simulation_cutoff,
        policy.reporting_currency,
        None,
        limits,
        observation_visits,
        cancellation,
    ) {
        Ok(Some(observation)) => observation,
        Ok(None) => {
            return Ok(RoundTripSimulation::EntryUnfilled {
                gap: RecommendationExecutionGapV1::NoEligibleObservation,
                partial_fill: None,
            });
        }
        Err(RecommendationBacktestError::ReportingCurrencyMismatch) => {
            return Ok(RoundTripSimulation::Unavailable(
                RecommendationSignalUnavailableReasonV1::ReportingCurrencyMismatch,
            ));
        }
        Err(RecommendationBacktestError::ExecutionTermsChanged) => {
            return Ok(RoundTripSimulation::Unavailable(
                RecommendationSignalUnavailableReasonV1::ExecutionTermsChanged,
            ));
        }
        Err(error) => return Err(error),
    };
    let entry_fill = simulate_leg(
        policy,
        entry_observation,
        quantity,
        OrderSide::Buy,
        origin.decision_at,
        entry_end,
        origin.entry_identity,
    )?;
    let Some(entry_fill) = entry_fill else {
        return Ok(RoundTripSimulation::EntryUnfilled {
            gap: RecommendationExecutionGapV1::InsufficientLiquidity,
            partial_fill: None,
        });
    };
    if entry_fill.quantity() != quantity {
        return Ok(RoundTripSimulation::EntryUnfilled {
            gap: RecommendationExecutionGapV1::InsufficientLiquidity,
            partial_fill: Some(entry_fill),
        });
    }

    let exit_start = target_at
        .checked_add_nanos(policy.execution_assumptions.latency_nanos())
        .map_err(|_| RecommendationBacktestError::Arithmetic)?;
    let exit_end = target_at
        .checked_add_nanos(policy.maximum_exit_lag_nanos)
        .map_err(|_| RecommendationBacktestError::Arithmetic)?;
    let exit_observation = match first_eligible_observation(
        dataset,
        instrument_id,
        exit_start,
        exit_end,
        simulation_cutoff,
        policy.reporting_currency,
        Some(entry_observation.execution_terms),
        limits,
        observation_visits,
        cancellation,
    ) {
        Ok(Some(observation)) => observation,
        Ok(None) => {
            return Ok(RoundTripSimulation::ExitUnfilled {
                entry_fill,
                gap: RecommendationExecutionGapV1::NoEligibleObservation,
                partial_fill: None,
            });
        }
        Err(RecommendationBacktestError::ReportingCurrencyMismatch) => {
            return Ok(RoundTripSimulation::Unavailable(
                RecommendationSignalUnavailableReasonV1::ReportingCurrencyMismatch,
            ));
        }
        Err(RecommendationBacktestError::ExecutionTermsChanged) => {
            return Ok(RoundTripSimulation::Unavailable(
                RecommendationSignalUnavailableReasonV1::ExecutionTermsChanged,
            ));
        }
        Err(error) => return Err(error),
    };
    if exit_observation.decision_at() > corporate_actions.valuation_cutoff()
        || exit_observation.decision_at() > corporate_actions.knowledge_cutoff()
    {
        return Ok(RoundTripSimulation::Unavailable(
            RecommendationSignalUnavailableReasonV1::InsufficientPointInTimeEvidence,
        ));
    }
    count_action_visits(corporate_actions, limits, observation_visits)?;
    let accounting = RecommendationAccounting::at(
        &entry_fill,
        entry_observation.execution_terms,
        None,
        corporate_actions,
        exit_observation.decision_at(),
    )
    .map_err(|_| RecommendationBacktestError::InvalidDataset)?;
    let Ok(quantity) = accounting.quantity().and_then(|quantity| {
        QuantityLots::try_from_decimal(quantity, exit_observation.execution_terms.lot_size())
            .map_err(Into::into)
    }) else {
        return Ok(RoundTripSimulation::Unavailable(
            RecommendationSignalUnavailableReasonV1::UnsupportedInstrument,
        ));
    };
    if quantity.get() <= 0 {
        return Ok(RoundTripSimulation::Unavailable(
            RecommendationSignalUnavailableReasonV1::UnsupportedInstrument,
        ));
    }
    let exit_fill = simulate_leg(
        policy,
        exit_observation,
        quantity,
        OrderSide::Sell,
        target_at,
        exit_end,
        origin.exit_identity,
    )?;
    let Some(exit_fill) = exit_fill else {
        return Ok(RoundTripSimulation::ExitUnfilled {
            entry_fill,
            gap: RecommendationExecutionGapV1::InsufficientLiquidity,
            partial_fill: None,
        });
    };
    if exit_fill.quantity() != quantity {
        return Ok(RoundTripSimulation::ExitUnfilled {
            entry_fill,
            gap: RecommendationExecutionGapV1::InsufficientLiquidity,
            partial_fill: Some(exit_fill),
        });
    }
    let outcome = match build_round_trip_outcome(
        dataset,
        corporate_actions,
        cancellation,
        entry_observation,
        exit_observation,
        entry_fill,
        exit_fill,
        policy,
        limits,
        total_equity_points,
        observation_visits,
    ) {
        Ok(outcome) => outcome,
        Err(RecommendationBacktestError::ReportingCurrencyMismatch) => {
            return Ok(RoundTripSimulation::Unavailable(
                RecommendationSignalUnavailableReasonV1::ReportingCurrencyMismatch,
            ));
        }
        Err(RecommendationBacktestError::ExecutionTermsChanged) => {
            return Ok(RoundTripSimulation::Unavailable(
                RecommendationSignalUnavailableReasonV1::ExecutionTermsChanged,
            ));
        }
        Err(error) => return Err(error),
    };
    Ok(RoundTripSimulation::Completed(Box::new(outcome)))
}

fn first_eligible_observation(
    dataset: &BacktestDataset,
    instrument_id: InstrumentId,
    starts_at: Timestamp,
    ends_at: Timestamp,
    simulation_cutoff: Timestamp,
    reporting_currency: Currency,
    expected_terms: Option<InstrumentExecutionTerms>,
    limits: RecommendationBacktestLimits,
    observation_visits: &mut usize,
    cancellation: &CancellationToken,
) -> Result<Option<ExecutionObservation>, RecommendationBacktestError> {
    if starts_at > ends_at || starts_at > simulation_cutoff {
        return Ok(None);
    }
    for observation in execution_observations_from(dataset, starts_at) {
        let observation = observation?;
        if cancellation.is_cancelled() {
            return Err(RecommendationBacktestError::Cancelled);
        }
        if observation.decision_at > ends_at || observation.decision_at > simulation_cutoff {
            break;
        }
        count_observation_visit(observation_visits, limits)?;
        if observation.instrument_id() != instrument_id {
            continue;
        }
        if observation.universe != HistoricalUniverseStatus::Eligible
            || observation.reference_price.is_none()
            || !observation.has_liquidity()
            || observation
                .bar_starts_at
                .is_some_and(|bar_start| bar_start < starts_at)
        {
            continue;
        }
        if observation.execution_terms.quote_currency() != reporting_currency {
            return Err(RecommendationBacktestError::ReportingCurrencyMismatch);
        }
        if expected_terms.is_some_and(|terms| terms != observation.execution_terms) {
            return Err(RecommendationBacktestError::ExecutionTermsChanged);
        }
        return Ok(Some(observation));
    }
    Ok(None)
}

#[allow(
    clippy::too_many_arguments,
    reason = "one simulated leg binds every exact order coordinate"
)]
fn simulate_leg(
    policy: RoundTripExecutionPolicy,
    observation: ExecutionObservation,
    quantity: QuantityLots,
    side: OrderSide,
    leg_signal_at: Timestamp,
    last_permissible_at: Timestamp,
    identity: [u8; 32],
) -> Result<Option<ResearchFill>, RecommendationBacktestError> {
    if last_permissible_at <= leg_signal_at || observation.decision_at > last_permissible_at {
        return Ok(None);
    }
    // The lookup window is inclusive; OrderIntent expiry is exclusive. In particular the last
    // permissible entry is target−1ns, so its intent expires at the exact financial target.
    let expires_at = last_permissible_at
        .checked_add_nanos(1)
        .map_err(|_| RecommendationBacktestError::Arithmetic)?;
    let policy_identity = policy.digest.bytes();
    let maximum_slippage = observation
        .spread_basis_points
        .get()
        .checked_add(1)
        .map(|spread| spread / 2)
        .and_then(|half_spread| {
            half_spread.checked_add(policy.execution_assumptions.slippage_basis_points().get())
        })
        .and_then(|value| {
            value.checked_add(
                policy
                    .execution_assumptions
                    .maximum_random_slippage_basis_points()
                    .get(),
            )
        })
        .filter(|value| *value <= 10_000)
        .map(BasisPoints::new)
        .ok_or(RecommendationBacktestError::InvalidPolicy)?;
    let intent = OrderIntent::try_new(OrderIntentInput {
        order_id: OrderId::try_from(Uuid::new_v5(&Uuid::NAMESPACE_OID, &identity))
            .map_err(|_| RecommendationBacktestError::InvalidPolicy)?,
        client_order_id: ClientOrderId::try_from(format!(
            "rbv1:{}",
            encode_hex_prefix(identity, 24)
        ))
        .map_err(|_| RecommendationBacktestError::InvalidPolicy)?,
        strategy_id: StrategyId::try_from(Uuid::new_v5(&Uuid::NAMESPACE_OID, &policy_identity))
            .map_err(|_| RecommendationBacktestError::InvalidPolicy)?,
        model_id: None,
        account_id: AccountId::try_from(Uuid::new_v5(&Uuid::NAMESPACE_URL, &policy_identity))
            .map_err(|_| RecommendationBacktestError::InvalidPolicy)?,
        execution_terms: observation.execution_terms,
        side,
        order_type: OrderType::Market,
        quantity,
        limit_price: None,
        stop_price: None,
        time_in_force: TimeInForce::ImmediateOrCancel,
        signal_at: leg_signal_at,
        expires_at,
        reason_codes: vec![
            OrderReasonCode::try_from(policy.reason_code)
                .map_err(|_| RecommendationBacktestError::InvalidPolicy)?,
        ],
        maximum_slippage,
        required_quality: DataQuality::DirectVerified,
    })
    .map_err(|_| RecommendationBacktestError::InvalidPolicy)?;
    let mut simulator = ResearchFillSimulator::new(
        policy.execution_assumptions,
        deterministic_seed(policy.seed, identity),
    );
    let capacity = match observation.liquidity {
        ExecutionLiquidity::QuoteDepth(depth) => simulator.observation_capacity(depth),
        ExecutionLiquidity::DailyTradedVolume(volume) => {
            simulator.daily_bar_capacity(volume, observation.execution_terms.lot_size())
        }
    }
    .map_err(|_| RecommendationBacktestError::Arithmetic)?;
    simulator
        .simulate_money(
            &intent,
            quantity,
            observation.decision_at,
            observation
                .reference_price
                .ok_or(RecommendationBacktestError::InvalidDataset)?,
            observation.spread_basis_points,
            capacity,
        )
        .map_err(|_| RecommendationBacktestError::Arithmetic)
}

fn build_round_trip_outcome(
    dataset: &BacktestDataset,
    corporate_actions: &CorporateActionPlan,
    cancellation: &CancellationToken,
    entry_observation: ExecutionObservation,
    exit_observation: ExecutionObservation,
    entry_fill: ResearchFill,
    exit_fill: ResearchFill,
    policy: RoundTripExecutionPolicy,
    limits: RecommendationBacktestLimits,
    total_equity_points: &mut usize,
    observation_visits: &mut usize,
) -> Result<RecommendationRoundTripOutcomeV1, RecommendationBacktestError> {
    let reporting_currency = policy.reporting_currency;
    let execution_terms = entry_observation.execution_terms;
    if exit_observation.execution_terms != execution_terms {
        return Err(RecommendationBacktestError::ExecutionTermsChanged);
    }
    let entry_notional = fill_notional(&entry_fill, execution_terms, reporting_currency)?;
    let entry_cost = entry_notional
        .checked_add(entry_fill.fee().amount())
        .ok_or(RecommendationBacktestError::Arithmetic)?;
    count_action_visits(corporate_actions, limits, observation_visits)?;
    let final_accounting = RecommendationAccounting::at(
        &entry_fill,
        execution_terms,
        Some(&exit_fill),
        corporate_actions,
        exit_fill.executed_at(),
    )
    .map_err(|_| RecommendationBacktestError::InvalidDataset)?;
    if !final_accounting
        .quantity()
        .map_err(|_| RecommendationBacktestError::InvalidDataset)?
        .is_zero()
    {
        return Err(RecommendationBacktestError::InvalidDataset);
    }
    let exit_proceeds = final_accounting.cash().amount();
    let unpaid_entitlement_value = final_accounting
        .receivable_value()
        .map_err(|_| RecommendationBacktestError::InvalidDataset)?
        .amount();
    let terminal_equity = exit_proceeds
        .checked_add(unpaid_entitlement_value)
        .ok_or(RecommendationBacktestError::Arithmetic)?;
    if entry_cost <= Decimal::ZERO || terminal_equity < Decimal::ZERO {
        return Err(RecommendationBacktestError::Arithmetic);
    }
    let cost_adjusted_total_return = terminal_equity
        .checked_sub(entry_cost)
        .and_then(|difference| difference.checked_div(entry_cost))
        .ok_or(RecommendationBacktestError::Arithmetic)?;
    let mut equity_path = Vec::new();
    push_equity_point(
        &mut equity_path,
        RecommendationEquityPointV1 {
            marked_at: entry_fill.executed_at(),
            available_at: entry_observation.available_at(),
            lineage_digest: entry_observation.lineage_digest,
            equity: entry_cost,
        },
        limits,
        *total_equity_points,
    )?;
    for observation in execution_observations_from(dataset, entry_fill.executed_at()) {
        let observation = observation?;
        if cancellation.is_cancelled() {
            return Err(RecommendationBacktestError::Cancelled);
        }
        if observation.decision_at >= exit_fill.executed_at() {
            break;
        }
        count_observation_visit(observation_visits, limits)?;
        if observation.decision_at <= entry_fill.executed_at() {
            continue;
        }
        if observation.instrument_id() != execution_terms.instrument_id() {
            continue;
        }
        if observation.universe != HistoricalUniverseStatus::Eligible
            || observation.reference_price.is_none()
        {
            continue;
        }
        if observation.execution_terms != execution_terms {
            return Err(RecommendationBacktestError::ExecutionTermsChanged);
        }
        count_action_visits(corporate_actions, limits, observation_visits)?;
        let accounting = RecommendationAccounting::at(
            &entry_fill,
            execution_terms,
            None,
            corporate_actions,
            observation.decision_at(),
        )
        .map_err(|_| RecommendationBacktestError::InvalidDataset)?;
        let quantity = accounting
            .quantity()
            .map_err(|_| RecommendationBacktestError::InvalidDataset)?;
        let receivable_value = accounting
            .receivable_value()
            .map_err(|_| RecommendationBacktestError::InvalidDataset)?
            .amount();
        let equity = observation
            .reference_price
            .ok_or(RecommendationBacktestError::InvalidDataset)?
            .amount()
            .checked_mul(quantity)
            .and_then(|value| value.checked_mul(execution_terms.contract_multiplier()))
            .and_then(|value| value.checked_add(accounting.cash().amount()))
            .and_then(|value| value.checked_add(receivable_value))
            .ok_or(RecommendationBacktestError::Arithmetic)?;
        push_equity_point(
            &mut equity_path,
            RecommendationEquityPointV1 {
                marked_at: observation.decision_at,
                available_at: observation.available_at(),
                lineage_digest: observation.lineage_digest,
                equity,
            },
            limits,
            *total_equity_points,
        )?;
    }
    push_equity_point(
        &mut equity_path,
        RecommendationEquityPointV1 {
            marked_at: exit_fill.executed_at(),
            available_at: exit_observation.available_at(),
            lineage_digest: exit_observation.lineage_digest,
            equity: terminal_equity,
        },
        limits,
        *total_equity_points,
    )?;
    *total_equity_points = (*total_equity_points)
        .checked_add(equity_path.len())
        .ok_or(RecommendationBacktestError::LimitExceeded)?;
    let maximum_drawdown = maximum_drawdown(&equity_path)?;
    let mut outcome = RecommendationRoundTripOutcomeV1 {
        execution_terms,
        entry_fill,
        exit_fill,
        entry_cost,
        exit_proceeds,
        unpaid_entitlement_value,
        cost_adjusted_total_return,
        maximum_drawdown,
        equity_path: equity_path.into_boxed_slice(),
        digest: Sha256Digest::new([0; 32]),
    };
    outcome.digest = round_trip_digest(&outcome)?;
    Ok(outcome)
}

fn fill_notional(
    fill: &ResearchFill,
    terms: InstrumentExecutionTerms,
    reporting_currency: Currency,
) -> Result<Decimal, RecommendationBacktestError> {
    if fill.instrument_id() != terms.instrument_id()
        || terms.quote_currency() != reporting_currency
        || fill.fee().currency() != reporting_currency
    {
        return Err(RecommendationBacktestError::ReportingCurrencyMismatch);
    }
    fill.price()
        .checked_mul_quantity(
            fill.quantity(),
            terms.price_tick(),
            terms.lot_size(),
            reporting_currency,
        )
        .and_then(|money| money.checked_mul_decimal(terms.contract_multiplier()))
        .map(|money| money.amount())
        .map_err(|_| RecommendationBacktestError::Arithmetic)
}

fn maximum_drawdown(
    equity_path: &[RecommendationEquityPointV1],
) -> Result<Decimal, RecommendationBacktestError> {
    let mut peak = equity_path
        .first()
        .map(|point| point.equity)
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(RecommendationBacktestError::Arithmetic)?;
    let mut maximum = Decimal::ZERO;
    for point in equity_path.iter().skip(1) {
        if point.equity > peak {
            peak = point.equity;
            continue;
        }
        let drawdown = peak
            .checked_sub(point.equity)
            .and_then(|decline| decline.checked_div(peak))
            .ok_or(RecommendationBacktestError::Arithmetic)?;
        maximum = maximum.max(drawdown);
    }
    Ok(maximum)
}

fn push_equity_point(
    path: &mut Vec<RecommendationEquityPointV1>,
    point: RecommendationEquityPointV1,
    limits: RecommendationBacktestLimits,
    committed_total: usize,
) -> Result<(), RecommendationBacktestError> {
    let next_length = path
        .len()
        .checked_add(1)
        .ok_or(RecommendationBacktestError::LimitExceeded)?;
    if next_length > limits.max_equity_points_per_outcome
        || committed_total
            .checked_add(next_length)
            .is_none_or(|total| total > limits.max_total_equity_points)
    {
        return Err(RecommendationBacktestError::LimitExceeded);
    }
    path.try_reserve(1)
        .map_err(|_| RecommendationBacktestError::LimitExceeded)?;
    path.push(point);
    Ok(())
}

fn aggregate_evidence(
    plan: &RecommendationSignalPlanV1,
    results: &[RecommendationSignalResultV1],
) -> Result<RecommendationAggregateEvidenceV1, RecommendationBacktestError> {
    if plan.completeness != RecommendationSignalPlanCompletenessV1::Complete {
        return Ok(RecommendationAggregateEvidenceV1::Unavailable(
            RecommendationAggregateUnavailableV1::TruncatedSignalPopulation,
        ));
    }
    for (result_index, (signal, result)) in plan.signals.iter().zip(results).enumerate() {
        if signal.instruction == RecommendationSignalInstructionV1::Entry
            && !matches!(
                &result.disposition,
                RecommendationSignalDispositionV1::Completed { .. }
                    | RecommendationSignalDispositionV1::BenchmarkUnavailable { .. }
                    | RecommendationSignalDispositionV1::Censored(_)
            )
        {
            return Ok(RecommendationAggregateEvidenceV1::Unavailable(
                RecommendationAggregateUnavailableV1::IncompleteDeclaredEntry { result_index },
            ));
        }
    }
    let mut fold_returns = vec![Vec::<Decimal>::new(); plan.folds.len()];
    let mut subject_returns = Vec::new();
    let mut subject_drawdowns = Vec::new();
    let mut benchmark_returns = Vec::new();
    let mut excess_returns = Vec::new();
    let mut accompanying_returns = Vec::new();
    let mut accompanying_excess_returns = Vec::new();
    let mut benchmark_complete = true;
    for result in results {
        let (subject, benchmark) = match &result.disposition {
            RecommendationSignalDispositionV1::Completed { subject, benchmark } => {
                (Some(subject), Some(benchmark))
            }
            RecommendationSignalDispositionV1::BenchmarkUnavailable { subject, .. } => {
                benchmark_complete = false;
                (Some(subject), None)
            }
            _ => (None, None),
        };
        let Some(subject) = subject else {
            continue;
        };
        let subject_return = subject.cost_adjusted_total_return;
        fold_returns[result.fold_index].push(subject_return);
        subject_returns.push(subject_return);
        subject_drawdowns.push(subject.maximum_drawdown);
        if let Some(benchmark) = benchmark {
            benchmark_returns.push(benchmark.cost_adjusted_total_return);
            excess_returns.push(
                subject_return
                    .checked_sub(benchmark.cost_adjusted_total_return)
                    .ok_or(RecommendationBacktestError::Arithmetic)?,
            );
        }
        if let Some(Ok(accompanying)) = result.accompanying_benchmark() {
            accompanying_returns.push(accompanying.cost_adjusted_total_return);
            accompanying_excess_returns.push(
                subject_return
                    .checked_sub(accompanying.cost_adjusted_total_return)
                    .ok_or(RecommendationBacktestError::Arithmetic)?,
            );
        }
    }
    for (fold_index, returns) in fold_returns.iter().enumerate() {
        if returns.is_empty() {
            return Ok(RecommendationAggregateEvidenceV1::Unavailable(
                RecommendationAggregateUnavailableV1::MissingCompletedObservationInFold {
                    fold_index,
                },
            ));
        }
    }
    let cost_adjusted_total_return = decimal_mean(&subject_returns)?;
    let worst_maximum_drawdown = subject_drawdowns
        .into_iter()
        .max()
        .ok_or(RecommendationBacktestError::Arithmetic)?;
    let positive_fold_count = fold_returns.iter().try_fold(0_usize, |count, returns| {
        let positive = decimal_mean(returns)? > Decimal::ZERO;
        count
            .checked_add(if positive { 1 } else { 0 })
            .ok_or(RecommendationBacktestError::Arithmetic)
    })?;
    let trial_count = fold_returns.len();
    let positive_fold_stability = Decimal::from(
        u64::try_from(positive_fold_count).map_err(|_| RecommendationBacktestError::Arithmetic)?,
    )
    .checked_div(Decimal::from(
        u64::try_from(trial_count).map_err(|_| RecommendationBacktestError::Arithmetic)?,
    ))
    .ok_or(RecommendationBacktestError::Arithmetic)?;
    let positive_fold_stability_ppm = u32::try_from(
        u64::try_from(positive_fold_count)
            .map_err(|_| RecommendationBacktestError::Arithmetic)?
            .checked_mul(1_000_000)
            .and_then(|value| value.checked_div(u64::try_from(trial_count).ok()?))
            .ok_or(RecommendationBacktestError::Arithmetic)?,
    )
    .map_err(|_| RecommendationBacktestError::Arithmetic)?;
    let benchmark = if benchmark_complete && benchmark_returns.len() == subject_returns.len() {
        RecommendationBenchmarkAggregateV1::Available {
            mean_cost_adjusted_total_return: decimal_mean(&benchmark_returns)?,
            mean_excess_return: decimal_mean(&excess_returns)?,
        }
    } else {
        RecommendationBenchmarkAggregateV1::Unavailable
    };
    let accompanying_benchmark = if accompanying_returns.len() == subject_returns.len() {
        RecommendationBenchmarkAggregateV1::Available {
            mean_cost_adjusted_total_return: decimal_mean(&accompanying_returns)?,
            mean_excess_return: decimal_mean(&accompanying_excess_returns)?,
        }
    } else {
        RecommendationBenchmarkAggregateV1::Unavailable
    };
    let mut aggregate = RecommendationAggregateV1 {
        observation_count: subject_returns.len(),
        trial_count,
        cost_adjusted_total_return,
        worst_maximum_drawdown,
        positive_fold_count,
        positive_fold_stability,
        positive_fold_stability_ppm,
        benchmark,
        accompanying_benchmark,
        digest: Sha256Digest::new([0; 32]),
    };
    aggregate.digest = aggregate_digest(aggregate)?;
    Ok(RecommendationAggregateEvidenceV1::Available(aggregate))
}

fn decimal_mean(values: &[Decimal]) -> Result<Decimal, RecommendationBacktestError> {
    let total = values.iter().try_fold(Decimal::ZERO, |total, value| {
        total
            .checked_add(*value)
            .ok_or(RecommendationBacktestError::Arithmetic)
    })?;
    total
        .checked_div(Decimal::from(
            u64::try_from(values.len()).map_err(|_| RecommendationBacktestError::Arithmetic)?,
        ))
        .ok_or(RecommendationBacktestError::Arithmetic)
}

fn count_observation_visit(
    visits: &mut usize,
    limits: RecommendationBacktestLimits,
) -> Result<(), RecommendationBacktestError> {
    *visits = (*visits)
        .checked_add(1)
        .filter(|count| *count <= limits.max_observation_visits)
        .ok_or(RecommendationBacktestError::LimitExceeded)?;
    Ok(())
}

fn count_action_visits(
    plan: &CorporateActionPlan,
    limits: RecommendationBacktestLimits,
    visits: &mut usize,
) -> Result<(), RecommendationBacktestError> {
    *visits = visits
        .checked_add(plan.steps().len())
        .filter(|value| *value <= limits.max_observation_visits())
        .ok_or(RecommendationBacktestError::LimitExceeded)?;
    Ok(())
}

fn policy_digest(value: &RecommendationBacktestPolicyV1) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/recommendation-backtest-policy/v1\0");
    value.study_qualification.hash_into(&mut hash);
    hash.update(value.subject_instrument_id.as_uuid().as_bytes());
    hash.update(value.benchmark.instrument_id.as_uuid().as_bytes());
    hash.update(value.benchmark.approval_digest.bytes());
    hash.update(
        value
            .accompanying_benchmark
            .instrument_id
            .as_uuid()
            .as_bytes(),
    );
    hash.update(value.accompanying_benchmark.approval_digest.bytes());
    hash.update(value.raw_price_evidence_digest.bytes());
    hash.update([match value.execution_basis {
        BacktestExecutionBasis::ObservedQuoteDepth => 1,
        BacktestExecutionBasis::CompletedDailyBar => 2,
    }]);
    if let Some(spread) = value.daily_bar_assumed_spread_basis_points() {
        hash.update(spread.get().to_be_bytes());
    }
    hash.update(value.corporate_action_content_digest.bytes());
    hash.update(value.corporate_action_audit_digest.bytes());
    hash.update(
        value
            .corporate_action_coverage_starts_at
            .unix_nanos()
            .to_be_bytes(),
    );
    update_text(&mut hash, value.reporting_currency.as_str());
    hash.update(value.subject_quantity.get().to_be_bytes());
    hash.update(value.benchmark_quantity.get().to_be_bytes());
    hash.update(RECOMMENDATION_TARGET_HORIZON_NANOS_V1.to_be_bytes());
    hash.update(value.maximum_entry_lag_nanos.to_be_bytes());
    hash.update(value.maximum_exit_lag_nanos.to_be_bytes());
    hash.update(value.execution_assumptions.digest().bytes());
    hash.update(value.seed.to_be_bytes());
    Sha256Digest::new(hash.finalize().into())
}

fn signal_plan_digest(
    value: &RecommendationSignalPlanV1,
) -> Result<Sha256Digest, RecommendationBacktestError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/recommendation-signal-plan/v1\0");
    value.study_qualification.hash_into(&mut hash);
    hash.update(value.preauthorized_signal_plan_digest.bytes());
    update_completeness(&mut hash, value.completeness)?;
    update_length(&mut hash, value.folds.len())?;
    for fold in &value.folds {
        update_text(&mut hash, fold.fold_id.as_str());
        hash.update(fold.starts_at.unix_nanos().to_be_bytes());
        hash.update(fold.ends_at.unix_nanos().to_be_bytes());
    }
    update_length(&mut hash, value.signals.len())?;
    for signal in &value.signals {
        update_text(&mut hash, signal.signal_id.as_str());
        update_length(&mut hash, signal.fold_index)?;
        hash.update(signal.signal_at.unix_nanos().to_be_bytes());
        hash.update(signal.available_at.unix_nanos().to_be_bytes());
        hash.update(signal.source_selection_as_of.unix_nanos().to_be_bytes());
        signal.study_qualification.hash_into(&mut hash);
        hash.update(signal.target_origin.unix_nanos().to_be_bytes());
        hash.update(signal.target_at.unix_nanos().to_be_bytes());
        hash.update(signal.evidence_digest.bytes());
        update_instruction(&mut hash, signal.instruction);
    }
    Ok(Sha256Digest::new(hash.finalize().into()))
}

fn publication_digest(value: RecommendationBacktestPublicationV1) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/recommendation-backtest-publication/v1\0");
    for timestamp in [
        value.simulation_cutoff,
        value.evaluated_at,
        value.published_at,
        value.available_at,
        value.expires_at,
    ] {
        hash.update(timestamp.unix_nanos().to_be_bytes());
    }
    Sha256Digest::new(hash.finalize().into())
}

fn execution_identity_digest(
    policy_digest: Sha256Digest,
    signal: &RecommendationSignalV1,
    role: &[u8],
    leg: &[u8],
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/recommendation-simulated-leg/v1\0");
    hash.update(policy_digest.bytes());
    update_text(&mut hash, signal.signal_id.as_str());
    hash.update((signal.fold_index as u64).to_be_bytes());
    hash.update(signal.signal_at.unix_nanos().to_be_bytes());
    hash.update(signal.available_at.unix_nanos().to_be_bytes());
    hash.update(signal.source_selection_as_of.unix_nanos().to_be_bytes());
    signal.study_qualification.hash_into(&mut hash);
    hash.update(signal.target_origin.unix_nanos().to_be_bytes());
    hash.update(signal.target_at.unix_nanos().to_be_bytes());
    hash.update(signal.evidence_digest.bytes());
    update_instruction(&mut hash, signal.instruction);
    hash.update((role.len() as u64).to_be_bytes());
    hash.update(role);
    hash.update((leg.len() as u64).to_be_bytes());
    hash.update(leg);
    hash.finalize().into()
}

fn deterministic_seed(base: u64, digest: [u8; 32]) -> u64 {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    base ^ u64::from_be_bytes(bytes)
}

fn round_trip_digest(
    value: &RecommendationRoundTripOutcomeV1,
) -> Result<Sha256Digest, RecommendationBacktestError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/recommendation-round-trip/v1\0");
    update_execution_terms(&mut hash, value.execution_terms);
    update_fill(&mut hash, &value.entry_fill);
    update_fill(&mut hash, &value.exit_fill);
    for decimal in [
        value.entry_cost,
        value.exit_proceeds,
        value.unpaid_entitlement_value,
        value.cost_adjusted_total_return,
        value.maximum_drawdown,
    ] {
        update_decimal(&mut hash, decimal);
    }
    update_length(&mut hash, value.equity_path.len())?;
    for point in &value.equity_path {
        hash.update(point.marked_at.unix_nanos().to_be_bytes());
        hash.update(point.available_at.unix_nanos().to_be_bytes());
        hash.update(point.lineage_digest.bytes());
        update_decimal(&mut hash, point.equity);
    }
    Ok(Sha256Digest::new(hash.finalize().into()))
}

fn signal_result_digest(
    value: &RecommendationSignalResultV1,
) -> Result<Sha256Digest, RecommendationBacktestError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/recommendation-signal-result/v1\0");
    update_text(&mut hash, value.signal_id.as_str());
    update_length(&mut hash, value.fold_index)?;
    hash.update(value.signal_at.unix_nanos().to_be_bytes());
    hash.update(value.signal_available_at.unix_nanos().to_be_bytes());
    hash.update(value.source_selection_as_of.unix_nanos().to_be_bytes());
    value.study_qualification.hash_into(&mut hash);
    hash.update(value.target_origin.unix_nanos().to_be_bytes());
    hash.update(value.target_at.unix_nanos().to_be_bytes());
    hash.update(value.signal_evidence_digest.bytes());
    match &value.disposition {
        RecommendationSignalDispositionV1::Completed { subject, benchmark } => {
            hash.update([0]);
            hash.update(subject.digest.bytes());
            hash.update(benchmark.digest.bytes());
        }
        RecommendationSignalDispositionV1::NoAction => hash.update([1]),
        RecommendationSignalDispositionV1::Unavailable(reason) => {
            hash.update([2, unavailable_reason_code(*reason)]);
        }
        RecommendationSignalDispositionV1::Censored(reason) => {
            hash.update([3, censor_reason_code(*reason)]);
        }
        RecommendationSignalDispositionV1::EntryUnfilled { gap, partial_fill } => {
            hash.update([4, execution_gap_code(*gap)]);
            update_optional_fill(&mut hash, partial_fill.as_ref());
        }
        RecommendationSignalDispositionV1::ExitUnfilled {
            entry_fill,
            gap,
            partial_fill,
        } => {
            hash.update([5, execution_gap_code(*gap)]);
            update_fill(&mut hash, entry_fill);
            update_optional_fill(&mut hash, partial_fill.as_ref());
        }
        RecommendationSignalDispositionV1::BenchmarkUnavailable { subject, gap } => {
            hash.update([6]);
            hash.update(subject.digest.bytes());
            update_benchmark_gap(&mut hash, *gap);
        }
    }
    match value.accompanying_benchmark() {
        None => hash.update([0]),
        Some(Ok(outcome)) => {
            hash.update([1]);
            hash.update(outcome.digest.bytes());
        }
        Some(Err(gap)) => {
            hash.update([2]);
            update_benchmark_gap(&mut hash, gap);
        }
    }
    Ok(Sha256Digest::new(hash.finalize().into()))
}

fn update_benchmark_gap(hash: &mut Sha256, gap: RecommendationBenchmarkGapV1) {
    match gap {
        RecommendationBenchmarkGapV1::EntryUnfilled(reason) => {
            hash.update([0, execution_gap_code(reason)])
        }
        RecommendationBenchmarkGapV1::ExitUnfilled(reason) => {
            hash.update([1, execution_gap_code(reason)])
        }
        RecommendationBenchmarkGapV1::Unavailable(reason) => {
            hash.update([2, unavailable_reason_code(reason)])
        }
    }
}

fn aggregate_digest(
    value: RecommendationAggregateV1,
) -> Result<Sha256Digest, RecommendationBacktestError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/recommendation-aggregate/v1\0");
    update_length(&mut hash, value.observation_count)?;
    update_length(&mut hash, value.trial_count)?;
    update_text(&mut hash, COST_ADJUSTED_TOTAL_RETURN_METRIC);
    update_decimal(&mut hash, value.cost_adjusted_total_return);
    update_text(&mut hash, MAXIMUM_DRAWDOWN_METRIC);
    update_decimal(&mut hash, value.worst_maximum_drawdown);
    update_text(&mut hash, POSITIVE_FOLD_STABILITY_METRIC);
    update_length(&mut hash, value.positive_fold_count)?;
    update_decimal(&mut hash, value.positive_fold_stability);
    hash.update(value.positive_fold_stability_ppm.to_be_bytes());
    for comparison in [value.benchmark, value.accompanying_benchmark] {
        match comparison {
            RecommendationBenchmarkAggregateV1::Available {
                mean_cost_adjusted_total_return,
                mean_excess_return,
            } => {
                hash.update([1]);
                update_decimal(&mut hash, mean_cost_adjusted_total_return);
                update_decimal(&mut hash, mean_excess_return);
            }
            RecommendationBenchmarkAggregateV1::Unavailable => hash.update([0]),
        }
    }
    Ok(Sha256Digest::new(hash.finalize().into()))
}

fn evidence_digest(
    value: &RecommendationBacktestStudyV1,
) -> Result<Sha256Digest, RecommendationBacktestError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/recommendation-backtest-evidence/v1\0");
    hash.update(value.dataset_identity.bytes());
    hash.update(value.dataset_manifest_content.bytes());
    hash.update(value.object_graph_digest.bytes());
    hash.update(value.point_in_time_content.bytes());
    hash.update(value.point_in_time_audit.bytes());
    hash.update(value.policy.digest.bytes());
    hash.update(value.signal_plan_digest.bytes());
    hash.update(value.preauthorized_signal_plan_digest.bytes());
    hash.update(value.publication.digest.bytes());
    for limit in [
        value.limits.max_folds,
        value.limits.max_signals,
        value.limits.max_equity_points_per_outcome,
        value.limits.max_total_equity_points,
        value.limits.max_observation_visits,
    ] {
        update_length(&mut hash, limit)?;
    }
    update_completeness(&mut hash, value.completeness)?;
    update_length(&mut hash, value.results.len())?;
    for result in &value.results {
        hash.update(result.digest.bytes());
    }
    match value.aggregate {
        RecommendationAggregateEvidenceV1::Available(aggregate) => {
            hash.update([1]);
            hash.update(aggregate.digest.bytes());
        }
        RecommendationAggregateEvidenceV1::Unavailable(
            RecommendationAggregateUnavailableV1::TruncatedSignalPopulation,
        ) => hash.update([0, 0]),
        RecommendationAggregateEvidenceV1::Unavailable(
            RecommendationAggregateUnavailableV1::IncompleteDeclaredEntry { result_index },
        ) => {
            hash.update([0, 1]);
            update_length(&mut hash, result_index)?;
        }
        RecommendationAggregateEvidenceV1::Unavailable(
            RecommendationAggregateUnavailableV1::MissingCompletedObservationInFold { fold_index },
        ) => {
            hash.update([0, 2]);
            update_length(&mut hash, fold_index)?;
        }
    }
    Ok(Sha256Digest::new(hash.finalize().into()))
}

fn update_completeness(
    hash: &mut Sha256,
    completeness: RecommendationSignalPlanCompletenessV1,
) -> Result<(), RecommendationBacktestError> {
    match completeness {
        RecommendationSignalPlanCompletenessV1::Complete => hash.update([0]),
        RecommendationSignalPlanCompletenessV1::Truncated { total_signal_count } => {
            hash.update([1]);
            update_length(hash, total_signal_count)?;
        }
    }
    Ok(())
}

fn update_instruction(hash: &mut Sha256, instruction: RecommendationSignalInstructionV1) {
    match instruction {
        RecommendationSignalInstructionV1::Entry => hash.update([0]),
        RecommendationSignalInstructionV1::NoAction => hash.update([1]),
        RecommendationSignalInstructionV1::Unavailable(reason) => {
            hash.update([2, unavailable_reason_code(reason)]);
        }
        RecommendationSignalInstructionV1::Censored(reason) => {
            hash.update([3, censor_reason_code(reason)]);
        }
    }
}

fn update_optional_fill(hash: &mut Sha256, fill: Option<&ResearchFill>) {
    match fill {
        Some(fill) => {
            hash.update([1]);
            update_fill(hash, fill);
        }
        None => hash.update([0]),
    }
}

fn update_fill(hash: &mut Sha256, fill: &ResearchFill) {
    hash.update(fill.order_id().as_uuid().as_bytes());
    hash.update(fill.intent_digest().as_bytes());
    hash.update(fill.instrument_id().as_uuid().as_bytes());
    hash.update(fill.signal_at().unix_nanos().to_be_bytes());
    hash.update(fill.executed_at().unix_nanos().to_be_bytes());
    hash.update([match fill.side() {
        OrderSide::Buy => 0,
        OrderSide::Sell => 1,
    }]);
    hash.update(fill.quantity().get().to_be_bytes());
    hash.update(fill.price().get().to_be_bytes());
    update_decimal(hash, fill.fee().amount());
    update_text(hash, fill.fee().currency().as_str());
    hash.update([u8::from(fill.partial())]);
    hash.update(fill.assumption_digest().bytes());
}

fn update_execution_terms(hash: &mut Sha256, terms: InstrumentExecutionTerms) {
    hash.update(terms.instrument_id().as_uuid().as_bytes());
    hash.update(terms.definition_revision().get().to_be_bytes());
    update_decimal(hash, terms.price_tick().as_decimal());
    update_decimal(hash, terms.lot_size().as_decimal());
    update_text(hash, terms.quote_currency().as_str());
    update_decimal(hash, terms.contract_multiplier());
}

fn update_decimal(hash: &mut Sha256, value: Decimal) {
    let value = value.normalize();
    hash.update(value.mantissa().to_be_bytes());
    hash.update(value.scale().to_be_bytes());
}

fn update_text(hash: &mut Sha256, value: &str) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value.as_bytes());
}

fn update_length(hash: &mut Sha256, value: usize) -> Result<(), RecommendationBacktestError> {
    hash.update(
        u64::try_from(value)
            .map_err(|_| RecommendationBacktestError::Arithmetic)?
            .to_be_bytes(),
    );
    Ok(())
}

const fn unavailable_reason_code(reason: RecommendationSignalUnavailableReasonV1) -> u8 {
    match reason {
        RecommendationSignalUnavailableReasonV1::InsufficientPointInTimeEvidence => 0,
        RecommendationSignalUnavailableReasonV1::ConflictingEvidence => 1,
        RecommendationSignalUnavailableReasonV1::UnsupportedInstrument => 2,
        RecommendationSignalUnavailableReasonV1::PolicyRejected => 3,
        RecommendationSignalUnavailableReasonV1::ReportingCurrencyMismatch => 4,
        RecommendationSignalUnavailableReasonV1::ExecutionTermsChanged => 5,
    }
}

const fn censor_reason_code(reason: RecommendationSignalCensorReasonV1) -> u8 {
    match reason {
        RecommendationSignalCensorReasonV1::TargetAfterSimulationCutoff => 0,
        RecommendationSignalCensorReasonV1::OutsideAuthorizedDataset => 1,
    }
}

const fn execution_gap_code(reason: RecommendationExecutionGapV1) -> u8 {
    match reason {
        RecommendationExecutionGapV1::NoEligibleObservation => 0,
        RecommendationExecutionGapV1::InsufficientLiquidity => 1,
    }
}

fn require_digest(digest: Sha256Digest) -> Result<(), RecommendationBacktestError> {
    if digest.bytes() == [0; 32] {
        Err(RecommendationBacktestError::InvalidEvidenceDigest)
    } else {
        Ok(())
    }
}

fn encode_hex_prefix(bytes: [u8; 32], byte_count: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(byte_count.saturating_mul(2));
    for byte in bytes.into_iter().take(byte_count) {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

/// Strict recommendation-outcome validation or arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RecommendationBacktestError {
    #[error("recommendation backtest was cancelled")]
    Cancelled,
    #[error("recommendation backtest policy is invalid")]
    InvalidPolicy,
    #[error("recommendation backtest limits are invalid")]
    InvalidLimits,
    #[error("recommendation signal plan is invalid")]
    InvalidSignalPlan,
    #[error("recommendation publication chronology is invalid")]
    InvalidPublication,
    #[error("recommendation evidence digest must be nonzero")]
    InvalidEvidenceDigest,
    #[error("recommendation dataset evidence is invalid")]
    InvalidDataset,
    #[error("recommendation reporting currency does not match execution evidence")]
    ReportingCurrencyMismatch,
    #[error("instrument execution terms changed inside one outcome")]
    ExecutionTermsChanged,
    #[error("recommendation backtest resource limit exceeded")]
    LimitExceeded,
    #[error("recommendation backtest exact arithmetic failed")]
    Arithmetic,
}
