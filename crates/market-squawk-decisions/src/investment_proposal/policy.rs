//! Code-owned recommendation policy and policy-weighted reliability semantics.

use std::num::NonZeroU32;

use market_squawk_domain::{BasisPoints, RoundingPolicy};

use crate::DecisionText;

use super::digest::hash_policy;
use super::evidence::RecommendationStudyQualification;
use super::{
    CONFIDENCE_PARTS_PER_MILLION, InvestmentProposalError, RECOMMENDATION_ASSUMPTION_COUNT,
    RECOMMENDATION_CONFIDENCE_COMPONENT_COUNT, RECOMMENDATION_INVALIDATION_COUNT,
    RECOMMENDATION_LIMITATION_COUNT, RecommendationEvidenceKind, RecommendationPolicyDigest,
};

const PRICE_RANGE_WEIGHT_COUNT: usize = 9;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SECOND;
const MINIMUM_ACTIONABLE_REALIZED_COVERAGE_PPM: u32 = 500_000;
const MAXIMUM_ACTIONABLE_CALIBRATION_ERROR_PPM: u32 = 50_000;

/// Whether V1 binds an outcome benchmark selected at proposal time.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProposalTimeBenchmarkAvailability {
    /// V1 does not select a benchmark and later code must not choose one after returns are known.
    UnavailableByPolicyV1,
}

/// Whether V1 binds action-specific forward cost estimates.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ActionSpecificCostAvailability {
    /// V1 retains cost-adjusted backtest evidence but no action-specific forward cost estimate.
    UnavailableByPolicyV1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RecommendationPolicySemantics {
    pub(super) version: NonZeroU32,
    pub(super) allow_retrospective_studies: bool,
    pub(super) action_zone_semantics_version: NonZeroU32,
    pub(super) horizon_nanos: i64,
    pub(super) proposal_lifetime_nanos: i64,
    pub(super) market_max_age_nanos: i64,
    pub(super) forecast_max_age_nanos: i64,
    pub(super) valuation_max_age_nanos: i64,
    pub(super) financial_model_max_age_nanos: i64,
    pub(super) backtest_max_age_nanos: i64,
    pub(super) out_of_sample_max_age_nanos: i64,
    pub(super) harmonic_pattern_max_age_nanos: i64,
    pub(super) liquidity_max_age_nanos: i64,
    pub(super) portfolio_risk_max_age_nanos: i64,
    pub(super) bullish_threshold: BasisPoints,
    pub(super) bearish_threshold: BasisPoints,
    pub(super) minimum_forecast_outcomes: NonZeroU32,
    pub(super) minimum_nominal_forecast_coverage_ppm: u32,
    pub(super) maximum_nominal_forecast_coverage_ppm: u32,
    pub(super) minimum_realized_forecast_coverage_ppm: u32,
    pub(super) maximum_forecast_calibration_error_ppm: u32,
    pub(super) minimum_backtest_observations: NonZeroU32,
    pub(super) minimum_backtest_trials: NonZeroU32,
    pub(super) minimum_backtest_stability_ppm: u32,
    pub(super) minimum_oos_completion_coverage_ppm: u32,
    pub(super) minimum_cost_adjusted_return: BasisPoints,
    pub(super) maximum_backtest_drawdown: BasisPoints,
    pub(super) maximum_liquidity_spread: BasisPoints,
    pub(super) minimum_liquidity_capacity_ppm: u32,
    pub(super) minimum_portfolio_risk_capacity_ppm: u32,
    pub(super) minimum_confidence_ppm: u32,
    pub(super) forecast_base_weight_bps: u32,
    pub(super) valuation_weight_bps: u32,
    pub(super) confidence_weights_ppm: [u32; RECOMMENDATION_CONFIDENCE_COMPONENT_COUNT],
    pub(super) price_range_weights_bps: [u32; PRICE_RANGE_WEIGHT_COUNT],
    pub(super) price_scale: u32,
    pub(super) rounding_policy: RoundingPolicy,
    pub(super) proposal_time_benchmark_availability: ProposalTimeBenchmarkAvailability,
    pub(super) action_specific_cost_availability: ActionSpecificCostAvailability,
    pub(super) assumptions: [DecisionText; RECOMMENDATION_ASSUMPTION_COUNT],
    pub(super) invalidation_conditions: [DecisionText; RECOMMENDATION_INVALIDATION_COUNT],
    pub(super) limitations: [DecisionText; RECOMMENDATION_LIMITATION_COUNT],
}

/// Validated caller-selected financial policy parameters under the fixed V1 interpretation.
/// Values change admission and economic zones; none is an empirical accuracy guarantee.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationPolicyParameters {
    /// Allows explicitly qualified later-snapshot historical studies; false requires as-known inputs.
    pub allow_retrospective_studies: bool,
    /// Positive exclusive proposal lifetime in nanoseconds.
    pub proposal_lifetime_nanos: i64,
    /// Positive maximum source age in nanoseconds.
    pub market_max_age_nanos: i64,
    /// Positive maximum source age in nanoseconds.
    pub forecast_max_age_nanos: i64,
    /// Positive maximum source age in nanoseconds.
    pub valuation_max_age_nanos: i64,
    /// Positive maximum source age in nanoseconds.
    pub financial_model_max_age_nanos: i64,
    /// Positive maximum source age in nanoseconds.
    pub backtest_max_age_nanos: i64,
    /// Positive maximum source age in nanoseconds.
    pub out_of_sample_max_age_nanos: i64,
    /// Positive maximum source age in nanoseconds.
    pub harmonic_pattern_max_age_nanos: i64,
    /// Positive maximum source age in nanoseconds.
    pub liquidity_max_age_nanos: i64,
    /// Positive maximum source age in nanoseconds.
    pub portfolio_risk_max_age_nanos: i64,
    /// Positive bullish-direction threshold in basis points.
    pub bullish_threshold: BasisPoints,
    /// Positive bearish-direction threshold in basis points.
    pub bearish_threshold: BasisPoints,
    /// Positive minimum count of completed forecast outcomes.
    pub minimum_forecast_outcomes: NonZeroU32,
    /// Admission threshold in parts per million, between zero and one million inclusive.
    pub minimum_nominal_forecast_coverage_ppm: u32,
    /// Admission threshold in parts per million, between zero and one million inclusive.
    pub maximum_nominal_forecast_coverage_ppm: u32,
    /// Minimum realized coverage; custom policies may tighten the 500,000 ppm action floor.
    pub minimum_realized_forecast_coverage_ppm: u32,
    /// Maximum absolute coverage error; custom policies may tighten the 50,000 ppm ceiling.
    pub maximum_forecast_calibration_error_ppm: u32,
    /// Positive minimum count of completed historical observations.
    pub minimum_backtest_observations: NonZeroU32,
    /// Positive minimum count of separately retained historical folds.
    pub minimum_backtest_trials: NonZeroU32,
    /// Admission threshold in parts per million, between zero and one million inclusive.
    pub minimum_backtest_stability_ppm: u32,
    /// Admission threshold in parts per million, between zero and one million inclusive.
    pub minimum_oos_completion_coverage_ppm: u32,
    /// Nonnegative minimum historical return in basis points.
    pub minimum_cost_adjusted_return: BasisPoints,
    /// Positive maximum historical drawdown in basis points.
    pub maximum_backtest_drawdown: BasisPoints,
    /// Positive maximum quoted spread in basis points.
    pub maximum_liquidity_spread: BasisPoints,
    /// Admission threshold in parts per million, between zero and one million inclusive.
    pub minimum_liquidity_capacity_ppm: u32,
    /// Admission threshold in parts per million, between zero and one million inclusive.
    pub minimum_portfolio_risk_capacity_ppm: u32,
    /// Admission threshold in parts per million, between zero and one million inclusive.
    pub minimum_confidence_ppm: u32,
    /// Forecast blend weight in basis points; together with valuation it sums to ten thousand.
    pub forecast_base_weight_bps: u32,
    /// Governed valuation blend weight in basis points.
    pub valuation_weight_bps: u32,
    /// Six component weights in published component order, summing to one million.
    pub confidence_weights_ppm: [u32; RECOMMENDATION_CONFIDENCE_COMPONENT_COUNT],
    /// Nine zone interpolation weights in exit/add/entry/trim/add-case order; strict ordering is validated.
    pub price_range_weights_bps: [u32; 9],
    /// Output price decimal scale, at most Decimal::MAX_SCALE.
    pub price_scale: u32,
    /// Explicit deterministic output rounding policy.
    pub rounding_policy: RoundingPolicy,
}

/// Closed, versioned semantics used by the deterministic recommendation authority.
///
/// V1 fixes the interpretation, horizon, and narratives. Callers may select validated financial
/// parameters; every parameter contributes to the canonical policy identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecommendationPolicy {
    pub(super) semantics: RecommendationPolicySemantics,
    pub(super) digest: RecommendationPolicyDigest,
}

impl RecommendationPolicy {
    /// Constructs the complete production V1 semantics and canonical digest.
    ///
    /// # Errors
    ///
    /// Returns an error only if code-owned policy constants violate their own closed invariants.
    pub fn v1() -> Result<Self, InvestmentProposalError> {
        let minimum_forecast_outcomes =
            NonZeroU32::new(30).ok_or(InvestmentProposalError::InvalidPolicy)?;
        let minimum_backtest_observations =
            NonZeroU32::new(100).ok_or(InvestmentProposalError::InvalidPolicy)?;
        let minimum_backtest_trials =
            NonZeroU32::new(3).ok_or(InvestmentProposalError::InvalidPolicy)?;
        let assumptions = [
            policy_text(
                "forecast and valuation remain comparable in the stated currency and horizon",
            )?,
            policy_text(
                "historical studies retain their data basis, limitations and trading costs",
            )?,
            policy_text("market liquidity and portfolio risk remain within policy bounds")?,
        ];
        let invalidation_conditions = [
            policy_text("any mandatory evidence expires or is superseded")?,
            policy_text("forecast and valuation move to opposing policy directions")?,
            policy_text("liquidity or portfolio risk falls below the admitted threshold")?,
        ];
        let limitations = [
            policy_text(
                "research proposal only; it cannot create an order or execution authority",
            )?,
            policy_text(
                "confidence is policy-weighted evidence reliability, not probability of profit",
            )?,
            policy_text("historical backtest performance does not guarantee future results")?,
        ];
        let semantics = RecommendationPolicySemantics {
            version: NonZeroU32::MIN,
            allow_retrospective_studies: true,
            action_zone_semantics_version: NonZeroU32::MIN,
            horizon_nanos: 365 * NANOS_PER_DAY,
            proposal_lifetime_nanos: 7 * NANOS_PER_DAY,
            market_max_age_nanos: 60 * NANOS_PER_SECOND,
            forecast_max_age_nanos: 7 * NANOS_PER_DAY,
            valuation_max_age_nanos: 30 * NANOS_PER_DAY,
            financial_model_max_age_nanos: 30 * NANOS_PER_DAY,
            backtest_max_age_nanos: 180 * NANOS_PER_DAY,
            out_of_sample_max_age_nanos: 180 * NANOS_PER_DAY,
            harmonic_pattern_max_age_nanos: 5 * NANOS_PER_DAY,
            liquidity_max_age_nanos: 60 * NANOS_PER_SECOND,
            portfolio_risk_max_age_nanos: 5 * 60 * NANOS_PER_SECOND,
            bullish_threshold: BasisPoints::new(1_000),
            bearish_threshold: BasisPoints::new(1_000),
            minimum_forecast_outcomes,
            minimum_nominal_forecast_coverage_ppm: 500_000,
            maximum_nominal_forecast_coverage_ppm: 990_000,
            minimum_realized_forecast_coverage_ppm: MINIMUM_ACTIONABLE_REALIZED_COVERAGE_PPM,
            // Conservative action-admission policy: at most five percentage points of
            // marginal coverage error, the smallest supported 95% band's tail budget.
            // This is neither a statistical independence claim nor guaranteed accuracy.
            maximum_forecast_calibration_error_ppm: MAXIMUM_ACTIONABLE_CALIBRATION_ERROR_PPM,
            minimum_backtest_observations,
            minimum_backtest_trials,
            minimum_backtest_stability_ppm: 600_000,
            minimum_oos_completion_coverage_ppm: 800_000,
            minimum_cost_adjusted_return: BasisPoints::new(200),
            maximum_backtest_drawdown: BasisPoints::new(4_000),
            maximum_liquidity_spread: BasisPoints::new(100),
            minimum_liquidity_capacity_ppm: 500_000,
            minimum_portfolio_risk_capacity_ppm: 300_000,
            minimum_confidence_ppm: 650_000,
            forecast_base_weight_bps: 6_000,
            valuation_weight_bps: 4_000,
            confidence_weights_ppm: [250_000, 150_000, 250_000, 100_000, 125_000, 125_000],
            price_range_weights_bps: [
                7_500, 6_500, 4_500, 3_500, 2_500, 1_500, 8_500, 7_000, 5_000,
            ],
            price_scale: 4,
            rounding_policy: RoundingPolicy::NearestEven,
            proposal_time_benchmark_availability:
                ProposalTimeBenchmarkAvailability::UnavailableByPolicyV1,
            action_specific_cost_availability:
                ActionSpecificCostAvailability::UnavailableByPolicyV1,
            assumptions,
            invalidation_conditions,
            limitations,
        };
        validate_policy(&semantics)?;
        let digest = RecommendationPolicyDigest::try_from_bytes(hash_policy(&semantics))?;
        Ok(Self { semantics, digest })
    }

    /// Validates custom financial semantics and commits them under the fixed V1 interpretation.
    pub fn try_new(
        parameters: RecommendationPolicyParameters,
    ) -> Result<Self, InvestmentProposalError> {
        let mut policy = Self::v1()?;
        policy.semantics.allow_retrospective_studies = parameters.allow_retrospective_studies;
        policy.semantics.proposal_lifetime_nanos = parameters.proposal_lifetime_nanos;
        policy.semantics.market_max_age_nanos = parameters.market_max_age_nanos;
        policy.semantics.forecast_max_age_nanos = parameters.forecast_max_age_nanos;
        policy.semantics.valuation_max_age_nanos = parameters.valuation_max_age_nanos;
        policy.semantics.financial_model_max_age_nanos = parameters.financial_model_max_age_nanos;
        policy.semantics.backtest_max_age_nanos = parameters.backtest_max_age_nanos;
        policy.semantics.out_of_sample_max_age_nanos = parameters.out_of_sample_max_age_nanos;
        policy.semantics.harmonic_pattern_max_age_nanos = parameters.harmonic_pattern_max_age_nanos;
        policy.semantics.liquidity_max_age_nanos = parameters.liquidity_max_age_nanos;
        policy.semantics.portfolio_risk_max_age_nanos = parameters.portfolio_risk_max_age_nanos;
        policy.semantics.bullish_threshold = parameters.bullish_threshold;
        policy.semantics.bearish_threshold = parameters.bearish_threshold;
        policy.semantics.minimum_forecast_outcomes = parameters.minimum_forecast_outcomes;
        policy.semantics.minimum_nominal_forecast_coverage_ppm =
            parameters.minimum_nominal_forecast_coverage_ppm;
        policy.semantics.maximum_nominal_forecast_coverage_ppm =
            parameters.maximum_nominal_forecast_coverage_ppm;
        policy.semantics.minimum_realized_forecast_coverage_ppm =
            parameters.minimum_realized_forecast_coverage_ppm;
        policy.semantics.maximum_forecast_calibration_error_ppm =
            parameters.maximum_forecast_calibration_error_ppm;
        policy.semantics.minimum_backtest_observations = parameters.minimum_backtest_observations;
        policy.semantics.minimum_backtest_trials = parameters.minimum_backtest_trials;
        policy.semantics.minimum_backtest_stability_ppm = parameters.minimum_backtest_stability_ppm;
        policy.semantics.minimum_oos_completion_coverage_ppm =
            parameters.minimum_oos_completion_coverage_ppm;
        policy.semantics.minimum_cost_adjusted_return = parameters.minimum_cost_adjusted_return;
        policy.semantics.maximum_backtest_drawdown = parameters.maximum_backtest_drawdown;
        policy.semantics.maximum_liquidity_spread = parameters.maximum_liquidity_spread;
        policy.semantics.minimum_liquidity_capacity_ppm = parameters.minimum_liquidity_capacity_ppm;
        policy.semantics.minimum_portfolio_risk_capacity_ppm =
            parameters.minimum_portfolio_risk_capacity_ppm;
        policy.semantics.minimum_confidence_ppm = parameters.minimum_confidence_ppm;
        policy.semantics.forecast_base_weight_bps = parameters.forecast_base_weight_bps;
        policy.semantics.valuation_weight_bps = parameters.valuation_weight_bps;
        policy.semantics.confidence_weights_ppm = parameters.confidence_weights_ppm;
        policy.semantics.price_range_weights_bps = parameters.price_range_weights_bps;
        policy.semantics.price_scale = parameters.price_scale;
        policy.semantics.rounding_policy = parameters.rounding_policy;
        validate_policy(&policy.semantics)?;
        policy.digest = RecommendationPolicyDigest::try_from_bytes(hash_policy(&policy.semantics))?;
        Ok(policy)
    }

    /// Returns every editable parameter in its exact declared unit.
    #[must_use]
    pub const fn parameters(&self) -> RecommendationPolicyParameters {
        RecommendationPolicyParameters {
            allow_retrospective_studies: self.semantics.allow_retrospective_studies,
            proposal_lifetime_nanos: self.semantics.proposal_lifetime_nanos,
            market_max_age_nanos: self.semantics.market_max_age_nanos,
            forecast_max_age_nanos: self.semantics.forecast_max_age_nanos,
            valuation_max_age_nanos: self.semantics.valuation_max_age_nanos,
            financial_model_max_age_nanos: self.semantics.financial_model_max_age_nanos,
            backtest_max_age_nanos: self.semantics.backtest_max_age_nanos,
            out_of_sample_max_age_nanos: self.semantics.out_of_sample_max_age_nanos,
            harmonic_pattern_max_age_nanos: self.semantics.harmonic_pattern_max_age_nanos,
            liquidity_max_age_nanos: self.semantics.liquidity_max_age_nanos,
            portfolio_risk_max_age_nanos: self.semantics.portfolio_risk_max_age_nanos,
            bullish_threshold: self.semantics.bullish_threshold,
            bearish_threshold: self.semantics.bearish_threshold,
            minimum_forecast_outcomes: self.semantics.minimum_forecast_outcomes,
            minimum_nominal_forecast_coverage_ppm: self
                .semantics
                .minimum_nominal_forecast_coverage_ppm,
            maximum_nominal_forecast_coverage_ppm: self
                .semantics
                .maximum_nominal_forecast_coverage_ppm,
            minimum_realized_forecast_coverage_ppm: self
                .semantics
                .minimum_realized_forecast_coverage_ppm,
            maximum_forecast_calibration_error_ppm: self
                .semantics
                .maximum_forecast_calibration_error_ppm,
            minimum_backtest_observations: self.semantics.minimum_backtest_observations,
            minimum_backtest_trials: self.semantics.minimum_backtest_trials,
            minimum_backtest_stability_ppm: self.semantics.minimum_backtest_stability_ppm,
            minimum_oos_completion_coverage_ppm: self.semantics.minimum_oos_completion_coverage_ppm,
            minimum_cost_adjusted_return: self.semantics.minimum_cost_adjusted_return,
            maximum_backtest_drawdown: self.semantics.maximum_backtest_drawdown,
            maximum_liquidity_spread: self.semantics.maximum_liquidity_spread,
            minimum_liquidity_capacity_ppm: self.semantics.minimum_liquidity_capacity_ppm,
            minimum_portfolio_risk_capacity_ppm: self.semantics.minimum_portfolio_risk_capacity_ppm,
            minimum_confidence_ppm: self.semantics.minimum_confidence_ppm,
            forecast_base_weight_bps: self.semantics.forecast_base_weight_bps,
            valuation_weight_bps: self.semantics.valuation_weight_bps,
            confidence_weights_ppm: self.semantics.confidence_weights_ppm,
            price_range_weights_bps: self.semantics.price_range_weights_bps,
            price_scale: self.semantics.price_scale,
            rounding_policy: self.semantics.rounding_policy,
        }
    }

    /// Reconstructs a supported code-owned policy and verifies its persisted identity.
    ///
    /// # Errors
    ///
    /// Rejects unsupported versions and mismatched semantic digests.
    pub fn try_recover(
        version: NonZeroU32,
        parameters: RecommendationPolicyParameters,
        expected_digest: RecommendationPolicyDigest,
    ) -> Result<Self, InvestmentProposalError> {
        let policy = match version.get() {
            1 => Self::try_new(parameters)?,
            _ => return Err(InvestmentProposalError::PolicyIdentityMismatch),
        };
        if policy.digest != expected_digest {
            return Err(InvestmentProposalError::PolicyIdentityMismatch);
        }
        Ok(policy)
    }

    /// Returns the code-owned semantic version.
    #[must_use]
    pub const fn version(&self) -> NonZeroU32 {
        self.semantics.version
    }

    /// Returns the version of the universal long-investment zone/action table.
    #[must_use]
    pub const fn action_zone_semantics_version(&self) -> NonZeroU32 {
        self.semantics.action_zone_semantics_version
    }

    /// Returns the commitment to every semantic field and fixed narrative.
    #[must_use]
    pub const fn digest(&self) -> RecommendationPolicyDigest {
        self.digest
    }

    /// Returns the fixed investment-analysis horizon as nanoseconds.
    #[must_use]
    pub const fn horizon_nanos(&self) -> i64 {
        self.semantics.horizon_nanos
    }

    /// Returns the exclusive proposal lifetime after the analysis cutoff.
    #[must_use]
    pub const fn proposal_lifetime_nanos(&self) -> i64 {
        self.semantics.proposal_lifetime_nanos
    }

    /// Returns the fixed evidence-bound assumptions.
    #[must_use]
    pub fn assumptions(&self) -> &[DecisionText; RECOMMENDATION_ASSUMPTION_COUNT] {
        &self.semantics.assumptions
    }

    /// Returns the fixed conditions that require a new analysis.
    #[must_use]
    pub fn invalidation_conditions(&self) -> &[DecisionText; RECOMMENDATION_INVALIDATION_COUNT] {
        &self.semantics.invalidation_conditions
    }

    /// Returns explicit research, confidence, and historical-performance limitations.
    #[must_use]
    pub fn limitations(&self) -> &[DecisionText; RECOMMENDATION_LIMITATION_COUNT] {
        &self.semantics.limitations
    }

    /// Returns whether an outcome benchmark was selected before proposal returns can be observed.
    #[must_use]
    pub const fn proposal_time_benchmark_availability(&self) -> ProposalTimeBenchmarkAvailability {
        self.semantics.proposal_time_benchmark_availability
    }

    /// Returns whether V1 has action-specific forward cost evidence.
    #[must_use]
    pub const fn action_specific_cost_availability(&self) -> ActionSpecificCostAvailability {
        self.semantics.action_specific_cost_availability
    }

    pub(super) const fn maximum_age_nanos(&self, kind: RecommendationEvidenceKind) -> i64 {
        match kind {
            RecommendationEvidenceKind::Market => self.semantics.market_max_age_nanos,
            RecommendationEvidenceKind::PriceForecast => self.semantics.forecast_max_age_nanos,
            RecommendationEvidenceKind::Valuation => self.semantics.valuation_max_age_nanos,
            RecommendationEvidenceKind::FinancialModel => {
                self.semantics.financial_model_max_age_nanos
            }
            RecommendationEvidenceKind::Backtest => self.semantics.backtest_max_age_nanos,
            RecommendationEvidenceKind::OutOfSample => self.semantics.out_of_sample_max_age_nanos,
            RecommendationEvidenceKind::HarmonicPattern => {
                self.semantics.harmonic_pattern_max_age_nanos
            }
            RecommendationEvidenceKind::Liquidity => self.semantics.liquidity_max_age_nanos,
            RecommendationEvidenceKind::PortfolioRisk => {
                self.semantics.portfolio_risk_max_age_nanos
            }
        }
    }
}

/// Semantic meaning of a confidence number. It is never an expected-return or profit probability.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RecommendationConfidenceMeaning {
    /// Deterministic policy weighting of six admitted evidence authorities under V1.
    PolicyWeightedEvidenceReliabilityV1,
}

/// One closed component of the policy-weighted evidence-reliability calculation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RecommendationConfidenceComponentKind {
    /// Difference between nominal and empirically realized forecast coverage.
    ForecastCalibration,
    /// Agreement of independently governed forecast and valuation evidence.
    ValuationAgreement,
    /// Stability of the cost-adjusted historical study under its retained qualification.
    BacktestStability,
    /// Evidentiary quality of the current market reference.
    MarketIntegrity,
    /// Spread- and capacity-adjusted liquidity reliability.
    LiquidityCapacity,
    /// Current account-specific portfolio risk capacity.
    PortfolioRiskCapacity,
}

/// Exact reason why a directional component or its weighted aggregate has no score.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationConfidenceUnavailableReason {
    /// A prospective Buy or Add lacks actual buy-side capacity.
    BuyAddCapacityUnavailable,
    /// A prospective Trim or Sell lacks actual sell-side capacity.
    TrimSellCapacityUnavailable,
    /// The financial/position gates did not establish a prospective action side.
    ActionSideNotEstablished,
    /// A true Hold excludes liquidity and leaves no configured weight on applicable evidence.
    NoApplicablePolicyWeight,
}

/// Availability of one evidence component; only a genuine Hold excludes liquidity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationConfidenceComponentValue {
    /// Actual bounded component reliability in parts per million.
    Available(u32),
    /// Required evidence cannot be established; no zero score or weight redistribution is inferred.
    Unavailable(RecommendationConfidenceUnavailableReason),
    /// A selected Hold requires no directional liquidity capacity.
    NotApplicable,
}

/// One evidence value and unchanged configured weight in the aggregate reliability calculation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationConfidenceComponent {
    pub(super) kind: RecommendationConfidenceComponentKind,
    pub(super) value: RecommendationConfidenceComponentValue,
    pub(super) weight_ppm: u32,
}

impl RecommendationConfidenceComponent {
    pub(super) const fn new(
        kind: RecommendationConfidenceComponentKind,
        value: RecommendationConfidenceComponentValue,
        weight_ppm: u32,
    ) -> Self {
        Self {
            kind,
            value,
            weight_ppm,
        }
    }

    /// Returns the typed evidence-reliability component.
    #[must_use]
    pub const fn kind(self) -> RecommendationConfidenceComponentKind {
        self.kind
    }

    /// Returns actual availability separately from the component's numeric value.
    #[must_use]
    pub const fn value(self) -> RecommendationConfidenceComponentValue {
        self.value
    }

    /// Returns the actual score, without replacing missing or inapplicable evidence with zero.
    #[must_use]
    pub const fn value_ppm(self) -> Option<u32> {
        match self.value {
            RecommendationConfidenceComponentValue::Available(value) => Some(value),
            RecommendationConfidenceComponentValue::Unavailable(_)
            | RecommendationConfidenceComponentValue::NotApplicable => None,
        }
    }

    /// Returns the unchanged configured component weight in parts per million.
    #[must_use]
    pub const fn weight_ppm(self) -> u32 {
        self.weight_ppm
    }
}

/// Reproducible policy-weighted evidence reliability with an explicit applicable-weight divisor.
///
/// Only forecast coverage carries empirical calibration evidence. The aggregate has not been
/// calibrated against realized recommendation outcomes and is not a profit probability. Missing
/// required evidence makes it unavailable regardless of that component's configured weight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationConfidence {
    pub(super) meaning: RecommendationConfidenceMeaning,
    pub(super) study_qualification: RecommendationStudyQualification,
    pub(super) value_ppm: Option<u32>,
    pub(super) unavailable_reason: Option<RecommendationConfidenceUnavailableReason>,
    pub(super) applicable_policy_weight_ppm: u32,
    pub(super) components:
        [RecommendationConfidenceComponent; RECOMMENDATION_CONFIDENCE_COMPONENT_COUNT],
}

impl RecommendationConfidence {
    /// Returns the closed interpretation of this confidence value.
    #[must_use]
    pub const fn meaning(self) -> RecommendationConfidenceMeaning {
        self.meaning
    }

    /// Returns the actual study qualification carried by this conditional reliability value.
    #[must_use]
    pub const fn study_qualification(self) -> RecommendationStudyQualification {
        self.study_qualification
    }

    /// Returns the available aggregate score; absent required evidence never becomes a score.
    #[must_use]
    pub const fn value_ppm(self) -> Option<u32> {
        self.value_ppm
    }

    /// Returns the exact reason why the aggregate cannot be calculated.
    #[must_use]
    pub const fn unavailable_reason(self) -> Option<RecommendationConfidenceUnavailableReason> {
        self.unavailable_reason
    }

    /// Returns the exact denominator: configured weights of all applicable components.
    /// A missing applicable component still prevents calculation, including when its weight is zero.
    #[must_use]
    pub const fn applicable_policy_weight_ppm(self) -> u32 {
        self.applicable_policy_weight_ppm
    }

    /// Returns all six fixed components, their actual availability and unchanged configured weights.
    #[must_use]
    pub const fn components(
        &self,
    ) -> &[RecommendationConfidenceComponent; RECOMMENDATION_CONFIDENCE_COMPONENT_COUNT] {
        &self.components
    }
}

pub(super) fn validate_policy(
    policy: &RecommendationPolicySemantics,
) -> Result<(), InvestmentProposalError> {
    let confidence_weight_sum = policy
        .confidence_weights_ppm
        .iter()
        .try_fold(0_u32, |total, value| total.checked_add(*value))
        .ok_or(InvestmentProposalError::InvalidPolicy)?;
    let price_weight_sum = policy
        .forecast_base_weight_bps
        .checked_add(policy.valuation_weight_bps)
        .ok_or(InvestmentProposalError::InvalidPolicy)?;
    let range_weights = policy.price_range_weights_bps;
    if policy.price_scale > rust_decimal::Decimal::MAX_SCALE
        || policy.horizon_nanos <= 0
        || policy.proposal_lifetime_nanos <= 0
        || [
            policy.market_max_age_nanos,
            policy.forecast_max_age_nanos,
            policy.valuation_max_age_nanos,
            policy.financial_model_max_age_nanos,
            policy.backtest_max_age_nanos,
            policy.out_of_sample_max_age_nanos,
            policy.harmonic_pattern_max_age_nanos,
            policy.liquidity_max_age_nanos,
            policy.portfolio_risk_max_age_nanos,
        ]
        .into_iter()
        .any(|age| age <= 0)
        || policy.action_zone_semantics_version != NonZeroU32::MIN
        || policy.bullish_threshold.get() <= 0
        || policy.bearish_threshold.get() <= 0
        || policy.minimum_cost_adjusted_return.get() < 0
        || policy.maximum_backtest_drawdown.get() <= 0
        || policy.maximum_liquidity_spread.get() <= 0
        || confidence_weight_sum != CONFIDENCE_PARTS_PER_MILLION
        || price_weight_sum != 10_000
        || [
            policy.minimum_backtest_stability_ppm,
            policy.minimum_oos_completion_coverage_ppm,
            policy.minimum_nominal_forecast_coverage_ppm,
            policy.maximum_nominal_forecast_coverage_ppm,
            policy.minimum_realized_forecast_coverage_ppm,
            policy.maximum_forecast_calibration_error_ppm,
            policy.minimum_liquidity_capacity_ppm,
            policy.minimum_portfolio_risk_capacity_ppm,
            policy.minimum_confidence_ppm,
        ]
        .into_iter()
        .any(|value| value > CONFIDENCE_PARTS_PER_MILLION)
        || policy.minimum_nominal_forecast_coverage_ppm
            > policy.maximum_nominal_forecast_coverage_ppm
        || policy.minimum_realized_forecast_coverage_ppm < MINIMUM_ACTIONABLE_REALIZED_COVERAGE_PPM
        || policy.maximum_forecast_calibration_error_ppm > MAXIMUM_ACTIONABLE_CALIBRATION_ERROR_PPM
        || range_weights.into_iter().any(|weight| weight >= 10_000)
        || !(range_weights[0] > range_weights[1]
            && range_weights[1] > range_weights[2]
            && range_weights[2] > range_weights[3]
            && range_weights[3] > range_weights[4]
            && range_weights[4] > range_weights[5]
            && range_weights[6] > range_weights[7]
            && range_weights[8] > 0)
    {
        return Err(InvestmentProposalError::InvalidPolicy);
    }
    Ok(())
}

fn policy_text(value: &str) -> Result<DecisionText, InvestmentProposalError> {
    DecisionText::try_new(value).map_err(|_| InvestmentProposalError::InvalidPolicy)
}
