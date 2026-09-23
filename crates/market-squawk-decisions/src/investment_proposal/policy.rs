//! Code-owned recommendation policy and policy-weighted reliability semantics.

use std::num::NonZeroU32;

use market_squawk_domain::{BasisPoints, RoundingPolicy};

use crate::DecisionText;

use super::digest::hash_policy;
use super::{
    CONFIDENCE_PARTS_PER_MILLION, InvestmentProposalError, RECOMMENDATION_ASSUMPTION_COUNT,
    RECOMMENDATION_CONFIDENCE_COMPONENT_COUNT, RECOMMENDATION_INVALIDATION_COUNT,
    RECOMMENDATION_LIMITATION_COUNT, RecommendationEvidenceKind, RecommendationPolicyDigest,
};

const PRICE_RANGE_WEIGHT_COUNT: usize = 9;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SECOND;

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
    pub(super) action_zone_semantics_version: NonZeroU32,
    pub(super) horizon_nanos: i64,
    pub(super) proposal_lifetime_nanos: i64,
    pub(super) market_max_age_nanos: i64,
    pub(super) forecast_max_age_nanos: i64,
    pub(super) valuation_max_age_nanos: i64,
    pub(super) backtest_max_age_nanos: i64,
    pub(super) liquidity_max_age_nanos: i64,
    pub(super) portfolio_risk_max_age_nanos: i64,
    pub(super) bullish_threshold: BasisPoints,
    pub(super) bearish_threshold: BasisPoints,
    pub(super) minimum_forecast_outcomes: NonZeroU32,
    pub(super) minimum_nominal_forecast_coverage_ppm: u32,
    pub(super) maximum_nominal_forecast_coverage_ppm: u32,
    pub(super) minimum_backtest_observations: NonZeroU32,
    pub(super) minimum_backtest_trials: NonZeroU32,
    pub(super) minimum_backtest_stability_ppm: u32,
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

/// Closed, versioned semantics used by the deterministic recommendation authority.
///
/// V1 is code-owned: callers can select or recover a supported version, but cannot supply action
/// thresholds, price weights, confidence weights, or narratives.
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
            policy_text("backtest evidence remains point-in-time and cost-adjusted")?,
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
            action_zone_semantics_version: NonZeroU32::MIN,
            horizon_nanos: 365 * NANOS_PER_DAY,
            proposal_lifetime_nanos: 7 * NANOS_PER_DAY,
            market_max_age_nanos: 60 * NANOS_PER_SECOND,
            forecast_max_age_nanos: 7 * NANOS_PER_DAY,
            valuation_max_age_nanos: 30 * NANOS_PER_DAY,
            backtest_max_age_nanos: 180 * NANOS_PER_DAY,
            liquidity_max_age_nanos: 60 * NANOS_PER_SECOND,
            portfolio_risk_max_age_nanos: 5 * 60 * NANOS_PER_SECOND,
            bullish_threshold: BasisPoints::new(1_000),
            bearish_threshold: BasisPoints::new(1_000),
            minimum_forecast_outcomes,
            minimum_nominal_forecast_coverage_ppm: 500_000,
            maximum_nominal_forecast_coverage_ppm: 990_000,
            minimum_backtest_observations,
            minimum_backtest_trials,
            minimum_backtest_stability_ppm: 600_000,
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

    /// Reconstructs a supported code-owned policy and verifies its persisted identity.
    ///
    /// # Errors
    ///
    /// Rejects unsupported versions and mismatched semantic digests.
    pub fn try_recover(
        version: NonZeroU32,
        expected_digest: RecommendationPolicyDigest,
    ) -> Result<Self, InvestmentProposalError> {
        let policy = match version.get() {
            1 => Self::v1()?,
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
            RecommendationEvidenceKind::Backtest => self.semantics.backtest_max_age_nanos,
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
    /// Stability of the cost-adjusted point-in-time backtest.
    BacktestStability,
    /// Evidentiary quality of the current market reference.
    MarketIntegrity,
    /// Spread- and capacity-adjusted liquidity reliability.
    LiquidityCapacity,
    /// Current account-specific portfolio risk capacity.
    PortfolioRiskCapacity,
}

/// One evidence value and fixed policy weight in the aggregate reliability calculation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationConfidenceComponent {
    pub(super) kind: RecommendationConfidenceComponentKind,
    pub(super) value_ppm: u32,
    pub(super) weight_ppm: u32,
}

impl RecommendationConfidenceComponent {
    pub(super) const fn new(
        kind: RecommendationConfidenceComponentKind,
        value_ppm: u32,
        weight_ppm: u32,
    ) -> Self {
        Self {
            kind,
            value_ppm,
            weight_ppm,
        }
    }

    /// Returns the typed evidence-reliability component.
    #[must_use]
    pub const fn kind(self) -> RecommendationConfidenceComponentKind {
        self.kind
    }

    /// Returns the component reliability in parts per million.
    #[must_use]
    pub const fn value_ppm(self) -> u32 {
        self.value_ppm
    }

    /// Returns the code-owned component weight in parts per million.
    #[must_use]
    pub const fn weight_ppm(self) -> u32 {
        self.weight_ppm
    }
}

/// Reproducible policy-weighted evidence reliability.
///
/// Only the forecast-coverage component carries empirical calibration evidence. The aggregate has
/// not been calibrated against realized recommendation outcomes and is not a profit probability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationConfidence {
    pub(super) meaning: RecommendationConfidenceMeaning,
    pub(super) value_ppm: u32,
    pub(super) components:
        [RecommendationConfidenceComponent; RECOMMENDATION_CONFIDENCE_COMPONENT_COUNT],
}

impl RecommendationConfidence {
    /// Returns the closed interpretation of this confidence value.
    #[must_use]
    pub const fn meaning(self) -> RecommendationConfidenceMeaning {
        self.meaning
    }

    /// Returns policy-weighted evidence reliability in parts per million.
    #[must_use]
    pub const fn value_ppm(self) -> u32 {
        self.value_ppm
    }

    /// Returns all six fixed components and code-owned weights.
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
    if policy.horizon_nanos <= 0
        || policy.proposal_lifetime_nanos <= 0
        || [
            policy.market_max_age_nanos,
            policy.forecast_max_age_nanos,
            policy.valuation_max_age_nanos,
            policy.backtest_max_age_nanos,
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
            policy.minimum_nominal_forecast_coverage_ppm,
            policy.maximum_nominal_forecast_coverage_ppm,
            policy.minimum_liquidity_capacity_ppm,
            policy.minimum_portfolio_risk_capacity_ppm,
            policy.minimum_confidence_ppm,
        ]
        .into_iter()
        .any(|value| value > CONFIDENCE_PARTS_PER_MILLION)
        || policy.minimum_nominal_forecast_coverage_ppm
            > policy.maximum_nominal_forecast_coverage_ppm
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
