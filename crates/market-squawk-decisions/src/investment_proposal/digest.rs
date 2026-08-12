//! Canonical SHA-256 commitments for policy, evidence, derivations, and stable identities.

use market_squawk_domain::{
    AccountId, BasisPoints, Currency, DataQuality, DigestAlgorithm, InstrumentId, Money,
    RoundingPolicy, Timestamp,
};
use market_squawk_modeling::ForecastCentralStatistic;
use market_squawk_valuation::ValuationAmountBasis;
use sha2::{Digest as _, Sha256};

use crate::{DecisionContentDigest, TargetPriceCases, TargetPriceRange};

use super::policy::RecommendationPolicySemantics;
use super::*;

struct CanonicalHasher(Sha256);

impl CanonicalHasher {
    fn new(domain: &[u8]) -> Self {
        let mut value = Self(Sha256::new());
        value.bytes(domain);
        value
    }

    fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }

    fn tag(&mut self, value: u8) {
        self.0.update([value]);
    }

    fn bytes(&mut self, value: &[u8]) {
        self.u64(u64::try_from(value.len()).unwrap_or(u64::MAX));
        self.0.update(value);
    }

    fn bool(&mut self, value: bool) {
        self.tag(u8::from(value));
    }

    fn u32(&mut self, value: u32) {
        self.0.update(value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.update(value.to_be_bytes());
    }

    fn i32(&mut self, value: i32) {
        self.0.update(value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.0.update(value.to_be_bytes());
    }

    fn i128(&mut self, value: i128) {
        self.0.update(value.to_be_bytes());
    }

    fn timestamp(&mut self, value: Timestamp) {
        self.i64(value.unix_nanos());
    }

    fn instrument(&mut self, value: InstrumentId) {
        self.0.update(value.as_uuid().as_bytes());
    }

    fn account(&mut self, value: AccountId) {
        self.0.update(value.as_uuid().as_bytes());
    }

    fn currency(&mut self, value: Currency) {
        self.0.update(value.as_str().as_bytes());
    }

    fn money(&mut self, value: Money) {
        self.i128(value.amount().mantissa());
        self.u32(value.amount().scale());
        self.currency(value.currency());
    }

    fn basis_points(&mut self, value: BasisPoints) {
        self.i32(value.get());
    }

    fn content(&mut self, value: DecisionContentDigest) {
        let digest = value.evidence_digest();
        self.tag(match digest.algorithm() {
            DigestAlgorithm::Sha256 => 1,
            DigestAlgorithm::Blake3 => 2,
        });
        self.0.update(digest.bytes());
    }

    fn window(&mut self, value: ProposalEvidenceWindow) {
        self.timestamp(value.observed_at);
        self.timestamp(value.available_at);
        self.timestamp(value.expires_at);
        self.content(value.content_identity);
    }

    fn target_range(&mut self, value: TargetPriceRange) {
        self.money(value.lower());
        self.money(value.upper());
    }

    fn target_cases(&mut self, value: TargetPriceCases) {
        self.money(value.downside());
        self.money(value.base());
        self.money(value.upside());
    }
}

pub(super) fn hash_policy(policy: &RecommendationPolicySemantics) -> [u8; 32] {
    let mut hash = CanonicalHasher::new(b"market-squawk/recommendation-policy/v1");
    hash.u32(policy.version.get());
    hash.u32(policy.action_zone_semantics_version.get());
    hash.i64(policy.horizon_nanos);
    hash.i64(policy.proposal_lifetime_nanos);
    hash.i64(policy.market_max_age_nanos);
    hash.i64(policy.forecast_max_age_nanos);
    hash.i64(policy.valuation_max_age_nanos);
    hash.i64(policy.backtest_max_age_nanos);
    hash.i64(policy.liquidity_max_age_nanos);
    hash.i64(policy.portfolio_risk_max_age_nanos);
    hash.basis_points(policy.bullish_threshold);
    hash.basis_points(policy.bearish_threshold);
    hash.u32(policy.minimum_forecast_outcomes.get());
    hash.u32(policy.minimum_nominal_forecast_coverage_ppm);
    hash.u32(policy.maximum_nominal_forecast_coverage_ppm);
    hash.u32(policy.minimum_backtest_observations.get());
    hash.u32(policy.minimum_backtest_trials.get());
    hash.u32(policy.minimum_backtest_stability_ppm);
    hash.basis_points(policy.minimum_cost_adjusted_return);
    hash.basis_points(policy.maximum_backtest_drawdown);
    hash.basis_points(policy.maximum_liquidity_spread);
    hash.u32(policy.minimum_liquidity_capacity_ppm);
    hash.u32(policy.minimum_portfolio_risk_capacity_ppm);
    hash.u32(policy.minimum_confidence_ppm);
    hash.u32(policy.forecast_base_weight_bps);
    hash.u32(policy.valuation_weight_bps);
    for weight in policy.confidence_weights_ppm {
        hash.u32(weight);
    }
    for weight in policy.price_range_weights_bps {
        hash.u32(weight);
    }
    hash.u32(policy.price_scale);
    hash.tag(rounding_tag(policy.rounding_policy));
    hash.tag(proposal_time_benchmark_availability_tag(
        policy.proposal_time_benchmark_availability,
    ));
    hash.tag(action_specific_cost_availability_tag(
        policy.action_specific_cost_availability,
    ));
    for text in &policy.assumptions {
        hash.bytes(text.as_str().as_bytes());
    }
    for text in &policy.invalidation_conditions {
        hash.bytes(text.as_str().as_bytes());
    }
    for text in &policy.limitations {
        hash.bytes(text.as_str().as_bytes());
    }
    hash.finish()
}

pub(super) fn hash_evidence(evidence: &InvestmentAnalysisEvidence) -> [u8; 32] {
    let mut hash = CanonicalHasher::new(b"market-squawk/investment-analysis-evidence/v3");
    hash.instrument(evidence.instrument_id);
    hash.currency(evidence.currency);
    hash.account(evidence.account_id);
    hash.timestamp(evidence.as_of);

    match evidence.market {
        Some(value) => {
            hash.tag(1);
            hash.instrument(value.instrument_id);
            hash.money(value.price);
            hash.tag(data_quality_tag(value.quality));
            hash.tag(market_reference_price_kind_tag(value.price_kind));
            hash.tag(market_reference_adjustment_basis_tag(
                value.adjustment_basis,
            ));
            hash.content(value.selection_receipt_identity);
            hash.content(value.selected_observation_identity);
            hash.window(value.window);
        }
        None => hash.tag(0),
    }
    match evidence.price_forecast {
        Some(value) => {
            hash.tag(1);
            hash.instrument(value.instrument_id);
            hash.target_cases(value.cases);
            hash.target_range(value.ranges.downside);
            hash.target_range(value.ranges.base);
            hash.target_range(value.ranges.upside);
            hash.timestamp(value.horizon_at);
            match value.expected_terminal_statistic {
                Some(statistic) => {
                    hash.tag(1);
                    hash.tag(expected_terminal_statistic_tag(statistic));
                }
                None => hash.tag(0),
            }
            match value.expected_terminal_price {
                Some(price) => {
                    hash.tag(1);
                    hash.money(price);
                }
                None => hash.tag(0),
            }
            match value.expected_terminal_horizon_at {
                Some(horizon_at) => {
                    hash.tag(1);
                    hash.timestamp(horizon_at);
                }
                None => hash.tag(0),
            }
            match value.expected_terminal_statistic_identity {
                Some(identity) => {
                    hash.tag(1);
                    hash.content(identity);
                }
                None => hash.tag(0),
            }
            hash.0.update(value.vintage_id.0);
            hash.content(value.output_binding_identity);
            hash.content(value.calibration_identity);
            hash.content(value.outcome_set_identity);
            hash.u32(value.calibration.nominal_coverage_ppm);
            hash.u32(value.calibration.realized_coverage_ppm);
            hash.u32(value.calibration.completed_outcomes.get());
            hash.window(value.window);
        }
        None => hash.tag(0),
    }
    match evidence.valuation {
        Some(value) => {
            hash.tag(1);
            hash.instrument(value.instrument_id);
            hash.money(value.fair_value);
            hash.tag(valuation_amount_basis_tag(value.basis));
            hash.timestamp(value.horizon_at);
            hash.0.update(value.measurement_id.bytes());
            hash.0.update(value.classification_decision_id.bytes());
            hash.0.update(value.selection_receipt_hash.bytes());
            hash.window(value.window);
        }
        None => hash.tag(0),
    }
    match evidence.backtest {
        Some(value) => {
            hash.tag(1);
            hash.instrument(value.instrument_id);
            hash.currency(value.currency);
            hash.i64(value.outcome_horizon_nanos);
            hash.basis_points(value.net_return);
            hash.basis_points(value.max_drawdown);
            hash.basis_points(value.fee_basis_points);
            hash.basis_points(value.slippage_basis_points);
            hash.basis_points(value.maximum_random_slippage_basis_points);
            hash.u32(value.observations.get());
            hash.u32(value.trials.get());
            hash.u32(value.stability_ppm);
            hash.timestamp(value.simulation_cutoff_at);
            hash.content(value.dataset_identity);
            hash.content(value.command_identity);
            hash.content(value.terminal_identity);
            hash.content(value.report_identity);
            hash.content(value.cohort_identity);
            hash.content(value.cost_model_identity);
            hash.window(value.window);
        }
        None => hash.tag(0),
    }
    match evidence.liquidity {
        Some(value) => {
            hash.tag(1);
            hash.instrument(value.instrument_id);
            hash.currency(value.currency);
            hash.basis_points(value.quoted_spread);
            hash.u32(value.capacity_ppm);
            hash.tag(data_quality_tag(value.quality));
            hash.content(value.assessment_identity);
            hash.window(value.window);
        }
        None => hash.tag(0),
    }
    match &evidence.portfolio_risk {
        Some(value) => {
            hash.tag(1);
            hash.instrument(value.instrument_id);
            hash.account(value.account_id);
            hash.currency(value.currency);
            hash.0.update(value.portfolio_revision.bytes());
            match value.position_state {
                PortfolioPositionState::NoPosition => hash.tag(0),
                PortfolioPositionState::Position {
                    add_allowed,
                    trim_allowed,
                    exit_allowed,
                } => {
                    hash.tag(1);
                    hash.bool(add_allowed);
                    hash.bool(trim_allowed);
                    hash.bool(exit_allowed);
                }
            }
            hash.u32(value.risk_capacity_ppm);
            hash.content(value.risk_report_identity);
            hash.window(value.window);
        }
        None => hash.tag(0),
    }
    hash.finish()
}

pub(super) fn hash_analysis(
    policy: RecommendationPolicyDigest,
    evidence: RecommendationEvidenceDigest,
    instrument: InstrumentId,
    account: AccountId,
    as_of: Timestamp,
) -> [u8; 32] {
    let mut hash = CanonicalHasher::new(b"market-squawk/investment-analysis-id/v1");
    hash.0.update(policy.0);
    hash.0.update(evidence.0);
    hash.instrument(instrument);
    hash.account(account);
    hash.timestamp(as_of);
    hash.finish()
}

pub(super) fn hash_generated_derivation(
    analysis_id: InvestmentAnalysisId,
    action: RecommendationAction,
    ladder: GeneratedPriceLadder,
    confidence: RecommendationConfidence,
    horizon_at: Timestamp,
    expires_at: Timestamp,
) -> [u8; 32] {
    let mut hash = CanonicalHasher::new(b"market-squawk/generated-investment-proposal/v1");
    hash.0.update(analysis_id.0);
    hash.tag(action_tag(action));
    hash_ladder(&mut hash, ladder);
    hash_confidence(&mut hash, confidence);
    hash.timestamp(horizon_at);
    hash.timestamp(expires_at);
    hash.tag(0);
    hash.finish()
}

pub(super) fn hash_no_action_derivation(
    analysis_id: InvestmentAnalysisId,
    reason: NoActionReason,
    invalidators: &[ProposalInvalidator],
    confidence: RecommendationConfidence,
    horizon_at: Timestamp,
    expires_at: Timestamp,
) -> [u8; 32] {
    let mut hash = CanonicalHasher::new(b"market-squawk/no-action-investment-proposal/v1");
    hash.0.update(analysis_id.0);
    hash.tag(no_action_reason_tag(reason));
    hash.u64(u64::try_from(invalidators.len()).unwrap_or(u64::MAX));
    for invalidator in invalidators {
        hash.tag(invalidator_tag(*invalidator));
    }
    hash_confidence(&mut hash, confidence);
    hash.timestamp(horizon_at);
    hash.timestamp(expires_at);
    hash.tag(0);
    hash.finish()
}

pub(super) fn hash_proposal_id(
    analysis_id: InvestmentAnalysisId,
    derivation_digest: RecommendationDerivationDigest,
) -> [u8; 32] {
    let mut hash = CanonicalHasher::new(b"market-squawk/investment-proposal-id/v1");
    hash.0.update(analysis_id.0);
    hash.0.update(derivation_digest.0);
    hash.finish()
}

fn hash_ladder(hash: &mut CanonicalHasher, ladder: GeneratedPriceLadder) {
    hash.target_cases(ladder.cases);
    hash.target_range(ladder.downside_range);
    hash.target_range(ladder.base_range);
    hash.target_range(ladder.upside_range);
    hash.target_range(ladder.entry_range);
    hash.target_range(ladder.add_range);
    hash.money(ladder.add_case);
    hash.target_range(ladder.trim_range);
    hash.target_range(ladder.exit_range);
}

fn hash_confidence(hash: &mut CanonicalHasher, confidence: RecommendationConfidence) {
    hash.tag(match confidence.meaning {
        RecommendationConfidenceMeaning::PolicyWeightedEvidenceReliabilityV1 => 1,
    });
    hash.u32(confidence.value_ppm);
    for component in confidence.components {
        hash.tag(confidence_component_tag(component.kind));
        hash.u32(component.value_ppm);
        hash.u32(component.weight_ppm);
    }
}

const fn rounding_tag(value: RoundingPolicy) -> u8 {
    match value {
        RoundingPolicy::NearestEven => 1,
        RoundingPolicy::AwayFromZero => 2,
        RoundingPolicy::TowardZero => 3,
        RoundingPolicy::Floor => 4,
        RoundingPolicy::Ceiling => 5,
    }
}

const fn proposal_time_benchmark_availability_tag(value: ProposalTimeBenchmarkAvailability) -> u8 {
    match value {
        ProposalTimeBenchmarkAvailability::UnavailableByPolicyV1 => 1,
    }
}

const fn action_specific_cost_availability_tag(value: ActionSpecificCostAvailability) -> u8 {
    match value {
        ActionSpecificCostAvailability::UnavailableByPolicyV1 => 1,
    }
}

const fn expected_terminal_statistic_tag(value: ForecastCentralStatistic) -> u8 {
    match value {
        ForecastCentralStatistic::ModelEstimatedConditionalMean => 1,
        ForecastCentralStatistic::Unavailable => 2,
    }
}

const fn market_reference_price_kind_tag(value: MarketReferencePriceKind) -> u8 {
    match value {
        MarketReferencePriceKind::LastTrade => 1,
        MarketReferencePriceKind::CheckedBidAskMidpoint => 2,
    }
}

const fn market_reference_adjustment_basis_tag(value: MarketReferenceAdjustmentBasis) -> u8 {
    match value {
        MarketReferenceAdjustmentBasis::UnadjustedSpot => 1,
    }
}

const fn valuation_amount_basis_tag(value: ValuationAmountBasis) -> u8 {
    match value {
        ValuationAmountBasis::PerInstrumentUnit => 1,
        ValuationAmountBasis::ReportingEntityTotal => 2,
        ValuationAmountBasis::PositionTotal => 3,
    }
}

const fn data_quality_tag(value: DataQuality) -> u8 {
    match value {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}

const fn action_tag(value: RecommendationAction) -> u8 {
    match value {
        RecommendationAction::Buy => 1,
        RecommendationAction::Add => 2,
        RecommendationAction::Hold => 3,
        RecommendationAction::Trim => 4,
        RecommendationAction::Sell => 5,
    }
}

const fn confidence_component_tag(value: RecommendationConfidenceComponentKind) -> u8 {
    match value {
        RecommendationConfidenceComponentKind::ForecastCalibration => 1,
        RecommendationConfidenceComponentKind::ValuationAgreement => 2,
        RecommendationConfidenceComponentKind::BacktestStability => 3,
        RecommendationConfidenceComponentKind::MarketIntegrity => 4,
        RecommendationConfidenceComponentKind::LiquidityCapacity => 5,
        RecommendationConfidenceComponentKind::PortfolioRiskCapacity => 6,
    }
}

const fn no_action_reason_tag(value: NoActionReason) -> u8 {
    match value {
        NoActionReason::ConflictingForecastAndValuation => 1,
        NoActionReason::BacktestBelowPolicy => 2,
        NoActionReason::LiquidityBelowPolicy => 3,
        NoActionReason::PortfolioRiskBelowPolicy => 4,
        NoActionReason::ConfidenceBelowPolicy => 5,
        NoActionReason::PositionStateNotActionable => 6,
        NoActionReason::GeneratedPriceOrderCollapsed => 7,
    }
}

const fn invalidator_tag(value: ProposalInvalidator) -> u8 {
    match value {
        ProposalInvalidator::ForecastValuationConflict => 1,
        ProposalInvalidator::BacktestPolicyBreach => 2,
        ProposalInvalidator::LiquidityPolicyBreach => 3,
        ProposalInvalidator::PortfolioRiskPolicyBreach => 4,
        ProposalInvalidator::ConfidencePolicyBreach => 5,
        ProposalInvalidator::PositionStateIncompatible => 6,
        ProposalInvalidator::GeneratedPriceOrderCollapsed => 7,
    }
}
