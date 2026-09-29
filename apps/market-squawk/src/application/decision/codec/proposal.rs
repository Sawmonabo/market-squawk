use std::{
    num::{NonZeroU32, NonZeroU64},
    str::FromStr as _,
};

use market_squawk_analytics::{
    FeatureImplementationDigest, HarmonicDirection, HarmonicPatternKind, HarmonicPatternQuality,
};
use market_squawk_decisions::{
    ChronologicalOutOfSampleEvidence, CostAdjustedBacktestEvidence, FinancialModelEvidence,
    FinancialModelMacroAssumptions, FinancialModelValueRange, ForecastCalibrationSummary,
    ForecastPriceRanges, HarmonicPatternEvidenceReceipt, InvestmentAnalysisEvidence,
    InvestmentAnalysisEvidenceInput, InvestmentAnalysisId, InvestmentProbabilityEvidence,
    InvestmentProposalAuthority, InvestmentProposalDecision, InvestmentProposalId,
    LiquidityEvidence, MacroRateMaturity, MacroRateReferenceEvidence,
    MarketReferenceAdjustmentBasis, MarketReferenceEvidence, MarketReferencePriceKind,
    PortfolioPositionState, PortfolioRiskEvidence, PriceForecastEvidence,
    ProbabilityCalibrationSummary, ProbabilityEventEvidence, ProbabilityEventKind,
    ProbabilityForecastEvidence, ProbabilityForecastEvidenceRecord, ProbabilityForecastReference,
    ProbabilityReliabilityEvidence, ProbabilityUnavailableReason, ProposalEvidenceWindow,
    ProposalForecastVintageId, ProposalUnavailableReason, RecommendationDerivationDigest,
    RecommendationEvidenceKind, RecommendationPolicy, RecommendationPolicyDigest,
    RecommendationPolicyParameters, RecommendationStudyQualification,
    SelectedCandidateAnalysisEvidence, TargetPriceCases, TargetPriceRange, ValuationEvidence,
};
use market_squawk_domain::{
    AccountId, BasisPoints, Currency, DataQuality, EvidenceDigest, HistoricalStudyBasis,
    HistoricalStudyLimitation, InstrumentId, Money, PriceTicks, RoundingPolicy, Timestamp,
};
use market_squawk_modeling::{
    CalibrationWindow, ForecastCentralStatistic, ForecastTargetMeaning, ForecastValue,
};
use market_squawk_portfolio::PortfolioRevisionToken;
use market_squawk_valuation::{
    AutomaticValuationAssumption, AutomaticValuationAssumptionKind, AutomaticValuationMethod,
    DecisionId, FairValueSelectionReceiptHash, MeasurementId, ValuationAmountBasis,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};

use super::super::DecisionApplicationError;
use super::common::content_digest;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InvestmentProposalWire {
    policy: RecommendationPolicyWire,
    evidence: InvestmentAnalysisEvidenceWire,
    outcome: InvestmentProposalOutcomeWire,
}

impl TryFrom<&InvestmentProposalDecision> for InvestmentProposalWire {
    type Error = DecisionApplicationError;

    fn try_from(value: &InvestmentProposalDecision) -> Result<Self, Self::Error> {
        let outcome = match value {
            InvestmentProposalDecision::Generated(proposal) => {
                InvestmentProposalOutcomeWire::Generated(ProposalIdentityWire {
                    analysis_id: proposal.analysis_id().bytes(),
                    proposal_id: proposal.proposal_id().bytes(),
                    derivation_digest: proposal.derivation_digest().bytes(),
                })
            }
            InvestmentProposalDecision::NoAction(proposal) => {
                InvestmentProposalOutcomeWire::NoAction(ProposalIdentityWire {
                    analysis_id: proposal.analysis_id().bytes(),
                    proposal_id: proposal.proposal_id().bytes(),
                    derivation_digest: proposal.derivation_digest().bytes(),
                })
            }
            InvestmentProposalDecision::Unavailable(analysis) => {
                InvestmentProposalOutcomeWire::Unavailable(UnavailableIdentityWire {
                    analysis_id: analysis.analysis_id().bytes(),
                    reason: analysis.reason().into(),
                })
            }
        };
        Ok(Self {
            policy: value.policy().into(),
            evidence: InvestmentAnalysisEvidenceWire::try_from(value.evidence())?,
            outcome,
        })
    }
}

impl InvestmentProposalWire {
    pub(super) async fn decode_with_replay(
        self,
        selected_candidate: Option<SelectedCandidateAnalysisEvidence>,
        canonical_request: &[u8],
        replay: &super::super::current_share::CurrentShareReplayCapability,
        context: &market_squawk_services::RequestContext,
    ) -> Result<InvestmentProposalDecision, DecisionApplicationError> {
        let Self { policy, evidence, outcome } = self;
        let policy = policy.decode()?;
        let mut evidence = evidence.decode_with_replay(canonical_request, &policy, replay, context).await?;
        if let Some(candidate) = selected_candidate {
            evidence = evidence.try_with_selected_candidate(candidate).map_err(invalid_state)?;
        }
        Self::decode_with_evidence(outcome, evidence, policy)
    }

    pub(super) fn analysis_id(&self) -> Result<InvestmentAnalysisId, DecisionApplicationError> {
        InvestmentAnalysisId::try_from_bytes(self.outcome.analysis_id()).map_err(invalid_state)
    }

    pub(super) fn has_current_share_projection(&self) -> bool {
        self.evidence.current_share_projection.0.is_some()
    }

    pub(super) fn key(&self) -> Result<String, DecisionApplicationError> {
        analysis_key(self.outcome.analysis_id())
    }

    pub(super) fn decode(self) -> Result<InvestmentProposalDecision, DecisionApplicationError> {
        let Self {
            policy,
            evidence,
            outcome,
        } = self;
        Self::decode_with_evidence(outcome, evidence.decode()?, policy.decode()?)
    }

    pub(super) fn decode_with_selected_candidate(
        self,
        selected_candidate: SelectedCandidateAnalysisEvidence,
    ) -> Result<InvestmentProposalDecision, DecisionApplicationError> {
        let Self {
            policy,
            evidence,
            outcome,
        } = self;
        let evidence = evidence
            .decode()?
            .try_with_selected_candidate(selected_candidate)
            .map_err(invalid_state)?;
        Self::decode_with_evidence(outcome, evidence, policy.decode()?)
    }

    fn decode_with_evidence(
        outcome: InvestmentProposalOutcomeWire,
        evidence: InvestmentAnalysisEvidence,
        policy: RecommendationPolicy,
    ) -> Result<InvestmentProposalDecision, DecisionApplicationError> {
        match outcome {
            InvestmentProposalOutcomeWire::Generated(identity) => {
                Ok(InvestmentProposalDecision::Generated(
                    InvestmentProposalAuthority::try_recover_generated(
                        evidence,
                        policy,
                        InvestmentAnalysisId::try_from_bytes(identity.analysis_id)
                            .map_err(invalid_state)?,
                        RecommendationDerivationDigest::try_from_bytes(identity.derivation_digest)
                            .map_err(invalid_state)?,
                        InvestmentProposalId::try_from_bytes(identity.proposal_id)
                            .map_err(invalid_state)?,
                    )
                    .map_err(invalid_state)?,
                ))
            }
            InvestmentProposalOutcomeWire::NoAction(identity) => {
                Ok(InvestmentProposalDecision::NoAction(
                    InvestmentProposalAuthority::try_recover_no_action(
                        evidence,
                        policy,
                        InvestmentAnalysisId::try_from_bytes(identity.analysis_id)
                            .map_err(invalid_state)?,
                        RecommendationDerivationDigest::try_from_bytes(identity.derivation_digest)
                            .map_err(invalid_state)?,
                        InvestmentProposalId::try_from_bytes(identity.proposal_id)
                            .map_err(invalid_state)?,
                    )
                    .map_err(invalid_state)?,
                ))
            }
            InvestmentProposalOutcomeWire::Unavailable(identity) => {
                Ok(InvestmentProposalDecision::Unavailable(
                    InvestmentProposalAuthority::try_recover_unavailable(
                        evidence,
                        policy,
                        InvestmentAnalysisId::try_from_bytes(identity.analysis_id)
                            .map_err(invalid_state)?,
                        identity.reason.decode()?,
                    )
                    .map_err(invalid_state)?,
                ))
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RecommendationPolicyWire {
    version: u32,
    digest: [u8; 32],
    parameters: RecommendationPolicyParametersWire,
}

impl From<&RecommendationPolicy> for RecommendationPolicyWire {
    fn from(value: &RecommendationPolicy) -> Self {
        Self {
            version: value.version().get(),
            digest: value.digest().bytes(),
            parameters: value.parameters().into(),
        }
    }
}

impl RecommendationPolicyWire {
    fn decode(self) -> Result<RecommendationPolicy, DecisionApplicationError> {
        RecommendationPolicy::try_recover(
            NonZeroU32::new(self.version)
                .ok_or(DecisionApplicationError::InvalidPersistentState)?,
            self.parameters.into(),
            RecommendationPolicyDigest::try_from_bytes(self.digest).map_err(invalid_state)?,
        )
        .map_err(invalid_state)
    }
}

/// Exact policy payload shared by persistence and stateless backend validation.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecommendationPolicyParametersWire {
    pub allow_retrospective_studies: bool,
    #[serde(with = "exact_nanoseconds")]
    pub proposal_lifetime_nanos: i64,
    #[serde(with = "exact_nanoseconds")]
    pub market_max_age_nanos: i64,
    #[serde(with = "exact_nanoseconds")]
    pub forecast_max_age_nanos: i64,
    #[serde(with = "exact_nanoseconds")]
    pub valuation_max_age_nanos: i64,
    #[serde(with = "exact_nanoseconds")]
    pub financial_model_max_age_nanos: i64,
    #[serde(with = "exact_nanoseconds")]
    pub backtest_max_age_nanos: i64,
    #[serde(with = "exact_nanoseconds")]
    pub out_of_sample_max_age_nanos: i64,
    #[serde(with = "exact_nanoseconds")]
    pub harmonic_pattern_max_age_nanos: i64,
    #[serde(with = "exact_nanoseconds")]
    pub liquidity_max_age_nanos: i64,
    #[serde(with = "exact_nanoseconds")]
    pub portfolio_risk_max_age_nanos: i64,
    pub bullish_threshold: BasisPoints,
    pub bearish_threshold: BasisPoints,
    pub minimum_forecast_outcomes: NonZeroU32,
    pub minimum_nominal_forecast_coverage_ppm: u32,
    pub maximum_nominal_forecast_coverage_ppm: u32,
    pub minimum_realized_forecast_coverage_ppm: u32,
    pub maximum_forecast_calibration_error_ppm: u32,
    pub minimum_backtest_observations: NonZeroU32,
    pub minimum_backtest_trials: NonZeroU32,
    pub minimum_backtest_stability_ppm: u32,
    pub minimum_oos_completion_coverage_ppm: u32,
    pub minimum_cost_adjusted_return: BasisPoints,
    pub maximum_backtest_drawdown: BasisPoints,
    pub maximum_liquidity_spread: BasisPoints,
    pub minimum_liquidity_capacity_ppm: u32,
    pub minimum_portfolio_risk_capacity_ppm: u32,
    pub minimum_confidence_ppm: u32,
    pub forecast_base_weight_bps: u32,
    pub valuation_weight_bps: u32,
    pub confidence_weights_ppm: [u32; 6],
    pub price_range_weights_bps: [u32; 9],
    pub price_scale: u32,
    pub rounding_policy: RoundingPolicy,
}

impl From<RecommendationPolicyParameters> for RecommendationPolicyParametersWire {
    fn from(value: RecommendationPolicyParameters) -> Self {
        Self {
            allow_retrospective_studies: value.allow_retrospective_studies,
            proposal_lifetime_nanos: value.proposal_lifetime_nanos,
            market_max_age_nanos: value.market_max_age_nanos,
            forecast_max_age_nanos: value.forecast_max_age_nanos,
            valuation_max_age_nanos: value.valuation_max_age_nanos,
            financial_model_max_age_nanos: value.financial_model_max_age_nanos,
            backtest_max_age_nanos: value.backtest_max_age_nanos,
            out_of_sample_max_age_nanos: value.out_of_sample_max_age_nanos,
            harmonic_pattern_max_age_nanos: value.harmonic_pattern_max_age_nanos,
            liquidity_max_age_nanos: value.liquidity_max_age_nanos,
            portfolio_risk_max_age_nanos: value.portfolio_risk_max_age_nanos,
            bullish_threshold: value.bullish_threshold,
            bearish_threshold: value.bearish_threshold,
            minimum_forecast_outcomes: value.minimum_forecast_outcomes,
            minimum_nominal_forecast_coverage_ppm: value.minimum_nominal_forecast_coverage_ppm,
            maximum_nominal_forecast_coverage_ppm: value.maximum_nominal_forecast_coverage_ppm,
            minimum_realized_forecast_coverage_ppm: value.minimum_realized_forecast_coverage_ppm,
            maximum_forecast_calibration_error_ppm: value.maximum_forecast_calibration_error_ppm,
            minimum_backtest_observations: value.minimum_backtest_observations,
            minimum_backtest_trials: value.minimum_backtest_trials,
            minimum_backtest_stability_ppm: value.minimum_backtest_stability_ppm,
            minimum_oos_completion_coverage_ppm: value.minimum_oos_completion_coverage_ppm,
            minimum_cost_adjusted_return: value.minimum_cost_adjusted_return,
            maximum_backtest_drawdown: value.maximum_backtest_drawdown,
            maximum_liquidity_spread: value.maximum_liquidity_spread,
            minimum_liquidity_capacity_ppm: value.minimum_liquidity_capacity_ppm,
            minimum_portfolio_risk_capacity_ppm: value.minimum_portfolio_risk_capacity_ppm,
            minimum_confidence_ppm: value.minimum_confidence_ppm,
            forecast_base_weight_bps: value.forecast_base_weight_bps,
            valuation_weight_bps: value.valuation_weight_bps,
            confidence_weights_ppm: value.confidence_weights_ppm,
            price_range_weights_bps: value.price_range_weights_bps,
            price_scale: value.price_scale,
            rounding_policy: value.rounding_policy,
        }
    }
}

impl From<RecommendationPolicyParametersWire> for RecommendationPolicyParameters {
    fn from(value: RecommendationPolicyParametersWire) -> Self {
        Self {
            allow_retrospective_studies: value.allow_retrospective_studies,
            proposal_lifetime_nanos: value.proposal_lifetime_nanos,
            market_max_age_nanos: value.market_max_age_nanos,
            forecast_max_age_nanos: value.forecast_max_age_nanos,
            valuation_max_age_nanos: value.valuation_max_age_nanos,
            financial_model_max_age_nanos: value.financial_model_max_age_nanos,
            backtest_max_age_nanos: value.backtest_max_age_nanos,
            out_of_sample_max_age_nanos: value.out_of_sample_max_age_nanos,
            harmonic_pattern_max_age_nanos: value.harmonic_pattern_max_age_nanos,
            liquidity_max_age_nanos: value.liquidity_max_age_nanos,
            portfolio_risk_max_age_nanos: value.portfolio_risk_max_age_nanos,
            bullish_threshold: value.bullish_threshold,
            bearish_threshold: value.bearish_threshold,
            minimum_forecast_outcomes: value.minimum_forecast_outcomes,
            minimum_nominal_forecast_coverage_ppm: value.minimum_nominal_forecast_coverage_ppm,
            maximum_nominal_forecast_coverage_ppm: value.maximum_nominal_forecast_coverage_ppm,
            minimum_realized_forecast_coverage_ppm: value.minimum_realized_forecast_coverage_ppm,
            maximum_forecast_calibration_error_ppm: value.maximum_forecast_calibration_error_ppm,
            minimum_backtest_observations: value.minimum_backtest_observations,
            minimum_backtest_trials: value.minimum_backtest_trials,
            minimum_backtest_stability_ppm: value.minimum_backtest_stability_ppm,
            minimum_oos_completion_coverage_ppm: value.minimum_oos_completion_coverage_ppm,
            minimum_cost_adjusted_return: value.minimum_cost_adjusted_return,
            maximum_backtest_drawdown: value.maximum_backtest_drawdown,
            maximum_liquidity_spread: value.maximum_liquidity_spread,
            minimum_liquidity_capacity_ppm: value.minimum_liquidity_capacity_ppm,
            minimum_portfolio_risk_capacity_ppm: value.minimum_portfolio_risk_capacity_ppm,
            minimum_confidence_ppm: value.minimum_confidence_ppm,
            forecast_base_weight_bps: value.forecast_base_weight_bps,
            valuation_weight_bps: value.valuation_weight_bps,
            confidence_weights_ppm: value.confidence_weights_ppm,
            price_range_weights_bps: value.price_range_weights_bps,
            price_scale: value.price_scale,
            rounding_policy: value.rounding_policy,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum InvestmentProposalOutcomeWire {
    Generated(ProposalIdentityWire),
    NoAction(ProposalIdentityWire),
    Unavailable(UnavailableIdentityWire),
}

impl InvestmentProposalOutcomeWire {
    const fn analysis_id(&self) -> [u8; 32] {
        match self {
            Self::Generated(identity) | Self::NoAction(identity) => identity.analysis_id,
            Self::Unavailable(identity) => identity.analysis_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ProposalIdentityWire {
    analysis_id: [u8; 32],
    proposal_id: [u8; 32],
    derivation_digest: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct UnavailableIdentityWire {
    analysis_id: [u8; 32],
    reason: ProposalUnavailableReasonWire,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct InvestmentAnalysisEvidenceWire {
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    current_share_projection: RequiredOption<super::super::current_share::CurrentShareReplayRecipe>,
    // Every saved analysis retains a record, including an explicit unavailable selection.
    benchmark_comparison: BenchmarkComparisonWire,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    forecast_chart: RequiredOption<ForecastChartWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    probabilities: RequiredOption<InvestmentProbabilityWire>,
    instrument_id: InstrumentId,
    currency: Currency,
    account_id: AccountId,
    as_of: Timestamp,
    admitted_at: Timestamp,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    market: RequiredOption<MarketReferenceEvidenceWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    price_forecast: RequiredOption<PriceForecastEvidenceWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    valuation: RequiredOption<ValuationEvidenceWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    financial_model: RequiredOption<FinancialModelEvidenceWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    backtest: RequiredOption<CostAdjustedBacktestEvidenceWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    out_of_sample: RequiredOption<ChronologicalOutOfSampleEvidenceWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    harmonic_pattern: RequiredOption<HarmonicPatternEvidenceWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    harmonic_history: RequiredOption<super::harmonic_history::HarmonicHistoryWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    liquidity: RequiredOption<LiquidityEvidenceWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    portfolio_risk: RequiredOption<PortfolioRiskEvidenceWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    valuation_method_set: RequiredOption<super::valuation_audit::MethodSetWire>,
}

impl TryFrom<&InvestmentAnalysisEvidence> for InvestmentAnalysisEvidenceWire {
    type Error = DecisionApplicationError;

    fn try_from(value: &InvestmentAnalysisEvidence) -> Result<Self, Self::Error> {
        let comparison = value
            .benchmark_comparison()
            .ok_or(DecisionApplicationError::InvalidPersistentState)?;
        // Validate the opaque application recipe before it can enter the durable journal.
        crate::application::saved_benchmark::SavedBenchmarkComparison::decode(comparison)
            .map_err(invalid_state)?;
        if let Some(chart) = value.forecast_chart() {
            crate::application::model::forecast::SavedForecastChart::decode(chart).map_err(invalid_state)?;
        }
        let projection = value.current_share_projection();
        Ok(Self {
            current_share_projection: RequiredOption(projection.map(Into::into)),
            forecast_chart: RequiredOption(value.forecast_chart().map(Into::into)),
            benchmark_comparison: comparison.into(),
            probabilities: RequiredOption(value.probabilities().map(Into::into)),
            instrument_id: value.instrument_id(),
            currency: value.currency(),
            account_id: value.account_id(),
            as_of: value.as_of(),
            admitted_at: value.admitted_at(),
            market: RequiredOption(value.market().map(Into::into)),
            price_forecast: RequiredOption(projection.map(|proof| PriceForecastEvidenceWire::from(&proof.original_forecast()))
                .or_else(|| value.price_forecast().map(Into::into))),
            valuation: RequiredOption(projection.map(|proof| ValuationEvidenceWire::from(&proof.original_valuation()))
                .or_else(|| value.valuation().map(Into::into))),
            financial_model: RequiredOption(projection.map(|proof| FinancialModelEvidenceWire::from(proof.original_financial_model()))
                .or_else(|| value.financial_model().map(Into::into))),
            backtest: RequiredOption(value.backtest().map(Into::into)),
            out_of_sample: RequiredOption(value.out_of_sample().map(Into::into)),
            harmonic_pattern: RequiredOption(value.harmonic_pattern().map(Into::into)),
            harmonic_history: RequiredOption(value.harmonic_history().map(Into::into)),
            liquidity: RequiredOption(value.liquidity().map(Into::into)),
            portfolio_risk: RequiredOption(value.portfolio_risk().map(Into::into)),
            valuation_method_set: RequiredOption(value.valuation_method_set().map(Into::into)),
        })
    }
}

impl InvestmentAnalysisEvidenceWire {
    fn decode(self) -> Result<InvestmentAnalysisEvidence, DecisionApplicationError> {
        if self.current_share_projection.0.is_some() {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        self.decode_original()
    }

    async fn decode_with_replay(
        self,
        canonical_request: &[u8],
        policy: &RecommendationPolicy,
        replay: &super::super::current_share::CurrentShareReplayCapability,
        context: &market_squawk_services::RequestContext,
    ) -> Result<InvestmentAnalysisEvidence, DecisionApplicationError> {
        let recipe = self.current_share_projection.0.clone();
        let original = self.decode_original()?;
        match recipe {
            Some(recipe) => replay.replay(original, canonical_request, policy, &recipe, context).await
                .map_err(|error| match error {
                    market_squawk_services::ServiceError::InvalidRequest
                    | market_squawk_services::ServiceError::InvalidResult => DecisionApplicationError::InvalidPersistentState,
                    market_squawk_services::ServiceError::ResourceExhausted => DecisionApplicationError::Capacity,
                    market_squawk_services::ServiceError::Internal => DecisionApplicationError::Persistence,
                    market_squawk_services::ServiceError::Cancelled
                    | market_squawk_services::ServiceError::DeadlineExceeded => DecisionApplicationError::Unavailable,
                    market_squawk_services::ServiceError::Unavailable
                    | market_squawk_services::ServiceError::NotFound
                    | market_squawk_services::ServiceError::Unauthorized => DecisionApplicationError::Unavailable,
                }),
            None => Ok(original),
        }
    }

    fn decode_original(self) -> Result<InvestmentAnalysisEvidence, DecisionApplicationError> {
        let evidence = InvestmentAnalysisEvidence::new(InvestmentAnalysisEvidenceInput {
            instrument_id: self.instrument_id,
            currency: self.currency,
            account_id: self.account_id,
            as_of: self.as_of,
            admitted_at: self.admitted_at,
            market: self
                .market
                .0
                .map(MarketReferenceEvidenceWire::decode)
                .transpose()?,
            price_forecast: self
                .price_forecast
                .0
                .map(PriceForecastEvidenceWire::decode)
                .transpose()?,
            valuation: self
                .valuation
                .0
                .map(ValuationEvidenceWire::decode)
                .transpose()?,
            financial_model: self
                .financial_model
                .0
                .map(FinancialModelEvidenceWire::decode)
                .transpose()?,
            backtest: self
                .backtest
                .0
                .map(CostAdjustedBacktestEvidenceWire::decode)
                .transpose()?,
            out_of_sample: self
                .out_of_sample
                .0
                .map(ChronologicalOutOfSampleEvidenceWire::decode)
                .transpose()?,
            harmonic_pattern: self
                .harmonic_pattern
                .0
                .map(HarmonicPatternEvidenceWire::decode)
                .transpose()?,
            liquidity: self
                .liquidity
                .0
                .map(LiquidityEvidenceWire::decode)
                .transpose()?,
            portfolio_risk: self
                .portfolio_risk
                .0
                .map(PortfolioRiskEvidenceWire::decode)
                .transpose()?,
        });
        let evidence = match self.forecast_chart.0 {
            Some(chart) => {
                let chart = chart.decode(&evidence)?;
                evidence.try_with_forecast_chart(chart).map_err(invalid_state)?
            }
            None => evidence,
        };
        let comparison = self.benchmark_comparison.decode(&evidence)?;
        let evidence = evidence
            .try_with_benchmark_comparison(comparison)
            .map_err(invalid_state)?;
        let evidence = match self.probabilities.0 {
            Some(group) => evidence
                .try_with_probabilities(group.decode()?)
                .map_err(invalid_state)?,
            None => evidence,
        };
        let evidence = match self.harmonic_history.0 {
            Some(audit) => evidence
                .try_with_harmonic_history(audit.decode()?)
                .map_err(invalid_state)?,
            None => evidence,
        };
        match self.valuation_method_set.0 {
            Some(audit) => evidence
                .try_with_valuation_method_set(audit.decode()?)
                .map_err(invalid_state),
            None => Ok(evidence),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ForecastChartWire {
    basis: [u8; 32],
    history: [u8; 32],
    source_cutoff: Timestamp,
    origin_at: Timestamp,
    vintage: [u8; 32],
    canonical_record: Box<[u8]>,
}
impl From<&market_squawk_decisions::SavedForecastChartEvidence> for ForecastChartWire {
    fn from(value: &market_squawk_decisions::SavedForecastChartEvidence) -> Self {
        Self { basis: value.basis_identity().evidence_digest().bytes(), history: value.history_identity().evidence_digest().bytes(),
            source_cutoff: value.source_cutoff(), origin_at: value.origin_at(),
            vintage: value.vintage_id().bytes(), canonical_record: value.canonical_record().into() }
    }
}
impl ForecastChartWire {
    fn decode(self, evidence: &InvestmentAnalysisEvidence)
        -> Result<market_squawk_decisions::SavedForecastChartEvidence, DecisionApplicationError> {
        let value = market_squawk_decisions::SavedForecastChartEvidence::try_new(
            evidence.instrument_id(),evidence.currency(),self.source_cutoff,self.origin_at,
            market_squawk_decisions::ProposalForecastVintageId::try_from_bytes(self.vintage).map_err(invalid_state)?,
            market_squawk_decisions::DecisionContentDigest::try_new(EvidenceDigest::new(market_squawk_domain::DigestAlgorithm::Sha256,self.basis)).map_err(invalid_state)?,
            market_squawk_decisions::DecisionContentDigest::try_new(EvidenceDigest::new(market_squawk_domain::DigestAlgorithm::Sha256,self.history)).map_err(invalid_state)?,
            self.canonical_record).map_err(invalid_state)?;
        crate::application::model::forecast::SavedForecastChart::decode(&value).map_err(invalid_state)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct BenchmarkComparisonWire {
    observed_through: Timestamp,
    canonical_record: Box<[u8]>,
}

impl From<&market_squawk_decisions::SavedBenchmarkComparisonEvidence> for BenchmarkComparisonWire {
    fn from(value: &market_squawk_decisions::SavedBenchmarkComparisonEvidence) -> Self {
        Self {
            observed_through: value.observed_through(),
            canonical_record: value.canonical_record().into(),
        }
    }
}

impl BenchmarkComparisonWire {
    fn decode(
        self,
        evidence: &InvestmentAnalysisEvidence,
    ) -> Result<market_squawk_decisions::SavedBenchmarkComparisonEvidence, DecisionApplicationError>
    {
        let value = market_squawk_decisions::SavedBenchmarkComparisonEvidence::try_new(
            evidence.instrument_id(),
            evidence.currency(),
            evidence.as_of(),
            self.observed_through,
            self.canonical_record,
        )
        .map_err(invalid_state)?;
        crate::application::saved_benchmark::SavedBenchmarkComparison::decode(&value)
            .map_err(invalid_state)?;
        Ok(value)
    }
}

/// Explicitly nullable on the wire. Fields use `deserialize_with` to reject omission: Serde's
/// ordinary missing-field deserializer would otherwise supply `None` through this wrapper.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub(super) struct RequiredOption<T>(pub(super) Option<T>);

impl<'de, T> Deserialize<'de> for RequiredOption<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<T>::deserialize(deserializer).map(Self)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ProposalEvidenceWindowWire {
    observed_at: Timestamp,
    source_knowledge_cutoff: Timestamp,
    available_at: Timestamp,
    expires_at: Timestamp,
    content_identity: EvidenceDigest,
}

impl From<ProposalEvidenceWindow> for ProposalEvidenceWindowWire {
    fn from(value: ProposalEvidenceWindow) -> Self {
        Self {
            observed_at: value.observed_at(),
            source_knowledge_cutoff: value.source_knowledge_cutoff(),
            available_at: value.available_at(),
            expires_at: value.expires_at(),
            content_identity: value.content_identity().evidence_digest(),
        }
    }
}

impl ProposalEvidenceWindowWire {
    fn decode(self) -> Result<ProposalEvidenceWindow, DecisionApplicationError> {
        ProposalEvidenceWindow::try_from_derived(
            self.observed_at,
            self.source_knowledge_cutoff,
            self.available_at,
            self.expires_at,
            content_digest(self.content_identity)?,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum MarketReferencePriceKindWire {
    LastTrade,
    CheckedBidAskMidpoint,
}

impl From<MarketReferencePriceKind> for MarketReferencePriceKindWire {
    fn from(value: MarketReferencePriceKind) -> Self {
        match value {
            MarketReferencePriceKind::LastTrade => Self::LastTrade,
            MarketReferencePriceKind::CheckedBidAskMidpoint => Self::CheckedBidAskMidpoint,
        }
    }
}

impl From<MarketReferencePriceKindWire> for MarketReferencePriceKind {
    fn from(value: MarketReferencePriceKindWire) -> Self {
        match value {
            MarketReferencePriceKindWire::LastTrade => Self::LastTrade,
            MarketReferencePriceKindWire::CheckedBidAskMidpoint => Self::CheckedBidAskMidpoint,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum MarketReferenceAdjustmentBasisWire {
    UnadjustedSpot,
}

impl From<MarketReferenceAdjustmentBasis> for MarketReferenceAdjustmentBasisWire {
    fn from(value: MarketReferenceAdjustmentBasis) -> Self {
        match value {
            MarketReferenceAdjustmentBasis::UnadjustedSpot => Self::UnadjustedSpot,
        }
    }
}

impl From<MarketReferenceAdjustmentBasisWire> for MarketReferenceAdjustmentBasis {
    fn from(value: MarketReferenceAdjustmentBasisWire) -> Self {
        match value {
            MarketReferenceAdjustmentBasisWire::UnadjustedSpot => Self::UnadjustedSpot,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct MarketReferenceEvidenceWire {
    instrument_id: InstrumentId,
    price: Money,
    quality: DataQuality,
    price_kind: MarketReferencePriceKindWire,
    adjustment_basis: MarketReferenceAdjustmentBasisWire,
    selection_receipt_identity: EvidenceDigest,
    selected_observation_identity: EvidenceDigest,
    window: ProposalEvidenceWindowWire,
}

impl From<&MarketReferenceEvidence> for MarketReferenceEvidenceWire {
    fn from(value: &MarketReferenceEvidence) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            price: value.price(),
            quality: value.quality(),
            price_kind: value.price_kind().into(),
            adjustment_basis: value.adjustment_basis().into(),
            selection_receipt_identity: value.selection_receipt_identity().evidence_digest(),
            selected_observation_identity: value.selected_observation_identity().evidence_digest(),
            window: value.window().into(),
        }
    }
}

impl MarketReferenceEvidenceWire {
    fn decode(self) -> Result<MarketReferenceEvidence, DecisionApplicationError> {
        MarketReferenceEvidence::try_new(
            self.instrument_id,
            self.price,
            self.quality,
            self.price_kind.into(),
            self.adjustment_basis.into(),
            content_digest(self.selection_receipt_identity)?,
            content_digest(self.selected_observation_identity)?,
            self.window.decode()?,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PriceRangeWire {
    lower: Money,
    upper: Money,
}

impl From<TargetPriceRange> for PriceRangeWire {
    fn from(value: TargetPriceRange) -> Self {
        Self {
            lower: value.lower(),
            upper: value.upper(),
        }
    }
}

impl PriceRangeWire {
    fn decode(self) -> Result<TargetPriceRange, DecisionApplicationError> {
        TargetPriceRange::try_new(self.lower, self.upper).map_err(invalid_state)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ForecastCalibrationWire {
    nominal_coverage_ppm: u32,
    realized_coverage_ppm: u32,
    completed_outcomes: u32,
}

impl From<ForecastCalibrationSummary> for ForecastCalibrationWire {
    fn from(value: ForecastCalibrationSummary) -> Self {
        Self {
            nominal_coverage_ppm: value.nominal_coverage_ppm(),
            realized_coverage_ppm: value.realized_coverage_ppm(),
            completed_outcomes: value.completed_outcomes().get(),
        }
    }
}

impl ForecastCalibrationWire {
    fn decode(self) -> Result<ForecastCalibrationSummary, DecisionApplicationError> {
        ForecastCalibrationSummary::try_new(
            self.nominal_coverage_ppm,
            self.realized_coverage_ppm,
            NonZeroU32::new(self.completed_outcomes)
                .ok_or(DecisionApplicationError::InvalidPersistentState)?,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PriceForecastEvidenceWire {
    instrument_id: InstrumentId,
    downside: Money,
    base: Money,
    upside: Money,
    downside_range: PriceRangeWire,
    base_range: PriceRangeWire,
    upside_range: PriceRangeWire,
    horizon_at: Timestamp,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    expected_terminal: RequiredOption<ExpectedTerminalPriceWire>,
    vintage_id: [u8; 32],
    output_binding_identity: EvidenceDigest,
    calibration_identity: EvidenceDigest,
    outcome_set_identity: EvidenceDigest,
    calibration: ForecastCalibrationWire,
    window: ProposalEvidenceWindowWire,
}

impl From<&PriceForecastEvidence> for PriceForecastEvidenceWire {
    fn from(value: &PriceForecastEvidence) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            downside: value.cases().downside(),
            base: value.cases().base(),
            upside: value.cases().upside(),
            downside_range: value.ranges().downside().into(),
            base_range: value.ranges().base().into(),
            upside_range: value.ranges().upside().into(),
            horizon_at: value.horizon_at(),
            expected_terminal: RequiredOption(ExpectedTerminalPriceWire::from_evidence(value)),
            vintage_id: value.vintage_id().bytes(),
            output_binding_identity: value.output_binding_identity().evidence_digest(),
            calibration_identity: value.calibration_identity().evidence_digest(),
            outcome_set_identity: value.outcome_set_identity().evidence_digest(),
            calibration: value.calibration().into(),
            window: value.window().into(),
        }
    }
}

impl PriceForecastEvidenceWire {
    fn decode(self) -> Result<PriceForecastEvidence, DecisionApplicationError> {
        let (
            expected_terminal_statistic,
            expected_terminal_price,
            expected_terminal_horizon_at,
            expected_terminal_statistic_identity,
        ) = match self.expected_terminal.0 {
            Some(expected) => (
                Some(expected.statistic.into()),
                Some(expected.price),
                Some(expected.horizon_at),
                Some(content_digest(expected.statistic_identity)?),
            ),
            None => (None, None, None, None),
        };
        PriceForecastEvidence::try_new(
            self.instrument_id,
            TargetPriceCases::try_new(self.downside, self.base, self.upside)
                .map_err(invalid_state)?,
            ForecastPriceRanges::try_new(
                self.downside_range.decode()?,
                self.base_range.decode()?,
                self.upside_range.decode()?,
            )
            .map_err(invalid_state)?,
            self.horizon_at,
            expected_terminal_statistic,
            expected_terminal_price,
            expected_terminal_horizon_at,
            expected_terminal_statistic_identity,
            ProposalForecastVintageId::try_from_bytes(self.vintage_id).map_err(invalid_state)?,
            content_digest(self.output_binding_identity)?,
            content_digest(self.calibration_identity)?,
            content_digest(self.outcome_set_identity)?,
            self.calibration.decode()?,
            self.window.decode()?,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ExpectedTerminalStatisticWire {
    ModelEstimatedConditionalMean,
}

impl From<ExpectedTerminalStatisticWire> for ForecastCentralStatistic {
    fn from(value: ExpectedTerminalStatisticWire) -> Self {
        match value {
            ExpectedTerminalStatisticWire::ModelEstimatedConditionalMean => {
                Self::ModelEstimatedConditionalMean
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ExpectedTerminalPriceWire {
    statistic: ExpectedTerminalStatisticWire,
    price: Money,
    horizon_at: Timestamp,
    statistic_identity: EvidenceDigest,
}

impl ExpectedTerminalPriceWire {
    fn from_evidence(value: &PriceForecastEvidence) -> Option<Self> {
        match (
            value.expected_terminal_statistic(),
            value.expected_terminal_price(),
            value.expected_terminal_horizon_at(),
            value.expected_terminal_statistic_identity(),
        ) {
            (
                Some(ForecastCentralStatistic::ModelEstimatedConditionalMean),
                Some(price),
                Some(horizon_at),
                Some(statistic_identity),
            ) => Some(Self {
                statistic: ExpectedTerminalStatisticWire::ModelEstimatedConditionalMean,
                price,
                horizon_at,
                statistic_identity: statistic_identity.evidence_digest(),
            }),
            (
                Some(
                    ForecastCentralStatistic::ModelEstimatedConditionalMean
                    | ForecastCentralStatistic::Unavailable,
                )
                | None,
                _,
                _,
                _,
            ) => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ValuationEvidenceWire {
    instrument_id: InstrumentId,
    fair_value: Money,
    basis: ValuationAmountBasisWire,
    horizon_at: Timestamp,
    measurement_id: String,
    provenance: ValuationEvidenceProvenanceWire,
    window: ProposalEvidenceWindowWire,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ValuationEvidenceProvenanceWire {
    AccountingSelection {
        classification_decision_id: String,
        selection_receipt_hash: String,
    },
    ResearchCalculation {
        account_id: AccountId,
        method: AutomaticValuationMethodWire,
        calculation_identity: String,
        input_set_identity: String,
    },
}

impl From<&ValuationEvidence> for ValuationEvidenceWire {
    fn from(value: &ValuationEvidence) -> Self {
        let provenance = match value.provenance() {
            market_squawk_decisions::ValuationEvidenceProvenance::AccountingSelection {
                classification_decision_id,
                selection_receipt_hash,
            } => ValuationEvidenceProvenanceWire::AccountingSelection {
                classification_decision_id: classification_decision_id.to_string(),
                selection_receipt_hash: selection_receipt_hash.to_string(),
            },
            market_squawk_decisions::ValuationEvidenceProvenance::ResearchCalculation {
                account_id,
                method,
                calculation_identity,
                input_set_identity,
            } => ValuationEvidenceProvenanceWire::ResearchCalculation {
                account_id,
                method: method.into(),
                calculation_identity: calculation_identity.to_string(),
                input_set_identity: input_set_identity.to_string(),
            },
        };
        Self {
            instrument_id: value.instrument_id(),
            fair_value: value.fair_value(),
            basis: value.basis().into(),
            horizon_at: value.horizon_at(),
            measurement_id: value.measurement_id().to_string(),
            provenance,
            window: value.window().into(),
        }
    }
}

impl ValuationEvidenceWire {
    fn decode(self) -> Result<ValuationEvidence, DecisionApplicationError> {
        let measurement_id =
            MeasurementId::from_str(&self.measurement_id).map_err(invalid_state)?;
        let window = self.window.decode()?;
        match self.provenance {
            ValuationEvidenceProvenanceWire::AccountingSelection {
                classification_decision_id,
                selection_receipt_hash,
            } => ValuationEvidence::try_recover_receipt_bound_projection(
                self.instrument_id,
                self.fair_value,
                self.basis.into(),
                self.horizon_at,
                measurement_id,
                DecisionId::from_str(&classification_decision_id).map_err(invalid_state)?,
                FairValueSelectionReceiptHash::from_str(&selection_receipt_hash)
                    .map_err(invalid_state)?,
                window,
            )
            .map_err(invalid_state),
            ValuationEvidenceProvenanceWire::ResearchCalculation {
                account_id,
                method,
                calculation_identity,
                input_set_identity,
            } => ValuationEvidence::try_recover_research_projection(
                self.instrument_id,
                self.fair_value,
                self.basis.into(),
                self.horizon_at,
                measurement_id,
                account_id,
                method.into(),
                market_squawk_valuation::AutomaticValuationIdentity::from_str(
                    &calculation_identity,
                )
                .map_err(invalid_state)?,
                market_squawk_valuation::AutomaticValuationInputSetIdentity::from_str(
                    &input_set_identity,
                )
                .map_err(invalid_state)?,
                window,
            )
            .map_err(invalid_state),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ValuationAmountBasisWire {
    PerInstrumentUnit,
    ReportingEntityTotal,
    PositionTotal,
    TotalCommonEquity,
}

impl From<ValuationAmountBasis> for ValuationAmountBasisWire {
    fn from(value: ValuationAmountBasis) -> Self {
        match value {
            ValuationAmountBasis::PerInstrumentUnit => Self::PerInstrumentUnit,
            ValuationAmountBasis::ReportingEntityTotal => Self::ReportingEntityTotal,
            ValuationAmountBasis::PositionTotal => Self::PositionTotal,
            ValuationAmountBasis::TotalCommonEquity => Self::TotalCommonEquity,
        }
    }
}

impl From<ValuationAmountBasisWire> for ValuationAmountBasis {
    fn from(value: ValuationAmountBasisWire) -> Self {
        match value {
            ValuationAmountBasisWire::PerInstrumentUnit => Self::PerInstrumentUnit,
            ValuationAmountBasisWire::ReportingEntityTotal => Self::ReportingEntityTotal,
            ValuationAmountBasisWire::PositionTotal => Self::PositionTotal,
            ValuationAmountBasisWire::TotalCommonEquity => Self::TotalCommonEquity,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum AutomaticValuationMethodWire {
    DiscountedCashFlow,
    ComparableCompanies,
    ResidualIncome,
    ForecastDistribution,
}

impl From<AutomaticValuationMethod> for AutomaticValuationMethodWire {
    fn from(value: AutomaticValuationMethod) -> Self {
        match value {
            AutomaticValuationMethod::DiscountedCashFlow => Self::DiscountedCashFlow,
            AutomaticValuationMethod::ComparableCompanies => Self::ComparableCompanies,
            AutomaticValuationMethod::ResidualIncome => Self::ResidualIncome,
            AutomaticValuationMethod::ForecastDistribution => Self::ForecastDistribution,
        }
    }
}

impl From<AutomaticValuationMethodWire> for AutomaticValuationMethod {
    fn from(value: AutomaticValuationMethodWire) -> Self {
        match value {
            AutomaticValuationMethodWire::DiscountedCashFlow => Self::DiscountedCashFlow,
            AutomaticValuationMethodWire::ComparableCompanies => Self::ComparableCompanies,
            AutomaticValuationMethodWire::ResidualIncome => Self::ResidualIncome,
            AutomaticValuationMethodWire::ForecastDistribution => Self::ForecastDistribution,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum FinancialModelAssumptionKindWire {
    TerminalGrowth,
    DiscountRate,
    ComparableWeight,
    CostOfEquity,
    ForecastProbability,
    UncertaintyLower,
    UncertaintyUpper,
}

impl From<AutomaticValuationAssumptionKind> for FinancialModelAssumptionKindWire {
    fn from(value: AutomaticValuationAssumptionKind) -> Self {
        match value {
            AutomaticValuationAssumptionKind::TerminalGrowth => Self::TerminalGrowth,
            AutomaticValuationAssumptionKind::DiscountRate => Self::DiscountRate,
            AutomaticValuationAssumptionKind::ComparableWeight => Self::ComparableWeight,
            AutomaticValuationAssumptionKind::CostOfEquity => Self::CostOfEquity,
            AutomaticValuationAssumptionKind::ForecastProbability => Self::ForecastProbability,
            AutomaticValuationAssumptionKind::UncertaintyLower => Self::UncertaintyLower,
            AutomaticValuationAssumptionKind::UncertaintyUpper => Self::UncertaintyUpper,
        }
    }
}

impl From<FinancialModelAssumptionKindWire> for AutomaticValuationAssumptionKind {
    fn from(value: FinancialModelAssumptionKindWire) -> Self {
        match value {
            FinancialModelAssumptionKindWire::TerminalGrowth => Self::TerminalGrowth,
            FinancialModelAssumptionKindWire::DiscountRate => Self::DiscountRate,
            FinancialModelAssumptionKindWire::ComparableWeight => Self::ComparableWeight,
            FinancialModelAssumptionKindWire::CostOfEquity => Self::CostOfEquity,
            FinancialModelAssumptionKindWire::ForecastProbability => Self::ForecastProbability,
            FinancialModelAssumptionKindWire::UncertaintyLower => Self::UncertaintyLower,
            FinancialModelAssumptionKindWire::UncertaintyUpper => Self::UncertaintyUpper,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct FinancialModelAssumptionWire {
    kind: FinancialModelAssumptionKindWire,
    identifier: Box<str>,
    value: Decimal,
    evidence: EvidenceDigest,
    available_at: Timestamp,
    expires_at: Timestamp,
}

impl From<&AutomaticValuationAssumption> for FinancialModelAssumptionWire {
    fn from(value: &AutomaticValuationAssumption) -> Self {
        Self {
            kind: value.kind().into(),
            identifier: value.identifier().into(),
            value: value.value(),
            evidence: value.evidence(),
            available_at: value.available_at(),
            expires_at: value.expires_at(),
        }
    }
}

impl FinancialModelAssumptionWire {
    fn decode(self) -> Result<AutomaticValuationAssumption, DecisionApplicationError> {
        AutomaticValuationAssumption::try_new(
            self.kind.into(),
            &self.identifier,
            self.value,
            self.evidence,
            self.available_at,
            self.expires_at,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum MacroRateMaturityWire {
    TenYear,
    ThirtyYear,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct FinancialModelMacroAssumptionsWire {
    maturity: MacroRateMaturityWire,
    annual_yield_percent: Decimal,
    context_identity: EvidenceDigest,
    evidence_identity: EvidenceDigest,
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: market_squawk_domain::CalendarDate,
    available_at: Timestamp,
    expires_at: Timestamp,
    premium: FinancialModelAssumptionWire,
    assumption: FinancialModelAssumptionWire,
}

impl From<&FinancialModelMacroAssumptions> for FinancialModelMacroAssumptionsWire {
    fn from(value: &FinancialModelMacroAssumptions) -> Self {
        let reference = value.reference();
        Self {
            maturity: match reference.maturity() {
                MacroRateMaturity::TenYear => MacroRateMaturityWire::TenYear,
                MacroRateMaturity::ThirtyYear => MacroRateMaturityWire::ThirtyYear,
            },
            annual_yield_percent: reference.annual_yield_percent(),
            context_identity: reference.context_identity(),
            evidence_identity: reference.evidence_identity(),
            knowledge_cutoff: reference.knowledge_cutoff(),
            effective_date_cutoff: reference.effective_date_cutoff(),
            available_at: reference.available_at(),
            expires_at: reference.expires_at(),
            premium: value.premium().into(),
            assumption: value.assumption().into(),
        }
    }
}

impl FinancialModelMacroAssumptionsWire {
    fn decode(self) -> Result<FinancialModelMacroAssumptions, DecisionApplicationError> {
        let expected = self.assumption.decode()?;
        let reference = MacroRateReferenceEvidence::try_new(
            match self.maturity {
                MacroRateMaturityWire::TenYear => MacroRateMaturity::TenYear,
                MacroRateMaturityWire::ThirtyYear => MacroRateMaturity::ThirtyYear,
            },
            self.annual_yield_percent,
            self.context_identity,
            self.evidence_identity,
            self.knowledge_cutoff,
            self.effective_date_cutoff,
            self.available_at,
            self.expires_at,
        )
        .map_err(invalid_state)?;
        let binding = FinancialModelMacroAssumptions::try_new(
            reference,
            self.premium.decode()?,
            expected.kind(),
            expected.identifier(),
        )
        .map_err(invalid_state)?;
        if binding.assumption() != &expected {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(binding)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct FinancialModelEvidenceWire {
    instrument_id: InstrumentId,
    account_id: AccountId,
    method: AutomaticValuationMethodWire,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    periods_per_year: RequiredOption<NonZeroU32>,
    range_lower: Money,
    range_central: Money,
    range_upper: Money,
    scenario_downside: Money,
    scenario_base: Money,
    scenario_upside: Money,
    sensitivity_lower: Money,
    sensitivity_upper: Money,
    horizon_at: Timestamp,
    pit_input_set_identity: EvidenceDigest,
    calculation_identity: EvidenceDigest,
    assumptions_identity: EvidenceDigest,
    scenario_identity: EvidenceDigest,
    sensitivity_identity: EvidenceDigest,
    assumptions: Vec<FinancialModelAssumptionWire>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    macro_assumptions: RequiredOption<FinancialModelMacroAssumptionsWire>,
    window: ProposalEvidenceWindowWire,
}

impl From<&FinancialModelEvidence> for FinancialModelEvidenceWire {
    fn from(value: &FinancialModelEvidence) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            account_id: value.account_id(),
            method: value.method().into(),
            periods_per_year: RequiredOption(value.periods_per_year()),
            range_lower: value.range().lower(),
            range_central: value.range().central(),
            range_upper: value.range().upper(),
            scenario_downside: value.scenarios().downside(),
            scenario_base: value.scenarios().base(),
            scenario_upside: value.scenarios().upside(),
            sensitivity_lower: value.sensitivity_range().lower(),
            sensitivity_upper: value.sensitivity_range().upper(),
            horizon_at: value.horizon_at(),
            pit_input_set_identity: value.pit_input_set_identity().evidence_digest(),
            calculation_identity: value.calculation_identity().evidence_digest(),
            assumptions_identity: value.assumptions_identity().evidence_digest(),
            scenario_identity: value.scenario_identity().evidence_digest(),
            sensitivity_identity: value.sensitivity_identity().evidence_digest(),
            assumptions: value.assumptions().iter().map(Into::into).collect(),
            macro_assumptions: RequiredOption(value.macro_assumptions().map(Into::into)),
            window: value.window().into(),
        }
    }
}

impl FinancialModelEvidenceWire {
    fn decode(self) -> Result<FinancialModelEvidence, DecisionApplicationError> {
        if self.assumptions.len() > 128 {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        let expected_assumptions_identity = content_digest(self.assumptions_identity)?;
        let evidence = FinancialModelEvidence::try_recover_projection(
            self.instrument_id,
            self.account_id,
            self.method.into(),
            self.periods_per_year.0,
            FinancialModelValueRange::try_new(
                self.range_lower,
                self.range_central,
                self.range_upper,
            )
            .map_err(invalid_state)?,
            TargetPriceCases::try_new(
                self.scenario_downside,
                self.scenario_base,
                self.scenario_upside,
            )
            .map_err(invalid_state)?,
            TargetPriceRange::try_new(self.sensitivity_lower, self.sensitivity_upper)
                .map_err(invalid_state)?,
            self.horizon_at,
            content_digest(self.pit_input_set_identity)?,
            content_digest(self.calculation_identity)?,
            self.assumptions
                .into_iter()
                .map(FinancialModelAssumptionWire::decode)
                .collect::<Result<Vec<_>, _>>()?
                .into_boxed_slice(),
            content_digest(self.scenario_identity)?,
            content_digest(self.sensitivity_identity)?,
            self.macro_assumptions
                .0
                .map(FinancialModelMacroAssumptionsWire::decode)
                .transpose()?,
            self.window.decode()?,
        )
        .map_err(invalid_state)?;
        if evidence.assumptions_identity() != expected_assumptions_identity {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(evidence)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RecommendationStudyQualificationWire {
    basis: HistoricalStudyBasis,
    limitations: Vec<HistoricalStudyLimitation>,
}

impl From<RecommendationStudyQualification> for RecommendationStudyQualificationWire {
    fn from(value: RecommendationStudyQualification) -> Self {
        Self {
            basis: value.basis(),
            limitations: value.limitations().to_vec(),
        }
    }
}

impl RecommendationStudyQualificationWire {
    fn decode(self) -> Result<RecommendationStudyQualification, DecisionApplicationError> {
        RecommendationStudyQualification::try_new(self.basis, &self.limitations)
            .map_err(invalid_state)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ChronologicalOutOfSampleEvidenceWire {
    instrument_id: InstrumentId,
    currency: Currency,
    qualification: RecommendationStudyQualificationWire,
    outcome_horizon_nanos: i64,
    evaluation_starts_at: Timestamp,
    evaluation_ends_at: Timestamp,
    simulation_cutoff_at: Timestamp,
    completed_observations: u32,
    total_signals: u32,
    fold_count: u32,
    completion_coverage_ppm: u32,
    dataset_identity: EvidenceDigest,
    signal_plan_identity: EvidenceDigest,
    aggregate_identity: EvidenceDigest,
    study_identity: EvidenceDigest,
    window: ProposalEvidenceWindowWire,
}

impl From<&ChronologicalOutOfSampleEvidence> for ChronologicalOutOfSampleEvidenceWire {
    fn from(value: &ChronologicalOutOfSampleEvidence) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            currency: value.currency(),
            qualification: value.qualification().into(),
            outcome_horizon_nanos: value.outcome_horizon_nanos(),
            evaluation_starts_at: value.evaluation_starts_at(),
            evaluation_ends_at: value.evaluation_ends_at(),
            simulation_cutoff_at: value.simulation_cutoff_at(),
            completed_observations: value.completed_observations().get(),
            total_signals: value.total_signals().get(),
            fold_count: value.fold_count().get(),
            completion_coverage_ppm: value.completion_coverage_ppm(),
            dataset_identity: value.dataset_identity().evidence_digest(),
            signal_plan_identity: value.signal_plan_identity().evidence_digest(),
            aggregate_identity: value.aggregate_identity().evidence_digest(),
            study_identity: value.study_identity().evidence_digest(),
            window: value.window().into(),
        }
    }
}

impl ChronologicalOutOfSampleEvidenceWire {
    fn decode(self) -> Result<ChronologicalOutOfSampleEvidence, DecisionApplicationError> {
        ChronologicalOutOfSampleEvidence::try_new(
            self.instrument_id,
            self.currency,
            self.qualification.decode()?,
            self.outcome_horizon_nanos,
            self.evaluation_starts_at,
            self.evaluation_ends_at,
            self.simulation_cutoff_at,
            nonzero(self.completed_observations)?,
            nonzero(self.total_signals)?,
            nonzero(self.fold_count)?,
            self.completion_coverage_ppm,
            content_digest(self.dataset_identity)?,
            content_digest(self.signal_plan_identity)?,
            content_digest(self.aggregate_identity)?,
            content_digest(self.study_identity)?,
            self.window.decode()?,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum HarmonicPatternKindWire {
    AbCd,
    Gartley,
    Bat,
    Butterfly,
    Crab,
    DeepCrab,
    Cypher,
    Shark,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum HarmonicDirectionWire {
    Bullish,
    Bearish,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum HarmonicPatternQualityWire {
    Valid,
    PreferredBatB,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct HarmonicPatternEvidenceWire {
    instrument_id: InstrumentId,
    timeframe_nanos: u64,
    kind: HarmonicPatternKindWire,
    direction: HarmonicDirectionWire,
    quality: HarmonicPatternQualityWire,
    completion_lower: PriceTicks,
    completion_upper: PriceTicks,
    targets: [PriceTicks; 3],
    invalidation: PriceTicks,
    observation_cutoff: Timestamp,
    confirmation_cutoff: Timestamp,
    decision_cutoff: Timestamp,
    expires_at: Timestamp,
    implementation_identity: [u8; 32],
    evidence_digest: EvidenceDigest,
    window: ProposalEvidenceWindowWire,
}

impl From<&HarmonicPatternEvidenceReceipt> for HarmonicPatternEvidenceWire {
    fn from(value: &HarmonicPatternEvidenceReceipt) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            timeframe_nanos: value.timeframe_nanos().get(),
            kind: match value.kind() {
                HarmonicPatternKind::AbCd => HarmonicPatternKindWire::AbCd,
                HarmonicPatternKind::Gartley => HarmonicPatternKindWire::Gartley,
                HarmonicPatternKind::Bat => HarmonicPatternKindWire::Bat,
                HarmonicPatternKind::Butterfly => HarmonicPatternKindWire::Butterfly,
                HarmonicPatternKind::Crab => HarmonicPatternKindWire::Crab,
                HarmonicPatternKind::DeepCrab => HarmonicPatternKindWire::DeepCrab,
                HarmonicPatternKind::Cypher => HarmonicPatternKindWire::Cypher,
                HarmonicPatternKind::Shark => HarmonicPatternKindWire::Shark,
            },
            direction: match value.direction() {
                HarmonicDirection::Bullish => HarmonicDirectionWire::Bullish,
                HarmonicDirection::Bearish => HarmonicDirectionWire::Bearish,
            },
            quality: match value.quality() {
                HarmonicPatternQuality::Valid => HarmonicPatternQualityWire::Valid,
                HarmonicPatternQuality::PreferredBatB => HarmonicPatternQualityWire::PreferredBatB,
            },
            completion_lower: value.completion_lower(),
            completion_upper: value.completion_upper(),
            targets: value.targets(),
            invalidation: value.invalidation(),
            observation_cutoff: value.observation_cutoff(),
            confirmation_cutoff: value.confirmation_cutoff(),
            decision_cutoff: value.decision_cutoff(),
            expires_at: value.expires_at(),
            implementation_identity: value.implementation_identity().as_bytes(),
            evidence_digest: value.evidence_digest(),
            window: value.window().into(),
        }
    }
}

impl HarmonicPatternEvidenceWire {
    fn decode(self) -> Result<HarmonicPatternEvidenceReceipt, DecisionApplicationError> {
        HarmonicPatternEvidenceReceipt::try_recover_projection(
            self.instrument_id,
            NonZeroU64::new(self.timeframe_nanos)
                .ok_or(DecisionApplicationError::InvalidPersistentState)?,
            match self.kind {
                HarmonicPatternKindWire::AbCd => HarmonicPatternKind::AbCd,
                HarmonicPatternKindWire::Gartley => HarmonicPatternKind::Gartley,
                HarmonicPatternKindWire::Bat => HarmonicPatternKind::Bat,
                HarmonicPatternKindWire::Butterfly => HarmonicPatternKind::Butterfly,
                HarmonicPatternKindWire::Crab => HarmonicPatternKind::Crab,
                HarmonicPatternKindWire::DeepCrab => HarmonicPatternKind::DeepCrab,
                HarmonicPatternKindWire::Cypher => HarmonicPatternKind::Cypher,
                HarmonicPatternKindWire::Shark => HarmonicPatternKind::Shark,
            },
            match self.direction {
                HarmonicDirectionWire::Bullish => HarmonicDirection::Bullish,
                HarmonicDirectionWire::Bearish => HarmonicDirection::Bearish,
            },
            match self.quality {
                HarmonicPatternQualityWire::Valid => HarmonicPatternQuality::Valid,
                HarmonicPatternQualityWire::PreferredBatB => HarmonicPatternQuality::PreferredBatB,
            },
            self.completion_lower,
            self.completion_upper,
            self.targets,
            self.invalidation,
            self.observation_cutoff,
            self.confirmation_cutoff,
            self.decision_cutoff,
            self.expires_at,
            FeatureImplementationDigest::try_from_sha256(self.implementation_identity)
                .map_err(invalid_state)?,
            self.evidence_digest,
            self.window.decode()?,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CostAdjustedBacktestEvidenceWire {
    instrument_id: InstrumentId,
    currency: Currency,
    qualification: RecommendationStudyQualificationWire,
    outcome_horizon_nanos: i64,
    net_return: BasisPoints,
    max_drawdown: BasisPoints,
    fee_basis_points: BasisPoints,
    slippage_basis_points: BasisPoints,
    maximum_random_slippage_basis_points: BasisPoints,
    observations: u32,
    trials: u32,
    stability_ppm: u32,
    simulation_cutoff_at: Timestamp,
    dataset_identity: EvidenceDigest,
    command_identity: EvidenceDigest,
    terminal_identity: EvidenceDigest,
    report_identity: EvidenceDigest,
    cohort_identity: EvidenceDigest,
    cost_model_identity: EvidenceDigest,
    window: ProposalEvidenceWindowWire,
}

impl From<&CostAdjustedBacktestEvidence> for CostAdjustedBacktestEvidenceWire {
    fn from(value: &CostAdjustedBacktestEvidence) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            currency: value.currency(),
            qualification: value.qualification().into(),
            outcome_horizon_nanos: value.outcome_horizon_nanos(),
            net_return: value.net_return(),
            max_drawdown: value.max_drawdown(),
            fee_basis_points: value.fee_basis_points(),
            slippage_basis_points: value.slippage_basis_points(),
            maximum_random_slippage_basis_points: value.maximum_random_slippage_basis_points(),
            observations: value.observations().get(),
            trials: value.trials().get(),
            stability_ppm: value.stability_ppm(),
            simulation_cutoff_at: value.simulation_cutoff_at(),
            dataset_identity: value.dataset_identity().evidence_digest(),
            command_identity: value.command_identity().evidence_digest(),
            terminal_identity: value.terminal_identity().evidence_digest(),
            report_identity: value.report_identity().evidence_digest(),
            cohort_identity: value.cohort_identity().evidence_digest(),
            cost_model_identity: value.cost_model_identity().evidence_digest(),
            window: value.window().into(),
        }
    }
}

impl CostAdjustedBacktestEvidenceWire {
    fn decode(self) -> Result<CostAdjustedBacktestEvidence, DecisionApplicationError> {
        CostAdjustedBacktestEvidence::try_new(
            self.instrument_id,
            self.currency,
            self.qualification.decode()?,
            self.outcome_horizon_nanos,
            self.net_return,
            self.max_drawdown,
            self.fee_basis_points,
            self.slippage_basis_points,
            self.maximum_random_slippage_basis_points,
            NonZeroU32::new(self.observations)
                .ok_or(DecisionApplicationError::InvalidPersistentState)?,
            NonZeroU32::new(self.trials).ok_or(DecisionApplicationError::InvalidPersistentState)?,
            self.stability_ppm,
            self.simulation_cutoff_at,
            content_digest(self.dataset_identity)?,
            content_digest(self.command_identity)?,
            content_digest(self.terminal_identity)?,
            content_digest(self.report_identity)?,
            content_digest(self.cohort_identity)?,
            content_digest(self.cost_model_identity)?,
            self.window.decode()?,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct LiquidityEvidenceWire {
    instrument_id: InstrumentId,
    currency: Currency,
    quoted_spread: BasisPoints,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    buy_add_capacity_ppm: RequiredOption<u32>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    trim_sell_capacity_ppm: RequiredOption<u32>,
    quality: DataQuality,
    assessment_identity: EvidenceDigest,
    window: ProposalEvidenceWindowWire,
}

impl From<&LiquidityEvidence> for LiquidityEvidenceWire {
    fn from(value: &LiquidityEvidence) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            currency: value.currency(),
            quoted_spread: value.quoted_spread(),
            buy_add_capacity_ppm: RequiredOption(value.buy_add_capacity_ppm()),
            trim_sell_capacity_ppm: RequiredOption(value.trim_sell_capacity_ppm()),
            quality: value.quality(),
            assessment_identity: value.assessment_identity().evidence_digest(),
            window: value.window().into(),
        }
    }
}

impl LiquidityEvidenceWire {
    fn decode(self) -> Result<LiquidityEvidence, DecisionApplicationError> {
        LiquidityEvidence::try_new(
            self.instrument_id,
            self.currency,
            self.quoted_spread,
            self.buy_add_capacity_ppm.0,
            self.trim_sell_capacity_ppm.0,
            self.quality,
            content_digest(self.assessment_identity)?,
            self.window.decode()?,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum PortfolioPositionStateWire {
    NoPosition,
    Position {
        add_allowed: bool,
        trim_allowed: bool,
        exit_allowed: bool,
    },
}

impl From<PortfolioPositionState> for PortfolioPositionStateWire {
    fn from(value: PortfolioPositionState) -> Self {
        match value {
            PortfolioPositionState::NoPosition => Self::NoPosition,
            PortfolioPositionState::Position {
                add_allowed,
                trim_allowed,
                exit_allowed,
            } => Self::Position {
                add_allowed,
                trim_allowed,
                exit_allowed,
            },
        }
    }
}

impl From<PortfolioPositionStateWire> for PortfolioPositionState {
    fn from(value: PortfolioPositionStateWire) -> Self {
        match value {
            PortfolioPositionStateWire::NoPosition => Self::NoPosition,
            PortfolioPositionStateWire::Position {
                add_allowed,
                trim_allowed,
                exit_allowed,
            } => Self::Position {
                add_allowed,
                trim_allowed,
                exit_allowed,
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PortfolioRiskEvidenceWire {
    instrument_id: InstrumentId,
    account_id: AccountId,
    currency: Currency,
    portfolio_revision: [u8; 32],
    position_state: PortfolioPositionStateWire,
    risk_capacity_ppm: u32,
    risk_report_identity: EvidenceDigest,
    window: ProposalEvidenceWindowWire,
}

impl From<&PortfolioRiskEvidence> for PortfolioRiskEvidenceWire {
    fn from(value: &PortfolioRiskEvidence) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            account_id: value.account_id(),
            currency: value.currency(),
            portfolio_revision: value.portfolio_revision().bytes(),
            position_state: value.position_state().into(),
            risk_capacity_ppm: value.risk_capacity_ppm(),
            risk_report_identity: value.risk_report_identity().evidence_digest(),
            window: value.window().into(),
        }
    }
}

impl PortfolioRiskEvidenceWire {
    fn decode(self) -> Result<PortfolioRiskEvidence, DecisionApplicationError> {
        PortfolioRiskEvidence::try_new(
            self.instrument_id,
            self.account_id,
            self.currency,
            PortfolioRevisionToken::from_bytes(self.portfolio_revision),
            self.position_state.into(),
            self.risk_capacity_ppm,
            content_digest(self.risk_report_identity)?,
            self.window.decode()?,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RecommendationEvidenceKindWire {
    Market,
    PriceForecast,
    Valuation,
    FinancialModel,
    Backtest,
    OutOfSample,
    HarmonicPattern,
    Liquidity,
    PortfolioRisk,
}

impl From<RecommendationEvidenceKind> for RecommendationEvidenceKindWire {
    fn from(value: RecommendationEvidenceKind) -> Self {
        match value {
            RecommendationEvidenceKind::Market => Self::Market,
            RecommendationEvidenceKind::PriceForecast => Self::PriceForecast,
            RecommendationEvidenceKind::Valuation => Self::Valuation,
            RecommendationEvidenceKind::FinancialModel => Self::FinancialModel,
            RecommendationEvidenceKind::Backtest => Self::Backtest,
            RecommendationEvidenceKind::OutOfSample => Self::OutOfSample,
            RecommendationEvidenceKind::HarmonicPattern => Self::HarmonicPattern,
            RecommendationEvidenceKind::Liquidity => Self::Liquidity,
            RecommendationEvidenceKind::PortfolioRisk => Self::PortfolioRisk,
        }
    }
}

impl From<RecommendationEvidenceKindWire> for RecommendationEvidenceKind {
    fn from(value: RecommendationEvidenceKindWire) -> Self {
        match value {
            RecommendationEvidenceKindWire::Market => Self::Market,
            RecommendationEvidenceKindWire::PriceForecast => Self::PriceForecast,
            RecommendationEvidenceKindWire::Valuation => Self::Valuation,
            RecommendationEvidenceKindWire::FinancialModel => Self::FinancialModel,
            RecommendationEvidenceKindWire::Backtest => Self::Backtest,
            RecommendationEvidenceKindWire::OutOfSample => Self::OutOfSample,
            RecommendationEvidenceKindWire::HarmonicPattern => Self::HarmonicPattern,
            RecommendationEvidenceKindWire::Liquidity => Self::Liquidity,
            RecommendationEvidenceKindWire::PortfolioRisk => Self::PortfolioRisk,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ProposalUnavailableReasonWire {
    UnprovenCurrentShareUnits,
    MissingEvidence {
        evidence: RecommendationEvidenceKindWire,
    },
    InstrumentMismatch {
        evidence: RecommendationEvidenceKindWire,
        expected: InstrumentId,
        actual: InstrumentId,
    },
    CurrencyMismatch {
        evidence: RecommendationEvidenceKindWire,
        expected: Currency,
        actual: Currency,
    },
    AccountMismatch {
        expected: AccountId,
        actual: AccountId,
    },
    NotAvailableAtCutoff {
        evidence: RecommendationEvidenceKindWire,
    },
    ExpiredEvidence {
        evidence: RecommendationEvidenceKindWire,
    },
    StaleEvidence {
        evidence: RecommendationEvidenceKindWire,
    },
    RejectedQuality {
        evidence: RecommendationEvidenceKindWire,
        quality: DataQuality,
    },
    ForecastHorizonMismatch {
        expected: Timestamp,
        actual: Timestamp,
    },
    ValuationHorizonMismatch {
        expected: Timestamp,
        actual: Timestamp,
    },
    FinancialModelHorizonMismatch {
        expected: Timestamp,
        actual: Timestamp,
    },
    BacktestHorizonMismatch {
        expected_nanos: i64,
        actual_nanos: i64,
    },
    OutOfSampleHorizonMismatch {
        expected_nanos: i64,
        actual_nanos: i64,
    },
    FinancialModelValuationMismatch,
    OutOfSampleBacktestMismatch,
    HistoricalStudyBasisNotAllowed {
        actual: HistoricalStudyBasis,
    },
    InsufficientForecastOutcomes {
        required: u32,
        actual: u32,
    },
    UnsupportedForecastCoverage {
        minimum_ppm: u32,
        maximum_ppm: u32,
        actual_ppm: u32,
    },
    ForecastCalibrationBelowPolicy {
        minimum_realized_ppm: u32,
        maximum_error_ppm: u32,
        nominal_ppm: u32,
        realized_ppm: u32,
    },
    InsufficientBacktestObservations {
        required: u32,
        actual: u32,
    },
    InsufficientBacktestTrials {
        required: u32,
        actual: u32,
    },
    ReservedPortfolioRevision,
}

impl From<ProposalUnavailableReason> for ProposalUnavailableReasonWire {
    fn from(value: ProposalUnavailableReason) -> Self {
        match value {
            ProposalUnavailableReason::UnprovenCurrentShareUnits => Self::UnprovenCurrentShareUnits,
            ProposalUnavailableReason::MissingEvidence(evidence) => Self::MissingEvidence {
                evidence: evidence.into(),
            },
            ProposalUnavailableReason::InstrumentMismatch {
                evidence,
                expected,
                actual,
            } => Self::InstrumentMismatch {
                evidence: evidence.into(),
                expected,
                actual,
            },
            ProposalUnavailableReason::CurrencyMismatch {
                evidence,
                expected,
                actual,
            } => Self::CurrencyMismatch {
                evidence: evidence.into(),
                expected,
                actual,
            },
            ProposalUnavailableReason::AccountMismatch { expected, actual } => {
                Self::AccountMismatch { expected, actual }
            }
            ProposalUnavailableReason::NotAvailableAtCutoff(evidence) => {
                Self::NotAvailableAtCutoff {
                    evidence: evidence.into(),
                }
            }
            ProposalUnavailableReason::ExpiredEvidence(evidence) => Self::ExpiredEvidence {
                evidence: evidence.into(),
            },
            ProposalUnavailableReason::StaleEvidence(evidence) => Self::StaleEvidence {
                evidence: evidence.into(),
            },
            ProposalUnavailableReason::RejectedQuality { evidence, quality } => {
                Self::RejectedQuality {
                    evidence: evidence.into(),
                    quality,
                }
            }
            ProposalUnavailableReason::ForecastHorizonMismatch { expected, actual } => {
                Self::ForecastHorizonMismatch { expected, actual }
            }
            ProposalUnavailableReason::ValuationHorizonMismatch { expected, actual } => {
                Self::ValuationHorizonMismatch { expected, actual }
            }
            ProposalUnavailableReason::FinancialModelHorizonMismatch { expected, actual } => {
                Self::FinancialModelHorizonMismatch { expected, actual }
            }
            ProposalUnavailableReason::BacktestHorizonMismatch {
                expected_nanos,
                actual_nanos,
            } => Self::BacktestHorizonMismatch {
                expected_nanos,
                actual_nanos,
            },
            ProposalUnavailableReason::OutOfSampleHorizonMismatch {
                expected_nanos,
                actual_nanos,
            } => Self::OutOfSampleHorizonMismatch {
                expected_nanos,
                actual_nanos,
            },
            ProposalUnavailableReason::FinancialModelValuationMismatch => {
                Self::FinancialModelValuationMismatch
            }
            ProposalUnavailableReason::OutOfSampleBacktestMismatch => {
                Self::OutOfSampleBacktestMismatch
            }
            ProposalUnavailableReason::HistoricalStudyBasisNotAllowed { actual } => {
                Self::HistoricalStudyBasisNotAllowed { actual }
            }
            ProposalUnavailableReason::InsufficientForecastOutcomes { required, actual } => {
                Self::InsufficientForecastOutcomes {
                    required: required.get(),
                    actual: actual.get(),
                }
            }
            ProposalUnavailableReason::UnsupportedForecastCoverage {
                minimum_ppm,
                maximum_ppm,
                actual_ppm,
            } => Self::UnsupportedForecastCoverage {
                minimum_ppm,
                maximum_ppm,
                actual_ppm,
            },
            ProposalUnavailableReason::ForecastCalibrationBelowPolicy {
                minimum_realized_ppm,
                maximum_error_ppm,
                nominal_ppm,
                realized_ppm,
            } => Self::ForecastCalibrationBelowPolicy {
                minimum_realized_ppm,
                maximum_error_ppm,
                nominal_ppm,
                realized_ppm,
            },
            ProposalUnavailableReason::InsufficientBacktestObservations { required, actual } => {
                Self::InsufficientBacktestObservations {
                    required: required.get(),
                    actual: actual.get(),
                }
            }
            ProposalUnavailableReason::InsufficientBacktestTrials { required, actual } => {
                Self::InsufficientBacktestTrials {
                    required: required.get(),
                    actual: actual.get(),
                }
            }
            ProposalUnavailableReason::ReservedPortfolioRevision => Self::ReservedPortfolioRevision,
        }
    }
}

impl ProposalUnavailableReasonWire {
    fn decode(self) -> Result<ProposalUnavailableReason, DecisionApplicationError> {
        Ok(match self {
            Self::UnprovenCurrentShareUnits => ProposalUnavailableReason::UnprovenCurrentShareUnits,
            Self::MissingEvidence { evidence } => {
                ProposalUnavailableReason::MissingEvidence(evidence.into())
            }
            Self::InstrumentMismatch {
                evidence,
                expected,
                actual,
            } => ProposalUnavailableReason::InstrumentMismatch {
                evidence: evidence.into(),
                expected,
                actual,
            },
            Self::CurrencyMismatch {
                evidence,
                expected,
                actual,
            } => ProposalUnavailableReason::CurrencyMismatch {
                evidence: evidence.into(),
                expected,
                actual,
            },
            Self::AccountMismatch { expected, actual } => {
                ProposalUnavailableReason::AccountMismatch { expected, actual }
            }
            Self::NotAvailableAtCutoff { evidence } => {
                ProposalUnavailableReason::NotAvailableAtCutoff(evidence.into())
            }
            Self::ExpiredEvidence { evidence } => {
                ProposalUnavailableReason::ExpiredEvidence(evidence.into())
            }
            Self::StaleEvidence { evidence } => {
                ProposalUnavailableReason::StaleEvidence(evidence.into())
            }
            Self::RejectedQuality { evidence, quality } => {
                ProposalUnavailableReason::RejectedQuality {
                    evidence: evidence.into(),
                    quality,
                }
            }
            Self::ForecastHorizonMismatch { expected, actual } => {
                ProposalUnavailableReason::ForecastHorizonMismatch { expected, actual }
            }
            Self::ValuationHorizonMismatch { expected, actual } => {
                ProposalUnavailableReason::ValuationHorizonMismatch { expected, actual }
            }
            Self::FinancialModelHorizonMismatch { expected, actual } => {
                ProposalUnavailableReason::FinancialModelHorizonMismatch { expected, actual }
            }
            Self::BacktestHorizonMismatch {
                expected_nanos,
                actual_nanos,
            } => ProposalUnavailableReason::BacktestHorizonMismatch {
                expected_nanos,
                actual_nanos,
            },
            Self::OutOfSampleHorizonMismatch {
                expected_nanos,
                actual_nanos,
            } => ProposalUnavailableReason::OutOfSampleHorizonMismatch {
                expected_nanos,
                actual_nanos,
            },
            Self::FinancialModelValuationMismatch => {
                ProposalUnavailableReason::FinancialModelValuationMismatch
            }
            Self::OutOfSampleBacktestMismatch => {
                ProposalUnavailableReason::OutOfSampleBacktestMismatch
            }
            Self::HistoricalStudyBasisNotAllowed { actual } => {
                ProposalUnavailableReason::HistoricalStudyBasisNotAllowed { actual }
            }
            Self::InsufficientForecastOutcomes { required, actual } => {
                ProposalUnavailableReason::InsufficientForecastOutcomes {
                    required: nonzero(required)?,
                    actual: nonzero(actual)?,
                }
            }
            Self::UnsupportedForecastCoverage {
                minimum_ppm,
                maximum_ppm,
                actual_ppm,
            } => ProposalUnavailableReason::UnsupportedForecastCoverage {
                minimum_ppm,
                maximum_ppm,
                actual_ppm,
            },
            Self::ForecastCalibrationBelowPolicy {
                minimum_realized_ppm,
                maximum_error_ppm,
                nominal_ppm,
                realized_ppm,
            } => ProposalUnavailableReason::ForecastCalibrationBelowPolicy {
                minimum_realized_ppm,
                maximum_error_ppm,
                nominal_ppm,
                realized_ppm,
            },
            Self::InsufficientBacktestObservations { required, actual } => {
                ProposalUnavailableReason::InsufficientBacktestObservations {
                    required: nonzero(required)?,
                    actual: nonzero(actual)?,
                }
            }
            Self::InsufficientBacktestTrials { required, actual } => {
                ProposalUnavailableReason::InsufficientBacktestTrials {
                    required: nonzero(required)?,
                    actual: nonzero(actual)?,
                }
            }
            Self::ReservedPortfolioRevision => ProposalUnavailableReason::ReservedPortfolioRevision,
        })
    }
}

fn nonzero(value: u32) -> Result<NonZeroU32, DecisionApplicationError> {
    NonZeroU32::new(value).ok_or(DecisionApplicationError::InvalidPersistentState)
}

fn analysis_key(bytes: [u8; 32]) -> Result<String, DecisionApplicationError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut key = String::new();
    key.try_reserve_exact(64)
        .map_err(|_error| DecisionApplicationError::Allocation)?;
    for byte in bytes {
        key.push(char::from(HEX[usize::from(byte >> 4)]));
        key.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(key)
}

fn invalid_state<E>(_error: E) -> DecisionApplicationError {
    DecisionApplicationError::InvalidPersistentState
}

/// Decimal strings keep nanosecond durations exact through Desktop JSON transports.
mod exact_nanoseconds {
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
        let text = String::deserialize(deserializer)?;
        let value = text.parse::<i64>().map_err(serde::de::Error::custom)?;
        if value.to_string() != text {
            return Err(serde::de::Error::custom(
                "duration must be a canonical exact integer",
            ));
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct InvestmentProbabilityWire {
    instrument_id: InstrumentId,
    horizon_nanos: NonZeroU64,
    price_higher: ProbabilityEventWire,
    benchmark_outperformance: ProbabilityEventWire,
    profit_after_costs: ProbabilityEventWire,
    digest: EvidenceDigest,
}
impl From<&InvestmentProbabilityEvidence> for InvestmentProbabilityWire {
    fn from(value: &InvestmentProbabilityEvidence) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            horizon_nanos: value.horizon_nanos(),
            price_higher: value.price_higher().into(),
            benchmark_outperformance: value.benchmark_outperformance().into(),
            profit_after_costs: value.profit_after_costs().into(),
            digest: value.digest().evidence_digest(),
        }
    }
}
impl InvestmentProbabilityWire {
    fn decode(self) -> Result<InvestmentProbabilityEvidence, DecisionApplicationError> {
        let value = InvestmentProbabilityEvidence::try_new(
            self.instrument_id,
            self.horizon_nanos,
            self.price_higher.decode()?,
            self.benchmark_outperformance.decode()?,
            self.profit_after_costs.decode()?,
        )
        .map_err(invalid_state)?;
        if value.digest().evidence_digest() != self.digest {
            return Err(invalid_state(()));
        }
        Ok(value)
    }
}
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProbabilityEventKindWire {
    PriceHigher,
    BenchmarkOutperformance,
    ProfitAfterCosts,
}
impl From<ProbabilityEventKind> for ProbabilityEventKindWire {
    fn from(v: ProbabilityEventKind) -> Self {
        match v {
            ProbabilityEventKind::PriceHigher => Self::PriceHigher,
            ProbabilityEventKind::BenchmarkOutperformance => Self::BenchmarkOutperformance,
            ProbabilityEventKind::ProfitAfterCosts => Self::ProfitAfterCosts,
        }
    }
}
impl From<ProbabilityEventKindWire> for ProbabilityEventKind {
    fn from(v: ProbabilityEventKindWire) -> Self {
        match v {
            ProbabilityEventKindWire::PriceHigher => Self::PriceHigher,
            ProbabilityEventKindWire::BenchmarkOutperformance => Self::BenchmarkOutperformance,
            ProbabilityEventKindWire::ProfitAfterCosts => Self::ProfitAfterCosts,
        }
    }
}
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProbabilityUnavailableWire {
    ForecastEvidenceUnavailable,
    SourceEvidenceUnavailable,
    BenchmarkEvidenceUnavailable,
    CostEvidenceUnavailable,
    CalibrationUnavailable,
    HorizonMismatch,
    OriginMismatch,
    ForecastExpired,
    ForecastFailed,
}
impl From<ProbabilityUnavailableReason> for ProbabilityUnavailableWire {
    fn from(v: ProbabilityUnavailableReason) -> Self {
        match v {
            ProbabilityUnavailableReason::ForecastEvidenceUnavailable => {
                Self::ForecastEvidenceUnavailable
            }
            ProbabilityUnavailableReason::SourceEvidenceUnavailable => {
                Self::SourceEvidenceUnavailable
            }
            ProbabilityUnavailableReason::BenchmarkEvidenceUnavailable => {
                Self::BenchmarkEvidenceUnavailable
            }
            ProbabilityUnavailableReason::CostEvidenceUnavailable => Self::CostEvidenceUnavailable,
            ProbabilityUnavailableReason::CalibrationUnavailable => Self::CalibrationUnavailable,
            ProbabilityUnavailableReason::HorizonMismatch => Self::HorizonMismatch,
            ProbabilityUnavailableReason::OriginMismatch => Self::OriginMismatch,
            ProbabilityUnavailableReason::ForecastExpired => Self::ForecastExpired,
            ProbabilityUnavailableReason::ForecastFailed => Self::ForecastFailed,
        }
    }
}
impl From<ProbabilityUnavailableWire> for ProbabilityUnavailableReason {
    fn from(v: ProbabilityUnavailableWire) -> Self {
        match v {
            ProbabilityUnavailableWire::ForecastEvidenceUnavailable => {
                Self::ForecastEvidenceUnavailable
            }
            ProbabilityUnavailableWire::SourceEvidenceUnavailable => {
                Self::SourceEvidenceUnavailable
            }
            ProbabilityUnavailableWire::BenchmarkEvidenceUnavailable => {
                Self::BenchmarkEvidenceUnavailable
            }
            ProbabilityUnavailableWire::CostEvidenceUnavailable => Self::CostEvidenceUnavailable,
            ProbabilityUnavailableWire::CalibrationUnavailable => Self::CalibrationUnavailable,
            ProbabilityUnavailableWire::HorizonMismatch => Self::HorizonMismatch,
            ProbabilityUnavailableWire::OriginMismatch => Self::OriginMismatch,
            ProbabilityUnavailableWire::ForecastExpired => Self::ForecastExpired,
            ProbabilityUnavailableWire::ForecastFailed => Self::ForecastFailed,
        }
    }
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum ProbabilityEventWire {
    Ready {
        evidence: ProbabilityForecastWire,
    },
    Unavailable {
        kind: ProbabilityEventKindWire,
        #[serde(deserialize_with = "RequiredOption::deserialize")]
        target: RequiredOption<ProbabilityTargetWire>,
        #[serde(deserialize_with = "RequiredOption::deserialize")]
        reference: RequiredOption<ProbabilityReferenceWire>,
        reason: ProbabilityUnavailableWire,
    },
}
impl From<&ProbabilityEventEvidence> for ProbabilityEventWire {
    fn from(v: &ProbabilityEventEvidence) -> Self {
        match v {
            ProbabilityEventEvidence::Ready(value) => Self::Ready {
                evidence: value.record().into(),
            },
            ProbabilityEventEvidence::Unavailable {
                kind,
                target,
                reference,
                reason,
            } => Self::Unavailable {
                kind: (*kind).into(),
                target: RequiredOption(target.map(ProbabilityTargetWire)),
                reference: RequiredOption(reference.map(Into::into)),
                reason: (*reason).into(),
            },
        }
    }
}
impl ProbabilityEventWire {
    fn decode(self) -> Result<ProbabilityEventEvidence, DecisionApplicationError> {
        Ok(match self {
            Self::Ready { evidence } => ProbabilityEventEvidence::Ready(evidence.decode()?),
            Self::Unavailable {
                kind,
                target,
                reference,
                reason,
            } => ProbabilityEventEvidence::Unavailable {
                kind: kind.into(),
                target: target.0.map(|v| v.0),
                reference: reference
                    .0
                    .map(ProbabilityReferenceWire::decode)
                    .transpose()?,
                reason: reason.into(),
            },
        })
    }
}

/// Serialize the existing typed target directly; invalid variants fail instead of receiving defaults.
#[derive(Clone, Debug, PartialEq)]
struct ProbabilityTargetWire(ForecastTargetMeaning);
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProbabilityTargetFields {
    horizon_nanos: NonZeroU64,
    origin_basis: market_squawk_data::FixedHorizonOriginBasis,
    event: market_squawk_data::ProbabilityEventTarget,
}
impl Serialize for ProbabilityTargetWire {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            ForecastTargetMeaning::FixedHorizonEvent {
                horizon_nanos,
                origin_basis,
                event,
            } => ProbabilityTargetFields {
                horizon_nanos,
                origin_basis,
                event,
            }
            .serialize(serializer),
            _ => Err(serde::ser::Error::custom(
                "probability target must be an original fixed-horizon event",
            )),
        }
    }
}
impl<'de> Deserialize<'de> for ProbabilityTargetWire {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let fields = ProbabilityTargetFields::deserialize(deserializer)?;
        let target = ForecastTargetMeaning::FixedHorizonEvent {
            horizon_nanos: fields.horizon_nanos,
            origin_basis: fields.origin_basis,
            event: fields.event,
        };
        ProbabilityEventKind::from_target(target).map_err(serde::de::Error::custom)?;
        Ok(Self(target))
    }
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ProbabilityReferenceWire {
    job_id: [u8; 16],
    generation: NonZeroU64,
    forecast_token: [u8; 16],
    request_identity: EvidenceDigest,
    profile_identity: EvidenceDigest,
}
impl From<ProbabilityForecastReference> for ProbabilityReferenceWire {
    fn from(v: ProbabilityForecastReference) -> Self {
        Self {
            job_id: v.job_id(),
            generation: v.generation(),
            forecast_token: v.forecast_token(),
            request_identity: v.request_identity().evidence_digest(),
            profile_identity: v.profile_identity().evidence_digest(),
        }
    }
}
impl ProbabilityReferenceWire {
    fn decode(self) -> Result<ProbabilityForecastReference, DecisionApplicationError> {
        ProbabilityForecastReference::try_new(
            self.job_id,
            self.generation,
            self.forecast_token,
            content_digest(self.request_identity)?,
            content_digest(self.profile_identity)?,
        )
        .map_err(invalid_state)
    }
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ProbabilityForecastWire {
    reference: ProbabilityReferenceWire,
    instrument_id: InstrumentId,
    target: ProbabilityTargetWire,
    observed_at: Timestamp,
    target_at: Timestamp,
    probability_mantissa: i128,
    probability_scale: u8,
    vintage_id: [u8; 32],
    output_binding_identity: EvidenceDigest,
    metadata_identity: EvidenceDigest,
    model_artifact_identity: EvidenceDigest,
    training_run_identity: EvidenceDigest,
    forecast_artifact_identity: EvidenceDigest,
    source_feature_identity: EvidenceDigest,
    calibration: ProbabilityCalibrationWire,
    window: ProposalEvidenceWindowWire,
}
impl From<&ProbabilityForecastEvidenceRecord> for ProbabilityForecastWire {
    fn from(r: &ProbabilityForecastEvidenceRecord) -> Self {
        Self {
            reference: r.reference.into(),
            instrument_id: r.instrument_id,
            target: ProbabilityTargetWire(r.target),
            observed_at: r.observed_at,
            target_at: r.target_at,
            probability_mantissa: r.probability.mantissa(),
            probability_scale: r.probability.scale(),
            vintage_id: r.vintage_id.bytes(),
            output_binding_identity: r.output_binding_identity.evidence_digest(),
            metadata_identity: r.metadata_identity.evidence_digest(),
            model_artifact_identity: r.model_artifact_identity.evidence_digest(),
            training_run_identity: r.training_run_identity.evidence_digest(),
            forecast_artifact_identity: r.forecast_artifact_identity.evidence_digest(),
            source_feature_identity: r.source_feature_identity.evidence_digest(),
            calibration: (&r.calibration).into(),
            window: r.window.into(),
        }
    }
}
impl ProbabilityForecastWire {
    fn decode(self) -> Result<ProbabilityForecastEvidence, DecisionApplicationError> {
        ProbabilityForecastEvidence::try_recover(ProbabilityForecastEvidenceRecord {
            reference: self.reference.decode()?,
            instrument_id: self.instrument_id,
            target: self.target.0,
            observed_at: self.observed_at,
            target_at: self.target_at,
            probability: ForecastValue::try_new(self.probability_mantissa, self.probability_scale)
                .map_err(invalid_state)?,
            vintage_id: ProposalForecastVintageId::try_from_bytes(self.vintage_id)
                .map_err(invalid_state)?,
            output_binding_identity: content_digest(self.output_binding_identity)?,
            metadata_identity: content_digest(self.metadata_identity)?,
            model_artifact_identity: content_digest(self.model_artifact_identity)?,
            training_run_identity: content_digest(self.training_run_identity)?,
            forecast_artifact_identity: content_digest(self.forecast_artifact_identity)?,
            source_feature_identity: content_digest(self.source_feature_identity)?,
            calibration: self.calibration.decode()?,
            window: self.window.decode()?,
        })
        .map_err(invalid_state)
    }
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ProbabilityWindowWire {
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    start: RequiredOption<Timestamp>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    end: RequiredOption<Timestamp>,
    observations: NonZeroU32,
}
impl From<CalibrationWindow> for ProbabilityWindowWire {
    fn from(w: CalibrationWindow) -> Self {
        Self {
            start: RequiredOption(w.start()),
            end: RequiredOption(w.end()),
            observations: w.observations(),
        }
    }
}
impl ProbabilityWindowWire {
    fn decode(self) -> Result<CalibrationWindow, DecisionApplicationError> {
        CalibrationWindow::try_new(
            self.start.0.ok_or_else(|| invalid_state(()))?,
            self.end.0.ok_or_else(|| invalid_state(()))?,
            self.observations,
        )
        .map_err(invalid_state)
    }
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ProbabilityBinWire {
    count: u32,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    mean_probability_bits: RequiredOption<u64>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    observed_frequency_bits: RequiredOption<u64>,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ProbabilityCalibrationWire {
    policy_identity: EvidenceDigest,
    outcomes_identity: EvidenceDigest,
    train_window: ProbabilityWindowWire,
    calibration_window: ProbabilityWindowWire,
    evaluation_window: ProbabilityWindowWire,
    slope_bits: u64,
    intercept_bits: u64,
    brier_score_bits: u64,
    log_loss_bits: u64,
    reliability_bins: [ProbabilityBinWire; 10],
}
impl From<&ProbabilityCalibrationSummary> for ProbabilityCalibrationWire {
    fn from(v: &ProbabilityCalibrationSummary) -> Self {
        Self {
            policy_identity: v.policy_identity.evidence_digest(),
            outcomes_identity: v.outcomes_identity.evidence_digest(),
            train_window: v.train_window.into(),
            calibration_window: v.calibration_window.into(),
            evaluation_window: v.evaluation_window.into(),
            slope_bits: v.slope_bits,
            intercept_bits: v.intercept_bits,
            brier_score_bits: v.brier_score_bits,
            log_loss_bits: v.log_loss_bits,
            reliability_bins: std::array::from_fn(|i| ProbabilityBinWire {
                count: v.reliability_bins[i].count,
                mean_probability_bits: RequiredOption(v.reliability_bins[i].mean_probability_bits),
                observed_frequency_bits: RequiredOption(
                    v.reliability_bins[i].observed_frequency_bits,
                ),
            }),
        }
    }
}
impl ProbabilityCalibrationWire {
    fn decode(self) -> Result<ProbabilityCalibrationSummary, DecisionApplicationError> {
        Ok(ProbabilityCalibrationSummary {
            policy_identity: content_digest(self.policy_identity)?,
            outcomes_identity: content_digest(self.outcomes_identity)?,
            train_window: self.train_window.decode()?,
            calibration_window: self.calibration_window.decode()?,
            evaluation_window: self.evaluation_window.decode()?,
            slope_bits: self.slope_bits,
            intercept_bits: self.intercept_bits,
            brier_score_bits: self.brier_score_bits,
            log_loss_bits: self.log_loss_bits,
            reliability_bins: self
                .reliability_bins
                .map(|v| ProbabilityReliabilityEvidence {
                    count: v.count,
                    mean_probability_bits: v.mean_probability_bits.0,
                    observed_frequency_bits: v.observed_frequency_bits.0,
                }),
        })
    }
}
