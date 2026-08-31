use std::{num::NonZeroU32, str::FromStr as _};

use market_squawk_decisions::{
    CostAdjustedPitBacktestEvidence, ForecastCalibrationSummary, ForecastPriceRanges,
    InvestmentAnalysisEvidence, InvestmentAnalysisEvidenceInput, InvestmentAnalysisId,
    InvestmentProposalAuthority, InvestmentProposalDecision, InvestmentProposalId,
    LiquidityEvidence, MarketReferenceAdjustmentBasis, MarketReferenceEvidence,
    MarketReferencePriceKind, PortfolioPositionState, PortfolioRiskEvidence, PriceForecastEvidence,
    ProposalEvidenceWindow, ProposalForecastVintageId, ProposalUnavailableReason,
    RecommendationDerivationDigest, RecommendationEvidenceKind, RecommendationPolicy,
    RecommendationPolicyDigest, SelectedCandidateAnalysisEvidence, TargetPriceCases,
    TargetPriceRange, ValuationEvidence,
};
use market_squawk_domain::{
    AccountId, BasisPoints, Currency, DataQuality, EvidenceDigest, InstrumentId, Money, Timestamp,
};
use market_squawk_modeling::ForecastCentralStatistic;
use market_squawk_portfolio::PortfolioRevisionToken;
use market_squawk_valuation::{
    DecisionId, FairValueSelectionReceiptHash, MeasurementId, ValuationAmountBasis,
};
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

impl From<&InvestmentProposalDecision> for InvestmentProposalWire {
    fn from(value: &InvestmentProposalDecision) -> Self {
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
        Self {
            policy: value.policy().into(),
            evidence: value.evidence().into(),
            outcome,
        }
    }
}

impl InvestmentProposalWire {
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
}

impl From<&RecommendationPolicy> for RecommendationPolicyWire {
    fn from(value: &RecommendationPolicy) -> Self {
        Self {
            version: value.version().get(),
            digest: value.digest().bytes(),
        }
    }
}

impl RecommendationPolicyWire {
    fn decode(self) -> Result<RecommendationPolicy, DecisionApplicationError> {
        RecommendationPolicy::try_recover(
            NonZeroU32::new(self.version)
                .ok_or(DecisionApplicationError::InvalidPersistentState)?,
            RecommendationPolicyDigest::try_from_bytes(self.digest).map_err(invalid_state)?,
        )
        .map_err(invalid_state)
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
    instrument_id: InstrumentId,
    currency: Currency,
    account_id: AccountId,
    as_of: Timestamp,
    market: RequiredOption<MarketReferenceEvidenceWire>,
    price_forecast: RequiredOption<PriceForecastEvidenceWire>,
    valuation: RequiredOption<ValuationEvidenceWire>,
    backtest: RequiredOption<CostAdjustedPitBacktestEvidenceWire>,
    liquidity: RequiredOption<LiquidityEvidenceWire>,
    portfolio_risk: RequiredOption<PortfolioRiskEvidenceWire>,
}

impl From<&InvestmentAnalysisEvidence> for InvestmentAnalysisEvidenceWire {
    fn from(value: &InvestmentAnalysisEvidence) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            currency: value.currency(),
            account_id: value.account_id(),
            as_of: value.as_of(),
            market: RequiredOption(value.market().map(Into::into)),
            price_forecast: RequiredOption(value.price_forecast().map(Into::into)),
            valuation: RequiredOption(value.valuation().map(Into::into)),
            backtest: RequiredOption(value.backtest().map(Into::into)),
            liquidity: RequiredOption(value.liquidity().map(Into::into)),
            portfolio_risk: RequiredOption(value.portfolio_risk().map(Into::into)),
        }
    }
}

impl InvestmentAnalysisEvidenceWire {
    fn decode(self) -> Result<InvestmentAnalysisEvidence, DecisionApplicationError> {
        Ok(InvestmentAnalysisEvidence::new(
            InvestmentAnalysisEvidenceInput {
                instrument_id: self.instrument_id,
                currency: self.currency,
                account_id: self.account_id,
                as_of: self.as_of,
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
                backtest: self
                    .backtest
                    .0
                    .map(CostAdjustedPitBacktestEvidenceWire::decode)
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
            },
        ))
    }
}

/// Nullable on the wire, but never omittable from the strict current evidence envelope.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(transparent)]
struct RequiredOption<T>(Option<T>);

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
    available_at: Timestamp,
    expires_at: Timestamp,
    content_identity: EvidenceDigest,
}

impl From<ProposalEvidenceWindow> for ProposalEvidenceWindowWire {
    fn from(value: ProposalEvidenceWindow) -> Self {
        Self {
            observed_at: value.observed_at(),
            available_at: value.available_at(),
            expires_at: value.expires_at(),
            content_identity: value.content_identity().evidence_digest(),
        }
    }
}

impl ProposalEvidenceWindowWire {
    fn decode(self) -> Result<ProposalEvidenceWindow, DecisionApplicationError> {
        ProposalEvidenceWindow::try_new(
            self.observed_at,
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
    classification_decision_id: String,
    selection_receipt_hash: String,
    window: ProposalEvidenceWindowWire,
}

impl From<&ValuationEvidence> for ValuationEvidenceWire {
    fn from(value: &ValuationEvidence) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            fair_value: value.fair_value(),
            basis: value.basis().into(),
            horizon_at: value.horizon_at(),
            measurement_id: value.measurement_id().to_string(),
            classification_decision_id: value.classification_decision_id().to_string(),
            selection_receipt_hash: value.selection_receipt_hash().to_string(),
            window: value.window().into(),
        }
    }
}

impl ValuationEvidenceWire {
    fn decode(self) -> Result<ValuationEvidence, DecisionApplicationError> {
        ValuationEvidence::try_recover_receipt_bound_projection(
            self.instrument_id,
            self.fair_value,
            self.basis.into(),
            self.horizon_at,
            MeasurementId::from_str(&self.measurement_id).map_err(invalid_state)?,
            DecisionId::from_str(&self.classification_decision_id).map_err(invalid_state)?,
            FairValueSelectionReceiptHash::from_str(&self.selection_receipt_hash)
                .map_err(invalid_state)?,
            self.window.decode()?,
        )
        .map_err(invalid_state)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ValuationAmountBasisWire {
    PerInstrumentUnit,
    ReportingEntityTotal,
    PositionTotal,
}

impl From<ValuationAmountBasis> for ValuationAmountBasisWire {
    fn from(value: ValuationAmountBasis) -> Self {
        match value {
            ValuationAmountBasis::PerInstrumentUnit => Self::PerInstrumentUnit,
            ValuationAmountBasis::ReportingEntityTotal => Self::ReportingEntityTotal,
            ValuationAmountBasis::PositionTotal => Self::PositionTotal,
        }
    }
}

impl From<ValuationAmountBasisWire> for ValuationAmountBasis {
    fn from(value: ValuationAmountBasisWire) -> Self {
        match value {
            ValuationAmountBasisWire::PerInstrumentUnit => Self::PerInstrumentUnit,
            ValuationAmountBasisWire::ReportingEntityTotal => Self::ReportingEntityTotal,
            ValuationAmountBasisWire::PositionTotal => Self::PositionTotal,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CostAdjustedPitBacktestEvidenceWire {
    instrument_id: InstrumentId,
    currency: Currency,
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

impl From<&CostAdjustedPitBacktestEvidence> for CostAdjustedPitBacktestEvidenceWire {
    fn from(value: &CostAdjustedPitBacktestEvidence) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            currency: value.currency(),
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

impl CostAdjustedPitBacktestEvidenceWire {
    fn decode(self) -> Result<CostAdjustedPitBacktestEvidence, DecisionApplicationError> {
        CostAdjustedPitBacktestEvidence::try_new(
            self.instrument_id,
            self.currency,
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
    capacity_ppm: u32,
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
            capacity_ppm: value.capacity_ppm(),
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
            self.capacity_ppm,
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
    Backtest,
    Liquidity,
    PortfolioRisk,
}

impl From<RecommendationEvidenceKind> for RecommendationEvidenceKindWire {
    fn from(value: RecommendationEvidenceKind) -> Self {
        match value {
            RecommendationEvidenceKind::Market => Self::Market,
            RecommendationEvidenceKind::PriceForecast => Self::PriceForecast,
            RecommendationEvidenceKind::Valuation => Self::Valuation,
            RecommendationEvidenceKind::Backtest => Self::Backtest,
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
            RecommendationEvidenceKindWire::Backtest => Self::Backtest,
            RecommendationEvidenceKindWire::Liquidity => Self::Liquidity,
            RecommendationEvidenceKindWire::PortfolioRisk => Self::PortfolioRisk,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ProposalUnavailableReasonWire {
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
    BacktestHorizonMismatch {
        expected_nanos: i64,
        actual_nanos: i64,
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
            ProposalUnavailableReason::BacktestHorizonMismatch {
                expected_nanos,
                actual_nanos,
            } => Self::BacktestHorizonMismatch {
                expected_nanos,
                actual_nanos,
            },
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
            Self::BacktestHorizonMismatch {
                expected_nanos,
                actual_nanos,
            } => ProposalUnavailableReason::BacktestHorizonMismatch {
                expected_nanos,
                actual_nanos,
            },
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
