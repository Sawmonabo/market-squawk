//! Pure evidence admission and deterministic recommendation derivation.

use market_squawk_domain::{
    BasisPoints, Currency, DataQuality, HistoricalStudyBasis, InstrumentId, Money, RoundingPolicy,
    Timestamp,
};

use crate::{DecisionContentDigest, TargetPriceCases, TargetPriceRange};
use sha2::{Digest as _, Sha256};

use super::digest::{
    hash_analysis, hash_evidence, hash_generated_derivation, hash_no_action_derivation,
    hash_policy, hash_proposal_id,
};
use super::evidence::{
    ChronologicalOutOfSampleEvidence, CostAdjustedBacktestEvidence, FinancialModelEvidence,
    ForecastPriceRanges, HarmonicPatternEvidenceReceipt, InvestmentAnalysisEvidence,
    LiquidityEvidence, MarketReferenceEvidence, PortfolioPositionState, PortfolioRiskEvidence,
    PriceForecastEvidence, ProposalEvidenceWindow, ValuationEvidence,
};
use super::output::{
    GeneratedInvestmentProposal, GeneratedPriceLadder, InvestmentProposalDecision,
    NoActionInvestmentProposal, NoActionReason, ProposalExecutionEligibility, ProposalInvalidator,
    ProposalUnavailableReason, RecommendationAction, UnavailableInvestmentAnalysis,
};
use super::policy::{
    RecommendationConfidence, RecommendationConfidenceComponent,
    RecommendationConfidenceComponentKind, RecommendationConfidenceComponentValue,
    RecommendationConfidenceMeaning, RecommendationConfidenceUnavailableReason,
    RecommendationPolicy, validate_policy,
};
use super::{
    CONFIDENCE_PARTS_PER_MILLION, InvestmentAnalysisId, InvestmentProposalError,
    InvestmentProposalId, RecommendationDerivationDigest, RecommendationEvidenceDigest,
    RecommendationEvidenceKind, RecommendationPolicyDigest,
};

struct AdmittedAlphaEvidence<'a> {
    market: &'a MarketReferenceEvidence,
    forecast: &'a PriceForecastEvidence,
    valuation: &'a ValuationEvidence,
    financial_model: &'a FinancialModelEvidence,
}

/// Non-circular historical entry result before study, account sizing, or execution admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecommendationAlphaDecision {
    /// A flat-position entry is inside the same financial entry zone used by full proposals.
    Entry,
    /// Admitted financial evidence does not support a flat-position entry.
    NoAction(NoActionReason),
    /// Required contemporaneous financial evidence was not admissible.
    Unavailable(ProposalUnavailableReason),
}

/// Exact pure financial gate evidence; it grants neither a recommendation nor execution authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecommendationAlphaEvaluation {
    decision: RecommendationAlphaDecision,
    price_ladder: Option<GeneratedPriceLadder>,
    digest: DecisionContentDigest,
}

impl RecommendationAlphaEvaluation {
    /// Returns the historical-entry conclusion.
    #[must_use]
    pub const fn decision(self) -> RecommendationAlphaDecision {
        self.decision
    }
    /// Returns the exact economic entry/action zones when calculation was admitted.
    #[must_use]
    pub const fn price_ladder(self) -> Option<GeneratedPriceLadder> {
        self.price_ladder
    }
    /// Returns the versioned complete policy/evidence-bound gate identity.
    #[must_use]
    pub const fn digest(self) -> DecisionContentDigest {
        self.digest
    }
}

struct AdmittedEvidence<'a> {
    market: &'a MarketReferenceEvidence,
    forecast: &'a PriceForecastEvidence,
    valuation: &'a ValuationEvidence,
    financial_model: &'a FinancialModelEvidence,
    backtest: &'a CostAdjustedBacktestEvidence,
    out_of_sample: &'a ChronologicalOutOfSampleEvidence,
    harmonic_pattern: Option<&'a HarmonicPatternEvidenceReceipt>,
    liquidity: &'a LiquidityEvidence,
    portfolio_risk: &'a PortfolioRiskEvidence,
}

/// Pure deterministic authority that derives research proposals from admitted evidence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InvestmentProposalAuthority;

impl InvestmentProposalAuthority {
    /// Calculates research entry zones from monetary values under the shared financial policy.
    ///
    /// This is only the arithmetic kernel. It does not admit sources, calibration, historical
    /// cutoffs, model selection, valuation approval, recommendations, or execution. Live callers
    /// must first pass the complete existing evidence gate; retrospective callers must establish
    /// their own historical source and calculation authority before using these values.
    pub fn calculate_research_entry_zones(
        market: Money,
        forecast_cases: TargetPriceCases,
        forecast_ranges: ForecastPriceRanges,
        valuation: Money,
        policy: &RecommendationPolicy,
    ) -> Result<(RecommendationAlphaDecision, Option<GeneratedPriceLadder>), InvestmentProposalError>
    {
        validate_policy(&policy.semantics)?;
        if RecommendationPolicyDigest::try_from_bytes(hash_policy(&policy.semantics))?
            != policy.digest
        {
            return Err(InvestmentProposalError::PolicyIdentityMismatch);
        }
        if market.amount() <= rust_decimal::Decimal::ZERO
            || valuation.amount() <= rust_decimal::Decimal::ZERO
            || market.currency() != valuation.currency()
            || market.currency() != forecast_cases.base().currency()
            || market.currency() != forecast_ranges.base.lower().currency()
            || forecast_cases.downside().amount() >= forecast_cases.base().amount()
            || forecast_cases.base().amount() >= forecast_cases.upside().amount()
            || !(forecast_ranges.downside.lower().amount()
                ..=forecast_ranges.downside.upper().amount())
                .contains(&forecast_cases.downside().amount())
            || !(forecast_ranges.base.lower().amount()..=forecast_ranges.base.upper().amount())
                .contains(&forecast_cases.base().amount())
            || !(forecast_ranges.upside.lower().amount()..=forecast_ranges.upside.upper().amount())
                .contains(&forecast_cases.upside().amount())
        {
            return Err(InvestmentProposalError::InvalidPrice);
        }
        if forecast_valuation_conflict(market, forecast_cases.base(), valuation, policy)? {
            return Ok((
                RecommendationAlphaDecision::NoAction(
                    NoActionReason::ConflictingForecastAndValuation,
                ),
                None,
            ));
        }
        let derived_base = blend(
            forecast_cases.base(),
            policy.semantics.forecast_base_weight_bps,
            valuation,
            policy.semantics.valuation_weight_bps,
            policy,
        )?;
        match generate_price_ladder(
            forecast_cases,
            forecast_ranges,
            valuation,
            policy,
            derived_base,
        ) {
            Ok(ladder) => {
                let mark = market.amount();
                let decision = if mark > ladder.exit_range.upper().amount()
                    && mark <= ladder.entry_range.upper().amount()
                {
                    RecommendationAlphaDecision::Entry
                } else {
                    RecommendationAlphaDecision::NoAction(
                        NoActionReason::PositionStateNotActionable,
                    )
                };
                Ok((decision, Some(ladder)))
            }
            Err(InvestmentProposalError::InvalidPrice) => Ok((
                RecommendationAlphaDecision::NoAction(NoActionReason::GeneratedPriceOrderCollapsed),
                None,
            )),
            Err(error) => Err(error),
        }
    }

    /// Evaluates the shared market, forecast, and governed-model entry gate without a backtest.
    /// This uses current recommendation freshness and admission. Retrospective studies need
    /// separate historical source-selection authority and must retain actual calculation clocks.
    pub fn evaluate_alpha(
        evidence: &InvestmentAnalysisEvidence,
        policy: &RecommendationPolicy,
    ) -> Result<RecommendationAlphaEvaluation, InvestmentProposalError> {
        if evidence
            .valuation
            .is_some_and(|value| !value.is_source_authenticated())
        {
            return Err(InvestmentProposalError::InvalidValuationSelection);
        }
        Self::reproduce_alpha(evidence, policy)
    }

    // Deterministic shared calculation only. Public alpha and proposal generation authenticate
    // fresh sources first; saved proposal recovery separately verifies every retained identity.
    fn reproduce_alpha(
        evidence: &InvestmentAnalysisEvidence,
        policy: &RecommendationPolicy,
    ) -> Result<RecommendationAlphaEvaluation, InvestmentProposalError> {
        if evidence.admitted_at < evidence.as_of {
            return Err(InvestmentProposalError::InvalidTimeOrder);
        }
        validate_policy(&policy.semantics)?;
        if RecommendationPolicyDigest::try_from_bytes(hash_policy(&policy.semantics))?
            != policy.digest
        {
            return Err(InvestmentProposalError::PolicyIdentityMismatch);
        }
        let target = evidence
            .price_forecast
            .as_ref()
            .map_or(evidence.as_of, |forecast| forecast.window.observed_at)
            .checked_add_nanos(policy.horizon_nanos())
            .map_err(|_| InvestmentProposalError::ArithmeticOverflow)?;
        let (decision, mut price_ladder) = match admit_alpha_evidence(evidence, policy, target) {
            Err(reason) => (RecommendationAlphaDecision::Unavailable(reason), None),
            Ok(alpha) => Self::calculate_research_entry_zones(
                alpha.market.price,
                alpha.forecast.cases,
                alpha.forecast.ranges,
                alpha.valuation.fair_value,
                policy,
            )?,
        };
        if let Some(ladder) = price_ladder.as_mut() {
            ladder.current_share_basis = evidence
                .current_share_projection
                .as_ref()
                .map(|value| value.identity());
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/recommendation-alpha-gate/v1\0");
        hash.update(policy.digest.bytes());
        hash.update(hash_evidence(evidence));
        let digest = DecisionContentDigest::try_new(market_squawk_domain::EvidenceDigest::new(
            market_squawk_domain::DigestAlgorithm::Sha256,
            hash.finalize().into(),
        ))
        .map_err(|_| InvestmentProposalError::ReservedIdentity)?;
        Ok(RecommendationAlphaEvaluation {
            decision,
            price_ladder,
            digest,
        })
    }

    /// Generates a research-only proposal, typed no-action, or typed unavailable result.
    ///
    /// Callers supply evidence and select a code-owned policy only. They cannot supply the action,
    /// price ladder, confidence, identities, assumptions, invalidators, or limitations.
    ///
    /// # Errors
    ///
    /// Returns an error only for representational arithmetic, an internally invalid policy, or a
    /// cryptographic reserved sentinel. Evidence failures are retained in the returned decision.
    pub fn generate(
        evidence: InvestmentAnalysisEvidence,
        policy: RecommendationPolicy,
    ) -> Result<InvestmentProposalDecision, InvestmentProposalError> {
        if evidence
            .valuation
            .is_some_and(|value| !value.is_source_authenticated())
        {
            return Err(InvestmentProposalError::InvalidValuationSelection);
        }
        Self::reproduce(evidence, policy)
    }

    fn reproduce(
        evidence: InvestmentAnalysisEvidence,
        policy: RecommendationPolicy,
    ) -> Result<InvestmentProposalDecision, InvestmentProposalError> {
        if evidence.admitted_at < evidence.as_of {
            return Err(InvestmentProposalError::InvalidTimeOrder);
        }
        validate_policy(&policy.semantics)?;
        if RecommendationPolicyDigest::try_from_bytes(hash_policy(&policy.semantics))?
            != policy.digest
        {
            return Err(InvestmentProposalError::PolicyIdentityMismatch);
        }

        let evidence_digest =
            RecommendationEvidenceDigest::try_from_bytes(hash_evidence(&evidence))?;
        let analysis_id = InvestmentAnalysisId::try_from_bytes(hash_analysis(
            policy.digest,
            evidence_digest,
            evidence.instrument_id,
            evidence.account_id,
            evidence.as_of,
        ))?;
        let horizon_at = evidence
            .price_forecast
            .as_ref()
            .map_or(evidence.as_of, |forecast| forecast.window.observed_at)
            .checked_add_nanos(policy.semantics.horizon_nanos)
            .map_err(|_| InvestmentProposalError::ArithmeticOverflow)?;
        let expires_at = evidence
            .admitted_at
            .checked_add_nanos(policy.semantics.proposal_lifetime_nanos)
            .map_err(|_| InvestmentProposalError::ArithmeticOverflow)?;

        let admitted = match admit_evidence(&evidence, &policy, horizon_at) {
            Ok(admitted) => admitted,
            Err(reason) => {
                return Ok(InvestmentProposalDecision::Unavailable(
                    UnavailableInvestmentAnalysis {
                        analysis_id,
                        policy,
                        evidence,
                        evidence_digest,
                        reason,
                        horizon_at,
                        expires_at,
                        execution_eligibility:
                            ProposalExecutionEligibility::ResearchOnlyExecutionIneligible,
                    },
                ));
            }
        };
        let expires_at = effective_proposal_expiry(&admitted, &policy, expires_at)?;

        let alpha = Self::reproduce_alpha(&evidence, &policy)?;
        let evidence_conflicts = matches!(
            alpha.decision(),
            RecommendationAlphaDecision::NoAction(NoActionReason::ConflictingForecastAndValuation)
        );
        let prospective_action = alpha
            .price_ladder()
            .and_then(|ladder| select_action(&admitted, ladder));
        let confidence =
            recommendation_confidence(&admitted, &policy, evidence_conflicts, prospective_action)?;
        if evidence_conflicts {
            return no_action(
                evidence,
                policy,
                analysis_id,
                evidence_digest,
                confidence,
                horizon_at,
                expires_at,
                NoActionReason::ConflictingForecastAndValuation,
                ProposalInvalidator::ForecastValuationConflict,
            );
        }
        if admitted.backtest.net_return < policy.semantics.minimum_cost_adjusted_return
            || admitted.backtest.max_drawdown > policy.semantics.maximum_backtest_drawdown
            || admitted.backtest.stability_ppm < policy.semantics.minimum_backtest_stability_ppm
        {
            return no_action(
                evidence,
                policy,
                analysis_id,
                evidence_digest,
                confidence,
                horizon_at,
                expires_at,
                NoActionReason::BacktestBelowPolicy,
                ProposalInvalidator::BacktestPolicyBreach,
            );
        }
        if admitted.out_of_sample.completed_observations()
            < policy.semantics.minimum_backtest_observations
            || admitted.out_of_sample.fold_count() < policy.semantics.minimum_backtest_trials
            || admitted.out_of_sample.completion_coverage_ppm()
                < policy.semantics.minimum_oos_completion_coverage_ppm
        {
            return no_action(
                evidence,
                policy,
                analysis_id,
                evidence_digest,
                confidence,
                horizon_at,
                expires_at,
                NoActionReason::OutOfSampleBelowPolicy,
                ProposalInvalidator::OutOfSamplePolicyBreach,
            );
        }
        if admitted.portfolio_risk.risk_capacity_ppm
            < policy.semantics.minimum_portfolio_risk_capacity_ppm
        {
            return no_action(
                evidence,
                policy,
                analysis_id,
                evidence_digest,
                confidence,
                horizon_at,
                expires_at,
                NoActionReason::PortfolioRiskBelowPolicy,
                ProposalInvalidator::PortfolioRiskPolicyBreach,
            );
        }
        let price_ladder = match alpha.price_ladder() {
            Some(ladder) => ladder,
            None => {
                return no_action(
                    evidence,
                    policy,
                    analysis_id,
                    evidence_digest,
                    confidence,
                    horizon_at,
                    expires_at,
                    NoActionReason::GeneratedPriceOrderCollapsed,
                    ProposalInvalidator::GeneratedPriceOrderCollapsed,
                );
            }
        };
        let action = match select_action(&admitted, price_ladder) {
            Some(action) => action,
            None => {
                return no_action(
                    evidence,
                    policy,
                    analysis_id,
                    evidence_digest,
                    confidence,
                    horizon_at,
                    expires_at,
                    NoActionReason::PositionStateNotActionable,
                    ProposalInvalidator::PositionStateIncompatible,
                );
            }
        };

        match action_liquidity_capacity(admitted.liquidity, action) {
            Err(_) => {
                return no_action(
                    evidence,
                    policy,
                    analysis_id,
                    evidence_digest,
                    confidence,
                    horizon_at,
                    expires_at,
                    NoActionReason::LiquidityCapacityUnavailable,
                    ProposalInvalidator::LiquidityPolicyBreach,
                );
            }
            Ok(Some(capacity))
                if admitted.liquidity.quoted_spread > policy.semantics.maximum_liquidity_spread
                    || capacity < policy.semantics.minimum_liquidity_capacity_ppm =>
            {
                return no_action(
                    evidence,
                    policy,
                    analysis_id,
                    evidence_digest,
                    confidence,
                    horizon_at,
                    expires_at,
                    NoActionReason::LiquidityBelowPolicy,
                    ProposalInvalidator::LiquidityPolicyBreach,
                );
            }
            Ok(_) => {}
        }
        let confidence_reason = match confidence.value_ppm {
            None => Some(NoActionReason::ConfidenceUnavailable),
            Some(value) if value < policy.semantics.minimum_confidence_ppm => {
                Some(NoActionReason::ConfidenceBelowPolicy)
            }
            Some(_) => None,
        };
        if let Some(reason) = confidence_reason {
            return no_action(
                evidence,
                policy,
                analysis_id,
                evidence_digest,
                confidence,
                horizon_at,
                expires_at,
                reason,
                ProposalInvalidator::ConfidencePolicyBreach,
            );
        }

        let derivation_digest =
            RecommendationDerivationDigest::try_from_bytes(hash_generated_derivation(
                analysis_id,
                action,
                price_ladder,
                confidence,
                horizon_at,
                expires_at,
            ))?;
        let proposal_id =
            InvestmentProposalId::try_from_bytes(hash_proposal_id(analysis_id, derivation_digest))?;
        Ok(InvestmentProposalDecision::Generated(
            GeneratedInvestmentProposal {
                analysis_id,
                proposal_id,
                policy,
                evidence,
                evidence_digest,
                derivation_digest,
                action,
                price_ladder,
                confidence,
                horizon_at,
                expires_at,
                execution_eligibility:
                    ProposalExecutionEligibility::ResearchOnlyExecutionIneligible,
            },
        ))
    }

    /// Revalidates and reproduces a persisted generated proposal without accepting derived fields.
    ///
    /// # Errors
    ///
    /// Rejects a changed output kind or any analysis, derivation, or proposal identity mismatch.
    pub fn try_recover_generated(
        evidence: InvestmentAnalysisEvidence,
        policy: RecommendationPolicy,
        expected_analysis_id: InvestmentAnalysisId,
        expected_derivation_digest: RecommendationDerivationDigest,
        expected_proposal_id: InvestmentProposalId,
    ) -> Result<GeneratedInvestmentProposal, InvestmentProposalError> {
        match Self::reproduce(evidence, policy)? {
            InvestmentProposalDecision::Generated(proposal)
                if proposal.analysis_id == expected_analysis_id
                    && proposal.derivation_digest == expected_derivation_digest
                    && proposal.proposal_id == expected_proposal_id =>
            {
                Ok(proposal)
            }
            InvestmentProposalDecision::Generated(_) => {
                Err(InvestmentProposalError::ProposalIdentityMismatch)
            }
            InvestmentProposalDecision::NoAction(_)
            | InvestmentProposalDecision::Unavailable(_) => {
                Err(InvestmentProposalError::ProposalKindMismatch)
            }
        }
    }

    /// Revalidates and reproduces a persisted no-action proposal without accepting derived fields.
    ///
    /// # Errors
    ///
    /// Rejects a changed output kind or any analysis, derivation, or proposal identity mismatch.
    pub fn try_recover_no_action(
        evidence: InvestmentAnalysisEvidence,
        policy: RecommendationPolicy,
        expected_analysis_id: InvestmentAnalysisId,
        expected_derivation_digest: RecommendationDerivationDigest,
        expected_proposal_id: InvestmentProposalId,
    ) -> Result<NoActionInvestmentProposal, InvestmentProposalError> {
        match Self::reproduce(evidence, policy)? {
            InvestmentProposalDecision::NoAction(proposal)
                if proposal.analysis_id == expected_analysis_id
                    && proposal.derivation_digest == expected_derivation_digest
                    && proposal.proposal_id == expected_proposal_id =>
            {
                Ok(proposal)
            }
            InvestmentProposalDecision::NoAction(_) => {
                Err(InvestmentProposalError::ProposalIdentityMismatch)
            }
            InvestmentProposalDecision::Generated(_)
            | InvestmentProposalDecision::Unavailable(_) => {
                Err(InvestmentProposalError::ProposalKindMismatch)
            }
        }
    }

    /// Revalidates and reproduces a persisted unavailable analysis.
    ///
    /// # Errors
    ///
    /// Rejects a changed output kind, unavailable reason, or analysis identity mismatch.
    pub fn try_recover_unavailable(
        evidence: InvestmentAnalysisEvidence,
        policy: RecommendationPolicy,
        expected_analysis_id: InvestmentAnalysisId,
        expected_reason: ProposalUnavailableReason,
    ) -> Result<UnavailableInvestmentAnalysis, InvestmentProposalError> {
        match Self::reproduce(evidence, policy)? {
            InvestmentProposalDecision::Unavailable(analysis)
                if analysis.analysis_id == expected_analysis_id
                    && analysis.reason == expected_reason =>
            {
                Ok(analysis)
            }
            InvestmentProposalDecision::Unavailable(_) => {
                Err(InvestmentProposalError::ProposalIdentityMismatch)
            }
            InvestmentProposalDecision::Generated(_) | InvestmentProposalDecision::NoAction(_) => {
                Err(InvestmentProposalError::ProposalKindMismatch)
            }
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "all persisted proposal identities, evidence, policy, confidence, and timing stay explicit"
)]
fn no_action(
    evidence: InvestmentAnalysisEvidence,
    policy: RecommendationPolicy,
    analysis_id: InvestmentAnalysisId,
    evidence_digest: RecommendationEvidenceDigest,
    confidence: RecommendationConfidence,
    horizon_at: Timestamp,
    expires_at: Timestamp,
    reason: NoActionReason,
    invalidator: ProposalInvalidator,
) -> Result<InvestmentProposalDecision, InvestmentProposalError> {
    let derivation_digest =
        RecommendationDerivationDigest::try_from_bytes(hash_no_action_derivation(
            analysis_id,
            reason,
            std::slice::from_ref(&invalidator),
            confidence,
            horizon_at,
            expires_at,
        ))?;
    let proposal_id =
        InvestmentProposalId::try_from_bytes(hash_proposal_id(analysis_id, derivation_digest))?;
    Ok(InvestmentProposalDecision::NoAction(
        NoActionInvestmentProposal {
            analysis_id,
            proposal_id,
            policy,
            evidence,
            evidence_digest,
            derivation_digest,
            reason,
            invalidators: [invalidator],
            confidence,
            horizon_at,
            expires_at,
            execution_eligibility: ProposalExecutionEligibility::ResearchOnlyExecutionIneligible,
        },
    ))
}

fn effective_proposal_expiry(
    evidence: &AdmittedEvidence<'_>,
    policy: &RecommendationPolicy,
    policy_expiry: Timestamp,
) -> Result<Timestamp, InvestmentProposalError> {
    let windows = [
        (RecommendationEvidenceKind::Market, evidence.market.window),
        (
            RecommendationEvidenceKind::PriceForecast,
            evidence.forecast.window,
        ),
        (
            RecommendationEvidenceKind::Valuation,
            evidence.valuation.window,
        ),
        (
            RecommendationEvidenceKind::FinancialModel,
            evidence.financial_model.window(),
        ),
        (
            RecommendationEvidenceKind::Backtest,
            evidence.backtest.window,
        ),
        (
            RecommendationEvidenceKind::OutOfSample,
            evidence.out_of_sample.window(),
        ),
        (
            RecommendationEvidenceKind::Liquidity,
            evidence.liquidity.window,
        ),
        (
            RecommendationEvidenceKind::PortfolioRisk,
            evidence.portfolio_risk.window,
        ),
    ];
    let expires_at =
        windows
            .into_iter()
            .try_fold(policy_expiry, |expires_at, (kind, window)| {
                let freshness_expiry = window
                    .observed_at
                    .checked_add_nanos(policy.maximum_age_nanos(kind))
                    .map_err(|_| InvestmentProposalError::ArithmeticOverflow)?;
                Ok(expires_at.min(window.expires_at).min(freshness_expiry))
            })?;
    match evidence.harmonic_pattern {
        Some(pattern) => {
            let window = pattern.window();
            let freshness_expiry = window
                .observed_at
                .checked_add_nanos(
                    policy.maximum_age_nanos(RecommendationEvidenceKind::HarmonicPattern),
                )
                .map_err(|_| InvestmentProposalError::ArithmeticOverflow)?;
            Ok(expires_at.min(window.expires_at).min(freshness_expiry))
        }
        None => Ok(expires_at),
    }
}

fn admit_alpha_evidence<'a>(
    evidence: &'a InvestmentAnalysisEvidence,
    policy: &RecommendationPolicy,
    horizon_at: Timestamp,
) -> Result<AdmittedAlphaEvidence<'a>, ProposalUnavailableReason> {
    let market = evidence
        .market
        .as_ref()
        .ok_or(ProposalUnavailableReason::MissingEvidence(
            RecommendationEvidenceKind::Market,
        ))?;
    admit_binding(
        evidence,
        RecommendationEvidenceKind::Market,
        market.instrument_id,
        market.price.currency(),
        market.window,
        policy,
    )?;
    admit_quality(RecommendationEvidenceKind::Market, market.quality, false)?;

    let forecast =
        evidence
            .price_forecast
            .as_ref()
            .ok_or(ProposalUnavailableReason::MissingEvidence(
                RecommendationEvidenceKind::PriceForecast,
            ))?;
    admit_binding(
        evidence,
        RecommendationEvidenceKind::PriceForecast,
        forecast.instrument_id,
        forecast.cases.base().currency(),
        forecast.window,
        policy,
    )?;
    if forecast.horizon_at != horizon_at || forecast.horizon_at <= evidence.admitted_at {
        return Err(ProposalUnavailableReason::ForecastHorizonMismatch {
            expected: horizon_at,
            actual: forecast.horizon_at,
        });
    }
    if forecast.calibration.completed_outcomes < policy.semantics.minimum_forecast_outcomes {
        return Err(ProposalUnavailableReason::InsufficientForecastOutcomes {
            required: policy.semantics.minimum_forecast_outcomes,
            actual: forecast.calibration.completed_outcomes,
        });
    }
    if !(policy.semantics.minimum_nominal_forecast_coverage_ppm
        ..=policy.semantics.maximum_nominal_forecast_coverage_ppm)
        .contains(&forecast.calibration.nominal_coverage_ppm)
    {
        return Err(ProposalUnavailableReason::UnsupportedForecastCoverage {
            minimum_ppm: policy.semantics.minimum_nominal_forecast_coverage_ppm,
            maximum_ppm: policy.semantics.maximum_nominal_forecast_coverage_ppm,
            actual_ppm: forecast.calibration.nominal_coverage_ppm,
        });
    }

    if forecast.calibration.realized_coverage_ppm
        < policy.semantics.minimum_realized_forecast_coverage_ppm
        || forecast
            .calibration
            .nominal_coverage_ppm
            .abs_diff(forecast.calibration.realized_coverage_ppm)
            > policy.semantics.maximum_forecast_calibration_error_ppm
    {
        return Err(ProposalUnavailableReason::ForecastCalibrationBelowPolicy {
            minimum_realized_ppm: policy.semantics.minimum_realized_forecast_coverage_ppm,
            maximum_error_ppm: policy.semantics.maximum_forecast_calibration_error_ppm,
            nominal_ppm: forecast.calibration.nominal_coverage_ppm,
            realized_ppm: forecast.calibration.realized_coverage_ppm,
        });
    }

    let valuation =
        evidence
            .valuation
            .as_ref()
            .ok_or(ProposalUnavailableReason::MissingEvidence(
                RecommendationEvidenceKind::Valuation,
            ))?;
    admit_binding(
        evidence,
        RecommendationEvidenceKind::Valuation,
        valuation.instrument_id,
        valuation.fair_value.currency(),
        valuation.window,
        policy,
    )?;
    if valuation.horizon_at != horizon_at {
        return Err(ProposalUnavailableReason::ValuationHorizonMismatch {
            expected: horizon_at,
            actual: valuation.horizon_at,
        });
    }

    let financial_model =
        evidence
            .financial_model
            .as_ref()
            .ok_or(ProposalUnavailableReason::MissingEvidence(
                RecommendationEvidenceKind::FinancialModel,
            ))?;
    admit_binding(
        evidence,
        RecommendationEvidenceKind::FinancialModel,
        financial_model.instrument_id(),
        financial_model.range().central().currency(),
        financial_model.window(),
        policy,
    )?;
    if financial_model.account_id() != evidence.account_id {
        return Err(ProposalUnavailableReason::AccountMismatch {
            expected: evidence.account_id,
            actual: financial_model.account_id(),
        });
    }
    if financial_model.horizon_at() != horizon_at {
        return Err(ProposalUnavailableReason::FinancialModelHorizonMismatch {
            expected: horizon_at,
            actual: financial_model.horizon_at(),
        });
    }
    if let super::evidence::ValuationEvidenceProvenance::ResearchCalculation {
        account_id,
        method,
        calculation_identity,
        input_set_identity,
    } = valuation.provenance
    {
        if account_id != evidence.account_id {
            return Err(ProposalUnavailableReason::AccountMismatch {
                expected: evidence.account_id,
                actual: account_id,
            });
        }
        if method != financial_model.method()
            || calculation_identity.bytes()
                != financial_model
                    .calculation_identity()
                    .evidence_digest()
                    .bytes()
            || input_set_identity.bytes()
                != financial_model
                    .pit_input_set_identity()
                    .evidence_digest()
                    .bytes()
        {
            return Err(ProposalUnavailableReason::FinancialModelValuationMismatch);
        }
    }
    if financial_model.range().central() != valuation.fair_value {
        return Err(ProposalUnavailableReason::FinancialModelValuationMismatch);
    }

    if evidence.current_share_projection.is_none() {
        return Err(ProposalUnavailableReason::UnprovenCurrentShareUnits);
    }

    Ok(AdmittedAlphaEvidence {
        market,
        forecast,
        valuation,
        financial_model,
    })
}

fn admit_evidence<'a>(
    evidence: &'a InvestmentAnalysisEvidence,
    policy: &RecommendationPolicy,
    horizon_at: Timestamp,
) -> Result<AdmittedEvidence<'a>, ProposalUnavailableReason> {
    let AdmittedAlphaEvidence {
        market,
        forecast,
        valuation,
        financial_model,
    } = admit_alpha_evidence(evidence, policy, horizon_at)?;
    let backtest = evidence
        .backtest
        .as_ref()
        .ok_or(ProposalUnavailableReason::MissingEvidence(
            RecommendationEvidenceKind::Backtest,
        ))?;
    if !policy.semantics.allow_retrospective_studies
        && backtest.qualification.basis() == HistoricalStudyBasis::RetrospectiveFrozenSnapshot
    {
        return Err(ProposalUnavailableReason::HistoricalStudyBasisNotAllowed {
            actual: backtest.qualification.basis(),
        });
    }
    admit_binding(
        evidence,
        RecommendationEvidenceKind::Backtest,
        backtest.instrument_id,
        backtest.currency,
        backtest.window,
        policy,
    )?;
    if backtest.outcome_horizon_nanos != policy.semantics.horizon_nanos {
        return Err(ProposalUnavailableReason::BacktestHorizonMismatch {
            expected_nanos: policy.semantics.horizon_nanos,
            actual_nanos: backtest.outcome_horizon_nanos,
        });
    }
    if backtest.observations < policy.semantics.minimum_backtest_observations {
        return Err(
            ProposalUnavailableReason::InsufficientBacktestObservations {
                required: policy.semantics.minimum_backtest_observations,
                actual: backtest.observations,
            },
        );
    }
    if backtest.trials < policy.semantics.minimum_backtest_trials {
        return Err(ProposalUnavailableReason::InsufficientBacktestTrials {
            required: policy.semantics.minimum_backtest_trials,
            actual: backtest.trials,
        });
    }

    let out_of_sample =
        evidence
            .out_of_sample
            .as_ref()
            .ok_or(ProposalUnavailableReason::MissingEvidence(
                RecommendationEvidenceKind::OutOfSample,
            ))?;
    admit_binding(
        evidence,
        RecommendationEvidenceKind::OutOfSample,
        out_of_sample.instrument_id(),
        out_of_sample.currency(),
        out_of_sample.window(),
        policy,
    )?;
    if out_of_sample.outcome_horizon_nanos() != policy.semantics.horizon_nanos {
        return Err(ProposalUnavailableReason::OutOfSampleHorizonMismatch {
            expected_nanos: policy.semantics.horizon_nanos,
            actual_nanos: out_of_sample.outcome_horizon_nanos(),
        });
    }
    if out_of_sample.dataset_identity() != backtest.dataset_identity
        || out_of_sample.signal_plan_identity() != backtest.command_identity
        || out_of_sample.aggregate_identity() != backtest.terminal_identity
        || out_of_sample.study_identity() != backtest.report_identity
        || out_of_sample.qualification() != backtest.qualification
        || out_of_sample.simulation_cutoff_at() != backtest.simulation_cutoff_at
        || out_of_sample.window() != backtest.window
    {
        return Err(ProposalUnavailableReason::OutOfSampleBacktestMismatch);
    }

    // The live four-method producer must actually evaluate the harmonic input. A genuine
    // no-pattern result is retained as an audit; absent history cannot pretend to be one.
    if evidence.valuation_method_set.is_some() && evidence.harmonic_history().is_none() {
        return Err(ProposalUnavailableReason::MissingEvidence(
            RecommendationEvidenceKind::HarmonicPattern,
        ));
    }
    let harmonic_pattern = evidence.harmonic_pattern.as_ref();
    if let Some(pattern) = harmonic_pattern {
        admit_binding(
            evidence,
            RecommendationEvidenceKind::HarmonicPattern,
            pattern.instrument_id(),
            evidence.currency,
            pattern.window(),
            policy,
        )?;
        if pattern.decision_cutoff() > evidence.as_of {
            return Err(ProposalUnavailableReason::NotAvailableAtCutoff(
                RecommendationEvidenceKind::HarmonicPattern,
            ));
        }
    }

    let liquidity =
        evidence
            .liquidity
            .as_ref()
            .ok_or(ProposalUnavailableReason::MissingEvidence(
                RecommendationEvidenceKind::Liquidity,
            ))?;
    admit_binding(
        evidence,
        RecommendationEvidenceKind::Liquidity,
        liquidity.instrument_id,
        liquidity.currency,
        liquidity.window,
        policy,
    )?;
    admit_quality(
        RecommendationEvidenceKind::Liquidity,
        liquidity.quality,
        true,
    )?;

    let portfolio_risk =
        evidence
            .portfolio_risk
            .as_ref()
            .ok_or(ProposalUnavailableReason::MissingEvidence(
                RecommendationEvidenceKind::PortfolioRisk,
            ))?;
    admit_binding(
        evidence,
        RecommendationEvidenceKind::PortfolioRisk,
        portfolio_risk.instrument_id,
        portfolio_risk.currency,
        portfolio_risk.window,
        policy,
    )?;
    if portfolio_risk.account_id != evidence.account_id {
        return Err(ProposalUnavailableReason::AccountMismatch {
            expected: evidence.account_id,
            actual: portfolio_risk.account_id,
        });
    }
    if portfolio_risk.portfolio_revision.bytes() == [0; 32] {
        return Err(ProposalUnavailableReason::ReservedPortfolioRevision);
    }

    Ok(AdmittedEvidence {
        market,
        forecast,
        valuation,
        financial_model,
        backtest,
        out_of_sample,
        harmonic_pattern,
        liquidity,
        portfolio_risk,
    })
}

fn admit_binding(
    aggregate: &InvestmentAnalysisEvidence,
    kind: RecommendationEvidenceKind,
    instrument_id: InstrumentId,
    currency: Currency,
    window: ProposalEvidenceWindow,
    policy: &RecommendationPolicy,
) -> Result<(), ProposalUnavailableReason> {
    if instrument_id != aggregate.instrument_id {
        return Err(ProposalUnavailableReason::InstrumentMismatch {
            evidence: kind,
            expected: aggregate.instrument_id,
            actual: instrument_id,
        });
    }
    if currency != aggregate.currency {
        return Err(ProposalUnavailableReason::CurrencyMismatch {
            evidence: kind,
            expected: aggregate.currency,
            actual: currency,
        });
    }
    // Published live investment analyses retain two clocks: immutable research information
    // and current execution prerequisites. Only an actual all-method live valuation audit
    // binds the latter market cutoff. Historical and unaudited evidence keeps the original
    // cutoff, including every model, valuation, study and harmonic input.
    let operational_cutoff = aggregate
        .valuation_method_set
        .as_ref()
        .map_or(aggregate.as_of, |audit| audit.market_cutoff());
    let cutoff = if matches!(
        kind,
        RecommendationEvidenceKind::Market
            | RecommendationEvidenceKind::Liquidity
            | RecommendationEvidenceKind::PortfolioRisk
    ) {
        operational_cutoff
    } else {
        aggregate.as_of
    };
    let source_owned = matches!(kind, RecommendationEvidenceKind::Market);
    if window.source_knowledge_cutoff > cutoff
        || window.available_at > aggregate.admitted_at
        || (source_owned && window.available_at > cutoff)
    {
        return Err(ProposalUnavailableReason::NotAvailableAtCutoff(kind));
    }
    if window.expires_at <= aggregate.admitted_at {
        return Err(ProposalUnavailableReason::ExpiredEvidence(kind));
    }
    let age = aggregate
        .admitted_at
        .unix_nanos()
        .checked_sub(window.observed_at.unix_nanos())
        .ok_or(ProposalUnavailableReason::StaleEvidence(kind))?;
    if age < 0 || age >= policy.maximum_age_nanos(kind) {
        return Err(ProposalUnavailableReason::StaleEvidence(kind));
    }
    Ok(())
}

fn admit_quality(
    evidence: RecommendationEvidenceKind,
    quality: DataQuality,
    liquidity: bool,
) -> Result<(), ProposalUnavailableReason> {
    let admitted = if liquidity {
        matches!(
            quality,
            DataQuality::DirectVerified | DataQuality::DirectUnverified | DataQuality::Aggregated
        )
    } else {
        matches!(
            quality,
            DataQuality::DirectVerified
                | DataQuality::DirectUnverified
                | DataQuality::OfficialDelayed
                | DataQuality::Aggregated
        )
    };
    if admitted {
        Ok(())
    } else {
        Err(ProposalUnavailableReason::RejectedQuality { evidence, quality })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PriceDirection {
    Bullish,
    Neutral,
    Bearish,
}

fn forecast_valuation_conflict(
    market: Money,
    forecast: Money,
    valuation: Money,
    policy: &RecommendationPolicy,
) -> Result<bool, InvestmentProposalError> {
    let forecast = price_direction(forecast, market, policy)?;
    let valuation = price_direction(valuation, market, policy)?;
    Ok(matches!(
        (forecast, valuation),
        (PriceDirection::Bullish, PriceDirection::Bearish)
            | (PriceDirection::Bearish, PriceDirection::Bullish)
    ))
}

fn price_direction(
    value: Money,
    mark: Money,
    policy: &RecommendationPolicy,
) -> Result<PriceDirection, InvestmentProposalError> {
    let bullish = add_rate(mark, policy.semantics.bullish_threshold)?;
    let bearish = subtract_rate(mark, policy.semantics.bearish_threshold)?;
    if value.amount() >= bullish.amount() {
        Ok(PriceDirection::Bullish)
    } else if value.amount() <= bearish.amount() {
        Ok(PriceDirection::Bearish)
    } else {
        Ok(PriceDirection::Neutral)
    }
}

fn select_action(
    evidence: &AdmittedEvidence<'_>,
    ladder: GeneratedPriceLadder,
) -> Option<RecommendationAction> {
    let mark = evidence.market.price.amount();
    let invalidation_ceiling = ladder.exit_range.upper().amount();
    let add_ceiling = ladder.add_range.upper().amount();
    let entry_ceiling = ladder.entry_range.upper().amount();
    let trim_floor = ladder.trim_range.lower().amount();
    let state = evidence.portfolio_risk.position_state;

    match state {
        PortfolioPositionState::NoPosition => (mark > invalidation_ceiling
            && mark <= entry_ceiling)
            .then_some(RecommendationAction::Buy),
        PortfolioPositionState::Position {
            add_allowed,
            trim_allowed,
            exit_allowed,
        } => {
            if mark <= invalidation_ceiling && exit_allowed {
                Some(RecommendationAction::Sell)
            } else if mark > invalidation_ceiling && mark <= add_ceiling && add_allowed {
                Some(RecommendationAction::Add)
            } else if mark >= trim_floor && trim_allowed {
                Some(RecommendationAction::Trim)
            } else {
                Some(RecommendationAction::Hold)
            }
        }
    }
}

fn generate_price_ladder(
    forecast_cases: TargetPriceCases,
    forecast_ranges: ForecastPriceRanges,
    valuation: Money,
    policy: &RecommendationPolicy,
    derived_base: Money,
) -> Result<GeneratedPriceLadder, InvestmentProposalError> {
    let base_range = TargetPriceRange::try_new(
        blend(
            forecast_ranges.base.lower(),
            policy.semantics.forecast_base_weight_bps,
            valuation,
            policy.semantics.valuation_weight_bps,
            policy,
        )?,
        blend(
            forecast_ranges.base.upper(),
            policy.semantics.forecast_base_weight_bps,
            valuation,
            policy.semantics.valuation_weight_bps,
            policy,
        )?,
    )
    .map_err(|_| InvestmentProposalError::InvalidPrice)?;
    let lower_anchor = forecast_ranges.downside.upper();
    let upper_anchor = base_range.lower();
    let weights = policy.semantics.price_range_weights_bps;
    let exit_range = range_between(lower_anchor, upper_anchor, weights[0], weights[1], policy)?;
    let add_range = range_between(lower_anchor, upper_anchor, weights[2], weights[3], policy)?;
    let entry_range = range_between(lower_anchor, upper_anchor, weights[4], weights[5], policy)?;
    let trim_range = range_between(
        base_range.upper(),
        forecast_ranges.upside.lower(),
        weights[6],
        weights[7],
        policy,
    )?;
    let add_case = blend(
        add_range.lower(),
        weights[8],
        add_range.upper(),
        10_000_u32
            .checked_sub(weights[8])
            .ok_or(InvestmentProposalError::InvalidPolicy)?,
        policy,
    )?;
    let cases = TargetPriceCases::try_new(
        forecast_cases.downside(),
        derived_base,
        forecast_cases.upside(),
    )
    .map_err(|_| InvestmentProposalError::InvalidPrice)?;
    GeneratedPriceLadder::try_new(
        cases,
        forecast_ranges.downside,
        base_range,
        forecast_ranges.upside,
        entry_range,
        add_range,
        add_case,
        trim_range,
        exit_range,
    )
}

fn range_between(
    lower_anchor: Money,
    upper_anchor: Money,
    lower_anchor_weight_for_lower_bps: u32,
    lower_anchor_weight_for_upper_bps: u32,
    policy: &RecommendationPolicy,
) -> Result<TargetPriceRange, InvestmentProposalError> {
    let lower = blend(
        lower_anchor,
        lower_anchor_weight_for_lower_bps,
        upper_anchor,
        10_000_u32
            .checked_sub(lower_anchor_weight_for_lower_bps)
            .ok_or(InvestmentProposalError::InvalidPolicy)?,
        policy,
    )?;
    let upper = blend(
        lower_anchor,
        lower_anchor_weight_for_upper_bps,
        upper_anchor,
        10_000_u32
            .checked_sub(lower_anchor_weight_for_upper_bps)
            .ok_or(InvestmentProposalError::InvalidPolicy)?,
        policy,
    )?;
    TargetPriceRange::try_new(lower, upper).map_err(|_| InvestmentProposalError::InvalidPrice)
}

fn blend(
    left: Money,
    left_weight_bps: u32,
    right: Money,
    right_weight_bps: u32,
    policy: &RecommendationPolicy,
) -> Result<Money, InvestmentProposalError> {
    left.checked_weighted_basis_points(
        left_weight_bps,
        right,
        right_weight_bps,
        policy.semantics.price_scale,
        policy.semantics.rounding_policy,
    )
    .map_err(|_| InvestmentProposalError::ArithmeticOverflow)
}

fn add_rate(mark: Money, rate: BasisPoints) -> Result<Money, InvestmentProposalError> {
    let adjustment = mark
        .checked_basis_points(rate, mark.amount().scale(), RoundingPolicy::NearestEven)
        .map_err(|_| InvestmentProposalError::ArithmeticOverflow)?;
    mark.checked_add(adjustment)
        .map_err(|_| InvestmentProposalError::ArithmeticOverflow)
}

fn subtract_rate(mark: Money, rate: BasisPoints) -> Result<Money, InvestmentProposalError> {
    let adjustment = mark
        .checked_basis_points(rate, mark.amount().scale(), RoundingPolicy::NearestEven)
        .map_err(|_| InvestmentProposalError::ArithmeticOverflow)?;
    mark.checked_sub(adjustment)
        .map_err(|_| InvestmentProposalError::ArithmeticOverflow)
}

fn recommendation_confidence(
    evidence: &AdmittedEvidence<'_>,
    policy: &RecommendationPolicy,
    forecast_and_valuation_conflict: bool,
    prospective_action: Option<RecommendationAction>,
) -> Result<RecommendationConfidence, InvestmentProposalError> {
    let calibration_difference = evidence
        .forecast
        .calibration
        .nominal_coverage_ppm
        .abs_diff(evidence.forecast.calibration.realized_coverage_ppm);
    let forecast_calibration = CONFIDENCE_PARTS_PER_MILLION
        .checked_sub(calibration_difference)
        .ok_or(InvestmentProposalError::InvalidPartsPerMillion)?;
    let valuation_agreement = if forecast_and_valuation_conflict {
        0
    } else if (evidence.forecast.ranges.base.lower().amount()
        ..=evidence.forecast.ranges.base.upper().amount())
        .contains(&evidence.valuation.fair_value.amount())
    {
        CONFIDENCE_PARTS_PER_MILLION
    } else if (evidence.forecast.ranges.downside.lower().amount()
        ..=evidence.forecast.ranges.upside.upper().amount())
        .contains(&evidence.valuation.fair_value.amount())
    {
        750_000
    } else {
        500_000
    };
    let market_integrity = match evidence.market.quality {
        DataQuality::DirectVerified => 1_000_000,
        DataQuality::DirectUnverified => 850_000,
        DataQuality::OfficialDelayed => 800_000,
        DataQuality::Aggregated => 750_000,
        DataQuality::Indicative
        | DataQuality::Modeled
        | DataQuality::Estimated
        | DataQuality::Stale
        | DataQuality::Quarantined => return Err(InvestmentProposalError::InvalidPolicy),
    };
    let maximum_spread = u32::try_from(policy.semantics.maximum_liquidity_spread.get())
        .map_err(|_| InvestmentProposalError::InvalidPolicy)?;
    let actual_spread = u32::try_from(evidence.liquidity.quoted_spread.get())
        .map_err(|_| InvestmentProposalError::InvalidPrice)?;
    let spread_reliability = if actual_spread >= maximum_spread {
        0
    } else {
        u32::try_from(
            u64::from(maximum_spread - actual_spread)
                .checked_mul(u64::from(CONFIDENCE_PARTS_PER_MILLION))
                .ok_or(InvestmentProposalError::ArithmeticOverflow)?
                / u64::from(maximum_spread),
        )
        .map_err(|_| InvestmentProposalError::ArithmeticOverflow)?
    };
    let liquidity = match prospective_action {
        Some(action) => match action_liquidity_capacity(evidence.liquidity, action) {
            Ok(Some(capacity)) => {
                RecommendationConfidenceComponentValue::Available(capacity.min(spread_reliability))
            }
            Ok(None) => RecommendationConfidenceComponentValue::NotApplicable,
            Err(reason) => RecommendationConfidenceComponentValue::Unavailable(reason),
        },
        None => RecommendationConfidenceComponentValue::Unavailable(
            RecommendationConfidenceUnavailableReason::ActionSideNotEstablished,
        ),
    };
    let values = [
        RecommendationConfidenceComponentValue::Available(forecast_calibration),
        RecommendationConfidenceComponentValue::Available(valuation_agreement),
        RecommendationConfidenceComponentValue::Available(evidence.backtest.stability_ppm),
        RecommendationConfidenceComponentValue::Available(market_integrity),
        liquidity,
        RecommendationConfidenceComponentValue::Available(
            evidence.portfolio_risk.risk_capacity_ppm,
        ),
    ];
    let kinds = [
        RecommendationConfidenceComponentKind::ForecastCalibration,
        RecommendationConfidenceComponentKind::ValuationAgreement,
        RecommendationConfidenceComponentKind::BacktestStability,
        RecommendationConfidenceComponentKind::MarketIntegrity,
        RecommendationConfidenceComponentKind::LiquidityCapacity,
        RecommendationConfidenceComponentKind::PortfolioRiskCapacity,
    ];
    let components = std::array::from_fn(|index| {
        RecommendationConfidenceComponent::new(
            kinds[index],
            values[index],
            policy.semantics.confidence_weights_ppm[index],
        )
    });
    let mut weighted_sum = 0_u64;
    let mut applicable_policy_weight_ppm = 0_u32;
    let mut unavailable_reason = None;
    for component in components {
        if component.value == RecommendationConfidenceComponentValue::NotApplicable {
            continue;
        }
        applicable_policy_weight_ppm = applicable_policy_weight_ppm
            .checked_add(component.weight_ppm)
            .ok_or(InvestmentProposalError::ArithmeticOverflow)?;
        match component.value {
            RecommendationConfidenceComponentValue::Available(value) => {
                weighted_sum = weighted_sum
                    .checked_add(u64::from(value) * u64::from(component.weight_ppm))
                    .ok_or(InvestmentProposalError::ArithmeticOverflow)?;
            }
            RecommendationConfidenceComponentValue::Unavailable(reason) => {
                unavailable_reason = Some(reason);
            }
            RecommendationConfidenceComponentValue::NotApplicable => {}
        }
    }
    if applicable_policy_weight_ppm == 0 && unavailable_reason.is_none() {
        unavailable_reason =
            Some(RecommendationConfidenceUnavailableReason::NoApplicablePolicyWeight);
    }
    let value_ppm = if unavailable_reason.is_some() {
        None
    } else {
        Some(
            u32::try_from(weighted_sum / u64::from(applicable_policy_weight_ppm))
                .map_err(|_| InvestmentProposalError::ArithmeticOverflow)?,
        )
    };
    Ok(RecommendationConfidence {
        meaning: RecommendationConfidenceMeaning::PolicyWeightedEvidenceReliabilityV1,
        study_qualification: evidence.backtest.qualification,
        value_ppm,
        unavailable_reason,
        applicable_policy_weight_ppm,
        components,
    })
}

fn action_liquidity_capacity(
    liquidity: &LiquidityEvidence,
    action: RecommendationAction,
) -> Result<Option<u32>, RecommendationConfidenceUnavailableReason> {
    match action {
        RecommendationAction::Buy | RecommendationAction::Add => liquidity
            .buy_add_capacity_ppm
            .map(Some)
            .ok_or(RecommendationConfidenceUnavailableReason::BuyAddCapacityUnavailable),
        RecommendationAction::Trim | RecommendationAction::Sell => liquidity
            .trim_sell_capacity_ppm
            .map(Some)
            .ok_or(RecommendationConfidenceUnavailableReason::TrimSellCapacityUnavailable),
        RecommendationAction::Hold => Ok(None),
    }
}
