//! Pure evidence admission and deterministic recommendation derivation.

use market_squawk_domain::{
    BasisPoints, Currency, DataQuality, InstrumentId, Money, RoundingPolicy, Timestamp,
};

use crate::{TargetPriceCases, TargetPriceRange};

use super::digest::{
    hash_analysis, hash_evidence, hash_generated_derivation, hash_no_action_derivation,
    hash_policy, hash_proposal_id,
};
use super::evidence::{
    CostAdjustedPitBacktestEvidence, InvestmentAnalysisEvidence, LiquidityEvidence,
    MarketReferenceEvidence, PortfolioPositionState, PortfolioRiskEvidence, PriceForecastEvidence,
    ProposalEvidenceWindow, ValuationEvidence,
};
use super::output::{
    GeneratedInvestmentProposal, GeneratedPriceLadder, InvestmentProposalDecision,
    NoActionInvestmentProposal, NoActionReason, ProposalExecutionEligibility, ProposalInvalidator,
    ProposalUnavailableReason, RecommendationAction, UnavailableInvestmentAnalysis,
};
use super::policy::{
    RecommendationConfidence, RecommendationConfidenceComponent,
    RecommendationConfidenceComponentKind, RecommendationConfidenceMeaning, RecommendationPolicy,
    validate_policy,
};
use super::{
    CONFIDENCE_PARTS_PER_MILLION, InvestmentAnalysisId, InvestmentProposalError,
    InvestmentProposalId, RecommendationDerivationDigest, RecommendationEvidenceDigest,
    RecommendationEvidenceKind, RecommendationPolicyDigest,
};

struct AdmittedEvidence<'a> {
    market: &'a MarketReferenceEvidence,
    forecast: &'a PriceForecastEvidence,
    valuation: &'a ValuationEvidence,
    backtest: &'a CostAdjustedPitBacktestEvidence,
    liquidity: &'a LiquidityEvidence,
    portfolio_risk: &'a PortfolioRiskEvidence,
}

/// Pure deterministic authority that derives research proposals from admitted evidence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InvestmentProposalAuthority;

impl InvestmentProposalAuthority {
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
            .as_of
            .checked_add_nanos(policy.semantics.horizon_nanos)
            .map_err(|_| InvestmentProposalError::ArithmeticOverflow)?;
        let expires_at = evidence
            .as_of
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

        let evidence_conflicts = forecast_valuation_conflict(&admitted, &policy)?;
        let confidence = recommendation_confidence(&admitted, &policy, evidence_conflicts)?;
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
        if admitted.liquidity.quoted_spread > policy.semantics.maximum_liquidity_spread
            || admitted.liquidity.capacity_ppm < policy.semantics.minimum_liquidity_capacity_ppm
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
        if confidence.value_ppm < policy.semantics.minimum_confidence_ppm {
            return no_action(
                evidence,
                policy,
                analysis_id,
                evidence_digest,
                confidence,
                horizon_at,
                expires_at,
                NoActionReason::ConfidenceBelowPolicy,
                ProposalInvalidator::ConfidencePolicyBreach,
            );
        }

        let derived_base = admitted
            .forecast
            .cases
            .base()
            .checked_weighted_basis_points(
                policy.semantics.forecast_base_weight_bps,
                admitted.valuation.fair_value,
                policy.semantics.valuation_weight_bps,
                policy.semantics.price_scale,
                policy.semantics.rounding_policy,
            )
            .map_err(|_| InvestmentProposalError::ArithmeticOverflow)?;
        let price_ladder = match generate_price_ladder(&admitted, &policy, derived_base) {
            Ok(ladder) => ladder,
            Err(InvestmentProposalError::InvalidPrice) => {
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
            Err(error) => return Err(error),
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
        match Self::generate(evidence, policy)? {
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
        match Self::generate(evidence, policy)? {
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
        match Self::generate(evidence, policy)? {
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
            RecommendationEvidenceKind::Backtest,
            evidence.backtest.window,
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
    windows
        .into_iter()
        .try_fold(policy_expiry, |expires_at, (kind, window)| {
            let freshness_expiry = window
                .observed_at
                .checked_add_nanos(policy.maximum_age_nanos(kind))
                .map_err(|_| InvestmentProposalError::ArithmeticOverflow)?;
            Ok(expires_at.min(window.expires_at).min(freshness_expiry))
        })
}

fn admit_evidence<'a>(
    evidence: &'a InvestmentAnalysisEvidence,
    policy: &RecommendationPolicy,
    horizon_at: Timestamp,
) -> Result<AdmittedEvidence<'a>, ProposalUnavailableReason> {
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
    if forecast.horizon_at != horizon_at {
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

    let backtest = evidence
        .backtest
        .as_ref()
        .ok_or(ProposalUnavailableReason::MissingEvidence(
            RecommendationEvidenceKind::Backtest,
        ))?;
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
        backtest,
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
    if window.available_at > aggregate.as_of {
        return Err(ProposalUnavailableReason::NotAvailableAtCutoff(kind));
    }
    if window.expires_at <= aggregate.as_of {
        return Err(ProposalUnavailableReason::ExpiredEvidence(kind));
    }
    let age = aggregate
        .as_of
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
    evidence: &AdmittedEvidence<'_>,
    policy: &RecommendationPolicy,
) -> Result<bool, InvestmentProposalError> {
    let mark = evidence.market.price;
    let forecast = price_direction(evidence.forecast.cases.base(), mark, policy)?;
    let valuation = price_direction(evidence.valuation.fair_value, mark, policy)?;
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
    evidence: &AdmittedEvidence<'_>,
    policy: &RecommendationPolicy,
    derived_base: Money,
) -> Result<GeneratedPriceLadder, InvestmentProposalError> {
    let forecast_ranges = evidence.forecast.ranges;
    let base_range = TargetPriceRange::try_new(
        blend(
            forecast_ranges.base.lower(),
            policy.semantics.forecast_base_weight_bps,
            evidence.valuation.fair_value,
            policy.semantics.valuation_weight_bps,
            policy,
        )?,
        blend(
            forecast_ranges.base.upper(),
            policy.semantics.forecast_base_weight_bps,
            evidence.valuation.fair_value,
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
        evidence.forecast.cases.downside(),
        derived_base,
        evidence.forecast.cases.upside(),
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
    let liquidity = evidence.liquidity.capacity_ppm.min(spread_reliability);
    let values = [
        forecast_calibration,
        valuation_agreement,
        evidence.backtest.stability_ppm,
        market_integrity,
        liquidity,
        evidence.portfolio_risk.risk_capacity_ppm,
    ];
    let kinds = [
        RecommendationConfidenceComponentKind::ForecastCalibration,
        RecommendationConfidenceComponentKind::ValuationAgreement,
        RecommendationConfidenceComponentKind::BacktestStability,
        RecommendationConfidenceComponentKind::MarketIntegrity,
        RecommendationConfidenceComponentKind::LiquidityCapacity,
        RecommendationConfidenceComponentKind::PortfolioRiskCapacity,
    ];
    let mut weighted_sum = 0_u64;
    let components = std::array::from_fn(|index| {
        weighted_sum +=
            u64::from(values[index]) * u64::from(policy.semantics.confidence_weights_ppm[index]);
        RecommendationConfidenceComponent::new(
            kinds[index],
            values[index],
            policy.semantics.confidence_weights_ppm[index],
        )
    });
    let value_ppm = u32::try_from(weighted_sum / u64::from(CONFIDENCE_PARTS_PER_MILLION))
        .map_err(|_| InvestmentProposalError::ArithmeticOverflow)?;
    Ok(RecommendationConfidence {
        meaning: RecommendationConfidenceMeaning::PolicyWeightedEvidenceReliabilityV1,
        value_ppm,
        components,
    })
}
