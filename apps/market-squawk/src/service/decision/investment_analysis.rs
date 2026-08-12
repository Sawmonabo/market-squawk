//! Pure read-side transport over retained, authority-recomputed investment analyses.

use std::{num::NonZeroU32, sync::Arc};

use market_squawk_decisions::{
    AnalyticalProfileBindingReference, CostAdjustedPitBacktestEvidence, DecisionContentDigest,
    FeasibleLotRangeAvailability, ForecastPriceRanges, GeneratedInvestmentProposal,
    GeneratedPriceLadder, InvestmentAnalysisCurrentIndexEntry, InvestmentAnalysisEvidence,
    InvestmentAnalysisId, InvestmentOutcomeProjection, InvestmentProposalDecision,
    InvestmentProposalIndexEntry, InvestmentProposalIndexOutcome, InvestmentSizingProjection,
    LiquidityEvidence, MarketReferenceAdjustmentBasis, MarketReferenceEvidence,
    MarketReferencePriceKind, NoActionInvestmentProposal, NoActionReason, PortfolioPositionState,
    PortfolioRiskEvidence, PriceForecastEvidence, ProposalEvidenceWindow, ProposalInvalidator,
    ProposalUnavailableReason, RecommendationAction, RecommendationConfidence,
    RecommendationConfidenceComponentKind, RecommendationConfidenceMeaning,
    RecommendationEvidenceKind, RecommendationOutcomeCohort, RecommendationOutcomeStatus,
    RecommendationOutcomeUnavailableReason, RecommendationPolicy, RecommendationTrackRecord,
    RecommendationTrackRecordPerformance, SizingUnavailableReason, TargetPriceCases,
    TargetPriceRange, UnavailableInvestmentAnalysis, ValuationEvidence,
};
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, Money, SourceIdentifier, Timestamp,
};
use market_squawk_modeling::ForecastCentralStatistic;
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use market_squawk_valuation::ValuationAmountBasis;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::application::decision::{DecisionApplication, InvestmentAnalysisRead};

use super::{decode, ensure_live, map_application, page_fetch_limit};

pub(super) const GET_INVESTMENT_ANALYSIS: &str = "Decision.GetInvestmentAnalysis";
pub(super) const LIST_INVESTMENT_ANALYSES: &str = "Decision.ListInvestmentAnalyses";
pub(super) const GET_RECOMMENDATION_TRACK_RECORD: &str = "Decision.GetRecommendationTrackRecord";

/// Closed read-only operation family over durable investment-analysis results.
pub(super) struct InvestmentAnalysisOperations {
    decisions: Arc<DecisionApplication>,
}

impl InvestmentAnalysisOperations {
    pub(super) fn new(decisions: Arc<DecisionApplication>) -> Self {
        Self { decisions }
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(
            operation,
            GET_INVESTMENT_ANALYSIS | LIST_INVESTMENT_ANALYSES | GET_RECOMMENDATION_TRACK_RECORD
        )
    }

    pub(super) fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let arguments = super::super::business_arguments(request.arguments());
        match request.name() {
            GET_INVESTMENT_ANALYSIS => {
                let input: InvestmentAnalysisRequest = decode(&arguments)?;
                let analysis = self
                    .decisions
                    .read_investment_analysis(analysis_id(&input.analysis_id)?)
                    .map_err(map_application)?;
                ensure_live(context)?;
                TypedToolResult::try_new(
                    investment_analysis_value(&analysis),
                    1,
                    ToolResultMetadata::complete_not_applicable(),
                    context.limits(),
                )
                .map_err(Into::into)
            }
            LIST_INVESTMENT_ANALYSES => {
                let input: InvestmentAnalysisListRequest = decode(&arguments)?;
                let after = input
                    .after_analysis_id
                    .as_deref()
                    .map(analysis_id)
                    .transpose()?;
                let fetch_limit = page_fetch_limit(input.limit)?;
                let page = self
                    .decisions
                    .read_investment_proposal_index_page_after(after, fetch_limit)
                    .map_err(map_application)?;
                let (mut analyses, available) = page.into_parts();
                let fetched = analyses.len();
                let expected_fetched = available.min(fetch_limit);
                if fetched != expected_fetched {
                    return Err(ServiceError::Internal);
                }
                let truncated = available > input.limit;
                if truncated {
                    analyses.truncate(input.limit);
                }
                let returned = analyses.len();
                let next_after_analysis_id = if truncated {
                    analyses
                        .last()
                        .map(|entry| hex(entry.analysis_id().bytes()))
                } else {
                    None
                };
                let metadata = if truncated {
                    ToolResultMetadata::try_truncated_not_applicable(available)?
                } else {
                    ToolResultMetadata::complete_not_applicable()
                };
                let completeness = if truncated { "truncated" } else { "complete" };
                let values = analyses
                    .iter()
                    .map(investment_analysis_locator_value)
                    .collect::<Vec<_>>();
                ensure_live(context)?;
                TypedToolResult::try_new(
                    json!({
                        "completeness": completeness,
                        "returnedCount": returned,
                        "availableCount": available,
                        "nextAfterAnalysisId": next_after_analysis_id,
                        "analyses": values,
                    }),
                    returned,
                    metadata,
                    context.limits(),
                )
                .map_err(Into::into)
            }
            GET_RECOMMENDATION_TRACK_RECORD => {
                let input: RecommendationTrackRecordRequest = decode(&arguments)?;
                let profile = AnalyticalProfileBindingReference::new(
                    SourceIdentifier::try_from(input.profile_id)
                        .map_err(|_error| ServiceError::InvalidRequest)?,
                    NonZeroU32::new(input.profile_revision).ok_or(ServiceError::InvalidRequest)?,
                    DecisionContentDigest::try_new(EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        decode_sha256(&input.profile_digest)?,
                    ))
                    .map_err(|_error| ServiceError::InvalidRequest)?,
                );
                let track_record = self
                    .decisions
                    .recommendation_track_record(
                        &profile,
                        input.horizon_nanos,
                        Timestamp::from_unix_nanos(input.evaluated_at_unix_nanos),
                    )
                    .map_err(map_application)?;
                let value = recommendation_track_record_value(&track_record);
                let count = track_record.groups().len();
                ensure_live(context)?;
                TypedToolResult::try_new(
                    value,
                    count,
                    ToolResultMetadata::complete_not_applicable(),
                    context.limits(),
                )
                .map_err(Into::into)
            }
            _ => Err(ServiceError::NotFound),
        }
    }
}

impl std::fmt::Debug for InvestmentAnalysisOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InvestmentAnalysisOperations")
            .field("decisions", &"[DURABLE DECISION AUTHORITY]")
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InvestmentAnalysisRequest {
    analysis_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InvestmentAnalysisListRequest {
    after_analysis_id: Option<String>,
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RecommendationTrackRecordRequest {
    profile_id: String,
    profile_revision: u32,
    profile_digest: String,
    horizon_nanos: i64,
    evaluated_at_unix_nanos: i64,
}

fn investment_analysis_value(read: &InvestmentAnalysisRead) -> Value {
    let decision = &read.decision;
    json!({
        "analysisId": hex(decision.analysis_id().bytes()),
        "executionEligibility": "research_only_execution_ineligible",
        "policy": recommendation_policy_value(decision.policy()),
        "evidence": investment_analysis_evidence_value(decision.evidence()),
        "evidenceDigest": hex(decision.evidence_digest().bytes()),
        "publication": read.current.as_ref().map(investment_analysis_current_value),
        "projection": read.outcome_projection.as_ref().map(outcome_projection_value),
        "sizing": read.sizing_projection.as_ref().map(sizing_projection_value),
        "realizedOutcome": read.current.as_ref().and_then(|value| value.current_outcome()).map(recommendation_outcome_current_value),
        "result": match decision {
            InvestmentProposalDecision::Generated(proposal) => generated_result_value(proposal),
            InvestmentProposalDecision::NoAction(proposal) => no_action_result_value(proposal),
            InvestmentProposalDecision::Unavailable(analysis) => unavailable_result_value(analysis),
        },
    })
}

fn investment_analysis_current_value(value: &InvestmentAnalysisCurrentIndexEntry) -> Value {
    let publication = value.publication();
    json!({
        "publicationId": hex(publication.publication_id().bytes()),
        "publishedAt": publication.published_at(),
        "executionEligibility": "research_only_execution_ineligible",
        "analyticalProfile": {
            "profileId": publication.analytical_profile().profile_id().as_str(),
            "revision": publication.analytical_profile().revision().get(),
            "contentDigest": content_identity_value(publication.analytical_profile().content_digest()),
        },
        "workflow": {
            "workflowId": publication.workflow().workflow_id().as_str(),
            "revision": publication.workflow().revision().get(),
            "contentDigest": content_identity_value(publication.workflow().content_digest()),
        },
        "accountSetup": {
            "accountId": publication.account_id(),
            "distinctFromAnalyticalProfile": true,
        },
        "outcomeProjectionDigest": value.outcome_projection_digest().map(|digest| hex(digest.bytes())),
        "sizingProjectionDigest": value.sizing_projection_digest().map(|digest| hex(digest.bytes())),
    })
}

fn outcome_projection_value(value: &InvestmentOutcomeProjection) -> Value {
    json!({
        "resultDigest": hex(value.result_digest().bytes()),
        "proposalId": hex(value.binding().proposal_id().bytes()),
        "derivationDigest": hex(value.binding().derivation_digest().bytes()),
        "authority": "analysis_only_no_mutation_no_execution",
        "executionEligible": false,
        "mark": money_value(value.mark()),
        "horizonAt": value.horizon_at(),
        "downside": gross_range_value(value.downside()),
        "base": gross_range_value(value.base()),
        "upside": gross_range_value(value.upside()),
        "netPnl": {"kind": "unavailable", "reason": "exact_forward_cost_evidence_not_supplied"},
        "benchmarkReturn": {"kind": "unavailable", "reason": "exact_proposal_time_benchmark_evidence_not_supplied"},
        "afterTaxPnl": {"kind": "unavailable", "reason": "exact_tax_evidence_not_supplied"},
    })
}

fn gross_range_value(value: market_squawk_decisions::GrossMarkRelativeRange) -> Value {
    let ratio = value.gross_return_from_mark();
    json!({
        "priceRange": price_range_value(value.price_range()),
        "grossReturnFromMark": {
            "lowerNumerator": money_value(ratio.lower().numerator()),
            "upperNumerator": money_value(ratio.upper().numerator()),
            "denominator": money_value(ratio.lower().denominator()),
        },
    })
}

fn sizing_projection_value(value: &InvestmentSizingProjection) -> Value {
    json!({
        "resultDigest": hex(value.result_digest().bytes()),
        "proposalId": hex(value.binding().proposal_id().bytes()),
        "derivationDigest": hex(value.binding().derivation_digest().bytes()),
        "authority": "analysis_only_no_mutation_no_execution",
        "executionEligible": false,
        "evaluatedAt": value.inputs().evaluated_at(),
        "currentLots": value.inputs().portfolio().current_lots().get(),
        "hardFeasibleLots": feasible_lots_value(value.hard_feasible_lots()),
        "preferredFeasibleLots": feasible_lots_value(value.preferred_feasible_lots()),
        "selectedTargetLots": Value::Null,
        "orderQuantity": Value::Null,
    })
}

fn feasible_lots_value(value: &FeasibleLotRangeAvailability) -> Value {
    match value {
        FeasibleLotRangeAvailability::Available(range) => json!({
            "kind": "available",
            "lower": range.lower().get(),
            "upper": range.upper().get(),
        }),
        FeasibleLotRangeAvailability::Unavailable(reasons) => json!({
            "kind": "unavailable",
            "reasons": reasons.iter().copied().map(sizing_unavailable_reason_name).collect::<Vec<_>>(),
        }),
    }
}

fn recommendation_outcome_current_value(
    value: &market_squawk_decisions::RecommendationOutcomeCurrentIndexEntry,
) -> Value {
    let mut result = recommendation_outcome_status_value(value.status());
    if let Some(object) = result.as_object_mut() {
        object.insert("seriesId".to_owned(), json!(hex(value.series_id().bytes())));
        object.insert("revision".to_owned(), json!(value.revision().get()));
        object.insert(
            "statusDigest".to_owned(),
            json!(hex(value.status_digest().bytes())),
        );
        object.insert("evaluatedAt".to_owned(), json!(value.evaluated_at()));
        object.insert("executionEligible".to_owned(), json!(false));
    }
    result
}

fn recommendation_outcome_status_value(value: RecommendationOutcomeStatus) -> Value {
    match value {
        RecommendationOutcomeStatus::Pending(reason) => json!({
            "kind": "pending",
            "reason": match reason {
                market_squawk_decisions::RecommendationOutcomePendingReason::AwaitingHorizon => "awaiting_horizon",
                market_squawk_decisions::RecommendationOutcomePendingReason::AwaitingOutcomeEvidence => "awaiting_outcome_evidence",
            },
        }),
        RecommendationOutcomeStatus::Unavailable(reason) => json!({
            "kind": "unavailable",
            "reason": recommendation_outcome_unavailable_reason_name(reason),
        }),
        RecommendationOutcomeStatus::Completed(outcome) => {
            let observation = outcome.observation();
            json!({
                "kind": "completed",
                "metric": "gross_instrument_price_return",
                "startMark": money_value(outcome.start_mark()),
                "endpointPrice": money_value(observation.endpoint_price()),
                "grossPriceReturn": outcome.gross_price_return().to_string(),
                "observedAt": observation.observed_at(),
                "availableAt": observation.available_at(),
                "selectionReceiptIdentity": content_identity_value(observation.selection_receipt_identity()),
                "selectedObservationIdentity": content_identity_value(observation.selected_observation_identity()),
                "corporateActionEvidenceIdentity": content_identity_value(observation.no_applicable_corporate_actions_identity()),
                "netReturn": {"kind": "unavailable", "reason": "exact_realized_cost_evidence_not_supplied"},
                "benchmarkReturn": {"kind": "unavailable", "reason": "exact_benchmark_outcome_evidence_not_supplied"},
                "afterTaxReturn": {"kind": "unavailable", "reason": "exact_tax_evidence_not_supplied"},
                "settlement": {"kind": "unavailable", "reason": "no_execution_or_settlement_evidence"},
            })
        }
    }
}

fn recommendation_track_record_value(value: &RecommendationTrackRecord) -> Value {
    json!({
        "analyticalProfile": {
            "profileId": value.analytical_profile().profile_id().as_str(),
            "revision": value.analytical_profile().revision().get(),
            "contentDigest": content_identity_value(value.analytical_profile().content_digest()),
        },
        "horizonNanos": value.horizon_nanos(),
        "evaluatedAt": value.evaluated_at(),
        "analysisUnavailableCount": value.analysis_unavailable_count(),
        "minimumCompletedSamples": market_squawk_decisions::RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED,
        "minimumCoveragePpm": market_squawk_decisions::RECOMMENDATION_TRACK_RECORD_MINIMUM_COVERAGE_PPM,
        "groups": value.groups().iter().map(|group| json!({
            "cohort": recommendation_outcome_cohort_name(group.cohort()),
            "publicationCount": group.publication_count(),
            "dueCount": group.due_count(),
            "completedCount": group.completed_count(),
            "pendingCount": group.pending_count(),
            "unavailableCount": group.unavailable_count(),
            "coveragePpm": group.coverage_ppm(),
            "performance": match group.performance() {
                RecommendationTrackRecordPerformance::UnavailableNoDueOutcomes => json!({"kind": "unavailable", "reason": "no_due_outcomes"}),
                RecommendationTrackRecordPerformance::UnavailableInsufficientCompletedSamples { required, actual } => json!({"kind": "unavailable", "reason": "insufficient_completed_samples", "required": required, "actual": actual}),
                RecommendationTrackRecordPerformance::UnavailableInsufficientCoverage { required_ppm, actual_ppm } => json!({"kind": "unavailable", "reason": "insufficient_coverage", "requiredPpm": required_ppm, "actualPpm": actual_ppm}),
                RecommendationTrackRecordPerformance::Available { mean_gross_price_return, positive_outcomes, zero_outcomes, negative_outcomes } => json!({
                    "kind": "available",
                    "metric": "mean_gross_instrument_price_return",
                    "meanGrossPriceReturn": mean_gross_price_return.to_string(),
                    "positiveOutcomes": positive_outcomes,
                    "zeroOutcomes": zero_outcomes,
                    "negativeOutcomes": negative_outcomes,
                }),
            },
        })).collect::<Vec<_>>(),
        "forecastCalibrationIncluded": false,
        "executionPerformanceIncluded": false,
    })
}

fn recommendation_policy_value(policy: &RecommendationPolicy) -> Value {
    json!({
        "version": policy.version().get(),
        "digest": hex(policy.digest().bytes()),
        "actionZoneSemanticsVersion": policy.action_zone_semantics_version().get(),
        "horizonNanos": policy.horizon_nanos(),
        "proposalLifetimeNanos": policy.proposal_lifetime_nanos(),
        "assumptions": policy.assumptions().iter().map(|value| value.as_str()).collect::<Vec<_>>(),
        "invalidationConditions": policy.invalidation_conditions().iter().map(|value| value.as_str()).collect::<Vec<_>>(),
        "limitations": policy.limitations().iter().map(|value| value.as_str()).collect::<Vec<_>>(),
    })
}

fn investment_analysis_evidence_value(evidence: &InvestmentAnalysisEvidence) -> Value {
    json!({
        "instrumentId": evidence.instrument_id(),
        "currency": evidence.currency().as_str(),
        "accountId": evidence.account_id(),
        "asOf": evidence.as_of(),
        "market": evidence.market().map(market_evidence_value),
        "priceForecast": evidence.price_forecast().map(price_forecast_evidence_value),
        "valuation": evidence.valuation().map(valuation_evidence_value),
        "backtest": evidence.backtest().map(backtest_evidence_value),
        "liquidity": evidence.liquidity().map(liquidity_evidence_value),
        "portfolioRisk": evidence.portfolio_risk().map(portfolio_risk_evidence_value),
    })
}

fn market_evidence_value(evidence: &MarketReferenceEvidence) -> Value {
    json!({
        "instrumentId": evidence.instrument_id(),
        "price": money_value(evidence.price()),
        "quality": data_quality_name(evidence.quality()),
        "priceKind": market_price_kind_name(evidence.price_kind()),
        "adjustmentBasis": market_adjustment_basis_name(evidence.adjustment_basis()),
        "selectionReceiptIdentity": content_identity_value(evidence.selection_receipt_identity()),
        "selectedObservationIdentity": content_identity_value(evidence.selected_observation_identity()),
        "window": evidence_window_value(evidence.window()),
    })
}

fn price_forecast_evidence_value(evidence: &PriceForecastEvidence) -> Value {
    let calibration = evidence.calibration();
    json!({
        "instrumentId": evidence.instrument_id(),
        "cases": price_cases_value(evidence.cases()),
        "ranges": forecast_ranges_value(evidence.ranges()),
        "horizonAt": evidence.horizon_at(),
        "expectedTerminal": expected_terminal_value(evidence),
        "vintageId": hex(evidence.vintage_id().bytes()),
        "outputBindingIdentity": content_identity_value(evidence.output_binding_identity()),
        "calibrationIdentity": content_identity_value(evidence.calibration_identity()),
        "outcomeSetIdentity": content_identity_value(evidence.outcome_set_identity()),
        "calibration": {
            "nominalCoveragePpm": calibration.nominal_coverage_ppm(),
            "realizedCoveragePpm": calibration.realized_coverage_ppm(),
            "completedOutcomes": calibration.completed_outcomes().get(),
        },
        "window": evidence_window_value(evidence.window()),
    })
}

fn expected_terminal_value(evidence: &PriceForecastEvidence) -> Option<Value> {
    match (
        evidence.expected_terminal_statistic(),
        evidence.expected_terminal_price(),
        evidence.expected_terminal_horizon_at(),
        evidence.expected_terminal_statistic_identity(),
    ) {
        (
            Some(ForecastCentralStatistic::ModelEstimatedConditionalMean),
            Some(price),
            Some(horizon_at),
            Some(statistic_identity),
        ) => Some(json!({
            "statistic": "model_estimated_conditional_mean",
            "price": money_value(price),
            "horizonAt": horizon_at,
            "statisticIdentity": content_identity_value(statistic_identity),
        })),
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

fn valuation_evidence_value(evidence: &ValuationEvidence) -> Value {
    json!({
        "instrumentId": evidence.instrument_id(),
        "fairValue": money_value(evidence.fair_value()),
        "basis": valuation_basis_name(evidence.basis()),
        "horizonAt": evidence.horizon_at(),
        "measurementId": hex(evidence.measurement_id().bytes()),
        "classificationDecisionId": hex(evidence.classification_decision_id().bytes()),
        "selectionReceiptHash": hex(evidence.selection_receipt_hash().bytes()),
        "window": evidence_window_value(evidence.window()),
    })
}

fn backtest_evidence_value(evidence: &CostAdjustedPitBacktestEvidence) -> Value {
    json!({
        "instrumentId": evidence.instrument_id(),
        "currency": evidence.currency().as_str(),
        "outcomeHorizonNanos": evidence.outcome_horizon_nanos(),
        "netReturnBasisPoints": evidence.net_return().get(),
        "maxDrawdownBasisPoints": evidence.max_drawdown().get(),
        "feeBasisPoints": evidence.fee_basis_points().get(),
        "slippageBasisPoints": evidence.slippage_basis_points().get(),
        "maximumRandomSlippageBasisPoints": evidence.maximum_random_slippage_basis_points().get(),
        "observations": evidence.observations().get(),
        "trials": evidence.trials().get(),
        "stabilityPpm": evidence.stability_ppm(),
        "simulationCutoffAt": evidence.simulation_cutoff_at(),
        "datasetIdentity": content_identity_value(evidence.dataset_identity()),
        "commandIdentity": content_identity_value(evidence.command_identity()),
        "terminalIdentity": content_identity_value(evidence.terminal_identity()),
        "reportIdentity": content_identity_value(evidence.report_identity()),
        "cohortIdentity": content_identity_value(evidence.cohort_identity()),
        "costModelIdentity": content_identity_value(evidence.cost_model_identity()),
        "window": evidence_window_value(evidence.window()),
    })
}

fn liquidity_evidence_value(evidence: &LiquidityEvidence) -> Value {
    json!({
        "instrumentId": evidence.instrument_id(),
        "currency": evidence.currency().as_str(),
        "quotedSpreadBasisPoints": evidence.quoted_spread().get(),
        "capacityPpm": evidence.capacity_ppm(),
        "quality": data_quality_name(evidence.quality()),
        "assessmentIdentity": content_identity_value(evidence.assessment_identity()),
        "window": evidence_window_value(evidence.window()),
    })
}

fn portfolio_risk_evidence_value(evidence: &PortfolioRiskEvidence) -> Value {
    json!({
        "instrumentId": evidence.instrument_id(),
        "accountId": evidence.account_id(),
        "currency": evidence.currency().as_str(),
        "portfolioRevision": hex(evidence.portfolio_revision().bytes()),
        "positionState": position_state_value(evidence.position_state()),
        "riskCapacityPpm": evidence.risk_capacity_ppm(),
        "riskReportIdentity": content_identity_value(evidence.risk_report_identity()),
        "window": evidence_window_value(evidence.window()),
    })
}

fn generated_result_value(proposal: &GeneratedInvestmentProposal) -> Value {
    json!({
        "kind": "generated",
        "proposalId": hex(proposal.proposal_id().bytes()),
        "derivationDigest": hex(proposal.derivation_digest().bytes()),
        "action": action_name(proposal.action()),
        "priceLadder": price_ladder_value(proposal.price_ladder()),
        "actionZoneSemantics": {
            "version": proposal.action_zone_semantics_version().get(),
            "referenceZone": proposal.action_trigger_reference_zone().map(price_range_value),
            "triggerFloorExclusive": proposal.action_trigger_floor_exclusive().map(money_value),
            "triggerFloorInclusive": proposal.action_trigger_floor_inclusive().map(money_value),
            "triggerCeilingInclusive": proposal.action_trigger_ceiling_inclusive().map(money_value),
        },
        "evidenceReliability": evidence_reliability_value(proposal.confidence()),
        "horizonAt": proposal.horizon_at(),
        "expiresAt": proposal.expires_at(),
    })
}

fn no_action_result_value(proposal: &NoActionInvestmentProposal) -> Value {
    json!({
        "kind": "no_action",
        "proposalId": hex(proposal.proposal_id().bytes()),
        "derivationDigest": hex(proposal.derivation_digest().bytes()),
        "reason": no_action_reason_name(proposal.reason()),
        "invalidators": proposal.invalidators().iter().copied().map(invalidator_name).collect::<Vec<_>>(),
        "evidenceReliability": evidence_reliability_value(proposal.confidence()),
        "horizonAt": proposal.horizon_at(),
        "expiresAt": proposal.expires_at(),
    })
}

fn unavailable_result_value(analysis: &UnavailableInvestmentAnalysis) -> Value {
    json!({
        "kind": "unavailable",
        "reason": unavailable_reason_value(analysis.reason()),
        "horizonAt": analysis.horizon_at(),
        "expiresAt": analysis.expires_at(),
    })
}

fn investment_analysis_locator_value(entry: &InvestmentProposalIndexEntry) -> Value {
    json!({
        "analysisId": hex(entry.analysis_id().bytes()),
        "proposalId": entry.proposal_id().map(|value| hex(value.bytes())),
        "derivationDigest": entry.derivation_digest().map(|value| hex(value.bytes())),
        "instrumentId": entry.instrument_id(),
        "accountId": entry.account_id(),
        "currency": entry.currency().as_str(),
        "asOf": entry.as_of(),
        "horizonAt": entry.horizon_at(),
        "expiresAt": entry.expires_at(),
        "policyDigest": hex(entry.policy_digest().bytes()),
        "evidenceDigest": hex(entry.evidence_digest().bytes()),
        "outcome": match entry.outcome() {
            InvestmentProposalIndexOutcome::Generated(action) => {
                json!({"kind": "generated", "action": action_name(action)})
            }
            InvestmentProposalIndexOutcome::NoAction(reason) => {
                json!({"kind": "no_action", "reason": no_action_reason_name(reason)})
            }
            InvestmentProposalIndexOutcome::Unavailable(reason) => {
                json!({"kind": "unavailable", "reason": unavailable_reason_value(reason)})
            }
        },
    })
}

fn price_ladder_value(ladder: GeneratedPriceLadder) -> Value {
    json!({
        "cases": price_cases_value(ladder.cases()),
        "ranges": {
            "downside": price_range_value(ladder.downside_range()),
            "base": price_range_value(ladder.base_range()),
            "upside": price_range_value(ladder.upside_range()),
            "entry": price_range_value(ladder.entry_range()),
            "add": price_range_value(ladder.add_range()),
            "trim": price_range_value(ladder.trim_range()),
            "exit": price_range_value(ladder.exit_range()),
        },
        "addCase": money_value(ladder.add_case()),
    })
}

fn price_cases_value(cases: TargetPriceCases) -> Value {
    json!({
        "downside": money_value(cases.downside()),
        "base": money_value(cases.base()),
        "upside": money_value(cases.upside()),
    })
}

fn forecast_ranges_value(ranges: ForecastPriceRanges) -> Value {
    json!({
        "downside": price_range_value(ranges.downside()),
        "base": price_range_value(ranges.base()),
        "upside": price_range_value(ranges.upside()),
    })
}

fn price_range_value(range: TargetPriceRange) -> Value {
    json!({
        "lower": money_value(range.lower()),
        "upper": money_value(range.upper()),
    })
}

fn money_value(money: Money) -> Value {
    json!({
        "amount": money.amount().to_string(),
        "currency": money.currency().as_str(),
    })
}

fn evidence_reliability_value(reliability: RecommendationConfidence) -> Value {
    json!({
        "meaning": confidence_meaning_name(reliability.meaning()),
        "valuePpm": reliability.value_ppm(),
        "components": reliability.components().iter().map(|component| json!({
            "kind": confidence_component_name(component.kind()),
            "valuePpm": component.value_ppm(),
            "weightPpm": component.weight_ppm(),
        })).collect::<Vec<_>>(),
    })
}

fn position_state_value(state: PortfolioPositionState) -> Value {
    match state {
        PortfolioPositionState::NoPosition => json!({"kind": "no_position"}),
        PortfolioPositionState::Position {
            add_allowed,
            trim_allowed,
            exit_allowed,
        } => json!({
            "kind": "position",
            "addAllowed": add_allowed,
            "trimAllowed": trim_allowed,
            "exitAllowed": exit_allowed,
        }),
    }
}

fn evidence_window_value(window: ProposalEvidenceWindow) -> Value {
    json!({
        "observedAt": window.observed_at(),
        "availableAt": window.available_at(),
        "expiresAt": window.expires_at(),
        "contentIdentity": content_identity_value(window.content_identity()),
    })
}

fn content_identity_value(identity: DecisionContentDigest) -> Value {
    let digest = identity.evidence_digest();
    json!({
        "algorithm": digest_algorithm_name(digest.algorithm()),
        "digest": hex(digest.bytes()),
    })
}

fn unavailable_reason_value(reason: ProposalUnavailableReason) -> Value {
    match reason {
        ProposalUnavailableReason::MissingEvidence(evidence) => json!({
            "kind": "missing_evidence",
            "evidence": evidence_kind_name(evidence),
        }),
        ProposalUnavailableReason::InstrumentMismatch {
            evidence,
            expected,
            actual,
        } => json!({
            "kind": "instrument_mismatch",
            "evidence": evidence_kind_name(evidence),
            "expected": expected,
            "actual": actual,
        }),
        ProposalUnavailableReason::CurrencyMismatch {
            evidence,
            expected,
            actual,
        } => json!({
            "kind": "currency_mismatch",
            "evidence": evidence_kind_name(evidence),
            "expected": expected.as_str(),
            "actual": actual.as_str(),
        }),
        ProposalUnavailableReason::AccountMismatch { expected, actual } => json!({
            "kind": "account_mismatch",
            "expected": expected,
            "actual": actual,
        }),
        ProposalUnavailableReason::NotAvailableAtCutoff(evidence) => json!({
            "kind": "not_available_at_cutoff",
            "evidence": evidence_kind_name(evidence),
        }),
        ProposalUnavailableReason::ExpiredEvidence(evidence) => json!({
            "kind": "expired_evidence",
            "evidence": evidence_kind_name(evidence),
        }),
        ProposalUnavailableReason::StaleEvidence(evidence) => json!({
            "kind": "stale_evidence",
            "evidence": evidence_kind_name(evidence),
        }),
        ProposalUnavailableReason::RejectedQuality { evidence, quality } => json!({
            "kind": "rejected_quality",
            "evidence": evidence_kind_name(evidence),
            "quality": data_quality_name(quality),
        }),
        ProposalUnavailableReason::ForecastHorizonMismatch { expected, actual } => json!({
            "kind": "forecast_horizon_mismatch",
            "expected": expected,
            "actual": actual,
        }),
        ProposalUnavailableReason::ValuationHorizonMismatch { expected, actual } => json!({
            "kind": "valuation_horizon_mismatch",
            "expected": expected,
            "actual": actual,
        }),
        ProposalUnavailableReason::BacktestHorizonMismatch {
            expected_nanos,
            actual_nanos,
        } => json!({
            "kind": "backtest_horizon_mismatch",
            "expectedNanos": expected_nanos,
            "actualNanos": actual_nanos,
        }),
        ProposalUnavailableReason::InsufficientForecastOutcomes { required, actual } => json!({
            "kind": "insufficient_forecast_outcomes",
            "required": required.get(),
            "actual": actual.get(),
        }),
        ProposalUnavailableReason::UnsupportedForecastCoverage {
            minimum_ppm,
            maximum_ppm,
            actual_ppm,
        } => json!({
            "kind": "unsupported_forecast_coverage",
            "minimumPpm": minimum_ppm,
            "maximumPpm": maximum_ppm,
            "actualPpm": actual_ppm,
        }),
        ProposalUnavailableReason::InsufficientBacktestObservations { required, actual } => json!({
            "kind": "insufficient_backtest_observations",
            "required": required.get(),
            "actual": actual.get(),
        }),
        ProposalUnavailableReason::InsufficientBacktestTrials { required, actual } => json!({
            "kind": "insufficient_backtest_trials",
            "required": required.get(),
            "actual": actual.get(),
        }),
        ProposalUnavailableReason::ReservedPortfolioRevision => {
            json!({"kind": "reserved_portfolio_revision"})
        }
    }
}

fn analysis_id(value: &str) -> Result<InvestmentAnalysisId, ServiceError> {
    let bytes = decode_sha256(value)?;
    InvestmentAnalysisId::try_from_bytes(bytes).map_err(|_error| ServiceError::InvalidRequest)
}

fn decode_sha256(value: &str) -> Result<[u8; 32], ServiceError> {
    let bytes = value.as_bytes();
    if bytes.len() != 64 {
        return Err(ServiceError::InvalidRequest);
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in bytes.chunks_exact(2).enumerate() {
        decoded[index] = hex_digit(pair[0])? * 16 + hex_digit(pair[1])?;
    }
    Ok(decoded)
}

const fn hex_digit(value: u8) -> Result<u8, ServiceError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(ServiceError::InvalidRequest),
    }
}

fn hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

const fn action_name(action: RecommendationAction) -> &'static str {
    match action {
        RecommendationAction::Buy => "buy",
        RecommendationAction::Add => "add",
        RecommendationAction::Hold => "hold",
        RecommendationAction::Trim => "trim",
        RecommendationAction::Sell => "sell",
    }
}

const fn recommendation_outcome_cohort_name(cohort: RecommendationOutcomeCohort) -> &'static str {
    match cohort {
        RecommendationOutcomeCohort::Generated(RecommendationAction::Buy) => "buy",
        RecommendationOutcomeCohort::Generated(RecommendationAction::Add) => "add",
        RecommendationOutcomeCohort::Generated(RecommendationAction::Hold) => "hold",
        RecommendationOutcomeCohort::Generated(RecommendationAction::Trim) => "trim",
        RecommendationOutcomeCohort::Generated(RecommendationAction::Sell) => "sell",
        RecommendationOutcomeCohort::NoActionControl => "no_action_control",
        RecommendationOutcomeCohort::AnalysisUnavailable => "analysis_unavailable",
    }
}

const fn recommendation_outcome_unavailable_reason_name(
    reason: RecommendationOutcomeUnavailableReason,
) -> &'static str {
    match reason {
        RecommendationOutcomeUnavailableReason::AnalysisUnavailable(_) => "analysis_unavailable",
        RecommendationOutcomeUnavailableReason::OutcomeObservationUnavailable => {
            "outcome_observation_unavailable"
        }
        RecommendationOutcomeUnavailableReason::AmbiguousOutcomeObservation => {
            "ambiguous_outcome_observation"
        }
        RecommendationOutcomeUnavailableReason::IncompleteOutcomeObservation => {
            "incomplete_outcome_observation"
        }
        RecommendationOutcomeUnavailableReason::CorporateActionEvidenceUnavailable => {
            "corporate_action_evidence_unavailable"
        }
    }
}

const fn sizing_unavailable_reason_name(reason: SizingUnavailableReason) -> &'static str {
    match reason {
        SizingUnavailableReason::CapacityNotSupplied(_) => "capacity_not_supplied",
        SizingUnavailableReason::CapacityNotYetAvailable(_) => "capacity_not_yet_available",
        SizingUnavailableReason::CapacityExpired(_) => "capacity_expired",
        SizingUnavailableReason::CapacityRangeContainsNoLots(_) => {
            "capacity_range_contains_no_lots"
        }
        SizingUnavailableReason::CashReserveExceedsGrossLiquidatableValue => {
            "cash_reserve_exceeds_gross_liquidatable_value"
        }
        SizingUnavailableReason::NoHardFeasibleLotIntersection => {
            "no_hard_feasible_lot_intersection"
        }
        SizingUnavailableReason::PreferredWeightRangeContainsNoLots => {
            "preferred_weight_range_contains_no_lots"
        }
        SizingUnavailableReason::NoPreferredFeasibleLotIntersection => {
            "no_preferred_feasible_lot_intersection"
        }
    }
}

const fn no_action_reason_name(reason: NoActionReason) -> &'static str {
    match reason {
        NoActionReason::ConflictingForecastAndValuation => "conflicting_forecast_and_valuation",
        NoActionReason::BacktestBelowPolicy => "backtest_below_policy",
        NoActionReason::LiquidityBelowPolicy => "liquidity_below_policy",
        NoActionReason::PortfolioRiskBelowPolicy => "portfolio_risk_below_policy",
        NoActionReason::ConfidenceBelowPolicy => "evidence_reliability_below_policy",
        NoActionReason::PositionStateNotActionable => "position_state_not_actionable",
        NoActionReason::GeneratedPriceOrderCollapsed => "generated_price_order_collapsed",
    }
}

const fn invalidator_name(invalidator: ProposalInvalidator) -> &'static str {
    match invalidator {
        ProposalInvalidator::ForecastValuationConflict => "forecast_valuation_conflict",
        ProposalInvalidator::BacktestPolicyBreach => "backtest_policy_breach",
        ProposalInvalidator::LiquidityPolicyBreach => "liquidity_policy_breach",
        ProposalInvalidator::PortfolioRiskPolicyBreach => "portfolio_risk_policy_breach",
        ProposalInvalidator::ConfidencePolicyBreach => "evidence_reliability_policy_breach",
        ProposalInvalidator::PositionStateIncompatible => "position_state_incompatible",
        ProposalInvalidator::GeneratedPriceOrderCollapsed => "generated_price_order_collapsed",
    }
}

const fn confidence_meaning_name(meaning: RecommendationConfidenceMeaning) -> &'static str {
    match meaning {
        RecommendationConfidenceMeaning::PolicyWeightedEvidenceReliabilityV1 => {
            "policy_weighted_evidence_reliability_v1"
        }
    }
}

const fn confidence_component_name(kind: RecommendationConfidenceComponentKind) -> &'static str {
    match kind {
        RecommendationConfidenceComponentKind::ForecastCalibration => "forecast_calibration",
        RecommendationConfidenceComponentKind::ValuationAgreement => "valuation_agreement",
        RecommendationConfidenceComponentKind::BacktestStability => "backtest_stability",
        RecommendationConfidenceComponentKind::MarketIntegrity => "market_integrity",
        RecommendationConfidenceComponentKind::LiquidityCapacity => "liquidity_capacity",
        RecommendationConfidenceComponentKind::PortfolioRiskCapacity => "portfolio_risk_capacity",
    }
}

const fn evidence_kind_name(kind: RecommendationEvidenceKind) -> &'static str {
    match kind {
        RecommendationEvidenceKind::Market => "market",
        RecommendationEvidenceKind::PriceForecast => "price_forecast",
        RecommendationEvidenceKind::Valuation => "valuation",
        RecommendationEvidenceKind::Backtest => "backtest",
        RecommendationEvidenceKind::Liquidity => "liquidity",
        RecommendationEvidenceKind::PortfolioRisk => "portfolio_risk",
    }
}

const fn data_quality_name(quality: DataQuality) -> &'static str {
    match quality {
        DataQuality::DirectVerified => "direct_verified",
        DataQuality::DirectUnverified => "direct_unverified",
        DataQuality::OfficialDelayed => "official_delayed",
        DataQuality::Aggregated => "aggregated",
        DataQuality::Indicative => "indicative",
        DataQuality::Modeled => "modeled",
        DataQuality::Estimated => "estimated",
        DataQuality::Stale => "stale",
        DataQuality::Quarantined => "quarantined",
    }
}

const fn market_price_kind_name(kind: MarketReferencePriceKind) -> &'static str {
    match kind {
        MarketReferencePriceKind::LastTrade => "last_trade",
        MarketReferencePriceKind::CheckedBidAskMidpoint => "checked_bid_ask_midpoint",
    }
}

const fn market_adjustment_basis_name(basis: MarketReferenceAdjustmentBasis) -> &'static str {
    match basis {
        MarketReferenceAdjustmentBasis::UnadjustedSpot => "unadjusted_spot",
    }
}

const fn valuation_basis_name(basis: ValuationAmountBasis) -> &'static str {
    match basis {
        ValuationAmountBasis::PerInstrumentUnit => "per_instrument_unit",
        ValuationAmountBasis::ReportingEntityTotal => "reporting_entity_total",
        ValuationAmountBasis::PositionTotal => "position_total",
    }
}

const fn digest_algorithm_name(algorithm: DigestAlgorithm) -> &'static str {
    match algorithm {
        DigestAlgorithm::Sha256 => "sha256",
        DigestAlgorithm::Blake3 => "blake3",
    }
}
