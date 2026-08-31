//! Pure read-side transport over retained, authority-recomputed investment analyses.

use std::sync::Arc;

use market_squawk_data::{MarketDataInstrumentCatalogError, MarketDataInstrumentReadCapability};
use market_squawk_decisions::{
    CostAdjustedPitBacktestEvidence, ExactFinancialRatio, ExpectedGrossPricePnlAvailability,
    ExpectedReturnAvailability, FeasibleLotRangeAvailability, GrossPricePnlAvailability,
    InvestmentAnalysisEvidence, InvestmentOutcomeProjection, InvestmentProposalDecision,
    InvestmentProposalIndexEntry, InvestmentProposalIndexOutcome, InvestmentSizingProjection,
    NoActionReason, PortfolioPositionState, ProposalInvalidator, ProposalUnavailableReason,
    RecommendationAction, RecommendationConfidence, RecommendationConfidenceComponentKind,
    RecommendationConfidenceMeaning, RecommendationOutcomeCohort, RecommendationOutcomeStatus,
    RecommendationOutcomeUnavailableReason, RecommendationTrackRecord,
    RecommendationTrackRecordGroup, RecommendationTrackRecordPerformance, SignedMoneyRange,
    SizingUnavailableReason, TargetPriceRange,
};
use market_squawk_domain::{AccountId, InstrumentId, Money};
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::application::decision::{DecisionApplication, InvestmentAnalysisRead};
use crate::portfolio_application::{
    PortfolioAccountCatalogError, PortfolioAccountCatalogReadCapability,
    PortfolioAccountCatalogSnapshot,
};

use super::{decode, ensure_live, map_application, page_fetch_limit};

pub(super) const GET_INVESTMENT_ANALYSIS: &str = "Decision.GetInvestmentAnalysis";
pub(super) const LIST_INVESTMENT_ANALYSES: &str = "Decision.ListInvestmentAnalyses";
pub(super) const GET_RECOMMENDATION_TRACK_RECORD: &str = "Decision.GetRecommendationTrackRecord";

/// Closed read-only operation family over durable investment-analysis results.
pub(super) struct InvestmentAnalysisOperations {
    decisions: Arc<DecisionApplication>,
    instruments: MarketDataInstrumentReadCapability,
    accounts: PortfolioAccountCatalogReadCapability,
}

impl InvestmentAnalysisOperations {
    pub(super) fn new(
        decisions: Arc<DecisionApplication>,
        instruments: MarketDataInstrumentReadCapability,
        accounts: PortfolioAccountCatalogReadCapability,
    ) -> Self {
        Self {
            decisions,
            instruments,
            accounts,
        }
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
                let action_token = action_token(&input.action_token)?;
                let analysis_id = self
                    .decisions
                    .resolve_investment_analysis_product_token(action_token)
                    .map_err(map_application)?;
                let analysis = self
                    .decisions
                    .read_investment_analysis(analysis_id)
                    .map_err(map_application)?;
                let account_catalog = self
                    .accounts
                    .snapshot_current(context.deadline(), context.cancellation())
                    .map_err(map_account_catalog)?;
                let value = investment_analysis_value(
                    &analysis,
                    action_token,
                    self.instrument_display(analysis.decision.evidence().instrument_id(), context)?,
                    portfolio_label(&account_catalog, analysis.decision.evidence().account_id())?,
                )?;
                self.accounts
                    .recheck(&account_catalog, context.deadline(), context.cancellation())
                    .map_err(map_account_catalog)?;
                ensure_live(context)?;
                TypedToolResult::try_new(
                    value,
                    1,
                    ToolResultMetadata::complete_not_applicable(),
                    context.limits(),
                )
                .map_err(Into::into)
            }
            LIST_INVESTMENT_ANALYSES => {
                let input: InvestmentAnalysisListRequest = decode(&arguments)?;
                let after = input
                    .after_action_token
                    .as_deref()
                    .map(action_token)
                    .transpose()?;
                let after = after
                    .map(|token| {
                        self.decisions
                            .resolve_investment_analysis_product_token(token)
                            .map_err(map_application)
                    })
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
                let next_after_action_token = if truncated {
                    analyses
                        .last()
                        .map(|entry| {
                            self.decisions
                                .investment_analysis_product_token(entry.analysis_id())
                                .map(|token| token.to_string())
                                .map_err(map_application)
                        })
                        .transpose()?
                } else {
                    None
                };
                let metadata = if truncated {
                    ToolResultMetadata::try_truncated_not_applicable(available)?
                } else {
                    ToolResultMetadata::complete_not_applicable()
                };
                let completeness = if truncated { "truncated" } else { "complete" };
                let account_catalog = self
                    .accounts
                    .snapshot_current(context.deadline(), context.cancellation())
                    .map_err(map_account_catalog)?;
                let mut values = Vec::new();
                values
                    .try_reserve_exact(analyses.len())
                    .map_err(|_| ServiceError::ResourceExhausted)?;
                for analysis in &analyses {
                    let token = self
                        .decisions
                        .investment_analysis_product_token(analysis.analysis_id())
                        .map_err(map_application)?;
                    values.push(investment_analysis_locator_value(
                        analysis,
                        token,
                        self.instrument_display(analysis.instrument_id(), context)?,
                        portfolio_label(&account_catalog, analysis.account_id())?,
                    ));
                }
                self.accounts
                    .recheck(&account_catalog, context.deadline(), context.cancellation())
                    .map_err(map_account_catalog)?;
                ensure_live(context)?;
                TypedToolResult::try_new(
                    json!({
                        "completeness": completeness,
                        "returnedCount": returned,
                        "availableCount": available,
                        "nextAfterActionToken": next_after_action_token,
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
                let action_token = action_token(&input.action_token)?;
                let analysis_id = self
                    .decisions
                    .resolve_investment_analysis_product_token(action_token)
                    .map_err(map_application)?;
                let analysis = self
                    .decisions
                    .read_investment_analysis(analysis_id)
                    .map_err(map_application)?;
                let publication = analysis
                    .current
                    .as_ref()
                    .ok_or(ServiceError::InvalidRequest)?
                    .publication();
                let track_record = self
                    .decisions
                    .recommendation_track_record(
                        publication.analytical_profile(),
                        analysis.decision.policy().horizon_nanos(),
                        super::super::runtime::current_timestamp()
                            .map_err(|_| ServiceError::Unavailable)?,
                    )
                    .map_err(map_application)?;
                let value = recommendation_track_record_value(action_token, &track_record)?;
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

impl InvestmentAnalysisOperations {
    fn instrument_display(
        &self,
        instrument_id: InstrumentId,
        context: &RequestContext,
    ) -> Result<InvestmentDisplay, ServiceError> {
        let record = self
            .instruments
            .latest(instrument_id, context.deadline(), context.cancellation())
            .map_err(map_instrument_catalog)?;
        let Some(record) = record else {
            return Ok(InvestmentDisplay::default());
        };
        let definition = record.definition();
        let name = definition
            .display_name()
            .map(|name| name.as_str().to_owned());
        Ok(InvestmentDisplay { symbol: None, name })
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
    action_token: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InvestmentAnalysisListRequest {
    after_action_token: Option<String>,
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RecommendationTrackRecordRequest {
    action_token: String,
}

#[derive(Default)]
struct InvestmentDisplay {
    symbol: Option<String>,
    name: Option<String>,
}

fn investment_analysis_value(
    read: &InvestmentAnalysisRead,
    action_token: Uuid,
    investment: InvestmentDisplay,
    portfolio_label: String,
) -> Result<Value, ServiceError> {
    let decision = &read.decision;
    let evidence = decision.evidence();
    let realized_outcome = read
        .current
        .as_ref()
        .and_then(|value| value.current_outcome())
        .map(recommendation_outcome_current_value)
        .transpose()?;
    Ok(json!({
        "actionToken": action_token,
        "investment": investment_value(&investment),
        "portfolioLabel": portfolio_label,
        "currency": evidence.currency().as_str(),
        "recommendation": recommendation_value(decision),
        "horizon": horizon_value(decision),
        "priceSummary": price_summary_value(decision),
        "reasons": recommendation_reasons(decision, &portfolio_label),
        "risks": investment_risks(decision),
        "assumptions": decision.policy().assumptions().iter().map(|value| value.as_str()).collect::<Vec<_>>(),
        "invalidators": invalidators_value(decision),
        "evidenceSummary": evidence_summary_value(decision),
        "analyticalEvidence": analytical_evidence_value(decision),
        "liquidity": liquidity_value(evidence),
        "portfolioContext": portfolio_context_value(evidence, &portfolio_label),
        "outcomeProjection": read.outcome_projection.as_ref().map(outcome_projection_value),
        "sizing": read.sizing_projection.as_ref().map(sizing_projection_value),
        "virtualPaperEligibility": virtual_paper_eligibility_value(),
        "realizedOutcome": realized_outcome,
        "trackRecordActionToken": read.current.as_ref().map(|_| action_token),
    }))
}

fn investment_value(value: &InvestmentDisplay) -> Value {
    json!({"symbol": value.symbol, "name": value.name})
}

fn recommendation_value(decision: &InvestmentProposalDecision) -> Value {
    match decision {
        InvestmentProposalDecision::Generated(proposal) => json!({
            "kind": "action",
            "action": action_name(proposal.action()),
            "summary": generated_action_summary(proposal.action()),
        }),
        InvestmentProposalDecision::NoAction(_) => json!({
            "kind": "abstain",
            "summary": "No investment action is supported for this saved horizon.",
        }),
        InvestmentProposalDecision::Unavailable(_) => json!({
            "kind": "unavailable",
            "summary": "The saved analysis cannot support an investment action.",
        }),
    }
}

fn horizon_value(decision: &InvestmentProposalDecision) -> Value {
    json!({
        "informationCurrentThrough": super::product_timestamp(decision.evidence().as_of()),
        "endsAt": super::product_timestamp(match decision {
            InvestmentProposalDecision::Generated(value) => value.horizon_at(),
            InvestmentProposalDecision::NoAction(value) => value.horizon_at(),
            InvestmentProposalDecision::Unavailable(value) => value.horizon_at(),
        }),
        "expiresAt": super::product_timestamp(match decision {
            InvestmentProposalDecision::Generated(value) => value.expires_at(),
            InvestmentProposalDecision::NoAction(value) => value.expires_at(),
            InvestmentProposalDecision::Unavailable(value) => value.expires_at(),
        }),
    })
}

fn price_summary_value(decision: &InvestmentProposalDecision) -> Value {
    let evidence = decision.evidence();
    let action_ranges = match decision {
        InvestmentProposalDecision::Generated(proposal) => {
            let ladder = proposal.price_ladder();
            Some(json!({
                "entry": price_range_value(ladder.entry_range()),
                "add": price_range_value(ladder.add_range()),
                "trim": price_range_value(ladder.trim_range()),
                "exit": price_range_value(ladder.exit_range()),
            }))
        }
        InvestmentProposalDecision::NoAction(_) | InvestmentProposalDecision::Unavailable(_) => {
            None
        }
    };
    json!({
        "current": evidence.market().map(|value| money_value(value.price())),
        "fairValue": evidence.valuation().map(|value| money_value(value.fair_value())),
        "scenarios": evidence.price_forecast().map(|value| json!({
            "endsAt": super::product_timestamp(value.horizon_at()),
            "downside": price_range_value(value.ranges().downside()),
            "base": price_range_value(value.ranges().base()),
            "upside": price_range_value(value.ranges().upside()),
        })),
        "actionRanges": action_ranges,
    })
}

fn outcome_projection_value(value: &InvestmentOutcomeProjection) -> Value {
    json!({
        "startingPrice": money_value(value.mark()),
        "endsAt": super::product_timestamp(value.horizon_at()),
        "positionScale": value.position_scale().map(|scale| json!({
            "quantityLots": scale.quantity().get().to_string(),
            "summary": "Gross dollar ranges use this exact saved quantity and instrument scale."
        })),
        "downside": gross_range_value(value.downside()),
        "base": gross_range_value(value.base()),
        "upside": gross_range_value(value.upside()),
        "expectedReturn": expected_return_value(value.expected_return()),
        "expectedGrossPricePnl": expected_gross_price_pnl_value(value.expected_gross_price_pnl()),
        "netPnl": {
            "state": "unavailable",
            "summary": "Net profit or loss is unavailable because exact forward trading costs were not supplied."
        },
        "benchmarkReturn": {
            "state": "unavailable",
            "summary": "Benchmark-relative return is unavailable because exact proposal-time benchmark evidence was not supplied."
        },
        "afterTaxPnl": {
            "state": "unavailable",
            "summary": "After-tax profit or loss is unavailable because account-, lot-, and jurisdiction-specific tax evidence was not supplied."
        },
        "limitations": [
            "Projected price changes do not include future trading costs.",
            "Projected price changes are not compared with a benchmark.",
            "Projected price changes do not include taxes."
        ],
    })
}

fn gross_range_value(value: market_squawk_decisions::GrossMarkRelativeRange) -> Value {
    let ratio = value.gross_return_from_mark();
    let mut result = Map::from_iter([
        (
            "priceRange".to_owned(),
            price_range_value(value.price_range()),
        ),
        (
            "absolutePriceChange".to_owned(),
            signed_money_range_value(value.absolute_change()),
        ),
        (
            "grossPricePnl".to_owned(),
            gross_price_pnl_value(value.gross_price_pnl()),
        ),
    ]);
    if let (Some(lower), Some(upper)) = (
        exact_money_ratio_percentage(ratio.lower().numerator(), ratio.lower().denominator()),
        exact_money_ratio_percentage(ratio.upper().numerator(), ratio.upper().denominator()),
    ) {
        result.insert(
            "priceChangePercent".to_owned(),
            json!({"lower": lower, "upper": upper}),
        );
    }
    Value::Object(result)
}

fn expected_return_value(value: ExpectedReturnAvailability) -> Value {
    match value {
        ExpectedReturnAvailability::Available(ratio) => {
            let percentage = exact_financial_ratio_percentage(ratio);
            json!({
                "state": "available",
                "grossPriceReturnPercent": percentage,
                "exactRatio": exact_financial_ratio_value(ratio),
                "summary": if percentage.is_some() {
                    "Expected gross price return comes from an admitted conditional-mean terminal price; it is not a probability of profit."
                } else {
                    "An exact conditional-mean gross price-return ratio is retained, but it has no finite decimal percentage without rounding."
                }
            })
        }
        ExpectedReturnAvailability::UnavailableAdmittedExpectedTerminalValueNotSupplied => json!({
            "state": "unavailable",
            "summary": "Expected return is unavailable because no admitted conditional-mean terminal price was supplied. Scenario ranges are not an expected value."
        }),
    }
}

fn expected_gross_price_pnl_value(value: ExpectedGrossPricePnlAvailability) -> Value {
    match value {
        ExpectedGrossPricePnlAvailability::Available(amount) => json!({
            "state": "available",
            "amount": signed_money_value(amount),
            "summary": "This is exact-quantity expected gross price profit or loss before costs and tax."
        }),
        ExpectedGrossPricePnlAvailability::UnavailableAdmittedExpectedTerminalValueNotSupplied => {
            json!({
                "state": "unavailable",
                "summary": "Expected gross profit or loss is unavailable because no admitted conditional-mean terminal price was supplied."
            })
        }
        ExpectedGrossPricePnlAvailability::UnavailableExactQuantityNotSupplied => json!({
            "state": "unavailable",
            "summary": "Expected gross profit or loss is unavailable because no exact quantity and instrument scale were supplied."
        }),
    }
}

fn gross_price_pnl_value(value: GrossPricePnlAvailability) -> Value {
    match value {
        GrossPricePnlAvailability::Available(range) => json!({
            "state": "available",
            "range": signed_money_range_value(range),
            "summary": "This is exact-quantity gross price profit or loss before costs and tax."
        }),
        GrossPricePnlAvailability::UnavailableExactQuantityNotSupplied => json!({
            "state": "unavailable",
            "summary": "Gross profit or loss is unavailable because no exact quantity and instrument scale were supplied."
        }),
    }
}

fn exact_financial_ratio_value(value: ExactFinancialRatio) -> Value {
    json!({
        "numerator": signed_money_value(value.numerator()),
        "denominator": money_value(value.denominator()),
    })
}

fn exact_financial_ratio_percentage(value: ExactFinancialRatio) -> Option<String> {
    exact_money_ratio_percentage(value.numerator(), value.denominator())
}

fn signed_money_range_value(value: SignedMoneyRange) -> Value {
    json!({
        "lower": signed_money_value(value.lower()),
        "upper": signed_money_value(value.upper()),
    })
}

fn signed_money_value(money: Money) -> Value {
    json!({
        "amount": money.amount().normalize().to_string(),
        "currency": money.currency().as_str(),
    })
}

fn sizing_projection_value(value: &InvestmentSizingProjection) -> Value {
    json!({
        "evaluatedAt": super::product_timestamp(value.inputs().evaluated_at()),
        "currentLots": value.inputs().portfolio().current_lots().get().to_string(),
        "hardFeasibleLots": feasible_lots_value(value.hard_feasible_lots()),
        "preferredFeasibleLots": feasible_lots_value(value.preferred_feasible_lots()),
        "summary": "These are research sizing ranges, not an order or a selected target.",
    })
}

fn feasible_lots_value(value: &FeasibleLotRangeAvailability) -> Value {
    match value {
        FeasibleLotRangeAvailability::Available(range) => json!({
            "kind": "available",
            "lower": range.lower().get().to_string(),
            "upper": range.upper().get().to_string(),
        }),
        FeasibleLotRangeAvailability::Unavailable(reasons) => json!({
            "kind": "unavailable",
            "reasons": reasons.iter().copied().map(sizing_unavailable_reason_name).collect::<Vec<_>>(),
        }),
    }
}

fn recommendation_outcome_current_value(
    value: &market_squawk_decisions::RecommendationOutcomeCurrentIndexEntry,
) -> Result<Value, ServiceError> {
    Ok(json!({
        "evaluatedAt": super::product_timestamp(value.evaluated_at()),
        "result": recommendation_outcome_status_value(value.status())?,
    }))
}

fn recommendation_outcome_status_value(
    value: RecommendationOutcomeStatus,
) -> Result<Value, ServiceError> {
    Ok(match value {
        RecommendationOutcomeStatus::Pending(reason) => json!({
            "kind": "pending",
            "summary": match reason {
                market_squawk_decisions::RecommendationOutcomePendingReason::AwaitingHorizon => "The recommendation horizon has not ended yet.",
                market_squawk_decisions::RecommendationOutcomePendingReason::AwaitingOutcomeEvidence => "The horizon has ended, but comparable outcome information is not available yet.",
            },
        }),
        RecommendationOutcomeStatus::Unavailable(reason) => json!({
            "kind": "unavailable",
            "summary": recommendation_outcome_unavailable_reason_summary(reason),
        }),
        RecommendationOutcomeStatus::Completed(outcome) => {
            let observation = outcome.observation();
            let gross_price_return_percent =
                percentage_from_decimal_ratio(outcome.gross_price_return())?;
            json!({
                "kind": "completed",
                "metric": "gross_instrument_price_return",
                "startMark": money_value(outcome.start_mark()),
                "endpointPrice": money_value(observation.endpoint_price()),
                "grossPriceReturnPercent": gross_price_return_percent,
                "observedAt": super::product_timestamp(observation.observed_at()),
                "availableAt": super::product_timestamp(observation.available_at()),
                "limitations": [
                    "This is the investment's gross price return, not an executed account return.",
                    "Trading costs, benchmark performance, taxes, and settlement are not included."
                ],
            })
        }
    })
}

fn recommendation_track_record_value(
    action_token: Uuid,
    value: &RecommendationTrackRecord,
) -> Result<Value, ServiceError> {
    let groups = value
        .groups()
        .iter()
        .map(recommendation_track_record_group_value)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({
        "actionToken": action_token,
        "evaluatedAt": super::product_timestamp(value.evaluated_at()),
        "unavailableAnalysisCount": value.analysis_unavailable_count(),
        "minimumCompletedSamples": market_squawk_decisions::RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED,
        "minimumCoveragePercent": percentage_from_ppm(market_squawk_decisions::RECOMMENDATION_TRACK_RECORD_MINIMUM_COVERAGE_PPM),
        "groups": groups,
        "forecastCalibrationIncluded": false,
        "executionResultsIncluded": false,
        "summary": "Comparable history reports realized gross price outcomes only. It excludes execution, taxes, and personal portfolio results.",
    }))
}

fn recommendation_track_record_group_value(
    group: &RecommendationTrackRecordGroup,
) -> Result<Value, ServiceError> {
    let performance = match group.performance() {
        RecommendationTrackRecordPerformance::UnavailableNoDueOutcomes => json!({
            "kind": "unavailable",
            "summary": "No recommendations in this group have reached their horizon yet."
        }),
        RecommendationTrackRecordPerformance::UnavailableInsufficientCompletedSamples {
            required,
            actual,
        } => json!({
            "kind": "unavailable",
            "summary": "Too few completed outcomes are available for a meaningful result.",
            "required": required,
            "actual": actual
        }),
        RecommendationTrackRecordPerformance::UnavailableInsufficientCoverage {
            required_ppm,
            actual_ppm,
        } => json!({
            "kind": "unavailable",
            "summary": "Too many due outcomes are still missing for a meaningful result.",
            "requiredPercent": percentage_from_ppm(required_ppm),
            "actualPercent": percentage_from_ppm(actual_ppm)
        }),
        RecommendationTrackRecordPerformance::Available {
            mean_gross_price_return,
            positive_outcomes,
            zero_outcomes,
            negative_outcomes,
        } => json!({
            "kind": "available",
            "meanGrossPriceReturnPercent": percentage_from_decimal_ratio(mean_gross_price_return)?,
            "positiveOutcomes": positive_outcomes,
            "unchangedOutcomes": zero_outcomes,
            "negativeOutcomes": negative_outcomes,
            "summary": "This is realized gross price history, not an executed or guaranteed return."
        }),
    };
    Ok(json!({
        "action": recommendation_outcome_cohort_name(group.cohort()),
        "recommendationCount": group.publication_count(),
        "dueCount": group.due_count(),
        "completedCount": group.completed_count(),
        "pendingCount": group.pending_count(),
        "unavailableCount": group.unavailable_count(),
        "coveragePercent": percentage_from_ppm(group.coverage_ppm()),
        "performance": performance,
    }))
}

fn recommendation_reasons(
    decision: &InvestmentProposalDecision,
    portfolio_label: &str,
) -> Vec<String> {
    let evidence = decision.evidence();
    let mut reasons = Vec::new();
    match decision {
        InvestmentProposalDecision::NoAction(value) => {
            reasons.push(no_action_reason_summary(value.reason()).to_owned());
        }
        InvestmentProposalDecision::Unavailable(value) => {
            reasons.push(unavailable_reason_summary(value.reason()).to_owned());
        }
        InvestmentProposalDecision::Generated(_) => {}
    }
    if let Some(forecast) = evidence.price_forecast() {
        let range = forecast.ranges().base();
        reasons.push(format!(
            "The base forecast spans {} to {} through the investment horizon.",
            money_text(range.lower()),
            money_text(range.upper())
        ));
    }
    if let Some(valuation) = evidence.valuation() {
        reasons.push(format!(
            "The saved valuation estimates fair value at {}.",
            money_text(valuation.fair_value())
        ));
    }
    if let Some(backtest) = evidence.backtest() {
        reasons.push(format!(
            "The cost-adjusted historical test returned {}% across {} observations.",
            percentage_from_basis_points(backtest.net_return().get()),
            backtest.observations()
        ));
    }
    if let Some(liquidity) = evidence.liquidity() {
        reasons.push(format!(
            "Liquidity evidence showed a {}% quoted spread and {}% usable capacity.",
            percentage_from_basis_points(liquidity.quoted_spread().get()),
            percentage_from_ppm(liquidity.capacity_ppm())
        ));
    }
    if let Some(portfolio) = evidence.portfolio_risk() {
        reasons.push(format!(
            "{} had {}% of its saved risk capacity available.",
            portfolio_label,
            percentage_from_ppm(portfolio.risk_capacity_ppm())
        ));
    }
    if reasons.is_empty() {
        reasons.push("The saved evidence met every required check for this action.".to_owned());
    }
    reasons
}

fn investment_risks(decision: &InvestmentProposalDecision) -> Vec<&str> {
    let evidence = decision.evidence();
    let mut risks = decision
        .policy()
        .limitations()
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();
    if evidence.price_forecast().is_some() || evidence.valuation().is_some() {
        risks.push("Forecast and valuation ranges are estimates, not guaranteed prices.");
    }
    if evidence.backtest().is_some() {
        risks.push("Historical test results may not repeat in future markets.");
    }
    if matches!(decision, InvestmentProposalDecision::Generated(_)) {
        risks.push("Research ranges do not place trades or guarantee account results.");
    }
    risks
}

fn invalidators_value<'a>(decision: &'a InvestmentProposalDecision) -> Vec<&'a str> {
    let mut invalidators: Vec<&'a str> = decision
        .policy()
        .invalidation_conditions()
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();
    if let InvestmentProposalDecision::NoAction(value) = decision {
        for invalidator in value.invalidators().iter().copied() {
            let summary: &'a str = invalidator_summary(invalidator);
            invalidators.push(summary);
        }
    }
    invalidators
}

fn evidence_summary_value(decision: &InvestmentProposalDecision) -> Value {
    let evidence = decision.evidence();
    json!({
        "coverage": coverage_summary_value(evidence),
        "calibration": calibration_summary_value(evidence),
        "outOfSample": out_of_sample_summary_value(evidence),
        "historicalTest": evidence.backtest().map(historical_test_summary_value),
        "costs": cost_summary_value(evidence.backtest()),
        "uncertainty": uncertainty_summary_value(decision),
    })
}

fn analytical_evidence_value(decision: &InvestmentProposalDecision) -> Value {
    let evidence = decision.evidence();
    let broader_research = broader_research_availability(evidence);
    let combined = !matches!(decision, InvestmentProposalDecision::Unavailable(_));
    json!({
        "currentMarket": evidence_family_value(
            evidence.market().is_some(),
            "An eligible current market observation anchored the saved analysis.",
            "An eligible current market observation was not available."
        ),
        "broaderResearch": evidence_family_value(
            broader_research,
            "Broader research inputs were retained with the selected candidate; no one input set the recommendation.",
            "No qualifying broader research contribution was retained with the selected candidate."
        ),
        "pricePattern": evidence_family_value(
            evidence.harmonic_pattern().is_some(),
            "A confirmed price-pattern observation was retained; it did not set evidence reliability or create an action by itself.",
            "No current valid price pattern was retained."
        ),
        "forecast": evidence_family_value(
            evidence.price_forecast().is_some(),
            "A horizon-aligned calibrated price forecast contributed to the decision.",
            "A horizon-aligned calibrated price forecast was not available."
        ),
        "financialModel": evidence_family_value(
            evidence.financial_model().is_some(),
            "A financial model using information available at the time, explicit assumptions, scenarios, and sensitivity contributed to the decision.",
            "A qualifying financial model using information available at the time was not available."
        ),
        "valuation": evidence_family_value(
            evidence.valuation().is_some(),
            "An independently governed per-investment valuation contributed to the decision.",
            "An independently governed valuation was not available."
        ),
        "historicalTest": evidence_family_value(
            evidence.backtest().is_some(),
            "A cost-adjusted point-in-time historical test contributed to the decision.",
            "A qualifying cost-adjusted point-in-time historical test was not available."
        ),
        "outOfSample": evidence_family_value(
            evidence.out_of_sample().is_some(),
            "Chronological independent historical results contributed to the decision.",
            "Qualifying independent historical results were not available."
        ),
        "liquidity": evidence_family_value(
            evidence.liquidity().is_some(),
            "Current spread and usable trading capacity contributed to the decision.",
            "Qualifying liquidity evidence was not available."
        ),
        "portfolioRisk": evidence_family_value(
            evidence.portfolio_risk().is_some(),
            "The saved portfolio position and remaining risk capacity contributed to the decision.",
            "Qualifying selected-portfolio risk evidence was not available."
        ),
        "combination": {
            "state": if combined { "multi_evidence" } else { "insufficient" },
            "summary": if combined {
                "The saved decision combined forecast, financial modeling, governed valuation, chronological historical testing, market integrity, liquidity, and portfolio risk. Research patterns can support interpretation but cannot produce evidence reliability on their own."
            } else {
                "The independent evidence families could not support a recommendation. No model, feature, or market observation was promoted into confidence by itself."
            }
        }
    })
}

fn broader_research_availability(evidence: &InvestmentAnalysisEvidence) -> bool {
    evidence.selected_candidate().is_some_and(|candidate| {
        candidate
            .score_contributions()
            .iter()
            .any(|contribution| contribution.observed().is_some())
    })
}

fn evidence_family_value(
    available: bool,
    available_summary: &str,
    unavailable_summary: &str,
) -> Value {
    json!({
        "state": if available { "available" } else { "unavailable" },
        "summary": if available { available_summary } else { unavailable_summary },
    })
}

fn liquidity_value(evidence: &InvestmentAnalysisEvidence) -> Value {
    match evidence.liquidity() {
        Some(value) => json!({
            "state": "available",
            "quotedSpreadPercent": percentage_from_basis_points(value.quoted_spread().get()),
            "policyRelativeCapacityPercent": percentage_from_ppm(value.capacity_ppm()),
            "summary": "Spread and usable capacity describe current marketability. They are not a promise of a future fill."
        }),
        None => json!({
            "state": "unavailable",
            "summary": "Current liquidity and marketability evidence was not available."
        }),
    }
}

fn portfolio_context_value(evidence: &InvestmentAnalysisEvidence, portfolio_label: &str) -> Value {
    match evidence.portfolio_risk() {
        Some(value) => {
            let position_state = match value.position_state() {
                PortfolioPositionState::NoPosition => "no_position",
                PortfolioPositionState::Position { .. } => "current_position",
            };
            json!({
                "state": "available",
                "portfolioLabel": portfolio_label,
                "positionState": position_state,
                "riskCapacityPercent": percentage_from_ppm(value.risk_capacity_ppm()),
                "summary": "This is the exact saved portfolio position and remaining risk-capacity context. It is not a proposal-bound incremental impact calculation and does not change holdings or set aside risk."
            })
        }
        None => json!({
            "state": "unavailable",
            "summary": "Portfolio and risk context is unavailable because no qualifying selected-portfolio risk advisory was retained."
        }),
    }
}

fn virtual_paper_eligibility_value() -> Value {
    json!({
        "state": "not_eligible",
        "executionAuthority": "none",
        "requiresExplicitPaperApproval": true,
        "requiresFreshRiskCheck": true,
        "summary": "This saved analysis cannot create a simulated or real order. A separate virtual-paper workflow must recheck the investment, current market, size, liquidity, and risk limits before any simulated order."
    })
}

fn coverage_summary_value(evidence: &InvestmentAnalysisEvidence) -> Value {
    let broader_research = broader_research_availability(evidence);
    let items = [
        ("current_market", evidence.market().is_some()),
        ("broader_research", broader_research),
        ("price_pattern", evidence.harmonic_pattern().is_some()),
        ("forecast", evidence.price_forecast().is_some()),
        ("financial_model", evidence.financial_model().is_some()),
        ("valuation", evidence.valuation().is_some()),
        ("historical_test", evidence.backtest().is_some()),
        ("out_of_sample", evidence.out_of_sample().is_some()),
        ("liquidity", evidence.liquidity().is_some()),
        ("portfolio_risk", evidence.portfolio_risk().is_some()),
    ];
    let available_count = items.iter().filter(|(_, available)| *available).count();
    json!({
        "availableCount": available_count,
        "possibleCount": items.len(),
        "items": items.into_iter().map(|(kind, available)| json!({
            "kind": kind,
            "state": if available { "available" } else { "unavailable" },
        })).collect::<Vec<_>>(),
        "summary": format!("{available_count} of {} evidence areas were available to this saved analysis.", items.len()),
    })
}

fn calibration_summary_value(evidence: &InvestmentAnalysisEvidence) -> Value {
    match evidence.price_forecast() {
        Some(value) => {
            let calibration = value.calibration();
            json!({
                "state": "available",
                "nominalCoveragePercent": percentage_from_ppm(calibration.nominal_coverage_ppm()),
                "realizedCoveragePercent": percentage_from_ppm(calibration.realized_coverage_ppm()),
                "completedOutcomes": calibration.completed_outcomes().get(),
                "summary": "Coverage compares forecast ranges with completed historical outcomes; it does not measure certainty of profit."
            })
        }
        None => json!({
            "state": "unavailable",
            "summary": "Forecast calibration was not available for this saved analysis."
        }),
    }
}

fn out_of_sample_summary_value(evidence: &InvestmentAnalysisEvidence) -> Value {
    match evidence.out_of_sample() {
        Some(value) => json!({
            "state": "available",
            "completedObservations": value.completed_observations().get(),
            "totalSignals": value.total_signals().get(),
            "folds": value.fold_count().get(),
            "completionCoveragePercent": percentage_from_ppm(value.completion_coverage_ppm()),
            "evaluatedFrom": super::product_timestamp(value.evaluation_starts_at()),
            "evaluatedThrough": super::product_timestamp(value.evaluation_ends_at()),
            "summary": "These results use chronological independent historical windows aligned to the recommendation horizon; they do not guarantee future profit."
        }),
        None => json!({
            "state": "unavailable",
            "summary": "Independent historical evidence aligned to the investment horizon was not available, so no investment action can be produced."
        }),
    }
}

fn historical_test_summary_value(evidence: &CostAdjustedPitBacktestEvidence) -> Value {
    json!({
        "netReturnPercent": percentage_from_basis_points(evidence.net_return().get()),
        "maximumDrawdownPercent": percentage_from_basis_points(evidence.max_drawdown().get()),
        "observations": evidence.observations().get(),
        "trials": evidence.trials().get(),
        "stabilityPercent": percentage_from_ppm(evidence.stability_ppm()),
        "evaluatedThrough": super::product_timestamp(evidence.simulation_cutoff_at()),
        "summary": "This is a cost-adjusted point-in-time historical test, not a promise of future performance."
    })
}

fn cost_summary_value(evidence: Option<&CostAdjustedPitBacktestEvidence>) -> Value {
    match evidence {
        Some(value) => json!({
            "state": "modeled",
            "feePercent": percentage_from_basis_points(value.fee_basis_points().get()),
            "slippagePercent": percentage_from_basis_points(value.slippage_basis_points().get()),
            "maximumRandomSlippagePercent": percentage_from_basis_points(value.maximum_random_slippage_basis_points().get()),
            "summary": "These modeled costs were included in the historical test, not in the future price ranges."
        }),
        None => json!({
            "state": "unavailable",
            "summary": "A modeled trading-cost summary was not available for this saved analysis."
        }),
    }
}

fn uncertainty_summary_value(decision: &InvestmentProposalDecision) -> Value {
    let reliability = match decision {
        InvestmentProposalDecision::Generated(value) => Some(value.confidence()),
        InvestmentProposalDecision::NoAction(value) => Some(value.confidence()),
        InvestmentProposalDecision::Unavailable(_) => None,
    };
    match reliability {
        Some(value) => evidence_reliability_value(value),
        None => json!({
            "state": "unavailable",
            "summary": "Evidence reliability could not be calculated because the analysis was unavailable."
        }),
    }
}

fn investment_analysis_locator_value(
    entry: &InvestmentProposalIndexEntry,
    action_token: Uuid,
    investment: InvestmentDisplay,
    portfolio_label: String,
) -> Value {
    json!({
        "actionToken": action_token,
        "investment": investment_value(&investment),
        "portfolioLabel": portfolio_label,
        "currency": entry.currency().as_str(),
        "horizon": {
            "informationCurrentThrough": super::product_timestamp(entry.as_of()),
            "endsAt": super::product_timestamp(entry.horizon_at()),
            "expiresAt": super::product_timestamp(entry.expires_at()),
        },
        "recommendation": match entry.outcome() {
            InvestmentProposalIndexOutcome::Generated(action) => {
                json!({"kind": "action", "action": action_name(action), "summary": generated_action_summary(action)})
            }
            InvestmentProposalIndexOutcome::NoAction(reason) => {
                json!({"kind": "abstain", "summary": no_action_reason_summary(reason)})
            }
            InvestmentProposalIndexOutcome::Unavailable(reason) => {
                json!({"kind": "unavailable", "summary": unavailable_reason_summary(reason)})
            }
        },
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
        "amount": money.amount().normalize().to_string(),
        "currency": money.currency().as_str(),
    })
}

fn evidence_reliability_value(reliability: RecommendationConfidence) -> Value {
    json!({
        "state": "available",
        "evidenceReliabilityPercent": percentage_from_ppm(reliability.value_ppm()),
        "components": reliability.components().iter().map(|component| json!({
            "kind": confidence_component_name(component.kind()),
            "reliabilityPercent": percentage_from_ppm(component.value_ppm()),
        })).collect::<Vec<_>>(),
        "summary": confidence_summary(reliability.meaning()),
    })
}

const fn unavailable_reason_summary(reason: ProposalUnavailableReason) -> &'static str {
    match reason {
        ProposalUnavailableReason::MissingEvidence(_) => {
            "Required supporting information was missing."
        }
        ProposalUnavailableReason::InstrumentMismatch { .. } => {
            "Supporting information did not refer to the same investment."
        }
        ProposalUnavailableReason::CurrencyMismatch { .. } => {
            "Supporting information used incompatible currencies."
        }
        ProposalUnavailableReason::AccountMismatch { .. } => {
            "Portfolio information did not refer to the selected account."
        }
        ProposalUnavailableReason::NotAvailableAtCutoff(_) => {
            "Required information was not available by the analysis cutoff."
        }
        ProposalUnavailableReason::ExpiredEvidence(_) => {
            "Required supporting information had expired."
        }
        ProposalUnavailableReason::StaleEvidence(_) => {
            "Required supporting information was too old."
        }
        ProposalUnavailableReason::RejectedQuality { .. } => {
            "Required supporting information did not meet the quality standard."
        }
        ProposalUnavailableReason::ForecastHorizonMismatch { .. }
        | ProposalUnavailableReason::ValuationHorizonMismatch { .. }
        | ProposalUnavailableReason::FinancialModelHorizonMismatch { .. }
        | ProposalUnavailableReason::BacktestHorizonMismatch { .. }
        | ProposalUnavailableReason::OutOfSampleHorizonMismatch { .. } => {
            "Supporting information did not use the same investment horizon."
        }
        ProposalUnavailableReason::FinancialModelValuationMismatch => {
            "The financial model and governed valuation did not describe the same saved value."
        }
        ProposalUnavailableReason::OutOfSampleBacktestMismatch => {
            "The independent evaluation did not match the saved historical study."
        }
        ProposalUnavailableReason::InsufficientForecastOutcomes { .. } => {
            "Too few completed forecast outcomes were available."
        }
        ProposalUnavailableReason::UnsupportedForecastCoverage { .. } => {
            "Forecast coverage was outside the accepted range."
        }
        ProposalUnavailableReason::InsufficientBacktestObservations { .. } => {
            "Too few historical observations were available."
        }
        ProposalUnavailableReason::InsufficientBacktestTrials { .. } => {
            "Too few historical trials were available."
        }
        ProposalUnavailableReason::ReservedPortfolioRevision => {
            "Portfolio information was not ready for analysis."
        }
    }
}

fn action_token(value: &str) -> Result<Uuid, ServiceError> {
    let token = Uuid::parse_str(value).map_err(|_| ServiceError::InvalidRequest)?;
    if token.is_nil() || token.to_string() != value {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(token)
}

fn portfolio_label(
    catalog: &PortfolioAccountCatalogSnapshot,
    account_id: AccountId,
) -> Result<String, ServiceError> {
    let index = catalog
        .heads()
        .iter()
        .position(|head| head.account_id() == account_id)
        .ok_or(ServiceError::Unavailable)?;
    let ordinal = index
        .checked_add(1)
        .ok_or(ServiceError::ResourceExhausted)?;
    Ok(format!("Portfolio {ordinal}"))
}

fn map_instrument_catalog(error: MarketDataInstrumentCatalogError) -> ServiceError {
    match error {
        MarketDataInstrumentCatalogError::Cancelled => ServiceError::Cancelled,
        MarketDataInstrumentCatalogError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        MarketDataInstrumentCatalogError::ResultByteLimitExceeded => {
            ServiceError::ResourceExhausted
        }
        MarketDataInstrumentCatalogError::InvalidInput
        | MarketDataInstrumentCatalogError::InvalidPopulationQuery
        | MarketDataInstrumentCatalogError::InvalidLimit => ServiceError::InvalidRequest,
        _ => ServiceError::Unavailable,
    }
}

fn map_account_catalog(error: PortfolioAccountCatalogError) -> ServiceError {
    match error {
        PortfolioAccountCatalogError::Portfolio(error) => error.as_service_error(),
        PortfolioAccountCatalogError::ResourceExhausted => ServiceError::ResourceExhausted,
        PortfolioAccountCatalogError::CorruptPublication
        | PortfolioAccountCatalogError::CatalogChanged => ServiceError::Unavailable,
    }
}

fn percentage_from_ppm(value: u32) -> String {
    exact_percentage(Decimal::from(value), Decimal::from(1_000_000_u32))
}

fn percentage_from_basis_points(value: i32) -> String {
    exact_percentage(Decimal::from(value), Decimal::from(10_000_u32))
}

fn percentage_from_decimal_ratio(value: Decimal) -> Result<String, ServiceError> {
    let hundred = Decimal::from(100_u32);
    let percentage = value
        .checked_mul(hundred)
        .ok_or(ServiceError::InvalidResult)?;
    if exact_decimal_ratio(percentage, hundred) != Some(value) {
        return Err(ServiceError::InvalidResult);
    }
    Ok(percentage.normalize().to_string())
}

fn exact_money_ratio_percentage(numerator: Money, denominator: Money) -> Option<String> {
    if numerator.currency() != denominator.currency() || denominator.amount().is_zero() {
        return None;
    }
    let ratio = exact_decimal_ratio(numerator.amount(), denominator.amount())?;
    let percentage = ratio.checked_mul(Decimal::from(100_u32))?;
    if exact_decimal_ratio(percentage, Decimal::from(100_u32))? != ratio {
        return None;
    }
    Some(percentage.normalize().to_string())
}

fn exact_decimal_ratio(numerator: Decimal, denominator: Decimal) -> Option<Decimal> {
    if denominator.is_zero() {
        return None;
    }
    let ten = Decimal::from(10_u32);
    let mut scaled_numerator = numerator;
    let mut decimal_scale = Decimal::from(1_u32);
    for scale in 0..=28 {
        if scaled_numerator.checked_rem(denominator)?.is_zero() {
            let integral_quotient = scaled_numerator.checked_div(denominator)?;
            let quotient = integral_quotient.checked_div(decimal_scale)?;
            if quotient.checked_mul(denominator)? == numerator {
                return Some(quotient);
            }
        }
        if scale == 28 {
            break;
        }
        scaled_numerator = scaled_numerator.checked_mul(ten)?;
        decimal_scale = decimal_scale.checked_mul(ten)?;
    }
    None
}

fn exact_percentage(numerator: Decimal, denominator: Decimal) -> String {
    ((numerator / denominator) * Decimal::from(100_u32))
        .normalize()
        .to_string()
}

fn money_text(value: Money) -> String {
    format!("{} {}", value.amount().normalize(), value.currency())
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

const fn generated_action_summary(action: RecommendationAction) -> &'static str {
    match action {
        RecommendationAction::Buy => {
            "The saved evidence supports starting a position within the entry range."
        }
        RecommendationAction::Add => "The saved evidence supports adding within the add range.",
        RecommendationAction::Hold => {
            "The saved evidence supports holding rather than changing the position."
        }
        RecommendationAction::Trim => {
            "The saved evidence supports reducing the position within the trim range."
        }
        RecommendationAction::Sell => {
            "The saved evidence supports exiting the position within the exit range."
        }
    }
}

const fn recommendation_outcome_cohort_name(cohort: RecommendationOutcomeCohort) -> &'static str {
    match cohort {
        RecommendationOutcomeCohort::Generated(RecommendationAction::Buy) => "buy",
        RecommendationOutcomeCohort::Generated(RecommendationAction::Add) => "add",
        RecommendationOutcomeCohort::Generated(RecommendationAction::Hold) => "hold",
        RecommendationOutcomeCohort::Generated(RecommendationAction::Trim) => "trim",
        RecommendationOutcomeCohort::Generated(RecommendationAction::Sell) => "sell",
        RecommendationOutcomeCohort::NoActionControl => "abstain",
        RecommendationOutcomeCohort::AnalysisUnavailable => "unavailable",
    }
}

const fn recommendation_outcome_unavailable_reason_summary(
    reason: RecommendationOutcomeUnavailableReason,
) -> &'static str {
    match reason {
        RecommendationOutcomeUnavailableReason::AnalysisUnavailable(_) => {
            "The original analysis was unavailable, so no comparable outcome can be measured."
        }
        RecommendationOutcomeUnavailableReason::OutcomeObservationUnavailable => {
            "A comparable price was not available at the end of the horizon."
        }
        RecommendationOutcomeUnavailableReason::AmbiguousOutcomeObservation => {
            "More than one possible end-of-horizon price remained unresolved."
        }
        RecommendationOutcomeUnavailableReason::IncompleteOutcomeObservation => {
            "The end-of-horizon price information was incomplete."
        }
        RecommendationOutcomeUnavailableReason::CorporateActionEvidenceUnavailable => {
            "Corporate-action information was insufficient for a comparable outcome."
        }
    }
}

const fn sizing_unavailable_reason_name(reason: SizingUnavailableReason) -> &'static str {
    match reason {
        SizingUnavailableReason::CapacityNotSupplied(_) => {
            "A required sizing limit was not supplied."
        }
        SizingUnavailableReason::CapacityNotYetAvailable(_) => {
            "A required sizing limit was not available yet."
        }
        SizingUnavailableReason::CapacityExpired(_) => "A required sizing limit had expired.",
        SizingUnavailableReason::CapacityRangeContainsNoLots(_) => {
            "A sizing limit did not permit a whole lot."
        }
        SizingUnavailableReason::CashReserveExceedsGrossLiquidatableValue => {
            "The required cash reserve exceeded available liquid value."
        }
        SizingUnavailableReason::NoHardFeasibleLotIntersection => {
            "The mandatory sizing limits did not overlap."
        }
        SizingUnavailableReason::PreferredWeightRangeContainsNoLots => {
            "The preferred range did not permit a whole lot."
        }
        SizingUnavailableReason::NoPreferredFeasibleLotIntersection => {
            "The preferred sizing limits did not overlap."
        }
    }
}

const fn no_action_reason_summary(reason: NoActionReason) -> &'static str {
    match reason {
        NoActionReason::ConflictingForecastAndValuation => {
            "The forecast and valuation evidence point in conflicting directions."
        }
        NoActionReason::BacktestBelowPolicy => {
            "The historical test did not meet the required standard."
        }
        NoActionReason::OutOfSampleBelowPolicy => {
            "The independent historical evaluation did not meet the required coverage standard."
        }
        NoActionReason::LiquidityBelowPolicy => {
            "Available liquidity did not meet the required standard."
        }
        NoActionReason::PortfolioRiskBelowPolicy => {
            "The portfolio risk assessment did not permit an action."
        }
        NoActionReason::ConfidenceBelowPolicy => {
            "Supporting-evidence reliability was below the required standard."
        }
        NoActionReason::PositionStateNotActionable => {
            "The current position state did not permit an action."
        }
        NoActionReason::GeneratedPriceOrderCollapsed => {
            "The calculated action ranges were not sufficiently distinct."
        }
    }
}

const fn invalidator_summary(invalidator: ProposalInvalidator) -> &'static str {
    match invalidator {
        ProposalInvalidator::ForecastValuationConflict => {
            "Forecast and valuation evidence no longer agree."
        }
        ProposalInvalidator::BacktestPolicyBreach => {
            "The historical result falls below the required standard."
        }
        ProposalInvalidator::OutOfSamplePolicyBreach => {
            "Independent historical coverage falls below the required standard."
        }
        ProposalInvalidator::LiquidityPolicyBreach => {
            "Liquidity falls below the required standard."
        }
        ProposalInvalidator::PortfolioRiskPolicyBreach => {
            "Portfolio risk no longer permits the action."
        }
        ProposalInvalidator::ConfidencePolicyBreach => {
            "Supporting-evidence reliability falls below the required standard."
        }
        ProposalInvalidator::PositionStateIncompatible => {
            "The current position state no longer permits the action."
        }
        ProposalInvalidator::GeneratedPriceOrderCollapsed => {
            "The action ranges are no longer sufficiently distinct."
        }
    }
}

const fn confidence_summary(meaning: RecommendationConfidenceMeaning) -> &'static str {
    match meaning {
        RecommendationConfidenceMeaning::PolicyWeightedEvidenceReliabilityV1 => {
            "This score summarizes supporting-evidence reliability. It is not the probability of profit."
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
