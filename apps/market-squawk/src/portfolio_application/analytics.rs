//! Revision-bound Task 12 performance, exposure, risk, and scenario results.

use std::num::NonZeroU32;

use market_squawk_adapter_portfolio::{
    ReconciliationField, ReconciliationTolerance, TransactionKind,
};
use market_squawk_analytics::{
    ExactDecimalScale, ExactRate, MonetaryBasis, MonetaryValue, PortfolioAllocation,
    PortfolioExposure, Quantile, ScenarioShock, ShockComposition, StatisticalInput,
    StatisticalScale, StatisticalUnit, discrete_expected_shortfall, historical_var,
    scenario_impact,
};
use market_squawk_domain::{Money, SourceIdentifier};
use market_squawk_portfolio::{
    AnalyticsPolicyBinding, CashFlowTiming, MoneyWeightedMethod, PerformancePeriod,
    PerformancePolicy, PerformanceReport, PortfolioAnalyticsEvidence, PortfolioLimitInput,
    PortfolioLimits,
};
use market_squawk_services::{RequestContext, TypedToolResult};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde_json::{Map, Number, Value, json};

use super::PortfolioApplicationServiceError;
use super::import::hex;
use super::model::{BasisResolution, PortfolioReadImage, PublishedRevision};
use super::read::{ReadScope, check_context, product_report_result, report_result};

/// Three 5% tail observations are the minimum retained evidence for the historical tail measures.
const MINIMUM_HISTORICAL_RISK_RETURNS: usize = 60;

pub(super) fn performance(
    image: &PortfolioReadImage,
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let history = admitted_history(image, revision, scope)?;
    let mut output = base_report(revision, "modified_dietz_v1");
    output.insert(
        "accountingEvidence".to_owned(),
        accounting_evidence(revision)?,
    );
    output.insert(
        "currentValue".to_owned(),
        money_value(total_value(revision, scope)?),
    );
    if history.len() < 2 {
        output.insert(
            "historyStatus".to_owned(),
            Value::String("insufficient_history".to_owned()),
        );
        return report_result(Value::Object(output), revision, scope, context);
    }
    let periods = performance_periods(&history, scope)?;
    if periods.is_empty() {
        output.insert(
            "historyStatus".to_owned(),
            Value::String("insufficient_comparable_history".to_owned()),
        );
        return report_result(Value::Object(output), revision, scope, context);
    }
    let evidence = analytics_evidence(revision)?;
    let policy = PerformancePolicy::new(
        CashFlowTiming::EndOfPeriod,
        MoneyWeightedMethod::ModifiedDietz,
        NonZeroU32::MIN,
    );
    let report = PerformanceReport::try_calculate(
        &revision.core,
        &evidence,
        &periods,
        policy,
        analytics_limits(revision, scope, history.len())?,
    )
    .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    output.insert(
        "timeWeightedReturn".to_owned(),
        Value::String(report.time_weighted_return().value().to_string()),
    );
    output.insert(
        "moneyWeightedReturn".to_owned(),
        Value::String(report.money_weighted_return().value().to_string()),
    );
    output.insert("periods".to_owned(), Value::from(report.periods()));
    output.insert(
        "analyticsEvidenceDigest".to_owned(),
        Value::String(hex(&report.analytics_evidence_digest().bytes())),
    );
    report_result(Value::Object(output), revision, scope, context)
}

/// Whole-snapshot totals; the caller budgets this summary together with its holdings page.
pub(super) fn exposure_summary(
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<Value, PortfolioApplicationServiceError> {
    check_context(context)?;
    let cash = revision
        .account
        .cash_balance()
        .checked_add(scoped_receivables(revision, scope, Some(context))?)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let mut exposure: Option<PortfolioExposure> = None;
    let mut position_count = 0_usize;
    for holding in &revision.holdings {
        check_context(context)?;
        if !scope.admits_instrument(holding.instrument_id()) {
            continue;
        }
        if holding.account_id() != scope.account_id {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        let value = MonetaryValue::new(holding.market_value(), MonetaryBasis::Total);
        exposure = Some(match exposure {
            Some(total) => total
                .checked_add(value)
                .map_err(|_| PortfolioApplicationServiceError::Analytics)?,
            None => PortfolioExposure::from_value(value),
        });
        position_count += 1;
    }
    let net = exposure.map(|report| report.net().money());
    let classifications = net.map_or_else(Vec::new, |amount| {
        vec![json!({"classification": "unclassified", "amount": money_value(amount)})]
    });
    let output = json!({
        "net": net.map(money_value),
        "gross": exposure.map(|report| money_value(report.gross().money())),
        "positionCount": position_count,
        "currency": currency_exposure(cash, net)?,
        "sector": classifications,
        "factor": classifications,
        "calculationStatus": if exposure.is_some() { "available" } else { "no_positions" },
        "classificationStatus": "not_supplied_by_portfolio_source",
    });
    check_context(context)?;
    Ok(output)
}

pub(super) fn risk(
    image: &PortfolioReadImage,
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let history = admitted_history(image, revision, scope)?;
    let admitted_comparisons = history.len().saturating_sub(1);
    let periods = performance_periods(&history, scope)?;
    let rejected_comparisons = admitted_comparisons.saturating_sub(periods.len());
    let returns = historical_returns(&periods)?;
    let available_at = revision
        .available_at
        .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
    let sufficient_tail_history = returns.len() >= MINIMUM_HISTORICAL_RISK_RETURNS;
    let (value_at_risk, expected_shortfall) = if !sufficient_tail_history {
        (None, None)
    } else {
        let losses = returns
            .iter()
            .map(|value| {
                StatisticalInput::try_new(
                    (-value).max(0.0),
                    StatisticalUnit::Return,
                    StatisticalScale::Unit,
                )
                .map_err(|_| PortfolioApplicationServiceError::Analytics)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let confidence =
            Quantile::try_new(0.95).map_err(|_| PortfolioApplicationServiceError::Analytics)?;
        (
            Some(
                historical_var(&losses, confidence)
                    .map_err(|_| PortfolioApplicationServiceError::Analytics)?
                    .value(),
            ),
            Some(
                discrete_expected_shortfall(&losses, confidence)
                    .map_err(|_| PortfolioApplicationServiceError::Analytics)?
                    .value(),
            ),
        )
    };
    // Portfolio revisions arrive on no admitted fixed cadence. Annualizing each update as one
    // trading day would invent a period frequency, so the ordinary product reports unavailable.
    let annualized_volatility = None;
    let measure = |label: &'static str,
                   value: Option<f64>,
                   status: &'static str,
                   explanation: &'static str|
     -> Result<Value, PortfolioApplicationServiceError> {
        Ok(json!({
            "label": label,
            "value": value.map(super::product::percentage).transpose()?,
            "status": if value.is_some() { "available" } else { status },
            "explanation": explanation,
        }))
    };
    let coverage_state = if returns.is_empty() {
        "unavailable"
    } else if !revision.discrepancies.is_empty() || rejected_comparisons > 0 {
        "partial"
    } else {
        "complete"
    };
    let coverage_explanation = match coverage_state {
        "complete" => format!(
            "All {admitted_comparisons} admitted portfolio comparisons in this period were used."
        ),
        "partial" => format!(
            "Used {} of {admitted_comparisons} admitted portfolio comparisons; {rejected_comparisons} could not be compared and {} current data issues need review.",
            periods.len(),
            revision.discrepancies.len(),
        ),
        _ => "At least two comparable portfolio observations are required.".to_owned(),
    };
    let period_start = periods
        .first()
        .map_or(revision.effective_at, |period| period.starts_at());
    let stress = standard_stress(revision, scope)?;
    let stress_impact = stress.get("impact").cloned().unwrap_or(Value::Null);
    let stress_available = !stress_impact.is_null();
    let output = json!({
        "accountName": super::product::account_display_name(image, revision.account.account_id())?,
        "asOf": super::product::timestamp(revision.effective_at),
        "availableAt": super::product::timestamp(available_at),
        "horizon": "One portfolio update",
        "coverage": {
            "state": coverage_state,
            "observations": returns.len(),
            "period": format!("{} through {}", super::product::timestamp(period_start), super::product::timestamp(revision.effective_at)),
            "explanation": coverage_explanation,
        },
        "measures": [
            measure("Value at risk", value_at_risk, "insufficient_history", "A positive percentage is the estimated one-update loss threshold at 95% confidence; at least 60 comparable returns are required.")?,
            measure("Expected shortfall", expected_shortfall, "insufficient_history", "A positive percentage is the average loss beyond the 95% threshold; at least 60 comparable returns are required.")?,
            measure("Annualized volatility", annualized_volatility, if returns.is_empty() { "insufficient_history" } else { "unavailable" }, "Annualized volatility is unavailable because portfolio updates do not have an admitted fixed schedule.")?,
        ],
        "stress": {
            "label": "Broad market decline of 10%",
            "impact": stress_impact,
            "status": if stress_available { "available" } else if revision.holdings.is_empty() { "incomplete" } else { "unavailable" },
            "explanation": if stress_available { "Negative money is an estimated portfolio loss under the stated shock." } else { "The current holdings do not support this stress estimate." },
            "assumptions": ["Every included holding falls 10% at the same time.", "Cash is unchanged and no trades, taxes, or fees are applied."],
        },
        "recommendation": {
            "action": "abstain",
            "horizon": "Until recommendation evidence is available",
            "summary": "No portfolio action is recommended from risk statistics alone.",
            "ranges": [],
            "reasons": ["Risk measures describe possible loss and variability; they do not establish whether an investment should be bought or sold."],
            "risks": ["Acting on risk measures without valuation, forecast, and recommendation evidence could produce an unsuitable trade."],
            "assumptions": ["This guidance uses only the retained portfolio history and the stated stress assumptions."],
            "invalidators": ["A new evidence-backed portfolio recommendation replaces this abstention."],
            "validity": {
                "state": "unavailable",
                "explanation": "No evidence-backed portfolio recommendation or review time is available.",
            },
            "uncertainty": {
                "level": "unavailable",
                "explanation": "The forecast, valuation, and recommendation evidence needed for portfolio guidance is unavailable.",
                "outOfSampleEvidence": "unavailable",
                "calibration": "unavailable",
                "tradingCosts": "unavailable",
                "pointInTimeInputs": if revision.discrepancies.is_empty() { "supported" } else { "partial" },
            },
        },
    });
    product_report_result(output, revision, scope, context)
}

fn admitted_history<'a>(
    image: &'a PortfolioReadImage,
    selected: &PublishedRevision,
    scope: &ReadScope,
) -> Result<Vec<&'a PublishedRevision>, PortfolioApplicationServiceError> {
    let history = image
        .accounts
        .get(&scope.account_id)
        .ok_or(PortfolioApplicationServiceError::NotFound)?;
    let selected_token = selected.token();
    let selected_index = history
        .revisions
        .iter()
        .position(|revision| revision.token() == selected_token)
        .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
    let mut admitted = history.revisions[..=selected_index]
        .iter()
        .filter(|revision| {
            revision
                .available_at
                .is_some_and(|available| scope.end.is_none_or(|end| available <= end))
        })
        .collect::<Vec<_>>();
    if let Some(start) = scope.start {
        let Some(first_in_range) = admitted
            .iter()
            .position(|revision| revision.effective_at >= start)
        else {
            return Ok(Vec::new());
        };
        // An in-range closing observation needs its preceding opening value. Without a
        // closing observation in the requested period, old history cannot establish a return.
        admitted.drain(..first_in_range.saturating_sub(1));
    }
    Ok(admitted)
}

fn performance_periods(
    history: &[&PublishedRevision],
    scope: &ReadScope,
) -> Result<Vec<PerformancePeriod>, PortfolioApplicationServiceError> {
    let mut periods = Vec::new();
    for pair in history.windows(2) {
        let opening = pair[0];
        let closing = pair[1];
        if opening.effective_at >= closing.effective_at {
            continue;
        }
        let opening_value = total_value(opening, scope)?;
        let closing_value = total_value(closing, scope)?;
        if opening_value.amount() <= Decimal::ZERO
            || opening_value.currency() != closing_value.currency()
        {
            continue;
        }
        let external_flow = closing
            .transactions
            .iter()
            .filter(|transaction| {
                transaction.kind() == TransactionKind::CashTransfer
                    && transaction.occurred_at() > opening.effective_at
                    && transaction.occurred_at() <= closing.effective_at
            })
            .try_fold(
                Money::new(Decimal::ZERO, opening_value.currency()),
                |total, transaction| {
                    total
                        .checked_add(transaction.amount())
                        .map_err(|_| PortfolioApplicationServiceError::Analytics)
                },
            )?;
        periods.push(
            PerformancePeriod::try_new(
                opening.effective_at,
                closing.effective_at,
                opening_value,
                closing_value,
                external_flow,
            )
            .map_err(|_| PortfolioApplicationServiceError::Analytics)?,
        );
    }
    Ok(periods)
}

fn historical_returns(
    periods: &[PerformancePeriod],
) -> Result<Vec<f64>, PortfolioApplicationServiceError> {
    periods
        .iter()
        .map(|period| {
            period
                .closing_value()
                .amount()
                .checked_sub(period.external_flow().amount())
                .and_then(|flow_adjusted| {
                    flow_adjusted.checked_sub(period.opening_value().amount())
                })
                .and_then(|change| change.checked_div(period.opening_value().amount()))
                .and_then(|value| value.to_f64())
                .ok_or(PortfolioApplicationServiceError::Analytics)
        })
        .collect()
}

fn total_value(
    revision: &PublishedRevision,
    scope: &ReadScope,
) -> Result<Money, PortfolioApplicationServiceError> {
    revision
        .holdings
        .iter()
        .filter(|holding| scope.admits_instrument(holding.instrument_id()))
        .try_fold(
            revision
                .account
                .cash_balance()
                .checked_add(scoped_receivables(revision, scope, None)?)
                .map_err(|_| PortfolioApplicationServiceError::Analytics)?,
            |total, holding| {
                total
                    .checked_add(holding.market_value())
                    .map_err(|_| PortfolioApplicationServiceError::Analytics)
            },
        )
}

fn allocations(
    revision: &PublishedRevision,
    scope: &ReadScope,
) -> Result<Vec<PortfolioAllocation>, PortfolioApplicationServiceError> {
    revision
        .holdings
        .iter()
        .filter(|holding| scope.admits_instrument(holding.instrument_id()))
        .map(|holding| {
            PortfolioAllocation::try_new(
                &format!("instrument-{}", holding.instrument_id()),
                MonetaryValue::new(holding.market_value(), MonetaryBasis::Total),
                ExactRate::try_new(Decimal::ZERO, ExactDecimalScale::Unit)
                    .map_err(|_| PortfolioApplicationServiceError::Analytics)?,
            )
            .map_err(|_| PortfolioApplicationServiceError::Analytics)
        })
        .collect()
}

fn currency_exposure(
    cash: Money,
    positions: Option<Money>,
) -> Result<Vec<Value>, PortfolioApplicationServiceError> {
    // Position exposure requires one currency. Cash may carry a different currency; preserve
    // the separate amount rather than inventing an exchange rate or retaining per-position rows.
    let mut totals = vec![cash];
    if let Some(positions) = positions {
        if positions.currency() == cash.currency() {
            totals[0] = cash
                .checked_add(positions)
                .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
        } else {
            totals.push(positions);
            totals.sort_unstable_by_key(|amount| amount.currency());
        }
    }
    Ok(totals
        .into_iter()
        .map(
            |amount| json!({"currency": amount.currency().as_str(), "amount": money_value(amount)}),
        )
        .collect())
}

fn standard_stress(
    revision: &PublishedRevision,
    scope: &ReadScope,
) -> Result<Value, PortfolioApplicationServiceError> {
    let allocations = allocations(revision, scope)?;
    if allocations.is_empty() {
        return Ok(json!({
            "id": "parallel_market_minus_10_percent",
            "status": "no_positions"
        }));
    }
    let shock = ExactRate::try_new(Decimal::new(-1, 1), ExactDecimalScale::Unit)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let shocks = allocations
        .iter()
        .map(|allocation| {
            ScenarioShock::try_new(allocation.dimension(), shock)
                .map_err(|_| PortfolioApplicationServiceError::Analytics)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let impact = scenario_impact(&allocations, &shocks, ShockComposition::Additive)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    Ok(json!({
        "id": "parallel_market_minus_10_percent",
        "impact": money_value(impact.total().money())
    }))
}

fn analytics_evidence(
    revision: &PublishedRevision,
) -> Result<PortfolioAnalyticsEvidence, PortfolioApplicationServiceError> {
    let policy = |name: &str| {
        AnalyticsPolicyBinding::try_new(
            SourceIdentifier::try_from(name)
                .map_err(|_| PortfolioApplicationServiceError::Analytics)?,
            NonZeroU32::MIN,
        )
        .map_err(|_| PortfolioApplicationServiceError::Analytics)
    };
    PortfolioAnalyticsEvidence::try_from_revision(
        &revision.core,
        revision.effective_at,
        revision.available_at.unwrap_or(revision.effective_at),
        policy("portfolio-valuation-policy")?,
        policy("portfolio-fx-policy")?,
        policy("portfolio-as-of-policy")?,
    )
    .map_err(|_| PortfolioApplicationServiceError::Analytics)
}

fn analytics_limits(
    revision: &PublishedRevision,
    scope: &ReadScope,
    history: usize,
) -> Result<PortfolioLimits, PortfolioApplicationServiceError> {
    let instruments = revision.holdings.len().max(1);
    PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: 1,
        max_instruments: instruments,
        max_lots: instruments,
        max_transactions: revision.transactions.len().max(1),
        max_factors: instruments,
        max_scenarios: instruments,
        max_history: history.max(1),
        max_results: scope.maximum_items.max(1),
        max_retained_bytes: scope
            .maximum_bytes
            .max(std::mem::size_of::<PerformanceReport>()),
    })
    .map_err(|_| PortfolioApplicationServiceError::Analytics)
}

fn base_report(revision: &PublishedRevision, policy: &str) -> Map<String, Value> {
    let mut output = Map::new();
    output.insert(
        "accountId".to_owned(),
        Value::String(revision.account.account_id().to_string()),
    );
    output.insert(
        "revisionId".to_owned(),
        Value::String(hex(&revision.token().bytes())),
    );
    output.insert("policy".to_owned(), Value::String(policy.to_owned()));
    output.insert(
        "effectiveAtUnixNanos".to_owned(),
        Value::String(revision.effective_at.unix_nanos().to_string()),
    );
    output.insert(
        "availableAtUnixNanos".to_owned(),
        revision.available_at.map_or(Value::Null, |timestamp| {
            Value::String(timestamp.unix_nanos().to_string())
        }),
    );
    output
}

/// Returns only source-backed accounting aggregates available to the installed portfolio reader.
///
/// Raw holdings are snapshot evidence, while normalized trade and income records still require
/// the explicit lifecycle/subtype interpretation authority before they can become a realized-gain
/// ledger. This result retains exact source totals and explains that boundary instead of using the
/// previous synthetic cash-only replay as accounting evidence.
fn accounting_evidence(
    revision: &PublishedRevision,
) -> Result<Value, PortfolioApplicationServiceError> {
    let currency = revision.account.currency();
    let reported_market_value = revision.holdings.iter().try_fold(
        Money::new(Decimal::ZERO, currency),
        |total, holding| {
            total
                .checked_add(holding.market_value())
                .map_err(|_| PortfolioApplicationServiceError::Analytics)
        },
    )?;
    let resolved_unrealized = revision.holdings.iter().try_fold(
        Some(Money::new(Decimal::ZERO, currency)),
        |total, holding| match (total, holding.basis()) {
            (Some(total), BasisResolution::Resolved { observation }) => holding
                .market_value()
                .checked_sub(observation.amount())
                .and_then(|gain| total.checked_add(gain))
                .map(Some)
                .map_err(|_| PortfolioApplicationServiceError::Analytics),
            _ => Ok(None),
        },
    )?;
    let source_income = source_transaction_total(revision, TransactionKind::Income)?;
    let source_fees = source_transaction_total(revision, TransactionKind::Fee)?;
    let reconciliation = revision
        .discrepancies
        .iter()
        .map(|discrepancy| {
            let ReconciliationTolerance::Absolute { amount } = discrepancy.tolerance_policy();
            json!({
                "field": reconciliation_field(discrepancy.field()),
                "supplied": money_value(discrepancy.supplied()),
                "calculated": money_value(discrepancy.calculated()),
                "currency": discrepancy.currency().as_str(),
                "tolerance": {
                    "kind": "absolute",
                    "amount": money_value(amount)
                },
                "sourceReference": discrepancy.source_reference().as_str()
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "cash": {
            "amount": money_value(revision.account.cash_balance()),
            "observedAtUnixNanos": revision.account.as_of().unix_nanos().to_string(),
            "sourceReference": revision.account.source_reference().as_str(),
            "status": "source_reported_snapshot"
        },
        "reportedMarketValue": money_value(reported_market_value),
        "unrealizedGain": resolved_unrealized.map_or_else(
            || json!({
                "status": "not_calculable_incomplete_source_basis",
                "reason": "one_or_more_holdings_has_missing_or_ambiguous_basis"
            }),
            |amount| json!({
                "status": "calculated_from_source_reported_mark_and_resolved_basis",
                "amount": money_value(amount)
            })
        ),
        "realizedGain": {
            "status": "requires_committed_trade_lifecycle_interpretation",
            "reason": "signed_trade_quantity_does_not_distinguish_sell_from_short_or_buy_from_cover"
        },
        "income": {
            "status": "source_classified_pending_explicit_subtype",
            "amount": money_value(source_income),
            "reason": "generic_income_does_not_distinguish_dividend_interest_or_withholding"
        },
        "fees": {
            "status": "source_classified",
            "amount": money_value(source_fees)
        },
        "reconciliation": {
            "status": if revision.discrepancies.is_empty() { "no_retained_discrepancies" } else { "discrepancies_require_review" },
            "discrepancies": reconciliation
        }
    }))
}

fn source_transaction_total(
    revision: &PublishedRevision,
    kind: TransactionKind,
) -> Result<Money, PortfolioApplicationServiceError> {
    revision
        .transactions
        .iter()
        .filter(|transaction| transaction.kind() == kind)
        .try_fold(
            Money::new(Decimal::ZERO, revision.account.currency()),
            |total, transaction| {
                total
                    .checked_add(transaction.amount())
                    .map_err(|_| PortfolioApplicationServiceError::Analytics)
            },
        )
}

const fn reconciliation_field(field: ReconciliationField) -> &'static str {
    match field {
        ReconciliationField::Cash => "cash",
        ReconciliationField::MarketValue => "market_value",
        ReconciliationField::CostBasis => "cost_basis",
    }
}

fn money_value(value: Money) -> Value {
    json!({
        "amount": value.amount().to_string(),
        "currency": value.currency().as_str()
    })
}

fn number(value: f64) -> Result<Value, PortfolioApplicationServiceError> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or(PortfolioApplicationServiceError::Analytics)
}

fn scoped_receivables(
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: Option<&RequestContext>,
) -> Result<Money, PortfolioApplicationServiceError> {
    let mut total = Money::new(Decimal::ZERO, revision.core.base_currency());
    for claim in revision.core.cash_entitlements() {
        if let Some(context) = context {
            check_context(context)?;
        }
        if !claim.settled() && scope.admits_instrument(claim.instrument()) {
            total = total
                .checked_add(claim.amount())
                .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
        }
    }
    Ok(total)
}
