//! Revision-bound Task 12 performance, exposure, risk, and scenario results.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use market_squawk_adapter_portfolio::TransactionKind;
use market_squawk_analytics::{
    Annualization, ExactDecimalScale, ExactRate, MissingValuePolicy, MonetaryBasis, MonetaryValue,
    PortfolioAllocation, Quantile, ReturnSeries, ScenarioShock, ShockComposition, StatisticalInput,
    StatisticalScale, StatisticalUnit, VarianceConvention, discrete_expected_shortfall,
    historical_var, portfolio_exposure, scenario_impact, volatility,
};
use market_squawk_domain::{Currency, Money, SourceIdentifier};
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
use super::model::{PortfolioReadImage, PublishedRevision};
use super::read::{ReadScope, report_result};

pub(super) fn performance(
    image: &PortfolioReadImage,
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let history = admitted_history(image, revision, scope)?;
    let mut output = base_report(revision, "modified_dietz_v1");
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

pub(super) fn exposure(
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let allocations = allocations(revision, scope)?;
    let mut output = base_report(revision, "task12_exact_exposure_v1");
    let instrument = revision
        .holdings
        .iter()
        .filter(|holding| scope.admits_instrument(holding.instrument_id()))
        .map(|holding| {
            json!({
                "instrumentId": holding.instrument_id().to_string(),
                "amount": money_value(holding.market_value())
            })
        })
        .collect::<Vec<_>>();
    output.insert("instrument".to_owned(), Value::Array(instrument));
    output.insert(
        "currency".to_owned(),
        Value::Array(currency_exposure(revision, scope)?),
    );
    if allocations.is_empty() {
        output.insert(
            "calculationStatus".to_owned(),
            Value::String("no_positions".to_owned()),
        );
        output.insert("sector".to_owned(), Value::Array(Vec::new()));
        output.insert("factor".to_owned(), Value::Array(Vec::new()));
        return report_result(Value::Object(output), revision, scope, context);
    }
    let report = portfolio_exposure(&allocations)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    output.insert("net".to_owned(), money_value(report.net().money()));
    output.insert("gross".to_owned(), money_value(report.gross().money()));
    let unclassified = money_value(report.net().money());
    output.insert(
        "sector".to_owned(),
        json!([{"classification": "unclassified", "amount": unclassified.clone()}]),
    );
    output.insert(
        "factor".to_owned(),
        json!([{"classification": "unclassified", "amount": unclassified}]),
    );
    output.insert(
        "classificationStatus".to_owned(),
        Value::String("not_supplied_by_portfolio_source".to_owned()),
    );
    report_result(Value::Object(output), revision, scope, context)
}

pub(super) fn risk(
    image: &PortfolioReadImage,
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let history = admitted_history(image, revision, scope)?;
    let periods = performance_periods(&history, scope)?;
    let returns = historical_returns(&periods)?;
    let mut output = base_report(revision, "task12_historical_risk_v1");
    output.insert("confidence".to_owned(), number(0.95)?);
    output.insert("scenario".to_owned(), standard_stress(revision, scope)?);
    if returns.is_empty() {
        output.insert(
            "historyStatus".to_owned(),
            Value::String("insufficient_history".to_owned()),
        );
        return report_result(Value::Object(output), revision, scope, context);
    }
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
    let value_at_risk = historical_var(&losses, confidence)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let expected_shortfall = discrete_expected_shortfall(&losses, confidence)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    output.insert("valueAtRisk".to_owned(), number(value_at_risk.value())?);
    output.insert(
        "expectedShortfall".to_owned(),
        number(expected_shortfall.value())?,
    );
    output.insert(
        "observations".to_owned(),
        Value::from(value_at_risk.observations()),
    );
    if returns.len() >= 2 {
        let series = ReturnSeries::try_new(
            returns
                .iter()
                .map(|value| {
                    StatisticalInput::try_new(
                        *value,
                        StatisticalUnit::Return,
                        StatisticalScale::Unit,
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| PortfolioApplicationServiceError::Analytics)?,
            Annualization::PeriodsPerYear(
                NonZeroU32::new(252).ok_or(PortfolioApplicationServiceError::Analytics)?,
            ),
        )
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
        match volatility(
            &series,
            VarianceConvention::Sample,
            MissingValuePolicy::Reject,
        ) {
            Ok(value) => {
                output.insert("annualizedVolatility".to_owned(), number(value.value())?);
            }
            Err(_) => {
                output.insert(
                    "volatilityStatus".to_owned(),
                    Value::String("zero_variance".to_owned()),
                );
            }
        }
    } else {
        output.insert(
            "volatilityStatus".to_owned(),
            Value::String("insufficient_history".to_owned()),
        );
    }
    output.insert(
        "trackingErrorStatus".to_owned(),
        Value::String("benchmark_not_supplied".to_owned()),
    );
    report_result(Value::Object(output), revision, scope, context)
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
    if let Some(start) = scope.start
        && let Some(first_in_range) = admitted
            .iter()
            .position(|revision| revision.effective_at >= start)
        && first_in_range > 0
    {
        admitted.drain(..first_in_range - 1);
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
        .try_fold(revision.account.cash_balance(), |total, holding| {
            total
                .checked_add(holding.market_value())
                .map_err(|_| PortfolioApplicationServiceError::Analytics)
        })
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
    revision: &PublishedRevision,
    scope: &ReadScope,
) -> Result<Vec<Value>, PortfolioApplicationServiceError> {
    let mut totals = BTreeMap::<Currency, Money>::new();
    let cash = revision.account.cash_balance();
    totals.insert(cash.currency(), cash);
    for holding in revision
        .holdings
        .iter()
        .filter(|holding| scope.admits_instrument(holding.instrument_id()))
    {
        let total = totals
            .entry(holding.currency())
            .or_insert_with(|| Money::new(Decimal::ZERO, holding.currency()));
        *total = total
            .checked_add(holding.market_value())
            .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    }
    Ok(totals
        .into_iter()
        .map(|(currency, amount)| {
            json!({"currency": currency.as_str(), "amount": money_value(amount)})
        })
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
