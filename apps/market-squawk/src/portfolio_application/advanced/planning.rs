//! Proposal-only rebalance calculations over pinned source holdings.

use std::num::NonZeroUsize;

use market_squawk_analytics::ExactRate;
use market_squawk_data::MarketDataInstrumentReadCapability;
use market_squawk_portfolio::{
    PortfolioError, PortfolioLimitInput, PortfolioLimits, RebalanceCalculation,
    RebalanceConstraintInput, RebalanceConstraints, RebalanceTarget,
};
use market_squawk_services::{RequestContext, TypedToolRequest, TypedToolResult};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};

use super::{
    base_report, money_value, parse_instrument, parse_money, parse_percentage, required_string,
};
use crate::portfolio_application::PortfolioApplicationServiceError;
use crate::portfolio_application::model::{HoldingObservation, PublishedRevision};
use crate::portfolio_application::read::{
    ReadScope, check_context, product_report_result, snapshot_token,
};

pub(super) fn rebalance(
    revision: &PublishedRevision,
    scope: &ReadScope,
    request: &TypedToolRequest,
    context: &RequestContext,
    instruments: Option<&MarketDataInstrumentReadCapability>,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    check_context(context)?;
    if !scope.instruments.is_empty() {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    let proposal = request
        .arguments()
        .get("proposal")
        .and_then(Value::as_object)
        .filter(|object| object.len() == 4)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    let values = proposal
        .get("targets")
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty() && values.len() == revision.holdings.len())
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(values.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    for value in values {
        check_context(context)?;
        let target = value
            .as_object()
            .filter(|object| object.len() == 2)
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        targets.push(
            RebalanceTarget::try_new(
                parse_instrument(required_string(target, "instrumentId")?)?,
                percentage(target, "targetPercent")?,
            )
            .map_err(calculation_error)?,
        );
    }
    let minimum_cash = proposal
        .get("minimumCash")
        .filter(|value| value.as_object().is_some_and(|object| object.len() == 2))
        .map(parse_money)
        .transpose()?
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    let allow_short = proposal
        .get("allowShort")
        .and_then(Value::as_bool)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    let count =
        NonZeroUsize::new(values.len()).ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    let constraints = RebalanceConstraints::try_new(RebalanceConstraintInput {
        max_proposals: count,
        max_turnover: percentage(proposal, "maxTurnoverPercent")?,
        minimum_cash,
        allow_short,
    })
    .map_err(calculation_error)?;
    let mut holdings = Vec::new();
    holdings
        .try_reserve_exact(revision.holdings.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    for holding in &revision.holdings {
        check_context(context)?;
        if holding.account_id() != scope.account_id {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        holdings.push((holding.instrument_id(), holding.market_value()));
    }
    // This operation returns one complete report, not a position page. Nested targets/trades
    // must cover the entire admitted publication; paging defaults are not financial constraints.
    // Existing request/result byte budgets and publication/kernel ceilings still apply.
    let limits = PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: 1,
        max_instruments: count.get(),
        max_lots: 1,
        max_transactions: 1,
        max_factors: 1,
        max_scenarios: 1,
        max_history: 1,
        max_results: count.get(),
        max_retained_bytes: scope.maximum_bytes,
    })
    .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    check_context(context)?;
    let calculation = RebalanceCalculation::try_calculate(
        revision.account.cash_balance(),
        &holdings,
        &targets,
        constraints,
        limits,
    )
    .map_err(calculation_error)?;
    check_context(context)?;
    let ids = calculation
        .trades()
        .iter()
        .map(|trade| trade.instrument_id())
        .collect::<Vec<_>>();
    let mut displays = crate::portfolio_application::instrument_display::resolve(
        instruments,
        &ids,
        revision.effective_at,
        revision.available_at,
        context,
    )?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(calculation.trades().len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    for trade in calculation.trades() {
        check_context(context)?;
        let instrument_id = trade.instrument_id();
        let index = revision
            .holdings
            .binary_search_by_key(&instrument_id, HoldingObservation::instrument_id)
            .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
        let current = revision.holdings[index].market_value();
        let projected = current
            .checked_add(trade.value_change())
            .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
        rows.push(json!({
            "instrumentId": instrument_id.to_string(),
            "investment": displays.remove(&instrument_id).unwrap_or(Value::Null),
            "currentValue": money_value(current),
            "valueChange": money_value(trade.value_change()),
            "projectedValue": money_value(projected),
        }));
    }
    let turnover_percent = calculation
        .turnover()
        .value()
        .checked_mul(Decimal::from(100_u32))
        .ok_or(PortfolioApplicationServiceError::Analytics)?;
    let mut output = base_report(revision, "bounded_rebalance_proposal_v1");
    output.insert(
        "snapshotToken".to_owned(),
        Value::String(snapshot_token(revision)),
    );
    output.insert("proposal".to_owned(), Value::Object(proposal.clone()));
    output.insert(
        "totalValue".to_owned(),
        money_value(calculation.total_value()),
    );
    output.insert("trades".to_owned(), Value::Array(rows));
    output.insert(
        "projectedCash".to_owned(),
        money_value(calculation.projected_cash()),
    );
    output.insert(
        "turnoverPercent".to_owned(),
        Value::String(turnover_percent.normalize().to_string()),
    );
    output.insert(
        "constrained".to_owned(),
        Value::Bool(calculation.constrained()),
    );
    check_context(context)?;
    product_report_result(Value::Object(output), revision, scope, context)
}

fn percentage(
    object: &Map<String, Value>,
    name: &str,
) -> Result<ExactRate, PortfolioApplicationServiceError> {
    let rate = parse_percentage(required_string(object, name)?)?;
    if !(Decimal::ZERO..=Decimal::ONE).contains(&rate.value()) {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    Ok(rate)
}

fn calculation_error(error: PortfolioError) -> PortfolioApplicationServiceError {
    match error {
        PortfolioError::InvalidDimension
        | PortfolioError::InvalidPolicy
        | PortfolioError::CurrencyMismatch => PortfolioApplicationServiceError::InvalidRequest,
        PortfolioError::LimitExceeded { .. } => PortfolioApplicationServiceError::ResourceExhausted,
        _ => PortfolioApplicationServiceError::Analytics,
    }
}
