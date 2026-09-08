//! Proposal-only rebalance calculations over pinned source holdings.

use std::collections::BTreeSet;

use market_squawk_domain::{InstrumentId, Money};
use market_squawk_services::{RequestContext, TypedToolRequest, TypedToolResult};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};

use super::{
    base_report, money_value, parse_decimal, parse_instrument, parse_money, required_string,
};
use crate::portfolio_application::PortfolioApplicationServiceError;
use crate::portfolio_application::model::PublishedRevision;
use crate::portfolio_application::read::{ReadScope, report_result};

struct Target {
    instrument_id: InstrumentId,
    weight: Decimal,
}

pub(super) fn rebalance(
    revision: &PublishedRevision,
    scope: &ReadScope,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    if !scope.instruments.is_empty() {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    let proposal = request
        .arguments()
        .get("proposal")
        .and_then(Value::as_object)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    let targets = parse_targets(proposal, revision, scope)?;
    let max_proposals = proposal
        .get("maxProposals")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
        .min(scope.maximum_items)
        .min(context.limits().maximum_result_items());
    if targets.len() > max_proposals {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    let max_turnover = parse_decimal(required_string(proposal, "maxTurnover")?)?;
    let minimum_cash = proposal
        .get("minimumCash")
        .map(parse_money)
        .transpose()?
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    let allow_short = proposal
        .get("allowShort")
        .and_then(Value::as_bool)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    if !(Decimal::ZERO..=Decimal::ONE).contains(&max_turnover)
        || minimum_cash.amount().is_sign_negative()
        || minimum_cash.currency() != revision.account.currency()
    {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    let total_value = total_value(revision, scope)?;
    if total_value.amount() <= Decimal::ZERO {
        return Err(PortfolioApplicationServiceError::Analytics);
    }
    let mut desired = Vec::new();
    for target in targets {
        let current = holding_value(revision, target.instrument_id)?.amount();
        let target_value = checked_mul(total_value.amount(), target.weight)?;
        desired.push((
            target.instrument_id,
            current,
            checked_sub(target_value, current)?,
        ));
    }
    let gross = desired
        .iter()
        .try_fold(Decimal::ZERO, |total, (_, _, delta)| {
            checked_add(total, delta.abs())
        })?;
    let one_way = checked_div(gross, Decimal::from(2_u32))?;
    let turnover_limit = checked_mul(total_value.amount(), max_turnover)?;
    let sales = desired
        .iter()
        .try_fold(Decimal::ZERO, |total, (_, _, delta)| {
            if delta.is_sign_negative() {
                checked_add(total, delta.abs())
            } else {
                Ok(total)
            }
        })?;
    let buys = desired
        .iter()
        .try_fold(Decimal::ZERO, |total, (_, _, delta)| {
            if delta.is_sign_positive() {
                checked_add(total, *delta)
            } else {
                Ok(total)
            }
        })?;
    let buy_capacity = checked_sub(
        checked_add(revision.account.cash_balance().amount(), sales)?,
        minimum_cash.amount(),
    )?
    .max(Decimal::ZERO);
    let turnover_scale = if one_way > turnover_limit {
        checked_div(turnover_limit, one_way)?
    } else {
        Decimal::ONE
    };
    let buy_scale = if buys > buy_capacity {
        checked_div(buy_capacity, buys)?
    } else {
        Decimal::ONE
    };
    let scale = turnover_scale.min(buy_scale);
    let mut trades = Vec::new();
    for (instrument_id, current, delta) in desired {
        let adjusted = checked_mul(delta, scale)?;
        if adjusted.is_zero() {
            continue;
        }
        if !allow_short && checked_add(current, adjusted)?.is_sign_negative() {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        trades.push((instrument_id, adjusted));
    }
    if trades.len() > max_proposals {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    let net = trades.iter().try_fold(Decimal::ZERO, |total, (_, delta)| {
        checked_add(total, *delta)
    })?;
    let projected_cash = Money::new(
        checked_sub(revision.account.cash_balance().amount(), net)?,
        revision.account.currency(),
    );
    if projected_cash.amount() < minimum_cash.amount() {
        return Err(PortfolioApplicationServiceError::Analytics);
    }
    let actual_gross = trades.iter().try_fold(Decimal::ZERO, |total, (_, delta)| {
        checked_add(total, delta.abs())
    })?;
    let turnover = checked_div(
        checked_div(actual_gross, Decimal::from(2_u32))?,
        total_value.amount(),
    )?;
    let rows = trades
        .into_iter()
        .map(|(instrument_id, delta)| {
            json!({
                "instrumentId": instrument_id.to_string(),
                "valueChange": money_value(Money::new(delta, revision.account.currency())),
            })
        })
        .collect::<Vec<_>>();
    let mut output = base_report(revision, "bounded_rebalance_proposal_v1");
    output.insert("trades".to_owned(), Value::Array(rows));
    output.insert("projectedCash".to_owned(), money_value(projected_cash));
    output.insert("turnover".to_owned(), Value::String(turnover.to_string()));
    output.insert("constrained".to_owned(), Value::Bool(scale < Decimal::ONE));
    output.insert(
        "authority".to_owned(),
        json!({
            "proposalOnly": true,
            "executionAuthority": false,
            "riskApprovalRequiredBeforeAnyOrder": true,
        }),
    );
    report_result(Value::Object(output), revision, scope, context)
}

fn parse_targets(
    proposal: &Map<String, Value>,
    revision: &PublishedRevision,
    scope: &ReadScope,
) -> Result<Vec<Target>, PortfolioApplicationServiceError> {
    let values = proposal
        .get("targets")
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty())
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    if values.len() > scope.maximum_items {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    let mut seen = BTreeSet::new();
    let mut total = Decimal::ZERO;
    let mut targets = Vec::new();
    for value in values {
        let target = value
            .as_object()
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        let instrument_id = parse_instrument(required_string(target, "instrumentId")?)?;
        let weight = parse_decimal(required_string(target, "targetWeight")?)?;
        if !scope.admits_instrument(instrument_id)
            || !(Decimal::ZERO..=Decimal::ONE).contains(&weight)
            || !seen.insert(instrument_id)
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let _holding = holding_value(revision, instrument_id)?;
        total = checked_add(total, weight)?;
        targets.push(Target {
            instrument_id,
            weight,
        });
    }
    let admitted_holdings = revision
        .holdings
        .iter()
        .map(|holding| holding.instrument_id())
        .collect::<BTreeSet<_>>();
    if admitted_holdings.len() != revision.holdings.len()
        || seen != admitted_holdings
        || total != Decimal::ONE
    {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    Ok(targets)
}

fn holding_value(
    revision: &PublishedRevision,
    instrument_id: InstrumentId,
) -> Result<Money, PortfolioApplicationServiceError> {
    let holding = revision
        .holdings
        .iter()
        .find(|holding| holding.instrument_id() == instrument_id)
        .ok_or(PortfolioApplicationServiceError::NotFound)?;
    if holding.market_value().currency() != revision.account.currency() {
        return Err(PortfolioApplicationServiceError::Analytics);
    }
    Ok(holding.market_value())
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

fn checked_add(left: Decimal, right: Decimal) -> Result<Decimal, PortfolioApplicationServiceError> {
    left.checked_add(right)
        .ok_or(PortfolioApplicationServiceError::Analytics)
}

fn checked_sub(left: Decimal, right: Decimal) -> Result<Decimal, PortfolioApplicationServiceError> {
    left.checked_sub(right)
        .ok_or(PortfolioApplicationServiceError::Analytics)
}

fn checked_mul(left: Decimal, right: Decimal) -> Result<Decimal, PortfolioApplicationServiceError> {
    left.checked_mul(right)
        .ok_or(PortfolioApplicationServiceError::Analytics)
}

fn checked_div(left: Decimal, right: Decimal) -> Result<Decimal, PortfolioApplicationServiceError> {
    left.checked_div(right)
        .ok_or(PortfolioApplicationServiceError::Analytics)
}
