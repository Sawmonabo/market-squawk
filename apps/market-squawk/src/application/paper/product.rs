//! Ordinary paper-trading product projection over private native execution evidence.

use std::collections::BTreeSet;
use std::time::Instant;

use chrono::{DateTime, SecondsFormat, Utc};
use market_squawk_adapter_paper::{
    PaperAccountRiskSnapshot, PaperExecutionSnapshot, PaperFillSnapshot, PaperOrderSnapshot,
    PaperOrderState,
};
use market_squawk_data::{InstrumentDefinitionReadCapability, MarketDataInstrumentReadCapability};
use market_squawk_decisions::TargetState;
use market_squawk_domain::{
    AccountId, BasisPoints, InstrumentDefinition, InstrumentExecutionTerms, InstrumentId, Money,
    OrderSide, PriceTicks, QuantityLots, Timestamp,
};
use market_squawk_execution::{CancelReceipt, CancelStatus, RiskLimitsSnapshot};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    PaperStrategyMode, ProductAuthorityTokens, ProductionExecutionAuditRecord,
    ProductionExecutionAuditSnapshot, ServiceError, TargetLadderSelector, product_risk_outcome,
    product_risk_reason,
};

pub(super) struct ProductInstrument {
    id: InstrumentId,
    definition: InstrumentDefinition,
    name: Box<str>,
    symbol: Option<Box<str>>,
}

pub(super) fn required_instruments(
    snapshot: &PaperExecutionSnapshot,
    audit: &ProductionExecutionAuditSnapshot,
    limits: &RiskLimitsSnapshot,
) -> Vec<InstrumentId> {
    let mut ids = BTreeSet::new();
    ids.extend(
        snapshot
            .positions()
            .iter()
            .map(|position| position.instrument_id()),
    );
    ids.extend(
        snapshot
            .orders()
            .iter()
            .map(|order| order.execution_terms().instrument_id()),
    );
    ids.extend(
        audit
            .records()
            .iter()
            .map(|record| record.event().instrument_id()),
    );
    ids.extend(limits.eligible_instruments().iter().copied());
    ids.into_iter().collect()
}

pub(super) fn execution_instruments(snapshot: &PaperExecutionSnapshot) -> Vec<InstrumentId> {
    snapshot
        .orders()
        .iter()
        .map(|order| order.execution_terms().instrument_id())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(super) fn load_instruments(
    ids: &[InstrumentId],
    definitions: &InstrumentDefinitionReadCapability,
    market_data: &MarketDataInstrumentReadCapability,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<ProductInstrument>, ServiceError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut executable = definitions
        .latest(ids, ids.len().max(1), deadline, cancellation)
        .map_err(|_| ServiceError::Unavailable)?;
    if executable.len() != ids.len() {
        return Err(ServiceError::Unavailable);
    }
    executable.sort_unstable_by_key(|candidate| candidate.instrument_id());
    if executable
        .windows(2)
        .any(|pair| pair[0].instrument_id() == pair[1].instrument_id())
    {
        return Err(ServiceError::Unavailable);
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(ids.len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    for id in ids {
        let definition = executable
            .binary_search_by_key(id, |candidate| candidate.instrument_id())
            .ok()
            .and_then(|index| executable.get(index))
            .cloned()
            .ok_or(ServiceError::Unavailable)?;
        let display = market_data
            .latest(*id, deadline, cancellation)
            .map_err(|_| ServiceError::Unavailable)?;
        let display_definition = display.as_ref().map(|record| record.definition());
        let name = display_definition
            .and_then(|value| value.display_name())
            .map(|name| name.as_str())
            .filter(|name| name.len() <= 256)
            .map(str::to_owned)
            .ok_or(ServiceError::Unavailable)?;
        output.push(ProductInstrument {
            id: *id,
            definition,
            name: name.into_boxed_str(),
            // No stable primary-symbol authority exists in the current canonical definition.
            // Venue/provider mapping order must not choose an ordinary product symbol.
            symbol: None,
        });
    }
    output.sort_unstable_by_key(|candidate| candidate.id);
    Ok(output)
}

pub(super) fn instrument<'a>(
    instruments: &'a [ProductInstrument],
    id: InstrumentId,
) -> Result<&'a ProductInstrument, ServiceError> {
    instruments
        .binary_search_by_key(&id, |candidate| candidate.id)
        .ok()
        .and_then(|index| instruments.get(index))
        .ok_or(ServiceError::Unavailable)
}

pub(super) fn investment(value: &ProductInstrument) -> Value {
    json!({"name": value.name.as_ref(), "symbol": value.symbol.as_deref()})
}

pub(super) fn money(value: Money) -> Value {
    json!({
        "amount": value.amount().normalize().to_string(),
        "currency": value.currency().as_str(),
    })
}

pub(super) fn timestamp(value: Timestamp) -> String {
    DateTime::<Utc>::from_timestamp_nanos(value.unix_nanos())
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
}

pub(super) fn percentage(value: BasisPoints) -> String {
    format!("{}%", Decimal::new(i64::from(value.get()), 2).normalize())
}

pub(super) fn price(
    value: PriceTicks,
    terms: InstrumentExecutionTerms,
) -> Result<Value, ServiceError> {
    let amount = value
        .checked_to_decimal(terms.price_tick())
        .map_err(|_| ServiceError::Unavailable)?;
    Ok(money(Money::new(amount, terms.quote_currency())))
}

pub(super) fn quantity(
    value: QuantityLots,
    terms: InstrumentExecutionTerms,
) -> Result<String, ServiceError> {
    value
        .checked_to_decimal(terms.lot_size())
        .map(|value| value.normalize().to_string())
        .map_err(|_| ServiceError::Unavailable)
}

fn position_quantity(value: i64, instrument: &ProductInstrument) -> Result<String, ServiceError> {
    Decimal::from(value)
        .checked_mul(instrument.definition.lot_size().as_decimal())
        .map(|value| value.normalize().to_string())
        .ok_or(ServiceError::Unavailable)
}

fn account_name(
    accounts: &[PaperAccountRiskSnapshot],
    account_id: AccountId,
) -> Result<String, ServiceError> {
    let position = accounts
        .iter()
        .position(|account| account.account_id() == account_id)
        .ok_or(ServiceError::Unavailable)?;
    Ok(format!("Virtual portfolio {}", position + 1))
}

pub(super) fn status(
    strategy_mode: PaperStrategyMode,
    snapshot: &PaperExecutionSnapshot,
    audit: &ProductionExecutionAuditSnapshot,
    limits: &RiskLimitsSnapshot,
    instruments: &[ProductInstrument],
    maximum_items: usize,
    financial_reconciliation_current: bool,
) -> Result<Value, ServiceError> {
    let accounts = snapshot
        .accounts()
        .iter()
        .take(maximum_items)
        .enumerate()
        .map(|(index, account)| {
            json!({
                "displayName": format!("Virtual portfolio {}", index + 1),
                "eligible": account.eligible(),
                "settledCapital": money(account.settled_capital()),
                "markedEquity": money(account.marked_equity()),
                "peakMarkedEquity": money(account.peak_marked_equity()),
                "grossExposure": money(account.marked_gross_exposure()),
                "unrealizedPnl": money(account.unrealized_pnl()),
                "realizedPnl": money(account.realized_pnl()),
                "maximumDrawdown": money(account.drawdown()),
            })
        })
        .collect::<Vec<_>>();
    let positions = snapshot
        .positions()
        .iter()
        .take(maximum_items)
        .map(|position| {
            let definition = instrument(instruments, position.instrument_id())?;
            Ok(json!({
                "accountName": account_name(snapshot.accounts(), position.account_id())?,
                "investment": investment(definition),
                "quantity": position_quantity(position.lots(), definition)?,
                "costBasis": money(position.cost_basis()),
            }))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    let eligible = limits
        .eligible_instruments()
        .iter()
        .take(maximum_items)
        .map(|id| instrument(instruments, *id).map(investment))
        .collect::<Result<Vec<_>, _>>()?;
    let decisions = audit
        .records()
        .iter()
        .take(maximum_items)
        .map(|record| decision(record, instruments))
        .collect::<Result<Vec<_>, _>>()?;
    let reconciliation_current = snapshot.complete()
        && !snapshot.reconciliation_required()
        && financial_reconciliation_current;
    let order_window_seconds = Decimal::from(limits.order_rate_window_nanos())
        .checked_div(Decimal::from(1_000_000_000_u64))
        .ok_or(ServiceError::Unavailable)?;
    let account_count = accounts.len();
    let position_count = positions.len();
    let eligible_count = eligible.len();
    let decision_count = decisions.len();
    Ok(json!({
        "sessionAvailability": "active",
        "safeguards": if reconciliation_current { "active" } else { "action_needed" },
        "modeLabel": match strategy_mode { PaperStrategyMode::Manual => "Manual practice", PaperStrategyMode::BookImbalance => "Guided practice" },
        "accountUpdate": if reconciliation_current { "complete" } else { "incomplete" },
        "accounts": {"rows": accounts, "returnedItems": account_count, "availableItems": snapshot.accounts().len()},
        "positions": {"rows": positions, "returnedItems": position_count, "availableItems": snapshot.positions().len()},
        "safety": {
            "maximumOrderValue": money(limits.maximum_order_notional()),
            "maximumTotalExposure": money(limits.maximum_gross_exposure()),
            "maximumPosition": "Restricted per investment",
            "leverageLimit": percentage(limits.maximum_leverage()),
            "minimumCapital": money(limits.minimum_capital()),
            "maximumLoss": money(limits.maximum_loss()),
            "maximumDrawdown": money(limits.maximum_drawdown()),
            "maximumFees": percentage(limits.maximum_fee()),
            "maximumPriceDeviation": percentage(limits.maximum_price_deviation()),
            "maximumSlippage": percentage(limits.maximum_slippage()),
            "orderPace": format!("Up to {} orders every {} seconds", limits.maximum_orders_per_window().get(), order_window_seconds.normalize()),
            "shorting": if limits.allow_short() { "allowed" } else { "disabled" },
            "emergencyStop": if limits.kill_switch() { "engaged" } else { "clear" },
            "eligibleInvestments": {"rows": eligible, "returnedItems": eligible_count, "availableItems": limits.eligible_instruments().len()},
        },
        "recentDecisions": {"rows": decisions, "returnedItems": decision_count, "availableItems": audit.available_items()},
        "reconciliation": {
            "state": if !snapshot.complete() { "incomplete" } else if reconciliation_current { "current" } else { "action_needed" },
            "activeOrders": snapshot.active_orders().len(),
            "completedOrders": snapshot.archived_orders().len(),
            "fills": snapshot.fills().len(),
            "accounts": snapshot.accounts().len(),
            "positions": snapshot.positions().len(),
        },
    }))
}

fn decision(
    record: &ProductionExecutionAuditRecord,
    instruments: &[ProductInstrument],
) -> Result<Value, ServiceError> {
    let event = record.event();
    let mut reasons = Vec::new();
    for raw in event.reasons() {
        let reason = product_risk_reason(raw);
        if !reasons.contains(&reason) {
            reasons.push(reason);
        }
    }
    if reasons.len() > 14 {
        return Err(ServiceError::Unavailable);
    }
    Ok(json!({
        "outcome": product_risk_outcome(event.kind()),
        "investment": investment(instrument(instruments, event.instrument_id())?),
        "marketObservedAt": timestamp(event.market_observed_at()),
        "validUntil": timestamp(event.valid_until()),
        "observedAt": timestamp(event.observed_at()),
        "reasons": reasons,
    }))
}

pub(super) fn order(
    order: &PaperOrderSnapshot,
    instruments: &[ProductInstrument],
    tokens: &mut ProductAuthorityTokens,
) -> Result<Value, ServiceError> {
    let terms = order.execution_terms();
    let definition = instrument(instruments, terms.instrument_id())?;
    Ok(json!({
        "actionToken": tokens.order_token(order.order_id())?,
        "state": order_state(order.state()),
        "investment": investment(definition),
        "direction": match order.side() { OrderSide::Buy => "buy", OrderSide::Sell => "sell" },
        "requestedQuantity": quantity(order.requested(), terms)?,
        "filledQuantity": quantity(order.cumulative_filled(), terms)?,
        "averageFillPrice": order.average_fill_price().map(|value| price(value, terms)).transpose()?,
        "maximumExecutionPrice": price(order.maximum_execution_price(), terms)?,
        "maximumSlippage": percentage(order.maximum_slippage()),
        "fees": money(order.cumulative_fees()),
        "acceptedAt": timestamp(order.accepted_at()),
        "expiresAt": timestamp(order.expires_at()),
        "targetLinked": order.target_reference().is_some(),
        "cancellationAvailable": matches!(order.state(), PaperOrderState::New | PaperOrderState::Accepted | PaperOrderState::PartiallyFilled),
    }))
}

pub(super) fn fill(
    fill: PaperFillSnapshot,
    orders: &[PaperOrderSnapshot],
    instruments: &[ProductInstrument],
) -> Result<Value, ServiceError> {
    let order = orders
        .iter()
        .find(|order| order.order_id() == fill.order_id())
        .ok_or(ServiceError::Unavailable)?;
    let terms = order.execution_terms();
    let definition = instrument(instruments, terms.instrument_id())?;
    Ok(json!({
        "investment": investment(definition),
        "quantity": quantity(fill.quantity(), terms)?,
        "averagePrice": price(fill.average_price(), terms)?,
        "maximumPrice": price(fill.maximum_price(), terms)?,
        "notional": money(fill.notional()),
        "fee": money(fill.fee()),
        "occurredAt": timestamp(fill.event_at()),
    }))
}

const fn order_state(state: PaperOrderState) -> &'static str {
    match state {
        PaperOrderState::New => "waiting",
        PaperOrderState::Accepted => "accepted",
        PaperOrderState::PartiallyFilled => "partially_filled",
        PaperOrderState::Filled => "filled",
        PaperOrderState::CancelPending => "cancel_requested",
        PaperOrderState::Canceled => "cancelled",
        PaperOrderState::Rejected => "declined",
        PaperOrderState::Expired => "expired",
    }
}

pub(super) fn cancel(
    receipt: CancelReceipt,
    action_token: &str,
    order: &PaperOrderSnapshot,
) -> Result<Value, ServiceError> {
    let terms = order.execution_terms();
    Ok(json!({
        "actionToken": action_token,
        "state": match receipt.status() { CancelStatus::Pending => "pending", CancelStatus::Canceled => "cancelled", CancelStatus::AlreadyTerminal => "already_complete" },
        "observedAt": timestamp(receipt.observed_at()),
        "filledQuantity": quantity(receipt.cumulative_filled(), terms)?,
        "averageFillPrice": receipt.average_fill_price().map(|value| price(value, terms)).transpose()?,
        "fees": money(receipt.cumulative_fees()),
    }))
}

pub(super) fn manual_target(
    target: &TargetState,
    target_token: &str,
    instrument: &ProductInstrument,
    can_sell: bool,
) -> Result<Value, ServiceError> {
    if target.target().thesis().as_str().len() > 4_096 {
        return Err(ServiceError::Unavailable);
    }
    let mut ladder = Vec::new();
    ladder
        .try_reserve_exact(10)
        .map_err(|_| ServiceError::ResourceExhausted)?;
    for level in [
        TargetLadderSelector::Downside,
        TargetLadderSelector::Add,
        TargetLadderSelector::EntryLower,
        TargetLadderSelector::EntryUpper,
        TargetLadderSelector::Base,
        TargetLadderSelector::TrimLower,
        TargetLadderSelector::TrimUpper,
        TargetLadderSelector::ExitLower,
        TargetLadderSelector::ExitUpper,
        TargetLadderSelector::Upside,
    ] {
        ladder.push(json!({
            "level": level.level(),
            "label": level.label(),
            "value": money(level.price(target)),
        }));
    }
    let mut side_choices = vec![json!({
        "value": "buy",
        "label": "Buy",
        "explanation": "Practice adding this investment to the virtual portfolio.",
    })];
    if can_sell {
        side_choices.push(json!({
            "value": "sell",
            "label": "Sell",
            "explanation": "Practice reducing a virtual position or establishing a permitted short position.",
        }));
    }
    let day = json!({"value": "day", "label": "Today", "explanation": "Cancel any unfilled amount when today's session ends."});
    let gtc = json!({"value": "good_til_cancelled", "label": "Until cancelled", "explanation": "Keep the virtual order available until it fills, expires, or you cancel it."});
    let ioc = json!({"value": "immediate_or_cancel", "label": "Fill now or cancel", "explanation": "Fill available quantity immediately and cancel the rest."});
    let fok = json!({"value": "fill_or_kill", "label": "All now or cancel", "explanation": "Fill the full quantity immediately or cancel the entire virtual order."});
    let order_choices = vec![
        json!({"value": "market", "label": "Market", "explanation": "Use the available simulated market price within the active safeguards.", "requiresLimitLevel": false, "requiresStopLevel": false, "timeInForceChoices": [day.clone(), ioc.clone(), fok.clone()]}),
        json!({"value": "limit", "label": "Limit", "explanation": "Do not trade beyond the selected target level.", "requiresLimitLevel": true, "requiresStopLevel": false, "timeInForceChoices": [day.clone(), gtc.clone(), ioc.clone(), fok.clone()]}),
        json!({"value": "stop", "label": "Stop", "explanation": "Activate the virtual order after the selected stop level is reached.", "requiresLimitLevel": false, "requiresStopLevel": true, "timeInForceChoices": [day.clone(), gtc.clone()]}),
        json!({"value": "stop_limit", "label": "Stop limit", "explanation": "Activate at the stop level and retain the selected execution limit.", "requiresLimitLevel": true, "requiresStopLevel": true, "timeInForceChoices": [day, gtc]}),
    ];
    Ok(json!({
        "targetToken": target_token,
        "investment": investment(instrument),
        "thesis": target.target().thesis().as_str(),
        "expiresAt": timestamp(target.target().target().expires_at()),
        "reviewDueAt": timestamp(target.target().review_due_at()),
        "ladder": ladder,
        "sideChoices": side_choices,
        "orderChoices": order_choices,
    }))
}
