//! Evidence-bound portfolio scenario, proposal, and candidate-impact operations.

mod planning;
mod scenario;

use market_squawk_analytics::{
    ExactDecimalScale, ExactRate, MonetaryBasis, MonetaryValue, PortfolioAllocation,
};
use market_squawk_domain::{Currency, InstrumentId, Money};
use market_squawk_services::{RequestContext, TypedToolRequest, TypedToolResult};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};

use super::PortfolioApplicationServiceError;
use super::model::PublishedRevision;
use super::read::ReadScope;

pub(super) fn call(
    revision: &PublishedRevision,
    scope: &ReadScope,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    match request.name() {
        "Portfolio.EvaluateScenario" => scenario::evaluate_one(revision, scope, request, context),
        "Portfolio.EvaluateScenarioBatch" => {
            scenario::evaluate_batch(revision, scope, request, context)
        }
        "Portfolio.ProposeRebalance" => planning::rebalance(revision, scope, request, context),
        _ => Err(PortfolioApplicationServiceError::InvalidRequest),
    }
}

pub(super) fn allocations(
    revision: &PublishedRevision,
    scope: &ReadScope,
) -> Result<Vec<PortfolioAllocation>, PortfolioApplicationServiceError> {
    revision
        .holdings
        .iter()
        .filter(|holding| scope.admits_instrument(holding.instrument_id()))
        .map(|holding| {
            PortfolioAllocation::try_new(
                &instrument_dimension(holding.instrument_id()),
                MonetaryValue::new(holding.market_value(), MonetaryBasis::Total),
                exact_rate(Decimal::ZERO)?,
            )
            .map_err(|_| PortfolioApplicationServiceError::Analytics)
        })
        .collect()
}

pub(super) fn base_report(revision: &PublishedRevision, _report_kind: &str) -> Map<String, Value> {
    let mut output = Map::new();
    output.insert(
        "accountId".to_owned(),
        Value::String(revision.account.account_id().to_string()),
    );
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
    output.insert(
        "dataConfidence".to_owned(),
        Value::String("limited".to_owned()),
    );
    output
}

pub(super) fn money_value(value: Money) -> Value {
    json!({
        "amount": value.amount().to_string(),
        "currency": value.currency().as_str(),
    })
}

pub(super) fn parse_money(value: &Value) -> Result<Money, PortfolioApplicationServiceError> {
    let object = value
        .as_object()
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    let amount = parse_decimal(required_string(object, "amount")?)?;
    let currency = Currency::try_from(required_string(object, "currency")?)
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
    Ok(Money::new(amount, currency))
}

pub(super) fn parse_decimal(value: &str) -> Result<Decimal, PortfolioApplicationServiceError> {
    value
        .parse::<Decimal>()
        .map(|decimal| decimal.normalize())
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)
}

pub(super) fn exact_rate(value: Decimal) -> Result<ExactRate, PortfolioApplicationServiceError> {
    ExactRate::try_new(value, ExactDecimalScale::Unit)
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)
}

pub(super) fn required_string<'value>(
    object: &'value Map<String, Value>,
    name: &str,
) -> Result<&'value str, PortfolioApplicationServiceError> {
    object
        .get(name)
        .and_then(Value::as_str)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)
}

pub(super) fn parse_instrument(
    value: &str,
) -> Result<InstrumentId, PortfolioApplicationServiceError> {
    value
        .parse()
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)
}

pub(super) fn instrument_dimension(instrument_id: InstrumentId) -> String {
    format!("instrument-{instrument_id}")
}
