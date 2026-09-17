//! Bounded exact scenario and scenario-batch evaluation over source-backed holdings.

use std::collections::BTreeSet;

use market_squawk_analytics::{ScenarioShock, ShockComposition, scenario_impact};
use market_squawk_domain::{InstrumentId, SourceIdentifier};
use market_squawk_services::{RequestContext, TypedToolRequest, TypedToolResult};
use serde_json::{Map, Value, json};

use super::{
    allocations, base_report, exact_rate, instrument_dimension, money_value, parse_decimal,
    parse_instrument, required_string,
};
use crate::portfolio_application::PortfolioApplicationServiceError;
use crate::portfolio_application::model::PublishedRevision;
use crate::portfolio_application::read::{ReadScope, report_result};

struct AdmittedScenario {
    id: SourceIdentifier,
    composition: ShockComposition,
    shocks: Vec<ScenarioShock>,
}

pub(super) fn evaluate_one(
    revision: &PublishedRevision,
    scope: &ReadScope,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let scenario = request
        .arguments()
        .get("scenario")
        .and_then(Value::as_object)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    let admitted = admit_scenario(scenario, revision, scope)?;
    let value = evaluate(revision, scope, &admitted)?;
    let mut output = base_report(revision, "exact_holding_scenario_v1");
    output.insert("scenario".to_owned(), value);
    report_result(Value::Object(output), revision, scope, context)
}

pub(super) fn evaluate_batch(
    revision: &PublishedRevision,
    scope: &ReadScope,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let values = request
        .arguments()
        .get("scenarios")
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty())
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    if values.len() > scope.maximum_items || values.len() > context.limits().maximum_result_items()
    {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    let mut ids = BTreeSet::new();
    let mut scenarios = Vec::new();
    let mut total_shocks = 0_usize;
    for value in values {
        let scenario = value
            .as_object()
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        let admitted = admit_scenario(scenario, revision, scope)?;
        if !ids.insert(admitted.id.clone()) {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        total_shocks = total_shocks
            .checked_add(admitted.shocks.len())
            .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
        scenarios.push(admitted);
    }
    let allocation_count = revision
        .holdings
        .iter()
        .filter(|holding| scope.admits_instrument(holding.instrument_id()))
        .count();
    let work = allocation_count
        .checked_mul(total_shocks)
        .and_then(|value| value.checked_add(allocation_count.checked_mul(scenarios.len())?))
        .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
    if work > scope.maximum_items.max(1).saturating_mul(64) {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    let results = scenarios
        .iter()
        .map(|scenario| evaluate(revision, scope, scenario))
        .collect::<Result<Vec<_>, _>>()?;
    let mut output = base_report(revision, "exact_holding_scenario_batch_v1");
    output.insert("scenarios".to_owned(), Value::Array(results));
    report_result(Value::Object(output), revision, scope, context)
}

fn admit_scenario(
    object: &Map<String, Value>,
    revision: &PublishedRevision,
    scope: &ReadScope,
) -> Result<AdmittedScenario, PortfolioApplicationServiceError> {
    let id = SourceIdentifier::try_from(required_string(object, "id")?)
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
    let composition = match required_string(object, "composition")? {
        "additive" => ShockComposition::Additive,
        "compounded" => ShockComposition::Compounded,
        _ => return Err(PortfolioApplicationServiceError::InvalidRequest),
    };
    let values = object
        .get("shocks")
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty())
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    if values.len() > scope.maximum_items {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    let mut shocks = Vec::new();
    for value in values {
        let shock = value
            .as_object()
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        let instrument_id = parse_instrument(required_string(shock, "instrumentId")?)?;
        require_holding(revision, scope, instrument_id)?;
        let rate = exact_rate(parse_decimal(required_string(shock, "rate")?)?)?;
        shocks.push(
            ScenarioShock::try_new(&instrument_dimension(instrument_id), rate)
                .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?,
        );
    }
    Ok(AdmittedScenario {
        id,
        composition,
        shocks,
    })
}

fn evaluate(
    revision: &PublishedRevision,
    scope: &ReadScope,
    scenario: &AdmittedScenario,
) -> Result<Value, PortfolioApplicationServiceError> {
    let allocations = allocations(revision, scope)?;
    if allocations.is_empty() {
        return Err(PortfolioApplicationServiceError::NotFound);
    }
    let result = scenario_impact(&allocations, &scenario.shocks, scenario.composition)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let instruments = revision
        .holdings
        .iter()
        .filter(|holding| scope.admits_instrument(holding.instrument_id()))
        .map(|holding| holding.instrument_id())
        .collect::<Vec<_>>();
    let contributions = instruments
        .iter()
        .zip(result.contributions())
        .map(|(instrument_id, contribution)| {
            json!({
                "instrumentId": instrument_id.to_string(),
                "amount": money_value(contribution.amount().money()),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "id": scenario.id.as_str(),
        "composition": match scenario.composition {
            ShockComposition::Additive => "additive",
            ShockComposition::Compounded => "compounded",
        },
        "contributions": contributions,
        "total": money_value(result.total().money()),
    }))
}

fn require_holding(
    revision: &PublishedRevision,
    scope: &ReadScope,
    instrument_id: InstrumentId,
) -> Result<(), PortfolioApplicationServiceError> {
    if !scope.admits_instrument(instrument_id)
        || !revision
            .holdings
            .iter()
            .any(|holding| holding.instrument_id() == instrument_id)
    {
        Err(PortfolioApplicationServiceError::NotFound)
    } else {
        Ok(())
    }
}
