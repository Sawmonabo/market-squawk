//! Bounded exact scenario and scenario-batch evaluation over source-backed holdings.

use std::collections::BTreeSet;

use market_squawk_analytics::{
    AnalyticsError, ExactDecimalScale, ExactRate, MAX_BATCH_OBSERVATIONS, MonetaryBasis,
    MonetaryValue, PortfolioAllocation, ScenarioShock, ShockComposition, scenario_impact,
};
use market_squawk_data::MarketDataInstrumentReadCapability;
use market_squawk_domain::SourceIdentifier;
use market_squawk_services::{RequestContext, TypedToolRequest, TypedToolResult};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};

use super::{
    base_report, instrument_dimension, money_value, parse_instrument, parse_percentage,
    required_string,
};
use crate::portfolio_application::PortfolioApplicationServiceError;
use crate::portfolio_application::model::{HoldingObservation, PublishedRevision};
use crate::portfolio_application::read::{
    ReadScope, check_context, product_report_result, snapshot_token,
};

struct AdmittedScenario<'request> {
    id: SourceIdentifier,
    composition: ShockComposition,
    shocks: Vec<ScenarioShock>,
    submitted_shocks: &'request [Value],
    holding_indices: BTreeSet<usize>,
}

pub(super) fn evaluate_one(
    revision: &PublishedRevision,
    scope: &ReadScope,
    request: &TypedToolRequest,
    context: &RequestContext,
    instruments: Option<&MarketDataInstrumentReadCapability>,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    check_context(context)?;
    let scenario = request
        .arguments()
        .get("scenario")
        .and_then(Value::as_object)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    let admitted = admit_scenario(scenario, revision, scope, context)?;
    let value = evaluate(revision, &admitted, context, instruments)?;
    let mut output = base_report(revision, "exact_holding_scenario_v1");
    output.insert(
        "snapshotToken".to_owned(),
        Value::String(snapshot_token(revision)),
    );
    output.insert("scenario".to_owned(), value);
    check_context(context)?;
    product_report_result(Value::Object(output), revision, scope, context)
}

pub(super) fn evaluate_batch(
    revision: &PublishedRevision,
    scope: &ReadScope,
    request: &TypedToolRequest,
    context: &RequestContext,
    instruments: Option<&MarketDataInstrumentReadCapability>,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    check_context(context)?;
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
    let mut results = Vec::new();
    results
        .try_reserve_exact(values.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    for value in values {
        check_context(context)?;
        let scenario = value
            .as_object()
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        let admitted = admit_scenario(scenario, revision, scope, context)?;
        if !ids.insert(admitted.id.clone()) {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        results.push(evaluate(revision, &admitted, context, instruments)?);
    }
    let mut output = base_report(revision, "exact_holding_scenario_batch_v1");
    output.insert(
        "snapshotToken".to_owned(),
        Value::String(snapshot_token(revision)),
    );
    output.insert("scenarios".to_owned(), Value::Array(results));
    check_context(context)?;
    product_report_result(Value::Object(output), revision, scope, context)
}

fn admit_scenario<'request>(
    object: &'request Map<String, Value>,
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<AdmittedScenario<'request>, PortfolioApplicationServiceError> {
    check_context(context)?;
    if object.len() != 3 {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
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
    if values.len() > scope.maximum_items
        || values.len() > context.limits().maximum_result_items()
        || values.len() > MAX_BATCH_OBSERVATIONS
    {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    let mut shocks = Vec::new();
    shocks
        .try_reserve_exact(values.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    let mut holding_indices = BTreeSet::new();
    for value in values {
        check_context(context)?;
        let shock = value
            .as_object()
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        if shock.len() != 2 {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let instrument_id = parse_instrument(required_string(shock, "instrumentId")?)?;
        if !scope.admits_instrument(instrument_id) {
            return Err(PortfolioApplicationServiceError::NotFound);
        }
        // The immutable publication has the same sorted, unique holdings used by position pages.
        let index = revision
            .holdings
            .binary_search_by_key(&instrument_id, HoldingObservation::instrument_id)
            .map_err(|_| PortfolioApplicationServiceError::NotFound)?;
        let holding = &revision.holdings[index];
        if holding.account_id() != scope.account_id {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        if holding.market_value().currency() != revision.account.currency() {
            return Err(PortfolioApplicationServiceError::Analytics);
        }
        let rate = parse_percentage(required_string(shock, "percentChange")?)?;
        shocks.push(
            ScenarioShock::try_new(&instrument_dimension(instrument_id), rate)
                .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?,
        );
        holding_indices.insert(index);
    }
    Ok(AdmittedScenario {
        id,
        composition,
        shocks,
        submitted_shocks: values,
        holding_indices,
    })
}

fn evaluate(
    revision: &PublishedRevision,
    scenario: &AdmittedScenario<'_>,
    context: &RequestContext,
    instruments: Option<&MarketDataInstrumentReadCapability>,
) -> Result<Value, PortfolioApplicationServiceError> {
    let mut allocations = Vec::new();
    let mut ids = Vec::new();
    allocations
        .try_reserve_exact(scenario.holding_indices.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    ids.try_reserve_exact(scenario.holding_indices.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    let zero = ExactRate::try_new(Decimal::ZERO, ExactDecimalScale::Unit)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    for &index in &scenario.holding_indices {
        check_context(context)?;
        let holding = &revision.holdings[index];
        ids.push(holding.instrument_id());
        allocations.push(
            PortfolioAllocation::try_new(
                &instrument_dimension(holding.instrument_id()),
                MonetaryValue::new(holding.market_value(), MonetaryBasis::Total),
                zero,
            )
            .map_err(|_| PortfolioApplicationServiceError::Analytics)?,
        );
    }
    check_context(context)?;
    let result =
        scenario_impact(&allocations, &scenario.shocks, scenario.composition).map_err(|error| {
            match error {
                AnalyticsError::ReturnBelowFloor => {
                    PortfolioApplicationServiceError::InvalidRequest
                }
                _ => PortfolioApplicationServiceError::Analytics,
            }
        })?;
    let mut displays = crate::portfolio_application::instrument_display::resolve(
        instruments,
        &ids,
        revision.effective_at,
        revision.available_at,
        context,
    )?;
    let mut contributions = Vec::new();
    contributions
        .try_reserve_exact(ids.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    for (instrument_id, contribution) in ids.iter().zip(result.contributions()) {
        check_context(context)?;
        contributions.push(json!({
            "instrumentId": instrument_id.to_string(),
            "investment": displays.remove(instrument_id).unwrap_or(Value::Null),
            "amount": money_value(contribution.amount().money()),
        }));
    }
    Ok(json!({
        "id": scenario.id.as_str(),
        "composition": match scenario.composition {
            ShockComposition::Additive => "additive",
            ShockComposition::Compounded => "compounded",
        },
        "shocks": scenario.submitted_shocks,
        "contributions": contributions,
        "total": money_value(result.total().money()),
    }))
}
