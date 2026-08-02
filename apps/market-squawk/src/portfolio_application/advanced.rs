//! Evidence-bound portfolio attribution, scenario, proposal, and candidate-impact operations.

mod planning;
mod scenario;

use std::collections::BTreeSet;

use market_squawk_analytics::{
    ExactDecimalScale, ExactRate, MonetaryBasis, MonetaryValue, PortfolioAllocation,
    portfolio_attribution,
};
use market_squawk_domain::{Currency, InstrumentId, Money};
use market_squawk_services::{RequestContext, TypedToolRequest, TypedToolResult};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};

use super::PortfolioApplicationServiceError;
use super::import::hex;
use super::model::{PortfolioReadImage, PublishedRevision};
use super::read::{ReadScope, report_result};

pub(super) fn call(
    image: &PortfolioReadImage,
    revision: &PublishedRevision,
    scope: &ReadScope,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    match request.name() {
        "Portfolio.GetAttribution" => attribution(image, revision, scope, request, context),
        "Portfolio.EvaluateScenario" => scenario::evaluate_one(revision, scope, request, context),
        "Portfolio.EvaluateScenarioBatch" => {
            scenario::evaluate_batch(revision, scope, request, context)
        }
        "Portfolio.ProposeRebalance" => planning::rebalance(revision, scope, request, context),
        "Portfolio.EvaluateCandidateImpact" => {
            planning::candidate_impact(revision, scope, request, context)
        }
        _ => Err(PortfolioApplicationServiceError::InvalidRequest),
    }
}

fn attribution(
    image: &PortfolioReadImage,
    selected: &PublishedRevision,
    scope: &ReadScope,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let baseline_id = required_string(request.arguments(), "baselineRevisionId")?;
    let history = image
        .accounts
        .get(&scope.account_id)
        .ok_or(PortfolioApplicationServiceError::NotFound)?;
    let selected_index = history
        .revisions
        .iter()
        .position(|revision| revision.token() == selected.token())
        .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
    let baseline = history.revisions[..selected_index]
        .iter()
        .find(|revision| hex(&revision.token().bytes()) == baseline_id)
        .ok_or(PortfolioApplicationServiceError::NotFound)?;
    if baseline.available_at.is_none()
        || selected.available_at.is_none()
        || baseline.account.currency() != selected.account.currency()
    {
        return Err(PortfolioApplicationServiceError::Analytics);
    }

    let mut seen = BTreeSet::new();
    let mut instrument_ids = Vec::new();
    let mut allocations = Vec::new();
    for opening in baseline
        .holdings
        .iter()
        .filter(|holding| scope.admits_instrument(holding.instrument_id()))
    {
        let opening_value = opening.market_value();
        if opening_value.currency() != selected.account.currency()
            || opening_value.amount() <= Decimal::ZERO
            || !seen.insert(opening.instrument_id())
        {
            return Err(PortfolioApplicationServiceError::Analytics);
        }
        let closing_value = selected
            .holdings
            .iter()
            .find(|holding| holding.instrument_id() == opening.instrument_id())
            .map_or(Decimal::ZERO, |holding| holding.market_value().amount());
        let return_rate = closing_value
            .checked_div(opening_value.amount())
            .and_then(|ratio| ratio.checked_sub(Decimal::ONE))
            .ok_or(PortfolioApplicationServiceError::Analytics)?;
        let rate = exact_rate(return_rate)?;
        instrument_ids.push(opening.instrument_id());
        allocations.push(
            PortfolioAllocation::try_new(
                &instrument_dimension(opening.instrument_id()),
                MonetaryValue::new(opening_value, MonetaryBasis::Total),
                rate,
            )
            .map_err(|_| PortfolioApplicationServiceError::Analytics)?,
        );
    }
    if allocations.is_empty() || allocations.len() > scope.maximum_items {
        return Err(PortfolioApplicationServiceError::Analytics);
    }
    let result = portfolio_attribution(&allocations)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let contributions = instrument_ids
        .iter()
        .zip(result.contributions())
        .map(|(instrument_id, contribution)| {
            json!({
                "instrumentId": instrument_id.to_string(),
                "amount": money_value(contribution.amount().money()),
            })
        })
        .collect::<Vec<_>>();
    let mut output = base_report(selected, "source_mark_change_attribution_v1");
    output.insert(
        "baselineRevisionId".to_owned(),
        Value::String(hex(&baseline.token().bytes())),
    );
    output.insert(
        "baselineEffectiveAtUnixNanos".to_owned(),
        Value::String(baseline.effective_at.unix_nanos().to_string()),
    );
    output.insert(
        "baselineAvailableAtUnixNanos".to_owned(),
        baseline.available_at.map_or(Value::Null, |value| {
            Value::String(value.unix_nanos().to_string())
        }),
    );
    output.insert("contributions".to_owned(), Value::Array(contributions));
    output.insert("total".to_owned(), money_value(result.total().money()));
    output.insert(
        "methodDisclosure".to_owned(),
        Value::String(
            "source_mark_change_without_cash_flow_or_corporate_action_adjustment".to_owned(),
        ),
    );
    report_result(Value::Object(output), selected, scope, context)
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

pub(super) fn base_report(revision: &PublishedRevision, policy: &str) -> Map<String, Value> {
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
    output.insert(
        "markEvidence".to_owned(),
        json!({
            "sourceId": revision.source_id.as_str(),
            "sourceCoverage": revision
                .source_coverage
                .iter()
                .map(|source| source.as_str())
                .collect::<Vec<_>>(),
            "artifactSha256": hex(&revision.artifact_sha256),
            "quality": "direct_unverified",
            "executionEligible": false,
        }),
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
