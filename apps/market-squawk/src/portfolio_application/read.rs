//! Bounded point-in-time portfolio request admission and result construction.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use market_squawk_adapter_portfolio::{BasisResolution, LotMethod, TransactionKind};
use market_squawk_domain::{AccountId, InstrumentId, Money, Timestamp};
use market_squawk_services::{
    RequestContext, ServiceLimits, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::analytics;
use super::import::hex;
use super::model::{PortfolioReadImage, PublishedRevision};
use super::{PortfolioApplicationLimits, PortfolioApplicationServiceError};

pub(super) struct ReadScope {
    pub(super) account_id: AccountId,
    pub(super) instruments: BTreeSet<InstrumentId>,
    pub(super) start: Option<Timestamp>,
    pub(super) end: Option<Timestamp>,
    pub(super) maximum_items: usize,
    pub(super) maximum_bytes: usize,
}

impl ReadScope {
    pub(super) fn from_request(
        request: &TypedToolRequest,
        application_limits: PortfolioApplicationLimits,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let account_id = request
            .arguments()
            .get("accountId")
            .and_then(Value::as_str)
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
            .parse()
            .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
        Self::from_request_with_account(request, application_limits, account_id)
    }

    pub(super) fn from_product_request(
        image: &PortfolioReadImage,
        request: &TypedToolRequest,
        application_limits: PortfolioApplicationLimits,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let account_token = request
            .arguments()
            .get("accountToken")
            .and_then(Value::as_str)
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        if request.arguments().contains_key("instrumentIds") {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let catalog = super::product::account_catalog(image)?;
        let account_id = super::product::resolve_account_token(&catalog, account_token)?;
        Self::from_request_with_account(request, application_limits, account_id)
    }

    fn from_request_with_account(
        request: &TypedToolRequest,
        application_limits: PortfolioApplicationLimits,
        account_id: AccountId,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let instruments = request
            .arguments()
            .get("instrumentIds")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
                            .parse()
                            .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)
                    })
                    .collect()
            })
            .transpose()?
            .unwrap_or_default();
        let (start, end) = request
            .arguments()
            .get("timeRange")
            .and_then(Value::as_object)
            .map(parse_time_range)
            .transpose()?
            .unwrap_or((None, None));
        let result_limits = request
            .arguments()
            .get("resultLimits")
            .and_then(Value::as_object)
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        let maximum_items = result_limits
            .get("maximumItems")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
            .min(application_limits.max_result_items);
        let maximum_bytes = result_limits
            .get("maximumBytes")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
            .min(application_limits.max_retained_bytes);
        Ok(Self {
            account_id,
            instruments,
            start,
            end,
            maximum_items,
            maximum_bytes,
        })
    }

    pub(super) fn admits_instrument(&self, instrument_id: InstrumentId) -> bool {
        self.instruments.is_empty() || self.instruments.contains(&instrument_id)
    }

    pub(super) fn admits_time(&self, timestamp: Timestamp) -> bool {
        self.start.is_none_or(|start| timestamp >= start)
            && self.end.is_none_or(|end| timestamp <= end)
    }
}

pub(super) fn call(
    image: &PortfolioReadImage,
    request: &TypedToolRequest,
    context: &RequestContext,
    limits: PortfolioApplicationLimits,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    match request.name() {
        "Portfolio.ListAccounts" => return list_accounts(image, request, context, limits),
        "Portfolio.ListRevisions" => return list_revisions(image, request, context, limits),
        _ => {}
    }
    let scope = if request.name() == "Portfolio.GetRisk" {
        ReadScope::from_product_request(image, request, limits)?
    } else {
        ReadScope::from_request(request, limits)?
    };
    let revision = select_revision(image, &scope)?;
    match request.name() {
        "Portfolio.GetHoldings" => holdings(revision, &scope, context),
        "Portfolio.GetTransactions" => transactions(revision, &scope, context),
        "Portfolio.GetPerformance" => analytics::performance(image, revision, &scope, context),
        "Portfolio.GetExposure" => analytics::exposure(revision, &scope, context),
        "Portfolio.GetRisk" => analytics::risk(image, revision, &scope, context),
        "Portfolio.GetAttribution"
        | "Portfolio.EvaluateScenario"
        | "Portfolio.EvaluateScenarioBatch"
        | "Portfolio.ProposeRebalance" => {
            super::advanced::call(image, revision, &scope, request, context)
        }
        _ => Err(PortfolioApplicationServiceError::InvalidRequest),
    }
}

fn list_accounts(
    image: &PortfolioReadImage,
    request: &TypedToolRequest,
    context: &RequestContext,
    application_limits: PortfolioApplicationLimits,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let (maximum_items, maximum_bytes) = read_result_limits(request, application_limits)?;
    let catalog = super::product::account_catalog(image)?;
    let after_account = request
        .arguments()
        .get("afterAccountToken")
        .and_then(Value::as_str)
        .map(|token| super::product::resolve_account_token(&catalog, token))
        .transpose()?;
    let rows = catalog
        .iter()
        .filter(|binding| after_account.is_none_or(|after| binding.account_id() > after))
        .map(|binding| {
            let revision = image
                .accounts
                .get(&binding.account_id())
                .and_then(|history| history.revisions.last())
                .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
            account_summary(binding, revision)
        })
        .collect::<Result<Vec<_>, _>>()?;
    portfolio_page(
        rows,
        maximum_items,
        maximum_bytes,
        context,
        json!({"scope": "portfolio_accounts"}),
    )
}

fn list_revisions(
    image: &PortfolioReadImage,
    request: &TypedToolRequest,
    context: &RequestContext,
    application_limits: PortfolioApplicationLimits,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let scope = ReadScope::from_request(request, application_limits)?;
    let after_snapshot = request
        .arguments()
        .get("afterSnapshotToken")
        .and_then(Value::as_str);
    let history = image
        .accounts
        .get(&scope.account_id)
        .ok_or(PortfolioApplicationServiceError::NotFound)?;
    let mut cursor_seen = after_snapshot.is_none();
    let mut rows = Vec::new();
    for revision in &history.revisions {
        let token = snapshot_token(revision);
        if !cursor_seen {
            if after_snapshot == Some(token.as_str()) {
                cursor_seen = true;
            }
            continue;
        }
        if !scope.admits_time(revision.effective_at)
            || scope.end.is_some_and(|end| {
                revision
                    .available_at
                    .is_none_or(|available| available > end)
            })
        {
            continue;
        }
        rows.push(revision_summary(revision));
    }
    if !cursor_seen {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    portfolio_page(
        rows,
        scope.maximum_items,
        scope.maximum_bytes,
        context,
        json!({"scope": "portfolio_history"}),
    )
}

fn account_summary(
    binding: &super::product::ProductAccountBinding,
    revision: &PublishedRevision,
) -> Result<Value, PortfolioApplicationServiceError> {
    if binding.account_id() != revision.account.account_id() {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    Ok(json!({
        "accountToken": binding.token(),
        "displayName": binding.display_name(),
        "currency": revision.account.currency().as_str(),
        "holdings": revision.holdings.len(),
        "dataIssues": revision.discrepancies.len(),
    }))
}

fn revision_summary(revision: &PublishedRevision) -> Value {
    json!({
        "snapshotToken": snapshot_token(revision),
        "effectiveAtUnixNanos": revision.effective_at.unix_nanos().to_string(),
        "availableAtUnixNanos": revision
            .available_at
            .map(|value| value.unix_nanos().to_string()),
        "holdingCount": revision.holdings.len(),
        "transactionCount": revision.transactions.len(),
        "dataIssueCount": revision.discrepancies.len(),
        "dataState": if revision.discrepancies.is_empty() { "ready" } else { "needs_review" },
    })
}

fn read_result_limits(
    request: &TypedToolRequest,
    application_limits: PortfolioApplicationLimits,
) -> Result<(usize, usize), PortfolioApplicationServiceError> {
    let limits = request
        .arguments()
        .get("resultLimits")
        .and_then(Value::as_object)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    let maximum_items = limits
        .get("maximumItems")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
        .min(application_limits.max_result_items);
    let maximum_bytes = limits
        .get("maximumBytes")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
        .min(application_limits.max_retained_bytes);
    Ok((maximum_items, maximum_bytes))
}

fn portfolio_page(
    rows: Vec<Value>,
    maximum_items: usize,
    maximum_bytes: usize,
    context: &RequestContext,
    coverage: Value,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let available = rows.len();
    let upper = available
        .min(maximum_items)
        .min(context.limits().maximum_result_items());
    let quality = json!({"state": "available", "confidence": "limited"});
    let limits = narrowed_limits(context, maximum_items, maximum_bytes)?;
    let mut count = upper;
    loop {
        let metadata = if count < available {
            ToolResultMetadata::try_truncated(available, coverage.clone(), quality.clone())
        } else {
            ToolResultMetadata::try_complete(coverage.clone(), quality.clone())
        }
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        match TypedToolResult::try_new(
            Value::Array(rows[..count].to_vec()),
            count,
            metadata,
            limits,
        ) {
            Ok(result) => return Ok(result),
            Err(_) if count > 0 => count -= 1,
            Err(_) => return Err(PortfolioApplicationServiceError::ResourceExhausted),
        }
    }
}

pub(super) fn select_revision<'image>(
    image: &'image PortfolioReadImage,
    scope: &ReadScope,
) -> Result<&'image PublishedRevision, PortfolioApplicationServiceError> {
    let history = image
        .accounts
        .get(&scope.account_id)
        .ok_or(PortfolioApplicationServiceError::NotFound)?;
    let revision = history
        .revisions
        .iter()
        .rev()
        .find(|revision| {
            scope.end.is_none_or(|end| {
                revision.effective_at <= end
                    && revision
                        .available_at
                        .is_some_and(|available| available <= end)
            })
        })
        .ok_or(PortfolioApplicationServiceError::NotFound)?;
    if history
        .revisions
        .last()
        .is_some_and(|head| head.token() == revision.token())
        && image
            .revisions
            .head(scope.account_id)
            .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?
            != revision.token()
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    Ok(revision)
}

fn holdings(
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let rows = revision
        .holdings
        .iter()
        .filter(|holding| scope.admits_instrument(holding.instrument_id()))
        .map(|holding| {
            Ok(json!({
                "accountId": holding.account_id().to_string(),
                "snapshotToken": snapshot_token(revision),
                "instrumentId": holding.instrument_id().to_string(),
                "currency": holding.currency().as_str(),
                "quantity": holding.quantity().to_string(),
                "lotSize": holding.lot_size().as_decimal().to_string(),
                "marketValue": money_value(holding.market_value()),
                "asOfUnixNanos": holding.as_of().unix_nanos().to_string(),
                "costBasis": basis_value(holding.basis()),
                "price": source_mark_details(holding.as_of().unix_nanos().to_string()),
            }))
        })
        .collect::<Result<Vec<_>, PortfolioApplicationServiceError>>()?;
    bounded_rows(rows, revision, scope, context)
}

fn transactions(
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let rows = revision
        .transactions
        .iter()
        .filter(|transaction| {
            transaction
                .instrument_id()
                .is_none_or(|instrument| scope.admits_instrument(instrument))
                && scope.admits_time(transaction.occurred_at())
        })
        .map(|transaction| {
            Ok(json!({
                "transactionToken": transaction_token(transaction),
                "accountId": transaction.account_id().to_string(),
                "snapshotToken": snapshot_token(revision),
                "instrumentId": transaction.instrument_id().map(|value| value.to_string()),
                "category": transaction_kind(transaction.kind()),
                "amount": money_value(transaction.amount()),
                "quantity": transaction.quantity().map(|value| value.to_string()),
                "occurredAtUnixNanos": transaction.occurred_at().unix_nanos().to_string(),
                "lotMethod": transaction.lot_method().map(lot_method),
            }))
        })
        .collect::<Result<Vec<_>, PortfolioApplicationServiceError>>()?;
    bounded_rows(rows, revision, scope, context)
}

pub(super) fn bounded_rows(
    rows: Vec<Value>,
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let available = rows.len();
    if available == 0 {
        return data_result(Value::Array(Vec::new()), 0, 0, revision, scope, context);
    }
    let upper = available
        .min(scope.maximum_items)
        .min(context.limits().maximum_result_items());
    if upper == 0 {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    let mut low = 0_usize;
    let mut high = upper;
    let mut selected = None;
    while low <= high {
        let count = low.saturating_add(high.saturating_sub(low) / 2);
        if count == 0 {
            low = 1;
            continue;
        }
        match data_result(
            Value::Array(rows[..count].to_vec()),
            count,
            available,
            revision,
            scope,
            context,
        ) {
            Ok(result) => {
                selected = Some(result);
                low = count.saturating_add(1);
            }
            Err(PortfolioApplicationServiceError::ResourceExhausted) => {
                if count == 0 {
                    break;
                }
                high = count - 1;
            }
            Err(error) => return Err(error),
        }
    }
    selected.ok_or(PortfolioApplicationServiceError::ResourceExhausted)
}

pub(super) fn report_result(
    mut value: Value,
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    project_report(&mut value);
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "snapshotToken".to_owned(),
            Value::String(snapshot_token(revision)),
        );
    }
    data_result(value, 1, 1, revision, scope, context)
}

pub(super) fn product_report_result(
    value: Value,
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    data_result(value, 1, 1, revision, scope, context)
}

pub(super) fn mutation_result(
    value: Value,
    context: &RequestContext,
    requested_maximum_bytes: usize,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let limits = narrowed_limits(context, 1, requested_maximum_bytes)?;
    TypedToolResult::try_new(
        value,
        1,
        ToolResultMetadata::complete_not_applicable(),
        limits,
    )
    .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)
}

fn data_result(
    value: Value,
    returned: usize,
    available: usize,
    revision: &PublishedRevision,
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let coverage = json!({"scope": "portfolio"});
    let quality = json!({
        "state": if revision.discrepancies.is_empty() { "available" } else { "needs_review" },
        "confidence": "limited",
        "dataIssueCount": revision.discrepancies.len(),
    });
    let metadata = if returned < available {
        ToolResultMetadata::try_truncated(available, coverage, quality)
    } else {
        ToolResultMetadata::try_complete(coverage, quality)
    }
    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let limits = narrowed_limits(context, scope.maximum_items, scope.maximum_bytes)?;
    TypedToolResult::try_new(value, returned, metadata, limits)
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)
}

fn narrowed_limits(
    context: &RequestContext,
    maximum_items: usize,
    maximum_bytes: usize,
) -> Result<ServiceLimits, PortfolioApplicationServiceError> {
    let current = context.limits();
    let maximum_result_items = current.maximum_result_items().min(maximum_items);
    let maximum_result_bytes = current.maximum_result_bytes().min(maximum_bytes);
    if maximum_result_items == 0 || maximum_result_bytes == 0 {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    ServiceLimits::try_new(
        current.maximum_inline_bytes().min(maximum_result_bytes),
        current.maximum_inline_items().min(maximum_result_items),
        maximum_result_bytes,
        maximum_result_items,
        current.result_structure(),
    )
    .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)
}

fn parse_time_range(
    range: &Map<String, Value>,
) -> Result<(Option<Timestamp>, Option<Timestamp>), PortfolioApplicationServiceError> {
    let start = range
        .get("start")
        .and_then(Value::as_str)
        .map(parse_timestamp)
        .transpose()?;
    let end = range
        .get("end")
        .and_then(Value::as_str)
        .map(parse_timestamp)
        .transpose()?;
    if start.is_none() || end.is_none() || start >= end {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    Ok((start, end))
}

fn parse_timestamp(value: &str) -> Result<Timestamp, PortfolioApplicationServiceError> {
    let timestamp = DateTime::parse_from_rfc3339(value)
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?
        .with_timezone(&Utc)
        .timestamp_nanos_opt()
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    Ok(Timestamp::from_unix_nanos(timestamp))
}

/// Makes the limits of a portfolio-import holding mark explicit.
///
/// A holding value is an exact source observation. The portfolio importer has no authority to
/// promote it to a live, delayed, stale, modeled, or venue-qualified market mark, and it has no
/// alternate mark authority to select as a fallback. Keeping those facts adjacent to every value
/// prevents presentation code from inferring a stronger mark state from the source record alone.
pub(super) fn source_mark_details(observed_at_unix_nanos: String) -> Value {
    json!({
        "asOfUnixNanos": observed_at_unix_nanos,
        "state": "reported",
        "confidence": "limited",
        "explanation": "This value came from the imported portfolio and has not been refreshed against the current market.",
    })
}

pub(super) fn snapshot_token(revision: &PublishedRevision) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!(
            "market-squawk/portfolio-snapshot/v1/{}",
            hex(&revision.token().bytes())
        )
        .as_bytes(),
    )
    .to_string()
}

fn transaction_token(
    transaction: &market_squawk_adapter_portfolio::PortfolioTransaction,
) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!(
            "market-squawk/portfolio-transaction/v1/{}/{}",
            transaction.account_id(),
            transaction.broker_transaction_id().as_str()
        )
        .as_bytes(),
    )
    .to_string()
}

fn basis_value(basis: &BasisResolution) -> Value {
    match basis {
        BasisResolution::Resolved { observation } => json!({
            "state": "available",
            "amount": money_value(observation.amount()),
            "method": lot_method(observation.lot_method()),
        }),
        BasisResolution::Missing => json!({"state": "not_available"}),
        BasisResolution::Ambiguous {
            candidates,
            lot_method: method,
        } => json!({
            "state": "needs_review",
            "choices": candidates.iter().copied().map(money_value).collect::<Vec<_>>(),
            "method": lot_method(*method),
        }),
    }
}

const fn lot_method(method: LotMethod) -> &'static str {
    match method {
        LotMethod::Fifo => "First in, first out",
        LotMethod::Lifo => "Last in, first out",
        LotMethod::SpecificIdentification => "Specific lots",
        LotMethod::AverageCost => "Average cost",
    }
}

const fn transaction_kind(kind: TransactionKind) -> &'static str {
    match kind {
        TransactionKind::Trade => "trade",
        TransactionKind::CashTransfer => "cash_transfer",
        TransactionKind::Income => "income",
        TransactionKind::Fee => "fee",
        TransactionKind::CorporateAction => "corporate_action",
    }
}

fn money_value(value: Money) -> Value {
    json!({
        "amount": value.amount().to_string(),
        "currency": value.currency().as_str(),
    })
}

fn project_report(value: &mut Value) {
    match value {
        Value::Array(values) => values.iter_mut().for_each(project_report),
        Value::Object(object) => {
            for forbidden in [
                "revisionId",
                "policy",
                "sourceId",
                "sourceCoverage",
                "sourceReference",
                "source_reference",
                "artifactSha256",
                "analyticsEvidenceDigest",
                "markEvidence",
                "authority",
                "reason",
            ] {
                object.remove(forbidden);
            }
            for child in object.values_mut() {
                project_report(child);
            }
            if object.contains_key("accountId") && object.contains_key("effectiveAtUnixNanos") {
                object.insert(
                    "dataConfidence".to_owned(),
                    Value::String("limited".to_owned()),
                );
            }
        }
        Value::String(state) => {
            let product = match state.as_str() {
                "source_reported_snapshot" => Some("available"),
                "calculated_from_source_reported_mark_and_resolved_basis" => Some("available"),
                "not_calculable_incomplete_source_basis"
                | "requires_committed_trade_lifecycle_interpretation"
                | "benchmark_not_supplied" => Some("not_available"),
                "source_classified_pending_explicit_subtype" => Some("partial"),
                "source_classified" => Some("available"),
                "no_retained_discrepancies" => Some("clear"),
                "discrepancies_require_review" => Some("needs_review"),
                "not_supplied_by_portfolio_source" => Some("not_available"),
                "task12_exact_exposure_v1" | "task12_historical_risk_v1" | "modified_dietz_v1" => {
                    None
                }
                _ => None,
            };
            if let Some(product) = product {
                *state = product.to_owned();
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::source_mark_details;

    #[test]
    fn ordinary_mark_is_plain_and_contains_no_source_authority() {
        let mark = source_mark_details("1700000000000000000".to_owned());
        let encoded = serde_json::to_string(&mark).expect("serialize product mark");

        assert_eq!(mark["asOfUnixNanos"], "1700000000000000000");
        assert_eq!(mark["state"], "reported");
        assert_eq!(mark["confidence"], "limited");
        for forbidden in [
            "source", "provider", "artifact", "revision", "policy", "venue",
        ] {
            assert!(!encoded.to_ascii_lowercase().contains(forbidden));
        }
    }
}
