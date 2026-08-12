//! Bounded point-in-time portfolio request admission and result construction.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use market_squawk_domain::{AccountId, InstrumentId, Timestamp};
use market_squawk_services::{
    RequestContext, ServiceLimits, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde_json::{Map, Value, json};

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
    pub(super) sources: BTreeSet<String>,
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
        let sources = request
            .arguments()
            .get("sourceCoverage")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .map(ToOwned::to_owned)
                            .ok_or(PortfolioApplicationServiceError::InvalidRequest)
                    })
                    .collect()
            })
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            account_id,
            instruments,
            start,
            end,
            maximum_items,
            maximum_bytes,
            sources,
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
    let scope = ReadScope::from_request(request, limits)?;
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
    let requested_sources = source_filters(request)?;
    let after_account = request
        .arguments()
        .get("afterAccountId")
        .and_then(Value::as_str)
        .map(str::parse::<AccountId>)
        .transpose()
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
    let rows = image
        .accounts
        .iter()
        .filter(|(account_id, _)| after_account.is_none_or(|after| **account_id > after))
        .filter_map(|(_, history)| history.revisions.last())
        .filter(|revision| {
            requested_sources.is_empty()
                || requested_sources.iter().all(|source| {
                    revision
                        .source_coverage
                        .iter()
                        .any(|covered| covered.as_str() == source)
                })
        })
        .map(account_summary)
        .collect::<Vec<_>>();
    portfolio_page(
        rows,
        maximum_items,
        maximum_bytes,
        context,
        json!({
            "authority": "immutable_portfolio_publication",
            "sourceFilters": requested_sources,
        }),
    )
}

fn list_revisions(
    image: &PortfolioReadImage,
    request: &TypedToolRequest,
    context: &RequestContext,
    application_limits: PortfolioApplicationLimits,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let scope = ReadScope::from_request(request, application_limits)?;
    let after_revision = request
        .arguments()
        .get("afterRevisionId")
        .and_then(Value::as_str);
    let history = image
        .accounts
        .get(&scope.account_id)
        .ok_or(PortfolioApplicationServiceError::NotFound)?;
    let mut cursor_seen = after_revision.is_none();
    let mut rows = Vec::new();
    for revision in &history.revisions {
        let revision_id = hex(&revision.token().bytes());
        if !cursor_seen {
            if after_revision == Some(revision_id.as_str()) {
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
            || (!scope.sources.is_empty()
                && !scope.sources.iter().all(|source| {
                    revision
                        .source_coverage
                        .iter()
                        .any(|covered| covered.as_str() == source)
                }))
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
        json!({
            "authority": "append_only_portfolio_revision_history",
            "accountId": scope.account_id.to_string(),
            "sourceFilters": scope.sources,
        }),
    )
}

fn account_summary(revision: &PublishedRevision) -> Value {
    json!({
        "accountId": revision.account.account_id().to_string(),
        "currency": revision.account.currency().as_str(),
        "currentRevision": revision_summary(revision),
        "holdingCount": revision.holdings.len(),
        "transactionCount": revision.transactions.len(),
        "reconciliationDiscrepancies": revision.discrepancies.len(),
    })
}

fn revision_summary(revision: &PublishedRevision) -> Value {
    json!({
        "revisionId": hex(&revision.token().bytes()),
        "effectiveAtUnixNanos": revision.effective_at.unix_nanos().to_string(),
        "availableAtUnixNanos": revision
            .available_at
            .map(|value| value.unix_nanos().to_string()),
        "sourceId": revision.source_id.as_str(),
        "sourceCoverage": revision
            .source_coverage
            .iter()
            .map(|source| source.as_str())
            .collect::<Vec<_>>(),
        "artifactSha256": hex(&revision.artifact_sha256),
        "holdingCount": revision.holdings.len(),
        "transactionCount": revision.transactions.len(),
        "reconciliationDiscrepancies": revision.discrepancies.len(),
    })
}

fn source_filters(
    request: &TypedToolRequest,
) -> Result<BTreeSet<String>, PortfolioApplicationServiceError> {
    request
        .arguments()
        .get("sourceCoverage")
        .map(|values| {
            values
                .as_array()
                .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(ToOwned::to_owned)
                        .ok_or(PortfolioApplicationServiceError::InvalidRequest)
                })
                .collect()
        })
        .transpose()
        .map(Option::unwrap_or_default)
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
    let quality = json!({
        "class": "direct_unverified",
        "executionEligible": false,
        "rawEvidenceRetained": true,
    });
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
            if count == 0 {
                Value::Null
            } else {
                Value::Array(rows[..count].to_vec())
            },
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
            }) && (scope.sources.is_empty()
                || scope.sources.iter().all(|requested| {
                    revision
                        .source_coverage
                        .iter()
                        .any(|source| source.as_str() == requested)
                }))
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
            let mut value = serde_json::to_value(holding)
                .map_err(|_| PortfolioApplicationServiceError::Publication)?;
            enrich_row(&mut value, revision)?;
            let object = value
                .as_object_mut()
                .ok_or(PortfolioApplicationServiceError::Publication)?;
            object.insert(
                "markEvidence".to_owned(),
                source_mark_details(
                    holding.source_reference().as_str(),
                    holding.as_of().unix_nanos().to_string(),
                ),
            );
            Ok(value)
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
            let mut value = serde_json::to_value(transaction)
                .map_err(|_| PortfolioApplicationServiceError::Publication)?;
            enrich_row(&mut value, revision)?;
            Ok(value)
        })
        .collect::<Result<Vec<_>, PortfolioApplicationServiceError>>()?;
    bounded_rows(rows, revision, scope, context)
}

fn enrich_row(
    value: &mut Value,
    revision: &PublishedRevision,
) -> Result<(), PortfolioApplicationServiceError> {
    let object = value
        .as_object_mut()
        .ok_or(PortfolioApplicationServiceError::Publication)?;
    object.insert(
        "revisionId".to_owned(),
        Value::String(hex(&revision.token().bytes())),
    );
    object.insert(
        "effectiveAtUnixNanos".to_owned(),
        Value::String(revision.effective_at.unix_nanos().to_string()),
    );
    object.insert(
        "availableAtUnixNanos".to_owned(),
        revision.available_at.map_or(Value::Null, |timestamp| {
            Value::String(timestamp.unix_nanos().to_string())
        }),
    );
    object.insert(
        "sourceId".to_owned(),
        Value::String(revision.source_id.as_str().to_owned()),
    );
    object.insert(
        "artifactSha256".to_owned(),
        Value::String(hex(&revision.artifact_sha256)),
    );
    Ok(())
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
    let coverage = source_coverage(revision);
    let quality = json!({
        "class": "direct_unverified",
        "executionEligible": false,
        "reconciliationDiscrepancies": revision.discrepancies.len(),
        "rawEvidenceRetained": true
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

fn source_coverage(revision: &PublishedRevision) -> Value {
    json!({
        "accountId": revision.account.account_id().to_string(),
        "revisionId": hex(&revision.token().bytes()),
        "sources": revision
            .source_coverage
            .iter()
            .map(|source| source.as_str())
            .collect::<Vec<_>>(),
        "effectiveAtUnixNanos": revision.effective_at.unix_nanos().to_string(),
        "availableAtUnixNanos": revision
            .available_at
            .map(|timestamp| timestamp.unix_nanos().to_string()),
        "artifactSha256": hex(&revision.artifact_sha256)
    })
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
pub(super) fn source_mark_details(source_reference: &str, observed_at_unix_nanos: String) -> Value {
    json!({
        "sourceReference": source_reference,
        "observedAtUnixNanos": observed_at_unix_nanos,
        "venue": Value::Null,
        "venueStatus": "not_supplied_by_portfolio_source",
        "state": "source_reported",
        "quality": "direct_unverified",
        "executionEligible": false,
        "freshness": {
            "status": "not_evaluated_no_market_policy",
            "reason": "portfolio_import_does_not_authorize_market_freshness"
        },
        "fallback": {
            "status": "not_applicable_no_alternate_mark_authority",
            "reason": "no_live_delayed_stale_or_modeled_mark_was_selected"
        }
    })
}

#[cfg(test)]
mod tests {
    use super::source_mark_details;

    #[test]
    fn source_reported_mark_never_claims_an_unevidenced_freshness_or_fallback() {
        let mark = source_mark_details("raw-holding-42", "1700000000000000000".to_owned());

        assert_eq!(mark["sourceReference"], "raw-holding-42");
        assert_eq!(mark["observedAtUnixNanos"], "1700000000000000000");
        assert_eq!(mark["state"], "source_reported");
        assert_eq!(
            mark["freshness"]["status"],
            "not_evaluated_no_market_policy"
        );
        assert_eq!(
            mark["fallback"]["status"],
            "not_applicable_no_alternate_mark_authority"
        );
    }
}
