//! Selected-account position pages and exposure bound to one immutable publication.

use std::ops::Bound::{Excluded, Unbounded};

use market_squawk_data::MarketDataInstrumentReadCapability;
use market_squawk_domain::InstrumentId;
use market_squawk_services::{
    RequestContext, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest as _, Sha256};

use super::import::hex;
use super::model::{HoldingObservation, PortfolioReadImage, PublishedRevision};
use super::read::{
    ReadScope, basis_value, check_context, money_value, narrowed_limits, select_revision,
    snapshot_token, source_mark_details,
};
use super::{PortfolioApplicationLimits, PortfolioApplicationServiceError};

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HoldingsCursor {
    version: u8,
    revision: String,
    scope: String,
    after_instrument_id: Option<InstrumentId>,
}

pub(super) fn call(
    image: &PortfolioReadImage,
    request: &TypedToolRequest,
    context: &RequestContext,
    application_limits: PortfolioApplicationLimits,
    instruments: Option<&MarketDataInstrumentReadCapability>,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    check_context(context)?;
    if request.arguments().contains_key("accountId") {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    let scope = ReadScope::from_product_request(image, request, application_limits)?;
    let limit = request
        .arguments()
        .get("limit")
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| (1..=100).contains(value))
                .ok_or(PortfolioApplicationServiceError::InvalidRequest)
        })
        .transpose()?
        .unwrap_or(25)
        .min(scope.maximum_items)
        .min(context.limits().maximum_result_items());
    if limit == 0 {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    let scope_digest = scope_digest(&scope, context)?;
    let cursor = request
        .arguments()
        .get("cursor")
        .filter(|value| !value.is_null())
        .map(|value| {
            let encoded = value
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= 512)
                .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
            let cursor: HoldingsCursor = serde_json::from_str(encoded)
                .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
            if cursor.version != 1 || cursor.scope != scope_digest {
                return Err(PortfolioApplicationServiceError::InvalidRequest);
            }
            Ok(cursor)
        })
        .transpose()?;
    let revision = match &cursor {
        Some(cursor) => pinned_revision(image, &scope, cursor, context)?,
        None => select_revision(image, &scope)?,
    };
    if revision.account.account_id() != scope.account_id {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    let after_index = cursor
        .as_ref()
        .and_then(|cursor| cursor.after_instrument_id)
        .map(|instrument| {
            if !scope.admits_instrument(instrument) {
                return Err(PortfolioApplicationServiceError::InvalidRequest);
            }
            revision
                .holdings
                .binary_search_by_key(&instrument, HoldingObservation::instrument_id)
                .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)
        })
        .transpose()?;
    let page_cursor = encode_cursor(
        revision,
        &scope_digest,
        cursor.as_ref().and_then(|value| value.after_instrument_id),
    )?;
    let page = page_rows(revision, &scope, after_index, limit, context)?;
    let count = page.len().min(limit);
    let mut ids = Vec::new();
    ids.try_reserve_exact(count)
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    ids.extend(page[..count].iter().map(|holding| holding.instrument_id()));
    let mut displays = super::instrument_display::resolve(
        instruments,
        &ids,
        revision.effective_at,
        revision.available_at,
        context,
    )?;
    check_context(context)?;
    let snapshot = snapshot_token(revision);
    let mut rows = Vec::new();
    rows.try_reserve_exact(count)
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    for holding in &page[..count] {
        check_context(context)?;
        if holding.account_id() != scope.account_id {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        let investment = displays
            .remove(&holding.instrument_id())
            .unwrap_or_else(|| json!({"name":null,"symbol":null}));
        rows.push(json!({
            "accountId": holding.account_id().to_string(),
            "snapshotToken": snapshot,
            "instrumentId": holding.instrument_id().to_string(),
            "currency": holding.currency().as_str(),
            "quantity": holding.quantity().to_string(),
            "lotSize": holding.lot_size().as_decimal().to_string(),
            "marketValue": money_value(holding.market_value()),
            "asOfUnixNanos": holding.as_of().unix_nanos().to_string(),
            "costBasis": basis_value(holding.basis()),
            "price": source_mark_details(holding.as_of().unix_nanos().to_string()),
            "investment": investment,
        }));
    }
    let limits = narrowed_limits(context, scope.maximum_items, scope.maximum_bytes)?;
    let metadata = ToolResultMetadata::try_complete(
        json!({
            "scope":"portfolio",
            "snapshotToken":snapshot,
            "effectiveAtUnixNanos":revision.effective_at.unix_nanos().to_string(),
            "availableAtUnixNanos":revision.available_at.map(|time| time.unix_nanos().to_string()),
        }),
        json!({
            "state":if revision.discrepancies.is_empty() { "available" } else { "needs_review" },
            "confidence":"limited",
            "dataIssueCount":revision.discrepancies.len(),
        }),
    )
    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let exposure = (request.name() == "Portfolio.GetExposure")
        .then(|| super::analytics::exposure_summary(revision, &scope, context))
        .transpose()?;
    loop {
        check_context(context)?;
        let count = rows.len();
        let next_cursor = if count < page.len() {
            let last = page
                .get(
                    count
                        .checked_sub(1)
                        .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?,
                )
                .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
            Some(encode_cursor(
                revision,
                &scope_digest,
                Some(last.instrument_id()),
            )?)
        } else {
            None
        };
        let mut output = json!({
            "holdings":rows,
            "pageCursor":page_cursor,
            "nextCursor":next_cursor,
            "snapshotToken":snapshot,
            "effectiveAtUnixNanos":revision.effective_at.unix_nanos().to_string(),
            "availableAtUnixNanos":revision.available_at.map(|time| time.unix_nanos().to_string()),
        });
        if let Some(exposure) = &exposure {
            output["exposure"] = exposure.clone();
        }
        let result = TypedToolResult::try_new(output, count, metadata.clone(), limits);
        match result {
            Ok(result) => {
                check_context(context)?;
                return Ok(result);
            }
            Err(_) if count > 1 => {
                rows.pop();
            }
            Err(_) => return Err(PortfolioApplicationServiceError::ResourceExhausted),
        }
    }
}

fn encode_cursor(
    revision: &PublishedRevision,
    scope: &str,
    after_instrument_id: Option<InstrumentId>,
) -> Result<String, PortfolioApplicationServiceError> {
    let encoded = serde_json::to_string(&HoldingsCursor {
        version: 1,
        revision: hex(&revision.token().bytes()),
        scope: scope.to_owned(),
        after_instrument_id,
    })
    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    if encoded.len() > 512 {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    Ok(encoded)
}

fn pinned_revision<'image>(
    image: &'image PortfolioReadImage,
    scope: &ReadScope,
    cursor: &HoldingsCursor,
    context: &RequestContext,
) -> Result<&'image PublishedRevision, PortfolioApplicationServiceError> {
    let history = image
        .accounts
        .get(&scope.account_id)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    for revision in &history.revisions {
        check_context(context)?;
        if hex(&revision.token().bytes()) != cursor.revision {
            continue;
        }
        if scope.end.is_some_and(|end| {
            revision.effective_at > end
                || revision
                    .available_at
                    .is_none_or(|available| available > end)
        }) {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
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
        return Ok(revision);
    }
    Err(PortfolioApplicationServiceError::InvalidRequest)
}

fn page_rows<'revision>(
    revision: &'revision PublishedRevision,
    scope: &ReadScope,
    after_index: Option<usize>,
    limit: usize,
    context: &RequestContext,
) -> Result<Vec<&'revision HoldingObservation>, PortfolioApplicationServiceError> {
    let mut rows = Vec::new();
    rows.try_reserve_exact(limit + 1)
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    // Imports sort merge_holdings by instrument ID; native publications preserve the core's
    // ordered, unique BTreeSet of instrument IDs. Both producers therefore support indexed seek.
    if scope.instruments.is_empty() {
        let start = after_index.map_or(0, |index| index + 1);
        for holding in revision.holdings.iter().skip(start).take(limit + 1) {
            check_context(context)?;
            rows.push(holding);
        }
    } else {
        let after = after_index.map_or(Unbounded, |index| {
            Excluded(revision.holdings[index].instrument_id())
        });
        for instrument in scope.instruments.range((after, Unbounded)) {
            check_context(context)?;
            if let Ok(index) = revision
                .holdings
                .binary_search_by_key(instrument, HoldingObservation::instrument_id)
            {
                rows.push(&revision.holdings[index]);
                if rows.len() > limit {
                    break;
                }
            }
        }
    }
    Ok(rows)
}

fn scope_digest(
    scope: &ReadScope,
    context: &RequestContext,
) -> Result<String, PortfolioApplicationServiceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-holdings-page/v1\0");
    digest.update(scope.account_id.as_uuid().as_bytes());
    digest.update(
        u64::try_from(scope.instruments.len())
            .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?
            .to_be_bytes(),
    );
    for instrument in &scope.instruments {
        check_context(context)?;
        digest.update(instrument.as_uuid().as_bytes());
    }
    for time in [scope.start, scope.end] {
        match time {
            Some(time) => {
                digest.update([1]);
                digest.update(time.unix_nanos().to_be_bytes());
            }
            None => digest.update([0]),
        }
    }
    Ok(hex(&digest.finalize().into()))
}
