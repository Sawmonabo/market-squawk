//! Selected-account position pages and exposure bound to one immutable publication.

use std::ops::Bound::{Excluded, Unbounded};

use market_squawk_data::MarketDataInstrumentReadCapability;
use market_squawk_domain::InstrumentId;
use market_squawk_services::{RequestContext, TypedToolRequest, TypedToolResult};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::import::hex;
use super::model::{HoldingObservation, PortfolioReadImage, PublishedRevision};
use super::read::{
    ReadScope, basis_value, check_context, money_value, snapshot_token, source_mark_details,
};
use super::snapshot_page::{self, PageOutput, SnapshotPage};
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
    let cursor: Option<HoldingsCursor> = snapshot_page::read_cursor(request, context)?;
    let selection = SnapshotPage::admit(
        image,
        request,
        context,
        application_limits,
        b"market-squawk/portfolio-holdings-page/v1\0",
        cursor.as_ref().map(|cursor| {
            (
                cursor.version,
                cursor.revision.as_str(),
                cursor.scope.as_str(),
            )
        }),
    )?;
    let revision = selection.revision;
    let scope = &selection.scope;
    let limit = selection.limit;
    let scope_digest = &selection.scope_digest;
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
        scope_digest,
        cursor.as_ref().and_then(|value| value.after_instrument_id),
    )?;
    let page = page_rows(revision, scope, after_index, limit, context)?;
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
    let exposure = (request.name() == "Portfolio.GetExposure")
        .then(|| super::analytics::exposure_summary(revision, scope, context))
        .transpose()?;
    selection.finish(
        context,
        PageOutput {
            row_field: "holdings",
            rows,
            available: page.len(),
            page_cursor,
            extra: exposure.map(|value| ("exposure", value)),
        },
        |index| encode_cursor(revision, scope_digest, Some(page[index].instrument_id())),
    )
}

fn encode_cursor(
    revision: &PublishedRevision,
    scope: &str,
    after_instrument_id: Option<InstrumentId>,
) -> Result<String, PortfolioApplicationServiceError> {
    snapshot_page::encode_cursor(&HoldingsCursor {
        version: 1,
        revision: hex(&revision.token().bytes()),
        scope: scope.to_owned(),
        after_instrument_id,
    })
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
