//! Bounded recorded activity pages pinned to one immutable account publication.

use market_squawk_data::MarketDataInstrumentReadCapability;
use market_squawk_services::{RequestContext, TypedToolRequest, TypedToolResult};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::import::hex;
use super::model::{PortfolioReadImage, PortfolioTransaction, PublishedRevision};
use super::read::{
    ReadScope, check_context, lot_method, money_value, snapshot_token, transaction_kind,
    transaction_token,
};
use super::snapshot_page::{self, PageOutput, SnapshotPage};
use super::{PortfolioApplicationLimits, PortfolioApplicationServiceError};

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TransactionsCursor {
    version: u8,
    revision: String,
    scope: String,
    after_transaction_index: Option<usize>,
}

pub(super) fn call(
    image: &PortfolioReadImage,
    request: &TypedToolRequest,
    context: &RequestContext,
    application_limits: PortfolioApplicationLimits,
    instruments: Option<&MarketDataInstrumentReadCapability>,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    let cursor: Option<TransactionsCursor> = snapshot_page::read_cursor(request, context)?;
    let selection = SnapshotPage::admit(
        image,
        request,
        context,
        application_limits,
        b"market-squawk/portfolio-transactions-page/v1\0",
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
    let after_index = cursor
        .as_ref()
        .and_then(|cursor| cursor.after_transaction_index);
    if let Some(index) = after_index {
        // The ordinal identifies a row only inside this exact pinned revision and scope.
        revision
            .transactions
            .get(index)
            .filter(|transaction| {
                transaction.account_id() == scope.account_id
                    && admits_transaction(scope, transaction)
            })
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        check_context(context)?;
    }
    let page_cursor = encode_cursor(revision, &selection.scope_digest, after_index)?;
    let page = page_rows(revision, scope, after_index, selection.limit, context)?;
    let count = page.len().min(selection.limit);
    let mut ids = Vec::new();
    ids.try_reserve_exact(count)
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    ids.extend(
        page[..count]
            .iter()
            .filter_map(|(_, row)| row.instrument_id()),
    );
    ids.sort_unstable();
    ids.dedup();
    let displays = super::instrument_display::resolve(
        instruments,
        &ids,
        revision.effective_at,
        revision.available_at,
        context,
    )?;
    let snapshot = snapshot_token(revision);
    let mut rows = Vec::new();
    rows.try_reserve_exact(count)
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    for (_, transaction) in &page[..count] {
        check_context(context)?;
        let investment = transaction
            .instrument_id()
            .map_or(Value::Null, |instrument| {
                displays
                    .get(&instrument)
                    .cloned()
                    .unwrap_or_else(|| json!({"name":null,"symbol":null}))
            });
        rows.push(json!({
            "transactionToken": transaction_token(transaction),
            "accountId": transaction.account_id().to_string(),
            "snapshotToken": snapshot,
            "instrumentId": transaction.instrument_id().map(|value| value.to_string()),
            "category": transaction_kind(transaction.kind()),
            "amount": money_value(transaction.amount()),
            "quantity": transaction.quantity().map(|value| value.to_string()),
            "occurredAtUnixNanos": transaction.occurred_at().unix_nanos().to_string(),
            "lotMethod": transaction.lot_method().map(lot_method),
            "investment": investment,
        }));
    }
    selection.finish(
        context,
        PageOutput {
            row_field: "transactions",
            rows,
            available: page.len(),
            page_cursor,
            extra: None,
        },
        |index| encode_cursor(revision, &selection.scope_digest, Some(page[index].0)),
    )
}

fn encode_cursor(
    revision: &PublishedRevision,
    scope: &str,
    after_transaction_index: Option<usize>,
) -> Result<String, PortfolioApplicationServiceError> {
    snapshot_page::encode_cursor(&TransactionsCursor {
        version: 1,
        revision: hex(&revision.token().bytes()),
        scope: scope.to_owned(),
        after_transaction_index,
    })
}

fn admits_transaction(scope: &ReadScope, transaction: &PortfolioTransaction) -> bool {
    // Cash-only activity remains included when an investment filter is supplied, matching the
    // original read. The time range includes both endpoints.
    transaction
        .instrument_id()
        .is_none_or(|instrument| scope.admits_instrument(instrument))
        && scope.admits_time(transaction.occurred_at())
}

fn page_rows<'revision>(
    revision: &'revision PublishedRevision,
    scope: &ReadScope,
    after_index: Option<usize>,
    limit: usize,
    context: &RequestContext,
) -> Result<Vec<(usize, &'revision PortfolioTransaction)>, PortfolioApplicationServiceError> {
    let mut rows = Vec::new();
    rows.try_reserve_exact(limit + 1)
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    // Both merge_transactions and build_native_revision sort by occurred_at then broker ID.
    // Restart rebuilds through those same producers. A pinned ordinal therefore seeks directly
    // without exposing a broker identity or scanning previously emitted activity.
    let start = after_index.map_or_else(
        || {
            scope.start.map_or(0, |start| {
                revision
                    .transactions
                    .partition_point(|transaction| transaction.occurred_at() < start)
            })
        },
        |index| index + 1,
    );
    for (index, transaction) in revision.transactions.iter().enumerate().skip(start) {
        check_context(context)?;
        if transaction.account_id() != scope.account_id {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        if scope.end.is_some_and(|end| transaction.occurred_at() > end) {
            break;
        }
        if !admits_transaction(scope, transaction) {
            continue;
        }
        rows.push((index, transaction));
        if rows.len() > limit {
            break;
        }
    }
    Ok(rows)
}
