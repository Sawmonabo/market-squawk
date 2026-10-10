//! Pinned saved-version pages and exact changes in reported portfolio market value.

use std::cmp::Ordering;

use market_squawk_data::MarketDataInstrumentReadCapability;
use market_squawk_domain::{Currency, InstrumentId, Money};
use market_squawk_services::{
    RequestContext, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use super::advanced::required_string;
use super::import::hex;
use super::model::{HoldingObservation, PortfolioReadImage, PublishedRevision};
use super::read::{
    ReadScope, check_context, money_value, narrowed_limits, revision_summary, select_revision,
    snapshot_token,
};
use super::{PortfolioApplicationLimits, PortfolioApplicationServiceError};

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HistoryCursor {
    version: u8,
    scope: String,
    selected: String,
    baseline: Option<String>,
    after: Option<String>,
}

impl HistoryCursor {
    fn encode(&self) -> Result<String, PortfolioApplicationServiceError> {
        let encoded = serde_json::to_string(self)
            .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        if encoded.len() > 512 {
            return Err(PortfolioApplicationServiceError::ResourceExhausted);
        }
        Ok(encoded)
    }
}

pub(super) fn call(
    image: &PortfolioReadImage,
    request: &TypedToolRequest,
    context: &RequestContext,
    limits: PortfolioApplicationLimits,
    instruments: Option<&MarketDataInstrumentReadCapability>,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    check_context(context)?;
    if request.arguments().contains_key("accountId") {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    let scope = ReadScope::from_product_request(image, request, limits)?;
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
    let scope_digest = scope_digest(&scope, request.name(), context)?;
    let cursor = request
        .arguments()
        .get("cursor")
        .filter(|value| !value.is_null())
        .map(|value| {
            let encoded = value
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= 512)
                .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
            let cursor: HistoryCursor = serde_json::from_str(encoded)
                .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
            if cursor.version != 1 || cursor.scope != scope_digest {
                return Err(PortfolioApplicationServiceError::InvalidRequest);
            }
            Ok(cursor)
        })
        .transpose()?;
    let history = &image
        .accounts
        .get(&scope.account_id)
        .ok_or(PortfolioApplicationServiceError::NotFound)?
        .revisions;
    let selected_token = match request.name() {
        "Portfolio.ListRevisions" => match &cursor {
            Some(cursor) => cursor.selected.clone(),
            None => snapshot_token(select_revision(image, &scope)?),
        },
        "Portfolio.GetAttribution" => {
            required_string(request.arguments(), "selectedSnapshotToken")?.to_owned()
        }
        _ => return Err(PortfolioApplicationServiceError::InvalidRequest),
    };
    let selected_index = find_revision(history, &selected_token, context)?;
    let selected = &history[selected_index];
    if selected.account.account_id() != scope.account_id {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    if !admits_revision(&scope, selected) {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    if selected_index + 1 == history.len()
        && image
            .revisions
            .head(scope.account_id)
            .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?
            != selected.token()
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    let baseline_token = (request.name() == "Portfolio.GetAttribution")
        .then(|| required_string(request.arguments(), "baselineSnapshotToken").map(str::to_owned))
        .transpose()?;
    let cursor = match cursor {
        Some(cursor) if cursor.selected == selected_token && cursor.baseline == baseline_token => {
            cursor
        }
        Some(_) => return Err(PortfolioApplicationServiceError::InvalidRequest),
        None => HistoryCursor {
            version: 1,
            scope: scope_digest,
            selected: selected_token,
            baseline: baseline_token.clone(),
            after: None,
        },
    };
    let mut page = HistoryPage::new(cursor, limit)?;
    if let Some(baseline_token) = baseline_token {
        let baseline_index = find_revision(history, &baseline_token, context)?;
        let baseline = &history[baseline_index];
        if baseline_index >= selected_index
            || baseline.account.account_id() != scope.account_id
            || baseline.effective_at > selected.effective_at
            || baseline.available_at.is_none()
            || selected.available_at.is_none()
            || baseline.available_at > selected.available_at
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let output = comparison(baseline, selected, &scope, &mut page, context, instruments)?;
        page.finish(
            "contributions",
            output,
            !selected.discrepancies.is_empty() || !baseline.discrepancies.is_empty(),
            &scope,
            context,
        )
    } else {
        let end = page
            .cursor
            .after
            .as_ref()
            .map(|token| {
                let index = find_revision(&history[..=selected_index], token, context)?;
                if !admits_revision(&scope, &history[index]) {
                    return Err(PortfolioApplicationServiceError::InvalidRequest);
                }
                Ok(index)
            })
            .transpose()?
            .unwrap_or(selected_index + 1);
        for revision in history[..end].iter().rev() {
            check_context(context)?;
            if !admits_revision(&scope, revision) {
                continue;
            }
            if page.rows.len() == limit {
                page.has_more = true;
                break;
            }
            page.rows
                .push((snapshot_token(revision), revision_summary(revision)));
        }
        page.finish(
            "revisions",
            json!({"selectedSnapshotToken":snapshot_token(selected)}),
            !selected.discrepancies.is_empty(),
            &scope,
            context,
        )
    }
}

fn find_revision(
    history: &[PublishedRevision],
    token: &str,
    context: &RequestContext,
) -> Result<usize, PortfolioApplicationServiceError> {
    for (index, revision) in history.iter().enumerate().rev() {
        check_context(context)?;
        if snapshot_token(revision) == token {
            return Ok(index);
        }
    }
    Err(PortfolioApplicationServiceError::InvalidRequest)
}

fn admits_revision(scope: &ReadScope, revision: &PublishedRevision) -> bool {
    scope.admits_time(revision.effective_at)
        && scope.end.is_none_or(|end| {
            revision
                .available_at
                .is_some_and(|available| available <= end)
        })
}

struct HistoryPage {
    cursor: HistoryCursor,
    limit: usize,
    rows: Vec<(String, Value)>,
    has_more: bool,
}

impl HistoryPage {
    fn new(cursor: HistoryCursor, limit: usize) -> Result<Self, PortfolioApplicationServiceError> {
        let mut rows = Vec::new();
        rows.try_reserve_exact(limit)
            .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
        Ok(Self {
            cursor,
            limit,
            rows,
            has_more: false,
        })
    }

    fn finish(
        mut self,
        row_field: &str,
        mut output: Value,
        needs_review: bool,
        scope: &ReadScope,
        context: &RequestContext,
    ) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
        let limits = narrowed_limits(context, scope.maximum_items, scope.maximum_bytes)?;
        let metadata = ToolResultMetadata::try_complete(
            json!({"scope":"portfolio_history","snapshotToken":self.cursor.selected}),
            json!({
                "state":if needs_review { "needs_review" } else { "available" },
                "confidence":"limited",
            }),
        )
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        output["pageCursor"] = Value::String(self.cursor.encode()?);
        loop {
            check_context(context)?;
            let count = self.rows.len();
            output["nextCursor"] = if self.has_more {
                let last = self
                    .rows
                    .last()
                    .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
                let mut next = self.cursor.clone();
                next.after = Some(last.0.clone());
                Value::String(next.encode()?)
            } else {
                Value::Null
            };
            output[row_field] =
                Value::Array(self.rows.iter().map(|(_, row)| row.clone()).collect());
            match TypedToolResult::try_new(output.clone(), count, metadata.clone(), limits) {
                Ok(result) => {
                    check_context(context)?;
                    return Ok(result);
                }
                Err(_) if count > 1 => {
                    self.rows.pop();
                    self.has_more = true;
                }
                Err(_) => return Err(PortfolioApplicationServiceError::ResourceExhausted),
            }
        }
    }
}

fn comparison(
    baseline: &PublishedRevision,
    selected: &PublishedRevision,
    scope: &ReadScope,
    page: &mut HistoryPage,
    context: &RequestContext,
    instruments: Option<&MarketDataInstrumentReadCapability>,
) -> Result<Value, PortfolioApplicationServiceError> {
    let currency = selected.account.currency();
    if baseline.account.currency() != currency {
        return Err(PortfolioApplicationServiceError::Analytics);
    }
    let (total, ids) = value_changes(
        &baseline.holdings,
        &selected.holdings,
        currency,
        scope,
        page,
        context,
    )?;
    let mut displays = super::instrument_display::resolve(
        instruments,
        &ids,
        selected.effective_at,
        selected.available_at,
        context,
    )?;
    for (instrument, (_, row)) in ids.into_iter().zip(&mut page.rows) {
        if let Some(display) = displays.remove(&instrument) {
            row["investment"] = display;
        }
    }
    Ok(json!({
        "snapshotToken":snapshot_token(selected),
        "baselineSnapshotToken":snapshot_token(baseline),
        "effectiveAtUnixNanos":selected.effective_at.unix_nanos().to_string(),
        "availableAtUnixNanos":selected.available_at.map(|time| time.unix_nanos().to_string()),
        "baselineEffectiveAtUnixNanos":baseline.effective_at.unix_nanos().to_string(),
        "baselineAvailableAtUnixNanos":baseline.available_at.map(|time| time.unix_nanos().to_string()),
        "total":money_value(total),
        "explanation":"Change in reported market value before cash-flow and corporate-action adjustments. This is not investment performance.",
    }))
}

fn value_changes(
    baseline: &[HoldingObservation],
    selected: &[HoldingObservation],
    currency: Currency,
    scope: &ReadScope,
    page: &mut HistoryPage,
    context: &RequestContext,
) -> Result<(Money, Vec<InstrumentId>), PortfolioApplicationServiceError> {
    let after = page
        .cursor
        .after
        .as_deref()
        .map(|value| {
            value
                .parse::<InstrumentId>()
                .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)
        })
        .transpose()?;
    let mut cursor_seen = after.is_none();
    let zero = Money::new(Decimal::ZERO, currency);
    let mut total = zero;
    let mut opening = baseline.iter().peekable();
    let mut closing = selected.iter().peekable();
    let mut ids = Vec::new();
    ids.try_reserve_exact(page.limit)
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    // Both producers publish unique holdings in instrument order. Merge the union without
    // allocating an account-sized map or dropping investments opened or closed in the interval.
    while opening.peek().is_some() || closing.peek().is_some() {
        check_context(context)?;
        let order = match (opening.peek(), closing.peek()) {
            (Some(left), Some(right)) => left.instrument_id().cmp(&right.instrument_id()),
            (Some(_), None) => Ordering::Less,
            _ => Ordering::Greater,
        };
        let left = (order != Ordering::Greater)
            .then(|| opening.next())
            .flatten();
        let right = (order != Ordering::Less).then(|| closing.next()).flatten();
        let instrument = left
            .or(right)
            .ok_or(PortfolioApplicationServiceError::CorruptPublication)?
            .instrument_id();
        if !scope.admits_instrument(instrument) {
            continue;
        }
        for holding in [left, right].into_iter().flatten() {
            if holding.account_id() != scope.account_id {
                return Err(PortfolioApplicationServiceError::CorruptPublication);
            }
            if holding.currency() != currency || holding.market_value().currency() != currency {
                return Err(PortfolioApplicationServiceError::Analytics);
            }
        }
        let opening_value = left.map_or(zero, HoldingObservation::market_value);
        let closing_value = right.map_or(zero, HoldingObservation::market_value);
        let amount = closing_value
            .checked_sub(opening_value)
            .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
        total = total
            .checked_add(amount)
            .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
        if after == Some(instrument) {
            cursor_seen = true;
        }
        if after.is_some_and(|after| instrument <= after) {
            continue;
        }
        if page.rows.len() == page.limit {
            page.has_more = true;
            continue;
        }
        ids.push(instrument);
        page.rows.push((
            instrument.to_string(),
            json!({
                "instrumentId":instrument.to_string(),
                "opening":money_value(opening_value),
                "closing":money_value(closing_value),
                "amount":money_value(amount),
                "investment":{"name":null,"symbol":null},
            }),
        ));
    }
    if !cursor_seen {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    Ok((total, ids))
}

fn scope_digest(
    scope: &ReadScope,
    operation: &str,
    context: &RequestContext,
) -> Result<String, PortfolioApplicationServiceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-history-page/v1\0");
    digest.update(operation.as_bytes());
    digest.update(scope.account_id.as_uuid().as_bytes());
    for time in [scope.start, scope.end] {
        match time {
            Some(time) => {
                digest.update([1]);
                digest.update(time.unix_nanos().to_be_bytes());
            }
            None => digest.update([0]),
        }
    }
    for instrument in &scope.instruments {
        check_context(context)?;
        digest.update(instrument.as_uuid().as_bytes());
    }
    Ok(hex(&digest.finalize().into()))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use market_squawk_domain::{AccountId, LotSize, SourceIdentifier, Timestamp};
    use market_squawk_services::{JsonStructureLimits, RequestId, ServiceLimits};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::*;
    use crate::portfolio_application::model::{BasisResolution, SignedQuantity};

    #[test]
    fn comparison_preserves_union_signed_zero_and_exact_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        let currency = Currency::try_from("USD")?;
        let account = AccountId::try_from(Uuid::from_u128(1))?;
        let holding = |id, amount| -> Result<_, Box<dyn std::error::Error>> {
            Ok(HoldingObservation {
                account_id: account,
                instrument_id: InstrumentId::try_from(Uuid::from_u128(id))?,
                currency,
                quantity: SignedQuantity(Decimal::ONE),
                lot_size: LotSize::try_from_decimal(Decimal::ONE)?,
                market_value: Money::new(amount, currency),
                as_of: Timestamp::from_unix_nanos(1),
                basis: BasisResolution::Missing,
                source_reference: SourceIdentifier::try_from("history-test")?,
            })
        };
        let baseline = [(1, 3), (2, 7), (3, -5), (4, 0)]
            .into_iter()
            .map(|(id, amount)| holding(id, Decimal::from(amount)))
            .collect::<Result<Vec<_>, _>>()?;
        let selected = [(1, 4), (3, -2), (4, 0), (5, 9)]
            .into_iter()
            .map(|(id, amount)| holding(id, Decimal::from(amount)))
            .collect::<Result<Vec<_>, _>>()?;
        let scope = ReadScope {
            account_id: account,
            instruments: Default::default(),
            start: None,
            end: None,
            maximum_items: 25,
            maximum_bytes: 16_384,
        };
        let context = RequestContext::new(
            RequestId::Integer(1),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
            ServiceLimits::try_new(
                16_384,
                25,
                16_384,
                25,
                JsonStructureLimits::try_new(16, 1024, 128, 128)?,
            )?,
        );
        let cursor = HistoryCursor {
            version: 1,
            scope: String::new(),
            selected: String::new(),
            baseline: None,
            after: None,
        };
        let mut page = HistoryPage::new(cursor.clone(), 25)?;
        let (total, ids) =
            value_changes(&baseline, &selected, currency, &scope, &mut page, &context)?;
        assert_eq!(total, Money::new(Decimal::from(6), currency));
        assert_eq!(ids.len(), 5);
        assert_eq!(
            page.rows
                .iter()
                .map(|(_, row)| row["amount"]["amount"].clone())
                .collect::<Vec<_>>(),
            vec![json!("1"), json!("-7"), json!("3"), json!("0"), json!("9")]
        );
        assert_eq!(page.rows[1].1["closing"]["amount"], "0");
        assert_eq!(page.rows[4].1["opening"]["amount"], "0");

        // Both an unrepresentable individual difference and an overflowing aggregate must fail.
        let mut overflow_page = HistoryPage::new(cursor.clone(), 25)?;
        assert!(matches!(
            value_changes(
                &[holding(1, Decimal::NEGATIVE_ONE)?],
                &[holding(1, Decimal::MAX)?],
                currency,
                &scope,
                &mut overflow_page,
                &context,
            ),
            Err(PortfolioApplicationServiceError::Analytics)
        ));
        let mut overflow_page = HistoryPage::new(cursor, 25)?;
        assert!(matches!(
            value_changes(
                &[],
                &[holding(1, Decimal::MAX)?, holding(2, Decimal::ONE)?],
                currency,
                &scope,
                &mut overflow_page,
                &context,
            ),
            Err(PortfolioApplicationServiceError::Analytics)
        ));
        Ok(())
    }
}
