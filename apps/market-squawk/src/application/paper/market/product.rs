//! Closed provider-neutral identities for ordinary Market product reads.

use chrono::DateTime;
use market_squawk_domain::{Currency, InstrumentId, Timestamp};
use market_squawk_services::{ServiceError, ServiceLimits, ToolResultMetadata, TypedToolResult};
use rust_decimal::Decimal;
use serde_json::{Value, json};

const PAGE_TOKEN_DOMAIN: &[u8] = b"market-squawk/market-page/v1\0";
pub(super) const MAXIMUM_PRODUCT_MARKET_ROWS: usize = 100;

use crate::application::market_selection::product::{ProductMarketIdentity, resolve_token, token};
pub(super) use crate::application::market_selection::product::{
    product_market_identities, resolve_selection_token,
};

pub(super) fn resolve_history_token(
    identities: &[ProductMarketIdentity],
    token: &str,
) -> Result<InstrumentId, ServiceError> {
    resolve_token(identities, token, |identity| identity.history_token())
}

pub(super) struct ProductPageSelection {
    instrument_ids: Vec<InstrumentId>,
    has_more: bool,
    next_page_token: Option<Box<str>>,
    available: usize,
}

impl ProductPageSelection {
    pub(super) fn instrument_ids(&self) -> &[InstrumentId] {
        &self.instrument_ids
    }

    pub(super) const fn has_more(&self) -> bool {
        self.has_more
    }

    pub(super) const fn available(&self) -> usize {
        self.available
    }
}

/// Resolves filtering and continuation against the complete canonical population before reads.
pub(super) fn select_product_page(
    identities: &[ProductMarketIdentity],
    query: Option<&str>,
    maximum_rows: usize,
    after: Option<&str>,
) -> Result<ProductPageSelection, ServiceError> {
    if maximum_rows == 0 || maximum_rows > MAXIMUM_PRODUCT_MARKET_ROWS {
        return Err(ServiceError::InvalidRequest);
    }
    let mut page_tokens = Vec::new();
    page_tokens
        .try_reserve_exact(identities.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    let query = query.unwrap_or_default();
    if query.chars().count() > 64 || query.chars().any(char::is_control) {
        return Err(ServiceError::InvalidRequest);
    }
    let mut visible = Vec::new();
    visible
        .try_reserve_exact(identities.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for identity in identities {
        if query.is_empty() || identity.matches_search_query() {
            visible.push(identity);
        }
    }
    for identity in &visible {
        let token = page_token(identity, query)?;
        if page_tokens
            .iter()
            .any(|(existing, _): &(Box<str>, &str)| existing == &token)
        {
            return Err(ServiceError::InvalidResult);
        }
        page_tokens.push((token, identity.selection_token()));
    }
    let start = match after {
        None => 0,
        Some(token) => {
            let selection_token = page_tokens
                .iter()
                .find_map(|(candidate, selection)| {
                    (candidate.as_ref() == token).then_some(*selection)
                })
                .ok_or(ServiceError::Unavailable)?;
            visible
                .iter()
                .position(|identity| identity.selection_token() == selection_token)
                .and_then(|index| index.checked_add(1))
                .ok_or(ServiceError::InvalidResult)?
        }
    };
    let end = start.saturating_add(maximum_rows).min(visible.len());
    let selected = visible.get(start..end).ok_or(ServiceError::InvalidResult)?;
    let mut instrument_ids = Vec::new();
    instrument_ids
        .try_reserve_exact(selected.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for identity in selected {
        instrument_ids.push(identity.instrument_id());
    }
    let has_more = end < visible.len();
    let next_page_token = if has_more {
        selected
            .last()
            .map(|identity| page_token(identity, query))
            .transpose()?
    } else {
        None
    };
    Ok(ProductPageSelection {
        instrument_ids,
        has_more,
        next_page_token,
        available: visible
            .len()
            .checked_sub(start)
            .ok_or(ServiceError::InvalidResult)?,
    })
}

pub(super) fn project_product_page(
    identities: &[ProductMarketIdentity],
    selection: ProductPageSelection,
    native_rows: &[Value],
) -> Result<Value, ServiceError> {
    let mut rows = Vec::new();
    rows.try_reserve_exact(selection.instrument_ids.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for instrument_id in &selection.instrument_ids {
        let identity = identities
            .iter()
            .find(|identity| identity.instrument_id() == *instrument_id)
            .ok_or(ServiceError::InvalidResult)?;
        let native_row = native_rows
            .iter()
            .find(|row| native_instrument_id(row) == Some(*instrument_id))
            .ok_or(ServiceError::Unavailable)?;
        rows.push(product_row(identity, native_row)?);
    }
    Ok(json!({
        "data": rows,
        "page": {
            "hasMore": selection.has_more,
            "nextPageToken": selection.next_page_token,
        },
    }))
}

pub(super) fn product_result(
    content: Value,
    available: usize,
    has_more: bool,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    let count = content
        .get("data")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    if count > available || has_more != (available > count) {
        return Err(ServiceError::InvalidResult);
    }
    let metadata = if has_more {
        ToolResultMetadata::try_truncated_not_applicable(available)
            .map_err(|_error| ServiceError::InvalidResult)?
    } else {
        ToolResultMetadata::complete_not_applicable()
    };
    TypedToolResult::try_new(content, count, metadata, limits)
        .map_err(|_error| ServiceError::ResourceExhausted)
}

pub(super) fn product_search_page(
    identities: &[ProductMarketIdentity],
    query: &str,
    maximum_rows: usize,
    after: Option<&str>,
) -> Result<(Value, usize, bool), ServiceError> {
    let query = query.trim();
    if query.is_empty()
        || query.chars().count() > 64
        || query.chars().any(char::is_control)
        || maximum_rows == 0
        || maximum_rows > MAXIMUM_PRODUCT_MARKET_ROWS
    {
        return Err(ServiceError::InvalidRequest);
    }
    let selection = select_product_page(identities, Some(query), maximum_rows, after)?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(selection.instrument_ids().len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for instrument_id in selection.instrument_ids() {
        let identity = identities
            .iter()
            .find(|identity| identity.instrument_id() == *instrument_id)
            .ok_or(ServiceError::InvalidResult)?;
        rows.push(json!({
            "selectionToken": identity.selection_token(),
            "symbol": identity.symbol(),
            "name": identity.name(),
            "kind": product_kind(identity.asset_class()),
        }));
    }
    let available = selection.available();
    let has_more = selection.has_more();
    Ok((
        json!({"data": rows, "page": {"hasMore": has_more, "nextPageToken": selection.next_page_token}}),
        available,
        has_more,
    ))
}

pub(super) fn product_row(
    identity: &ProductMarketIdentity,
    native_row: &Value,
) -> Result<Value, ServiceError> {
    let row = native_row.as_object().ok_or(ServiceError::InvalidResult)?;
    if native_instrument_id(native_row) != Some(identity.instrument_id()) {
        return Err(ServiceError::InvalidResult);
    }
    let current_price = row.get("currentPrice").and_then(Value::as_object);
    let price = current_price
        .map(|price| -> Result<Value, ServiceError> {
            let value = exact_decimal_text(price, "value")?;
            let currency = currency_text(price, "currency")?;
            Ok(json!({
                "value": value,
                "currency": currency,
            }))
        })
        .transpose()?;
    let as_of = current_price
        .and_then(|price| price.get("observedAt").filter(|value| !value.is_null()))
        .map(|value| canonical_time(value).map(Value::String))
        .transpose()?;
    if price.is_some() != as_of.is_some() {
        return Err(ServiceError::Unavailable);
    }
    let availability = match row.get("availability").and_then(Value::as_str) {
        Some("live") => "current",
        Some("delayed") => "delayed",
        Some("last_known") => "last_known",
        Some("end_of_day" | "stored") => "previous_close",
        Some("stale" | "unavailable") => "unavailable",
        _ => return Err(ServiceError::InvalidResult),
    };
    let availability = if price.is_none() {
        "unavailable"
    } else {
        availability
    };
    let price_basis = current_price
        .map(|price| match exact_text(price, "basis")? {
            basis @ ("last_trade" | "bid_ask_midpoint" | "previous_close") => Ok(basis),
            _ => Err(ServiceError::InvalidResult),
        })
        .transpose()?;
    let price_current_through = current_price
        .and_then(|price| price.get("currentThrough").filter(|value| !value.is_null()))
        .map(canonical_time)
        .transpose()?;
    let quote = product_quote(row)?;
    let change = product_price_change(row, current_price, availability)?;
    Ok(json!({
        "selectionToken": identity.selection_token(),
        "historyToken": identity.history_token(),
        "identity": {
            "symbol": identity.symbol(),
            "name": identity.name(),
            "assetClass": identity.asset_class(),
        },
        "price": price,
        "priceBasis": price_basis,
        "priceCurrentThrough": price_current_through,
        "quote": quote,
        "changePercent": change.percent,
        "changeBasis": change.basis,
        "changeUnavailableReason": change.unavailable_reason,
        "asOf": as_of,
        "availability": availability,
    }))
}

struct ProductPriceChange {
    percent: Option<String>,
    basis: Value,
    unavailable_reason: Option<&'static str>,
}

/// Selects presentation evidence only. Expired observations retain their original clocks
/// without gaining current-mark authority or displacing a newer completed close.
pub(super) fn retained_display_price(
    native_row: &Value,
    completed_close: Option<&Value>,
    selected_at: Timestamp,
) -> Result<Option<Value>, ServiceError> {
    let row = native_row.as_object().ok_or(ServiceError::InvalidResult)?;
    let quote = product_quote(row)?;
    let Some(quote) = quote.as_object() else {
        return Ok(None);
    };
    let close_at = completed_close
        .map(|close| canonical_time(&close["currentPrice"]["observedAt"]))
        .transpose()?
        .map(|at| DateTime::parse_from_rfc3339(&at).map_err(|_| ServiceError::InvalidResult))
        .transpose()?;
    for (basis, value_field, time_field) in [
        ("last_trade", "lastPrice", "lastObservedAt"),
        ("bid_ask_midpoint", "midPrice", "quoteObservedAt"),
    ] {
        if basis == "last_trade" && exact_text(quote, "tradeStatus")? != "available" {
            continue;
        }
        let Some(value) = quote.get(value_field).filter(|value| !value.is_null()) else {
            continue;
        };
        let Some(observed_at) = quote.get(time_field).filter(|value| !value.is_null()) else {
            continue;
        };
        let observed_at = canonical_time(observed_at)?;
        let observed =
            DateTime::parse_from_rfc3339(&observed_at).map_err(|_| ServiceError::InvalidResult)?;
        let observed_nanos = observed
            .timestamp_nanos_opt()
            .ok_or(ServiceError::InvalidResult)?;
        let amount = exact_decimal_text(quote, value_field)?
            .parse::<Decimal>()
            .map_err(|_| ServiceError::InvalidResult)?;
        if amount <= Decimal::ZERO
            || observed_nanos > selected_at.unix_nanos()
            || close_at.is_some_and(|close| observed <= close)
        {
            continue;
        }
        return Ok(Some(json!({
            "value": value,
            "currency": currency_text(quote, "currency")?,
            "basis": basis,
            "observedAt": observed_at,
            "currentThrough": Value::Null,
        })));
    }
    Ok(None)
}

impl ProductPriceChange {
    fn unavailable(reason: &'static str) -> Self {
        Self {
            percent: None,
            basis: Value::Null,
            unavailable_reason: Some(reason),
        }
    }
}

/// Display quotes and trades carry original, unadjusted economics. The sole baseline
/// producer is the admitted raw completed-session reader, never a provider snapshot bar.
fn product_price_change(
    row: &serde_json::Map<String, Value>,
    current_price: Option<&serde_json::Map<String, Value>>,
    availability: &str,
) -> Result<ProductPriceChange, ServiceError> {
    let Some(price) = current_price.filter(|_| {
        matches!(
            availability,
            "current" | "delayed" | "last_known" | "previous_close"
        )
    }) else {
        return Ok(ProductPriceChange::unavailable("current_price_unavailable"));
    };
    let price_basis = exact_text(price, "basis")?;
    if price_basis == "previous_close" {
        if availability != "previous_close" {
            return Ok(ProductPriceChange::unavailable("current_price_unavailable"));
        }
    } else {
        let (fresh_field, value_field, time_field) = match price_basis {
            "last_trade" => ("lastFresh", "lastPrice", "lastObservedAt"),
            "bid_ask_midpoint" => ("quoteFresh", "midPrice", "quoteObservedAt"),
            _ => return Ok(ProductPriceChange::unavailable("current_price_unavailable")),
        };
        let quote = row.get("quote");
        if availability != "last_known"
            && quote
                .and_then(|quote| quote.get(fresh_field))
                .and_then(Value::as_bool)
                != Some(true)
        {
            return Ok(ProductPriceChange::unavailable("current_price_unavailable"));
        }
        if quote.and_then(|quote| quote.get(value_field)) != price.get("value")
            || quote.and_then(|quote| quote.get(time_field)) != price.get("observedAt")
        {
            return Ok(ProductPriceChange::unavailable("incompatible_basis"));
        }
    }
    let Some(close) = row.get("previousClose").filter(|value| !value.is_null()) else {
        return Ok(ProductPriceChange::unavailable(
            "previous_close_unavailable",
        ));
    };
    let close = close.as_object().ok_or(ServiceError::InvalidResult)?;
    let price_value = exact_decimal_text(price, "value")?
        .parse::<Decimal>()
        .map_err(|_| ServiceError::InvalidResult)?;
    let close_text = exact_decimal_text(close, "value")?;
    let close_value = close_text
        .parse::<Decimal>()
        .map_err(|_| ServiceError::InvalidResult)?;
    let currency = currency_text(price, "currency")?;
    let observed_at = canonical_time(price.get("observedAt").ok_or(ServiceError::InvalidResult)?)?;
    let close_at = canonical_time(close.get("asOf").ok_or(ServiceError::InvalidResult)?)?;
    let observed =
        DateTime::parse_from_rfc3339(&observed_at).map_err(|_| ServiceError::InvalidResult)?;
    let closed =
        DateTime::parse_from_rfc3339(&close_at).map_err(|_| ServiceError::InvalidResult)?;
    let session_date = exact_text(close, "sessionDate")?;
    let session = chrono::NaiveDate::parse_from_str(session_date, "%Y-%m-%d")
        .map_err(|_| ServiceError::InvalidResult)?;
    if session.to_string() != session_date {
        return Err(ServiceError::InvalidResult);
    }
    // The admitted calendar/history producer binds this exact observation to a native
    // session. Never infer that date from UTC or from when the user opened the card.
    let Some(price_session) = row.get("priceSession").and_then(Value::as_object) else {
        return Ok(ProductPriceChange::unavailable("incompatible_basis"));
    };
    let price_session_date = exact_text(price_session, "date")?;
    let price_session_date = chrono::NaiveDate::parse_from_str(price_session_date, "%Y-%m-%d")
        .map_err(|_| ServiceError::InvalidResult)?;
    let start = canonical_time(
        price_session
            .get("startsAt")
            .ok_or(ServiceError::InvalidResult)?,
    )?;
    let end = canonical_time(
        price_session
            .get("endsAt")
            .ok_or(ServiceError::InvalidResult)?,
    )?;
    let start = DateTime::parse_from_rfc3339(&start).map_err(|_| ServiceError::InvalidResult)?;
    let end = DateTime::parse_from_rfc3339(&end).map_err(|_| ServiceError::InvalidResult)?;
    if row.get("instrumentId") != close.get("instrumentId")
        || currency != currency_text(close, "currency")?
        || exact_text(close, "adjustment")? != "raw"
        || price_value <= Decimal::ZERO
        || close_value <= Decimal::ZERO
        || closed >= observed
        || session >= price_session_date
        || price_session.get("observedAt") != price.get("observedAt")
        || price_session.get("value") != price.get("value")
        || price_session.get("basis") != price.get("basis")
        || start >= end
        || (price_basis != "previous_close" && (observed < start || observed >= end))
    {
        return Ok(ProductPriceChange::unavailable("incompatible_basis"));
    }
    let Some(percent) = price_value
        .checked_sub(close_value)
        .and_then(|difference| difference.checked_div(close_value))
        .and_then(|change| change.checked_mul(Decimal::from(100_u8)))
    else {
        return Ok(ProductPriceChange::unavailable("arithmetic_unavailable"));
    };
    Ok(ProductPriceChange {
        percent: Some(percent.normalize().to_string()),
        basis: json!({
            "priceBasis": price_basis,
            "priceAsOf": observed_at,
            "previousClose": {"value": close_text, "currency": currency,
                "sessionDate": session_date, "asOf": close_at},
            "adjustment": "raw",
        }),
        unavailable_reason: None,
    })
}

/// Carries independently timed quote and trade evidence through the closed product boundary.
fn product_quote(row: &serde_json::Map<String, Value>) -> Result<Value, ServiceError> {
    let Some(native) = row.get("quote").filter(|value| !value.is_null()) else {
        return Ok(Value::Null);
    };
    let native = native.as_object().ok_or(ServiceError::InvalidResult)?;
    let mut quote = serde_json::Map::new();
    quote.insert("currency".into(), json!(currency_text(row, "currency")?));
    let size_basis = exact_text(native, "quoteSizeBasis")?;
    if !matches!(size_basis, "quantity" | "source_units") {
        return Err(ServiceError::InvalidResult);
    }
    quote.insert("quoteSizeBasis".into(), json!(size_basis));
    for field in [
        "bidPrice",
        "bidSize",
        "askPrice",
        "askSize",
        "midPrice",
        "lastPrice",
        "lastSize",
    ] {
        let value = native.get(field).ok_or(ServiceError::InvalidResult)?;
        quote.insert(
            field.into(),
            if value.is_null() {
                Value::Null
            } else {
                json!(exact_decimal_text(native, field)?)
            },
        );
    }
    for field in [
        "quoteObservedAt",
        "lastObservedAt",
        "quoteCurrentThrough",
        "lastCurrentThrough",
    ] {
        let value = native.get(field).ok_or(ServiceError::InvalidResult)?;
        quote.insert(
            field.into(),
            if value.is_null() {
                Value::Null
            } else {
                json!(canonical_time(value)?)
            },
        );
    }
    for field in ["quoteFresh", "lastFresh"] {
        quote.insert(
            field.into(),
            json!(
                native
                    .get(field)
                    .and_then(Value::as_bool)
                    .ok_or(ServiceError::InvalidResult)?
            ),
        );
    }
    let status = exact_text(native, "tradeStatus")?;
    if !matches!(status, "available" | "ambiguous" | "unavailable")
        || (status != "available"
            && (!native["lastPrice"].is_null()
                || !native["lastSize"].is_null()
                || native["lastFresh"] == true))
    {
        return Err(ServiceError::InvalidResult);
    }
    quote.insert("tradeStatus".into(), json!(status));
    Ok(Value::Object(quote))
}

fn page_token(last: &ProductMarketIdentity, query: &str) -> Result<Box<str>, ServiceError> {
    token(
        "page_",
        PAGE_TOKEN_DOMAIN,
        &[
            last.population_binding(),
            query.as_bytes(),
            last.selection_token().as_bytes(),
        ],
    )
}

fn product_kind(asset_class: &str) -> &'static str {
    match asset_class {
        "equity" => "stock",
        "fixed_income" => "bond",
        "option" => "option",
        "future" => "future",
        "foreign_exchange" => "currency",
        "crypto" => "crypto",
        "commodity" => "commodity",
        "fund" => "fund",
        "index" => "index",
        "cash" => "cash",
        _ => "cash",
    }
}

fn exact_text<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a str, ServiceError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidResult)
}

fn exact_decimal_text<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a str, ServiceError> {
    let value = exact_text(object, field)?;
    let decimal = value
        .parse::<Decimal>()
        .map_err(|_error| ServiceError::InvalidResult)?;
    if decimal.is_zero() && value.starts_with('-') || decimal.normalize().to_string() != value {
        return Err(ServiceError::InvalidResult);
    }
    Ok(value)
}

fn currency_text<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a str, ServiceError> {
    let value = exact_text(object, field)?;
    let parsed = Currency::try_from(value).map_err(|_error| ServiceError::InvalidResult)?;
    if parsed.as_str() != value || !value.bytes().all(|byte| byte.is_ascii_uppercase()) {
        return Err(ServiceError::InvalidResult);
    }
    Ok(value)
}

fn canonical_time(value: &Value) -> Result<String, ServiceError> {
    let value = value.as_str().ok_or(ServiceError::InvalidResult)?;
    let parsed =
        DateTime::parse_from_rfc3339(value).map_err(|_error| ServiceError::InvalidResult)?;
    let nanos = parsed
        .timestamp_nanos_opt()
        .ok_or(ServiceError::InvalidResult)?;
    let canonical = super::serialization::timestamp_value(
        market_squawk_domain::Timestamp::from_unix_nanos(nanos),
    );
    let canonical = canonical.as_str();
    if canonical != value {
        return Err(ServiceError::InvalidResult);
    }
    Ok(canonical.to_owned())
}

fn native_instrument_id(value: &Value) -> Option<InstrumentId> {
    value.get("instrumentId")?.as_str()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_squawk_data::{
        CatalogAuthority, CatalogConfig, CatalogLimit, CatalogResultLimits,
        MarketDataInstrumentReadCapability, MarketDataInstrumentSynchronization,
        MarketDataInstrumentSynchronizationCapability,
    };
    use market_squawk_domain::{
        AssetClass, AssignmentVerification, AuthorizationBasis, CanonicalStateDigest,
        CanonicalizationRule, ConnectionGeneration, CoverageStatus, DataQuality,
        DecodedLiveProvenanceInput, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
        ExactPayloadEvidence, ExternalIdentifier, ExternalIdentifierRecord,
        ExternalIdentifierRecordInput, IdentifierEntitlement, IdentifierRightsPolicyReference,
        LiveEventClass, LiveEvidenceBinding, LiveProvenance, MarketDataDisplayName,
        MarketDataInstrumentDefinition, MarketDataInstrumentDefinitionInput, MarketDataReference,
        MetadataRevision, PayloadReference, ProviderChannel, ProviderInstrumentId, ProviderProduct,
        RevisionBoundPayloadEvidence, RuleVersion, SourceId, SourceIdentifier, Ticker, Timestamp,
        VenueId, VenueMapping, VenueSymbol,
    };
    use market_squawk_platform::LocalPaths;
    use std::{
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };
    use tokio_util::sync::CancellationToken;

    #[test]
    fn canonical_ticker_search_preserves_selection_and_population_bound_continuation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path().join("catalog"))?;
        let authority = Arc::new(Mutex::new(CatalogAuthority::open(CatalogConfig::try_new(
            paths.catalog()?.clone(),
            Duration::from_millis(750),
            CatalogLimit::new(32)?,
            CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
        )?)?));
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(2);
        let evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [1; 32],
        ));
        let name_source = SourceId::try_from("official-listing")?;
        let name_rights = IdentifierRightsPolicyReference::new(
            SourceIdentifier::try_from("local-use-v1")?,
            IdentifierEntitlement::LicensedInternalUse,
            SourceIdentifier::try_from("https://example.test/listing")?,
        );
        let mut definitions = Vec::new();
        for (id, symbol, name) in [
            (
                "00000000-0000-0000-0000-000000000101",
                "SPY",
                Some("State Street SPDR S&P 500 ETF Trust"),
            ),
            (
                "00000000-0000-0000-0000-000000000102",
                "VTI",
                Some("Vanguard Total Stock Market ETF"),
            ),
            ("00000000-0000-0000-0000-000000000103", "BTC/USD", None),
        ] {
            let mut venue_mappings = vec![VenueMapping::new(
                VenueId::try_from("ARCX")?,
                VenueSymbol::try_from(symbol)?,
            )];
            if name.is_none() {
                // Real crypto references can have no name and differing venue symbols.
                venue_mappings.push(VenueMapping::new(
                    VenueId::try_from("XNAS")?,
                    VenueSymbol::try_from("BTC-USD")?,
                ));
            }
            definitions.push(MarketDataInstrumentDefinition::try_new(
                MarketDataInstrumentDefinitionInput {
                    instrument_id: id.parse()?,
                    reference_evidence: RevisionBoundPayloadEvidence::new(
                        MetadataRevision::new(SourceIdentifier::try_from("official-listing-v1")?),
                        evidence.clone(),
                    ),
                    effective_interval: EffectiveInterval::new(
                        Timestamp::from_unix_nanos(100),
                        None,
                    )?,
                    asset_class: if name.is_some() {
                        AssetClass::Fund
                    } else {
                        AssetClass::Crypto
                    },
                    display_name: name
                        .map(|name| {
                            MarketDataDisplayName::try_new(
                                name,
                                name_source.clone(),
                                evidence.clone(),
                                name_rights.clone(),
                            )
                        })
                        .transpose()?,
                    quote_currency: Currency::try_from("USD")?,
                    quote_currency_evidence: evidence.clone(),
                    venue_mappings,
                    provider_identities: Vec::new(),
                    identifiers: if symbol == "SPY" {
                        vec![ExternalIdentifierRecord::new(
                            ExternalIdentifierRecordInput {
                                identifier: ExternalIdentifier::Ticker(Ticker::try_from(symbol)?),
                                assignment_verification: AssignmentVerification::VerifiedAssigned,
                                source_id: name_source.clone(),
                                source_evidence: evidence.clone(),
                                source_timestamp: Some(Timestamp::from_unix_nanos(100)),
                                observed_at: Timestamp::from_unix_nanos(100),
                                validity: EffectiveInterval::new(
                                    Timestamp::from_unix_nanos(100),
                                    None,
                                )?,
                                rights_policy: name_rights.clone(),
                            },
                        )]
                    } else {
                        Vec::new()
                    },
                },
            )?);
        }
        MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&authority)).synchronize(
            MarketDataInstrumentSynchronization::try_new(definitions, 3)?,
            deadline,
            &cancellation,
        )?;
        let reader = MarketDataInstrumentReadCapability::new(
            Arc::clone(&authority),
            deadline,
            &cancellation,
        )?;
        let mut records = Vec::new();
        for id in [
            "00000000-0000-0000-0000-000000000101",
            "00000000-0000-0000-0000-000000000102",
            "00000000-0000-0000-0000-000000000103",
        ] {
            records.push(
                reader
                    .latest(id.parse()?, deadline, &cancellation)?
                    .ok_or("missing identity")?,
            );
        }
        let cutoff = records
            .iter()
            .map(|record| record.published_at())
            .max()
            .ok_or("missing cutoff")?;
        // A retained closing quote can predate registration of its exact reference. The
        // shared production validator must still reject that quote as a current financial mark.
        use crate::application::market_selection::{NativeReferenceUse, validate_native_reference};
        let definition = &records[0];
        let reference = MarketDataReference::try_from_assigned_identifier(
            definition.definition(),
            definition.revision_digest(),
            &definition.definition().identifiers()[0],
            ProviderInstrumentId::try_from("SPY")?,
            Timestamp::from_unix_nanos(110),
        )?;
        let provenance = |instrument,
                          source_at,
                          received_at|
         -> Result<LiveProvenance, Box<dyn std::error::Error>> {
            let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]);
            let binding = LiveEvidenceBinding::new(
                SourceId::try_from("retained-quote")?,
                SourceIdentifier::try_from("session-1")?,
                MetadataRevision::new(SourceIdentifier::try_from("revision-1")?),
                AuthorizationBasis::new(SourceIdentifier::try_from("quote-authority")?),
                VenueId::try_from("ARCX")?,
                instrument,
                ConnectionGeneration::new(1)?,
                ProviderProduct::new(SourceIdentifier::try_from("SPY")?),
                ProviderChannel::new(SourceIdentifier::try_from("quotes")?),
                LiveEventClass::Quote,
                SourceIdentifier::try_from("SPY")?,
                digest,
                CanonicalStateDigest::new(
                    digest,
                    CanonicalizationRule::new(
                        SourceIdentifier::try_from("quote-v1")?,
                        RuleVersion::new(1)?,
                    ),
                ),
                None,
            )?;
            Ok(LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
                binding,
                Some(Timestamp::from_unix_nanos(source_at)),
                Timestamp::from_unix_nanos(received_at),
                Timestamp::from_unix_nanos(received_at),
                Timestamp::from_unix_nanos(received_at + 1),
                DataQuality::DirectUnverified,
                CoverageStatus::Sufficient,
                PayloadReference::SourceReference(SourceIdentifier::try_from("frame-1")?),
            ))?)
        };
        let closing_quote = provenance(reference.instrument_id(), 90, 110)?;
        validate_native_reference(
            &reference,
            definition,
            &closing_quote,
            cutoff,
            NativeReferenceUse::RetainedDisplay,
        )?;
        assert_eq!(
            closing_quote.source_timestamp(),
            Some(Timestamp::from_unix_nanos(90))
        );
        assert!(matches!(
            validate_native_reference(
                &reference,
                definition,
                &closing_quote,
                cutoff,
                NativeReferenceUse::CurrentMark
            ),
            Err(ServiceError::InvalidResult)
        ));
        let current_quote = provenance(reference.instrument_id(), 110, 110)?;
        validate_native_reference(
            &reference,
            definition,
            &current_quote,
            cutoff,
            NativeReferenceUse::CurrentMark,
        )?;
        let wrong_digest = MarketDataReference::try_from_assigned_identifier(
            definition.definition(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, [2; 32]),
            &definition.definition().identifiers()[0],
            ProviderInstrumentId::try_from("SPY")?,
            Timestamp::from_unix_nanos(110),
        )?;
        let wrong_instrument = provenance(records[1].definition().instrument_id(), 110, 110)?;
        let early_receipt = provenance(reference.instrument_id(), 90, 99)?;
        for reference_use in [
            NativeReferenceUse::RetainedDisplay,
            NativeReferenceUse::CurrentMark,
        ] {
            for (candidate, observation, knowledge_at) in [
                (&wrong_digest, &current_quote, cutoff),
                (&reference, &wrong_instrument, cutoff),
                (&reference, &early_receipt, cutoff),
                (&reference, &current_quote, Timestamp::from_unix_nanos(110)),
            ] {
                assert!(matches!(
                    validate_native_reference(
                        candidate,
                        definition,
                        observation,
                        knowledge_at,
                        reference_use
                    ),
                    Err(ServiceError::InvalidResult)
                ));
            }
        }

        // Reference enrichment must not erase an original retained quote. Reopen its exact
        // definition, then verify the current assignment independently; current marks stay strict.
        use crate::application::market_selection::validate_retained_native_reference;
        let original = reader
            .read_revision(reference.definition_digest(), deadline, &cancellation)?
            .ok_or("missing original reference")?;
        assert_eq!(&original, definition);
        let mut enriched = serde_json::to_value(definition.definition())?;
        enriched["reference_evidence"]["metadata_revision"] = json!("enriched-listing-v2");
        enriched["effective_interval"]["starts_at"] = json!(120);
        enriched["identifiers"][0]["source_id"] = json!("native-corroboration");
        MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&authority)).synchronize(
            MarketDataInstrumentSynchronization::try_new(
                vec![serde_json::from_value(enriched.clone())?],
                1,
            )?,
            deadline,
            &cancellation,
        )?;
        let enriched_record = reader
            .latest(reference.instrument_id(), deadline, &cancellation)?
            .ok_or("missing enriched reference")?;
        validate_retained_native_reference(
            &reference,
            &original,
            &enriched_record,
            &closing_quote,
            enriched_record.published_at(),
        )?;
        assert!(
            validate_native_reference(
                &reference,
                &enriched_record,
                &current_quote,
                enriched_record.published_at(),
                NativeReferenceUse::CurrentMark,
            )
            .is_err()
        );
        assert!(
            validate_retained_native_reference(
                &wrong_digest,
                &original,
                &enriched_record,
                &current_quote,
                enriched_record.published_at(),
            )
            .is_err()
        );
        assert!(
            validate_retained_native_reference(
                &reference,
                &original,
                &enriched_record,
                &wrong_instrument,
                enriched_record.published_at(),
            )
            .is_err()
        );
        enriched["reference_evidence"]["metadata_revision"] = json!("removed-assignment-v3");
        enriched["effective_interval"]["starts_at"] = json!(130);
        enriched["identifiers"] = json!([]);
        MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&authority)).synchronize(
            MarketDataInstrumentSynchronization::try_new(
                vec![serde_json::from_value(enriched)?],
                1,
            )?,
            deadline,
            &cancellation,
        )?;
        let removed = reader
            .latest(reference.instrument_id(), deadline, &cancellation)?
            .ok_or("missing removed assignment")?;
        assert!(
            validate_retained_native_reference(
                &reference,
                &original,
                &removed,
                &closing_quote,
                removed.published_at(),
            )
            .is_err()
        );
        assert_eq!(
            reader.read_revision(reference.definition_digest(), deadline, &cancellation)?,
            Some(original)
        );

        let crypto = product_market_identities(&records, cutoff, Some("BTC"))?;
        let (crypto_page, count, _) = product_search_page(&crypto, "BTC", 100, None)?;
        assert_eq!(count, 1);
        assert_eq!(
            crypto_page["data"][0]["name"],
            "Investment name unavailable"
        );
        assert!(crypto_page["data"][0]["symbol"].is_null());
        assert_eq!(
            resolve_selection_token(
                &crypto,
                crypto_page["data"][0]["selectionToken"]
                    .as_str()
                    .ok_or("missing crypto token")?
            )?
            .to_string(),
            "00000000-0000-0000-0000-000000000103"
        );
        let ticker = product_market_identities(&records, cutoff, Some("spy"))?;
        let (ticker_page, count, more) = product_search_page(&ticker, "spy", 100, None)?;
        assert_eq!(count, 1);
        assert!(!more);
        assert_eq!(ticker_page["data"][0]["symbol"], "SPY");
        let token = ticker_page["data"][0]["selectionToken"]
            .as_str()
            .ok_or("missing token")?;
        let selected = resolve_selection_token(&ticker, token)?;
        assert_eq!(selected.to_string(), "00000000-0000-0000-0000-000000000101");
        let named = product_market_identities(&records, cutoff, Some("spdr"))?;
        let (name_page, _, _) = product_search_page(&named, "spdr", 100, None)?;
        assert_eq!(name_page["data"][0]["selectionToken"], token);
        assert_eq!(
            resolve_history_token(&named, ticker[0].history_token())?,
            ticker[0].instrument_id()
        );

        let identity = ticker
            .iter()
            .find(|identity| identity.instrument_id() == selected)
            .ok_or("missing selected identity")?;
        let unavailable_page = project_product_page(
            &ticker,
            select_product_page(std::slice::from_ref(identity), None, 1, None)?,
            &[json!({
                "instrumentId": selected.to_string(),
                "currentPrice": null,
                "availability": "unavailable",
            })],
        )?;
        let unavailable = &unavailable_page["data"][0];
        assert_eq!(unavailable["selectionToken"], token);
        assert_eq!(unavailable["historyToken"], identity.history_token());
        assert_eq!(unavailable["identity"]["symbol"], "SPY");
        assert_eq!(unavailable["identity"]["name"], identity.name());
        assert_eq!(unavailable["availability"], "unavailable");
        assert!(unavailable["price"].is_null());
        assert!(unavailable["asOf"].is_null());
        assert_eq!(unavailable_page["page"]["hasMore"], false);

        // The product boundary must preserve exact quote evidence and ambiguity rather than
        // relabeling a midpoint as the last trade or dropping the independent quote.
        let observed = "2026-08-09T14:30:00.000000000Z";
        let through = "2026-08-09T14:30:05.000000000Z";
        let mut quote_row = json!({
            "instrumentId": selected.to_string(), "availability": "live", "currency": "USD",
            "currentPrice": {"value": "68000.15", "currency": "USD", "basis": "bid_ask_midpoint",
                "observedAt": observed, "currentThrough": through},
            "quote": {"quoteSizeBasis": "quantity", "bidPrice": "68000.1", "bidSize": "2", "askPrice": "68000.2", "askSize": "3",
                "midPrice": "68000.15", "lastPrice": null, "lastSize": null,
                "quoteObservedAt": observed, "quoteCurrentThrough": through,
                "lastObservedAt": null, "lastCurrentThrough": null,
                "quoteFresh": true, "lastFresh": false, "tradeStatus": "ambiguous"},
        });
        let projected = product_row(identity, &quote_row)?;
        assert_eq!(projected["priceBasis"], "bid_ask_midpoint");
        assert_eq!(projected["quote"]["tradeStatus"], "ambiguous");
        assert_eq!(projected["quote"]["bidPrice"], "68000.1");
        assert_eq!(projected["quote"]["quoteCurrentThrough"], through);
        assert!(projected["quote"]["lastPrice"].is_null());
        assert!(projected["changePercent"].is_null());
        assert_eq!(
            projected["changeUnavailableReason"],
            "previous_close_unavailable"
        );
        quote_row["previousClose"] = json!({
            "instrumentId": selected.to_string(), "value": "64000", "currency": "USD",
            "sessionDate": "2026-08-08", "asOf": "2026-08-08T20:00:00.000000000Z",
            "adjustment": "raw",
        });
        quote_row["priceSession"] = json!({
            "date": "2026-08-09", "startsAt": "2026-08-09T04:00:00.000000000Z",
            "endsAt": "2026-08-10T04:00:00.000000000Z",
            "observedAt": observed, "value": "68000.15", "basis": "bid_ask_midpoint",
        });
        let changed = product_row(identity, &quote_row)?;
        assert_eq!(changed["changePercent"], "6.250234375");
        assert_eq!(changed["changeBasis"]["priceBasis"], "bid_ask_midpoint");
        assert_eq!(changed["changeBasis"]["priceAsOf"], observed);
        assert_eq!(changed["changeBasis"]["previousClose"]["value"], "64000");
        assert_eq!(
            changed["changeBasis"]["previousClose"]["sessionDate"],
            "2026-08-08"
        );
        assert!(changed["changeUnavailableReason"].is_null());
        let mut declining = quote_row.clone();
        declining["currentPrice"]["value"] = json!("63999.99");
        declining["quote"]["midPrice"] = json!("63999.99");
        declining["quote"]["bidPrice"] = json!("63999.98");
        declining["quote"]["askPrice"] = json!("64000");
        declining["priceSession"]["value"] = json!("63999.99");
        assert_eq!(
            product_row(identity, &declining)?["changePercent"],
            "-0.000015625"
        );
        declining["currentPrice"]["observedAt"] = json!(through);
        assert_eq!(
            product_row(identity, &declining)?["changeUnavailableReason"],
            "incompatible_basis"
        );
        // A current label requires freshness; all comparisons require compatible economics
        // and a preceding completed date.
        // In particular a close from later in the read cannot become this price's baseline.
        for (field, invalid) in [
            (
                "instrumentId",
                records[1].definition().instrument_id().to_string(),
            ),
            ("currency", "EUR".to_owned()),
            ("adjustment", "split_adjusted".to_owned()),
            ("value", "0".to_owned()),
            ("sessionDate", "2026-08-09".to_owned()),
            ("asOf", through.to_owned()),
        ] {
            let mut incompatible = quote_row.clone();
            incompatible["previousClose"][field] = json!(invalid);
            let result = product_row(identity, &incompatible)?;
            assert!(result["changePercent"].is_null());
            assert_eq!(result["changeUnavailableReason"], "incompatible_basis");
        }
        quote_row["quote"]["quoteFresh"] = json!(false);
        assert_eq!(
            product_row(identity, &quote_row)?["changeUnavailableReason"],
            "current_price_unavailable"
        );
        quote_row["quote"]["quoteFresh"] = json!(true);
        quote_row["quote"]["lastPrice"] = json!("68000.15");
        assert!(matches!(
            product_row(identity, &quote_row),
            Err(ServiceError::InvalidResult)
        ));
        quote_row["quote"]["lastPrice"] = Value::Null;
        quote_row["currentPrice"]["basis"] = json!("previous_close");
        quote_row["availability"] = json!("end_of_day");
        let closed = product_row(identity, &quote_row)?;
        assert_eq!(closed["availability"], "previous_close");
        assert_eq!(closed["priceBasis"], "previous_close");
        assert_eq!(closed["quote"], projected["quote"]);
        assert!(closed["changePercent"].is_null());
        assert!(closed["changeBasis"].is_null());
        assert_eq!(closed["changeUnavailableReason"], "incompatible_basis");
        quote_row["quote"]["quoteFresh"] = json!(false);
        let retained = product_row(identity, &quote_row)?;
        assert_eq!(retained["priceBasis"], "previous_close");
        assert_eq!(retained["quote"]["bidPrice"], "68000.1");
        assert_eq!(retained["quote"]["quoteObservedAt"], observed);
        assert_eq!(retained["quote"]["quoteFresh"], false);

        // A dated retained trade remains comparable after its current-mark lease expires.
        // This is the native MSFT failure: a later trade must not disappear behind a close.
        let read_at = DateTime::parse_from_rfc3339("2026-10-02T22:35:00Z")?
            .timestamp_nanos_opt()
            .ok_or("read time out of range")?;
        let read_at = Timestamp::from_unix_nanos(read_at);
        let close = json!({
            "currentPrice": {"observedAt": "2026-10-01T20:00:00.000000000Z"},
        });
        let mut historical = json!({
            "instrumentId": selected.to_string(), "availability": "stale", "currency": "USD",
            "currentPrice": null,
            "previousClose": {
                "instrumentId": selected.to_string(), "value": "512.71", "currency": "USD",
                "sessionDate": "2026-10-01", "asOf": "2026-10-01T20:00:00.000000000Z",
                "adjustment": "raw",
            },
            "quote": {
                "quoteSizeBasis": "source_units", "bidPrice": "493.32", "bidSize": "40",
                "askPrice": "545.76", "askSize": "40", "midPrice": "519.54",
                "lastPrice": "517.13", "lastSize": "4", "tradeStatus": "available",
                "quoteObservedAt": "2026-10-02T20:00:01.412460730Z",
                "lastObservedAt": "2026-10-02T20:01:26.342735121Z",
                "quoteFresh": false, "lastFresh": false,
                "quoteCurrentThrough": null, "lastCurrentThrough": null,
            },
        });
        historical["currentPrice"] = retained_display_price(&historical, Some(&close), read_at)?
            .ok_or("missing retained price")?;
        historical["availability"] = json!("last_known");
        historical["priceSession"] = json!({
            "date": "2026-10-02", "startsAt": "2026-10-02T04:00:00.000000000Z",
            "endsAt": "2026-10-03T04:00:00.000000000Z",
            "observedAt": historical["currentPrice"]["observedAt"],
            "value": historical["currentPrice"]["value"], "basis": "last_trade",
        });
        let historical_result = product_row(identity, &historical)?;
        assert_eq!(historical_result["availability"], "last_known");
        assert_eq!(historical_result["price"]["value"], "517.13");
        assert_eq!(historical_result["priceBasis"], "last_trade");
        assert_eq!(
            historical_result["asOf"],
            historical["quote"]["lastObservedAt"]
        );
        assert_eq!(historical_result["quote"]["lastFresh"], false);
        assert!(historical_result["priceCurrentThrough"].is_null());
        assert_eq!(
            historical_result["changePercent"]
                .as_str()
                .ok_or("missing change")?
                .parse::<Decimal>()?
                .round_dp(2),
            Decimal::new(86, 2)
        );
        assert_eq!(
            historical_result["changeBasis"]["previousClose"]["value"],
            "512.71"
        );
        assert_eq!(
            historical_result["changeBasis"]["priceAsOf"],
            historical_result["asOf"]
        );
        assert!(historical_result["changeUnavailableReason"].is_null());
        let mut unresolved = historical.clone();
        unresolved["quote"]["tradeStatus"] = json!("ambiguous");
        unresolved["quote"]["lastPrice"] = Value::Null;
        unresolved["quote"]["lastSize"] = Value::Null;
        assert_eq!(
            retained_display_price(&unresolved, Some(&close), read_at)?
                .ok_or("missing retained midpoint")?["basis"],
            "bid_ask_midpoint"
        );
        let newer_close = json!({
            "currentPrice": {"observedAt": "2026-10-02T21:00:00.000000000Z"},
        });
        assert!(retained_display_price(&historical, Some(&newer_close), read_at)?.is_none());
        let mut future = historical.clone();
        for field in ["lastObservedAt", "quoteObservedAt"] {
            future["quote"][field] = json!("2026-10-03T20:00:00.000000000Z");
        }
        assert!(retained_display_price(&future, Some(&close), read_at)?.is_none());
        // A completed Friday price has its own admitted basis; daily change still compares
        // Thursday, without requiring a fresh quote or manufacturing a current-mark lease.
        let mut closing = historical.clone();
        closing["availability"] = json!("end_of_day");
        closing["currentPrice"]["basis"] = json!("previous_close");
        closing["currentPrice"]["observedAt"] = json!("2026-10-02T20:00:00.000000000Z");
        closing["priceSession"]["basis"] = json!("previous_close");
        closing["priceSession"]["observedAt"] = closing["currentPrice"]["observedAt"].clone();
        let closing_result = product_row(identity, &closing)?;
        assert_eq!(closing_result["availability"], "previous_close");
        assert_eq!(
            closing_result["changeBasis"]["priceBasis"],
            "previous_close"
        );
        assert_eq!(
            closing_result["changePercent"],
            historical_result["changePercent"]
        );
        assert_eq!(closing_result["quote"]["lastFresh"], false);
        // UTC midnight does not create a new native session. The admitted original Friday
        // day continues until 04:00Z; a Saturday read keeps the Thursday daily comparator.
        let mut late = historical.clone();
        for object in ["currentPrice", "priceSession"] {
            late[object]["observedAt"] = json!("2026-10-03T00:00:00.000000000Z");
        }
        late["quote"]["lastObservedAt"] = late["currentPrice"]["observedAt"].clone();
        assert_eq!(
            product_row(identity, &late)?["changePercent"],
            historical_result["changePercent"]
        );
        late["previousClose"]["sessionDate"] = json!("2026-10-02");
        assert_eq!(
            product_row(identity, &late)?["changeUnavailableReason"],
            "incompatible_basis"
        );
        historical["previousClose"]["sessionDate"] = json!("2026-10-02");
        historical["previousClose"]["asOf"] = json!("2026-10-02T20:00:00.000000000Z");
        assert_eq!(
            product_row(identity, &historical)?["changeUnavailableReason"],
            "incompatible_basis"
        );

        let all = product_market_identities(&records, cutoff, Some("etf"))?;
        let (first, count, more) = product_search_page(&all, "etf", 1, None)?;
        assert_eq!(count, 2);
        assert!(more);
        let cursor = first["page"]["nextPageToken"]
            .as_str()
            .ok_or("missing cursor")?;
        let (second, count, more) = product_search_page(&all, "etf", 1, Some(cursor))?;
        assert_eq!(count, 1);
        assert!(!more);
        assert_ne!(
            first["data"][0]["selectionToken"],
            second["data"][0]["selectionToken"]
        );
        assert!(matches!(
            product_search_page(&all, "ETF", 1, Some(cursor)),
            Err(ServiceError::Unavailable)
        ));
        let reduced = product_market_identities(&records[..1], cutoff, Some("etf"))?;
        assert!(matches!(
            product_search_page(&reduced, "etf", 1, Some(cursor)),
            Err(ServiceError::Unavailable)
        ));
        Ok(())
    }
}
