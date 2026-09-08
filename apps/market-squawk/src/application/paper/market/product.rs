//! Closed provider-neutral identities for ordinary Market product reads.

use chrono::DateTime;
use market_squawk_data::MarketDataInstrumentRecord;
use market_squawk_domain::{AssetClass, Currency, InstrumentDefinition, InstrumentId};
use market_squawk_services::{ServiceError, ServiceLimits, ToolResultMetadata, TypedToolResult};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use crate::application::domain_support::{opaque_product_text_token, try_boxed_product_text};

const MARKET_TOKEN_DOMAIN: &[u8] = b"market-squawk/market-selection/v1\0";
const HISTORY_TOKEN_DOMAIN: &[u8] = b"market-squawk/market-history/v1\0";
const PAGE_TOKEN_DOMAIN: &[u8] = b"market-squawk/market-page/v1\0";
const MAXIMUM_TOKEN_BYTES: usize = 96;
pub(super) const MAXIMUM_PRODUCT_MARKET_ROWS: usize = 100;
pub(super) const MAXIMUM_PRODUCT_MARKET_POPULATION: usize = 4_096;

#[derive(Debug)]
pub(super) struct ProductMarketIdentity {
    instrument_id: InstrumentId,
    selection_token: Box<str>,
    history_token: Box<str>,
    name: Box<str>,
    asset_class: &'static str,
    population_binding: [u8; 32],
}

impl ProductMarketIdentity {
    pub(super) fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(super) fn selection_token(&self) -> &str {
        &self.selection_token
    }

    pub(super) fn history_token(&self) -> &str {
        &self.history_token
    }

    pub(super) fn name(&self) -> &str {
        &self.name
    }
}

/// Builds the complete bounded token population and rejects every ambiguity or collision.
pub(super) fn product_market_identities(
    definitions: &[InstrumentDefinition],
    market_data: &[MarketDataInstrumentRecord],
) -> Result<Vec<ProductMarketIdentity>, ServiceError> {
    if definitions.is_empty()
        || definitions.len() > MAXIMUM_PRODUCT_MARKET_POPULATION
        || market_data.len() != definitions.len()
    {
        return Err(ServiceError::Unavailable);
    }
    let population_binding = population_binding(definitions, market_data)?;
    let mut identities = Vec::new();
    identities
        .try_reserve_exact(definitions.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for (definition, record) in definitions.iter().zip(market_data) {
        if record.definition().instrument_id() != definition.instrument_id() {
            return Err(ServiceError::InvalidResult);
        }
        let instrument_bytes = definition.instrument_id().as_uuid().into_bytes();
        let selection_token = token(
            "market_",
            MARKET_TOKEN_DOMAIN,
            &[&population_binding, &instrument_bytes],
        )?;
        let history_token = token(
            "history_",
            HISTORY_TOKEN_DOMAIN,
            &[&population_binding, &instrument_bytes],
        )?;
        if identities.iter().any(|identity: &ProductMarketIdentity| {
            identity.selection_token() == selection_token.as_ref()
                || identity.history_token() == history_token.as_ref()
        }) {
            return Err(ServiceError::InvalidResult);
        }
        identities.push(ProductMarketIdentity {
            instrument_id: definition.instrument_id(),
            selection_token,
            history_token,
            name: record
                .definition()
                .display_name()
                .map(|name| try_boxed_product_text(name.as_str(), 256))
                .transpose()
                .map_err(|_error| ServiceError::ResourceExhausted)?
                .ok_or(ServiceError::Unavailable)?,
            asset_class: product_asset_class(definition.asset_class()),
            population_binding,
        });
    }
    identities.sort_unstable_by(|left, right| left.selection_token.cmp(&right.selection_token));
    Ok(identities)
}

pub(super) fn resolve_selection_token(
    identities: &[ProductMarketIdentity],
    token: &str,
) -> Result<InstrumentId, ServiceError> {
    resolve_token(identities, token, |identity| identity.selection_token())
}

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
        if query.is_empty() || identity.name().contains(query) {
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
            "selectionToken": identity.selection_token,
            "symbol": Value::Null,
            "name": identity.name,
            "kind": product_kind(identity.asset_class),
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

fn resolve_token(
    identities: &[ProductMarketIdentity],
    token: &str,
    field: impl Fn(&ProductMarketIdentity) -> &str,
) -> Result<InstrumentId, ServiceError> {
    let mut matches = identities
        .iter()
        .filter(|identity| field(identity) == token);
    let instrument_id = matches
        .next()
        .map(ProductMarketIdentity::instrument_id)
        .ok_or(ServiceError::Unavailable)?;
    if matches.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(instrument_id)
}

pub(super) fn product_row(
    identity: &ProductMarketIdentity,
    native_row: &Value,
) -> Result<Value, ServiceError> {
    let row = native_row.as_object().ok_or(ServiceError::InvalidResult)?;
    if native_instrument_id(native_row) != Some(identity.instrument_id) {
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
        .and_then(|price| {
            price
                .get("currentThrough")
                .filter(|value| !value.is_null())
                .or_else(|| price.get("observedAt").filter(|value| !value.is_null()))
        })
        .map(|value| canonical_time(value).map(Value::String))
        .transpose()?;
    if price.is_some() != as_of.is_some() {
        return Err(ServiceError::Unavailable);
    }
    let availability = match row.get("availability").and_then(Value::as_str) {
        Some("live") => "current",
        Some("delayed") => "delayed",
        Some("end_of_day" | "stored") => "previous_close",
        Some("stale" | "unavailable") => "unavailable",
        _ => return Err(ServiceError::InvalidResult),
    };
    Ok(json!({
        "selectionToken": identity.selection_token,
        "historyToken": identity.history_token,
        "identity": {
            "symbol": Value::Null,
            "name": identity.name,
            "assetClass": identity.asset_class,
        },
        "price": price,
        "changePercent": Value::Null,
        "asOf": as_of,
        "availability": availability,
    }))
}

fn page_token(last: &ProductMarketIdentity, query: &str) -> Result<Box<str>, ServiceError> {
    token(
        "page_",
        PAGE_TOKEN_DOMAIN,
        &[
            &last.population_binding,
            query.as_bytes(),
            last.selection_token.as_bytes(),
        ],
    )
}

fn token(prefix: &str, domain: &[u8], components: &[&[u8]]) -> Result<Box<str>, ServiceError> {
    opaque_product_text_token(prefix, domain, components, MAXIMUM_TOKEN_BYTES)
        .map_err(|_error| ServiceError::ResourceExhausted)
}

fn population_binding(
    definitions: &[InstrumentDefinition],
    market_data: &[MarketDataInstrumentRecord],
) -> Result<[u8; 32], ServiceError> {
    let mut digest = Sha256::new();
    for (definition, record) in definitions.iter().zip(market_data) {
        if definition.instrument_id() != record.definition().instrument_id() {
            return Err(ServiceError::InvalidResult);
        }
        digest.update(definition.instrument_id().as_uuid().as_bytes());
        digest.update(definition.definition_revision().get().to_be_bytes());
        digest.update(record.revision_digest().bytes());
    }
    Ok(digest.finalize().into())
}

fn product_asset_class(asset_class: AssetClass) -> &'static str {
    match asset_class {
        AssetClass::Equity => "equity",
        AssetClass::FixedIncome => "fixed_income",
        AssetClass::Option => "option",
        AssetClass::Future => "future",
        AssetClass::ForeignExchange => "foreign_exchange",
        AssetClass::Crypto => "crypto",
        AssetClass::Commodity => "commodity",
        AssetClass::Fund => "fund",
        AssetClass::Index => "index",
        AssetClass::Cash => "cash",
    }
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
