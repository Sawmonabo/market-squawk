use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use market_squawk_domain::{CalendarDate, SourceIdentifier, Timestamp, VenueId};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::config::{
    TRADIER_CONSOLIDATED_VENUE, TRADIER_DERIVED_INDEX_VENUE, TradierInstrumentKind,
    TradierLogicalProfile, TradierSourceConfig,
};

use super::{
    TradierDerivedIndexBatch, TradierDerivedIndexObservation, TradierOptionChain,
    TradierOptionContract, TradierOptionGreeks, TradierOptionSide, TradierQuoteBatch,
    TradierQuoteRequest, TradierQuoteSide, TradierQuoteSnapshot, TradierRestError,
    TradierRestEvidence,
};

const MAX_DECIMAL_BYTES: usize = 128;
const MAX_OPTION_CONTRACTS: usize = 10_000;
const MAX_GREEKS_TIMESTAMP_BYTES: usize = 128;

pub(super) fn quotes(
    config: &TradierSourceConfig,
    request: &TradierQuoteRequest,
    evidence: Arc<TradierRestEvidence>,
) -> Result<TradierQuoteBatch, TradierRestError> {
    let observations = quote_observations(config, request, Arc::clone(&evidence))?;
    Ok(TradierQuoteBatch::new(observations, evidence))
}

pub(super) fn derived_indexes(
    config: &TradierSourceConfig,
    request: &TradierQuoteRequest,
    evidence: Arc<TradierRestEvidence>,
) -> Result<TradierDerivedIndexBatch, TradierRestError> {
    if config.profile() != TradierLogicalProfile::DerivedIndexes {
        return Err(TradierRestError::InvalidProfile);
    }
    let quotes = quote_observations(config, request, Arc::clone(&evidence))?;
    let venue = VenueId::try_from(TRADIER_DERIVED_INDEX_VENUE)
        .map_err(|_| TradierRestError::InvalidResponse)?;
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(quotes.len())
        .map_err(|_| TradierRestError::Allocation)?;
    for quote in quotes {
        let value = quote.last().ok_or(TradierRestError::MissingObservation)?;
        let effective_at = quote
            .trade_at()
            .ok_or(TradierRestError::MissingObservation)?;
        observations.push(TradierDerivedIndexObservation::new(
            quote.symbol().clone(),
            quote.instrument(),
            venue.clone(),
            value,
            effective_at,
            Arc::clone(&evidence),
        ));
    }
    Ok(TradierDerivedIndexBatch::new(observations, evidence))
}

pub(super) fn option_chain(
    underlying: SourceIdentifier,
    expiration: CalendarDate,
    evidence: Arc<TradierRestEvidence>,
) -> Result<TradierOptionChain, TradierRestError> {
    let envelope = serde_json::from_slice::<OptionEnvelope>(evidence.payload())
        .map_err(|_| TradierRestError::InvalidResponse)?;
    let wires = envelope
        .options
        .and_then(|options| options.option)
        .map_or_else(Vec::new, OneOrMany::into_vec);
    if wires.len() > MAX_OPTION_CONTRACTS {
        return Err(TradierRestError::ResponseLimitExceeded);
    }
    let mut contracts = Vec::new();
    contracts
        .try_reserve_exact(wires.len())
        .map_err(|_| TradierRestError::Allocation)?;
    let mut unique = BTreeMap::new();
    for wire in wires {
        if wire.instrument_type != "option" || wire.underlying != underlying.as_str() {
            return Err(TradierRestError::UnexpectedObservation);
        }
        let symbol = SourceIdentifier::try_from(wire.symbol.as_str())
            .map_err(|_| TradierRestError::InvalidResponse)?;
        if unique.insert(symbol.clone(), ()).is_some() {
            return Err(TradierRestError::DuplicateObservation);
        }
        let root_symbol = SourceIdentifier::try_from(wire.root_symbol.as_str())
            .map_err(|_| TradierRestError::InvalidResponse)?;
        let contract_expiration = parse_date(&wire.expiration_date)?;
        if contract_expiration != expiration {
            return Err(TradierRestError::UnexpectedObservation);
        }
        let side = match wire.option_type.as_str() {
            "call" => TradierOptionSide::Call,
            "put" => TradierOptionSide::Put,
            _ => return Err(TradierRestError::InvalidResponse),
        };
        let strike = required_positive(wire.strike)?;
        let contract_size = required_positive(wire.contract_size)?;
        let bid = optional_nonnegative_price(wire.bid)?;
        let ask = optional_nonnegative_price(wire.ask)?;
        let last = optional_nonnegative_price(wire.last)?;
        let bid_size = optional_nonnegative(wire.bidsize)?;
        let ask_size = optional_nonnegative(wire.asksize)?;
        let volume = optional_nonnegative(wire.volume)?;
        let open_interest = optional_nonnegative(wire.open_interest)?;
        let greeks = wire.greeks.map(normalize_greeks).transpose()?;
        contracts.push(TradierOptionContract::new(
            symbol,
            root_symbol,
            side,
            strike,
            contract_size,
            contract_expiration,
            bid,
            ask,
            last,
            bid_size,
            ask_size,
            volume,
            open_interest,
            greeks,
            Arc::clone(&evidence),
        ));
    }
    Ok(TradierOptionChain::new(
        underlying, expiration, contracts, evidence,
    ))
}

fn quote_observations(
    config: &TradierSourceConfig,
    request: &TradierQuoteRequest,
    evidence: Arc<TradierRestEvidence>,
) -> Result<Vec<TradierQuoteSnapshot>, TradierRestError> {
    let envelope = serde_json::from_slice::<QuoteEnvelope>(evidence.payload())
        .map_err(|_| TradierRestError::InvalidResponse)?;
    let wires = envelope.quotes.quote.into_vec();
    if wires.len() != request.symbols().len() {
        return Err(TradierRestError::MissingObservation);
    }
    let venue = VenueId::try_from(match config.profile() {
        TradierLogicalProfile::ConsolidatedSecurities => TRADIER_CONSOLIDATED_VENUE,
        TradierLogicalProfile::DerivedIndexes => TRADIER_DERIVED_INDEX_VENUE,
    })
    .map_err(|_| TradierRestError::InvalidResponse)?;
    let requested = request
        .symbols()
        .iter()
        .map(|symbol| (symbol.as_str(), ()))
        .collect::<BTreeMap<_, _>>();
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(wires.len())
        .map_err(|_| TradierRestError::Allocation)?;
    let mut observed = BTreeMap::new();
    for wire in wires {
        if !requested.contains_key(wire.symbol.as_str()) {
            return Err(TradierRestError::UnexpectedObservation);
        }
        let mapping = config
            .mapping(&wire.symbol)
            .ok_or(TradierRestError::UnexpectedObservation)?;
        if observed.insert(wire.symbol.clone(), ()).is_some() {
            return Err(TradierRestError::DuplicateObservation);
        }
        validate_instrument_type(mapping.kind(), &wire.instrument_type)?;
        let symbol = SourceIdentifier::try_from(wire.symbol.as_str())
            .map_err(|_| TradierRestError::InvalidResponse)?;
        let (last, trade_at) = optional_trade(wire.last, wire.trade_date)?;
        let bid = quote_side(
            wire.bid,
            wire.bidsize,
            wire.bidexch,
            wire.bid_date,
            mapping.kind().quote_quantity_multiplier(),
        )?;
        let ask = quote_side(
            wire.ask,
            wire.asksize,
            wire.askexch,
            wire.ask_date,
            mapping.kind().quote_quantity_multiplier(),
        )?;
        if last.is_none() && bid.is_none() && ask.is_none() {
            return Err(TradierRestError::MissingObservation);
        }
        observations.push(TradierQuoteSnapshot::new(
            symbol,
            mapping.instrument(),
            mapping.kind(),
            venue.clone(),
            config.profile().quality_ceiling(),
            last,
            trade_at,
            bid,
            ask,
            Arc::clone(&evidence),
        ));
    }
    observations.sort_unstable_by(|left, right| left.symbol().cmp(right.symbol()));
    Ok(observations)
}

fn validate_instrument_type(
    kind: TradierInstrumentKind,
    provider_type: &str,
) -> Result<(), TradierRestError> {
    let valid = match kind {
        TradierInstrumentKind::Equity | TradierInstrumentKind::Etf => {
            matches!(provider_type, "stock" | "etf")
        }
        TradierInstrumentKind::Option => provider_type == "option",
        TradierInstrumentKind::DerivedIndex => provider_type == "index",
    };
    if valid {
        Ok(())
    } else {
        Err(TradierRestError::UnexpectedObservation)
    }
}

fn optional_trade(
    price: Option<ExactScalar>,
    timestamp: Option<ExactScalar>,
) -> Result<(Option<Decimal>, Option<Timestamp>), TradierRestError> {
    match (price, timestamp) {
        (None, None) => Ok((None, None)),
        (Some(price), Some(timestamp)) => {
            let price = exact_decimal(price)?;
            if price.is_sign_negative() {
                return Err(TradierRestError::InvalidDecimal);
            }
            if price.is_zero() {
                return Ok((None, None));
            }
            Ok((Some(price), Some(epoch_millis(timestamp)?)))
        }
        (Some(price), None) => {
            if exact_decimal(price)?.is_zero() {
                Ok((None, None))
            } else {
                Err(TradierRestError::InvalidResponse)
            }
        }
        (None, Some(_)) => Err(TradierRestError::InvalidResponse),
    }
}

fn quote_side(
    price: Option<ExactScalar>,
    quantity: Option<ExactScalar>,
    exchange: Option<String>,
    timestamp: Option<ExactScalar>,
    multiplier: u32,
) -> Result<Option<TradierQuoteSide>, TradierRestError> {
    match (price, quantity) {
        (None, None) => Ok(None),
        (Some(price), Some(quantity)) => {
            let price = exact_decimal(price)?;
            let quantity = exact_decimal(quantity)?;
            if price.is_sign_negative() || quantity.is_sign_negative() {
                return Err(TradierRestError::InvalidDecimal);
            }
            if price.is_zero() && quantity.is_zero() {
                return Ok(None);
            }
            if price.is_zero() || quantity.is_zero() {
                return Err(TradierRestError::InvalidResponse);
            }
            let exchange = exchange
                .filter(|value| !value.is_empty())
                .ok_or(TradierRestError::InvalidResponse)
                .and_then(|value| {
                    SourceIdentifier::try_from(value).map_err(|_| TradierRestError::InvalidResponse)
                })?;
            let at = timestamp
                .ok_or(TradierRestError::InvalidResponse)
                .and_then(epoch_millis)?;
            let quantity = quantity
                .checked_mul(Decimal::from(multiplier))
                .ok_or(TradierRestError::InvalidDecimal)?;
            Ok(Some(TradierQuoteSide::new(price, quantity, exchange, at)))
        }
        (None, Some(_)) | (Some(_), None) => Err(TradierRestError::InvalidResponse),
    }
}

fn normalize_greeks(wire: GreeksWire) -> Result<TradierOptionGreeks, TradierRestError> {
    let updated_at = wire
        .updated_at
        .map(|value| {
            if value.is_empty()
                || value.len() > MAX_GREEKS_TIMESTAMP_BYTES
                || !value.is_ascii()
                || value.bytes().any(|byte| byte.is_ascii_control())
            {
                Err(TradierRestError::InvalidResponse)
            } else {
                Ok(value.into_boxed_str())
            }
        })
        .transpose()?;
    Ok(TradierOptionGreeks::new(
        optional_finite(wire.delta)?,
        optional_finite(wire.gamma)?,
        optional_finite(wire.theta)?,
        optional_finite(wire.vega)?,
        optional_finite(wire.rho)?,
        optional_finite(wire.phi)?,
        optional_nonnegative(wire.bid_iv)?,
        optional_nonnegative(wire.mid_iv)?,
        optional_nonnegative(wire.ask_iv)?,
        optional_nonnegative(wire.smv_vol)?,
        updated_at,
    ))
}

fn required_positive(value: ExactScalar) -> Result<Decimal, TradierRestError> {
    let value = exact_decimal(value)?;
    if value.is_sign_negative() || value.is_zero() {
        Err(TradierRestError::InvalidDecimal)
    } else {
        Ok(value)
    }
}

fn optional_nonnegative_price(
    value: Option<ExactScalar>,
) -> Result<Option<Decimal>, TradierRestError> {
    let value = optional_nonnegative(value)?;
    Ok(value.filter(|value| !value.is_zero()))
}

fn optional_nonnegative(value: Option<ExactScalar>) -> Result<Option<Decimal>, TradierRestError> {
    match value {
        Some(value) => {
            let value = exact_decimal(value)?;
            if value.is_sign_negative() {
                Err(TradierRestError::InvalidDecimal)
            } else {
                Ok(Some(value))
            }
        }
        None => Ok(None),
    }
}

fn optional_finite(value: Option<ExactScalar>) -> Result<Option<Decimal>, TradierRestError> {
    value.map(exact_decimal).transpose()
}

fn exact_decimal(value: ExactScalar) -> Result<Decimal, TradierRestError> {
    let value = value.into_string();
    if value.is_empty()
        || value.len() > MAX_DECIMAL_BYTES
        || !value.is_ascii()
        || value.contains(['e', 'E'])
    {
        return Err(TradierRestError::InvalidDecimal);
    }
    Decimal::from_str_exact(&value).map_err(|_| TradierRestError::InvalidDecimal)
}

fn epoch_millis(value: ExactScalar) -> Result<Timestamp, TradierRestError> {
    let value = value.into_string();
    if value.is_empty() || value.len() > 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(TradierRestError::InvalidTimestamp);
    }
    value
        .parse::<i64>()
        .ok()
        .and_then(|millis| millis.checked_mul(1_000_000))
        .filter(|nanos| *nanos > 0)
        .map(Timestamp::from_unix_nanos)
        .ok_or(TradierRestError::InvalidTimestamp)
}

fn parse_date(value: &str) -> Result<CalendarDate, TradierRestError> {
    if value.len() != 10
        || value.as_bytes()[4] != b'-'
        || value.as_bytes()[7] != b'-'
        || !value
            .bytes()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
    {
        return Err(TradierRestError::InvalidDate);
    }
    let year = u16::from_str(&value[0..4]).map_err(|_| TradierRestError::InvalidDate)?;
    let month = u8::from_str(&value[5..7]).map_err(|_| TradierRestError::InvalidDate)?;
    let day = u8::from_str(&value[8..10]).map_err(|_| TradierRestError::InvalidDate)?;
    CalendarDate::new(year, month, day).map_err(|_| TradierRestError::InvalidDate)
}

#[derive(Deserialize)]
struct QuoteEnvelope {
    quotes: QuoteContainer,
}

#[derive(Deserialize)]
struct QuoteContainer {
    quote: OneOrMany<QuoteWire>,
}

#[derive(Deserialize)]
struct QuoteWire {
    symbol: String,
    #[serde(rename = "type")]
    instrument_type: String,
    last: Option<ExactScalar>,
    trade_date: Option<ExactScalar>,
    bid: Option<ExactScalar>,
    bidsize: Option<ExactScalar>,
    bidexch: Option<String>,
    bid_date: Option<ExactScalar>,
    ask: Option<ExactScalar>,
    asksize: Option<ExactScalar>,
    askexch: Option<String>,
    ask_date: Option<ExactScalar>,
}

#[derive(Deserialize)]
struct OptionEnvelope {
    options: Option<OptionContainer>,
}

#[derive(Deserialize)]
struct OptionContainer {
    option: Option<OneOrMany<OptionWire>>,
}

#[derive(Deserialize)]
struct OptionWire {
    symbol: String,
    #[serde(rename = "type")]
    instrument_type: String,
    underlying: String,
    root_symbol: String,
    option_type: String,
    strike: ExactScalar,
    contract_size: ExactScalar,
    expiration_date: String,
    bid: Option<ExactScalar>,
    ask: Option<ExactScalar>,
    last: Option<ExactScalar>,
    bidsize: Option<ExactScalar>,
    asksize: Option<ExactScalar>,
    volume: Option<ExactScalar>,
    open_interest: Option<ExactScalar>,
    greeks: Option<GreeksWire>,
}

#[derive(Deserialize)]
struct GreeksWire {
    delta: Option<ExactScalar>,
    gamma: Option<ExactScalar>,
    theta: Option<ExactScalar>,
    vega: Option<ExactScalar>,
    rho: Option<ExactScalar>,
    phi: Option<ExactScalar>,
    bid_iv: Option<ExactScalar>,
    mid_iv: Option<ExactScalar>,
    ask_iv: Option<ExactScalar>,
    smv_vol: Option<ExactScalar>,
    updated_at: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    fn into_vec(self) -> Vec<T> {
        match self {
            Self::One(value) => vec![value],
            Self::Many(values) => values,
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ExactScalar {
    Text(String),
    Number(serde_json::Number),
}

impl ExactScalar {
    fn into_string(self) -> String {
        match self {
            Self::Text(value) => value,
            Self::Number(value) => value.to_string(),
        }
    }
}
