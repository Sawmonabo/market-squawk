use std::collections::BTreeSet;

use chrono::Datelike;
use market_squawk_domain::CalendarDate;
use serde_json::{Map, Value};

use crate::transport::{SchwabRestPayload, SchwabSealedRestResponse};
use crate::{ParseBounds, SchwabAdapterError};

use super::native::{NativeNumber, take_object};
use super::{
    NativeField, NativeFieldEntry, NativeScalar, ParseContext, ParsedNative, ProviderIdentifier,
    parse_json_payload,
};

/// Closed quote-component field dictionary shared by quote/regular/extended/future/forex blocks.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum QuoteComponentField {
    FiftyTwoWeekHigh,
    FiftyTwoWeekLow,
    AskMicId,
    AskPrice,
    AskSize,
    AskTime,
    BidMicId,
    BidPrice,
    BidSize,
    BidTime,
    ClosePrice,
    HighPrice,
    LastMicId,
    LastPrice,
    LastSize,
    LowPrice,
    Mark,
    MarkChange,
    MarkPercentChange,
    NetChange,
    NetPercentChange,
    OpenPrice,
    PostMarketChange,
    PostMarketPercentChange,
    QuoteTime,
    SecurityStatus,
    TotalVolume,
    TradeTime,
    Volatility,
}

impl QuoteComponentField {
    fn from_key(key: &str) -> Option<Self> {
        Some(match key {
            "52WeekHigh" => Self::FiftyTwoWeekHigh,
            "52WeekLow" => Self::FiftyTwoWeekLow,
            "askMICId" => Self::AskMicId,
            "askPrice" => Self::AskPrice,
            "askSize" => Self::AskSize,
            "askTime" => Self::AskTime,
            "bidMICId" => Self::BidMicId,
            "bidPrice" => Self::BidPrice,
            "bidSize" => Self::BidSize,
            "bidTime" => Self::BidTime,
            "closePrice" => Self::ClosePrice,
            "highPrice" => Self::HighPrice,
            "lastMICId" => Self::LastMicId,
            "lastPrice" => Self::LastPrice,
            "lastSize" => Self::LastSize,
            "lowPrice" => Self::LowPrice,
            "mark" => Self::Mark,
            "markChange" => Self::MarkChange,
            "markPercentChange" => Self::MarkPercentChange,
            "netChange" => Self::NetChange,
            "netPercentChange" => Self::NetPercentChange,
            "openPrice" => Self::OpenPrice,
            "postMarketChange" => Self::PostMarketChange,
            "postMarketPercentChange" => Self::PostMarketPercentChange,
            "quoteTime" => Self::QuoteTime,
            "securityStatus" => Self::SecurityStatus,
            "totalVolume" => Self::TotalVolume,
            "tradeTime" => Self::TradeTime,
            "volatility" => Self::Volatility,
            _ => return None,
        })
    }
}

/// Closed quote reference-field dictionary.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ReferenceField {
    Cusip,
    Description,
    Exchange,
    ExchangeName,
    IsHardToBorrow,
    IsShortable,
    HtbQuantity,
    HtbRate,
    ContractType,
    DaysToExpiration,
    ExpirationDay,
    ExpirationMonth,
    ExpirationYear,
    Multiplier,
    SettlementType,
    StrikePrice,
    Underlying,
    FutureActiveSymbol,
    FutureExpirationDate,
    FutureIsActive,
    Product,
    TradingHours,
}

impl ReferenceField {
    fn from_key(key: &str) -> Option<Self> {
        Some(match key {
            "cusip" => Self::Cusip,
            "description" => Self::Description,
            "exchange" => Self::Exchange,
            "exchangeName" => Self::ExchangeName,
            "isHardToBorrow" => Self::IsHardToBorrow,
            "isShortable" => Self::IsShortable,
            "htbQuantity" => Self::HtbQuantity,
            "htbRate" => Self::HtbRate,
            "contractType" => Self::ContractType,
            "daysToExpiration" => Self::DaysToExpiration,
            "expirationDay" => Self::ExpirationDay,
            "expirationMonth" => Self::ExpirationMonth,
            "expirationYear" => Self::ExpirationYear,
            "multiplier" => Self::Multiplier,
            "settlementType" => Self::SettlementType,
            "strikePrice" => Self::StrikePrice,
            "underlying" => Self::Underlying,
            "futureActiveSymbol" => Self::FutureActiveSymbol,
            "futureExpirationDate" => Self::FutureExpirationDate,
            "futureIsActive" => Self::FutureIsActive,
            "product" => Self::Product,
            "tradingHours" => Self::TradingHours,
            _ => return None,
        })
    }
}

/// Closed provider fundamental-field dictionary.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FundamentalField {
    AverageTenDaysVolume,
    AverageOneYearVolume,
    DeclarationDate,
    DividendAmount,
    DividendDate,
    DividendFrequency,
    DividendPayAmount,
    DividendPayDate,
    DividendYield,
    EarningsPerShare,
    FundLeverageFactor,
    FundStrategy,
    NextDividendPayDate,
    NextDividendDate,
    PriceToEarningsRatio,
}

impl FundamentalField {
    fn from_key(key: &str) -> Option<Self> {
        Some(match key {
            "avg10DaysVolume" => Self::AverageTenDaysVolume,
            "avg1YearVolume" => Self::AverageOneYearVolume,
            "declarationDate" => Self::DeclarationDate,
            "divAmount" => Self::DividendAmount,
            "divExDate" => Self::DividendDate,
            "divFreq" => Self::DividendFrequency,
            "divPayAmount" => Self::DividendPayAmount,
            "divPayDate" => Self::DividendPayDate,
            "divYield" => Self::DividendYield,
            "eps" => Self::EarningsPerShare,
            "fundLeverageFactor" => Self::FundLeverageFactor,
            "fundStrategy" => Self::FundStrategy,
            "nextDivPayDate" => Self::NextDividendPayDate,
            "nextDivExDate" => Self::NextDividendDate,
            "peRatio" => Self::PriceToEarningsRatio,
            _ => return None,
        })
    }
}

/// Closed native quote record. No field is promoted to consolidated/NBBO semantics here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabQuote {
    symbol: ProviderIdentifier,
    asset_main_type: NativeField<Box<str>>,
    asset_sub_type: NativeField<Box<str>>,
    realtime: NativeField<bool>,
    ssid: NativeField<u64>,
    quote: Box<[NativeFieldEntry<QuoteComponentField>]>,
    regular: Box<[NativeFieldEntry<QuoteComponentField>]>,
    extended: Box<[NativeFieldEntry<QuoteComponentField>]>,
    reference: Box<[NativeFieldEntry<ReferenceField>]>,
    fundamental: Box<[NativeFieldEntry<FundamentalField>]>,
}

impl SchwabQuote {
    pub const fn symbol(&self) -> &ProviderIdentifier {
        &self.symbol
    }
    pub const fn asset_main_type(&self) -> &NativeField<Box<str>> {
        &self.asset_main_type
    }
    pub const fn asset_sub_type(&self) -> &NativeField<Box<str>> {
        &self.asset_sub_type
    }
    pub const fn realtime(&self) -> &NativeField<bool> {
        &self.realtime
    }
    pub const fn ssid(&self) -> &NativeField<u64> {
        &self.ssid
    }
    pub fn quote_fields(&self) -> &[NativeFieldEntry<QuoteComponentField>] {
        &self.quote
    }
    pub fn regular_fields(&self) -> &[NativeFieldEntry<QuoteComponentField>] {
        &self.regular
    }
    pub fn extended_fields(&self) -> &[NativeFieldEntry<QuoteComponentField>] {
        &self.extended
    }
    pub fn reference_fields(&self) -> &[NativeFieldEntry<ReferenceField>] {
        &self.reference
    }
    pub fn fundamental_fields(&self) -> &[NativeFieldEntry<FundamentalField>] {
        &self.fundamental
    }
}

/// Bounded quote response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuoteResponse {
    quotes: Box<[SchwabQuote]>,
}
impl QuoteResponse {
    pub fn quotes(&self) -> &[SchwabQuote] {
        &self.quotes
    }
}

/// Parses quote-map or one-quote payloads, rejects duplicate/mismatched symbols, and summarizes
/// unknown provider fields without exposing their values as an application schema.
pub fn parse_quote_response(
    bytes: &[u8],
    bounds: ParseBounds,
) -> Result<ParsedNative<QuoteResponse>, SchwabAdapterError> {
    let (value, digest, mut context) = parse_json_payload(bytes, bounds)?;
    let object = take_object(value)?;
    let mut quotes = Vec::new();
    for (key, value) in object {
        context.take_record()?;
        let quote = parse_quote(&key, value, &mut context, &format!("$.{key}"))?;
        quotes
            .try_reserve(1)
            .map_err(|_| SchwabAdapterError::BoundsExceeded)?;
        quotes.push(quote);
    }
    if quotes.is_empty() {
        return Err(SchwabAdapterError::SchemaViolation);
    }
    Ok(ParsedNative::new(
        "schwab.marketdata.quotes",
        digest,
        context.finish(),
        QuoteResponse {
            quotes: quotes.into_boxed_slice(),
        },
    ))
}

fn parse_quote(
    key: &str,
    value: Value,
    context: &mut ParseContext,
    path: &str,
) -> Result<SchwabQuote, SchwabAdapterError> {
    let mut object = take_object(value)?;
    let symbol = ProviderIdentifier::try_new(key.to_owned())?;
    if let Some(wire_symbol) = remove_optional_text(&mut object, "symbol")? {
        if wire_symbol.as_ref() != symbol.as_str() {
            return Err(SchwabAdapterError::SchemaViolation);
        }
    }
    let asset_main_type = remove_native_text(&mut object, "assetMainType")?;
    let asset_sub_type = remove_native_text(&mut object, "assetSubType")?;
    let realtime = remove_native_bool(&mut object, "realtime")?;
    let ssid = remove_native_u64(&mut object, "ssid")?;
    let quote = parse_typed_block(
        &mut object,
        "quote",
        path,
        context,
        QuoteComponentField::from_key,
    )?;
    let regular = parse_typed_block(
        &mut object,
        "regular",
        path,
        context,
        QuoteComponentField::from_key,
    )?;
    let extended = parse_typed_block(
        &mut object,
        "extended",
        path,
        context,
        QuoteComponentField::from_key,
    )?;
    let reference = parse_typed_block(
        &mut object,
        "reference",
        path,
        context,
        ReferenceField::from_key,
    )?;
    let fundamental = parse_typed_block(
        &mut object,
        "fundamental",
        path,
        context,
        FundamentalField::from_key,
    )?;
    record_remaining(&object, path, context)?;
    Ok(SchwabQuote {
        symbol,
        asset_main_type,
        asset_sub_type,
        realtime,
        ssid,
        quote,
        regular,
        extended,
        reference,
        fundamental,
    })
}

/// Option side from native chain maps.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptionSide {
    Call,
    Put,
}

/// Closed option contract field dictionary. Values remain exact native scalars until the shared
/// option schema mapper supplies instrument identity, clocks, entitlement, and value states.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum OptionContractField {
    PutCall,
    Symbol,
    Description,
    ExchangeName,
    Bid,
    Ask,
    Last,
    Mark,
    BidSize,
    AskSize,
    LastSize,
    HighPrice,
    LowPrice,
    OpenPrice,
    ClosePrice,
    TotalVolume,
    TradeDate,
    QuoteTimeInLong,
    TradeTimeInLong,
    NetChange,
    Volatility,
    Delta,
    Gamma,
    Theta,
    Vega,
    Rho,
    OpenInterest,
    TimeValue,
    TheoreticalOptionValue,
    TheoreticalVolatility,
    StrikePrice,
    ExpirationDate,
    DaysToExpiration,
    ExpirationType,
    LastTradingDay,
    Multiplier,
    SettlementType,
    DeliverableNote,
    PercentChange,
    MarkChange,
    MarkPercentChange,
    InTheMoney,
    Mini,
    NonStandard,
}

impl OptionContractField {
    fn from_key(key: &str) -> Option<Self> {
        Some(match key {
            "putCall" => Self::PutCall,
            "symbol" => Self::Symbol,
            "description" => Self::Description,
            "exchangeName" => Self::ExchangeName,
            "bid" => Self::Bid,
            "ask" => Self::Ask,
            "last" => Self::Last,
            "mark" => Self::Mark,
            "bidSize" => Self::BidSize,
            "askSize" => Self::AskSize,
            "lastSize" => Self::LastSize,
            "highPrice" => Self::HighPrice,
            "lowPrice" => Self::LowPrice,
            "openPrice" => Self::OpenPrice,
            "closePrice" => Self::ClosePrice,
            "totalVolume" => Self::TotalVolume,
            "tradeDate" => Self::TradeDate,
            "quoteTimeInLong" => Self::QuoteTimeInLong,
            "tradeTimeInLong" => Self::TradeTimeInLong,
            "netChange" => Self::NetChange,
            "volatility" => Self::Volatility,
            "delta" => Self::Delta,
            "gamma" => Self::Gamma,
            "theta" => Self::Theta,
            "vega" => Self::Vega,
            "rho" => Self::Rho,
            "openInterest" => Self::OpenInterest,
            "timeValue" => Self::TimeValue,
            "theoreticalOptionValue" => Self::TheoreticalOptionValue,
            "theoreticalVolatility" => Self::TheoreticalVolatility,
            "strikePrice" => Self::StrikePrice,
            "expirationDate" => Self::ExpirationDate,
            "daysToExpiration" => Self::DaysToExpiration,
            "expirationType" => Self::ExpirationType,
            "lastTradingDay" => Self::LastTradingDay,
            "multiplier" => Self::Multiplier,
            "settlementType" => Self::SettlementType,
            "deliverableNote" => Self::DeliverableNote,
            "percentChange" => Self::PercentChange,
            "markChange" => Self::MarkChange,
            "markPercentChange" => Self::MarkPercentChange,
            "inTheMoney" => Self::InTheMoney,
            "mini" => Self::Mini,
            "nonStandard" => Self::NonStandard,
            _ => return None,
        })
    }
}

/// One exact provider-native option contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionContract {
    side: OptionSide,
    expiration_group: Box<str>,
    strike_group: Box<str>,
    fields: Box<[NativeFieldEntry<OptionContractField>]>,
}
impl OptionContract {
    pub const fn side(&self) -> OptionSide {
        self.side
    }
    pub fn expiration_group(&self) -> &str {
        &self.expiration_group
    }
    pub fn strike_group(&self) -> &str {
        &self.strike_group
    }
    pub fn fields(&self) -> &[NativeFieldEntry<OptionContractField>] {
        &self.fields
    }
}

/// Bounded native option chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionChain {
    symbol: ProviderIdentifier,
    status: NativeField<Box<str>>,
    strategy: NativeField<Box<str>>,
    underlying_price: NativeField<NativeNumber>,
    volatility: NativeField<NativeNumber>,
    interest_rate: NativeField<NativeNumber>,
    days_to_expiration: NativeField<u64>,
    number_of_contracts: NativeField<u64>,
    underlying: Box<[NativeFieldEntry<QuoteComponentField>]>,
    contracts: Box<[OptionContract]>,
}
impl OptionChain {
    pub const fn symbol(&self) -> &ProviderIdentifier {
        &self.symbol
    }
    pub const fn status(&self) -> &NativeField<Box<str>> {
        &self.status
    }
    pub const fn strategy(&self) -> &NativeField<Box<str>> {
        &self.strategy
    }
    pub const fn underlying_price(&self) -> &NativeField<NativeNumber> {
        &self.underlying_price
    }
    pub const fn volatility(&self) -> &NativeField<NativeNumber> {
        &self.volatility
    }
    pub const fn interest_rate(&self) -> &NativeField<NativeNumber> {
        &self.interest_rate
    }
    pub const fn days_to_expiration(&self) -> &NativeField<u64> {
        &self.days_to_expiration
    }
    pub const fn number_of_contracts(&self) -> &NativeField<u64> {
        &self.number_of_contracts
    }
    pub fn underlying_fields(&self) -> &[NativeFieldEntry<QuoteComponentField>] {
        &self.underlying
    }
    pub fn contracts(&self) -> &[OptionContract] {
        &self.contracts
    }
}

impl SchwabSealedRestResponse {
    /// Selects one deterministic, actually returned contract whose expiration is still future at
    /// the sealed response's receipt date.
    ///
    /// Same-day contracts are conservatively excluded so a doctor cannot subscribe to a contract
    /// that expires between the REST response and its bounded Streamer probe.
    pub fn select_unexpired_option_contract(
        &self,
    ) -> Result<Option<ProviderIdentifier>, SchwabAdapterError> {
        let SchwabRestPayload::OptionChain(parsed) = &self.parts().payload else {
            return Err(SchwabAdapterError::InvalidInput);
        };
        let received_date =
            calendar_date_from_unix_millis(self.receipt().received_at_unix_millis())?;
        let mut selected: Option<(CalendarDate, ProviderIdentifier)> = None;
        for contract in parsed.value().contracts() {
            let Some(expiration) = option_contract_expiration(contract) else {
                continue;
            };
            if expiration <= received_date {
                continue;
            }
            let Some(symbol) = option_contract_text(contract, OptionContractField::Symbol) else {
                continue;
            };
            let Ok(symbol) = ProviderIdentifier::try_new(symbol.to_owned()) else {
                continue;
            };
            let candidate = (expiration, symbol);
            if selected.as_ref().is_none_or(|current| {
                (candidate.0, candidate.1.as_str()) < (current.0, current.1.as_str())
            }) {
                selected = Some(candidate);
            }
        }
        Ok(selected.map(|(_, symbol)| symbol))
    }
}

fn calendar_date_from_unix_millis(value: u64) -> Result<CalendarDate, SchwabAdapterError> {
    let value = i64::try_from(value).map_err(|_| SchwabAdapterError::ArithmeticOverflow)?;
    let received = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(value)
        .ok_or(SchwabAdapterError::ArithmeticOverflow)?;
    let year =
        u16::try_from(received.year()).map_err(|_| SchwabAdapterError::ArithmeticOverflow)?;
    let month =
        u8::try_from(received.month()).map_err(|_| SchwabAdapterError::ArithmeticOverflow)?;
    let day = u8::try_from(received.day()).map_err(|_| SchwabAdapterError::ArithmeticOverflow)?;
    CalendarDate::new(year, month, day).map_err(|_| SchwabAdapterError::SchemaViolation)
}

fn option_contract_expiration(contract: &OptionContract) -> Option<CalendarDate> {
    let value = contract
        .expiration_group()
        .split_once(':')
        .map_or(contract.expiration_group(), |(date, _)| date);
    let mut parts = value.split('-');
    let year = parts.next()?.parse::<u16>().ok()?;
    let month = parts.next()?.parse::<u8>().ok()?;
    let day = parts.next()?.parse::<u8>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    CalendarDate::new(year, month, day).ok()
}

fn option_contract_text(contract: &OptionContract, name: OptionContractField) -> Option<&str> {
    contract
        .fields()
        .iter()
        .find(|field| field.name() == &name)
        .and_then(|field| field.value().text())
}

pub fn parse_option_chain_response(
    bytes: &[u8],
    bounds: ParseBounds,
) -> Result<ParsedNative<OptionChain>, SchwabAdapterError> {
    let (value, digest, mut context) = parse_json_payload(bytes, bounds)?;
    let mut object = take_object(value)?;
    context.take_record()?;
    let symbol = remove_required_identifier(&mut object, "symbol")?;
    let status = remove_native_text(&mut object, "status")?;
    let strategy = remove_native_text(&mut object, "strategy")?;
    let underlying_price = remove_native_number(&mut object, "underlyingPrice")?;
    let volatility = remove_native_number(&mut object, "volatility")?;
    let interest_rate = remove_native_number(&mut object, "interestRate")?;
    let days_to_expiration = remove_native_u64(&mut object, "daysToExpiration")?;
    let number_of_contracts = remove_native_u64(&mut object, "numberOfContracts")?;
    let underlying = parse_typed_block(
        &mut object,
        "underlying",
        "$",
        &mut context,
        QuoteComponentField::from_key,
    )?;
    let mut contracts = Vec::new();
    parse_contract_map(
        object.remove("callExpDateMap"),
        OptionSide::Call,
        "$.callExpDateMap",
        &mut context,
        &mut contracts,
    )?;
    parse_contract_map(
        object.remove("putExpDateMap"),
        OptionSide::Put,
        "$.putExpDateMap",
        &mut context,
        &mut contracts,
    )?;
    if let NativeField::Value(expected) = &number_of_contracts {
        if u64::try_from(contracts.len()).map_err(|_| SchwabAdapterError::ArithmeticOverflow)?
            != *expected
        {
            return Err(SchwabAdapterError::SchemaViolation);
        }
    }
    record_remaining(&object, "$", &mut context)?;
    Ok(ParsedNative::new(
        "schwab.marketdata.option-chain",
        digest,
        context.finish(),
        OptionChain {
            symbol,
            status,
            strategy,
            underlying_price,
            volatility,
            interest_rate,
            days_to_expiration,
            number_of_contracts,
            underlying,
            contracts: contracts.into_boxed_slice(),
        },
    ))
}

fn parse_contract_map(
    value: Option<Value>,
    side: OptionSide,
    path: &str,
    context: &mut ParseContext,
    output: &mut Vec<OptionContract>,
) -> Result<(), SchwabAdapterError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_null() {
        return Ok(());
    }
    let expirations = take_object(value)?;
    for (expiration, strikes_value) in expirations {
        if expiration.is_empty() {
            return Err(SchwabAdapterError::SchemaViolation);
        }
        let strikes = take_object(strikes_value)?;
        for (strike, contracts_value) in strikes {
            if strike.parse::<NativeDecimalCheck>().is_err() {
                return Err(SchwabAdapterError::SchemaViolation);
            }
            let values = contracts_value
                .as_array()
                .ok_or(SchwabAdapterError::SchemaViolation)?;
            if values.is_empty() {
                return Err(SchwabAdapterError::SchemaViolation);
            }
            for value in values {
                context.take_record()?;
                let contract_path = format!("{path}.{expiration}.{strike}");
                let fields = parse_typed_object(
                    value.clone(),
                    &contract_path,
                    context,
                    OptionContractField::from_key,
                )?;
                output
                    .try_reserve(1)
                    .map_err(|_| SchwabAdapterError::BoundsExceeded)?;
                output.push(OptionContract {
                    side,
                    expiration_group: expiration.clone().into_boxed_str(),
                    strike_group: strike.clone().into_boxed_str(),
                    fields,
                });
            }
        }
    }
    Ok(())
}

struct NativeDecimalCheck;
impl std::str::FromStr for NativeDecimalCheck {
    type Err = ();
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<rust_decimal::Decimal>()
            .map(|_| Self)
            .map_err(|_| ())
    }
}

/// Typed expiration-chain item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpirationEntry {
    pub expiration_date: Box<str>,
    pub days_to_expiration: u64,
    pub expiration_type: NativeField<Box<str>>,
    pub standard: NativeField<bool>,
}
/// Bounded expiration response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpirationResponse {
    expirations: Box<[ExpirationEntry]>,
}
impl ExpirationResponse {
    pub fn expirations(&self) -> &[ExpirationEntry] {
        &self.expirations
    }
}

pub fn parse_expiration_response(
    bytes: &[u8],
    bounds: ParseBounds,
) -> Result<ParsedNative<ExpirationResponse>, SchwabAdapterError> {
    let (value, digest, mut context) = parse_json_payload(bytes, bounds)?;
    let mut object = take_object(value)?;
    let values = object
        .remove("expirationList")
        .ok_or(SchwabAdapterError::SchemaViolation)?;
    let values = values
        .as_array()
        .ok_or(SchwabAdapterError::SchemaViolation)?;
    let mut expirations = Vec::new();
    let mut identities = BTreeSet::new();
    for value in values {
        context.take_record()?;
        let mut item = take_object(value.clone())?;
        let date = remove_required_text(&mut item, "expirationDate")?;
        let days = remove_required_u64(&mut item, "daysToExpiration")?;
        let key = (date.clone(), days);
        if !identities.insert(key) {
            return Err(SchwabAdapterError::SchemaViolation);
        }
        let expiration_type = remove_native_text(&mut item, "expirationType")?;
        let standard = remove_native_bool(&mut item, "standard")?;
        record_remaining(&item, "$.expirationList[]", &mut context)?;
        expirations.push(ExpirationEntry {
            expiration_date: date.into_boxed_str(),
            days_to_expiration: days,
            expiration_type,
            standard,
        });
    }
    record_remaining(&object, "$", &mut context)?;
    Ok(ParsedNative::new(
        "schwab.marketdata.expiration-chain",
        digest,
        context.finish(),
        ExpirationResponse {
            expirations: expirations.into_boxed_slice(),
        },
    ))
}

/// One exact historical candle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalCandle {
    pub open: NativeNumber,
    pub high: NativeNumber,
    pub low: NativeNumber,
    pub close: NativeNumber,
    pub volume: NativeNumber,
    pub datetime_millis: u64,
}
/// Price-history response with provider empty/previous-close evidence retained.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PriceHistoryResponse {
    pub symbol: ProviderIdentifier,
    pub empty: bool,
    pub previous_close: NativeField<NativeNumber>,
    pub previous_close_date_millis: NativeField<u64>,
    candles: Box<[HistoricalCandle]>,
}
impl PriceHistoryResponse {
    pub fn candles(&self) -> &[HistoricalCandle] {
        &self.candles
    }
}

pub fn parse_price_history_response(
    bytes: &[u8],
    bounds: ParseBounds,
) -> Result<ParsedNative<PriceHistoryResponse>, SchwabAdapterError> {
    let (value, digest, mut context) = parse_json_payload(bytes, bounds)?;
    let mut object = take_object(value)?;
    let symbol = remove_required_identifier(&mut object, "symbol")?;
    let empty = remove_required_bool(&mut object, "empty")?;
    let previous_close = remove_native_number(&mut object, "previousClose")?;
    let previous_close_date_millis = remove_native_u64(&mut object, "previousCloseDate")?;
    let values = object
        .remove("candles")
        .ok_or(SchwabAdapterError::SchemaViolation)?;
    let values = values
        .as_array()
        .ok_or(SchwabAdapterError::SchemaViolation)?;
    if empty != values.is_empty() {
        return Err(SchwabAdapterError::SchemaViolation);
    }
    let mut candles = Vec::new();
    let mut prior = None;
    for value in values {
        context.take_record()?;
        let mut candle = take_object(value.clone())?;
        let item = HistoricalCandle {
            open: remove_required_number(&mut candle, "open")?,
            high: remove_required_number(&mut candle, "high")?,
            low: remove_required_number(&mut candle, "low")?,
            close: remove_required_number(&mut candle, "close")?,
            volume: remove_required_number(&mut candle, "volume")?,
            datetime_millis: remove_required_u64(&mut candle, "datetime")?,
        };
        if prior.is_some_and(|prior| item.datetime_millis <= prior) {
            return Err(SchwabAdapterError::SchemaViolation);
        }
        prior = Some(item.datetime_millis);
        record_remaining(&candle, "$.candles[]", &mut context)?;
        candles.push(item);
    }
    record_remaining(&object, "$", &mut context)?;
    Ok(ParsedNative::new(
        "schwab.marketdata.price-history",
        digest,
        context.finish(),
        PriceHistoryResponse {
            symbol,
            empty,
            previous_close,
            previous_close_date_millis,
            candles: candles.into_boxed_slice(),
        },
    ))
}

/// Provider-native market-hours evidence; session maps remain exact scalar fields and do not
/// invent a calendar rule or session classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketHours {
    pub market_type: Box<str>,
    pub product: Box<str>,
    pub date: Box<str>,
    pub is_open: bool,
    pub category: NativeField<Box<str>>,
    pub sessions: Box<[NativeFieldEntry<Box<str>>]>,
}

pub fn parse_market_hours_response(
    bytes: &[u8],
    bounds: ParseBounds,
) -> Result<ParsedNative<Box<[MarketHours]>>, SchwabAdapterError> {
    let (value, digest, mut context) = parse_json_payload(bytes, bounds)?;
    let root = take_object(value)?;
    let mut output = Vec::new();
    for (market_type, products_value) in root {
        let products = take_object(products_value)?;
        for (product, hours_value) in products {
            context.take_record()?;
            let mut hours = take_object(hours_value)?;
            let date = remove_required_text(&mut hours, "date")?;
            let is_open = remove_required_bool(&mut hours, "isOpen")?;
            let category = remove_native_text(&mut hours, "category")?;
            let sessions = parse_named_scalar_block(
                &mut hours,
                "sessionHours",
                &format!("$.{market_type}.{product}"),
                &mut context,
            )?;
            record_remaining(&hours, &format!("$.{market_type}.{product}"), &mut context)?;
            output.push(MarketHours {
                market_type: market_type.clone().into_boxed_str(),
                product: product.into_boxed_str(),
                date: date.into_boxed_str(),
                is_open,
                category,
                sessions,
            });
        }
    }
    Ok(ParsedNative::new(
        "schwab.marketdata.market-hours",
        digest,
        context.finish(),
        output.into_boxed_slice(),
    ))
}

/// Bounded mover set with a closed scalar field projection per mover.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MoversResponse {
    pub index_symbol: NativeField<Box<str>>,
    pub frequency: NativeField<u64>,
    pub movers: Box<[Box<[NativeFieldEntry<Box<str>>]>]>,
}

pub fn parse_movers_response(
    bytes: &[u8],
    bounds: ParseBounds,
) -> Result<ParsedNative<MoversResponse>, SchwabAdapterError> {
    let (value, digest, mut context) = parse_json_payload(bytes, bounds)?;
    let mut object = take_object(value)?;
    let index_symbol = remove_native_text(&mut object, "screenersSymbol")?;
    let frequency = remove_native_u64(&mut object, "frequency")?;
    let values = object
        .remove("screeners")
        .ok_or(SchwabAdapterError::SchemaViolation)?;
    let values = values
        .as_array()
        .ok_or(SchwabAdapterError::SchemaViolation)?;
    let mut movers = Vec::new();
    for value in values {
        context.take_record()?;
        movers.push(parse_closed_named_scalars(
            value.clone(),
            &[
                "symbol",
                "description",
                "lastPrice",
                "netChange",
                "marketShare",
                "totalVolume",
                "trades",
                "netPercentChange",
            ],
            "$.screeners[]",
            &mut context,
        )?);
    }
    record_remaining(&object, "$", &mut context)?;
    Ok(ParsedNative::new(
        "schwab.marketdata.movers",
        digest,
        context.finish(),
        MoversResponse {
            index_symbol,
            frequency,
            movers: movers.into_boxed_slice(),
        },
    ))
}

/// Provider-native instrument/reference record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabInstrument {
    pub cusip: NativeField<Box<str>>,
    pub symbol: NativeField<Box<str>>,
    pub description: NativeField<Box<str>>,
    pub exchange: NativeField<Box<str>>,
    pub asset_type: NativeField<Box<str>>,
    pub fields: Box<[NativeFieldEntry<Box<str>>]>,
    pub fundamental: Box<[NativeFieldEntry<FundamentalField>]>,
}
/// Bounded instrument search/detail response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentResponse {
    instruments: Box<[SchwabInstrument]>,
}
impl InstrumentResponse {
    pub fn instruments(&self) -> &[SchwabInstrument] {
        &self.instruments
    }
}

pub fn parse_instrument_response(
    bytes: &[u8],
    bounds: ParseBounds,
) -> Result<ParsedNative<InstrumentResponse>, SchwabAdapterError> {
    let (value, digest, mut context) = parse_json_payload(bytes, bounds)?;
    let mut object = take_object(value)?;
    let values = if let Some(value) = object.remove("instruments") {
        value
            .as_array()
            .cloned()
            .ok_or(SchwabAdapterError::SchemaViolation)?
    } else {
        vec![Value::Object(std::mem::take(&mut object))]
    };
    if values.is_empty() {
        return Err(SchwabAdapterError::SchemaViolation);
    }
    let mut instruments = Vec::new();
    for value in values {
        context.take_record()?;
        let mut item = take_object(value)?;
        let cusip = remove_native_text(&mut item, "cusip")?;
        let symbol = remove_native_text(&mut item, "symbol")?;
        let description = remove_native_text(&mut item, "description")?;
        let exchange = remove_native_text(&mut item, "exchange")?;
        let asset_type = remove_native_text(&mut item, "assetType")?;
        let fundamental = parse_typed_block(
            &mut item,
            "fundamental",
            "$.instruments[]",
            &mut context,
            FundamentalField::from_key,
        )?;
        let fields = parse_known_remaining(
            &mut item,
            &["bondFactor", "bondMultiplier", "bondPrice", "type"],
            "$.instruments[]",
            &mut context,
        )?;
        instruments.push(SchwabInstrument {
            cusip,
            symbol,
            description,
            exchange,
            asset_type,
            fields,
            fundamental,
        });
    }
    record_remaining(&object, "$", &mut context)?;
    Ok(ParsedNative::new(
        "schwab.marketdata.instruments",
        digest,
        context.finish(),
        InstrumentResponse {
            instruments: instruments.into_boxed_slice(),
        },
    ))
}

fn parse_typed_block<K: Clone>(
    object: &mut Map<String, Value>,
    key: &str,
    path: &str,
    context: &mut ParseContext,
    classify: fn(&str) -> Option<K>,
) -> Result<Box<[NativeFieldEntry<K>]>, SchwabAdapterError> {
    let Some(value) = object.remove(key) else {
        return Ok(Box::default());
    };
    if value.is_null() {
        return Ok(Box::default());
    }
    parse_typed_object(value, &format!("{path}.{key}"), context, classify)
}
fn parse_typed_object<K: Clone>(
    value: Value,
    path: &str,
    context: &mut ParseContext,
    classify: fn(&str) -> Option<K>,
) -> Result<Box<[NativeFieldEntry<K>]>, SchwabAdapterError> {
    let values = take_object(value)?;
    let mut fields = Vec::new();
    for (key, value) in values {
        if let Some(name) = classify(&key) {
            fields.push(NativeFieldEntry::new(
                name,
                NativeScalar::try_from_json(value)?,
            ));
        } else {
            context.record_unknown(path, &key, &value)?;
        }
    }
    Ok(fields.into_boxed_slice())
}
fn parse_named_scalar_block(
    object: &mut Map<String, Value>,
    key: &str,
    path: &str,
    context: &mut ParseContext,
) -> Result<Box<[NativeFieldEntry<Box<str>>]>, SchwabAdapterError> {
    let Some(value) = object.remove(key) else {
        return Ok(Box::default());
    };
    let values = take_object(value)?;
    let mut fields = Vec::new();
    for (name, value) in values {
        match value {
            Value::Array(values) => {
                for (index, value) in values.into_iter().enumerate() {
                    let nested = take_object(value)?;
                    for (key, scalar) in nested {
                        fields.push(NativeFieldEntry::new(
                            format!("{name}[{index}].{key}").into_boxed_str(),
                            NativeScalar::try_from_json(scalar)?,
                        ));
                    }
                }
            }
            Value::Object(nested) => {
                for (key, scalar) in nested {
                    fields.push(NativeFieldEntry::new(
                        format!("{name}.{key}").into_boxed_str(),
                        NativeScalar::try_from_json(scalar)?,
                    ));
                }
            }
            scalar => fields.push(NativeFieldEntry::new(
                name.into_boxed_str(),
                NativeScalar::try_from_json(scalar)?,
            )),
        }
    }
    let _ = (path, context);
    Ok(fields.into_boxed_slice())
}
fn parse_closed_named_scalars(
    value: Value,
    allowed: &[&str],
    path: &str,
    context: &mut ParseContext,
) -> Result<Box<[NativeFieldEntry<Box<str>>]>, SchwabAdapterError> {
    let mut object = take_object(value)?;
    let mut output = Vec::new();
    for key in allowed {
        if let Some(value) = object.remove(*key) {
            output.push(NativeFieldEntry::new(
                (*key).to_owned().into_boxed_str(),
                NativeScalar::try_from_json(value)?,
            ));
        }
    }
    record_remaining(&object, path, context)?;
    Ok(output.into_boxed_slice())
}
fn parse_known_remaining(
    object: &mut Map<String, Value>,
    keys: &[&str],
    path: &str,
    context: &mut ParseContext,
) -> Result<Box<[NativeFieldEntry<Box<str>>]>, SchwabAdapterError> {
    let mut output = Vec::new();
    for key in keys {
        if let Some(value) = object.remove(*key) {
            output.push(NativeFieldEntry::new(
                (*key).to_owned().into_boxed_str(),
                NativeScalar::try_from_json(value)?,
            ));
        }
    }
    record_remaining(object, path, context)?;
    Ok(output.into_boxed_slice())
}
fn record_remaining(
    object: &Map<String, Value>,
    path: &str,
    context: &mut ParseContext,
) -> Result<(), SchwabAdapterError> {
    for (key, value) in object {
        context.record_unknown(path, key, value)?;
    }
    Ok(())
}
fn remove_required_identifier(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<ProviderIdentifier, SchwabAdapterError> {
    ProviderIdentifier::try_new(remove_required_text(object, key)?)
}
fn remove_optional_text(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<Box<str>>, SchwabAdapterError> {
    match object.remove(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(value.into_boxed_str())),
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
fn remove_required_text(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<String, SchwabAdapterError> {
    match object.remove(key) {
        Some(Value::String(value)) if !value.is_empty() => Ok(value),
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
fn remove_required_bool(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<bool, SchwabAdapterError> {
    match object.remove(key) {
        Some(Value::Bool(value)) => Ok(value),
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
fn remove_required_u64(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<u64, SchwabAdapterError> {
    match object.remove(key) {
        Some(Value::Number(value)) => value.as_u64().ok_or(SchwabAdapterError::SchemaViolation),
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
fn remove_required_number(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<NativeNumber, SchwabAdapterError> {
    match object.remove(key) {
        Some(Value::Number(value)) => Ok(NativeNumber::from_json(value)),
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
fn remove_native_text(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<NativeField<Box<str>>, SchwabAdapterError> {
    match object.remove(key) {
        None => Ok(NativeField::Absent),
        Some(Value::Null) => Ok(NativeField::Null),
        Some(Value::String(value)) if !value.is_empty() => {
            Ok(NativeField::Value(value.into_boxed_str()))
        }
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
fn remove_native_bool(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<NativeField<bool>, SchwabAdapterError> {
    match object.remove(key) {
        None => Ok(NativeField::Absent),
        Some(Value::Null) => Ok(NativeField::Null),
        Some(Value::Bool(value)) => Ok(NativeField::Value(value)),
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
fn remove_native_u64(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<NativeField<u64>, SchwabAdapterError> {
    match object.remove(key) {
        None => Ok(NativeField::Absent),
        Some(Value::Null) => Ok(NativeField::Null),
        Some(Value::Number(value)) => value
            .as_u64()
            .map(NativeField::Value)
            .ok_or(SchwabAdapterError::SchemaViolation),
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
fn remove_native_number(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<NativeField<NativeNumber>, SchwabAdapterError> {
    match object.remove(key) {
        None => Ok(NativeField::Absent),
        Some(Value::Null) => Ok(NativeField::Null),
        Some(Value::Number(value)) => Ok(NativeField::Value(NativeNumber::from_json(value))),
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
