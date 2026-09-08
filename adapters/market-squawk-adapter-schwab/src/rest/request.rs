use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU16;

use chrono::NaiveDate;
use rust_decimal::Decimal;
use url::{Position, Url};

use crate::{
    HttpMethod, RequestAdmission, SCHWAB_MARKET_DATA_BASE, SCHWAB_USER_PREFERENCE_ENDPOINT,
    SchwabAdapterError,
};

const MAX_IDENTIFIER_BYTES: usize = 128;

/// Bounded Schwab symbol or provider identifier.
///
/// The byte bound is an input/memory safety rule, not a provider batch or symbol-count limit.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProviderIdentifier(Box<str>);

impl ProviderIdentifier {
    /// Validates a single provider identifier without assuming an asset class.
    pub fn try_new(value: impl Into<String>) -> Result<Self, SchwabAdapterError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_IDENTIFIER_BYTES
            || !value.is_ascii()
            || value.bytes().any(|byte| {
                byte.is_ascii_control() || matches!(byte, b',' | b'&' | b'=' | b'?' | b'#')
            })
        {
            return Err(SchwabAdapterError::InvalidInput);
        }
        Ok(Self(value.into_boxed_str()))
    }

    /// Exact provider identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ProviderIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ProviderIdentifier")
            .field(&self.0)
            .finish()
    }
}

/// Closed read-only HTTP route allowlist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadOnlyRoute {
    /// `GET /quotes`.
    Quotes,
    /// `GET /{symbol}/quotes`.
    SingleQuote,
    /// `GET /chains`.
    Chains,
    /// `GET /expirationchain`.
    ExpirationChain,
    /// `GET /pricehistory`.
    PriceHistory,
    /// `GET /movers/{symbol}`.
    Movers,
    /// `GET /markets`.
    Markets,
    /// `GET /markets/{market}`.
    SingleMarket,
    /// `GET /instruments`.
    Instruments,
    /// `GET /instruments/{cusip}`.
    InstrumentByCusip,
    /// Minimum read-only `GET /trader/v1/userPreference` Streamer bootstrap.
    UserPreference,
}

impl ReadOnlyRoute {
    /// Classifies and validates an exact GET URL against the allowlist.
    pub fn classify(method: HttpMethod, url: &str) -> Result<Self, SchwabAdapterError> {
        if method != HttpMethod::Get {
            return Err(SchwabAdapterError::RouteNotAllowed);
        }
        if url == SCHWAB_USER_PREFERENCE_ENDPOINT {
            return Ok(Self::UserPreference);
        }
        let url = Url::parse(url).map_err(|_| SchwabAdapterError::RouteNotAllowed)?;
        validate_market_origin(&url)?;
        let segments = url
            .path_segments()
            .ok_or(SchwabAdapterError::RouteNotAllowed)?
            .collect::<Vec<_>>();
        if segments.len() < 3 || segments[0..2] != ["marketdata", "v1"] {
            return Err(SchwabAdapterError::RouteNotAllowed);
        }
        let tail = &segments[2..];
        let route = match tail {
            ["quotes"] => Self::Quotes,
            [symbol, "quotes"] if !symbol.is_empty() => Self::SingleQuote,
            ["chains"] => Self::Chains,
            ["expirationchain"] => Self::ExpirationChain,
            ["pricehistory"] => Self::PriceHistory,
            ["movers", symbol] if !symbol.is_empty() => Self::Movers,
            ["markets"] => Self::Markets,
            ["markets", market] if !market.is_empty() => Self::SingleMarket,
            ["instruments"] => Self::Instruments,
            ["instruments", cusip] if !cusip.is_empty() => Self::InstrumentByCusip,
            _ => return Err(SchwabAdapterError::RouteNotAllowed),
        };
        validate_query(route, &url)?;
        Ok(route)
    }
}

/// Exact typed GET request with no bearer token embedded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadOnlyRequest {
    route: ReadOnlyRoute,
    url: Url,
    requested_items: usize,
}

impl ReadOnlyRequest {
    fn try_new(
        route: ReadOnlyRoute,
        url: Url,
        requested_items: usize,
        admission: RequestAdmission,
    ) -> Result<Self, SchwabAdapterError> {
        if requested_items == 0 || requested_items > admission.max_items() {
            return Err(SchwabAdapterError::RequestNotAdmitted);
        }
        let request_target = &url[Position::BeforePath..Position::AfterQuery];
        if request_target.as_bytes().len() > admission.max_request_bytes()
            || ReadOnlyRoute::classify(HttpMethod::Get, url.as_str())? != route
        {
            return Err(SchwabAdapterError::RequestNotAdmitted);
        }
        Ok(Self {
            route,
            url,
            requested_items,
        })
    }

    /// Exact method, always GET.
    pub const fn method(&self) -> HttpMethod {
        HttpMethod::Get
    }

    /// Closed route identity.
    pub const fn route(&self) -> ReadOnlyRoute {
        self.route
    }

    /// Allowlist-validated URL without credentials.
    pub fn url(&self) -> &str {
        self.url.as_str()
    }

    /// Exact encoded origin-form target sent on the HTTP request line.
    pub fn request_target(&self) -> &str {
        &self.url[Position::BeforePath..Position::AfterQuery]
    }

    pub(crate) const fn wire_url(&self) -> &Url {
        &self.url
    }

    /// Same-unit work requested, for runtime accounting.
    pub const fn requested_items(&self) -> usize {
        self.requested_items
    }

    /// Builds the sole admitted non-market-data bootstrap request.
    pub fn user_preference(admission: RequestAdmission) -> Result<Self, SchwabAdapterError> {
        let url = Url::parse(SCHWAB_USER_PREFERENCE_ENDPOINT)
            .map_err(|_| SchwabAdapterError::RouteNotAllowed)?;
        Self::try_new(ReadOnlyRoute::UserPreference, url, 1, admission)
    }
}

/// Optional quote response families.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum QuoteField {
    /// Current quote components.
    Quote,
    /// Provider reference record.
    Reference,
    /// Provider fundamental record.
    Fundamental,
    /// Regular-session components.
    Regular,
}

impl QuoteField {
    const fn wire(self) -> &'static str {
        match self {
            Self::Quote => "quote",
            Self::Reference => "reference",
            Self::Fundamental => "fundamental",
            Self::Regular => "regular",
        }
    }
}

/// Bounded batch quote request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuoteRequest(ReadOnlyRequest);

impl QuoteRequest {
    /// Builds `GET /quotes` for a duplicate-free runtime-admitted symbol set.
    pub fn try_new(
        symbols: Vec<ProviderIdentifier>,
        fields: Vec<QuoteField>,
        indicative: Option<bool>,
        admission: RequestAdmission,
    ) -> Result<Self, SchwabAdapterError> {
        validate_unique_symbols(&symbols, admission)?;
        let mut url = market_url(&["quotes"])?;
        append_symbols(&mut url, "symbols", &symbols);
        append_quote_fields(&mut url, fields)?;
        if let Some(indicative) = indicative {
            url.query_pairs_mut()
                .append_pair("indicative", bool_wire(indicative));
        }
        Ok(Self(ReadOnlyRequest::try_new(
            ReadOnlyRoute::Quotes,
            url,
            symbols.len(),
            admission,
        )?))
    }

    /// Exact HTTP request.
    pub const fn request(&self) -> &ReadOnlyRequest {
        &self.0
    }
}

/// One-symbol quote request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SingleQuoteRequest(ReadOnlyRequest);

impl SingleQuoteRequest {
    /// Builds `GET /{symbol}/quotes`.
    pub fn try_new(
        symbol: ProviderIdentifier,
        fields: Vec<QuoteField>,
        indicative: Option<bool>,
        admission: RequestAdmission,
    ) -> Result<Self, SchwabAdapterError> {
        let mut url = market_url(&[symbol.as_str(), "quotes"])?;
        append_quote_fields(&mut url, fields)?;
        if let Some(indicative) = indicative {
            url.query_pairs_mut()
                .append_pair("indicative", bool_wire(indicative));
        }
        Ok(Self(ReadOnlyRequest::try_new(
            ReadOnlyRoute::SingleQuote,
            url,
            1,
            admission,
        )?))
    }

    /// Exact HTTP request.
    pub const fn request(&self) -> &ReadOnlyRequest {
        &self.0
    }
}

/// Option contract-side filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChainContractType {
    /// Calls only.
    Call,
    /// Puts only.
    Put,
    /// Both sides.
    All,
}

impl ChainContractType {
    const fn wire(self) -> &'static str {
        match self {
            Self::Call => "CALL",
            Self::Put => "PUT",
            Self::All => "ALL",
        }
    }
}

/// Schwab option-chain strategy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChainStrategy {
    Single,
    Analytical,
    Covered,
    Vertical,
    Calendar,
    Strangle,
    Straddle,
    Butterfly,
    Condor,
    Diagonal,
    Collar,
    Roll,
}

impl ChainStrategy {
    const fn wire(self) -> &'static str {
        match self {
            Self::Single => "SINGLE",
            Self::Analytical => "ANALYTICAL",
            Self::Covered => "COVERED",
            Self::Vertical => "VERTICAL",
            Self::Calendar => "CALENDAR",
            Self::Strangle => "STRANGLE",
            Self::Straddle => "STRADDLE",
            Self::Butterfly => "BUTTERFLY",
            Self::Condor => "CONDOR",
            Self::Diagonal => "DIAGONAL",
            Self::Collar => "COLLAR",
            Self::Roll => "ROLL",
        }
    }
}

/// Expiration-month filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpirationMonth {
    Jan,
    Feb,
    Mar,
    Apr,
    May,
    Jun,
    Jul,
    Aug,
    Sep,
    Oct,
    Nov,
    Dec,
    All,
}

impl ExpirationMonth {
    const fn wire(self) -> &'static str {
        match self {
            Self::Jan => "JAN",
            Self::Feb => "FEB",
            Self::Mar => "MAR",
            Self::Apr => "APR",
            Self::May => "MAY",
            Self::Jun => "JUN",
            Self::Jul => "JUL",
            Self::Aug => "AUG",
            Self::Sep => "SEP",
            Self::Oct => "OCT",
            Self::Nov => "NOV",
            Self::Dec => "DEC",
            Self::All => "ALL",
        }
    }
}

/// Standard/non-standard option filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptionType {
    Standard,
    NonStandard,
    All,
}

impl OptionType {
    const fn wire(self) -> &'static str {
        match self {
            Self::Standard => "S",
            Self::NonStandard => "NS",
            Self::All => "ALL",
        }
    }
}

/// Typed option-chain request builder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainRequest {
    symbol: ProviderIdentifier,
    contract_type: Option<ChainContractType>,
    strike_count: Option<NonZeroU16>,
    include_underlying_quote: Option<bool>,
    strategy: Option<ChainStrategy>,
    interval: Option<Decimal>,
    strike: Option<Decimal>,
    from_date: Option<NaiveDate>,
    to_date: Option<NaiveDate>,
    volatility: Option<Decimal>,
    underlying_price: Option<Decimal>,
    interest_rate: Option<Decimal>,
    days_to_expiration: Option<NonZeroU16>,
    expiration_month: Option<ExpirationMonth>,
    option_type: Option<OptionType>,
}

impl ChainRequest {
    /// Starts a chain request for one underlying.
    pub const fn new(symbol: ProviderIdentifier) -> Self {
        Self {
            symbol,
            contract_type: None,
            strike_count: None,
            include_underlying_quote: None,
            strategy: None,
            interval: None,
            strike: None,
            from_date: None,
            to_date: None,
            volatility: None,
            underlying_price: None,
            interest_rate: None,
            days_to_expiration: None,
            expiration_month: None,
            option_type: None,
        }
    }

    pub fn contract_type(mut self, value: ChainContractType) -> Self {
        self.contract_type = Some(value);
        self
    }
    pub fn strike_count(mut self, value: NonZeroU16) -> Self {
        self.strike_count = Some(value);
        self
    }
    pub fn include_underlying_quote(mut self, value: bool) -> Self {
        self.include_underlying_quote = Some(value);
        self
    }
    pub fn strategy(mut self, value: ChainStrategy) -> Self {
        self.strategy = Some(value);
        self
    }
    pub fn interval(mut self, value: Decimal) -> Self {
        self.interval = Some(value);
        self
    }
    pub fn strike(mut self, value: Decimal) -> Self {
        self.strike = Some(value);
        self
    }
    pub fn date_range(
        mut self,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Self, SchwabAdapterError> {
        if from > to {
            return Err(SchwabAdapterError::InvalidInput);
        }
        self.from_date = Some(from);
        self.to_date = Some(to);
        Ok(self)
    }
    pub fn volatility(mut self, value: Decimal) -> Self {
        self.volatility = Some(value);
        self
    }
    pub fn underlying_price(mut self, value: Decimal) -> Self {
        self.underlying_price = Some(value);
        self
    }
    pub fn interest_rate(mut self, value: Decimal) -> Self {
        self.interest_rate = Some(value);
        self
    }
    pub fn days_to_expiration(mut self, value: NonZeroU16) -> Self {
        self.days_to_expiration = Some(value);
        self
    }
    pub fn expiration_month(mut self, value: ExpirationMonth) -> Self {
        self.expiration_month = Some(value);
        self
    }
    pub fn option_type(mut self, value: OptionType) -> Self {
        self.option_type = Some(value);
        self
    }

    /// Encodes the allowlisted request under the current runtime admission.
    pub fn build(
        &self,
        admission: RequestAdmission,
    ) -> Result<ReadOnlyRequest, SchwabAdapterError> {
        if self.interval.is_some_and(|value| value.is_sign_negative())
            || self.strike.is_some_and(|value| value.is_sign_negative())
            || self
                .volatility
                .is_some_and(|value| value.is_sign_negative())
            || self
                .underlying_price
                .is_some_and(|value| value.is_sign_negative())
        {
            return Err(SchwabAdapterError::InvalidInput);
        }
        let mut url = market_url(&["chains"])?;
        let mut query = url.query_pairs_mut();
        query.append_pair("symbol", self.symbol.as_str());
        append_enum(
            &mut query,
            "contractType",
            self.contract_type.map(ChainContractType::wire),
        );
        append_display(&mut query, "strikeCount", self.strike_count);
        append_bool(
            &mut query,
            "includeUnderlyingQuote",
            self.include_underlying_quote,
        );
        append_enum(
            &mut query,
            "strategy",
            self.strategy.map(ChainStrategy::wire),
        );
        append_decimal(&mut query, "interval", self.interval);
        append_decimal(&mut query, "strike", self.strike);
        append_date(&mut query, "fromDate", self.from_date);
        append_date(&mut query, "toDate", self.to_date);
        append_decimal(&mut query, "volatility", self.volatility);
        append_decimal(&mut query, "underlyingPrice", self.underlying_price);
        append_decimal(&mut query, "interestRate", self.interest_rate);
        append_display(&mut query, "daysToExpiration", self.days_to_expiration);
        append_enum(
            &mut query,
            "expMonth",
            self.expiration_month.map(ExpirationMonth::wire),
        );
        append_enum(
            &mut query,
            "optionType",
            self.option_type.map(OptionType::wire),
        );
        drop(query);
        ReadOnlyRequest::try_new(ReadOnlyRoute::Chains, url, 1, admission)
    }
}

/// Typed expiration-inventory request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpirationChainRequest {
    symbol: ProviderIdentifier,
    contract_type: Option<ChainContractType>,
    expiration_month: Option<ExpirationMonth>,
    option_type: Option<OptionType>,
    from_date: Option<NaiveDate>,
    to_date: Option<NaiveDate>,
}

impl ExpirationChainRequest {
    pub const fn new(symbol: ProviderIdentifier) -> Self {
        Self {
            symbol,
            contract_type: None,
            expiration_month: None,
            option_type: None,
            from_date: None,
            to_date: None,
        }
    }
    pub fn contract_type(mut self, value: ChainContractType) -> Self {
        self.contract_type = Some(value);
        self
    }
    pub fn expiration_month(mut self, value: ExpirationMonth) -> Self {
        self.expiration_month = Some(value);
        self
    }
    pub fn option_type(mut self, value: OptionType) -> Self {
        self.option_type = Some(value);
        self
    }
    pub fn date_range(
        mut self,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Self, SchwabAdapterError> {
        if from > to {
            return Err(SchwabAdapterError::InvalidInput);
        }
        self.from_date = Some(from);
        self.to_date = Some(to);
        Ok(self)
    }
    pub fn build(
        &self,
        admission: RequestAdmission,
    ) -> Result<ReadOnlyRequest, SchwabAdapterError> {
        let mut url = market_url(&["expirationchain"])?;
        let mut query = url.query_pairs_mut();
        query.append_pair("symbol", self.symbol.as_str());
        append_enum(
            &mut query,
            "contractType",
            self.contract_type.map(ChainContractType::wire),
        );
        append_enum(
            &mut query,
            "expMonth",
            self.expiration_month.map(ExpirationMonth::wire),
        );
        append_enum(
            &mut query,
            "optionType",
            self.option_type.map(OptionType::wire),
        );
        append_date(&mut query, "fromDate", self.from_date);
        append_date(&mut query, "toDate", self.to_date);
        drop(query);
        ReadOnlyRequest::try_new(ReadOnlyRoute::ExpirationChain, url, 1, admission)
    }
}

/// Price-history period family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PriceHistoryPeriodType {
    Day,
    Month,
    Year,
    YearToDate,
}
impl PriceHistoryPeriodType {
    const fn wire(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::Month => "month",
            Self::Year => "year",
            Self::YearToDate => "ytd",
        }
    }
}

/// Price-history frequency family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PriceHistoryFrequencyType {
    Minute,
    Daily,
    Weekly,
    Monthly,
}
impl PriceHistoryFrequencyType {
    const fn wire(self) -> &'static str {
        match self {
            Self::Minute => "minute",
            Self::Daily => "daily",
            Self::Weekly => "weekly",
            Self::Monthly => "monthly",
        }
    }
}

/// Nonzero provider frequency value, validated against its typed family by the caller's plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PriceHistoryFrequency(NonZeroU16);
impl PriceHistoryFrequency {
    pub const fn new(value: NonZeroU16) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

/// Typed price-history request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PriceHistoryRequest {
    symbol: ProviderIdentifier,
    period_type: Option<PriceHistoryPeriodType>,
    period: Option<NonZeroU16>,
    frequency_type: Option<PriceHistoryFrequencyType>,
    frequency: Option<PriceHistoryFrequency>,
    start_millis: Option<u64>,
    end_millis: Option<u64>,
    extended_hours: Option<bool>,
    previous_close: Option<bool>,
}

impl PriceHistoryRequest {
    pub const fn new(symbol: ProviderIdentifier) -> Self {
        Self {
            symbol,
            period_type: None,
            period: None,
            frequency_type: None,
            frequency: None,
            start_millis: None,
            end_millis: None,
            extended_hours: None,
            previous_close: None,
        }
    }
    pub fn period(mut self, kind: PriceHistoryPeriodType, value: NonZeroU16) -> Self {
        self.period_type = Some(kind);
        self.period = Some(value);
        self
    }
    pub fn frequency(
        mut self,
        kind: PriceHistoryFrequencyType,
        value: PriceHistoryFrequency,
    ) -> Self {
        self.frequency_type = Some(kind);
        self.frequency = Some(value);
        self
    }
    pub fn range_millis(mut self, start: u64, end: u64) -> Result<Self, SchwabAdapterError> {
        if start >= end {
            return Err(SchwabAdapterError::InvalidInput);
        }
        self.start_millis = Some(start);
        self.end_millis = Some(end);
        Ok(self)
    }
    pub fn extended_hours(mut self, value: bool) -> Self {
        self.extended_hours = Some(value);
        self
    }
    pub fn previous_close(mut self, value: bool) -> Self {
        self.previous_close = Some(value);
        self
    }
    pub fn build(
        &self,
        admission: RequestAdmission,
    ) -> Result<ReadOnlyRequest, SchwabAdapterError> {
        if self.period_type.is_some() != self.period.is_some()
            || self.frequency_type.is_some() != self.frequency.is_some()
            || self.start_millis.is_some() != self.end_millis.is_some()
        {
            return Err(SchwabAdapterError::InvalidInput);
        }
        let mut url = market_url(&["pricehistory"])?;
        let mut query = url.query_pairs_mut();
        query.append_pair("symbol", self.symbol.as_str());
        append_enum(
            &mut query,
            "periodType",
            self.period_type.map(PriceHistoryPeriodType::wire),
        );
        append_display(&mut query, "period", self.period);
        append_enum(
            &mut query,
            "frequencyType",
            self.frequency_type.map(PriceHistoryFrequencyType::wire),
        );
        if let Some(value) = self.frequency {
            query.append_pair("frequency", &value.get().to_string());
        }
        append_display(&mut query, "startDate", self.start_millis);
        append_display(&mut query, "endDate", self.end_millis);
        append_bool(&mut query, "needExtendedHoursData", self.extended_hours);
        append_bool(&mut query, "needPreviousClose", self.previous_close);
        drop(query);
        ReadOnlyRequest::try_new(ReadOnlyRoute::PriceHistory, url, 1, admission)
    }
}

/// Schwab market-hours identifier.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MarketId {
    Equity,
    Option,
    Bond,
    Future,
    Forex,
}
impl MarketId {
    const fn wire(self) -> &'static str {
        match self {
            Self::Equity => "equity",
            Self::Option => "option",
            Self::Bond => "bond",
            Self::Future => "future",
            Self::Forex => "forex",
        }
    }
}

/// One-market hours request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SingleMarketRequest(ReadOnlyRequest);
impl SingleMarketRequest {
    pub fn try_new(
        market: MarketId,
        date: Option<NaiveDate>,
        admission: RequestAdmission,
    ) -> Result<Self, SchwabAdapterError> {
        let mut url = market_url(&["markets", market.wire()])?;
        if let Some(date) = date {
            url.query_pairs_mut().append_pair("date", &date.to_string());
        }
        Ok(Self(ReadOnlyRequest::try_new(
            ReadOnlyRoute::SingleMarket,
            url,
            1,
            admission,
        )?))
    }
    pub const fn request(&self) -> &ReadOnlyRequest {
        &self.0
    }
}

/// Builds a multi-market hours request.
pub fn build_market_hours_request(
    markets: Vec<MarketId>,
    date: Option<NaiveDate>,
    admission: RequestAdmission,
) -> Result<ReadOnlyRequest, SchwabAdapterError> {
    if markets.is_empty()
        || markets.len() > admission.max_items()
        || markets.iter().collect::<BTreeSet<_>>().len() != markets.len()
    {
        return Err(SchwabAdapterError::RequestNotAdmitted);
    }
    let mut url = market_url(&["markets"])?;
    let joined = markets
        .iter()
        .map(|market| market.wire())
        .collect::<Vec<_>>()
        .join(",");
    url.query_pairs_mut().append_pair("markets", &joined);
    if let Some(date) = date {
        url.query_pairs_mut().append_pair("date", &date.to_string());
    }
    ReadOnlyRequest::try_new(ReadOnlyRoute::Markets, url, markets.len(), admission)
}

/// Movers sort direction/magnitude.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MoverSort {
    Volume,
    Trades,
    PercentChangeUp,
    PercentChangeDown,
}
impl MoverSort {
    const fn wire(self) -> &'static str {
        match self {
            Self::Volume => "VOLUME",
            Self::Trades => "TRADES",
            Self::PercentChangeUp => "PERCENT_CHANGE_UP",
            Self::PercentChangeDown => "PERCENT_CHANGE_DOWN",
        }
    }
}
/// Movers frequency window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MoverFrequency {
    Zero,
    One,
    Five,
    Ten,
    Thirty,
    Sixty,
}
impl MoverFrequency {
    const fn wire(self) -> &'static str {
        match self {
            Self::Zero => "0",
            Self::One => "1",
            Self::Five => "5",
            Self::Ten => "10",
            Self::Thirty => "30",
            Self::Sixty => "60",
        }
    }
}

pub fn build_movers_request(
    symbol: ProviderIdentifier,
    sort: Option<MoverSort>,
    frequency: Option<MoverFrequency>,
    admission: RequestAdmission,
) -> Result<ReadOnlyRequest, SchwabAdapterError> {
    let mut url = market_url(&["movers", symbol.as_str()])?;
    let mut query = url.query_pairs_mut();
    append_enum(&mut query, "sort", sort.map(MoverSort::wire));
    append_enum(&mut query, "frequency", frequency.map(MoverFrequency::wire));
    drop(query);
    ReadOnlyRequest::try_new(ReadOnlyRoute::Movers, url, 1, admission)
}

/// Instrument search projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstrumentProjection {
    SymbolSearch,
    SymbolRegex,
    DescriptionSearch,
    DescriptionRegex,
    Search,
    Fundamental,
}
impl InstrumentProjection {
    const fn wire(self) -> &'static str {
        match self {
            Self::SymbolSearch => "symbol-search",
            Self::SymbolRegex => "symbol-regex",
            Self::DescriptionSearch => "desc-search",
            Self::DescriptionRegex => "desc-regex",
            Self::Search => "search",
            Self::Fundamental => "fundamental",
        }
    }
}

pub fn build_instrument_search_request(
    search: ProviderIdentifier,
    projection: InstrumentProjection,
    admission: RequestAdmission,
) -> Result<ReadOnlyRequest, SchwabAdapterError> {
    let mut url = market_url(&["instruments"])?;
    url.query_pairs_mut()
        .append_pair("symbol", search.as_str())
        .append_pair("projection", projection.wire());
    ReadOnlyRequest::try_new(ReadOnlyRoute::Instruments, url, 1, admission)
}

pub fn build_instrument_by_cusip_request(
    cusip: &str,
    admission: RequestAdmission,
) -> Result<ReadOnlyRequest, SchwabAdapterError> {
    if cusip.len() != 9
        || !cusip.is_ascii()
        || !cusip
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'*' | b'@' | b'#'))
    {
        return Err(SchwabAdapterError::InvalidInput);
    }
    let url = market_url(&["instruments", cusip])?;
    ReadOnlyRequest::try_new(ReadOnlyRoute::InstrumentByCusip, url, 1, admission)
}

fn market_url(segments: &[&str]) -> Result<Url, SchwabAdapterError> {
    let mut url =
        Url::parse(SCHWAB_MARKET_DATA_BASE).map_err(|_| SchwabAdapterError::RouteNotAllowed)?;
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|_| SchwabAdapterError::RouteNotAllowed)?;
        path.pop_if_empty();
        for segment in segments {
            path.push(segment);
        }
    }
    Ok(url)
}

fn validate_market_origin(url: &Url) -> Result<(), SchwabAdapterError> {
    if url.scheme() != "https"
        || url.host_str() != Some("api.schwabapi.com")
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(SchwabAdapterError::RouteNotAllowed);
    }
    Ok(())
}

fn validate_query(route: ReadOnlyRoute, url: &Url) -> Result<(), SchwabAdapterError> {
    let allowed: &[&str] = match route {
        ReadOnlyRoute::Quotes => &["symbols", "fields", "indicative"],
        ReadOnlyRoute::SingleQuote => &["fields", "indicative"],
        ReadOnlyRoute::Chains => &[
            "symbol",
            "contractType",
            "strikeCount",
            "includeUnderlyingQuote",
            "strategy",
            "interval",
            "strike",
            "fromDate",
            "toDate",
            "volatility",
            "underlyingPrice",
            "interestRate",
            "daysToExpiration",
            "expMonth",
            "optionType",
        ],
        ReadOnlyRoute::ExpirationChain => &[
            "symbol",
            "contractType",
            "expMonth",
            "optionType",
            "fromDate",
            "toDate",
        ],
        ReadOnlyRoute::PriceHistory => &[
            "symbol",
            "periodType",
            "period",
            "frequencyType",
            "frequency",
            "startDate",
            "endDate",
            "needExtendedHoursData",
            "needPreviousClose",
        ],
        ReadOnlyRoute::Movers => &["sort", "frequency"],
        ReadOnlyRoute::Markets => &["markets", "date"],
        ReadOnlyRoute::SingleMarket => &["date"],
        ReadOnlyRoute::Instruments => &["symbol", "projection"],
        ReadOnlyRoute::InstrumentByCusip | ReadOnlyRoute::UserPreference => &[],
    };
    let mut seen = BTreeSet::new();
    for (key, _) in url.query_pairs() {
        if !allowed.contains(&key.as_ref()) || !seen.insert(key.into_owned()) {
            return Err(SchwabAdapterError::RouteNotAllowed);
        }
    }
    let required = match route {
        ReadOnlyRoute::Quotes => Some("symbols"),
        ReadOnlyRoute::Chains
        | ReadOnlyRoute::ExpirationChain
        | ReadOnlyRoute::PriceHistory
        | ReadOnlyRoute::Instruments => Some("symbol"),
        ReadOnlyRoute::Markets => Some("markets"),
        _ => None,
    };
    if required.is_some_and(|key| !seen.contains(key)) {
        return Err(SchwabAdapterError::RouteNotAllowed);
    }
    Ok(())
}

fn validate_unique_symbols(
    symbols: &[ProviderIdentifier],
    admission: RequestAdmission,
) -> Result<(), SchwabAdapterError> {
    if symbols.is_empty()
        || symbols.len() > admission.max_items()
        || symbols.iter().collect::<BTreeSet<_>>().len() != symbols.len()
    {
        return Err(SchwabAdapterError::RequestNotAdmitted);
    }
    Ok(())
}

fn append_symbols(url: &mut Url, name: &str, symbols: &[ProviderIdentifier]) {
    let joined = symbols
        .iter()
        .map(ProviderIdentifier::as_str)
        .collect::<Vec<_>>()
        .join(",");
    url.query_pairs_mut().append_pair(name, &joined);
}
fn append_quote_fields(url: &mut Url, fields: Vec<QuoteField>) -> Result<(), SchwabAdapterError> {
    if fields.is_empty() {
        return Ok(());
    }
    let unique = fields.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != fields.len() {
        return Err(SchwabAdapterError::InvalidInput);
    }
    let joined = fields
        .iter()
        .map(|field| field.wire())
        .collect::<Vec<_>>()
        .join(",");
    url.query_pairs_mut().append_pair("fields", &joined);
    Ok(())
}
fn bool_wire(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}
fn append_bool(
    query: &mut url::form_urlencoded::Serializer<'_, url::UrlQuery<'_>>,
    name: &str,
    value: Option<bool>,
) {
    if let Some(value) = value {
        query.append_pair(name, bool_wire(value));
    }
}
fn append_enum(
    query: &mut url::form_urlencoded::Serializer<'_, url::UrlQuery<'_>>,
    name: &str,
    value: Option<&str>,
) {
    if let Some(value) = value {
        query.append_pair(name, value);
    }
}
fn append_display<T: fmt::Display>(
    query: &mut url::form_urlencoded::Serializer<'_, url::UrlQuery<'_>>,
    name: &str,
    value: Option<T>,
) {
    if let Some(value) = value {
        query.append_pair(name, &value.to_string());
    }
}
fn append_decimal(
    query: &mut url::form_urlencoded::Serializer<'_, url::UrlQuery<'_>>,
    name: &str,
    value: Option<Decimal>,
) {
    if let Some(value) = value {
        query.append_pair(name, &value.normalize().to_string());
    }
}
fn append_date(
    query: &mut url::form_urlencoded::Serializer<'_, url::UrlQuery<'_>>,
    name: &str,
    value: Option<NaiveDate>,
) {
    if let Some(value) = value {
        query.append_pair(name, &value.to_string());
    }
}
