use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::YahooAdapterError;

/// Why a user-visible operation may request optional Yahoo enrichment.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExplicitDemandPurpose {
    ViewedInstrument,
    Watchlist,
    TargetedHistory,
    IndexOrFundDetail,
    OptionsEnrichment,
    SearchOrLookup,
    CrossSourceValidation,
}

/// Proof that work originates from an explicit operation rather than a recurring scheduler.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExplicitDemand {
    operation_id: String,
    requested_at_unix_ms: i64,
    purpose: ExplicitDemandPurpose,
}

impl ExplicitDemand {
    pub fn new(
        operation_id: impl Into<String>,
        requested_at_unix_ms: i64,
        purpose: ExplicitDemandPurpose,
        max_string_bytes: usize,
    ) -> Result<Self, YahooAdapterError> {
        let operation_id = operation_id.into();
        if operation_id.is_empty() {
            return Err(YahooAdapterError::EmptyDemandId);
        }
        if operation_id.len() > max_string_bytes {
            return Err(YahooAdapterError::DemandIdTooLong);
        }
        if operation_id.chars().any(char::is_control) {
            return Err(YahooAdapterError::EmptyDemandId);
        }
        Ok(Self {
            operation_id,
            requested_at_unix_ms,
            purpose,
        })
    }

    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    pub const fn requested_at_unix_ms(&self) -> i64 {
        self.requested_at_unix_ms
    }

    pub const fn purpose(&self) -> ExplicitDemandPurpose {
        self.purpose
    }
}

/// Selected non-crypto asset families for this enrichment lane.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum YahooAssetClass {
    Equity,
    Etf,
    Index,
    MutualFund,
    OptionUnderlying,
    ReferenceHint,
}

/// Provider-native symbol validated for safe path/query construction.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct YahooSymbol(String);

impl YahooSymbol {
    pub fn parse(
        value: impl Into<String>,
        max_string_bytes: usize,
    ) -> Result<Self, YahooAdapterError> {
        let value = value.into();
        if value.is_empty() {
            return Err(YahooAdapterError::EmptySymbol);
        }
        if value.len() > max_string_bytes {
            return Err(YahooAdapterError::SymbolTooLong);
        }
        if value.chars().any(|character| {
            character.is_control()
                || character.is_whitespace()
                || matches!(character, '/' | '?' | '&' | '#' | ',' | '\\')
        }) {
            return Err(YahooAdapterError::InvalidSymbol);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooTarget {
    pub symbol: YahooSymbol,
    pub asset_class: YahooAssetClass,
}

/// Provider field presence is retained instead of collapsing absent, null, and malformed values.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", content = "value", rename_all = "kebab-case")]
pub enum ProviderField<T> {
    Missing,
    Null,
    Value(T),
    Invalid,
}

impl<T> ProviderField<T> {
    pub const fn is_value(&self) -> bool {
        matches!(self, Self::Value(_))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EvidenceAuthority {
    ExperimentalSupplementOnly,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum YahooEnrichmentState {
    Experimental,
    Degraded,
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum QualityIssue {
    MissingProviderTimestamp,
    Delayed { seconds: u32 },
    OneSidedQuote,
    NonPositiveQuoteSide,
    CrossedQuote,
    PartialProviderResult,
    MissingRequestedSymbol { symbol: YahooSymbol },
    InvalidField { field: String },
    ArrayLengthMismatch { field: String },
    UnsupportedAsset { quote_type: String },
    ProviderError { code: String, description: String },
    EmptyResult,
    ExpirationMismatch,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ParseContext {
    pub received_at_unix_ms: i64,
    pub available_at_unix_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooProvenance {
    pub provider: String,
    pub pinned_client_version: String,
    pub pinned_client_commit: String,
    pub request_family: String,
    pub request_target: String,
    pub provider_symbol: ProviderField<YahooSymbol>,
    pub exchange: ProviderField<String>,
    pub full_exchange_name: ProviderField<String>,
    pub market: ProviderField<String>,
    pub country: ProviderField<String>,
    pub exchange_timezone_name: ProviderField<String>,
    pub exchange_delay_seconds: ProviderField<u32>,
    pub provider_event_time_unix_seconds: ProviderField<i64>,
    pub received_at_unix_ms: i64,
    pub available_at_unix_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooEnrichment<T> {
    pub state: YahooEnrichmentState,
    pub authority: EvidenceAuthority,
    pub provenance: YahooProvenance,
    pub issues: Vec<QualityIssue>,
    pub data: Option<T>,
}

impl<T> YahooEnrichment<T> {
    /// Yahoo evidence is structurally unable to replace a governed observation by itself.
    pub const fn governed_override_permitted(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooQuote {
    pub symbol: YahooSymbol,
    pub quote_type: ProviderField<String>,
    pub currency: ProviderField<String>,
    pub market_state: ProviderField<String>,
    pub regular_market_time_unix_seconds: ProviderField<i64>,
    pub regular_market_price: ProviderField<Decimal>,
    pub bid: ProviderField<Decimal>,
    pub bid_size: ProviderField<u64>,
    pub ask: ProviderField<Decimal>,
    pub ask_size: ProviderField<u64>,
    pub open: ProviderField<Decimal>,
    pub day_low: ProviderField<Decimal>,
    pub day_high: ProviderField<Decimal>,
    pub previous_close: ProviderField<Decimal>,
    pub volume: ProviderField<u64>,
    pub pre_market_price: ProviderField<Decimal>,
    pub pre_market_time_unix_seconds: ProviderField<i64>,
    pub post_market_price: ProviderField<Decimal>,
    pub post_market_time_unix_seconds: ProviderField<i64>,
    pub short_name: ProviderField<String>,
    pub long_name: ProviderField<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooReturnedDisposition<T> {
    pub requested_symbols: Vec<YahooSymbol>,
    pub provider_returned_symbols: Vec<YahooSymbol>,
    pub valid_observations: usize,
    pub missing_symbols: Vec<YahooSymbol>,
    pub rejected_symbols: Vec<YahooSymbol>,
    pub observations: Vec<YahooEnrichment<T>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooBar {
    pub timestamp_unix_seconds: i64,
    pub open: ProviderField<Decimal>,
    pub high: ProviderField<Decimal>,
    pub low: ProviderField<Decimal>,
    pub close: ProviderField<Decimal>,
    pub adjusted_close: ProviderField<Decimal>,
    pub volume: ProviderField<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum YahooChartEventKind {
    Dividend,
    Split,
    CapitalGain,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooChartEvent {
    pub kind: YahooChartEventKind,
    pub timestamp_unix_seconds: i64,
    pub amount: ProviderField<Decimal>,
    pub currency: ProviderField<String>,
    pub numerator: ProviderField<Decimal>,
    pub denominator: ProviderField<Decimal>,
    pub split_ratio: ProviderField<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooChart {
    pub symbol: YahooSymbol,
    pub instrument_type: ProviderField<String>,
    pub currency: ProviderField<String>,
    pub data_granularity: ProviderField<String>,
    pub range: ProviderField<String>,
    pub first_trade_time_unix_seconds: ProviderField<i64>,
    pub regular_market_time_unix_seconds: ProviderField<i64>,
    pub previous_close: ProviderField<Decimal>,
    pub chart_previous_close: ProviderField<Decimal>,
    pub valid_ranges: Vec<String>,
    pub valid_bar_count: usize,
    pub bars: Vec<YahooBar>,
    pub events: Vec<YahooChartEvent>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooReference {
    pub symbol: YahooSymbol,
    pub quote_type: ProviderField<String>,
    pub short_name: ProviderField<String>,
    pub long_name: ProviderField<String>,
    pub underlying_symbol: ProviderField<String>,
    pub currency: ProviderField<String>,
    pub market_state: ProviderField<String>,
    pub regular_market_time_unix_seconds: ProviderField<i64>,
    pub regular_market_price: ProviderField<Decimal>,
    pub nav_price: ProviderField<Decimal>,
    pub total_assets: ProviderField<Decimal>,
    pub category: ProviderField<String>,
    pub fund_family: ProviderField<String>,
    pub sector: ProviderField<String>,
    pub industry: ProviderField<String>,
    pub website: ProviderField<String>,
    pub business_summary: ProviderField<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooFundHolding {
    pub symbol: ProviderField<YahooSymbol>,
    pub name: ProviderField<String>,
    pub holding_percent: ProviderField<Decimal>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooFundData {
    pub symbol: YahooSymbol,
    pub quote_type: ProviderField<String>,
    pub description: ProviderField<String>,
    pub category_name: ProviderField<String>,
    pub family: ProviderField<String>,
    pub legal_type: ProviderField<String>,
    pub annual_report_expense_ratio: ProviderField<Decimal>,
    pub annual_holdings_turnover: ProviderField<Decimal>,
    pub total_net_assets: ProviderField<Decimal>,
    pub asset_classes: BTreeMap<String, ProviderField<Decimal>>,
    pub equity_metrics: BTreeMap<String, ProviderField<Decimal>>,
    pub bond_metrics: BTreeMap<String, ProviderField<Decimal>>,
    pub bond_ratings: BTreeMap<String, ProviderField<Decimal>>,
    pub sector_weightings: BTreeMap<String, ProviderField<Decimal>>,
    pub top_holdings: Vec<YahooFundHolding>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum YahooOptionSide {
    Call,
    Put,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooOptionContract {
    pub side: YahooOptionSide,
    pub contract_symbol: YahooSymbol,
    pub last_trade_time_unix_seconds: ProviderField<i64>,
    pub strike: ProviderField<Decimal>,
    pub last_price: ProviderField<Decimal>,
    pub bid: ProviderField<Decimal>,
    pub ask: ProviderField<Decimal>,
    pub change: ProviderField<Decimal>,
    pub percent_change: ProviderField<Decimal>,
    pub volume: ProviderField<u64>,
    pub open_interest: ProviderField<u64>,
    pub implied_volatility: ProviderField<Decimal>,
    pub in_the_money: ProviderField<bool>,
    pub contract_size: ProviderField<String>,
    pub currency: ProviderField<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooOptionChain {
    pub underlying_symbol: YahooSymbol,
    pub requested_expiration_unix_seconds: Option<i64>,
    pub returned_expiration_unix_seconds: ProviderField<i64>,
    pub expiration_dates_unix_seconds: Vec<i64>,
    pub strikes: Vec<Decimal>,
    pub has_mini_options: ProviderField<bool>,
    pub underlying_quote: ProviderField<YahooQuote>,
    pub valid_contract_count: usize,
    pub contracts: Vec<YahooOptionContract>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct YahooLookupHint {
    pub symbol: YahooSymbol,
    pub quote_type: ProviderField<String>,
    pub exchange: ProviderField<String>,
    pub short_name: ProviderField<String>,
    pub long_name: ProviderField<String>,
    pub sector: ProviderField<String>,
    pub industry: ProviderField<String>,
    pub score: ProviderField<Decimal>,
}
