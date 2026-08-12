//! User-authorized Alpaca Basic market-data surfaces.
//!
//! The crate keeps the provider's free-plan products separate: real-time IEX equity events,
//! delayed IEX historical bars, and the modified/delayed indicative options stream. None of these
//! profiles supplies consolidated US equity coverage, OPRA data, execution authority, or
//! [`market_squawk_domain::DataQuality::DirectVerified`] evidence.

mod config;
mod credentials;
mod decoder;
mod error;
mod historical;
mod historical_calendar;
mod live;

pub use config::{
    ALPACA_BASIC_EQUITY_SYMBOL_LIMIT, ALPACA_BASIC_HISTORICAL_REQUESTS_PER_MINUTE,
    ALPACA_BASIC_OPTION_SYMBOL_LIMIT, ALPACA_HISTORICAL_EXCLUSION_NANOS,
    ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS, ALPACA_HISTORICAL_MIN_LOOKBACK_DAYS, AlpacaAdjustment,
    AlpacaHistoricalEquityConfig, AlpacaHistoricalEquityDataset, AlpacaHistoricalEquityDatasetPlan,
    AlpacaHistoricalEquityPreflightPlan, AlpacaHistoricalLookback, AlpacaHistoricalSeriesSemantics,
    AlpacaIexLiveConfig, AlpacaInstrumentMapping, AlpacaOptionMapping, AlpacaOptionsLiveConfig,
    AlpacaTimeframe, AlpacaTransportLimits,
};
pub use credentials::AlpacaCredentials;
pub use decoder::{AlpacaIexDecoder, AlpacaOptionsDecoder};
pub use error::AlpacaError;
pub use historical::{
    AlpacaHistoricalBarTimeAuthority, AlpacaHistoricalBarTimeRequest,
    AlpacaHistoricalEquityPreflightClient, AlpacaHistoricalEquityPreflightReceipt,
    AlpacaHistoricalEquitySource, AlpacaHistoricalPaginationDisposition,
    AlpacaHistoricalReturnedBarTime, AlpacaRateLimitEvidence,
};
pub use historical_calendar::{
    ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES, AlpacaAuthenticatedCalendarExecutor,
    AlpacaAuthenticatedCalendarRequest, AlpacaAuthenticatedCalendarResponse,
    AlpacaTradingApiEnvironment,
};
pub use live::{AlpacaIexLiveSource, AlpacaOptionsLiveSource};

#[cfg(test)]
mod tests;
