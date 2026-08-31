//! User-authorized Alpaca Basic market-data surfaces.
//!
//! The crate keeps the provider's free-plan products separate: real-time IEX equity events,
//! delayed IEX historical bars, and the modified/delayed indicative options stream. None of these
//! profiles supplies consolidated US equity coverage, OPRA data, execution authority, or
//! [`market_squawk_domain::DataQuality::DirectVerified`] evidence.

mod boot_snapshot;
mod config;
mod credentials;
mod decoder;
mod doctor;
mod error;
mod historical;
mod historical_calendar;
mod historical_transport;
mod live;
mod market_publication;
mod option_chain;

pub use config::{
    ALPACA_APPLICATION_MAX_REQUESTS_PER_MINUTE, ALPACA_BASIC_EQUITY_SYMBOL_LIMIT,
    ALPACA_BASIC_HISTORICAL_REQUESTS_PER_MINUTE, ALPACA_BASIC_OPTION_CHAIN_PAGE_ROWS,
    ALPACA_BASIC_OPTION_SYMBOL_LIMIT, ALPACA_HISTORICAL_EXCLUSION_NANOS,
    ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS, ALPACA_HISTORICAL_MIN_LOOKBACK_DAYS,
    ALPACA_OPTION_CHAIN_MAX_PAGES, ALPACA_RECURRING_TARGET_REQUESTS_PER_MINUTE, AlpacaAdjustment,
    AlpacaHistoricalEquityConfig, AlpacaHistoricalEquityDataset, AlpacaHistoricalEquityDatasetPlan,
    AlpacaHistoricalEquityPreflightPlan, AlpacaHistoricalLookback, AlpacaHistoricalSeriesSemantics,
    AlpacaIexBootSnapshotPolicy, AlpacaIexLiveConfig, AlpacaInstrumentMapping,
    AlpacaOptionChainConfig, AlpacaOptionMapping, AlpacaOptionsLiveConfig, AlpacaTimeframe,
    AlpacaTransportLimits,
};
pub use credentials::AlpacaCredentials;
pub use decoder::{AlpacaIexDecoder, AlpacaOptionsDecoder};
pub use doctor::{
    ALPACA_PAPER_IEX_DOCTOR_BATCH_SYMBOL_COUNT, AlpacaDoctorBatchObservation,
    AlpacaDoctorCalendarObservation, AlpacaDoctorHistoricalObservation, AlpacaDoctorHttpEvidence,
    AlpacaDoctorHttpPageEvidence, AlpacaDoctorObservationDisposition,
    AlpacaDoctorObservationOrigin, AlpacaDoctorObservedField, AlpacaDoctorQuoteObservation,
    AlpacaDoctorRateEvidence, AlpacaDoctorRetryAfter, AlpacaDoctorStreamObservation,
    AlpacaPaperIexDoctor, AlpacaPaperIexDoctorObservation,
};
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
#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
pub use historical_transport::{
    AlpacaHistoricalScriptedHeader, AlpacaHistoricalScriptedResponse,
    AlpacaHistoricalScriptedTransportCounters, AlpacaHistoricalScriptedTransportFactory,
};
pub use live::{AlpacaIexLiveSource, AlpacaOptionsLiveSource};
pub use market_publication::{
    AlpacaMarketEventRecord, AlpacaMarketEventSurface, AlpacaPreparedMarketEventPublication,
};
pub use option_chain::{
    AlpacaOptionChainClient, AlpacaOptionChainContractAuthority,
    AlpacaOptionChainPublicationRequest, AlpacaOptionChainSealRejoin,
    AlpacaPreparedOptionMarketPublication,
};

#[cfg(test)]
mod tests;
