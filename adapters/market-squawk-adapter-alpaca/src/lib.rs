//! User-authorized Alpaca Basic market-data surfaces.
//!
//! The crate keeps the provider's free-plan products separate: real-time IEX equity events,
//! delayed IEX historical bars, and the modified/delayed indicative options stream. None of these
//! profiles supplies consolidated US equity coverage, OPRA data, execution authority, or
//! [`market_squawk_domain::DataQuality::DirectVerified`] evidence.

mod config;
mod credentials;
mod decoder;
mod doctor;
mod error;
mod historical;
mod historical_calendar;
mod live;
#[cfg(feature = "scripted-transport-fixture")]
mod scripted;

pub use config::{
    ALPACA_APPLICATION_MAX_REQUESTS_PER_MINUTE, ALPACA_BASIC_EQUITY_SYMBOL_LIMIT,
    ALPACA_BASIC_HISTORICAL_REQUESTS_PER_MINUTE, ALPACA_BASIC_OPTION_SYMBOL_LIMIT,
    ALPACA_HISTORICAL_EXCLUSION_NANOS, ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS,
    ALPACA_HISTORICAL_MIN_LOOKBACK_DAYS, ALPACA_RECURRING_TARGET_REQUESTS_PER_MINUTE,
    AlpacaAdjustment, AlpacaHistoricalEquityConfig, AlpacaHistoricalEquityDataset,
    AlpacaHistoricalEquityDatasetPlan, AlpacaHistoricalEquityPreflightPlan,
    AlpacaHistoricalLookback, AlpacaHistoricalSeriesSemantics, AlpacaIexLiveConfig,
    AlpacaInstrumentMapping, AlpacaOptionMapping, AlpacaOptionsLiveConfig, AlpacaTimeframe,
    AlpacaTransportLimits,
};
#[cfg(feature = "scripted-transport-fixture")]
pub use config::{
    ALPACA_INSTALLED_FIXTURE_IEX_SOURCE_ID, ALPACA_INSTALLED_FIXTURE_IEX_VALIDITY_NANOS,
    AlpacaInstalledFixtureIexConfig,
};
pub use credentials::AlpacaCredentials;
pub use decoder::{AlpacaIexDecoder, AlpacaOptionsDecoder};
#[cfg(feature = "scripted-transport-fixture")]
pub use doctor::AlpacaPaperIexDoctorFixtureObservation;
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
pub use live::{AlpacaIexLiveSource, AlpacaOptionsLiveSource};
#[cfg(feature = "scripted-transport-fixture")]
pub use scripted::{
    AlpacaInstalledFixtureIexLiveSource, AlpacaScriptedDoctorExecutor,
    AlpacaScriptedTransportEvent, AlpacaScriptedTransportEventKind, AlpacaScriptedTransportFactory,
    AlpacaScriptedTransportTranscript,
};

#[cfg(test)]
mod tests;
