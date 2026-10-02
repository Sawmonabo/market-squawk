//! User-authorized Alpaca Basic market-data surfaces.
//!
//! The crate keeps the provider's free-plan products separate: real-time IEX equity events,
//! delayed IEX historical bars, and the modified/delayed indicative options stream. None of these
//! profiles supplies consolidated US equity coverage, OPRA data, execution authority, or
//! [`market_squawk_domain::DataQuality::DirectVerified`] evidence.

mod asset_reference;
mod boot_snapshot;
mod budget;
pub use asset_reference::{
    ALPACA_ASSET_REFERENCE_ENDPOINT, AlpacaAssetReferenceClient, AlpacaAssetReferenceRejoin,
    AlpacaOriginalAssetReference, AlpacaPendingAssetReference,
};
pub mod calendar_decode;
pub use calendar_decode::{
    AlpacaCalendarDecodeError, AlpacaNativeCalendarSession, AlpacaRetainedCalendarSessions,
};
mod calendar_evidence;
mod calendar_metadata;
mod config;
pub use calendar_metadata::{try_alpaca_calendar_metadata, validate_alpaca_calendar_metadata};
mod corporate_actions;
pub use corporate_actions::{
    AlpacaCorporateActionCategory, AlpacaCorporateActionDate, AlpacaCorporateActionDates,
    AlpacaCorporateActionDisposition, AlpacaCorporateActionIdentity,
    AlpacaCorporateActionInstrument, AlpacaCorporateActionsClient, AlpacaCorporateActionsCoverage,
    AlpacaCorporateActionsRequest, AlpacaCorporateActionsSealRejoin,
    AlpacaPreparedCorporateActionsPublication,
};
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
mod options_contract_reference;
pub use market_publication::{
    AlpacaMarketEventSurface, AlpacaMarketSealRejoin, AlpacaPreparedMarketEventPublication,
};
pub use option_chain::{
    AlpacaOptionChainClient, AlpacaOptionChainContractAuthority,
    AlpacaOptionChainPublicationRequest, AlpacaOptionChainSealRejoin,
    AlpacaPreparedOptionMarketPublication,
};
pub use options_contract_reference::{
    ALPACA_OPTION_CONTRACT_REFERENCE_ENDPOINT, ALPACA_OPTION_CONTRACT_REFERENCE_MAX_PAGES,
    ALPACA_OPTION_CONTRACT_REFERENCE_PAGE_ROWS, AlpacaOptionContractReferenceClient,
    AlpacaOptionContractReferenceRejoin, AlpacaOptionContractReferenceRequest,
    AlpacaOptionContractReferenceSet, AlpacaOptionDeliverable, AlpacaOriginalOptionContract,
    AlpacaPendingOptionContractReferencePage, AlpacaSealedOptionContractReferencePage,
};

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
pub use decoder::{AlpacaIexDecoder, AlpacaMarketDecodeHandoff, AlpacaOptionsDecoder};
pub use doctor::{
    ALPACA_PAPER_IEX_DOCTOR_BATCH_SYMBOL_COUNT, AlpacaDoctorBatchObservation,
    AlpacaDoctorCalendarObservation, AlpacaDoctorHistoricalObservation, AlpacaDoctorHttpEvidence,
    AlpacaDoctorHttpPageEvidence, AlpacaDoctorObservationDisposition,
    AlpacaDoctorObservationOrigin, AlpacaDoctorObservedField, AlpacaDoctorQuoteObservation,
    AlpacaDoctorRateEvidence, AlpacaDoctorRetryAfter, AlpacaDoctorStreamObservation,
    AlpacaPaperIexDoctor, AlpacaPaperIexDoctorObservation,
};
pub use error::{AlpacaCaptureRejoinStage, AlpacaError};
pub use historical::{
    AlpacaHistoricalPendingExtractionSeal,
    alpaca_history_symbol_asof,
    AlpacaHistoricalBarTimeAuthority, AlpacaHistoricalBarTimeRequest,
    AlpacaHistoricalEquityPreflightClient, AlpacaHistoricalEquityPreflightReceipt,
    AlpacaHistoricalEquitySource, AlpacaHistoricalPaginationDisposition,
    AlpacaHistoricalReturnedBarTime, AlpacaRateLimitEvidence,
};
pub use historical_calendar::{
    ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES, AlpacaAuthenticatedCalendarExecutor,
    AlpacaAuthenticatedCalendarRequest, AlpacaAuthenticatedCalendarResponse, AlpacaCalendarMarket,
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

#[cfg(test)]
mod tests;
