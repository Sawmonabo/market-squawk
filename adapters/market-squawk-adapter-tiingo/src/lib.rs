//! Bounded Tiingo Starter adapter core for supported mutual-fund NAV and curated daily EOD data.
//!
//! The crate owns only provider-specific authentication, request planning, native decoding,
//! NAV-versus-EOD semantics, exact canonical mapping, and persistent quota-state contracts. It
//! does not allocate canonical revisions, publish datasets, or claim that a provider EOD row is
//! an intraday trade or bar. Its optional HTTP source emits bounded raw/capture/native receipts;
//! canonical NAV mapping requires their shared durable seal and shared observed-revision input.

mod canonical;
mod credentials;
mod decoder;
mod error;
mod http;
mod model;
mod nav;
mod quota;
mod request;

pub use canonical::{
    TiingoFundNavContractEvidence, TiingoFundNavMapError, TiingoFundNavMappingInput,
    TiingoFundNavRevisionLinks, TiingoMappedFundNav, map_fund_nav,
};
pub use credentials::{TiingoApiToken, TiingoRequestBuilder};
pub use decoder::{TiingoDecoder, TiingoSchemaCircuit, TiingoSchemaCircuitState};
pub use error::{
    TiingoAdapterError, TiingoProviderFailure, TiingoSchemaChange, TiingoSchemaChangeReason,
};
pub use http::{
    TiingoCaptureMaterialError, TiingoCapturedPage, TiingoDecodeFailure, TiingoHistoryCapture,
    TiingoHistoryTerminalDisposition, TiingoHttpResponseMaterial, TiingoHttpSource,
    TiingoHttpSourceError, TiingoProviderHttpFailure, TiingoQuotaStore, TiingoQuotaStoreError,
    TiingoRateLimitDisposition, TiingoRawMaterial, TiingoTransportFailure,
    TiingoTransportFailureKind, tiingo_provider_rate_declaration,
};
pub use model::{
    TiingoApplicationPage, TiingoCoverage, TiingoEodReceipt, TiingoEodRow, TiingoMetadata,
    TiingoMetadataReceipt, TiingoPaginationEvidence, TiingoRequestDisposition,
    TiingoResponseEvidence, TiingoTicker,
};
pub use nav::{
    TiingoAvailabilityGuidance, TiingoFundContext, TiingoFundSupport, TiingoNavClocks,
    TiingoNavInvalidReason, TiingoNavObservationCandidate, TiingoNavValueState,
    TiingoProviderRevisionEvidence, TiingoSourcePublicationEvidence, classify_fund_support,
    normalize_mutual_fund_row, unavailable_nav_candidate,
};
pub use quota::{
    TIINGO_APPLICATION_BYTES_PER_MONTH, TIINGO_APPLICATION_REQUESTS_PER_DAY,
    TIINGO_APPLICATION_REQUESTS_PER_HOUR, TIINGO_APPLICATION_UNIQUE_SYMBOLS_PER_MONTH,
    TIINGO_PROVIDER_BYTES_PER_MONTH, TIINGO_PROVIDER_REQUESTS_PER_DAY,
    TIINGO_PROVIDER_REQUESTS_PER_HOUR, TIINGO_PROVIDER_UNIQUE_SYMBOLS_PER_MONTH,
    TiingoPendingResponseReservation, TiingoQuotaAdmission, TiingoQuotaError, TiingoQuotaLedger,
    TiingoQuotaPermit, TiingoQuotaSnapshot, TiingoQuotaWindows,
};
pub use request::{
    MAX_HISTORY_CALENDAR_DAYS_PER_PAGE, MAX_HISTORY_PAGES, TiingoEndpointFamily, TiingoHistoryPlan,
    TiingoRequestScope, TiingoRequestSpec,
};

#[cfg(test)]
mod tests;
