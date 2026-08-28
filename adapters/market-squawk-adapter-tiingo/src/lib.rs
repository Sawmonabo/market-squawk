//! Bounded Tiingo Starter adapter core for supported mutual-fund NAV and curated daily EOD data.
//!
//! The crate owns only provider-specific authentication, request planning, native decoding,
//! NAV-versus-EOD semantics, exact provider-native publication handoffs, and Tiingo-specific shared
//! authority requirements. It does not allocate canonical revisions, mint dataset/PIT receipts,
//! or claim that a provider EOD row is an intraday trade or bar. Its optional HTTP source emits
//! one bounded raw/capture/native page at a time; shared durable authority must seal/checkpoint
//! each page and own quota, schema-circuit, revision, immutable publication, and PIT selection.

mod authority;
mod canonical;
mod credentials;
mod decoder;
mod eod;
mod error;
mod history;
mod http;
mod model;
mod nav;
mod quota;
mod request;

pub use authority::{
    TiingoCompletedResponseDisposition, TiingoHistoryCheckpointReceipt,
    TiingoProviderAdmissionDecision, TiingoProviderAdmissionRequest, TiingoProviderAuthority,
    TiingoProviderAuthorityError, TiingoProviderAuthorityInstallation,
    TiingoProviderAuthorityRequirements, TiingoProviderPermit, TiingoRateLimitDisposition,
    TiingoResponseSettlement,
};
pub use canonical::{
    TiingoCompletedFundNavHistoryCandidate, TiingoFundNavCanonicalCandidate,
    TiingoFundNavContractEvidence, TiingoFundNavHistoryFinancialCoverage, TiingoFundNavMapError,
    TiingoFundNavMappingInput, TiingoPendingFundNavHistoryPublication,
    TiingoPendingLatestFundNavPublication, map_fund_nav_candidate,
};
pub use credentials::{TiingoApiToken, TiingoRequestBuilder};
pub use decoder::{TiingoDecoder, TiingoSchemaCircuitState};
pub use eod::{
    TiingoCompletedEodHistoryCandidate, TiingoEodBarCandidate, TiingoEodBarTimeAuthority,
    TiingoEodBarTimeRequest, TiingoEodContractEvidence, TiingoEodExpectedSessionAuthority,
    TiingoEodExpectedSessionEvidence, TiingoEodExpectedSessionRequest,
    TiingoEodExpectedSessionValidationReceipt, TiingoEodFinancialCoverageDisposition,
    TiingoEodInstrumentAuthority, TiingoEodInstrumentKind, TiingoEodMapError,
    TiingoEodMappingInput, TiingoEodPageCandidate, TiingoEodPagePublicationRoute,
    TiingoEodProviderActionEvidence, TiingoEodSurface, TiingoEodSurfaceGap,
    TiingoEodSurfaceGapReason, TiingoPendingEodHistoryPublication,
    TiingoPendingLatestEodPublication, map_eod_page_candidate,
};
pub use error::{
    TiingoAdapterError, TiingoProviderFailure, TiingoSchemaChange, TiingoSchemaChangeReason,
};
pub use history::{
    TiingoCompletedHistoryCapture, TiingoHistoryEvidenceError, TiingoHistoryTerminalDisposition,
    TiingoSealedHistoryPage,
};
pub use http::{
    TiingoCaptureMaterialError, TiingoCapturedPage, TiingoDecodeFailure,
    TiingoHttpResponseMaterial, TiingoHttpSource, TiingoHttpSourceError, TiingoProviderHttpFailure,
    TiingoRawMaterial, TiingoTransportFailure, TiingoTransportFailureKind,
    tiingo_provider_rate_declaration,
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
    missing_nav_candidate, normalize_mutual_fund_row,
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
