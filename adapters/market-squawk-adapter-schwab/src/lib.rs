//! Read-only Charles Schwab Trader API market-data contracts.
//!
//! This crate owns the provider-native protocol boundary only. It deliberately contains no
//! account, position, transaction, order, preview, replace, cancel, or money-movement route and
//! cannot construct an `ACCOUNT_ACTIVITY` Streamer subscription. Capacity values are supplied by
//! the shared runtime after measurement; this crate does not invent provider limits.

#![forbid(unsafe_code)]

mod authority;
mod bounds;
mod callback;
mod canonical;
mod error;
mod oauth;
mod option_publication;
mod publication;
mod rest;
mod rest_quote_publication;
mod streamer;
mod streamer_publication;
mod transport;
mod vertical;

pub use authority::{
    ProtectedSchwabOAuthAuthority, ReqwestSchwabOAuthWire, SchwabApplicationCredentialReplacement,
    SchwabApplicationCredentialReplacementBinding, SchwabApplicationCredentialReplacementFailure,
    SchwabOAuthAuthorityConfiguration, SchwabOAuthAuthorityError, SchwabOAuthAuthorityReceipt,
    SchwabOAuthAuthorityStatus, SchwabOAuthInteraction, SchwabOAuthSecretPolicy, SchwabOAuthWire,
    SchwabOAuthWireBounds, SchwabOAuthWireError, SchwabOAuthWireRequest, SchwabOAuthWireResponse,
};
pub use bounds::{
    AdaptiveAssessment, CapacityCounters, CapacityObservation, CapacityUnit, ParseBounds,
    RequestAdmission,
};
pub use callback::{
    OAuthLoopbackBounds, OAuthLoopbackError, OAuthLoopbackReceiver, OAuthLoopbackTlsAcceptError,
    OAuthLoopbackTlsAcceptFuture, OAuthLoopbackTlsAcceptor, OAuthLoopbackTlsStream,
};
pub use canonical::{
    SchwabCanonicalError, SchwabCanonicalField, SchwabCanonicalStreamerField,
    SchwabCanonicalStreamerRecord, SchwabInstrumentCandidate, SchwabOptionCandidateAbstention,
    SchwabOptionCandidateOutcome, SchwabOptionSnapshotCandidate, SchwabQuoteAbstention,
    SchwabQuoteCanonicalOutcome, SchwabResolvedProviderIdentity, SchwabStreamerFieldDictionary,
    SchwabStreamerSemanticField, canonicalize_instrument_candidates, canonicalize_option_chain,
    canonicalize_quote, canonicalize_streamer_batch, canonicalize_streamer_quote_record,
};
pub use error::SchwabAdapterError;
pub use oauth::{
    ACCESS_TOKEN_MAX_LIFETIME_SECONDS, AuthorizationRequest, CallbackOutcome, OAuthCallback,
    OAuthTokenHttpRequest, REFRESH_TOKEN_LIFETIME_SECONDS, RefreshTokenGeneration,
    SchwabApplicationCredentialEnvelope, TokenDecision, TokenGrant, TokenLifecycle,
    TransientTokenResponse, parse_token_response,
};
pub use option_publication::{
    SchwabRestOptionContractRequest, SchwabRestOptionDisposition,
    SchwabRestOptionDispositionReason, SchwabRestOptionMarketDataEvidence,
    SchwabRestOptionPublicationError, SchwabRestOptionPublicationOutcome,
    SchwabRestOptionPublicationRequest, SchwabRestOptionUnderlyingRequest,
    SchwabSealedRawRestOptionPublication, SchwabSealedRestOptionPublication,
};
pub use publication::{
    SchwabDailyPriceHistoryPublicationRequest, SchwabPendingDailyPriceHistoryPublication,
    SchwabPriceHistoryMarketDataEvidence, SchwabPriceHistoryPublicationError,
    SchwabRestDelayEvidence, SchwabSealedDailyPriceHistoryPublication,
};
pub use rest::{
    ChainContractType, ChainRequest, ChainStrategy, ExpirationChainRequest, ExpirationMonth,
    ExpirationResponse, FundamentalField, HistoricalCandle, InstrumentProjection,
    InstrumentResponse, MarketHours, MarketId, MoverFrequency, MoverSort, MoversResponse,
    NativeField, NativeFieldEntry, NativeNumber, NativeScalar, OptionChain, OptionContract,
    OptionContractField, OptionSide, OptionType, ParsedNative, PriceHistoryFrequency,
    PriceHistoryFrequencyType, PriceHistoryPeriodType, PriceHistoryRequest, PriceHistoryResponse,
    ProviderIdentifier, QuoteComponentField, QuoteField, QuoteRequest, QuoteResponse,
    ReadOnlyRequest, ReadOnlyRoute, ReferenceField, SchwabInstrument, SchwabQuote,
    SingleMarketRequest, SingleQuoteRequest, UnknownFieldSummary,
    build_instrument_by_cusip_request, build_instrument_search_request, build_market_hours_request,
    build_movers_request, parse_expiration_response, parse_instrument_response,
    parse_market_hours_response, parse_movers_response, parse_option_chain_response,
    parse_price_history_response, parse_quote_response,
};
pub use rest_quote_publication::{
    SchwabRestQuoteDisposition, SchwabRestQuoteDispositionReason,
    SchwabRestQuoteMarketDataEvidence, SchwabRestQuotePublicationError,
    SchwabRestQuotePublicationOutcome, SchwabRestQuotePublicationRequest,
    SchwabRestQuoteRecordRequest, SchwabSealedRawRestQuotePublication,
    SchwabSealedRestQuotePublication,
};
pub use streamer::{
    ConnectionGeneration, ConnectionState, DesiredStateController, MarketDataService,
    StreamerAdmission, StreamerBootstrap, StreamerBootstrapResponse, StreamerCommand,
    StreamerDataBatch, StreamerFieldEvidence, StreamerFrame, StreamerNativeValue,
    StreamerNestedField, StreamerNotification, StreamerNotificationField, StreamerResponse,
    StreamerResponseCode, StreamerSubscription, TransientStreamerRequest, parse_streamer_frame,
    parse_user_preference,
};
pub use streamer_publication::{
    SchwabSealedRawStreamerPublication, SchwabSealedStreamerQuotePublication,
    SchwabStreamerDelayEvidence, SchwabStreamerPublicationError,
    SchwabStreamerQuoteMarketDataEvidence, SchwabStreamerQuotePublicationOutcome,
    SchwabStreamerQuotePublicationRequest, SchwabStreamerQuoteRecordRequest,
    SchwabStreamerRecordDisposition, SchwabStreamerRecordDispositionReason,
};
pub use transport::{
    AccessTokenAdmission, AccessTokenGeneration, CapturedRestResponse, ExecutedRestResponse,
    InboundStreamerFrame, ProductionSchwabStreamerConnector, RawRestResponseReceipt,
    RawStreamerFrame, RawStreamerFrameKind, ReqwestSchwabHttpWire, ResponseHeaderEvidence,
    RestExecutionOutcome, RestItemAccounting, RestTransportBounds, SchwabAccessTokenSource,
    SchwabCaptureCoordinates, SchwabHttpWire, SchwabHttpWireRequest, SchwabHttpWireResponse,
    SchwabPendingRawRestCapture, SchwabPendingRestCapture, SchwabPendingStreamerCapture,
    SchwabRawRestCaptureSealRejoin, SchwabRestCaptureSealRejoin, SchwabRestExecutor,
    SchwabRestFamily, SchwabRestPayload, SchwabSealedRawRestCapture, SchwabSealedRestResponse,
    SchwabSealedStreamerCapture, SchwabStreamerConnection, SchwabStreamerConnectionControl,
    SchwabStreamerConnectionControlSource, SchwabStreamerConnectionEvidence,
    SchwabStreamerConnector, SchwabStreamerExecutor, SchwabStreamerFrameSealEvidence,
    SchwabStreamerServiceResponseEvidence, SchwabTransportError, SchwabTransportTelemetry,
    SchwabTransportTelemetrySnapshot, SchwabUserPreferenceEvidence, StreamerCaptureSink,
    StreamerCaptureSinkError, StreamerMicrobatch, StreamerMicrobatchReceipt, StreamerRunExit,
    StreamerTransportBounds, TokenAuthorityError, TransientAccessToken,
};
pub use vertical::{
    SchwabCapabilityCurrentness, SchwabFamilyDoctorInput, SchwabObservedCapabilityFamily,
    SchwabPriceHistoryCapabilityObservation, SchwabRestFamilyDoctorInput,
    SchwabStreamerDoctorCaptureRejection, SchwabStreamerFamilyDoctorAccumulator,
    SchwabStreamerFamilyDoctorHandoff, SchwabStreamerFamilyDoctorInput, SchwabVerticalError,
};

/// Exact Schwab OAuth authorization endpoint.
pub const SCHWAB_AUTHORIZE_ENDPOINT: &str = "https://api.schwabapi.com/v1/oauth/authorize";
/// Exact Schwab OAuth token endpoint.
pub const SCHWAB_TOKEN_ENDPOINT: &str = "https://api.schwabapi.com/v1/oauth/token";
/// Code-owned HTTPS loopback callback registered with Schwab.
pub const SCHWAB_CALLBACK_URI: &str = "https://127.0.0.1:8182";
/// Exact Schwab market-data REST base.
pub const SCHWAB_MARKET_DATA_BASE: &str = "https://api.schwabapi.com/marketdata/v1";
/// Sole admitted non-market-data route, used only to extract Streamer coordinates.
pub const SCHWAB_USER_PREFERENCE_ENDPOINT: &str =
    "https://api.schwabapi.com/trader/v1/userPreference";

/// HTTP method admitted by one typed outbound request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpMethod {
    /// Read-only provider request.
    Get,
    /// OAuth token exchange or refresh.
    Post,
}

#[cfg(test)]
mod tests;
