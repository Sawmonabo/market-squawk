//! Bounded authenticated GET execution for the frozen read-only route allowlist.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures_util::StreamExt as _;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_platform::RawCaptureRecord;
use market_squawk_sources::{
    ProviderCaptureMaterial, ProviderCapturePageReceipt, ProviderCaptureSealExpectation,
    ProviderCaptureSealRequest, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    ProviderEventMicrobatchMaterial, ProviderEventMicrobatchSealExpectation,
    ProviderEventMicrobatchToken, ProviderWholeCaptureToken, SealedProviderCaptureMaterial,
    SealedProviderCaptureSetReceipt, SealedProviderEventMicrobatchReceipt,
};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE,
    HeaderMap, HeaderName, RETRY_AFTER, USER_AGENT,
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{
    CapacityObservation, CapacityUnit, ExpirationResponse, InstrumentResponse, MarketHours,
    MoversResponse, OptionChain, ParseBounds, ParsedNative, PriceHistoryResponse,
    ProviderIdentifier, QuoteResponse, ReadOnlyRequest, ReadOnlyRoute, SchwabAdapterError,
    StreamerBootstrapResponse, parse_expiration_response, parse_instrument_response,
    parse_market_hours_response, parse_movers_response, parse_option_chain_response,
    parse_price_history_response, parse_quote_response, parse_user_preference,
};

use super::{
    AccessTokenAdmission, RawRestResponseReceipt, ResponseHeaderEvidence, RestTransportBounds,
    SchwabCaptureCoordinates, SchwabTransportError, SchwabTransportTelemetry, SensitiveBytesOwner,
    TransientAccessToken, duration_millis, unix_millis, unix_seconds,
};

const USER_AGENT_VALUE: &str = "market-squawk-schwab-read-only/1";

/// Borrowed immediate GET operation passed to an injectable wire implementation.
pub struct SchwabHttpWireRequest<'a> {
    request: &'a ReadOnlyRequest,
    bearer: &'a str,
    bounds: RestTransportBounds,
}

impl<'a> SchwabHttpWireRequest<'a> {
    fn new(request: &'a ReadOnlyRequest, bearer: &'a str, bounds: RestTransportBounds) -> Self {
        Self {
            request,
            bearer,
            bounds,
        }
    }

    /// Allowlist-validated request metadata without credentials.
    pub const fn request(&self) -> &ReadOnlyRequest {
        self.request
    }

    /// Caller-owned transport bounds.
    pub const fn bounds(&self) -> RestTransportBounds {
        self.bounds
    }

    fn bearer(&self) -> &str {
        self.bearer
    }
}

impl fmt::Debug for SchwabHttpWireRequest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabHttpWireRequest")
            .field("request", &self.request)
            .field("bearer", &"[REDACTED]")
            .field("bounds", &self.bounds)
            .finish()
    }
}

/// Complete bounded HTTP response returned by an injectable wire implementation.
///
/// This exact raw-body owner is deliberately non-cloneable. Its debug view is metadata-only.
pub struct SchwabHttpWireResponse {
    status: u16,
    final_url: Box<str>,
    declared_body_bytes: Option<u64>,
    headers: Box<[ResponseHeaderEvidence]>,
    body: Bytes,
}

impl fmt::Debug for SchwabHttpWireResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabHttpWireResponse")
            .field("status", &self.status)
            .field("final_url", &self.final_url)
            .field("declared_body_bytes", &self.declared_body_bytes)
            .field("headers", &self.headers)
            .field("body_bytes", &self.body.len())
            .field("body", &"[EXACT RAW BODY REDACTED]")
            .finish()
    }
}

impl SchwabHttpWireResponse {
    /// Constructs a bounded mock/custom transport response using the same safety checks as the
    /// production wire.
    pub fn try_new(
        status: u16,
        final_url: String,
        declared_body_bytes: Option<u64>,
        headers: Vec<ResponseHeaderEvidence>,
        body: Bytes,
        bounds: RestTransportBounds,
    ) -> Result<Self, SchwabTransportError> {
        if !(100..=599).contains(&status)
            || final_url.is_empty()
            || body.len() > bounds.max_response_bytes()
            || declared_body_bytes.is_some_and(|length| {
                usize::try_from(length).map_or(true, |length| {
                    length > bounds.max_response_bytes() || length != body.len()
                })
            })
        {
            return Err(SchwabTransportError::Protocol);
        }
        validate_header_evidence(&headers, bounds)?;
        Ok(Self {
            status,
            final_url: final_url.into_boxed_str(),
            declared_body_bytes,
            headers: headers.into_boxed_slice(),
            body,
        })
    }
}

/// Injectable HTTP boundary used by production reqwest and one deterministic local mock proof.
pub trait SchwabHttpWire: fmt::Debug + Send + Sync {
    fn get<'a>(
        &'a self,
        request: SchwabHttpWireRequest<'a>,
    ) -> Pin<
        Box<dyn Future<Output = Result<SchwabHttpWireResponse, SchwabTransportError>> + Send + 'a>,
    >;
}

/// Hardened production reqwest wire. Redirects, implicit retries, proxies, and decompression are
/// disabled so captured bytes are the exact application payload returned by the selected route.
#[derive(Debug)]
pub struct ReqwestSchwabHttpWire {
    client: reqwest::Client,
}

impl ReqwestSchwabHttpWire {
    /// Builds one HTTPS-only client under explicit caller bounds.
    pub fn try_new(bounds: RestTransportBounds) -> Result<Self, SchwabTransportError> {
        let client = reqwest::Client::builder()
            .https_only(true)
            .tls_backend_rustls()
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .retry(reqwest::retry::never())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(bounds.connect_timeout())
            .read_timeout(bounds.read_timeout())
            .timeout(bounds.total_timeout())
            .build()
            .map_err(|_| SchwabTransportError::InvalidConfiguration)?;
        Ok(Self { client })
    }
}

/// Adapter-owned bearer material transferred into reqwest without copying its allocation.
pub(crate) struct ReqwestSchwabAuthorizationMaterial {
    owner: SensitiveBytesOwner,
}

impl ReqwestSchwabAuthorizationMaterial {
    pub(crate) fn try_new(bearer: &str) -> Result<Self, SchwabTransportError> {
        let mut authorization = Zeroizing::new(Vec::new());
        authorization
            .try_reserve_exact("Bearer ".len().saturating_add(bearer.len()))
            .map_err(|_| SchwabTransportError::InvalidToken)?;
        authorization.extend_from_slice(b"Bearer ");
        authorization.extend_from_slice(bearer.as_bytes());
        Ok(Self {
            owner: SensitiveBytesOwner::new(std::mem::take(&mut *authorization)),
        })
    }

    #[cfg(test)]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.owner.as_bytes()
    }

    pub(crate) fn into_header(self) -> Result<reqwest::header::HeaderValue, SchwabTransportError> {
        let mut header = reqwest::header::HeaderValue::from_maybe_shared(self.owner.into_shared())
            .map_err(|_| SchwabTransportError::InvalidToken)?;
        header.set_sensitive(true);
        Ok(header)
    }

    #[cfg(test)]
    pub(crate) fn arm_drop_audit(&mut self, audit: super::SensitiveDropAudit) {
        self.owner.arm_drop_audit(audit);
    }
}

impl fmt::Debug for ReqwestSchwabAuthorizationMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReqwestSchwabAuthorizationMaterial([REDACTED])")
    }
}

impl SchwabHttpWire for ReqwestSchwabHttpWire {
    fn get<'a>(
        &'a self,
        request: SchwabHttpWireRequest<'a>,
    ) -> Pin<
        Box<dyn Future<Output = Result<SchwabHttpWireResponse, SchwabTransportError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let authorization =
                ReqwestSchwabAuthorizationMaterial::try_new(request.bearer())?.into_header()?;
            let response = self
                .client
                .get(request.request().url())
                .header(ACCEPT, "application/json")
                .header(ACCEPT_ENCODING, "identity")
                .header(USER_AGENT, USER_AGENT_VALUE)
                .header(AUTHORIZATION, authorization)
                .send()
                .await
                .map_err(map_reqwest_error)?;
            let status = response.status().as_u16();
            let final_url = response.url().as_str().to_owned();
            let declared_body_bytes = declared_length(response.headers())?;
            if declared_body_bytes.is_some_and(|length| {
                usize::try_from(length).map_or(true, |length| {
                    length > request.bounds().max_response_bytes()
                })
            }) {
                return Err(SchwabTransportError::PayloadTooLarge);
            }
            let headers = selected_headers(response.headers(), request.bounds())?;
            validate_content_encoding(&headers)?;
            let body = collect_body(response, request.bounds().max_response_bytes()).await?;
            SchwabHttpWireResponse::try_new(
                status,
                final_url,
                declared_body_bytes,
                headers,
                body,
                request.bounds(),
            )
        })
    }
}

/// Exact bounded raw response and receipt returned before canonical mapping.
#[derive(Eq, PartialEq)]
pub struct CapturedRestResponse {
    receipt: RawRestResponseReceipt,
    accounting: RestItemAccounting,
    body: Bytes,
}

impl fmt::Debug for CapturedRestResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapturedRestResponse")
            .field("receipt", &self.receipt)
            .field("accounting", &self.accounting)
            .field("body", &"[EXACT RAW BODY REDACTED]")
            .finish()
    }
}

impl CapturedRestResponse {
    pub const fn receipt(&self) -> &RawRestResponseReceipt {
        &self.receipt
    }

    /// Borrows the exact bounded response body while its sole owning capture remains intact.
    ///
    /// This narrow view lets an application bind the same completed response to its registered
    /// current-session raw-frame authority before consuming this value into the archival sealer.
    /// It transfers no response ownership and grants no reconstruction, serialization, or
    /// response-construction authority.
    pub fn exact_body(&self) -> &[u8] {
        &self.body
    }

    /// Same-unit request completion retained even when the body is rejected or cannot be parsed.
    pub const fn accounting(&self) -> RestItemAccounting {
        self.accounting
    }

    fn capacity_observation(
        &self,
        validation_failed: bool,
    ) -> Result<CapacityObservation, SchwabTransportError> {
        capacity_observation_from_receipt(&self.receipt, self.accounting, validation_failed)
    }

    /// Consumes one rejected or invalid market-data body into the sole raw-sealing handoff.
    ///
    /// The typed adapter error or provider disposition remains owned by the caller. This handoff
    /// proves only that the exact bounded response body, status, headers, latency, and same-unit
    /// item accounting crossed the application-owned physical sealer.
    pub fn into_pending_capture(
        self,
        coordinates: SchwabCaptureCoordinates,
        event_id: Uuid,
    ) -> Result<SchwabPendingRawRestCapture, SchwabTransportError> {
        let Self {
            receipt,
            accounting,
            body,
        } = self;
        let material = raw_rest_capture_material(&receipt, body, &coordinates, event_id)?;
        let (seal_expectation, seal_request) = material.into_sealing_parts();
        Ok(SchwabPendingRawRestCapture {
            rejoin: SchwabRawRestCaptureSealRejoin {
                coordinates,
                receipt,
                accounting,
                seal_expectation,
            },
            seal_request,
        })
    }
}

/// Closed provider-native market-data REST payload selected solely by the allowlisted route.
#[derive(Debug)]
pub enum SchwabRestPayload {
    Quotes(ParsedNative<QuoteResponse>),
    OptionChain(ParsedNative<OptionChain>),
    Expirations(ParsedNative<ExpirationResponse>),
    PriceHistory(ParsedNative<PriceHistoryResponse>),
    MarketHours(ParsedNative<Box<[MarketHours]>>),
    Movers(ParsedNative<MoversResponse>),
    Instruments(ParsedNative<InstrumentResponse>),
}

/// Closed selected REST response family, preserving exact route evidence separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabRestFamily {
    Quotes,
    OptionChain,
    ExpirationChain,
    DailyPriceHistory,
    MarketHours,
    Movers,
    Instruments,
}

impl SchwabRestPayload {
    /// Exact provider-native records returned by the response, separate from lookup completion.
    pub fn record_count(&self) -> usize {
        match self {
            Self::Quotes(value) => value.value().quotes().len(),
            Self::OptionChain(value) => value.value().contracts().len(),
            Self::Expirations(value) => value.value().expirations().len(),
            Self::PriceHistory(value) => value.value().candles().len(),
            Self::MarketHours(value) => value.value().len(),
            Self::Movers(value) => value.value().movers.len(),
            Self::Instruments(value) => value.value().instruments().len(),
        }
    }

    /// Returns the closed response family without erasing the exact request route.
    pub const fn family(&self) -> SchwabRestFamily {
        match self {
            Self::Quotes(_) => SchwabRestFamily::Quotes,
            Self::OptionChain(_) => SchwabRestFamily::OptionChain,
            Self::Expirations(_) => SchwabRestFamily::ExpirationChain,
            Self::PriceHistory(_) => SchwabRestFamily::DailyPriceHistory,
            Self::MarketHours(_) => SchwabRestFamily::MarketHours,
            Self::Movers(_) => SchwabRestFamily::Movers,
            Self::Instruments(_) => SchwabRestFamily::Instruments,
        }
    }

    pub(crate) fn raw_sha256(&self) -> [u8; 32] {
        match self {
            Self::Quotes(value) => value.raw_sha256(),
            Self::OptionChain(value) => value.raw_sha256(),
            Self::Expirations(value) => value.raw_sha256(),
            Self::PriceHistory(value) => value.raw_sha256(),
            Self::MarketHours(value) => value.raw_sha256(),
            Self::Movers(value) => value.raw_sha256(),
            Self::Instruments(value) => value.raw_sha256(),
        }
    }
}

/// Same-unit completion accounting plus dense provider record count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestItemAccounting {
    pub requested: u64,
    pub returned: u64,
    pub missing: u64,
    pub unexpected: u64,
    pub provider_records: u64,
}

impl RestItemAccounting {
    fn validate(self) -> Result<Self, SchwabTransportError> {
        if self.requested == 0
            || self
                .returned
                .checked_add(self.missing)
                .ok_or(SchwabTransportError::Overflow)?
                != self.requested
        {
            return Err(SchwabTransportError::Protocol);
        }
        Ok(self)
    }
}

/// Successful captured and provider-native-validated market-data REST operation.
#[derive(Debug)]
pub struct ExecutedRestResponse {
    capture: CapturedRestResponse,
    payload: SchwabRestPayload,
}

/// Bodyless accepted `userPreference` evidence retained only for Streamer bootstrap/currentness.
///
/// The exact provider body is discarded before this value leaves the executor because that body
/// can contain account-shaped fields unrelated to market-data operation. This value implements
/// neither `Clone` nor serialization.
pub struct SchwabUserPreferenceEvidence {
    receipt: RawRestResponseReceipt,
    bootstrap: StreamerBootstrapResponse,
    accounting: RestItemAccounting,
}

impl fmt::Debug for SchwabUserPreferenceEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabUserPreferenceEvidence")
            .field("receipt", &self.receipt)
            .field("bootstrap", &self.bootstrap)
            .field("accounting", &self.accounting)
            .field("raw_body", &"[DISCARDED BEFORE RETURN]")
            .finish()
    }
}

impl SchwabUserPreferenceEvidence {
    pub const fn receipt(&self) -> &RawRestResponseReceipt {
        &self.receipt
    }

    pub const fn bootstrap(&self) -> &StreamerBootstrapResponse {
        &self.bootstrap
    }

    pub const fn accounting(&self) -> RestItemAccounting {
        self.accounting
    }
}

impl ExecutedRestResponse {
    pub const fn capture(&self) -> &CapturedRestResponse {
        &self.capture
    }

    pub const fn payload(&self) -> &SchwabRestPayload {
        &self.payload
    }

    pub const fn accounting(&self) -> RestItemAccounting {
        self.capture.accounting
    }

    /// Consumes one accepted market-data response into the sole raw-sealing handoff.
    ///
    /// The raw body moves into source-neutral [`ProviderCaptureMaterial`]. The opaque continuation
    /// retains only the exact typed provider payload, bounded receipt/accounting, and externally
    /// supplied source coordinates. It owns no raw-body clone, store, sealer, publication revision,
    /// canonical identity, or point-in-time authority. `userPreference` is deliberately excluded
    /// because its raw response may contain account-shaped material.
    pub fn into_pending_capture(
        self,
        coordinates: SchwabCaptureCoordinates,
        event_id: Uuid,
    ) -> Result<SchwabPendingRestCapture, SchwabTransportError> {
        let Self {
            capture:
                CapturedRestResponse {
                    receipt,
                    accounting,
                    body,
                },
            payload,
        } = self;
        if !payload_matches_capturable_route(receipt.route(), &payload) {
            return Err(SchwabTransportError::CaptureMaterial);
        }
        let material = rest_capture_material(&receipt, body, &coordinates, event_id)?;
        let (seal_expectation, seal_request) = material.into_whole_seal_parts();
        Ok(SchwabPendingRestCapture {
            rejoin: SchwabRestCaptureSealRejoin {
                coordinates,
                receipt,
                payload,
                accounting,
                seal_expectation,
            },
            seal_request,
        })
    }
}

/// One non-cloneable rejected/invalid REST response waiting for the shared raw sealer.
pub struct SchwabPendingRawRestCapture {
    rejoin: SchwabRawRestCaptureSealRejoin,
    seal_request: ProviderCaptureSealRequest,
}

impl fmt::Debug for SchwabPendingRawRestCapture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabPendingRawRestCapture")
            .field("rejoin", &self.rejoin)
            .field("seal_request", &"EXACT RAW BODY PENDING PHYSICAL SEAL")
            .finish()
    }
}

impl SchwabPendingRawRestCapture {
    /// Splits the evidence continuation from the sole consuming physical-seal request.
    pub fn into_sealing_parts(
        self,
    ) -> (SchwabRawRestCaptureSealRejoin, ProviderCaptureSealRequest) {
        (self.rejoin, self.seal_request)
    }
}

/// Opaque rejected/invalid REST continuation awaiting its exact common seal witness.
pub struct SchwabRawRestCaptureSealRejoin {
    coordinates: SchwabCaptureCoordinates,
    receipt: RawRestResponseReceipt,
    accounting: RestItemAccounting,
    seal_expectation: ProviderEventMicrobatchSealExpectation,
}

impl fmt::Debug for SchwabRawRestCaptureSealRejoin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabRawRestCaptureSealRejoin")
            .field("coordinates", &self.coordinates)
            .field("route", &self.receipt.route())
            .field("token_generation", &self.receipt.token_generation())
            .field("request_sha256", &self.receipt.request_sha256())
            .field("status", &self.receipt.status())
            .field("body_sha256", &self.receipt.body_sha256())
            .field("accounting", &self.accounting)
            .field("sealed_transition", &"AWAITING_COMMON_PHYSICAL_SEAL")
            .finish()
    }
}

impl SchwabRawRestCaptureSealRejoin {
    /// Rejoins the exact application-owned seal without claiming typed provider validity.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<SchwabSealedRawRestCapture, SchwabTransportError> {
        let token = rejoin_raw_rest_capture(
            self.seal_expectation,
            sealed,
            &self.coordinates,
            &self.receipt,
        )?;
        Ok(SchwabSealedRawRestCapture {
            coordinates: self.coordinates,
            receipt: self.receipt,
            accounting: self.accounting,
            token,
        })
    }
}

/// Physically sealed rejected/invalid REST response evidence.
///
/// This value cannot become a typed or canonical success response. It retains the exact response
/// receipt and item counts so doctor and degradation paths can report evidence rather than infer
/// an unavailable family from a transport error.
pub struct SchwabSealedRawRestCapture {
    coordinates: SchwabCaptureCoordinates,
    receipt: RawRestResponseReceipt,
    accounting: RestItemAccounting,
    token: ProviderEventMicrobatchToken,
}

impl fmt::Debug for SchwabSealedRawRestCapture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabSealedRawRestCapture")
            .field("coordinates", &self.coordinates)
            .field("receipt", &self.receipt)
            .field("accounting", &self.accounting)
            .field("raw_body", &"PHYSICALLY SEALED")
            .finish()
    }
}

impl SchwabSealedRawRestCapture {
    pub const fn coordinates(&self) -> &SchwabCaptureCoordinates {
        &self.coordinates
    }

    pub const fn receipt(&self) -> &RawRestResponseReceipt {
        &self.receipt
    }

    pub const fn accounting(&self) -> RestItemAccounting {
        self.accounting
    }

    pub fn persisted_receipt(&self) -> &SealedProviderEventMicrobatchReceipt {
        self.token.persisted_receipt()
    }
}

/// One non-cloneable accepted REST response waiting for the shared raw sealer.
///
/// Splitting consumes this value once. Neither half can manufacture a second handoff.
pub struct SchwabPendingRestCapture {
    rejoin: SchwabRestCaptureSealRejoin,
    seal_request: ProviderCaptureSealRequest,
}

impl fmt::Debug for SchwabPendingRestCapture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabPendingRestCapture")
            .field("rejoin", &self.rejoin)
            .field("seal_request", &"EXACT RAW BODY PENDING PHYSICAL SEAL")
            .finish()
    }
}

impl SchwabPendingRestCapture {
    /// Splits the opaque typed continuation from the sole consuming physical-seal request.
    pub fn into_sealing_parts(self) -> (SchwabRestCaptureSealRejoin, ProviderCaptureSealRequest) {
        (self.rejoin, self.seal_request)
    }
}

/// Opaque typed continuation awaiting its exact common consuming material-seal witness.
///
/// Only adapter-owned family publication code can consume this continuation with the matching
/// `SealedProviderCaptureMaterial`; public callers cannot substitute receipt equality for the
/// process-local seal witness.
pub struct SchwabRestCaptureSealRejoin {
    coordinates: SchwabCaptureCoordinates,
    receipt: RawRestResponseReceipt,
    payload: SchwabRestPayload,
    accounting: RestItemAccounting,
    seal_expectation: ProviderCaptureSealExpectation,
}

impl fmt::Debug for SchwabRestCaptureSealRejoin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabRestCaptureSealRejoin")
            .field("coordinates", &self.coordinates)
            .field("route", &self.receipt.route())
            .field("token_generation", &self.receipt.token_generation())
            .field("request_sha256", &self.receipt.request_sha256())
            .field("body_sha256", &self.receipt.body_sha256())
            .field(
                "received_at_unix_millis",
                &self.receipt.received_at_unix_millis(),
            )
            .field("payload_family", &rest_payload_family(&self.payload))
            .field("accounting", &self.accounting)
            .field("sealed_transition", &"AWAITING_COMMON_PHYSICAL_SEAL")
            .finish()
    }
}

impl SchwabRestCaptureSealRejoin {
    /// Rejoins the exact application-owned physical seal into an opaque sealed REST response.
    ///
    /// This is the common terminal boundary for every selected read-only REST family. It exposes
    /// no raw-body clone, capture token, canonical publication authority, or account-shaped User
    /// Preference response.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<SchwabSealedRestResponse, SchwabTransportError> {
        self.try_rejoin_parts(sealed)
            .map(|parts| SchwabSealedRestResponse { parts })
    }

    pub(crate) fn try_rejoin_whole(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<SchwabSealedRestResponseParts, SchwabTransportError> {
        self.try_rejoin_parts(sealed)
    }

    fn try_rejoin_parts(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<SchwabSealedRestResponseParts, SchwabTransportError> {
        let token = rejoin_rest_capture(
            self.seal_expectation,
            sealed,
            &self.coordinates,
            &self.receipt,
        )?;
        if self.payload.raw_sha256() != self.receipt.body_sha256()
            || u64::try_from(self.payload.record_count())
                .map_err(|_| SchwabTransportError::Overflow)?
                != self.accounting.provider_records
        {
            return Err(SchwabTransportError::CaptureMaterial);
        }
        Ok(SchwabSealedRestResponseParts {
            coordinates: self.coordinates,
            receipt: self.receipt,
            payload: self.payload,
            accounting: self.accounting,
            token,
        })
    }
}

/// Opaque physically sealed selected REST response awaiting its truthful family-specific sink.
///
/// The wrapper is deliberately non-cloneable and non-serializable. It proves exact raw retention
/// and typed provider parsing, but does not claim that every family already has a canonical domain
/// schema or durable analytical publisher.
pub struct SchwabSealedRestResponse {
    parts: SchwabSealedRestResponseParts,
}

impl fmt::Debug for SchwabSealedRestResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabSealedRestResponse")
            .field("family", &self.family())
            .field("route", &self.route())
            .field("receipt", &self.parts.receipt)
            .field("accounting", &self.parts.accounting)
            .field("raw_body", &"PHYSICALLY SEALED")
            .finish()
    }
}

impl SchwabSealedRestResponse {
    /// Returns the typed provider response family.
    pub const fn family(&self) -> SchwabRestFamily {
        self.parts.payload.family()
    }

    /// Returns the exact allowlisted request route, including grouped route aliases.
    pub const fn route(&self) -> ReadOnlyRoute {
        self.parts.receipt.route()
    }

    /// Returns the exact bounded transport and raw-body receipt.
    pub const fn receipt(&self) -> &RawRestResponseReceipt {
        &self.parts.receipt
    }

    /// Returns request/response completion accounting without exposing canonical authority.
    pub const fn accounting(&self) -> RestItemAccounting {
        self.parts.accounting
    }

    /// Returns cloneable logical/physical response evidence without exposing publication authority.
    pub fn persisted_receipt(&self) -> &SealedProviderCaptureSetReceipt {
        self.parts.token.persisted_receipt()
    }

    pub(crate) const fn parts(&self) -> &SchwabSealedRestResponseParts {
        &self.parts
    }

    pub(crate) fn into_parts(self) -> SchwabSealedRestResponseParts {
        self.parts
    }
}

pub(crate) struct SchwabSealedRestResponseParts {
    pub(crate) coordinates: SchwabCaptureCoordinates,
    pub(crate) receipt: RawRestResponseReceipt,
    pub(crate) payload: SchwabRestPayload,
    pub(crate) accounting: RestItemAccounting,
    pub(crate) token: ProviderWholeCaptureToken,
}

fn payload_matches_capturable_route(route: ReadOnlyRoute, payload: &SchwabRestPayload) -> bool {
    matches!(
        (route, payload),
        (
            ReadOnlyRoute::Quotes | ReadOnlyRoute::SingleQuote,
            SchwabRestPayload::Quotes(_)
        ) | (ReadOnlyRoute::Chains, SchwabRestPayload::OptionChain(_))
            | (
                ReadOnlyRoute::ExpirationChain,
                SchwabRestPayload::Expirations(_)
            )
            | (
                ReadOnlyRoute::PriceHistory,
                SchwabRestPayload::PriceHistory(_)
            )
            | (
                ReadOnlyRoute::Markets | ReadOnlyRoute::SingleMarket,
                SchwabRestPayload::MarketHours(_)
            )
            | (ReadOnlyRoute::Movers, SchwabRestPayload::Movers(_))
            | (
                ReadOnlyRoute::Instruments | ReadOnlyRoute::InstrumentByCusip,
                SchwabRestPayload::Instruments(_)
            )
    )
}

fn rest_payload_family(payload: &SchwabRestPayload) -> &'static str {
    match payload {
        SchwabRestPayload::Quotes(_) => "quotes",
        SchwabRestPayload::OptionChain(_) => "option-chain",
        SchwabRestPayload::Expirations(_) => "expiration-chain",
        SchwabRestPayload::PriceHistory(_) => "price-history",
        SchwabRestPayload::MarketHours(_) => "market-hours",
        SchwabRestPayload::Movers(_) => "movers",
        SchwabRestPayload::Instruments(_) => "instruments",
    }
}

fn rest_capture_material(
    receipt: &RawRestResponseReceipt,
    body: Bytes,
    coordinates: &SchwabCaptureCoordinates,
    event_id: Uuid,
) -> Result<ProviderCaptureMaterial, SchwabTransportError> {
    if !(200..=299).contains(&receipt.status()) || event_id.is_nil() {
        return Err(SchwabTransportError::CaptureMaterial);
    }
    let received_nanos = i64::try_from(receipt.received_at_unix_millis())
        .ok()
        .and_then(|value| value.checked_mul(1_000_000))
        .ok_or(SchwabTransportError::CaptureMaterial)?;
    let received_at = Timestamp::from_unix_nanos(received_nanos);
    let request_identity = EvidenceDigest::new(DigestAlgorithm::Sha256, receipt.request_sha256());
    let page = ProviderCapturePageReceipt::try_new(
        0,
        request_identity,
        None,
        None,
        receipt.status(),
        receipt.body_bytes(),
        EvidenceDigest::new(DigestAlgorithm::Sha256, receipt.body_sha256()),
        received_at,
    )
    .map_err(|_| SchwabTransportError::CaptureMaterial)?;
    let capture = ProviderCaptureSetReceipt::try_new(
        coordinates.source_id().clone(),
        coordinates.metadata_revision().clone(),
        coordinates.dataset().clone(),
        request_identity,
        ProviderCaptureTerminalDisposition::StandaloneResponse,
        vec![page],
    )
    .map_err(|_| SchwabTransportError::CaptureMaterial)?;
    let record = RawCaptureRecord::try_new_live(
        event_id,
        Arc::from(coordinates.source_id().as_str()),
        coordinates.connection_id(),
        Some(0),
        None,
        chrono::DateTime::from_timestamp_nanos(received_nanos),
        body,
    )
    .map_err(|_| SchwabTransportError::CaptureMaterial)?;
    ProviderCaptureMaterial::try_new(capture, vec![record])
        .map_err(|_| SchwabTransportError::CaptureMaterial)
}

fn raw_rest_capture_material(
    receipt: &RawRestResponseReceipt,
    body: Bytes,
    coordinates: &SchwabCaptureCoordinates,
    event_id: Uuid,
) -> Result<ProviderEventMicrobatchMaterial, SchwabTransportError> {
    if event_id.is_nil() {
        return Err(SchwabTransportError::CaptureMaterial);
    }
    let received_nanos = i64::try_from(receipt.received_at_unix_millis())
        .ok()
        .and_then(|value| value.checked_mul(1_000_000))
        .ok_or(SchwabTransportError::CaptureMaterial)?;
    let record = RawCaptureRecord::try_new_live(
        event_id,
        Arc::from(coordinates.source_id().as_str()),
        coordinates.connection_id(),
        Some(0),
        None,
        chrono::DateTime::from_timestamp_nanos(received_nanos),
        body,
    )
    .map_err(|_| SchwabTransportError::CaptureMaterial)?;
    ProviderEventMicrobatchMaterial::try_new(
        coordinates.source_id().clone(),
        coordinates.metadata_revision().clone(),
        coordinates.dataset().clone(),
        coordinates.dataset().clone(),
        vec![record],
    )
    .map_err(|_| SchwabTransportError::CaptureMaterial)
}

fn rejoin_raw_rest_capture(
    expectation: ProviderEventMicrobatchSealExpectation,
    sealed: SealedProviderCaptureMaterial,
    coordinates: &SchwabCaptureCoordinates,
    receipt: &RawRestResponseReceipt,
) -> Result<ProviderEventMicrobatchToken, SchwabTransportError> {
    let token = expectation
        .try_rejoin(sealed)
        .map_err(|_| SchwabTransportError::CaptureMaterial)?;
    let persisted = token.persisted_receipt();
    let capture = persisted.capture();
    let [frame] = capture.frames() else {
        return Err(SchwabTransportError::CaptureMaterial);
    };
    let [physical] = persisted.segment().frames() else {
        return Err(SchwabTransportError::CaptureMaterial);
    };
    let received_at = receipt
        .received_at_unix_millis()
        .checked_mul(1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .map(Timestamp::from_unix_nanos)
        .ok_or(SchwabTransportError::CaptureMaterial)?;
    let payload_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, receipt.body_sha256());
    if capture.source_id() != coordinates.source_id()
        || capture.metadata_revision() != coordinates.metadata_revision()
        || capture.dataset() != coordinates.dataset()
        || capture.stream_identity() != coordinates.dataset()
        || frame.event_id() == [0; 16]
        || frame.connection_id() != *coordinates.connection_id().as_bytes()
        || frame.source_sequence() != Some(0)
        || frame.exchange_at().is_some()
        || frame.received_at() != received_at
        || frame.payload_bytes() != receipt.body_bytes()
        || frame.payload_digest() != payload_digest
        || physical.provider_payload_bytes() != receipt.body_bytes()
        || physical.provider_payload_digest() != payload_digest
    {
        return Err(SchwabTransportError::CaptureMaterial);
    }
    Ok(token)
}

fn rejoin_rest_capture(
    expectation: ProviderCaptureSealExpectation,
    sealed: SealedProviderCaptureMaterial,
    coordinates: &SchwabCaptureCoordinates,
    receipt: &RawRestResponseReceipt,
) -> Result<ProviderWholeCaptureToken, SchwabTransportError> {
    let token = expectation
        .try_rejoin(sealed)
        .and_then(|capture| capture.try_into_whole())
        .map_err(|_| SchwabTransportError::CaptureMaterial)?;
    let capture = token.persisted_receipt().capture();
    if capture.source_id() != coordinates.source_id()
        || capture.metadata_revision() != coordinates.metadata_revision()
        || capture.dataset() != coordinates.dataset()
    {
        return Err(SchwabTransportError::CaptureMaterial);
    }
    let [page] = capture.pages() else {
        return Err(SchwabTransportError::CaptureMaterial);
    };
    let received_at = receipt
        .received_at_unix_millis()
        .checked_mul(1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .map(Timestamp::from_unix_nanos)
        .ok_or(SchwabTransportError::CaptureMaterial)?;
    if capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        || page.request_identity()
            != EvidenceDigest::new(DigestAlgorithm::Sha256, receipt.request_sha256())
        || page.http_status() != receipt.status()
        || page.body_digest() != EvidenceDigest::new(DigestAlgorithm::Sha256, receipt.body_sha256())
        || page.body_bytes() != receipt.body_bytes()
        || page.received_at() != received_at
    {
        return Err(SchwabTransportError::CaptureMaterial);
    }
    Ok(token)
}

/// Completed network outcome.
///
/// Market-data rejection/schema failure retains exact raw evidence. `userPreference` outcomes
/// retain only bounded receipt/error/bootstrap evidence and discard the account-shaped body.
#[derive(Debug)]
pub enum RestExecutionOutcome {
    Accepted(ExecutedRestResponse),
    AcceptedUserPreference(SchwabUserPreferenceEvidence),
    ProviderRejected(CapturedRestResponse),
    UserPreferenceRejected(RawRestResponseReceipt),
    InvalidPayload {
        capture: CapturedRestResponse,
        error: SchwabAdapterError,
    },
    InvalidUserPreference {
        receipt: RawRestResponseReceipt,
        error: SchwabAdapterError,
    },
}

impl RestExecutionOutcome {
    /// Returns exact response-scoped capacity evidence for this completed transport operation.
    ///
    /// Values are derived from the retained receipt and same-unit accounting. Callers cannot
    /// substitute a status, byte count, latency, Retry-After observation, or completion count.
    pub fn capacity_observation(&self) -> Result<CapacityObservation, SchwabTransportError> {
        match self {
            Self::Accepted(response) => response.capture.capacity_observation(false),
            Self::AcceptedUserPreference(response) => {
                capacity_observation_from_receipt(&response.receipt, response.accounting, false)
            }
            Self::ProviderRejected(capture) => capture.capacity_observation(false),
            Self::UserPreferenceRejected(receipt) => {
                capacity_observation_from_receipt(receipt, unavailable_accounting(1)?, false)
            }
            Self::InvalidPayload { capture, .. } => capture.capacity_observation(true),
            Self::InvalidUserPreference { receipt, .. } => {
                capacity_observation_from_receipt(receipt, unavailable_accounting(1)?, true)
            }
        }
    }
}

fn capacity_observation_from_receipt(
    receipt: &RawRestResponseReceipt,
    accounting: RestItemAccounting,
    validation_failed: bool,
) -> Result<CapacityObservation, SchwabTransportError> {
    let unit = match receipt.route() {
        ReadOnlyRoute::Quotes | ReadOnlyRoute::SingleQuote => CapacityUnit::Symbols,
        ReadOnlyRoute::Markets | ReadOnlyRoute::SingleMarket => CapacityUnit::MarketSegments,
        ReadOnlyRoute::UserPreference => CapacityUnit::Requests,
        ReadOnlyRoute::Chains
        | ReadOnlyRoute::ExpirationChain
        | ReadOnlyRoute::PriceHistory
        | ReadOnlyRoute::Movers
        | ReadOnlyRoute::Instruments
        | ReadOnlyRoute::InstrumentByCusip => CapacityUnit::LookupKeys,
    };
    CapacityObservation::from_transport(
        unit,
        accounting.requested,
        accounting.returned,
        accounting.missing,
        0,
        0,
        accounting.unexpected,
        u64::try_from(receipt.request_url().len()).map_err(|_| SchwabTransportError::Overflow)?,
        receipt.body_bytes(),
        receipt.latency_ms(),
        receipt.status(),
        receipt.retry_after_present(),
        validation_failed,
    )
    .validate()
    .map_err(|_| SchwabTransportError::Protocol)
}

/// Read-only REST executor with no credential or response persistence.
#[derive(Debug)]
pub struct SchwabRestExecutor {
    wire: Arc<dyn SchwabHttpWire>,
    transport_bounds: RestTransportBounds,
    parse_bounds: ParseBounds,
    token_admission: AccessTokenAdmission,
    telemetry: SchwabTransportTelemetry,
}

impl SchwabRestExecutor {
    /// Builds the hardened production reqwest executor.
    pub fn try_production(
        transport_bounds: RestTransportBounds,
        parse_bounds: ParseBounds,
        token_admission: AccessTokenAdmission,
        telemetry: SchwabTransportTelemetry,
    ) -> Result<Self, SchwabTransportError> {
        let wire = Arc::new(ReqwestSchwabHttpWire::try_new(transport_bounds)?);
        Self::try_new(
            wire,
            transport_bounds,
            parse_bounds,
            token_admission,
            telemetry,
        )
    }

    /// Injects a bounded wire, used by deterministic local verification and controlled proxies.
    pub fn try_new(
        wire: Arc<dyn SchwabHttpWire>,
        transport_bounds: RestTransportBounds,
        parse_bounds: ParseBounds,
        token_admission: AccessTokenAdmission,
        telemetry: SchwabTransportTelemetry,
    ) -> Result<Self, SchwabTransportError> {
        if parse_bounds.max_response_bytes() > transport_bounds.max_response_bytes() {
            return Err(SchwabTransportError::InvalidConfiguration);
        }
        Ok(Self {
            wire,
            transport_bounds,
            parse_bounds,
            token_admission,
            telemetry,
        })
    }

    /// Returns shared measured telemetry.
    pub const fn telemetry(&self) -> &SchwabTransportTelemetry {
        &self.telemetry
    }

    /// Executes one exact allowlisted GET using an immediate transient access token.
    pub async fn execute(
        &self,
        request: &ReadOnlyRequest,
        token: &TransientAccessToken,
        cancellation: CancellationToken,
    ) -> Result<RestExecutionOutcome, SchwabTransportError> {
        ReadOnlyRoute::classify(request.method(), request.url())?;
        token.validate_at(unix_seconds()?, self.token_admission)?;
        let requested =
            u64::try_from(request.requested_items()).map_err(|_| SchwabTransportError::Overflow)?;
        let request_bytes =
            u64::try_from(request.url().len()).map_err(|_| SchwabTransportError::Overflow)?;
        self.telemetry
            .record_rest_attempt(requested, request_bytes)?;
        let started = Instant::now();
        let operation = self.wire.get(SchwabHttpWireRequest::new(
            request,
            token.expose_bearer(),
            self.transport_bounds,
        ));
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(SchwabTransportError::Cancelled),
            result = tokio::time::timeout(self.transport_bounds.total_timeout(), operation) => {
                result.map_err(|_| SchwabTransportError::Deadline)?
            }
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.telemetry.record_rest_failure()?;
                return Err(error);
            }
        };
        validate_final_url(request, &response.final_url)?;
        let latency_ms = duration_millis(started.elapsed())?;
        let received_at_unix_millis = unix_millis()?;
        let SchwabHttpWireResponse {
            status,
            final_url: _,
            declared_body_bytes,
            headers,
            body,
        } = response;
        let body_bytes = u64::try_from(body.len()).map_err(|_| SchwabTransportError::Overflow)?;
        self.telemetry
            .record_rest_response(status, body_bytes, latency_ms)?;
        let receipt = RawRestResponseReceipt::new(
            request.route(),
            token.generation(),
            request.url().to_owned().into_boxed_str(),
            status,
            received_at_unix_millis,
            &body,
            declared_body_bytes,
            latency_ms,
            headers,
        )?;

        if request.route() == ReadOnlyRoute::UserPreference {
            if !(200..=299).contains(&status) {
                self.telemetry.record_rest_accounting(0, requested, 0, 0)?;
                return Ok(RestExecutionOutcome::UserPreferenceRejected(receipt));
            }
            if !response_is_json(receipt.headers()) {
                self.telemetry.record_validation_failure()?;
                self.telemetry.record_rest_accounting(0, requested, 0, 0)?;
                return Ok(RestExecutionOutcome::InvalidUserPreference {
                    receipt,
                    error: SchwabAdapterError::SchemaViolation,
                });
            }
            let bootstrap = match parse_user_preference(&body, self.parse_bounds) {
                Ok(bootstrap) => bootstrap,
                Err(error) => {
                    self.telemetry.record_validation_failure()?;
                    self.telemetry.record_rest_accounting(0, requested, 0, 0)?;
                    return Ok(RestExecutionOutcome::InvalidUserPreference { receipt, error });
                }
            };
            if bootstrap.raw_sha256() != receipt.body_sha256() || requested != 1 {
                self.telemetry.record_validation_failure()?;
                return Err(SchwabTransportError::Protocol);
            }
            let accounting = RestItemAccounting {
                requested,
                returned: 1,
                missing: 0,
                unexpected: 0,
                provider_records: 1,
            }
            .validate()?;
            self.telemetry.record_rest_accounting(
                accounting.returned,
                accounting.missing,
                accounting.unexpected,
                accounting.provider_records,
            )?;
            return Ok(RestExecutionOutcome::AcceptedUserPreference(
                SchwabUserPreferenceEvidence {
                    receipt,
                    bootstrap,
                    accounting,
                },
            ));
        }

        if !(200..=299).contains(&status) {
            let accounting = unavailable_accounting(requested)?;
            self.telemetry.record_rest_accounting(
                accounting.returned,
                accounting.missing,
                accounting.unexpected,
                accounting.provider_records,
            )?;
            return Ok(RestExecutionOutcome::ProviderRejected(
                CapturedRestResponse {
                    receipt,
                    accounting,
                    body,
                },
            ));
        }
        if !response_is_json(receipt.headers()) {
            self.telemetry.record_validation_failure()?;
            let accounting = unavailable_accounting(requested)?;
            self.telemetry.record_rest_accounting(
                accounting.returned,
                accounting.missing,
                accounting.unexpected,
                accounting.provider_records,
            )?;
            return Ok(RestExecutionOutcome::InvalidPayload {
                capture: CapturedRestResponse {
                    receipt,
                    accounting,
                    body,
                },
                error: SchwabAdapterError::SchemaViolation,
            });
        }
        let payload = match parse_payload(request.route(), &body, self.parse_bounds) {
            Ok(payload) => payload,
            Err(error) => {
                self.telemetry.record_validation_failure()?;
                let accounting = unavailable_accounting(requested)?;
                self.telemetry.record_rest_accounting(
                    accounting.returned,
                    accounting.missing,
                    accounting.unexpected,
                    accounting.provider_records,
                )?;
                return Ok(RestExecutionOutcome::InvalidPayload {
                    capture: CapturedRestResponse {
                        receipt,
                        accounting,
                        body,
                    },
                    error,
                });
            }
        };
        if payload.raw_sha256() != receipt.body_sha256() {
            self.telemetry.record_validation_failure()?;
            return Err(SchwabTransportError::Protocol);
        }
        let accounting = account_items(request, &payload)?.validate()?;
        self.telemetry.record_rest_accounting(
            accounting.returned,
            accounting.missing,
            accounting.unexpected,
            accounting.provider_records,
        )?;
        Ok(RestExecutionOutcome::Accepted(ExecutedRestResponse {
            capture: CapturedRestResponse {
                receipt,
                accounting,
                body,
            },
            payload,
        }))
    }
}

fn unavailable_accounting(requested: u64) -> Result<RestItemAccounting, SchwabTransportError> {
    RestItemAccounting {
        requested,
        returned: 0,
        missing: requested,
        unexpected: 0,
        provider_records: 0,
    }
    .validate()
}

fn parse_payload(
    route: ReadOnlyRoute,
    bytes: &[u8],
    bounds: ParseBounds,
) -> Result<SchwabRestPayload, SchwabAdapterError> {
    match route {
        ReadOnlyRoute::Quotes | ReadOnlyRoute::SingleQuote => {
            parse_quote_response(bytes, bounds).map(SchwabRestPayload::Quotes)
        }
        ReadOnlyRoute::Chains => {
            parse_option_chain_response(bytes, bounds).map(SchwabRestPayload::OptionChain)
        }
        ReadOnlyRoute::ExpirationChain => {
            parse_expiration_response(bytes, bounds).map(SchwabRestPayload::Expirations)
        }
        ReadOnlyRoute::PriceHistory => {
            parse_price_history_response(bytes, bounds).map(SchwabRestPayload::PriceHistory)
        }
        ReadOnlyRoute::Markets | ReadOnlyRoute::SingleMarket => {
            parse_market_hours_response(bytes, bounds).map(SchwabRestPayload::MarketHours)
        }
        ReadOnlyRoute::Movers => {
            parse_movers_response(bytes, bounds).map(SchwabRestPayload::Movers)
        }
        ReadOnlyRoute::Instruments | ReadOnlyRoute::InstrumentByCusip => {
            parse_instrument_response(bytes, bounds).map(SchwabRestPayload::Instruments)
        }
        ReadOnlyRoute::UserPreference => Err(SchwabAdapterError::RouteNotAllowed),
    }
}

fn account_items(
    request: &ReadOnlyRequest,
    payload: &SchwabRestPayload,
) -> Result<RestItemAccounting, SchwabTransportError> {
    let requested =
        u64::try_from(request.requested_items()).map_err(|_| SchwabTransportError::Overflow)?;
    let records =
        u64::try_from(payload.record_count()).map_err(|_| SchwabTransportError::Overflow)?;
    let (returned, unexpected) = match (request.route(), payload) {
        (ReadOnlyRoute::Quotes | ReadOnlyRoute::SingleQuote, SchwabRestPayload::Quotes(value)) => {
            let expected = expected_symbols(request)?;
            let actual = value
                .value()
                .quotes()
                .iter()
                .map(|quote| quote.symbol().as_str().to_owned())
                .collect::<BTreeSet<_>>();
            set_overlap(&expected, &actual)?
        }
        (ReadOnlyRoute::Chains, SchwabRestPayload::OptionChain(value)) => identity_match(
            query_value(request.url(), "symbol")?,
            value.value().symbol().as_str(),
        ),
        (ReadOnlyRoute::PriceHistory, SchwabRestPayload::PriceHistory(value)) => identity_match(
            query_value(request.url(), "symbol")?,
            value.value().symbol.as_str(),
        ),
        (
            ReadOnlyRoute::Markets | ReadOnlyRoute::SingleMarket,
            SchwabRestPayload::MarketHours(value),
        ) => {
            let expected = expected_markets(request)?;
            let actual = value
                .value()
                .iter()
                .map(|hours| hours.market_type.to_ascii_uppercase())
                .collect::<BTreeSet<_>>();
            set_overlap(&expected, &actual)?
        }
        (ReadOnlyRoute::Movers, SchwabRestPayload::Movers(value)) => {
            let expected = last_path_segment(request.url())?;
            match &value.value().index_symbol {
                crate::NativeField::Value(actual) => identity_match(expected, actual),
                crate::NativeField::Absent | crate::NativeField::Null => (0, 0),
            }
        }
        (ReadOnlyRoute::ExpirationChain, SchwabRestPayload::Expirations(value)) => {
            (u64::from(!value.value().expirations().is_empty()), 0)
        }
        (ReadOnlyRoute::Instruments, SchwabRestPayload::Instruments(value)) => {
            (u64::from(!value.value().instruments().is_empty()), 0)
        }
        (ReadOnlyRoute::InstrumentByCusip, SchwabRestPayload::Instruments(value)) => {
            let expected = last_path_segment(request.url())?;
            let actual = value
                .value()
                .instruments()
                .iter()
                .filter_map(|instrument| match &instrument.cusip {
                    crate::NativeField::Value(value) => Some(value.as_ref().to_owned()),
                    crate::NativeField::Absent | crate::NativeField::Null => None,
                })
                .collect::<BTreeSet<_>>();
            set_overlap(&BTreeSet::from([expected]), &actual)?
        }
        _ => return Err(SchwabTransportError::Protocol),
    };
    if returned > requested {
        return Err(SchwabTransportError::Protocol);
    }
    Ok(RestItemAccounting {
        requested,
        returned,
        missing: requested - returned,
        unexpected,
        provider_records: records,
    })
}

fn expected_symbols(request: &ReadOnlyRequest) -> Result<BTreeSet<String>, SchwabTransportError> {
    match request.route() {
        ReadOnlyRoute::Quotes => {
            let joined = query_value(request.url(), "symbols")?;
            let values = joined
                .split(',')
                .map(|value| ProviderIdentifier::try_new(value.to_owned()))
                .collect::<Result<Vec<_>, _>>()?;
            let set = values
                .into_iter()
                .map(|value| value.as_str().to_owned())
                .collect::<BTreeSet<_>>();
            if set.len() != request.requested_items() {
                return Err(SchwabTransportError::Protocol);
            }
            Ok(set)
        }
        ReadOnlyRoute::SingleQuote => Ok(BTreeSet::from([single_quote_symbol(request.url())?])),
        _ => Err(SchwabTransportError::Protocol),
    }
}

fn expected_markets(request: &ReadOnlyRequest) -> Result<BTreeSet<String>, SchwabTransportError> {
    match request.route() {
        ReadOnlyRoute::Markets => {
            let joined = query_value(request.url(), "markets")?;
            let set = joined
                .split(',')
                .map(str::to_ascii_uppercase)
                .collect::<BTreeSet<_>>();
            if set.len() != request.requested_items() {
                return Err(SchwabTransportError::Protocol);
            }
            Ok(set)
        }
        ReadOnlyRoute::SingleMarket => Ok(BTreeSet::from([
            last_path_segment(request.url())?.to_ascii_uppercase()
        ])),
        _ => Err(SchwabTransportError::Protocol),
    }
}

fn set_overlap(
    expected: &BTreeSet<String>,
    actual: &BTreeSet<String>,
) -> Result<(u64, u64), SchwabTransportError> {
    let returned = u64::try_from(expected.intersection(actual).count())
        .map_err(|_| SchwabTransportError::Overflow)?;
    let unexpected = u64::try_from(actual.difference(expected).count())
        .map_err(|_| SchwabTransportError::Overflow)?;
    Ok((returned, unexpected))
}

fn identity_match(expected: String, actual: &str) -> (u64, u64) {
    if expected == actual { (1, 0) } else { (0, 1) }
}

fn query_value(url: &str, name: &str) -> Result<String, SchwabTransportError> {
    let url = Url::parse(url).map_err(|_| SchwabTransportError::Protocol)?;
    let mut values = url
        .query_pairs()
        .filter(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned());
    let value = values.next().ok_or(SchwabTransportError::Protocol)?;
    if value.is_empty() || values.next().is_some() {
        return Err(SchwabTransportError::Protocol);
    }
    Ok(value)
}

fn single_quote_symbol(url: &str) -> Result<String, SchwabTransportError> {
    let url = Url::parse(url).map_err(|_| SchwabTransportError::Protocol)?;
    let segments = url
        .path_segments()
        .ok_or(SchwabTransportError::Protocol)?
        .collect::<Vec<_>>();
    match segments.as_slice() {
        ["marketdata", "v1", symbol, "quotes"] if !symbol.is_empty() => decode_path_segment(symbol),
        _ => Err(SchwabTransportError::Protocol),
    }
}

fn last_path_segment(url: &str) -> Result<String, SchwabTransportError> {
    Url::parse(url)
        .map_err(|_| SchwabTransportError::Protocol)?
        .path_segments()
        .and_then(Iterator::last)
        .filter(|value| !value.is_empty())
        .map(decode_path_segment)
        .transpose()?
        .ok_or(SchwabTransportError::Protocol)
}

fn decode_path_segment(value: &str) -> Result<String, SchwabTransportError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::new();
    decoded
        .try_reserve(bytes.len())
        .map_err(|_| SchwabTransportError::PayloadTooLarge)?;
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            decoded.push(bytes[index]);
            index = index.checked_add(1).ok_or(SchwabTransportError::Overflow)?;
            continue;
        }
        let high = bytes
            .get(index + 1)
            .copied()
            .and_then(hex_value)
            .ok_or(SchwabTransportError::Protocol)?;
        let low = bytes
            .get(index + 2)
            .copied()
            .and_then(hex_value)
            .ok_or(SchwabTransportError::Protocol)?;
        decoded.push((high << 4) | low);
        index = index.checked_add(3).ok_or(SchwabTransportError::Overflow)?;
    }
    String::from_utf8(decoded).map_err(|_| SchwabTransportError::Protocol)
}

const fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn validate_final_url(
    request: &ReadOnlyRequest,
    final_url: &str,
) -> Result<(), SchwabTransportError> {
    let expected = Url::parse(request.url()).map_err(|_| SchwabTransportError::Protocol)?;
    let actual = Url::parse(final_url).map_err(|_| SchwabTransportError::Protocol)?;
    if expected != actual
        || ReadOnlyRoute::classify(request.method(), final_url)? != request.route()
    {
        return Err(SchwabTransportError::Protocol);
    }
    Ok(())
}

fn response_is_json(headers: &[ResponseHeaderEvidence]) -> bool {
    headers.iter().any(|header| {
        header.name() == "content-type"
            && std::str::from_utf8(header.value()).is_ok_and(|value| {
                value
                    .split(';')
                    .next()
                    .is_some_and(|media| media.trim().eq_ignore_ascii_case("application/json"))
            })
    })
}

fn validate_content_encoding(
    headers: &[ResponseHeaderEvidence],
) -> Result<(), SchwabTransportError> {
    if headers.iter().any(|header| {
        header.name() == "content-encoding" && !header.value().eq_ignore_ascii_case(b"identity")
    }) {
        return Err(SchwabTransportError::Protocol);
    }
    Ok(())
}

fn selected_headers(
    headers: &HeaderMap,
    bounds: RestTransportBounds,
) -> Result<Vec<ResponseHeaderEvidence>, SchwabTransportError> {
    let mut output = Vec::new();
    let mut total = 0usize;
    for (name, value) in headers {
        if !retain_header(name) {
            continue;
        }
        if headers.get_all(name).iter().count() != 1 {
            return Err(SchwabTransportError::Protocol);
        }
        total = total
            .checked_add(name.as_str().len())
            .and_then(|current| current.checked_add(value.as_bytes().len()))
            .ok_or(SchwabTransportError::Overflow)?;
        if output.len() >= bounds.max_header_count() || total > bounds.max_header_bytes() {
            return Err(SchwabTransportError::HeaderBoundsExceeded);
        }
        output.push(ResponseHeaderEvidence::try_new(
            name.as_str().to_owned(),
            value.as_bytes().to_vec(),
        )?);
    }
    validate_header_evidence(&output, bounds)?;
    Ok(output)
}

fn retain_header(name: &HeaderName) -> bool {
    let value = name.as_str();
    name == CONTENT_TYPE
        || name == CONTENT_ENCODING
        || name == CONTENT_LENGTH
        || name == RETRY_AFTER
        || value.starts_with("x-ratelimit")
        || value.starts_with("ratelimit")
}

fn validate_header_evidence(
    headers: &[ResponseHeaderEvidence],
    bounds: RestTransportBounds,
) -> Result<(), SchwabTransportError> {
    if headers.len() > bounds.max_header_count() {
        return Err(SchwabTransportError::HeaderBoundsExceeded);
    }
    let mut names = BTreeSet::new();
    let mut total = 0usize;
    for header in headers {
        if header.name().is_empty()
            || !header
                .name()
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || !retain_header(
                &HeaderName::from_bytes(header.name().as_bytes())
                    .map_err(|_| SchwabTransportError::Protocol)?,
            )
            || !names.insert(header.name())
        {
            return Err(SchwabTransportError::Protocol);
        }
        total = total
            .checked_add(header.name().len())
            .and_then(|value| value.checked_add(header.value().len()))
            .ok_or(SchwabTransportError::Overflow)?;
        if total > bounds.max_header_bytes() {
            return Err(SchwabTransportError::HeaderBoundsExceeded);
        }
    }
    Ok(())
}

fn declared_length(headers: &HeaderMap) -> Result<Option<u64>, SchwabTransportError> {
    let mut values = headers.get_all(CONTENT_LENGTH).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(SchwabTransportError::Protocol);
    }
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > 20 || !bytes.iter().all(u8::is_ascii_digit) {
        return Err(SchwabTransportError::Protocol);
    }
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Some)
        .ok_or(SchwabTransportError::Protocol)
}

async fn collect_body(
    response: reqwest::Response,
    maximum: usize,
) -> Result<Bytes, SchwabTransportError> {
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(map_reqwest_error)?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .filter(|length| *length <= maximum)
            .ok_or(SchwabTransportError::PayloadTooLarge)?;
        body.try_reserve(next.saturating_sub(body.len()))
            .map_err(|_| SchwabTransportError::PayloadTooLarge)?;
        body.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(body))
}

fn map_reqwest_error(error: reqwest::Error) -> SchwabTransportError {
    if error.is_timeout() {
        SchwabTransportError::Deadline
    } else {
        SchwabTransportError::Network
    }
}
