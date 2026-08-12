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
    ProviderCaptureMaterial, ProviderCapturePageReceipt, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition,
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
    ExpirationResponse, InstrumentResponse, MarketHours, MoversResponse, OptionChain, ParseBounds,
    ParsedNative, PriceHistoryResponse, ProviderIdentifier, QuoteResponse, ReadOnlyRequest,
    ReadOnlyRoute, SchwabAdapterError, StreamerBootstrapResponse, parse_expiration_response,
    parse_instrument_response, parse_market_hours_response, parse_movers_response,
    parse_option_chain_response, parse_price_history_response, parse_quote_response,
    parse_user_preference,
};

use super::{
    AccessTokenAdmission, RawRestResponseReceipt, ResponseHeaderEvidence, RestTransportBounds,
    SchwabCaptureCoordinates, SchwabTransportError, SchwabTransportTelemetry, TransientAccessToken,
    duration_millis, unix_millis, unix_seconds,
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
#[derive(Clone, Debug)]
pub struct SchwabHttpWireResponse {
    status: u16,
    final_url: Box<str>,
    declared_body_bytes: Option<u64>,
    headers: Box<[ResponseHeaderEvidence]>,
    body: Bytes,
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

    pub const fn status(&self) -> u16 {
        self.status
    }

    pub fn final_url(&self) -> &str {
        &self.final_url
    }

    pub const fn declared_body_bytes(&self) -> Option<u64> {
        self.declared_body_bytes
    }

    pub fn headers(&self) -> &[ResponseHeaderEvidence] {
        &self.headers
    }

    pub const fn body(&self) -> &Bytes {
        &self.body
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

impl SchwabHttpWire for ReqwestSchwabHttpWire {
    fn get<'a>(
        &'a self,
        request: SchwabHttpWireRequest<'a>,
    ) -> Pin<
        Box<dyn Future<Output = Result<SchwabHttpWireResponse, SchwabTransportError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let mut authorization = Zeroizing::new(String::from("Bearer "));
            authorization.push_str(request.bearer());
            let mut authorization = reqwest::header::HeaderValue::from_str(&authorization)
                .map_err(|_| SchwabTransportError::InvalidToken)?;
            authorization.set_sensitive(true);
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedRestResponse {
    receipt: RawRestResponseReceipt,
    body: Bytes,
}

impl CapturedRestResponse {
    pub const fn receipt(&self) -> &RawRestResponseReceipt {
        &self.receipt
    }

    pub const fn body(&self) -> &Bytes {
        &self.body
    }

    /// Converts an accepted exact provider response into the shared source-neutral capture
    /// material. The registered source/runtime owns all source identities and durable UUIDs.
    ///
    /// OAuth errors, bearer tokens, request headers, and `userPreference` bootstrap/account
    /// material are deliberately ineligible for this conversion.
    pub fn try_into_provider_capture_material(
        self,
        coordinates: SchwabCaptureCoordinates,
        event_id: Uuid,
    ) -> Result<ProviderCaptureMaterial, SchwabTransportError> {
        if self.receipt.route() == ReadOnlyRoute::UserPreference
            || !(200..=299).contains(&self.receipt.status())
            || event_id.is_nil()
        {
            return Err(SchwabTransportError::CaptureMaterial);
        }
        let received_nanos = i64::try_from(self.receipt.received_at_unix_millis())
            .ok()
            .and_then(|value| value.checked_mul(1_000_000))
            .ok_or(SchwabTransportError::CaptureMaterial)?;
        let received_at = Timestamp::from_unix_nanos(received_nanos);
        let page = ProviderCapturePageReceipt::try_new(
            0,
            EvidenceDigest::new(DigestAlgorithm::Sha256, self.receipt.request_sha256()),
            None,
            None,
            self.receipt.status(),
            self.receipt.body_bytes(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, self.receipt.body_sha256()),
            received_at,
        )
        .map_err(|_| SchwabTransportError::CaptureMaterial)?;
        let receipt = ProviderCaptureSetReceipt::try_new(
            coordinates.source_id().clone(),
            coordinates.metadata_revision().clone(),
            coordinates.dataset().clone(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, self.receipt.request_sha256()),
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![page],
        )
        .map_err(|_| SchwabTransportError::CaptureMaterial)?;
        let received_at = chrono::DateTime::from_timestamp_nanos(received_nanos);
        let record = RawCaptureRecord::try_new_live(
            event_id,
            Arc::from(coordinates.source_id().as_str()),
            coordinates.connection_id(),
            Some(0),
            None,
            received_at,
            self.body,
        )
        .map_err(|_| SchwabTransportError::CaptureMaterial)?;
        ProviderCaptureMaterial::try_new(receipt, vec![record])
            .map_err(|_| SchwabTransportError::CaptureMaterial)
    }
}

/// Closed provider-native REST payload selected solely by the allowlisted route.
#[derive(Debug)]
pub enum SchwabRestPayload {
    Quotes(ParsedNative<QuoteResponse>),
    OptionChain(ParsedNative<OptionChain>),
    Expirations(ParsedNative<ExpirationResponse>),
    PriceHistory(ParsedNative<PriceHistoryResponse>),
    MarketHours(ParsedNative<Box<[MarketHours]>>),
    Movers(ParsedNative<MoversResponse>),
    Instruments(ParsedNative<InstrumentResponse>),
    StreamerBootstrap(StreamerBootstrapResponse),
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
            Self::StreamerBootstrap(_) => 1,
        }
    }

    fn raw_sha256(&self) -> [u8; 32] {
        match self {
            Self::Quotes(value) => value.raw_sha256(),
            Self::OptionChain(value) => value.raw_sha256(),
            Self::Expirations(value) => value.raw_sha256(),
            Self::PriceHistory(value) => value.raw_sha256(),
            Self::MarketHours(value) => value.raw_sha256(),
            Self::Movers(value) => value.raw_sha256(),
            Self::Instruments(value) => value.raw_sha256(),
            Self::StreamerBootstrap(value) => value.raw_sha256(),
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

/// Successful captured and provider-native-validated REST operation.
#[derive(Debug)]
pub struct ExecutedRestResponse {
    capture: CapturedRestResponse,
    payload: SchwabRestPayload,
    accounting: RestItemAccounting,
}

impl ExecutedRestResponse {
    pub const fn capture(&self) -> &CapturedRestResponse {
        &self.capture
    }

    pub const fn payload(&self) -> &SchwabRestPayload {
        &self.payload
    }

    pub const fn accounting(&self) -> RestItemAccounting {
        self.accounting
    }
}

/// Completed network outcome. Provider rejection and schema failure retain exact raw evidence.
#[derive(Debug)]
pub enum RestExecutionOutcome {
    Accepted(ExecutedRestResponse),
    ProviderRejected(CapturedRestResponse),
    InvalidPayload {
        capture: CapturedRestResponse,
        error: SchwabAdapterError,
    },
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
        validate_final_url(request, response.final_url())?;
        let latency_ms = duration_millis(started.elapsed())?;
        let received_at_unix_millis = unix_millis()?;
        let body_bytes =
            u64::try_from(response.body().len()).map_err(|_| SchwabTransportError::Overflow)?;
        self.telemetry
            .record_rest_response(response.status(), body_bytes, latency_ms)?;
        let receipt = RawRestResponseReceipt::new(
            request.route(),
            token.generation(),
            request.url().to_owned().into_boxed_str(),
            response.status(),
            received_at_unix_millis,
            response.body(),
            response.declared_body_bytes(),
            latency_ms,
            response.headers().to_vec().into_boxed_slice(),
        )?;
        let capture = CapturedRestResponse {
            receipt,
            body: response.body().clone(),
        };
        if !(200..=299).contains(&response.status()) {
            self.telemetry.record_rest_accounting(0, requested, 0, 0)?;
            return Ok(RestExecutionOutcome::ProviderRejected(capture));
        }
        if !response_is_json(response.headers()) {
            self.telemetry.record_validation_failure()?;
            self.telemetry.record_rest_accounting(0, requested, 0, 0)?;
            return Ok(RestExecutionOutcome::InvalidPayload {
                capture,
                error: SchwabAdapterError::SchemaViolation,
            });
        }
        let payload = match parse_payload(request.route(), response.body(), self.parse_bounds) {
            Ok(payload) => payload,
            Err(error) => {
                self.telemetry.record_validation_failure()?;
                self.telemetry.record_rest_accounting(0, requested, 0, 0)?;
                return Ok(RestExecutionOutcome::InvalidPayload { capture, error });
            }
        };
        if payload.raw_sha256() != capture.receipt().body_sha256() {
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
            capture,
            payload,
            accounting,
        }))
    }
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
        ReadOnlyRoute::UserPreference => {
            parse_user_preference(bytes, bounds).map(SchwabRestPayload::StreamerBootstrap)
        }
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
        (ReadOnlyRoute::ExpirationChain, SchwabRestPayload::Expirations(_))
        | (ReadOnlyRoute::Instruments, SchwabRestPayload::Instruments(_))
        | (ReadOnlyRoute::InstrumentByCusip, SchwabRestPayload::Instruments(_))
        | (ReadOnlyRoute::UserPreference, SchwabRestPayload::StreamerBootstrap(_)) => (1, 0),
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
