//! Sealed authenticated transport for exact Alpaca v3 IEX/UTC calendar-range requests.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::StreamExt as _;
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_platform::RawCaptureRecord;
use market_squawk_sources::{
    HttpRequestBounds, ProviderCaptureMaterial, ProviderCapturePageReceipt,
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
};
use reqwest::header::{ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, HeaderMap, HeaderValue};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS, AlpacaCredentials, AlpacaError};

/// Maximum retained raw body for one exact bounded v3 calendar-range response.
///
/// One admitted historical plan spans at most ten years. This ceiling admits that complete
/// provider range without allowing the transport to inherit the wider caller HTTP bound.
pub const ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAXIMUM_RETRY_AFTER_FIELD_BYTES: usize = 128;
const USER_AGENT: &str = "market-squawk/0.1 alpaca-historical-calendar";

/// Exact Alpaca Trading API account environment retained by account activation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AlpacaTradingApiEnvironment {
    /// The live Trading API account origin.
    Live,
    /// The paper Trading API account origin.
    Paper,
}

impl AlpacaTradingApiEnvironment {
    /// Returns the sole official origin for this explicit account environment.
    pub const fn origin(self) -> &'static str {
        match self {
            Self::Live => "https://api.alpaca.markets",
            Self::Paper => "https://paper-api.alpaca.markets",
        }
    }
}

/// Exact credential-free coordinates independently comparable with the application calendar
/// producer request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaAuthenticatedCalendarRequest {
    environment: AlpacaTradingApiEnvironment,
    start_date: CalendarDate,
    end_date: CalendarDate,
    path_and_query: Box<str>,
    url: Url,
}

impl AlpacaAuthenticatedCalendarRequest {
    /// Constructs only `GET /v3/calendar/IEX` with exact inclusive range and UTC coordinates.
    pub fn try_new(
        environment: AlpacaTradingApiEnvironment,
        start_date: CalendarDate,
        end_date: CalendarDate,
    ) -> Result<Self, AlpacaError> {
        let inclusive_span_days = end_date
            .days_since_unix_epoch()
            .checked_sub(start_date.days_since_unix_epoch())
            .and_then(|days| days.checked_add(1));
        if start_date > end_date
            || start_date.year() > 9_999
            || end_date.year() > 9_999
            || inclusive_span_days
                .is_none_or(|days| days > i32::from(ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS) + 1)
        {
            return Err(AlpacaError::InvalidHistoricalPlan);
        }
        let path_and_query =
            format!("/v3/calendar/IEX?start={start_date}&end={end_date}&timezone=UTC");
        let target = format!("{}{path_and_query}", environment.origin());
        let url = Url::parse(&target).map_err(|_| AlpacaError::InvalidHistoricalPlan)?;
        if url.as_str() != target {
            return Err(AlpacaError::InvalidHistoricalPlan);
        }
        Ok(Self {
            environment,
            start_date,
            end_date,
            path_and_query: path_and_query.into_boxed_str(),
            url,
        })
    }

    /// Returns the only admitted HTTP method.
    pub const fn method(&self) -> &'static str {
        "GET"
    }

    /// Returns the exact explicit account origin.
    pub const fn origin(&self) -> &'static str {
        self.environment.origin()
    }

    /// Returns the exact current v3 path and complete IEX/range/UTC query.
    pub const fn path_and_query(&self) -> &str {
        &self.path_and_query
    }

    /// Returns the inclusive first requested civil date.
    pub const fn start_date(&self) -> CalendarDate {
        self.start_date
    }

    /// Returns the inclusive last requested civil date.
    pub const fn end_date(&self) -> CalendarDate {
        self.end_date
    }
}

/// Secret-free bounded response facts from one exact authenticated calendar-range request.
#[derive(Debug, Eq, PartialEq)]
pub struct AlpacaAuthenticatedCalendarResponse {
    request: AlpacaAuthenticatedCalendarRequest,
    status: u16,
    body: Bytes,
    received_at: Timestamp,
    retry_after: Option<Box<[u8]>>,
}

impl AlpacaAuthenticatedCalendarResponse {
    /// Returns the exact request whose response was collected.
    pub const fn request(&self) -> &AlpacaAuthenticatedCalendarRequest {
        &self.request
    }

    /// Returns the provider HTTP status without interpreting calendar availability.
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the bounded exact raw response body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Returns the exact local time recorded immediately after the complete bounded body arrived.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns the bounded raw `Retry-After` field, when exactly one was supplied.
    pub fn retry_after(&self) -> Option<&[u8]> {
        self.retry_after.as_deref()
    }

    /// Consumes the response without ever exposing credential material.
    pub fn into_body(self) -> Bytes {
        self.body
    }

    /// Binds an accepted exact range response to standalone source-neutral capture material.
    ///
    /// Only HTTP 200 is eligible. Refusal/retry attempts remain runtime telemetry and cannot be
    /// passed through this publication boundary. The exact request identity contains only method,
    /// origin, IEX path/range, and UTC coordinates; credentials and headers are structurally absent.
    pub fn provider_capture_material(
        &self,
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        dataset: SourceIdentifier,
    ) -> Result<ProviderCaptureMaterial, AlpacaError> {
        if self.status != 200 || self.body.is_empty() {
            return Err(AlpacaError::CaptureMaterial);
        }
        let request_identity = calendar_request_identity(&self.request)?;
        let body_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&self.body).into());
        let page = ProviderCapturePageReceipt::try_new(
            0,
            request_identity,
            None,
            None,
            self.status,
            u64::try_from(self.body.len()).map_err(|_| AlpacaError::CaptureMaterial)?,
            body_digest,
            self.received_at,
        )
        .map_err(|_| AlpacaError::CaptureMaterial)?;
        let capture = ProviderCaptureSetReceipt::try_new(
            source_id.clone(),
            metadata_revision,
            dataset,
            request_identity,
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![page],
        )
        .map_err(|_| AlpacaError::CaptureMaterial)?;
        let connection_id =
            Uuid::new_v5(&Uuid::NAMESPACE_URL, &capture.observation_digest().bytes());
        let mut event_identity = Sha256::new();
        event_identity.update(b"market-squawk/alpaca-iex-calendar-capture-event/v1\0");
        event_identity.update(request_identity.bytes());
        event_identity.update(body_digest.bytes());
        let event_id = Uuid::new_v5(&connection_id, &event_identity.finalize());
        if connection_id.is_nil() || event_id.is_nil() {
            return Err(AlpacaError::CaptureMaterial);
        }
        let record = RawCaptureRecord::try_new_live(
            event_id,
            Arc::from(source_id.as_str()),
            connection_id,
            Some(0),
            None,
            DateTime::<Utc>::from_timestamp_nanos(self.received_at.unix_nanos()),
            self.body.clone(),
        )
        .map_err(|_| AlpacaError::CaptureMaterial)?;
        ProviderCaptureMaterial::try_new(capture, vec![record])
            .map_err(|_| AlpacaError::CaptureMaterial)
    }
}

/// Credential-bearing executor that can issue only the exact calendar-range request grammar.
pub struct AlpacaAuthenticatedCalendarExecutor {
    credentials: Arc<AlpacaCredentials>,
    client: reqwest::Client,
    bounds: HttpRequestBounds,
}

impl AlpacaAuthenticatedCalendarExecutor {
    /// Constructs a hardened, redirect-free executor around the already-loaded credential arc.
    pub fn try_new(
        credentials: Arc<AlpacaCredentials>,
        bounds: HttpRequestBounds,
    ) -> Result<Self, AlpacaError> {
        Ok(Self {
            credentials,
            client: hardened_client(bounds, USER_AGENT)?,
            bounds,
        })
    }

    /// Executes one exact request with caller-owned absolute deadline and cancellation.
    pub async fn execute(
        &self,
        request: AlpacaAuthenticatedCalendarRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaAuthenticatedCalendarResponse, AlpacaError> {
        let response = authenticated_bounded_get(
            &self.client,
            &self.credentials,
            &request.url,
            self.bounds,
            ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES,
            deadline,
            cancellation,
        )
        .await?;
        let retry_after = singleton_bounded_header(
            &response.headers,
            reqwest::header::RETRY_AFTER,
            MAXIMUM_RETRY_AFTER_FIELD_BYTES,
        )?;
        Ok(AlpacaAuthenticatedCalendarResponse {
            request,
            status: response.status,
            body: Bytes::from(response.body),
            received_at: response.received_at,
            retry_after,
        })
    }
}

impl std::fmt::Debug for AlpacaAuthenticatedCalendarExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaAuthenticatedCalendarExecutor")
            .field("credentials", &"[REDACTED ZEROIZING ARC]")
            .field("bounds", &self.bounds)
            .finish_non_exhaustive()
    }
}

pub(crate) struct AuthenticatedGetResponse {
    pub(crate) status: u16,
    pub(crate) body: Box<[u8]>,
    pub(crate) headers: HeaderMap,
    pub(crate) received_at: Timestamp,
}

pub(crate) fn hardened_client(
    bounds: HttpRequestBounds,
    user_agent: &'static str,
) -> Result<reqwest::Client, AlpacaError> {
    reqwest::Client::builder()
        .https_only(true)
        .tls_backend_rustls()
        .min_tls_version(reqwest::tls::Version::TLS_1_2)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .retry(reqwest::retry::never())
        .referer(false)
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .connect_timeout(Duration::from_nanos(bounds.connect_timeout_nanos()))
        .read_timeout(Duration::from_nanos(bounds.read_timeout_nanos()))
        .timeout(Duration::from_nanos(bounds.total_timeout_nanos()))
        .user_agent(user_agent)
        .build()
        .map_err(|_| AlpacaError::Network)
}

pub(crate) async fn authenticated_bounded_get(
    client: &reqwest::Client,
    credentials: &AlpacaCredentials,
    url: &Url,
    bounds: HttpRequestBounds,
    hard_maximum_bytes: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<AuthenticatedGetResponse, AlpacaError> {
    if cancellation.is_cancelled() {
        return Err(AlpacaError::Cancelled);
    }
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(AlpacaError::DeadlineExceeded)?;
    let total_timeout = Duration::from_nanos(bounds.total_timeout_nanos()).min(remaining);
    if total_timeout.is_zero() {
        return Err(AlpacaError::DeadlineExceeded);
    }
    let operation = client
        .get(url.clone())
        .headers(authorization_headers(credentials)?);
    let response = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Err(AlpacaError::Cancelled),
        result = tokio::time::timeout(total_timeout, operation.send()) => match result {
            Ok(Ok(response)) => response,
            Ok(Err(_error)) => return Err(AlpacaError::Network),
            Err(_elapsed) => return Err(AlpacaError::DeadlineExceeded),
        },
    };
    if response.url() != url {
        return Err(AlpacaError::Protocol);
    }
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    if singleton_bounded_header(&headers, CONTENT_ENCODING, 64)?
        .is_some_and(|encoding| !encoding.eq_ignore_ascii_case(b"identity"))
    {
        return Err(AlpacaError::Protocol);
    }
    let configured_maximum = usize::try_from(bounds.max_response_bytes())
        .map_err(|_| AlpacaError::InvalidTransportLimits)?;
    let maximum = configured_maximum.min(hard_maximum_bytes);
    if maximum == 0
        || response
            .content_length()
            .is_some_and(|length| usize::try_from(length).map_or(true, |length| length > maximum))
    {
        return Err(AlpacaError::BodyTooLarge);
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(AlpacaError::DeadlineExceeded)?;
        let read_timeout = Duration::from_nanos(bounds.read_timeout_nanos()).min(remaining);
        if read_timeout.is_zero() {
            return Err(AlpacaError::DeadlineExceeded);
        }
        let next = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(AlpacaError::Cancelled),
            result = tokio::time::timeout(read_timeout, stream.next()) => match result {
                Ok(next) => next,
                Err(_elapsed) => return Err(AlpacaError::DeadlineExceeded),
            },
        };
        let Some(chunk) = next else { break };
        let chunk = chunk.map_err(|_| AlpacaError::Network)?;
        let next_length = body
            .len()
            .checked_add(chunk.len())
            .ok_or(AlpacaError::BodyTooLarge)?;
        if next_length > maximum {
            return Err(AlpacaError::BodyTooLarge);
        }
        body.try_reserve_exact(chunk.len())
            .map_err(|_| AlpacaError::Allocation)?;
        body.extend_from_slice(&chunk);
    }
    Ok(AuthenticatedGetResponse {
        status,
        body: body.into_boxed_slice(),
        headers,
        received_at: system_timestamp()?,
    })
}

fn calendar_request_identity(
    request: &AlpacaAuthenticatedCalendarRequest,
) -> Result<EvidenceDigest, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-iex-calendar-capture-request/v1\0");
    hash_request_field(&mut digest, request.method())?;
    hash_request_field(&mut digest, request.origin())?;
    hash_request_field(&mut digest, request.path_and_query())?;
    digest.update(request.start_date().year().to_be_bytes());
    digest.update([request.start_date().month(), request.start_date().day()]);
    digest.update(request.end_date().year().to_be_bytes());
    digest.update([request.end_date().month(), request.end_date().day()]);
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_request_field(digest: &mut Sha256, value: &str) -> Result<(), AlpacaError> {
    digest.update(
        u32::try_from(value.len())
            .map_err(|_| AlpacaError::CaptureMaterial)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

fn system_timestamp() -> Result<Timestamp, AlpacaError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AlpacaError::Network)?
        .as_nanos();
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(nanos).map_err(|_| AlpacaError::Network)?,
    ))
}

fn authorization_headers(credentials: &AlpacaCredentials) -> Result<HeaderMap, AlpacaError> {
    let mut headers = HeaderMap::new();
    let mut key =
        HeaderValue::from_str(credentials.key_id()).map_err(|_| AlpacaError::InvalidCredentials)?;
    key.set_sensitive(true);
    let mut secret = HeaderValue::from_str(credentials.secret_key())
        .map_err(|_| AlpacaError::InvalidCredentials)?;
    secret.set_sensitive(true);
    headers.insert("apca-api-key-id", key);
    headers.insert("apca-api-secret-key", secret);
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    Ok(headers)
}

pub(crate) fn singleton_bounded_header(
    headers: &HeaderMap,
    name: reqwest::header::HeaderName,
    maximum_bytes: usize,
) -> Result<Option<Box<[u8]>>, AlpacaError> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() || value.as_bytes().len() > maximum_bytes {
        return Err(AlpacaError::Protocol);
    }
    Ok(Some(value.as_bytes().to_vec().into_boxed_slice()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authenticated_calendar_request_is_one_exact_inclusive_iex_utc_range() {
        let start = CalendarDate::new(2024, 11, 25).expect("valid start");
        let end = CalendarDate::new(2024, 11, 29).expect("valid end");
        let request = AlpacaAuthenticatedCalendarRequest::try_new(
            AlpacaTradingApiEnvironment::Paper,
            start,
            end,
        )
        .expect("bounded range");

        assert_eq!(request.method(), "GET");
        assert_eq!(request.origin(), "https://paper-api.alpaca.markets");
        assert_eq!(
            request.path_and_query(),
            "/v3/calendar/IEX?start=2024-11-25&end=2024-11-29&timezone=UTC"
        );
        assert_eq!(request.start_date(), start);
        assert_eq!(request.end_date(), end);
        let body = Bytes::from_static(br#"{"market":{"exchange":"IEX"},"calendar":[]}"#);
        let accepted = AlpacaAuthenticatedCalendarResponse {
            request: request.clone(),
            status: 200,
            body: body.clone(),
            received_at: Timestamp::from_unix_nanos(1_735_800_000_000_000_000),
            retry_after: None,
        };
        let material = accepted
            .provider_capture_material(
                SourceId::try_from("alpaca-iex-calendar-test").expect("source identity"),
                MetadataRevision::new(
                    SourceIdentifier::try_from("alpaca-iex-calendar-test-v1")
                        .expect("revision identity"),
                ),
                SourceIdentifier::try_from("alpaca:historical-equity:test")
                    .expect("dataset identity"),
            )
            .expect("accepted calendar capture");
        assert_eq!(material.receipt().pages().len(), 1);
        assert_eq!(material.receipt().pages()[0].http_status(), 200);
        assert_eq!(material.records()[0].payload(), body);
        assert_eq!(material.records()[0].source_sequence(), Some(0));

        let refusal = AlpacaAuthenticatedCalendarResponse {
            request: request.clone(),
            status: 429,
            body,
            received_at: Timestamp::from_unix_nanos(1_735_800_001_000_000_000),
            retry_after: Some(b"1".to_vec().into_boxed_slice()),
        };
        assert!(
            refusal
                .provider_capture_material(
                    SourceId::try_from("alpaca-iex-calendar-test").expect("source identity"),
                    MetadataRevision::new(
                        SourceIdentifier::try_from("alpaca-iex-calendar-test-v1")
                            .expect("revision identity"),
                    ),
                    SourceIdentifier::try_from("alpaca:historical-equity:test")
                        .expect("dataset identity"),
                )
                .is_err()
        );
        assert!(
            AlpacaAuthenticatedCalendarRequest::try_new(
                AlpacaTradingApiEnvironment::Paper,
                end,
                start,
            )
            .is_err()
        );
        let overlong_end = CalendarDate::new(2035, 11, 29).expect("valid overlong end");
        assert!(
            AlpacaAuthenticatedCalendarRequest::try_new(
                AlpacaTradingApiEnvironment::Paper,
                start,
                overlong_end,
            )
            .is_err()
        );
    }
}
