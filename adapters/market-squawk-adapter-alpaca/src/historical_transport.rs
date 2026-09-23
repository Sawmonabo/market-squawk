//! Sealed production and debug-fixture execution for Alpaca historical HTTP requests.

use std::sync::Arc;
use std::time::Instant;

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
use std::collections::VecDeque;
#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
use std::sync::Mutex;
#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
use std::time::Duration;

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
use bytes::Bytes;
#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
use chrono::DateTime;
#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
use market_squawk_domain::{CalendarDate, Timestamp};
use market_squawk_sources::HttpRequestBounds;
#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
use reqwest::header::{CONTENT_ENCODING, HeaderMap, HeaderName, HeaderValue};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::historical_calendar::{
    AuthenticatedGetResponse, authenticated_bounded_get, hardened_client,
};
#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
use crate::historical_calendar::{authorization_headers, singleton_bounded_header};
use crate::{AlpacaCredentials, AlpacaError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlpacaHistoricalEndpoint {
    Bars,
    Calendar,
}

#[derive(Clone)]
pub(crate) struct AlpacaHistoricalTransport {
    inner: Arc<AlpacaHistoricalTransportInner>,
}

enum AlpacaHistoricalTransportInner {
    Hardened(reqwest::Client),
    #[cfg(any(
        test,
        all(feature = "scripted-historical-transport-fixture", debug_assertions)
    ))]
    Scripted(Arc<AlpacaHistoricalScriptedState>),
}

impl AlpacaHistoricalTransport {
    pub(crate) fn try_hardened(
        bounds: HttpRequestBounds,
        user_agent: &'static str,
    ) -> Result<Self, AlpacaError> {
        Ok(Self {
            inner: Arc::new(AlpacaHistoricalTransportInner::Hardened(hardened_client(
                bounds, user_agent,
            )?)),
        })
    }

    #[cfg(any(
        test,
        all(feature = "scripted-historical-transport-fixture", debug_assertions)
    ))]
    fn try_scripted(
        bar: AlpacaHistoricalScriptedResponse,
        calendar: AlpacaHistoricalScriptedResponse,
    ) -> Result<Self, AlpacaError> {
        let mut steps = VecDeque::new();
        steps
            .try_reserve_exact(2)
            .map_err(|_| AlpacaError::Allocation)?;
        steps.push_back(AlpacaHistoricalScriptedStep::Bar(bar));
        steps.push_back(AlpacaHistoricalScriptedStep::Calendar(calendar));
        Ok(Self {
            inner: Arc::new(AlpacaHistoricalTransportInner::Scripted(Arc::new(
                AlpacaHistoricalScriptedState {
                    steps: Mutex::new(steps),
                    bar_dispatches: AtomicU64::new(0),
                    calendar_dispatches: AtomicU64::new(0),
                },
            ))),
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the sealed endpoint, credentials, request, policy bounds, and caller controls stay explicit"
    )]
    pub(crate) async fn authenticated_get(
        &self,
        _endpoint: AlpacaHistoricalEndpoint,
        credentials: &AlpacaCredentials,
        url: &Url,
        bounds: HttpRequestBounds,
        hard_maximum_bytes: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AuthenticatedGetResponse, AlpacaError> {
        match self.inner.as_ref() {
            AlpacaHistoricalTransportInner::Hardened(client) => {
                authenticated_bounded_get(
                    client,
                    credentials,
                    url,
                    bounds,
                    hard_maximum_bytes,
                    deadline,
                    cancellation,
                )
                .await
            }
            #[cfg(any(
                test,
                all(feature = "scripted-historical-transport-fixture", debug_assertions)
            ))]
            AlpacaHistoricalTransportInner::Scripted(state) => {
                if cancellation.is_cancelled() {
                    return Err(AlpacaError::Cancelled);
                }
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .ok_or(AlpacaError::DeadlineExceeded)?;
                if Duration::from_nanos(bounds.total_timeout_nanos())
                    .min(remaining)
                    .is_zero()
                {
                    return Err(AlpacaError::DeadlineExceeded);
                }
                let configured_maximum = usize::try_from(bounds.max_response_bytes())
                    .map_err(|_| AlpacaError::InvalidTransportLimits)?;
                let maximum = configured_maximum.min(hard_maximum_bytes);
                if maximum == 0 {
                    return Err(AlpacaError::BodyTooLarge);
                }
                drop(authorization_headers(credentials)?);
                let response = state.execute(_endpoint, url, maximum, deadline, cancellation)?;
                if singleton_bounded_header(&response.headers, CONTENT_ENCODING, 64)?
                    .is_some_and(|encoding| !encoding.eq_ignore_ascii_case(b"identity"))
                {
                    return Err(AlpacaError::Protocol);
                }
                Ok(response)
            }
        }
    }
}

impl std::fmt::Debug for AlpacaHistoricalTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode = match self.inner.as_ref() {
            AlpacaHistoricalTransportInner::Hardened(_) => "hardened",
            #[cfg(any(
                test,
                all(feature = "scripted-historical-transport-fixture", debug_assertions)
            ))]
            AlpacaHistoricalTransportInner::Scripted(_) => "scripted",
        };
        formatter
            .debug_struct("AlpacaHistoricalTransport")
            .field("mode", &mode)
            .finish_non_exhaustive()
    }
}

/// Closed response-header facts admitted by the debug-only historical transport fixture.
#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlpacaHistoricalScriptedHeader {
    /// One bounded raw `Retry-After` field.
    RetryAfter(Box<[u8]>),
    /// One bounded raw `Content-Type` field.
    ContentType(Box<[u8]>),
    /// One bounded raw `Content-Encoding` field.
    ContentEncoding(Box<[u8]>),
    /// Provider-declared request-window capacity.
    RateLimitLimit(u32),
    /// Provider-declared remaining request capacity.
    RateLimitRemaining(u32),
    /// Provider-declared Unix reset coordinate.
    RateLimitReset(i64),
}

/// One bounded canned response consumed only after an exact scripted historical dispatch.
#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
pub struct AlpacaHistoricalScriptedResponse {
    status: u16,
    headers: HeaderMap,
    body: Box<[u8]>,
    received_at: Timestamp,
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
impl AlpacaHistoricalScriptedResponse {
    /// Validates one closed status/header/body/time response fact without accepting request routing.
    pub fn try_new(
        status: u16,
        header_facts: Vec<AlpacaHistoricalScriptedHeader>,
        body: impl Into<Bytes>,
        received_at: Timestamp,
    ) -> Result<Self, AlpacaError> {
        if reqwest::StatusCode::from_u16(status).is_err()
            || header_facts.len() > 6
            || received_at.unix_nanos() < 0
        {
            return Err(AlpacaError::Protocol);
        }
        let body = body.into();
        if u64::try_from(body.len()).map_or(true, |length| {
            length > market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGE_BYTES
        }) {
            return Err(AlpacaError::BodyTooLarge);
        }
        let mut headers = HeaderMap::new();
        for fact in header_facts {
            match fact {
                AlpacaHistoricalScriptedHeader::RetryAfter(value) => {
                    insert_raw_header(&mut headers, reqwest::header::RETRY_AFTER, value, 128)?
                }
                AlpacaHistoricalScriptedHeader::ContentType(value) => {
                    insert_raw_header(&mut headers, reqwest::header::CONTENT_TYPE, value, 256)?
                }
                AlpacaHistoricalScriptedHeader::ContentEncoding(value) => {
                    insert_raw_header(&mut headers, CONTENT_ENCODING, value, 64)?;
                }
                AlpacaHistoricalScriptedHeader::RateLimitLimit(value) => insert_integer_header(
                    &mut headers,
                    HeaderName::from_static("x-ratelimit-limit"),
                    value,
                )?,
                AlpacaHistoricalScriptedHeader::RateLimitRemaining(value) => {
                    insert_integer_header(
                        &mut headers,
                        HeaderName::from_static("x-ratelimit-remaining"),
                        value,
                    )?;
                }
                AlpacaHistoricalScriptedHeader::RateLimitReset(value) => insert_integer_header(
                    &mut headers,
                    HeaderName::from_static("x-ratelimit-reset"),
                    value,
                )?,
            }
        }
        Ok(Self {
            status,
            headers,
            body: body.to_vec().into_boxed_slice(),
            received_at,
        })
    }
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
impl std::fmt::Debug for AlpacaHistoricalScriptedResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaHistoricalScriptedResponse")
            .field("status", &self.status)
            .field("header_count", &self.headers.len())
            .field("body_bytes", &self.body.len())
            .field("received_at", &self.received_at)
            .finish()
    }
}

/// Read-only dispatch totals for one debug-only composite historical fixture owner.
#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlpacaHistoricalScriptedTransportCounters {
    bar_dispatches: u64,
    calendar_dispatches: u64,
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
impl AlpacaHistoricalScriptedTransportCounters {
    /// Returns committed exact historical-bar transport dispatches.
    pub const fn bar_dispatches(self) -> u64 {
        self.bar_dispatches
    }

    /// Returns committed exact calendar-range transport dispatches.
    pub const fn calendar_dispatches(self) -> u64 {
        self.calendar_dispatches
    }
}

/// Debug-only composite owner of one ordered bar response and one ordered calendar response.
#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
pub struct AlpacaHistoricalScriptedTransportFactory {
    transport: AlpacaHistoricalTransport,
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
impl AlpacaHistoricalScriptedTransportFactory {
    /// Creates one shared sealed owner whose required dispatch order is bars then calendar.
    pub fn try_new(
        bar: AlpacaHistoricalScriptedResponse,
        calendar: AlpacaHistoricalScriptedResponse,
    ) -> Result<Self, AlpacaError> {
        Ok(Self {
            transport: AlpacaHistoricalTransport::try_scripted(bar, calendar)?,
        })
    }

    /// Constructs the real preflight client with only its final HTTP execution scripted.
    pub fn preflight_client(
        &self,
        credentials: Arc<AlpacaCredentials>,
        bounds: HttpRequestBounds,
    ) -> Result<crate::AlpacaHistoricalEquityPreflightClient, AlpacaError> {
        crate::AlpacaHistoricalEquityPreflightClient::try_new_with_transport(
            credentials,
            bounds,
            self.transport.clone(),
        )
    }

    /// Constructs the real calendar executor with only its final HTTP execution scripted.
    pub fn calendar_executor(
        &self,
        credentials: Arc<AlpacaCredentials>,
        bounds: HttpRequestBounds,
    ) -> Result<crate::AlpacaAuthenticatedCalendarExecutor, AlpacaError> {
        crate::AlpacaAuthenticatedCalendarExecutor::try_new_with_transport(
            credentials,
            bounds,
            self.transport.clone(),
        )
    }

    /// Returns counters that reveal no credentials, headers, request targets, or response bodies.
    pub fn counters(&self) -> AlpacaHistoricalScriptedTransportCounters {
        let AlpacaHistoricalTransportInner::Scripted(state) = self.transport.inner.as_ref() else {
            return AlpacaHistoricalScriptedTransportCounters {
                bar_dispatches: 0,
                calendar_dispatches: 0,
            };
        };
        AlpacaHistoricalScriptedTransportCounters {
            bar_dispatches: state.bar_dispatches.load(Ordering::Acquire),
            calendar_dispatches: state.calendar_dispatches.load(Ordering::Acquire),
        }
    }
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
impl std::fmt::Debug for AlpacaHistoricalScriptedTransportFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaHistoricalScriptedTransportFactory")
            .field("counters", &self.counters())
            .finish_non_exhaustive()
    }
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
struct AlpacaHistoricalScriptedState {
    steps: Mutex<VecDeque<AlpacaHistoricalScriptedStep>>,
    bar_dispatches: AtomicU64,
    calendar_dispatches: AtomicU64,
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
enum AlpacaHistoricalScriptedStep {
    Bar(AlpacaHistoricalScriptedResponse),
    Calendar(AlpacaHistoricalScriptedResponse),
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
impl AlpacaHistoricalScriptedState {
    fn execute(
        &self,
        endpoint: AlpacaHistoricalEndpoint,
        url: &Url,
        maximum: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AuthenticatedGetResponse, AlpacaError> {
        validate_scripted_request(endpoint, url)?;
        if cancellation.is_cancelled() {
            return Err(AlpacaError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(AlpacaError::DeadlineExceeded);
        }
        let mut steps = self.steps.lock().map_err(|_| AlpacaError::Protocol)?;
        let expected_endpoint = match steps.front() {
            Some(AlpacaHistoricalScriptedStep::Bar(_)) => AlpacaHistoricalEndpoint::Bars,
            Some(AlpacaHistoricalScriptedStep::Calendar(_)) => AlpacaHistoricalEndpoint::Calendar,
            None => return Err(AlpacaError::Protocol),
        };
        if endpoint != expected_endpoint || cancellation.is_cancelled() {
            return if cancellation.is_cancelled() {
                Err(AlpacaError::Cancelled)
            } else {
                Err(AlpacaError::Protocol)
            };
        }
        if Instant::now() >= deadline {
            return Err(AlpacaError::DeadlineExceeded);
        }
        let step = steps.pop_front().ok_or(AlpacaError::Protocol)?;
        let response = match step {
            AlpacaHistoricalScriptedStep::Bar(response) => {
                increment_counter(&self.bar_dispatches)?;
                response
            }
            AlpacaHistoricalScriptedStep::Calendar(response) => {
                increment_counter(&self.calendar_dispatches)?;
                response
            }
        };
        drop(steps);
        if response.body.len() > maximum {
            return Err(AlpacaError::BodyTooLarge);
        }
        Ok(AuthenticatedGetResponse {
            status: response.status,
            body: response.body,
            headers: response.headers,
            received_at: response.received_at,
        })
    }
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
fn increment_counter(counter: &AtomicU64) -> Result<(), AlpacaError> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            value.checked_add(1)
        })
        .map(|_| ())
        .map_err(|_| AlpacaError::Protocol)
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
fn insert_raw_header(
    headers: &mut HeaderMap,
    name: HeaderName,
    value: Box<[u8]>,
    maximum: usize,
) -> Result<(), AlpacaError> {
    if value.is_empty() || value.len() > maximum || headers.contains_key(&name) {
        return Err(AlpacaError::Protocol);
    }
    let value = HeaderValue::from_bytes(&value).map_err(|_| AlpacaError::Protocol)?;
    headers.insert(name, value);
    Ok(())
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
fn insert_integer_header(
    headers: &mut HeaderMap,
    name: HeaderName,
    value: impl std::fmt::Display,
) -> Result<(), AlpacaError> {
    if headers.contains_key(&name) {
        return Err(AlpacaError::Protocol);
    }
    let value = HeaderValue::from_str(&value.to_string()).map_err(|_| AlpacaError::Protocol)?;
    headers.insert(name, value);
    Ok(())
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
fn validate_scripted_request(
    endpoint: AlpacaHistoricalEndpoint,
    url: &Url,
) -> Result<(), AlpacaError> {
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port_or_known_default() != Some(443)
        || url.fragment().is_some()
    {
        return Err(AlpacaError::Protocol);
    }
    match endpoint {
        AlpacaHistoricalEndpoint::Bars => validate_bar_request(url),
        AlpacaHistoricalEndpoint::Calendar => validate_calendar_request(url),
    }
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
fn validate_bar_request(url: &Url) -> Result<(), AlpacaError> {
    if url.host_str() != Some("data.alpaca.markets") {
        return Err(AlpacaError::Protocol);
    }
    let mut segments = url.path_segments().ok_or(AlpacaError::Protocol)?;
    let symbol = match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (Some("v2"), Some("stocks"), Some(symbol), Some("bars"), None) => symbol,
        _ => return Err(AlpacaError::Protocol),
    };
    if symbol.is_empty()
        || symbol.len() > 32
        || symbol == "*"
        || !symbol.bytes().all(|byte| {
            byte.is_ascii_uppercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
    {
        return Err(AlpacaError::Protocol);
    }
    let mut query = url.query_pairs();
    let timeframe = exact_query_value(&mut query, "timeframe")?;
    let start = exact_query_value(&mut query, "start")?;
    let end = exact_query_value(&mut query, "end")?;
    let limit = exact_query_value(&mut query, "limit")?;
    let adjustment = exact_query_value(&mut query, "adjustment")?;
    let feed = exact_query_value(&mut query, "feed")?;
    let sort = exact_query_value(&mut query, "sort")?;
    let page_token = query.next();
    if query.next().is_some()
        || !valid_timeframe(&timeframe)
        || !valid_utc_timestamp(&start)
        || !valid_utc_timestamp(&end)
        || limit
            .parse::<u16>()
            .ok()
            .is_none_or(|value| value == 0 || value > 10_000)
        || !matches!(
            adjustment.as_ref(),
            "raw" | "split" | "dividend" | "spin-off" | "all"
        )
        || feed != "iex"
        || sort != "asc"
        || page_token.as_ref().is_some_and(|(name, value)| {
            name != "page_token"
                || value.is_empty()
                || value.len() > 256
                || !value.is_ascii()
                || value
                    .bytes()
                    .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        })
    {
        return Err(AlpacaError::Protocol);
    }
    Ok(())
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
fn validate_calendar_request(url: &Url) -> Result<(), AlpacaError> {
    if !matches!(
        url.host_str(),
        Some("api.alpaca.markets" | "paper-api.alpaca.markets")
    ) || url.path() != "/v3/calendar/IEX"
    {
        return Err(AlpacaError::Protocol);
    }
    let mut query = url.query_pairs();
    let start = exact_query_value(&mut query, "start")?;
    let end = exact_query_value(&mut query, "end")?;
    let timezone = exact_query_value(&mut query, "timezone")?;
    if query.next().is_some()
        || parse_calendar_date(&start).is_none()
        || parse_calendar_date(&end).is_none()
        || start > end
        || timezone != "UTC"
    {
        return Err(AlpacaError::Protocol);
    }
    Ok(())
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
fn exact_query_value<'a>(
    query: &mut url::form_urlencoded::Parse<'a>,
    expected_name: &str,
) -> Result<std::borrow::Cow<'a, str>, AlpacaError> {
    match query.next() {
        Some((name, value)) if name == expected_name => Ok(value),
        _ => Err(AlpacaError::Protocol),
    }
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
fn valid_timeframe(value: &str) -> bool {
    value == "1Day"
        || value == "1Week"
        || value
            .strip_suffix("Min")
            .and_then(|multiple| multiple.parse::<u8>().ok())
            .is_some_and(|multiple| (1..=59).contains(&multiple))
        || value
            .strip_suffix("Hour")
            .and_then(|multiple| multiple.parse::<u8>().ok())
            .is_some_and(|multiple| (1..=23).contains(&multiple))
        || value
            .strip_suffix("Month")
            .and_then(|multiple| multiple.parse::<u8>().ok())
            .is_some_and(|multiple| matches!(multiple, 1 | 2 | 3 | 4 | 6 | 12))
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
fn valid_utc_timestamp(value: &str) -> bool {
    value.len() <= 64
        && (value.ends_with('Z') || value.ends_with("+00:00"))
        && DateTime::parse_from_rfc3339(value)
            .is_ok_and(|value| value.offset().local_minus_utc() == 0)
}

#[cfg(any(
    test,
    all(feature = "scripted-historical-transport-fixture", debug_assertions)
))]
fn parse_calendar_date(value: &str) -> Option<CalendarDate> {
    let mut parts = value.split('-');
    let year = parts.next()?.parse().ok()?;
    let month = parts.next()?.parse().ok()?;
    let day = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    CalendarDate::new(year, month, day).ok()
}
