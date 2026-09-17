use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt as _;
use futures_util::future::BoxFuture;
use market_squawk_domain::Timestamp;
use market_squawk_sources::{HttpRequestBounds, InFlightExtractionRequest};
use tokio_util::sync::CancellationToken;

use crate::CensusSourceError;
use crate::query::CensusAuthorizedUrl;

const USER_AGENT: &str = concat!(
    "market-squawk/",
    env!("CARGO_PKG_VERSION"),
    " census-data-api-adapter"
);
const MAX_RETAINED_HEADER_BYTES: usize = 128;

#[derive(Debug)]
pub(super) struct CensusHttpRequest<'a> {
    pub(super) authorized: CensusAuthorizedUrl<'a>,
}

#[derive(Clone, Debug)]
pub(super) struct CensusHttpResponse {
    pub(super) status: u16,
    pub(super) key_error: bool,
    pub(super) retry_after: Option<Vec<u8>>,
    pub(super) content_encoding: Option<Vec<u8>>,
    pub(super) content_type: Option<Vec<u8>>,
    pub(super) rate_headers: CensusRateLimitHeaders,
    pub(super) body: Bytes,
    pub(super) received_at: Timestamp,
    pub(super) latency: Duration,
}

/// Closed set of provider rate headers retained for doctor evidence.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct CensusRateLimitHeaders {
    pub(super) limit: Option<Vec<u8>>,
    pub(super) remaining: Option<Vec<u8>>,
    pub(super) reset: Option<Vec<u8>>,
}

pub(super) trait CensusTransport: std::fmt::Debug + Send + Sync {
    fn execute<'a>(
        &'a self,
        request: CensusHttpRequest<'a>,
        in_flight: &'a InFlightExtractionRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<CensusHttpResponse, CensusSourceError>>;
}

#[derive(Debug)]
pub(super) struct ReqwestCensusTransport {
    client: reqwest::Client,
}

impl ReqwestCensusTransport {
    pub(super) fn try_new(bounds: HttpRequestBounds) -> Result<Self, CensusSourceError> {
        let client = reqwest::Client::builder()
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
            .user_agent(USER_AGENT)
            .build()
            .map_err(|_| CensusSourceError::InvalidMetadata)?;
        Ok(Self { client })
    }
}

impl CensusTransport for ReqwestCensusTransport {
    fn execute<'a>(
        &'a self,
        request: CensusHttpRequest<'a>,
        in_flight: &'a InFlightExtractionRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<CensusHttpResponse, CensusSourceError>> {
        Box::pin(async move {
            let operation = async {
                in_flight
                    .validate_current()
                    .map_err(|_| CensusSourceError::Authority)?;
                let started = Instant::now();
                let mut request_builder =
                    self.client.get(request.authorized.transport_url().clone());
                if let Some(key) = request.authorized.key_query_value() {
                    request_builder = request_builder.query(&[("key", key)]);
                }
                let response = request_builder
                    .header(reqwest::header::ACCEPT, "application/json")
                    .header(reqwest::header::ACCEPT_ENCODING, "identity")
                    .send()
                    .await
                    .map_err(|_| CensusSourceError::Network)?;
                in_flight
                    .validate_current()
                    .map_err(|_| CensusSourceError::Authority)?;
                if response.content_length().is_some_and(|length| {
                    usize::try_from(length).map_or(true, |length| length > max_bytes)
                }) {
                    return Err(CensusSourceError::BodyTooLarge);
                }
                let status = response.status().as_u16();
                let key_error = response.headers().contains_key("x-datawebapi-keyerror");
                let retry_after = bounded_retry_after(response.headers());
                let content_encoding =
                    bounded_header(response.headers(), reqwest::header::CONTENT_ENCODING)?;
                let content_type =
                    bounded_header(response.headers(), reqwest::header::CONTENT_TYPE)?;
                let rate_headers = CensusRateLimitHeaders {
                    limit: supported_rate_header(
                        response.headers(),
                        &["x-ratelimit-limit", "x-rate-limit-limit"],
                    )?,
                    remaining: supported_rate_header(
                        response.headers(),
                        &["x-ratelimit-remaining", "x-rate-limit-remaining"],
                    )?,
                    reset: supported_rate_header(
                        response.headers(),
                        &["x-ratelimit-reset", "x-rate-limit-reset"],
                    )?,
                };
                let body = collect_bounded(response.bytes_stream(), in_flight, max_bytes).await?;
                in_flight
                    .validate_current()
                    .map_err(|_| CensusSourceError::Authority)?;
                let received_at = system_timestamp()?;
                let latency = started.elapsed();
                Ok(CensusHttpResponse {
                    status,
                    key_error,
                    retry_after,
                    content_encoding,
                    content_type,
                    rate_headers,
                    body,
                    received_at,
                    latency,
                })
            };
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(CensusSourceError::Cancelled),
                result = tokio::time::timeout(timeout, operation) => {
                    result.map_err(|_| CensusSourceError::DeadlineExceeded)?
                }
            }
        })
    }
}

fn bounded_header(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
) -> Result<Option<Vec<u8>>, CensusSourceError> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(CensusSourceError::Protocol);
    }
    let value = value.as_bytes();
    if value.len() > MAX_RETAINED_HEADER_BYTES {
        return Err(CensusSourceError::Protocol);
    }
    Ok(Some(value.to_vec()))
}

fn bounded_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Vec<u8>> {
    let mut values = headers.get_all(reqwest::header::RETRY_AFTER).iter();
    let value = values.next()?.as_bytes();
    if values.next().is_some() || value.len() > MAX_RETAINED_HEADER_BYTES || !value.is_ascii() {
        return None;
    }
    Some(value.to_vec())
}

fn supported_rate_header(
    headers: &reqwest::header::HeaderMap,
    names: &[&str],
) -> Result<Option<Vec<u8>>, CensusSourceError> {
    let mut retained: Option<Vec<u8>> = None;
    for name in names {
        for value in headers.get_all(*name).iter() {
            let value = value.as_bytes();
            if value.len() > MAX_RETAINED_HEADER_BYTES || !value.is_ascii() {
                return Err(CensusSourceError::Protocol);
            }
            if retained
                .as_deref()
                .is_some_and(|retained| retained != value)
            {
                return Err(CensusSourceError::Protocol);
            }
            if retained.is_none() {
                retained = Some(value.to_vec());
            }
        }
    }
    Ok(retained)
}

async fn collect_bounded<S, E>(
    mut stream: S,
    in_flight: &InFlightExtractionRequest,
    max_bytes: usize,
) -> Result<Bytes, CensusSourceError>
where
    S: futures_util::Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut body = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        in_flight
            .validate_current()
            .map_err(|_| CensusSourceError::Authority)?;
        let chunk = chunk.map_err(|_| CensusSourceError::Network)?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or(CensusSourceError::BodyTooLarge)?;
        if next > max_bytes {
            return Err(CensusSourceError::BodyTooLarge);
        }
        in_flight
            .validate_response_size(
                u64::try_from(next).map_err(|_| CensusSourceError::BodyTooLarge)?,
            )
            .map_err(|_| CensusSourceError::BodyTooLarge)?;
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

pub(super) fn system_timestamp() -> Result<Timestamp, CensusSourceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CensusSourceError::Clock)?
        .as_nanos();
    let nanos = i64::try_from(nanos).map_err(|_| CensusSourceError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
