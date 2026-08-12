use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt as _;
use futures_util::future::BoxFuture;
use market_squawk_domain::Timestamp;
use market_squawk_sources::{HttpRequestBounds, InFlightExtractionRequest};
use tokio_util::sync::CancellationToken;

use crate::{CensusAuthorizedUrl, CensusSourceError};

const USER_AGENT: &str = concat!(
    "market-squawk/",
    env!("CARGO_PKG_VERSION"),
    " census-data-api-adapter"
);

#[derive(Clone, Debug)]
pub(super) struct CensusHttpRequest {
    pub(super) authorized: CensusAuthorizedUrl,
}

#[derive(Clone, Debug)]
pub(super) struct CensusHttpResponse {
    pub(super) status: u16,
    pub(super) retry_after: Option<Vec<u8>>,
    pub(super) content_encoding: Option<Vec<u8>>,
    pub(super) content_type: Option<Vec<u8>>,
    pub(super) body: Bytes,
    pub(super) received_at: Timestamp,
    pub(super) latency: Duration,
}

pub(super) trait CensusTransport: std::fmt::Debug + Send + Sync {
    fn execute<'a>(
        &'a self,
        request: CensusHttpRequest,
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
        request: CensusHttpRequest,
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
                let response = self
                    .client
                    .get(request.authorized.transport_url().clone())
                    .query(&[("key", request.authorized.key_query_value())])
                    .header(reqwest::header::ACCEPT, "application/json")
                    .header(reqwest::header::ACCEPT_ENCODING, "identity")
                    .send()
                    .await
                    .map_err(|_| CensusSourceError::Network)?;
                if response.content_length().is_some_and(|length| {
                    usize::try_from(length).map_or(true, |length| length > max_bytes)
                }) {
                    return Err(CensusSourceError::BodyTooLarge);
                }
                let status = response.status().as_u16();
                let retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .map(|value| value.as_bytes().to_vec());
                let content_encoding = response
                    .headers()
                    .get(reqwest::header::CONTENT_ENCODING)
                    .map(|value| value.as_bytes().to_vec());
                let content_type = response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .map(|value| value.as_bytes().to_vec());
                let body = collect_bounded(response.bytes_stream(), in_flight, max_bytes).await?;
                let received_at = system_timestamp()?;
                let latency = started.elapsed();
                Ok(CensusHttpResponse {
                    status,
                    retry_after,
                    content_encoding,
                    content_type,
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
