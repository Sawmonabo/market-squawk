//! Hardened, bounded BEA HTTP transport.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt as _;
use futures_util::future::BoxFuture;
use market_squawk_domain::Timestamp;
use market_squawk_sources::{HttpRequestBounds, InFlightExtractionRequest};
use tokio_util::sync::CancellationToken;

use crate::{BeaAuthorizedRequest, BeaSourceError};

const USER_AGENT: &str = concat!(
    "market-squawk/",
    env!("CARGO_PKG_VERSION"),
    " bea-data-api-adapter"
);
const MAX_RESPONSE_HEADER_VALUE_BYTES: usize = 1_024;

#[derive(Clone, Debug)]
pub(crate) struct BeaHttpResponse {
    pub(crate) status: u16,
    pub(crate) retry_after: Option<Vec<u8>>,
    pub(crate) content_encoding: Option<Vec<u8>>,
    pub(crate) content_type: Option<Vec<u8>>,
    pub(crate) body: Bytes,
    pub(crate) received_at: Timestamp,
    pub(crate) latency: Duration,
}

pub(crate) trait BeaTransport: std::fmt::Debug + Send + Sync {
    fn execute<'a>(
        &'a self,
        request: BeaAuthorizedRequest,
        in_flight: &'a InFlightExtractionRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<BeaHttpResponse, BeaSourceError>>;
}

#[derive(Debug)]
pub(crate) struct ReqwestBeaTransport {
    client: reqwest::Client,
}

impl ReqwestBeaTransport {
    pub(crate) fn try_new(bounds: HttpRequestBounds) -> Result<Self, BeaSourceError> {
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
            .map_err(|_| BeaSourceError::InvalidMetadata)?;
        Ok(Self { client })
    }
}

impl BeaTransport for ReqwestBeaTransport {
    fn execute<'a>(
        &'a self,
        request: BeaAuthorizedRequest,
        in_flight: &'a InFlightExtractionRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<BeaHttpResponse, BeaSourceError>> {
        Box::pin(async move {
            let operation = async {
                in_flight
                    .validate_current()
                    .map_err(|_| BeaSourceError::Authority)?;
                let started = Instant::now();
                let response = self
                    .client
                    .get(request.expose_url())
                    .header(reqwest::header::ACCEPT, "application/json")
                    .header(reqwest::header::ACCEPT_ENCODING, "identity")
                    .send()
                    .await
                    .map_err(|_| BeaSourceError::Network)?;
                if response.content_length().is_some_and(|length| {
                    usize::try_from(length).map_or(true, |length| length > max_bytes)
                }) {
                    return Err(BeaSourceError::BodyTooLarge);
                }
                let status = response.status().as_u16();
                let retry_after =
                    bounded_header(response.headers().get(reqwest::header::RETRY_AFTER))?;
                let content_encoding =
                    bounded_header(response.headers().get(reqwest::header::CONTENT_ENCODING))?;
                let content_type =
                    bounded_header(response.headers().get(reqwest::header::CONTENT_TYPE))?;
                let body = collect_bounded(response.bytes_stream(), in_flight, max_bytes).await?;
                Ok(BeaHttpResponse {
                    status,
                    retry_after,
                    content_encoding,
                    content_type,
                    body,
                    received_at: system_timestamp()?,
                    latency: started.elapsed(),
                })
            };
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(BeaSourceError::Cancelled),
                result = tokio::time::timeout(timeout, operation) => {
                    result.map_err(|_| BeaSourceError::DeadlineExceeded)?
                }
            }
        })
    }
}

fn bounded_header(
    value: Option<&reqwest::header::HeaderValue>,
) -> Result<Option<Vec<u8>>, BeaSourceError> {
    value
        .map(|value| {
            let bytes = value.as_bytes();
            if bytes.len() > MAX_RESPONSE_HEADER_VALUE_BYTES {
                return Err(BeaSourceError::Protocol);
            }
            Ok(bytes.to_vec())
        })
        .transpose()
}

async fn collect_bounded<S, E>(
    mut stream: S,
    in_flight: &InFlightExtractionRequest,
    max_bytes: usize,
) -> Result<Bytes, BeaSourceError>
where
    S: futures_util::Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut body = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        in_flight
            .validate_current()
            .map_err(|_| BeaSourceError::Authority)?;
        let chunk = chunk.map_err(|_| BeaSourceError::Network)?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or(BeaSourceError::BodyTooLarge)?;
        if next > max_bytes {
            return Err(BeaSourceError::BodyTooLarge);
        }
        in_flight
            .validate_response_size(u64::try_from(next).map_err(|_| BeaSourceError::BodyTooLarge)?)
            .map_err(|_| BeaSourceError::BodyTooLarge)?;
        body.extend_from_slice(&chunk);
    }
    if body.is_empty() {
        return Err(BeaSourceError::Protocol);
    }
    Ok(body.freeze())
}

pub(crate) fn system_timestamp() -> Result<Timestamp, BeaSourceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BeaSourceError::Clock)?
        .as_nanos();
    let nanos = i64::try_from(nanos).map_err(|_| BeaSourceError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
