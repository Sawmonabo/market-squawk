//! Hardened bounded HTTP transport for Coinbase Direct bootstrap evidence.

use std::time::Duration;

use bytes::Bytes;
use futures_util::{StreamExt as _, future::BoxFuture};
use market_squawk_sources::{HttpRequestBounds, MAX_RAW_FRAME_BYTES, TlsProviderCapability};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER,
    USER_AGENT,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const USER_AGENT_VALUE: &str = "market-squawk-coinbase-direct/0.1";
const MAX_RESPONSE_HEADER_BYTES: usize = 256;
const MAX_RETRY_AFTER_BYTES: usize = 128;

/// One fully bounded GET operation.
#[derive(Debug)]
pub(super) struct CoinbaseDirectHttpRequest {
    url: Box<str>,
    max_body_bytes: u64,
    max_segments: usize,
    timeout: Duration,
    cancellation: CancellationToken,
}

impl CoinbaseDirectHttpRequest {
    pub(super) fn new(
        url: &str,
        max_body_bytes: u64,
        max_segments: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            url: url.to_owned().into_boxed_str(),
            max_body_bytes,
            max_segments,
            timeout,
            cancellation,
        }
    }

    #[cfg(test)]
    pub(super) fn url(&self) -> &str {
        &self.url
    }

    #[cfg(test)]
    pub(super) const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

/// Complete bounded response bytes retained in capture-ready segments.
#[derive(Clone, Debug)]
pub(super) struct CoinbaseDirectHttpResponse {
    pub(super) status: u16,
    pub(super) final_url: Box<str>,
    pub(super) declared_body_length: Option<u64>,
    pub(super) retry_after: Option<Box<[u8]>>,
    pub(super) content_type: Option<Box<[u8]>>,
    pub(super) content_encoding: Option<Box<[u8]>>,
    pub(super) segments: Vec<Bytes>,
}

/// Injectable HTTP boundary used by production reqwest and deterministic in-memory tests.
pub(super) trait CoinbaseDirectHttpTransport: std::fmt::Debug + Send + Sync {
    fn get(
        &self,
        request: CoinbaseDirectHttpRequest,
    ) -> BoxFuture<'_, Result<CoinbaseDirectHttpResponse, CoinbaseDirectHttpTransportError>>;
}

/// Bounded transport-level failure before generation-bound capture construction.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum CoinbaseDirectHttpTransportError {
    #[error("Coinbase Direct HTTP transport failed")]
    Network,
    #[error("Coinbase Direct HTTP transport deadline elapsed")]
    Deadline,
    #[error("Coinbase Direct HTTP transport was cancelled")]
    Cancelled,
    #[error("Coinbase Direct HTTP response exceeded its byte ceiling")]
    BodyTooLarge,
    #[error("Coinbase Direct HTTP response exceeded its segment ceiling")]
    SegmentLimit,
    #[error("Coinbase Direct HTTP response was not protocol-safe")]
    Protocol,
    #[error("Coinbase Direct HTTP response allocation failed")]
    Allocation,
}

impl From<reqwest::Error> for CoinbaseDirectHttpTransportError {
    fn from(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Deadline
        } else {
            Self::Network
        }
    }
}

/// Production hardened reqwest transport.
#[derive(Debug)]
pub(super) struct ReqwestCoinbaseDirectHttpTransport {
    client: reqwest::Client,
}

impl ReqwestCoinbaseDirectHttpTransport {
    pub(super) fn try_new(
        bounds: HttpRequestBounds,
        tls_provider: TlsProviderCapability,
    ) -> Result<Self, CoinbaseDirectHttpTransportError> {
        let _consumed_provider_identity = tls_provider.provider_id();
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
            .connect_timeout(Duration::from_nanos(bounds.connect_timeout_nanos()))
            .read_timeout(Duration::from_nanos(bounds.read_timeout_nanos()))
            .timeout(Duration::from_nanos(bounds.total_timeout_nanos()))
            .build()
            .map_err(|_error| CoinbaseDirectHttpTransportError::Protocol)?;
        Ok(Self { client })
    }
}

impl CoinbaseDirectHttpTransport for ReqwestCoinbaseDirectHttpTransport {
    fn get(
        &self,
        request: CoinbaseDirectHttpRequest,
    ) -> BoxFuture<'_, Result<CoinbaseDirectHttpResponse, CoinbaseDirectHttpTransportError>> {
        Box::pin(async move {
            let operation = async {
                let response = self
                    .client
                    .get(request.url.as_ref())
                    .header(ACCEPT, "application/json")
                    .header(ACCEPT_ENCODING, "identity")
                    .header(USER_AGENT, USER_AGENT_VALUE)
                    .send()
                    .await
                    .map_err(CoinbaseDirectHttpTransportError::from)?;
                let status = response.status().as_u16();
                let final_url = response.url().as_str().to_owned().into_boxed_str();
                let declared_body_length = declared_body_length(response.headers())?;
                if declared_body_length.is_some_and(|length| length > request.max_body_bytes) {
                    return Err(CoinbaseDirectHttpTransportError::BodyTooLarge);
                }
                let retry_after =
                    bounded_header(response.headers(), RETRY_AFTER, MAX_RETRY_AFTER_BYTES)?;
                let content_type =
                    bounded_header(response.headers(), CONTENT_TYPE, MAX_RESPONSE_HEADER_BYTES)?;
                let content_encoding = bounded_header(
                    response.headers(),
                    CONTENT_ENCODING,
                    MAX_RESPONSE_HEADER_BYTES,
                )?;
                let segments = collect_bounded_segments(
                    response.bytes_stream(),
                    request.max_body_bytes,
                    request.max_segments,
                )
                .await?;
                Ok(CoinbaseDirectHttpResponse {
                    status,
                    final_url,
                    declared_body_length,
                    retry_after,
                    content_type,
                    content_encoding,
                    segments,
                })
            };
            tokio::select! {
                biased;
                () = request.cancellation.cancelled() => {
                    Err(CoinbaseDirectHttpTransportError::Cancelled)
                }
                result = tokio::time::timeout(request.timeout, operation) => {
                    result.map_err(|_elapsed| CoinbaseDirectHttpTransportError::Deadline)?
                }
            }
        })
    }
}

fn declared_body_length(
    headers: &reqwest::header::HeaderMap,
) -> Result<Option<u64>, CoinbaseDirectHttpTransportError> {
    let mut values = headers.get_all(CONTENT_LENGTH).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(CoinbaseDirectHttpTransportError::Protocol);
    }
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > 20 || !bytes.iter().all(u8::is_ascii_digit) {
        return Err(CoinbaseDirectHttpTransportError::Protocol);
    }
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Some)
        .ok_or(CoinbaseDirectHttpTransportError::Protocol)
}

fn bounded_header(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
    maximum: usize,
) -> Result<Option<Box<[u8]>>, CoinbaseDirectHttpTransportError> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(CoinbaseDirectHttpTransportError::Protocol);
    }
    let bytes = value.as_bytes();
    if bytes.len() > maximum {
        Err(CoinbaseDirectHttpTransportError::Protocol)
    } else {
        Ok(Some(Box::from(bytes)))
    }
}

async fn collect_bounded_segments<S, E>(
    mut stream: S,
    max_body_bytes: u64,
    max_segments: usize,
) -> Result<Vec<Bytes>, CoinbaseDirectHttpTransportError>
where
    S: futures_util::Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<CoinbaseDirectHttpTransportError>,
{
    if max_body_bytes == 0 || max_segments == 0 {
        return Err(CoinbaseDirectHttpTransportError::Protocol);
    }
    let mut segments = Vec::new();
    segments
        .try_reserve_exact(max_segments)
        .map_err(|_error| CoinbaseDirectHttpTransportError::Allocation)?;
    let mut current = Vec::new();
    let mut body_length = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(Into::into)?;
        let chunk_length = u64::try_from(chunk.len())
            .map_err(|_error| CoinbaseDirectHttpTransportError::BodyTooLarge)?;
        body_length = body_length
            .checked_add(chunk_length)
            .filter(|length| *length <= max_body_bytes)
            .ok_or(CoinbaseDirectHttpTransportError::BodyTooLarge)?;
        let mut remaining = chunk.as_ref();
        while !remaining.is_empty() {
            let room = MAX_RAW_FRAME_BYTES
                .checked_sub(current.len())
                .ok_or(CoinbaseDirectHttpTransportError::Protocol)?;
            let take = room.min(remaining.len());
            current
                .try_reserve(take)
                .map_err(|_error| CoinbaseDirectHttpTransportError::Allocation)?;
            current.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];
            if current.len() == MAX_RAW_FRAME_BYTES {
                push_segment(&mut segments, &mut current, max_segments)?;
            }
        }
    }
    if !current.is_empty() {
        push_segment(&mut segments, &mut current, max_segments)?;
    }
    Ok(segments)
}

fn push_segment(
    segments: &mut Vec<Bytes>,
    current: &mut Vec<u8>,
    max_segments: usize,
) -> Result<(), CoinbaseDirectHttpTransportError> {
    if segments.len() == max_segments {
        return Err(CoinbaseDirectHttpTransportError::SegmentLimit);
    }
    segments.push(Bytes::from(std::mem::take(current)));
    Ok(())
}
