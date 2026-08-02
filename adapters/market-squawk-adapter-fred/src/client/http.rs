use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt};
use market_squawk_domain::Timestamp;
use tokio_util::sync::CancellationToken;

use super::{FredApiKey, FredSourceError};

#[derive(Clone, Debug)]
pub(super) struct FredHttpResponse {
    pub(super) status: u16,
    pub(super) retry_after: Option<Vec<u8>>,
    pub(super) content_encoding: Option<Vec<u8>>,
    pub(super) body: Bytes,
    pub(super) received_at: Timestamp,
}

#[derive(Clone)]
pub(super) struct FredHttpRequest {
    pub(super) public_url: url::Url,
    pub(super) api_key: FredApiKey,
}

impl std::fmt::Debug for FredHttpRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FredHttpRequest")
            .field("public_url", &self.public_url)
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

pub(super) trait FredTransport: std::fmt::Debug + Send + Sync {
    fn execute(
        &self,
        request: FredHttpRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<FredHttpResponse, FredSourceError>>;
}

#[derive(Debug)]
pub(super) struct ReqwestFredTransport {
    client: reqwest::Client,
}

impl ReqwestFredTransport {
    pub(super) fn try_new(
        bounds: market_squawk_sources::HttpRequestBounds,
    ) -> Result<Self, FredSourceError> {
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
            .user_agent("market-squawk/0.1 fred-adapter")
            .build()
            .map_err(|_| FredSourceError::InvalidConfiguration)?;
        Ok(Self { client })
    }
}

impl FredTransport for ReqwestFredTransport {
    fn execute(
        &self,
        request: FredHttpRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<FredHttpResponse, FredSourceError>> {
        Box::pin(async move {
            let operation = async {
                let response = self
                    .client
                    .get(request.public_url)
                    .query(&[("api_key", request.api_key.expose())])
                    .send()
                    .await
                    .map_err(|_| FredSourceError::Network)?;
                if response
                    .content_length()
                    .is_some_and(|length| usize::try_from(length).map_or(true, |n| n > max_bytes))
                {
                    return Err(FredSourceError::BodyTooLarge);
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
                let body = collect_bounded_stream(response.bytes_stream(), max_bytes).await?;
                Ok(FredHttpResponse {
                    status,
                    retry_after,
                    content_encoding,
                    body,
                    received_at: system_timestamp()?,
                })
            };
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(FredSourceError::Cancelled),
                result = tokio::time::timeout(timeout, operation) => {
                    result.map_err(|_| FredSourceError::DeadlineExceeded)?
                }
            }
        })
    }
}

pub(super) async fn collect_bounded_stream<S, E>(
    mut stream: S,
    max_bytes: usize,
) -> Result<Bytes, FredSourceError>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut body = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| FredSourceError::Network)?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or(FredSourceError::BodyTooLarge)?;
        if next > max_bytes {
            return Err(FredSourceError::BodyTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

pub(super) fn system_timestamp() -> Result<Timestamp, FredSourceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| FredSourceError::Network)?
        .as_nanos();
    let nanos = i64::try_from(nanos).map_err(|_| FredSourceError::Network)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
