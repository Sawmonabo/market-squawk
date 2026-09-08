//! Hardened, bounded BEA HTTP transport.

use std::fmt;
use std::mem;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::StreamExt as _;
use futures_util::future::BoxFuture;
use market_squawk_domain::Timestamp;
use market_squawk_sources::{HttpRequestBounds, InFlightExtractionRequest};
use tokio_util::sync::CancellationToken;
use zeroize::{Zeroize, Zeroizing};

use crate::auth::BeaSensitiveBody;
use crate::{BEA_API_ENDPOINT, BeaAuthorizedRequest, BeaSourceError, BeaUserId};

const USER_AGENT: &str = concat!(
    "market-squawk/",
    env!("CARGO_PKG_VERSION"),
    " bea-data-api-adapter"
);
const MAX_RESPONSE_HEADER_VALUE_BYTES: usize = 1_024;

pub(crate) struct BeaHttpResponse {
    pub(crate) status: u16,
    pub(crate) retry_after: Option<BeaSensitiveHeader>,
    pub(crate) content_encoding: Option<BeaSensitiveHeader>,
    pub(crate) content_type: Option<BeaSensitiveHeader>,
    pub(crate) body: BeaSensitiveBody,
    pub(crate) received_at: Timestamp,
    pub(crate) latency: Duration,
}

impl BeaHttpResponse {
    /// Validates every captured header before releasing any ordinary retained bytes.
    pub(crate) fn retain_secret_free_headers(
        &mut self,
        user_id: &BeaUserId,
    ) -> Result<BeaRetainedResponseHeaders, BeaSourceError> {
        let invalid = [
            &mut self.retry_after,
            &mut self.content_encoding,
            &mut self.content_type,
        ]
        .into_iter()
        .filter_map(Option::as_mut)
        .any(|header| !header.validate_secret_free(user_id));
        if invalid {
            self.zeroize_headers();
            return Err(BeaSourceError::Protocol);
        }
        Ok(BeaRetainedResponseHeaders {
            retry_after: self.retry_after.take().map(BeaSensitiveHeader::into_vec),
            content_encoding: self
                .content_encoding
                .take()
                .map(BeaSensitiveHeader::into_vec),
            content_type: self.content_type.take().map(BeaSensitiveHeader::into_vec),
        })
    }

    fn zeroize_headers(&mut self) {
        for header in [
            &mut self.retry_after,
            &mut self.content_encoding,
            &mut self.content_type,
        ]
        .into_iter()
        .filter_map(Option::as_mut)
        {
            header.zeroize();
        }
    }
}

impl fmt::Debug for BeaHttpResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BeaHttpResponse")
            .field("status", &self.status)
            .field(
                "retry_after_bytes",
                &self.retry_after.as_ref().map(BeaSensitiveHeader::len),
            )
            .field(
                "content_encoding_bytes",
                &self.content_encoding.as_ref().map(BeaSensitiveHeader::len),
            )
            .field(
                "content_type_bytes",
                &self.content_type.as_ref().map(BeaSensitiveHeader::len),
            )
            .field("body_bytes", &self.body.len())
            .field("received_at", &self.received_at)
            .field("latency", &self.latency)
            .finish()
    }
}

/// A bounded response header that wipes its storage on every drop path.
pub(crate) struct BeaSensitiveHeader(Zeroizing<Vec<u8>>);

impl BeaSensitiveHeader {
    #[cfg(test)]
    pub(crate) fn try_from_vec(mut value: Vec<u8>) -> Result<Self, BeaSourceError> {
        if value.len() > MAX_RESPONSE_HEADER_VALUE_BYTES {
            value.zeroize();
            return Err(BeaSourceError::Protocol);
        }
        Ok(Self(Zeroizing::new(value)))
    }

    fn try_from_slice(value: &[u8]) -> Result<Self, BeaSourceError> {
        if value.len() > MAX_RESPONSE_HEADER_VALUE_BYTES {
            return Err(BeaSourceError::Protocol);
        }
        let mut retained = Zeroizing::new(Vec::new());
        retained
            .try_reserve_exact(value.len())
            .map_err(|_| BeaSourceError::Allocation)?;
        retained.extend_from_slice(value);
        Ok(Self(retained))
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn validate_secret_free(&mut self, user_id: &BeaUserId) -> bool {
        let secret = user_id.expose_secret().as_bytes();
        let endpoint = BEA_API_ENDPOINT.as_bytes();
        let invalid = self.0.windows(secret.len()).any(|value| value == secret)
            || self
                .0
                .windows(endpoint.len())
                .any(|value| value == endpoint)
            || self
                .0
                .windows(b"userid".len())
                .any(|value| value.eq_ignore_ascii_case(b"userid"));
        if invalid {
            self.zeroize();
        }
        !invalid
    }

    fn into_vec(mut self) -> Vec<u8> {
        mem::take(&mut *self.0)
    }

    #[cfg(test)]
    pub(crate) fn is_zeroized(&self) -> bool {
        self.0.iter().all(|byte| *byte == 0)
    }
}

impl Zeroize for BeaSensitiveHeader {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

impl fmt::Debug for BeaSensitiveHeader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BeaSensitiveHeader")
            .field("bytes", &self.len())
            .field("contents", &"[ZEROIZING REDACTED]")
            .finish()
    }
}

/// Header values released only after exact secret, endpoint, and `UserID` scans succeed.
pub(crate) struct BeaRetainedResponseHeaders {
    pub(crate) retry_after: Option<Vec<u8>>,
    pub(crate) content_encoding: Option<Vec<u8>>,
    pub(crate) content_type: Option<Vec<u8>>,
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
) -> Result<Option<BeaSensitiveHeader>, BeaSourceError> {
    value
        .map(|value| BeaSensitiveHeader::try_from_slice(value.as_bytes()))
        .transpose()
}

async fn collect_bounded<S, E>(
    mut stream: S,
    in_flight: &InFlightExtractionRequest,
    max_bytes: usize,
) -> Result<BeaSensitiveBody, BeaSourceError>
where
    S: futures_util::Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut body = Zeroizing::new(Vec::new());
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
        body.try_reserve(chunk.len())
            .map_err(|_| BeaSourceError::Allocation)?;
        body.extend_from_slice(&chunk);
    }
    if body.is_empty() {
        return Err(BeaSourceError::Protocol);
    }
    Ok(BeaSensitiveBody::from_zeroizing(body))
}

pub(crate) fn system_timestamp() -> Result<Timestamp, BeaSourceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BeaSourceError::Clock)?
        .as_nanos();
    let nanos = i64::try_from(nanos).map_err(|_| BeaSourceError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
