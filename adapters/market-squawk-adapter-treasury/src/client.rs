use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt};
use market_squawk_domain::Timestamp;
use market_squawk_sources::{
    ExtractionAuthority, ExtractionSourceError, NetworkAccessPolicy, SourceError, SourceMetadata,
};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE, RETRY_AFTER, USER_AGENT,
};
use tokio_util::sync::CancellationToken;

use crate::TreasurySourceError;

pub(crate) const JSON_MEDIA_TYPE: &str = "application/json";
pub(crate) const XML_MEDIA_TYPE: &str = "application/atom+xml, application/xml, text/xml";
const USER_AGENT_VALUE: &str = "market-squawk/0.1 treasury-adapter";

#[derive(Debug)]
pub(crate) struct TreasuryHttpClient {
    transport: Arc<dyn TreasuryTransport>,
    max_response_bytes: usize,
    total_timeout: Duration,
}

impl TreasuryHttpClient {
    pub(crate) fn try_new(metadata: &SourceMetadata) -> Result<Self, TreasurySourceError> {
        let NetworkAccessPolicy::Allowlisted(policy) = metadata.network_policy() else {
            return Err(TreasurySourceError::InvalidMetadata);
        };
        let bounds = policy.request_bounds();
        let max_response_bytes = usize::try_from(
            bounds
                .max_response_bytes()
                .min(market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGE_BYTES),
        )
        .map_err(|_| TreasurySourceError::InvalidMetadata)?;
        let total_timeout = Duration::from_nanos(bounds.total_timeout_nanos());
        let transport = Arc::new(ReqwestTreasuryTransport::try_new(bounds)?);
        Ok(Self {
            transport,
            max_response_bytes,
            total_timeout,
        })
    }

    #[cfg(test)]
    pub(crate) fn try_new_with_transport(
        metadata: &SourceMetadata,
        transport: Arc<dyn TreasuryTransport>,
    ) -> Result<Self, TreasurySourceError> {
        let NetworkAccessPolicy::Allowlisted(policy) = metadata.network_policy() else {
            return Err(TreasurySourceError::InvalidMetadata);
        };
        let bounds = policy.request_bounds();
        Ok(Self {
            transport,
            max_response_bytes: usize::try_from(
                bounds
                    .max_response_bytes()
                    .min(market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGE_BYTES),
            )
            .map_err(|_| TreasurySourceError::InvalidMetadata)?,
            total_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "request authority and bounds remain explicit"
    )]
    pub(crate) async fn fetch(
        &self,
        metadata: &SourceMetadata,
        authority: &ExtractionAuthority,
        url: &str,
        accept: &'static str,
        parser_max_bytes: usize,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedResponse, ExtractionSourceError> {
        let now = system_timestamp().map_err(map_adapter_error)?;
        authority.validate_current()?;
        if authority.metadata() != metadata || !metadata.is_effective_at(now) {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        let timeout =
            remaining_timeout(deadline, now, self.total_timeout).map_err(map_adapter_error)?;
        let permit = authority.try_network_request(url)?;
        let in_flight = permit.authorize_send(url)?;
        let max_response_bytes = parser_max_bytes.min(self.max_response_bytes);
        let response = self
            .transport
            .execute(
                TreasuryHttpRequest {
                    url: url.to_owned(),
                    accept,
                },
                max_response_bytes,
                timeout,
                cancellation.clone(),
            )
            .await
            .map_err(map_adapter_error)?;
        if response.status == 429 || response.status == 503 {
            let deadline =
                in_flight.apply_retry_after_header(response.retry_after.as_deref(), 0)?;
            return Err(ExtractionSourceError::Source(
                SourceError::BudgetWaitUntil { deadline },
            ));
        }
        if response.status == 401 || response.status == 403 {
            return Err(ExtractionSourceError::Source(SourceError::Unauthorized));
        }
        if response.status != 200 {
            return Err(ExtractionSourceError::Source(
                SourceError::ProviderUnavailable,
            ));
        }
        if response
            .content_encoding
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
            || !content_type_matches(response.content_type.as_deref(), accept)
        {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        in_flight.validate_response_size(
            u64::try_from(response.body.len())
                .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?,
        )?;
        Ok(RetrievedResponse {
            bytes: response.body,
            received_at: response.received_at,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TreasuryHttpRequest {
    pub(crate) url: String,
    pub(crate) accept: &'static str,
}

#[derive(Clone, Debug)]
pub(crate) struct TreasuryHttpResponse {
    pub(crate) status: u16,
    pub(crate) retry_after: Option<Vec<u8>>,
    pub(crate) content_encoding: Option<Vec<u8>>,
    pub(crate) content_type: Option<Vec<u8>>,
    pub(crate) body: Bytes,
    pub(crate) received_at: Timestamp,
}

pub(crate) trait TreasuryTransport: std::fmt::Debug + Send + Sync {
    fn execute(
        &self,
        request: TreasuryHttpRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<TreasuryHttpResponse, TreasurySourceError>>;
}

#[derive(Debug)]
struct ReqwestTreasuryTransport {
    client: reqwest::Client,
}

impl ReqwestTreasuryTransport {
    fn try_new(
        bounds: market_squawk_sources::HttpRequestBounds,
    ) -> Result<Self, TreasurySourceError> {
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
            .map_err(|_| TreasurySourceError::InvalidMetadata)?;
        Ok(Self { client })
    }
}

impl TreasuryTransport for ReqwestTreasuryTransport {
    fn execute(
        &self,
        request: TreasuryHttpRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<TreasuryHttpResponse, TreasurySourceError>> {
        Box::pin(async move {
            let operation = async {
                let response = self
                    .client
                    .get(request.url)
                    .header(ACCEPT, request.accept)
                    .header(ACCEPT_ENCODING, "identity")
                    .header(USER_AGENT, USER_AGENT_VALUE)
                    .send()
                    .await
                    .map_err(|_| TreasurySourceError::Source(SourceError::Network))?;
                if response.content_length().is_some_and(|length| {
                    usize::try_from(length).map_or(true, |length| length > max_bytes)
                }) {
                    return Err(TreasurySourceError::BodyTooLarge);
                }
                let status = response.status().as_u16();
                let retry_after = response
                    .headers()
                    .get(RETRY_AFTER)
                    .map(|value| value.as_bytes().to_vec());
                let content_encoding = response
                    .headers()
                    .get(CONTENT_ENCODING)
                    .map(|value| value.as_bytes().to_vec());
                let content_type = response
                    .headers()
                    .get(CONTENT_TYPE)
                    .map(|value| value.as_bytes().to_vec());
                let body = collect_bounded_stream(response.bytes_stream(), max_bytes).await?;
                Ok(TreasuryHttpResponse {
                    status,
                    retry_after,
                    content_encoding,
                    content_type,
                    body,
                    received_at: system_timestamp()?,
                })
            };
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(TreasurySourceError::Cancelled),
                result = tokio::time::timeout(timeout, operation) => {
                    result.map_err(|_| TreasurySourceError::DeadlineExceeded)?
                }
            }
        })
    }
}

fn map_adapter_error(error: TreasurySourceError) -> ExtractionSourceError {
    match error {
        TreasurySourceError::Cancelled => ExtractionSourceError::Cancelled,
        TreasurySourceError::DeadlineExceeded => ExtractionSourceError::DeadlineExceeded,
        TreasurySourceError::Source(error) => ExtractionSourceError::Source(error),
        TreasurySourceError::InvalidMetadata
        | TreasurySourceError::InvalidOwnerUseAttestation
        | TreasurySourceError::InvalidBackfillCheckpoint
        | TreasurySourceError::BackfillIncomplete
        | TreasurySourceError::QueryBindingMismatch
        | TreasurySourceError::InvalidProtocol
        | TreasurySourceError::Protocol(_)
        | TreasurySourceError::Rate(_)
        | TreasurySourceError::HealthUnavailable
        | TreasurySourceError::RevisionAuthority(_) => {
            ExtractionSourceError::Source(SourceError::InvalidProtocolState)
        }
        TreasurySourceError::BodyTooLarge => ExtractionSourceError::Source(SourceError::Network),
    }
}

pub(crate) struct RetrievedResponse {
    pub(crate) bytes: Bytes,
    pub(crate) received_at: Timestamp,
}

async fn collect_bounded_stream<S, E>(
    mut stream: S,
    max_bytes: usize,
) -> Result<Bytes, TreasurySourceError>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut body = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| TreasurySourceError::Source(SourceError::Network))?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or(TreasurySourceError::BodyTooLarge)?;
        if next > max_bytes {
            return Err(TreasurySourceError::BodyTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

fn content_type_matches(value: Option<&[u8]>, accept: &str) -> bool {
    value
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| {
            accept
                .split(',')
                .map(str::trim)
                .any(|allowed| media_type.eq_ignore_ascii_case(allowed))
        })
}

fn remaining_timeout(
    deadline: Timestamp,
    now: Timestamp,
    configured: Duration,
) -> Result<Duration, TreasurySourceError> {
    let remaining = deadline
        .unix_nanos()
        .checked_sub(now.unix_nanos())
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or(TreasurySourceError::DeadlineExceeded)?;
    Ok(configured.min(Duration::from_nanos(remaining)))
}

pub(crate) fn system_timestamp() -> Result<Timestamp, TreasurySourceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| TreasurySourceError::Source(SourceError::TrustedTimeUnavailable))?
        .as_nanos();
    let nanos = i64::try_from(nanos)
        .map_err(|_| TreasurySourceError::Source(SourceError::TrustedTimeUnavailable))?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::stream;

    use super::{TreasurySourceError, collect_bounded_stream};

    #[tokio::test]
    async fn streamed_body_limit_is_enforced_across_chunks() {
        let chunks = stream::iter([
            Ok::<_, std::io::Error>(Bytes::from_static(b"abcd")),
            Ok(Bytes::from_static(b"efgh")),
        ]);

        assert!(matches!(
            collect_bounded_stream(chunks, 7).await,
            Err(TreasurySourceError::BodyTooLarge)
        ));
    }
}
