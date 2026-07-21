use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::{Stream, StreamExt};
use market_squawk_domain::Timestamp;
use market_squawk_sources::{
    BudgetDecision, NetworkAccessPolicy, SharedProviderBudget, SourceError, SourceMetadata,
    apply_http_retry_after,
};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER,
    USER_AGENT,
};
use tokio_util::sync::CancellationToken;

use crate::TreasurySourceError;

pub(crate) const JSON_MEDIA_TYPE: &str = "application/json";
pub(crate) const XML_MEDIA_TYPE: &str = "application/atom+xml, application/xml, text/xml";
const USER_AGENT_VALUE: &str = "market-squawk/0.1 treasury-adapter";

#[derive(Debug)]
pub(crate) struct TreasuryHttpClient {
    client: reqwest::Client,
    max_response_bytes: usize,
    total_timeout: Duration,
}

impl TreasuryHttpClient {
    pub(crate) fn try_new(metadata: &SourceMetadata) -> Result<Self, TreasurySourceError> {
        let NetworkAccessPolicy::Allowlisted(policy) = metadata.network_policy() else {
            return Err(TreasurySourceError::InvalidMetadata);
        };
        let bounds = policy.request_bounds();
        let max_response_bytes = usize::try_from(bounds.max_response_bytes())
            .map_err(|_| TreasurySourceError::InvalidMetadata)?;
        let total_timeout = Duration::from_nanos(bounds.total_timeout_nanos());
        let client = reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_nanos(bounds.connect_timeout_nanos()))
            .read_timeout(Duration::from_nanos(bounds.read_timeout_nanos()))
            .timeout(total_timeout)
            .build()
            .map_err(|_| TreasurySourceError::InvalidMetadata)?;
        Ok(Self {
            client,
            max_response_bytes,
            total_timeout,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "request authority and bounds remain explicit"
    )]
    pub(crate) async fn fetch(
        &self,
        metadata: &SourceMetadata,
        budget: &SharedProviderBudget,
        url: &str,
        accept: &'static str,
        parser_max_bytes: usize,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedResponse, TreasurySourceError> {
        let now = system_timestamp()?;
        if !metadata.is_effective_at(now) {
            return Err(TreasurySourceError::Source(SourceError::Unauthorized));
        }
        metadata
            .network_policy()
            .authorize(url)
            .map_err(|_| TreasurySourceError::Source(SourceError::InvalidProtocolState))?;
        let timeout = remaining_timeout(deadline, now, self.total_timeout)?;
        let permit = match budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            refusal => {
                return Err(TreasurySourceError::Source(
                    SourceError::from_applied_budget_refusal(refusal),
                ));
            }
        };
        let max_response_bytes = parser_max_bytes.min(self.max_response_bytes);
        let operation = async {
            let response = self
                .client
                .get(url)
                .header(ACCEPT, accept)
                .header(ACCEPT_ENCODING, "identity")
                .header(USER_AGENT, USER_AGENT_VALUE)
                .send()
                .await
                .map_err(|_| TreasurySourceError::Source(SourceError::Network))?;
            let status = response.status();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            {
                let retry_after = response
                    .headers()
                    .get(RETRY_AFTER)
                    .map(|value| value.as_bytes());
                let decision = apply_http_retry_after(budget, retry_after, 0);
                return Err(TreasurySourceError::Source(
                    SourceError::from_applied_budget_refusal(decision),
                ));
            }
            if status == reqwest::StatusCode::UNAUTHORIZED
                || status == reqwest::StatusCode::FORBIDDEN
            {
                return Err(TreasurySourceError::Source(SourceError::Unauthorized));
            }
            if status != reqwest::StatusCode::OK {
                return Err(TreasurySourceError::Source(
                    SourceError::ProviderUnavailable,
                ));
            }
            if response
                .headers()
                .get(CONTENT_ENCODING)
                .is_some_and(|value| {
                    !value
                        .to_str()
                        .is_ok_and(|encoding| encoding.eq_ignore_ascii_case("identity"))
                })
            {
                return Err(TreasurySourceError::InvalidProtocol);
            }
            if !content_type_matches(response.headers().get(CONTENT_TYPE), accept) {
                return Err(TreasurySourceError::InvalidProtocol);
            }
            if let Some(content_length) = response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                && content_length > max_response_bytes as u64
            {
                return Err(TreasurySourceError::BodyTooLarge);
            }
            let bytes = collect_bounded_stream(response.bytes_stream(), max_response_bytes).await?;
            let received_at = system_timestamp()?;
            Ok(RetrievedResponse { bytes, received_at })
        };
        let result = tokio::select! {
            () = cancellation.cancelled() => Err(TreasurySourceError::Cancelled),
            result = tokio::time::timeout(timeout, operation) => {
                result.map_err(|_| TreasurySourceError::DeadlineExceeded)?
            }
        };
        drop(permit);
        result
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

fn content_type_matches(value: Option<&reqwest::header::HeaderValue>, accept: &str) -> bool {
    value
        .and_then(|value| value.to_str().ok())
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
