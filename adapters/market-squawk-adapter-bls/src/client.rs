use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::{Stream, StreamExt};
use market_squawk_domain::Timestamp;
use market_squawk_sources::{
    BudgetDecision, ExtractionSourceError, NetworkAccessPolicy, SharedProviderBudget, SourceError,
    SourceMetadata, apply_http_retry_after,
};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::observations::MAX_RESPONSE_BYTES;
use crate::{BlsAccessTier, BlsRequestChunk, BlsResponse};

const MAX_REGISTRATION_KEY_BYTES: usize = 256;
const BLS_V1_ENDPOINT: &str = "https://api.bls.gov/publicAPI/v1/timeseries/data/";
const BLS_V2_ENDPOINT: &str = "https://api.bls.gov/publicAPI/v2/timeseries/data/";

/// User-owned BLS v2 registration credential retained only in zeroizing memory.
#[derive(Clone)]
pub struct BlsRegistrationKey(Zeroizing<String>);

impl BlsRegistrationKey {
    /// Constructs an opaque bounded provider key without assuming an undocumented fixed format.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, whitespace-containing, or non-ASCII credentials.
    pub fn try_new(value: String) -> Result<Self, BlsSourceError> {
        if value.is_empty()
            || value.len() > MAX_REGISTRATION_KEY_BYTES
            || !value.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(BlsSourceError::InvalidRegistrationKey);
        }
        Ok(Self(Zeroizing::new(value)))
    }

    pub(crate) fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for BlsRegistrationKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BlsRegistrationKey([REDACTED])")
    }
}

/// Authorization mode for the public v1 or user-registered v2 BLS interface.
#[derive(Clone, Debug)]
pub enum BlsAuthorization {
    /// Public unregistered v1 access under provider-published limits.
    PublicV1,
    /// Registered v2 access using a user-supplied key.
    RegisteredV2(BlsRegistrationKey),
}

impl BlsAuthorization {
    /// Returns the exact provider tier selected by this authorization mode.
    pub const fn tier(&self) -> BlsAccessTier {
        match self {
            Self::PublicV1 => BlsAccessTier::PublicV1,
            Self::RegisteredV2(_) => BlsAccessTier::RegisteredV2,
        }
    }

    /// Returns the exact official JSON POST endpoint that metadata must allowlist.
    pub const fn endpoint(&self) -> &'static str {
        match self {
            Self::PublicV1 => BLS_V1_ENDPOINT,
            Self::RegisteredV2(_) => BLS_V2_ENDPOINT,
        }
    }

    fn registration_key(&self) -> Option<&str> {
        match self {
            Self::PublicV1 => None,
            Self::RegisteredV2(key) => Some(key.expose()),
        }
    }
}

/// BLS adapter configuration, transport, or protocol failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum BlsSourceError {
    /// The user-supplied registered-v2 credential violates the bounded opaque-secret contract.
    #[error("invalid BLS registration key")]
    InvalidRegistrationKey,
    /// Series, year, or deterministic request-plan configuration is invalid.
    #[error("invalid BLS source configuration")]
    InvalidConfiguration,
    /// Exact user-authorized BLS series metadata is missing, malformed, or unverified.
    #[error("invalid BLS series metadata")]
    InvalidSeriesMetadata,
    /// Provider data or canonical normalization violated its exact schema.
    #[error("invalid BLS protocol data")]
    Protocol,
    /// The source metadata or runtime registration does not authorize this adapter configuration.
    #[error("BLS source metadata is incompatible with the adapter configuration")]
    InvalidMetadata,
    /// Provider response crossed the configured byte ceiling.
    #[error("BLS response exceeded its byte limit")]
    BodyTooLarge,
    /// The allowlisted transport failed without retaining request or credential data.
    #[error("BLS network operation failed")]
    Network,
}

#[derive(Serialize)]
struct BlsProviderRequest<'a> {
    seriesid: &'a [String],
    startyear: String,
    endyear: String,
    #[serde(rename = "registrationkey", skip_serializing_if = "Option::is_none")]
    registration_key: Option<&'a str>,
}

#[derive(Clone)]
pub(crate) struct RetrievedBlsPage {
    pub(crate) bytes: Bytes,
    pub(crate) response: BlsResponse,
    pub(crate) received_at: Timestamp,
    pub(crate) sha256_hex: String,
}

pub(crate) struct BlsHttpClient {
    client: reqwest::Client,
    authorization: BlsAuthorization,
    max_response_bytes: usize,
    total_timeout: Duration,
}

impl std::fmt::Debug for BlsHttpClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsHttpClient")
            .field("authorization", &self.authorization)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("total_timeout", &self.total_timeout)
            .finish_non_exhaustive()
    }
}

impl BlsHttpClient {
    pub(crate) fn try_new(
        metadata: &SourceMetadata,
        authorization: BlsAuthorization,
    ) -> Result<Self, BlsSourceError> {
        metadata
            .network_policy()
            .authorize(authorization.endpoint())
            .map_err(|_| BlsSourceError::InvalidMetadata)?;
        let NetworkAccessPolicy::Allowlisted(endpoint_policy) = metadata.network_policy() else {
            return Err(BlsSourceError::InvalidMetadata);
        };
        let bounds = endpoint_policy.request_bounds();
        let max_response_bytes = usize::try_from(bounds.max_response_bytes())
            .map_err(|_| BlsSourceError::InvalidMetadata)?
            .min(MAX_RESPONSE_BYTES);
        let connect_timeout = Duration::from_nanos(bounds.connect_timeout_nanos());
        let read_timeout = Duration::from_nanos(bounds.read_timeout_nanos());
        let total_timeout = Duration::from_nanos(bounds.total_timeout_nanos());
        let client = reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .retry(reqwest::retry::never())
            .connect_timeout(connect_timeout)
            .read_timeout(read_timeout)
            .timeout(total_timeout)
            .build()
            .map_err(|_| BlsSourceError::InvalidMetadata)?;
        Ok(Self {
            client,
            authorization,
            max_response_bytes,
            total_timeout,
        })
    }

    pub(crate) async fn fetch(
        &self,
        metadata: &SourceMetadata,
        budget: &SharedProviderBudget,
        chunk: &BlsRequestChunk,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedBlsPage, ExtractionSourceError> {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        let now = system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
        if !metadata.is_effective_at(now) {
            return Err(SourceError::Unauthorized.into());
        }
        metadata
            .network_policy()
            .authorize(self.authorization.endpoint())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let timeout = remaining_timeout(deadline, now, self.total_timeout)?;
        let permit = match budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            refusal => return Err(SourceError::from_applied_budget_refusal(refusal).into()),
        };

        let request = BlsProviderRequest {
            seriesid: chunk.series(),
            startyear: chunk.start_year().to_string(),
            endyear: chunk.end_year().to_string(),
            registration_key: self.authorization.registration_key(),
        };
        let request_body =
            serde_json::to_vec(&request).map_err(|_| SourceError::InvalidProtocolState)?;
        let operation = async {
            let response = self
                .client
                .post(self.authorization.endpoint())
                .header(ACCEPT, "application/json")
                .header(ACCEPT_ENCODING, "identity")
                .header(CONTENT_TYPE, "application/json")
                .body(request_body)
                .send()
                .await
                .map_err(|_| SourceError::Network)?;
            let status = response.status();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            {
                let retry_after = response
                    .headers()
                    .get(RETRY_AFTER)
                    .map(|value| value.as_bytes());
                let refusal = apply_http_retry_after(budget, retry_after, 0);
                return Err(SourceError::from_applied_budget_refusal(refusal).into());
            }
            if status == reqwest::StatusCode::UNAUTHORIZED
                || status == reqwest::StatusCode::FORBIDDEN
            {
                return Err(SourceError::Unauthorized.into());
            }
            if status != reqwest::StatusCode::OK {
                return Err(SourceError::ProviderUnavailable.into());
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
                return Err(SourceError::InvalidProtocolState.into());
            }
            if let Some(content_length) = response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                && content_length > self.max_response_bytes as u64
            {
                return Err(SourceError::FrameTooLarge {
                    max: self.max_response_bytes,
                }
                .into());
            }
            let bytes = collect_bounded_stream(response.bytes_stream(), self.max_response_bytes)
                .await
                .map_err(|error| map_source_error(error, self.max_response_bytes))?;
            let requested = chunk
                .series()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let parsed = BlsResponse::parse_for_request(
                &bytes,
                self.authorization.tier(),
                &requested,
                chunk.start_year(),
                chunk.end_year(),
            )
            .map_err(|_| SourceError::InvalidProtocolState)?;
            let received_at =
                system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
            let digest = Sha256::digest(&bytes);
            Ok(RetrievedBlsPage {
                bytes,
                response: parsed,
                received_at,
                sha256_hex: format!("{digest:x}"),
            })
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(ExtractionSourceError::Cancelled),
            result = tokio::time::timeout(timeout, operation) => {
                result.map_err(|_| ExtractionSourceError::DeadlineExceeded)?
            }
        };
        permit.release();
        result
    }
}

async fn collect_bounded_stream<S, E>(
    mut stream: S,
    max_bytes: usize,
) -> Result<Bytes, BlsSourceError>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut body = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BlsSourceError::Network)?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or(BlsSourceError::BodyTooLarge)?;
        if next > max_bytes {
            return Err(BlsSourceError::BodyTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

fn map_source_error(error: BlsSourceError, max_response_bytes: usize) -> SourceError {
    match error {
        BlsSourceError::BodyTooLarge => SourceError::FrameTooLarge {
            max: max_response_bytes,
        },
        BlsSourceError::Network => SourceError::Network,
        BlsSourceError::InvalidRegistrationKey
        | BlsSourceError::InvalidConfiguration
        | BlsSourceError::InvalidSeriesMetadata
        | BlsSourceError::Protocol
        | BlsSourceError::InvalidMetadata => SourceError::InvalidProtocolState,
    }
}

fn system_timestamp() -> Result<Timestamp, BlsSourceError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BlsSourceError::Protocol)?;
    let nanos = u128::from(duration.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(duration.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(BlsSourceError::Protocol)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

pub(crate) fn ensure_deadline_open(deadline: Timestamp) -> Result<(), ExtractionSourceError> {
    let now = system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
    if deadline <= now {
        Err(ExtractionSourceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn remaining_timeout(
    deadline: Timestamp,
    now: Timestamp,
    configured_total: Duration,
) -> Result<Duration, ExtractionSourceError> {
    let remaining = deadline
        .unix_nanos()
        .checked_sub(now.unix_nanos())
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value > 0)
        .map(Duration::from_nanos)
        .ok_or(ExtractionSourceError::DeadlineExceeded)?;
    Ok(remaining.min(configured_total))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::stream;

    use super::{BlsProviderRequest, collect_bounded_stream};

    #[tokio::test]
    async fn response_body_limit_applies_across_streamed_chunks() {
        let body = stream::iter([
            Ok::<_, std::io::Error>(Bytes::from_static(b"1234")),
            Ok(Bytes::from_static(b"5678")),
        ]);
        assert!(collect_bounded_stream(body, 7).await.is_err());
    }

    #[test]
    fn registered_request_uses_the_provider_exact_json_key() -> Result<(), serde_json::Error> {
        let series = vec!["LNS14000000".to_owned()];
        let request = BlsProviderRequest {
            seriesid: &series,
            startyear: "2020".to_owned(),
            endyear: "2026".to_owned(),
            registration_key: Some("secret"),
        };
        let value = serde_json::to_value(request)?;
        assert_eq!(value["registrationkey"], "secret");
        assert!(value.get("registrationKey").is_none());
        Ok(())
    }
}
