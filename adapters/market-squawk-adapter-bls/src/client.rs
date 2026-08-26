use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_sources::{
    ExtractionAuthority, ExtractionAuthorityError, ExtractionRequestPermit,
    ExtractionSourceError, NetworkAccessPolicy, SourceError, SourceMetadata,
};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE, RETRY_AFTER, USER_AGENT,
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
const USER_AGENT_VALUE: &str = "market-squawk/0.1 bls-adapter";

/// User-owned BLS v2 registration credential retained only in zeroizing memory.
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

/// Secret-free root credential coordinate retained by activation and publication evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "generation_digest")]
pub enum BlsCredentialRejoin {
    /// Public v1 intentionally uses no provider credential.
    PublicNoCredential,
    /// Registered v2 used the exact protected root credential generation identified here.
    RegisteredGeneration(EvidenceDigest),
}

impl BlsCredentialRejoin {
    fn validate(self) -> Result<(), BlsSourceError> {
        match self {
            Self::PublicNoCredential => Ok(()),
            Self::RegisteredGeneration(digest)
                if digest.algorithm() == DigestAlgorithm::Sha256
                    && digest.bytes() != [0; 32] =>
            {
                Ok(())
            }
            Self::RegisteredGeneration(_) => Err(BlsSourceError::InvalidRegistrationKey),
        }
    }
}

/// Non-cloneable authorization for one exact public or registered BLS runtime instance.
///
/// Registered key bytes have one owner, are never included in `Debug`, and cannot be separated
/// from the protected root generation coordinate used by doctor and publication rejoin evidence.
pub struct BlsAuthorization {
    tier: BlsAccessTier,
    registration_key: Option<BlsRegistrationKey>,
    credential_rejoin: BlsCredentialRejoin,
}

impl BlsAuthorization {
    /// Constructs the explicit no-credential public-v1 mode.
    pub const fn public_v1() -> Self {
        Self {
            tier: BlsAccessTier::PublicV1,
            registration_key: None,
            credential_rejoin: BlsCredentialRejoin::PublicNoCredential,
        }
    }

    /// Binds one zeroizing registered-v2 key to its exact protected root generation.
    pub fn registered_v2(
        registration_key: BlsRegistrationKey,
        credential_generation_digest: EvidenceDigest,
    ) -> Result<Self, BlsSourceError> {
        let credential_rejoin =
            BlsCredentialRejoin::RegisteredGeneration(credential_generation_digest);
        credential_rejoin.validate()?;
        Ok(Self {
            tier: BlsAccessTier::RegisteredV2,
            registration_key: Some(registration_key),
            credential_rejoin,
        })
    }

    /// Returns the exact provider tier selected by this authorization mode.
    pub const fn tier(&self) -> BlsAccessTier {
        self.tier
    }

    /// Returns the exact official JSON POST endpoint that metadata must allowlist.
    pub const fn endpoint(&self) -> &'static str {
        match self.tier {
            BlsAccessTier::PublicV1 => BLS_V1_ENDPOINT,
            BlsAccessTier::RegisteredV2 => BLS_V2_ENDPOINT,
        }
    }

    /// Returns the explicit no-credential marker or registered root generation coordinate.
    pub const fn credential_rejoin(&self) -> BlsCredentialRejoin {
        self.credential_rejoin
    }

    fn registration_key(&self) -> Option<&str> {
        self.registration_key.as_ref().map(BlsRegistrationKey::expose)
    }

    fn validate(&self) -> Result<(), BlsSourceError> {
        self.credential_rejoin.validate()?;
        match (self.tier, &self.registration_key, self.credential_rejoin) {
            (BlsAccessTier::PublicV1, None, BlsCredentialRejoin::PublicNoCredential)
            | (
                BlsAccessTier::RegisteredV2,
                Some(_),
                BlsCredentialRejoin::RegisteredGeneration(_),
            ) => Ok(()),
            _ => Err(BlsSourceError::InvalidRegistrationKey),
        }
    }
}

impl std::fmt::Debug for BlsAuthorization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsAuthorization")
            .field("tier", &self.tier)
            .field("credential_rejoin", &self.credential_rejoin)
            .field(
                "registration_key",
                &self.registration_key.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
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
    /// The required owner-authorized private-research policy is absent or uses placeholder evidence.
    #[error("invalid BLS private-research usage policy")]
    InvalidUsagePolicy,
    /// Raw capture or canonical handoff evidence is not publication-safe.
    #[error("invalid BLS publication evidence")]
    InvalidPublication,
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
    /// Local source-health synchronization is unavailable.
    #[error("BLS source health is unavailable")]
    HealthUnavailable,
    /// Bounded locally observed revision evidence could not be constructed.
    #[error(transparent)]
    RevisionAuthority(#[from] market_squawk_sources::ObservedRevisionError),
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
    transport: Arc<dyn BlsTransport>,
    tier: BlsAccessTier,
    endpoint: &'static str,
    max_response_bytes: usize,
    total_timeout: Duration,
}

impl std::fmt::Debug for BlsHttpClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsHttpClient")
            .field("tier", &self.tier)
            .field("endpoint", &self.endpoint)
            .field("transport", &self.transport)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("total_timeout", &self.total_timeout)
            .finish_non_exhaustive()
    }
}

impl BlsHttpClient {
    pub(crate) fn try_new(
        metadata: &SourceMetadata,
        authorization: &BlsAuthorization,
    ) -> Result<Self, BlsSourceError> {
        authorization.validate()?;
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
        let total_timeout = Duration::from_nanos(bounds.total_timeout_nanos());
        let transport = Arc::new(ReqwestBlsTransport::try_new(bounds)?);
        Ok(Self {
            transport,
            tier: authorization.tier(),
            endpoint: authorization.endpoint(),
            max_response_bytes,
            total_timeout,
        })
    }

    #[cfg(test)]
    pub(crate) fn try_new_with_transport(
        metadata: &SourceMetadata,
        authorization: &BlsAuthorization,
        transport: Arc<dyn BlsTransport>,
    ) -> Result<Self, BlsSourceError> {
        authorization.validate()?;
        metadata
            .network_policy()
            .authorize(authorization.endpoint())
            .map_err(|_| BlsSourceError::InvalidMetadata)?;
        let NetworkAccessPolicy::Allowlisted(endpoint_policy) = metadata.network_policy() else {
            return Err(BlsSourceError::InvalidMetadata);
        };
        let bounds = endpoint_policy.request_bounds();
        Ok(Self {
            transport,
            tier: authorization.tier(),
            endpoint: authorization.endpoint(),
            max_response_bytes: usize::try_from(bounds.max_response_bytes())
                .map_err(|_| BlsSourceError::InvalidMetadata)?
                .min(MAX_RESPONSE_BYTES),
            total_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
        })
    }

    pub(crate) async fn fetch(
        &self,
        metadata: &SourceMetadata,
        authorization: &BlsAuthorization,
        authority: &ExtractionAuthority,
        chunk: &BlsRequestChunk,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedBlsPage, ExtractionSourceError> {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        let now = system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
        authority.validate_current()?;
        if authority.metadata() != metadata || !metadata.is_effective_at(now) {
            return Err(SourceError::InvalidProtocolState.into());
        }
        authorization
            .validate()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        if authorization.tier() != self.tier || authorization.endpoint() != self.endpoint {
            return Err(SourceError::InvalidProtocolState.into());
        }
        metadata
            .network_policy()
            .authorize(self.endpoint)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let request = BlsProviderRequest {
            seriesid: chunk.series(),
            startyear: chunk.start_year().to_string(),
            endyear: chunk.end_year().to_string(),
            registration_key: authorization.registration_key(),
        };
        let request_body = Zeroizing::new(
            serde_json::to_vec(&request).map_err(|_| SourceError::InvalidProtocolState)?,
        );
        let request_body = Bytes::from_owner(request_body);
        let permit = acquire_request_permit(
            authority,
            self.endpoint,
            deadline,
            cancellation.clone(),
        )
        .await?;
        let now = system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
        let timeout = remaining_timeout(deadline, now, self.total_timeout)?;
        let in_flight = permit.authorize_send(self.endpoint)?;
        let response = self
            .transport
            .execute(
                BlsHttpRequest {
                    url: self.endpoint.to_owned(),
                    body: request_body,
                },
                self.max_response_bytes,
                timeout,
                cancellation.clone(),
            )
            .await?;
        if response.status == 429 || response.status == 503 {
            let deadline =
                in_flight.apply_retry_after_header(response.retry_after.as_deref(), 0)?;
            return Err(SourceError::BudgetWaitUntil { deadline }.into());
        }
        if response.status == 401 || response.status == 403 {
            return Err(SourceError::Unauthorized.into());
        }
        if response.status != 200 {
            return Err(SourceError::ProviderUnavailable.into());
        }
        if response
            .content_encoding
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
            || !content_type_is_json(response.content_type.as_deref())
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        in_flight.validate_response_size(
            u64::try_from(response.body.len()).map_err(|_| SourceError::InvalidProtocolState)?,
        )?;
        let requested = chunk
            .series()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let parsed = BlsResponse::parse_for_request(
            &response.body,
            self.tier,
            &requested,
            chunk.start_year(),
            chunk.end_year(),
        )
        .map_err(|_| SourceError::InvalidProtocolState)?;
        let digest = Sha256::digest(&response.body);
        let page = RetrievedBlsPage {
            bytes: response.body,
            response: parsed,
            received_at: response.received_at,
            sha256_hex: format!("{digest:x}"),
        };
        in_flight.record_success()?;
        Ok(page)
    }
}

pub(crate) struct BlsHttpRequest {
    pub(crate) url: String,
    pub(crate) body: Bytes,
}

impl std::fmt::Debug for BlsHttpRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsHttpRequest")
            .field("url", &self.url)
            .field("body_bytes", &self.body.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BlsHttpResponse {
    pub(crate) status: u16,
    pub(crate) retry_after: Option<Vec<u8>>,
    pub(crate) content_encoding: Option<Vec<u8>>,
    pub(crate) content_type: Option<Vec<u8>>,
    pub(crate) body: Bytes,
    pub(crate) received_at: Timestamp,
}

pub(crate) trait BlsTransport: std::fmt::Debug + Send + Sync {
    fn execute(
        &self,
        request: BlsHttpRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<BlsHttpResponse, ExtractionSourceError>>;
}

#[derive(Debug)]
struct ReqwestBlsTransport {
    client: reqwest::Client,
}

impl ReqwestBlsTransport {
    fn try_new(bounds: market_squawk_sources::HttpRequestBounds) -> Result<Self, BlsSourceError> {
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
            .map_err(|_| BlsSourceError::InvalidMetadata)?;
        Ok(Self { client })
    }
}

impl BlsTransport for ReqwestBlsTransport {
    fn execute(
        &self,
        request: BlsHttpRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<BlsHttpResponse, ExtractionSourceError>> {
        Box::pin(async move {
            let operation = async {
                let response = self
                    .client
                    .post(request.url)
                    .header(ACCEPT, "application/json")
                    .header(ACCEPT_ENCODING, "identity")
                    .header(CONTENT_TYPE, "application/json")
                    .header(USER_AGENT, USER_AGENT_VALUE)
                    .body(request.body)
                    .send()
                    .await
                    .map_err(|_| SourceError::Network)?;
                if response.content_length().is_some_and(|length| {
                    usize::try_from(length).map_or(true, |length| length > max_bytes)
                }) {
                    return Err(SourceError::FrameTooLarge { max: max_bytes }.into());
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
                let body = collect_bounded_stream(response.bytes_stream(), max_bytes)
                    .await
                    .map_err(|error| map_source_error(error, max_bytes))?;
                Ok(BlsHttpResponse {
                    status,
                    retry_after,
                    content_encoding,
                    content_type,
                    body,
                    received_at: system_timestamp()
                        .map_err(|_| SourceError::TrustedTimeUnavailable)?,
                })
            };
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(ExtractionSourceError::Cancelled),
                result = tokio::time::timeout(timeout, operation) => {
                    result.map_err(|_| ExtractionSourceError::DeadlineExceeded)?
                }
            }
        })
    }
}

fn content_type_is_json(value: Option<&[u8]>) -> bool {
    value
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

async fn acquire_request_permit(
    authority: &ExtractionAuthority,
    target: &str,
    deadline: Timestamp,
    cancellation: CancellationToken,
) -> Result<ExtractionRequestPermit, ExtractionSourceError> {
    loop {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        match authority.try_network_request(target) {
            Ok(permit) => return Ok(permit),
            Err(ExtractionAuthorityError::BudgetWaitUntil {
                deadline: wait_until,
            }) => {
                let wait = authority.remaining_budget_wait(wait_until)?;
                let now = system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
                let remaining = remaining_timeout(deadline, now, Duration::MAX)?;
                if wait > remaining {
                    return Err(ExtractionSourceError::DeadlineExceeded);
                }
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {
                        return Err(ExtractionSourceError::Cancelled);
                    }
                    () = tokio::time::sleep(wait) => {}
                }
            }
            Err(error) => return Err(error.into()),
        }
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
        | BlsSourceError::InvalidUsagePolicy
        | BlsSourceError::InvalidPublication
        | BlsSourceError::InvalidSeriesMetadata
        | BlsSourceError::Protocol
        | BlsSourceError::InvalidMetadata
        | BlsSourceError::HealthUnavailable
        | BlsSourceError::RevisionAuthority(_) => SourceError::InvalidProtocolState,
    }
}

pub(crate) fn system_timestamp() -> Result<Timestamp, BlsSourceError> {
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
