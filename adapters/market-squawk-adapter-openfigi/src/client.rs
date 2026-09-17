use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt as _;
use market_squawk_domain::{
    AssetClass, ChecksumCapability, DataQuality, DeliveryEvidence, SequenceCapability, Timestamp,
};
use market_squawk_sources::{
    AuthorizationMode, CoverageDomain, ExtractionAuthority, HistoricalCapability,
    NetworkAccessPolicy, SourceClass, SourceMetadata, SourceMetadataProvider,
    SourceProtocolProfile, TlsProviderCapability,
};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue,
    RETRY_AFTER, USER_AGENT,
};
use tokio_util::sync::CancellationToken;

use crate::{
    MAX_OPENFIGI_RESPONSE_BYTES, OPENFIGI_V3_MAPPING_URL, OPENFIGI_V3_PROVIDER, OpenFigiAccess,
    OpenFigiApiKey, OpenFigiClientError, OpenFigiListingMappingJob, OpenFigiMappingReceipt,
    OpenFigiRateLimitError, OpenFigiRateLimitEvidence, OpenFigiRawPayload, encode_mapping_request,
    parse_mapping_response,
};

const API_KEY_HEADER: HeaderName = HeaderName::from_static("x-openfigi-apikey");
const RATE_LIMIT: HeaderName = HeaderName::from_static("ratelimit-limit");
const RATE_REMAINING: HeaderName = HeaderName::from_static("ratelimit-remaining");
const RATE_RESET: HeaderName = HeaderName::from_static("ratelimit-reset");
const USER_AGENT_VALUE: &str = concat!(
    "market-squawk/",
    env!("CARGO_PKG_VERSION"),
    " openfigi-v3-mapping-adapter"
);

/// Registered one-shot OpenFIGI V3 mapping client.
///
/// Every request requires an exact current [`ExtractionAuthority`]. The client performs no
/// polling, retry, background refresh, or independent rate admission.
pub struct OpenFigiClient {
    metadata: SourceMetadata,
    access: OpenFigiAccess,
    client: reqwest::Client,
    max_response_bytes: usize,
    total_timeout: Duration,
    read_timeout: Duration,
    latest_rate_limit: Mutex<Option<OpenFigiRateLimitEvidence>>,
}

impl std::fmt::Debug for OpenFigiClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenFigiClient")
            .field("source_id", self.metadata.source_id())
            .field("revision", self.metadata.revision())
            .field("access", &self.access)
            .finish_non_exhaustive()
    }
}

impl OpenFigiClient {
    /// Constructs the hardened fixed-endpoint transport.
    ///
    /// # Errors
    ///
    /// Rejects metadata that is live/executable, lacks exact instrument/venue/network/shared-
    /// budget authority, exceeds the official access-tier rate, or uses a non-hardened client
    /// profile.
    pub fn try_new(
        metadata: SourceMetadata,
        access: OpenFigiAccess,
        tls_provider: TlsProviderCapability,
    ) -> Result<Self, OpenFigiClientError> {
        validate_metadata(&metadata, access)?;
        let NetworkAccessPolicy::Allowlisted(endpoint) = metadata.network_policy() else {
            return Err(OpenFigiClientError::InvalidMetadata);
        };
        let profile = endpoint.client_profile();
        if !profile.automatic_redirects_disabled()
            || !profile.ambient_system_proxy_disabled()
            || !profile.implicit_retries_disabled()
            || !profile.counts_post_decompression_bytes()
        {
            return Err(OpenFigiClientError::InvalidMetadata);
        }
        let _provider_identity = tls_provider.provider_id();
        let bounds = endpoint.request_bounds();
        let max_response_bytes = usize::try_from(bounds.max_response_bytes())
            .map_err(|_| OpenFigiClientError::InvalidMetadata)?
            .min(MAX_OPENFIGI_RESPONSE_BYTES);
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
            .map_err(|_| OpenFigiClientError::InvalidMetadata)?;
        Ok(Self {
            metadata,
            access,
            client,
            max_response_bytes,
            total_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
            read_timeout: Duration::from_nanos(bounds.read_timeout_nanos()),
            latest_rate_limit: Mutex::new(None),
        })
    }

    /// Maps one bounded batch of current Nasdaq listing symbol/MIC pairs.
    ///
    /// `api_key` must be absent for public metadata and present for API-key metadata. It is
    /// borrowed only while constructing and sending this one request.
    ///
    /// # Errors
    ///
    /// Fails closed on stale authority, uncovered venues, unavailable shared budget, cancellation,
    /// deadline, transport/representation/rate-header failure, response overflow, invalid V3 JSON,
    /// or result cardinality mismatch. Ambiguity and per-job provider conflicts are successful
    /// typed outcomes that cannot be promoted implicitly.
    pub async fn map_nasdaq_listings(
        &self,
        authority: &ExtractionAuthority,
        jobs: Vec<OpenFigiListingMappingJob>,
        api_key: Option<&OpenFigiApiKey>,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<OpenFigiMappingReceipt, OpenFigiClientError> {
        self.validate_authority(authority)?;
        self.validate_credentials(api_key)?;
        for job in &jobs {
            if !self
                .metadata
                .coverage()
                .topology()
                .contains_venue(job.mic())
            {
                return Err(OpenFigiClientError::InvalidMetadata);
            }
        }
        let request_bytes = Bytes::from(encode_mapping_request(&jobs, self.access)?);
        if cancellation.is_cancelled() {
            return Err(OpenFigiClientError::Cancelled);
        }
        let now = system_timestamp()?;
        let timeout = remaining_timeout(deadline, now, self.total_timeout)?;
        let permit = authority.try_network_request(OPENFIGI_V3_MAPPING_URL)?;
        let in_flight = permit.authorize_send(OPENFIGI_V3_MAPPING_URL)?;

        let mut request = self
            .client
            .post(OPENFIGI_V3_MAPPING_URL)
            .header(ACCEPT, "application/json")
            .header(ACCEPT_ENCODING, "identity")
            .header(CONTENT_TYPE, "application/json")
            .header(USER_AGENT, USER_AGENT_VALUE)
            .body(request_bytes.clone());
        if let Some(api_key) = api_key {
            let mut value = HeaderValue::from_str(api_key.expose_secret())
                .map_err(|_| OpenFigiClientError::CredentialMismatch)?;
            value.set_sensitive(true);
            request = request.header(&API_KEY_HEADER, value);
        }
        let requested_at = system_timestamp()?;

        let operation = async {
            let response = request
                .send()
                .await
                .map_err(|_| OpenFigiClientError::Network)?;
            in_flight.validate_current()?;
            let status = response.status().as_u16();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .map(|value| value.as_bytes().to_vec());
            let rate_limit = parse_rate_headers(response.headers());
            if let Ok(evidence) = &rate_limit {
                self.record_rate_limit(evidence.clone())?;
            }
            let content_encoding = response
                .headers()
                .get(CONTENT_ENCODING)
                .map(|value| value.as_bytes().to_vec());
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .map(|value| value.as_bytes().to_vec());
            if response.content_length().is_some_and(|length| {
                usize::try_from(length).map_or(true, |length| length > self.max_response_bytes)
            }) {
                return Err(OpenFigiClientError::ResponseTooLarge {
                    max: self.max_response_bytes,
                });
            }
            let body = read_body(
                response,
                &in_flight,
                self.max_response_bytes,
                self.read_timeout,
            )
            .await?;
            if status == 429 || status == 503 {
                let deadline = in_flight.apply_retry_after_header(retry_after.as_deref(), 1_000)?;
                return Err(OpenFigiClientError::Authority(
                    market_squawk_sources::ExtractionAuthorityError::BudgetWaitUntil { deadline },
                ));
            }
            if status == 401 || status == 403 {
                return Err(OpenFigiClientError::Unauthorized);
            }
            if status == 500 {
                return Err(OpenFigiClientError::ProviderUnavailable);
            }
            if status != 200 {
                return Err(OpenFigiClientError::ProviderRejected { status });
            }
            if content_encoding
                .as_deref()
                .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
                || !content_type_is_json(content_type.as_deref())
            {
                return Err(OpenFigiClientError::InvalidRepresentation);
            }
            let rate_limit = rate_limit?;
            let received_at = system_timestamp()?;
            let results = parse_mapping_response(&jobs, &body)?;
            in_flight.validate_current()?;
            in_flight.release();
            Ok(OpenFigiMappingReceipt::try_new(
                self.metadata.source_id().clone(),
                self.metadata.revision().clone(),
                self.metadata.coverage().evidence().clone(),
                self.access,
                requested_at,
                received_at,
                OpenFigiRawPayload::new(request_bytes),
                OpenFigiRawPayload::new(body),
                rate_limit,
                results,
            )?)
        };
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(OpenFigiClientError::Cancelled),
            result = tokio::time::timeout(timeout, operation) => {
                result.map_err(|_| OpenFigiClientError::DeadlineExceeded)?
            }
        }
    }

    /// Returns the latest complete rate-window header set observed by this client.
    ///
    /// Provider headers never expand the registry-owned local budget.
    pub fn latest_rate_limit_evidence(
        &self,
    ) -> Result<Option<OpenFigiRateLimitEvidence>, OpenFigiClientError> {
        self.latest_rate_limit
            .lock()
            .map(|evidence| evidence.clone())
            .map_err(|_| OpenFigiClientError::RateEvidenceUnavailable)
    }

    fn validate_authority(
        &self,
        authority: &ExtractionAuthority,
    ) -> Result<(), OpenFigiClientError> {
        authority.validate_current()?;
        if authority.metadata() != &self.metadata {
            return Err(OpenFigiClientError::InvalidMetadata);
        }
        Ok(())
    }

    fn validate_credentials(
        &self,
        api_key: Option<&OpenFigiApiKey>,
    ) -> Result<(), OpenFigiClientError> {
        if matches!(
            (self.access, api_key.is_some()),
            (OpenFigiAccess::Public, false) | (OpenFigiAccess::ApiKey, true)
        ) {
            Ok(())
        } else {
            Err(OpenFigiClientError::CredentialMismatch)
        }
    }

    fn record_rate_limit(
        &self,
        evidence: OpenFigiRateLimitEvidence,
    ) -> Result<(), OpenFigiClientError> {
        let mut latest = self
            .latest_rate_limit
            .lock()
            .map_err(|_| OpenFigiClientError::RateEvidenceUnavailable)?;
        *latest = Some(evidence);
        Ok(())
    }
}

impl SourceMetadataProvider for OpenFigiClient {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

async fn read_body(
    response: reqwest::Response,
    in_flight: &market_squawk_sources::InFlightExtractionRequest,
    max_response_bytes: usize,
    read_timeout: Duration,
) -> Result<Bytes, OpenFigiClientError> {
    let mut stream = response.bytes_stream();
    let mut body = BytesMut::new();
    loop {
        in_flight.validate_current()?;
        let next = tokio::time::timeout(read_timeout, stream.next())
            .await
            .map_err(|_| OpenFigiClientError::DeadlineExceeded)?;
        let Some(chunk) = next else { break };
        let chunk = chunk.map_err(|_| OpenFigiClientError::Network)?;
        let next_len =
            body.len()
                .checked_add(chunk.len())
                .ok_or(OpenFigiClientError::ResponseTooLarge {
                    max: max_response_bytes,
                })?;
        if next_len > max_response_bytes {
            return Err(OpenFigiClientError::ResponseTooLarge {
                max: max_response_bytes,
            });
        }
        in_flight.validate_response_size(u64::try_from(next_len).map_err(|_| {
            OpenFigiClientError::ResponseTooLarge {
                max: max_response_bytes,
            }
        })?)?;
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

fn parse_rate_headers(
    headers: &HeaderMap,
) -> Result<OpenFigiRateLimitEvidence, OpenFigiRateLimitError> {
    let limit = unique_header(headers, &RATE_LIMIT)?;
    let remaining = unique_header(headers, &RATE_REMAINING)?;
    let reset = unique_header(headers, &RATE_RESET)?;
    OpenFigiRateLimitEvidence::try_from_raw(limit, remaining, reset)
}

fn unique_header<'a>(
    headers: &'a HeaderMap,
    name: &HeaderName,
) -> Result<&'a [u8], OpenFigiRateLimitError> {
    let mut values = headers.get_all(name).iter();
    let value = values.next().ok_or(OpenFigiRateLimitError::Missing)?;
    if values.next().is_some() {
        return Err(OpenFigiRateLimitError::Duplicate);
    }
    Ok(value.as_bytes())
}

fn validate_metadata(
    metadata: &SourceMetadata,
    access: OpenFigiAccess,
) -> Result<(), OpenFigiClientError> {
    let capabilities = metadata.capabilities();
    let expected_authorization = match access {
        OpenFigiAccess::Public => AuthorizationMode::PublicInterface,
        OpenFigiAccess::ApiKey => AuthorizationMode::UserAuthorized,
    };
    if metadata.source_class() != SourceClass::LicensedDataset
        || metadata.provider().as_str() != OPENFIGI_V3_PROVIDER
        || metadata.authorization().mode() != expected_authorization
        || metadata.quality_ceiling() != DataQuality::Aggregated
        || metadata.coverage().domain() != CoverageDomain::Instruments
        || metadata.coverage().asset_classes().is_empty()
        || metadata
            .coverage()
            .asset_classes()
            .iter()
            .any(|asset| !matches!(asset, AssetClass::Equity | AssetClass::Fund))
        || metadata.coverage().topology().is_not_applicable()
        || metadata.coverage().topology().is_consolidated()
        || metadata.coverage().live().is_some()
        || metadata.coverage().delivery() != DeliveryEvidence::Indirect
        || capabilities.live()
        || !capabilities.extraction()
        || capabilities.sequence() != SequenceCapability::Unsupported
        || capabilities.checksum() != ChecksumCapability::Unsupported
        || capabilities.historical() != HistoricalCapability::None
        || capabilities.source_timestamps()
        || !matches!(metadata.protocol_profile(), SourceProtocolProfile::NotLive)
        || metadata
            .network_policy()
            .authorize(OPENFIGI_V3_MAPPING_URL)
            .is_err()
    {
        return Err(OpenFigiClientError::InvalidMetadata);
    }
    let budget = metadata
        .budget_policy()
        .ok_or(OpenFigiClientError::InvalidMetadata)?;
    let (max_requests, minimum_window) = access.request_window();
    let has_conservative_window = (0..budget.window_count()).any(|index| {
        budget.window(index).is_some_and(|window| {
            window.requests_per_window() <= max_requests && window.window_nanos() >= minimum_window
        })
    });
    if !has_conservative_window {
        return Err(OpenFigiClientError::InvalidMetadata);
    }
    Ok(())
}

fn content_type_is_json(value: Option<&[u8]>) -> bool {
    value
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

fn remaining_timeout(
    deadline: Timestamp,
    now: Timestamp,
    configured: Duration,
) -> Result<Duration, OpenFigiClientError> {
    let remaining = deadline
        .unix_nanos()
        .checked_sub(now.unix_nanos())
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value > 0)
        .map(Duration::from_nanos)
        .ok_or(OpenFigiClientError::DeadlineExceeded)?;
    Ok(remaining.min(configured))
}

fn system_timestamp() -> Result<Timestamp, OpenFigiClientError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OpenFigiClientError::Clock)?;
    let nanos = u128::from(duration.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(duration.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(OpenFigiClientError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
