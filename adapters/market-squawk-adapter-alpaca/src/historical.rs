use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures_util::{StreamExt as _, future::BoxFuture};
use market_squawk_domain::{
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, SourceIdentifier,
    Timestamp,
};
use market_squawk_sources::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryBatch, DiscoveryRequest,
    ExtractionAuthority, ExtractionAuthorityError, ExtractionBatch, ExtractionRecord,
    ExtractionRequest, ExtractionRequestPermit, ExtractionSource, ExtractionSourceError,
    SourceError, SourceMetadata, SourceMetadataProvider, SourceObject,
    payload_matches_exact_evidence,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Number;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::config::{ALPACA_HISTORICAL_EXCLUSION_NANOS, ALPACA_STOCKS_BASE_ENDPOINT};
use crate::{
    AlpacaCredentials, AlpacaError, AlpacaHistoricalEquityConfig, AlpacaHistoricalEquityDataset,
};

const MAX_PAGE_TOKEN_BYTES: usize = 256;

/// Validated provider rate-limit response evidence from the most recent historical request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlpacaRateLimitEvidence {
    /// Provider-declared request ceiling, when supplied.
    pub limit: Option<u32>,
    /// Provider-declared remaining requests, when supplied.
    pub remaining: Option<u32>,
    /// Provider-declared Unix reset coordinate, when supplied.
    pub reset_unix_seconds: Option<i64>,
}

/// Registry-authorized, extraction-only Alpaca IEX historical-bars source.
pub struct AlpacaHistoricalEquitySource {
    config: AlpacaHistoricalEquityConfig,
    credentials: Arc<AlpacaCredentials>,
    client: reqwest::Client,
    rate_evidence: Mutex<Option<AlpacaRateLimitEvidence>>,
}

impl std::fmt::Debug for AlpacaHistoricalEquitySource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaHistoricalEquitySource")
            .field("source_id", self.config.metadata().source_id())
            .field("revision", self.config.metadata().revision())
            .field("credentials", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl AlpacaHistoricalEquitySource {
    /// Constructs the hardened no-redirect/no-proxy/no-retry HTTP transport.
    pub fn try_new(
        config: AlpacaHistoricalEquityConfig,
        credentials: Arc<AlpacaCredentials>,
    ) -> Result<Self, AlpacaError> {
        let bounds = config.request_bounds();
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
            .user_agent("market-squawk/0.1 alpaca-basic-adapter")
            .build()
            .map_err(|_| AlpacaError::Network)?;
        Ok(Self {
            config,
            credentials,
            client,
            rate_evidence: Mutex::new(None),
        })
    }

    /// Returns the latest structurally valid provider rate-limit header evidence.
    ///
    /// Local admission always remains governed by the registry's shared 200-per-minute budget;
    /// provider headers can make that budget more conservative but never expand it.
    pub fn rate_limit_evidence(&self) -> Result<Option<AlpacaRateLimitEvidence>, AlpacaError> {
        self.rate_evidence
            .lock()
            .map(|evidence| *evidence)
            .map_err(|_| AlpacaError::Protocol)
    }

    async fn discover_impl(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryBatch, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.effective_at().is_some() {
            return Err(SourceError::InvalidProtocolState.into());
        }
        let dataset = self
            .config
            .dataset(request.dataset())
            .ok_or(SourceError::InvalidProtocolState)?;
        enforce_historical_window(dataset).map_err(map_adapter_error)?;
        let mut objects = Vec::new();
        let mut page_token = None;
        let mut seen_tokens = BTreeSet::new();
        let mut page_index = 0_u16;
        loop {
            if objects.len() == usize::from(request.max_results()) {
                return Err(
                    market_squawk_sources::ExtractionError::DiscoveryLimitExceeded {
                        requested: request.max_results(),
                    }
                    .into(),
                );
            }
            let page = self
                .fetch_page(
                    &authority,
                    dataset,
                    page_token.as_deref(),
                    request.deadline(),
                    cancellation.clone(),
                )
                .await?;
            validate_page(dataset, &page.parsed).map_err(map_adapter_error)?;
            if page.parsed.bars.is_empty() {
                if page.parsed.next_page_token.is_some() {
                    return Err(SourceError::InvalidProtocolState.into());
                }
                break;
            }
            let digest = exact_evidence(&page.body);
            let object_id = page_object_id(page_index, page_token.as_deref(), &digest)
                .map_err(map_adapter_error)?;
            let effective = EffectiveInterval::new(page.received_at, None)
                .map_err(|_| SourceError::InvalidProtocolState)?;
            objects.push(SourceObject::try_new_with_availability(
                self.config.metadata().source_id().clone(),
                self.config.metadata().revision().clone(),
                &request,
                object_id,
                SourceIdentifier::try_from("application/vnd.alpaca.iex-bars+json")
                    .map_err(|_| SourceError::InvalidProtocolState)?,
                digest,
                effective,
                None,
                AvailabilityEvidence::LocalFirstObserved {
                    observed_at: page.received_at,
                },
                Some(
                    u64::try_from(page.body.len())
                        .map_err(|_| SourceError::InvalidProtocolState)?,
                ),
            )?);
            let Some(next) = page.parsed.next_page_token else {
                break;
            };
            validate_page_token(&next).map_err(map_adapter_error)?;
            if !seen_tokens.insert(next.clone()) {
                return Err(SourceError::GenerationResynchronizationRequired.into());
            }
            page_token = Some(next);
            page_index = page_index
                .checked_add(1)
                .ok_or(SourceError::InvalidProtocolState)?;
        }
        authority.validate_current()?;
        DiscoveryBatch::try_new(&request, objects).map_err(Into::into)
    }

    async fn extract_impl(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<ExtractionBatch, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.object().source_id() != self.config.metadata().source_id()
            || request.object().metadata_revision() != self.config.metadata().revision()
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        let dataset = self
            .config
            .dataset(request.object().dataset())
            .ok_or(SourceError::InvalidProtocolState)?;
        enforce_historical_window(dataset).map_err(map_adapter_error)?;
        let identity =
            parse_page_object_id(request.object().object_id()).map_err(map_adapter_error)?;
        let page = self
            .fetch_page(
                &authority,
                dataset,
                identity.page_token.as_deref(),
                request.deadline(),
                cancellation,
            )
            .await?;
        validate_page(dataset, &page.parsed).map_err(map_adapter_error)?;
        if identity.digest != exact_evidence(&page.body)
            || !payload_matches_exact_evidence(&page.body, request.object().evidence())
            || request
                .object()
                .expected_bytes()
                .is_some_and(|expected| u64::try_from(page.body.len()).ok() != Some(expected))
        {
            return Err(SourceError::GenerationResynchronizationRequired.into());
        }
        if page.parsed.bars.len()
            > usize::try_from(request.max_records())
                .map_err(|_| SourceError::InvalidProtocolState)?
        {
            return Err(
                market_squawk_sources::ExtractionError::RecordLimitExceeded {
                    requested: request.max_records(),
                }
                .into(),
            );
        }
        let schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(page.parsed.bars.len())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        for bar in page.parsed.bars {
            let normalized = normalize_bar(dataset, bar).map_err(map_adapter_error)?;
            let payload = serde_json::to_vec(&normalized)
                .map(Bytes::from)
                .map_err(|_| SourceError::InvalidProtocolState)?;
            let evidence = exact_evidence(&payload);
            let revision =
                record_revision(normalized.effective_at, &evidence).map_err(map_adapter_error)?;
            records.push(ExtractionRecord::try_new(
                &request,
                schema.clone(),
                evidence,
                normalized.effective_at,
                None,
                AvailabilityEvidence::LocalFirstObserved {
                    observed_at: page.received_at,
                },
                revision,
                None,
                payload,
            )?);
        }
        authority.validate_current()?;
        ExtractionBatch::try_new(&request, records).map_err(Into::into)
    }

    fn validate_authority(
        &self,
        authority: &ExtractionAuthority,
    ) -> Result<(), ExtractionSourceError> {
        authority.validate_current()?;
        if authority.metadata() != self.config.metadata() {
            return Err(SourceError::InvalidProtocolState.into());
        }
        Ok(())
    }

    async fn fetch_page(
        &self,
        authority: &ExtractionAuthority,
        dataset: &AlpacaHistoricalEquityDataset,
        page_token: Option<&str>,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<FetchedPage, ExtractionSourceError> {
        self.validate_authority(authority)?;
        let target = request_url(dataset, page_token).map_err(map_adapter_error)?;
        let permit =
            acquire_request_permit(authority, target.as_str(), deadline, cancellation.clone())
                .await?;
        let bounds = permit.request_bounds()?;
        let in_flight = permit.authorize_send(target.as_str())?;
        let now = system_timestamp().map_err(map_adapter_error)?;
        let wall_remaining = deadline
            .unix_nanos()
            .checked_sub(now.unix_nanos())
            .and_then(|nanos| u64::try_from(nanos).ok())
            .map(Duration::from_nanos)
            .ok_or(ExtractionSourceError::DeadlineExceeded)?;
        let timeout = Duration::from_nanos(bounds.total_timeout_nanos()).min(wall_remaining);
        let operation = self.client.get(target).headers(self.auth_headers()?).send();
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ExtractionSourceError::Cancelled),
            result = tokio::time::timeout(timeout, operation) => match result {
                Ok(Ok(response)) => response,
                Ok(Err(_)) => return Err(SourceError::Network.into()),
                Err(_) => return Err(ExtractionSourceError::DeadlineExceeded),
            }
        };
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .map(|value| value.as_bytes().to_vec());
        let rate_evidence = parse_rate_evidence(response.headers()).map_err(map_adapter_error)?;
        if matches!(status, 401 | 403) {
            return Err(SourceError::Unauthorized.into());
        }
        if matches!(status, 429 | 503) {
            let deadline = in_flight.apply_retry_after_header(retry_after.as_deref(), 1_000)?;
            return Err(SourceError::BudgetWaitUntil { deadline }.into());
        }
        if status != 200 {
            return Err(SourceError::Network.into());
        }
        if response
            .headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .is_some_and(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        if let Some(length) = response.content_length() {
            in_flight.validate_response_size(length)?;
        }
        let max_bytes = usize::try_from(bounds.max_response_bytes())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let read_timeout = Duration::from_nanos(bounds.read_timeout_nanos());
        let mut body = BytesMut::new();
        let mut stream = response.bytes_stream();
        loop {
            in_flight.validate_current()?;
            let next = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(ExtractionSourceError::Cancelled),
                result = tokio::time::timeout(read_timeout, stream.next()) => match result {
                    Ok(next) => next,
                    Err(_) => return Err(ExtractionSourceError::DeadlineExceeded),
                }
            };
            let Some(chunk) = next else { break };
            let chunk = chunk.map_err(|_| SourceError::Network)?;
            let next_len = body
                .len()
                .checked_add(chunk.len())
                .ok_or(SourceError::FrameTooLarge { max: max_bytes })?;
            if next_len > max_bytes {
                return Err(SourceError::FrameTooLarge { max: max_bytes }.into());
            }
            in_flight.validate_response_size(
                u64::try_from(next_len).map_err(|_| SourceError::InvalidProtocolState)?,
            )?;
            body.extend_from_slice(&chunk);
        }
        drop(stream);
        in_flight.validate_current()?;
        in_flight.release();
        let body = body.freeze();
        let parsed = serde_json::from_slice::<BarPage>(&body)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        *self
            .rate_evidence
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)? = Some(rate_evidence);
        Ok(FetchedPage {
            body,
            parsed,
            received_at: system_timestamp().map_err(map_adapter_error)?,
        })
    }

    fn auth_headers(&self) -> Result<reqwest::header::HeaderMap, ExtractionSourceError> {
        let mut headers = reqwest::header::HeaderMap::new();
        let mut key = reqwest::header::HeaderValue::from_str(self.credentials.key_id())
            .map_err(|_| SourceError::Unauthorized)?;
        key.set_sensitive(true);
        let mut secret = reqwest::header::HeaderValue::from_str(self.credentials.secret_key())
            .map_err(|_| SourceError::Unauthorized)?;
        secret.set_sensitive(true);
        headers.insert("apca-api-key-id", key);
        headers.insert("apca-api-secret-key", secret);
        headers.insert(
            reqwest::header::ACCEPT_ENCODING,
            reqwest::header::HeaderValue::from_static("identity"),
        );
        Ok(headers)
    }
}

impl SourceMetadataProvider for AlpacaHistoricalEquitySource {
    fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

impl ExtractionSource for AlpacaHistoricalEquitySource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        Box::pin(self.discover_impl(authority, request, cancellation))
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        Box::pin(self.extract_impl(authority, request, cancellation))
    }
}

struct FetchedPage {
    body: Bytes,
    parsed: BarPage,
    received_at: Timestamp,
}

#[derive(Deserialize)]
struct BarPage {
    bars: Vec<BarWire>,
    symbol: String,
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct BarWire {
    #[serde(rename = "t")]
    timestamp: String,
    #[serde(rename = "o")]
    open: Number,
    #[serde(rename = "h")]
    high: Number,
    #[serde(rename = "l")]
    low: Number,
    #[serde(rename = "c")]
    close: Number,
    #[serde(rename = "v")]
    volume: Number,
    #[serde(rename = "n", default)]
    trade_count: Option<Number>,
    #[serde(rename = "vw", default)]
    vwap: Option<Number>,
}

#[derive(Serialize)]
struct CanonicalBar {
    schema_version: u16,
    observation_type: &'static str,
    provider: &'static str,
    feed: &'static str,
    symbol: String,
    instrument_id: market_squawk_domain::InstrumentId,
    timeframe: String,
    adjustment: &'static str,
    effective_at: Timestamp,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    trade_count: Option<String>,
    vwap: Option<String>,
}

fn normalize_bar(
    dataset: &AlpacaHistoricalEquityDataset,
    bar: BarWire,
) -> Result<CanonicalBar, AlpacaError> {
    let effective_at = parse_timestamp(&bar.timestamp)?;
    if effective_at < dataset.start() || effective_at > dataset.end() {
        return Err(AlpacaError::Protocol);
    }
    let (open, open_value) = decimal(&bar.open, false)?;
    let (high, high_value) = decimal(&bar.high, false)?;
    let (low, low_value) = decimal(&bar.low, false)?;
    let (close, close_value) = decimal(&bar.close, false)?;
    let (volume, _) = decimal(&bar.volume, true)?;
    if low_value > high_value
        || open_value < low_value
        || open_value > high_value
        || close_value < low_value
        || close_value > high_value
    {
        return Err(AlpacaError::Protocol);
    }
    let trade_count = bar
        .trade_count
        .map(|value| unsigned_integer(&value))
        .transpose()?;
    let vwap = bar
        .vwap
        .map(|value| decimal(&value, false).map(|(lexeme, _)| lexeme))
        .transpose()?;
    Ok(CanonicalBar {
        schema_version: 1,
        observation_type: "market_bar",
        provider: "alpaca",
        feed: "iex",
        symbol: dataset.mapping().symbol().to_owned(),
        instrument_id: dataset.mapping().instrument(),
        timeframe: dataset.timeframe().provider_value(),
        adjustment: dataset.adjustment().as_str(),
        effective_at,
        open,
        high,
        low,
        close,
        volume,
        trade_count,
        vwap,
    })
}

fn validate_page(
    dataset: &AlpacaHistoricalEquityDataset,
    page: &BarPage,
) -> Result<(), AlpacaError> {
    if page.symbol != dataset.mapping().symbol()
        || page.bars.len() > usize::from(dataset.page_limit())
    {
        return Err(AlpacaError::Protocol);
    }
    let mut previous = None;
    for bar in &page.bars {
        let timestamp = parse_timestamp(&bar.timestamp)?;
        if timestamp < dataset.start()
            || timestamp > dataset.end()
            || previous.is_some_and(|prior| timestamp <= prior)
        {
            return Err(AlpacaError::Protocol);
        }
        previous = Some(timestamp);
    }
    if let Some(token) = &page.next_page_token {
        validate_page_token(token)?;
    }
    Ok(())
}

fn request_url(
    dataset: &AlpacaHistoricalEquityDataset,
    page_token: Option<&str>,
) -> Result<url::Url, AlpacaError> {
    if let Some(token) = page_token {
        validate_page_token(token)?;
    }
    let mut url = url::Url::parse(ALPACA_STOCKS_BASE_ENDPOINT)
        .map_err(|_| AlpacaError::InvalidHistoricalPlan)?;
    url.path_segments_mut()
        .map_err(|_| AlpacaError::InvalidHistoricalPlan)?
        .push(dataset.mapping().symbol())
        .push("bars");
    let start = timestamp_text(dataset.start())?;
    let end = timestamp_text(dataset.end())?;
    url.query_pairs_mut()
        .append_pair("timeframe", &dataset.timeframe().provider_value())
        .append_pair("start", &start)
        .append_pair("end", &end)
        .append_pair("limit", &dataset.page_limit().to_string())
        .append_pair("adjustment", dataset.adjustment().as_str())
        .append_pair("feed", "iex")
        .append_pair("sort", "asc");
    if let Some(token) = page_token {
        url.query_pairs_mut().append_pair("page_token", token);
    }
    Ok(url)
}

async fn acquire_request_permit(
    authority: &ExtractionAuthority,
    target: &str,
    wall_deadline: Timestamp,
    cancellation: CancellationToken,
) -> Result<ExtractionRequestPermit, ExtractionSourceError> {
    loop {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        match authority.try_network_request(target) {
            Ok(permit) => return Ok(permit),
            Err(ExtractionAuthorityError::BudgetWaitUntil { deadline }) => {
                let wait = authority.remaining_budget_wait(deadline)?;
                let remaining = wall_deadline
                    .unix_nanos()
                    .checked_sub(system_timestamp().map_err(map_adapter_error)?.unix_nanos())
                    .and_then(|nanos| u64::try_from(nanos).ok())
                    .map(Duration::from_nanos)
                    .ok_or(ExtractionSourceError::DeadlineExceeded)?;
                if wait > remaining {
                    return Err(ExtractionSourceError::DeadlineExceeded);
                }
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Err(ExtractionSourceError::Cancelled),
                    () = tokio::time::sleep(wait) => {}
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn enforce_historical_window(dataset: &AlpacaHistoricalEquityDataset) -> Result<(), AlpacaError> {
    let cutoff = system_timestamp()?
        .checked_sub_nanos(
            i64::try_from(ALPACA_HISTORICAL_EXCLUSION_NANOS)
                .map_err(|_| AlpacaError::InvalidHistoricalPlan)?,
        )
        .map_err(|_| AlpacaError::InvalidHistoricalPlan)?;
    if dataset.end() > cutoff {
        return Err(AlpacaError::InvalidHistoricalPlan);
    }
    Ok(())
}

fn parse_rate_evidence(
    headers: &reqwest::header::HeaderMap,
) -> Result<AlpacaRateLimitEvidence, AlpacaError> {
    Ok(AlpacaRateLimitEvidence {
        limit: optional_header_integer(headers, "x-ratelimit-limit")?,
        remaining: optional_header_integer(headers, "x-ratelimit-remaining")?,
        reset_unix_seconds: optional_header_integer(headers, "x-ratelimit-reset")?,
    })
}

fn optional_header_integer<T>(
    headers: &reqwest::header::HeaderMap,
    name: &'static str,
) -> Result<Option<T>, AlpacaError>
where
    T: std::str::FromStr,
{
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .ok()
                .and_then(|value| value.parse().ok())
                .ok_or(AlpacaError::Protocol)
        })
        .transpose()
}

struct PageObjectIdentity {
    page_token: Option<String>,
    digest: ExactPayloadEvidence,
}

fn page_object_id(
    page_index: u16,
    token: Option<&str>,
    evidence: &ExactPayloadEvidence,
) -> Result<SourceIdentifier, AlpacaError> {
    let token = token.map_or_else(|| "-".to_owned(), |value| URL_SAFE_NO_PAD.encode(value));
    let digest = hex(evidence.content_digest().bytes());
    Ok(SourceIdentifier::try_from(format!(
        "alpaca-iex-bars:{page_index}:{token}:{digest}"
    ))?)
}

fn parse_page_object_id(value: &SourceIdentifier) -> Result<PageObjectIdentity, AlpacaError> {
    let mut fields = value.as_str().split(':');
    if fields.next() != Some("alpaca-iex-bars") {
        return Err(AlpacaError::Protocol);
    }
    let _page_index = fields
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or(AlpacaError::Protocol)?;
    let encoded_token = fields.next().ok_or(AlpacaError::Protocol)?;
    let digest = fields.next().ok_or(AlpacaError::Protocol)?;
    if fields.next().is_some()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(AlpacaError::Protocol);
    }
    let page_token = if encoded_token == "-" {
        None
    } else {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded_token)
            .map_err(|_| AlpacaError::Protocol)?;
        let token = String::from_utf8(bytes).map_err(|_| AlpacaError::Protocol)?;
        validate_page_token(&token)?;
        Some(token)
    };
    let digest_bytes = decode_hex(digest)?;
    Ok(PageObjectIdentity {
        page_token,
        digest: ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest_bytes,
        )),
    })
}

fn record_revision(
    timestamp: Timestamp,
    evidence: &ExactPayloadEvidence,
) -> Result<SourceIdentifier, AlpacaError> {
    let digest = hex(evidence.content_digest().bytes());
    SourceIdentifier::try_from(format!(
        "alpaca-iex-bar:{}:{}",
        timestamp.unix_nanos(),
        &digest[..16]
    ))
    .map_err(Into::into)
}

fn validate_page_token(value: &str) -> Result<(), AlpacaError> {
    if value.is_empty()
        || value.len() > MAX_PAGE_TOKEN_BYTES
        || !value.is_ascii()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(AlpacaError::Protocol);
    }
    Ok(())
}

fn exact_evidence(payload: &[u8]) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(payload).into(),
    ))
}

fn decimal(value: &Number, allow_zero: bool) -> Result<(String, Decimal), AlpacaError> {
    let lexeme = value.to_string();
    let decimal = Decimal::from_str_exact(&lexeme).map_err(|_| AlpacaError::Protocol)?;
    if decimal.is_sign_negative() || (!allow_zero && decimal.is_zero()) {
        return Err(AlpacaError::Protocol);
    }
    Ok((lexeme, decimal))
}

fn unsigned_integer(value: &Number) -> Result<String, AlpacaError> {
    value
        .as_u64()
        .map(|_| value.to_string())
        .ok_or(AlpacaError::Protocol)
}

fn parse_timestamp(value: &str) -> Result<Timestamp, AlpacaError> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|timestamp| timestamp.timestamp_nanos_opt())
        .map(Timestamp::from_unix_nanos)
        .ok_or(AlpacaError::Protocol)
}

fn timestamp_text(value: Timestamp) -> Result<String, AlpacaError> {
    Ok(DateTime::<Utc>::from_timestamp_nanos(value.unix_nanos())
        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
}

fn system_timestamp() -> Result<Timestamp, AlpacaError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AlpacaError::Network)?
        .as_nanos();
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(nanos).map_err(|_| AlpacaError::Network)?,
    ))
}

fn hex(bytes: [u8; 32]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(ALPHABET[usize::from(byte >> 4)]));
        output.push(char::from(ALPHABET[usize::from(byte & 0x0f)]));
    }
    output
}

fn decode_hex(value: &str) -> Result<[u8; 32], AlpacaError> {
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_value(pair[0]).ok_or(AlpacaError::Protocol)?;
        let low = hex_value(pair[1]).ok_or(AlpacaError::Protocol)?;
        bytes[index] = (high << 4) | low;
    }
    Ok(bytes)
}

const fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn map_adapter_error(error: AlpacaError) -> ExtractionSourceError {
    match error {
        AlpacaError::DeadlineExceeded => ExtractionSourceError::DeadlineExceeded,
        AlpacaError::Cancelled => ExtractionSourceError::Cancelled,
        AlpacaError::InvalidCredentials | AlpacaError::InvalidAuthorization => {
            SourceError::Unauthorized.into()
        }
        AlpacaError::Network => SourceError::Network.into(),
        AlpacaError::BodyTooLarge => SourceError::FrameTooLarge {
            max: market_squawk_sources::MAX_RAW_FRAME_BYTES,
        }
        .into(),
        _ => SourceError::InvalidProtocolState.into(),
    }
}
