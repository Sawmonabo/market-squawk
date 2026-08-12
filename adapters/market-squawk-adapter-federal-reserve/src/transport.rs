//! Bounded official-HTTPS transport and exact response receipts.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt as _};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_sources::{
    ExtractionAuthority, ExtractionSourceError, NetworkAccessPolicy, ProviderCaptureError,
    ProviderCapturePageReceipt, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    SourceError, SourceMetadata,
};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE,
    IF_NONE_MATCH, LAST_MODIFIED, RETRY_AFTER, USER_AGENT,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::source::{BoardDatasetProfile, BoardSourceError};
use crate::{BoardAdapterError, BoardFileFormat, ParsedBoardDataset};

const USER_AGENT_VALUE: &str = "market-squawk/1.0 federal-reserve-board-adapter";
const MAX_VALIDATOR_BYTES: usize = 8 * 1024;

/// Opaque HTTP validators retained exactly and admitted only after bounded syntax checks.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct BoardHttpValidators {
    etag: Option<Box<[u8]>>,
    last_modified: Option<Box<str>>,
}

impl std::fmt::Debug for BoardHttpValidators {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoardHttpValidators")
            .field("etag_bytes", &self.etag.as_deref().map(<[u8]>::len))
            .field("has_last_modified", &self.last_modified.is_some())
            .finish()
    }
}

impl BoardHttpValidators {
    /// Constructs optional exact validators. An empty pair is valid for a successful response.
    pub fn try_new(
        etag: Option<Vec<u8>>,
        last_modified: Option<String>,
    ) -> Result<Self, BoardSourceError> {
        let etag = etag.map(Into::into);
        let last_modified = last_modified.map(Into::into);
        if etag.as_deref().is_some_and(|value| !valid_etag(value))
            || last_modified
                .as_deref()
                .is_some_and(|value| !valid_http_date(value))
        {
            return Err(BoardSourceError::InvalidValidator);
        }
        Ok(Self {
            etag,
            last_modified,
        })
    }

    /// Returns the exact ETag bytes.
    pub fn etag(&self) -> Option<&[u8]> {
        self.etag.as_deref()
    }

    /// Returns the exact HTTP-date validator.
    pub fn last_modified(&self) -> Option<&str> {
        self.last_modified.as_deref()
    }

    /// Returns whether either validator is present.
    pub const fn is_present(&self) -> bool {
        self.etag.is_some() || self.last_modified.is_some()
    }
}

/// Conditional request bound to the exact previously retained representation digest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardConditionalRequest {
    validators: BoardHttpValidators,
    prior_payload_digest: [u8; 32],
}

impl BoardConditionalRequest {
    /// Builds a conditional request only when at least one valid server validator exists.
    pub fn try_new(
        validators: BoardHttpValidators,
        prior_payload_digest: [u8; 32],
    ) -> Result<Self, BoardSourceError> {
        if !validators.is_present() || prior_payload_digest.iter().all(|byte| *byte == 0) {
            return Err(BoardSourceError::InvalidValidator);
        }
        Ok(Self {
            validators,
            prior_payload_digest,
        })
    }

    /// Returns exact validators sent to the Board route.
    pub const fn validators(&self) -> &BoardHttpValidators {
        &self.validators
    }

    /// Returns the locally retained representation identity a `304` may reuse.
    pub const fn prior_payload_digest(&self) -> [u8; 32] {
        self.prior_payload_digest
    }
}

/// Complete exact-response receipt for one modified Board file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardHttpReceipt {
    contract_digest: [u8; 32],
    contract_request_digest: [u8; 32],
    request_digest: [u8; 32],
    status: u16,
    request_started_at: Timestamp,
    received_at: Timestamp,
    latency_nanos: u64,
    declared_body_bytes: Option<u64>,
    body_bytes: u64,
    body_digest: [u8; 32],
    content_type: Box<str>,
    validators: BoardHttpValidators,
    conditional: Option<BoardConditionalRequest>,
}

impl BoardHttpReceipt {
    /// Returns the exact dataset contract identity.
    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }
    /// Returns the code-owned non-conditional file-request identity.
    pub const fn contract_request_digest(&self) -> [u8; 32] {
        self.contract_request_digest
    }
    /// Returns the exact HTTP request identity, including prior validators when conditional.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
    /// Returns HTTP 200.
    pub const fn status(&self) -> u16 {
        self.status
    }
    /// Returns when the local request began.
    pub const fn request_started_at(&self) -> Timestamp {
        self.request_started_at
    }
    /// Returns when the complete exact body was observed.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    /// Returns measured request latency.
    pub const fn latency_nanos(&self) -> u64 {
        self.latency_nanos
    }
    /// Returns the provider-declared body length when supplied.
    pub const fn declared_body_bytes(&self) -> Option<u64> {
        self.declared_body_bytes
    }
    /// Returns exact observed body bytes.
    pub const fn body_bytes(&self) -> u64 {
        self.body_bytes
    }
    /// Returns SHA-256 of exact response bytes.
    pub const fn body_digest(&self) -> [u8; 32] {
        self.body_digest
    }
    /// Returns the admitted response media type without parameters.
    pub fn content_type(&self) -> &str {
        &self.content_type
    }
    /// Returns response validators that may be used for a later conditional request.
    pub const fn validators(&self) -> &BoardHttpValidators {
        &self.validators
    }
    /// Returns prior validators when this was a conditional request whose file changed.
    pub const fn conditional(&self) -> Option<&BoardConditionalRequest> {
        self.conditional.as_ref()
    }

    /// Builds the shared one-page capture receipt only when its smaller hard ceiling admits this
    /// file. Larger Board release files retain this complete adapter receipt instead.
    pub fn try_shared_capture_receipt(
        &self,
        metadata: &SourceMetadata,
        dataset: SourceIdentifier,
    ) -> Result<Option<ProviderCaptureSetReceipt>, ProviderCaptureError> {
        if self.body_bytes > market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGE_BYTES {
            return Ok(None);
        }
        let page = ProviderCapturePageReceipt::try_new(
            0,
            evidence(self.request_digest),
            None,
            None,
            self.status,
            self.body_bytes,
            evidence(self.body_digest),
            self.received_at,
        )?;
        ProviderCaptureSetReceipt::try_new(
            metadata.source_id().clone(),
            metadata.revision().clone(),
            dataset,
            evidence(self.request_digest),
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![page],
        )
        .map(Some)
    }
}

/// Exact `304 Not Modified` receipt. It carries no fabricated body or publication event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardNotModifiedReceipt {
    contract_digest: [u8; 32],
    contract_request_digest: [u8; 32],
    request_digest: [u8; 32],
    request_started_at: Timestamp,
    received_at: Timestamp,
    latency_nanos: u64,
    conditional: BoardConditionalRequest,
    response_validators: BoardHttpValidators,
}

impl BoardNotModifiedReceipt {
    /// Returns the exact contract identity.
    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }
    /// Returns the code-owned non-conditional file-request identity.
    pub const fn contract_request_digest(&self) -> [u8; 32] {
        self.contract_request_digest
    }
    /// Returns the exact conditional HTTP request identity.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
    /// Returns the prior representation proven unchanged by the server response.
    pub const fn prior_payload_digest(&self) -> [u8; 32] {
        self.conditional.prior_payload_digest
    }
    /// Returns the conditional validators sent.
    pub const fn conditional(&self) -> &BoardConditionalRequest {
        &self.conditional
    }
    /// Returns validators supplied on the `304`, without overwriting absent validators.
    pub const fn response_validators(&self) -> &BoardHttpValidators {
        &self.response_validators
    }
    /// Returns request start time.
    pub const fn request_started_at(&self) -> Timestamp {
        self.request_started_at
    }
    /// Returns complete-response observation time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    /// Returns measured latency.
    pub const fn latency_nanos(&self) -> u64 {
        self.latency_nanos
    }
}

/// Parsed exact file plus its complete HTTP receipt.
#[derive(Debug)]
pub struct BoardRetrievedFile {
    bytes: Bytes,
    parsed: ParsedBoardDataset,
    receipt: BoardHttpReceipt,
}

impl BoardRetrievedFile {
    /// Returns exact provider bytes for raw persistence.
    pub const fn exact_bytes(&self) -> &Bytes {
        &self.bytes
    }
    /// Returns the strictly parsed dataset.
    pub const fn parsed(&self) -> &ParsedBoardDataset {
        &self.parsed
    }
    /// Returns exact transport evidence.
    pub const fn receipt(&self) -> &BoardHttpReceipt {
        &self.receipt
    }

    pub(crate) fn into_parts(self) -> (Bytes, ParsedBoardDataset, BoardHttpReceipt) {
        (self.bytes, self.parsed, self.receipt)
    }
}

/// Conditional or modified file retrieval outcome.
#[derive(Debug)]
pub enum BoardRetrievalOutcome {
    /// HTTP 200 produced new exact bytes and a validated parsed dataset.
    Modified(Box<BoardRetrievedFile>),
    /// HTTP 304 proved the caller's exact prior digest unchanged.
    NotModified(Box<BoardNotModifiedReceipt>),
}

#[derive(Clone, Debug)]
pub(crate) struct BoardHttpRequest {
    pub(crate) url: String,
    pub(crate) accept: &'static str,
    pub(crate) conditional: Option<BoardConditionalRequest>,
}

#[derive(Clone, Debug)]
pub(crate) struct BoardHttpResponse {
    pub(crate) status: u16,
    pub(crate) retry_after: Option<Vec<u8>>,
    pub(crate) content_encoding: Option<Vec<u8>>,
    pub(crate) content_type: Option<Vec<u8>>,
    pub(crate) etag: Option<Vec<u8>>,
    pub(crate) last_modified: Option<Vec<u8>>,
    pub(crate) declared_body_bytes: Option<u64>,
    pub(crate) body: Bytes,
    pub(crate) received_at: Timestamp,
    pub(crate) latency: Duration,
}

pub(crate) trait BoardTransport: std::fmt::Debug + Send + Sync {
    fn execute(
        &self,
        request: BoardHttpRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<BoardHttpResponse, BoardSourceError>>;
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BoardAttemptTelemetry {
    pub(crate) attempted: bool,
    pub(crate) status: Option<u16>,
    pub(crate) body_bytes: u64,
    pub(crate) body_digest: Option<[u8; 32]>,
    pub(crate) received_at: Timestamp,
    pub(crate) latency_nanos: u64,
    pub(crate) retry_after_present: bool,
}

#[derive(Debug)]
pub(crate) struct BoardFetchFailure {
    pub(crate) error: ExtractionSourceError,
    pub(crate) telemetry: BoardAttemptTelemetry,
}

#[derive(Debug)]
pub(crate) struct BoardHttpClient {
    transport: Arc<dyn BoardTransport>,
    max_response_bytes: usize,
    total_timeout: Duration,
}

impl BoardHttpClient {
    pub(crate) fn try_new(metadata: &SourceMetadata) -> Result<Self, BoardSourceError> {
        let NetworkAccessPolicy::Allowlisted(policy) = metadata.network_policy() else {
            return Err(BoardSourceError::InvalidMetadata);
        };
        let bounds = policy.request_bounds();
        Ok(Self {
            transport: Arc::new(ReqwestBoardTransport::try_new(bounds)?),
            max_response_bytes: usize::try_from(bounds.max_response_bytes())
                .map_err(|_| BoardSourceError::InvalidMetadata)?,
            total_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
        })
    }

    #[cfg(test)]
    pub(crate) fn try_new_with_transport(
        metadata: &SourceMetadata,
        transport: Arc<dyn BoardTransport>,
    ) -> Result<Self, BoardSourceError> {
        let NetworkAccessPolicy::Allowlisted(policy) = metadata.network_policy() else {
            return Err(BoardSourceError::InvalidMetadata);
        };
        let bounds = policy.request_bounds();
        Ok(Self {
            transport,
            max_response_bytes: usize::try_from(bounds.max_response_bytes())
                .map_err(|_| BoardSourceError::InvalidMetadata)?,
            total_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
        })
    }

    pub(crate) async fn fetch(
        &self,
        metadata: &SourceMetadata,
        authority: &ExtractionAuthority,
        profile: &BoardDatasetProfile,
        conditional: Option<&BoardConditionalRequest>,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<BoardRetrievalOutcome, BoardFetchFailure> {
        let started_at = system_timestamp()
            .map_err(|error| failure_without_response(map_source_error(error)))?;
        authority
            .validate_current()
            .map_err(|error| failure_without_response(error.into()))?;
        if authority.metadata() != metadata || !metadata.is_effective_at(started_at) {
            return Err(failure_without_response(
                SourceError::InvalidProtocolState.into(),
            ));
        }
        let timeout = remaining_timeout(deadline, started_at, self.total_timeout)
            .map_err(|error| failure_without_response(map_source_error(error)))?;
        let request = profile.contract().request();
        let request_identity = request_identity(request.request_digest(), conditional);
        let permit = authority
            .try_network_request(request.url())
            .map_err(|error| failure_without_response(error.into()))?;
        let in_flight = permit
            .authorize_send(request.url())
            .map_err(|error| failure_without_response(error.into()))?;
        let maximum = profile
            .parse_limits()
            .max_source_bytes()
            .min(self.max_response_bytes);
        let transport_started = Instant::now();
        let response = match self
            .transport
            .execute(
                BoardHttpRequest {
                    url: request.url().to_owned(),
                    accept: request.accept(),
                    conditional: conditional.cloned(),
                },
                maximum,
                timeout,
                cancellation.clone(),
            )
            .await
        {
            Ok(response) => response,
            Err(error) => {
                in_flight.release();
                return Err(failure_after_execute(
                    map_source_error(error),
                    transport_started.elapsed(),
                ));
            }
        };
        let telemetry = telemetry(&response);
        if response.received_at < started_at {
            in_flight.release();
            return Err(BoardFetchFailure {
                error: SourceError::InvalidProtocolState.into(),
                telemetry,
            });
        }
        if response.status == 429 || response.status == 503 {
            let error = match in_flight.apply_retry_after_header(response.retry_after.as_deref(), 0)
            {
                Ok(deadline) => SourceError::BudgetWaitUntil { deadline }.into(),
                Err(error) => error.into(),
            };
            return Err(BoardFetchFailure { error, telemetry });
        }
        if matches!(response.status, 401 | 403) {
            in_flight.release();
            return Err(BoardFetchFailure {
                error: SourceError::Unauthorized.into(),
                telemetry,
            });
        }
        let observed_bytes = match u64::try_from(response.body.len()) {
            Ok(value) => value,
            Err(_) => {
                in_flight.release();
                return Err(BoardFetchFailure {
                    error: SourceError::InvalidProtocolState.into(),
                    telemetry,
                });
            }
        };
        if in_flight.validate_response_size(observed_bytes).is_err()
            || response
                .declared_body_bytes
                .is_some_and(|value| value != observed_bytes)
            || response
                .content_encoding
                .as_deref()
                .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
        {
            in_flight.release();
            return Err(BoardFetchFailure {
                error: SourceError::InvalidProtocolState.into(),
                telemetry,
            });
        }
        let validators = match validators_from_response(&response) {
            Ok(value) => value,
            Err(_) => {
                in_flight.release();
                return Err(BoardFetchFailure {
                    error: SourceError::InvalidProtocolState.into(),
                    telemetry,
                });
            }
        };
        if response.status == 304 {
            let Some(conditional) = conditional.cloned() else {
                in_flight.release();
                return Err(BoardFetchFailure {
                    error: SourceError::InvalidProtocolState.into(),
                    telemetry,
                });
            };
            if !response.body.is_empty()
                || response.declared_body_bytes.is_some_and(|value| value != 0)
            {
                in_flight.release();
                return Err(BoardFetchFailure {
                    error: SourceError::InvalidProtocolState.into(),
                    telemetry,
                });
            }
            in_flight.release();
            return Ok(BoardRetrievalOutcome::NotModified(Box::new(
                BoardNotModifiedReceipt {
                    contract_digest: request.contract_digest(),
                    contract_request_digest: request.request_digest(),
                    request_digest: request_identity,
                    request_started_at: started_at,
                    received_at: response.received_at,
                    latency_nanos: duration_nanos(response.latency),
                    conditional,
                    response_validators: validators,
                },
            )));
        }
        if response.status != 200 {
            in_flight.release();
            return Err(BoardFetchFailure {
                error: SourceError::ProviderUnavailable.into(),
                telemetry,
            });
        }
        let content_type = match admitted_content_type(
            response.content_type.as_deref(),
            profile.contract().format(),
        ) {
            Some(value) if !response.body.is_empty() => value,
            _ => {
                in_flight.release();
                return Err(BoardFetchFailure {
                    error: SourceError::InvalidProtocolState.into(),
                    telemetry,
                });
            }
        };
        if let Err(error) = validate_parse_continuation(deadline, cancellation) {
            in_flight.release();
            return Err(BoardFetchFailure { error, telemetry });
        }
        let parsed = match profile.parse(&response.body) {
            Ok(value) => value,
            Err(_) => {
                in_flight.release();
                return Err(BoardFetchFailure {
                    error: SourceError::InvalidProtocolState.into(),
                    telemetry,
                });
            }
        };
        if let Err(error) = validate_parse_continuation(deadline, cancellation) {
            in_flight.release();
            return Err(BoardFetchFailure { error, telemetry });
        }
        let body_digest = parsed.source_payload_digest();
        if body_digest != sha256(&response.body) {
            in_flight.release();
            return Err(BoardFetchFailure {
                error: SourceError::InvalidProtocolState.into(),
                telemetry,
            });
        }
        in_flight.release();
        Ok(BoardRetrievalOutcome::Modified(Box::new(
            BoardRetrievedFile {
                bytes: response.body,
                parsed,
                receipt: BoardHttpReceipt {
                    contract_digest: request.contract_digest(),
                    contract_request_digest: request.request_digest(),
                    request_digest: request_identity,
                    status: 200,
                    request_started_at: started_at,
                    received_at: response.received_at,
                    latency_nanos: duration_nanos(response.latency),
                    declared_body_bytes: response.declared_body_bytes,
                    body_bytes: observed_bytes,
                    body_digest,
                    content_type: content_type.into(),
                    validators,
                    conditional: conditional.cloned(),
                },
            },
        )))
    }
}

#[derive(Debug)]
struct ReqwestBoardTransport {
    client: reqwest::Client,
}

impl ReqwestBoardTransport {
    fn try_new(bounds: market_squawk_sources::HttpRequestBounds) -> Result<Self, BoardSourceError> {
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
            .map_err(|_| BoardSourceError::InvalidMetadata)?;
        Ok(Self { client })
    }
}

impl BoardTransport for ReqwestBoardTransport {
    fn execute(
        &self,
        request: BoardHttpRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<BoardHttpResponse, BoardSourceError>> {
        Box::pin(async move {
            let operation = async {
                let started = Instant::now();
                let mut builder = self
                    .client
                    .get(request.url)
                    .header(ACCEPT, request.accept)
                    .header(ACCEPT_ENCODING, "identity")
                    .header(USER_AGENT, USER_AGENT_VALUE);
                if let Some(conditional) = request.conditional {
                    if let Some(etag) = conditional.validators.etag() {
                        builder = builder.header(
                            IF_NONE_MATCH,
                            reqwest::header::HeaderValue::from_bytes(etag)
                                .map_err(|_| BoardSourceError::InvalidValidator)?,
                        );
                    }
                    if let Some(modified) = conditional.validators.last_modified() {
                        builder = builder.header(IF_MODIFIED_SINCE, modified);
                    }
                }
                let response = builder
                    .send()
                    .await
                    .map_err(|_| BoardSourceError::Network)?;
                if response.content_length().is_some_and(|length| {
                    usize::try_from(length).map_or(true, |length| length > max_bytes)
                }) {
                    return Err(BoardSourceError::BodyTooLarge);
                }
                let status = response.status().as_u16();
                let headers = response.headers();
                let retry_after = header_bytes(headers, RETRY_AFTER);
                let content_encoding = header_bytes(headers, CONTENT_ENCODING);
                let content_type = header_bytes(headers, CONTENT_TYPE);
                let etag = header_bytes(headers, ETAG);
                let last_modified = header_bytes(headers, LAST_MODIFIED);
                let declared_body_bytes = response.content_length();
                let body = collect_bounded_stream(response.bytes_stream(), max_bytes).await?;
                Ok(BoardHttpResponse {
                    status,
                    retry_after,
                    content_encoding,
                    content_type,
                    etag,
                    last_modified,
                    declared_body_bytes,
                    body,
                    received_at: system_timestamp()?,
                    latency: started.elapsed(),
                })
            };
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(BoardSourceError::Cancelled),
                result = tokio::time::timeout(timeout, operation) => result.map_err(|_| BoardSourceError::DeadlineExceeded)?,
            }
        })
    }
}

fn header_bytes(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
) -> Option<Vec<u8>> {
    headers.get(name).map(|value| value.as_bytes().to_vec())
}

async fn collect_bounded_stream<S, E>(
    mut stream: S,
    max_bytes: usize,
) -> Result<Bytes, BoardSourceError>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut body = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BoardSourceError::Network)?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or(BoardSourceError::BodyTooLarge)?;
        if next > max_bytes {
            return Err(BoardSourceError::BodyTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

fn validators_from_response(
    response: &BoardHttpResponse,
) -> Result<BoardHttpValidators, BoardSourceError> {
    let last_modified = response
        .last_modified
        .as_deref()
        .map(|value| {
            std::str::from_utf8(value)
                .map(str::to_owned)
                .map_err(|_| BoardSourceError::InvalidValidator)
        })
        .transpose()?;
    BoardHttpValidators::try_new(response.etag.clone(), last_modified)
}

fn valid_etag(value: &[u8]) -> bool {
    if value.len() < 2 || value.len() > MAX_VALIDATOR_BYTES {
        return false;
    }
    let opaque = if value.starts_with(b"W/\"") && value.ends_with(b"\"") {
        &value[3..value.len() - 1]
    } else if value.starts_with(b"\"") && value.ends_with(b"\"") {
        &value[1..value.len() - 1]
    } else {
        return false;
    };
    opaque
        .iter()
        .all(|byte| *byte == 0x21 || (0x23..=0x7e).contains(byte) || *byte >= 0x80)
}

fn valid_http_date(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_VALIDATOR_BYTES
        && value.is_ascii()
        && httpdate::parse_http_date(value).is_ok()
}

fn admitted_content_type(value: Option<&[u8]>, format: BoardFileFormat) -> Option<String> {
    let media = std::str::from_utf8(value?)
        .ok()?
        .split(';')
        .next()?
        .trim()
        .to_ascii_lowercase();
    let allowed = match format {
        BoardFileFormat::DdpCsvSeriesColumnV1 => matches!(
            media.as_str(),
            "text/csv" | "application/csv" | "application/octet-stream"
        ),
        BoardFileFormat::SdmxCompactXmlV1 => matches!(
            media.as_str(),
            "application/xml" | "text/xml" | "application/octet-stream"
        ),
        BoardFileFormat::SdmxCompactZipV1 => matches!(
            media.as_str(),
            "application/zip" | "application/x-zip-compressed" | "application/octet-stream"
        ),
    };
    allowed.then_some(media)
}

fn telemetry(response: &BoardHttpResponse) -> BoardAttemptTelemetry {
    BoardAttemptTelemetry {
        attempted: true,
        status: Some(response.status),
        body_bytes: response.body.len() as u64,
        body_digest: Some(sha256(&response.body)),
        received_at: response.received_at,
        latency_nanos: duration_nanos(response.latency),
        retry_after_present: response.retry_after.is_some(),
    }
}

fn failure_without_response(error: ExtractionSourceError) -> BoardFetchFailure {
    BoardFetchFailure {
        error,
        telemetry: BoardAttemptTelemetry {
            attempted: false,
            status: None,
            body_bytes: 0,
            body_digest: None,
            received_at: Timestamp::from_unix_nanos(0),
            latency_nanos: 0,
            retry_after_present: false,
        },
    }
}

fn failure_after_execute(error: ExtractionSourceError, latency: Duration) -> BoardFetchFailure {
    let mut failure = failure_without_response(error);
    failure.telemetry.attempted = true;
    failure.telemetry.latency_nanos = duration_nanos(latency);
    failure
}

fn map_source_error(error: BoardSourceError) -> ExtractionSourceError {
    match error {
        BoardSourceError::Cancelled => ExtractionSourceError::Cancelled,
        BoardSourceError::DeadlineExceeded => ExtractionSourceError::DeadlineExceeded,
        BoardSourceError::Network | BoardSourceError::BodyTooLarge => SourceError::Network.into(),
        BoardSourceError::InvalidMetadata
        | BoardSourceError::InvalidProfile
        | BoardSourceError::InvalidValidator
        | BoardSourceError::Protocol(_)
        | BoardSourceError::HealthUnavailable
        | BoardSourceError::CanonicalMapping => SourceError::InvalidProtocolState.into(),
    }
}

fn remaining_timeout(
    deadline: Timestamp,
    now: Timestamp,
    configured: Duration,
) -> Result<Duration, BoardSourceError> {
    let remaining = deadline
        .unix_nanos()
        .checked_sub(now.unix_nanos())
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or(BoardSourceError::DeadlineExceeded)?;
    Ok(configured.min(Duration::from_nanos(remaining)))
}

fn validate_parse_continuation(
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<(), ExtractionSourceError> {
    if cancellation.is_cancelled() {
        return Err(ExtractionSourceError::Cancelled);
    }
    if system_timestamp().map_err(map_source_error)? >= deadline {
        Err(ExtractionSourceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn duration_nanos(value: Duration) -> u64 {
    u64::try_from(value.as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) fn system_timestamp() -> Result<Timestamp, BoardSourceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BoardSourceError::Network)?
        .as_nanos();
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(nanos).map_err(|_| BoardSourceError::Network)?,
    ))
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn request_identity(
    base_request_digest: [u8; 32],
    conditional: Option<&BoardConditionalRequest>,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/federal-reserve-board-http-request/v1");
    hash.update(base_request_digest);
    match conditional {
        Some(conditional) => {
            hash.update([1]);
            hash.update(conditional.prior_payload_digest);
            hash_optional_field(&mut hash, conditional.validators.etag());
            hash_optional_field(
                &mut hash,
                conditional.validators.last_modified().map(str::as_bytes),
            );
        }
        None => hash.update([0]),
    }
    hash.finalize().into()
}

fn hash_optional_field(hash: &mut Sha256, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            hash.update([1]);
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value);
        }
        None => hash.update([0]),
    }
}

fn evidence(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

impl From<BoardAdapterError> for BoardSourceError {
    fn from(error: BoardAdapterError) -> Self {
        Self::Protocol(error)
    }
}
