use std::fmt;
use std::fs::File;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, ETAG, RETRY_AFTER,
    USER_AGENT,
};
use serde::Serialize;
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::catalog::{Catalog, CatalogError, CatalogTransportMetadata, MAX_CATALOG_BYTES};
use crate::decode::{DecodeError, DecodeLimits, DecodeSummary, IexEventSink, PcapStreamDecoder};
use crate::model::TradeDate;
use crate::planning::{ColdJobPlan, PlanError};
use crate::receipt::{
    CaptureError, CaptureResponseMetadata, GzipPcapReceiptBuilder, PcapMaterializationReceipt,
};

/// Exact public IEX HIST catalog surface owned by this adapter.
pub const IEX_HIST_CATALOG_URL: &str = "https://iextrading.com/api/1.0/hist";

const USER_AGENT_VALUE: &str = concat!(
    "market-squawk/",
    env!("CARGO_PKG_VERSION"),
    " iex-hist-cold-adapter"
);
const MAX_HEADER_BYTES: usize = 512;
const STREAM_BUFFER_BYTES: usize = 128 * 1024;
const MAX_RETRY_ATTEMPTS: u8 = 3;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, StreamFailure>> + Send>>;

/// Application-owned retry policy. These values are not provider limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    max_attempts: u8,
    initial_delay: Duration,
    max_delay: Duration,
}

impl RetryPolicy {
    /// Creates a bounded retry policy.
    ///
    /// # Errors
    ///
    /// Rejects zero/excessive attempts, zero delays, or a descending delay ceiling.
    pub fn new(
        max_attempts: u8,
        initial_delay: Duration,
        max_delay: Duration,
    ) -> Result<Self, TransportErrorKind> {
        if !(1..=MAX_RETRY_ATTEMPTS).contains(&max_attempts)
            || initial_delay.is_zero()
            || max_delay < initial_delay
            || max_delay > MAX_RETRY_DELAY
        {
            return Err(TransportErrorKind::InvalidConfiguration);
        }
        Ok(Self {
            max_attempts,
            initial_delay,
            max_delay,
        })
    }

    fn wait_for_attempt(self, attempt: u8) -> Duration {
        let exponent = u32::from(attempt.saturating_sub(1)).min(7);
        self.initial_delay
            .checked_mul(2_u32.saturating_pow(exponent))
            .unwrap_or(self.max_delay)
            .min(self.max_delay)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(5),
        }
    }
}

/// Transport and decoder controls for explicit cold jobs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IexHistTransportConfig {
    retry_policy: RetryPolicy,
    decode_limits: DecodeLimits,
    connect_timeout: Duration,
    read_timeout: Duration,
}

impl IexHistTransportConfig {
    /// Creates the transport configuration.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive per-request timeout values.
    pub fn new(
        retry_policy: RetryPolicy,
        decode_limits: DecodeLimits,
        connect_timeout: Duration,
        read_timeout: Duration,
    ) -> Result<Self, TransportErrorKind> {
        let maximum = Duration::from_secs(120);
        if connect_timeout.is_zero()
            || read_timeout.is_zero()
            || connect_timeout > maximum
            || read_timeout > maximum
        {
            return Err(TransportErrorKind::InvalidConfiguration);
        }
        Ok(Self {
            retry_policy,
            decode_limits,
            connect_timeout,
            read_timeout,
        })
    }
}

impl Default for IexHistTransportConfig {
    fn default() -> Self {
        Self {
            retry_policy: RetryPolicy::default(),
            decode_limits: DecodeLimits::default(),
            connect_timeout: Duration::from_secs(15),
            read_timeout: Duration::from_secs(60),
        }
    }
}

/// One status-driven retry decision retained as runtime telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RetryObservation {
    /// One-based HTTP attempt that received the status.
    pub attempt: u8,
    /// Exact status that caused the retry.
    pub status: u16,
    /// Provider `Retry-After` interpretation, when present.
    pub provider_retry_after_ms: Option<u64>,
    /// Application wait actually applied after capping to policy/deadline.
    pub applied_wait_ms: u64,
}

/// Request and actual-byte telemetry for one explicit catalog or selected-file operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TransportTelemetry {
    attempts_total: u8,
    http_429_total: u8,
    http_503_total: u8,
    network_failures_total: u8,
    response_bytes: u64,
    expanded_pcap_bytes: u64,
    last_status: Option<u16>,
    retries: Vec<RetryObservation>,
}

impl TransportTelemetry {
    fn new() -> Self {
        Self {
            attempts_total: 0,
            http_429_total: 0,
            http_503_total: 0,
            network_failures_total: 0,
            response_bytes: 0,
            expanded_pcap_bytes: 0,
            last_status: None,
            retries: Vec::new(),
        }
    }

    /// Returns actual HTTP attempts, not requested descriptors or observations.
    #[must_use]
    pub const fn attempts_total(&self) -> u8 {
        self.attempts_total
    }

    /// Returns exact HTTP 429 responses.
    #[must_use]
    pub const fn http_429_total(&self) -> u8 {
        self.http_429_total
    }

    /// Returns exact HTTP 503 responses.
    #[must_use]
    pub const fn http_503_total(&self) -> u8 {
        self.http_503_total
    }

    /// Returns exact request or response-stream network failures.
    #[must_use]
    pub const fn network_failures_total(&self) -> u8 {
        self.network_failures_total
    }

    /// Returns the last HTTP status received, if headers were obtained.
    #[must_use]
    pub const fn last_status(&self) -> Option<u16> {
        self.last_status
    }

    /// Returns actual compressed/catalog response bytes read.
    #[must_use]
    pub const fn response_bytes(&self) -> u64 {
        self.response_bytes
    }

    /// Returns exact expanded PCAP bytes.
    #[must_use]
    pub const fn expanded_pcap_bytes(&self) -> u64 {
        self.expanded_pcap_bytes
    }

    /// Returns bounded status-driven retry observations.
    #[must_use]
    pub fn retries(&self) -> &[RetryObservation] {
        &self.retries
    }

    fn begin_attempt(&mut self) -> Result<u8, TransportErrorKind> {
        self.attempts_total = self
            .attempts_total
            .checked_add(1)
            .ok_or(TransportErrorKind::TelemetryOverflow)?;
        Ok(self.attempts_total)
    }

    fn record_status(&mut self, status: u16) -> Result<(), TransportErrorKind> {
        self.last_status = Some(status);
        match status {
            429 => {
                self.http_429_total = self
                    .http_429_total
                    .checked_add(1)
                    .ok_or(TransportErrorKind::TelemetryOverflow)?;
            }
            503 => {
                self.http_503_total = self
                    .http_503_total
                    .checked_add(1)
                    .ok_or(TransportErrorKind::TelemetryOverflow)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn record_network_failure(&mut self) -> Result<(), TransportErrorKind> {
        self.network_failures_total = self
            .network_failures_total
            .checked_add(1)
            .ok_or(TransportErrorKind::TelemetryOverflow)?;
        Ok(())
    }

    fn record_retry(&mut self, observation: RetryObservation) -> Result<(), TransportErrorKind> {
        self.retries
            .try_reserve(1)
            .map_err(|_| TransportErrorKind::Capacity)?;
        self.retries.push(observation);
        Ok(())
    }

    fn add_response_bytes(&mut self, bytes: usize) -> Result<(), TransportErrorKind> {
        self.response_bytes = self
            .response_bytes
            .checked_add(u64::try_from(bytes).map_err(|_| TransportErrorKind::ByteLimit)?)
            .ok_or(TransportErrorKind::ByteLimit)?;
        Ok(())
    }
}

/// Parsed catalog plus exact request/byte telemetry.
#[derive(Debug)]
pub struct CatalogFetch {
    catalog: Catalog,
    exact_body: Bytes,
    telemetry: TransportTelemetry,
}

impl CatalogFetch {
    /// Returns the admitted catalog generation.
    #[must_use]
    pub const fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Returns request and byte telemetry.
    #[must_use]
    pub const fn telemetry(&self) -> &TransportTelemetry {
        &self.telemetry
    }

    /// Returns the exact bounded response body for optional caller-owned raw-evidence sealing.
    #[must_use]
    pub fn exact_body(&self) -> &[u8] {
        &self.exact_body
    }

    /// Consumes the result into the catalog, exact body, and telemetry.
    #[must_use]
    pub fn into_parts(self) -> (Catalog, Bytes, TransportTelemetry) {
        (self.catalog, self.exact_body, self.telemetry)
    }
}

/// Temporary exact compressed and PCAP files owned until the caller seals or drops them.
pub struct StagedCaptureFiles {
    compressed: NamedTempFile,
    pcap: NamedTempFile,
}

impl fmt::Debug for StagedCaptureFiles {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StagedCaptureFiles")
            .field("compressed", &"opaque-temporary-file")
            .field("pcap", &"opaque-temporary-file")
            .finish()
    }
}

impl StagedCaptureFiles {
    /// Reopens the exact compressed artifact without exposing a path.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the temporary artifact can no longer be reopened.
    pub fn reopen_compressed(&self) -> std::io::Result<File> {
        self.compressed.reopen()
    }

    /// Reopens the exact expanded PCAP without exposing a path.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the temporary artifact can no longer be reopened.
    pub fn reopen_pcap(&self) -> std::io::Result<File> {
        self.pcap.reopen()
    }
}

/// Complete selected-file transfer, integrity, decode, and temporary raw-artifact outcome.
#[derive(Debug)]
pub struct DownloadedIexCapture {
    materialization: PcapMaterializationReceipt,
    decode: DecodeSummary,
    telemetry: TransportTelemetry,
    staged_files: StagedCaptureFiles,
}

impl DownloadedIexCapture {
    /// Returns the exact compressed/expanded materialization receipt.
    #[must_use]
    pub const fn materialization(&self) -> &PcapMaterializationReceipt {
        &self.materialization
    }

    /// Returns complete decoder accounting.
    #[must_use]
    pub const fn decode(&self) -> &DecodeSummary {
        &self.decode
    }

    /// Returns request, retry, and actual-byte telemetry.
    #[must_use]
    pub const fn telemetry(&self) -> &TransportTelemetry {
        &self.telemetry
    }

    /// Consumes the outcome and transfers temporary-file ownership to the caller.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        PcapMaterializationReceipt,
        DecodeSummary,
        TransportTelemetry,
        StagedCaptureFiles,
    ) {
        (
            self.materialization,
            self.decode,
            self.telemetry,
            self.staged_files,
        )
    }
}

/// HTTPS-only IEX HIST transport for explicitly selected cold jobs.
#[derive(Debug)]
pub struct IexHistColdTransport {
    client: reqwest::Client,
    config: IexHistTransportConfig,
}

impl IexHistColdTransport {
    /// Constructs an HTTPS-only, no-redirect, no-implicit-decompression client.
    ///
    /// # Errors
    ///
    /// Fails if the hardened HTTP client cannot be built.
    pub fn try_new(config: IexHistTransportConfig) -> Result<Self, TransportErrorKind> {
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
            .connect_timeout(config.connect_timeout)
            .read_timeout(config.read_timeout)
            .build()
            .map_err(|_| TransportErrorKind::InvalidConfiguration)?;
        Ok(Self { client, config })
    }

    /// Fetches one bounded mutable catalog generation. It performs no archive scheduling.
    ///
    /// # Errors
    ///
    /// Fails with retained telemetry on cancellation, deadline, retry exhaustion, transport drift,
    /// byte overflow, or catalog validation failure.
    pub async fn fetch_catalog(
        &self,
        observed_on: TradeDate,
        deadline_unix_nanos: i64,
        cancellation: &CancellationToken,
    ) -> Result<CatalogFetch, IexHistTransportError> {
        let deadline = Deadline::new(deadline_unix_nanos)
            .map_err(|kind| IexHistTransportError::new(kind, TransportTelemetry::new()))?;
        let mut telemetry = TransportTelemetry::new();
        loop {
            let attempt = telemetry
                .begin_attempt()
                .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
            let send = self
                .client
                .get(IEX_HIST_CATALOG_URL)
                .header(ACCEPT, "application/json")
                .header(ACCEPT_ENCODING, "identity")
                .header(USER_AGENT, USER_AGENT_VALUE)
                .send();
            let response = match await_deadline(send, deadline, cancellation).await {
                Ok(Ok(response)) => response,
                Ok(Err(_)) => {
                    telemetry
                        .record_network_failure()
                        .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
                    if attempt >= self.config.retry_policy.max_attempts {
                        return Err(IexHistTransportError::new(
                            TransportErrorKind::Network,
                            telemetry,
                        ));
                    }
                    let wait = self.config.retry_policy.wait_for_attempt(attempt);
                    wait_retry(wait, deadline, cancellation)
                        .await
                        .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
                    continue;
                }
                Err(kind) => return Err(IexHistTransportError::new(kind, telemetry)),
            };
            let status = response.status().as_u16();
            telemetry
                .record_status(status)
                .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
            if matches!(status, 429 | 503) {
                if attempt >= self.config.retry_policy.max_attempts {
                    return Err(IexHistTransportError::new(
                        TransportErrorKind::RetryExhausted { status },
                        telemetry,
                    ));
                }
                let provider_wait = parse_retry_after(response.headers())
                    .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
                let wait = provider_wait
                    .unwrap_or_else(|| self.config.retry_policy.wait_for_attempt(attempt))
                    .min(self.config.retry_policy.max_delay);
                telemetry
                    .record_retry(RetryObservation {
                        attempt,
                        status,
                        provider_retry_after_ms: provider_wait.map(duration_millis),
                        applied_wait_ms: duration_millis(wait),
                    })
                    .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
                wait_retry(wait, deadline, cancellation)
                    .await
                    .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
                continue;
            }
            if status != 200 {
                return Err(IexHistTransportError::new(
                    TransportErrorKind::HttpStatus { status },
                    telemetry,
                ));
            }
            let headers = catalog_headers(&response)
                .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
            let mut stream: ByteStream = Box::pin(
                response
                    .bytes_stream()
                    .map(|item| item.map_err(|_| StreamFailure::Network)),
            );
            let body = collect_catalog_body(&mut stream, deadline, cancellation, &mut telemetry)
                .await
                .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
            let catalog = Catalog::parse(
                &body,
                CatalogTransportMetadata {
                    status,
                    content_type: headers.content_type,
                    content_length: headers.content_length,
                    etag: headers.etag,
                    retrieved_at_unix_nanos: system_unix_nanos()
                        .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?,
                    observed_on,
                },
            )
            .map_err(|error| {
                IexHistTransportError::new(TransportErrorKind::Catalog(error), telemetry.clone())
            })?;
            return Ok(CatalogFetch {
                catalog,
                exact_body: Bytes::from(body),
                telemetry,
            });
        }
    }

    /// Downloads, stages, expands, receipts, and decodes one exact selected file.
    ///
    /// No retry occurs after response-body streaming starts, because partial raw or event output
    /// must never be silently replayed. The caller owns publication staging and must commit only
    /// after this method returns successfully.
    ///
    /// # Errors
    ///
    /// Fails with retained request telemetry on admission, cancellation, deadline, status/retry,
    /// network, disk, gzip, receipt, PCAP, continuity, or sink failure.
    pub async fn download_decode(
        &self,
        plan: &ColdJobPlan,
        staging_directory: &Path,
        cancellation: &CancellationToken,
        sink: &mut dyn IexEventSink,
    ) -> Result<DownloadedIexCapture, IexHistTransportError> {
        let deadline = Deadline::new(plan.deadline_unix_nanos)
            .map_err(|kind| IexHistTransportError::new(kind, TransportTelemetry::new()))?;
        let mut telemetry = TransportTelemetry::new();
        validate_disk(plan, staging_directory, DiskPhase::BeforeTransfer)
            .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
        loop {
            let attempt = telemetry
                .begin_attempt()
                .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
            let send = self
                .client
                .get(&plan.selected_file.download_url)
                .header(ACCEPT, "application/gzip, application/octet-stream")
                .header(ACCEPT_ENCODING, "identity")
                .header(USER_AGENT, USER_AGENT_VALUE)
                .send();
            let response = match await_deadline(send, deadline, cancellation).await {
                Ok(Ok(response)) => response,
                Ok(Err(_)) => {
                    telemetry
                        .record_network_failure()
                        .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
                    if attempt >= self.config.retry_policy.max_attempts {
                        return Err(IexHistTransportError::new(
                            TransportErrorKind::Network,
                            telemetry,
                        ));
                    }
                    let wait = self.config.retry_policy.wait_for_attempt(attempt);
                    wait_retry(wait, deadline, cancellation)
                        .await
                        .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
                    continue;
                }
                Err(kind) => return Err(IexHistTransportError::new(kind, telemetry)),
            };
            let status = response.status().as_u16();
            telemetry
                .record_status(status)
                .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
            if matches!(status, 429 | 503) {
                if attempt >= self.config.retry_policy.max_attempts {
                    return Err(IexHistTransportError::new(
                        TransportErrorKind::RetryExhausted { status },
                        telemetry,
                    ));
                }
                let provider_wait = parse_retry_after(response.headers())
                    .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
                let wait = provider_wait
                    .unwrap_or_else(|| self.config.retry_policy.wait_for_attempt(attempt))
                    .min(self.config.retry_policy.max_delay);
                telemetry
                    .record_retry(RetryObservation {
                        attempt,
                        status,
                        provider_retry_after_ms: provider_wait.map(duration_millis),
                        applied_wait_ms: duration_millis(wait),
                    })
                    .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
                wait_retry(wait, deadline, cancellation)
                    .await
                    .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
                continue;
            }
            if status != 200 {
                return Err(IexHistTransportError::new(
                    TransportErrorKind::HttpStatus { status },
                    telemetry,
                ));
            }
            let metadata = file_response_metadata(plan, &response)
                .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
            let stream: ByteStream = Box::pin(
                response
                    .bytes_stream()
                    .map(|item| item.map_err(|_| StreamFailure::Network)),
            );
            return materialize_selected_stream(
                plan,
                metadata,
                stream,
                staging_directory,
                deadline,
                cancellation,
                self.config.decode_limits,
                sink,
                telemetry,
            )
            .await;
        }
    }
}

#[derive(Debug)]
struct CatalogHeaders {
    content_type: String,
    content_length: u64,
    etag: Option<String>,
}

fn catalog_headers(response: &reqwest::Response) -> Result<CatalogHeaders, TransportErrorKind> {
    let content_type = singleton_header(response.headers(), CONTENT_TYPE)?
        .ok_or(TransportErrorKind::InvalidResponseMetadata)?;
    let content_length = parse_content_length(response.headers())?;
    if content_length == 0 || content_length > u64::try_from(MAX_CATALOG_BYTES).unwrap_or(u64::MAX)
    {
        return Err(TransportErrorKind::ByteLimit);
    }
    Ok(CatalogHeaders {
        content_type,
        content_length,
        etag: singleton_header(response.headers(), ETAG)?,
    })
}

fn file_response_metadata(
    plan: &ColdJobPlan,
    response: &reqwest::Response,
) -> Result<CaptureResponseMetadata, TransportErrorKind> {
    let content_length = parse_content_length(response.headers())?;
    if content_length != plan.advertised_compressed_bytes {
        return Err(TransportErrorKind::InvalidResponseMetadata);
    }
    Ok(CaptureResponseMetadata {
        response_url: response.url().as_str().to_owned(),
        status: response.status().as_u16(),
        content_length,
        content_encoding: singleton_header(response.headers(), CONTENT_ENCODING)?,
        etag: singleton_header(response.headers(), ETAG)?,
        response_started_at_unix_nanos: system_unix_nanos()?,
    })
}

async fn collect_catalog_body(
    stream: &mut ByteStream,
    deadline: Deadline,
    cancellation: &CancellationToken,
    telemetry: &mut TransportTelemetry,
) -> Result<Vec<u8>, TransportErrorKind> {
    let mut body = Vec::new();
    body.try_reserve(64 * 1024)
        .map_err(|_| TransportErrorKind::Capacity)?;
    loop {
        let next = next_stream_item(stream, deadline, cancellation).await?;
        let Some(chunk) = next else {
            break;
        };
        let chunk = admit_stream_chunk(chunk, telemetry)?;
        telemetry.add_response_bytes(chunk.len())?;
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or(TransportErrorKind::ByteLimit)?;
        if next_len > MAX_CATALOG_BYTES {
            return Err(TransportErrorKind::ByteLimit);
        }
        body.try_reserve(chunk.len())
            .map_err(|_| TransportErrorKind::Capacity)?;
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[allow(
    clippy::too_many_arguments,
    reason = "one cold file operation carries its complete admission and publication boundaries"
)]
async fn materialize_selected_stream(
    plan: &ColdJobPlan,
    metadata: CaptureResponseMetadata,
    mut stream: ByteStream,
    staging_directory: &Path,
    deadline: Deadline,
    cancellation: &CancellationToken,
    decode_limits: DecodeLimits,
    sink: &mut dyn IexEventSink,
    mut telemetry: TransportTelemetry,
) -> Result<DownloadedIexCapture, IexHistTransportError> {
    let operation = async {
        let compressed = create_staged_file(staging_directory)?;
        let pcap = create_staged_file(staging_directory)?;
        let mut receipt =
            GzipPcapReceiptBuilder::new(plan, metadata).map_err(TransportErrorKind::Capture)?;
        let mut compressed_writer = tokio::fs::File::from_std(
            compressed
                .reopen()
                .map_err(|_| TransportErrorKind::StagingIo)?,
        );

        loop {
            let next = next_stream_item(&mut stream, deadline, cancellation).await?;
            let Some(chunk) = next else {
                break;
            };
            let chunk = admit_stream_chunk(chunk, &mut telemetry)?;
            telemetry.add_response_bytes(chunk.len())?;
            receipt
                .push_compressed(&chunk)
                .map_err(TransportErrorKind::Capture)?;
            write_all_deadline(&mut compressed_writer, &chunk, deadline, cancellation).await?;
        }
        flush_sync(&mut compressed_writer, deadline, cancellation).await?;
        drop(compressed_writer);
        validate_disk(plan, staging_directory, DiskPhase::BeforeExpansion)?;

        let compressed_reader = tokio::fs::File::from_std(
            compressed
                .reopen()
                .map_err(|_| TransportErrorKind::StagingIo)?,
        );
        let mut gzip = GzipDecoder::new(BufReader::new(compressed_reader));
        let mut pcap_writer =
            tokio::fs::File::from_std(pcap.reopen().map_err(|_| TransportErrorKind::StagingIo)?);
        let mut output = vec![0_u8; STREAM_BUFFER_BYTES];
        loop {
            let read = read_deadline(&mut gzip, &mut output, deadline, cancellation).await?;
            if read == 0 {
                break;
            }
            receipt
                .push_pcap(&output[..read])
                .map_err(TransportErrorKind::Capture)?;
            telemetry.expanded_pcap_bytes = telemetry
                .expanded_pcap_bytes
                .checked_add(u64::try_from(read).map_err(|_| TransportErrorKind::ByteLimit)?)
                .ok_or(TransportErrorKind::ByteLimit)?;
            write_all_deadline(&mut pcap_writer, &output[..read], deadline, cancellation).await?;
        }
        let mut compressed_reader = gzip.into_inner();
        let buffered = u64::try_from(compressed_reader.buffer().len())
            .map_err(|_| TransportErrorKind::ByteLimit)?;
        let physical_position = await_deadline(
            compressed_reader.get_mut().stream_position(),
            deadline,
            cancellation,
        )
        .await?
        .map_err(|_| TransportErrorKind::StagingIo)?;
        let consumed = physical_position
            .checked_sub(buffered)
            .ok_or(TransportErrorKind::CorruptGzip)?;
        if consumed != plan.advertised_compressed_bytes {
            return Err(TransportErrorKind::TrailingGzipData);
        }
        flush_sync(&mut pcap_writer, deadline, cancellation).await?;
        drop(pcap_writer);
        validate_disk(plan, staging_directory, DiskPhase::AfterExpansion)?;

        let materialization = receipt
            .finish(system_unix_nanos()?)
            .map_err(TransportErrorKind::Capture)?;
        let mut decoder = PcapStreamDecoder::new(plan, &materialization, decode_limits)
            .map_err(TransportErrorKind::Decode)?;
        let mut pcap_reader =
            tokio::fs::File::from_std(pcap.reopen().map_err(|_| TransportErrorKind::StagingIo)?);
        await_deadline(
            pcap_reader.seek(std::io::SeekFrom::Start(0)),
            deadline,
            cancellation,
        )
        .await?
        .map_err(|_| TransportErrorKind::StagingIo)?;
        loop {
            let read = read_deadline(&mut pcap_reader, &mut output, deadline, cancellation).await?;
            if read == 0 {
                break;
            }
            decoder
                .push(&output[..read], sink)
                .map_err(TransportErrorKind::Decode)?;
        }
        let decode = decoder.finish().map_err(TransportErrorKind::Decode)?;
        Ok((
            materialization,
            decode,
            StagedCaptureFiles { compressed, pcap },
        ))
    };
    match operation.await {
        Ok((materialization, decode, staged_files)) => Ok(DownloadedIexCapture {
            materialization,
            decode,
            telemetry,
            staged_files,
        }),
        Err(kind) => Err(IexHistTransportError::new(kind, telemetry)),
    }
}

fn admit_stream_chunk(
    chunk: Result<Bytes, StreamFailure>,
    telemetry: &mut TransportTelemetry,
) -> Result<Bytes, TransportErrorKind> {
    match chunk {
        Ok(bytes) => Ok(bytes),
        Err(StreamFailure::Network) => {
            telemetry.record_network_failure()?;
            Err(TransportErrorKind::Network)
        }
    }
}

fn create_staged_file(directory: &Path) -> Result<NamedTempFile, TransportErrorKind> {
    let metadata =
        std::fs::symlink_metadata(directory).map_err(|_| TransportErrorKind::StagingIo)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(TransportErrorKind::UnsafeStagingDirectory);
    }
    NamedTempFile::new_in(directory).map_err(|_| TransportErrorKind::StagingIo)
}

#[derive(Clone, Copy)]
enum DiskPhase {
    BeforeTransfer,
    BeforeExpansion,
    AfterExpansion,
}

fn validate_disk(
    plan: &ColdJobPlan,
    staging_directory: &Path,
    phase: DiskPhase,
) -> Result<(), TransportErrorKind> {
    let metadata =
        std::fs::symlink_metadata(staging_directory).map_err(|_| TransportErrorKind::StagingIo)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(TransportErrorKind::UnsafeStagingDirectory);
    }
    let free =
        fs2::available_space(staging_directory).map_err(|_| TransportErrorKind::DiskProbe)?;
    let reserve = plan
        .required_disk_bytes
        .checked_sub(plan.advertised_compressed_bytes)
        .and_then(|value| value.checked_sub(plan.max_pcap_bytes))
        .ok_or(TransportErrorKind::Plan(PlanError::DiskArithmetic))?;
    let required = match phase {
        DiskPhase::BeforeTransfer => plan.required_disk_bytes,
        DiskPhase::BeforeExpansion => plan
            .max_pcap_bytes
            .checked_add(reserve)
            .ok_or(TransportErrorKind::Plan(PlanError::DiskArithmetic))?,
        DiskPhase::AfterExpansion => reserve,
    };
    if free < required {
        Err(TransportErrorKind::InsufficientDisk {
            required,
            available: free,
        })
    } else {
        Ok(())
    }
}

async fn write_all_deadline(
    writer: &mut tokio::fs::File,
    bytes: &[u8],
    deadline: Deadline,
    cancellation: &CancellationToken,
) -> Result<(), TransportErrorKind> {
    await_deadline(writer.write_all(bytes), deadline, cancellation)
        .await?
        .map_err(|_| TransportErrorKind::StagingIo)
}

async fn flush_sync(
    writer: &mut tokio::fs::File,
    deadline: Deadline,
    cancellation: &CancellationToken,
) -> Result<(), TransportErrorKind> {
    await_deadline(writer.flush(), deadline, cancellation)
        .await?
        .map_err(|_| TransportErrorKind::StagingIo)?;
    await_deadline(writer.sync_all(), deadline, cancellation)
        .await?
        .map_err(|_| TransportErrorKind::StagingIo)
}

async fn read_deadline<R>(
    reader: &mut R,
    output: &mut [u8],
    deadline: Deadline,
    cancellation: &CancellationToken,
) -> Result<usize, TransportErrorKind>
where
    R: tokio::io::AsyncRead + Unpin,
{
    await_deadline(reader.read(output), deadline, cancellation)
        .await?
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::InvalidData {
                TransportErrorKind::CorruptGzip
            } else {
                TransportErrorKind::StagingIo
            }
        })
}

async fn next_stream_item(
    stream: &mut ByteStream,
    deadline: Deadline,
    cancellation: &CancellationToken,
) -> Result<Option<Result<Bytes, StreamFailure>>, TransportErrorKind> {
    await_deadline(stream.next(), deadline, cancellation).await
}

async fn wait_retry(
    wait: Duration,
    deadline: Deadline,
    cancellation: &CancellationToken,
) -> Result<(), TransportErrorKind> {
    if Instant::now()
        .checked_add(wait)
        .is_none_or(|instant| instant >= deadline.instant)
    {
        return Err(TransportErrorKind::DeadlineExceeded);
    }
    await_deadline(tokio::time::sleep(wait), deadline, cancellation).await
}

async fn await_deadline<F>(
    future: F,
    deadline: Deadline,
    cancellation: &CancellationToken,
) -> Result<F::Output, TransportErrorKind>
where
    F: Future,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(TransportErrorKind::Cancelled),
        () = tokio::time::sleep_until(deadline.instant) => Err(TransportErrorKind::DeadlineExceeded),
        output = future => Ok(output),
    }
}

#[derive(Clone, Copy)]
struct Deadline {
    instant: Instant,
}

impl Deadline {
    fn new(deadline_unix_nanos: i64) -> Result<Self, TransportErrorKind> {
        let now_unix_nanos = system_unix_nanos()?;
        let remaining = deadline_unix_nanos
            .checked_sub(now_unix_nanos)
            .ok_or(TransportErrorKind::DeadlineExceeded)?;
        if remaining <= 0 {
            return Err(TransportErrorKind::DeadlineExceeded);
        }
        let remaining = Duration::from_nanos(
            u64::try_from(remaining).map_err(|_| TransportErrorKind::DeadlineExceeded)?,
        );
        let instant = Instant::now()
            .checked_add(remaining)
            .ok_or(TransportErrorKind::DeadlineExceeded)?;
        Ok(Self { instant })
    }
}

fn singleton_header(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
) -> Result<Option<String>, TransportErrorKind> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() || value.as_bytes().len() > MAX_HEADER_BYTES {
        return Err(TransportErrorKind::InvalidResponseMetadata);
    }
    let value = value
        .to_str()
        .map_err(|_| TransportErrorKind::InvalidResponseMetadata)?;
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
    {
        return Err(TransportErrorKind::InvalidResponseMetadata);
    }
    Ok(Some(value.to_owned()))
}

fn parse_content_length(headers: &reqwest::header::HeaderMap) -> Result<u64, TransportErrorKind> {
    let value = singleton_header(headers, CONTENT_LENGTH)?
        .ok_or(TransportErrorKind::InvalidResponseMetadata)?;
    if value.is_empty()
        || value.len() > 20
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return Err(TransportErrorKind::InvalidResponseMetadata);
    }
    value
        .parse()
        .map_err(|_| TransportErrorKind::InvalidResponseMetadata)
}

fn parse_retry_after(
    headers: &reqwest::header::HeaderMap,
) -> Result<Option<Duration>, TransportErrorKind> {
    let Some(value) = singleton_header(headers, RETRY_AFTER)? else {
        return Ok(None);
    };
    if value.bytes().all(|byte| byte.is_ascii_digit()) {
        let seconds = value
            .parse::<u64>()
            .map_err(|_| TransportErrorKind::InvalidRetryAfter)?;
        return Ok(Some(Duration::from_secs(seconds)));
    }
    let time =
        httpdate::parse_http_date(&value).map_err(|_| TransportErrorKind::InvalidRetryAfter)?;
    Ok(Some(
        time.duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO),
    ))
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn system_unix_nanos() -> Result<i64, TransportErrorKind> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| TransportErrorKind::Clock)?;
    u128::from(duration.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(duration.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(TransportErrorKind::Clock)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StreamFailure {
    Network,
}

/// Failure with exact telemetry accumulated before the terminal refusal.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{kind}")]
pub struct IexHistTransportError {
    kind: TransportErrorKind,
    telemetry: TransportTelemetry,
}

impl IexHistTransportError {
    fn new(kind: TransportErrorKind, telemetry: TransportTelemetry) -> Self {
        Self { kind, telemetry }
    }

    /// Returns the typed terminal cause.
    #[must_use]
    pub const fn kind(&self) -> &TransportErrorKind {
        &self.kind
    }

    /// Returns exact attempts, statuses, retries, and actual byte counts observed before failure.
    #[must_use]
    pub const fn telemetry(&self) -> &TransportTelemetry {
        &self.telemetry
    }
}

/// Typed catalog/download/decompression/decoder transport refusal.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TransportErrorKind {
    /// Configuration violates hard application bounds.
    #[error("IEX HIST transport configuration is invalid")]
    InvalidConfiguration,
    /// Operation was explicitly cancelled.
    #[error("IEX HIST cold operation was cancelled")]
    Cancelled,
    /// Operation exceeded its terminal deadline.
    #[error("IEX HIST cold operation exceeded its deadline")]
    DeadlineExceeded,
    /// HTTP request or body stream failed.
    #[error("IEX HIST network transport failed")]
    Network,
    /// Non-retryable HTTP status.
    #[error("IEX HIST returned HTTP status {status}")]
    HttpStatus {
        /// Exact response status.
        status: u16,
    },
    /// Bounded retry attempts were exhausted.
    #[error("IEX HIST retry policy was exhausted after status {status}")]
    RetryExhausted {
        /// Last retryable response status.
        status: u16,
    },
    /// Response header or final URL did not match the selected contract.
    #[error("IEX HIST response metadata is invalid")]
    InvalidResponseMetadata,
    /// Retry-After header was malformed.
    #[error("IEX HIST Retry-After header is invalid")]
    InvalidRetryAfter,
    /// Catalog/response bytes exceeded the admitted bound.
    #[error("IEX HIST byte admission was exceeded")]
    ByteLimit,
    /// Fallible bounded allocation failed.
    #[error("IEX HIST transport capacity is unavailable")]
    Capacity,
    /// Telemetry arithmetic overflowed.
    #[error("IEX HIST telemetry arithmetic overflowed")]
    TelemetryOverflow,
    /// Trusted system clock could not be represented.
    #[error("IEX HIST trusted clock is unavailable")]
    Clock,
    /// Staging directory was not a direct directory.
    #[error("IEX HIST staging directory is unsafe")]
    UnsafeStagingDirectory,
    /// Staging file I/O failed.
    #[error("IEX HIST staging I/O failed")]
    StagingIo,
    /// Free-disk measurement failed.
    #[error("IEX HIST free-disk measurement failed")]
    DiskProbe,
    /// Available disk fell below the phase-specific reserve.
    #[error("IEX HIST needs {required} bytes but only {available} bytes are available")]
    InsufficientDisk {
        /// Required free bytes for the phase.
        required: u64,
        /// Measured free bytes.
        available: u64,
    },
    /// Gzip decoder rejected compressed structure or integrity.
    #[error("IEX HIST gzip stream is corrupt")]
    CorruptGzip,
    /// Bytes remained after the single admitted gzip member.
    #[error("IEX HIST gzip stream contains trailing data or another member")]
    TrailingGzipData,
    /// Cold plan admission failed.
    #[error("IEX HIST cold plan failed: {0}")]
    Plan(PlanError),
    /// Catalog validation failed.
    #[error("IEX HIST catalog validation failed: {0}")]
    Catalog(CatalogError),
    /// Exact compressed/PCAP receipt validation failed.
    #[error("IEX HIST capture validation failed: {0}")]
    Capture(CaptureError),
    /// PCAP/feed decode validation failed.
    #[error("IEX HIST decode failed: {0}")]
    Decode(DecodeError),
}

#[cfg(test)]
pub(crate) async fn materialize_mock_stream(
    plan: &ColdJobPlan,
    metadata: CaptureResponseMetadata,
    chunks: Vec<Bytes>,
    staging_directory: &Path,
    cancellation: &CancellationToken,
    sink: &mut dyn IexEventSink,
) -> Result<DownloadedIexCapture, IexHistTransportError> {
    let deadline = Deadline::new(plan.deadline_unix_nanos)
        .map_err(|kind| IexHistTransportError::new(kind, TransportTelemetry::new()))?;
    let stream: ByteStream = Box::pin(futures_util::stream::iter(
        chunks.into_iter().map(Ok::<_, StreamFailure>),
    ));
    let mut telemetry = TransportTelemetry::new();
    telemetry
        .begin_attempt()
        .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
    telemetry
        .record_status(metadata.status)
        .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
    materialize_selected_stream(
        plan,
        metadata,
        stream,
        staging_directory,
        deadline,
        cancellation,
        DecodeLimits::default(),
        sink,
        telemetry,
    )
    .await
}
