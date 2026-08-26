use std::fmt;
use std::fs::File;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::time::{Duration, UNIX_EPOCH};

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
use crate::decode::{
    DecodeActuals, DecodeError, DecodeFailure, DecodeSummary, IexEventSink, PcapStreamDecoder,
};
use crate::model::PcapObjectEncoding;
use crate::planning::{
    ColdJobPlan, IexHistCapacityAuthority, IexHistCapacityCategory, IexHistCapacityError,
    IexHistCapacityFootprint, IexHistCapacityRequest, IexHistExecutionPermit,
    IexHistTerminalReason, PlanError,
};
use crate::receipt::{
    CaptureChronologyDisposition, CaptureError, CaptureResponseMetadata, GzipPcapReceiptBuilder,
    PcapMaterializationReceipt,
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
const CATALOG_ATOMIC_OVERHEAD_BYTES: u64 = 1024 * 1024;

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

/// HTTP transport controls for explicit cold jobs; decoder controls live only in the cold plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IexHistTransportConfig {
    retry_policy: RetryPolicy,
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
            connect_timeout,
            read_timeout,
        })
    }
}

impl Default for IexHistTransportConfig {
    fn default() -> Self {
        Self {
            retry_policy: RetryPolicy::default(),
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
    staged_provider_object_bytes: u64,
    staged_pcap_bytes: u64,
    staged_decoded_event_batch_bytes: u64,
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
            staged_provider_object_bytes: 0,
            staged_pcap_bytes: 0,
            staged_decoded_event_batch_bytes: 0,
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

    /// Returns exact provider-object bytes successfully written to temporary storage.
    #[must_use]
    pub const fn staged_provider_object_bytes(&self) -> u64 {
        self.staged_provider_object_bytes
    }

    /// Returns exact materialized-PCAP bytes successfully written to temporary storage.
    #[must_use]
    pub const fn staged_pcap_bytes(&self) -> u64 {
        self.staged_pcap_bytes
    }

    /// Returns exact framed provider-native event-batch bytes staged transactionally by decode.
    #[must_use]
    pub const fn staged_decoded_event_batch_bytes(&self) -> u64 {
        self.staged_decoded_event_batch_bytes
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

    fn add_staged_provider_object_bytes(
        &mut self,
        bytes: usize,
    ) -> Result<(), TransportErrorKind> {
        self.staged_provider_object_bytes = self
            .staged_provider_object_bytes
            .checked_add(u64::try_from(bytes).map_err(|_| TransportErrorKind::ByteLimit)?)
            .ok_or(TransportErrorKind::ByteLimit)?;
        Ok(())
    }

    fn add_staged_pcap_bytes(&mut self, bytes: usize) -> Result<(), TransportErrorKind> {
        self.staged_pcap_bytes = self
            .staged_pcap_bytes
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
    capacity_permit: IexHistExecutionPermit,
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
    pub fn into_parts(
        self,
    ) -> (Catalog, Bytes, TransportTelemetry, IexHistExecutionPermit) {
        (
            self.catalog,
            self.exact_body,
            self.telemetry,
            self.capacity_permit,
        )
    }
}

/// Temporary exact compressed and PCAP files owned until the caller seals or drops them.
pub struct StagedCaptureFiles {
    provider_object: NamedTempFile,
    expanded_pcap: Option<NamedTempFile>,
}

impl fmt::Debug for StagedCaptureFiles {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StagedCaptureFiles")
            .field("provider_object", &"opaque-temporary-file")
            .field(
                "expanded_pcap",
                &self.expanded_pcap.as_ref().map(|_| "opaque-temporary-file"),
            )
            .finish()
    }
}

impl StagedCaptureFiles {
    /// Reopens the exact provider object without exposing a path.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the temporary artifact can no longer be reopened.
    pub fn reopen_provider_object(&self) -> std::io::Result<File> {
        self.provider_object.reopen()
    }

    /// Reopens the exact expanded PCAP without exposing a path.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the temporary artifact can no longer be reopened.
    pub fn reopen_pcap(&self) -> std::io::Result<File> {
        self.expanded_pcap
            .as_ref()
            .unwrap_or(&self.provider_object)
            .reopen()
    }

    /// Consumes both temporary objects so application storage can rename/persist them in place.
    ///
    /// This is the zero-copy ownership handoff. A store that instead copies must keep the already
    /// reserved temporary and durable categories charged until the temporary objects are dropped.
    #[must_use]
    pub fn into_temp_files(self) -> (NamedTempFile, Option<NamedTempFile>) {
        (self.provider_object, self.expanded_pcap)
    }
}

/// Complete selected-file transfer, expansion, integrity, and temporary raw-artifact outcome.
///
/// Decode is deliberately a later phase. The application must first seal both artifacts and
/// durably record the exact application-owned provider-object/PCAP seal evidence.
#[derive(Debug)]
pub struct MaterializedIexCapture {
    materialization: PcapMaterializationReceipt,
    telemetry: TransportTelemetry,
    staged_files: StagedCaptureFiles,
    capacity_permit: IexHistExecutionPermit,
}

impl MaterializedIexCapture {
    /// Returns the exact compressed/expanded materialization receipt.
    #[must_use]
    pub const fn materialization(&self) -> &PcapMaterializationReceipt {
        &self.materialization
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
        TransportTelemetry,
        StagedCaptureFiles,
        IexHistExecutionPermit,
    ) {
        (
            self.materialization,
            self.telemetry,
            self.staged_files,
            self.capacity_permit,
        )
    }
}

/// Complete decode result that retains reservation ownership for the downstream handoff.
#[derive(Debug)]
pub struct DecodedIexCapture<S> {
    summary: DecodeSummary,
    sink: S,
    telemetry: TransportTelemetry,
    capacity_permit: IexHistExecutionPermit,
}

impl<S> DecodedIexCapture<S> {
    #[must_use]
    pub const fn summary(&self) -> &DecodeSummary { &self.summary }
    #[must_use]
    pub const fn telemetry(&self) -> &TransportTelemetry { &self.telemetry }
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (DecodeSummary, S, TransportTelemetry, IexHistExecutionPermit) {
        (self.summary, self.sink, self.telemetry, self.capacity_permit)
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
        capacity_authority: &dyn IexHistCapacityAuthority,
        deadline_unix_nanos: i64,
        cancellation: &CancellationToken,
    ) -> Result<CatalogFetch, IexHistTransportError> {
        let footprint = IexHistCapacityFootprint::catalog(
            u64::try_from(MAX_CATALOG_BYTES).unwrap_or(u64::MAX),
            CATALOG_ATOMIC_OVERHEAD_BYTES,
            capacity_authority
                .required_free_reserve_bytes()
                .map_err(|error| {
                    IexHistTransportError::new(
                        TransportErrorKind::CapacityAuthority(error),
                        TransportTelemetry::new(),
                    )
                })?,
        )
        .map_err(|error| {
            IexHistTransportError::new(TransportErrorKind::Plan(error), TransportTelemetry::new())
        })?;
        let request = IexHistCapacityRequest::catalog(footprint, deadline_unix_nanos)
            .map_err(|error| {
                IexHistTransportError::new(
                    TransportErrorKind::CapacityAuthority(error),
                    TransportTelemetry::new(),
                )
            })?;
        let mut capacity_permit =
            IexHistExecutionPermit::acquire(capacity_authority, request, None).map_err(|error| {
                IexHistTransportError::new(
                    TransportErrorKind::CapacityAuthority(error),
                    TransportTelemetry::new(),
                )
            })?;
        let deadline = Deadline::from_permit(&capacity_permit)
            .map_err(|kind| IexHistTransportError::new(kind, TransportTelemetry::new()))?;
        let mut telemetry = TransportTelemetry::new();
        let operation = async {
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
                            telemetry.clone(),
                        ));
                    }
                    let wait = self.config.retry_policy.wait_for_attempt(attempt);
                    wait_retry(wait, deadline, cancellation)
                        .await
                        .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
                    continue;
                }
                Err(kind) => return Err(IexHistTransportError::new(kind, telemetry.clone())),
            };
            let status = response.status().as_u16();
            telemetry
                .record_status(status)
                .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
            if matches!(status, 429 | 503) {
                if attempt >= self.config.retry_policy.max_attempts {
                    return Err(IexHistTransportError::new(
                        TransportErrorKind::RetryExhausted { status },
                        telemetry.clone(),
                    ));
                }
                let retry_clock = capacity_permit.trusted_clock().map_err(|error| {
                    IexHistTransportError::new(
                        TransportErrorKind::CapacityAuthority(error),
                        telemetry.clone(),
                    )
                })?;
                let provider_wait = parse_retry_after(response.headers(), retry_clock)
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
                    telemetry.clone(),
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
            let observation = capacity_permit
                .observe_catalog_body(&body)
                .map_err(|error| {
                    IexHistTransportError::new(
                        TransportErrorKind::CapacityAuthority(error),
                        telemetry.clone(),
                    )
                })?;
            let catalog = Catalog::parse(
                &body,
                CatalogTransportMetadata {
                    status,
                    content_type: headers.content_type,
                    content_length: headers.content_length,
                    etag: headers.etag,
                    observation,
                },
            )
            .map_err(|error| {
                IexHistTransportError::new(
                    TransportErrorKind::Catalog(error),
                    telemetry.clone(),
                )
            })?;
            return Ok((catalog, Bytes::from(body)));
          }
        }
        .await;
        match operation {
            Ok((catalog, exact_body)) => {
                if let Err(error) = capacity_permit.record_usage(
                    IexHistCapacityCategory::NetworkResponse,
                    telemetry.response_bytes,
                ) {
                    let _ = capacity_permit
                        .settle(crate::planning::IexHistCapacityDisposition::Failed);
                    return Err(IexHistTransportError::new(
                        TransportErrorKind::CapacityAuthority(error),
                        telemetry,
                    ));
                }
                Ok(CatalogFetch { catalog, exact_body, telemetry, capacity_permit })
            }
            Err(error) => {
                let _ = capacity_permit.record_usage(
                    IexHistCapacityCategory::NetworkResponse,
                    telemetry.response_bytes,
                );
                let settlement = capacity_permit
                    .settle(crate::planning::IexHistCapacityDisposition::Failed);
                Err(IexHistTransportError::new(
                    settlement
                        .err()
                        .map_or(error.kind, TransportErrorKind::CapacityAuthority),
                    telemetry,
                ))
            }
        }
    }

    /// Downloads, stages, expands, and receipts one exact selected file.
    ///
    /// No retry occurs after response-body streaming starts. A failure discards temporary objects;
    /// restart repeats the exact selected-file request from byte zero. Successful temporary files
    /// still have no durable authority until the application consumes them into its immutable
    /// provider-object storage and commits the corresponding acquisition evidence.
    ///
    /// # Errors
    ///
    /// Fails with retained request telemetry on admission, cancellation, deadline, status/retry,
    /// network, disk, gzip, or exact materialization-receipt failure.
    pub async fn download_materialize(
        &self,
        plan: &ColdJobPlan,
        capacity_authority: &dyn IexHistCapacityAuthority,
        deadline_unix_nanos: i64,
        cancellation: &CancellationToken,
    ) -> Result<MaterializedIexCapture, IexHistTransportError> {
        let authority_free_reserve_bytes = capacity_authority
            .required_free_reserve_bytes()
            .map_err(|error| {
                IexHistTransportError::new(
                    TransportErrorKind::CapacityAuthority(error),
                    TransportTelemetry::new(),
                )
            })?;
        let request = IexHistCapacityRequest::selected_file(
            plan,
            deadline_unix_nanos,
            authority_free_reserve_bytes,
        )
            .map_err(|error| {
                IexHistTransportError::new(
                    TransportErrorKind::CapacityAuthority(error),
                    TransportTelemetry::new(),
                )
            })?;
        let capacity_permit = IexHistExecutionPermit::acquire(
            capacity_authority,
            request,
            Some(plan),
        )
        .map_err(|error| {
            IexHistTransportError::new(
                TransportErrorKind::CapacityAuthority(error),
                TransportTelemetry::new(),
            )
        })?;
        let staging_directory = capacity_permit
            .staging_directory()
            .map_err(|error| {
                IexHistTransportError::new(
                    TransportErrorKind::CapacityAuthority(error),
                    TransportTelemetry::new(),
                )
            })?
            .to_path_buf();
        let deadline = Deadline::from_permit(&capacity_permit)
            .map_err(|kind| IexHistTransportError::new(kind, TransportTelemetry::new()))?;
        let mut telemetry = TransportTelemetry::new();
        let response = async {
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
                            telemetry.clone(),
                        ));
                    }
                    let wait = self.config.retry_policy.wait_for_attempt(attempt);
                    wait_retry(wait, deadline, cancellation)
                        .await
                        .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
                    continue;
                }
                Err(kind) => return Err(IexHistTransportError::new(kind, telemetry.clone())),
            };
            let status = response.status().as_u16();
            telemetry
                .record_status(status)
                .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
            if matches!(status, 429 | 503) {
                if attempt >= self.config.retry_policy.max_attempts {
                    return Err(IexHistTransportError::new(
                        TransportErrorKind::RetryExhausted { status },
                        telemetry.clone(),
                    ));
                }
                let retry_clock = capacity_permit.trusted_clock().map_err(|error| {
                    IexHistTransportError::new(
                        TransportErrorKind::CapacityAuthority(error),
                        telemetry.clone(),
                    )
                })?;
                let provider_wait = parse_retry_after(response.headers(), retry_clock)
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
                    telemetry.clone(),
                ));
            }
            let metadata = file_response_metadata(plan, &response, &capacity_permit)
                .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
            let stream: ByteStream = Box::pin(
                response
                    .bytes_stream()
                    .map(|item| item.map_err(|_| StreamFailure::Network)),
            );
            return Ok((metadata, stream));
          }
        }
        .await;
        let (metadata, stream) = match response {
            Ok(value) => value,
            Err(error) => {
                let settlement = capacity_permit
                    .settle(crate::planning::IexHistCapacityDisposition::Failed);
                return Err(IexHistTransportError::new(
                    settlement
                        .err()
                        .map_or(error.kind, TransportErrorKind::CapacityAuthority),
                    telemetry,
                ));
            }
        };
        materialize_selected_stream(
                plan,
                metadata,
                stream,
                &staging_directory,
                deadline,
                cancellation,
                telemetry,
                capacity_permit,
            )
            .await
    }

    /// Re-reads and decodes one application-sealed complete PCAP from byte zero.
    ///
    /// The caller supplies an already opened controlled object rather than a path. The decoder
    /// independently rechecks its exact byte count and SHA-256 against the acquisition receipt.
    /// The decoder owns `sink`: failure aborts its staged transaction, while success returns the
    /// committed sink with the exact [`DecodeSummary`].
    pub async fn decode_sealed_pcap<S: IexEventSink>(
        &self,
        plan: &ColdJobPlan,
        capture: &PcapMaterializationReceipt,
        pcap: File,
        capacity_authority: &dyn IexHistCapacityAuthority,
        deadline_unix_nanos: i64,
        cancellation: &CancellationToken,
        sink: S,
    ) -> Result<DecodedIexCapture<S>, IexHistTransportError> {
        let authority_free_reserve_bytes = capacity_authority
            .required_free_reserve_bytes()
            .map_err(|error| {
                IexHistTransportError::new(
                    TransportErrorKind::CapacityAuthority(error),
                    TransportTelemetry::new(),
                )
            })?;
        let request = IexHistCapacityRequest::selected_file(
            plan,
            deadline_unix_nanos,
            authority_free_reserve_bytes,
        )
            .map_err(|error| {
                IexHistTransportError::new(
                    TransportErrorKind::CapacityAuthority(error),
                    TransportTelemetry::new(),
                )
            })?;
        let capacity_permit = IexHistExecutionPermit::acquire(
            capacity_authority,
            request,
            Some(plan),
        )
        .map_err(|error| {
            IexHistTransportError::new(
                TransportErrorKind::CapacityAuthority(error),
                TransportTelemetry::new(),
            )
        })?;
        let deadline = Deadline::from_permit(&capacity_permit)
            .map_err(|kind| IexHistTransportError::new(kind, TransportTelemetry::new()))?;
        let telemetry = TransportTelemetry::new();
        decode_sealed_pcap_file(
            plan,
            capture,
            pcap,
            deadline,
            cancellation,
            sink,
            telemetry,
            capacity_permit,
        )
        .await
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
    capacity_permit: &IexHistExecutionPermit,
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
        response_started_clock: capacity_permit
            .trusted_clock()
            .map_err(TransportErrorKind::CapacityAuthority)?,
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
    reason = "one cold file operation carries its complete acquisition and handoff boundaries"
)]
async fn materialize_selected_stream(
    plan: &ColdJobPlan,
    metadata: CaptureResponseMetadata,
    mut stream: ByteStream,
    staging_directory: &Path,
    deadline: Deadline,
    cancellation: &CancellationToken,
    mut telemetry: TransportTelemetry,
    mut capacity_permit: IexHistExecutionPermit,
) -> Result<MaterializedIexCapture, IexHistTransportError> {
    let capture_started = Instant::now();
    let attempt = capacity_permit.attempt();
    let operation = async {
        let provider_object = create_staged_file(staging_directory)?;
        let mut receipt =
            GzipPcapReceiptBuilder::new(plan, attempt, metadata).map_err(TransportErrorKind::Capture)?;
        let mut provider_object_writer = tokio::fs::File::from_std(
            provider_object
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
            if plan.object_encoding == PcapObjectEncoding::Identity {
                receipt
                    .push_pcap(&chunk)
                    .map_err(TransportErrorKind::Capture)?;
                telemetry.expanded_pcap_bytes = telemetry
                    .expanded_pcap_bytes
                    .checked_add(
                        u64::try_from(chunk.len()).map_err(|_| TransportErrorKind::ByteLimit)?,
                    )
                    .ok_or(TransportErrorKind::ByteLimit)?;
            }
            write_all_deadline(
                &mut provider_object_writer,
                &chunk,
                deadline,
                cancellation,
            )
            .await?;
            telemetry.add_staged_provider_object_bytes(chunk.len())?;
            if plan.object_encoding == PcapObjectEncoding::Identity {
                telemetry.add_staged_pcap_bytes(chunk.len())?;
            }
        }
        flush_sync(&mut provider_object_writer, deadline, cancellation).await?;
        drop(provider_object_writer);

        let expanded_pcap = match plan.object_encoding {
            PcapObjectEncoding::Identity => None,
            PcapObjectEncoding::Gzip => {
                let pcap = create_staged_file(staging_directory)?;
                let provider_object_reader = tokio::fs::File::from_std(
                    provider_object
                        .reopen()
                        .map_err(|_| TransportErrorKind::StagingIo)?,
                );
                let mut gzip = GzipDecoder::new(BufReader::new(provider_object_reader));
                let mut pcap_writer = tokio::fs::File::from_std(
                    pcap.reopen().map_err(|_| TransportErrorKind::StagingIo)?,
                );
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
                        .checked_add(
                            u64::try_from(read).map_err(|_| TransportErrorKind::ByteLimit)?,
                        )
                        .ok_or(TransportErrorKind::ByteLimit)?;
                    write_all_deadline(
                        &mut pcap_writer,
                        &output[..read],
                        deadline,
                        cancellation,
                    )
                    .await?;
                    telemetry.add_staged_pcap_bytes(read)?;
                }
                let mut provider_object_reader = gzip.into_inner();
                let buffered = u64::try_from(provider_object_reader.buffer().len())
                    .map_err(|_| TransportErrorKind::ByteLimit)?;
                let physical_position = await_deadline(
                    provider_object_reader.get_mut().stream_position(),
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
                Some(pcap)
            }
        };

        let completed_clock = capacity_permit
            .trusted_clock()
            .map_err(TransportErrorKind::CapacityAuthority)?;
        let monotonic_duration_nanos = u64::try_from(capture_started.elapsed().as_nanos())
            .map_err(|_| TransportErrorKind::Clock)?;
        let materialization = receipt
            .finish(completed_clock, monotonic_duration_nanos)
            .map_err(TransportErrorKind::Capture)?;
        Ok((
            materialization,
            StagedCaptureFiles {
                provider_object,
                expanded_pcap,
            },
        ))
    };
    match operation.await {
        Ok((materialization, staged_files)) => {
            if let Err(error) = record_materialization_usage(
                &mut capacity_permit,
                plan.object_encoding,
                &telemetry,
            ) {
                let _ = capacity_permit
                    .settle(crate::planning::IexHistCapacityDisposition::Failed);
                return Err(IexHistTransportError::new(
                    TransportErrorKind::CapacityAuthority(error),
                    telemetry,
                ));
            }
            Ok(MaterializedIexCapture {
                materialization,
                telemetry,
                staged_files,
                capacity_permit,
            })
        }
        Err(kind) => {
            let usage_error = record_materialization_usage(
                &mut capacity_permit,
                plan.object_encoding,
                &telemetry,
            )
            .err();
            let settlement = capacity_permit
                .settle(crate::planning::IexHistCapacityDisposition::Failed);
            Err(IexHistTransportError::new(
                usage_error
                    .or_else(|| settlement.err())
                    .map_or(kind, TransportErrorKind::CapacityAuthority),
                telemetry,
            ))
        }
    }
}

fn record_materialization_usage(
    capacity_permit: &mut IexHistExecutionPermit,
    object_encoding: PcapObjectEncoding,
    telemetry: &TransportTelemetry,
) -> Result<(), IexHistCapacityError> {
    capacity_permit.record_usage(
        IexHistCapacityCategory::NetworkResponse,
        telemetry.response_bytes,
    )?;
    if object_encoding == PcapObjectEncoding::Gzip {
        capacity_permit.record_usage(
            IexHistCapacityCategory::TemporaryCompressed,
            telemetry.staged_provider_object_bytes,
        )?;
    }
    capacity_permit.record_usage(
        IexHistCapacityCategory::TemporaryPcap,
        telemetry.staged_pcap_bytes,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "one decode phase carries its exact plan, capture, limits, and staging boundary"
)]
async fn decode_sealed_pcap_file<S: IexEventSink>(
    plan: &ColdJobPlan,
    capture: &PcapMaterializationReceipt,
    pcap: File,
    deadline: Deadline,
    cancellation: &CancellationToken,
    sink: S,
    mut telemetry: TransportTelemetry,
    mut capacity_permit: IexHistExecutionPermit,
) -> Result<DecodedIexCapture<S>, IexHistTransportError> {
    let operation = async {
        capture
            .validate_against(plan)
            .map_err(|error| DecodeOperationFailure::without_actuals(
                TransportErrorKind::Capture(error),
            ))?;
        if capture.chronology_disposition() != CaptureChronologyDisposition::Admitted {
            return Err(DecodeOperationFailure::without_actuals(
                TransportErrorKind::ChronologyQuarantined,
            ));
        }
        let decode_limits = plan.decode_contract().limits();
        let decode_attempt = capacity_permit
            .decode_attempt_evidence(plan)
            .map_err(|error| DecodeOperationFailure::without_actuals(
                TransportErrorKind::CapacityAuthority(error),
            ))?;
        let mut decoder = PcapStreamDecoder::new(plan, capture, decode_attempt, sink)
            .map_err(DecodeOperationFailure::from_decode)?;
        let mut pcap_reader = tokio::fs::File::from_std(pcap);
        let seek = await_deadline(
            pcap_reader.seek(std::io::SeekFrom::Start(0)),
            deadline,
            cancellation,
        )
        .await
        .map_err(|kind| DecodeOperationFailure::new(kind, decoder.actuals()))?;
        seek.map_err(|_| DecodeOperationFailure::new(
            TransportErrorKind::StagingIo,
            decoder.actuals(),
        ))?;
        let mut output = vec![0_u8; decode_limits.max_stream_chunk_bytes];
        loop {
            let read = read_deadline(&mut pcap_reader, &mut output, deadline, cancellation)
                .await
                .map_err(|kind| DecodeOperationFailure::new(kind, decoder.actuals()))?;
            if read == 0 {
                break;
            }
            telemetry.expanded_pcap_bytes = telemetry
                .expanded_pcap_bytes
                .checked_add(u64::try_from(read).map_err(|_| {
                    DecodeOperationFailure::new(
                        TransportErrorKind::ByteLimit,
                        decoder.actuals(),
                    )
                })?)
                .ok_or_else(|| DecodeOperationFailure::new(
                    TransportErrorKind::ByteLimit,
                    decoder.actuals(),
                ))?;
            decoder
                .push(&output[..read])
                .map_err(DecodeOperationFailure::from_decode)?;
        }
        let (summary, sink) = decoder.finish().map_err(DecodeOperationFailure::from_decode)?;
        let actuals = summary.actuals();
        summary
            .validate_against(plan, capture, decode_attempt)
            .map_err(|error| DecodeOperationFailure::new(
                TransportErrorKind::Decode(error),
                actuals,
            ))?;
        Ok((summary, sink, actuals))
    };
    match operation.await {
        Ok((summary, sink, actuals)) => {
            telemetry.staged_decoded_event_batch_bytes =
                actuals.decoded_event_batch_bytes_staged();
            if let Err(error) = record_decode_usage(&mut capacity_permit, actuals) {
                let _ = capacity_permit
                    .settle(crate::planning::IexHistCapacityDisposition::Failed);
                return Err(IexHistTransportError::new(
                    TransportErrorKind::CapacityAuthority(error),
                    telemetry,
                ));
            }
            Ok(DecodedIexCapture { summary, sink, telemetry, capacity_permit })
        }
        Err(failure) => {
            telemetry.staged_decoded_event_batch_bytes =
                failure.actuals.decoded_event_batch_bytes_staged();
            let usage_error = record_decode_usage(&mut capacity_permit, failure.actuals).err();
            let disposition = terminal_disposition(&failure.kind);
            let settlement = capacity_permit.settle(disposition);
            Err(IexHistTransportError::new(
                usage_error
                    .or_else(|| settlement.err())
                    .map_or(failure.kind, TransportErrorKind::CapacityAuthority),
                telemetry,
            ))
        }
    }
}

#[derive(Debug)]
struct DecodeOperationFailure {
    kind: TransportErrorKind,
    actuals: DecodeActuals,
}

impl DecodeOperationFailure {
    const fn new(kind: TransportErrorKind, actuals: DecodeActuals) -> Self {
        Self { kind, actuals }
    }

    fn without_actuals(kind: TransportErrorKind) -> Self {
        Self::new(kind, DecodeActuals::default())
    }

    fn from_decode(failure: DecodeFailure) -> Self {
        Self::new(
            TransportErrorKind::Decode(failure.error),
            failure.actuals,
        )
    }
}

fn record_decode_usage(
    capacity_permit: &mut IexHistExecutionPermit,
    actuals: DecodeActuals,
) -> Result<(), IexHistCapacityError> {
    capacity_permit.record_usage(
        IexHistCapacityCategory::DurablePcap,
        actuals.pcap_bytes_read(),
    )?;
    capacity_permit.record_usage(
        IexHistCapacityCategory::DecodedEventBatch,
        actuals.decoded_event_batch_bytes_staged(),
    )
}

fn terminal_disposition(
    kind: &TransportErrorKind,
) -> crate::planning::IexHistCapacityDisposition {
    use crate::planning::IexHistCapacityDisposition::{Failed, Quarantined, Unavailable};

    match kind {
        TransportErrorKind::ChronologyQuarantined => Quarantined(IexHistTerminalReason::ClockAnomaly),
        TransportErrorKind::Decode(error) => decode_terminal_disposition(error),
        TransportErrorKind::CapacityAuthority(
            IexHistCapacityError::Unavailable
            | IexHistCapacityError::InvalidLease
            | IexHistCapacityError::Clock
            | IexHistCapacityError::CatalogStale
            | IexHistCapacityError::InvalidCatalogObservation
            | IexHistCapacityError::InvalidDecodeEvidence
            | IexHistCapacityError::AlreadySettled
            | IexHistCapacityError::Settlement,
        ) => Unavailable(IexHistTerminalReason::AuthorityUnavailable),
        TransportErrorKind::Capture(_)
        | TransportErrorKind::CorruptGzip
        | TransportErrorKind::TrailingGzipData => {
            Quarantined(IexHistTerminalReason::CorruptRawEvidence)
        }
        TransportErrorKind::CapacityAuthority(
            IexHistCapacityError::InvalidRequest
            | IexHistCapacityError::UsageExceeded
            | IexHistCapacityError::IncompleteSettlement,
        ) => Quarantined(IexHistTerminalReason::InvalidDecoderContract),
        TransportErrorKind::Cancelled
        | TransportErrorKind::DeadlineExceeded
        | TransportErrorKind::Network
        | TransportErrorKind::RetryExhausted { .. }
        | TransportErrorKind::Capacity
        | TransportErrorKind::CapacityAuthority(
            IexHistCapacityError::Busy | IexHistCapacityError::InsufficientCapacity,
        )
        | TransportErrorKind::StagingIo => Failed,
        TransportErrorKind::InvalidConfiguration
        | TransportErrorKind::HttpStatus { .. }
        | TransportErrorKind::InvalidResponseMetadata
        | TransportErrorKind::InvalidRetryAfter
        | TransportErrorKind::ByteLimit
        | TransportErrorKind::TelemetryOverflow
        | TransportErrorKind::Clock
        | TransportErrorKind::UnsafeStagingDirectory
        | TransportErrorKind::Plan(_)
        | TransportErrorKind::Catalog(_) => {
            Quarantined(IexHistTerminalReason::InvalidDecoderContract)
        }
    }
}

fn decode_terminal_disposition(
    error: &DecodeError,
) -> crate::planning::IexHistCapacityDisposition {
    use crate::planning::IexHistCapacityDisposition::{Failed, Quarantined, Unavailable};

    match error {
        DecodeError::UnsupportedVersion | DecodeError::UnsupportedPcap => {
            Unavailable(IexHistTerminalReason::UnsupportedVersion)
        }
        DecodeError::InvalidDecoderContract | DecodeError::InvalidLimits => {
            Unavailable(IexHistTerminalReason::InvalidDecoderContract)
        }
        DecodeError::InvalidDecodeAttempt => {
            Unavailable(IexHistTerminalReason::AuthorityUnavailable)
        }
        DecodeError::CaptureChronologyQuarantined
        | DecodeError::InvalidCaptureTimestamp
        | DecodeError::CaptureClockRegression
        | DecodeError::SendClockRegression
        | DecodeError::SendCaptureClockSkew { .. }
        | DecodeError::ProviderTimestampRegression { .. }
        | DecodeError::InvalidTimestamp
        | DecodeError::EventAfterSendTime => Quarantined(IexHistTerminalReason::ClockAnomaly),
        DecodeError::InvalidContinuityCoordinate
        | DecodeError::CaptureStartsMidSession
        | DecodeError::SessionReset
        | DecodeError::SequenceGap { .. }
        | DecodeError::DuplicateOrOutOfOrderSequence
        | DecodeError::StreamOffsetGap { .. }
        | DecodeError::DuplicateOrOutOfOrderOffset
        | DecodeError::SequenceOverflow
        | DecodeError::StreamOffsetOverflow
        | DecodeError::InvalidSessionMarkers
        | DecodeError::MessageAfterSessionEnd
        | DecodeError::IncompleteSession
        | DecodeError::MissingRequiredChannel { .. }
        | DecodeError::ReservedChannelPayload { .. } => {
            Quarantined(IexHistTerminalReason::ContinuityFault)
        }
        DecodeError::ChunkTooLarge
        | DecodeError::PacketTooLarge
        | DecodeError::PacketLimit
        | DecodeError::MessageLimit
        | DecodeError::DecodedEventBatchBytesExceeded
        | DecodeError::ProviderTimestampStateLimit => {
            Quarantined(IexHistTerminalReason::ResourceLimitExceeded)
        }
        DecodeError::ReceiptMismatch
        | DecodeError::SummaryIdentityMismatch
        | DecodeError::PcapLengthMismatch
        | DecodeError::PcapChecksumMismatch
        | DecodeError::TruncatedPcap
        | DecodeError::InvalidPcapHeader
        | DecodeError::TruncatedPacket
        | DecodeError::UnsupportedPacket
        | DecodeError::InvalidIpv4Checksum
        | DecodeError::MalformedUdpLength
        | DecodeError::InvalidUdpChecksum
        | DecodeError::TruncatedTransport
        | DecodeError::WrongFeedOrChannel
        | DecodeError::MalformedTransportLength
        | DecodeError::MalformedMessageLength
        | DecodeError::TruncatedMessage
        | DecodeError::InvalidPriceOrSize
        | DecodeError::InvalidText
        | DecodeError::InvalidMessageValue => {
            Quarantined(IexHistTerminalReason::CorruptRawEvidence)
        }
        DecodeError::Poisoned | DecodeError::Serialization | DecodeError::SinkCommitMismatch => {
            Quarantined(IexHistTerminalReason::DownstreamIntegrityFault)
        }
        DecodeError::Capacity | DecodeError::SinkRejected => Failed,
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
    fn from_permit(
        capacity_permit: &IexHistExecutionPermit,
    ) -> Result<Self, TransportErrorKind> {
        let deadline_unix_nanos = capacity_permit.attempt().deadline_unix_nanos();
        let now_unix_nanos = capacity_permit
            .trusted_clock()
            .map_err(TransportErrorKind::CapacityAuthority)?
            .unix_nanos();
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
    authority_clock: crate::planning::IexHistTrustedClockReading,
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
    let retry_unix_nanos = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| TransportErrorKind::InvalidRetryAfter)?
        .as_nanos();
    let retry_unix_nanos = i128::try_from(retry_unix_nanos)
        .map_err(|_| TransportErrorKind::InvalidRetryAfter)?;
    let now_unix_nanos = i128::from(authority_clock.unix_nanos());
    let wait_nanos = retry_unix_nanos.saturating_sub(now_unix_nanos);
    let wait_nanos = u64::try_from(wait_nanos).unwrap_or(u64::MAX);
    Ok(Some(Duration::from_nanos(wait_nanos)))
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
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
    /// Shared provider/network/durable-capacity authority refused or failed the operation.
    #[error("IEX HIST application capacity authority failed: {0}")]
    CapacityAuthority(IexHistCapacityError),
    /// Staging directory was not a direct directory.
    #[error("IEX HIST staging directory is unsafe")]
    UnsafeStagingDirectory,
    /// Staging file I/O failed.
    #[error("IEX HIST staging I/O failed")]
    StagingIo,
    /// Complete bytes carried quarantined capture-clock evidence and cannot be decoded/published.
    #[error("IEX HIST capture chronology is quarantined")]
    ChronologyQuarantined,
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
    capacity_permit: IexHistExecutionPermit,
    cancellation: &CancellationToken,
) -> Result<MaterializedIexCapture, IexHistTransportError> {
    let staging_directory = capacity_permit
        .staging_directory()
        .map_err(|error| {
            IexHistTransportError::new(
                TransportErrorKind::CapacityAuthority(error),
                TransportTelemetry::new(),
            )
        })?
        .to_path_buf();
    let deadline = Deadline::from_permit(&capacity_permit)
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
        &staging_directory,
        deadline,
        cancellation,
        telemetry,
        capacity_permit,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn decode_mock_pcap<S: IexEventSink>(
    plan: &ColdJobPlan,
    capture: &PcapMaterializationReceipt,
    pcap: File,
    capacity_permit: IexHistExecutionPermit,
    cancellation: &CancellationToken,
    sink: S,
) -> Result<DecodedIexCapture<S>, IexHistTransportError> {
    let deadline = Deadline::from_permit(&capacity_permit)
        .map_err(|kind| IexHistTransportError::new(kind, TransportTelemetry::new()))?;
    decode_sealed_pcap_file(
        plan,
        capture,
        pcap,
        deadline,
        cancellation,
        sink,
        TransportTelemetry::new(),
        capacity_permit,
    )
    .await
}
