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
    ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG,
    IF_RANGE, RANGE, RETRY_AFTER, USER_AGENT,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::catalog::{Catalog, CatalogError, CatalogTransportMetadata, MAX_CATALOG_BYTES};
use crate::decode::{
    DecodeActuals, DecodeError, DecodeFailure, DecodeSummary, IexEventSink, PcapStreamDecoder,
};
use crate::durable::IexHistResumeClaim;
use crate::model::{PcapObjectEncoding, Sha256Digest};
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

    fn add_staged_provider_object_bytes(&mut self, bytes: usize) -> Result<(), TransportErrorKind> {
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
    pub fn into_parts(self) -> (Catalog, Bytes, TransportTelemetry, IexHistExecutionPermit) {
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

/// Caller-controlled physical prefix and exact shared-adoption receipt for a later range attempt.
///
/// Construction is intentionally cheap and untrusted. [`IexHistColdTransport::resume_materialize`]
/// revalidates the adoption receipt, claim, exact file length, and SHA-256 before issuing any
/// request.
pub struct IexHistResumeCandidate {
    adoption: IexHistResumeAdoptionReceipt,
    controlled_provider_object: File,
}

impl fmt::Debug for IexHistResumeCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IexHistResumeCandidate")
            .field("claim_sha256", &self.adoption.claim().claim_sha256())
            .field("adoption_receipt_sha256", &self.adoption.receipt_sha256())
            .field("controlled_provider_object", &"opaque-controlled-file")
            .finish()
    }
}

impl IexHistResumeCandidate {
    /// Binds an untrusted reopened controlled file to the shared-adoption receipt.
    #[must_use]
    pub const fn new(
        adoption: IexHistResumeAdoptionReceipt,
        controlled_provider_object: File,
    ) -> Self {
        Self {
            adoption,
            controlled_provider_object,
        }
    }
}

/// Why a transfer stopped after yielding a safely claimable nonempty prefix.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IexHistResumeCause {
    /// The response body stream failed after at least one new byte.
    Network,
}

const RESUME_ADOPTION_SCHEMA_VERSION: u16 = 1;

/// Fixed-size interruption telemetry retained across restart without an unbounded retry vector.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IexHistResumeTelemetryEvidence {
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
    retry_count: u8,
    retries: [Option<RetryObservation>; MAX_RETRY_ATTEMPTS as usize],
}

impl IexHistResumeTelemetryEvidence {
    fn try_from_runtime(
        telemetry: &TransportTelemetry,
    ) -> Result<Self, IexHistResumeAdoptionBindingError> {
        let retry_count = u8::try_from(telemetry.retries.len())
            .map_err(|_| IexHistResumeAdoptionBindingError::InvalidBinding)?;
        if retry_count > MAX_RETRY_ATTEMPTS {
            return Err(IexHistResumeAdoptionBindingError::InvalidBinding);
        }
        let mut retries = [None; MAX_RETRY_ATTEMPTS as usize];
        for (target, observation) in retries.iter_mut().zip(&telemetry.retries) {
            *target = Some(*observation);
        }
        let evidence = Self {
            attempts_total: telemetry.attempts_total,
            http_429_total: telemetry.http_429_total,
            http_503_total: telemetry.http_503_total,
            network_failures_total: telemetry.network_failures_total,
            response_bytes: telemetry.response_bytes,
            expanded_pcap_bytes: telemetry.expanded_pcap_bytes,
            staged_provider_object_bytes: telemetry.staged_provider_object_bytes,
            staged_pcap_bytes: telemetry.staged_pcap_bytes,
            staged_decoded_event_batch_bytes: telemetry.staged_decoded_event_batch_bytes,
            last_status: telemetry.last_status,
            retry_count,
            retries,
        };
        evidence.validate()?;
        Ok(evidence)
    }

    fn validate(self) -> Result<(), IexHistResumeAdoptionBindingError> {
        let retry_count = usize::from(self.retry_count);
        if self.attempts_total == 0
            || self.attempts_total > MAX_RETRY_ATTEMPTS
            || self.http_429_total > self.attempts_total
            || self.http_503_total > self.attempts_total
            || self.network_failures_total > self.attempts_total
            || retry_count > self.retries.len()
            || self.retries[..retry_count].iter().any(Option::is_none)
            || self.retries[retry_count..].iter().any(Option::is_some)
            || self.retries[..retry_count]
                .iter()
                .flatten()
                .any(|retry| retry.attempt == 0 || retry.attempt >= self.attempts_total)
        {
            return Err(IexHistResumeAdoptionBindingError::InvalidBinding);
        }
        Ok(())
    }

    /// Returns exact request or response-stream network failures.
    #[must_use]
    pub const fn network_failures_total(self) -> u8 {
        self.network_failures_total
    }

    /// Returns exact response bytes received in the interrupted segment.
    #[must_use]
    pub const fn response_bytes(self) -> u64 {
        self.response_bytes
    }

    /// Returns exact full-prefix bytes written to the controlled staging object.
    #[must_use]
    pub const fn staged_provider_object_bytes(self) -> u64 {
        self.staged_provider_object_bytes
    }
}

/// Common-platform physical receipt coordinates required by the adapter rejoin.
///
/// The adapter deliberately does not implement this trait for any local storage type. The shared
/// physical store implements it on its non-forgeable seal receipt (or a composition-owned wrapper)
/// so the adapter can verify exact bytes without acquiring storage or publication authority.
pub trait IexHistSharedPhysicalSealReceipt {
    /// Identity of the shared durable volume that owns the sealed prefix.
    fn storage_root_sha256(&self) -> Sha256Digest;
    /// SHA-256 of the exact provider-object prefix from byte zero.
    fn object_sha256(&self) -> Sha256Digest;
    /// Exact sealed provider-object prefix bytes.
    fn object_bytes(&self) -> u64;
    /// Nonzero identity of the shared platform's physical seal receipt.
    fn physical_receipt_sha256(&self) -> Sha256Digest;
}

/// One-use request that transfers an incomplete prefix to shared physical storage.
///
/// It carries the exact claim, interruption, and telemetry but no execution permit. Only
/// [`IexHistPendingResume::try_adopt`] retains and settles that permit.
pub struct IexHistResumeAdoptionRequest {
    claim: IexHistResumeClaim,
    provider_object: NamedTempFile,
    cause: IexHistResumeCause,
    telemetry: TransportTelemetry,
}

impl fmt::Debug for IexHistResumeAdoptionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IexHistResumeAdoptionRequest")
            .field("claim_sha256", &self.claim.claim_sha256())
            .field("cause", &self.cause)
            .field("telemetry", &self.telemetry)
            .field("provider_object", &"opaque-temporary-file")
            .finish()
    }
}

impl IexHistResumeAdoptionRequest {
    /// Returns the exact incomplete-prefix claim being physically adopted.
    #[must_use]
    pub const fn claim(&self) -> &IexHistResumeClaim {
        &self.claim
    }

    /// Returns the exact interruption category bound to this request.
    #[must_use]
    pub const fn cause(&self) -> IexHistResumeCause {
        self.cause
    }

    /// Returns exact request and byte telemetry bound to this request.
    #[must_use]
    pub const fn telemetry(&self) -> &TransportTelemetry {
        &self.telemetry
    }

    /// Consumes the request into evidence and the one-use temporary file.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        IexHistResumeClaim,
        NamedTempFile,
        IexHistResumeCause,
        TransportTelemetry,
    ) {
        (self.claim, self.provider_object, self.cause, self.telemetry)
    }
}

/// Shared-platform adopter invoked while the selected-file capacity permit remains private.
pub trait IexHistResumePhysicalAdopter {
    /// Non-forgeable physical receipt returned by the shared storage boundary.
    type Receipt: IexHistSharedPhysicalSealReceipt;
    /// Shared adoption failure retained by the caller.
    type Error;

    /// Consumes and physically seals the exact one-use prefix request.
    fn adopt(
        &mut self,
        request: IexHistResumeAdoptionRequest,
    ) -> Result<Self::Receipt, Self::Error>;
}

/// Serializable join between resumable provider evidence and a shared physical seal receipt.
///
/// This receipt is composition lineage, not storage authority: its physical coordinates must come
/// from [`IexHistResumePhysicalAdopter`] and are revalidated before every range request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IexHistResumeAdoptionReceipt {
    schema_version: u16,
    claim: IexHistResumeClaim,
    cause: IexHistResumeCause,
    telemetry: IexHistResumeTelemetryEvidence,
    storage_root_sha256: Sha256Digest,
    object_sha256: Sha256Digest,
    object_bytes: u64,
    physical_receipt_sha256: Sha256Digest,
    receipt_sha256: Sha256Digest,
}

impl IexHistResumeAdoptionReceipt {
    fn try_bind<R: IexHistSharedPhysicalSealReceipt>(
        plan: &ColdJobPlan,
        claim: IexHistResumeClaim,
        cause: IexHistResumeCause,
        telemetry: TransportTelemetry,
        physical: &R,
    ) -> Result<Self, IexHistResumeAdoptionBindingError> {
        let telemetry = IexHistResumeTelemetryEvidence::try_from_runtime(&telemetry)?;
        let receipt_sha256 = resume_adoption_identity(
            &claim,
            cause,
            &telemetry,
            physical.storage_root_sha256(),
            physical.object_sha256(),
            physical.object_bytes(),
            physical.physical_receipt_sha256(),
        );
        let receipt = Self {
            schema_version: RESUME_ADOPTION_SCHEMA_VERSION,
            claim,
            cause,
            telemetry,
            storage_root_sha256: physical.storage_root_sha256(),
            object_sha256: physical.object_sha256(),
            object_bytes: physical.object_bytes(),
            physical_receipt_sha256: physical.physical_receipt_sha256(),
            receipt_sha256,
        };
        receipt.validate_against(plan)?;
        Ok(receipt)
    }

    /// Revalidates exact claim, interruption, telemetry, and shared physical coordinates.
    pub fn validate_against(
        &self,
        plan: &ColdJobPlan,
    ) -> Result<(), IexHistResumeAdoptionBindingError> {
        self.claim
            .validate_against(plan)
            .map_err(|_| IexHistResumeAdoptionBindingError::InvalidBinding)?;
        self.telemetry.validate()?;
        let segment_bytes = self
            .claim
            .prefix_bytes()
            .checked_sub(self.claim.segment_start_bytes())
            .ok_or(IexHistResumeAdoptionBindingError::InvalidBinding)?;
        if self.schema_version != RESUME_ADOPTION_SCHEMA_VERSION
            || self.telemetry.network_failures_total() == 0
            || self.telemetry.response_bytes() != segment_bytes
            || self.telemetry.staged_provider_object_bytes() != self.claim.prefix_bytes()
            || self.storage_root_sha256 != self.claim.segment_attempt().storage_root_sha256()
            || self.object_sha256 != self.claim.prefix_sha256()
            || self.object_bytes != self.claim.prefix_bytes()
            || !nonzero_sha256(self.physical_receipt_sha256)
            || resume_adoption_identity(
                &self.claim,
                self.cause,
                &self.telemetry,
                self.storage_root_sha256,
                self.object_sha256,
                self.object_bytes,
                self.physical_receipt_sha256,
            ) != self.receipt_sha256
        {
            return Err(IexHistResumeAdoptionBindingError::InvalidBinding);
        }
        Ok(())
    }

    /// Returns the exact resumable-prefix claim.
    #[must_use]
    pub const fn claim(&self) -> &IexHistResumeClaim {
        &self.claim
    }

    /// Returns the exact interruption category.
    #[must_use]
    pub const fn cause(&self) -> IexHistResumeCause {
        self.cause
    }

    /// Returns exact interruption telemetry.
    #[must_use]
    pub const fn telemetry(&self) -> IexHistResumeTelemetryEvidence {
        self.telemetry
    }

    /// Returns the shared platform's exact physical receipt identity.
    #[must_use]
    pub const fn physical_receipt_sha256(&self) -> Sha256Digest {
        self.physical_receipt_sha256
    }

    /// Returns the complete adoption binding identity.
    #[must_use]
    pub const fn receipt_sha256(&self) -> Sha256Digest {
        self.receipt_sha256
    }
}

/// Invalid shared physical sidecar for a resumable provider prefix.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum IexHistResumeAdoptionBindingError {
    /// Claim, interruption telemetry, or physical seal coordinates did not bind exactly.
    #[error("IEX HIST resumable-prefix shared-adoption binding is invalid")]
    InvalidBinding,
}

/// Complete shared adoption, retaining the shared receipt itself beside the serializable join.
#[derive(Debug)]
pub struct IexHistAdoptedResume<R> {
    adoption: IexHistResumeAdoptionReceipt,
    physical_receipt: R,
}

impl<R> IexHistAdoptedResume<R> {
    /// Returns the serializable exact adoption join.
    #[must_use]
    pub const fn adoption(&self) -> &IexHistResumeAdoptionReceipt {
        &self.adoption
    }

    /// Consumes the rejoin into the adoption evidence and shared physical receipt.
    #[must_use]
    pub fn into_parts(self) -> (IexHistResumeAdoptionReceipt, R) {
        (self.adoption, self.physical_receipt)
    }
}

/// Failure while a pending prefix is adopted and the private permit settles interrupted.
#[derive(Debug)]
pub enum IexHistResumeAdoptionError<E> {
    /// Shared physical adoption failed; an optional secondary settlement failure is retained.
    Shared {
        /// Original shared-store failure.
        error: E,
        /// Secondary failure while releasing the private permit interrupted.
        settlement_error: Option<IexHistCapacityError>,
    },
    /// Shared receipt did not bind to the exact claim; settlement failure is retained separately.
    InvalidPhysicalReceipt {
        /// Secondary failure while releasing the private permit interrupted.
        settlement_error: Option<IexHistCapacityError>,
    },
    /// Shared adoption succeeded, but the private permit could not settle interrupted.
    Settlement(IexHistCapacityError),
}

/// Incomplete provider-object handoff awaiting adoption by shared physical storage.
///
/// Neither the claim nor the temporary file is a durable checkpoint. The caller must consume and
/// adopt the file and atomically retain its own physical receipt with the claim while the permit
/// still protects capacity. The current permit has no partial-durability disposition and must be
/// released as interrupted after adoption; it must never be settled as checkpointed or completed.
pub struct IexHistPendingResume {
    claim: IexHistResumeClaim,
    provider_object: NamedTempFile,
    cause: IexHistResumeCause,
    telemetry: TransportTelemetry,
    capacity_permit: IexHistExecutionPermit,
}

impl fmt::Debug for IexHistPendingResume {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IexHistPendingResume")
            .field("claim_sha256", &self.claim.claim_sha256())
            .field("prefix_bytes", &self.claim.prefix_bytes())
            .field("cause", &self.cause)
            .field("telemetry", &self.telemetry)
            .field("provider_object", &"opaque-temporary-file")
            .finish_non_exhaustive()
    }
}

impl IexHistPendingResume {
    /// Returns the exact incomplete-prefix claim.
    #[must_use]
    pub const fn claim(&self) -> &IexHistResumeClaim {
        &self.claim
    }

    /// Returns the terminal interruption category retained with the claim.
    #[must_use]
    pub const fn cause(&self) -> IexHistResumeCause {
        self.cause
    }

    /// Returns exact attempt and byte telemetry accumulated in this response segment.
    #[must_use]
    pub const fn telemetry(&self) -> &TransportTelemetry {
        &self.telemetry
    }

    /// Physically adopts the exact prefix while retaining exclusive control of the capacity permit.
    ///
    /// Success and every failure path release the permit only as `Interrupted`; callers can never
    /// settle partial bytes as checkpointed or completed.
    pub fn try_adopt<A: IexHistResumePhysicalAdopter>(
        self,
        plan: &ColdJobPlan,
        adopter: &mut A,
    ) -> Result<IexHistAdoptedResume<A::Receipt>, IexHistResumeAdoptionError<A::Error>> {
        let Self {
            claim,
            provider_object,
            cause,
            telemetry,
            capacity_permit,
        } = self;
        if claim.validate_against(plan).is_err() {
            return Err(IexHistResumeAdoptionError::InvalidPhysicalReceipt {
                settlement_error: capacity_permit
                    .settle(crate::planning::IexHistCapacityDisposition::Interrupted)
                    .err(),
            });
        }
        let request = IexHistResumeAdoptionRequest {
            claim: claim.clone(),
            provider_object,
            cause,
            telemetry: telemetry.clone(),
        };
        let physical_receipt = match adopter.adopt(request) {
            Ok(receipt) => receipt,
            Err(error) => {
                return Err(IexHistResumeAdoptionError::Shared {
                    error,
                    settlement_error: capacity_permit
                        .settle(crate::planning::IexHistCapacityDisposition::Interrupted)
                        .err(),
                });
            }
        };
        let adoption = match IexHistResumeAdoptionReceipt::try_bind(
            plan,
            claim,
            cause,
            telemetry,
            &physical_receipt,
        ) {
            Ok(adoption) => adoption,
            Err(_) => {
                return Err(IexHistResumeAdoptionError::InvalidPhysicalReceipt {
                    settlement_error: capacity_permit
                        .settle(crate::planning::IexHistCapacityDisposition::Interrupted)
                        .err(),
                });
            }
        };
        capacity_permit
            .settle(crate::planning::IexHistCapacityDisposition::Interrupted)
            .map_err(IexHistResumeAdoptionError::Settlement)?;
        Ok(IexHistAdoptedResume {
            adoption,
            physical_receipt,
        })
    }
}

fn nonzero_sha256(value: Sha256Digest) -> bool {
    value.as_bytes().iter().any(|byte| *byte != 0)
}

fn resume_adoption_identity(
    claim: &IexHistResumeClaim,
    cause: IexHistResumeCause,
    telemetry: &IexHistResumeTelemetryEvidence,
    storage_root_sha256: Sha256Digest,
    object_sha256: Sha256Digest,
    object_bytes: u64,
    physical_receipt_sha256: Sha256Digest,
) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/iex-hist-resume-adoption/v1");
    hasher.update(claim.claim_sha256().as_bytes());
    hasher.update(match cause {
        IexHistResumeCause::Network => b"network".as_slice(),
    });
    hasher.update([telemetry.attempts_total]);
    hasher.update([telemetry.http_429_total]);
    hasher.update([telemetry.http_503_total]);
    hasher.update([telemetry.network_failures_total]);
    hasher.update(telemetry.response_bytes.to_le_bytes());
    hasher.update(telemetry.expanded_pcap_bytes.to_le_bytes());
    hasher.update(telemetry.staged_provider_object_bytes.to_le_bytes());
    hasher.update(telemetry.staged_pcap_bytes.to_le_bytes());
    hasher.update(telemetry.staged_decoded_event_batch_bytes.to_le_bytes());
    hasher.update([u8::from(telemetry.last_status.is_some())]);
    hasher.update(telemetry.last_status.unwrap_or_default().to_le_bytes());
    hasher.update([telemetry.retry_count]);
    for retry in telemetry.retries.iter().flatten() {
        hasher.update([retry.attempt]);
        hasher.update(retry.status.to_le_bytes());
        hasher.update([u8::from(retry.provider_retry_after_ms.is_some())]);
        hasher.update(
            retry
                .provider_retry_after_ms
                .unwrap_or_default()
                .to_le_bytes(),
        );
        hasher.update(retry.applied_wait_ms.to_le_bytes());
    }
    hasher.update(storage_root_sha256.as_bytes());
    hasher.update(object_sha256.as_bytes());
    hasher.update(object_bytes.to_le_bytes());
    hasher.update(physical_receipt_sha256.as_bytes());
    hasher.update(b"capacity-settlement/interrupted");
    Sha256Digest::from_bytes(hasher.finalize().into())
}

/// Selected-file transfer outcome: exact complete materialization or an adoptable prefix claim.
#[derive(Debug)]
pub enum IexHistDownloadOutcome {
    /// Exact complete provider object and materialized PCAP.
    Materialized(Box<MaterializedIexCapture>),
    /// Incomplete exact prefix awaiting shared physical adoption before resume.
    ResumePending(Box<IexHistPendingResume>),
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
    pub const fn summary(&self) -> &DecodeSummary {
        &self.summary
    }
    #[must_use]
    pub const fn telemetry(&self) -> &TransportTelemetry {
        &self.telemetry
    }
    #[must_use]
    pub fn into_parts(self) -> (DecodeSummary, S, TransportTelemetry, IexHistExecutionPermit) {
        (
            self.summary,
            self.sink,
            self.telemetry,
            self.capacity_permit,
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
        let request =
            IexHistCapacityRequest::catalog(footprint, deadline_unix_nanos).map_err(|error| {
                IexHistTransportError::new(
                    TransportErrorKind::CapacityAuthority(error),
                    TransportTelemetry::new(),
                )
            })?;
        let mut capacity_permit =
            IexHistExecutionPermit::acquire(capacity_authority, request, None).map_err(
                |error| {
                    IexHistTransportError::new(
                        TransportErrorKind::CapacityAuthority(error),
                        TransportTelemetry::new(),
                    )
                },
            )?;
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
                let body =
                    collect_catalog_body(&mut stream, deadline, cancellation, &mut telemetry)
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
                    let _ =
                        capacity_permit.settle(crate::planning::IexHistCapacityDisposition::Failed);
                    return Err(IexHistTransportError::new(
                        TransportErrorKind::CapacityAuthority(error),
                        telemetry,
                    ));
                }
                Ok(CatalogFetch {
                    catalog,
                    exact_body,
                    telemetry,
                    capacity_permit,
                })
            }
            Err(error) => {
                let _ = capacity_permit.record_usage(
                    IexHistCapacityCategory::NetworkResponse,
                    telemetry.response_bytes,
                );
                let settlement =
                    capacity_permit.settle(crate::planning::IexHistCapacityDisposition::Failed);
                Err(IexHistTransportError::new(
                    settlement
                        .err()
                        .map_or(error.kind, TransportErrorKind::CapacityAuthority),
                    telemetry,
                ))
            }
        }
    }

    /// Downloads, stages, expands, and receipts one exact selected file from byte zero.
    ///
    /// No retry occurs after response-body streaming starts. When a nonempty prefix and a strong
    /// validator exist, interruption returns a pending claim and controlled temporary file for
    /// shared physical adoption. Successful temporary files still have no durable authority until
    /// the application consumes them into shared immutable storage.
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
    ) -> Result<IexHistDownloadOutcome, IexHistTransportError> {
        let (capacity_permit, staging_directory, deadline) =
            acquire_materialization_permit(plan, capacity_authority, deadline_unix_nanos)?;
        let mut telemetry = TransportTelemetry::new();
        let response = self
            .request_selected_stream(
                plan,
                &capacity_permit,
                deadline,
                cancellation,
                &mut telemetry,
                None,
            )
            .await;
        let (metadata, stream) = match response {
            Ok(value) => value,
            Err(error) => {
                return Err(settle_transfer_failure(
                    capacity_permit,
                    error,
                    telemetry,
                    plan.object_encoding,
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
            None,
        )
        .await
    }

    /// Revalidates an application-controlled prefix, then requests only its exact remaining range.
    ///
    /// A full response, changed/weak validator, or malformed range fails closed. The caller may
    /// start a distinct byte-zero attempt; this method never mixes such bytes into the prefix.
    pub async fn resume_materialize(
        &self,
        plan: &ColdJobPlan,
        capacity_authority: &dyn IexHistCapacityAuthority,
        deadline_unix_nanos: i64,
        cancellation: &CancellationToken,
        candidate: IexHistResumeCandidate,
    ) -> Result<IexHistDownloadOutcome, IexHistTransportError> {
        candidate.adoption.validate_against(plan).map_err(|_| {
            IexHistTransportError::new(
                TransportErrorKind::Capture(CaptureError::InvalidResumeClaim),
                TransportTelemetry::new(),
            )
        })?;
        let (capacity_permit, staging_directory, deadline) =
            acquire_materialization_permit(plan, capacity_authority, deadline_unix_nanos)?;
        let mut telemetry = TransportTelemetry::new();
        let verified = verify_resume_candidate(
            plan,
            candidate,
            &staging_directory,
            deadline,
            cancellation,
            &mut telemetry,
        )
        .await;
        let (resume_adoption, provider_object) = match verified {
            Ok(value) => value,
            Err(kind) => {
                let error = IexHistTransportError::new(kind, telemetry.clone());
                return Err(settle_transfer_failure(
                    capacity_permit,
                    error,
                    telemetry,
                    plan.object_encoding,
                ));
            }
        };
        let response = self
            .request_selected_stream(
                plan,
                &capacity_permit,
                deadline,
                cancellation,
                &mut telemetry,
                Some(resume_adoption.claim()),
            )
            .await;
        let (metadata, stream) = match response {
            Ok(value) => value,
            Err(error) => {
                return Err(settle_transfer_failure(
                    capacity_permit,
                    error,
                    telemetry,
                    plan.object_encoding,
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
            Some((resume_adoption, provider_object)),
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the request loop retains its exact plan, permit, deadline, telemetry, and range"
    )]
    async fn request_selected_stream(
        &self,
        plan: &ColdJobPlan,
        capacity_permit: &IexHistExecutionPermit,
        deadline: Deadline,
        cancellation: &CancellationToken,
        telemetry: &mut TransportTelemetry,
        resume_claim: Option<&IexHistResumeClaim>,
    ) -> Result<(CaptureResponseMetadata, ByteStream), IexHistTransportError> {
        loop {
            let attempt = telemetry
                .begin_attempt()
                .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
            let mut request = self
                .client
                .get(&plan.selected_file.download_url)
                .header(ACCEPT, "application/gzip, application/octet-stream")
                .header(ACCEPT_ENCODING, "identity")
                .header(USER_AGENT, USER_AGENT_VALUE);
            if let Some(claim) = resume_claim {
                request = request
                    .header(RANGE, format!("bytes={}-", claim.prefix_bytes()))
                    .header(IF_RANGE, claim.strong_etag());
            }
            let response = match await_deadline(request.send(), deadline, cancellation).await {
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
            match (resume_claim, status) {
                (None, 200) | (Some(_), 206) => {}
                (Some(_), _) => {
                    return Err(IexHistTransportError::new(
                        TransportErrorKind::ResumeNotHonored { status },
                        telemetry.clone(),
                    ));
                }
                (None, _) => {
                    return Err(IexHistTransportError::new(
                        TransportErrorKind::HttpStatus { status },
                        telemetry.clone(),
                    ));
                }
            }
            let metadata = file_response_metadata(plan, &response, capacity_permit, resume_claim)
                .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
            let stream: ByteStream = Box::pin(
                response
                    .bytes_stream()
                    .map(|item| item.map_err(|_| StreamFailure::Network)),
            );
            return Ok((metadata, stream));
        }
    }

    /// Re-reads and decodes one application-sealed complete PCAP from byte zero.
    ///
    /// The caller supplies an already opened controlled object rather than a path. The decoder
    /// independently rechecks its exact byte count and SHA-256 against the acquisition receipt.
    /// The decoder owns `sink`: failure aborts its staged transaction, while success returns the
    /// committed sink with the exact [`DecodeSummary`].
    #[allow(
        clippy::too_many_arguments,
        reason = "the decode authority boundary carries its exact plan, capture, file, authority, deadline, cancellation, and sink"
    )]
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
        let capacity_permit =
            IexHistExecutionPermit::acquire(capacity_authority, request, Some(plan)).map_err(
                |error| {
                    IexHistTransportError::new(
                        TransportErrorKind::CapacityAuthority(error),
                        TransportTelemetry::new(),
                    )
                },
            )?;
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

fn acquire_materialization_permit(
    plan: &ColdJobPlan,
    capacity_authority: &dyn IexHistCapacityAuthority,
    deadline_unix_nanos: i64,
) -> Result<(IexHistExecutionPermit, std::path::PathBuf, Deadline), IexHistTransportError> {
    let authority_free_reserve_bytes =
        capacity_authority
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
    let capacity_permit = IexHistExecutionPermit::acquire(capacity_authority, request, Some(plan))
        .map_err(|error| {
            IexHistTransportError::new(
                TransportErrorKind::CapacityAuthority(error),
                TransportTelemetry::new(),
            )
        })?;
    let staging_directory = match capacity_permit.staging_directory() {
        Ok(directory) => directory.to_path_buf(),
        Err(error) => {
            let _ = capacity_permit.settle(crate::planning::IexHistCapacityDisposition::Failed);
            return Err(IexHistTransportError::new(
                TransportErrorKind::CapacityAuthority(error),
                TransportTelemetry::new(),
            ));
        }
    };
    let deadline = match Deadline::from_permit(&capacity_permit) {
        Ok(deadline) => deadline,
        Err(kind) => {
            let _ = capacity_permit.settle(crate::planning::IexHistCapacityDisposition::Failed);
            return Err(IexHistTransportError::new(kind, TransportTelemetry::new()));
        }
    };
    Ok((capacity_permit, staging_directory, deadline))
}

fn settle_transfer_failure(
    mut capacity_permit: IexHistExecutionPermit,
    error: IexHistTransportError,
    telemetry: TransportTelemetry,
    object_encoding: PcapObjectEncoding,
) -> IexHistTransportError {
    let usage_error =
        record_materialization_usage(&mut capacity_permit, object_encoding, &telemetry).err();
    let disposition = terminal_disposition(&error.kind);
    let settlement = capacity_permit.settle(disposition);
    IexHistTransportError::new(
        usage_error
            .or_else(|| settlement.err())
            .map_or(error.kind, TransportErrorKind::CapacityAuthority),
        telemetry,
    )
}

async fn verify_resume_candidate(
    plan: &ColdJobPlan,
    candidate: IexHistResumeCandidate,
    staging_directory: &Path,
    deadline: Deadline,
    cancellation: &CancellationToken,
    telemetry: &mut TransportTelemetry,
) -> Result<(IexHistResumeAdoptionReceipt, NamedTempFile), TransportErrorKind> {
    let IexHistResumeCandidate {
        adoption,
        controlled_provider_object,
    } = candidate;
    adoption
        .validate_against(plan)
        .map_err(|_| TransportErrorKind::Capture(CaptureError::InvalidResumeClaim))?;
    let claim = adoption.claim();
    let metadata = controlled_provider_object
        .metadata()
        .map_err(|_| TransportErrorKind::StagingIo)?;
    if !metadata.is_file() || metadata.len() != claim.prefix_bytes() {
        return Err(TransportErrorKind::Capture(
            CaptureError::ResumePrefixMismatch,
        ));
    }
    let provider_object = create_staged_file(staging_directory)?;
    let mut reader = tokio::fs::File::from_std(controlled_provider_object);
    let mut writer = tokio::fs::File::from_std(
        provider_object
            .reopen()
            .map_err(|_| TransportErrorKind::StagingIo)?,
    );
    await_deadline(
        reader.seek(std::io::SeekFrom::Start(0)),
        deadline,
        cancellation,
    )
    .await?
    .map_err(|_| TransportErrorKind::StagingIo)?;
    let mut hasher = Sha256::new();
    let mut bytes_read = 0_u64;
    let mut buffer = zeroed_buffer(STREAM_BUFFER_BYTES)?;
    loop {
        let read = read_deadline(&mut reader, &mut buffer, deadline, cancellation).await?;
        if read == 0 {
            break;
        }
        bytes_read = bytes_read
            .checked_add(u64::try_from(read).map_err(|_| TransportErrorKind::ByteLimit)?)
            .ok_or(TransportErrorKind::ByteLimit)?;
        if bytes_read > claim.prefix_bytes() {
            return Err(TransportErrorKind::Capture(
                CaptureError::ResumePrefixMismatch,
            ));
        }
        hasher.update(&buffer[..read]);
        write_all_deadline(&mut writer, &buffer[..read], deadline, cancellation).await?;
        telemetry.add_staged_provider_object_bytes(read)?;
        if plan.object_encoding() == PcapObjectEncoding::Identity {
            telemetry.add_staged_pcap_bytes(read)?;
            telemetry.expanded_pcap_bytes = telemetry
                .expanded_pcap_bytes
                .checked_add(u64::try_from(read).map_err(|_| TransportErrorKind::ByteLimit)?)
                .ok_or(TransportErrorKind::ByteLimit)?;
        }
    }
    flush_sync(&mut writer, deadline, cancellation).await?;
    drop(writer);
    let prefix_sha256 = Sha256Digest::from_bytes(hasher.finalize().into());
    if bytes_read != claim.prefix_bytes() || prefix_sha256 != claim.prefix_sha256() {
        return Err(TransportErrorKind::Capture(
            CaptureError::ResumePrefixMismatch,
        ));
    }
    Ok((adoption, provider_object))
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
    resume_claim: Option<&IexHistResumeClaim>,
) -> Result<CaptureResponseMetadata, TransportErrorKind> {
    let content_length = parse_content_length(response.headers())?;
    let range_start = resume_claim.map_or(0, IexHistResumeClaim::prefix_bytes);
    let expected_content_length = plan
        .advertised_compressed_bytes
        .checked_sub(range_start)
        .ok_or(TransportErrorKind::InvalidResponseMetadata)?;
    let content_range = singleton_header(response.headers(), CONTENT_RANGE)?;
    let range_matches = match resume_claim {
        None => content_range.is_none(),
        Some(_) => exact_content_range_matches(
            content_range.as_deref(),
            range_start,
            plan.advertised_compressed_bytes,
        ),
    };
    if content_length != expected_content_length || !range_matches {
        return Err(TransportErrorKind::InvalidResponseMetadata);
    }
    Ok(CaptureResponseMetadata {
        response_url: response.url().as_str().to_owned(),
        status: response.status().as_u16(),
        range_start,
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
    resumed: Option<(IexHistResumeAdoptionReceipt, NamedTempFile)>,
) -> Result<IexHistDownloadOutcome, IexHistTransportError> {
    let capture_started = Instant::now();
    let attempt = capacity_permit.attempt();
    let operation = async {
        let (mut receipt, provider_object) = match resumed {
            Some((adoption, provider_object)) => (
                GzipPcapReceiptBuilder::resume(plan, attempt, metadata, adoption)
                    .map_err(TransportErrorKind::Capture)?,
                provider_object,
            ),
            None => (
                GzipPcapReceiptBuilder::new(plan, attempt, metadata)
                    .map_err(TransportErrorKind::Capture)?,
                create_staged_file(staging_directory)?,
            ),
        };
        if receipt.segment_start_bytes() > 0 {
            rehash_staged_prefix(
                &provider_object,
                plan.object_encoding,
                &mut receipt,
                deadline,
                cancellation,
            )
            .await?;
        }
        let mut provider_object_writer = tokio::fs::File::from_std(
            provider_object
                .reopen()
                .map_err(|_| TransportErrorKind::StagingIo)?,
        );
        let append_position = await_deadline(
            provider_object_writer.seek(std::io::SeekFrom::End(0)),
            deadline,
            cancellation,
        )
        .await?
        .map_err(|_| TransportErrorKind::StagingIo)?;
        if append_position != receipt.segment_start_bytes() {
            return Err(TransportErrorKind::Capture(
                CaptureError::ResumePrefixMismatch,
            ));
        }

        loop {
            let next = match next_stream_item(&mut stream, deadline, cancellation).await {
                Ok(next) => next,
                Err(kind) => return Err(kind),
            };
            let Some(chunk) = next else {
                if receipt.compressed_bytes() != plan.advertised_compressed_bytes {
                    let kind = TransportErrorKind::Network;
                    telemetry.record_network_failure()?;
                    if receipt.compressed_bytes() > receipt.segment_start_bytes()
                        && let Some(claim) = checkpoint_interrupted_stream(
                            plan,
                            &receipt,
                            &mut provider_object_writer,
                            &capacity_permit,
                            capture_started,
                            deadline,
                            cancellation,
                        )
                        .await
                    {
                        return Ok(StreamMaterialization::Pending(Box::new(
                            PendingStreamMaterialization {
                                claim,
                                provider_object,
                                cause: IexHistResumeCause::Network,
                            },
                        )));
                    }
                    return Err(kind);
                }
                break;
            };
            let chunk = match admit_stream_chunk(chunk, &mut telemetry) {
                Ok(chunk) => chunk,
                Err(kind) => {
                    if receipt.compressed_bytes() > receipt.segment_start_bytes()
                        && let Some(claim) = checkpoint_interrupted_stream(
                            plan,
                            &receipt,
                            &mut provider_object_writer,
                            &capacity_permit,
                            capture_started,
                            deadline,
                            cancellation,
                        )
                        .await
                    {
                        return Ok(StreamMaterialization::Pending(Box::new(
                            PendingStreamMaterialization {
                                claim,
                                provider_object,
                                cause: IexHistResumeCause::Network,
                            },
                        )));
                    }
                    return Err(kind);
                }
            };
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
            write_all_deadline(&mut provider_object_writer, &chunk, deadline, cancellation).await?;
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
                let mut output = zeroed_buffer(STREAM_BUFFER_BYTES)?;
                loop {
                    let read =
                        read_deadline(&mut gzip, &mut output, deadline, cancellation).await?;
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
                    write_all_deadline(&mut pcap_writer, &output[..read], deadline, cancellation)
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
        Ok(StreamMaterialization::Complete(Box::new(
            CompleteStreamMaterialization {
                materialization,
                staged_files: StagedCaptureFiles {
                    provider_object,
                    expanded_pcap,
                },
            },
        )))
    };
    match operation.await {
        Ok(StreamMaterialization::Complete(complete)) => {
            let CompleteStreamMaterialization {
                materialization,
                staged_files,
            } = *complete;
            if let Err(error) =
                record_materialization_usage(&mut capacity_permit, plan.object_encoding, &telemetry)
            {
                let _ = capacity_permit.settle(crate::planning::IexHistCapacityDisposition::Failed);
                return Err(IexHistTransportError::new(
                    TransportErrorKind::CapacityAuthority(error),
                    telemetry,
                ));
            }
            Ok(IexHistDownloadOutcome::Materialized(Box::new(
                MaterializedIexCapture {
                    materialization,
                    telemetry,
                    staged_files,
                    capacity_permit,
                },
            )))
        }
        Ok(StreamMaterialization::Pending(pending)) => {
            let PendingStreamMaterialization {
                claim,
                provider_object,
                cause,
            } = *pending;
            if let Err(error) =
                record_materialization_usage(&mut capacity_permit, plan.object_encoding, &telemetry)
            {
                let _ = capacity_permit.settle(crate::planning::IexHistCapacityDisposition::Failed);
                return Err(IexHistTransportError::new(
                    TransportErrorKind::CapacityAuthority(error),
                    telemetry,
                ));
            }
            Ok(IexHistDownloadOutcome::ResumePending(Box::new(
                IexHistPendingResume {
                    claim,
                    provider_object,
                    cause,
                    telemetry,
                    capacity_permit,
                },
            )))
        }
        Err(kind) => {
            let usage_error = record_materialization_usage(
                &mut capacity_permit,
                plan.object_encoding,
                &telemetry,
            )
            .err();
            let settlement = capacity_permit.settle(terminal_disposition(&kind));
            Err(IexHistTransportError::new(
                usage_error
                    .or_else(|| settlement.err())
                    .map_or(kind, TransportErrorKind::CapacityAuthority),
                telemetry,
            ))
        }
    }
}

enum StreamMaterialization {
    Complete(Box<CompleteStreamMaterialization>),
    Pending(Box<PendingStreamMaterialization>),
}

struct CompleteStreamMaterialization {
    materialization: PcapMaterializationReceipt,
    staged_files: StagedCaptureFiles,
}

struct PendingStreamMaterialization {
    claim: IexHistResumeClaim,
    provider_object: NamedTempFile,
    cause: IexHistResumeCause,
}

async fn checkpoint_interrupted_stream(
    plan: &ColdJobPlan,
    receipt: &GzipPcapReceiptBuilder,
    writer: &mut tokio::fs::File,
    capacity_permit: &IexHistExecutionPermit,
    capture_started: Instant,
    deadline: Deadline,
    cancellation: &CancellationToken,
) -> Option<IexHistResumeClaim> {
    // Checkpointing is opportunistic cleanup after a network interruption. It remains governed by
    // the original attempt and can never delay or replace that terminal network cause.
    flush_sync(writer, deadline, cancellation).await.ok()?;
    let checkpoint_clock = capacity_permit.trusted_clock().ok()?;
    let segment_duration_nanos = u64::try_from(capture_started.elapsed().as_nanos()).ok()?;
    receipt
        .checkpoint(plan, checkpoint_clock, segment_duration_nanos)
        .ok()
}

async fn rehash_staged_prefix(
    provider_object: &NamedTempFile,
    object_encoding: PcapObjectEncoding,
    receipt: &mut GzipPcapReceiptBuilder,
    deadline: Deadline,
    cancellation: &CancellationToken,
) -> Result<(), TransportErrorKind> {
    let mut reader = tokio::fs::File::from_std(
        provider_object
            .reopen()
            .map_err(|_| TransportErrorKind::StagingIo)?,
    );
    let mut buffer = zeroed_buffer(STREAM_BUFFER_BYTES)?;
    loop {
        let read = read_deadline(&mut reader, &mut buffer, deadline, cancellation).await?;
        if read == 0 {
            break;
        }
        receipt
            .push_compressed(&buffer[..read])
            .map_err(TransportErrorKind::Capture)?;
        if object_encoding == PcapObjectEncoding::Identity {
            receipt
                .push_pcap(&buffer[..read])
                .map_err(TransportErrorKind::Capture)?;
        }
    }
    receipt
        .verify_resume_prefix()
        .map_err(TransportErrorKind::Capture)
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
        capture.validate_against(plan).map_err(|error| {
            DecodeOperationFailure::without_actuals(TransportErrorKind::Capture(error))
        })?;
        if capture.chronology_disposition() != CaptureChronologyDisposition::Admitted {
            return Err(DecodeOperationFailure::without_actuals(
                TransportErrorKind::ChronologyQuarantined,
            ));
        }
        let decode_limits = plan.decode_contract().limits();
        let decode_attempt = capacity_permit
            .decode_attempt_evidence(plan)
            .map_err(|error| {
                DecodeOperationFailure::without_actuals(TransportErrorKind::CapacityAuthority(
                    error,
                ))
            })?;
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
        seek.map_err(|_| {
            DecodeOperationFailure::new(TransportErrorKind::StagingIo, decoder.actuals())
        })?;
        let mut output = zeroed_buffer(decode_limits.max_stream_chunk_bytes)
            .map_err(|kind| DecodeOperationFailure::new(kind, decoder.actuals()))?;
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
                    DecodeOperationFailure::new(TransportErrorKind::ByteLimit, decoder.actuals())
                })?)
                .ok_or_else(|| {
                    DecodeOperationFailure::new(TransportErrorKind::ByteLimit, decoder.actuals())
                })?;
            decoder
                .push(&output[..read])
                .map_err(DecodeOperationFailure::from_decode)?;
        }
        let (summary, sink) = decoder
            .finish()
            .map_err(DecodeOperationFailure::from_decode)?;
        let actuals = summary.actuals();
        summary
            .validate_against(plan, capture, decode_attempt)
            .map_err(|error| {
                DecodeOperationFailure::new(TransportErrorKind::Decode(error), actuals)
            })?;
        Ok((summary, sink, actuals))
    };
    match operation.await {
        Ok((summary, sink, actuals)) => {
            telemetry.staged_decoded_event_batch_bytes = actuals.decoded_event_batch_bytes_staged();
            if let Err(error) = record_decode_usage(&mut capacity_permit, actuals) {
                let _ = capacity_permit.settle(crate::planning::IexHistCapacityDisposition::Failed);
                return Err(IexHistTransportError::new(
                    TransportErrorKind::CapacityAuthority(error),
                    telemetry,
                ));
            }
            Ok(DecodedIexCapture {
                summary,
                sink,
                telemetry,
                capacity_permit,
            })
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
        Self::new(TransportErrorKind::Decode(failure.error), failure.actuals)
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

fn terminal_disposition(kind: &TransportErrorKind) -> crate::planning::IexHistCapacityDisposition {
    use crate::planning::IexHistCapacityDisposition::{Failed, Quarantined, Unavailable};

    match kind {
        TransportErrorKind::ChronologyQuarantined => {
            Quarantined(IexHistTerminalReason::ClockAnomaly)
        }
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
        | TransportErrorKind::ResumeNotHonored { .. }
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

fn decode_terminal_disposition(error: &DecodeError) -> crate::planning::IexHistCapacityDisposition {
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

fn zeroed_buffer(bytes: usize) -> Result<Vec<u8>, TransportErrorKind> {
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(bytes)
        .map_err(|_| TransportErrorKind::Capacity)?;
    buffer.resize(bytes, 0);
    Ok(buffer)
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
    fn from_permit(capacity_permit: &IexHistExecutionPermit) -> Result<Self, TransportErrorKind> {
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
    parse_canonical_u64(&value).ok_or(TransportErrorKind::InvalidResponseMetadata)
}

fn parse_canonical_u64(value: &str) -> Option<u64> {
    if value.is_empty()
        || value.len() > 20
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return None;
    }
    value.parse().ok()
}

pub(crate) fn exact_content_range_matches(
    value: Option<&str>,
    expected_start: u64,
    total_bytes: u64,
) -> bool {
    let Some(value) = value else {
        return false;
    };
    let Some(value) = value.strip_prefix("bytes ") else {
        return false;
    };
    let Some((range, total)) = value.split_once('/') else {
        return false;
    };
    let Some((start, end)) = range.split_once('-') else {
        return false;
    };
    expected_start > 0
        && expected_start < total_bytes
        && parse_canonical_u64(start) == Some(expected_start)
        && parse_canonical_u64(end) == total_bytes.checked_sub(1)
        && parse_canonical_u64(total) == Some(total_bytes)
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
    let retry_unix_nanos =
        i128::try_from(retry_unix_nanos).map_err(|_| TransportErrorKind::InvalidRetryAfter)?;
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
    /// A range request returned a full or otherwise non-partial status and was rejected unchanged.
    #[error("IEX HIST did not honor the exact range request (status {status})")]
    ResumeNotHonored {
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
pub(crate) enum MockStreamChunk {
    Bytes(Bytes),
}

#[cfg(test)]
pub(crate) async fn materialize_mock_stream(
    plan: &ColdJobPlan,
    metadata: CaptureResponseMetadata,
    chunks: Vec<MockStreamChunk>,
    capacity_permit: IexHistExecutionPermit,
    cancellation: &CancellationToken,
) -> Result<IexHistDownloadOutcome, IexHistTransportError> {
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
    let stream: ByteStream = Box::pin(futures_util::stream::iter(chunks.into_iter().map(
        |chunk| match chunk {
            MockStreamChunk::Bytes(bytes) => Ok(bytes),
        },
    )));
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
        None,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn resume_mock_stream(
    plan: &ColdJobPlan,
    metadata: CaptureResponseMetadata,
    chunks: Vec<MockStreamChunk>,
    capacity_permit: IexHistExecutionPermit,
    cancellation: &CancellationToken,
    candidate: IexHistResumeCandidate,
) -> Result<IexHistDownloadOutcome, IexHistTransportError> {
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
    let (adoption, provider_object) = verify_resume_candidate(
        plan,
        candidate,
        &staging_directory,
        deadline,
        cancellation,
        &mut telemetry,
    )
    .await
    .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
    telemetry
        .begin_attempt()
        .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
    telemetry
        .record_status(metadata.status)
        .map_err(|kind| IexHistTransportError::new(kind, telemetry.clone()))?;
    let stream: ByteStream = Box::pin(futures_util::stream::iter(chunks.into_iter().map(
        |chunk| match chunk {
            MockStreamChunk::Bytes(bytes) => Ok(bytes),
        },
    )));
    materialize_selected_stream(
        plan,
        metadata,
        stream,
        &staging_directory,
        deadline,
        cancellation,
        telemetry,
        capacity_permit,
        Some((adoption, provider_object)),
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
