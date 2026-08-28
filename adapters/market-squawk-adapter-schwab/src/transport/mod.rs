//! Bounded read-only network execution and exact provider-payload receipts.
//!
//! The transport accepts short-lived access-token values from an external protected authority.
//! It never serializes, persists, caches, or returns those values. All numeric bounds in this
//! module are caller-owned resource and retry policies, not Schwab capacity claims.

mod http;
mod streamer;

use std::fmt;
use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use market_squawk_domain::{MetadataRevision, SourceId, SourceIdentifier};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use zeroize::Zeroizing;

use crate::{ConnectionGeneration, ReadOnlyRoute, SchwabAdapterError};

pub(crate) use http::SchwabSealedRestResponseParts;
pub use http::{
    CapturedRestResponse, ExecutedRestResponse, ReqwestSchwabHttpWire, RestExecutionOutcome,
    RestItemAccounting, SchwabHttpWire, SchwabHttpWireRequest, SchwabHttpWireResponse,
    SchwabPendingRawRestCapture, SchwabPendingRestCapture, SchwabRawRestCaptureSealRejoin,
    SchwabRestCaptureSealRejoin, SchwabRestExecutor, SchwabRestFamily, SchwabRestPayload,
    SchwabSealedRawRestCapture, SchwabSealedRestResponse, SchwabUserPreferenceEvidence,
};
pub(crate) use streamer::SchwabSealedStreamerCaptureParts;
pub use streamer::{
    InboundStreamerFrame, ProductionSchwabStreamerConnector, RawStreamerFrame,
    RawStreamerFrameKind, SchwabPendingStreamerCapture, SchwabSealedStreamerCapture,
    SchwabStreamerConnection, SchwabStreamerConnectionControl,
    SchwabStreamerConnectionControlSource, SchwabStreamerConnectionEvidence,
    SchwabStreamerConnector, SchwabStreamerExecutor, SchwabStreamerFrameSealEvidence,
    SchwabStreamerServiceResponseEvidence, StreamerCaptureSink, StreamerCaptureSinkError,
    StreamerMicrobatch, StreamerMicrobatchReceipt, StreamerRunExit,
};

/// Source-neutral capture coordinates supplied by the registered provider/runtime owner.
///
/// This adapter never manufactures source identity, metadata revision, dataset identity, or
/// durable record UUIDs. The shared owner supplies them at the publication boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabCaptureCoordinates {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    connection_id: uuid::Uuid,
}

impl SchwabCaptureCoordinates {
    /// Binds exact registered-source coordinates to one capture conversion.
    pub fn try_new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        dataset: SourceIdentifier,
        connection_id: uuid::Uuid,
    ) -> Result<Self, SchwabTransportError> {
        if connection_id.is_nil() {
            return Err(SchwabTransportError::InvalidConfiguration);
        }
        Ok(Self {
            source_id,
            metadata_revision,
            dataset,
            connection_id,
        })
    }

    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    pub const fn connection_id(&self) -> uuid::Uuid {
        self.connection_id
    }
}

/// Opaque generation issued by the protected OAuth authority for one access token.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AccessTokenGeneration(NonZeroU64);

impl AccessTokenGeneration {
    /// Constructs an opaque nonzero generation.
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Returns the secret-free generation ordinal.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Caller-owned admission for transient bearer values.
///
/// `max_token_bytes` is a local memory/header-safety bound, not a provider token-size claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccessTokenAdmission {
    max_token_bytes: NonZeroUsize,
    minimum_remaining_lifetime: Duration,
}

impl AccessTokenAdmission {
    /// Constructs an explicit token admission.
    pub const fn new(max_token_bytes: NonZeroUsize, minimum_remaining_lifetime: Duration) -> Self {
        Self {
            max_token_bytes,
            minimum_remaining_lifetime,
        }
    }

    /// Maximum bearer bytes admitted into one immediate request.
    pub const fn max_token_bytes(self) -> usize {
        self.max_token_bytes.get()
    }

    /// Required remaining lifetime before beginning an operation.
    pub const fn minimum_remaining_lifetime(self) -> Duration {
        self.minimum_remaining_lifetime
    }
}

/// One transient access-token value returned by an external protected OAuth authority.
///
/// This type implements neither `Clone` nor serialization. Its bearer bytes zeroize on drop and
/// its `Debug` output is redacted.
pub struct TransientAccessToken {
    bearer: Zeroizing<String>,
    generation: AccessTokenGeneration,
    issued_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
}

impl TransientAccessToken {
    /// Constructs one bounded transient token generation.
    pub fn try_new(
        bearer: String,
        generation: AccessTokenGeneration,
        issued_at_unix_seconds: u64,
        expires_at_unix_seconds: u64,
        admission: AccessTokenAdmission,
    ) -> Result<Self, SchwabTransportError> {
        let bearer = Zeroizing::new(bearer);
        if bearer.is_empty()
            || bearer.len() > admission.max_token_bytes()
            || bearer.as_bytes().contains(&0)
            || bearer.contains(['\r', '\n'])
            || expires_at_unix_seconds <= issued_at_unix_seconds
        {
            return Err(SchwabTransportError::InvalidToken);
        }
        Ok(Self {
            bearer,
            generation,
            issued_at_unix_seconds,
            expires_at_unix_seconds,
        })
    }

    /// Opaque token generation safe for receipts and telemetry.
    pub const fn generation(&self) -> AccessTokenGeneration {
        self.generation
    }

    /// Secret-free issue time supplied by the OAuth authority.
    pub const fn issued_at_unix_seconds(&self) -> u64 {
        self.issued_at_unix_seconds
    }

    /// Secret-free expiration time supplied by the OAuth authority.
    pub const fn expires_at_unix_seconds(&self) -> u64 {
        self.expires_at_unix_seconds
    }

    /// Validates sufficient lifetime for an immediate operation.
    pub fn validate_at(
        &self,
        now_unix_seconds: u64,
        admission: AccessTokenAdmission,
    ) -> Result<(), SchwabTransportError> {
        let minimum = u64::try_from(admission.minimum_remaining_lifetime().as_secs())
            .map_err(|_| SchwabTransportError::InvalidToken)?;
        let required_until = now_unix_seconds
            .checked_add(minimum)
            .ok_or(SchwabTransportError::InvalidToken)?;
        if now_unix_seconds < self.issued_at_unix_seconds
            || required_until >= self.expires_at_unix_seconds
        {
            return Err(SchwabTransportError::TokenRefreshRequired);
        }
        Ok(())
    }

    pub(crate) fn expose_bearer(&self) -> &str {
        &self.bearer
    }
}

impl fmt::Debug for TransientAccessToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransientAccessToken")
            .field("bearer", &"[REDACTED]")
            .field("generation", &self.generation)
            .field("issued_at_unix_seconds", &self.issued_at_unix_seconds)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .finish()
    }
}

/// External protected authority capable of issuing an immediate transient access token.
pub trait SchwabAccessTokenSource: fmt::Debug + Send + Sync {
    /// Acquires the currently authorized token generation without exposing persistent storage.
    fn acquire(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<TransientAccessToken, TokenAuthorityError>> + Send + '_>>;
}

/// Secret-free failure reported by the external OAuth authority.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TokenAuthorityError {
    /// The protected authority is temporarily unavailable.
    #[error("Schwab access-token authority is unavailable")]
    Unavailable,
    /// Owner authorization must be repeated.
    #[error("Schwab owner reauthorization is required")]
    ReauthorizationRequired,
}

/// Explicit finite bounds for one REST transport instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestTransportBounds {
    connect_timeout: Duration,
    read_timeout: Duration,
    total_timeout: Duration,
    max_response_bytes: NonZeroUsize,
    max_header_count: NonZeroUsize,
    max_header_bytes: NonZeroUsize,
}

impl RestTransportBounds {
    /// Constructs local safety bounds. None of these values represents Schwab capacity.
    pub fn try_new(
        connect_timeout: Duration,
        read_timeout: Duration,
        total_timeout: Duration,
        max_response_bytes: NonZeroUsize,
        max_header_count: NonZeroUsize,
        max_header_bytes: NonZeroUsize,
    ) -> Result<Self, SchwabTransportError> {
        if connect_timeout.is_zero()
            || read_timeout.is_zero()
            || total_timeout.is_zero()
            || connect_timeout > total_timeout
            || read_timeout > total_timeout
        {
            return Err(SchwabTransportError::InvalidConfiguration);
        }
        Ok(Self {
            connect_timeout,
            read_timeout,
            total_timeout,
            max_response_bytes,
            max_header_count,
            max_header_bytes,
        })
    }

    pub const fn connect_timeout(self) -> Duration {
        self.connect_timeout
    }

    pub const fn read_timeout(self) -> Duration {
        self.read_timeout
    }

    pub const fn total_timeout(self) -> Duration {
        self.total_timeout
    }

    pub const fn max_response_bytes(self) -> usize {
        self.max_response_bytes.get()
    }

    pub const fn max_header_count(self) -> usize {
        self.max_header_count.get()
    }

    pub const fn max_header_bytes(self) -> usize {
        self.max_header_bytes.get()
    }
}

/// Explicit local execution/reconnect/microbatch bounds for one Streamer owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamerTransportBounds {
    connect_timeout: Duration,
    io_timeout: Duration,
    reconnect_delay: Duration,
    max_reconnect_attempts: usize,
    max_frame_bytes: NonZeroUsize,
    max_microbatch_frames: NonZeroUsize,
    max_microbatch_bytes: NonZeroUsize,
    microbatch_flush_interval: Duration,
}

impl StreamerTransportBounds {
    /// Constructs finite caller policy without asserting a provider symbol/rate ceiling.
    #[allow(
        clippy::too_many_arguments,
        reason = "all independent transport safety bounds remain explicit"
    )]
    pub fn try_new(
        connect_timeout: Duration,
        io_timeout: Duration,
        reconnect_delay: Duration,
        max_reconnect_attempts: usize,
        max_frame_bytes: NonZeroUsize,
        max_microbatch_frames: NonZeroUsize,
        max_microbatch_bytes: NonZeroUsize,
        microbatch_flush_interval: Duration,
    ) -> Result<Self, SchwabTransportError> {
        if connect_timeout.is_zero()
            || io_timeout.is_zero()
            || microbatch_flush_interval.is_zero()
            || max_frame_bytes.get() > max_microbatch_bytes.get()
        {
            return Err(SchwabTransportError::InvalidConfiguration);
        }
        Ok(Self {
            connect_timeout,
            io_timeout,
            reconnect_delay,
            max_reconnect_attempts,
            max_frame_bytes,
            max_microbatch_frames,
            max_microbatch_bytes,
            microbatch_flush_interval,
        })
    }

    pub const fn connect_timeout(self) -> Duration {
        self.connect_timeout
    }

    pub const fn io_timeout(self) -> Duration {
        self.io_timeout
    }

    pub const fn reconnect_delay(self) -> Duration {
        self.reconnect_delay
    }

    pub const fn max_reconnect_attempts(self) -> usize {
        self.max_reconnect_attempts
    }

    pub const fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes.get()
    }

    pub const fn max_microbatch_frames(self) -> usize {
        self.max_microbatch_frames.get()
    }

    pub const fn max_microbatch_bytes(self) -> usize {
        self.max_microbatch_bytes.get()
    }

    pub const fn microbatch_flush_interval(self) -> Duration {
        self.microbatch_flush_interval
    }
}

/// One bounded response header retained as exact bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResponseHeaderEvidence {
    name: Box<str>,
    value: Box<[u8]>,
}

impl ResponseHeaderEvidence {
    /// Constructs exact retained header evidence for an injected bounded wire response.
    /// The enclosing response applies aggregate header-count and byte bounds.
    pub fn try_new(name: String, value: Vec<u8>) -> Result<Self, SchwabTransportError> {
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || value.contains(&b'\r')
            || value.contains(&b'\n')
        {
            return Err(SchwabTransportError::Protocol);
        }
        Ok(Self {
            name: name.into_boxed_str(),
            value: value.into_boxed_slice(),
        })
    }

    /// Lower-case HTTP header name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Exact header-value bytes.
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

/// Exact capture metadata for one REST response body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawRestResponseReceipt {
    route: ReadOnlyRoute,
    token_generation: AccessTokenGeneration,
    request_url: Box<str>,
    request_sha256: [u8; 32],
    status: u16,
    received_at_unix_millis: u64,
    body_bytes: u64,
    body_sha256: [u8; 32],
    declared_body_bytes: Option<u64>,
    latency_ms: u64,
    headers: Box<[ResponseHeaderEvidence]>,
}

impl RawRestResponseReceipt {
    pub(crate) fn new(
        route: ReadOnlyRoute,
        token_generation: AccessTokenGeneration,
        request_url: Box<str>,
        status: u16,
        received_at_unix_millis: u64,
        body: &[u8],
        declared_body_bytes: Option<u64>,
        latency_ms: u64,
        headers: Box<[ResponseHeaderEvidence]>,
    ) -> Result<Self, SchwabTransportError> {
        let body_bytes = u64::try_from(body.len()).map_err(|_| SchwabTransportError::Overflow)?;
        let request_sha256 = request_identity(&request_url);
        Ok(Self {
            route,
            token_generation,
            request_url,
            request_sha256,
            status,
            received_at_unix_millis,
            body_bytes,
            body_sha256: Sha256::digest(body).into(),
            declared_body_bytes,
            latency_ms,
            headers,
        })
    }

    pub const fn route(&self) -> ReadOnlyRoute {
        self.route
    }

    pub const fn token_generation(&self) -> AccessTokenGeneration {
        self.token_generation
    }

    pub fn request_url(&self) -> &str {
        &self.request_url
    }

    pub const fn request_sha256(&self) -> [u8; 32] {
        self.request_sha256
    }

    pub const fn status(&self) -> u16 {
        self.status
    }

    pub const fn received_at_unix_millis(&self) -> u64 {
        self.received_at_unix_millis
    }

    pub const fn body_bytes(&self) -> u64 {
        self.body_bytes
    }

    pub const fn body_sha256(&self) -> [u8; 32] {
        self.body_sha256
    }

    pub const fn declared_body_bytes(&self) -> Option<u64> {
        self.declared_body_bytes
    }

    pub const fn latency_ms(&self) -> u64 {
        self.latency_ms
    }

    pub fn headers(&self) -> &[ResponseHeaderEvidence] {
        &self.headers
    }

    /// True only when an exact retained `Retry-After` header is present.
    pub fn retry_after_present(&self) -> bool {
        self.headers
            .iter()
            .any(|header| header.name() == "retry-after")
    }
}

/// Snapshot of actual transport activity. Values are measurements, never provider guarantees.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SchwabTransportTelemetrySnapshot {
    pub rest_requests_total: u64,
    pub rest_responses_total: u64,
    pub rest_failures_total: u64,
    pub rest_429_total: u64,
    pub requested_items_total: u64,
    pub returned_items_total: u64,
    pub missing_items_total: u64,
    pub unexpected_items_total: u64,
    pub rest_records_total: u64,
    pub request_target_bytes_total: u64,
    pub rest_response_bytes_total: u64,
    pub rest_latency_ms_total: u64,
    pub rest_latency_ms_max: u64,
    pub validation_failures_total: u64,
    pub streamer_connect_attempts_total: u64,
    pub streamer_connections_total: u64,
    pub streamer_reconnects_total: u64,
    pub streamer_connect_failures_total: u64,
    pub streamer_disconnects_total: u64,
    pub streamer_clean_closes_total: u64,
    pub streamer_gap_ms_total: u64,
    pub streamer_gap_ms_max: u64,
    pub streamer_requests_total: u64,
    pub streamer_request_bytes_total: u64,
    pub streamer_frames_total: u64,
    pub streamer_frame_bytes_total: u64,
    pub streamer_frames_captured_total: u64,
    pub streamer_frame_bytes_captured_total: u64,
    pub streamer_microbatches_total: u64,
    pub streamer_events_total: u64,
    pub streamer_responses_total: u64,
    pub streamer_notifications_total: u64,
}

#[derive(Debug, Default)]
struct TelemetryState {
    snapshot: SchwabTransportTelemetrySnapshot,
    gap_started: Option<Instant>,
}

/// Shared checked runtime telemetry for REST and the sole Streamer owner.
#[derive(Clone, Debug, Default)]
pub struct SchwabTransportTelemetry {
    state: Arc<Mutex<TelemetryState>>,
}

impl SchwabTransportTelemetry {
    /// Returns a coherent current measurement snapshot.
    pub fn snapshot(&self) -> Result<SchwabTransportTelemetrySnapshot, SchwabTransportError> {
        self.state
            .lock()
            .map(|state| state.snapshot)
            .map_err(|_| SchwabTransportError::TelemetryUnavailable)
    }

    pub(crate) fn record_rest_attempt(
        &self,
        items: u64,
        request_bytes: u64,
    ) -> Result<(), SchwabTransportError> {
        self.with_state(|state| {
            add(&mut state.rest_requests_total, 1)?;
            add(&mut state.requested_items_total, items)?;
            add(&mut state.request_target_bytes_total, request_bytes)
        })
    }

    pub(crate) fn record_rest_failure(&self) -> Result<(), SchwabTransportError> {
        self.with_state(|state| add(&mut state.rest_failures_total, 1))
    }

    pub(crate) fn record_rest_response(
        &self,
        status: u16,
        response_bytes: u64,
        latency_ms: u64,
    ) -> Result<(), SchwabTransportError> {
        self.with_state(|state| {
            add(&mut state.rest_responses_total, 1)?;
            add(&mut state.rest_response_bytes_total, response_bytes)?;
            add(&mut state.rest_latency_ms_total, latency_ms)?;
            state.rest_latency_ms_max = state.rest_latency_ms_max.max(latency_ms);
            if status == 429 {
                add(&mut state.rest_429_total, 1)?;
            }
            Ok(())
        })
    }

    pub(crate) fn record_rest_accounting(
        &self,
        returned: u64,
        missing: u64,
        unexpected: u64,
        records: u64,
    ) -> Result<(), SchwabTransportError> {
        self.with_state(|state| {
            add(&mut state.returned_items_total, returned)?;
            add(&mut state.missing_items_total, missing)?;
            add(&mut state.unexpected_items_total, unexpected)?;
            add(&mut state.rest_records_total, records)
        })
    }

    pub(crate) fn record_validation_failure(&self) -> Result<(), SchwabTransportError> {
        self.with_state(|state| add(&mut state.validation_failures_total, 1))
    }

    pub(crate) fn record_stream_connect_attempt(
        &self,
        reconnect: bool,
    ) -> Result<(), SchwabTransportError> {
        self.with_state(|state| {
            add(&mut state.streamer_connect_attempts_total, 1)?;
            if reconnect {
                add(&mut state.streamer_reconnects_total, 1)?;
            }
            Ok(())
        })
    }

    pub(crate) fn record_stream_connect_failure(&self) -> Result<(), SchwabTransportError> {
        self.with_state(|state| add(&mut state.streamer_connect_failures_total, 1))
    }

    pub(crate) fn record_stream_connected(&self) -> Result<(), SchwabTransportError> {
        self.with_locked(|state| {
            add(&mut state.snapshot.streamer_connections_total, 1)?;
            if let Some(started) = state.gap_started.take() {
                let gap = duration_millis(started.elapsed())?;
                add(&mut state.snapshot.streamer_gap_ms_total, gap)?;
                state.snapshot.streamer_gap_ms_max = state.snapshot.streamer_gap_ms_max.max(gap);
            }
            Ok(())
        })
    }

    pub(crate) fn record_stream_disconnect(&self) -> Result<(), SchwabTransportError> {
        self.with_locked(|state| {
            add(&mut state.snapshot.streamer_disconnects_total, 1)?;
            state.gap_started.get_or_insert_with(Instant::now);
            Ok(())
        })
    }

    pub(crate) fn record_stream_clean_close(&self) -> Result<(), SchwabTransportError> {
        self.with_state(|state| add(&mut state.streamer_clean_closes_total, 1))
    }

    pub(crate) fn record_stream_request(&self, bytes: u64) -> Result<(), SchwabTransportError> {
        self.with_state(|state| {
            add(&mut state.streamer_requests_total, 1)?;
            add(&mut state.streamer_request_bytes_total, bytes)
        })
    }

    pub(crate) fn record_stream_frame(&self, bytes: u64) -> Result<(), SchwabTransportError> {
        self.with_state(|state| {
            add(&mut state.streamer_frames_total, 1)?;
            add(&mut state.streamer_frame_bytes_total, bytes)
        })
    }

    pub(crate) fn record_stream_semantics(
        &self,
        events: u64,
        responses: u64,
        notifications: u64,
    ) -> Result<(), SchwabTransportError> {
        self.with_state(|state| {
            add(&mut state.streamer_events_total, events)?;
            add(&mut state.streamer_responses_total, responses)?;
            add(&mut state.streamer_notifications_total, notifications)
        })
    }

    pub(crate) fn record_stream_microbatch(
        &self,
        frames: u64,
        bytes: u64,
    ) -> Result<(), SchwabTransportError> {
        self.with_state(|state| {
            add(&mut state.streamer_frames_captured_total, frames)?;
            add(&mut state.streamer_frame_bytes_captured_total, bytes)?;
            add(&mut state.streamer_microbatches_total, 1)
        })
    }

    fn with_state(
        &self,
        operation: impl FnOnce(
            &mut SchwabTransportTelemetrySnapshot,
        ) -> Result<(), SchwabTransportError>,
    ) -> Result<(), SchwabTransportError> {
        self.with_locked(|state| operation(&mut state.snapshot))
    }

    fn with_locked(
        &self,
        operation: impl FnOnce(&mut TelemetryState) -> Result<(), SchwabTransportError>,
    ) -> Result<(), SchwabTransportError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SchwabTransportError::TelemetryUnavailable)?;
        operation(&mut state)
    }
}

/// Secret-free transport and capture failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SchwabTransportError {
    #[error("invalid Schwab transport configuration")]
    InvalidConfiguration,
    #[error("invalid transient Schwab access token")]
    InvalidToken,
    #[error("Schwab access token refresh is required")]
    TokenRefreshRequired,
    #[error("Schwab access-token authority is unavailable")]
    TokenAuthorityUnavailable,
    #[error("Schwab network operation failed")]
    Network,
    #[error("Schwab network operation deadline elapsed")]
    Deadline,
    #[error("Schwab network operation was cancelled")]
    Cancelled,
    #[error("Schwab HTTP or WebSocket protocol was rejected")]
    Protocol,
    #[error("Schwab response or frame exceeded its byte bound")]
    PayloadTooLarge,
    #[error("Schwab response headers exceeded their bound")]
    HeaderBoundsExceeded,
    #[error("Schwab capture sink rejected an exact payload")]
    CaptureRejected,
    #[error("Schwab source-neutral capture material could not be constructed")]
    CaptureMaterial,
    #[error("Schwab Streamer reconnect policy was exhausted")]
    ReconnectExhausted,
    #[error("Schwab Streamer closed and requires resynchronization")]
    ResynchronizationRequired,
    #[error("Schwab transport telemetry is unavailable")]
    TelemetryUnavailable,
    #[error("Schwab checked transport accounting overflowed")]
    Overflow,
    #[error("Schwab provider-native validation failed")]
    Adapter,
}

impl From<SchwabAdapterError> for SchwabTransportError {
    fn from(_error: SchwabAdapterError) -> Self {
        Self::Adapter
    }
}

impl From<TokenAuthorityError> for SchwabTransportError {
    fn from(error: TokenAuthorityError) -> Self {
        match error {
            TokenAuthorityError::Unavailable => Self::TokenAuthorityUnavailable,
            TokenAuthorityError::ReauthorizationRequired => Self::TokenRefreshRequired,
        }
    }
}

pub(crate) fn unix_seconds() -> Result<u64, SchwabTransportError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| SchwabTransportError::Protocol)
}

pub(crate) fn unix_millis() -> Result<u64, SchwabTransportError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SchwabTransportError::Protocol)?
        .as_millis();
    u64::try_from(millis).map_err(|_| SchwabTransportError::Overflow)
}

pub(crate) fn duration_millis(duration: Duration) -> Result<u64, SchwabTransportError> {
    u64::try_from(duration.as_millis()).map_err(|_| SchwabTransportError::Overflow)
}

pub(crate) fn request_identity(url: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"GET\0");
    hasher.update(url.as_bytes());
    hasher.finalize().into()
}

pub(crate) fn hash_frame(
    hasher: &mut Sha256,
    kind: u8,
    payload: &Bytes,
) -> Result<(), SchwabTransportError> {
    hasher.update([kind]);
    let length = u64::try_from(payload.len()).map_err(|_| SchwabTransportError::Overflow)?;
    hasher.update(length.to_be_bytes());
    hasher.update(Sha256::digest(payload));
    Ok(())
}

pub(crate) fn hash_observation(
    hasher: &mut Sha256,
    generation: ConnectionGeneration,
    ordinal: NonZeroU64,
    received_at_unix_millis: u64,
    payload_sha256: [u8; 32],
) {
    hasher.update(generation.get().to_be_bytes());
    hasher.update(ordinal.get().to_be_bytes());
    hasher.update(received_at_unix_millis.to_be_bytes());
    hasher.update(payload_sha256);
}

fn add(target: &mut u64, value: u64) -> Result<(), SchwabTransportError> {
    *target = target
        .checked_add(value)
        .ok_or(SchwabTransportError::Overflow)?;
    Ok(())
}
