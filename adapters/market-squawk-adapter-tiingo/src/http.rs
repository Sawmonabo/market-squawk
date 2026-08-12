use std::fmt;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures_util::StreamExt as _;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{RawCaptureRecord, RawCaptureRecordError};
use market_squawk_sources::{
    BackoffPolicy, BudgetDecision, BudgetPoolError, BudgetScope, BudgetUnavailableReason,
    BudgetWindowSemantics, MonotonicInstant, ProviderBudgetPolicy, ProviderBudgetWindow,
    ProviderCaptureError, ProviderCaptureMaterial, ProviderCapturePageReceipt,
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition, ProviderRateAuthority,
    ProviderRateDeclaration, SharedProviderBudget, apply_http_retry_after,
};
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE, RETRY_AFTER};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{
    TIINGO_APPLICATION_REQUESTS_PER_DAY, TIINGO_APPLICATION_REQUESTS_PER_HOUR, TiingoAdapterError,
    TiingoApiToken, TiingoDecoder, TiingoEndpointFamily, TiingoEodReceipt, TiingoHistoryPlan,
    TiingoMetadataReceipt, TiingoProviderFailure, TiingoQuotaAdmission, TiingoQuotaError,
    TiingoQuotaLedger, TiingoQuotaSnapshot, TiingoQuotaWindows, TiingoRequestBuilder,
    TiingoRequestSpec, TiingoSchemaCircuitState, TiingoTicker,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(20);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const PROVIDER_HOUR_NANOS: u64 = 3_600_000_000_000;
const PROVIDER_DAY_NANOS: u64 = 86_400_000_000_000;
const INITIAL_BACKOFF_NANOS: u64 = 1_000_000_000;
const MAXIMUM_BACKOFF_NANOS: u64 = PROVIDER_HOUR_NANOS;
const BACKOFF_JITTER_BASIS_POINTS: u16 = 1_000;
const DATASET_METADATA: &str = "tiingo-daily-metadata";
const DATASET_LATEST: &str = "tiingo-daily-latest";
const DATASET_HISTORY_WINDOW: &str = "tiingo-daily-history-window";

/// Durable-store failure at the Tiingo-specific 4D quota boundary.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TiingoQuotaStoreError {
    /// The durable quota store could not be read or atomically replaced.
    #[error("Tiingo quota persistence is unavailable")]
    Unavailable,
    /// The exact expected predecessor did not match durable state.
    #[error("Tiingo quota state changed concurrently")]
    Conflict,
    /// Persisted bytes did not decode to the closed quota schema or failed validation.
    #[error("Tiingo quota persistence is corrupt")]
    Corrupt,
}

/// Restart-durable compare-and-swap boundary for Tiingo's unique-symbol and bandwidth dimensions.
///
/// Implementations persist only [`TiingoQuotaSnapshot`]. They must never persist the API token,
/// authorization header, request object, or response body through this interface.
pub trait TiingoQuotaStore: fmt::Debug + Send + Sync {
    /// Loads the exact current state, or `None` before first initialization.
    fn load(&self) -> Result<Option<TiingoQuotaSnapshot>, TiingoQuotaStoreError>;

    /// Atomically replaces the exact predecessor digest with `next`.
    fn compare_and_swap(
        &self,
        expected: Option<EvidenceDigest>,
        next: &TiingoQuotaSnapshot,
    ) -> Result<(), TiingoQuotaStoreError>;
}

/// Failure while binding one standalone Tiingo response to an exact raw `MSJ1` record.
#[derive(Debug, Error)]
pub enum TiingoCaptureMaterialError {
    /// Caller-supplied capture identities or exact record material were invalid.
    #[error(transparent)]
    RawRecord(#[from] RawCaptureRecordError),
    /// The raw record did not bind exactly to the source-neutral response receipt.
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
}

/// Provider refusal scheduling result after applying bounded `Retry-After` semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoRateLimitDisposition {
    /// Every worker sharing the provider allocation must wait until this monotonic coordinate.
    WaitUntil(MonotonicInstant),
    /// The shared provider allocation became unavailable for the exact closed reason.
    Unavailable(BudgetUnavailableReason),
}

/// Exact bounded HTTP material retained before provider-native decoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoHttpResponseMaterial {
    status: u16,
    final_url: Url,
    body: Bytes,
    body_digest: EvidenceDigest,
    retry_after: Option<Box<[u8]>>,
    content_type: Option<Box<[u8]>>,
    content_encoding: Option<Box<[u8]>>,
    received_at: Timestamp,
    latency_nanos: u64,
}

impl TiingoHttpResponseMaterial {
    /// Returns the exact HTTP status.
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the final credential-free URL. Redirects are disabled by the production client.
    pub const fn final_url(&self) -> &Url {
        &self.final_url
    }

    /// Returns the exact bounded response body.
    pub const fn body(&self) -> &Bytes {
        &self.body
    }

    /// Returns the exact body SHA-256 identity.
    pub const fn body_digest(&self) -> EvidenceDigest {
        self.body_digest
    }

    /// Returns the exact response-body byte count.
    pub fn response_bytes(&self) -> u64 {
        u64::try_from(self.body.len()).unwrap_or(u64::MAX)
    }

    /// Returns the bounded raw `Retry-After` header, when supplied.
    pub fn retry_after(&self) -> Option<&[u8]> {
        self.retry_after.as_deref()
    }

    /// Returns the bounded raw `Content-Type` header, when supplied.
    pub fn content_type(&self) -> Option<&[u8]> {
        self.content_type.as_deref()
    }

    /// Returns the bounded raw `Content-Encoding` header, when supplied.
    pub fn content_encoding(&self) -> Option<&[u8]> {
        self.content_encoding.as_deref()
    }

    /// Returns the socket-boundary time after the complete retained body arrived.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns measured monotonic request-to-complete-body latency.
    pub const fn latency_nanos(&self) -> u64 {
        self.latency_nanos
    }
}

/// Successful raw body plus source-neutral capture receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoRawMaterial {
    request: TiingoRequestSpec,
    http: TiingoHttpResponseMaterial,
    capture: ProviderCaptureSetReceipt,
}

impl TiingoRawMaterial {
    /// Returns the exact credential-free request semantics.
    pub const fn request(&self) -> &TiingoRequestSpec {
        &self.request
    }

    /// Returns exact bounded HTTP material.
    pub const fn http(&self) -> &TiingoHttpResponseMaterial {
        &self.http
    }

    /// Returns the source-neutral standalone-response terminal receipt.
    pub const fn capture(&self) -> &ProviderCaptureSetReceipt {
        &self.capture
    }

    /// Builds source-neutral material ready for `MSJ1` sealing.
    ///
    /// Event and connection UUIDs remain caller-owned capture-authority coordinates. Each Tiingo
    /// application history window is deliberately converted as its own one-record standalone
    /// capture; this method never recasts application date windows as provider cursor pages.
    pub fn capture_material(
        &self,
        event_id: Uuid,
        connection_id: Uuid,
    ) -> Result<ProviderCaptureMaterial, TiingoCaptureMaterialError> {
        let record = RawCaptureRecord::try_new_live(
            event_id,
            Arc::from(self.capture.source_id().as_str()),
            connection_id,
            Some(0),
            None,
            DateTime::<Utc>::from_timestamp_nanos(self.http.received_at.unix_nanos()),
            self.http.body.clone(),
        )?;
        ProviderCaptureMaterial::try_new(self.capture.clone(), vec![record]).map_err(Into::into)
    }
}

/// One successful raw page paired with its strict provider-native decode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoCapturedPage<T> {
    raw: TiingoRawMaterial,
    decoded: T,
}

impl<T> TiingoCapturedPage<T> {
    /// Returns exact raw body and capture evidence.
    pub const fn raw(&self) -> &TiingoRawMaterial {
        &self.raw
    }

    /// Returns the strict provider-native decoded value.
    pub const fn decoded(&self) -> &T {
        &self.decoded
    }

    /// Builds the exact source-neutral raw material required before canonical publication.
    pub fn capture_material(
        &self,
        event_id: Uuid,
        connection_id: Uuid,
    ) -> Result<ProviderCaptureMaterial, TiingoCaptureMaterialError> {
        self.raw.capture_material(event_id, connection_id)
    }
}

/// Terminal proof for an ordered Tiingo history plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoHistoryTerminalDisposition {
    /// Every application-created date window completed; Tiingo supplied no cursor contract.
    ApplicationDateWindowsExhaustedWithoutProviderCursor,
}

/// Complete, bounded history response graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoHistoryCapture {
    plan: TiingoHistoryPlan,
    pages: Box<[TiingoCapturedPage<TiingoEodReceipt>]>,
    terminal: TiingoHistoryTerminalDisposition,
    total_response_bytes: u64,
    total_latency_nanos: u64,
    graph_digest: EvidenceDigest,
}

impl TiingoHistoryCapture {
    /// Returns the exact complete application-created request plan.
    pub const fn plan(&self) -> &TiingoHistoryPlan {
        &self.plan
    }

    /// Returns every ordered response page.
    pub fn pages(&self) -> &[TiingoCapturedPage<TiingoEodReceipt>] {
        &self.pages
    }

    /// Returns terminal evidence without claiming a provider cursor existed.
    pub const fn terminal(&self) -> TiingoHistoryTerminalDisposition {
        self.terminal
    }

    /// Returns exact retained response bytes across all windows.
    pub const fn total_response_bytes(&self) -> u64 {
        self.total_response_bytes
    }

    /// Returns checked aggregate measured latency across all window requests.
    pub const fn total_latency_nanos(&self) -> u64 {
        self.total_latency_nanos
    }

    /// Returns the identity of the request plan and every ordered response observation.
    pub const fn graph_digest(&self) -> EvidenceDigest {
        self.graph_digest
    }
}

/// Preserved non-success provider response and any applied shared-backoff decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoProviderHttpFailure {
    provider: TiingoProviderFailure,
    http: TiingoHttpResponseMaterial,
    rate_limit: Option<TiingoRateLimitDisposition>,
}

impl TiingoProviderHttpFailure {
    /// Returns the existing bounded provider failure contract.
    pub const fn provider(&self) -> &TiingoProviderFailure {
        &self.provider
    }

    /// Returns exact HTTP body/header/clock/latency material.
    pub const fn http(&self) -> &TiingoHttpResponseMaterial {
        &self.http
    }

    /// Returns the applied shared refusal decision for HTTP 429/503.
    pub const fn rate_limit(&self) -> Option<TiingoRateLimitDisposition> {
        self.rate_limit
    }
}

/// A strict decoding failure that retains the exact successful raw response.
#[derive(Debug)]
pub struct TiingoDecodeFailure {
    error: TiingoAdapterError,
    raw: TiingoRawMaterial,
}

impl TiingoDecodeFailure {
    /// Returns the schema-circuit or strict decoding failure.
    pub const fn error(&self) -> &TiingoAdapterError {
        &self.error
    }

    /// Returns exact raw material that caused the failure.
    pub const fn raw(&self) -> &TiingoRawMaterial {
        &self.raw
    }
}

/// Bounded transport failure class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoTransportFailureKind {
    /// Cancellation was observed before completion.
    Cancelled,
    /// The caller deadline or hardened total timeout elapsed.
    DeadlineExceeded,
    /// DNS, connection, TLS, or response streaming failed.
    Network,
    /// The body crossed its request-family byte ceiling.
    BodyTooLarge,
    /// A retained response header crossed its code-owned byte ceiling.
    InvalidResponseHeaders,
    /// Local wall or monotonic clock material could not be represented.
    ClockUnavailable,
}

/// Transport failure with exact received-byte and measured-latency accounting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoTransportFailure {
    kind: TiingoTransportFailureKind,
    received_body_bytes: u64,
    quota_charge_bytes: u64,
    latency_nanos: u64,
}

impl TiingoTransportFailure {
    /// Returns the closed transport failure class.
    pub const fn kind(&self) -> TiingoTransportFailureKind {
        self.kind
    }

    /// Returns exact body bytes delivered by the response stream before failure.
    pub const fn received_body_bytes(&self) -> u64 {
        self.received_body_bytes
    }

    /// Returns bytes charged to quota. Incomplete responses use the admitted maximum because the
    /// adapter cannot prove how many bytes Tiingo counted after a cancelled or failed stream.
    pub const fn quota_charge_bytes(&self) -> u64 {
        self.quota_charge_bytes
    }

    /// Returns measured monotonic latency until failure.
    pub const fn latency_nanos(&self) -> u64 {
        self.latency_nanos
    }
}

/// Standalone Tiingo HTTP/source failure.
#[derive(Debug, Error)]
pub enum TiingoHttpSourceError {
    /// Code-owned identity, duration, HTTP-client, or provider-policy construction failed.
    #[error("invalid Tiingo HTTP source configuration")]
    InvalidConfiguration,
    /// Product-wide durable provider-rate registration failed.
    #[error(transparent)]
    ProviderRate(#[from] BudgetPoolError),
    /// Tiingo-specific quota state was unavailable or changed concurrently.
    #[error(transparent)]
    QuotaStore(#[from] TiingoQuotaStoreError),
    /// The Tiingo 4D quota ledger rejected a transition.
    #[error(transparent)]
    Quota(#[from] TiingoQuotaError),
    /// A lower application quota dimension denied this request without transport.
    #[error("Tiingo application quota denied the request")]
    QuotaDenied(TiingoQuotaAdmission),
    /// The shared provider budget requires waiting until this process-monotonic coordinate.
    #[error("Tiingo provider budget requires waiting")]
    BudgetWaitUntil(MonotonicInstant),
    /// The shared provider budget is unavailable.
    #[error("Tiingo provider budget is unavailable")]
    BudgetUnavailable(BudgetUnavailableReason),
    /// Request construction or an existing provider-native adapter contract failed.
    #[error(transparent)]
    Adapter(#[from] TiingoAdapterError),
    /// Bounded HTTP transport failed after exact/conservative byte accounting.
    #[error("Tiingo HTTP transport failed")]
    Transport(TiingoTransportFailure),
    /// Tiingo returned a bounded non-success response, retained exactly.
    #[error("Tiingo returned a bounded non-success HTTP response")]
    Provider(Box<TiingoProviderHttpFailure>),
    /// A successful HTTP response violated URL or content-header invariants.
    #[error("Tiingo HTTP response violated the transport contract")]
    InvalidHttpResponse(Box<TiingoHttpResponseMaterial>),
    /// A source-neutral capture receipt could not be constructed.
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
    /// Strict native decoding failed; exact raw material remains attached.
    #[error("Tiingo provider-native decoding failed")]
    Decode(Box<TiingoDecodeFailure>),
    /// Complete history material crossed the aggregate retained-byte or latency bound.
    #[error("Tiingo history capture exceeded its aggregate bound")]
    HistoryCaptureTooLarge,
    /// Trusted wall-clock material could not be represented.
    #[error("Tiingo source clock is unavailable")]
    ClockUnavailable,
}

/// Credential-bearing, request-serialized Tiingo Starter HTTP source.
pub struct TiingoHttpSource {
    requests: TiingoRequestBuilder,
    transport: Arc<dyn TiingoTransport>,
    budget: SharedProviderBudget,
    quota_store: Arc<dyn TiingoQuotaStore>,
    runtime: tokio::sync::Mutex<TiingoRuntime>,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    metadata_dataset: SourceIdentifier,
    latest_dataset: SourceIdentifier,
    history_dataset: SourceIdentifier,
}

struct TiingoRuntime {
    quota: TiingoQuotaLedger,
    quota_available: bool,
    decoder: TiingoDecoder,
}

impl fmt::Debug for TiingoHttpSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TiingoHttpSource")
            .field("source_id", &self.source_id)
            .field("metadata_revision", &self.metadata_revision)
            .field("credential", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl TiingoHttpSource {
    /// Opens a hardened production transport and durable 4D quota state.
    ///
    /// A crash-retained response reservation is conservatively charged at its admitted maximum
    /// and compare-and-swap persisted during construction. No credential is passed to either
    /// durable authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "security, source identity, schema identity, and two durable authorities remain explicit"
    )]
    pub fn try_new(
        token: TiingoApiToken,
        rate_authority: &ProviderRateAuthority,
        quota_store: Arc<dyn TiingoQuotaStore>,
        initial_quota_windows: TiingoQuotaWindows,
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        native_contract_revision: SourceIdentifier,
    ) -> Result<Self, TiingoHttpSourceError> {
        let client = hardened_client()?;
        let transport: Arc<dyn TiingoTransport> =
            Arc::new(ReqwestTiingoTransport::new(client.clone()));
        Self::try_new_inner(
            token,
            rate_authority,
            quota_store,
            initial_quota_windows,
            source_id,
            metadata_revision,
            native_contract_revision,
            client,
            transport,
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new_with_transport(
        token: TiingoApiToken,
        rate_authority: &ProviderRateAuthority,
        quota_store: Arc<dyn TiingoQuotaStore>,
        initial_quota_windows: TiingoQuotaWindows,
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        native_contract_revision: SourceIdentifier,
        transport: Arc<dyn TiingoTransport>,
    ) -> Result<Self, TiingoHttpSourceError> {
        let client = hardened_client()?;
        Self::try_new_inner(
            token,
            rate_authority,
            quota_store,
            initial_quota_windows,
            source_id,
            metadata_revision,
            native_contract_revision,
            client,
            transport,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_new_inner(
        token: TiingoApiToken,
        rate_authority: &ProviderRateAuthority,
        quota_store: Arc<dyn TiingoQuotaStore>,
        initial_quota_windows: TiingoQuotaWindows,
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        native_contract_revision: SourceIdentifier,
        client: reqwest::Client,
        transport: Arc<dyn TiingoTransport>,
    ) -> Result<Self, TiingoHttpSourceError> {
        let declaration = tiingo_provider_rate_declaration()?;
        let budget = rate_authority.register_budget(declaration)?;
        let quota = restore_quota(&quota_store, initial_quota_windows)?;
        Ok(Self {
            requests: TiingoRequestBuilder::new(client, token),
            transport,
            budget,
            quota_store,
            runtime: tokio::sync::Mutex::new(TiingoRuntime {
                quota,
                quota_available: true,
                decoder: TiingoDecoder::new(native_contract_revision),
            }),
            source_id,
            metadata_revision,
            metadata_dataset: identifier(DATASET_METADATA)?,
            latest_dataset: identifier(DATASET_LATEST)?,
            history_dataset: identifier(DATASET_HISTORY_WINDOW)?,
        })
    }

    /// Fetches exact per-symbol metadata under deadline, cancellation, both quotas, and capture.
    pub async fn fetch_metadata(
        &self,
        ticker: TiingoTicker,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<TiingoCapturedPage<TiingoMetadataReceipt>, TiingoHttpSourceError> {
        let spec = TiingoRequestSpec::metadata(ticker)?;
        let mut runtime = self.runtime.lock().await;
        let raw = self
            .fetch_raw_locked(&mut runtime, spec.clone(), deadline, cancellation)
            .await?;
        let decoded_at = system_timestamp()?;
        match runtime.decoder.decode_metadata(
            spec,
            raw.http.status,
            &raw.http.body,
            raw.http.received_at,
            decoded_at,
        ) {
            Ok(decoded) => Ok(TiingoCapturedPage { raw, decoded }),
            Err(error) => Err(TiingoHttpSourceError::Decode(Box::new(
                TiingoDecodeFailure { error, raw },
            ))),
        }
    }

    /// Fetches the latest daily row without claiming an intraday mutual-fund price.
    pub async fn fetch_latest(
        &self,
        ticker: TiingoTicker,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<TiingoCapturedPage<TiingoEodReceipt>, TiingoHttpSourceError> {
        let spec = TiingoRequestSpec::latest(ticker)?;
        let mut runtime = self.runtime.lock().await;
        self.fetch_eod_locked(&mut runtime, spec, deadline, cancellation)
            .await
    }

    /// Fetches every ordered application date window and returns terminal graph evidence only
    /// after the complete bounded plan succeeds.
    pub async fn fetch_history(
        &self,
        plan: TiingoHistoryPlan,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<TiingoHistoryCapture, TiingoHttpSourceError> {
        let mut runtime = self.runtime.lock().await;
        let mut pages = Vec::new();
        pages
            .try_reserve_exact(plan.pages().len())
            .map_err(|_| TiingoHttpSourceError::HistoryCaptureTooLarge)?;
        let mut total_response_bytes = 0_u64;
        let mut total_latency_nanos = 0_u64;
        for spec in plan.pages() {
            let page = self
                .fetch_eod_locked(&mut runtime, spec.clone(), deadline, cancellation)
                .await?;
            total_response_bytes = total_response_bytes
                .checked_add(page.raw.http.response_bytes())
                .filter(|bytes| *bytes <= market_squawk_sources::MAX_PROVIDER_CAPTURE_BYTES)
                .ok_or(TiingoHttpSourceError::HistoryCaptureTooLarge)?;
            total_latency_nanos = total_latency_nanos
                .checked_add(page.raw.http.latency_nanos)
                .ok_or(TiingoHttpSourceError::HistoryCaptureTooLarge)?;
            pages.push(page);
        }
        let graph_digest = history_graph_digest(&plan, &pages, total_response_bytes);
        Ok(TiingoHistoryCapture {
            plan,
            pages: pages.into_boxed_slice(),
            terminal:
                TiingoHistoryTerminalDisposition::ApplicationDateWindowsExhaustedWithoutProviderCursor,
            total_response_bytes,
            total_latency_nanos,
            graph_digest,
        })
    }

    /// Returns an exact snapshot suitable for telemetry or durable-state verification.
    pub async fn quota_snapshot(&self) -> TiingoQuotaSnapshot {
        self.runtime.lock().await.quota.snapshot().clone()
    }

    /// Advances caller-supplied conservative quota windows and persists the exact transition.
    ///
    /// The source never invents Tiingo reset timezone semantics. A scheduler must call this only
    /// with externally governed boundaries after the retained hour boundary has elapsed.
    pub async fn advance_quota_windows(
        &self,
        observed_at: Timestamp,
        next: TiingoQuotaWindows,
    ) -> Result<TiingoQuotaSnapshot, TiingoHttpSourceError> {
        let mut runtime = self.runtime.lock().await;
        if !runtime.quota_available {
            return Err(TiingoQuotaStoreError::Unavailable.into());
        }
        let predecessor = runtime.quota.snapshot().digest();
        runtime.quota.advance_windows(observed_at, next)?;
        persist_quota_transition(&self.quota_store, &mut runtime, Some(predecessor))?;
        Ok(runtime.quota.snapshot().clone())
    }

    /// Returns the current in-process fail-closed provider-native schema circuit.
    pub async fn schema_circuit_state(&self) -> TiingoSchemaCircuitState {
        self.runtime.lock().await.decoder.circuit().state().clone()
    }

    async fn fetch_eod_locked(
        &self,
        runtime: &mut TiingoRuntime,
        spec: TiingoRequestSpec,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<TiingoCapturedPage<TiingoEodReceipt>, TiingoHttpSourceError> {
        let raw = self
            .fetch_raw_locked(runtime, spec.clone(), deadline, cancellation)
            .await?;
        let decoded_at = system_timestamp()?;
        match runtime.decoder.decode_eod(
            spec,
            raw.http.status,
            &raw.http.body,
            raw.http.received_at,
            decoded_at,
        ) {
            Ok(decoded) => Ok(TiingoCapturedPage { raw, decoded }),
            Err(error) => Err(TiingoHttpSourceError::Decode(Box::new(
                TiingoDecodeFailure { error, raw },
            ))),
        }
    }

    async fn fetch_raw_locked(
        &self,
        runtime: &mut TiingoRuntime,
        spec: TiingoRequestSpec,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<TiingoRawMaterial, TiingoHttpSourceError> {
        if cancellation.is_cancelled() {
            return Err(TiingoHttpSourceError::Transport(transport_failure(
                TiingoTransportFailureKind::Cancelled,
                0,
                0,
                Instant::now(),
            )));
        }
        let now = system_timestamp()?;
        let timeout = remaining_timeout(deadline, now)?;
        let request = self.requests.build(&spec)?;
        let reservation = NonZeroU64::new(
            u64::try_from(spec.max_response_bytes())
                .map_err(|_| TiingoHttpSourceError::InvalidConfiguration)?,
        )
        .ok_or(TiingoHttpSourceError::InvalidConfiguration)?;
        if !runtime.quota_available {
            return Err(TiingoHttpSourceError::QuotaStore(
                TiingoQuotaStoreError::Unavailable,
            ));
        }
        let admission = runtime.quota.classify(spec.ticker(), reservation)?;
        if admission != TiingoQuotaAdmission::Admitted {
            return Err(TiingoHttpSourceError::QuotaDenied(admission));
        }
        let provider_permit = match self.budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            BudgetDecision::WaitUntil(deadline) => {
                return Err(TiingoHttpSourceError::BudgetWaitUntil(deadline));
            }
            BudgetDecision::Unavailable(reason) => {
                return Err(TiingoHttpSourceError::BudgetUnavailable(reason));
            }
        };
        let prior = runtime.quota.snapshot().clone();
        let quota_permit = match runtime.quota.reserve(spec.ticker().clone(), reservation)? {
            Ok(permit) => permit,
            Err(admission) => {
                provider_permit.release();
                return Err(TiingoHttpSourceError::QuotaDenied(admission));
            }
        };
        persist_quota_transition(&self.quota_store, runtime, Some(prior.digest()))?;

        let response = match self
            .transport
            .execute(
                request,
                spec.max_response_bytes(),
                timeout,
                cancellation.clone(),
            )
            .await
        {
            Ok(response) => response,
            Err(failure) => {
                settle_response_quota(
                    &self.quota_store,
                    runtime,
                    &quota_permit,
                    spec.ticker(),
                    failure.quota_charge_bytes,
                )?;
                provider_permit.release();
                return Err(TiingoHttpSourceError::Transport(failure));
            }
        };
        settle_response_quota(
            &self.quota_store,
            runtime,
            &quota_permit,
            spec.ticker(),
            response.response_bytes(),
        )?;
        provider_permit.release();

        if !(200..=299).contains(&response.status) {
            let rate_limit = if matches!(response.status, 429 | 503) {
                Some(rate_limit_disposition(apply_http_retry_after(
                    &self.budget,
                    response.retry_after(),
                    0,
                ))?)
            } else {
                None
            };
            let provider = TiingoProviderFailure::new(response.status, &response.body);
            return Err(TiingoHttpSourceError::Provider(Box::new(
                TiingoProviderHttpFailure {
                    provider,
                    http: response,
                    rate_limit,
                },
            )));
        }
        if response.final_url != *spec.url()
            || response
                .content_encoding()
                .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
            || !content_type_is_json(response.content_type())
        {
            return Err(TiingoHttpSourceError::InvalidHttpResponse(Box::new(
                response,
            )));
        }
        let raw = self.capture_success(spec, response)?;
        self.budget
            .record_success()
            .map_err(TiingoHttpSourceError::BudgetUnavailable)?;
        Ok(raw)
    }

    fn capture_success(
        &self,
        request: TiingoRequestSpec,
        http: TiingoHttpResponseMaterial,
    ) -> Result<TiingoRawMaterial, TiingoHttpSourceError> {
        let request_identity = request.request_identity();
        let page = ProviderCapturePageReceipt::try_new(
            0,
            request_identity,
            None,
            None,
            http.status,
            http.response_bytes(),
            http.body_digest,
            http.received_at,
        )?;
        let dataset = match request.endpoint() {
            TiingoEndpointFamily::Metadata => self.metadata_dataset.clone(),
            TiingoEndpointFamily::LatestDailyPrices => self.latest_dataset.clone(),
            TiingoEndpointFamily::HistoricalDailyPrices => self.history_dataset.clone(),
        };
        let capture = ProviderCaptureSetReceipt::try_new(
            self.source_id.clone(),
            self.metadata_revision.clone(),
            dataset,
            request_identity,
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![page],
        )?;
        Ok(TiingoRawMaterial {
            request,
            http,
            capture,
        })
    }
}

/// Builds the exact product-wide request policy registered for the Tiingo Starter credential.
pub fn tiingo_provider_rate_declaration() -> Result<ProviderRateDeclaration, TiingoHttpSourceError>
{
    let provider = identifier("tiingo-starter")?;
    let subject = ProviderRateDeclaration::governed_provider_subject(&provider)?;
    let windows = [
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(
                u32::try_from(TIINGO_APPLICATION_REQUESTS_PER_HOUR)
                    .map_err(|_| TiingoHttpSourceError::InvalidConfiguration)?,
            )
            .ok_or(TiingoHttpSourceError::InvalidConfiguration)?,
            NonZeroU64::new(PROVIDER_HOUR_NANOS)
                .ok_or(TiingoHttpSourceError::InvalidConfiguration)?,
            BudgetWindowSemantics::Sliding,
        )
        .map_err(|_| TiingoHttpSourceError::InvalidConfiguration)?,
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(
                u32::try_from(TIINGO_APPLICATION_REQUESTS_PER_DAY)
                    .map_err(|_| TiingoHttpSourceError::InvalidConfiguration)?,
            )
            .ok_or(TiingoHttpSourceError::InvalidConfiguration)?,
            NonZeroU64::new(PROVIDER_DAY_NANOS)
                .ok_or(TiingoHttpSourceError::InvalidConfiguration)?,
            BudgetWindowSemantics::Sliding,
        )
        .map_err(|_| TiingoHttpSourceError::InvalidConfiguration)?,
    ];
    let policy = ProviderBudgetPolicy::try_new_conjunctive(
        BudgetScope::with_authorization_account(provider, subject.clone()),
        &windows,
        NonZeroU16::new(1).ok_or(TiingoHttpSourceError::InvalidConfiguration)?,
        BackoffPolicy::try_new(
            NonZeroU64::new(INITIAL_BACKOFF_NANOS)
                .ok_or(TiingoHttpSourceError::InvalidConfiguration)?,
            NonZeroU64::new(MAXIMUM_BACKOFF_NANOS)
                .ok_or(TiingoHttpSourceError::InvalidConfiguration)?,
            BACKOFF_JITTER_BASIS_POINTS,
        )
        .map_err(|_| TiingoHttpSourceError::InvalidConfiguration)?,
    )
    .map_err(|_| TiingoHttpSourceError::InvalidConfiguration)?;
    ProviderRateDeclaration::try_for_authorization_subject(policy, &subject)
        .map_err(TiingoHttpSourceError::ProviderRate)
}

fn restore_quota(
    store: &Arc<dyn TiingoQuotaStore>,
    initial_windows: TiingoQuotaWindows,
) -> Result<TiingoQuotaLedger, TiingoHttpSourceError> {
    match store.load()? {
        Some(snapshot) => {
            let predecessor = snapshot.digest();
            let mut ledger = TiingoQuotaLedger::try_restore(snapshot)?;
            if ledger.reconcile_incomplete_response()? {
                store.compare_and_swap(Some(predecessor), ledger.snapshot())?;
            }
            Ok(ledger)
        }
        None => {
            let ledger = TiingoQuotaLedger::new(initial_windows);
            store.compare_and_swap(None, ledger.snapshot())?;
            Ok(ledger)
        }
    }
}

fn persist_quota_transition(
    store: &Arc<dyn TiingoQuotaStore>,
    runtime: &mut TiingoRuntime,
    expected: Option<EvidenceDigest>,
) -> Result<(), TiingoHttpSourceError> {
    if let Err(error) = store.compare_and_swap(expected, runtime.quota.snapshot()) {
        runtime.quota_available = false;
        return Err(TiingoHttpSourceError::QuotaStore(error));
    }
    Ok(())
}

fn settle_response_quota(
    store: &Arc<dyn TiingoQuotaStore>,
    runtime: &mut TiingoRuntime,
    permit: &crate::TiingoQuotaPermit,
    ticker: &TiingoTicker,
    actual_response_bytes: u64,
) -> Result<(), TiingoHttpSourceError> {
    let predecessor = runtime.quota.snapshot().digest();
    let transition = runtime
        .quota
        .commit_response(permit, ticker, actual_response_bytes);
    persist_quota_transition(store, runtime, Some(predecessor))?;
    transition.map_err(TiingoHttpSourceError::Quota)
}

fn rate_limit_disposition(
    decision: BudgetDecision,
) -> Result<TiingoRateLimitDisposition, TiingoHttpSourceError> {
    match decision {
        BudgetDecision::WaitUntil(deadline) => Ok(TiingoRateLimitDisposition::WaitUntil(deadline)),
        BudgetDecision::Unavailable(reason) => Ok(TiingoRateLimitDisposition::Unavailable(reason)),
        BudgetDecision::Ready(permit) => {
            permit.release();
            Err(TiingoHttpSourceError::BudgetUnavailable(
                BudgetUnavailableReason::StateCorrupt,
            ))
        }
    }
}

fn history_graph_digest(
    plan: &TiingoHistoryPlan,
    pages: &[TiingoCapturedPage<TiingoEodReceipt>],
    total_response_bytes: u64,
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/tiingo/history-capture/v1\0");
    hash.update(plan.request_set_identity().bytes());
    hash.update(total_response_bytes.to_be_bytes());
    hash.update(u64::try_from(pages.len()).unwrap_or(u64::MAX).to_be_bytes());
    for page in pages {
        hash.update(page.raw.capture.observation_digest().bytes());
    }
    hash.update(b"application-date-windows-exhausted-without-provider-cursor");
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn identifier(value: &str) -> Result<SourceIdentifier, TiingoHttpSourceError> {
    SourceIdentifier::try_from(value).map_err(|_| TiingoHttpSourceError::InvalidConfiguration)
}

fn content_type_is_json(value: Option<&[u8]>) -> bool {
    value
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

fn hardened_client() -> Result<reqwest::Client, TiingoHttpSourceError> {
    let _tls = market_squawk_sources::install_ring_tls_provider()
        .map_err(|_| TiingoHttpSourceError::InvalidConfiguration)?;
    reqwest::Client::builder()
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
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_TIMEOUT)
        .timeout(TOTAL_TIMEOUT)
        .build()
        .map_err(|_| TiingoHttpSourceError::InvalidConfiguration)
}

fn remaining_timeout(
    deadline: Timestamp,
    now: Timestamp,
) -> Result<Duration, TiingoHttpSourceError> {
    deadline
        .unix_nanos()
        .checked_sub(now.unix_nanos())
        .and_then(|nanos| u64::try_from(nanos).ok())
        .filter(|nanos| *nanos > 0)
        .map(Duration::from_nanos)
        .map(|remaining| remaining.min(TOTAL_TIMEOUT))
        .ok_or_else(|| {
            TiingoHttpSourceError::Transport(transport_failure(
                TiingoTransportFailureKind::DeadlineExceeded,
                0,
                0,
                Instant::now(),
            ))
        })
}

fn system_timestamp() -> Result<Timestamp, TiingoHttpSourceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| TiingoHttpSourceError::ClockUnavailable)?;
    let nanos = u128::from(elapsed.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(elapsed.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(TiingoHttpSourceError::ClockUnavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn latency_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn transport_failure(
    kind: TiingoTransportFailureKind,
    received_body_bytes: u64,
    quota_charge_bytes: u64,
    started: Instant,
) -> TiingoTransportFailure {
    TiingoTransportFailure {
        kind,
        received_body_bytes,
        quota_charge_bytes,
        latency_nanos: latency_nanos(started),
    }
}

pub(crate) trait TiingoTransport: fmt::Debug + Send + Sync {
    fn execute(
        &self,
        request: reqwest::Request,
        max_response_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<TiingoHttpResponseMaterial, TiingoTransportFailure>>;
}

#[derive(Debug)]
struct ReqwestTiingoTransport {
    client: reqwest::Client,
}

impl ReqwestTiingoTransport {
    const fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl TiingoTransport for ReqwestTiingoTransport {
    fn execute(
        &self,
        request: reqwest::Request,
        max_response_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<TiingoHttpResponseMaterial, TiingoTransportFailure>> {
        Box::pin(async move {
            let started = Instant::now();
            let deadline = tokio::time::Instant::now() + timeout;
            let response = tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Err(transport_failure(
                        TiingoTransportFailureKind::Cancelled,
                        0,
                        u64::try_from(max_response_bytes).unwrap_or(u64::MAX),
                        started,
                    ));
                }
                () = tokio::time::sleep_until(deadline) => {
                    return Err(transport_failure(
                        TiingoTransportFailureKind::DeadlineExceeded,
                        0,
                        u64::try_from(max_response_bytes).unwrap_or(u64::MAX),
                        started,
                    ));
                }
                response = self.client.execute(request) => response.map_err(|_| {
                    transport_failure(
                        TiingoTransportFailureKind::Network,
                        0,
                        u64::try_from(max_response_bytes).unwrap_or(u64::MAX),
                        started,
                    )
                })?,
            };
            if response.content_length().is_some_and(|length| {
                usize::try_from(length).map_or(true, |length| length > max_response_bytes)
            }) {
                let reservation = u64::try_from(max_response_bytes).unwrap_or(u64::MAX);
                return Err(transport_failure(
                    TiingoTransportFailureKind::BodyTooLarge,
                    0,
                    reservation,
                    started,
                ));
            }
            let status = response.status().as_u16();
            let final_url = response.url().clone();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .filter(|value| value.as_bytes().len() <= 128)
                .map(|value| value.as_bytes().to_vec().into_boxed_slice());
            let content_type = match response.headers().get(CONTENT_TYPE) {
                Some(value) if value.as_bytes().len() > 256 => {
                    return Err(transport_failure(
                        TiingoTransportFailureKind::InvalidResponseHeaders,
                        0,
                        u64::try_from(max_response_bytes).unwrap_or(u64::MAX),
                        started,
                    ));
                }
                Some(value) => Some(value.as_bytes().to_vec().into_boxed_slice()),
                None => None,
            };
            let content_encoding = match response.headers().get(CONTENT_ENCODING) {
                Some(value) if value.as_bytes().len() > 64 => {
                    return Err(transport_failure(
                        TiingoTransportFailureKind::InvalidResponseHeaders,
                        0,
                        u64::try_from(max_response_bytes).unwrap_or(u64::MAX),
                        started,
                    ));
                }
                Some(value) => Some(value.as_bytes().to_vec().into_boxed_slice()),
                None => None,
            };
            let mut stream = response.bytes_stream();
            let mut body = BytesMut::new();
            loop {
                let next = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {
                        let observed = u64::try_from(body.len()).unwrap_or(u64::MAX);
                        return Err(transport_failure(
                            TiingoTransportFailureKind::Cancelled,
                            observed,
                            u64::try_from(max_response_bytes).unwrap_or(u64::MAX),
                            started,
                        ));
                    }
                    () = tokio::time::sleep_until(deadline) => {
                        let observed = u64::try_from(body.len()).unwrap_or(u64::MAX);
                        return Err(transport_failure(
                            TiingoTransportFailureKind::DeadlineExceeded,
                            observed,
                            u64::try_from(max_response_bytes).unwrap_or(u64::MAX),
                            started,
                        ));
                    }
                    next = stream.next() => next,
                };
                let Some(chunk) = next else {
                    break;
                };
                let chunk = chunk.map_err(|_| {
                    let observed = u64::try_from(body.len()).unwrap_or(u64::MAX);
                    transport_failure(
                        TiingoTransportFailureKind::Network,
                        observed,
                        u64::try_from(max_response_bytes).unwrap_or(u64::MAX),
                        started,
                    )
                })?;
                let next_bytes = body.len().checked_add(chunk.len()).ok_or_else(|| {
                    transport_failure(
                        TiingoTransportFailureKind::BodyTooLarge,
                        u64::MAX,
                        u64::try_from(max_response_bytes).unwrap_or(u64::MAX),
                        started,
                    )
                })?;
                if next_bytes > max_response_bytes {
                    let observed = u64::try_from(next_bytes).unwrap_or(u64::MAX);
                    return Err(transport_failure(
                        TiingoTransportFailureKind::BodyTooLarge,
                        observed,
                        u64::try_from(max_response_bytes).unwrap_or(u64::MAX),
                        started,
                    ));
                }
                body.extend_from_slice(&chunk);
            }
            let body = body.freeze();
            let body_bytes = u64::try_from(body.len()).unwrap_or(u64::MAX);
            let body_digest =
                EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&body).into());
            Ok(TiingoHttpResponseMaterial {
                status,
                final_url,
                body,
                body_digest,
                retry_after,
                content_type,
                content_encoding,
                received_at: system_timestamp().map_err(|_| {
                    transport_failure(
                        TiingoTransportFailureKind::ClockUnavailable,
                        body_bytes,
                        u64::try_from(max_response_bytes).unwrap_or(u64::MAX),
                        started,
                    )
                })?,
                latency_nanos: latency_nanos(started),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::error::Error;
    use std::num::NonZeroU64;
    use std::sync::{Arc, Mutex};

    use market_squawk_domain::{
        CalendarDate, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
    };
    use market_squawk_sources::{
        AuthorizationMode, ProviderRateDecision, ProviderRateGroupId, ProviderRatePermitId,
        ProviderRateRegistration, ProviderRateRunId, ProviderRateStore, ProviderRateStoreError,
        RetryAfter,
    };
    use reqwest::header::AUTHORIZATION;
    use uuid::Uuid;

    use super::*;

    #[derive(Debug)]
    struct MemoryQuotaStore {
        snapshot: Mutex<Option<TiingoQuotaSnapshot>>,
    }

    impl MemoryQuotaStore {
        fn with_snapshot(snapshot: TiingoQuotaSnapshot) -> Self {
            Self {
                snapshot: Mutex::new(Some(snapshot)),
            }
        }
    }

    impl TiingoQuotaStore for MemoryQuotaStore {
        fn load(&self) -> Result<Option<TiingoQuotaSnapshot>, TiingoQuotaStoreError> {
            self.snapshot
                .lock()
                .map_err(|_| TiingoQuotaStoreError::Unavailable)
                .map(|state| state.clone())
        }

        fn compare_and_swap(
            &self,
            expected: Option<EvidenceDigest>,
            next: &TiingoQuotaSnapshot,
        ) -> Result<(), TiingoQuotaStoreError> {
            let mut state = self
                .snapshot
                .lock()
                .map_err(|_| TiingoQuotaStoreError::Unavailable)?;
            if state.as_ref().map(TiingoQuotaSnapshot::digest) != expected {
                return Err(TiingoQuotaStoreError::Conflict);
            }
            *state = Some(next.clone());
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct MemoryRateStore {
        next_permit: Mutex<u128>,
    }

    impl ProviderRateStore for MemoryRateStore {
        fn start_run(&self, _now: Timestamp) -> Result<ProviderRateRunId, ProviderRateStoreError> {
            Ok(ProviderRateRunId::from_bytes([1; 16]))
        }

        fn register(
            &self,
            _run_id: ProviderRateRunId,
            declaration: &ProviderRateDeclaration,
            _now: Timestamp,
        ) -> Result<ProviderRateRegistration, ProviderRateStoreError> {
            Ok(ProviderRateRegistration::new(
                ProviderRateGroupId::from_bytes([2; 16]),
                declaration.policy_digest(),
                declaration.declaration_digest(),
            ))
        }

        fn try_acquire(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _now: Timestamp,
        ) -> Result<ProviderRateDecision, ProviderRateStoreError> {
            let mut next = self
                .next_permit
                .lock()
                .map_err(|_| ProviderRateStoreError::Unavailable)?;
            *next = next
                .checked_add(1)
                .ok_or(ProviderRateStoreError::Capacity)?;
            Ok(ProviderRateDecision::Ready(
                ProviderRatePermitId::from_bytes(next.to_be_bytes()),
            ))
        }

        fn release(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _permit_id: ProviderRatePermitId,
        ) -> Result<(), ProviderRateStoreError> {
            Ok(())
        }

        fn apply_retry_after(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _now: Timestamp,
            _retry_after: RetryAfter,
        ) -> Result<ProviderRateDecision, ProviderRateStoreError> {
            Ok(ProviderRateDecision::Unavailable(
                BudgetUnavailableReason::Disabled,
            ))
        }

        fn apply_refusal(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _now: Timestamp,
            _jitter_sample_basis_points: u16,
        ) -> Result<ProviderRateDecision, ProviderRateStoreError> {
            Ok(ProviderRateDecision::Unavailable(
                BudgetUnavailableReason::Disabled,
            ))
        }

        fn record_success(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _now: Timestamp,
        ) -> Result<(), ProviderRateStoreError> {
            Ok(())
        }

        fn bind_authorization_subject(
            &self,
            _run_id: ProviderRateRunId,
            _mode: AuthorizationMode,
            _evidence: EvidenceDigest,
            _subject: &SourceIdentifier,
            _now: Timestamp,
        ) -> Result<(), ProviderRateStoreError> {
            Ok(())
        }

        fn resolve_authorization_subject(
            &self,
            _mode: AuthorizationMode,
            _evidence: EvidenceDigest,
        ) -> Result<Option<SourceIdentifier>, ProviderRateStoreError> {
            Ok(None)
        }
    }

    #[derive(Debug)]
    struct MockTransport {
        replies: Mutex<VecDeque<MockReply>>,
    }

    #[derive(Debug)]
    struct MockReply {
        status: u16,
        body: Bytes,
        retry_after: Option<Box<[u8]>>,
    }

    impl MockTransport {
        fn new(replies: Vec<MockReply>) -> Self {
            Self {
                replies: Mutex::new(replies.into()),
            }
        }
    }

    impl TiingoTransport for MockTransport {
        fn execute(
            &self,
            request: reqwest::Request,
            max_response_bytes: usize,
            _timeout: Duration,
            _cancellation: CancellationToken,
        ) -> BoxFuture<'_, Result<TiingoHttpResponseMaterial, TiingoTransportFailure>> {
            Box::pin(async move {
                let started = Instant::now();
                let Some(authorization) = request.headers().get(AUTHORIZATION) else {
                    return Err(transport_failure(
                        TiingoTransportFailureKind::Network,
                        0,
                        0,
                        started,
                    ));
                };
                assert!(authorization.is_sensitive());
                assert!(!request.url().as_str().contains("fixture-token"));
                let reply = self
                    .replies
                    .lock()
                    .map_err(|_| {
                        transport_failure(TiingoTransportFailureKind::Network, 0, 0, started)
                    })?
                    .pop_front()
                    .ok_or_else(|| {
                        transport_failure(TiingoTransportFailureKind::Network, 0, 0, started)
                    })?;
                assert!(reply.body.len() <= max_response_bytes);
                Ok(TiingoHttpResponseMaterial {
                    status: reply.status,
                    final_url: request.url().clone(),
                    body_digest: EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        Sha256::digest(&reply.body).into(),
                    ),
                    body: reply.body,
                    retry_after: reply.retry_after,
                    content_type: Some(Box::from(&b"application/json"[..])),
                    content_encoding: None,
                    received_at: system_timestamp().map_err(|_| {
                        transport_failure(
                            TiingoTransportFailureKind::ClockUnavailable,
                            0,
                            0,
                            started,
                        )
                    })?,
                    latency_nanos: 1,
                })
            })
        }
    }

    fn identifier(value: &str) -> Result<SourceIdentifier, Box<dyn Error>> {
        Ok(SourceIdentifier::try_from(value)?)
    }

    #[tokio::test]
    async fn bounded_http_journey_reconciles_restart_quota_and_proves_all_terminal_shapes()
    -> Result<(), Box<dyn Error>> {
        let now = system_timestamp()?;
        let windows = TiingoQuotaWindows::try_new(
            now,
            Timestamp::from_unix_nanos(now.unix_nanos() + 3_600_000_000_000),
            Timestamp::from_unix_nanos(now.unix_nanos() + 86_400_000_000_000),
            Timestamp::from_unix_nanos(now.unix_nanos() + 2_592_000_000_000_000),
        )?;
        let ticker = TiingoTicker::try_new("VTSAX")?;
        let mut interrupted = TiingoQuotaLedger::new(windows);
        let Ok(_pending) = interrupted.reserve(
            ticker.clone(),
            NonZeroU64::new(64).ok_or("nonzero crash reservation")?,
        )?
        else {
            return Err("unexpected crash-reservation denial".into());
        };
        let quota_store = Arc::new(MemoryQuotaStore::with_snapshot(
            interrupted.snapshot().clone(),
        ));
        let rate_authority = ProviderRateAuthority::try_new(Arc::new(MemoryRateStore::default()))?;

        let success_bodies = vec![
            Bytes::from_static(br#"{"ticker":"VTSAX","name":"Vanguard Total Stock Market Index Fund Admiral Shares","exchangeCode":"MF","description":"Mutual fund","startDate":"2000-11-13","endDate":"2026-01-02"}"#),
            Bytes::from_static(br#"[{"date":"2026-01-02T00:00:00.000Z","open":151.23,"high":151.23,"low":151.23,"close":151.23,"volume":0,"adjOpen":151.23,"adjHigh":151.23,"adjLow":151.23,"adjClose":151.23,"adjVolume":0,"divCash":0,"splitFactor":1}]"#),
            Bytes::from_static(br#"[{"date":"2025-01-01T00:00:00.000Z","open":140,"high":140,"low":140,"close":140,"volume":0,"adjOpen":140,"adjHigh":140,"adjLow":140,"adjClose":140,"adjVolume":0,"divCash":0,"splitFactor":1}]"#),
            Bytes::from_static(br#"[{"date":"2026-01-02T00:00:00.000Z","open":151.23,"high":151.23,"low":151.23,"close":151.23,"volume":0,"adjOpen":151.23,"adjHigh":151.23,"adjLow":151.23,"adjClose":151.23,"adjVolume":0,"divCash":0,"splitFactor":1}]"#),
        ];
        let refusal_body = Bytes::from_static(br#"{"detail":"rate limit"}"#);
        let expected_bytes = 64_u64
            + success_bodies
                .iter()
                .map(|body| u64::try_from(body.len()))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .sum::<u64>()
            + u64::try_from(refusal_body.len())?;
        let mut replies = success_bodies
            .into_iter()
            .map(|body| MockReply {
                status: 200,
                body,
                retry_after: None,
            })
            .collect::<Vec<_>>();
        replies.push(MockReply {
            status: 429,
            body: refusal_body.clone(),
            retry_after: Some(Box::from(&b"60"[..])),
        });
        let source = TiingoHttpSource::try_new_with_transport(
            TiingoApiToken::try_new("fixture-token".to_owned())?,
            &rate_authority,
            quota_store.clone(),
            windows,
            SourceId::try_from("tiingo-starter")?,
            MetadataRevision::new(identifier("tiingo-source-metadata-v1")?),
            identifier("tiingo-daily-native-v1")?,
            Arc::new(MockTransport::new(replies)),
        )?;
        let deadline =
            Timestamp::from_unix_nanos(system_timestamp()?.unix_nanos() + 60_000_000_000);
        let cancellation = CancellationToken::new();

        let metadata = source
            .fetch_metadata(ticker.clone(), deadline, &cancellation)
            .await?;
        assert_eq!(metadata.decoded().metadata().ticker(), &ticker);
        assert_eq!(
            metadata.raw().capture().terminal(),
            ProviderCaptureTerminalDisposition::StandaloneResponse
        );
        let material = metadata
            .raw()
            .capture_material(Uuid::from_u128(1), Uuid::from_u128(2))?;
        assert_eq!(material.receipt(), metadata.raw().capture());
        assert_eq!(material.records().len(), 1);
        assert_eq!(
            material.records()[0].payload(),
            metadata.raw().http().body()
        );
        let latest = source
            .fetch_latest(ticker.clone(), deadline, &cancellation)
            .await?;
        assert_eq!(latest.decoded().rows().len(), 1);
        let history = source
            .fetch_history(
                TiingoHistoryPlan::try_new(
                    ticker,
                    CalendarDate::new(2025, 1, 1)?,
                    CalendarDate::new(2026, 1, 2)?,
                )?,
                deadline,
                &cancellation,
            )
            .await?;
        assert_eq!(history.pages().len(), 2);
        assert_eq!(
            history.terminal(),
            TiingoHistoryTerminalDisposition::ApplicationDateWindowsExhaustedWithoutProviderCursor
        );
        assert!(
            history
                .pages()
                .iter()
                .all(|page| page.raw().capture().terminal()
                    == ProviderCaptureTerminalDisposition::StandaloneResponse)
        );
        match source
            .fetch_latest(TiingoTicker::try_new("VTSAX")?, deadline, &cancellation)
            .await
        {
            Err(TiingoHttpSourceError::Provider(failure)) => {
                assert_eq!(failure.provider().status(), 429);
                assert_eq!(failure.provider().response_bytes(), &refusal_body);
                assert_eq!(
                    failure.rate_limit(),
                    Some(TiingoRateLimitDisposition::Unavailable(
                        BudgetUnavailableReason::Disabled
                    ))
                );
            }
            _ => return Err("expected preserved Tiingo 429 refusal".into()),
        }

        let snapshot = source.quota_snapshot().await;
        assert!(snapshot.pending_response().is_none());
        assert_eq!(snapshot.requests_this_hour(), 6);
        assert_eq!(snapshot.response_bytes_this_month(), expected_bytes);
        let encoded = serde_json::to_vec(&snapshot)?;
        let restored: TiingoQuotaSnapshot = serde_json::from_slice(&encoded)?;
        assert_eq!(restored, snapshot);

        let restarted = TiingoHttpSource::try_new_with_transport(
            TiingoApiToken::try_new("fixture-token".to_owned())?,
            &rate_authority,
            quota_store,
            windows,
            SourceId::try_from("tiingo-starter")?,
            MetadataRevision::new(identifier("tiingo-source-metadata-v1")?),
            identifier("tiingo-daily-native-v1")?,
            Arc::new(MockTransport::new(Vec::new())),
        )?;
        assert_eq!(restarted.quota_snapshot().await, snapshot);
        Ok(())
    }
}
