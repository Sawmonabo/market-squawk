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
    BackoffPolicy, BudgetPoolError, BudgetScope, BudgetUnavailableReason,
    BudgetWindowSemantics, MonotonicInstant, ProviderBudgetPolicy, ProviderBudgetWindow,
    ProviderCaptureError, ProviderCaptureMaterial, ProviderCapturePageReceipt,
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition, ProviderRateDeclaration,
};
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE, RETRY_AFTER};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{
    TIINGO_APPLICATION_REQUESTS_PER_DAY, TIINGO_APPLICATION_REQUESTS_PER_HOUR, TiingoAdapterError,
    TiingoApiToken, TiingoCompletedResponseDisposition, TiingoDecoder, TiingoEndpointFamily,
    TiingoEodReceipt, TiingoMetadataReceipt, TiingoProviderAdmissionDecision,
    TiingoProviderAdmissionRequest, TiingoProviderAuthority, TiingoProviderAuthorityError,
    TiingoProviderAuthorityInstallation, TiingoProviderAuthorityRequirements,
    TiingoProviderFailure, TiingoProviderPermit, TiingoQuotaAdmission, TiingoRateLimitDisposition,
    TiingoCompletedHistoryCapture, TiingoHistoryCheckpointReceipt, TiingoHistoryEvidenceError,
    TiingoHistoryPlan, TiingoRequestBuilder, TiingoRequestSpec, TiingoResponseSettlement,
    TiingoSchemaChange, TiingoSchemaCircuitState, TiingoSealedHistoryPage, TiingoTicker,
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
const TIINGO_SOURCE_ID: &str = "tiingo-starter";

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

struct TiingoPendingRawMaterial {
    raw: TiingoRawMaterial,
    permit: TiingoProviderPermit,
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
    /// The single shared provider/account authority failed closed.
    #[error(transparent)]
    ProviderAuthority(#[from] TiingoProviderAuthorityError),
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
    /// Transport failed and the shared authority could not atomically settle its permit.
    #[error("Tiingo transport failure could not be settled in shared authority")]
    TransportSettlementPersistence {
        /// Exact bounded transport failure that remains reconcilable.
        failure: TiingoTransportFailure,
        /// Shared authority failure; the permit must remain crash-reconcilable and blocking.
        #[source]
        authority: TiingoProviderAuthorityError,
    },
    /// Tiingo returned a bounded non-success response, retained exactly.
    #[error("Tiingo returned a bounded non-success HTTP response")]
    Provider(Box<TiingoProviderHttpFailure>),
    /// A successful HTTP response violated URL or content-header invariants.
    #[error("Tiingo HTTP response violated the transport contract")]
    InvalidHttpResponse(Box<TiingoHttpResponseMaterial>),
    /// A complete HTTP response could not atomically commit its terminal queue meaning.
    #[error("Tiingo HTTP response could not be settled in shared authority")]
    HttpSettlementPersistence {
        /// Exact retained response material.
        response: Box<TiingoHttpResponseMaterial>,
        /// Shared authority failure; concurrency must remain unavailable until reconciliation.
        #[source]
        authority: TiingoProviderAuthorityError,
    },
    /// A complete successful response could not be bound to source-neutral capture evidence.
    #[error("Tiingo response could not be bound to source-neutral capture evidence")]
    CaptureResponse {
        /// Exact retained successful response material.
        response: Box<TiingoHttpResponseMaterial>,
        /// Exact source-neutral capture contract failure.
        #[source]
        capture: ProviderCaptureError,
    },
    /// Exact sealed pages could not close the durably checkpointed history request graph.
    #[error(transparent)]
    HistoryEvidence(#[from] TiingoHistoryEvidenceError),
    /// Strict native decoding failed; exact raw material remains attached.
    #[error("Tiingo provider-native decoding failed")]
    Decode(Box<TiingoDecodeFailure>),
    /// A non-schema decode failure could not atomically settle its durable permit.
    #[error("Tiingo decode failure could not be settled in shared authority")]
    DecodeSettlementPersistence {
        /// Exact raw response plus strict decode failure.
        failure: Box<TiingoDecodeFailure>,
        /// Shared authority failure; the pending permit must remain blocking.
        #[source]
        authority: TiingoProviderAuthorityError,
    },
    /// Strict decoding succeeded but its terminal permit settlement failed closed.
    #[error("Tiingo decoded response could not be settled in shared authority")]
    DecodedSuccessSettlementPersistence {
        /// Exact raw response; deterministic strict decode can be repeated after reconciliation.
        raw: Box<TiingoRawMaterial>,
        /// Shared authority failure; the pending permit must remain blocking.
        #[source]
        authority: TiingoProviderAuthorityError,
    },
    /// A complete raw response could not enter strict decoding because its decode clock failed.
    #[error("Tiingo decode clock is unavailable")]
    DecodeClockUnavailable {
        /// Exact raw response rejected by the authoritative settlement.
        raw: Box<TiingoRawMaterial>,
    },
    /// Decode-clock failure could not atomically reject the complete raw response.
    #[error("Tiingo decode-clock failure could not be settled in shared authority")]
    DecodeClockSettlementPersistence {
        /// Exact raw response that remains deterministically decodable after reconciliation.
        raw: Box<TiingoRawMaterial>,
        /// Shared authority failure; the pending permit must remain blocking.
        #[source]
        authority: TiingoProviderAuthorityError,
    },
    /// A schema-changing body was retained, but durable circuit opening failed closed.
    #[error("Tiingo schema change could not be committed to shared authority")]
    SchemaCircuitPersistence {
        /// Exact raw response and schema failure that must remain reviewable.
        failure: Box<TiingoDecodeFailure>,
        /// Shared authority failure; its queue must remain unavailable across restart.
        #[source]
        authority: TiingoProviderAuthorityError,
    },
    /// Trusted wall-clock material could not be represented.
    #[error("Tiingo source clock is unavailable")]
    ClockUnavailable,
}

/// Credential-bearing, request-serialized Tiingo Starter HTTP source.
pub struct TiingoHttpSource {
    requests: TiingoRequestBuilder,
    transport: Arc<dyn TiingoTransport>,
    authority: Arc<dyn TiingoProviderAuthority>,
    authority_installation: TiingoProviderAuthorityInstallation,
    runtime: tokio::sync::Mutex<TiingoRuntime>,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    native_contract_revision: SourceIdentifier,
    entitlement_generation: SourceIdentifier,
    metadata_dataset: SourceIdentifier,
    latest_dataset: SourceIdentifier,
    history_dataset: SourceIdentifier,
}

struct TiingoRuntime {
    decoder: TiingoDecoder,
    latched_schema_change: Option<TiingoSchemaChange>,
}

impl fmt::Debug for TiingoHttpSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TiingoHttpSource")
            .field("source_id", &self.source_id)
            .field("metadata_revision", &self.metadata_revision)
            .field(
                "authority_generation",
                self.authority_installation.authority_generation(),
            )
            .field("credential", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl TiingoHttpSource {
    /// Opens a hardened production transport behind the one shared durable provider authority.
    ///
    /// The authority must be backed by the product-wide provider/account SQLite queue extended
    /// with Tiingo monthly-symbol, monthly-bandwidth, and schema-circuit state. The API token is
    /// never passed through that capability.
    #[allow(
        clippy::too_many_arguments,
        reason = "security, source identity, schema identity, and durable authority remain explicit"
    )]
    pub fn try_new(
        token: TiingoApiToken,
        authority: Arc<dyn TiingoProviderAuthority>,
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        native_contract_revision: SourceIdentifier,
        entitlement_generation: SourceIdentifier,
    ) -> Result<Self, TiingoHttpSourceError> {
        let client = hardened_client()?;
        let transport: Arc<dyn TiingoTransport> =
            Arc::new(ReqwestTiingoTransport::new(client.clone()));
        Self::try_new_inner(
            token,
            authority,
            source_id,
            metadata_revision,
            native_contract_revision,
            entitlement_generation,
            client,
            transport,
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new_with_transport(
        token: TiingoApiToken,
        authority: Arc<dyn TiingoProviderAuthority>,
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        native_contract_revision: SourceIdentifier,
        entitlement_generation: SourceIdentifier,
        transport: Arc<dyn TiingoTransport>,
    ) -> Result<Self, TiingoHttpSourceError> {
        let client = hardened_client()?;
        Self::try_new_inner(
            token,
            authority,
            source_id,
            metadata_revision,
            native_contract_revision,
            entitlement_generation,
            client,
            transport,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_new_inner(
        token: TiingoApiToken,
        authority: Arc<dyn TiingoProviderAuthority>,
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        native_contract_revision: SourceIdentifier,
        entitlement_generation: SourceIdentifier,
        client: reqwest::Client,
        transport: Arc<dyn TiingoTransport>,
    ) -> Result<Self, TiingoHttpSourceError> {
        if source_id.as_str() != TIINGO_SOURCE_ID {
            return Err(TiingoHttpSourceError::InvalidConfiguration);
        }
        let provider_rate_declaration = tiingo_provider_rate_declaration()?;
        provider_rate_declaration.validate()?;
        let requirements = TiingoProviderAuthorityRequirements::new(
            provider_rate_declaration,
            source_id.clone(),
            metadata_revision.clone(),
            native_contract_revision.clone(),
            entitlement_generation.clone(),
        );
        let authority_installation = authority.validate_requirements(&requirements)?;
        authority_installation.validate_against(&requirements)?;
        let initial_schema_state = authority.schema_circuit_state(&native_contract_revision)?;
        let latched_schema_change = match initial_schema_state {
            TiingoSchemaCircuitState::Closed => None,
            TiingoSchemaCircuitState::Open(change) => Some(change),
        };
        Ok(Self {
            requests: TiingoRequestBuilder::new(client, token),
            transport,
            authority,
            authority_installation,
            runtime: tokio::sync::Mutex::new(TiingoRuntime {
                decoder: TiingoDecoder::new(
                    native_contract_revision.clone(),
                    entitlement_generation.clone(),
                ),
                latched_schema_change,
            }),
            source_id,
            metadata_revision,
            native_contract_revision,
            entitlement_generation,
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
        let pending = self
            .fetch_raw_locked(&mut runtime, spec.clone(), None, deadline, cancellation)
            .await?;
        let raw = pending.raw;
        let decoded_at = self.decode_timestamp_or_reject(&pending.permit, &raw)?;
        match runtime.decoder.decode_metadata(
            spec,
            raw.http.status,
            &raw.http.body,
            raw.http.received_at,
            decoded_at,
        ) {
            Ok(decoded) => {
                self.settle_decoded_success(&pending.permit, &raw)?;
                Ok(TiingoCapturedPage { raw, decoded })
            }
            Err(error) => self.decode_failure(&mut runtime, error, raw, pending.permit),
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
        self.fetch_eod_locked(&mut runtime, spec, None, deadline, cancellation)
            .await
    }

    /// Pre-admits aggregate history capacity and initializes or resumes durable graph state.
    pub fn prepare_history_plan(
        &self,
        plan: &TiingoHistoryPlan,
    ) -> Result<TiingoHistoryCheckpointReceipt, TiingoHttpSourceError> {
        let checkpoint = self.authority.prepare_history_plan(plan)?;
        checkpoint.validate_for(
            plan,
            &self.authority_installation,
            checkpoint.next_page_index(),
            checkpoint.predecessor_page_identity(),
        )?;
        Ok(checkpoint)
    }

    /// Commits one exact externally sealed history page before its successor can dispatch.
    pub fn checkpoint_history_page(
        &self,
        plan: &TiingoHistoryPlan,
        checkpoint: &TiingoHistoryCheckpointReceipt,
        page: &TiingoSealedHistoryPage,
    ) -> Result<TiingoHistoryCheckpointReceipt, TiingoHttpSourceError> {
        checkpoint.validate_for(
            plan,
            &self.authority_installation,
            checkpoint.next_page_index(),
            checkpoint.predecessor_page_identity(),
        )?;
        let current_index = usize::try_from(checkpoint.next_page_index())
            .map_err(|_| TiingoHttpSourceError::InvalidConfiguration)?;
        let Some(expected_request) = plan.pages().get(current_index) else {
            return Err(TiingoHttpSourceError::InvalidConfiguration);
        };
        if page.request() != expected_request
            || page.source_id() != &self.source_id
            || page.source_contract_revision() != &self.metadata_revision
            || page.native_contract_revision() != &self.native_contract_revision
            || page.entitlement_generation() != &self.entitlement_generation
        {
            return Err(TiingoProviderAuthorityError::InvalidReceipt.into());
        }
        let next = self.authority.checkpoint_history_page(checkpoint, page)?;
        let expected_next = checkpoint
            .next_page_index()
            .checked_add(1)
            .ok_or(TiingoProviderAuthorityError::InvalidReceipt)?;
        next.validate_for(
            plan,
            &self.authority_installation,
            expected_next,
            Some(page.page_identity()),
        )?;
        Ok(next)
    }

    /// Closes the exact request graph after every page has been externally sealed/checkpointed.
    pub fn complete_history_capture(
        &self,
        plan: TiingoHistoryPlan,
        pages: Vec<TiingoSealedHistoryPage>,
        checkpoint: &TiingoHistoryCheckpointReceipt,
    ) -> Result<TiingoCompletedHistoryCapture, TiingoHttpSourceError> {
        TiingoCompletedHistoryCapture::try_new(
            plan,
            pages,
            checkpoint,
            &self.authority_installation,
        )
        .map_err(Into::into)
    }

    /// Fetches exactly the one page authorized by the shared durable history checkpoint.
    ///
    /// The caller must seal and durably checkpoint this page before requesting the next page from
    /// its [`crate::TiingoHistoryPlan`]. The source deliberately never buffers a complete history
    /// graph, so a later failure cannot discard already charged successful pages.
    pub async fn fetch_history_page(
        &self,
        plan: &TiingoHistoryPlan,
        checkpoint: &TiingoHistoryCheckpointReceipt,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<TiingoCapturedPage<TiingoEodReceipt>, TiingoHttpSourceError> {
        checkpoint.validate_for(
            plan,
            &self.authority_installation,
            checkpoint.next_page_index(),
            checkpoint.predecessor_page_identity(),
        )?;
        let page_index = usize::try_from(checkpoint.next_page_index())
            .map_err(|_| TiingoHttpSourceError::InvalidConfiguration)?;
        let request = plan
            .pages()
            .get(page_index)
            .ok_or(TiingoHttpSourceError::InvalidConfiguration)?
            .clone();
        let mut runtime = self.runtime.lock().await;
        self.fetch_eod_locked(
            &mut runtime,
            request,
            Some(checkpoint),
            deadline,
            cancellation,
        )
        .await
    }

    /// Returns restart-durable schema-circuit state from the single shared authority.
    pub async fn schema_circuit_state(
        &self,
    ) -> Result<TiingoSchemaCircuitState, TiingoHttpSourceError> {
        let runtime = self.runtime.lock().await;
        if let Some(change) = &runtime.latched_schema_change {
            return Ok(TiingoSchemaCircuitState::Open(change.clone()));
        }
        self.authority
            .schema_circuit_state(runtime.decoder.contract_revision())
            .map_err(Into::into)
    }

    async fn fetch_eod_locked(
        &self,
        runtime: &mut TiingoRuntime,
        spec: TiingoRequestSpec,
        history_checkpoint: Option<&TiingoHistoryCheckpointReceipt>,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<TiingoCapturedPage<TiingoEodReceipt>, TiingoHttpSourceError> {
        let pending = self
            .fetch_raw_locked(
                runtime,
                spec.clone(),
                history_checkpoint,
                deadline,
                cancellation,
            )
            .await?;
        let raw = pending.raw;
        let decoded_at = self.decode_timestamp_or_reject(&pending.permit, &raw)?;
        match runtime.decoder.decode_eod(
            spec,
            raw.http.status,
            &raw.http.body,
            raw.http.received_at,
            decoded_at,
        ) {
            Ok(decoded) => {
                self.settle_decoded_success(&pending.permit, &raw)?;
                Ok(TiingoCapturedPage { raw, decoded })
            }
            Err(error) => self.decode_failure(runtime, error, raw, pending.permit),
        }
    }

    fn settle_decoded_success(
        &self,
        permit: &TiingoProviderPermit,
        raw: &TiingoRawMaterial,
    ) -> Result<(), TiingoHttpSourceError> {
        let settlement = TiingoResponseSettlement::Complete {
            response_bytes: raw.http.response_bytes(),
            disposition: TiingoCompletedResponseDisposition::DecodedSuccess,
        };
        match self.authority.settle_response(permit, &settlement) {
            Ok(None) => Ok(()),
            Ok(Some(_)) => Err(TiingoHttpSourceError::DecodedSuccessSettlementPersistence {
                raw: Box::new(raw.clone()),
                authority: TiingoProviderAuthorityError::InvalidReceipt,
            }),
            Err(authority) => Err(TiingoHttpSourceError::DecodedSuccessSettlementPersistence {
                raw: Box::new(raw.clone()),
                authority,
            }),
        }
    }

    fn decode_timestamp_or_reject(
        &self,
        permit: &TiingoProviderPermit,
        raw: &TiingoRawMaterial,
    ) -> Result<Timestamp, TiingoHttpSourceError> {
        if let Ok(decoded_at) = system_timestamp() {
            return Ok(decoded_at);
        }
        let settlement = TiingoResponseSettlement::Complete {
            response_bytes: raw.http.response_bytes(),
            disposition: TiingoCompletedResponseDisposition::Rejected,
        };
        match self.authority.settle_response(permit, &settlement) {
            Ok(None) => Err(TiingoHttpSourceError::DecodeClockUnavailable {
                raw: Box::new(raw.clone()),
            }),
            Ok(Some(_)) => Err(TiingoHttpSourceError::DecodeClockSettlementPersistence {
                raw: Box::new(raw.clone()),
                authority: TiingoProviderAuthorityError::InvalidReceipt,
            }),
            Err(authority) => Err(TiingoHttpSourceError::DecodeClockSettlementPersistence {
                raw: Box::new(raw.clone()),
                authority,
            }),
        }
    }

    fn decode_failure<T>(
        &self,
        runtime: &mut TiingoRuntime,
        error: TiingoAdapterError,
        raw: TiingoRawMaterial,
        permit: TiingoProviderPermit,
    ) -> Result<T, TiingoHttpSourceError> {
        let schema_change = match &error {
            TiingoAdapterError::SchemaChanged(change) => Some(change.clone()),
            _ => None,
        };
        let failure = Box::new(TiingoDecodeFailure { error, raw });
        let disposition = if let Some(change) = &schema_change {
            runtime.latched_schema_change = Some(change.clone());
            TiingoCompletedResponseDisposition::SchemaChanged {
                contract_revision: runtime.decoder.contract_revision().clone(),
                change: change.clone(),
            }
        } else {
            TiingoCompletedResponseDisposition::Rejected
        };
        let settlement = TiingoResponseSettlement::Complete {
            response_bytes: failure.raw.http.response_bytes(),
            disposition,
        };
        let settlement_result = self.authority.settle_response(&permit, &settlement);
        let authority = match settlement_result {
            Ok(None) => return Err(TiingoHttpSourceError::Decode(failure)),
            Ok(Some(_)) => TiingoProviderAuthorityError::InvalidReceipt,
            Err(authority) => authority,
        };
        if schema_change.is_some() {
            Err(TiingoHttpSourceError::SchemaCircuitPersistence {
                failure,
                authority,
            })
        } else {
            Err(TiingoHttpSourceError::DecodeSettlementPersistence {
                failure,
                authority,
            })
        }
    }

    fn ensure_schema_decode_admitted(
        &self,
        runtime: &mut TiingoRuntime,
    ) -> Result<(), TiingoHttpSourceError> {
        if runtime.latched_schema_change.is_some() {
            return Err(TiingoAdapterError::SchemaCircuitOpen.into());
        }
        match self
            .authority
            .schema_circuit_state(runtime.decoder.contract_revision())?
        {
            TiingoSchemaCircuitState::Closed => Ok(()),
            TiingoSchemaCircuitState::Open(change) => {
                runtime.latched_schema_change = Some(change);
                Err(TiingoAdapterError::SchemaCircuitOpen.into())
            }
        }
    }

    async fn fetch_raw_locked(
        &self,
        runtime: &mut TiingoRuntime,
        spec: TiingoRequestSpec,
        history_checkpoint: Option<&TiingoHistoryCheckpointReceipt>,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<TiingoPendingRawMaterial, TiingoHttpSourceError> {
        self.ensure_schema_decode_admitted(runtime)?;
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
        let admission_request = TiingoProviderAdmissionRequest::new(
            spec.ticker().clone(),
            spec.request_identity(),
            reservation,
            history_checkpoint.cloned(),
        );
        let permit = match self.authority.try_acquire(&admission_request)? {
            TiingoProviderAdmissionDecision::Ready(permit) => permit,
            TiingoProviderAdmissionDecision::WaitUntil(deadline) => {
                return Err(TiingoHttpSourceError::BudgetWaitUntil(deadline));
            }
            TiingoProviderAdmissionDecision::QuotaDenied(admission) => {
                return Err(TiingoHttpSourceError::QuotaDenied(admission));
            }
            TiingoProviderAdmissionDecision::SchemaCircuitOpen(change) => {
                runtime.latched_schema_change = Some(change);
                return Err(TiingoAdapterError::SchemaCircuitOpen.into());
            }
            TiingoProviderAdmissionDecision::Unavailable(reason) => {
                return Err(TiingoHttpSourceError::BudgetUnavailable(reason));
            }
        };
        if !permit.matches(
            &admission_request,
            &self.authority_installation,
        ) {
            return Err(TiingoProviderAuthorityError::InvalidReceipt.into());
        }

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
                let settlement = TiingoResponseSettlement::Incomplete {
                    observed_response_bytes: failure.received_body_bytes,
                    charged_response_bytes: failure.quota_charge_bytes,
                };
                return match self.authority.settle_response(&permit, &settlement) {
                    Ok(None) => Err(TiingoHttpSourceError::Transport(failure)),
                    Ok(Some(_)) => Err(TiingoHttpSourceError::TransportSettlementPersistence {
                        failure,
                        authority: TiingoProviderAuthorityError::InvalidReceipt,
                    }),
                    Err(authority) => {
                        Err(TiingoHttpSourceError::TransportSettlementPersistence {
                            failure,
                            authority,
                        })
                    }
                };
            }
        };

        if !(200..=299).contains(&response.status) {
            let rate_limited = matches!(response.status, 429 | 503);
            let disposition = if rate_limited {
                TiingoCompletedResponseDisposition::ProviderRateLimited {
                    retry_after: response.retry_after.clone(),
                    jitter_sample_basis_points: BACKOFF_JITTER_BASIS_POINTS,
                }
            } else {
                TiingoCompletedResponseDisposition::ProviderRefusal
            };
            let settlement = TiingoResponseSettlement::Complete {
                response_bytes: response.response_bytes(),
                disposition,
            };
            let rate_limit = match self.authority.settle_response(&permit, &settlement) {
                Ok(rate_limit) if rate_limited == rate_limit.is_some() => rate_limit,
                Ok(_) => {
                    return Err(TiingoHttpSourceError::HttpSettlementPersistence {
                        response: Box::new(response),
                        authority: TiingoProviderAuthorityError::InvalidReceipt,
                    });
                }
                Err(authority) => {
                    return Err(TiingoHttpSourceError::HttpSettlementPersistence {
                        response: Box::new(response),
                        authority,
                    });
                }
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
            let settlement = TiingoResponseSettlement::Complete {
                response_bytes: response.response_bytes(),
                disposition: TiingoCompletedResponseDisposition::Rejected,
            };
            match self.authority.settle_response(&permit, &settlement) {
                Ok(None) => {}
                Ok(Some(_)) => {
                    return Err(TiingoHttpSourceError::HttpSettlementPersistence {
                        response: Box::new(response),
                        authority: TiingoProviderAuthorityError::InvalidReceipt,
                    });
                }
                Err(authority) => {
                    return Err(TiingoHttpSourceError::HttpSettlementPersistence {
                        response: Box::new(response),
                        authority,
                    });
                }
            }
            return Err(TiingoHttpSourceError::InvalidHttpResponse(Box::new(
                response,
            )));
        }
        let retained_response = response.clone();
        let raw = match self.capture_success(spec, response) {
            Ok(raw) => raw,
            Err(capture) => {
                let settlement = TiingoResponseSettlement::Complete {
                    response_bytes: retained_response.response_bytes(),
                    disposition: TiingoCompletedResponseDisposition::Rejected,
                };
                match self.authority.settle_response(&permit, &settlement) {
                    Ok(None) => {
                        return Err(TiingoHttpSourceError::CaptureResponse {
                            response: Box::new(retained_response),
                            capture,
                        });
                    }
                    Ok(Some(_)) => {
                        return Err(TiingoHttpSourceError::HttpSettlementPersistence {
                            response: Box::new(retained_response),
                            authority: TiingoProviderAuthorityError::InvalidReceipt,
                        });
                    }
                    Err(authority) => {
                        return Err(TiingoHttpSourceError::HttpSettlementPersistence {
                            response: Box::new(retained_response),
                            authority,
                        });
                    }
                }
            }
        };
        Ok(TiingoPendingRawMaterial { raw, permit })
    }

    fn capture_success(
        &self,
        request: TiingoRequestSpec,
        http: TiingoHttpResponseMaterial,
    ) -> Result<TiingoRawMaterial, ProviderCaptureError> {
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
                        u64::MAX,
                        started,
                    )
                })?;
                if next_bytes > max_response_bytes {
                    let observed = u64::try_from(next_bytes).unwrap_or(u64::MAX);
                    return Err(transport_failure(
                        TiingoTransportFailureKind::BodyTooLarge,
                        observed,
                        observed,
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
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use market_squawk_domain::{
        CalendarDate, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
    };
    use market_squawk_platform::LocalPaths;
    use crate::{
        TiingoHistoryPlan, TiingoQuotaError, TiingoQuotaLedger, TiingoQuotaPermit,
        TiingoQuotaSnapshot, TiingoQuotaWindows,
    };
    use reqwest::header::AUTHORIZATION;
    use uuid::Uuid;

    use super::*;

    #[derive(Debug)]
    struct MemoryProviderAuthority {
        state: Mutex<MemoryProviderState>,
        authority_generation: SourceIdentifier,
        source_id: SourceId,
        source_contract_revision: MetadataRevision,
        contract_revision: SourceIdentifier,
        entitlement_generation: SourceIdentifier,
    }

    #[derive(Debug)]
    struct MemoryProviderState {
        ledger: TiingoQuotaLedger,
        pending: Option<(TiingoProviderPermit, TiingoQuotaPermit)>,
        installation: Option<TiingoProviderAuthorityInstallation>,
        circuit: TiingoSchemaCircuitState,
        provider_disabled: bool,
        history_plan: Option<TiingoHistoryPlan>,
        history_checkpoint: Option<TiingoHistoryCheckpointReceipt>,
        next_permit: u64,
    }

    impl MemoryProviderAuthority {
        fn try_new(
            mut ledger: TiingoQuotaLedger,
            source_id: SourceId,
            source_contract_revision: MetadataRevision,
            contract_revision: SourceIdentifier,
            entitlement_generation: SourceIdentifier,
        ) -> Result<Self, TiingoProviderAuthorityError> {
            ledger
                .reconcile_incomplete_response()
                .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
            Ok(Self {
                state: Mutex::new(MemoryProviderState {
                    ledger,
                    pending: None,
                    installation: None,
                    circuit: TiingoSchemaCircuitState::Closed,
                    provider_disabled: false,
                    history_plan: None,
                    history_checkpoint: None,
                    next_permit: 0,
                }),
                authority_generation: SourceIdentifier::try_from("fixture-rate-authority-1")
                    .map_err(|_| TiingoProviderAuthorityError::Corrupt)?,
                source_id,
                source_contract_revision,
                contract_revision,
                entitlement_generation,
            })
        }

        fn snapshot(&self) -> Result<TiingoQuotaSnapshot, TiingoProviderAuthorityError> {
            self.state
                .lock()
                .map_err(|_| TiingoProviderAuthorityError::Unavailable)
                .map(|state| state.ledger.snapshot().clone())
        }
    }

    impl TiingoProviderAuthority for MemoryProviderAuthority {
        fn validate_requirements(
            &self,
            requirements: &TiingoProviderAuthorityRequirements,
        ) -> Result<TiingoProviderAuthorityInstallation, TiingoProviderAuthorityError> {
            let expected = tiingo_provider_rate_declaration()
                .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
            requirements
                .provider_rate_declaration()
                .validate()
                .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
            if requirements.provider_rate_declaration() != &expected
                || requirements.provider_unique_symbols_per_month()
                    != crate::TIINGO_PROVIDER_UNIQUE_SYMBOLS_PER_MONTH
                || requirements.application_unique_symbols_per_month()
                    != crate::TIINGO_APPLICATION_UNIQUE_SYMBOLS_PER_MONTH
                || requirements.provider_bytes_per_month()
                    != crate::TIINGO_PROVIDER_BYTES_PER_MONTH
                || requirements.application_bytes_per_month()
                    != crate::TIINGO_APPLICATION_BYTES_PER_MONTH
                || requirements.source_id() != &self.source_id
                || requirements.source_contract_revision() != &self.source_contract_revision
                || requirements.native_contract_revision() != &self.contract_revision
                || requirements.entitlement_generation() != &self.entitlement_generation
            {
                return Err(TiingoProviderAuthorityError::InvalidReceipt);
            }
            let mut state = self
                .state
                .lock()
                .map_err(|_| TiingoProviderAuthorityError::Unavailable)?;
            if let Some(existing) = &state.installation {
                existing.validate_against(requirements)?;
                return Ok(existing.clone());
            }
            let installation = TiingoProviderAuthorityInstallation::try_new(
                requirements,
                self.authority_generation.clone(),
                SourceIdentifier::try_from("fixture-tiingo-sqlite-schema-1")
                    .map_err(|_| TiingoProviderAuthorityError::Corrupt)?,
                EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    Sha256::digest(b"fixture-tiingo-authority-installation").into(),
                ),
                system_timestamp().map_err(|_| TiingoProviderAuthorityError::Unavailable)?,
            )?;
            state.installation = Some(installation.clone());
            Ok(installation)
        }

        fn prepare_history_plan(
            &self,
            plan: &TiingoHistoryPlan,
        ) -> Result<TiingoHistoryCheckpointReceipt, TiingoProviderAuthorityError> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| TiingoProviderAuthorityError::Unavailable)?;
            let installation_identity = state
                .installation
                .as_ref()
                .ok_or(TiingoProviderAuthorityError::Conflict)?
                .installation_identity();
            if let (Some(existing_plan), Some(existing_checkpoint)) =
                (&state.history_plan, &state.history_checkpoint)
            {
                if existing_plan == plan {
                    return Ok(existing_checkpoint.clone());
                }
                return Err(TiingoProviderAuthorityError::Conflict);
            }
            let mut authority_hasher = Sha256::new();
            authority_hasher.update(b"fixture-history-plan");
            authority_hasher.update(plan.request_set_identity().bytes());
            let authority_receipt = EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                authority_hasher.finalize().into(),
            );
            let checkpoint = TiingoHistoryCheckpointReceipt::try_new(
                plan,
                0,
                None,
                self.authority_generation.clone(),
                installation_identity,
                authority_receipt,
                system_timestamp().map_err(|_| TiingoProviderAuthorityError::Unavailable)?,
            )?;
            state.history_plan = Some(plan.clone());
            state.history_checkpoint = Some(checkpoint.clone());
            Ok(checkpoint)
        }

        fn checkpoint_history_page(
            &self,
            checkpoint: &TiingoHistoryCheckpointReceipt,
            page: &TiingoSealedHistoryPage,
        ) -> Result<TiingoHistoryCheckpointReceipt, TiingoProviderAuthorityError> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| TiingoProviderAuthorityError::Unavailable)?;
            let installation_identity = state
                .installation
                .as_ref()
                .ok_or(TiingoProviderAuthorityError::Conflict)?
                .installation_identity();
            let plan = state
                .history_plan
                .as_ref()
                .ok_or(TiingoProviderAuthorityError::Conflict)?
                .clone();
            if state.history_checkpoint.as_ref() != Some(checkpoint) {
                return Err(TiingoProviderAuthorityError::Conflict);
            }
            if checkpoint.installation_identity() != installation_identity {
                return Err(TiingoProviderAuthorityError::InvalidReceipt);
            }
            let index = usize::try_from(checkpoint.next_page_index())
                .map_err(|_| TiingoProviderAuthorityError::InvalidReceipt)?;
            if plan.pages().get(index) != Some(page.request()) {
                return Err(TiingoProviderAuthorityError::InvalidReceipt);
            }
            let next_index = checkpoint
                .next_page_index()
                .checked_add(1)
                .ok_or(TiingoProviderAuthorityError::Corrupt)?;
            let mut authority_hasher = Sha256::new();
            authority_hasher.update(checkpoint.authority_receipt().bytes());
            authority_hasher.update(page.page_identity().bytes());
            authority_hasher.update(next_index.to_be_bytes());
            let authority_receipt = EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                authority_hasher.finalize().into(),
            );
            let next = TiingoHistoryCheckpointReceipt::try_new(
                &plan,
                next_index,
                Some(page.page_identity()),
                self.authority_generation.clone(),
                installation_identity,
                authority_receipt,
                system_timestamp().map_err(|_| TiingoProviderAuthorityError::Unavailable)?,
            )?;
            state.history_checkpoint = Some(next.clone());
            Ok(next)
        }

        fn try_acquire(
            &self,
            request: &TiingoProviderAdmissionRequest,
        ) -> Result<TiingoProviderAdmissionDecision, TiingoProviderAuthorityError> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| TiingoProviderAuthorityError::Unavailable)?;
            let installation_identity = state
                .installation
                .as_ref()
                .ok_or(TiingoProviderAuthorityError::Conflict)?
                .installation_identity();
            if let TiingoSchemaCircuitState::Open(change) = &state.circuit {
                return Ok(TiingoProviderAdmissionDecision::SchemaCircuitOpen(
                    change.clone(),
                ));
            }
            if state.provider_disabled {
                return Ok(TiingoProviderAdmissionDecision::Unavailable(
                    BudgetUnavailableReason::Disabled,
                ));
            }
            if state.pending.is_some() {
                return Err(TiingoProviderAuthorityError::Conflict);
            }
            if let Some(checkpoint) = request.history_checkpoint() {
                let plan = state
                    .history_plan
                    .as_ref()
                    .ok_or(TiingoProviderAuthorityError::Conflict)?;
                let current = state
                    .history_checkpoint
                    .as_ref()
                    .ok_or(TiingoProviderAuthorityError::Conflict)?;
                let page_index = usize::try_from(checkpoint.next_page_index())
                    .map_err(|_| TiingoProviderAuthorityError::InvalidReceipt)?;
                if current != checkpoint
                    || checkpoint.installation_identity() != installation_identity
                    || plan
                        .pages()
                        .get(page_index)
                        .map(TiingoRequestSpec::request_identity)
                        != Some(request.request_identity())
                {
                    return Err(TiingoProviderAuthorityError::Conflict);
                }
            }
            let mut next_ledger = state.ledger.clone();
            let internal = match next_ledger
                .reserve(request.ticker().clone(), request.maximum_response_bytes())
                .map_err(|_| TiingoProviderAuthorityError::Corrupt)?
            {
                Ok(permit) => permit,
                Err(admission) => {
                    return Ok(TiingoProviderAdmissionDecision::QuotaDenied(admission));
                }
            };
            let next_permit = state
                .next_permit
                .checked_add(1)
                .ok_or(TiingoProviderAuthorityError::Corrupt)?;
            let permit_identity = EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(next_permit.to_be_bytes()).into(),
            );
            let permit = TiingoProviderPermit::try_new(
                request.ticker().clone(),
                request.request_identity(),
                request.maximum_response_bytes(),
                self.authority_generation.clone(),
                installation_identity,
                request
                    .history_checkpoint()
                    .map(TiingoHistoryCheckpointReceipt::receipt_identity),
                permit_identity,
                system_timestamp().map_err(|_| TiingoProviderAuthorityError::Unavailable)?,
            )?;
            state.ledger = next_ledger;
            state.next_permit = next_permit;
            state.pending = Some((permit.clone(), internal));
            Ok(TiingoProviderAdmissionDecision::Ready(permit))
        }

        fn settle_response(
            &self,
            permit: &TiingoProviderPermit,
            settlement: &TiingoResponseSettlement,
        ) -> Result<Option<TiingoRateLimitDisposition>, TiingoProviderAuthorityError> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| TiingoProviderAuthorityError::Unavailable)?;
            let Some((pending_permit, pending)) = state.pending.as_ref() else {
                return Err(TiingoProviderAuthorityError::Conflict);
            };
            let settlement_is_valid = match settlement {
                TiingoResponseSettlement::Complete { response_bytes, .. } => {
                    *response_bytes <= permit.maximum_response_bytes().get()
                }
                TiingoResponseSettlement::Incomplete {
                    observed_response_bytes,
                    charged_response_bytes,
                } => {
                    observed_response_bytes <= charged_response_bytes
                        && *charged_response_bytes >= permit.maximum_response_bytes().get()
                }
            };
            if pending_permit != permit
                || pending.ticker() != permit.ticker()
                || permit.authority_generation() != &self.authority_generation
                || state
                    .installation
                    .as_ref()
                    .is_none_or(|installation| {
                        permit.installation_identity() != installation.installation_identity()
                    })
                || !settlement_is_valid
            {
                return Err(TiingoProviderAuthorityError::InvalidReceipt);
            }
            let mut next_circuit = state.circuit.clone();
            let mut next_provider_disabled = state.provider_disabled;
            let rate_limit = match settlement.complete_disposition() {
                None
                | Some(
                    TiingoCompletedResponseDisposition::DecodedSuccess
                    | TiingoCompletedResponseDisposition::ProviderRefusal
                    | TiingoCompletedResponseDisposition::Rejected,
                ) => None,
                Some(TiingoCompletedResponseDisposition::ProviderRateLimited {
                    retry_after,
                    jitter_sample_basis_points,
                }) => {
                    if retry_after.as_ref().is_some_and(|value| value.len() > 128)
                        || *jitter_sample_basis_points > 10_000
                    {
                        return Err(TiingoProviderAuthorityError::InvalidReceipt);
                    }
                    next_provider_disabled = true;
                    Some(TiingoRateLimitDisposition::Unavailable(
                        BudgetUnavailableReason::Disabled,
                    ))
                }
                Some(TiingoCompletedResponseDisposition::SchemaChanged {
                    contract_revision,
                    change,
                }) => {
                    if contract_revision != &self.contract_revision {
                        return Err(TiingoProviderAuthorityError::InvalidReceipt);
                    }
                    match &next_circuit {
                        TiingoSchemaCircuitState::Closed => {
                            next_circuit = TiingoSchemaCircuitState::Open(change.clone());
                        }
                        TiingoSchemaCircuitState::Open(existing) if existing == change => {}
                        TiingoSchemaCircuitState::Open(_) => {
                            return Err(TiingoProviderAuthorityError::Conflict);
                        }
                    }
                    None
                }
            };
            let mut next_ledger = state.ledger.clone();
            match next_ledger.commit_response(
                pending,
                permit.ticker(),
                settlement.charged_response_bytes(),
            ) {
                Ok(()) => {}
                Err(TiingoQuotaError::ResponseExceededReservation) => {
                    next_provider_disabled = true;
                }
                Err(_) => return Err(TiingoProviderAuthorityError::Corrupt),
            }
            state.ledger = next_ledger;
            state.circuit = next_circuit;
            state.provider_disabled = next_provider_disabled;
            state.pending = None;
            Ok(rate_limit)
        }

        fn schema_circuit_state(
            &self,
            contract_revision: &SourceIdentifier,
        ) -> Result<TiingoSchemaCircuitState, TiingoProviderAuthorityError> {
            if contract_revision != &self.contract_revision {
                return Err(TiingoProviderAuthorityError::InvalidReceipt);
            }
            self.state
                .lock()
                .map_err(|_| TiingoProviderAuthorityError::Unavailable)
                .map(|state| state.circuit.clone())
        }

    }

    struct TemporaryDirectory(PathBuf);

    impl TemporaryDirectory {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "market-squawk-tiingo-http-{}",
                Uuid::new_v4()
            )))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TemporaryDirectory {
        fn drop(&mut self) {
            let _ignored = std::fs::remove_dir_all(&self.0);
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
        let source_id = SourceId::try_from(TIINGO_SOURCE_ID)?;
        let source_contract_revision =
            MetadataRevision::new(identifier("tiingo-source-metadata-v1")?);
        let contract_revision = identifier("tiingo-daily-native-v1")?;
        let entitlement_generation = identifier("tiingo-entitlement-generation-11")?;
        let authority = Arc::new(MemoryProviderAuthority::try_new(
            interrupted,
            source_id.clone(),
            source_contract_revision.clone(),
            contract_revision.clone(),
            entitlement_generation.clone(),
        )?);

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
            authority.clone(),
            source_id.clone(),
            source_contract_revision.clone(),
            contract_revision.clone(),
            entitlement_generation.clone(),
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
        let history_plan = TiingoHistoryPlan::try_new(
            ticker,
            CalendarDate::new(2025, 1, 1)?,
            CalendarDate::new(2026, 1, 2)?,
        )?;
        let temporary = TemporaryDirectory::new();
        let paths = LocalPaths::prepare(temporary.path())?;
        let store = paths.sealed_research_journal_store()?;
        let mut history_checkpoint = source.prepare_history_plan(&history_plan)?;
        let mut sealed_history_pages = Vec::new();
        while usize::try_from(history_checkpoint.next_page_index())? < history_plan.pages().len() {
            let page_index = usize::try_from(history_checkpoint.next_page_index())?;
            let expected_request = history_plan
                .pages()
                .get(page_index)
                .ok_or("missing expected Tiingo history page")?
                .clone();
            let captured = source
                .fetch_history_page(
                    &history_plan,
                    &history_checkpoint,
                    deadline,
                    &cancellation,
                )
                .await?;
            let sealed_capture = captured
                .capture_material(
                    Uuid::from_u128(10 + u128::try_from(page_index)?),
                    Uuid::from_u128(3),
                )?
                .seal(&store)?;
            let sealed_page = TiingoSealedHistoryPage::try_new(
                &expected_request,
                captured.decoded(),
                &sealed_capture,
            )?;
            history_checkpoint = source.checkpoint_history_page(
                &history_plan,
                &history_checkpoint,
                &sealed_page,
            )?;
            sealed_history_pages.push(sealed_page);
        }
        let completed_history = source.complete_history_capture(
            history_plan,
            sealed_history_pages,
            &history_checkpoint,
        )?;
        assert_eq!(completed_history.pages().len(), 2);
        assert_eq!(
            completed_history.checkpoint_receipt_identity(),
            history_checkpoint.receipt_identity()
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

        let snapshot = authority.snapshot()?;
        assert!(snapshot.pending_response().is_none());
        assert_eq!(snapshot.requests_this_hour(), 6);
        assert_eq!(snapshot.response_bytes_this_month(), expected_bytes);
        let encoded = serde_json::to_vec(&snapshot)?;
        let restored: TiingoQuotaSnapshot = serde_json::from_slice(&encoded)?;
        assert_eq!(restored, snapshot);

        let restarted = TiingoHttpSource::try_new_with_transport(
            TiingoApiToken::try_new("fixture-token".to_owned())?,
            authority.clone(),
            source_id.clone(),
            source_contract_revision.clone(),
            contract_revision,
            entitlement_generation,
            Arc::new(MockTransport::new(Vec::new())),
        )?;
        assert_eq!(authority.snapshot()?, snapshot);
        assert_eq!(
            restarted.schema_circuit_state().await?,
            TiingoSchemaCircuitState::Closed
        );

        let drift_contract = identifier("tiingo-daily-native-v1")?;
        let drift_authority = Arc::new(MemoryProviderAuthority::try_new(
            TiingoQuotaLedger::new(windows),
            source_id.clone(),
            source_contract_revision.clone(),
            drift_contract.clone(),
            identifier("tiingo-entitlement-generation-11")?,
        )?);
        let drift_source = TiingoHttpSource::try_new_with_transport(
            TiingoApiToken::try_new("fixture-token".to_owned())?,
            drift_authority.clone(),
            source_id.clone(),
            source_contract_revision.clone(),
            drift_contract.clone(),
            identifier("tiingo-entitlement-generation-11")?,
            Arc::new(MockTransport::new(vec![MockReply {
                status: 200,
                body: Bytes::from_static(b"{}"),
                retry_after: None,
            }])),
        )?;
        match drift_source
            .fetch_latest(
                TiingoTicker::try_new("VTSAX")?,
                deadline,
                &cancellation,
            )
            .await
        {
            Err(TiingoHttpSourceError::Decode(failure))
                if matches!(failure.error(), TiingoAdapterError::SchemaChanged(_)) => {}
            _ => return Err("expected strict schema drift to open the shared circuit".into()),
        }
        assert!(matches!(
            drift_source.schema_circuit_state().await?,
            TiingoSchemaCircuitState::Open(_)
        ));
        let drift_restarted = TiingoHttpSource::try_new_with_transport(
            TiingoApiToken::try_new("fixture-token".to_owned())?,
            drift_authority,
            source_id,
            source_contract_revision,
            drift_contract,
            identifier("tiingo-entitlement-generation-11")?,
            Arc::new(MockTransport::new(Vec::new())),
        )?;
        assert!(matches!(
            drift_restarted
                .fetch_latest(
                    TiingoTicker::try_new("VTSAX")?,
                    deadline,
                    &cancellation,
                )
                .await,
            Err(TiingoHttpSourceError::Adapter(
                TiingoAdapterError::SchemaCircuitOpen
            ))
        ));
        Ok(())
    }
}
