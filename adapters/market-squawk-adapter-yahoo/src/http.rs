use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures_util::StreamExt as _;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{RawCaptureRecord, RawCaptureRecordError};
use market_squawk_sources::{
    ProviderCaptureError, ProviderCaptureMaterial, ProviderCapturePageReceipt,
    ProviderCaptureSealExpectation, ProviderCaptureSealRequest, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, ProviderWholeCaptureToken, SealedProviderCaptureMaterial,
    SealedProviderCaptureSetReceipt,
};
use reqwest::cookie::Jar;
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER,
    USER_AGENT,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::durable::{
    MAX_YAHOO_DURABLE_CACHE_BODY_BYTES, YahooDurableCacheEntry, YahooDurableState,
};
use crate::native::{
    YahooNativeEvidenceError, YahooNativePublicationEvidence, YahooPendingChartHistory,
};
use crate::{
    AdapterBounds, AdmissionDecision, AdmissionPolicy, AdmissionRejection, AttemptDisposition,
    AttemptKind, AttemptOutcome, AttemptPermit, ChartInterval, ChartWindow, ExplicitDemand,
    LookupKind, ParseContext, YAHOO_SOURCE_ID, YahooAdapterError, YahooAdmission, YahooAssetClass,
    YahooChart, YahooDurableStateStore, YahooEnrichment, YahooFundData, YahooHttpMethod,
    YahooHttpRequest, YahooLocale, YahooLookupHint, YahooOptionChain, YahooQuote, YahooReference,
    YahooRequestFamily, YahooRequestPlan, YahooRequestPlanner, YahooRetryAfterDirective,
    YahooReturnedDisposition, YahooSymbol, YahooTarget, parse_chart_response, parse_fund_response,
    parse_lookup_response, parse_option_response, parse_quote_response, parse_reference_response,
};

const FALLBACK_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const BASIC_COOKIE_URL: &str = "https://fc.yahoo.com";
const BASIC_CRUMB_URL: &str = "https://query1.finance.yahoo.com/v1/test/getcrumb";
const CONSENT_BOOTSTRAP_URL: &str = "https://guce.yahoo.com/consent";
const CONSENT_SUBMIT_URL: &str = "https://consent.yahoo.com/v2/collectConsent";
const CONSENT_COPY_URL: &str = "https://guce.yahoo.com/copyConsent";
const CSRF_CRUMB_URL: &str = "https://query2.finance.yahoo.com/v1/test/getcrumb";
const MAX_RATE_LIMIT_TEXT_SCAN_BYTES: usize = 64 * 1024;

/// Application safety configuration. No field is a Yahoo provider quota or capacity promise.
#[derive(Clone, Copy, Debug)]
pub struct YahooHttpSessionConfig {
    pub adapter_bounds: AdapterBounds,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub total_timeout: Duration,
    pub max_session_response_bytes: usize,
    pub max_crumb_bytes: usize,
    pub max_cache_entries: usize,
    pub max_cache_bytes: usize,
    pub max_redirects: usize,
    pub max_attempt_receipts: usize,
    pub admission_policy: AdmissionPolicy,
}

impl YahooHttpSessionConfig {
    fn validate(self) -> Result<Self, YahooHttpFailureKind> {
        self.adapter_bounds
            .validate()
            .map_err(|_| YahooHttpFailureKind::InvalidConfiguration)?;
        for (name, value) in [
            (
                "max_session_response_bytes",
                self.max_session_response_bytes,
            ),
            ("max_crumb_bytes", self.max_crumb_bytes),
            ("max_cache_entries", self.max_cache_entries),
            ("max_cache_bytes", self.max_cache_bytes),
            ("max_redirects", self.max_redirects),
            ("max_attempt_receipts", self.max_attempt_receipts),
        ] {
            if value == 0 {
                let _ = name;
                return Err(YahooHttpFailureKind::InvalidConfiguration);
            }
        }
        if self.connect_timeout.is_zero()
            || self.read_timeout.is_zero()
            || self.total_timeout.is_zero()
        {
            return Err(YahooHttpFailureKind::InvalidConfiguration);
        }
        Ok(self)
    }
}

/// Caller-owned freshness and fixed monotonic deadline for one explicit-demand execution.
#[derive(Clone, Copy, Debug)]
pub struct YahooExecutionLimits {
    pub deadline: Instant,
    pub maximum_cache_age: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum YahooExecutionDisposition {
    Network,
    CacheHit,
    Coalesced,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "target", content = "family", rename_all = "kebab-case")]
pub enum YahooAttemptTarget {
    CookieBootstrap,
    BasicCrumb,
    ConsentBootstrap,
    ConsentSubmission,
    ConsentCopy,
    CsrfCrumb,
    Data(YahooRequestFamily),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct YahooHttpAttemptReceipt {
    pub kind: AttemptKind,
    pub target: YahooAttemptTarget,
    pub status: Option<u16>,
    pub response_bytes: usize,
    pub response_sha256_hex: Option<String>,
    pub started_at_unix_ms: i64,
    pub completed_at_unix_ms: i64,
    pub latency_ms: u64,
    pub disposition: AttemptDisposition,
}

#[derive(Clone, Debug)]
pub struct YahooRawReceipt {
    pub request: YahooHttpRequest,
    pub request_identity_sha256_hex: String,
    pub request_family: YahooRequestFamily,
    pub request_target_without_crumb: String,
    pub effective_arguments: BTreeMap<String, String>,
    pub response_status: u16,
    pub response_content_type: Option<String>,
    pub response_sha256_hex: String,
    pub response_bytes: Bytes,
    pub received_at_unix_ms: i64,
    pub available_at_unix_ms: i64,
    pub attempts: Box<[YahooHttpAttemptReceipt]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum YahooParsedResponse {
    Quote(YahooReturnedDisposition<YahooQuote>),
    Chart(YahooEnrichment<YahooChart>),
    Reference(YahooEnrichment<YahooReference>),
    Fund(YahooEnrichment<YahooFundData>),
    OptionChain(YahooEnrichment<YahooOptionChain>),
    Lookup(YahooReturnedDisposition<YahooLookupHint>),
}

/// One typed response served to the explicit caller that requested it.
///
/// The value is deliberately not cloneable. Network-owned responses may be consumed once into a
/// pending raw-publication handoff; cache and coalesced responses remain readable with their
/// original provider receipt but cannot manufacture another publication.
#[derive(Debug)]
pub struct YahooHttpResult {
    disposition: YahooExecutionDisposition,
    raw: Arc<YahooRawReceipt>,
    parsed: Arc<YahooParsedResponse>,
}

impl YahooHttpResult {
    /// Returns whether the caller owns a network response or reused existing provider evidence.
    pub const fn disposition(&self) -> YahooExecutionDisposition {
        self.disposition
    }

    /// Returns the exact original provider request, body, clocks, and actual-attempt receipts.
    pub fn raw_receipt(&self) -> &YahooRawReceipt {
        self.raw.as_ref()
    }

    /// Returns the typed response parsed from the exact original provider body.
    pub fn parsed_response(&self) -> &YahooParsedResponse {
        self.parsed.as_ref()
    }

    /// Consumes one network-owned result into a closed, typed raw-publication handoff.
    ///
    /// Cache and coalesced results keep the original receipt for display and analysis, but cannot
    /// create another provider receipt. The returned value owns no sealer, store, revision,
    /// manifest, PIT selector, or application authority.
    pub fn into_pending_publication(
        self,
        binding: YahooPublicationBinding,
    ) -> Result<YahooPendingPublication, YahooPublicationBridgeError> {
        if self.disposition != YahooExecutionDisposition::Network {
            return Err(YahooPublicationBridgeError::NonPublicationResult);
        }
        let native_evidence =
            YahooNativePublicationEvidence::try_new(self.raw.as_ref(), self.parsed.as_ref())?;
        let material = self.raw.capture_material(&binding)?;
        let (expectation, seal_request) = material.into_whole_seal_parts();
        Ok(YahooPendingPublication {
            rejoin: YahooPublicationSealRejoin {
                raw: self.raw,
                parsed: self.parsed,
                binding,
                native_evidence,
                expectation,
            },
            seal_request,
        })
    }
}

/// One noncloneable network response waiting for the shared raw sealer.
#[derive(Debug)]
pub struct YahooPendingPublication {
    rejoin: YahooPublicationSealRejoin,
    seal_request: ProviderCaptureSealRequest,
}

impl YahooPendingPublication {
    /// Splits the one-shot value into the application-sealed request and its opaque typed rejoin.
    pub fn into_sealing_parts(self) -> (YahooPublicationSealRejoin, ProviderCaptureSealRequest) {
        (self.rejoin, self.seal_request)
    }
}

/// Opaque continuation held until the shared sealer returns the matching consuming seal.
pub struct YahooPublicationSealRejoin {
    raw: Arc<YahooRawReceipt>,
    parsed: Arc<YahooParsedResponse>,
    binding: YahooPublicationBinding,
    native_evidence: YahooNativePublicationEvidence,
    expectation: ProviderCaptureSealExpectation,
}

impl YahooPublicationSealRejoin {
    /// Returns the provider-native chart candidate without granting publication authority.
    ///
    /// The owning continuation remains noncloneable and must later be consumed by the common
    /// material-bound seal rejoin before any canonical or durable composition.
    pub fn pending_chart_history(&self) -> Option<YahooPendingChartHistory<'_>> {
        self.native_evidence.pending_chart_history(
            &self.binding,
            self.raw.as_ref(),
            self.parsed.as_ref(),
        )
    }

    /// Rejoins only the physical result split from this exact network response.
    ///
    /// The returned family is typed but deliberately remains pre-canonical: reference and fund
    /// responses are hints, while quote, chart, and option publication still require externally
    /// resolved canonical identity and the corresponding closed shared native-lineage tag.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<YahooSealedPublication, YahooPublicationBridgeError> {
        let token = self.expectation.try_rejoin(sealed)?.try_into_whole()?;
        let family = match self.parsed.as_ref() {
            YahooParsedResponse::Quote(_) => YahooSealedPublicationFamily::CurrentQuotes,
            YahooParsedResponse::Chart(_) => YahooSealedPublicationFamily::HistoricalBars,
            YahooParsedResponse::OptionChain(_) => YahooSealedPublicationFamily::Options,
            YahooParsedResponse::Reference(_) => YahooSealedPublicationFamily::ReferenceHint,
            YahooParsedResponse::Fund(_) => YahooSealedPublicationFamily::FundHint,
            YahooParsedResponse::Lookup(_) => YahooSealedPublicationFamily::LookupHint,
        };
        Ok(YahooSealedPublication {
            family,
            token,
            raw: self.raw,
            parsed: self.parsed,
            binding: self.binding,
            native_evidence: self.native_evidence,
        })
    }
}

impl fmt::Debug for YahooPublicationSealRejoin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parsed_response_family = match self.parsed.as_ref() {
            YahooParsedResponse::Quote(_) => "quote",
            YahooParsedResponse::Chart(_) => "chart_history",
            YahooParsedResponse::Reference(_) => "reference_summary",
            YahooParsedResponse::Fund(_) => "fund_summary",
            YahooParsedResponse::OptionChain(_) => "option_chain",
            YahooParsedResponse::Lookup(_) => "lookup",
        };
        formatter
            .debug_struct("YahooPublicationSealRejoin")
            .field(
                "request_identity_sha256_hex",
                &self.raw.request_identity_sha256_hex,
            )
            .field("request_family", &self.raw.request_family)
            .field("parsed_response_family", &parsed_response_family)
            .field("source_id", &self.binding.source_id())
            .field("metadata_revision", &self.binding.metadata_revision())
            .field("sealed_transition", &"AWAITING_COMMON_MATERIAL_BINDING")
            .finish()
    }
}

/// Truthful post-seal route for the exact Yahoo response family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum YahooSealedPublicationFamily {
    CurrentQuotes,
    HistoricalBars,
    Options,
    ReferenceHint,
    FundHint,
    LookupHint,
}

/// Non-cloneable typed response plus the sole whole-capture authority for its physical raw seal.
///
/// This adapter-local boundary never manufactures canonical identity. The application must
/// consume this value into the correct shared quote, history, or option mapper; hints remain raw.
pub struct YahooSealedPublication {
    family: YahooSealedPublicationFamily,
    token: ProviderWholeCaptureToken,
    raw: Arc<YahooRawReceipt>,
    parsed: Arc<YahooParsedResponse>,
    binding: YahooPublicationBinding,
    native_evidence: YahooNativePublicationEvidence,
}

impl YahooSealedPublication {
    pub const fn family(&self) -> YahooSealedPublicationFamily {
        self.family
    }

    pub fn raw_receipt(&self) -> &YahooRawReceipt {
        self.raw.as_ref()
    }

    pub fn parsed_response(&self) -> &YahooParsedResponse {
        self.parsed.as_ref()
    }

    pub const fn publication_binding(&self) -> &YahooPublicationBinding {
        &self.binding
    }

    pub fn sealed_capture_receipt(&self) -> &SealedProviderCaptureSetReceipt {
        self.token.persisted_receipt()
    }

    pub fn pending_chart_history(&self) -> Option<YahooPendingChartHistory<'_>> {
        self.native_evidence.pending_chart_history(
            &self.binding,
            self.raw.as_ref(),
            self.parsed.as_ref(),
        )
    }

    /// Consumes this adapter handoff for the family-specific canonical mapper.
    pub(crate) fn into_parts(
        self,
    ) -> (
        YahooSealedPublicationFamily,
        ProviderWholeCaptureToken,
        Arc<YahooRawReceipt>,
        Arc<YahooParsedResponse>,
        YahooPublicationBinding,
    ) {
        (self.family, self.token, self.raw, self.parsed, self.binding)
    }
}

impl fmt::Debug for YahooSealedPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("YahooSealedPublication")
            .field("family", &self.family)
            .field(
                "request_identity_sha256_hex",
                &self.raw.request_identity_sha256_hex,
            )
            .field("source_id", &self.binding.source_id())
            .field("metadata_revision", &self.binding.metadata_revision())
            .finish_non_exhaustive()
    }
}

/// Caller-owned capture identity that is independent of Yahoo cookie/crumb state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct YahooPublicationBinding {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    event_id: Uuid,
    connection_id: Uuid,
}

impl YahooPublicationBinding {
    /// Constructs an exact Yahoo source binding owned by the application/capture lifecycle.
    pub fn try_new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        event_id: Uuid,
        connection_id: Uuid,
    ) -> Result<Self, YahooPublicationBridgeError> {
        if source_id.as_str() != YAHOO_SOURCE_ID || event_id.is_nil() || connection_id.is_nil() {
            return Err(YahooPublicationBridgeError::InvalidPublicationBinding);
        }
        Ok(Self {
            source_id,
            metadata_revision,
            event_id,
            connection_id,
        })
    }

    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    pub const fn event_id(&self) -> Uuid {
        self.event_id
    }

    pub const fn connection_id(&self) -> Uuid {
        self.connection_id
    }
}

impl YahooRawReceipt {
    fn capture_material(
        &self,
        binding: &YahooPublicationBinding,
    ) -> Result<ProviderCaptureMaterial, YahooPublicationBridgeError> {
        if self.request.family != self.request_family
            || self.request.request_key != self.request_target_without_crumb
            || self.request.target != self.request_target_without_crumb
            || self.request.effective_arguments != self.effective_arguments
            || request_identity(&self.request) != self.request_identity_sha256_hex
            || !(200..300).contains(&self.response_status)
            || !self
                .response_content_type
                .as_deref()
                .is_some_and(content_type_is_json)
            || self.received_at_unix_ms > self.available_at_unix_ms
        {
            return Err(YahooPublicationBridgeError::InvalidRawReceipt);
        }
        let request_identity = evidence_digest_from_hex(&self.request_identity_sha256_hex)?;
        let retained_body_digest = evidence_digest_from_hex(&self.response_sha256_hex)?;
        let computed_body_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&self.response_bytes).into(),
        );
        if retained_body_digest != computed_body_digest {
            return Err(YahooPublicationBridgeError::InvalidDigest);
        }
        let received_at_nanos = self
            .received_at_unix_ms
            .checked_mul(1_000_000)
            .ok_or(YahooPublicationBridgeError::InvalidTimestamp)?;
        let received_at = Timestamp::from_unix_nanos(received_at_nanos);
        let received_at_utc = DateTime::<Utc>::from_timestamp_millis(self.received_at_unix_ms)
            .ok_or(YahooPublicationBridgeError::InvalidTimestamp)?;
        let body_bytes = u64::try_from(self.response_bytes.len())
            .map_err(|_| YahooPublicationBridgeError::InvalidBodyLength)?;
        let page = ProviderCapturePageReceipt::try_new(
            0,
            request_identity,
            None,
            None,
            self.response_status,
            body_bytes,
            computed_body_digest,
            received_at,
        )?;
        let dataset = SourceIdentifier::try_from(dataset_identity(self.request_family))
            .map_err(|_| YahooPublicationBridgeError::InvalidDataset)?;
        let capture = ProviderCaptureSetReceipt::try_new(
            binding.source_id.clone(),
            binding.metadata_revision.clone(),
            dataset,
            request_identity,
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![page],
        )?;
        let record = RawCaptureRecord::try_new_live(
            binding.event_id,
            Arc::<str>::from(binding.source_id.as_str()),
            binding.connection_id,
            Some(0),
            None,
            received_at_utc,
            self.response_bytes.clone(),
        )?;
        ProviderCaptureMaterial::try_new(capture, vec![record]).map_err(Into::into)
    }
}

#[derive(Debug, Error)]
pub enum YahooPublicationBridgeError {
    #[error("cache and coalesced Yahoo results cannot create another publication")]
    NonPublicationResult,
    #[error("the Yahoo application publication binding is invalid")]
    InvalidPublicationBinding,
    #[error("Yahoo canonical publication request does not match its exact raw response")]
    InvalidCanonicalRequest,
    #[error("Yahoo canonical identity, economics, or clock authority is inconsistent")]
    InvalidCanonicalAuthority,
    #[error("Yahoo response has no canonical rows for the requested family")]
    EmptyCanonicalOutput,
    #[error("Yahoo canonical/native output is invalid or misaligned")]
    InvalidCanonicalOutput,
    #[error("the Yahoo raw request, body, clocks, or schema receipt is inconsistent")]
    InvalidRawReceipt,
    #[error("Yahoo publication timestamp is outside the canonical range")]
    InvalidTimestamp,
    #[error("Yahoo publication digest is malformed or does not match exact bytes")]
    InvalidDigest,
    #[error("the code-owned Yahoo dataset identity is invalid")]
    InvalidDataset,
    #[error("Yahoo publication body length is outside the canonical range")]
    InvalidBodyLength,
    #[error("Yahoo raw capture record is invalid")]
    RawRecord(#[from] RawCaptureRecordError),
    #[error("Yahoo provider capture material is invalid")]
    ProviderCapture(#[from] ProviderCaptureError),
    #[error("Yahoo provider-native pending evidence is inconsistent")]
    NativeEvidence(#[from] YahooNativeEvidenceError),
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum YahooHttpFailureKind {
    #[error("Yahoo HTTP session configuration is invalid")]
    InvalidConfiguration,
    #[error("Yahoo request is not a code-owned selected endpoint")]
    InvalidRequest,
    #[error("Yahoo admission authority is unavailable")]
    AdmissionUnavailable,
    #[error("the serialized Yahoo lane is serving another request")]
    Busy,
    #[error("the Yahoo provider-wide circuit is open until {retry_at_unix_ms}")]
    CircuitOpen { retry_at_unix_ms: i64 },
    #[error("Yahoo explicit-demand work was cancelled")]
    Cancelled,
    #[error("Yahoo explicit-demand deadline elapsed")]
    DeadlineExceeded,
    #[error("Yahoo transport failed")]
    Network,
    #[error("Yahoo response exceeded its application byte bound")]
    ResponseTooLarge,
    #[error("Yahoo returned an unsupported content encoding")]
    UnsupportedEncoding,
    #[error("Yahoo returned HTTP status {status}")]
    ProviderStatus { status: u16 },
    #[error("Yahoo cookie/crumb establishment failed")]
    CrumbUnavailable,
    #[error("Yahoo consent response did not contain the bounded expected fields")]
    ConsentSchema,
    #[error("Yahoo response failed its typed schema boundary")]
    Schema,
    #[error("Yahoo attempt receipt bound was exceeded")]
    AttemptReceiptLimit,
    #[error("Yahoo shared session state is unavailable")]
    StateUnavailable,
}

#[derive(Clone, Debug, Error)]
#[error("{kind}")]
pub struct YahooHttpFailure {
    pub kind: YahooHttpFailureKind,
    pub attempts: Box<[YahooHttpAttemptReceipt]>,
}

#[derive(Clone)]
pub struct YahooHttpSession {
    inner: Arc<SessionInner>,
}

impl fmt::Debug for YahooHttpSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("YahooHttpSession")
            .field("config", &self.inner.config)
            .field("admission", &self.inner.admission)
            .finish_non_exhaustive()
    }
}

struct SessionInner {
    config: YahooHttpSessionConfig,
    endpoints: EndpointSet,
    admission: YahooAdmission,
    durable: Option<YahooDurableStateStore>,
    network: AsyncMutex<NetworkState>,
    shared: Mutex<SharedState>,
}

#[derive(Clone)]
struct EndpointSet {
    cookie: Url,
    basic_crumb: Url,
    consent_bootstrap: Url,
    consent_submit: Url,
    consent_copy: Url,
    csrf_crumb: Url,
    data_rewrite_base: Option<Url>,
    allow_plain_http: bool,
    allowed_hosts: Arc<[String]>,
}

struct NetworkState {
    strategy: CookieStrategy,
    client: WireClient,
    _cookie_jar: Arc<Jar>,
    crumb: Option<Zeroizing<String>>,
}

enum WireClient {
    Production(reqwest::Client),
    #[cfg(test)]
    Scripted(AsyncMutex<ScriptedWireState>),
}

impl WireClient {
    fn production(&self) -> Option<&reqwest::Client> {
        match self {
            Self::Production(client) => Some(client),
            #[cfg(test)]
            Self::Scripted(_) => None,
        }
    }
}

#[cfg(test)]
struct ScriptedWireState {
    responses: VecDeque<ScriptedHttpResponse>,
    observed_targets: Vec<String>,
}

#[cfg(test)]
pub(crate) struct ScriptedHttpResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub retry_after: Option<YahooRetryAfterDirective>,
    pub body: Bytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CookieStrategy {
    Basic,
    Csrf,
}

struct SharedState {
    cache: BTreeMap<String, CacheEntry>,
    cache_bytes: usize,
    next_sequence: u64,
    durable_state_version: u64,
    durable_healthy: bool,
    in_flight: BTreeMap<String, watch::Sender<Option<SharedOutcome>>>,
}

#[derive(Clone)]
struct CacheEntry {
    request: YahooHttpRequest,
    payload: Arc<ExecutionPayload>,
    stored_at_unix_ms: i64,
    bytes: usize,
    sequence: u64,
}

#[derive(Clone)]
struct ExecutionPayload {
    raw: Arc<YahooRawReceipt>,
    parsed: Arc<YahooParsedResponse>,
}

type SharedOutcome = Result<Arc<ExecutionPayload>, Arc<YahooHttpFailure>>;

enum BeginExecution {
    Cached(Arc<ExecutionPayload>),
    Join(watch::Receiver<Option<SharedOutcome>>),
    Owner {
        permit: AttemptPermit,
        sender: watch::Sender<Option<SharedOutcome>>,
    },
    Refused(YahooHttpFailureKind),
}

impl YahooHttpSession {
    pub fn new(config: YahooHttpSessionConfig) -> Result<Self, YahooHttpFailureKind> {
        Self::build(config, EndpointSet::production()?, None)
    }

    /// Constructs one explicit-demand session with crash-safe provider cache and admission state.
    ///
    /// The store is consumed so another session cannot independently own the same provider lane.
    /// Persisted state contains no cookie jar or crumb; those are always re-established in memory.
    pub fn new_with_durable_state(
        config: YahooHttpSessionConfig,
        store: YahooDurableStateStore,
    ) -> Result<Self, YahooHttpFailureKind> {
        Self::build(config, EndpointSet::production()?, Some(store))
    }

    fn build(
        config: YahooHttpSessionConfig,
        endpoints: EndpointSet,
        durable: Option<YahooDurableStateStore>,
    ) -> Result<Self, YahooHttpFailureKind> {
        let config = config.validate()?;
        if durable.is_some() && config.max_cache_bytes > MAX_YAHOO_DURABLE_CACHE_BODY_BYTES {
            return Err(YahooHttpFailureKind::InvalidConfiguration);
        }
        let restored = durable
            .as_ref()
            .map(YahooDurableStateStore::load)
            .transpose()
            .map_err(|_| YahooHttpFailureKind::StateUnavailable)?
            .flatten();
        let (admission, shared) = restore_shared_state(&config, restored)?;
        let network = NetworkState::new(CookieStrategy::Basic, &config, &endpoints)?;
        Ok(Self {
            inner: Arc::new(SessionInner {
                config,
                endpoints,
                admission,
                durable,
                network: AsyncMutex::new(network),
                shared: Mutex::new(shared),
            }),
        })
    }

    pub fn admission(&self) -> YahooAdmission {
        self.inner.admission.clone()
    }

    pub async fn execute_plan(
        &self,
        plan: YahooRequestPlan,
        limits: YahooExecutionLimits,
        cancellation: &CancellationToken,
    ) -> Result<Vec<YahooHttpResult>, YahooHttpFailure> {
        let mut results = Vec::new();
        results.try_reserve(plan.requests.len()).map_err(|_| {
            YahooHttpFailure::without_attempts(YahooHttpFailureKind::StateUnavailable)
        })?;
        for request in plan.requests {
            results.push(self.execute(request, limits, cancellation).await?);
        }
        Ok(results)
    }

    pub async fn execute(
        &self,
        request: YahooHttpRequest,
        limits: YahooExecutionLimits,
        cancellation: &CancellationToken,
    ) -> Result<YahooHttpResult, YahooHttpFailure> {
        if cancellation.is_cancelled() {
            return Err(YahooHttpFailure::without_attempts(
                YahooHttpFailureKind::Cancelled,
            ));
        }
        if limits.deadline <= Instant::now() {
            return Err(YahooHttpFailure::without_attempts(
                YahooHttpFailureKind::DeadlineExceeded,
            ));
        }
        validate_selected_request(&request, self.inner.config.adapter_bounds)?;
        let identity = request_identity(&request);
        match self.begin(&request, &identity, limits.maximum_cache_age)? {
            BeginExecution::Cached(payload) => Ok(result_from_payload(
                payload,
                YahooExecutionDisposition::CacheHit,
            )),
            BeginExecution::Join(receiver) => {
                self.await_join(receiver, limits.deadline, cancellation)
                    .await
            }
            BeginExecution::Refused(kind) => Err(YahooHttpFailure::without_attempts(kind)),
            BeginExecution::Owner { permit, sender } => {
                let outcome = self
                    .execute_owner(
                        &request,
                        identity.clone(),
                        permit,
                        limits.deadline,
                        cancellation,
                    )
                    .await;
                let outcome = self.publish_outcome(&request, &identity, &sender, outcome);
                match outcome {
                    Ok(payload) => Ok(result_from_payload(
                        payload,
                        YahooExecutionDisposition::Network,
                    )),
                    Err(failure) => Err((*failure).clone()),
                }
            }
        }
    }

    fn begin(
        &self,
        request: &YahooHttpRequest,
        identity: &str,
        maximum_cache_age: Duration,
    ) -> Result<BeginExecution, YahooHttpFailure> {
        let mut shared = self.inner.shared.lock().map_err(|_| {
            YahooHttpFailure::without_attempts(YahooHttpFailureKind::StateUnavailable)
        })?;
        if !shared.durable_healthy {
            return Ok(BeginExecution::Refused(
                YahooHttpFailureKind::StateUnavailable,
            ));
        }
        let now = wall_time_ms().map_err(|_| {
            YahooHttpFailure::without_attempts(YahooHttpFailureKind::StateUnavailable)
        })?;
        let maximum_cache_age_ms =
            i64::try_from(duration_ms(maximum_cache_age)).unwrap_or(i64::MAX);
        if !maximum_cache_age.is_zero()
            && let Some(entry) = shared.cache.get(identity)
            && now
                .checked_sub(entry.stored_at_unix_ms)
                .is_some_and(|age| age >= 0 && age <= maximum_cache_age_ms)
        {
            let payload = Arc::clone(&entry.payload);
            self.inner
                .admission
                .record_cache_hit()
                .map_err(YahooHttpFailure::from_admission)?;
            if let Err(kind) = self.persist_quiescent(&mut shared) {
                shared.durable_healthy = false;
                return Err(YahooHttpFailure::without_attempts(kind));
            }
            return Ok(BeginExecution::Cached(payload));
        }
        if let Some(sender) = shared.in_flight.get(identity) {
            self.inner
                .admission
                .record_coalesced_caller()
                .map_err(YahooHttpFailure::from_admission)?;
            return Ok(BeginExecution::Join(sender.subscribe()));
        }
        match self
            .inner
            .admission
            .admit(request, identity, AttemptKind::Primary, now)
            .map_err(YahooHttpFailure::from_admission)?
        {
            AdmissionDecision::Execute(permit) => {
                let (sender, _) = watch::channel(None);
                shared.in_flight.insert(identity.to_owned(), sender.clone());
                Ok(BeginExecution::Owner { permit, sender })
            }
            AdmissionDecision::JoinInFlight { .. } => Err(YahooHttpFailure::without_attempts(
                YahooHttpFailureKind::StateUnavailable,
            )),
            AdmissionDecision::Busy { .. } => {
                Ok(BeginExecution::Refused(YahooHttpFailureKind::Busy))
            }
            AdmissionDecision::CircuitOpen { retry_at_unix_ms } => {
                Ok(BeginExecution::Refused(YahooHttpFailureKind::CircuitOpen {
                    retry_at_unix_ms,
                }))
            }
        }
    }

    async fn await_join(
        &self,
        mut receiver: watch::Receiver<Option<SharedOutcome>>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<YahooHttpResult, YahooHttpFailure> {
        loop {
            if let Some(outcome) = receiver.borrow().clone() {
                return match outcome {
                    Ok(payload) => Ok(result_from_payload(
                        payload,
                        YahooExecutionDisposition::Coalesced,
                    )),
                    Err(failure) => Err((*failure).clone()),
                };
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| {
                    YahooHttpFailure::without_attempts(YahooHttpFailureKind::DeadlineExceeded)
                })?;
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Err(YahooHttpFailure::without_attempts(YahooHttpFailureKind::Cancelled));
                }
                result = tokio::time::timeout(remaining, receiver.changed()) => {
                    match result {
                        Err(_) => return Err(YahooHttpFailure::without_attempts(YahooHttpFailureKind::DeadlineExceeded)),
                        Ok(Err(_)) => return Err(YahooHttpFailure::without_attempts(YahooHttpFailureKind::StateUnavailable)),
                        Ok(Ok(())) => {}
                    }
                }
            }
        }
    }

    async fn execute_owner(
        &self,
        request: &YahooHttpRequest,
        identity: String,
        mut permit: AttemptPermit,
        caller_deadline: Instant,
        cancellation: &CancellationToken,
    ) -> SharedOutcome {
        let configured_deadline = Instant::now()
            .checked_add(self.inner.config.total_timeout)
            .unwrap_or(caller_deadline);
        let deadline = caller_deadline.min(configured_deadline);
        let mut attempts = Vec::new();
        let result = self
            .execute_network(
                request,
                &identity,
                &mut permit,
                &mut attempts,
                deadline,
                cancellation,
            )
            .await;
        let completed_at = wall_time_ms().unwrap_or(i64::MAX);
        match result {
            Ok(payload) => {
                if permit.finish(true, completed_at).is_err() {
                    return Err(Arc::new(YahooHttpFailure::new(
                        YahooHttpFailureKind::AdmissionUnavailable,
                        attempts,
                    )));
                }
                Ok(Arc::new(payload))
            }
            Err(kind) => {
                let _ = permit.finish(false, completed_at);
                Err(Arc::new(YahooHttpFailure::new(kind, attempts)))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_network(
        &self,
        request: &YahooHttpRequest,
        identity: &str,
        permit: &mut AttemptPermit,
        attempts: &mut Vec<YahooHttpAttemptReceipt>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ExecutionPayload, YahooHttpFailureKind> {
        let remaining = remaining(deadline)?;
        let mut network = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(YahooHttpFailureKind::Cancelled),
            result = tokio::time::timeout(remaining, self.inner.network.lock()) => {
                result.map_err(|_| YahooHttpFailureKind::DeadlineExceeded)?
            }
        };

        let mut strategy_fallback_available = true;
        if network.crumb.is_none()
            && let Err(first_error) = self
                .establish_crumb(&mut network, permit, attempts, deadline, cancellation)
                .await
        {
            if !allows_cookie_strategy_fallback(&first_error) {
                return Err(first_error);
            }
            if self.circuit_is_open()? {
                return Err(first_error);
            }
            strategy_fallback_available = false;
            network.switch_strategy(&self.inner.config, &self.inner.endpoints)?;
            self.establish_crumb(&mut network, permit, attempts, deadline, cancellation)
                .await?;
        }

        let primary = self
            .fetch_data(
                &network,
                request,
                AttemptKind::Primary,
                permit,
                attempts,
                deadline,
                cancellation,
            )
            .await;
        match primary {
            Ok(wire) => self.finish_data_response(request, identity, wire, attempts, permit),
            Err(DataFailure::Terminal(kind)) => Err(kind),
            Err(DataFailure::StrategyFallback { status }) => {
                if !strategy_fallback_available || self.circuit_is_open()? {
                    return Err(YahooHttpFailureKind::ProviderStatus { status });
                }
                network.switch_strategy(&self.inner.config, &self.inner.endpoints)?;
                self.establish_crumb(&mut network, permit, attempts, deadline, cancellation)
                    .await?;
                let fallback = self
                    .fetch_data(
                        &network,
                        request,
                        AttemptKind::CookieStrategyFallback,
                        permit,
                        attempts,
                        deadline,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| match failure {
                        DataFailure::Terminal(kind) => kind,
                        DataFailure::StrategyFallback { status } => {
                            YahooHttpFailureKind::ProviderStatus { status }
                        }
                    })?;
                self.finish_data_response(request, identity, fallback, attempts, permit)
            }
        }
    }

    fn finish_data_response(
        &self,
        request: &YahooHttpRequest,
        identity: &str,
        wire: WireResponse,
        attempts: &mut Vec<YahooHttpAttemptReceipt>,
        permit: &mut AttemptPermit,
    ) -> Result<ExecutionPayload, YahooHttpFailureKind> {
        let received_at_unix_ms = wire.completed_at_unix_ms;
        let parse_context = ParseContext {
            received_at_unix_ms,
            available_at_unix_ms: wall_time_ms()
                .map_err(|_| YahooHttpFailureKind::StateUnavailable)?,
        };
        let parsed = match parse_selected_response(
            request,
            &parse_context,
            self.inner.config.adapter_bounds,
            &wire.bytes,
        ) {
            Ok(parsed) => parsed,
            Err(_) => {
                self.record_wire(
                    permit,
                    attempts,
                    wire,
                    0,
                    request_units(request),
                    0,
                    AttemptDisposition::SchemaFailure,
                )?;
                return Err(YahooHttpFailureKind::Schema);
            }
        };
        let measure = parsed.measure(request_units(request));
        let disposition = if measure.missing_units == 0 {
            AttemptDisposition::Success
        } else {
            AttemptDisposition::Partial
        };
        let content_type = wire.content_type.clone();
        let response_status = wire.status;
        let response_sha256_hex = wire
            .response_sha256_hex
            .clone()
            .ok_or(YahooHttpFailureKind::Schema)?;
        let response_bytes = wire.bytes.clone();
        self.record_wire(
            permit,
            attempts,
            wire,
            measure.returned_units,
            measure.missing_units,
            measure.returned_records,
            disposition,
        )?;
        let available_at_unix_ms = parse_context.available_at_unix_ms;
        let raw = Arc::new(YahooRawReceipt {
            request: request.clone(),
            request_identity_sha256_hex: identity.to_owned(),
            request_family: request.family,
            request_target_without_crumb: request.target.clone(),
            effective_arguments: request.effective_arguments.clone(),
            response_status,
            response_content_type: content_type,
            response_sha256_hex,
            response_bytes,
            received_at_unix_ms,
            available_at_unix_ms,
            attempts: attempts.clone().into_boxed_slice(),
        });
        Ok(ExecutionPayload {
            raw,
            parsed: Arc::new(parsed),
        })
    }

    fn publish_outcome(
        &self,
        request: &YahooHttpRequest,
        identity: &str,
        sender: &watch::Sender<Option<SharedOutcome>>,
        outcome: SharedOutcome,
    ) -> SharedOutcome {
        let finalized = match self.inner.shared.lock() {
            Ok(mut shared) => {
                shared.in_flight.remove(identity);
                let result = (|| {
                    if let Ok(payload) = &outcome {
                        let stored_at_unix_ms =
                            wall_time_ms().map_err(|_| YahooHttpFailureKind::StateUnavailable)?;
                        insert_cache(
                            &mut shared,
                            identity,
                            request,
                            payload,
                            stored_at_unix_ms,
                            &self.inner.config,
                        );
                    }
                    self.persist_quiescent(&mut shared)
                })();
                match result {
                    Ok(()) => outcome,
                    Err(_) => {
                        shared.durable_healthy = false;
                        state_failure(&outcome)
                    }
                }
            }
            Err(_) => state_failure(&outcome),
        };
        sender.send_replace(Some(finalized.clone()));
        finalized
    }

    fn persist_quiescent(&self, shared: &mut SharedState) -> Result<(), YahooHttpFailureKind> {
        let Some(store) = &self.inner.durable else {
            return Ok(());
        };
        let admission = self
            .inner
            .admission
            .snapshot()
            .map_err(|_| YahooHttpFailureKind::AdmissionUnavailable)?;
        if admission.active_request_key.is_some()
            || matches!(admission.circuit, crate::CircuitSnapshot::HalfOpen)
        {
            return Ok(());
        }
        let cache = durable_cache_snapshot(shared)?;
        shared.durable_state_version = store
            .compare_and_store(shared.durable_state_version, admission, cache)
            .map_err(|_| YahooHttpFailureKind::StateUnavailable)?;
        Ok(())
    }

    fn circuit_is_open(&self) -> Result<bool, YahooHttpFailureKind> {
        let snapshot = self
            .inner
            .admission
            .snapshot()
            .map_err(|_| YahooHttpFailureKind::AdmissionUnavailable)?;
        Ok(matches!(
            snapshot.circuit,
            crate::CircuitSnapshot::Open { .. }
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn establish_crumb(
        &self,
        network: &mut NetworkState,
        permit: &mut AttemptPermit,
        attempts: &mut Vec<YahooHttpAttemptReceipt>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), YahooHttpFailureKind> {
        if network.crumb.is_some() {
            return Ok(());
        }
        match network.strategy {
            CookieStrategy::Basic => {
                let bootstrap = self
                    .send(
                        &network.client,
                        SendSpec::get(
                            self.inner.endpoints.cookie.clone(),
                            AttemptKind::CookieBootstrap,
                            YahooAttemptTarget::CookieBootstrap,
                            0,
                            self.inner.config.max_session_response_bytes,
                        ),
                        permit,
                        attempts,
                        deadline,
                        cancellation,
                    )
                    .await?;
                self.accept_session_response(bootstrap, permit, attempts)?;
                let crumb = self
                    .send(
                        &network.client,
                        SendSpec::get(
                            self.inner.endpoints.basic_crumb.clone(),
                            AttemptKind::CrumbAcquisition,
                            YahooAttemptTarget::BasicCrumb,
                            0,
                            self.inner.config.max_crumb_bytes,
                        ),
                        permit,
                        attempts,
                        deadline,
                        cancellation,
                    )
                    .await?;
                network.crumb = Some(self.accept_crumb_response(crumb, permit, attempts)?);
            }
            CookieStrategy::Csrf => {
                let bootstrap = self
                    .send(
                        &network.client,
                        SendSpec::get(
                            self.inner.endpoints.consent_bootstrap.clone(),
                            AttemptKind::ConsentBootstrap,
                            YahooAttemptTarget::ConsentBootstrap,
                            0,
                            self.inner.config.max_session_response_bytes,
                        ),
                        permit,
                        attempts,
                        deadline,
                        cancellation,
                    )
                    .await?;
                if wire_indicates_rate_limit(&bootstrap) {
                    return Err(self.record_provider_backoff(bootstrap, permit, attempts)?);
                }
                if !bootstrap.is_success() {
                    let status = bootstrap.status;
                    self.record_wire(
                        permit,
                        attempts,
                        bootstrap,
                        0,
                        0,
                        0,
                        AttemptDisposition::TransportFailure,
                    )?;
                    return Err(YahooHttpFailureKind::ProviderStatus { status });
                }
                let consent =
                    parse_consent_fields(&bootstrap.bytes, self.inner.config.max_crumb_bytes);
                self.record_wire(
                    permit,
                    attempts,
                    bootstrap,
                    0,
                    0,
                    0,
                    if consent.is_ok() {
                        AttemptDisposition::Success
                    } else {
                        AttemptDisposition::SchemaFailure
                    },
                )?;
                let (csrf_token, session_id) = consent?;
                let form = consent_form(&csrf_token, &session_id);
                let mut submit_url = self.inner.endpoints.consent_submit.clone();
                submit_url
                    .query_pairs_mut()
                    .append_pair("sessionId", &session_id);
                let submit = self
                    .send(
                        &network.client,
                        SendSpec::form(
                            reqwest::Method::POST,
                            submit_url,
                            AttemptKind::ConsentSubmission,
                            YahooAttemptTarget::ConsentSubmission,
                            form.clone(),
                            self.inner.config.max_session_response_bytes,
                        ),
                        permit,
                        attempts,
                        deadline,
                        cancellation,
                    )
                    .await?;
                self.accept_session_response(submit, permit, attempts)?;
                let mut copy_url = self.inner.endpoints.consent_copy.clone();
                copy_url
                    .query_pairs_mut()
                    .append_pair("sessionId", &session_id);
                let copy = self
                    .send(
                        &network.client,
                        SendSpec::form(
                            reqwest::Method::GET,
                            copy_url,
                            AttemptKind::ConsentCopy,
                            YahooAttemptTarget::ConsentCopy,
                            form,
                            self.inner.config.max_session_response_bytes,
                        ),
                        permit,
                        attempts,
                        deadline,
                        cancellation,
                    )
                    .await?;
                self.accept_session_response(copy, permit, attempts)?;
                let crumb = self
                    .send(
                        &network.client,
                        SendSpec::get(
                            self.inner.endpoints.csrf_crumb.clone(),
                            AttemptKind::CrumbAcquisition,
                            YahooAttemptTarget::CsrfCrumb,
                            0,
                            self.inner.config.max_crumb_bytes,
                        ),
                        permit,
                        attempts,
                        deadline,
                        cancellation,
                    )
                    .await?;
                network.crumb = Some(self.accept_crumb_response(crumb, permit, attempts)?);
            }
        }
        Ok(())
    }

    fn accept_session_response(
        &self,
        wire: WireResponse,
        permit: &mut AttemptPermit,
        attempts: &mut Vec<YahooHttpAttemptReceipt>,
    ) -> Result<(), YahooHttpFailureKind> {
        if wire_indicates_rate_limit(&wire) {
            return Err(self.record_provider_backoff(wire, permit, attempts)?);
        }
        // The pinned cookie bootstrap and consent submit/copy paths validate the following crumb,
        // not these intermediate statuses. A completed non-429 response is therefore progress.
        self.record_wire(permit, attempts, wire, 0, 0, 0, AttemptDisposition::Success)
    }

    fn accept_crumb_response(
        &self,
        wire: WireResponse,
        permit: &mut AttemptPermit,
        attempts: &mut Vec<YahooHttpAttemptReceipt>,
    ) -> Result<Zeroizing<String>, YahooHttpFailureKind> {
        if wire_indicates_rate_limit(&wire) {
            return Err(self.record_provider_backoff(wire, permit, attempts)?);
        }
        let crumb = std::str::from_utf8(&wire.bytes)
            .ok()
            .map(str::trim)
            .filter(|value| {
                !value.is_empty()
                    && value.len() <= self.inner.config.max_crumb_bytes
                    && !value.contains('<')
                    && !value.chars().any(char::is_control)
            })
            .map(str::to_owned);
        self.record_wire(
            permit,
            attempts,
            wire,
            0,
            0,
            0,
            if crumb.is_some() {
                AttemptDisposition::Success
            } else {
                AttemptDisposition::SchemaFailure
            },
        )?;
        crumb
            .map(Zeroizing::new)
            .ok_or(YahooHttpFailureKind::CrumbUnavailable)
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_data(
        &self,
        network: &NetworkState,
        request: &YahooHttpRequest,
        kind: AttemptKind,
        permit: &mut AttemptPermit,
        attempts: &mut Vec<YahooHttpAttemptReceipt>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<WireResponse, DataFailure> {
        let crumb = network.crumb.as_ref().ok_or(DataFailure::Terminal(
            YahooHttpFailureKind::CrumbUnavailable,
        ))?;
        let mut target = Url::parse(&request.target)
            .map_err(|_| DataFailure::Terminal(YahooHttpFailureKind::InvalidRequest))?;
        if let Some(base) = &self.inner.endpoints.data_rewrite_base {
            let mut rewritten = base.clone();
            rewritten.set_path(target.path());
            rewritten.set_query(target.query());
            target = rewritten;
        }
        target
            .query_pairs_mut()
            .append_pair("crumb", crumb.as_str());
        let wire = self
            .send(
                &network.client,
                SendSpec::get(
                    target,
                    kind,
                    YahooAttemptTarget::Data(request.family),
                    request_units(request),
                    self.inner.config.adapter_bounds.max_response_bytes,
                ),
                permit,
                attempts,
                deadline,
                cancellation,
            )
            .await
            .map_err(DataFailure::Terminal)?;
        if wire_indicates_rate_limit(&wire) {
            return Err(DataFailure::Terminal(
                self.record_provider_backoff(wire, permit, attempts)
                    .unwrap_or(YahooHttpFailureKind::AdmissionUnavailable),
            ));
        }
        if !wire.is_success() || !wire.is_json() || wire.final_url_is_consent() {
            let status = wire.status;
            self.record_wire(
                permit,
                attempts,
                wire,
                0,
                request_units(request),
                0,
                if status >= 400 {
                    AttemptDisposition::TransportFailure
                } else {
                    AttemptDisposition::SchemaFailure
                },
            )
            .map_err(DataFailure::Terminal)?;
            return Err(DataFailure::StrategyFallback { status });
        }
        Ok(wire)
    }

    #[allow(clippy::too_many_arguments)]
    async fn send(
        &self,
        client: &WireClient,
        spec: SendSpec,
        permit: &mut AttemptPermit,
        attempts: &mut Vec<YahooHttpAttemptReceipt>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<WireResponse, YahooHttpFailureKind> {
        self.ensure_attempt_slot(attempts)?;
        #[cfg(test)]
        if let WireClient::Scripted(state) = client {
            return self
                .send_scripted(state, spec, deadline, cancellation)
                .await;
        }
        let client = client
            .production()
            .ok_or(YahooHttpFailureKind::InvalidConfiguration)?;
        let started_at_unix_ms =
            wall_time_ms().map_err(|_| YahooHttpFailureKind::StateUnavailable)?;
        let started = Instant::now();
        let kind = spec.kind;
        let target = spec.target;
        let observation_units = spec.observation_units;
        let maximum_bytes = spec.maximum_bytes;
        let mut builder = client
            .request(spec.method, spec.url)
            .header(ACCEPT, "application/json,text/plain,*/*")
            .header(ACCEPT_ENCODING, "identity")
            .header(USER_AGENT, FALLBACK_USER_AGENT);
        if let Some(form) = spec.form.as_ref() {
            let encoded: String = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(
                    form.iter()
                        .map(|(key, value)| (key.as_str(), value.as_str())),
                )
                .finish();
            builder = builder
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(encoded);
        }
        let response = match deadline_wait(deadline, cancellation, builder.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                self.record_failed_attempt(
                    permit,
                    attempts,
                    kind,
                    target,
                    observation_units,
                    started_at_unix_ms,
                    started,
                    YahooHttpFailureKind::Network,
                )?;
                return Err(YahooHttpFailureKind::Network);
            }
            Err(kind_error) => {
                self.record_failed_attempt(
                    permit,
                    attempts,
                    kind,
                    target,
                    observation_units,
                    started_at_unix_ms,
                    started,
                    kind_error.clone(),
                )?;
                return Err(kind_error);
            }
        };
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_retry_after);
        let final_url = response.url().clone();
        if status == 429 {
            let completed_at_unix_ms =
                wall_time_ms().map_err(|_| YahooHttpFailureKind::StateUnavailable)?;
            let wire = WireResponse {
                kind,
                target,
                status,
                response_sha256_hex: None,
                bytes: Bytes::new(),
                content_type,
                retry_after,
                final_url,
                started_at_unix_ms,
                completed_at_unix_ms,
                latency_ms: duration_ms(started.elapsed()),
                observation_units,
            };
            return Err(self.record_provider_backoff(wire, permit, attempts)?);
        }
        if response
            .headers()
            .get(CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| !value.eq_ignore_ascii_case("identity"))
        {
            self.record_failed_attempt(
                permit,
                attempts,
                kind,
                target,
                observation_units,
                started_at_unix_ms,
                started,
                YahooHttpFailureKind::UnsupportedEncoding,
            )?;
            return Err(YahooHttpFailureKind::UnsupportedEncoding);
        }
        if response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            .is_some_and(|length| length > maximum_bytes)
        {
            self.record_failed_attempt(
                permit,
                attempts,
                kind,
                target,
                observation_units,
                started_at_unix_ms,
                started,
                YahooHttpFailureKind::ResponseTooLarge,
            )?;
            return Err(YahooHttpFailureKind::ResponseTooLarge);
        }
        let mut stream = response.bytes_stream();
        let mut bytes = BytesMut::new();
        while let Some(chunk) = match deadline_wait(deadline, cancellation, stream.next()).await {
            Ok(chunk) => chunk,
            Err(kind_error) => {
                self.record_failed_attempt(
                    permit,
                    attempts,
                    kind,
                    target,
                    observation_units,
                    started_at_unix_ms,
                    started,
                    kind_error.clone(),
                )?;
                return Err(kind_error);
            }
        } {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(_) => {
                    self.record_failed_attempt(
                        permit,
                        attempts,
                        kind,
                        target,
                        observation_units,
                        started_at_unix_ms,
                        started,
                        YahooHttpFailureKind::Network,
                    )?;
                    return Err(YahooHttpFailureKind::Network);
                }
            };
            let new_length = bytes
                .len()
                .checked_add(chunk.len())
                .ok_or(YahooHttpFailureKind::ResponseTooLarge)?;
            if new_length > maximum_bytes {
                self.record_failed_attempt(
                    permit,
                    attempts,
                    kind,
                    target,
                    observation_units,
                    started_at_unix_ms,
                    started,
                    YahooHttpFailureKind::ResponseTooLarge,
                )?;
                return Err(YahooHttpFailureKind::ResponseTooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        let bytes = bytes.freeze();
        Ok(WireResponse {
            kind,
            target,
            status,
            response_sha256_hex: Some(sha256_hex(&bytes)),
            bytes,
            content_type,
            retry_after,
            final_url,
            started_at_unix_ms,
            completed_at_unix_ms: wall_time_ms()
                .map_err(|_| YahooHttpFailureKind::StateUnavailable)?,
            latency_ms: duration_ms(started.elapsed()),
            observation_units,
        })
    }

    #[cfg(test)]
    async fn send_scripted(
        &self,
        state: &AsyncMutex<ScriptedWireState>,
        spec: SendSpec,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<WireResponse, YahooHttpFailureKind> {
        let started = Instant::now();
        let started_at_unix_ms =
            wall_time_ms().map_err(|_| YahooHttpFailureKind::StateUnavailable)?;
        let remaining = remaining(deadline)?;
        let mut state = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(YahooHttpFailureKind::Cancelled),
            value = tokio::time::timeout(remaining, state.lock()) => {
                value.map_err(|_| YahooHttpFailureKind::DeadlineExceeded)?
            }
        };
        state.observed_targets.push(spec.url.to_string());
        let response = state
            .responses
            .pop_front()
            .ok_or(YahooHttpFailureKind::Network)?;
        if response.status != 429 && response.body.len() > spec.maximum_bytes {
            return Err(YahooHttpFailureKind::ResponseTooLarge);
        }
        let bytes = response.body;
        Ok(WireResponse {
            kind: spec.kind,
            target: spec.target,
            status: response.status,
            response_sha256_hex: Some(sha256_hex(&bytes)),
            bytes,
            content_type: Some(response.content_type.to_owned()),
            retry_after: response.retry_after,
            final_url: spec.url,
            started_at_unix_ms,
            completed_at_unix_ms: wall_time_ms()
                .map_err(|_| YahooHttpFailureKind::StateUnavailable)?,
            latency_ms: duration_ms(started.elapsed()),
            observation_units: spec.observation_units,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn record_failed_attempt(
        &self,
        permit: &mut AttemptPermit,
        attempts: &mut Vec<YahooHttpAttemptReceipt>,
        kind: AttemptKind,
        target: YahooAttemptTarget,
        observation_units: usize,
        started_at_unix_ms: i64,
        started: Instant,
        failure: YahooHttpFailureKind,
    ) -> Result<(), YahooHttpFailureKind> {
        let disposition = match failure {
            YahooHttpFailureKind::Cancelled => AttemptDisposition::Cancelled,
            YahooHttpFailureKind::DeadlineExceeded => AttemptDisposition::DeadlineExceeded,
            YahooHttpFailureKind::InvalidConfiguration
            | YahooHttpFailureKind::InvalidRequest
            | YahooHttpFailureKind::AdmissionUnavailable
            | YahooHttpFailureKind::Busy
            | YahooHttpFailureKind::CircuitOpen { .. }
            | YahooHttpFailureKind::ProviderStatus { .. }
            | YahooHttpFailureKind::CrumbUnavailable
            | YahooHttpFailureKind::ConsentSchema
            | YahooHttpFailureKind::Schema
            | YahooHttpFailureKind::AttemptReceiptLimit
            | YahooHttpFailureKind::StateUnavailable
            | YahooHttpFailureKind::Network
            | YahooHttpFailureKind::ResponseTooLarge
            | YahooHttpFailureKind::UnsupportedEncoding => AttemptDisposition::TransportFailure,
        };
        let completed_at_unix_ms = wall_time_ms().unwrap_or(started_at_unix_ms);
        self.push_attempt(
            permit,
            attempts,
            YahooHttpAttemptReceipt {
                kind,
                target,
                status: None,
                response_bytes: 0,
                response_sha256_hex: None,
                started_at_unix_ms,
                completed_at_unix_ms,
                latency_ms: duration_ms(started.elapsed()),
                disposition,
            },
            0,
            observation_units,
            0,
        )
    }

    fn record_provider_backoff(
        &self,
        wire: WireResponse,
        permit: &mut AttemptPermit,
        attempts: &mut Vec<YahooHttpAttemptReceipt>,
    ) -> Result<YahooHttpFailureKind, YahooHttpFailureKind> {
        let retry_after = wire.retry_after;
        let status = wire.status;
        let completed_at = wire.completed_at_unix_ms;
        let units = wire.observation_units;
        self.record_wire(
            permit,
            attempts,
            wire,
            0,
            units,
            0,
            AttemptDisposition::ProviderBackoff {
                status,
                retry_after,
            },
        )?;
        let retry_at_unix_ms = self
            .inner
            .admission
            .snapshot()
            .map_err(|_| YahooHttpFailureKind::AdmissionUnavailable)?
            .circuit
            .retry_at(completed_at);
        Ok(YahooHttpFailureKind::CircuitOpen { retry_at_unix_ms })
    }

    #[allow(clippy::too_many_arguments)]
    fn record_wire(
        &self,
        permit: &mut AttemptPermit,
        attempts: &mut Vec<YahooHttpAttemptReceipt>,
        wire: WireResponse,
        returned_units: usize,
        missing_units: usize,
        returned_records: usize,
        disposition: AttemptDisposition,
    ) -> Result<(), YahooHttpFailureKind> {
        let receipt = YahooHttpAttemptReceipt {
            kind: wire.kind,
            target: wire.target,
            status: Some(wire.status),
            response_bytes: wire.bytes.len(),
            response_sha256_hex: wire.response_sha256_hex,
            started_at_unix_ms: wire.started_at_unix_ms,
            completed_at_unix_ms: wire.completed_at_unix_ms,
            latency_ms: wire.latency_ms,
            disposition,
        };
        self.push_attempt(
            permit,
            attempts,
            receipt,
            returned_units,
            missing_units,
            returned_records,
        )
    }

    fn push_attempt(
        &self,
        permit: &mut AttemptPermit,
        attempts: &mut Vec<YahooHttpAttemptReceipt>,
        receipt: YahooHttpAttemptReceipt,
        returned_units: usize,
        missing_units: usize,
        returned_records: usize,
    ) -> Result<(), YahooHttpFailureKind> {
        if attempts.len() >= self.inner.config.max_attempt_receipts {
            return Err(YahooHttpFailureKind::AttemptReceiptLimit);
        }
        permit
            .record_actual_attempt(
                receipt.kind,
                AttemptOutcome {
                    returned_units,
                    missing_units,
                    returned_records,
                    response_bytes: receipt.response_bytes,
                    latency_ms: receipt.latency_ms,
                    disposition: receipt.disposition,
                },
                receipt.completed_at_unix_ms,
            )
            .map_err(|_| YahooHttpFailureKind::AdmissionUnavailable)?;
        attempts.push(receipt);
        Ok(())
    }

    fn ensure_attempt_slot(
        &self,
        attempts: &[YahooHttpAttemptReceipt],
    ) -> Result<(), YahooHttpFailureKind> {
        if attempts.len() >= self.inner.config.max_attempt_receipts {
            return Err(YahooHttpFailureKind::AttemptReceiptLimit);
        }
        Ok(())
    }
}

impl crate::CircuitSnapshot {
    fn retry_at(&self, fallback: i64) -> i64 {
        match self {
            Self::Open { retry_at_unix_ms } => *retry_at_unix_ms,
            Self::Closed | Self::HalfOpen => fallback,
        }
    }
}

#[derive(Clone, Copy)]
struct ResponseMeasure {
    returned_units: usize,
    missing_units: usize,
    returned_records: usize,
}

impl YahooParsedResponse {
    fn measure(&self, requested_units: usize) -> ResponseMeasure {
        match self {
            Self::Quote(value) => measured_disposition(value, requested_units),
            Self::Lookup(value) => measured_disposition(value, requested_units),
            Self::Chart(value) => {
                let returned = usize::from(
                    value
                        .data
                        .as_ref()
                        .is_some_and(crate::YahooChart::has_usable_market_data),
                );
                let records = value.data.as_ref().map_or(0, |chart| {
                    chart
                        .valid_bar_count
                        .saturating_add(chart.provider_action_count())
                });
                measured_single(requested_units, returned, records)
            }
            Self::Reference(value) => measured_single(
                requested_units,
                usize::from(value.data.is_some()),
                usize::from(value.data.is_some()),
            ),
            Self::Fund(value) => measured_single(
                requested_units,
                usize::from(value.data.is_some()),
                value
                    .data
                    .as_ref()
                    .map_or(0, |fund| 1usize.saturating_add(fund.top_holdings.len())),
            ),
            Self::OptionChain(value) => measured_single(
                requested_units,
                usize::from(value.data.is_some()),
                value
                    .data
                    .as_ref()
                    .map_or(0, |chain| chain.valid_contract_count),
            ),
        }
    }
}

fn measured_disposition<T>(
    value: &YahooReturnedDisposition<T>,
    requested_units: usize,
) -> ResponseMeasure {
    let returned_units = value.valid_observations.min(requested_units);
    ResponseMeasure {
        returned_units,
        missing_units: requested_units.saturating_sub(returned_units),
        returned_records: value.valid_observations,
    }
}

fn measured_single(
    requested_units: usize,
    returned_units: usize,
    returned_records: usize,
) -> ResponseMeasure {
    let returned_units = returned_units.min(requested_units);
    ResponseMeasure {
        returned_units,
        missing_units: requested_units.saturating_sub(returned_units),
        returned_records,
    }
}

fn parse_selected_response(
    request: &YahooHttpRequest,
    context: &ParseContext,
    bounds: AdapterBounds,
    bytes: &[u8],
) -> Result<YahooParsedResponse, YahooAdapterError> {
    match request.family {
        YahooRequestFamily::Quote => {
            parse_quote_response(request, context, bounds, bytes).map(YahooParsedResponse::Quote)
        }
        YahooRequestFamily::ChartHistory => {
            parse_chart_response(request, context, bounds, bytes).map(YahooParsedResponse::Chart)
        }
        YahooRequestFamily::ReferenceSummary => {
            parse_reference_response(request, context, bounds, bytes)
                .map(YahooParsedResponse::Reference)
        }
        YahooRequestFamily::FundSummary => {
            parse_fund_response(request, context, bounds, bytes).map(YahooParsedResponse::Fund)
        }
        YahooRequestFamily::OptionChain => parse_option_response(request, context, bounds, bytes)
            .map(YahooParsedResponse::OptionChain),
        YahooRequestFamily::Search | YahooRequestFamily::Lookup => {
            parse_lookup_response(request, context, bounds, bytes).map(YahooParsedResponse::Lookup)
        }
    }
}

fn validate_selected_request(
    request: &YahooHttpRequest,
    bounds: AdapterBounds,
) -> Result<(), YahooHttpFailure> {
    if rebuild_selected_request(request, bounds).as_ref() != Some(request) {
        return Err(YahooHttpFailure::without_attempts(
            YahooHttpFailureKind::InvalidRequest,
        ));
    }
    Ok(())
}

/// Rebuilds the selected endpoint through the sole code-owned planner.
///
/// `YahooHttpRequest` remains deserializable because the bounded durable cache persists it. A
/// deserialized value is therefore untrusted until every parser-affecting input, URL component,
/// query argument, schema pin, and target has reproduced the exact planner output.
fn rebuild_selected_request(
    request: &YahooHttpRequest,
    bounds: AdapterBounds,
) -> Option<YahooHttpRequest> {
    let bounds = bounds.validate().ok()?;
    let demand = ExplicitDemand::new(
        request.demand.operation_id().to_owned(),
        request.demand.requested_at_unix_ms(),
        request.demand.purpose(),
        bounds.max_string_bytes,
    )
    .ok()?;
    if demand != request.demand {
        return None;
    }
    for target in &request.requested_targets {
        if !admitted_asset(target.asset_class)
            || YahooSymbol::parse(target.symbol.as_str().to_owned(), bounds.max_string_bytes)
                .ok()
                .as_ref()
                != Some(&target.symbol)
        {
            return None;
        }
    }

    let url = Url::parse(&request.target).ok()?;
    if url.scheme() != "https"
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    let query = unique_query(&url)?;
    if query.contains_key("crumb") {
        return None;
    }

    let plan = match request.family {
        YahooRequestFamily::Quote => planner_with_request_locale(bounds, &query)?
            .quote(demand, request.requested_targets.clone())
            .ok()?,
        YahooRequestFamily::ChartHistory => {
            let target = exactly_one_target(request)?;
            let interval = ChartInterval::from_provider_value(query.get("interval")?)?;
            let include_pre_post = match query.get("includePrePost")?.as_str() {
                "false" => false,
                "true" => true,
                _ => return None,
            };
            neutral_locale_planner(bounds)?
                .chart_history(
                    demand,
                    vec![target],
                    selected_chart_window(&query)?,
                    interval,
                    include_pre_post,
                )
                .ok()?
        }
        YahooRequestFamily::ReferenceSummary => planner_with_request_locale(bounds, &query)?
            .reference(demand, exactly_one_target(request)?)
            .ok()?,
        YahooRequestFamily::FundSummary => planner_with_request_locale(bounds, &query)?
            .fund(demand, exactly_one_target(request)?)
            .ok()?,
        YahooRequestFamily::OptionChain => {
            let expiration = match query.get("date") {
                Some(value) => Some(value.parse().ok()?),
                None => None,
            };
            neutral_locale_planner(bounds)?
                .option_chain(demand, exactly_one_target(request)?, expiration)
                .ok()?
        }
        YahooRequestFamily::Search => {
            let text = query.get("q")?.clone();
            let requested_results = query.get("quotesCount")?.parse().ok()?;
            neutral_locale_planner(bounds)?
                .search(demand, text, requested_results)
                .ok()?
        }
        YahooRequestFamily::Lookup => {
            let text = query.get("query")?.clone();
            let requested_results = query.get("count")?.parse().ok()?;
            let kind = match query.get("type")?.as_str() {
                "equity" => LookupKind::Equity,
                "mutualfund" => LookupKind::MutualFund,
                "etf" => LookupKind::Etf,
                "index" => LookupKind::Index,
                _ => return None,
            };
            planner_with_request_locale(bounds, &query)?
                .lookup(demand, text, kind, requested_results)
                .ok()?
        }
    };
    exactly_one_planned_request(plan)
}

fn unique_query(url: &Url) -> Option<BTreeMap<String, String>> {
    let mut query = BTreeMap::new();
    for (key, value) in url.query_pairs() {
        if query.insert(key.into_owned(), value.into_owned()).is_some() {
            return None;
        }
    }
    Some(query)
}

fn planner_with_request_locale(
    bounds: AdapterBounds,
    query: &BTreeMap<String, String>,
) -> Option<YahooRequestPlanner> {
    let locale = YahooLocale::new(
        query.get("lang")?.clone(),
        query.get("region")?.clone(),
        bounds.max_string_bytes,
    )
    .ok()?;
    YahooRequestPlanner::new(bounds, locale).ok()
}

fn neutral_locale_planner(bounds: AdapterBounds) -> Option<YahooRequestPlanner> {
    let locale = YahooLocale::new("a", "a", bounds.max_string_bytes).ok()?;
    YahooRequestPlanner::new(bounds, locale).ok()
}

fn exactly_one_target(request: &YahooHttpRequest) -> Option<YahooTarget> {
    let [target] = request.requested_targets.as_slice() else {
        return None;
    };
    Some(target.clone())
}

fn exactly_one_planned_request(mut plan: YahooRequestPlan) -> Option<YahooHttpRequest> {
    if plan.requests.len() != 1 {
        return None;
    }
    plan.requests.pop()
}

fn selected_chart_window(query: &BTreeMap<String, String>) -> Option<ChartWindow> {
    match (
        query.get("range"),
        query.get("period1"),
        query.get("period2"),
    ) {
        (Some(range), None, None) => ChartWindow::from_provider_range(range),
        (None, Some(start), Some(end)) => {
            let start_unix_seconds = start.parse().ok()?;
            let end_exclusive_unix_seconds = end.parse().ok()?;
            (start_unix_seconds < end_exclusive_unix_seconds).then_some(ChartWindow::UnixRange {
                start_unix_seconds,
                end_exclusive_unix_seconds,
            })
        }
        _ => None,
    }
}

const fn admitted_asset(asset: YahooAssetClass) -> bool {
    matches!(
        asset,
        YahooAssetClass::Equity
            | YahooAssetClass::Etf
            | YahooAssetClass::Index
            | YahooAssetClass::MutualFund
            | YahooAssetClass::OptionUnderlying
            | YahooAssetClass::ReferenceHint
    )
}

fn request_units(request: &YahooHttpRequest) -> usize {
    if !request.requested_targets.is_empty() {
        request.requested_targets.len()
    } else {
        request
            .effective_arguments
            .get("requested_result_count")
            .and_then(|value| value.parse().ok())
            .unwrap_or(1)
    }
}

pub(crate) fn request_identity(request: &YahooHttpRequest) -> String {
    let mut digest = Sha256::new();
    digest_field(&mut digest, b"market-squawk.yahoo.request-identity.v2");
    digest_field(
        &mut digest,
        match request.method {
            YahooHttpMethod::Get => b"get",
        },
    );
    digest_field(&mut digest, request_family_identity(request.family));
    digest_field(&mut digest, request.target.as_bytes());
    digest_field(&mut digest, request.request_key.as_bytes());
    digest_field(
        &mut digest,
        if request.requires_cookie_crumb_session {
            b"cookie-crumb-required"
        } else {
            b"cookie-crumb-not-required"
        },
    );
    digest_field(
        &mut digest,
        &u64::try_from(request.requested_targets.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for target in &request.requested_targets {
        digest_field(&mut digest, target.symbol.as_str().as_bytes());
        digest_field(&mut digest, asset_class_identity(target.asset_class));
    }
    digest_field(
        &mut digest,
        &u64::try_from(request.effective_arguments.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for (key, value) in &request.effective_arguments {
        digest_field(&mut digest, key.as_bytes());
        digest_field(&mut digest, value.as_bytes());
    }
    // Explicit-demand identity authorizes the caller but does not change the upstream request or
    // parser inputs. Cache/coalesced callers retain the original receipt and are never presented as
    // a new provider acquisition.
    sha256_finish(digest)
}

fn digest_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

const fn request_family_identity(family: YahooRequestFamily) -> &'static [u8] {
    match family {
        YahooRequestFamily::Quote => b"quote",
        YahooRequestFamily::ChartHistory => b"chart-history",
        YahooRequestFamily::ReferenceSummary => b"reference-summary",
        YahooRequestFamily::FundSummary => b"fund-summary",
        YahooRequestFamily::OptionChain => b"option-chain",
        YahooRequestFamily::Search => b"search",
        YahooRequestFamily::Lookup => b"lookup",
    }
}

const fn asset_class_identity(asset_class: YahooAssetClass) -> &'static [u8] {
    match asset_class {
        YahooAssetClass::Equity => b"equity",
        YahooAssetClass::Etf => b"etf",
        YahooAssetClass::Index => b"index",
        YahooAssetClass::MutualFund => b"mutual-fund",
        YahooAssetClass::OptionUnderlying => b"option-underlying",
        YahooAssetClass::ReferenceHint => b"reference-hint",
    }
}

fn result_from_payload(
    payload: Arc<ExecutionPayload>,
    disposition: YahooExecutionDisposition,
) -> YahooHttpResult {
    YahooHttpResult {
        disposition,
        raw: Arc::clone(&payload.raw),
        parsed: Arc::clone(&payload.parsed),
    }
}

fn insert_cache(
    shared: &mut SharedState,
    identity: &str,
    request: &YahooHttpRequest,
    payload: &Arc<ExecutionPayload>,
    stored_at_unix_ms: i64,
    config: &YahooHttpSessionConfig,
) {
    let bytes = payload.raw.response_bytes.len();
    if bytes > config.max_cache_bytes {
        return;
    }
    if let Some(previous) = shared.cache.remove(identity) {
        shared.cache_bytes = shared.cache_bytes.saturating_sub(previous.bytes);
    }
    while shared.cache.len() >= config.max_cache_entries
        || shared.cache_bytes.saturating_add(bytes) > config.max_cache_bytes
    {
        let Some(oldest_key) = shared
            .cache
            .iter()
            .min_by_key(|(_, entry)| entry.sequence)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        if let Some(removed) = shared.cache.remove(&oldest_key) {
            shared.cache_bytes = shared.cache_bytes.saturating_sub(removed.bytes);
        }
    }
    let sequence = shared.next_sequence;
    shared.next_sequence = shared.next_sequence.saturating_add(1);
    shared.cache.insert(
        identity.to_owned(),
        CacheEntry {
            request: request.clone(),
            payload: Arc::clone(payload),
            stored_at_unix_ms,
            bytes,
            sequence,
        },
    );
    shared.cache_bytes = shared.cache_bytes.saturating_add(bytes);
}

fn restore_shared_state(
    config: &YahooHttpSessionConfig,
    restored: Option<YahooDurableState>,
) -> Result<(YahooAdmission, SharedState), YahooHttpFailureKind> {
    let Some(restored) = restored else {
        return Ok((
            YahooAdmission::new(config.admission_policy),
            SharedState {
                cache: BTreeMap::new(),
                cache_bytes: 0,
                next_sequence: 1,
                durable_state_version: 0,
                durable_healthy: true,
                in_flight: BTreeMap::new(),
            },
        ));
    };
    if restored.cache.len() > config.max_cache_entries {
        return Err(YahooHttpFailureKind::StateUnavailable);
    }
    let admission = YahooAdmission::try_restore(config.admission_policy, restored.admission)
        .map_err(|_| YahooHttpFailureKind::StateUnavailable)?;
    let mut cache = BTreeMap::new();
    let mut cache_bytes = 0_usize;
    for persisted in restored.cache {
        let identity = persisted.request_identity_sha256_hex.clone();
        let entry = restore_cache_entry(config, persisted)?;
        cache_bytes = cache_bytes
            .checked_add(entry.bytes)
            .ok_or(YahooHttpFailureKind::StateUnavailable)?;
        if cache_bytes > config.max_cache_bytes || cache.insert(identity, entry).is_some() {
            return Err(YahooHttpFailureKind::StateUnavailable);
        }
    }
    let maximum_sequence = cache
        .values()
        .map(|entry| entry.sequence)
        .max()
        .unwrap_or(0);
    let next_sequence = maximum_sequence
        .checked_add(1)
        .ok_or(YahooHttpFailureKind::StateUnavailable)?;
    Ok((
        admission,
        SharedState {
            cache,
            cache_bytes,
            next_sequence,
            durable_state_version: restored.state_version,
            durable_healthy: true,
            in_flight: BTreeMap::new(),
        },
    ))
}

fn restore_cache_entry(
    config: &YahooHttpSessionConfig,
    persisted: YahooDurableCacheEntry,
) -> Result<CacheEntry, YahooHttpFailureKind> {
    validate_selected_request(&persisted.request, config.adapter_bounds)
        .map_err(|_| YahooHttpFailureKind::StateUnavailable)?;
    let body_length = persisted.response_bytes.len();
    if body_length == 0
        || body_length > config.adapter_bounds.max_response_bytes
        || body_length > config.max_cache_bytes
        || request_identity(&persisted.request) != persisted.request_identity_sha256_hex
        || evidence_digest_from_hex(&persisted.request_identity_sha256_hex).is_err()
        || sha256_hex(&persisted.response_bytes) != persisted.response_sha256_hex
        || !(200..300).contains(&persisted.response_status)
        || !persisted
            .response_content_type
            .as_deref()
            .is_some_and(content_type_is_json)
        || persisted.received_at_unix_ms > persisted.available_at_unix_ms
        || persisted.available_at_unix_ms > persisted.stored_at_unix_ms
        || DateTime::<Utc>::from_timestamp_millis(persisted.received_at_unix_ms).is_none()
        || DateTime::<Utc>::from_timestamp_millis(persisted.available_at_unix_ms).is_none()
        || persisted.attempts.is_empty()
        || persisted.attempts.len() > config.max_attempt_receipts
        || persisted.attempts.iter().any(|attempt| {
            attempt.started_at_unix_ms > attempt.completed_at_unix_ms
                || attempt.completed_at_unix_ms > persisted.received_at_unix_ms
        })
    {
        return Err(YahooHttpFailureKind::StateUnavailable);
    }
    let final_attempt = persisted
        .attempts
        .iter()
        .rev()
        .find(|attempt| attempt.target == YahooAttemptTarget::Data(persisted.request.family))
        .ok_or(YahooHttpFailureKind::StateUnavailable)?;
    if final_attempt.status != Some(persisted.response_status)
        || final_attempt.response_bytes != body_length
        || final_attempt.response_sha256_hex.as_deref()
            != Some(persisted.response_sha256_hex.as_str())
        || final_attempt.completed_at_unix_ms != persisted.received_at_unix_ms
        || !matches!(
            final_attempt.disposition,
            AttemptDisposition::Success | AttemptDisposition::Partial
        )
    {
        return Err(YahooHttpFailureKind::StateUnavailable);
    }
    let context = ParseContext {
        received_at_unix_ms: persisted.received_at_unix_ms,
        available_at_unix_ms: persisted.available_at_unix_ms,
    };
    let parsed = parse_selected_response(
        &persisted.request,
        &context,
        config.adapter_bounds,
        &persisted.response_bytes,
    )
    .map_err(|_| YahooHttpFailureKind::StateUnavailable)?;
    let raw = YahooRawReceipt {
        request: persisted.request.clone(),
        request_identity_sha256_hex: persisted.request_identity_sha256_hex,
        request_family: persisted.request.family,
        request_target_without_crumb: persisted.request.target.clone(),
        effective_arguments: persisted.request.effective_arguments.clone(),
        response_status: persisted.response_status,
        response_content_type: persisted.response_content_type,
        response_sha256_hex: persisted.response_sha256_hex,
        response_bytes: Bytes::from(persisted.response_bytes),
        received_at_unix_ms: persisted.received_at_unix_ms,
        available_at_unix_ms: persisted.available_at_unix_ms,
        attempts: persisted.attempts.into_boxed_slice(),
    };
    Ok(CacheEntry {
        request: persisted.request,
        payload: Arc::new(ExecutionPayload {
            raw: Arc::new(raw),
            parsed: Arc::new(parsed),
        }),
        stored_at_unix_ms: persisted.stored_at_unix_ms,
        bytes: body_length,
        sequence: persisted.sequence,
    })
}

fn durable_cache_snapshot(
    shared: &SharedState,
) -> Result<Vec<YahooDurableCacheEntry>, YahooHttpFailureKind> {
    let mut persisted = Vec::new();
    persisted
        .try_reserve_exact(shared.cache.len())
        .map_err(|_| YahooHttpFailureKind::StateUnavailable)?;
    for (identity, entry) in &shared.cache {
        let raw = entry.payload.raw.as_ref();
        if identity != &raw.request_identity_sha256_hex
            || entry.bytes != raw.response_bytes.len()
            || raw.request != entry.request
            || raw.request_family != entry.request.family
            || raw.request_target_without_crumb != entry.request.target
            || raw.effective_arguments != entry.request.effective_arguments
        {
            return Err(YahooHttpFailureKind::StateUnavailable);
        }
        persisted.push(YahooDurableCacheEntry {
            request_identity_sha256_hex: identity.clone(),
            request: entry.request.clone(),
            response_status: raw.response_status,
            response_content_type: raw.response_content_type.clone(),
            response_sha256_hex: raw.response_sha256_hex.clone(),
            response_bytes: raw.response_bytes.to_vec(),
            received_at_unix_ms: raw.received_at_unix_ms,
            available_at_unix_ms: raw.available_at_unix_ms,
            attempts: raw.attempts.to_vec(),
            stored_at_unix_ms: entry.stored_at_unix_ms,
            sequence: entry.sequence,
        });
    }
    Ok(persisted)
}

fn state_failure(outcome: &SharedOutcome) -> SharedOutcome {
    let attempts = match outcome {
        Ok(payload) => payload.raw.attempts.to_vec(),
        Err(failure) => failure.attempts.to_vec(),
    };
    Err(Arc::new(YahooHttpFailure::new(
        YahooHttpFailureKind::StateUnavailable,
        attempts,
    )))
}

fn content_type_is_json(value: &str) -> bool {
    value.split(';').next().is_some_and(|mime| {
        mime.trim().eq_ignore_ascii_case("application/json") || mime.trim().ends_with("+json")
    })
}

async fn deadline_wait<T>(
    deadline: Instant,
    cancellation: &CancellationToken,
    future: impl std::future::Future<Output = T>,
) -> Result<T, YahooHttpFailureKind> {
    let remaining = remaining(deadline)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(YahooHttpFailureKind::Cancelled),
        value = tokio::time::timeout(remaining, future) => {
            value.map_err(|_| YahooHttpFailureKind::DeadlineExceeded)
        }
    }
}

fn remaining(deadline: Instant) -> Result<Duration, YahooHttpFailureKind> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|value| !value.is_zero())
        .ok_or(YahooHttpFailureKind::DeadlineExceeded)
}

fn wall_time_ms() -> Result<i64, ()> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ())?;
    i64::try_from(value.as_millis()).map_err(|_| ())
}

fn duration_ms(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    sha256_finish(digest)
}

fn evidence_digest_from_hex(value: &str) -> Result<EvidenceDigest, YahooPublicationBridgeError> {
    if value.len() != 64 {
        return Err(YahooPublicationBridgeError::InvalidDigest);
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or(YahooPublicationBridgeError::InvalidDigest)?;
        let low = hex_nibble(pair[1]).ok_or(YahooPublicationBridgeError::InvalidDigest)?;
        decoded[index] = (high << 4) | low;
    }
    if decoded.iter().all(|byte| *byte == 0) {
        return Err(YahooPublicationBridgeError::InvalidDigest);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, decoded))
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

pub(crate) const fn dataset_identity(family: YahooRequestFamily) -> &'static str {
    match family {
        YahooRequestFamily::Quote => "yahoo-finance.experimental.quote",
        YahooRequestFamily::ChartHistory => "yahoo-finance.experimental.chart-history",
        YahooRequestFamily::ReferenceSummary => "yahoo-finance.experimental.reference-summary",
        YahooRequestFamily::FundSummary => "yahoo-finance.experimental.fund-summary",
        YahooRequestFamily::OptionChain => "yahoo-finance.experimental.option-chain",
        YahooRequestFamily::Search => "yahoo-finance.experimental.search",
        YahooRequestFamily::Lookup => "yahoo-finance.experimental.lookup",
    }
}

fn sha256_finish(digest: Sha256) -> String {
    let bytes = digest.finalize();
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(value, "{byte:02x}");
    }
    value
}

fn parse_retry_after(value: &str) -> Option<YahooRetryAfterDirective> {
    if let Ok(seconds) = value.trim().parse::<u64>() {
        seconds.checked_mul(1_000)?;
        return Some(YahooRetryAfterDirective::DeltaSeconds { seconds });
    }
    let retry_at = httpdate::parse_http_date(value).ok()?;
    let retry_at_unix_ms =
        i64::try_from(retry_at.duration_since(UNIX_EPOCH).ok()?.as_millis()).ok()?;
    Some(YahooRetryAfterDirective::HttpDate { retry_at_unix_ms })
}

fn bytes_contain_ascii_case_insensitive(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .get(..haystack.len().min(MAX_RATE_LIMIT_TEXT_SCAN_BYTES))
            .is_some_and(|bounded| {
                bounded
                    .windows(needle.len())
                    .any(|window| window.eq_ignore_ascii_case(needle))
            })
}

fn wire_indicates_rate_limit(wire: &WireResponse) -> bool {
    wire.status == 429
        || (wire.status == 503 && wire.retry_after.is_some())
        || [
            b"too many requests".as_slice(),
            b"rate limit".as_slice(),
            b"rate-limit".as_slice(),
            b"rate_limit".as_slice(),
            b"ratelimit".as_slice(),
        ]
        .iter()
        .any(|needle| bytes_contain_ascii_case_insensitive(&wire.bytes, needle))
}

fn parse_consent_fields(
    bytes: &[u8],
    maximum_value_bytes: usize,
) -> Result<(String, String), YahooHttpFailureKind> {
    let html = std::str::from_utf8(bytes).map_err(|_| YahooHttpFailureKind::ConsentSchema)?;
    let csrf = extract_input_value(html, "csrfToken", maximum_value_bytes)?;
    let session = extract_input_value(html, "sessionId", maximum_value_bytes)?;
    Ok((csrf, session))
}

fn extract_input_value(
    html: &str,
    requested_name: &str,
    maximum_value_bytes: usize,
) -> Result<String, YahooHttpFailureKind> {
    for tag in html
        .split('<')
        .filter_map(|value| value.split_once('>').map(|pair| pair.0))
    {
        if !tag.trim_start().starts_with("input") {
            continue;
        }
        let name = html_attribute(tag, "name");
        if name.as_deref() != Some(requested_name) {
            continue;
        }
        let value = html_attribute(tag, "value").ok_or(YahooHttpFailureKind::ConsentSchema)?;
        if value.is_empty()
            || value.len() > maximum_value_bytes
            || value.chars().any(char::is_control)
        {
            return Err(YahooHttpFailureKind::ConsentSchema);
        }
        return Ok(value);
    }
    Err(YahooHttpFailureKind::ConsentSchema)
}

fn html_attribute(tag: &str, key: &str) -> Option<String> {
    let mut rest = tag;
    while let Some(position) = rest.find(key) {
        rest = &rest[position + key.len()..];
        let trimmed = rest.trim_start();
        let after_equal = trimmed.strip_prefix('=')?.trim_start();
        let quote = after_equal.chars().next()?;
        if quote != '\'' && quote != '"' {
            continue;
        }
        let value = &after_equal[quote.len_utf8()..];
        let end = value.find(quote)?;
        return Some(value[..end].to_owned());
    }
    None
}

fn consent_form(csrf_token: &str, session_id: &str) -> Vec<(String, String)> {
    [
        ("agree", "agree"),
        ("agree", "agree"),
        ("consentUUID", "default"),
        ("sessionId", session_id),
        ("csrfToken", csrf_token),
        ("originalDoneUrl", "https://finance.yahoo.com/"),
        ("namespace", "yahoo"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .collect()
}

impl YahooHttpFailure {
    fn new(kind: YahooHttpFailureKind, attempts: Vec<YahooHttpAttemptReceipt>) -> Self {
        Self {
            kind,
            attempts: attempts.into_boxed_slice(),
        }
    }

    fn without_attempts(kind: YahooHttpFailureKind) -> Self {
        Self::new(kind, Vec::new())
    }

    fn from_admission(_: AdmissionRejection) -> Self {
        Self::without_attempts(YahooHttpFailureKind::AdmissionUnavailable)
    }
}

#[derive(Clone)]
struct SendSpec {
    method: reqwest::Method,
    url: Url,
    kind: AttemptKind,
    target: YahooAttemptTarget,
    observation_units: usize,
    maximum_bytes: usize,
    form: Option<Vec<(String, String)>>,
}

impl SendSpec {
    fn get(
        url: Url,
        kind: AttemptKind,
        target: YahooAttemptTarget,
        observation_units: usize,
        maximum_bytes: usize,
    ) -> Self {
        Self {
            method: reqwest::Method::GET,
            url,
            kind,
            target,
            observation_units,
            maximum_bytes,
            form: None,
        }
    }

    fn form(
        method: reqwest::Method,
        url: Url,
        kind: AttemptKind,
        target: YahooAttemptTarget,
        form: Vec<(String, String)>,
        maximum_bytes: usize,
    ) -> Self {
        Self {
            method,
            url,
            kind,
            target,
            observation_units: 0,
            maximum_bytes,
            form: Some(form),
        }
    }
}

struct WireResponse {
    kind: AttemptKind,
    target: YahooAttemptTarget,
    status: u16,
    response_sha256_hex: Option<String>,
    bytes: Bytes,
    content_type: Option<String>,
    retry_after: Option<YahooRetryAfterDirective>,
    final_url: Url,
    started_at_unix_ms: i64,
    completed_at_unix_ms: i64,
    latency_ms: u64,
    observation_units: usize,
}

impl WireResponse {
    fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    fn is_json(&self) -> bool {
        self.content_type.as_deref().is_some_and(|value| {
            value.split(';').next().is_some_and(|mime| {
                mime.trim().eq_ignore_ascii_case("application/json")
                    || mime.trim().ends_with("+json")
            })
        })
    }

    fn final_url_is_consent(&self) -> bool {
        matches!(
            self.final_url.host_str(),
            Some("guce.yahoo.com" | "consent.yahoo.com")
        )
    }
}

enum DataFailure {
    Terminal(YahooHttpFailureKind),
    StrategyFallback { status: u16 },
}

const fn allows_cookie_strategy_fallback(failure: &YahooHttpFailureKind) -> bool {
    matches!(
        failure,
        YahooHttpFailureKind::Network
            | YahooHttpFailureKind::UnsupportedEncoding
            | YahooHttpFailureKind::ProviderStatus { .. }
            | YahooHttpFailureKind::CrumbUnavailable
            | YahooHttpFailureKind::ConsentSchema
            | YahooHttpFailureKind::Schema
    )
}

impl EndpointSet {
    fn production() -> Result<Self, YahooHttpFailureKind> {
        let parse =
            |value| Url::parse(value).map_err(|_| YahooHttpFailureKind::InvalidConfiguration);
        Ok(Self {
            cookie: parse(BASIC_COOKIE_URL)?,
            basic_crumb: parse(BASIC_CRUMB_URL)?,
            consent_bootstrap: parse(CONSENT_BOOTSTRAP_URL)?,
            consent_submit: parse(CONSENT_SUBMIT_URL)?,
            consent_copy: parse(CONSENT_COPY_URL)?,
            csrf_crumb: parse(CSRF_CRUMB_URL)?,
            data_rewrite_base: None,
            allow_plain_http: false,
            allowed_hosts: Arc::from([
                "fc.yahoo.com".to_owned(),
                "query1.finance.yahoo.com".to_owned(),
                "query2.finance.yahoo.com".to_owned(),
                "guce.yahoo.com".to_owned(),
                "consent.yahoo.com".to_owned(),
                "finance.yahoo.com".to_owned(),
            ]),
        })
    }

    #[cfg(test)]
    fn local(base: Url) -> Self {
        let endpoint = |path: &str| base.join(path).expect("test path must join");
        let host = base.host_str().expect("test host").to_owned();
        Self {
            cookie: endpoint("cookie"),
            basic_crumb: endpoint("crumb"),
            consent_bootstrap: endpoint("consent"),
            consent_submit: endpoint("submit"),
            consent_copy: endpoint("copy"),
            csrf_crumb: endpoint("csrf-crumb"),
            data_rewrite_base: Some(base),
            allow_plain_http: true,
            allowed_hosts: Arc::from([host]),
        }
    }
}

impl NetworkState {
    fn new(
        strategy: CookieStrategy,
        config: &YahooHttpSessionConfig,
        endpoints: &EndpointSet,
    ) -> Result<Self, YahooHttpFailureKind> {
        let cookie_jar = Arc::new(Jar::default());
        let client = build_client(config, endpoints, Arc::clone(&cookie_jar))?;
        Ok(Self {
            strategy,
            client: WireClient::Production(client),
            _cookie_jar: cookie_jar,
            crumb: None,
        })
    }

    fn switch_strategy(
        &mut self,
        config: &YahooHttpSessionConfig,
        endpoints: &EndpointSet,
    ) -> Result<(), YahooHttpFailureKind> {
        let strategy = match self.strategy {
            CookieStrategy::Basic => CookieStrategy::Csrf,
            CookieStrategy::Csrf => CookieStrategy::Basic,
        };
        #[cfg(test)]
        if matches!(self.client, WireClient::Scripted(_)) {
            self.strategy = strategy;
            self.crumb = None;
            return Ok(());
        }
        *self = Self::new(strategy, config, endpoints)?;
        Ok(())
    }
}

fn build_client(
    config: &YahooHttpSessionConfig,
    endpoints: &EndpointSet,
    cookie_jar: Arc<Jar>,
) -> Result<reqwest::Client, YahooHttpFailureKind> {
    let allowed_hosts = Arc::clone(&endpoints.allowed_hosts);
    let maximum_redirects = config.max_redirects;
    let mut builder = reqwest::Client::builder()
        .cookie_provider(cookie_jar)
        .connect_timeout(config.connect_timeout)
        .read_timeout(config.read_timeout)
        .min_tls_version(reqwest::tls::Version::TLS_1_2)
        .retry(reqwest::retry::never())
        .no_proxy()
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= maximum_redirects {
                return attempt.stop();
            }
            let allowed = attempt
                .url()
                .host_str()
                .is_some_and(|host| allowed_hosts.iter().any(|allowed| allowed == host));
            if allowed {
                attempt.follow()
            } else {
                attempt.stop()
            }
        }));
    if !endpoints.allow_plain_http {
        builder = builder.https_only(true);
    }
    builder
        .build()
        .map_err(|_| YahooHttpFailureKind::InvalidConfiguration)
}

#[cfg(test)]
impl YahooHttpSession {
    pub(crate) fn new_for_test_with_durable(
        config: YahooHttpSessionConfig,
        base: Url,
        responses: Vec<ScriptedHttpResponse>,
        durable: Option<YahooDurableStateStore>,
    ) -> Result<Self, YahooHttpFailureKind> {
        let config = config.validate()?;
        if durable.is_some() && config.max_cache_bytes > MAX_YAHOO_DURABLE_CACHE_BODY_BYTES {
            return Err(YahooHttpFailureKind::InvalidConfiguration);
        }
        let endpoints = EndpointSet::local(base);
        let restored = durable
            .as_ref()
            .map(YahooDurableStateStore::load)
            .transpose()
            .map_err(|_| YahooHttpFailureKind::StateUnavailable)?
            .flatten();
        let (admission, shared) = restore_shared_state(&config, restored)?;
        let cookie_jar = Arc::new(Jar::default());
        Ok(Self {
            inner: Arc::new(SessionInner {
                config,
                endpoints,
                admission,
                durable,
                network: AsyncMutex::new(NetworkState {
                    strategy: CookieStrategy::Basic,
                    client: WireClient::Scripted(AsyncMutex::new(ScriptedWireState {
                        responses: responses.into(),
                        observed_targets: Vec::new(),
                    })),
                    _cookie_jar: cookie_jar,
                    crumb: None,
                }),
                shared: Mutex::new(shared),
            }),
        })
    }

    pub(crate) async fn scripted_observed_targets(&self) -> Vec<String> {
        let network = self.inner.network.lock().await;
        let WireClient::Scripted(state) = &network.client else {
            return Vec::new();
        };
        state.lock().await.observed_targets.clone()
    }
}
