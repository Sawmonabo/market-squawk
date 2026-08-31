use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use chrono::{DateTime, Datelike as _, Utc};
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailabilityEvidence, BarTimeSemantics, Currency, DataQuality,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentId,
    MarketBarAdjustment, MarketBarObservation, MarketDataInstrumentDefinition, MetadataRevision,
    Money, PayloadHash, PayloadReference, ProviderInstrumentId, ResearchContext,
    ResearchObservation, ResearchProvenance, ResearchProvenanceInput, ResearchTime, RevisionNumber,
    SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_platform::RawCaptureRecord;
use market_squawk_sources::{
    AvailabilityEvidence, BudgetDecision, BudgetDispatchDecision, BudgetPermit, BudgetReservation,
    BudgetReservationDecision, CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryBatch, DiscoveryRequest,
    ExtractionAuthority, ExtractionBatch, ExtractionRecord, ExtractionRequest,
    ExtractionRevisionPlan, ExtractionSource, ExtractionSourceError, HttpRequestBounds,
    ProviderCaptureMaterial, ProviderCapturePageReceipt, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, SharedProviderBudget, SourceError, SourceMetadata,
    SourceMetadataProvider, SourceObject, SourceObjectCaptureIdentity, apply_http_retry_after,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Number;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::{
    ALPACA_HISTORICAL_EXCLUSION_NANOS, ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS,
    ALPACA_STOCKS_BASE_ENDPOINT, AlpacaHistoricalEquityPreflightPlan,
    AlpacaProviderInstrumentCoordinate,
};
use crate::historical_calendar::singleton_bounded_header;
use crate::historical_transport::{AlpacaHistoricalEndpoint, AlpacaHistoricalTransport};
use crate::{
    AlpacaCredentials, AlpacaError, AlpacaHistoricalEquityConfig, AlpacaHistoricalEquityDataset,
};

const MAX_PAGE_TOKEN_BYTES: usize = 256;
const MAXIMUM_PREFLIGHT_PAGES: usize = 16;
const MAXIMUM_PREFLIGHT_RETAINED_BYTES: usize = 32 * 1024 * 1024;
const MAXIMUM_PREFLIGHT_RETURNED_TIMESTAMPS: usize =
    ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS as usize + 2;
const PREFLIGHT_USER_AGENT: &str = "market-squawk/0.1 alpaca-historical-preflight";
const COMPLETE_DAILY_HISTORY_MEDIA_TYPE: &str =
    "application/vnd.market-squawk.alpaca-iex-complete-daily-history+json";

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

/// One exact provider-authored bar timestamp and its UTC civil request date.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AlpacaHistoricalReturnedBarTime {
    provider_timestamp: Timestamp,
    calendar_date: market_squawk_domain::CalendarDate,
}

impl AlpacaHistoricalReturnedBarTime {
    /// Returns the provider timestamp retained without rewriting.
    pub const fn provider_timestamp(self) -> Timestamp {
        self.provider_timestamp
    }

    /// Returns the exact UTC civil date encoded by the provider's UTC timestamp.
    pub const fn calendar_date(self) -> market_squawk_domain::CalendarDate {
        self.calendar_date
    }
}

/// Exact observed pagination terminal state. It proves only that Alpaca supplied no next token;
/// it does not claim that every possible market session or bar was returned.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AlpacaHistoricalPaginationDisposition {
    /// The retained final raw response supplied no next-page token.
    ProviderTerminalWithoutNextToken,
}

#[derive(Debug, Eq, PartialEq)]
struct AlpacaHistoricalPreflightPage {
    request_url: Box<str>,
    request_page_token: Option<Box<str>>,
    response_page_token: Option<Box<str>>,
    body: Bytes,
    evidence: ExactPayloadEvidence,
    received_at: Timestamp,
}

/// Secret-free content-addressed receipt for one exact, non-truncated provider pagination graph.
#[derive(Debug, Eq, PartialEq)]
pub struct AlpacaHistoricalEquityPreflightReceipt {
    plan: AlpacaHistoricalEquityPreflightPlan,
    pages: Box<[AlpacaHistoricalPreflightPage]>,
    returned_bar_times: Box<[AlpacaHistoricalReturnedBarTime]>,
    pagination: AlpacaHistoricalPaginationDisposition,
    total_response_bytes: usize,
    last_rate_limit: AlpacaRateLimitEvidence,
    digest: EvidenceDigest,
}

impl AlpacaHistoricalEquityPreflightReceipt {
    /// Returns the exact unregistered request plan that produced this receipt.
    pub const fn plan(&self) -> &AlpacaHistoricalEquityPreflightPlan {
        &self.plan
    }

    /// Returns every distinct provider timestamp and exact UTC date in sorted provider order.
    pub const fn returned_bar_times(&self) -> &[AlpacaHistoricalReturnedBarTime] {
        &self.returned_bar_times
    }

    /// Returns the observed provider pagination terminal disposition.
    pub const fn pagination(&self) -> AlpacaHistoricalPaginationDisposition {
        self.pagination
    }

    /// Returns the total exact raw response bytes retained across the pagination graph.
    pub const fn total_response_bytes(&self) -> usize {
        self.total_response_bytes
    }

    /// Returns the complete request/page/payload/timestamp graph identity.
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }

    /// Returns the most recent structurally valid provider rate evidence in the retained graph.
    pub const fn last_rate_limit_evidence(&self) -> AlpacaRateLimitEvidence {
        self.last_rate_limit
    }

    /// Rebuilds exact bounded source-neutral material for durable publication of this complete
    /// IEX historical response graph.
    ///
    /// The binding configuration is checked against the preflight plan before any material is
    /// returned. Exact response bodies are copied into validated raw records under the preflight
    /// and shared capture byte ceilings; API keys, request headers, rate permits, clients, and
    /// account/trading routes are structurally absent.
    pub fn provider_capture_material(
        &self,
        config: &AlpacaHistoricalEquityConfig,
    ) -> Result<ProviderCaptureMaterial, AlpacaError> {
        let dataset = exactly_one_dataset(config)?;
        let maximum_page_bytes = usize::try_from(config.request_bounds().max_response_bytes())
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        if !dataset.matches_preflight(self.plan())
            || self
                .pages
                .iter()
                .any(|page| page.body.len() > maximum_page_bytes)
        {
            return Err(AlpacaError::CaptureMaterial);
        }
        build_historical_capture_material(
            config.metadata().source_id(),
            config.metadata().revision(),
            dataset.dataset(),
            self,
        )
    }
}

/// One-operation credential-bearing client that produces a secret-free preflight receipt.
pub struct AlpacaHistoricalEquityPreflightClient {
    credentials: Arc<AlpacaCredentials>,
    transport: AlpacaHistoricalTransport,
    bounds: HttpRequestBounds,
}

/// Registry-authorized, extraction-only Alpaca IEX historical-bars source.
pub struct AlpacaHistoricalEquitySource {
    config: AlpacaHistoricalEquityConfig,
    instrument_authorities: HashMap<String, HistoricalInstrumentAuthority>,
    bar_time_authority: Arc<dyn AlpacaHistoricalBarTimeAuthority>,
    preflight: Arc<AlpacaHistoricalEquityPreflightReceipt>,
}

/// Exact bounded input to the provider-specific historical bar-time authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaHistoricalBarTimeRequest {
    instrument_id: InstrumentId,
    venue_id: VenueId,
    provider_instrument_id: ProviderInstrumentId,
    timeframe: SourceIdentifier,
    provider_timestamp: Timestamp,
}

impl AlpacaHistoricalBarTimeRequest {
    pub(crate) const fn new(
        instrument_id: InstrumentId,
        venue_id: VenueId,
        provider_instrument_id: ProviderInstrumentId,
        timeframe: SourceIdentifier,
        provider_timestamp: Timestamp,
    ) -> Self {
        Self {
            instrument_id,
            venue_id,
            provider_instrument_id,
            timeframe,
            provider_timestamp,
        }
    }

    /// Returns the stable internal instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact venue whose session rules close the bar.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the exact provider instrument identity.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the exact Alpaca timeframe identity.
    pub const fn timeframe(&self) -> &SourceIdentifier {
        &self.timeframe
    }

    /// Returns the provider-authored timestamp retained without rewriting.
    pub const fn provider_timestamp(&self) -> Timestamp {
        self.provider_timestamp
    }
}

/// Revocable least-authority resolver for exact Alpaca bar-period and session evidence.
///
/// Implementations may consult an independently governed provider calendar, but receive no
/// credentials, transport, source registry, or extraction budget authority through this contract.
pub trait AlpacaHistoricalBarTimeAuthority: Send + Sync + 'static {
    /// Rejects use after the governing calendar/session authority has been revoked.
    fn validate_current(&self) -> Result<(), AlpacaError>;

    /// Resolves one exact provider timestamp without inferring a calendar period in this adapter.
    fn resolve(
        &self,
        request: &AlpacaHistoricalBarTimeRequest,
    ) -> Result<BarTimeSemantics, AlpacaError>;
}

#[derive(Clone, Debug)]
struct HistoricalInstrumentAuthority {
    coordinate: AlpacaProviderInstrumentCoordinate,
    currency: Currency,
}

#[derive(Clone, Copy)]
struct CanonicalInstrumentAuthorityIndexEntry<'a> {
    instrument_id: market_squawk_domain::InstrumentId,
    source_id: &'a SourceId,
    provider_instrument_id: &'a ProviderInstrumentId,
    definition_index: usize,
    definition: &'a MarketDataInstrumentDefinition,
}

impl AlpacaHistoricalEquityPreflightClient {
    /// Constructs the credential-bearing client used only inside one admitted account operation.
    pub fn try_new(
        credentials: Arc<AlpacaCredentials>,
        bounds: HttpRequestBounds,
    ) -> Result<Self, AlpacaError> {
        Ok(Self {
            credentials,
            transport: AlpacaHistoricalTransport::try_hardened(bounds, PREFLIGHT_USER_AGENT)?,
            bounds,
        })
    }

    #[cfg(any(
        test,
        all(feature = "scripted-historical-transport-fixture", debug_assertions)
    ))]
    pub(crate) fn try_new_with_transport(
        credentials: Arc<AlpacaCredentials>,
        bounds: HttpRequestBounds,
        transport: AlpacaHistoricalTransport,
    ) -> Result<Self, AlpacaError> {
        Ok(Self {
            credentials,
            transport,
            bounds,
        })
    }

    /// Fetches and retains one exact terminal pagination graph under fixed code-owned caps.
    ///
    /// The receipt records provider terminal pagination, not market-session completeness. Any
    /// response chain that would cross a page, byte, timestamp, deadline, or allocation bound is
    /// rejected rather than truncated.
    pub async fn fetch(
        &self,
        plan: AlpacaHistoricalEquityPreflightPlan,
        budget: &SharedProviderBudget,
        deadline: std::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<Arc<AlpacaHistoricalEquityPreflightReceipt>, AlpacaError> {
        enforce_preflight_window(&plan)?;
        let mut pages = Vec::new();
        pages
            .try_reserve_exact(MAXIMUM_PREFLIGHT_PAGES)
            .map_err(|_| AlpacaError::Allocation)?;
        let mut returned_bar_times: Vec<AlpacaHistoricalReturnedBarTime> = Vec::new();
        returned_bar_times
            .try_reserve_exact(MAXIMUM_PREFLIGHT_RETURNED_TIMESTAMPS)
            .map_err(|_| AlpacaError::Allocation)?;
        let mut seen_page_tokens = BTreeSet::new();
        let mut request_page_token: Option<String> = None;
        let mut previous_bar_time = None;
        let mut total_response_bytes = 0_usize;
        let last_rate_limit = loop {
            if pages.len() == MAXIMUM_PREFLIGHT_PAGES {
                return Err(AlpacaError::BodyTooLarge);
            }
            let url = preflight_request_url(&plan, request_page_token.as_deref())?;
            let fetched = self
                .fetch_page(&url, budget, deadline, cancellation)
                .await?;
            total_response_bytes = total_response_bytes
                .checked_add(fetched.body.len())
                .filter(|bytes| *bytes <= MAXIMUM_PREFLIGHT_RETAINED_BYTES)
                .ok_or(AlpacaError::BodyTooLarge)?;
            let parsed = serde_json::from_slice::<BarPage>(&fetched.body)
                .map_err(|_| AlpacaError::Protocol)?;
            validate_preflight_page(&plan, &parsed)?;
            if parsed.bars.is_empty() && parsed.next_page_token.is_some() {
                return Err(AlpacaError::Protocol);
            }
            for bar in &parsed.bars {
                let returned = parse_returned_bar_time(&bar.timestamp)?;
                if previous_bar_time.is_some_and(|previous| returned <= previous)
                    || returned_bar_times
                        .last()
                        .is_some_and(|previous| previous.calendar_date == returned.calendar_date)
                    || returned_bar_times.len() == MAXIMUM_PREFLIGHT_RETURNED_TIMESTAMPS
                {
                    return Err(AlpacaError::Protocol);
                }
                previous_bar_time = Some(returned);
                returned_bar_times.push(returned);
            }
            let response_page_token = parsed.next_page_token;
            if let Some(token) = &response_page_token {
                validate_page_token(token)?;
                if !seen_page_tokens.insert(token.clone()) {
                    return Err(AlpacaError::Protocol);
                }
            }
            let body = Bytes::from(fetched.body);
            let evidence = exact_evidence(&body);
            let rate_limit = fetched.rate_limit;
            pages.push(AlpacaHistoricalPreflightPage {
                request_url: url.as_str().to_owned().into_boxed_str(),
                request_page_token: request_page_token
                    .as_deref()
                    .map(str::to_owned)
                    .map(String::into_boxed_str),
                response_page_token: response_page_token
                    .as_deref()
                    .map(str::to_owned)
                    .map(String::into_boxed_str),
                body,
                evidence,
                received_at: fetched.received_at,
            });
            match response_page_token {
                Some(next) => request_page_token = Some(next),
                None => break rate_limit,
            }
        };

        let pagination = AlpacaHistoricalPaginationDisposition::ProviderTerminalWithoutNextToken;
        let digest = preflight_receipt_digest(
            &plan,
            &pages,
            &returned_bar_times,
            pagination,
            total_response_bytes,
        )?;
        Ok(Arc::new(AlpacaHistoricalEquityPreflightReceipt {
            plan,
            pages: pages.into_boxed_slice(),
            returned_bar_times: returned_bar_times.into_boxed_slice(),
            pagination,
            total_response_bytes,
            last_rate_limit,
            digest,
        }))
    }

    async fn fetch_page(
        &self,
        url: &url::Url,
        budget: &SharedProviderBudget,
        deadline: std::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreflightFetchedPage, AlpacaError> {
        loop {
            let reservation = acquire_preflight_budget(budget, deadline, cancellation).await?;
            let maximum = usize::try_from(self.bounds.max_response_bytes())
                .map_err(|_| AlpacaError::InvalidTransportLimits)?;
            let permit =
                commit_preflight_budget(reservation, budget, deadline, cancellation).await?;
            let response = self
                .transport
                .authenticated_get(
                    AlpacaHistoricalEndpoint::Bars,
                    &self.credentials,
                    url,
                    self.bounds,
                    maximum,
                    deadline,
                    cancellation,
                )
                .await?;
            let received_at = response.received_at;
            let retry_after =
                singleton_bounded_header(&response.headers, reqwest::header::RETRY_AFTER, 128)?;
            if matches!(response.status, 429 | 503) {
                let refusal = apply_http_retry_after(budget, retry_after.as_deref(), 1_000);
                permit.release();
                wait_for_budget_decision(budget, refusal, deadline, cancellation).await?;
                continue;
            }
            if matches!(response.status, 401 | 403) {
                return Err(AlpacaError::InvalidAuthorization);
            }
            if response.status != 200 {
                return Err(AlpacaError::Network);
            }
            let rate_limit = parse_rate_evidence(&response.headers)?;
            budget.record_success().map_err(|_| AlpacaError::Network)?;
            permit.release();
            return Ok(PreflightFetchedPage {
                body: response.body,
                rate_limit,
                received_at,
            });
        }
    }
}

impl std::fmt::Debug for AlpacaHistoricalEquityPreflightClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaHistoricalEquityPreflightClient")
            .field("credentials", &"[REDACTED ZEROIZING ARC]")
            .field("bounds", &self.bounds)
            .finish_non_exhaustive()
    }
}

struct PreflightFetchedPage {
    body: Box<[u8]>,
    rate_limit: AlpacaRateLimitEvidence,
    received_at: Timestamp,
}

impl std::fmt::Debug for AlpacaHistoricalEquitySource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaHistoricalEquitySource")
            .field("source_id", self.config.metadata().source_id())
            .field("revision", self.config.metadata().revision())
            .field("preflight_digest", &self.preflight.digest)
            .finish_non_exhaustive()
    }
}

impl AlpacaHistoricalEquitySource {
    /// Validates the exact FIGI-backed canonical provider mapping before any preflight network
    /// request is admitted.
    pub fn validate_one_preflight_instrument(
        _metadata: &SourceMetadata,
        plan: &AlpacaHistoricalEquityPreflightPlan,
        canonical_instrument: &MarketDataInstrumentDefinition,
    ) -> Result<(), AlpacaError> {
        validate_definition_coordinate(
            canonical_instrument,
            plan.mapping().provider_coordinate(),
            plan.mapping().asset_class(),
            plan.start(),
            plan.end(),
        )
        .map(|_currency| ())
    }

    /// Constructs a read-only source over the exact retained preflight graph after validating
    /// final series semantics and canonical provider identity.
    pub fn try_from_preflight(
        config: AlpacaHistoricalEquityConfig,
        canonical_instruments: Vec<MarketDataInstrumentDefinition>,
        bar_time_authority: Arc<dyn AlpacaHistoricalBarTimeAuthority>,
        preflight: Arc<AlpacaHistoricalEquityPreflightReceipt>,
    ) -> Result<Self, AlpacaError> {
        bar_time_authority.validate_current()?;
        let instrument_authorities =
            validate_instrument_authorities(&config, &canonical_instruments)?;
        let dataset = exactly_one_dataset(&config)?;
        if !dataset.matches_preflight(preflight.plan())
            || preflight.pagination
                != AlpacaHistoricalPaginationDisposition::ProviderTerminalWithoutNextToken
            || preflight.returned_bar_times.is_empty()
            || preflight_receipt_digest(
                preflight.plan(),
                &preflight.pages,
                &preflight.returned_bar_times,
                preflight.pagination,
                preflight.total_response_bytes,
            )? != preflight.digest
        {
            return Err(AlpacaError::Protocol);
        }
        Ok(Self {
            config,
            instrument_authorities,
            bar_time_authority,
            preflight,
        })
    }

    /// Returns the latest structurally valid provider rate-limit header evidence.
    ///
    /// Local admission always remains governed by the registry's shared 200-per-minute budget;
    /// provider headers can make that budget more conservative but never expand it.
    pub fn rate_limit_evidence(&self) -> Result<Option<AlpacaRateLimitEvidence>, AlpacaError> {
        Ok(Some(self.preflight.last_rate_limit))
    }

    /// Returns the exact complete IEX historical response graph ready for application-owned
    /// durable sealing before any canonical rows are published.
    pub fn provider_capture_material(&self) -> Result<ProviderCaptureMaterial, AlpacaError> {
        self.preflight.provider_capture_material(&self.config)
    }

    /// Derives the stable analytical series for one exact secret-free plan admission.
    ///
    /// This performs the same canonical FIGI/provider-identity checks as source construction but
    /// does not construct a client or receive credentials. It is therefore safe for a bounded
    /// long-lived plan directory to retain only the checked configuration and definition.
    pub fn one_plan_analytical_dataset_identifier(
        config: &AlpacaHistoricalEquityConfig,
        canonical_instrument: &MarketDataInstrumentDefinition,
    ) -> Result<SourceIdentifier, AlpacaError> {
        let dataset = exactly_one_dataset(config)?;
        let authorities =
            validate_instrument_authorities(config, std::slice::from_ref(canonical_instrument))?;
        let authority = authorities
            .get(dataset.dataset().as_str())
            .ok_or(AlpacaError::InvalidCoverage)?;
        dataset.analytical_dataset_identifier(config.metadata(), authority.currency)
    }

    /// Validates one exact extraction batch against its secret-free one-plan configuration.
    pub fn one_plan_analytical_dataset_identifier_for_batch(
        config: &AlpacaHistoricalEquityConfig,
        canonical_instrument: &MarketDataInstrumentDefinition,
        batch: &ExtractionBatch,
    ) -> Result<SourceIdentifier, AlpacaError> {
        let object = batch.request().object();
        if object.source_id() != config.metadata().source_id()
            || object.metadata_revision() != config.metadata().revision()
        {
            return Err(AlpacaError::Protocol);
        }
        let dataset = exactly_one_dataset(config)?;
        if object.dataset() != dataset.dataset() {
            return Err(AlpacaError::Protocol);
        }
        let authorities =
            validate_instrument_authorities(config, std::slice::from_ref(canonical_instrument))?;
        let authority = authorities
            .get(dataset.dataset().as_str())
            .ok_or(AlpacaError::InvalidCoverage)?;
        validate_analytical_batch(batch, dataset, config.metadata().source_id(), authority)?;
        dataset.analytical_dataset_identifier(config.metadata(), authority.currency)
    }

    /// Builds the source-honest revision plan for an exact one-plan batch without transport state.
    pub fn one_plan_revision_plan(
        config: &AlpacaHistoricalEquityConfig,
        batch: &ExtractionBatch,
    ) -> Result<ExtractionRevisionPlan, AlpacaError> {
        if batch.request().object().source_id() != config.metadata().source_id()
            || batch.request().object().metadata_revision() != config.metadata().revision()
            || batch.request().object().dataset() != exactly_one_dataset(config)?.dataset()
        {
            return Err(AlpacaError::InvalidCoverage);
        }
        ExtractionRevisionPlan::locally_observed(batch.records().len())
            .map_err(|_| AlpacaError::Protocol)
    }

    /// Derives the stable storage-safe analytical series for one exact checked extraction batch.
    ///
    /// The provider dataset is independently recomputed from source generation and request-plan
    /// fields. Every typed bar must then prove the same canonical instrument, provider identity,
    /// IEX venue/feed, timeframe, adjustment, timestamp basis, and session-ruleset evidence.
    pub fn analytical_dataset_identifier(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<SourceIdentifier, AlpacaError> {
        let object = batch.request().object();
        if object.source_id() != self.config.metadata().source_id()
            || object.metadata_revision() != self.config.metadata().revision()
        {
            return Err(AlpacaError::Protocol);
        }
        let dataset = self
            .config
            .dataset(object.dataset())
            .ok_or(AlpacaError::InvalidHistoricalPlan)?;
        dataset.verify_provider_identity(self.config.metadata())?;
        let authority = self
            .instrument_authorities
            .get(dataset.dataset().as_str())
            .ok_or(AlpacaError::InvalidCoverage)?;
        validate_analytical_batch(
            batch,
            dataset,
            self.config.metadata().source_id(),
            authority,
        )?;
        dataset.analytical_dataset_identifier(self.config.metadata(), authority.currency)
    }

    /// Builds honest locally observed revision authority for one exact historical-bar batch.
    ///
    /// Alpaca historical bars do not carry a provider revision chronology. Durable revision
    /// assignment therefore uses exact canonical semantic content rather than fabricating a
    /// provider order from bar time or arrival order.
    pub fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<ExtractionRevisionPlan, AlpacaError> {
        if batch.request().object().source_id() != self.config.metadata().source_id()
            || batch.request().object().metadata_revision() != self.config.metadata().revision()
        {
            return Err(AlpacaError::InvalidCoverage);
        }
        ExtractionRevisionPlan::locally_observed(batch.records().len())
            .map_err(|_| AlpacaError::Protocol)
    }

    async fn discover_impl(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryBatch, ExtractionSourceError> {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        self.validate_authority(&authority)?;
        if request.effective_at().is_some() {
            return Err(SourceError::InvalidProtocolState.into());
        }
        let dataset = self
            .config
            .dataset(request.dataset())
            .ok_or(SourceError::InvalidProtocolState)?;
        dataset
            .verify_provider_identity(self.config.metadata())
            .map_err(map_adapter_error)?;
        enforce_historical_window(dataset).map_err(map_adapter_error)?;
        let capture = self
            .provider_capture_material()
            .map_err(map_adapter_error)?;
        let capture_identity = SourceObjectCaptureIdentity::try_from_capture(capture.receipt())
            .map_err(|_| SourceError::GenerationResynchronizationRequired)?;
        let mut returned_bar_count = 0_usize;
        for (page_index, page) in self.preflight.pages.iter().enumerate() {
            if cancellation.is_cancelled() {
                return Err(ExtractionSourceError::Cancelled);
            }
            let parsed = serde_json::from_slice::<BarPage>(&page.body)
                .map_err(|_| SourceError::GenerationResynchronizationRequired)?;
            validate_page(dataset, &parsed).map_err(map_adapter_error)?;
            if parsed.next_page_token.as_deref() != page.response_page_token.as_deref()
                || exact_evidence(&page.body) != page.evidence
            {
                return Err(SourceError::GenerationResynchronizationRequired.into());
            }
            if parsed.bars.is_empty() {
                if parsed.next_page_token.is_some() || page_index + 1 != self.preflight.pages.len()
                {
                    return Err(SourceError::GenerationResynchronizationRequired.into());
                }
                continue;
            }
            returned_bar_count = returned_bar_count
                .checked_add(parsed.bars.len())
                .ok_or(SourceError::InvalidProtocolState)?;
        }
        if returned_bar_count != self.preflight.returned_bar_times.len() || returned_bar_count == 0
        {
            return Err(SourceError::GenerationResynchronizationRequired.into());
        }
        let observed_at = self
            .preflight
            .pages
            .last()
            .map(|page| page.received_at)
            .ok_or(SourceError::GenerationResynchronizationRequired)?;
        let effective = EffectiveInterval::new(observed_at, None)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let evidence =
            ExactPayloadEvidence::from_content_digest(capture.receipt().content_digest());
        let object = SourceObject::try_new_with_capture_identity(
            self.config.metadata().source_id().clone(),
            self.config.metadata().revision().clone(),
            &request,
            complete_range_object_id(capture.receipt().content_digest())
                .map_err(map_adapter_error)?,
            SourceIdentifier::try_from(COMPLETE_DAILY_HISTORY_MEDIA_TYPE)
                .map_err(|_| SourceError::InvalidProtocolState)?,
            evidence,
            capture_identity,
            effective,
            None,
            AvailabilityEvidence::LocalFirstObserved { observed_at },
            Some(
                u64::try_from(self.preflight.total_response_bytes)
                    .map_err(|_| SourceError::InvalidProtocolState)?,
            ),
        )?;
        ensure_wall_deadline(request.deadline()).map_err(map_adapter_error)?;
        authority.validate_current()?;
        DiscoveryBatch::try_new(&request, vec![object]).map_err(Into::into)
    }

    async fn extract_impl(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<ExtractionBatch, ExtractionSourceError> {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
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
        dataset
            .verify_provider_identity(self.config.metadata())
            .map_err(map_adapter_error)?;
        enforce_historical_window(dataset).map_err(map_adapter_error)?;
        let capture = self
            .provider_capture_material()
            .map_err(map_adapter_error)?;
        let capture_identity = SourceObjectCaptureIdentity::try_from_capture(capture.receipt())
            .map_err(|_| SourceError::GenerationResynchronizationRequired)?;
        let expected_evidence =
            ExactPayloadEvidence::from_content_digest(capture.receipt().content_digest());
        if request.object().object_id()
            != &complete_range_object_id(capture.receipt().content_digest())
                .map_err(map_adapter_error)?
            || request.object().media_type().as_str() != COMPLETE_DAILY_HISTORY_MEDIA_TYPE
            || request.object().evidence() != &expected_evidence
            || request.object().capture_identity() != capture_identity
            || request.object().expected_bytes()
                != Some(
                    u64::try_from(self.preflight.total_response_bytes)
                        .map_err(|_| SourceError::InvalidProtocolState)?,
                )
        {
            return Err(SourceError::GenerationResynchronizationRequired.into());
        }
        if self.preflight.returned_bar_times.len()
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
            .try_reserve_exact(self.preflight.returned_bar_times.len())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let instrument_authority = self
            .instrument_authorities
            .get(dataset.dataset().as_str())
            .ok_or(SourceError::InvalidProtocolState)?;
        let ingested_at = system_timestamp().map_err(map_adapter_error)?;
        let mut previous_response_token: Option<&str> = None;
        let mut returned_index = 0_usize;
        for (page_index, page) in self.preflight.pages.iter().enumerate() {
            let parsed = serde_json::from_slice::<BarPage>(&page.body)
                .map_err(|_| SourceError::GenerationResynchronizationRequired)?;
            validate_page(dataset, &parsed).map_err(map_adapter_error)?;
            if page.request_page_token.as_deref() != previous_response_token
                || parsed.next_page_token.as_deref() != page.response_page_token.as_deref()
                || exact_evidence(&page.body) != page.evidence
                || (page_index + 1 < self.preflight.pages.len()
                    && page.response_page_token.is_none())
                || (page_index + 1 == self.preflight.pages.len()
                    && page.response_page_token.is_some())
                || (parsed.bars.is_empty()
                    && (parsed.next_page_token.is_some()
                        || page_index + 1 != self.preflight.pages.len()))
            {
                return Err(SourceError::GenerationResynchronizationRequired.into());
            }
            for bar in parsed.bars {
                let returned =
                    parse_returned_bar_time(&bar.timestamp).map_err(map_adapter_error)?;
                if self.preflight.returned_bar_times.get(returned_index) != Some(&returned) {
                    return Err(SourceError::GenerationResynchronizationRequired.into());
                }
                returned_index = returned_index
                    .checked_add(1)
                    .ok_or(SourceError::InvalidProtocolState)?;
                let normalized = normalize_bar(
                    dataset,
                    instrument_authority,
                    self.config.metadata().source_id(),
                    &page.evidence,
                    page.received_at,
                    ingested_at,
                    self.bar_time_authority.as_ref(),
                    bar,
                )
                .map_err(map_adapter_error)?;
                let payload = serde_json::to_vec(&normalized)
                    .map(Bytes::from)
                    .map_err(|_| SourceError::InvalidProtocolState)?;
                let evidence = exact_evidence(&payload);
                let effective_at =
                    observation_effective_at(&normalized).map_err(map_adapter_error)?;
                let revision =
                    record_revision(effective_at, &evidence).map_err(map_adapter_error)?;
                records.push(ExtractionRecord::try_new(
                    &request,
                    schema.clone(),
                    evidence,
                    effective_at,
                    None,
                    AvailabilityEvidence::LocalFirstObserved {
                        observed_at: page.received_at,
                    },
                    revision,
                    None,
                    payload,
                )?);
            }
            previous_response_token = page.response_page_token.as_deref();
        }
        if returned_index != self.preflight.returned_bar_times.len()
            || records.len() != self.preflight.returned_bar_times.len()
        {
            return Err(SourceError::GenerationResynchronizationRequired.into());
        }
        self.bar_time_authority
            .validate_current()
            .map_err(map_adapter_error)?;
        ensure_wall_deadline(request.deadline()).map_err(map_adapter_error)?;
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
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
}

fn exactly_one_dataset(
    config: &AlpacaHistoricalEquityConfig,
) -> Result<&AlpacaHistoricalEquityDataset, AlpacaError> {
    let mut datasets = config.datasets();
    let dataset = datasets.next().ok_or(AlpacaError::InvalidHistoricalPlan)?;
    if datasets.next().is_some() {
        return Err(AlpacaError::InvalidHistoricalPlan);
    }
    dataset.verify_provider_identity(config.metadata())?;
    Ok(dataset)
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

fn validate_instrument_authorities(
    config: &AlpacaHistoricalEquityConfig,
    canonical_instruments: &[MarketDataInstrumentDefinition],
) -> Result<HashMap<String, HistoricalInstrumentAuthority>, AlpacaError> {
    let dataset_count = config.datasets().len();
    if canonical_instruments.is_empty() || canonical_instruments.len() > dataset_count {
        return Err(AlpacaError::InvalidCoverage);
    }

    let index_capacity = canonical_instruments
        .iter()
        .try_fold(0_usize, |count, definition| {
            count.checked_add(definition.provider_identities().len())
        });
    let mut canonical_index = Vec::new();
    canonical_index
        .try_reserve_exact(index_capacity.ok_or(AlpacaError::Allocation)?)
        .map_err(|_| AlpacaError::Allocation)?;
    for (definition_index, definition) in canonical_instruments.iter().enumerate() {
        for provider_identity in definition.provider_identities() {
            canonical_index.push(CanonicalInstrumentAuthorityIndexEntry {
                instrument_id: definition.instrument_id(),
                source_id: provider_identity.source_id(),
                provider_instrument_id: provider_identity.provider_instrument_id(),
                definition_index,
                definition,
            });
        }
    }
    canonical_index.sort_unstable_by(compare_canonical_index_entries);
    canonical_index
        .dedup_by(|left, right| compare_canonical_index_entries(left, right) == Ordering::Equal);

    let mut used_definitions = Vec::new();
    used_definitions
        .try_reserve_exact(canonical_instruments.len())
        .map_err(|_| AlpacaError::Allocation)?;
    used_definitions.resize(canonical_instruments.len(), false);

    let mut authorities = HashMap::new();
    authorities
        .try_reserve(dataset_count)
        .map_err(|_| AlpacaError::Allocation)?;
    for dataset in config.datasets() {
        let coordinate = dataset.mapping().provider_coordinate();
        let provider_instrument_id = coordinate.identity_key().provider_instrument_id();
        let provider_identity_source = coordinate.identity_key().source_id();
        let instrument_id = dataset.mapping().instrument();
        let first = canonical_index.partition_point(|entry| {
            compare_canonical_index_key(
                entry,
                instrument_id,
                provider_identity_source,
                provider_instrument_id,
            ) == Ordering::Less
        });
        let last = first
            + canonical_index[first..].partition_point(|entry| {
                compare_canonical_index_key(
                    entry,
                    instrument_id,
                    provider_identity_source,
                    provider_instrument_id,
                ) == Ordering::Equal
            });
        let mut resolved = None;
        for entry in &canonical_index[first..last] {
            let definition = entry.definition;
            let Ok(currency) = validate_definition_coordinate(
                definition,
                coordinate,
                dataset.mapping().asset_class(),
                dataset.start(),
                dataset.end(),
            ) else {
                continue;
            };
            if resolved.is_some() {
                return Err(AlpacaError::InvalidCoverage);
            }
            resolved = Some((entry.definition_index, currency));
        }
        let (definition_index, currency) = resolved.ok_or(AlpacaError::InvalidCoverage)?;
        used_definitions[definition_index] = true;
        let authority = HistoricalInstrumentAuthority {
            coordinate: coordinate.clone(),
            currency,
        };
        if authorities
            .insert(try_owned_bounded(dataset.dataset().as_str())?, authority)
            .is_some()
        {
            return Err(AlpacaError::InvalidCoverage);
        }
    }
    if used_definitions.iter().any(|used| !used) {
        return Err(AlpacaError::InvalidCoverage);
    }
    Ok(authorities)
}

fn validate_definition_coordinate(
    definition: &MarketDataInstrumentDefinition,
    coordinate: &AlpacaProviderInstrumentCoordinate,
    asset_class: market_squawk_domain::AssetClass,
    start: Timestamp,
    end: Timestamp,
) -> Result<Currency, AlpacaError> {
    let identity_key = coordinate.identity_key();
    let selected = definition.provider_identity_at(
        identity_key.source_id(),
        identity_key.provider_instrument_id(),
        start,
    );
    let selected_at_end = definition.provider_identity_at(
        identity_key.source_id(),
        identity_key.provider_instrument_id(),
        end,
    );
    if definition.instrument_id() != coordinate.instrument()
        || definition.asset_class() != asset_class
        || !interval_covers(definition.effective_interval(), start, end)
        || !definition.venue_mappings().iter().any(|mapping| {
            mapping.venue_id() == coordinate.venue()
                && mapping.venue_symbol() == coordinate.venue_symbol()
        })
        || selected.is_none()
        || selected != selected_at_end
        || selected.is_none_or(|identity| {
            identity.instrument_id() != coordinate.instrument()
                || identity.metadata_revision() != coordinate.provider_identity_revision()
                || identity.evidence().content_digest() != coordinate.provider_identity_digest()
                || identity.validity() != coordinate.provider_identity_validity()
                || !interval_covers(identity.validity(), start, end)
        })
    {
        return Err(AlpacaError::InvalidCoverage);
    }
    Ok(definition.quote_currency())
}

fn compare_canonical_index_entries(
    left: &CanonicalInstrumentAuthorityIndexEntry<'_>,
    right: &CanonicalInstrumentAuthorityIndexEntry<'_>,
) -> Ordering {
    left.instrument_id
        .cmp(&right.instrument_id)
        .then_with(|| left.source_id.cmp(right.source_id))
        .then_with(|| {
            left.provider_instrument_id
                .cmp(right.provider_instrument_id)
        })
        .then_with(|| left.definition_index.cmp(&right.definition_index))
}

fn compare_canonical_index_key(
    entry: &CanonicalInstrumentAuthorityIndexEntry<'_>,
    instrument_id: market_squawk_domain::InstrumentId,
    source_id: &SourceId,
    provider_instrument_id: &ProviderInstrumentId,
) -> Ordering {
    entry
        .instrument_id
        .cmp(&instrument_id)
        .then_with(|| entry.source_id.cmp(source_id))
        .then_with(|| entry.provider_instrument_id.cmp(provider_instrument_id))
}

fn try_owned_bounded(value: &str) -> Result<String, AlpacaError> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| AlpacaError::Allocation)?;
    owned.push_str(value);
    Ok(owned)
}

fn interval_covers(interval: EffectiveInterval, start: Timestamp, end: Timestamp) -> bool {
    interval.starts_at() <= start && interval.ends_at().is_none_or(|until| end < until)
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

fn validate_analytical_batch(
    batch: &ExtractionBatch,
    dataset: &AlpacaHistoricalEquityDataset,
    source_id: &SourceId,
    authority: &HistoricalInstrumentAuthority,
) -> Result<(), AlpacaError> {
    if batch.records().is_empty() {
        return Err(AlpacaError::Protocol);
    }
    let venue_id = authority.coordinate.venue().clone();
    let feed = SourceIdentifier::try_from("iex")?;
    let interval = SourceIdentifier::try_from(dataset.timeframe().provider_value())?;
    let adjustment = market_bar_adjustment(dataset.adjustment());
    for record in batch.records() {
        if record.source_id() != source_id
            || record.metadata_revision() != batch.request().object().metadata_revision()
            || record.dataset() != dataset.dataset()
            || record.schema().as_str() != CURRENT_RESEARCH_RECORD_SCHEMA
        {
            return Err(AlpacaError::Protocol);
        }
        let ResearchObservation::MarketBar(bar) =
            serde_json::from_slice(record.payload()).map_err(|_| AlpacaError::Protocol)?
        else {
            return Err(AlpacaError::Protocol);
        };
        let context = bar.context();
        let provenance = context.provenance();
        let Some(effective_at) = context.time().effective().exact_timestamp() else {
            return Err(AlpacaError::Protocol);
        };
        if record.effective_time().exact_timestamp() != Some(effective_at)
            || record.available_at() != provenance.availability().conservative_available_at()
            || effective_at < dataset.start()
            || effective_at > dataset.end()
            || provenance.source_id() != source_id
            || provenance.instrument_id() != Some(dataset.mapping().instrument())
            || provenance.venue_id() != Some(&venue_id)
            || provenance.source_timestamp() != Some(effective_at)
            || provenance.quality() != DataQuality::Aggregated
            || bar.provider_instrument_id()
                != authority.coordinate.identity_key().provider_instrument_id()
            || bar.feed() != &feed
            || bar.interval() != &interval
            || bar.adjustment() != adjustment
            || bar.currency() != authority.currency
            || bar.time_semantics().timestamp_basis()
                != dataset.series_semantics().timestamp_basis()
            || bar.time_semantics().session() != dataset.series_semantics().session()
        {
            return Err(AlpacaError::Protocol);
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "canonical mapping, provider-page evidence, and local observation times stay explicit"
)]
fn normalize_bar(
    dataset: &AlpacaHistoricalEquityDataset,
    authority: &HistoricalInstrumentAuthority,
    source_id: &SourceId,
    provider_page_evidence: &ExactPayloadEvidence,
    received_at: Timestamp,
    ingested_at: Timestamp,
    bar_time_authority: &dyn AlpacaHistoricalBarTimeAuthority,
    bar: BarWire,
) -> Result<ResearchObservation, AlpacaError> {
    let effective_at = parse_timestamp(&bar.timestamp)?;
    if effective_at < dataset.start() || effective_at > dataset.end() {
        return Err(AlpacaError::Protocol);
    }
    let venue_id = authority.coordinate.venue().clone();
    let timeframe = SourceIdentifier::try_from(dataset.timeframe().provider_value())?;
    let request = AlpacaHistoricalBarTimeRequest::new(
        dataset.mapping().instrument(),
        venue_id.clone(),
        authority
            .coordinate
            .identity_key()
            .provider_instrument_id()
            .clone(),
        timeframe.clone(),
        effective_at,
    );
    bar_time_authority.validate_current()?;
    let time_semantics = bar_time_authority.resolve(&request)?;
    bar_time_authority.validate_current()?;
    if time_semantics.period_start() >= time_semantics.period_end_exclusive()
        || time_semantics.provider_timestamp() != effective_at
        || time_semantics.timestamp_basis() != dataset.series_semantics().timestamp_basis()
        || time_semantics.session() != dataset.series_semantics().session()
        || time_semantics.session().ruleset().as_str().is_empty()
        || time_semantics.session().evidence().bytes() == [0; 32]
        || received_at < time_semantics.period_end_exclusive()
    {
        return Err(AlpacaError::Protocol);
    }
    let open_value = decimal(&bar.open, false)?;
    let high_value = decimal(&bar.high, false)?;
    let low_value = decimal(&bar.low, false)?;
    let close_value = decimal(&bar.close, false)?;
    let volume = decimal(&bar.volume, true)?;
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
    let vwap = bar.vwap.map(|value| decimal(&value, false)).transpose()?;
    let source_identifier = bar_source_identifier(dataset, effective_at)?;
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: source_id.clone(),
        instrument_id: Some(dataset.mapping().instrument()),
        venue_id: Some(venue_id),
        source_identifier,
        source_timestamp: Some(effective_at),
        received_at,
        ingested_at,
        quality: DataQuality::Aggregated,
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            DigestAlgorithm::Sha256,
            provider_page_evidence.content_digest().bytes(),
        )),
        availability: ResearchAvailabilityEvidence::local_first_observed(received_at),
    })
    .map_err(|_| AlpacaError::Protocol)?;
    let time = ResearchTime::new(
        effective_at,
        None,
        RevisionNumber::new(1).map_err(|_| AlpacaError::Protocol)?,
        None,
    )
    .map_err(|_| AlpacaError::Protocol)?;
    let context = ResearchContext::new(provenance, time).map_err(|_| AlpacaError::Protocol)?;
    MarketBarObservation::new(
        context,
        authority
            .coordinate
            .identity_key()
            .provider_instrument_id()
            .clone(),
        SourceIdentifier::try_from("iex")?,
        timeframe,
        time_semantics,
        market_bar_adjustment(dataset.adjustment()),
        Money::new(open_value, authority.currency),
        Money::new(high_value, authority.currency),
        Money::new(low_value, authority.currency),
        Money::new(close_value, authority.currency),
        volume,
        trade_count,
        vwap.map(|value| Money::new(value, authority.currency)),
    )
    .map(ResearchObservation::MarketBar)
    .map_err(|_| AlpacaError::Protocol)
}

fn observation_effective_at(observation: &ResearchObservation) -> Result<Timestamp, AlpacaError> {
    let ResearchObservation::MarketBar(bar) = observation else {
        return Err(AlpacaError::Protocol);
    };
    bar.context()
        .time()
        .effective()
        .exact_timestamp()
        .ok_or(AlpacaError::Protocol)
}

fn market_bar_adjustment(adjustment: crate::AlpacaAdjustment) -> MarketBarAdjustment {
    match adjustment {
        crate::AlpacaAdjustment::Raw => MarketBarAdjustment::Raw,
        crate::AlpacaAdjustment::Split => MarketBarAdjustment::Split,
        crate::AlpacaAdjustment::Dividend => MarketBarAdjustment::Dividend,
        crate::AlpacaAdjustment::SpinOff => MarketBarAdjustment::SpinOff,
        crate::AlpacaAdjustment::All => MarketBarAdjustment::All,
    }
}

fn bar_source_identifier(
    dataset: &AlpacaHistoricalEquityDataset,
    effective_at: Timestamp,
) -> Result<SourceIdentifier, AlpacaError> {
    SourceIdentifier::try_from(format!(
        "alpaca:iex:{}:{}:{}:{}",
        dataset.mapping().symbol(),
        dataset.timeframe().provider_value(),
        dataset.adjustment().as_str(),
        effective_at.unix_nanos()
    ))
    .map_err(Into::into)
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

fn validate_preflight_page(
    plan: &AlpacaHistoricalEquityPreflightPlan,
    page: &BarPage,
) -> Result<(), AlpacaError> {
    if page.symbol != plan.mapping().symbol() || page.bars.len() > usize::from(plan.page_limit()) {
        return Err(AlpacaError::Protocol);
    }
    let mut previous = None;
    for bar in &page.bars {
        let timestamp = parse_returned_bar_time(&bar.timestamp)?.provider_timestamp;
        if timestamp < plan.start()
            || timestamp > plan.end()
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

fn preflight_request_url(
    plan: &AlpacaHistoricalEquityPreflightPlan,
    page_token: Option<&str>,
) -> Result<url::Url, AlpacaError> {
    if let Some(token) = page_token {
        validate_page_token(token)?;
    }
    let mut url = url::Url::parse(ALPACA_STOCKS_BASE_ENDPOINT)
        .map_err(|_| AlpacaError::InvalidHistoricalPlan)?;
    url.path_segments_mut()
        .map_err(|_| AlpacaError::InvalidHistoricalPlan)?
        .push(plan.mapping().symbol())
        .push("bars");
    let start = timestamp_text(plan.start())?;
    let end = timestamp_text(plan.end())?;
    url.query_pairs_mut()
        .append_pair("timeframe", &plan.timeframe().provider_value())
        .append_pair("start", &start)
        .append_pair("end", &end)
        .append_pair("limit", &plan.page_limit().to_string())
        .append_pair("adjustment", plan.adjustment().as_str())
        .append_pair("feed", "iex")
        .append_pair("sort", "asc");
    if let Some(token) = page_token {
        url.query_pairs_mut().append_pair("page_token", token);
    }
    Ok(url)
}

async fn acquire_preflight_budget(
    budget: &SharedProviderBudget,
    deadline: std::time::Instant,
    cancellation: &CancellationToken,
) -> Result<BudgetReservation, AlpacaError> {
    loop {
        if cancellation.is_cancelled() {
            return Err(AlpacaError::Cancelled);
        }
        match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => return Ok(reservation),
            BudgetReservationDecision::WaitUntil(wait_until) => {
                wait_for_budget_deadline(budget, wait_until, deadline, cancellation).await?;
            }
            BudgetReservationDecision::Unavailable(_reason) => {
                return Err(AlpacaError::Network);
            }
        }
    }
}

async fn commit_preflight_budget(
    mut reservation: BudgetReservation,
    budget: &SharedProviderBudget,
    deadline: std::time::Instant,
    cancellation: &CancellationToken,
) -> Result<BudgetPermit, AlpacaError> {
    loop {
        if cancellation.is_cancelled() {
            return Err(AlpacaError::Cancelled);
        }
        if std::time::Instant::now() >= deadline {
            return Err(AlpacaError::DeadlineExceeded);
        }
        match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => return Ok(permit),
            BudgetDispatchDecision::WaitUntil(wait_until) => {
                wait_for_budget_deadline(budget, wait_until, deadline, cancellation).await?;
                reservation = acquire_preflight_budget(budget, deadline, cancellation).await?;
            }
            BudgetDispatchDecision::Unavailable(_reason) => return Err(AlpacaError::Network),
        }
    }
}

async fn wait_for_budget_deadline(
    budget: &SharedProviderBudget,
    wait_until: market_squawk_sources::MonotonicInstant,
    deadline: std::time::Instant,
    cancellation: &CancellationToken,
) -> Result<(), AlpacaError> {
    let wait = budget
        .remaining_wait(wait_until)
        .map_err(|_| AlpacaError::Network)?;
    let remaining = deadline
        .checked_duration_since(std::time::Instant::now())
        .ok_or(AlpacaError::DeadlineExceeded)?;
    if wait > remaining {
        return Err(AlpacaError::DeadlineExceeded);
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(AlpacaError::Cancelled),
        () = tokio::time::sleep(wait) => Ok(()),
    }
}

async fn wait_for_budget_decision(
    budget: &SharedProviderBudget,
    decision: BudgetDecision,
    deadline: std::time::Instant,
    cancellation: &CancellationToken,
) -> Result<(), AlpacaError> {
    let wait_until = match decision {
        BudgetDecision::WaitUntil(wait_until) => wait_until,
        BudgetDecision::Ready(permit) => {
            permit.release();
            return Err(AlpacaError::Protocol);
        }
        BudgetDecision::Unavailable(_reason) => return Err(AlpacaError::Network),
    };
    wait_for_budget_deadline(budget, wait_until, deadline, cancellation).await
}

fn enforce_preflight_window(plan: &AlpacaHistoricalEquityPreflightPlan) -> Result<(), AlpacaError> {
    enforce_window(plan.start(), plan.end())
}

fn enforce_historical_window(dataset: &AlpacaHistoricalEquityDataset) -> Result<(), AlpacaError> {
    enforce_window(dataset.start(), dataset.end())
}

fn enforce_window(start: Timestamp, end: Timestamp) -> Result<(), AlpacaError> {
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;
    let lookback = end
        .unix_nanos()
        .checked_sub(start.unix_nanos())
        .ok_or(AlpacaError::InvalidHistoricalPlan)?;
    let minimum = i64::from(crate::ALPACA_HISTORICAL_MIN_LOOKBACK_DAYS)
        .checked_mul(NANOS_PER_DAY)
        .ok_or(AlpacaError::InvalidHistoricalPlan)?;
    let maximum = i64::from(crate::ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS)
        .checked_mul(NANOS_PER_DAY)
        .ok_or(AlpacaError::InvalidHistoricalPlan)?;
    let cutoff = system_timestamp()?
        .checked_sub_nanos(
            i64::try_from(ALPACA_HISTORICAL_EXCLUSION_NANOS)
                .map_err(|_| AlpacaError::InvalidHistoricalPlan)?,
        )
        .map_err(|_| AlpacaError::InvalidHistoricalPlan)?;
    if lookback < minimum || lookback > maximum || lookback % NANOS_PER_DAY != 0 || end > cutoff {
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

fn complete_range_object_id(
    content_digest: EvidenceDigest,
) -> Result<SourceIdentifier, AlpacaError> {
    if content_digest.algorithm() != DigestAlgorithm::Sha256 || content_digest.bytes() == [0; 32] {
        return Err(AlpacaError::Protocol);
    }
    SourceIdentifier::try_from(format!(
        "alpaca-iex-complete-daily-history:{}",
        hex(content_digest.bytes())
    ))
    .map_err(Into::into)
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

fn build_historical_capture_material(
    source_id: &SourceId,
    metadata_revision: &MetadataRevision,
    dataset: &SourceIdentifier,
    preflight: &AlpacaHistoricalEquityPreflightReceipt,
) -> Result<ProviderCaptureMaterial, AlpacaError> {
    if preflight.pagination
        != AlpacaHistoricalPaginationDisposition::ProviderTerminalWithoutNextToken
        || preflight.pages.is_empty()
        || preflight_receipt_digest(
            &preflight.plan,
            &preflight.pages,
            &preflight.returned_bar_times,
            preflight.pagination,
            preflight.total_response_bytes,
        )? != preflight.digest
    {
        return Err(AlpacaError::CaptureMaterial);
    }

    let request_set_identity = historical_capture_request_set_identity(
        source_id,
        metadata_revision,
        dataset,
        preflight.plan(),
    )?;
    let mut page_receipts = Vec::new();
    page_receipts
        .try_reserve_exact(preflight.pages.len())
        .map_err(|_| AlpacaError::Allocation)?;
    let mut previous_response_token: Option<&str> = None;
    let mut previous_bar_time = None;
    let mut returned_bar_index = 0_usize;
    for (index, page) in preflight.pages.iter().enumerate() {
        let ordinal = u16::try_from(index).map_err(|_| AlpacaError::CaptureMaterial)?;
        if page.request_page_token.as_deref() != previous_response_token {
            return Err(AlpacaError::CaptureMaterial);
        }
        let expected_url =
            preflight_request_url(preflight.plan(), page.request_page_token.as_deref())?;
        let parsed = serde_json::from_slice::<BarPage>(&page.body)
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        validate_preflight_page(preflight.plan(), &parsed)
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        for bar in &parsed.bars {
            let returned = parse_returned_bar_time(&bar.timestamp)
                .map_err(|_| AlpacaError::CaptureMaterial)?;
            if previous_bar_time.is_some_and(|previous| returned <= previous)
                || previous_bar_time
                    .is_some_and(|previous| previous.calendar_date == returned.calendar_date)
                || preflight.returned_bar_times.get(returned_bar_index) != Some(&returned)
            {
                return Err(AlpacaError::CaptureMaterial);
            }
            previous_bar_time = Some(returned);
            returned_bar_index = returned_bar_index
                .checked_add(1)
                .ok_or(AlpacaError::CaptureMaterial)?;
        }
        if page.request_url.as_ref() != expected_url.as_str()
            || parsed.next_page_token.as_deref() != page.response_page_token.as_deref()
            || exact_evidence(&page.body) != page.evidence
            || (index + 1 < preflight.pages.len() && page.response_page_token.is_none())
            || (index + 1 == preflight.pages.len() && page.response_page_token.is_some())
        {
            return Err(AlpacaError::CaptureMaterial);
        }
        let body_bytes =
            u64::try_from(page.body.len()).map_err(|_| AlpacaError::CaptureMaterial)?;
        let request_identity = historical_capture_page_request_identity(
            request_set_identity,
            ordinal,
            &page.request_url,
        )?;
        let page_receipt = ProviderCapturePageReceipt::try_new(
            ordinal,
            request_identity,
            page.request_page_token.as_deref().map(token_digest),
            page.response_page_token.as_deref().map(token_digest),
            200,
            body_bytes,
            page.evidence.content_digest(),
            page.received_at,
        )
        .map_err(|_| AlpacaError::CaptureMaterial)?;
        page_receipts.push(page_receipt);
        previous_response_token = page.response_page_token.as_deref();
    }
    if returned_bar_index != preflight.returned_bar_times.len() {
        return Err(AlpacaError::CaptureMaterial);
    }

    let capture = ProviderCaptureSetReceipt::try_new(
        source_id.clone(),
        metadata_revision.clone(),
        dataset.clone(),
        request_set_identity,
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
        page_receipts,
    )
    .map_err(|_| AlpacaError::CaptureMaterial)?;
    let connection_id = Uuid::new_v5(&Uuid::NAMESPACE_URL, &capture.observation_digest().bytes());
    if connection_id.is_nil() {
        return Err(AlpacaError::CaptureMaterial);
    }
    let mut records = Vec::new();
    records
        .try_reserve_exact(preflight.pages.len())
        .map_err(|_| AlpacaError::Allocation)?;
    let source: Arc<str> = Arc::from(source_id.as_str());
    for (page, receipt) in preflight.pages.iter().zip(capture.pages()) {
        let mut event_identity = Sha256::new();
        event_identity.update(b"market-squawk/alpaca-iex-historical-capture-event/v1\0");
        event_identity.update(receipt.ordinal().to_be_bytes());
        event_identity.update(receipt.request_identity().bytes());
        event_identity.update(receipt.body_digest().bytes());
        let event_id = Uuid::new_v5(&connection_id, &event_identity.finalize());
        if event_id.is_nil() {
            return Err(AlpacaError::CaptureMaterial);
        }
        records.push(
            RawCaptureRecord::try_new_live(
                event_id,
                Arc::clone(&source),
                connection_id,
                Some(u64::from(receipt.ordinal())),
                None,
                DateTime::<Utc>::from_timestamp_nanos(page.received_at.unix_nanos()),
                page.body.clone(),
            )
            .map_err(|_| AlpacaError::CaptureMaterial)?,
        );
    }
    ProviderCaptureMaterial::try_new(capture, records).map_err(|_| AlpacaError::CaptureMaterial)
}

fn historical_capture_request_set_identity(
    source_id: &SourceId,
    metadata_revision: &MetadataRevision,
    dataset: &SourceIdentifier,
    plan: &AlpacaHistoricalEquityPreflightPlan,
) -> Result<EvidenceDigest, AlpacaError> {
    let initial_url = preflight_request_url(plan, None)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-iex-historical-capture-request-set/v1\0");
    hash_preflight_text(&mut digest, source_id.as_str())?;
    hash_preflight_text(
        &mut digest,
        metadata_revision.as_source_identifier().as_str(),
    )?;
    hash_preflight_text(&mut digest, dataset.as_str())?;
    hash_preflight_text(&mut digest, "GET")?;
    hash_preflight_text(&mut digest, initial_url.as_str())?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn historical_capture_page_request_identity(
    request_set_identity: EvidenceDigest,
    ordinal: u16,
    request_url: &str,
) -> Result<EvidenceDigest, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-iex-historical-capture-page/v1\0");
    digest.update(request_set_identity.bytes());
    digest.update(ordinal.to_be_bytes());
    hash_preflight_text(&mut digest, request_url)?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn token_digest(value: &str) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
}

fn exact_evidence(payload: &[u8]) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(payload).into(),
    ))
}

fn decimal(value: &Number, allow_zero: bool) -> Result<Decimal, AlpacaError> {
    let lexeme = value.to_string();
    let decimal = Decimal::from_str_exact(&lexeme).map_err(|_| AlpacaError::Protocol)?;
    if decimal.is_sign_negative() || (!allow_zero && decimal.is_zero()) {
        return Err(AlpacaError::Protocol);
    }
    Ok(decimal.normalize())
}

fn unsigned_integer(value: &Number) -> Result<u64, AlpacaError> {
    value.as_u64().ok_or(AlpacaError::Protocol)
}

fn parse_timestamp(value: &str) -> Result<Timestamp, AlpacaError> {
    parse_returned_bar_time(value).map(|returned| returned.provider_timestamp)
}

fn parse_returned_bar_time(value: &str) -> Result<AlpacaHistoricalReturnedBarTime, AlpacaError> {
    if value.len() > 64 || !(value.ends_with('Z') || value.ends_with("+00:00")) {
        return Err(AlpacaError::Protocol);
    }
    let parsed = DateTime::parse_from_rfc3339(value).map_err(|_| AlpacaError::Protocol)?;
    if parsed.offset().local_minus_utc() != 0 {
        return Err(AlpacaError::Protocol);
    }
    let provider_timestamp = parsed
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(AlpacaError::Protocol)?;
    let utc = parsed.with_timezone(&Utc);
    let year = u16::try_from(utc.year()).map_err(|_| AlpacaError::Protocol)?;
    let month = u8::try_from(utc.month()).map_err(|_| AlpacaError::Protocol)?;
    let day = u8::try_from(utc.day()).map_err(|_| AlpacaError::Protocol)?;
    let calendar_date = market_squawk_domain::CalendarDate::new(year, month, day)
        .map_err(|_| AlpacaError::Protocol)?;
    Ok(AlpacaHistoricalReturnedBarTime {
        provider_timestamp,
        calendar_date,
    })
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

fn ensure_wall_deadline(deadline: Timestamp) -> Result<(), AlpacaError> {
    if system_timestamp()? >= deadline {
        Err(AlpacaError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn preflight_receipt_digest(
    plan: &AlpacaHistoricalEquityPreflightPlan,
    pages: &[AlpacaHistoricalPreflightPage],
    returned_bar_times: &[AlpacaHistoricalReturnedBarTime],
    pagination: AlpacaHistoricalPaginationDisposition,
    total_response_bytes: usize,
) -> Result<EvidenceDigest, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-historical-preflight-receipt/v1\0");
    hash_preflight_text(&mut digest, plan.mapping().symbol())?;
    digest.update(plan.mapping().instrument().as_uuid().as_bytes());
    hash_preflight_evidence(
        &mut digest,
        plan.mapping().provider_coordinate().binding_digest(),
    );
    digest.update([match plan.mapping().asset_class() {
        market_squawk_domain::AssetClass::Equity => 1,
        market_squawk_domain::AssetClass::Fund => 2,
        _ => return Err(AlpacaError::InvalidHistoricalPlan),
    }]);
    hash_preflight_text(&mut digest, &plan.timeframe().provider_value())?;
    digest.update(plan.start().unix_nanos().to_be_bytes());
    digest.update(plan.end().unix_nanos().to_be_bytes());
    hash_preflight_text(&mut digest, plan.adjustment().as_str())?;
    digest.update(plan.page_limit().to_be_bytes());
    digest.update(
        u16::try_from(pages.len())
            .map_err(|_| AlpacaError::BodyTooLarge)?
            .to_be_bytes(),
    );
    let mut recomputed_total = 0_usize;
    for (index, page) in pages.iter().enumerate() {
        let actual = exact_evidence(&page.body);
        if actual != page.evidence {
            return Err(AlpacaError::Protocol);
        }
        recomputed_total = recomputed_total
            .checked_add(page.body.len())
            .ok_or(AlpacaError::BodyTooLarge)?;
        digest.update(
            u16::try_from(index)
                .map_err(|_| AlpacaError::BodyTooLarge)?
                .to_be_bytes(),
        );
        hash_preflight_text(&mut digest, &page.request_url)?;
        hash_optional_preflight_text(&mut digest, page.request_page_token.as_deref())?;
        hash_optional_preflight_text(&mut digest, page.response_page_token.as_deref())?;
        hash_preflight_evidence(&mut digest, page.evidence.content_digest());
        digest.update(page.received_at.unix_nanos().to_be_bytes());
        digest.update(
            u64::try_from(page.body.len())
                .map_err(|_| AlpacaError::BodyTooLarge)?
                .to_be_bytes(),
        );
    }
    if recomputed_total != total_response_bytes
        || total_response_bytes > MAXIMUM_PREFLIGHT_RETAINED_BYTES
    {
        return Err(AlpacaError::Protocol);
    }
    digest.update(
        u16::try_from(returned_bar_times.len())
            .map_err(|_| AlpacaError::BodyTooLarge)?
            .to_be_bytes(),
    );
    let mut previous = None;
    for returned in returned_bar_times {
        if previous.is_some_and(|value| returned <= value) {
            return Err(AlpacaError::Protocol);
        }
        previous = Some(returned);
        digest.update(returned.provider_timestamp.unix_nanos().to_be_bytes());
        digest.update(returned.calendar_date.year().to_be_bytes());
        digest.update([returned.calendar_date.month(), returned.calendar_date.day()]);
    }
    digest.update([match pagination {
        AlpacaHistoricalPaginationDisposition::ProviderTerminalWithoutNextToken => 1,
    }]);
    digest.update(
        u64::try_from(total_response_bytes)
            .map_err(|_| AlpacaError::BodyTooLarge)?
            .to_be_bytes(),
    );
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_preflight_text(digest: &mut Sha256, value: &str) -> Result<(), AlpacaError> {
    digest.update(
        u32::try_from(value.len())
            .map_err(|_| AlpacaError::Allocation)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

fn hash_optional_preflight_text(
    digest: &mut Sha256,
    value: Option<&str>,
) -> Result<(), AlpacaError> {
    match value {
        Some(value) => {
            digest.update([1]);
            hash_preflight_text(digest, value)
        }
        None => {
            digest.update([0]);
            Ok(())
        }
    }
}

fn hash_preflight_evidence(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(evidence.bytes());
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

#[cfg(test)]
mod capture_tests {
    use std::error::Error;
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::historical_transport::{
        AlpacaHistoricalScriptedHeader, AlpacaHistoricalScriptedResponse,
        AlpacaHistoricalScriptedTransportFactory,
    };
    use crate::{AlpacaAuthenticatedCalendarRequest, AlpacaTradingApiEnvironment};
    use market_squawk_domain::{
        AssetClass, MetadataRevision, ProviderIdentityEvidence, ProviderIdentityRecord,
        ProviderIdentityRecordInput, VenueMapping, VenueSymbol,
    };
    use market_squawk_sources::{
        AuthorizationMode, BackoffPolicy, BudgetScope, PreparedProviderRateRegistrationBatch,
        ProviderBudgetPolicy, ProviderRateAuthority, ProviderRateDeclaration,
        ProviderRateDispatchDecision, ProviderRateGroupId, ProviderRatePermitId,
        ProviderRateRegistration, ProviderRateReservationDecision, ProviderRateReservationId,
        ProviderRateRunId, ProviderRateStore, ProviderRateStoreError, RetryAfter,
    };

    #[derive(Debug, Default)]
    struct ReadyRateStore {
        dispatches: AtomicUsize,
    }

    impl ReadyRateStore {
        fn dispatches(&self) -> usize {
            self.dispatches.load(AtomicOrdering::SeqCst)
        }
    }

    #[derive(Debug)]
    struct ReadyPreparedRateBatch {
        registrations: Box<[ProviderRateRegistration]>,
    }

    impl PreparedProviderRateRegistrationBatch for ReadyPreparedRateBatch {
        fn registrations(&self) -> &[ProviderRateRegistration] {
            &self.registrations
        }

        fn commit(self: Box<Self>) -> Result<(), ProviderRateStoreError> {
            Ok(())
        }
    }

    impl ProviderRateStore for ReadyRateStore {
        fn start_run(&self, _now: Timestamp) -> Result<ProviderRateRunId, ProviderRateStoreError> {
            Ok(ProviderRateRunId::from_bytes([1; 16]))
        }

        fn prepare_registration_batch(
            &self,
            _run_id: ProviderRateRunId,
            declarations: &[ProviderRateDeclaration],
            _now: Timestamp,
        ) -> Result<Box<dyn PreparedProviderRateRegistrationBatch>, ProviderRateStoreError>
        {
            let registrations = declarations
                .iter()
                .map(|declaration| {
                    ProviderRateRegistration::new(
                        ProviderRateGroupId::from_bytes([2; 16]),
                        declaration.policy_digest(),
                        declaration.declaration_digest(),
                    )
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            Ok(Box::new(ReadyPreparedRateBatch { registrations }))
        }

        fn try_reserve(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _now: Timestamp,
        ) -> Result<ProviderRateReservationDecision, ProviderRateStoreError> {
            Ok(ProviderRateReservationDecision::Ready(
                ProviderRateReservationId::from_bytes([3; 16]),
            ))
        }

        fn commit_dispatch(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _reservation_id: ProviderRateReservationId,
            _now: Timestamp,
        ) -> Result<ProviderRateDispatchDecision, ProviderRateStoreError> {
            self.dispatches.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(ProviderRateDispatchDecision::Ready(
                ProviderRatePermitId::from_bytes([4; 16]),
            ))
        }

        fn cancel_reservation(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _reservation_id: ProviderRateReservationId,
        ) -> Result<(), ProviderRateStoreError> {
            Ok(())
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
        ) -> Result<ProviderRateReservationDecision, ProviderRateStoreError> {
            Err(ProviderRateStoreError::Unavailable)
        }

        fn apply_refusal(
            &self,
            _run_id: ProviderRateRunId,
            _registration: ProviderRateRegistration,
            _now: Timestamp,
            _jitter_sample_basis_points: u16,
        ) -> Result<ProviderRateReservationDecision, ProviderRateStoreError> {
            Err(ProviderRateStoreError::Unavailable)
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

    fn one_request_budget(
        store: Arc<ReadyRateStore>,
    ) -> Result<SharedProviderBudget, Box<dyn Error>> {
        const WINDOW_NANOS: u64 = 60_000_000_000;
        let provider = SourceIdentifier::try_from("alpaca-ready-budget-test")?;
        let subject = SourceIdentifier::try_from("alpaca-ready-budget-subject")?;
        let policy = ProviderBudgetPolicy::try_new(
            BudgetScope::with_authorization_account(provider, subject.clone()),
            NonZeroU32::MIN,
            NonZeroU64::new(WINDOW_NANOS).ok_or("budget window must be nonzero")?,
            NonZeroU16::MIN,
            BackoffPolicy::try_new(
                NonZeroU64::MIN,
                NonZeroU64::new(WINDOW_NANOS).ok_or("backoff maximum must be nonzero")?,
                0,
            )?,
        )?;
        let declaration = ProviderRateDeclaration::try_for_authorization_subject(policy, &subject)?;
        Ok(ProviderRateAuthority::try_new(store)?.register_budget(declaration)?)
    }

    #[tokio::test]
    async fn ready_preflight_reservation_cancelled_before_commit_is_uncharged()
    -> Result<(), Box<dyn Error>> {
        let store = Arc::new(ReadyRateStore::default());
        let budget = one_request_budget(store.clone())?;
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(1);
        let reservation = acquire_preflight_budget(&budget, deadline, &cancellation).await?;
        cancellation.cancel();

        let transport_dispatches = AtomicUsize::new(0);
        let result =
            match commit_preflight_budget(reservation, &budget, deadline, &cancellation).await {
                Ok(permit) => {
                    transport_dispatches.fetch_add(1, AtomicOrdering::SeqCst);
                    permit.release();
                    Ok(())
                }
                Err(error) => Err(error),
            };
        let durable_dispatches = store.dispatches();
        let next_reservation = budget.try_reserve_request();

        assert!(
            matches!(result, Err(AlpacaError::Cancelled)),
            "cancelled ready reservation reached dispatch: {result:?}"
        );
        assert_eq!(transport_dispatches.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(durable_dispatches, 0);
        assert!(matches!(
            next_reservation,
            BudgetReservationDecision::Ready(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn scripted_historical_transport_is_shared_and_counts_only_dispatches()
    -> Result<(), Box<dyn Error>> {
        let instrument = "00000001-0002-0003-0004-000000000001".parse()?;
        let identity = provider_identity(instrument)?;
        let mapping = crate::AlpacaInstrumentMapping::try_new(
            &identity,
            &venue_mapping()?,
            instrument,
            AssetClass::Equity,
        )?;
        let plan = AlpacaHistoricalEquityPreflightPlan::try_new(
            mapping,
            crate::AlpacaTimeframe::day(),
            Timestamp::from_unix_nanos(1_735_776_900_000_000_000),
            crate::AlpacaHistoricalLookback::try_from_days(30)?,
            crate::AlpacaAdjustment::All,
        )?;
        let returned_at = plan.start();
        let returned_at_text = timestamp_text(returned_at)?;
        let bar_body = Bytes::from(format!(
            r#"{{"bars":[{{"t":"{returned_at_text}","o":1,"h":2,"l":1,"c":2,"v":10}}],"symbol":"AAPL","next_page_token":null}}"#,
        ));
        let bar_received_at = Timestamp::from_unix_nanos(1_735_800_000_000_000_000);
        let calendar_received_at = Timestamp::from_unix_nanos(1_735_800_001_000_000_000);
        let calendar_body = Bytes::from_static(
            br#"[{"date":"2024-12-02","open":"09:30","close":"16:00","session_open":"04:00","session_close":"20:00"}]"#,
        );
        let fixture = AlpacaHistoricalScriptedTransportFactory::try_new(
            AlpacaHistoricalScriptedResponse::try_new(
                200,
                vec![
                    AlpacaHistoricalScriptedHeader::RateLimitLimit(200),
                    AlpacaHistoricalScriptedHeader::RateLimitRemaining(199),
                    AlpacaHistoricalScriptedHeader::RateLimitReset(1_735_800_060),
                ],
                bar_body,
                bar_received_at,
            )?,
            AlpacaHistoricalScriptedResponse::try_new(
                200,
                Vec::new(),
                calendar_body.clone(),
                calendar_received_at,
            )?,
        )?;
        let credentials = Arc::new(AlpacaCredentials::try_new(
            "alpaca-key".to_owned(),
            "alpaca-secret".to_owned(),
        )?);
        let bounds = HttpRequestBounds::default();
        let preflight = fixture.preflight_client(Arc::clone(&credentials), bounds)?;
        let calendar = fixture.calendar_executor(credentials, bounds)?;
        let rate_store = Arc::new(ReadyRateStore::default());
        let budget = one_request_budget(rate_store.clone())?;
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(1);

        let receipt = preflight
            .fetch(plan.clone(), &budget, deadline, &cancellation)
            .await?;
        let session_date = receipt.returned_bar_times()[0].calendar_date();
        let calendar_request = AlpacaAuthenticatedCalendarRequest::try_new(
            AlpacaTradingApiEnvironment::Paper,
            session_date,
            session_date,
        )?;

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            calendar
                .execute(calendar_request.clone(), deadline, &cancelled)
                .await,
            Err(AlpacaError::Cancelled)
        ));
        assert_eq!(fixture.counters().bar_dispatches(), 1);
        assert_eq!(fixture.counters().calendar_dispatches(), 0);

        let calendar_response = calendar
            .execute(calendar_request.clone(), deadline, &cancellation)
            .await?;

        assert_eq!(receipt.plan(), &plan);
        assert_eq!(
            receipt.returned_bar_times()[0].provider_timestamp(),
            returned_at
        );
        assert_eq!(
            receipt.last_rate_limit_evidence(),
            AlpacaRateLimitEvidence {
                limit: Some(200),
                remaining: Some(199),
                reset_unix_seconds: Some(1_735_800_060),
            }
        );
        assert_eq!(calendar_response.request(), &calendar_request);
        assert_eq!(calendar_response.body(), calendar_body);
        assert_eq!(calendar_response.received_at(), calendar_received_at);
        assert_eq!(rate_store.dispatches(), 1);
        assert_eq!(fixture.counters().bar_dispatches(), 1);
        assert_eq!(fixture.counters().calendar_dispatches(), 1);
        Ok(())
    }

    #[test]
    fn historical_capture_preserves_terminal_pages_and_refuses_broken_token_chain() {
        let instrument = "00000001-0002-0003-0004-000000000001"
            .parse()
            .expect("non-nil instrument identity");
        let identity = provider_identity(instrument).expect("valid provider identity");
        let mapping = crate::AlpacaInstrumentMapping::try_new(
            &identity,
            &venue_mapping().expect("valid venue mapping"),
            instrument,
            AssetClass::Equity,
        )
        .expect("valid exact provider mapping");
        let plan = AlpacaHistoricalEquityPreflightPlan::try_new(
            mapping,
            crate::AlpacaTimeframe::day(),
            Timestamp::from_unix_nanos(1_735_776_900_000_000_000),
            crate::AlpacaHistoricalLookback::try_from_days(30).expect("bounded lookback"),
            crate::AlpacaAdjustment::All,
        )
        .expect("valid plan");
        let first_time = plan
            .start()
            .checked_add_nanos(86_400_000_000_000)
            .expect("first time");
        let second_time = first_time
            .checked_add_nanos(86_400_000_000_000)
            .expect("second time");
        let first_returned =
            parse_returned_bar_time(&timestamp_text(first_time).expect("first provider timestamp"))
                .expect("first returned bar time");
        let second_returned = parse_returned_bar_time(
            &timestamp_text(second_time).expect("second provider timestamp"),
        )
        .expect("second returned bar time");
        let first_body = Bytes::from(format!(
            r#"{{"bars":[{{"t":"{}","o":1,"h":2,"l":1,"c":2,"v":10}}],"symbol":"AAPL","next_page_token":"page-two"}}"#,
            timestamp_text(first_time).expect("first timestamp text")
        ));
        let second_body = Bytes::from(format!(
            r#"{{"bars":[{{"t":"{}","o":2,"h":3,"l":2,"c":3,"v":20}}],"symbol":"AAPL","next_page_token":null}}"#,
            timestamp_text(second_time).expect("second timestamp text")
        ));
        let pages = vec![
            AlpacaHistoricalPreflightPage {
                request_url: preflight_request_url(&plan, None)
                    .expect("initial request")
                    .as_str()
                    .to_owned()
                    .into_boxed_str(),
                request_page_token: None,
                response_page_token: Some("page-two".into()),
                evidence: exact_evidence(&first_body),
                body: first_body,
                received_at: Timestamp::from_unix_nanos(1_735_800_000_000_000_000),
            },
            AlpacaHistoricalPreflightPage {
                request_url: preflight_request_url(&plan, Some("page-two"))
                    .expect("terminal request")
                    .as_str()
                    .to_owned()
                    .into_boxed_str(),
                request_page_token: Some("page-two".into()),
                response_page_token: None,
                evidence: exact_evidence(&second_body),
                body: second_body,
                received_at: Timestamp::from_unix_nanos(1_735_800_001_000_000_000),
            },
        ];
        let total_response_bytes = pages.iter().map(|page| page.body.len()).sum();
        let returned_bar_times = vec![first_returned, second_returned];
        let pagination = AlpacaHistoricalPaginationDisposition::ProviderTerminalWithoutNextToken;
        let digest = preflight_receipt_digest(
            &plan,
            &pages,
            &returned_bar_times,
            pagination,
            total_response_bytes,
        )
        .expect("exact preflight receipt");
        let mut preflight = AlpacaHistoricalEquityPreflightReceipt {
            plan,
            pages: pages.into_boxed_slice(),
            returned_bar_times: returned_bar_times.into_boxed_slice(),
            pagination,
            total_response_bytes,
            last_rate_limit: AlpacaRateLimitEvidence {
                limit: Some(200),
                remaining: Some(199),
                reset_unix_seconds: Some(1_735_800_060),
            },
            digest,
        };
        let source_id =
            SourceId::try_from("alpaca-iex-history-capture-test").expect("bounded source identity");
        let revision = MetadataRevision::new(
            SourceIdentifier::try_from("alpaca-iex-history-capture-test-v1")
                .expect("bounded revision identity"),
        );
        let dataset = SourceIdentifier::try_from("alpaca:historical-equity:test")
            .expect("bounded dataset identity");

        let material =
            build_historical_capture_material(&source_id, &revision, &dataset, &preflight)
                .expect("complete bounded capture material");
        assert_eq!(material.receipt().pages().len(), 2);
        assert_eq!(material.records().len(), 2);
        assert_eq!(
            material.receipt().terminal(),
            ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
        );
        assert_eq!(material.records()[0].payload(), preflight.pages[0].body);
        assert_eq!(material.records()[1].payload(), preflight.pages[1].body);
        assert_eq!(material.records()[0].source_sequence(), Some(0));
        assert_eq!(material.records()[1].source_sequence(), Some(1));

        preflight.pages[1].request_page_token = Some("wrong-token".into());
        preflight.digest = preflight_receipt_digest(
            &preflight.plan,
            &preflight.pages,
            &preflight.returned_bar_times,
            preflight.pagination,
            preflight.total_response_bytes,
        )
        .expect("self-consistent but broken graph receipt");
        assert!(matches!(
            build_historical_capture_material(&source_id, &revision, &dataset, &preflight),
            Err(AlpacaError::CaptureMaterial)
        ));
    }

    fn provider_identity(
        instrument_id: InstrumentId,
    ) -> Result<ProviderIdentityRecord, Box<dyn Error>> {
        Ok(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
            instrument_id,
            source_id: SourceId::try_from(crate::config::ALPACA_PROVIDER)?,
            provider_instrument_id: ProviderInstrumentId::try_from("AAPL-ASSET-ID")?,
            evidence: ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                [21; 32],
            )),
            source_timestamp: None,
            observed_at: Timestamp::from_unix_nanos(0),
            metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
                "alpaca-aapl-identity-v1",
            )?),
            validity: EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
            supersedes: None,
        }))
    }

    fn venue_mapping() -> Result<VenueMapping, Box<dyn Error>> {
        Ok(VenueMapping::new(
            VenueId::try_from(crate::config::IEX_VENUE)?,
            VenueSymbol::try_from("AAPL")?,
        ))
    }
}
