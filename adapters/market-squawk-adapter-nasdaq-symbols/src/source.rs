use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AssetClass, CoverageDelay, DataQuality, DeliveryEvidence, DigestAlgorithm, EffectiveInterval,
    EvidenceDigest, ExactPayloadEvidence, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_platform::SealedResearchJournalStore;
use market_squawk_sources::{
    AuthorizationMode, AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, CoverageDomain,
    DiscoveryBatch, DiscoveryRequest, EndpointPolicy, ExtractionAuthority,
    ExtractionAuthorityError, ExtractionBatch, ExtractionBatchAccumulator, ExtractionRecord,
    ExtractionRequest, ExtractionSource, ExtractionSourceError, HistoricalCapability,
    HttpRequestBounds, MonotonicInstant, NetworkAccessPolicy, ProviderCaptureMaterial,
    ProviderCapturePageReceipt, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    SourceClass, SourceError, SourceMetadata, SourceMetadataProvider, SourceObject,
    SourceObjectCaptureIdentity, SourceProtocolProfile, payload_matches_exact_evidence,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::archive::{NasdaqReferenceBlockingAdmission, validate_footer_clock};
use crate::client::{NasdaqHttpClient, RetrievedDirectory, ensure_deadline_open, system_timestamp};
use crate::model::{NasdaqDirectoryKind, NasdaqListingRecord};
use crate::parser::{NasdaqParseError, ParsedDirectory, parse_directory};
use crate::{
    NasdaqHttpResponseEvidence, NasdaqPendingReferenceHandoff, NasdaqReferenceDoctorReport,
    NasdaqReferenceError, NasdaqReferenceFileIdentity, OPTIONS_URL,
};

/// Official current Nasdaq-listed Symbol Directory object.
pub const NASDAQ_LISTED_URL: &str = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt";
/// Official current other-exchange-listed Symbol Directory object.
pub const OTHER_LISTED_URL: &str = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt";
/// Stable dataset namespace used for explicit discovery requests.
pub const NASDAQ_SYMBOL_DIRECTORY_DATASET: &str = "nasdaq.symbol-directory.us-listed.v1";
/// Provider identity required by the adapter's immutable source metadata.
pub const NASDAQ_SYMBOL_DIRECTORY_PROVIDER: &str = "nasdaq-trader-symbol-directory";
/// Exact listing-venue MICs represented by the admitted equity directory files.
pub const NASDAQ_SYMBOL_DIRECTORY_VENUES: [&str; 7] =
    ["XNAS", "XASE", "XNYS", "ARCX", "XCHI", "BATS", "IEXG"];
/// Minimum app-owned total acquisition budget for the bounded large-object path.
pub const NASDAQ_REFERENCE_MIN_TOTAL_TIMEOUT_NANOS: u64 = 300_000_000_000;
/// Maximum requests admitted by the shared application queue in its minute window.
pub const NASDAQ_APPLICATION_REQUESTS_PER_MINUTE: u32 = 8;
/// Minimum duration of the shared application request window.
pub const NASDAQ_APPLICATION_BUDGET_WINDOW_NANOS: u64 = 60_000_000_000;
/// Every Nasdaq reference family shares one single-flight application queue.
pub const NASDAQ_APPLICATION_MAX_CONCURRENT_REQUESTS: u16 = 1;
/// Minimum admitted fallback-backoff cap after a provider refusal.
pub const NASDAQ_APPLICATION_MIN_BACKOFF_MAXIMUM_NANOS: u64 = 60_000_000_000;

/// Builds the exact three-file hardened endpoint allowlist for admitted Nasdaq acquisition.
///
/// Root composition uses this same policy for `SourceMetadata`, preventing a broader application
/// allowlist from silently expanding the extraction authority admitted by this adapter.
///
/// # Errors
///
/// Rejects request bounds or an exact official locator that cannot be represented.
pub fn nasdaq_reference_endpoint_policy(
    bounds: HttpRequestBounds,
) -> Result<EndpointPolicy, NasdaqSymbolDirectorySourceError> {
    EndpointPolicy::try_new_with_bounds([NASDAQ_LISTED_URL, OTHER_LISTED_URL, OPTIONS_URL], bounds)
        .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)
}

const MAX_CACHED_OBJECTS: usize = 4;
const MEDIA_TYPE: &str = "text/plain";

/// Indivisible complete-directory discovery and exact provider body handoff.
///
/// The two independently complete HTTP responses are framed as one ordered bounded-extraction
/// request graph, so neither file can masquerade as the complete U.S.-listed directory. This
/// session path does not claim logical-object, catalog, or point-in-time publication.
#[derive(Debug)]
pub struct NasdaqSymbolDirectoryDiscovery {
    batch: DiscoveryBatch,
    capture_material: ProviderCaptureMaterial,
    response_evidence: Box<[NasdaqHttpResponseEvidence]>,
}

impl NasdaqSymbolDirectoryDiscovery {
    /// Returns both exact source objects under one discovery request.
    pub const fn batch(&self) -> &DiscoveryBatch {
        &self.batch
    }

    /// Returns the exact two-body request graph used by the existing bounded extraction path.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture_material
    }

    /// Returns independently retained status/media/length/validator/clock evidence for both files.
    pub fn response_evidence(&self) -> &[NasdaqHttpResponseEvidence] {
        &self.response_evidence
    }

    /// Consumes the capture-first application handoff.
    pub fn into_parts(
        self,
    ) -> (
        DiscoveryBatch,
        ProviderCaptureMaterial,
        Box<[NasdaqHttpResponseEvidence]>,
    ) {
        (self.batch, self.capture_material, self.response_evidence)
    }
}

/// Exact, fixed-endpoint Symbol Directory source configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NasdaqSymbolDirectoryConfig {
    dataset: SourceIdentifier,
}

impl NasdaqSymbolDirectoryConfig {
    /// Constructs the stable complete current-directory dataset contract.
    ///
    /// # Errors
    ///
    /// Returns [`NasdaqSymbolDirectorySourceError::InvalidConfiguration`] if the code-owned
    /// dataset identity no longer satisfies domain bounds.
    pub fn try_new() -> Result<Self, NasdaqSymbolDirectorySourceError> {
        let dataset = SourceIdentifier::try_from(NASDAQ_SYMBOL_DIRECTORY_DATASET)
            .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidConfiguration)?;
        Ok(Self { dataset })
    }

    /// Returns the exact dataset identity callers must use for discovery.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }
}

/// Bounded health for one independently fetched directory file.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NasdaqDirectoryHealth {
    last_attempt_at: Option<Timestamp>,
    last_success_at: Option<Timestamp>,
    last_payload_digest: Option<[u8; 32]>,
    consecutive_failures: u32,
}

/// Typed provider-refusal evidence returned by the fresh family doctor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NasdaqReferenceRetryEvidence {
    family: NasdaqDirectoryKind,
    status: u16,
    retry_after_header_present: bool,
    transport_elapsed_nanos: u64,
    observed_at: Timestamp,
    retry_deadline: MonotonicInstant,
}

impl NasdaqReferenceRetryEvidence {
    pub(crate) fn try_new(
        family: NasdaqDirectoryKind,
        status: u16,
        retry_after_header_present: bool,
        transport_elapsed_nanos: u64,
        observed_at: Timestamp,
        retry_deadline: MonotonicInstant,
    ) -> Result<Self, SourceError> {
        if !matches!(status, 429 | 503) || transport_elapsed_nanos == 0 {
            return Err(SourceError::InvalidProtocolState);
        }
        Ok(Self {
            family,
            status,
            retry_after_header_present,
            transport_elapsed_nanos,
            observed_at,
            retry_deadline,
        })
    }

    /// Returns the exact family whose endpoint refused the request.
    pub const fn family(&self) -> NasdaqDirectoryKind {
        self.family
    }

    /// Returns the exact HTTP refusal status (`429` or `503`).
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns whether the provider supplied a `Retry-After` field.
    pub const fn retry_after_header_present(&self) -> bool {
        self.retry_after_header_present
    }

    /// Returns monotonic send-through-response-header elapsed time.
    pub const fn transport_elapsed_nanos(&self) -> u64 {
        self.transport_elapsed_nanos
    }

    /// Returns the local wall-clock observation of the refusal.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns the process-local shared-budget retry deadline.
    pub const fn retry_deadline(&self) -> MonotonicInstant {
        self.retry_deadline
    }
}

/// Fresh live provider acquisition plus its pending typed handoff and validation-only report.
#[derive(Debug)]
pub struct NasdaqLiveReferenceDoctorResult {
    object: NasdaqPendingReferenceHandoff,
    report: NasdaqReferenceDoctorReport,
}

impl NasdaqLiveReferenceDoctorResult {
    /// Returns the exact generation ready for bounded typed pages and indexed queries.
    pub const fn object(&self) -> &NasdaqPendingReferenceHandoff {
        &self.object
    }

    /// Returns the fresh endpoint/schema/raw/index validation report.
    pub const fn report(&self) -> &NasdaqReferenceDoctorReport {
        &self.report
    }

    /// Consumes the result into its one-shot typed handoff and validation report.
    pub fn into_parts(self) -> (NasdaqPendingReferenceHandoff, NasdaqReferenceDoctorReport) {
        (self.object, self.report)
    }
}

impl NasdaqDirectoryHealth {
    /// Returns the most recent attempted request time.
    pub const fn last_attempt_at(self) -> Option<Timestamp> {
        self.last_attempt_at
    }

    /// Returns the most recent fully validated response time.
    pub const fn last_success_at(self) -> Option<Timestamp> {
        self.last_success_at
    }

    /// Returns the SHA-256 digest of the most recent fully validated exact file.
    pub const fn last_payload_digest(self) -> Option<[u8; 32]> {
        self.last_payload_digest
    }

    /// Returns consecutive failed fetch or validation attempts.
    pub const fn consecutive_failures(self) -> u32 {
        self.consecutive_failures
    }
}

#[derive(Debug, Default)]
struct HealthState {
    nasdaq_listed: NasdaqDirectoryHealth,
    other_listed: NasdaqDirectoryHealth,
    bonds: NasdaqDirectoryHealth,
    options: NasdaqDirectoryHealth,
}

impl HealthState {
    fn get(&self, kind: NasdaqDirectoryKind) -> NasdaqDirectoryHealth {
        match kind {
            NasdaqDirectoryKind::NasdaqListed => self.nasdaq_listed,
            NasdaqDirectoryKind::OtherListed => self.other_listed,
            NasdaqDirectoryKind::Bonds => self.bonds,
            NasdaqDirectoryKind::Options => self.options,
        }
    }

    fn get_mut(&mut self, kind: NasdaqDirectoryKind) -> &mut NasdaqDirectoryHealth {
        match kind {
            NasdaqDirectoryKind::NasdaqListed => &mut self.nasdaq_listed,
            NasdaqDirectoryKind::OtherListed => &mut self.other_listed,
            NasdaqDirectoryKind::Bonds => &mut self.bonds,
            NasdaqDirectoryKind::Options => &mut self.options,
        }
    }
}

#[derive(Clone, Debug)]
struct CachedDirectory {
    object_id: SourceIdentifier,
    retrieved: RetrievedDirectory,
    parsed: ParsedDirectory,
}

#[derive(Debug, Default)]
struct DirectoryCache {
    entries: VecDeque<Arc<CachedDirectory>>,
}

impl DirectoryCache {
    fn find(&self, object_id: &SourceIdentifier) -> Option<Arc<CachedDirectory>> {
        self.entries
            .iter()
            .find(|entry| &entry.object_id == object_id)
            .cloned()
    }

    fn insert(&mut self, entry: Arc<CachedDirectory>) {
        self.entries
            .retain(|cached| cached.object_id.as_str() != entry.object_id.as_str());
        self.entries.push_front(entry);
        self.entries.truncate(MAX_CACHED_OBJECTS);
    }
}

/// Registered, authority-bound Nasdaq Trader Symbol Directory extraction producer.
pub struct NasdaqSymbolDirectorySource {
    metadata: SourceMetadata,
    config: NasdaqSymbolDirectoryConfig,
    http: NasdaqHttpClient,
    final_verification_admission: NasdaqReferenceBlockingAdmission,
    cache: Mutex<DirectoryCache>,
    health: Mutex<HealthState>,
}

impl std::fmt::Debug for NasdaqSymbolDirectorySource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NasdaqSymbolDirectorySource")
            .field("source_id", self.metadata.source_id())
            .field("revision", self.metadata.revision())
            .field("dataset", self.config.dataset())
            .finish_non_exhaustive()
    }
}

impl NasdaqSymbolDirectorySource {
    /// Binds the fixed official endpoints to immutable registered source metadata.
    ///
    /// # Errors
    ///
    /// Fails closed unless metadata declares an extraction-only, delayed, official-quality,
    /// multi-venue reference source, one safe shared provider budget, and all three admitted files.
    pub fn try_new(
        metadata: SourceMetadata,
        config: NasdaqSymbolDirectoryConfig,
    ) -> Result<Self, NasdaqSymbolDirectorySourceError> {
        Self::validate_metadata(&metadata)?;
        let http = NasdaqHttpClient::try_new(&metadata)?;
        Ok(Self {
            metadata,
            config,
            http,
            final_verification_admission: NasdaqReferenceBlockingAdmission::new(),
            cache: Mutex::new(DirectoryCache::default()),
            health: Mutex::new(HealthState::default()),
        })
    }

    /// Returns the exact dataset identity callers must use for discovery.
    pub const fn dataset(&self) -> &SourceIdentifier {
        self.config.dataset()
    }

    pub(crate) fn final_verification_admission(&self) -> NasdaqReferenceBlockingAdmission {
        self.final_verification_admission.clone()
    }

    /// Returns a bounded health snapshot for one independent official file.
    ///
    /// # Errors
    ///
    /// Fails closed if local health synchronization is poisoned.
    pub fn health(
        &self,
        kind: NasdaqDirectoryKind,
    ) -> Result<NasdaqDirectoryHealth, NasdaqSymbolDirectorySourceError> {
        self.health
            .lock()
            .map(|health| health.get(kind))
            .map_err(|_| NasdaqSymbolDirectorySourceError::HealthUnavailable)
    }

    /// Discovers the complete two-file current directory together with exact raw provider bodies.
    ///
    /// This is the existing session-scoped extraction contract. Logical-object storage and the
    /// consuming typed handoff are available only through [`Self::ingest_reference_object`].
    pub fn discover_with_capture(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<NasdaqSymbolDirectoryDiscovery, ExtractionSourceError>> {
        Box::pin(self.discover_with_capture_impl(authority, request, cancellation))
    }

    /// Streams one exact official file unchanged into the shared logical-object store, then
    /// completely validates it into a consuming provider-native handoff.
    ///
    /// This does not publish a catalog generation, establish point-in-time availability, or make
    /// the object durable in any application-facing workflow.
    pub async fn ingest_reference_object(
        &self,
        authority: &ExtractionAuthority,
        family: NasdaqDirectoryKind,
        store: &SealedResearchJournalStore,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<NasdaqPendingReferenceHandoff, NasdaqReferenceIngestError> {
        self.validate_authority(authority)?;
        ensure_deadline_open(deadline)?;
        if directory_locator(family).is_none() {
            return Err(NasdaqReferenceError::ProviderContractUnavailable.into());
        }
        self.record_attempt(family)?;
        let result = async {
            let fetched = self
                .http
                .fetch_logical_object(
                    &self.metadata,
                    authority,
                    family,
                    store,
                    deadline,
                    cancellation,
                )
                .await?;
            let file_identity = NasdaqReferenceFileIdentity::try_new(
                self.metadata.source_id().as_str(),
                self.metadata.revision().as_source_identifier().as_str(),
                self.config.dataset().as_str(),
                family,
            )?;
            let control = crate::archive::NasdaqReferenceOperationControl::try_new_for_source(
                deadline,
                cancellation,
                authority,
            )?;
            let handoff = NasdaqPendingReferenceHandoff::try_from_verified(
                file_identity,
                fetched.response_evidence,
                fetched.verified_object,
                self.final_verification_admission(),
                &control,
            )
            .map_err(NasdaqReferenceIngestError::from)?;
            authority.validate_current()?;
            Ok::<_, NasdaqReferenceIngestError>(handoff)
        }
        .await;
        match result {
            Ok(handoff) => {
                let mut health = self
                    .health
                    .lock()
                    .map_err(|_| NasdaqSymbolDirectorySourceError::HealthUnavailable)?;
                let state = health.get_mut(family);
                state.last_success_at = Some(handoff.generation_evidence().first_observed_at());
                state.last_payload_digest =
                    Some(handoff.generation_evidence().raw_content_digest().bytes());
                state.consecutive_failures = 0;
                Ok(handoff)
            }
            Err(error) => {
                if let Ok(mut health) = self.health.lock() {
                    let state = health.get_mut(family);
                    state.consecutive_failures = state.consecutive_failures.saturating_add(1);
                }
                Err(error)
            }
        }
    }

    /// Performs a fresh official fetch and complete same-descriptor schema/index validation.
    ///
    /// The receipt deliberately requires root-owned freshness classification: HTTP success and
    /// valid file clocks do not establish currentness because Nasdaq publishes no exact interval.
    pub async fn live_reference_doctor(
        &self,
        authority: &ExtractionAuthority,
        family: NasdaqDirectoryKind,
        store: &SealedResearchJournalStore,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<NasdaqLiveReferenceDoctorResult, NasdaqReferenceIngestError> {
        let object = self
            .ingest_reference_object(authority, family, store, deadline, cancellation)
            .await?;
        let report = object.validation_report();
        Ok(NasdaqLiveReferenceDoctorResult { object, report })
    }

    async fn discover_with_capture_impl(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<NasdaqSymbolDirectoryDiscovery, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.dataset() != self.config.dataset()
            || request.effective_at().is_some()
            || request.max_results() < 2
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        ensure_deadline_open(request.deadline())?;
        let mut objects = Vec::with_capacity(2);
        let mut captures = Vec::with_capacity(2);
        let mut response_evidence = Vec::with_capacity(2);
        for kind in [
            NasdaqDirectoryKind::NasdaqListed,
            NasdaqDirectoryKind::OtherListed,
        ] {
            let entry = self
                .fetch_validated(&authority, kind, request.deadline(), &cancellation)
                .await?;
            captures.push(self.capture_material(&entry)?);
            response_evidence.push(entry.retrieved.response_evidence.clone());
            self.cache
                .lock()
                .map_err(|_| SourceError::InvalidProtocolState)?
                .insert(entry);
        }
        let capture_material = ProviderCaptureMaterial::try_combine_request_graph(
            self.config.dataset().clone(),
            request_graph_identity(&self.metadata, &self.config)?,
            captures,
        )
        .map_err(|_| SourceError::InvalidProtocolState)?;
        let capture_identity =
            SourceObjectCaptureIdentity::try_from_capture(capture_material.receipt())
                .map_err(|_| SourceError::InvalidProtocolState)?;
        let cached = self
            .cache
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        for kind in [
            NasdaqDirectoryKind::NasdaqListed,
            NasdaqDirectoryKind::OtherListed,
        ] {
            let entry = cached
                .entries
                .iter()
                .find(|entry| entry.retrieved.kind == kind)
                .ok_or(SourceError::InvalidProtocolState)?;
            objects.push(self.source_object(&request, entry, capture_identity)?);
        }
        drop(cached);
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        ensure_deadline_open(request.deadline())?;
        authority.validate_current()?;
        let batch = DiscoveryBatch::try_new(&request, objects)?;
        Ok(NasdaqSymbolDirectoryDiscovery {
            batch,
            capture_material,
            response_evidence: response_evidence.into_boxed_slice(),
        })
    }

    async fn extract_impl(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<ExtractionBatch, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        self.validate_extraction_request(&request)?;
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        ensure_deadline_open(request.deadline())?;
        let (kind, object_digest) = parse_object_id(request.object().object_id())?;
        let cached = self
            .cache
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?
            .find(request.object().object_id());
        let entry = match cached {
            Some(entry) => entry,
            None => {
                let fetched = self
                    .fetch_validated(&authority, kind, request.deadline(), &cancellation)
                    .await?;
                if fetched.object_id.as_str() != request.object().object_id().as_str() {
                    return Err(SourceError::GenerationResynchronizationRequired.into());
                }
                self.cache
                    .lock()
                    .map_err(|_| SourceError::InvalidProtocolState)?
                    .insert(Arc::clone(&fetched));
                fetched
            }
        };
        if entry.retrieved.kind != kind
            || entry.parsed.kind != kind
            || entry.retrieved.sha256_hex.as_str() != object_digest
            || request.object().evidence() != &exact_evidence(&entry.retrieved.bytes)
            || !payload_matches_exact_evidence(&entry.retrieved.bytes, request.object().evidence())
            || request.object().expected_bytes() != u64::try_from(entry.retrieved.bytes.len()).ok()
        {
            return Err(SourceError::GenerationResynchronizationRequired.into());
        }
        let record_count = entry.parsed.rows.len();
        if record_count > request.max_records() as usize {
            return Err(
                market_squawk_sources::ExtractionError::RecordLimitExceeded {
                    requested: request.max_records(),
                }
                .into(),
            );
        }

        let schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let source_last_modified_at = request
            .object()
            .published_at()
            .ok_or(SourceError::InvalidProtocolState)?;
        let first_observed_at = match request.object().availability() {
            AvailabilityEvidence::LocalFirstObserved { observed_at } => *observed_at,
            _ => return Err(SourceError::InvalidProtocolState.into()),
        };
        let source_evidence = request.object().evidence().clone();
        let mut batch = ExtractionBatchAccumulator::try_new(&request)?;
        for (index, row) in entry.parsed.rows.iter().enumerate() {
            if cancellation.is_cancelled() {
                return Err(ExtractionSourceError::Cancelled);
            }
            if index.is_multiple_of(256) {
                ensure_deadline_open(request.deadline())?;
                authority.validate_current()?;
            }
            let normalized = NasdaqListingRecord::try_new(
                row.row_number,
                entry.parsed.file_creation_time.clone(),
                source_last_modified_at,
                first_observed_at,
                source_evidence.clone(),
                row.fields.clone(),
            )
            .map_err(|_| SourceError::InvalidProtocolState)?;
            let payload = serde_json::to_vec(&normalized)
                .map(Bytes::from)
                .map_err(|_| SourceError::InvalidProtocolState)?;
            let evidence = exact_evidence(&payload);
            let revision = SourceIdentifier::try_from(format!(
                "nasdaq-symbols:{}:row-{}:{object_digest}",
                kind.object_component(),
                row.row_number
            ))
            .map_err(|_| SourceError::InvalidProtocolState)?;
            let record = ExtractionRecord::try_new(
                &request,
                schema.clone(),
                evidence,
                source_last_modified_at,
                Some(source_last_modified_at),
                request.object().availability().clone(),
                revision,
                None,
                payload,
            )?;
            batch.push(record)?;
        }
        ensure_deadline_open(request.deadline())?;
        authority.validate_current()?;
        batch.finish().map_err(Into::into)
    }

    fn source_object(
        &self,
        request: &DiscoveryRequest,
        entry: &CachedDirectory,
        capture_identity: SourceObjectCaptureIdentity,
    ) -> Result<SourceObject, ExtractionSourceError> {
        let evidence = exact_evidence(&entry.retrieved.bytes);
        let effective = EffectiveInterval::new(entry.retrieved.last_modified_at, None)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let expected_bytes = u64::try_from(entry.retrieved.bytes.len())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        SourceObject::try_new_with_capture_identity(
            self.metadata.source_id().clone(),
            self.metadata.revision().clone(),
            request,
            entry.object_id.clone(),
            SourceIdentifier::try_from(MEDIA_TYPE)
                .map_err(|_| SourceError::InvalidProtocolState)?,
            evidence,
            capture_identity,
            effective,
            Some(entry.retrieved.last_modified_at),
            AvailabilityEvidence::LocalFirstObserved {
                observed_at: entry.retrieved.received_at,
            },
            Some(expected_bytes),
        )
        .map_err(Into::into)
    }

    fn capture_material(
        &self,
        entry: &CachedDirectory,
    ) -> Result<ProviderCaptureMaterial, ExtractionSourceError> {
        let locator =
            directory_locator(entry.retrieved.kind).ok_or(SourceError::InvalidProtocolState)?;
        let request_identity = capture_request_identity(&self.metadata, entry.retrieved.kind)?;
        let body_digest = exact_evidence(&entry.retrieved.bytes).content_digest();
        let body_bytes = u64::try_from(entry.retrieved.bytes.len())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let page = ProviderCapturePageReceipt::try_new(
            0,
            request_identity,
            None,
            None,
            200,
            body_bytes,
            body_digest,
            entry.retrieved.received_at,
        )
        .map_err(|_| SourceError::InvalidProtocolState)?;
        let receipt = ProviderCaptureSetReceipt::try_new(
            self.metadata.source_id().clone(),
            self.metadata.revision().clone(),
            SourceIdentifier::try_from(locator).map_err(|_| SourceError::InvalidProtocolState)?,
            request_identity,
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![page],
        )
        .map_err(|_| SourceError::InvalidProtocolState)?;
        let connection_id = deterministic_capture_uuid(b"connection", &receipt);
        let event_id = deterministic_capture_uuid(b"event", &receipt);
        let record = market_squawk_platform::RawCaptureRecord::try_new_live(
            event_id,
            Arc::from(self.metadata.source_id().as_str()),
            connection_id,
            Some(0),
            None,
            DateTime::<Utc>::from_timestamp_nanos(entry.retrieved.received_at.unix_nanos()),
            entry.retrieved.bytes.clone(),
        )
        .map_err(|_| SourceError::InvalidProtocolState)?;
        ProviderCaptureMaterial::try_new(receipt, vec![record])
            .map_err(|_| SourceError::InvalidProtocolState.into())
    }

    async fn fetch_validated(
        &self,
        authority: &ExtractionAuthority,
        kind: NasdaqDirectoryKind,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<Arc<CachedDirectory>, ExtractionSourceError> {
        self.record_attempt(kind)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let result = async {
            let retrieved = self
                .http
                .fetch(&self.metadata, authority, kind, deadline, cancellation)
                .await?;
            let parsed =
                parse_directory(kind, &retrieved.bytes, cancellation).map_err(map_parse_error)?;
            validate_footer_clock(&parsed.file_creation_time, retrieved.last_modified_at)
                .map_err(|_| SourceError::InvalidProtocolState)?;
            let object_id = SourceIdentifier::try_from(format!(
                "nasdaq-symbols:{}:{}",
                kind.object_component(),
                retrieved.sha256_hex
            ))
            .map_err(|_| SourceError::InvalidProtocolState)?;
            Ok(Arc::new(CachedDirectory {
                object_id,
                retrieved,
                parsed,
            }))
        }
        .await;
        self.record_result(kind, &result)?;
        result
    }

    fn validate_extraction_request(
        &self,
        request: &ExtractionRequest,
    ) -> Result<(), ExtractionSourceError> {
        let object = request.object();
        let availability_matches = matches!(
            object.availability(),
            AvailabilityEvidence::LocalFirstObserved { observed_at }
                if *observed_at >= object.effective_interval().starts_at()
        );
        if object.source_id() != self.metadata.source_id()
            || object.metadata_revision() != self.metadata.revision()
            || object.dataset() != self.config.dataset()
            || object.media_type().as_str() != MEDIA_TYPE
            || object.effective_interval().ends_at().is_some()
            || object.published_at() != Some(object.effective_interval().starts_at())
            || !availability_matches
            || object.expected_bytes().is_none()
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        Ok(())
    }

    fn validate_authority(
        &self,
        authority: &ExtractionAuthority,
    ) -> Result<(), ExtractionSourceError> {
        authority.validate_current()?;
        if authority.metadata() != &self.metadata {
            return Err(SourceError::InvalidProtocolState.into());
        }
        Ok(())
    }

    fn validate_metadata(
        metadata: &SourceMetadata,
    ) -> Result<(), NasdaqSymbolDirectorySourceError> {
        let asset_classes = metadata.coverage().asset_classes();
        let budget = metadata
            .budget_policy()
            .ok_or(NasdaqSymbolDirectorySourceError::InvalidMetadata)?;
        let has_safe_minute_window = (0..budget.window_count())
            .filter_map(|index| budget.window(index))
            .any(|window| {
                window.requests_per_window() <= NASDAQ_APPLICATION_REQUESTS_PER_MINUTE
                    && window.window_nanos() >= NASDAQ_APPLICATION_BUDGET_WINDOW_NANOS
            });
        let safe_budget = budget.scope().as_source_identifier() == metadata.provider()
            && budget.scope().authorization_account().is_none()
            && budget.max_concurrent() == NASDAQ_APPLICATION_MAX_CONCURRENT_REQUESTS
            && has_safe_minute_window
            && budget.backoff().maximum_nanos() >= NASDAQ_APPLICATION_MIN_BACKOFF_MAXIMUM_NANOS;
        let required_assets = asset_classes.len() == 3
            && asset_classes.contains(&AssetClass::Equity)
            && asset_classes.contains(&AssetClass::Fund)
            && asset_classes.contains(&AssetClass::Option);
        let required_venues =
            NASDAQ_SYMBOL_DIRECTORY_VENUES
                .iter()
                .try_fold(true, |all_present, mic| {
                    let venue = VenueId::try_from(*mic)
                        .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)?;
                    Ok::<_, NasdaqSymbolDirectorySourceError>(
                        all_present && metadata.coverage().topology().contains_venue(&venue),
                    )
                })?;
        if metadata.source_class() != SourceClass::Exchange
            || metadata.provider().as_str() != NASDAQ_SYMBOL_DIRECTORY_PROVIDER
            || metadata.coverage().domain() != CoverageDomain::Instruments
            || metadata.quality_ceiling() != DataQuality::OfficialDelayed
            || metadata.authorization().mode() != AuthorizationMode::PublicInterface
            || !safe_budget
            || !required_assets
            || !metadata.coverage().topology().is_consolidated()
            || metadata.coverage().topology().venues().len() != NASDAQ_SYMBOL_DIRECTORY_VENUES.len()
            || !required_venues
            || !matches!(metadata.coverage().delay(), CoverageDelay::Delayed(_))
            || metadata.coverage().delivery() != DeliveryEvidence::Indirect
            || metadata.capabilities().live()
            || !metadata.capabilities().extraction()
            // Nasdaq exposes only current directory objects. Retaining separately observed raw
            // objects downstream does not turn the provider surface into a historical API.
            || metadata.capabilities().historical() != HistoricalCapability::None
            || metadata.capabilities().source_timestamps()
            || !matches!(metadata.protocol_profile(), SourceProtocolProfile::NotLive)
        {
            return Err(NasdaqSymbolDirectorySourceError::InvalidMetadata);
        }
        metadata
            .network_policy()
            .authorize(NASDAQ_LISTED_URL)
            .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)?;
        metadata
            .network_policy()
            .authorize(OTHER_LISTED_URL)
            .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)?;
        metadata
            .network_policy()
            .authorize(OPTIONS_URL)
            .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)?;
        let NetworkAccessPolicy::Allowlisted(policy) = metadata.network_policy() else {
            return Err(NasdaqSymbolDirectorySourceError::InvalidMetadata);
        };
        if policy != &nasdaq_reference_endpoint_policy(policy.request_bounds())? {
            return Err(NasdaqSymbolDirectorySourceError::InvalidMetadata);
        }
        if policy.request_bounds().max_response_bytes()
            < u64::try_from(crate::MAX_OPTIONS_SOURCE_BYTES)
                .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)?
        {
            return Err(NasdaqSymbolDirectorySourceError::InvalidMetadata);
        }
        if policy.request_bounds().total_timeout_nanos() < NASDAQ_REFERENCE_MIN_TOTAL_TIMEOUT_NANOS
        {
            return Err(NasdaqSymbolDirectorySourceError::InvalidMetadata);
        }
        Ok(())
    }

    fn record_attempt(
        &self,
        kind: NasdaqDirectoryKind,
    ) -> Result<(), NasdaqSymbolDirectorySourceError> {
        let now = system_timestamp()?;
        let mut health = self
            .health
            .lock()
            .map_err(|_| NasdaqSymbolDirectorySourceError::HealthUnavailable)?;
        health.get_mut(kind).last_attempt_at = Some(now);
        Ok(())
    }

    fn record_result(
        &self,
        kind: NasdaqDirectoryKind,
        result: &Result<Arc<CachedDirectory>, ExtractionSourceError>,
    ) -> Result<(), ExtractionSourceError> {
        let mut health = self
            .health
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let value = health.get_mut(kind);
        match result {
            Ok(entry) => {
                value.last_success_at = Some(entry.retrieved.received_at);
                value.last_payload_digest = Some(Sha256::digest(&entry.retrieved.bytes).into());
                value.consecutive_failures = 0;
            }
            Err(_) => {
                value.consecutive_failures = value.consecutive_failures.saturating_add(1);
            }
        }
        Ok(())
    }
}

impl SourceMetadataProvider for NasdaqSymbolDirectorySource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl ExtractionSource for NasdaqSymbolDirectorySource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        Box::pin(async move {
            self.discover_with_capture_impl(authority, request, cancellation)
                .await
                .map(NasdaqSymbolDirectoryDiscovery::into_parts)
                .map(|(batch, _capture, _response_evidence)| batch)
        })
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

pub(crate) const fn directory_locator(kind: NasdaqDirectoryKind) -> Option<&'static str> {
    match kind {
        NasdaqDirectoryKind::NasdaqListed => Some(NASDAQ_LISTED_URL),
        NasdaqDirectoryKind::OtherListed => Some(OTHER_LISTED_URL),
        NasdaqDirectoryKind::Bonds => None,
        NasdaqDirectoryKind::Options => Some(OPTIONS_URL),
    }
}

fn capture_request_identity(
    metadata: &SourceMetadata,
    kind: NasdaqDirectoryKind,
) -> Result<EvidenceDigest, ExtractionSourceError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/nasdaq-symbol-directory-http-request/v1");
    hash_capture_field(&mut hash, metadata.source_id().as_str().as_bytes())?;
    hash_capture_field(
        &mut hash,
        metadata
            .revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    hash_capture_field(&mut hash, b"GET")?;
    let locator = directory_locator(kind).ok_or(SourceError::InvalidProtocolState)?;
    hash_capture_field(&mut hash, locator.as_bytes())?;
    hash_capture_field(&mut hash, b"accept:text/plain")?;
    hash_capture_field(&mut hash, b"accept-encoding:identity")?;
    hash_capture_field(
        &mut hash,
        concat!(
            "user-agent:market-squawk/",
            env!("CARGO_PKG_VERSION"),
            " nasdaq-symbol-directory-adapter"
        )
        .as_bytes(),
    )?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn request_graph_identity(
    metadata: &SourceMetadata,
    config: &NasdaqSymbolDirectoryConfig,
) -> Result<EvidenceDigest, ExtractionSourceError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/nasdaq-symbol-directory-request-graph/v1");
    hash_capture_field(&mut hash, metadata.source_id().as_str().as_bytes())?;
    hash_capture_field(
        &mut hash,
        metadata
            .revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    hash_capture_field(&mut hash, config.dataset().as_str().as_bytes())?;
    for kind in [
        NasdaqDirectoryKind::NasdaqListed,
        NasdaqDirectoryKind::OtherListed,
    ] {
        hash.update(capture_request_identity(metadata, kind)?.bytes());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn hash_capture_field(hash: &mut Sha256, value: &[u8]) -> Result<(), ExtractionSourceError> {
    let length = u16::try_from(value.len()).map_err(|_| SourceError::InvalidProtocolState)?;
    hash.update(length.to_be_bytes());
    hash.update(value);
    Ok(())
}

fn deterministic_capture_uuid(tag: &[u8], receipt: &ProviderCaptureSetReceipt) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/nasdaq-symbol-directory-provider-capture-id/v1");
    hash.update((tag.len() as u64).to_be_bytes());
    hash.update(tag);
    hash.update(receipt.request_set_identity().bytes());
    hash.update(receipt.observation_digest().bytes());
    let digest = hash.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

/// Adapter configuration, metadata, local state, or clock failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NasdaqSymbolDirectorySourceError {
    /// The code-owned dataset contract could not be constructed.
    #[error("invalid Nasdaq Symbol Directory configuration")]
    InvalidConfiguration,
    /// Registered metadata is incompatible with this exact adapter contract.
    #[error("Nasdaq Symbol Directory metadata is incompatible with the adapter")]
    InvalidMetadata,
    /// Local bounded health state is unavailable.
    #[error("Nasdaq Symbol Directory health is unavailable")]
    HealthUnavailable,
    /// The process clock could not produce a bounded Unix-nanosecond observation.
    #[error("Nasdaq Symbol Directory observation clock is unavailable")]
    Clock,
}

/// Network, authority, logical-object, or validation failure for one family object.
#[derive(Debug, Error)]
pub enum NasdaqReferenceIngestError {
    /// Source-neutral authority, deadline, or transport admission failed.
    #[error("Nasdaq reference extraction failed: {0}")]
    Extraction(#[from] ExtractionSourceError),
    /// Shared logical-object capture or provider-native validation failed.
    #[error("Nasdaq reference logical-object handoff failed: {0}")]
    Handoff(#[from] NasdaqReferenceError),
    /// Adapter configuration, health, or clock state was unavailable.
    #[error("Nasdaq reference source state failed: {0}")]
    Source(#[from] NasdaqSymbolDirectorySourceError),
    /// Nasdaq returned a typed `429`/`503` refusal and the shared queue installed its cooldown.
    #[error("Nasdaq reference provider requested a retry")]
    RetryRequired {
        /// Exact provider-family refusal and shared-budget deadline evidence.
        evidence: NasdaqReferenceRetryEvidence,
    },
}

impl From<SourceError> for NasdaqReferenceIngestError {
    fn from(error: SourceError) -> Self {
        Self::Extraction(ExtractionSourceError::Source(error))
    }
}

impl From<ExtractionAuthorityError> for NasdaqReferenceIngestError {
    fn from(error: ExtractionAuthorityError) -> Self {
        Self::Extraction(ExtractionSourceError::Authority(error))
    }
}

pub(crate) fn parse_object_id(
    object_id: &SourceIdentifier,
) -> Result<(NasdaqDirectoryKind, &str), ExtractionSourceError> {
    let mut fields = object_id.as_str().split(':');
    if fields.next() != Some("nasdaq-symbols") {
        return Err(SourceError::InvalidProtocolState.into());
    }
    let kind = match fields.next() {
        Some("nasdaq-listed") => NasdaqDirectoryKind::NasdaqListed,
        Some("other-listed") => NasdaqDirectoryKind::OtherListed,
        Some("bonds") | Some("options") => return Err(SourceError::InvalidProtocolState.into()),
        _ => return Err(SourceError::InvalidProtocolState.into()),
    };
    let digest = fields.next().ok_or(SourceError::InvalidProtocolState)?;
    if fields.next().is_some()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SourceError::InvalidProtocolState.into());
    }
    Ok((kind, digest))
}

fn exact_evidence(payload: &[u8]) -> ExactPayloadEvidence {
    let digest = Sha256::digest(payload);
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.into(),
    ))
}

fn map_parse_error(error: NasdaqParseError) -> ExtractionSourceError {
    match error {
        NasdaqParseError::Cancelled => ExtractionSourceError::Cancelled,
        NasdaqParseError::BodyTooLarge { max } => {
            ExtractionSourceError::Source(SourceError::FrameTooLarge { max })
        }
        _ => ExtractionSourceError::Source(SourceError::InvalidProtocolState),
    }
}
