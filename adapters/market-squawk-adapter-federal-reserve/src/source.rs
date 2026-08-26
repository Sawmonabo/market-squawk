//! Shared-authority extraction source for one exact selected Board release file.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailabilityEvidence, DataQuality, DigestAlgorithm,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MacroMissingValue, MacroObservation,
    PayloadHash, PayloadReference, ResearchContext, ResearchObservation, ResearchPeriod,
    ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime,
    RevisionNumber, SourceIdentifier, Timestamp, VersionPinnedSourceLocator,
};
use market_squawk_sources::{
    AuthorizationMode, CoverageDomain, DiscoveryBatch, DiscoveryRequest, ExtractionAuthority,
    ExtractionBatch, ExtractionBatchAccumulator, ExtractionRequest, ExtractionRevisionPlan,
    ExtractionSource, ExtractionSourceError, HistoricalCapability, ProviderCaptureMaterial,
    SourceClass, SourceError, SourceMetadata, SourceMetadataProvider, SourceObject,
    SourceProtocolProfile,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[cfg(any(test, all(feature = "scripted-transport-fixture", debug_assertions)))]
use crate::transport::BoardTransport;
use crate::transport::{
    BoardAttemptTelemetry, BoardConditionalRequest, BoardFetchFailure, BoardHttpClient,
    BoardHttpReceipt, BoardRetrievalOutcome, BoardRetrievedFile, system_timestamp,
};
#[cfg(all(feature = "scripted-transport-fixture", debug_assertions))]
use crate::transport::{BoardScriptedProductionTransport, BoardScriptedTransportFactory};
use crate::{
    BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_DATE_COUNT,
    BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_OBSERVATION_COUNT, BoardAdapterError,
    BoardArtifactKind, BoardDatasetContract, BoardFileFormat, BoardH15AnalyticalCapability,
    BoardParseLimits, BoardPeriod, BoardPeriodValue, BoardSeries, BoardValue, ParsedBoardDataset,
    parse_csv, parse_sdmx_xml, parse_sdmx_zip,
};

const BOARD_PROVIDER_ID: &str = "federal-reserve-board";
const BOARD_APPLICATION_MINIMUM_WINDOW_NANOS: u64 = 60_000_000_000;
const MAX_STRUCTURAL_ARTIFACTS: usize = 255;

/// Exact external structural artifact retained for an uncompressed SDMX data file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoardStructuralArtifact {
    name: Box<str>,
    bytes: Bytes,
    sha256: [u8; 32],
}

impl BoardStructuralArtifact {
    /// Retains one bounded exact schema/structure file.
    pub fn try_new(
        name: impl Into<Box<str>>,
        bytes: impl Into<Bytes>,
    ) -> Result<Self, BoardSourceError> {
        let name = name.into();
        let bytes = bytes.into();
        if name.is_empty()
            || name.len() > 512
            || name.starts_with('/')
            || name.ends_with('/')
            || name.contains(['\\', ':', '\0'])
            || name
                .split('/')
                .any(|component| component.is_empty() || matches!(component, "." | ".."))
            || bytes.is_empty()
        {
            return Err(BoardSourceError::InvalidProfile);
        }
        Ok(Self {
            name,
            sha256: Sha256::digest(&bytes).into(),
            bytes,
        })
    }

    /// Returns the exact contract-relative artifact name.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Returns exact artifact bytes.
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }
    /// Returns exact SHA-256 evidence.
    pub const fn sha256(&self) -> [u8; 32] {
        self.sha256
    }
}

/// One exact selected Board file, parser budget, and required structural evidence.
#[derive(Clone, Debug)]
pub struct BoardDatasetProfile {
    contract: BoardDatasetContract,
    parse_limits: BoardParseLimits,
    structural_artifacts: Vec<BoardStructuralArtifact>,
    dataset: SourceIdentifier,
    analytical_dataset: SourceIdentifier,
}

impl BoardDatasetProfile {
    /// Builds the closed rolling 100-date H.15 dashboard production profile.
    pub fn h15_treasury_constant_maturities_rolling_dashboard() -> Result<Self, BoardSourceError> {
        Self::try_new(
            BoardDatasetContract::h15_treasury_constant_maturities_rolling_dashboard_csv()
                .map_err(BoardSourceError::Protocol)?,
            BoardParseLimits::h15_treasury_constant_maturities_rolling_dashboard(),
            Vec::new(),
        )
    }

    /// Builds a source profile. External artifacts are required only for uncompressed SDMX XML;
    /// CSV has none and ZIP must contain its complete closed artifact set.
    pub fn try_new(
        contract: BoardDatasetContract,
        parse_limits: BoardParseLimits,
        structural_artifacts: Vec<BoardStructuralArtifact>,
    ) -> Result<Self, BoardSourceError> {
        validate_profile_parse_limits(&contract, parse_limits)?;
        validate_structural_artifacts(&contract, parse_limits, &structural_artifacts)?;
        let dataset = SourceIdentifier::try_from(format!(
            "federal-reserve-board:{}:{}:{}",
            contract.release().code().to_ascii_lowercase(),
            contract.family().as_str(),
            lower_hex(contract.contract_digest()),
        ))
        .map_err(|_| BoardSourceError::InvalidProfile)?;
        let analytical_dataset = SourceIdentifier::try_from(format!(
            "federal-reserve-board.{}.{}.{}",
            contract.release().code().to_ascii_lowercase(),
            contract.family().as_str(),
            lower_hex(contract.contract_digest()),
        ))
        .map_err(|_| BoardSourceError::InvalidProfile)?;
        Ok(Self {
            contract,
            parse_limits,
            structural_artifacts,
            dataset,
            analytical_dataset,
        })
    }

    /// Returns the exact file/request/parser contract.
    pub const fn contract(&self) -> &BoardDatasetContract {
        &self.contract
    }
    /// Returns the exact parser safeguards.
    pub const fn parse_limits(&self) -> BoardParseLimits {
        self.parse_limits
    }
    /// Returns the stable provider-dataset identity used by discovery.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }
    /// Returns the storage-safe analytical identity for this exact contract.
    pub const fn analytical_dataset(&self) -> &SourceIdentifier {
        &self.analytical_dataset
    }
    /// Returns external schema/structure artifacts for XML profiles.
    pub fn structural_artifacts(&self) -> &[BoardStructuralArtifact] {
        &self.structural_artifacts
    }

    /// Describes the analytical limits of the exact rolling H.15 dashboard profile.
    /// Concrete use still requires the common manifest, PIT, and research-use authorities.
    #[must_use]
    pub fn h15_analytical_capability(&self) -> Option<BoardH15AnalyticalCapability> {
        BoardH15AnalyticalCapability::for_profile(self)
    }

    pub(crate) fn parse(&self, bytes: &[u8]) -> Result<ParsedBoardDataset, BoardAdapterError> {
        let parsed = match self.contract.format() {
            BoardFileFormat::DdpCsvSeriesColumnV1 => {
                parse_csv(&self.contract, bytes, self.parse_limits)
            }
            BoardFileFormat::SdmxCompactZipV1 => {
                parse_sdmx_zip(&self.contract, bytes, self.parse_limits)
            }
            BoardFileFormat::SdmxCompactXmlV1 => {
                let artifacts = self
                    .structural_artifacts
                    .iter()
                    .map(|artifact| (artifact.name(), artifact.bytes().as_ref()))
                    .collect::<Vec<_>>();
                parse_sdmx_xml(&self.contract, bytes, &artifacts, self.parse_limits)
            }
        }?;
        if self
            .contract
            .is_h15_treasury_constant_maturities_rolling_dashboard()
        {
            validate_rolling_dashboard_shape(&parsed)?;
        }
        Ok(parsed)
    }
}

/// Bounded local runtime telemetry. Numeric provider capacity is deliberately absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct BoardSourceHealth {
    requests_total: u64,
    modified_responses_total: u64,
    not_modified_responses_total: u64,
    throttled_responses_total: u64,
    failed_responses_total: u64,
    last_status: Option<u16>,
    last_response_bytes: u64,
    last_response_digest: Option<[u8; 32]>,
    last_received_at: Option<Timestamp>,
    last_latency_nanos: u64,
    last_retry_after_present: bool,
}

impl BoardSourceHealth {
    const fn new() -> Self {
        Self {
            requests_total: 0,
            modified_responses_total: 0,
            not_modified_responses_total: 0,
            throttled_responses_total: 0,
            failed_responses_total: 0,
            last_status: None,
            last_response_bytes: 0,
            last_response_digest: None,
            last_received_at: None,
            last_latency_nanos: 0,
            last_retry_after_present: false,
        }
    }
    /// Returns actual transport attempts.
    pub const fn requests_total(self) -> u64 {
        self.requests_total
    }
    /// Returns validated HTTP 200 files.
    pub const fn modified_responses_total(self) -> u64 {
        self.modified_responses_total
    }
    /// Returns validated HTTP 304 responses.
    pub const fn not_modified_responses_total(self) -> u64 {
        self.not_modified_responses_total
    }
    /// Returns observed HTTP 429/503 refusals.
    pub const fn throttled_responses_total(self) -> u64 {
        self.throttled_responses_total
    }
    /// Returns all other failed attempts.
    pub const fn failed_responses_total(self) -> u64 {
        self.failed_responses_total
    }
    /// Returns last HTTP status when a response existed.
    pub const fn last_status(self) -> Option<u16> {
        self.last_status
    }
    /// Returns exact last response bytes.
    pub const fn last_response_bytes(self) -> u64 {
        self.last_response_bytes
    }
    /// Returns exact last response digest, including refusal bodies.
    pub const fn last_response_digest(self) -> Option<[u8; 32]> {
        self.last_response_digest
    }
    /// Returns last complete response time.
    pub const fn last_received_at(self) -> Option<Timestamp> {
        self.last_received_at
    }
    /// Returns measured last latency.
    pub const fn last_latency_nanos(self) -> u64 {
        self.last_latency_nanos
    }
    /// Returns whether the last response supplied `Retry-After`.
    pub const fn last_retry_after_present(self) -> bool {
        self.last_retry_after_present
    }
}

/// Rich extraction result retaining parsed native evidence and its exact HTTP receipt.
#[derive(Debug)]
pub struct BoardExtractionOutput {
    batch: ExtractionBatch,
    parsed: ParsedBoardDataset,
    receipt: BoardHttpReceipt,
    capture: ProviderCaptureMaterial,
}

impl BoardExtractionOutput {
    /// Returns the canonical shared extraction batch.
    pub const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }
    /// Returns complete parsed source-native evidence.
    pub const fn parsed(&self) -> &ParsedBoardDataset {
        &self.parsed
    }
    /// Returns exact transport evidence.
    pub const fn receipt(&self) -> &BoardHttpReceipt {
        &self.receipt
    }
    /// Returns the complete source-neutral exact-body material that must be sealed before the
    /// canonical batch is admitted to durable publication.
    pub const fn capture(&self) -> &ProviderCaptureMaterial {
        &self.capture
    }
    /// Consumes the indivisible extraction handoff into canonical, native, transport, and raw
    /// material parts. Composition must seal `capture` before publishing `batch`.
    pub fn into_parts(
        self,
    ) -> (
        ExtractionBatch,
        ParsedBoardDataset,
        BoardHttpReceipt,
        ProviderCaptureMaterial,
    ) {
        (self.batch, self.parsed, self.receipt, self.capture)
    }
}

/// Official Board source for exactly one code-owned release-file profile.
pub struct BoardSource {
    metadata: SourceMetadata,
    profile: BoardDatasetProfile,
    client: BoardHttpClient,
    cached_discovery: Mutex<Option<BoardRetrievedFile>>,
    health: Mutex<BoardSourceHealth>,
}

impl std::fmt::Debug for BoardSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoardSource")
            .field("source_id", self.metadata.source_id())
            .field("revision", self.metadata.revision())
            .field("dataset", self.profile.dataset())
            .field(
                "contract_digest",
                &self.profile.contract().contract_digest(),
            )
            .finish_non_exhaustive()
    }
}

impl BoardSource {
    /// Binds immutable selected-profile metadata to the no-credential Board transport.
    pub fn try_new(
        metadata: SourceMetadata,
        profile: BoardDatasetProfile,
    ) -> Result<Self, BoardSourceError> {
        validate_one_batch_source_profile(&profile)?;
        Self::validate_metadata(&metadata, &profile)?;
        let client = BoardHttpClient::try_new(&metadata)?;
        Ok(Self {
            metadata,
            profile,
            client,
            cached_discovery: Mutex::new(None),
            health: Mutex::new(BoardSourceHealth::new()),
        })
    }

    /// Returns the storage-safe analytical identity for this configured provider dataset.
    pub fn analytical_dataset_identifier(
        &self,
        provider_dataset: &SourceIdentifier,
    ) -> Result<SourceIdentifier, BoardSourceError> {
        if provider_dataset != self.profile.dataset() {
            return Err(BoardSourceError::InvalidProfile);
        }
        Ok(self.profile.analytical_dataset().clone())
    }

    #[cfg(any(test, all(feature = "scripted-transport-fixture", debug_assertions)))]
    pub(crate) fn try_new_with_transport(
        metadata: SourceMetadata,
        profile: BoardDatasetProfile,
        transport: Arc<dyn BoardTransport>,
    ) -> Result<Self, BoardSourceError> {
        validate_one_batch_source_profile(&profile)?;
        Self::validate_metadata(&metadata, &profile)?;
        let client = BoardHttpClient::try_new_with_transport(&metadata, transport)?;
        Ok(Self {
            metadata,
            profile,
            client,
            cached_discovery: Mutex::new(None),
            health: Mutex::new(BoardSourceHealth::new()),
        })
    }

    fn validate_metadata(
        metadata: &SourceMetadata,
        profile: &BoardDatasetProfile,
    ) -> Result<(), BoardSourceError> {
        let budget = metadata
            .budget_policy()
            .ok_or(BoardSourceError::InvalidMetadata)?;
        let has_one_per_minute_or_stricter = (0..budget.window_count())
            .filter_map(|index| budget.window(index))
            .any(|window| {
                window.requests_per_window() == 1
                    && window.window_nanos() >= BOARD_APPLICATION_MINIMUM_WINDOW_NANOS
            });
        if metadata.source_class() != SourceClass::OfficialAgency
            || metadata.provider().as_str() != BOARD_PROVIDER_ID
            || metadata.authorization().mode() != AuthorizationMode::PublicInterface
            || metadata.coverage().domain() != CoverageDomain::Macroeconomic
            || metadata.quality_ceiling() != DataQuality::OfficialDelayed
            || metadata.capabilities().live()
            || !metadata.capabilities().extraction()
            || metadata.capabilities().historical() != HistoricalCapability::Historical
            || !matches!(metadata.protocol_profile(), SourceProtocolProfile::NotLive)
            || budget.max_concurrent() != 1
            || !has_one_per_minute_or_stricter
        {
            return Err(BoardSourceError::InvalidMetadata);
        }
        metadata
            .network_policy()
            .authorize(profile.contract().url())
            .map_err(|_| BoardSourceError::InvalidMetadata)
    }

    /// Returns the configured exact dataset profile.
    pub const fn profile(&self) -> &BoardDatasetProfile {
        &self.profile
    }
    /// Returns bounded runtime telemetry.
    pub fn health(&self) -> Result<BoardSourceHealth, BoardSourceError> {
        self.health
            .lock()
            .map(|value| *value)
            .map_err(|_| BoardSourceError::HealthUnavailable)
    }

    /// Retrieves a modified file or a truthful `304` under shared extraction/rate authority.
    pub async fn retrieve(
        &self,
        authority: &ExtractionAuthority,
        conditional: Option<&BoardConditionalRequest>,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<BoardRetrievalOutcome, ExtractionSourceError> {
        self.validate_authority(authority)?;
        let result = self
            .client
            .fetch(
                &self.metadata,
                authority,
                &self.profile,
                conditional,
                deadline,
                cancellation,
            )
            .await;
        match result {
            Ok(outcome) => {
                self.record_success(&outcome)?;
                Ok(outcome)
            }
            Err(failure) => {
                self.record_failure(&failure)?;
                Err(failure.error)
            }
        }
    }

    /// Produces the indivisible canonical/native/raw handoff for application composition.
    ///
    /// The caller must seal [`BoardExtractionOutput::capture`] successfully before admitting its
    /// canonical batch to publication. The generic [`ExtractionSource::extract`] seam deliberately
    /// fails closed because that trait cannot transfer required raw capture material.
    pub async fn extract_with_evidence(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<BoardExtractionOutput, BoardExtractionError> {
        self.validate_authority(&authority)?;
        validate_extraction_request(&self.metadata, &self.profile, &request)?;
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled.into());
        }
        ensure_deadline(request.deadline())?;
        let expected_digest = object_body_digest(request.object().object_id(), &self.profile)?;
        let cached = self
            .cached_discovery
            .lock()
            .map_err(|_| invalid_protocol())?
            .take();
        let retrieved = match cached {
            Some(value) if value.receipt().body_digest() == expected_digest => value,
            _ => match self
                .retrieve(&authority, None, request.deadline(), &cancellation)
                .await?
            {
                BoardRetrievalOutcome::Modified(value) => *value,
                BoardRetrievalOutcome::NotModified(_) => return Err(invalid_protocol().into()),
            },
        };
        if retrieved.receipt().body_digest() != expected_digest
            || !market_squawk_sources::payload_matches_exact_evidence(
                retrieved.exact_bytes(),
                request.object().evidence(),
            )
        {
            return Err(BoardExtractionError::Source(
                SourceError::GenerationResynchronizationRequired.into(),
            ));
        }
        if retrieved.receipt().body_bytes() > market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGE_BYTES
        {
            return Err(BoardExtractionError::CaptureBodyTooLarge {
                body_bytes: retrieved.receipt().body_bytes(),
                max: market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGE_BYTES,
            });
        }
        let ingested_at = system_timestamp().map_err(map_adapter_error)?;
        let mut accumulator = ExtractionBatchAccumulator::try_new(&request)?;
        for record in canonical_records(
            &self.metadata,
            retrieved.parsed(),
            retrieved.receipt(),
            ingested_at,
        )? {
            accumulator.push(market_squawk_sources::ExtractionRecord::try_new_with_time(
                &request,
                SourceIdentifier::try_from(market_squawk_sources::CURRENT_RESEARCH_RECORD_SCHEMA)
                    .map_err(|_| invalid_protocol())?,
                record.evidence,
                record.effective,
                None,
                record.availability,
                record.revision,
                None,
                record.payload,
            )?)?;
        }
        let batch = accumulator.finish()?;
        let (bytes, parsed, receipt) = retrieved.into_parts();
        let capture = capture_material(
            &self.metadata,
            self.profile.dataset().clone(),
            &receipt,
            bytes,
        )?;
        Ok(BoardExtractionOutput {
            batch,
            parsed,
            receipt,
            capture,
        })
    }

    /// Board DDP has no provider vintage chronology; revisions are locally observed acquisitions.
    pub fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<ExtractionRevisionPlan, BoardSourceError> {
        if batch.request().object().source_id() != self.metadata.source_id()
            || batch.request().object().metadata_revision() != self.metadata.revision()
        {
            return Err(BoardSourceError::InvalidProfile);
        }
        ExtractionRevisionPlan::locally_observed(batch.records().len())
            .map_err(|_| BoardSourceError::CanonicalMapping)
    }

    async fn discover_impl(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryBatch, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.dataset() != self.profile.dataset()
            || request.effective_at().is_some()
            || request.max_results() < 1
        {
            return Err(invalid_protocol());
        }
        let retrieved = match self
            .retrieve(&authority, None, request.deadline(), &cancellation)
            .await?
        {
            BoardRetrievalOutcome::Modified(value) => *value,
            BoardRetrievalOutcome::NotModified(_) => return Err(invalid_protocol()),
        };
        let object = source_object(&self.metadata, &self.profile, &request, &retrieved)?;
        *self
            .cached_discovery
            .lock()
            .map_err(|_| invalid_protocol())? = Some(retrieved);
        DiscoveryBatch::try_new(&request, vec![object]).map_err(Into::into)
    }

    fn validate_authority(
        &self,
        authority: &ExtractionAuthority,
    ) -> Result<(), ExtractionSourceError> {
        authority.validate_current()?;
        if authority.metadata() != &self.metadata {
            Err(invalid_protocol())
        } else {
            Ok(())
        }
    }

    fn record_success(&self, outcome: &BoardRetrievalOutcome) -> Result<(), ExtractionSourceError> {
        let mut health = self.health.lock().map_err(|_| invalid_protocol())?;
        health.requests_total = health.requests_total.saturating_add(1);
        match outcome {
            BoardRetrievalOutcome::Modified(value) => {
                health.modified_responses_total = health.modified_responses_total.saturating_add(1);
                health.last_status = Some(200);
                health.last_response_bytes = value.receipt().body_bytes();
                health.last_response_digest = Some(value.receipt().body_digest());
                health.last_received_at = Some(value.receipt().received_at());
                health.last_latency_nanos = value.receipt().latency_nanos();
                health.last_retry_after_present = false;
            }
            BoardRetrievalOutcome::NotModified(value) => {
                health.not_modified_responses_total =
                    health.not_modified_responses_total.saturating_add(1);
                health.last_status = Some(304);
                health.last_response_bytes = 0;
                health.last_response_digest = Some(Sha256::digest([]).into());
                health.last_received_at = Some(value.received_at());
                health.last_latency_nanos = value.latency_nanos();
                health.last_retry_after_present = false;
            }
        }
        Ok(())
    }

    fn record_failure(&self, failure: &BoardFetchFailure) -> Result<(), ExtractionSourceError> {
        if !failure.telemetry.attempted {
            return Ok(());
        }
        let mut health = self.health.lock().map_err(|_| invalid_protocol())?;
        health.requests_total = health.requests_total.saturating_add(1);
        if matches!(failure.telemetry.status, Some(429 | 503)) {
            health.throttled_responses_total = health.throttled_responses_total.saturating_add(1);
        } else {
            health.failed_responses_total = health.failed_responses_total.saturating_add(1);
        }
        apply_telemetry(&mut health, failure.telemetry);
        Ok(())
    }
}

#[cfg(all(feature = "scripted-transport-fixture", debug_assertions))]
impl BoardScriptedTransportFactory {
    /// Constructs the real production source with only its exact HTTP execution scripted.
    ///
    /// The profile must be the frozen rolling H.15 dashboard contract and its metadata must admit
    /// the exact official URL. Discovery, cached extraction, raw-capture material, normalization,
    /// health, and revision behavior remain [`BoardSource`] production behavior.
    pub fn production_source(
        &self,
        metadata: SourceMetadata,
        profile: BoardDatasetProfile,
    ) -> Result<BoardSource, BoardSourceError> {
        validate_one_batch_source_profile(&profile)?;
        let expected =
            BoardDatasetContract::h15_treasury_constant_maturities_rolling_dashboard_csv()
                .map_err(BoardSourceError::Protocol)?;
        if profile.contract().contract_digest() != expected.contract_digest()
            || profile.contract().url() != expected.url()
        {
            return Err(BoardSourceError::InvalidProfile);
        }
        let market_squawk_sources::NetworkAccessPolicy::Allowlisted(policy) =
            metadata.network_policy()
        else {
            return Err(BoardSourceError::InvalidMetadata);
        };
        let bounds = policy.request_bounds();
        let maximum_response_bytes = profile.parse_limits().max_source_bytes().min(
            usize::try_from(bounds.max_response_bytes())
                .map_err(|_| BoardSourceError::InvalidMetadata)?,
        );
        let transport = Arc::new(BoardScriptedProductionTransport::new(
            self.production_queue(),
            maximum_response_bytes,
            std::time::Duration::from_nanos(bounds.total_timeout_nanos()),
        ));
        BoardSource::try_new_with_transport(metadata, profile, transport)
    }
}

impl SourceMetadataProvider for BoardSource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl ExtractionSource for BoardSource {
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
        // Canonical rows may not escape through a trait that cannot also transfer the exact raw
        // provider body needed for MSJ1 sealing. Composition must use `extract_with_evidence`.
        let _ = (authority, request, cancellation);
        Box::pin(async { Err(SourceError::InvalidProtocolState.into()) })
    }
}

fn capture_material(
    metadata: &SourceMetadata,
    dataset: SourceIdentifier,
    receipt: &BoardHttpReceipt,
    bytes: Bytes,
) -> Result<ProviderCaptureMaterial, BoardExtractionError> {
    let capture = receipt
        .try_shared_capture_receipt(metadata, dataset)?
        .ok_or(BoardExtractionError::CaptureBodyTooLarge {
            body_bytes: receipt.body_bytes(),
            max: market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGE_BYTES,
        })?;
    let received_at = DateTime::<Utc>::from_timestamp_nanos(receipt.received_at().unix_nanos());
    let record = market_squawk_platform::RawCaptureRecord::try_new_live(
        deterministic_uuid(b"event", receipt),
        Arc::from(metadata.source_id().as_str()),
        deterministic_uuid(b"connection", receipt),
        Some(0),
        None,
        received_at,
        bytes,
    )?;
    ProviderCaptureMaterial::try_new(capture, vec![record]).map_err(Into::into)
}

fn deterministic_uuid(tag: &[u8], receipt: &BoardHttpReceipt) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/federal-reserve-board-raw-capture-id/v1");
    hash.update((tag.len() as u64).to_be_bytes());
    hash.update(tag);
    hash.update(receipt.request_digest());
    hash.update(receipt.body_digest());
    hash.update(receipt.received_at().unix_nanos().to_be_bytes());
    let digest: [u8; 32] = hash.finalize().into();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[derive(Debug, Error)]
pub enum BoardSourceError {
    /// Source metadata does not authorize the selected no-key official profile and shared budget.
    #[error("Federal Reserve Board source metadata is incompatible")]
    InvalidMetadata,
    /// Dataset profile or structural-artifact binding is invalid.
    #[error("Federal Reserve Board dataset profile is invalid")]
    InvalidProfile,
    /// The selected full-history file cannot fit the source's indivisible one-batch handoff.
    #[error("Federal Reserve Board full history requires partitioned extraction")]
    PartitionedExtractionRequired,
    /// An HTTP validator is malformed, unbounded, or not bound to prior exact bytes.
    #[error("Federal Reserve Board conditional validator is invalid")]
    InvalidValidator,
    /// Bounded transport failed.
    #[error("Federal Reserve Board network request failed")]
    Network,
    /// Response body crossed its effective bound.
    #[error("Federal Reserve Board response body is too large")]
    BodyTooLarge,
    /// Cancellation was requested.
    #[error("Federal Reserve Board request was cancelled")]
    Cancelled,
    /// Exact request deadline elapsed.
    #[error("Federal Reserve Board request deadline elapsed")]
    DeadlineExceeded,
    /// Core parser rejected provider bytes.
    #[error("Federal Reserve Board protocol failed: {0}")]
    Protocol(BoardAdapterError),
    /// Runtime health synchronization failed.
    #[error("Federal Reserve Board source health is unavailable")]
    HealthUnavailable,
    /// Canonical macro mapping could not preserve required evidence.
    #[error("Federal Reserve Board canonical macro mapping failed")]
    CanonicalMapping,
}

/// Fail-closed rich extraction error; canonical rows never escape without exact sealable bytes.
#[derive(Debug, Error)]
pub enum BoardExtractionError {
    /// Shared source/extraction authority rejected the operation.
    #[error("Federal Reserve Board extraction authority failed")]
    Source(#[from] ExtractionSourceError),
    /// The source-neutral capture receipt/material contract rejected exact body evidence.
    #[error("Federal Reserve Board capture material is invalid")]
    Capture(#[from] market_squawk_sources::ProviderCaptureError),
    /// The exact provider body cannot fit one raw journal frame under the shared hard ceiling.
    #[error("Federal Reserve Board body size {body_bytes} exceeds capture-page limit {max}")]
    CaptureBodyTooLarge { body_bytes: u64, max: u64 },
    /// Exact bytes could not become a strict newly captured raw record.
    #[error("Federal Reserve Board raw capture record is invalid")]
    RawCapture(#[from] market_squawk_platform::RawCaptureRecordError),
}

impl From<market_squawk_sources::ExtractionError> for BoardExtractionError {
    fn from(error: market_squawk_sources::ExtractionError) -> Self {
        Self::Source(error.into())
    }
}

struct CanonicalBoardRecord {
    effective: ResearchTemporalCoordinate,
    availability: market_squawk_sources::AvailabilityEvidence,
    revision: SourceIdentifier,
    evidence: ExactPayloadEvidence,
    payload: Bytes,
}

fn canonical_records(
    metadata: &SourceMetadata,
    parsed: &ParsedBoardDataset,
    receipt: &BoardHttpReceipt,
    ingested_at: Timestamp,
) -> Result<Vec<CanonicalBoardRecord>, ExtractionSourceError> {
    let capacity = usize::try_from(parsed.observation_count()).map_err(|_| invalid_protocol())?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(capacity)
        .map_err(|_| invalid_protocol())?;
    for series in parsed.series() {
        for observation in series.observations() {
            records.push(canonical_record(
                metadata,
                parsed,
                series,
                observation.period(),
                observation.value(),
                observation.row_digest(),
                receipt,
                ingested_at,
            )?);
        }
    }
    Ok(records)
}

#[allow(clippy::too_many_arguments)]
fn canonical_record(
    metadata: &SourceMetadata,
    parsed: &ParsedBoardDataset,
    series: &BoardSeries,
    period: &BoardPeriod,
    value: &BoardValue,
    row_digest: [u8; 32],
    receipt: &BoardHttpReceipt,
    ingested_at: Timestamp,
) -> Result<CanonicalBoardRecord, ExtractionSourceError> {
    let revision = identifier(format!(
        "frb-ddp:{}:{}:{}:{}",
        parsed.release().code().to_ascii_lowercase(),
        encode_component(series.series_name()),
        encode_component(period.raw()),
        lower_hex(row_digest)
    ))?;
    let availability = ResearchAvailabilityEvidence::local_first_observed(receipt.received_at());
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: metadata.source_id().clone(),
        instrument_id: None,
        venue_id: None,
        source_identifier: revision.clone(),
        source_timestamp: None,
        received_at: receipt.received_at(),
        ingested_at,
        quality: DataQuality::OfficialDelayed,
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            DigestAlgorithm::Sha256,
            receipt.body_digest(),
        )),
        availability,
    })
    .map_err(|_| invalid_protocol())?;
    let effective = temporal_coordinate(period)?;
    let time = ResearchTime::try_new_with_coordinates(
        effective.clone(),
        None,
        RevisionNumber::new(1).map_err(|_| invalid_protocol())?,
        None,
    )
    .map_err(|_| invalid_protocol())?;
    let context = ResearchContext::new(provenance, time).map_err(|_| invalid_protocol())?;
    let series_id = identifier(format!(
        "federal-reserve-board:{}:{}",
        parsed.release().code().to_ascii_lowercase(),
        encode_component(series.unique_id())
    ))?;
    let unit = identifier(format!(
        "federal-reserve-board-unit:{}:multiplier:{}",
        encode_component(series.unit()),
        encode_component(&series.multiplier().to_string())
    ))?;
    let macro_observation = match value {
        BoardValue::Observed { value, .. } => {
            MacroObservation::new(context, series_id, *value, unit)
        }
        BoardValue::Missing { missing } => MacroObservation::missing(
            context,
            series_id,
            MacroMissingValue::new(
                identifier(missing.marker())?,
                Some(identifier(missing.status())?),
            ),
            unit,
        ),
    };
    let payload = serde_json::to_vec(&ResearchObservation::Macro(macro_observation))
        .map(Bytes::from)
        .map_err(|_| invalid_protocol())?;
    let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&payload).into());
    Ok(CanonicalBoardRecord {
        effective,
        availability: market_squawk_sources::AvailabilityEvidence::LocalFirstObserved {
            observed_at: receipt.received_at(),
        },
        revision,
        evidence: ExactPayloadEvidence::from_content_digest(digest),
        payload,
    })
}

fn temporal_coordinate(
    period: &BoardPeriod,
) -> Result<ResearchTemporalCoordinate, ExtractionSourceError> {
    match period.value() {
        BoardPeriodValue::CalendarDate { date } => {
            Ok(ResearchTemporalCoordinate::calendar_date(*date))
        }
        BoardPeriodValue::Week { year, week } => {
            source_period(period, *year, u16::from(*week), "weekly")
        }
        BoardPeriodValue::Month { year, month } => {
            source_period(period, *year, u16::from(*month), "monthly")
        }
        BoardPeriodValue::Quarter { year, quarter } => {
            source_period(period, *year, u16::from(*quarter), "quarterly")
        }
        BoardPeriodValue::Annual { year } => source_period(period, *year, 1, "annual"),
    }
}

fn source_period(
    period: &BoardPeriod,
    year: u16,
    ordinal: u16,
    frequency: &str,
) -> Result<ResearchTemporalCoordinate, ExtractionSourceError> {
    let period = ResearchPeriod::try_new(
        identifier(format!("federal-reserve-board-{frequency}"))?,
        year,
        NonZeroU16::new(ordinal).ok_or_else(invalid_protocol)?,
        identifier(period.raw())?,
    )
    .map_err(|_| invalid_protocol())?;
    Ok(ResearchTemporalCoordinate::source_period(period))
}

fn source_object(
    metadata: &SourceMetadata,
    profile: &BoardDatasetProfile,
    request: &DiscoveryRequest,
    retrieved: &BoardRetrievedFile,
) -> Result<SourceObject, ExtractionSourceError> {
    let body_digest = retrieved.receipt().body_digest();
    let object_id = SourceIdentifier::try_from(format!(
        "federal-reserve-board-file:{}:{}:{}",
        profile.contract().family().as_str(),
        lower_hex(profile.contract().contract_digest()),
        lower_hex(body_digest)
    ))
    .map_err(|_| invalid_protocol())?;
    let locator = VersionPinnedSourceLocator::new(
        identifier(format!(
            "federal-reserve-board-request:{}",
            lower_hex(profile.contract().request().request_digest())
        ))?,
        identifier(lower_hex(body_digest))?,
    );
    let evidence = ExactPayloadEvidence::with_version_pinned_locator(
        EvidenceDigest::new(DigestAlgorithm::Sha256, body_digest),
        locator,
    );
    let effective = EffectiveInterval::new(retrieved.receipt().received_at(), None)
        .map_err(|_| invalid_protocol())?;
    SourceObject::try_new_with_availability(
        metadata.source_id().clone(),
        metadata.revision().clone(),
        request,
        object_id,
        identifier(media_type(profile.contract().format()))?,
        evidence,
        effective,
        None,
        market_squawk_sources::AvailabilityEvidence::LocalFirstObserved {
            observed_at: retrieved.receipt().received_at(),
        },
        Some(retrieved.receipt().body_bytes()),
    )
    .map_err(Into::into)
}

fn validate_extraction_request(
    metadata: &SourceMetadata,
    profile: &BoardDatasetProfile,
    request: &ExtractionRequest,
) -> Result<(), ExtractionSourceError> {
    if request.object().source_id() != metadata.source_id()
        || request.object().metadata_revision() != metadata.revision()
        || request.object().dataset() != profile.dataset()
        || request.object().media_type().as_str() != media_type(profile.contract().format())
    {
        Err(invalid_protocol())
    } else {
        Ok(())
    }
}

fn object_body_digest(
    object_id: &SourceIdentifier,
    profile: &BoardDatasetProfile,
) -> Result<[u8; 32], ExtractionSourceError> {
    let mut fields = object_id.as_str().split(':');
    let contract_digest = lower_hex(profile.contract().contract_digest());
    if fields.next() != Some("federal-reserve-board-file")
        || fields.next() != Some(profile.contract().family().as_str())
        || fields.next() != Some(contract_digest.as_str())
    {
        return Err(invalid_protocol());
    }
    let digest = parse_lower_hex(fields.next().ok_or_else(invalid_protocol)?)?;
    if fields.next().is_some() {
        Err(invalid_protocol())
    } else {
        Ok(digest)
    }
}

fn validate_structural_artifacts(
    contract: &BoardDatasetContract,
    limits: BoardParseLimits,
    artifacts: &[BoardStructuralArtifact],
) -> Result<(), BoardSourceError> {
    if artifacts.len() > MAX_STRUCTURAL_ARTIFACTS {
        return Err(BoardSourceError::InvalidProfile);
    }
    match contract.format() {
        BoardFileFormat::DdpCsvSeriesColumnV1 | BoardFileFormat::SdmxCompactZipV1
            if artifacts.is_empty() =>
        {
            return Ok(());
        }
        BoardFileFormat::DdpCsvSeriesColumnV1 | BoardFileFormat::SdmxCompactZipV1 => {
            return Err(BoardSourceError::InvalidProfile);
        }
        BoardFileFormat::SdmxCompactXmlV1 => {}
    }
    let package = contract.sdmx().ok_or(BoardSourceError::InvalidProfile)?;
    let expected = package
        .artifacts()
        .iter()
        .filter(|artifact| artifact.kind() != BoardArtifactKind::DataXml)
        .collect::<Vec<_>>();
    if expected.len() != artifacts.len() {
        return Err(BoardSourceError::InvalidProfile);
    }
    let mut names = BTreeSet::new();
    let mut by_name = BTreeMap::new();
    let mut total = 0_u64;
    for artifact in artifacts {
        if !names.insert(artifact.name().to_ascii_lowercase())
            || by_name.insert(artifact.name(), artifact).is_some()
        {
            return Err(BoardSourceError::InvalidProfile);
        }
        let size =
            u64::try_from(artifact.bytes().len()).map_err(|_| BoardSourceError::InvalidProfile)?;
        if size > limits.max_entry_bytes() {
            return Err(BoardSourceError::InvalidProfile);
        }
        total = total
            .checked_add(size)
            .ok_or(BoardSourceError::InvalidProfile)?;
        if total > limits.max_decompressed_bytes() {
            return Err(BoardSourceError::InvalidProfile);
        }
    }
    for contract in expected {
        let artifact = by_name
            .get(contract.name())
            .ok_or(BoardSourceError::InvalidProfile)?;
        if contract.expected_sha256() != Some(artifact.sha256()) {
            return Err(BoardSourceError::InvalidProfile);
        }
    }
    Ok(())
}

fn validate_profile_parse_limits(
    contract: &BoardDatasetContract,
    limits: BoardParseLimits,
) -> Result<(), BoardSourceError> {
    if contract.is_h15_treasury_constant_maturities_rolling_dashboard()
        && limits != BoardParseLimits::h15_treasury_constant_maturities_rolling_dashboard()
    {
        Err(BoardSourceError::InvalidProfile)
    } else {
        Ok(())
    }
}

fn validate_one_batch_source_profile(
    profile: &BoardDatasetProfile,
) -> Result<(), BoardSourceError> {
    if profile
        .contract()
        .is_h15_treasury_constant_maturities_full_history()
    {
        Err(BoardSourceError::PartitionedExtractionRequired)
    } else {
        Ok(())
    }
}

fn validate_rolling_dashboard_shape(parsed: &ParsedBoardDataset) -> Result<(), BoardAdapterError> {
    if parsed.series().len() != 11
        || parsed.observation_count()
            != BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_OBSERVATION_COUNT
        || parsed.series().iter().any(|series| {
            series.observations().len()
                != BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_DATE_COUNT
        })
    {
        Err(BoardAdapterError::CsvSchemaDrift)
    } else {
        Ok(())
    }
}

fn apply_telemetry(health: &mut BoardSourceHealth, telemetry: BoardAttemptTelemetry) {
    health.last_status = telemetry.status;
    health.last_response_bytes = telemetry.body_bytes;
    health.last_response_digest = telemetry.body_digest;
    health.last_received_at =
        (telemetry.received_at.unix_nanos() != 0).then_some(telemetry.received_at);
    health.last_latency_nanos = telemetry.latency_nanos;
    health.last_retry_after_present = telemetry.retry_after_present;
}

fn ensure_deadline(deadline: Timestamp) -> Result<(), ExtractionSourceError> {
    if system_timestamp().map_err(map_adapter_error)? >= deadline {
        Err(ExtractionSourceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_adapter_error(error: BoardSourceError) -> ExtractionSourceError {
    match error {
        BoardSourceError::Cancelled => ExtractionSourceError::Cancelled,
        BoardSourceError::DeadlineExceeded => ExtractionSourceError::DeadlineExceeded,
        BoardSourceError::Network | BoardSourceError::BodyTooLarge => SourceError::Network.into(),
        BoardSourceError::InvalidMetadata
        | BoardSourceError::InvalidProfile
        | BoardSourceError::PartitionedExtractionRequired
        | BoardSourceError::InvalidValidator
        | BoardSourceError::Protocol(_)
        | BoardSourceError::HealthUnavailable
        | BoardSourceError::CanonicalMapping => invalid_protocol(),
    }
}

fn invalid_protocol() -> ExtractionSourceError {
    SourceError::InvalidProtocolState.into()
}
fn media_type(format: BoardFileFormat) -> &'static str {
    match format {
        BoardFileFormat::DdpCsvSeriesColumnV1 => "text/csv",
        BoardFileFormat::SdmxCompactXmlV1 => "application/xml",
        BoardFileFormat::SdmxCompactZipV1 => "application/zip",
    }
}
fn identifier(value: impl AsRef<str>) -> Result<SourceIdentifier, ExtractionSourceError> {
    SourceIdentifier::try_from(value.as_ref()).map_err(|_| invalid_protocol())
}

fn encode_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            output.push(char::from(byte));
        } else {
            output.push('%');
            output.push(char::from(HEX[usize::from(byte >> 4)]));
            output.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    output
}

fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn parse_lower_hex(value: &str) -> Result<[u8; 32], ExtractionSourceError> {
    if value.len() != 64 {
        return Err(invalid_protocol());
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_digit(pair[0])? << 4) | hex_digit(pair[1])?;
    }
    Ok(output)
}

fn hex_digit(value: u8) -> Result<u8, ExtractionSourceError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(invalid_protocol()),
    }
}
