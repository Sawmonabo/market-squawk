//! Registered FRED/ALFRED acquisition, seal-first handoff, and exact read composition.
//!
//! The activated research runtime remains the sole owner of the credential-bearing source and
//! registry/shared-rate authority. This leaf validates and acquires one complete provider page
//! chain, seals every exact metadata/page request graph through [`ResearchService`], and retains
//! the one-use physical authorities needed by provider publication. It also closes the
//! post-publication handoff to the existing manifest-pinned FRED operation.
//!
//! Adapter-authored native lineage and the exact observation-page mapping remain attached through
//! raw sealing, immutable publication, catalog reconstruction, and the existing FRED point-in-time
//! operation. No application layer re-encodes provider semantics or reconstructs page placement.

use std::{sync::Arc, time::Instant};

use market_squawk_adapter_fred::{FredSource, MAX_FRED_SERIES_METADATA_REVISIONS};
use market_squawk_data::{
    AnalyticalGeneration, AnalyticalReadError, DatasetId, DatasetManifestRef,
    GenerationOwnedProviderCaptureEvidence, IngestError, IngestIdentity, IngestPrecommitAuthority,
    ManifestObject, PersistedProviderCaptureBindingEvidence, PersistedProviderCaptureBindingRow,
    ProviderMacroPlanChunkInput, ProviderMacroPlanPublicationInput,
    ProviderMacroPlanPublicationReceipt, ProviderMacroPlanSemantics, RightsDecisionInput,
    SourceOperation, extraction_provider_payload_digest,
};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, SourceId, SourceIdentifier,
};
use market_squawk_services::{RequestContext, ServiceError};
use market_squawk_sources::{
    ExtractionBatch, ExtractionRevisionPlan, FRED_ALFRED_API_SURFACE_ID, ProviderCaptureError,
    ProviderCaptureTerminalDisposition, ProviderNativeLineageBatch,
    ProviderNativeLineageImplementation, ProviderWholeCaptureToken, SealedProviderCaptureBinding,
    SourceMetadata,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    DomainLifecycle, PreparedExtraction, ProductionResearchIngestCoordinator,
    ProviderOperationDiagnostic, ProviderOperationPhase, ResearchIngestCompositionError,
    ResearchRevisionPlanError, await_extraction_diagnostic, await_publication_diagnostic,
    ensure_operation_live, operation_deadline, system_timestamp, wall_deadline,
};
use crate::provider_activation::{FredPointInTimeReadCapability, FredPointInTimeReadError};
use crate::{ResearchService, ResearchServiceError};

const FRED_PROVIDER_ID: &str = "fred";
const FRED_NATIVE_IMPLEMENTATION: &str = "fred_alfred_series_observations_v1";
const FRED_CAPTURE_COMPONENTS: usize = 2;
const FRED_METADATA_CAPTURE_PAGE_ORDINAL: usize = 0;
const FRED_OBSERVATION_CAPTURE_PAGE_ORDINAL: usize = 1;
const FRED_MACRO_SEMANTICS_SCHEMA: &str = "fred-alfred-page-semantics-v1";
const FRED_PLAN_COMPLETION_DOMAIN: &[u8] = b"market-squawk/fred-complete-page-plan/v1";
const FRED_PAGE_CANDIDATE_DOMAIN: &[u8] = b"market-squawk/fred-page-candidate/v1";
const FRED_PLAN_INGEST_DOMAIN: &[u8] = b"market-squawk/fred-plan-ingest/v1";

/// Complete sealed FRED/ALFRED dataset awaiting the adapter's exact native-lineage handoff.
#[derive(Debug)]
pub(crate) struct FredSealedDatasetPublication {
    profile: SourceIdentifier,
    source_id: SourceId,
    provider_dataset: SourceIdentifier,
    analytical_dataset: DatasetId,
    provider_row_count: u64,
    canonical_row_count: u64,
    pages: Box<[FredSealedPagePublication]>,
    publication_lease: Arc<dyn IngestPrecommitAuthority>,
}

impl FredSealedDatasetPublication {
    /// Returns the exact activated built-in profile used for acquisition.
    pub(crate) const fn profile(&self) -> &SourceIdentifier {
        &self.profile
    }

    /// Returns the provider request identity retained by every page.
    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the storage-safe analytical dataset selected by the FRED adapter.
    pub(crate) const fn analytical_dataset(&self) -> &DatasetId {
        &self.analytical_dataset
    }

    /// Returns the exact provider-declared row count consumed by the complete page chain.
    pub(crate) const fn provider_row_count(&self) -> u64 {
        self.provider_row_count
    }

    /// Returns the exact number of individually sealed provider pages.
    pub(crate) fn page_count(&self) -> usize {
        self.pages.len()
    }

    /// Returns immutable evidence for every sealed page in provider offset order.
    pub(crate) fn pages(&self) -> &[FredSealedPagePublication] {
        &self.pages
    }

    /// Freezes the exact cardinality required from the final immutable generation.
    pub(crate) fn generation_expectation(&self) -> FredPublishedGenerationExpectation {
        FredPublishedGenerationExpectation {
            provider_dataset: self.provider_dataset.clone(),
            analytical_dataset: self.analytical_dataset.clone(),
            provider_row_count: self.provider_row_count,
            row_count: self.canonical_row_count,
            object_count: self.pages.len(),
            source_id: self.source_id.clone(),
        }
    }
}

/// One canonical FRED page bound to a physically sealed metadata/observations request graph.
#[derive(Debug)]
pub(crate) struct FredSealedPagePublication {
    object_id: SourceIdentifier,
    payload_digest: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    parts: FredSealedPagePublicationParts,
}

impl FredSealedPagePublication {
    /// Returns the exact offset/page/content identity selected during complete discovery.
    pub(crate) const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }

    /// Returns the exact provider observation-page payload identity.
    pub(crate) const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns the digest binding both raw provider responses to their immutable physical seal.
    pub(crate) const fn sealed_capture_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_capture_receipt_digest
    }

    /// Returns the canonical record count aligned to the provider page.
    pub(crate) fn record_count(&self) -> usize {
        self.parts.batch.records().len()
    }

    /// Consumes the exact inputs needed by the shared provider publication transition.
    pub(crate) fn into_parts(self) -> FredSealedPagePublicationParts {
        self.parts
    }
}

/// One-use publication inputs after provider acquisition and application-owned raw sealing.
///
/// `row_capture_page_ordinals` is all `1`: page zero is the exact series-metadata response and
/// page one is the exact observations response that authored every canonical row.
#[derive(Debug)]
pub(crate) struct FredSealedPagePublicationParts {
    pub(crate) source: SourceMetadata,
    pub(crate) rights: RightsDecisionInput,
    pub(crate) analytical_dataset: DatasetId,
    pub(crate) batch: ExtractionBatch,
    pub(crate) revisions: ExtractionRevisionPlan,
    pub(crate) sealed_capture: ProviderWholeCaptureToken,
    pub(crate) native_lineage: ProviderNativeLineageBatch,
    pub(crate) row_capture_page_ordinals: Vec<u16>,
}

/// Exact final-generation cardinality minted only from a complete sealed page chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FredPublishedGenerationExpectation {
    provider_dataset: SourceIdentifier,
    analytical_dataset: DatasetId,
    provider_row_count: u64,
    row_count: u64,
    object_count: usize,
    source_id: SourceId,
}

/// Exact generation/capability pair consumed by `FredLatestKnownOperation` composition.
#[derive(Debug)]
pub(crate) struct FredPublishedGenerationHandoff {
    capability: FredPointInTimeReadCapability,
    generation: AnalyticalGeneration,
    manifest: DatasetManifestRef,
    restart: FredMacroRestartSelector,
}

impl FredPublishedGenerationHandoff {
    /// Resolves and reopens the latest durable generation for the configured provider dataset.
    ///
    /// Absence is returned separately. Once a generation exists, every catalog binding, native
    /// page coordinate, exact raw graph, and manifest object must reconstruct one complete FRED
    /// page chain; mismatch never falls through to a different generation or reacquisition.
    pub(crate) fn try_reopen_latest(
        research: &ResearchService,
        provider_dataset: SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<Self>, FredProductionPublicationError> {
        let capability = FredPointInTimeReadCapability::try_new(
            research.analytical_reader(),
            provider_dataset.clone(),
        )?;
        let Some(generation) = research.analytical_reader().latest(
            capability.analytical_dataset(),
            deadline,
            cancellation,
        )?
        else {
            return Ok(None);
        };
        Self::try_reopen_existing(research, provider_dataset, generation).map(Some)
    }

    fn try_reopen_existing(
        research: &ResearchService,
        provider_dataset: SourceIdentifier,
        generation: AnalyticalGeneration,
    ) -> Result<Self, FredProductionPublicationError> {
        let capability = FredPointInTimeReadCapability::try_new(
            research.analytical_reader(),
            provider_dataset.clone(),
        )?;
        let manifest = capability.try_pin_generation(&generation)?;
        let owned = research
            .analytical()
            .generation_owned_provider_capture_evidence(
                &manifest,
                research.provider_capture_store().as_ref(),
            )?;
        if owned.pinned().manifest() != &manifest
            || owned.source_id() != generation.source_id()
            || owned.objects().is_empty()
        {
            return Err(FredProductionPublicationError::RestartVerificationMismatch);
        }
        let binding_count = owned.objects().iter().try_fold(0_usize, |count, object| {
            if object.inputs().is_empty() {
                return None;
            }
            count.checked_add(object.inputs().len())
        });
        let Some(binding_count) = binding_count.filter(|count| *count > 0) else {
            return Err(FredProductionPublicationError::RestartVerificationMismatch);
        };
        let mut selected_bindings = Vec::new();
        selected_bindings
            .try_reserve_exact(binding_count)
            .map_err(|_error| FredProductionPublicationError::Capacity)?;
        let mut published_objects = Vec::new();
        published_objects
            .try_reserve_exact(owned.objects().len())
            .map_err(|_error| FredProductionPublicationError::Capacity)?;
        let mut row_count = 0_u64;
        for (publication_ordinal, object) in owned.objects().iter().enumerate() {
            if object.publication_ordinal() != publication_ordinal
                || owned
                    .pinned()
                    .objects()
                    .get(object.generation_object_ordinal())
                    != Some(object.object())
            {
                return Err(FredProductionPublicationError::RestartVerificationMismatch);
            }
            let mut object_rows = 0_u64;
            for input in object.inputs() {
                let coordinate = restored_fred_binding_coordinate(
                    input.binding(),
                    generation.source_id(),
                    &provider_dataset,
                )?;
                object_rows = object_rows
                    .checked_add(
                        u64::try_from(coordinate.record_count)
                            .map_err(|_error| FredProductionPublicationError::Capacity)?,
                    )
                    .ok_or(FredProductionPublicationError::Capacity)?;
                selected_bindings.push(coordinate);
            }
            if object.object().object().row_count() != object_rows {
                return Err(FredProductionPublicationError::RestartVerificationMismatch);
            }
            row_count = row_count
                .checked_add(object_rows)
                .ok_or(FredProductionPublicationError::Capacity)?;
            published_objects.push(object.object().object().clone());
        }
        let provider_row_count = selected_bindings
            .first()
            .and_then(|binding| u64::try_from(binding.provider_page.total()).ok())
            .ok_or(FredProductionPublicationError::RestartVerificationMismatch)?;
        let expectation = FredPublishedGenerationExpectation {
            provider_dataset,
            analytical_dataset: capability.analytical_dataset().clone(),
            provider_row_count,
            row_count,
            object_count: selected_bindings.len(),
            source_id: generation.source_id().clone(),
        };
        let restart = FredMacroRestartSelector::try_reopen(
            research,
            manifest.clone(),
            &expectation,
            selected_bindings,
            published_objects,
        )?;
        Ok(Self {
            capability,
            generation,
            manifest,
            restart,
        })
    }

    /// Reopens and pins a complete committed generation for the existing FRED application read.
    ///
    /// This accepts only the exact dataset and row/object cardinality minted by the complete
    /// seal-first acquisition. The analytical reader reopens the immutable manifest, and the
    /// FRED capability independently validates the retained source and dataset owner before the
    /// handoff can reach operation composition.
    pub(crate) fn try_from_published(
        research: &ResearchService,
        expectation: FredPublishedGenerationExpectation,
        selected_bindings: Vec<FredPublishedBindingCoordinate>,
        receipt: &ProviderMacroPlanPublicationReceipt,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, FredProductionPublicationError> {
        if cancellation.is_cancelled() {
            return Err(FredProductionPublicationError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(FredProductionPublicationError::DeadlineExceeded);
        }
        let manifest = receipt.manifest();
        if manifest.dataset_id() != &expectation.analytical_dataset
            || usize::from(receipt.total_chunks()) != expectation.object_count
            || receipt.total_rows() != expectation.row_count
            || selected_bindings.len() != expectation.object_count
            || selected_bindings.iter().try_fold(0_u64, |rows, binding| {
                u64::try_from(binding.record_count)
                    .ok()
                    .and_then(|binding_rows| rows.checked_add(binding_rows))
            }) != Some(expectation.row_count)
        {
            return Err(FredProductionPublicationError::IncompletePublication);
        }
        validate_fred_binding_coordinates_in_provider_order(
            &selected_bindings,
            expectation.provider_row_count,
            expectation.row_count,
        )?;
        let reopened = research
            .analytical()
            .verify_provider_macro_plan_restart(&receipt.restart_selector())?;
        if reopened.manifest() != manifest {
            return Err(FredProductionPublicationError::RestartVerificationMismatch);
        }
        let generation = research
            .analytical_reader()
            .exact(manifest, deadline, cancellation)?;
        let handoff =
            Self::try_reopen_existing(research, expectation.provider_dataset.clone(), generation)?;
        if &handoff.manifest != manifest
            || handoff.generation.source_id() != &expectation.source_id
            || handoff.restart.row_count != expectation.row_count
            || handoff.restart.bindings.as_ref() != selected_bindings.as_slice()
        {
            return Err(FredProductionPublicationError::RestartVerificationMismatch);
        }
        Ok(handoff)
    }

    /// Returns the exact immutable manifest pinned for the existing typed operation.
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the exact raw/native catalog selector revalidated after immutable publication.
    pub(crate) const fn restart_selector(&self) -> &FredMacroRestartSelector {
        &self.restart
    }

    /// Consumes the handoff into the exact arguments expected by operation composition.
    pub(crate) fn into_operation_parts(
        self,
    ) -> (FredPointInTimeReadCapability, AnalyticalGeneration) {
        (self.capability, self.generation)
    }
}

fn restored_fred_binding_coordinate(
    evidence: &PersistedProviderCaptureBindingEvidence,
    source_id: &SourceId,
    provider_dataset: &SourceIdentifier,
) -> Result<FredPublishedBindingCoordinate, FredProductionPublicationError> {
    let capture = evidence.capture();
    let pages = capture.pages();
    let native = evidence.native_lineage();
    if capture.source_id() != source_id
        || capture.dataset() != provider_dataset
        || pages.len() != FRED_CAPTURE_COMPONENTS
        || native.implementation() != FRED_NATIVE_IMPLEMENTATION
        || native.version() == 0
        || native.fingerprint().bytes() == [0; 32]
        || native.batch_digest().bytes() == [0; 32]
        || native.row_count() != evidence.record_count()
        || evidence.record_count() == 0
    {
        return Err(FredProductionPublicationError::RestartVerificationMismatch);
    }
    let sidecar: RestoredFredNativeBatchV1 = serde_json::from_slice(
        native
            .batch_sidecar_semantic_payload()
            .ok_or(FredProductionPublicationError::RestartVerificationMismatch)?,
    )
    .map_err(|_error| FredProductionPublicationError::RestartVerificationMismatch)?;
    sidecar.validate(provider_dataset, evidence.record_count(), evidence.rows())?;
    let object_id = SourceIdentifier::try_from(format!(
        "fred-page-v2:{}:{}:{}:{}:{}:{}:{}",
        sidecar.page.offset,
        sidecar.page.limit,
        sidecar.page.returned,
        sidecar.page.count,
        u8::from(sidecar.page.terminal),
        lower_hex(
            pages[FRED_OBSERVATION_CAPTURE_PAGE_ORDINAL]
                .body_digest()
                .bytes()
        ),
        lower_hex(
            pages[FRED_METADATA_CAPTURE_PAGE_ORDINAL]
                .body_digest()
                .bytes()
        ),
    ))
    .map_err(|_error| FredProductionPublicationError::RestartVerificationMismatch)?;
    let provider_page = FredSource::page_object_identity(&object_id)?;
    let coordinate = FredPublishedBindingCoordinate {
        provider_page,
        binding_digest: evidence.binding_digest(),
        sealed_capture_receipt_digest: evidence.sealed_capture_receipt_digest(),
        extraction_content_identity: evidence.extraction_content_identity(),
        native_schema_version: native.version(),
        native_schema_fingerprint: native.fingerprint(),
        native_batch_digest: native.batch_digest(),
        record_count: evidence.record_count(),
    };
    if !valid_fred_persisted_binding(evidence, &coordinate, source_id, provider_dataset, None) {
        return Err(FredProductionPublicationError::RestartVerificationMismatch);
    }
    Ok(coordinate)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoredFredNativeBatchV1 {
    version: u16,
    family: String,
    namespace: String,
    provider_dataset: SourceIdentifier,
    response_mode: RestoredFredResponseModeV1,
    series_revisions: Vec<RestoredFredSeriesV1>,
    semantic_rows: usize,
    page: RestoredFredPageV1,
}

impl RestoredFredNativeBatchV1 {
    fn validate(
        &self,
        provider_dataset: &SourceIdentifier,
        record_count: usize,
        rows: &[PersistedProviderCaptureBindingRow],
    ) -> Result<(), FredProductionPublicationError> {
        let expected_namespace = provider_dataset
            .as_str()
            .split(':')
            .next()
            .filter(|namespace| matches!(*namespace, "fred" | "alfred"))
            .ok_or(FredProductionPublicationError::RestartVerificationMismatch)?;
        let expected_series = FredSource::series_identifier(provider_dataset)?;
        let (expected_realtime_start, expected_realtime_end) =
            FredSource::dataset_realtime_interval(provider_dataset)?;
        let consumed = self
            .page
            .offset
            .checked_add(self.page.returned)
            .ok_or(FredProductionPublicationError::Capacity)?;
        if self.version != 1
            || self.family != "fred_alfred_series_observations"
            || self.namespace != expected_namespace
            || &self.provider_dataset != provider_dataset
            || self.response_mode.output_type != 1
            || self.response_mode.file_type != "json"
            || self.response_mode.order_by != "observation_date"
            || self.response_mode.sort_order != "asc"
            || !restored_metadata_revisions_are_unambiguous(
                &self.series_revisions,
                &expected_series,
            )
            || self.page.realtime_start != expected_realtime_start
            || self.page.realtime_end != expected_realtime_end
            || self.page.observation_start > self.page.observation_end
            || self.page.units.is_empty()
            || self.page.count == 0
            || self.page.limit == 0
            || self.page.returned == 0
            || self.semantic_rows != record_count
            || self.page.returned > self.page.limit
            || consumed > self.page.count
            || self.page.terminal != (consumed == self.page.count)
            || self.page.next_offset != (!self.page.terminal).then_some(consumed)
            || rows.len() != record_count
        {
            return Err(FredProductionPublicationError::RestartVerificationMismatch);
        }
        let mut restored_rows = Vec::new();
        restored_rows
            .try_reserve_exact(rows.len())
            .map_err(|_error| FredProductionPublicationError::Capacity)?;
        for row in rows {
            restored_rows.push(
                serde_json::from_slice::<RestoredFredNativeRowV1>(row.native_semantic_payload())
                    .map_err(|_error| {
                        FredProductionPublicationError::RestartVerificationMismatch
                    })?,
            );
        }
        for (ordinal, restored) in restored_rows.iter().enumerate() {
            let metadata = self
                .series_revisions
                .get(usize::from(restored.metadata_revision_ordinal))
                .ok_or(FredProductionPublicationError::RestartVerificationMismatch)?;
            let clipped_start = restored
                .provider_realtime_start
                .max(self.page.realtime_start);
            let clipped_end = restored.provider_realtime_end.min(self.page.realtime_end);
            if restored.realtime_start > restored.realtime_end
                || restored.provider_realtime_start > restored.provider_realtime_end
                || clipped_start > clipped_end
                || restored.raw_value.is_empty()
                || restored.realtime_start < self.page.realtime_start
                || restored.realtime_end > self.page.realtime_end
                || restored.realtime_start < metadata.realtime_start
                || restored.realtime_end > metadata.realtime_end
                || restored.realtime_start < restored.provider_realtime_start
                || restored.realtime_end > restored.provider_realtime_end
                || restored.realtime_end != clipped_end.min(metadata.realtime_end)
                || restored.value.is_none() != (restored.missing_marker.as_deref() == Some("."))
            {
                return Err(FredProductionPublicationError::RestartVerificationMismatch);
            }
            let previous = ordinal
                .checked_sub(1)
                .and_then(|index| restored_rows.get(index));
            if previous.is_none_or(|row| !same_restored_provider_observation(row, restored)) {
                if restored.realtime_start != clipped_start {
                    return Err(FredProductionPublicationError::RestartVerificationMismatch);
                }
            } else if previous.is_none_or(|row| {
                row.realtime_end.days_since_unix_epoch().checked_add(1)
                    != Some(restored.realtime_start.days_since_unix_epoch())
            }) {
                return Err(FredProductionPublicationError::RestartVerificationMismatch);
            }
            let next = restored_rows.get(ordinal.saturating_add(1));
            if next.is_none_or(|row| !same_restored_provider_observation(row, restored))
                && restored.realtime_end != clipped_end
            {
                return Err(FredProductionPublicationError::RestartVerificationMismatch);
            }
        }
        Ok(())
    }
}

fn restored_metadata_revisions_are_unambiguous(
    revisions: &[RestoredFredSeriesV1],
    expected_series: &SourceIdentifier,
) -> bool {
    if revisions.is_empty() || revisions.len() > MAX_FRED_SERIES_METADATA_REVISIONS {
        return false;
    }
    let mut previous_end = None;
    for revision in revisions {
        if &revision.id != expected_series
            || revision.realtime_start > revision.realtime_end
            || revision.observation_start > revision.observation_end
            || revision.title.is_empty()
            || revision.frequency.is_empty()
            || revision.frequency_short.is_empty()
            || revision.units.is_empty()
            || revision.units_short.is_empty()
            || revision.seasonal_adjustment.is_empty()
            || revision.seasonal_adjustment_short.is_empty()
            || revision.last_updated.is_empty()
            || previous_end.is_some_and(|end: CalendarDate| revision.realtime_start <= end)
        {
            return false;
        }
        previous_end = Some(revision.realtime_end);
    }
    true
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoredFredResponseModeV1 {
    output_type: u8,
    file_type: String,
    order_by: String,
    sort_order: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoredFredSeriesV1 {
    id: SourceIdentifier,
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
    title: String,
    observation_start: CalendarDate,
    observation_end: CalendarDate,
    frequency: String,
    frequency_short: String,
    units: String,
    units_short: String,
    seasonal_adjustment: String,
    seasonal_adjustment_short: String,
    last_updated: String,
    #[serde(rename = "popularity")]
    _popularity: u32,
    #[serde(rename = "notes")]
    _notes: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoredFredNativeRowV1 {
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
    provider_realtime_start: CalendarDate,
    provider_realtime_end: CalendarDate,
    observation_date: CalendarDate,
    raw_value: String,
    value: Option<Value>,
    missing_marker: Option<String>,
    metadata_revision_ordinal: u16,
}

fn same_restored_provider_observation(
    left: &RestoredFredNativeRowV1,
    right: &RestoredFredNativeRowV1,
) -> bool {
    left.provider_realtime_start == right.provider_realtime_start
        && left.provider_realtime_end == right.provider_realtime_end
        && left.observation_date == right.observation_date
        && left.raw_value == right.raw_value
        && left.value == right.value
        && left.missing_marker == right.missing_marker
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoredFredPageV1 {
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
    observation_start: CalendarDate,
    observation_end: CalendarDate,
    units: String,
    count: usize,
    offset: usize,
    limit: usize,
    next_offset: Option<usize>,
    terminal: bool,
    returned: usize,
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

fn same_fred_plan_rights_scope(
    retained: &RightsDecisionInput,
    candidate: &RightsDecisionInput,
) -> bool {
    retained.source_id == candidate.source_id
        && retained.basis == candidate.basis
        && retained.authorization_evidence == candidate.authorization_evidence
        && retained.authorization_expires_at == candidate.authorization_expires_at
        && retained.permitted_operations == candidate.permitted_operations
}

fn fred_page_candidate_digest(
    provider_dataset: &SourceIdentifier,
    page: market_squawk_adapter_fred::FredPageObjectIdentity,
    binding_digest: EvidenceDigest,
    native_sidecar_digest: EvidenceDigest,
) -> Result<EvidenceDigest, FredProductionPublicationError> {
    let mut digest = Sha256::new();
    digest.update(FRED_PAGE_CANDIDATE_DOMAIN);
    fred_hash_text(&mut digest, provider_dataset.as_str())?;
    digest.update(
        u64::try_from(page.offset())
            .map_err(|_error| FredProductionPublicationError::Capacity)?
            .to_be_bytes(),
    );
    digest.update(
        u64::try_from(page.limit())
            .map_err(|_error| FredProductionPublicationError::Capacity)?
            .to_be_bytes(),
    );
    digest.update(
        u64::try_from(page.returned())
            .map_err(|_error| FredProductionPublicationError::Capacity)?
            .to_be_bytes(),
    );
    digest.update(
        u64::try_from(page.total())
            .map_err(|_error| FredProductionPublicationError::Capacity)?
            .to_be_bytes(),
    );
    digest.update([u8::from(page.terminal())]);
    digest.update(page.page_digest());
    digest.update(page.metadata_digest());
    fred_hash_evidence(&mut digest, binding_digest)?;
    fred_hash_evidence(&mut digest, native_sidecar_digest)?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn fred_plan_completion_digest(
    expectation: &FredPublishedGenerationExpectation,
    source_generation_digest: EvidenceDigest,
    bindings: &[FredPublishedBindingCoordinate],
    candidate_digests: &[EvidenceDigest],
) -> Result<EvidenceDigest, FredProductionPublicationError> {
    if bindings.is_empty() || bindings.len() != candidate_digests.len() {
        return Err(FredProductionPublicationError::IncompletePublication);
    }
    let mut digest = Sha256::new();
    digest.update(FRED_PLAN_COMPLETION_DOMAIN);
    fred_hash_text(&mut digest, expectation.source_id.as_str())?;
    fred_hash_text(&mut digest, expectation.provider_dataset.as_str())?;
    fred_hash_text(&mut digest, expectation.analytical_dataset.as_str())?;
    digest.update(expectation.row_count.to_be_bytes());
    digest.update(expectation.provider_row_count.to_be_bytes());
    digest.update(
        u16::try_from(expectation.object_count)
            .map_err(|_error| FredProductionPublicationError::Capacity)?
            .to_be_bytes(),
    );
    fred_hash_evidence(&mut digest, source_generation_digest)?;
    for (ordinal, (binding, candidate_digest)) in bindings.iter().zip(candidate_digests).enumerate()
    {
        digest.update(
            u16::try_from(ordinal)
                .map_err(|_error| FredProductionPublicationError::Capacity)?
                .to_be_bytes(),
        );
        fred_hash_evidence(&mut digest, *candidate_digest)?;
        fred_hash_evidence(&mut digest, binding.binding_digest)?;
        fred_hash_evidence(&mut digest, binding.sealed_capture_receipt_digest)?;
        fred_hash_evidence(&mut digest, binding.extraction_content_identity)?;
        fred_hash_evidence(&mut digest, binding.native_schema_fingerprint)?;
        fred_hash_evidence(&mut digest, binding.native_batch_digest)?;
        digest.update(binding.native_schema_version.to_be_bytes());
        digest.update(
            u64::try_from(binding.record_count)
                .map_err(|_error| FredProductionPublicationError::Capacity)?
                .to_be_bytes(),
        );
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn fred_plan_ingest_identity(
    analytical_dataset: &DatasetId,
    provider_dataset: &SourceIdentifier,
    source_generation_digest: EvidenceDigest,
    publication_digest: EvidenceDigest,
) -> Result<String, FredProductionPublicationError> {
    let mut digest = Sha256::new();
    digest.update(FRED_PLAN_INGEST_DOMAIN);
    fred_hash_text(&mut digest, analytical_dataset.as_str())?;
    fred_hash_text(&mut digest, provider_dataset.as_str())?;
    fred_hash_evidence(&mut digest, source_generation_digest)?;
    fred_hash_evidence(&mut digest, publication_digest)?;
    Ok(format!(
        "fred-plan-v1:{}",
        lower_hex(digest.finalize().into())
    ))
}

fn fred_hash_text(digest: &mut Sha256, value: &str) -> Result<(), FredProductionPublicationError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_error| FredProductionPublicationError::Capacity)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

fn fred_hash_evidence(
    digest: &mut Sha256,
    evidence: EvidenceDigest,
) -> Result<(), FredProductionPublicationError> {
    if evidence.algorithm() != DigestAlgorithm::Sha256 || evidence.bytes() == [0; 32] {
        return Err(FredProductionPublicationError::IncompletePublication);
    }
    digest.update(evidence.bytes());
    Ok(())
}

/// Live-retained identity of one just-published FRED page binding.
#[derive(Clone, Debug, Eq, PartialEq)]
struct FredPublishedBindingCoordinate {
    provider_page: market_squawk_adapter_fred::FredPageObjectIdentity,
    binding_digest: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    extraction_content_identity: EvidenceDigest,
    native_schema_version: u16,
    native_schema_fingerprint: EvidenceDigest,
    native_batch_digest: EvidenceDigest,
    record_count: usize,
}

/// Exact immutable FRED generation and every raw/native input needed after restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FredMacroRestartSelector {
    manifest: DatasetManifestRef,
    bindings: Box<[FredPublishedBindingCoordinate]>,
    objects: Box<[ManifestObject]>,
    source_id: SourceId,
    metadata_revision: SourceIdentifier,
    provider_dataset: SourceIdentifier,
    row_count: u64,
}

impl FredMacroRestartSelector {
    fn try_reopen(
        research: &ResearchService,
        manifest: DatasetManifestRef,
        expectation: &FredPublishedGenerationExpectation,
        selected_bindings: Vec<FredPublishedBindingCoordinate>,
        objects: Vec<ManifestObject>,
    ) -> Result<Self, FredProductionPublicationError> {
        if manifest.dataset_id() != &expectation.analytical_dataset
            || expectation.object_count == 0
            || expectation.row_count == 0
            || selected_bindings.len() != expectation.object_count
            || objects.is_empty()
        {
            return Err(FredProductionPublicationError::RestartVerificationMismatch);
        }
        validate_fred_binding_coordinates_in_provider_order(
            &selected_bindings,
            expectation.provider_row_count,
            expectation.row_count,
        )?;
        let owned = research
            .analytical()
            .generation_owned_provider_capture_evidence(
                &manifest,
                research.provider_capture_store().as_ref(),
            )?;
        if owned.pinned().manifest() != &manifest || owned.source_id() != &expectation.source_id {
            return Err(FredProductionPublicationError::RestartVerificationMismatch);
        }
        let verified = validate_fred_owned_generation(
            &owned,
            &expectation.source_id,
            &expectation.provider_dataset,
            &objects,
            &selected_bindings,
            None,
            |_evidence| Ok(()),
        )?;
        if verified.row_count != expectation.row_count {
            return Err(FredProductionPublicationError::RestartVerificationMismatch);
        }
        let selector = Self {
            manifest,
            bindings: selected_bindings.into_boxed_slice(),
            objects: objects.into_boxed_slice(),
            source_id: expectation.source_id.clone(),
            metadata_revision: verified.metadata_revision,
            provider_dataset: expectation.provider_dataset.clone(),
            row_count: verified.row_count,
        };
        selector.verify(research)?;
        Ok(selector)
    }

    /// Returns the exact immutable analytical generation.
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Reopens and verifies every ordered FRED raw/native binding from the durable catalog.
    pub(crate) fn verify(
        &self,
        research: &ResearchService,
    ) -> Result<Box<[PersistedProviderCaptureBindingEvidence]>, FredProductionPublicationError>
    {
        let owned = research
            .analytical()
            .generation_owned_provider_capture_evidence(
                &self.manifest,
                research.provider_capture_store().as_ref(),
            )?;
        if owned.pinned().manifest() != &self.manifest || owned.source_id() != &self.source_id {
            return Err(FredProductionPublicationError::RestartVerificationMismatch);
        }
        let mut retained = Vec::new();
        retained
            .try_reserve_exact(self.bindings.len())
            .map_err(|_error| FredProductionPublicationError::Capacity)?;
        let verified = validate_fred_owned_generation(
            &owned,
            &self.source_id,
            &self.provider_dataset,
            &self.objects,
            &self.bindings,
            Some(&self.metadata_revision),
            |evidence| {
                retained.push(evidence.clone());
                Ok(())
            },
        )?;
        if verified.row_count != self.row_count
            || verified.metadata_revision != self.metadata_revision
        {
            return Err(FredProductionPublicationError::RestartVerificationMismatch);
        }
        Ok(retained.into_boxed_slice())
    }
}

struct VerifiedFredOwnedGeneration {
    metadata_revision: SourceIdentifier,
    row_count: u64,
}

fn validate_fred_owned_generation(
    owned: &GenerationOwnedProviderCaptureEvidence,
    expected_source_id: &SourceId,
    expected_provider_dataset: &SourceIdentifier,
    expected_objects: &[ManifestObject],
    selected_bindings: &[FredPublishedBindingCoordinate],
    expected_metadata_revision: Option<&SourceIdentifier>,
    mut retain_binding: impl FnMut(
        &PersistedProviderCaptureBindingEvidence,
    ) -> Result<(), FredProductionPublicationError>,
) -> Result<VerifiedFredOwnedGeneration, FredProductionPublicationError> {
    if owned.source_id() != expected_source_id
        || owned.objects().is_empty()
        || owned.objects().len() != expected_objects.len()
        || selected_bindings.is_empty()
    {
        return Err(FredProductionPublicationError::RestartVerificationMismatch);
    }
    let mut metadata_revision = expected_metadata_revision.cloned();
    let mut input_ordinal = 0_usize;
    let mut row_count = 0_u64;
    for (publication_ordinal, (object, expected_object)) in
        owned.objects().iter().zip(expected_objects).enumerate()
    {
        if object.publication_ordinal() != publication_ordinal
            || object.object().object() != expected_object
            || object.inputs().is_empty()
            || owned
                .pinned()
                .objects()
                .get(object.generation_object_ordinal())
                != Some(object.object())
        {
            return Err(FredProductionPublicationError::RestartVerificationMismatch);
        }
        let mut object_rows = 0_u64;
        for (object_input_ordinal, input) in object.inputs().iter().enumerate() {
            let selected = selected_bindings
                .get(input_ordinal)
                .ok_or(FredProductionPublicationError::RestartVerificationMismatch)?;
            let evidence = input.binding();
            if input.input_ordinal() != input_ordinal
                || input.object_input_ordinal() != object_input_ordinal
                || evidence.binding_digest() != selected.binding_digest
                || !valid_fred_persisted_binding(
                    evidence,
                    selected,
                    expected_source_id,
                    expected_provider_dataset,
                    metadata_revision.as_ref(),
                )
            {
                return Err(FredProductionPublicationError::RestartVerificationMismatch);
            }
            let evidence_revision = evidence
                .capture()
                .metadata_revision()
                .as_source_identifier()
                .clone();
            metadata_revision.get_or_insert(evidence_revision);
            let evidence_rows = u64::try_from(evidence.record_count())
                .map_err(|_error| FredProductionPublicationError::Capacity)?;
            object_rows = object_rows
                .checked_add(evidence_rows)
                .ok_or(FredProductionPublicationError::Capacity)?;
            retain_binding(evidence)?;
            input_ordinal = input_ordinal
                .checked_add(1)
                .ok_or(FredProductionPublicationError::Capacity)?;
        }
        if expected_object.row_count() != object_rows {
            return Err(FredProductionPublicationError::RestartVerificationMismatch);
        }
        row_count = row_count
            .checked_add(object_rows)
            .ok_or(FredProductionPublicationError::Capacity)?;
    }
    if input_ordinal != selected_bindings.len() {
        return Err(FredProductionPublicationError::RestartVerificationMismatch);
    }
    Ok(VerifiedFredOwnedGeneration {
        metadata_revision: metadata_revision
            .ok_or(FredProductionPublicationError::RestartVerificationMismatch)?,
        row_count,
    })
}

fn validate_fred_binding_coordinates_in_provider_order(
    bindings: &[FredPublishedBindingCoordinate],
    expected_provider_row_count: u64,
    expected_canonical_row_count: u64,
) -> Result<(), FredProductionPublicationError> {
    let expected_provider_total = usize::try_from(expected_provider_row_count)
        .map_err(|_error| FredProductionPublicationError::Capacity)?;
    let mut expected_offset = 0_usize;
    let mut canonical_rows = 0_u64;
    let mut expected_limit = None;
    let mut expected_metadata_digest = None;
    for (ordinal, binding) in bindings.iter().enumerate() {
        let page = binding.provider_page;
        if page.offset() != expected_offset
            || page.total() != expected_provider_total
            || page.terminal() != (ordinal + 1 == bindings.len())
            || expected_limit.is_some_and(|limit| limit != page.limit())
            || expected_metadata_digest.is_some_and(|digest| digest != page.metadata_digest())
            || bindings[..ordinal]
                .iter()
                .any(|prior| prior.binding_digest == binding.binding_digest)
        {
            return Err(FredProductionPublicationError::RestartVerificationMismatch);
        }
        expected_limit = Some(page.limit());
        expected_metadata_digest = Some(page.metadata_digest());
        canonical_rows = canonical_rows
            .checked_add(
                u64::try_from(binding.record_count)
                    .map_err(|_error| FredProductionPublicationError::Capacity)?,
            )
            .ok_or(FredProductionPublicationError::Capacity)?;
        expected_offset = expected_offset
            .checked_add(page.returned())
            .ok_or(FredProductionPublicationError::Capacity)?;
    }
    if expected_offset != expected_provider_total || canonical_rows != expected_canonical_row_count
    {
        return Err(FredProductionPublicationError::RestartVerificationMismatch);
    }
    Ok(())
}

fn valid_fred_persisted_binding(
    evidence: &PersistedProviderCaptureBindingEvidence,
    selected: &FredPublishedBindingCoordinate,
    source_id: &SourceId,
    provider_dataset: &SourceIdentifier,
    metadata_revision: Option<&SourceIdentifier>,
) -> bool {
    let capture = evidence.capture();
    let pages = capture.pages();
    let components = capture.request_graph_components();
    evidence.binding_digest() == selected.binding_digest
        && capture.source_id() == source_id
        && capture.dataset() == provider_dataset
        && metadata_revision
            .is_none_or(|revision| capture.metadata_revision().as_source_identifier() == revision)
        && capture.terminal() == ProviderCaptureTerminalDisposition::CompleteRequestGraph
        && capture.semantic_binding().is_none()
        && pages.len() == FRED_CAPTURE_COMPONENTS
        && components.len() == FRED_CAPTURE_COMPONENTS
        && pages[FRED_METADATA_CAPTURE_PAGE_ORDINAL]
            .body_digest()
            .bytes()
            == selected.provider_page.metadata_digest()
        && pages[FRED_OBSERVATION_CAPTURE_PAGE_ORDINAL]
            .body_digest()
            .bytes()
            == selected.provider_page.page_digest()
        && components.iter().enumerate().all(|(ordinal, component)| {
            usize::from(component.ordinal()) == ordinal
                && usize::from(component.first_page_ordinal()) == ordinal
                && component.page_count().get() == 1
                && component.terminal() == ProviderCaptureTerminalDisposition::StandaloneResponse
        })
        && evidence.scope() == "whole"
        && evidence.layout() == "whole_single_segment"
        && evidence.component_ordinal().is_none()
        && evidence.sealed_capture_receipt_digest() == selected.sealed_capture_receipt_digest
        && evidence.extraction_content_identity() == selected.extraction_content_identity
        && evidence.record_count() == selected.record_count
        && evidence.record_count() == evidence.rows().len()
        && evidence.native_lineage().implementation() == FRED_NATIVE_IMPLEMENTATION
        && evidence.native_lineage().version() == selected.native_schema_version
        && evidence.native_lineage().fingerprint() == selected.native_schema_fingerprint
        && evidence.native_lineage().batch_digest() == selected.native_batch_digest
        && evidence.native_lineage().row_count() == evidence.record_count()
        && evidence.rows().iter().enumerate().all(|(ordinal, row)| {
            row.canonical_row_ordinal() == u32::try_from(ordinal).unwrap_or(u32::MAX)
                && usize::from(row.capture_page_ordinal()) == FRED_OBSERVATION_CAPTURE_PAGE_ORDINAL
        })
}

impl ProductionResearchIngestCoordinator {
    /// Acquires and seals one complete FRED/ALFRED observations dataset from an active runtime.
    ///
    /// The source is obtained only through the coordinator's current registered runtime. Its
    /// registry-minted extraction authority carries the shared provider-rate authority into every
    /// FRED metadata, discovery, and persistence request. All exact raw response graphs are
    /// physically sealed before this method returns.
    pub(crate) async fn acquire_and_seal_fred_dataset(
        &self,
        profile: &SourceIdentifier,
        provider_dataset: &SourceIdentifier,
        context: &RequestContext,
    ) -> Result<FredSealedDatasetPublication, FredProductionPublicationError> {
        let runtime_diagnostic = |error: ServiceError| {
            ProviderOperationDiagnostic::from_service(ProviderOperationPhase::Runtime, error)
        };
        let _call = DomainLifecycle::enter(&self.lifecycle, context).map_err(runtime_diagnostic)?;
        let operation_deadline = operation_deadline(context, self.limits.operation_duration)
            .map_err(runtime_diagnostic)?;
        let operation = self.lifecycle.shutdown_token().child_token();
        let prepared = self.prepare(profile).map_err(runtime_diagnostic)?;
        validate_fred_runtime(profile, provider_dataset, &prepared)?;

        prepared
            .rights
            .validate_at(system_timestamp().map_err(runtime_diagnostic)?)?;
        let expected_subject = FredSource::series_identifier(provider_dataset)?;
        let subject = prepared
            .source
            .rights_subject(provider_dataset)
            .map_err(FredProductionPublicationError::RevisionPlan)?
            .ok_or(FredProductionPublicationError::InvalidRuntimeBinding)?;
        if subject != expected_subject {
            return Err(FredProductionPublicationError::InvalidRuntimeBinding);
        }
        prepared.rights.validate_subject(Some(&subject))?;
        let publication_lease: Arc<dyn IngestPrecommitAuthority> =
            Arc::new(prepared.admission.acquire_publication_lease().await?);
        publication_lease.validate_precommit()?;

        let wall_deadline =
            wall_deadline(operation_deadline, &operation).map_err(runtime_diagnostic)?;
        let discovery_request = market_squawk_sources::DiscoveryRequest::try_new(
            provider_dataset.clone(),
            None,
            self.limits.discovery_objects,
            wall_deadline,
        )?;
        let discovery = await_extraction_diagnostic(
            prepared.source.discover_managed_diagnostic(
                prepared.authority.clone(),
                discovery_request,
                operation.clone(),
            ),
            context,
            &operation,
            &prepared.admission,
            operation_deadline,
            ProviderOperationPhase::Discovery,
        )
        .await?;
        if discovery.capture_material.is_some() {
            return Err(FredProductionPublicationError::InvalidRuntimeBinding);
        }
        let chain =
            validate_complete_page_chain(&discovery.batch, &prepared.metadata, provider_dataset)?;

        let analytical_identifier = FredSource::analytical_dataset_identifier(provider_dataset)?;
        let analytical_dataset = DatasetId::try_from(analytical_identifier.as_str())
            .map_err(|_error| FredProductionPublicationError::InvalidRuntimeBinding)?;
        let mut pages = Vec::new();
        pages
            .try_reserve_exact(chain.objects.len())
            .map_err(|_error| FredProductionPublicationError::Capacity)?;

        for (object, identity) in chain.objects.into_iter().zip(chain.identities) {
            ensure_operation_live(operation_deadline, &operation).map_err(|error| {
                ProviderOperationDiagnostic::from_service(ProviderOperationPhase::Extraction, error)
            })?;
            prepared
                .admission
                .ensure_live()
                .map_err(|_error| FredProductionPublicationError::RuntimeRevoked)?;
            let extracted = self
                .extract_prepared_object_diagnostic(
                    PreparedExtraction {
                        source: Arc::clone(&prepared.source),
                        metadata: prepared.metadata.clone(),
                        rights: prepared.rights.clone(),
                        authority: prepared.authority.clone(),
                        admission: prepared.admission.clone(),
                    },
                    object.clone(),
                    None,
                    context,
                    &operation,
                    operation_deadline,
                    wall_deadline,
                )
                .await?;
            let super::AuthorizedExtraction {
                metadata: source,
                publication,
                company_identity,
                revisions,
                analytical_dataset: page_analytical_dataset,
                payload_digest: extraction_payload_digest,
                rights,
                admission: _,
            } = extracted;
            let revisions =
                revisions.ok_or(FredProductionPublicationError::InvalidRuntimeBinding)?;
            let super::ManagedPendingProviderPublication {
                batch,
                capture_material,
                provider_native,
            } = publication
                .into_pending_provider()
                .ok_or(FredProductionPublicationError::InvalidRuntimeBinding)?;
            let super::ManagedProviderNativePublication {
                native_lineage,
                row_capture_page_ordinals,
            } = provider_native.ok_or(FredProductionPublicationError::InvalidRuntimeBinding)?;
            if company_identity.is_some()
                || source != prepared.metadata
                || page_analytical_dataset != analytical_dataset
                || extraction_payload_digest != extraction_provider_payload_digest(&batch)
                || rights.payload_digest != extraction_payload_digest
                || batch.request().object().object_id() != object.object_id()
                || batch.request().object().evidence().content_digest()
                    != object.evidence().content_digest()
                || batch.records().len() < identity.returned()
                || revisions.len() != batch.records().len()
                || !revisions.native_lineage_required()
                || native_lineage.schema().implementation()
                    != ProviderNativeLineageImplementation::FredAlfredSeriesObservationsV1
                || native_lineage.validate(&batch).is_err()
                || row_capture_page_ordinals.len() != batch.records().len()
                || row_capture_page_ordinals
                    .iter()
                    .any(|ordinal| usize::from(*ordinal) != FRED_OBSERVATION_CAPTURE_PAGE_ORDINAL)
            {
                return Err(FredProductionPublicationError::InvalidRuntimeBinding);
            }

            let (expectation, seal_request) = capture_material.into_whole_seal_parts();
            let sealed = await_publication_diagnostic(
                self.research
                    .seal_provider_capture(seal_request, &operation, operation_deadline),
                context,
                &operation,
                &prepared.admission,
                operation_deadline,
                ProviderOperationPhase::RawSeal,
            )
            .await?;
            let sealed_capture = expectation.try_rejoin(sealed)?.try_into_whole()?;
            validate_sealed_fred_graph(&sealed_capture, &source, provider_dataset, identity)?;
            let sealed_capture_receipt_digest = sealed_capture.persisted_receipt().receipt_digest();
            pages.push(FredSealedPagePublication {
                object_id: object.object_id().clone(),
                payload_digest: object.evidence().content_digest(),
                sealed_capture_receipt_digest,
                parts: FredSealedPagePublicationParts {
                    source,
                    rights,
                    analytical_dataset: page_analytical_dataset,
                    batch,
                    revisions,
                    sealed_capture,
                    native_lineage,
                    row_capture_page_ordinals,
                },
            });
        }

        ensure_operation_live(operation_deadline, &operation).map_err(|error| {
            ProviderOperationDiagnostic::from_service(ProviderOperationPhase::Extraction, error)
        })?;
        prepared
            .admission
            .ensure_live()
            .map_err(|_error| FredProductionPublicationError::RuntimeRevoked)?;
        let observed_rows = pages.iter().try_fold(0_u64, |rows, page| {
            u64::try_from(page.record_count())
                .ok()
                .and_then(|page_rows| rows.checked_add(page_rows))
        });
        let canonical_row_count = observed_rows
            .filter(|rows| *rows != 0)
            .ok_or(FredProductionPublicationError::IncompletePageChain)?;
        if chain.provider_row_count == 0 || canonical_row_count < chain.provider_row_count {
            return Err(FredProductionPublicationError::IncompletePageChain);
        }
        Ok(FredSealedDatasetPublication {
            profile: profile.clone(),
            source_id: prepared.metadata.source_id().clone(),
            provider_dataset: provider_dataset.clone(),
            analytical_dataset,
            provider_row_count: chain.provider_row_count,
            canonical_row_count,
            pages: pages.into_boxed_slice(),
            publication_lease,
        })
    }

    /// Publishes every sealed FRED page in provider order and proves raw/native restart evidence.
    pub(crate) async fn publish_sealed_fred_dataset(
        &self,
        sealed: FredSealedDatasetPublication,
        context: &RequestContext,
    ) -> Result<FredPublishedGenerationHandoff, FredProductionPublicationError> {
        let _call = DomainLifecycle::enter(&self.lifecycle, context)?;
        let deadline = operation_deadline(context, self.limits.operation_duration)?;
        let operation = self.lifecycle.shutdown_token().child_token();
        let expectation = sealed.generation_expectation();
        let FredSealedDatasetPublication {
            profile,
            source_id,
            provider_dataset,
            analytical_dataset,
            provider_row_count,
            canonical_row_count,
            pages,
            publication_lease,
        } = sealed;
        if profile.as_str() != FRED_ALFRED_API_SURFACE_ID
            || source_id != expectation.source_id
            || provider_dataset != expectation.provider_dataset
            || analytical_dataset != expectation.analytical_dataset
            || provider_row_count != expectation.provider_row_count
            || canonical_row_count != expectation.row_count
            || pages.len() != expectation.object_count
            || pages.is_empty()
        {
            return Err(FredProductionPublicationError::IncompletePublication);
        }
        publication_lease.validate_precommit()?;
        let mut published_rows = 0_u64;
        let mut selected_bindings = Vec::new();
        selected_bindings
            .try_reserve_exact(pages.len())
            .map_err(|_error| FredProductionPublicationError::Capacity)?;
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(pages.len())
            .map_err(|_error| FredProductionPublicationError::Capacity)?;
        let mut candidate_digests = Vec::new();
        candidate_digests
            .try_reserve_exact(pages.len())
            .map_err(|_error| FredProductionPublicationError::Capacity)?;
        let expected_provider_total = usize::try_from(provider_row_count)
            .map_err(|_error| FredProductionPublicationError::Capacity)?;
        let page_count = pages.len();
        let total_chunks =
            u16::try_from(page_count).map_err(|_error| FredProductionPublicationError::Capacity)?;
        let mut expected_provider_offset = 0_usize;
        let mut expected_provider_limit = None;
        let mut publication_source = None;
        let mut publication_rights: Option<RightsDecisionInput> = None;
        let mut source_generation_digest = None;
        for (page_ordinal, page) in pages.into_iter().enumerate() {
            ensure_operation_live(deadline, &operation)?;
            publication_lease.validate_precommit()?;
            let FredSealedPagePublication {
                object_id,
                payload_digest,
                sealed_capture_receipt_digest,
                parts,
            } = page;
            let FredSealedPagePublicationParts {
                source,
                rights,
                analytical_dataset: page_analytical_dataset,
                batch,
                revisions,
                sealed_capture,
                native_lineage,
                row_capture_page_ordinals,
            } = parts;
            let provider_page = FredSource::page_object_identity(&object_id)?;
            if source.source_id() != &source_id
                || page_analytical_dataset != analytical_dataset
                || batch.request().object().object_id() != &object_id
                || batch.request().object().dataset() != &provider_dataset
                || batch.request().object().evidence().content_digest() != payload_digest
                || sealed_capture.persisted_receipt().receipt_digest()
                    != sealed_capture_receipt_digest
                || &rights.source_id != &source_id
                || rights.payload_digest != extraction_provider_payload_digest(&batch)
                || revisions.len() != batch.records().len()
                || revisions.is_locally_observed()
                || !revisions.native_lineage_required()
                || native_lineage.schema().implementation()
                    != ProviderNativeLineageImplementation::FredAlfredSeriesObservationsV1
                || native_lineage.validate(&batch).is_err()
                || row_capture_page_ordinals.len() != batch.records().len()
                || row_capture_page_ordinals
                    .iter()
                    .any(|ordinal| usize::from(*ordinal) != FRED_OBSERVATION_CAPTURE_PAGE_ORDINAL)
                || provider_page.offset() != expected_provider_offset
                || batch.records().len() < provider_page.returned()
                || provider_page.total() != expected_provider_total
                || provider_page.terminal() != (page_ordinal + 1 == page_count)
                || expected_provider_limit.is_some_and(|limit| limit != provider_page.limit())
            {
                return Err(FredProductionPublicationError::IncompletePublication);
            }
            let page_rows = u64::try_from(batch.records().len())
                .map_err(|_error| FredProductionPublicationError::Capacity)?;
            let page_source_generation_digest = source
                .revision_evidence()
                .payload_evidence()
                .content_digest();
            if source_generation_digest
                .is_some_and(|expected| expected != page_source_generation_digest)
                || publication_source
                    .as_ref()
                    .is_some_and(|expected| expected != &source)
                || publication_rights
                    .as_ref()
                    .is_some_and(|expected| !same_fred_plan_rights_scope(expected, &rights))
            {
                return Err(FredProductionPublicationError::IncompletePublication);
            }
            source_generation_digest.get_or_insert(page_source_generation_digest);
            publication_source.get_or_insert_with(|| source.clone());
            match publication_rights.as_mut() {
                Some(retained) if rights.retrieved_at > retained.retrieved_at => {
                    retained.retrieved_at = rights.retrieved_at;
                }
                Some(_) => {}
                None => publication_rights = Some(rights.clone()),
            }
            let binding = SealedProviderCaptureBinding::try_whole(
                sealed_capture,
                batch,
                native_lineage,
                row_capture_page_ordinals,
            )?;
            binding.validate()?;
            if binding.capture_evidence().source_id() != &source_id
                || binding.capture_evidence().dataset() != &provider_dataset
                || binding.sealed_capture_receipt_digest() != sealed_capture_receipt_digest
            {
                return Err(FredProductionPublicationError::IncompletePublication);
            }
            let coordinate = FredPublishedBindingCoordinate {
                provider_page,
                binding_digest: binding.evidence_digest().evidence(),
                sealed_capture_receipt_digest: binding.sealed_capture_receipt_digest(),
                extraction_content_identity: binding.content_identity().digest(),
                native_schema_version: binding.native_lineage().schema().version(),
                native_schema_fingerprint: binding.native_lineage().schema().fingerprint(),
                native_batch_digest: binding.native_lineage().batch_digest(),
                record_count: binding.record_count(),
            };
            let native_sidecar = binding
                .native_lineage()
                .batch_sidecar()
                .ok_or(FredProductionPublicationError::IncompletePublication)?;
            let candidate_digest = fred_page_candidate_digest(
                &provider_dataset,
                provider_page,
                binding.evidence_digest().evidence(),
                native_sidecar.semantic_payload_digest(),
            )?;
            let semantics = ProviderMacroPlanSemantics::try_new(
                SourceIdentifier::try_from(FRED_MACRO_SEMANTICS_SCHEMA)
                    .map_err(|_error| FredProductionPublicationError::IncompletePublication)?,
                binding.native_lineage().schema().fingerprint(),
                native_sidecar.semantic_payload_digest(),
                native_sidecar
                    .semantic_payload()
                    .to_vec()
                    .into_boxed_slice(),
            )?;
            chunks.push(ProviderMacroPlanChunkInput::try_new(
                u16::try_from(page_ordinal)
                    .map_err(|_error| FredProductionPublicationError::Capacity)?,
                total_chunks,
                candidate_digest,
                page_source_generation_digest,
                semantics,
                binding,
                revisions,
            )?);
            publication_lease.validate_precommit()?;
            published_rows = published_rows
                .checked_add(page_rows)
                .ok_or(FredProductionPublicationError::Capacity)?;
            expected_provider_limit = Some(provider_page.limit());
            expected_provider_offset = expected_provider_offset
                .checked_add(provider_page.returned())
                .ok_or(FredProductionPublicationError::Capacity)?;
            selected_bindings.push(coordinate);
            candidate_digests.push(candidate_digest);
        }
        if published_rows != canonical_row_count
            || expected_provider_offset != expected_provider_total
            || selected_bindings.len() != expectation.object_count
            || chunks.len() != expectation.object_count
        {
            return Err(FredProductionPublicationError::IncompletePublication);
        }
        validate_fred_binding_coordinates_in_provider_order(
            &selected_bindings,
            expectation.provider_row_count,
            expectation.row_count,
        )?;
        let source_generation_digest = source_generation_digest
            .ok_or(FredProductionPublicationError::IncompletePublication)?;
        let completion_digest = fred_plan_completion_digest(
            &expectation,
            source_generation_digest,
            &selected_bindings,
            &candidate_digests,
        )?;
        let input = ProviderMacroPlanPublicationInput::try_new(
            analytical_dataset.clone(),
            completion_digest,
            canonical_row_count,
            chunks,
        )?;
        if input.source_id() != &source_id
            || input.provider_dataset() != &provider_dataset
            || input.source_generation_digest() != source_generation_digest
            || input.total_chunks() != total_chunks
            || input.total_rows() != canonical_row_count
        {
            return Err(FredProductionPublicationError::IncompletePublication);
        }
        let publication_digest = input.publication_digest();
        let source =
            publication_source.ok_or(FredProductionPublicationError::IncompletePublication)?;
        let mut rights =
            publication_rights.ok_or(FredProductionPublicationError::IncompletePublication)?;
        rights.payload_digest = publication_digest;
        let identity = IngestIdentity::try_new(
            source_id.clone(),
            publication_digest,
            SourceOperation::Persist,
            fred_plan_ingest_identity(
                &analytical_dataset,
                &provider_dataset,
                source_generation_digest,
                publication_digest,
            )?,
        )?;
        let registered_at = rights.retrieved_at;
        let reservation = self.research.analytical().reserve_source_ingest(
            &source,
            registered_at,
            rights,
            &identity,
            &operation,
        );
        tokio::pin!(reservation);
        let reservation = tokio::select! {
            biased;
            () = context.cancellation().cancelled() => {
                operation.cancel();
                return Err(FredProductionPublicationError::Cancelled);
            }
            () = tokio::time::sleep_until(deadline.into()) => {
                operation.cancel();
                return Err(FredProductionPublicationError::DeadlineExceeded);
            }
            () = operation.cancelled() => {
                return Err(FredProductionPublicationError::RuntimeRevoked);
            }
            result = reservation.as_mut() => result?,
        };
        publication_lease.validate_precommit()?;
        let pending = self
            .research
            .analytical()
            .prepare_provider_macro_plan_publication(reservation, input)?;
        let publication = pending.commit(
            self.research.analytical(),
            operation.clone(),
            publication_lease,
        );
        tokio::pin!(publication);
        let receipt = tokio::select! {
            biased;
            () = context.cancellation().cancelled() => {
                operation.cancel();
                return Err(FredProductionPublicationError::Cancelled);
            }
            () = tokio::time::sleep_until(deadline.into()) => {
                operation.cancel();
                return Err(FredProductionPublicationError::DeadlineExceeded);
            }
            () = operation.cancelled() => {
                return Err(FredProductionPublicationError::RuntimeRevoked);
            }
            result = publication.as_mut() => result?,
        };
        FredPublishedGenerationHandoff::try_from_published(
            self.research.as_ref(),
            expectation,
            selected_bindings,
            &receipt,
            deadline,
            &operation,
        )
    }
}

struct FredCompletePageChain {
    provider_row_count: u64,
    objects: Vec<market_squawk_sources::SourceObject>,
    identities: Vec<market_squawk_adapter_fred::FredPageObjectIdentity>,
}

fn validate_fred_runtime(
    profile: &SourceIdentifier,
    provider_dataset: &SourceIdentifier,
    prepared: &PreparedExtraction,
) -> Result<(), FredProductionPublicationError> {
    if profile.as_str() != FRED_ALFRED_API_SURFACE_ID
        || prepared.metadata.provider().as_str() != FRED_PROVIDER_ID
        || prepared.source.discovery_dataset_identifier().is_some()
        || FredSource::analytical_dataset_identifier(provider_dataset).is_err()
    {
        return Err(FredProductionPublicationError::InvalidRuntimeBinding);
    }
    Ok(())
}

fn validate_complete_page_chain(
    discovery: &market_squawk_sources::DiscoveryBatch,
    source: &SourceMetadata,
    provider_dataset: &SourceIdentifier,
) -> Result<FredCompletePageChain, FredProductionPublicationError> {
    if discovery.request().dataset() != provider_dataset || discovery.objects().is_empty() {
        return Err(FredProductionPublicationError::IncompletePageChain);
    }
    let mut objects = Vec::new();
    let mut identities = Vec::new();
    objects
        .try_reserve_exact(discovery.objects().len())
        .map_err(|_error| FredProductionPublicationError::Capacity)?;
    identities
        .try_reserve_exact(discovery.objects().len())
        .map_err(|_error| FredProductionPublicationError::Capacity)?;
    let mut expected_offset = 0_usize;
    let mut expected_limit = None;
    let mut expected_total = None;
    let mut expected_metadata_digest = None;

    for (ordinal, object) in discovery.objects().iter().enumerate() {
        let identity = FredSource::page_object_identity(object.object_id())?;
        let final_object = ordinal + 1 == discovery.objects().len();
        if object.source_id() != source.source_id()
            || object.metadata_revision() != source.revision()
            || object.dataset() != provider_dataset
            || identity.offset() != expected_offset
            || identity.page_digest() != object.evidence().content_digest().bytes()
            || expected_limit.is_some_and(|limit| limit != identity.limit())
            || expected_total.is_some_and(|total| total != identity.total())
            || expected_metadata_digest.is_some_and(|digest| digest != identity.metadata_digest())
            || identity.terminal() != final_object
        {
            return Err(FredProductionPublicationError::IncompletePageChain);
        }
        expected_limit = Some(identity.limit());
        expected_total = Some(identity.total());
        expected_metadata_digest = Some(identity.metadata_digest());
        expected_offset = expected_offset
            .checked_add(identity.returned())
            .ok_or(FredProductionPublicationError::IncompletePageChain)?;
        objects.push(object.clone());
        identities.push(identity);
    }
    let provider_row_count = expected_total
        .filter(|total| *total == expected_offset)
        .and_then(|total| u64::try_from(total).ok())
        .ok_or(FredProductionPublicationError::IncompletePageChain)?;
    Ok(FredCompletePageChain {
        provider_row_count,
        objects,
        identities,
    })
}

fn validate_sealed_fred_graph(
    token: &ProviderWholeCaptureToken,
    source: &SourceMetadata,
    provider_dataset: &SourceIdentifier,
    identity: market_squawk_adapter_fred::FredPageObjectIdentity,
) -> Result<(), FredProductionPublicationError> {
    let capture = token.persisted_receipt().capture();
    let pages = capture.pages();
    let components = capture.request_graph_components();
    if capture.source_id() != source.source_id()
        || capture.metadata_revision() != source.revision()
        || capture.dataset() != provider_dataset
        || capture.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
        || capture.semantic_binding().is_some()
        || pages.len() != FRED_CAPTURE_COMPONENTS
        || components.len() != FRED_CAPTURE_COMPONENTS
        || pages[FRED_METADATA_CAPTURE_PAGE_ORDINAL]
            .body_digest()
            .bytes()
            != identity.metadata_digest()
        || pages[FRED_OBSERVATION_CAPTURE_PAGE_ORDINAL]
            .body_digest()
            .bytes()
            != identity.page_digest()
        || components.iter().enumerate().any(|(ordinal, component)| {
            usize::from(component.ordinal()) != ordinal
                || usize::from(component.first_page_ordinal()) != ordinal
                || component.page_count().get() != 1
                || component.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        })
    {
        return Err(FredProductionPublicationError::InvalidSealedCapture);
    }
    Ok(())
}

/// Failure before complete FRED native publication or exact operation handoff.
#[derive(Debug, Error)]
pub(crate) enum FredProductionPublicationError {
    /// A shared application operation rejected the current request or lifecycle state.
    #[error("FRED/ALFRED application operation failed")]
    Service(#[from] ServiceError),
    /// Internal payload-free detail identifies the failed runtime/provider/storage phase.
    #[error("FRED/ALFRED provider operation failed")]
    ProviderOperation(#[from] ProviderOperationDiagnostic),
    /// Provider-generation publication authority could not be retained through commit.
    #[error("FRED/ALFRED publication authority failed")]
    Composition(#[from] ResearchIngestCompositionError),
    /// The FRED adapter rejected the provider dataset or page identity.
    #[error("FRED/ALFRED adapter identity is invalid")]
    Adapter(#[from] market_squawk_adapter_fred::FredSourceError),
    /// A discovery or extraction request exceeded the shared bounded contract.
    #[error("FRED/ALFRED extraction request is invalid")]
    Extraction(#[from] market_squawk_sources::ExtractionError),
    /// Adapter revision evidence could not be aligned to the canonical page.
    #[error("FRED/ALFRED revision evidence is invalid")]
    RevisionPlan(ResearchRevisionPlanError),
    /// Raw response material could not be rejoined to its exact physical seal.
    #[error("FRED/ALFRED sealed capture is invalid")]
    Capture(#[from] ProviderCaptureError),
    /// The application-owned physical sealer or analytical service failed.
    #[error("FRED/ALFRED research service failed")]
    Research(#[from] ResearchServiceError),
    /// Durable raw/native catalog reconstruction failed closed.
    #[error("FRED/ALFRED durable provider evidence failed")]
    Ingest(#[from] IngestError),
    /// The complete-plan persist identity could not be admitted by the retained rights scope.
    #[error("FRED/ALFRED complete-plan rights identity is invalid")]
    Rights(#[from] market_squawk_data::RightsError),
    /// The immutable analytical generation could not be reopened exactly.
    #[error("FRED/ALFRED analytical generation read failed")]
    AnalyticalRead(#[from] AnalyticalReadError),
    /// The FRED typed read capability rejected the dataset/source/generation binding.
    #[error("FRED/ALFRED point-in-time capability binding is invalid")]
    PointInTimeRead(#[from] FredPointInTimeReadError),
    /// The registered runtime does not represent the code-owned FRED/ALFRED surface.
    #[error("registered runtime is not the exact activated FRED/ALFRED source")]
    InvalidRuntimeBinding,
    /// Discovery did not return one complete, consistent provider page chain.
    #[error("FRED/ALFRED provider page chain is incomplete")]
    IncompletePageChain,
    /// A sealed request graph did not retain the exact metadata and observations responses.
    #[error("FRED/ALFRED sealed metadata/page graph is invalid")]
    InvalidSealedCapture,
    /// A provider runtime replacement or revocation invalidated this operation.
    #[error("FRED/ALFRED provider runtime was revoked")]
    RuntimeRevoked,
    /// Bounded application allocation failed.
    #[error("FRED/ALFRED application capacity is unavailable")]
    Capacity,
    /// The final generation is not the complete sealed provider dataset.
    #[error("FRED/ALFRED final immutable publication is incomplete")]
    IncompletePublication,
    /// Restart reopening changed exact immutable generation identity or cardinality.
    #[error("FRED/ALFRED restart verification changed generation identity")]
    RestartVerificationMismatch,
    /// The caller cancelled the exact generation handoff.
    #[error("FRED/ALFRED operation was cancelled")]
    Cancelled,
    /// The exact generation handoff exceeded its deadline.
    #[error("FRED/ALFRED operation deadline exceeded")]
    DeadlineExceeded,
}
