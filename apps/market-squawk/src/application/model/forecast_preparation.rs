//! Authority-derived forecast preparation and one-use job-admission receipts.

use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fmt,
    mem::size_of,
    num::{NonZeroU16, NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use market_squawk_data::{DatasetManifestRef, ForecastDatasetEvidenceFence, Sha256Digest};
use market_squawk_domain::{CalendarDate, DataQuality, InstrumentId, ModelId, SourceId, Timestamp};
use market_squawk_modeling::{
    BundleId, ForecastHorizon, ForecastObservedPoint, ForecastOutputBinding, ForecastRequest,
    ModelBundle, ModelFeatureValue, ModelFormat, ModelInput, ModelMetadata, ModelOutputSemantics,
    TrainingDatasetIdentity,
};
use market_squawk_services::{RequestOrigin, ServiceDomain, ToolDescriptor, TypedToolRequest};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ForecastModelCalibrationState, ForecastModelEvidenceProjection, ForecastModelEvidenceState,
    forecast::{ForecastProductIdentity, ForecastProductTarget, GENERATE_FORECAST},
    forecast_model_evidence_projection, forecast_model_evidence_projection_for_horizon,
    runtime::{
        ProductionModelRuntime, ProductionModelRuntimeError, RetainedForecastRuntime,
        RetainedRuntimeBackup,
    },
};
use crate::application::lifecycle::WorkspaceRuntimeIdentity;

const MAXIMUM_RECEIPTS: usize = 256;
const MAXIMUM_RECEIPT_LIFETIME: Duration = Duration::from_secs(15 * 60);
const MAXIMUM_RETAINED_REQUEST_BYTES: usize = 256 * 1024 * 1024;
const MAXIMUM_SINGLE_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_FORECAST_VALIDITY_NANOS: u64 = 30 * 24 * 60 * 60 * 1_000_000_000;
const RECEIPT_ID_ATTEMPTS: usize = 16;
const MAXIMUM_EVIDENCE_PAIRS: usize = 4_096;
const MAXIMUM_EVIDENCE_CATALOG_BYTES: usize = 256 * 1024 * 1024;

/// Resource ceilings for one process-owned forecast preparation authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForecastPreparationLimits {
    maximum_receipts: NonZeroUsize,
    receipt_lifetime: Duration,
    maximum_retained_request_bytes: NonZeroUsize,
}

impl ForecastPreparationLimits {
    /// Constructs bounded receipt retention.
    pub fn try_new(
        maximum_receipts: NonZeroUsize,
        receipt_lifetime: Duration,
        maximum_retained_request_bytes: NonZeroUsize,
    ) -> Result<Self, ForecastPreparationError> {
        if maximum_receipts.get() > MAXIMUM_RECEIPTS
            || receipt_lifetime.is_zero()
            || receipt_lifetime > MAXIMUM_RECEIPT_LIFETIME
            || maximum_retained_request_bytes.get() > MAXIMUM_RETAINED_REQUEST_BYTES
        {
            return Err(ForecastPreparationError::InvalidLimits);
        }
        Ok(Self {
            maximum_receipts,
            receipt_lifetime,
            maximum_retained_request_bytes,
        })
    }

    /// Returns bounded production defaults.
    pub fn standard() -> Result<Self, ForecastPreparationError> {
        Self::try_new(
            NonZeroUsize::new(64).ok_or(ForecastPreparationError::InvalidLimits)?,
            Duration::from_secs(10 * 60),
            NonZeroUsize::new(64 * 1024 * 1024).ok_or(ForecastPreparationError::InvalidLimits)?,
        )
    }
}

/// Exact admitted model metadata supplied to the analytical evidence owner.
#[derive(Clone, Debug)]
pub(crate) struct ForecastModelRequirement {
    runtime_generation_sha256: Sha256Digest,
    bundle: Arc<ModelBundle>,
    product_evidence: ForecastModelEvidenceProjection,
}

impl ForecastModelRequirement {
    /// Returns the model-runtime generation from which this requirement was retained.
    pub(crate) const fn runtime_generation_sha256(&self) -> Sha256Digest {
        self.runtime_generation_sha256
    }

    /// Returns the complete admitted model, feature, label, and dataset contract.
    pub(crate) fn metadata(&self) -> &ModelMetadata {
        self.bundle.metadata()
    }

    pub(crate) const fn product_evidence(&self) -> &ForecastModelEvidenceProjection {
        &self.product_evidence
    }

    fn bind_selected_horizon(
        &self,
        horizon: ForecastHorizon,
    ) -> Result<Self, ForecastPreparationError> {
        Ok(Self {
            runtime_generation_sha256: self.runtime_generation_sha256,
            bundle: Arc::clone(&self.bundle),
            product_evidence: forecast_model_evidence_projection_for_horizon(&self.bundle, horizon)
                .map_err(|_| ForecastPreparationError::InvalidEvidence)?,
        })
    }

    fn matches_coordinate(&self, selection: &ForecastPreparationSelection) -> bool {
        self.metadata().model_id() == selection.model_id
            && self.metadata().bundle_id() == &selection.bundle_id
            && self.metadata().bundle_version() == selection.bundle_version
    }
}

/// One data-owner-derived instrument scope compatible with an admitted model dataset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForecastInstrumentAvailability {
    instrument_id: InstrumentId,
    observed_from: Timestamp,
    observed_through: Timestamp,
    available_at: Timestamp,
    observed_points: NonZeroUsize,
    decimal_scale: u8,
}

impl ForecastInstrumentAvailability {
    /// Constructs a bounded, ordered point-in-time instrument summary.
    pub(crate) fn try_new(
        instrument_id: InstrumentId,
        observed_from: Timestamp,
        observed_through: Timestamp,
        available_at: Timestamp,
        observed_points: NonZeroUsize,
        decimal_scale: u8,
    ) -> Result<Self, ForecastEvidenceReadError> {
        if observed_from > observed_through
            || available_at < observed_through
            || observed_points.get() > market_squawk_modeling::MAX_FORECAST_OBSERVED_POINTS
            || decimal_scale > market_squawk_modeling::MAX_FORECAST_DECIMAL_SCALE
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        Ok(Self {
            instrument_id,
            observed_from,
            observed_through,
            available_at,
            observed_points,
            decimal_scale,
        })
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn observed_from(&self) -> Timestamp {
        self.observed_from
    }

    pub(crate) const fn observed_through(&self) -> Timestamp {
        self.observed_through
    }

    pub(crate) const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    pub(crate) const fn observed_points(&self) -> NonZeroUsize {
        self.observed_points
    }

    pub(crate) const fn decimal_scale(&self) -> u8 {
        self.decimal_scale
    }
}

/// Closed forecast policy supported by one exact data-owner generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ForecastEvidencePolicy {
    maximum_horizon_points: NonZeroU16,
    horizon_step_nanos: NonZeroU64,
    maximum_validity_nanos: NonZeroU64,
    minimum_observed_points: NonZeroUsize,
}

impl ForecastEvidencePolicy {
    /// Constructs a policy no broader than the installed forecast contract.
    pub(crate) fn try_new(
        maximum_horizon_points: NonZeroU16,
        horizon_step_nanos: NonZeroU64,
        maximum_validity_nanos: NonZeroU64,
        minimum_observed_points: NonZeroUsize,
    ) -> Result<Self, ForecastEvidenceReadError> {
        ForecastHorizon::try_new(maximum_horizon_points, horizon_step_nanos)
            .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)?;
        if maximum_validity_nanos.get() > MAXIMUM_FORECAST_VALIDITY_NANOS
            || minimum_observed_points.get() > market_squawk_modeling::MAX_FORECAST_OBSERVED_POINTS
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        Ok(Self {
            maximum_horizon_points,
            horizon_step_nanos,
            maximum_validity_nanos,
            minimum_observed_points,
        })
    }

    fn admits(self, selection: &ForecastPreparationSelection) -> bool {
        selection.horizon.points().get() <= self.maximum_horizon_points.get()
            && selection.horizon.step_nanos() == self.horizon_step_nanos
            && selection.validity_nanos <= self.maximum_validity_nanos.get()
    }

    pub(crate) const fn maximum_horizon_points(self) -> NonZeroU16 {
        self.maximum_horizon_points
    }

    pub(crate) const fn horizon_step_nanos(self) -> NonZeroU64 {
        self.horizon_step_nanos
    }

    pub(crate) const fn maximum_validity_nanos(self) -> NonZeroU64 {
        self.maximum_validity_nanos
    }

    pub(crate) const fn minimum_observed_points(self) -> NonZeroUsize {
        self.minimum_observed_points
    }
}

/// Sealed proof that one separately admitted AnalysisV1 selection is the inference counterpart
/// of the model's immutable TrainingV1 selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForecastDatasetPairingReceipt {
    training: TrainingDatasetIdentity,
    analysis: TrainingDatasetIdentity,
    training_fence: ForecastDatasetEvidenceFence,
    analysis_fence: ForecastDatasetEvidenceFence,
    training_production_identity: Sha256Digest,
    training_production_receipt_sha256: Sha256Digest,
    analysis_production_identity: Sha256Digest,
    analysis_production_receipt_sha256: Sha256Digest,
    fixed_horizon_nanos: NonZeroU64,
    shared_compatibility_sha256: Sha256Digest,
    pairing_sha256: Sha256Digest,
}

impl ForecastDatasetPairingReceipt {
    #[allow(
        clippy::too_many_arguments,
        reason = "each independent training, analysis, production, and compatibility fence remains explicit"
    )]
    pub(crate) fn try_new(
        training: TrainingDatasetIdentity,
        analysis: TrainingDatasetIdentity,
        training_fence: ForecastDatasetEvidenceFence,
        analysis_fence: ForecastDatasetEvidenceFence,
        training_production_identity: Sha256Digest,
        training_production_receipt_sha256: Sha256Digest,
        analysis_production_identity: Sha256Digest,
        analysis_production_receipt_sha256: Sha256Digest,
        fixed_horizon_nanos: NonZeroU64,
        shared_compatibility_sha256: Sha256Digest,
    ) -> Result<Self, ForecastEvidenceReadError> {
        if training.manifest() != training_fence.manifest()
            || training.catalog_identity() != training_fence.catalog_identity()
            || training.export_digest() != training_fence.export_sha256()
            || training.selection_digest() != training_fence.selection_sha256()
            || training.selection_as_of() != training_fence.as_of()
            || training.selected_component_rows() != training_fence.selected_rows()
            || analysis.manifest() != analysis_fence.manifest()
            || analysis.catalog_identity() != analysis_fence.catalog_identity()
            || analysis.export_digest() != analysis_fence.export_sha256()
            || analysis.selection_digest() != analysis_fence.selection_sha256()
            || analysis.selection_as_of() != analysis_fence.as_of()
            || analysis.selected_component_rows() != analysis_fence.selected_rows()
            || analysis.manifest() == training.manifest()
            || analysis.build_spec_digest() == training.build_spec_digest()
            || analysis.universe_digest() != training.universe_digest()
            || analysis.policy_digest() != training.policy_digest()
            || analysis.selection_as_of() != training.selection_as_of()
            || analysis.selected_component_rows() != training.selected_component_rows()
            || analysis_production_identity == training_production_identity
            || analysis_production_receipt_sha256 == training_production_receipt_sha256
            || [
                training_production_identity,
                training_production_receipt_sha256,
                analysis_production_identity,
                analysis_production_receipt_sha256,
                shared_compatibility_sha256,
            ]
            .iter()
            .any(|digest| digest.bytes() == [0; 32])
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        let pairing_sha256 = pairing_digest(
            &training,
            &analysis,
            &training_fence,
            &analysis_fence,
            training_production_identity,
            training_production_receipt_sha256,
            analysis_production_identity,
            analysis_production_receipt_sha256,
            fixed_horizon_nanos,
            shared_compatibility_sha256,
        )?;
        Ok(Self {
            training,
            analysis,
            training_fence,
            analysis_fence,
            training_production_identity,
            training_production_receipt_sha256,
            analysis_production_identity,
            analysis_production_receipt_sha256,
            fixed_horizon_nanos,
            shared_compatibility_sha256,
            pairing_sha256,
        })
    }

    pub(crate) const fn training(&self) -> &TrainingDatasetIdentity {
        &self.training
    }

    pub(crate) const fn analysis(&self) -> &TrainingDatasetIdentity {
        &self.analysis
    }

    pub(crate) const fn analysis_fence(&self) -> &ForecastDatasetEvidenceFence {
        &self.analysis_fence
    }

    pub(crate) const fn training_fence(&self) -> &ForecastDatasetEvidenceFence {
        &self.training_fence
    }

    pub(crate) const fn training_production_identity(&self) -> Sha256Digest {
        self.training_production_identity
    }

    pub(crate) const fn training_production_receipt_sha256(&self) -> Sha256Digest {
        self.training_production_receipt_sha256
    }

    pub(crate) const fn analysis_production_identity(&self) -> Sha256Digest {
        self.analysis_production_identity
    }

    pub(crate) const fn analysis_production_receipt_sha256(&self) -> Sha256Digest {
        self.analysis_production_receipt_sha256
    }

    pub(crate) const fn fixed_horizon_nanos(&self) -> NonZeroU64 {
        self.fixed_horizon_nanos
    }

    pub(crate) const fn pairing_sha256(&self) -> Sha256Digest {
        self.pairing_sha256
    }
}

/// One exact admitted feature dataset and its data-owner-derived availability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForecastEvidenceDataset {
    model_id: ModelId,
    bundle_id: BundleId,
    bundle_version: NonZeroU64,
    pairing: ForecastDatasetPairingReceipt,
    instruments: Vec<ForecastInstrumentAvailability>,
    policies: Vec<ForecastEvidencePolicy>,
}

impl ForecastEvidenceDataset {
    /// Constructs one canonical nonempty compatibility record.
    pub(crate) fn try_new(
        model_id: ModelId,
        bundle_id: BundleId,
        bundle_version: NonZeroU64,
        pairing: ForecastDatasetPairingReceipt,
        mut instruments: Vec<ForecastInstrumentAvailability>,
        mut policies: Vec<ForecastEvidencePolicy>,
    ) -> Result<Self, ForecastEvidenceReadError> {
        instruments.sort_unstable_by_key(ForecastInstrumentAvailability::instrument_id);
        policies.sort_unstable_by_key(|policy| {
            (
                policy.horizon_step_nanos.get(),
                policy.maximum_horizon_points.get(),
                policy.maximum_validity_nanos.get(),
                policy.minimum_observed_points.get(),
            )
        });
        if instruments.is_empty()
            || policies.is_empty()
            || instruments
                .windows(2)
                .any(|pair| pair[0].instrument_id == pair[1].instrument_id)
            || policies.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        Ok(Self {
            model_id,
            bundle_id,
            bundle_version,
            pairing,
            instruments,
            policies,
        })
    }

    pub(crate) const fn model_id(&self) -> ModelId {
        self.model_id
    }

    pub(crate) const fn bundle_id(&self) -> &BundleId {
        &self.bundle_id
    }

    pub(crate) const fn bundle_version(&self) -> NonZeroU64 {
        self.bundle_version
    }

    pub(crate) const fn pairing(&self) -> &ForecastDatasetPairingReceipt {
        &self.pairing
    }

    pub(crate) const fn dataset(&self) -> &TrainingDatasetIdentity {
        self.pairing.training()
    }

    pub(crate) const fn analysis_manifest(&self) -> &DatasetManifestRef {
        self.pairing.analysis_fence().manifest()
    }

    pub(crate) fn instruments(&self) -> &[ForecastInstrumentAvailability] {
        &self.instruments
    }

    pub(crate) fn policies(&self) -> &[ForecastEvidencePolicy] {
        &self.policies
    }

    pub(crate) fn retained_dynamic_bytes(&self) -> Result<usize, ForecastEvidenceReadError> {
        evidence_dataset_dynamic_bytes(self)
    }
}

/// Data-owner result for one coherent compatible-dataset catalog read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForecastEvidenceCatalogSnapshot {
    authority_generation_sha256: Sha256Digest,
    datasets: Vec<ForecastEvidenceDataset>,
}

impl ForecastEvidenceCatalogSnapshot {
    /// Constructs a catalog snapshot bound to a nonzero data-owner generation.
    pub(crate) fn try_new(
        authority_generation_sha256: Sha256Digest,
        mut datasets: Vec<ForecastEvidenceDataset>,
    ) -> Result<Self, ForecastEvidenceReadError> {
        if authority_generation_sha256.bytes() == [0; 32] || datasets.len() > MAXIMUM_EVIDENCE_PAIRS
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        datasets.sort_unstable_by(compare_evidence_datasets);
        if datasets
            .windows(2)
            .any(|pair| same_evidence_coordinate(&pair[0], &pair[1]))
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        let retained_bytes = datasets
            .capacity()
            .checked_mul(size_of::<ForecastEvidenceDataset>())
            .ok_or(ForecastEvidenceReadError::Capacity)?;
        let retained_bytes = datasets.iter().try_fold(retained_bytes, |total, dataset| {
            total
                .checked_add(dataset.retained_dynamic_bytes()?)
                .ok_or(ForecastEvidenceReadError::Capacity)
        })?;
        if retained_bytes > MAXIMUM_EVIDENCE_CATALOG_BYTES {
            return Err(ForecastEvidenceReadError::Capacity);
        }
        Ok(Self {
            authority_generation_sha256,
            datasets,
        })
    }

    pub(crate) fn datasets(&self) -> &[ForecastEvidenceDataset] {
        &self.datasets
    }
}

fn compare_evidence_datasets(
    left: &ForecastEvidenceDataset,
    right: &ForecastEvidenceDataset,
) -> Ordering {
    left.model_id
        .cmp(&right.model_id)
        .then_with(|| left.bundle_id.as_str().cmp(right.bundle_id.as_str()))
        .then_with(|| left.bundle_version.cmp(&right.bundle_version))
        .then_with(|| compare_manifest(left.dataset().manifest(), right.dataset().manifest()))
        .then_with(|| compare_manifest(left.analysis_manifest(), right.analysis_manifest()))
}

fn same_evidence_coordinate(
    left: &ForecastEvidenceDataset,
    right: &ForecastEvidenceDataset,
) -> bool {
    left.model_id == right.model_id
        && left.bundle_id == right.bundle_id
        && left.bundle_version == right.bundle_version
        && left.dataset().manifest() == right.dataset().manifest()
        && left.analysis_manifest() == right.analysis_manifest()
}

fn compare_manifest(left: &DatasetManifestRef, right: &DatasetManifestRef) -> Ordering {
    left.dataset_id()
        .as_str()
        .cmp(right.dataset_id().as_str())
        .then_with(|| left.manifest_version().cmp(&right.manifest_version()))
        .then_with(|| left.schema().name().cmp(right.schema().name()))
        .then_with(|| left.schema_version().cmp(&right.schema_version()))
        .then_with(|| {
            left.schema()
                .fingerprint()
                .cmp(&right.schema().fingerprint())
        })
        .then_with(|| {
            left.content_hash()
                .bytes()
                .cmp(&right.content_hash().bytes())
        })
}

fn evidence_dataset_dynamic_bytes(
    dataset: &ForecastEvidenceDataset,
) -> Result<usize, ForecastEvidenceReadError> {
    let manifests = [
        dataset.dataset().manifest(),
        dataset.pairing().analysis().manifest(),
        dataset.pairing().training_fence().manifest(),
        dataset.pairing().analysis_fence().manifest(),
    ];
    let manifest_bytes = manifests.iter().try_fold(0_usize, |total, manifest| {
        total
            .checked_add(manifest.dataset_id().as_str().len())
            .and_then(|bytes| bytes.checked_add(manifest.schema().name().len()))
            .ok_or(ForecastEvidenceReadError::Capacity)
    })?;
    dataset
        .bundle_id
        .as_str()
        .len()
        .checked_add(manifest_bytes)
        .and_then(|bytes| {
            bytes.checked_add(
                dataset
                    .instruments
                    .capacity()
                    .checked_mul(size_of::<ForecastInstrumentAvailability>())?,
            )
        })
        .and_then(|bytes| {
            bytes.checked_add(
                dataset
                    .policies
                    .capacity()
                    .checked_mul(size_of::<ForecastEvidencePolicy>())?,
            )
        })
        .ok_or(ForecastEvidenceReadError::Capacity)
}

/// Complete admitted-model query supplied to the analytical evidence owner.
#[derive(Clone, Debug)]
pub(crate) struct ForecastEvidenceCatalogRequest {
    runtime_generation_sha256: Sha256Digest,
    models: Box<[ForecastModelRequirement]>,
}

impl ForecastEvidenceCatalogRequest {
    pub(crate) const fn runtime_generation_sha256(&self) -> Sha256Digest {
        self.runtime_generation_sha256
    }

    pub(crate) fn models(&self) -> &[ForecastModelRequirement] {
        &self.models
    }
}

/// User-selected forecast scope. History and matrix values are deliberately absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForecastPreparationSelection {
    model_id: ModelId,
    bundle_id: BundleId,
    bundle_version: NonZeroU64,
    dataset_manifest: DatasetManifestRef,
    analysis_manifest: DatasetManifestRef,
    instrument_id: InstrumentId,
    horizon: ForecastHorizon,
    validity_nanos: u64,
}

impl ForecastPreparationSelection {
    /// Constructs a bounded selection over already enumerated model and dataset options.
    #[allow(
        clippy::too_many_arguments,
        reason = "model, dataset, instrument, horizon, and validity remain explicit"
    )]
    pub fn try_new(
        model_id: ModelId,
        bundle_id: BundleId,
        bundle_version: NonZeroU64,
        dataset_manifest: DatasetManifestRef,
        analysis_manifest: DatasetManifestRef,
        instrument_id: InstrumentId,
        horizon: ForecastHorizon,
        validity_nanos: u64,
    ) -> Result<Self, ForecastPreparationError> {
        if validity_nanos == 0 || validity_nanos > MAXIMUM_FORECAST_VALIDITY_NANOS {
            return Err(ForecastPreparationError::InvalidSelection);
        }
        Ok(Self {
            model_id,
            bundle_id,
            bundle_version,
            dataset_manifest,
            analysis_manifest,
            instrument_id,
            horizon,
            validity_nanos,
        })
    }

    pub(crate) const fn dataset_manifest(&self) -> &DatasetManifestRef {
        &self.dataset_manifest
    }

    pub(crate) const fn analysis_manifest(&self) -> &DatasetManifestRef {
        &self.analysis_manifest
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn horizon(&self) -> ForecastHorizon {
        self.horizon
    }
}

/// Exact materialization request accepted only by the analytical evidence owner.
#[derive(Clone, Debug)]
pub(crate) struct ForecastEvidenceMaterializationRequest {
    model: ForecastModelRequirement,
    selection: ForecastPreparationSelection,
    pairing: ForecastDatasetPairingReceipt,
    authority_generation_sha256: Sha256Digest,
    knowledge_cutoff: Timestamp,
    macro_effective_date_cutoff: CalendarDate,
}

/// Exact label-free current-PIT generation/query fence used to construct the serving feature at
/// time `T`. Historical TrainingV1/AnalysisV1 evidence never supplies these coordinates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForecastServingInputFence {
    manifest: DatasetManifestRef,
    source_id: SourceId,
    object_graph_sha256: Sha256Digest,
    selection_sha256: Sha256Digest,
    result_sha256: Sha256Digest,
    knowledge_cutoff: Timestamp,
    prior_observed_at: Timestamp,
    observed_through: Timestamp,
    feature_sha256: Sha256Digest,
}

impl ForecastServingInputFence {
    #[allow(
        clippy::too_many_arguments,
        reason = "manifest, query, cutoff, temporal, and derived-feature evidence stay explicit"
    )]
    pub(crate) fn try_new(
        manifest: DatasetManifestRef,
        source_id: SourceId,
        object_graph_sha256: Sha256Digest,
        selection_sha256: Sha256Digest,
        result_sha256: Sha256Digest,
        knowledge_cutoff: Timestamp,
        prior_observed_at: Timestamp,
        observed_through: Timestamp,
        feature_sha256: Sha256Digest,
    ) -> Result<Self, ForecastEvidenceReadError> {
        if prior_observed_at >= observed_through
            || observed_through > knowledge_cutoff
            || [
                object_graph_sha256,
                selection_sha256,
                result_sha256,
                feature_sha256,
            ]
            .iter()
            .any(|digest| digest.bytes() == [0; 32])
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        Ok(Self {
            manifest,
            source_id,
            object_graph_sha256,
            selection_sha256,
            result_sha256,
            knowledge_cutoff,
            prior_observed_at,
            observed_through,
            feature_sha256,
        })
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn object_graph_sha256(&self) -> Sha256Digest {
        self.object_graph_sha256
    }

    pub(crate) const fn selection_sha256(&self) -> Sha256Digest {
        self.selection_sha256
    }

    pub(crate) const fn result_sha256(&self) -> Sha256Digest {
        self.result_sha256
    }

    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    pub(crate) const fn prior_observed_at(&self) -> Timestamp {
        self.prior_observed_at
    }

    pub(crate) const fn observed_through(&self) -> Timestamp {
        self.observed_through
    }

    pub(crate) const fn feature_sha256(&self) -> Sha256Digest {
        self.feature_sha256
    }
}

impl ForecastEvidenceMaterializationRequest {
    pub(crate) const fn model(&self) -> &ForecastModelRequirement {
        &self.model
    }

    pub(crate) const fn selection(&self) -> &ForecastPreparationSelection {
        &self.selection
    }

    pub(crate) const fn pairing(&self) -> &ForecastDatasetPairingReceipt {
        &self.pairing
    }

    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    pub(crate) const fn macro_effective_date_cutoff(&self) -> CalendarDate {
        self.macro_effective_date_cutoff
    }
}

/// Typed history and feature matrix produced only by the injected analytical authority.
pub(crate) struct PreparedForecastEvidence {
    request: ForecastEvidenceMaterializationRequest,
    serving_input: ForecastServingInputFence,
    observed_cutoff: Timestamp,
    available_at: Timestamp,
    decimal_scale: u8,
    observed_history: Box<[ForecastObservedPoint]>,
    inputs: Box<[Box<[f64]>]>,
    evidence_sha256: Sha256Digest,
}

impl PreparedForecastEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "the materialized PIT cutoff, history, matrix, and sealed pairing remain explicit"
    )]
    pub(crate) fn try_new(
        request: ForecastEvidenceMaterializationRequest,
        serving_input: ForecastServingInputFence,
        observed_cutoff: Timestamp,
        available_at: Timestamp,
        decimal_scale: u8,
        observed_history: Vec<ForecastObservedPoint>,
        inputs: Vec<Box<[f64]>>,
    ) -> Result<Self, ForecastEvidenceReadError> {
        let horizon_nanos = i64::try_from(request.selection.horizon.step_nanos().get())
            .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)?;
        let target_at = observed_cutoff
            .checked_add_nanos(horizon_nanos)
            .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)?;
        if observed_history.is_empty()
            || inputs.is_empty()
            || observed_history.len() > market_squawk_modeling::MAX_FORECAST_OBSERVED_POINTS
            || decimal_scale > market_squawk_modeling::MAX_FORECAST_DECIMAL_SCALE
            || observed_history.last().map(|point| point.observed_at()) != Some(observed_cutoff)
            || serving_input.observed_through() != observed_cutoff
            || available_at > serving_input.knowledge_cutoff()
            || target_at <= serving_input.knowledge_cutoff()
            || observed_history
                .iter()
                .any(|point| point.available_at() > available_at)
            || inputs
                .iter()
                .any(|row| row.is_empty() || row.iter().any(|value| !value.is_finite()))
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        let evidence_sha256 = evidence_digest(
            &request,
            &serving_input,
            observed_cutoff,
            available_at,
            decimal_scale,
            &observed_history,
            &inputs,
        )?;
        Ok(Self {
            request,
            serving_input,
            observed_cutoff,
            available_at,
            decimal_scale,
            observed_history: observed_history.into_boxed_slice(),
            inputs: inputs.into_boxed_slice(),
            evidence_sha256,
        })
    }

    pub(crate) const fn evidence_sha256(&self) -> Sha256Digest {
        self.evidence_sha256
    }
}

impl fmt::Debug for PreparedForecastEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedForecastEvidence")
            .field("request", &self.request)
            .field("serving_input", &self.serving_input)
            .field("observed_cutoff", &self.observed_cutoff)
            .field("available_at", &self.available_at)
            .field("decimal_scale", &self.decimal_scale)
            .field("observed_points", &self.observed_history.len())
            .field("input_rows", &self.inputs.len())
            .field("evidence_sha256", &self.evidence_sha256)
            .finish()
    }
}

/// Exact data-owner fence revalidated immediately before durable job admission.
#[derive(Clone, Debug)]
pub(crate) struct ForecastEvidenceRevalidation {
    request: ForecastEvidenceMaterializationRequest,
    serving_input: ForecastServingInputFence,
    evidence_sha256: Sha256Digest,
}

impl ForecastEvidenceRevalidation {
    pub(crate) const fn request(&self) -> &ForecastEvidenceMaterializationRequest {
        &self.request
    }

    pub(crate) const fn evidence_sha256(&self) -> Sha256Digest {
        self.evidence_sha256
    }

    pub(crate) const fn serving_input(&self) -> &ForecastServingInputFence {
        &self.serving_input
    }
}

/// Narrow analytical authority required by model-owned forecast preparation.
#[async_trait]
pub(crate) trait ForecastEvidenceReader: fmt::Debug + Send + Sync {
    /// Enumerates only compatible point-in-time datasets, instruments, and closed policies.
    async fn catalog(
        &self,
        request: ForecastEvidenceCatalogRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<ForecastEvidenceCatalogSnapshot, ForecastEvidenceReadError>;

    /// Constructs exact observed history and the coefficient-ordered feature matrix.
    async fn prepare(
        &self,
        request: ForecastEvidenceMaterializationRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedForecastEvidence, ForecastEvidenceReadError>;

    /// Re-derives the retained evidence fence immediately before job-runner admission.
    async fn revalidate(
        &self,
        expected: &ForecastEvidenceRevalidation,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), ForecastEvidenceReadError>;
}

/// Public admitted-model and analytical-compatibility inventory.
#[derive(Clone, Debug)]
pub struct ForecastPreparationCatalog {
    runtime_generation_sha256: Sha256Digest,
    models: Box<[ForecastModelSummary]>,
    evidence: ForecastEvidenceCatalogSnapshot,
}

impl ForecastPreparationCatalog {
    #[must_use]
    pub const fn runtime_generation_sha256(&self) -> Sha256Digest {
        self.runtime_generation_sha256
    }

    #[must_use]
    pub fn models(&self) -> &[ForecastModelSummary] {
        &self.models
    }

    pub(crate) const fn evidence(&self) -> &ForecastEvidenceCatalogSnapshot {
        &self.evidence
    }
}

/// User-displayable admitted model coordinate and immutable evidence identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForecastModelSummary {
    model_id: ModelId,
    bundle_id: BundleId,
    bundle_version: NonZeroU64,
    metadata_sha256: Sha256Digest,
    artifact_sha256: Sha256Digest,
    dataset_manifest: DatasetManifestRef,
    dataset_export_sha256: Sha256Digest,
    dataset_policy_sha256: Sha256Digest,
    feature_count: usize,
    product_evidence: ForecastModelEvidenceProjection,
    format: ModelFormat,
    output_semantics: ModelOutputSemantics,
    output_binding: ForecastOutputBinding,
    intended_use: Box<str>,
    limitations: Box<[Box<str>]>,
    fallback_reason: Box<str>,
}

impl ForecastModelSummary {
    fn from_requirement(requirement: &ForecastModelRequirement) -> Self {
        let metadata = requirement.metadata();
        Self {
            model_id: metadata.model_id(),
            bundle_id: metadata.bundle_id().clone(),
            bundle_version: metadata.bundle_version(),
            metadata_sha256: metadata.metadata_hash(),
            artifact_sha256: metadata.artifact_hash(),
            dataset_manifest: metadata.dataset().manifest().clone(),
            dataset_export_sha256: metadata.dataset().export_digest(),
            dataset_policy_sha256: metadata.dataset().policy_digest(),
            feature_count: metadata.features().len(),
            product_evidence: requirement.product_evidence.clone(),
            format: metadata.format(),
            output_semantics: metadata.output_semantics(),
            output_binding: metadata.output_binding().clone(),
            intended_use: metadata.intended_use().into(),
            limitations: metadata.limitations().into(),
            fallback_reason: metadata.fallback_reason().into(),
        }
    }

    #[must_use]
    pub const fn model_id(&self) -> ModelId {
        self.model_id
    }

    #[must_use]
    pub const fn bundle_id(&self) -> &BundleId {
        &self.bundle_id
    }

    #[must_use]
    pub const fn bundle_version(&self) -> NonZeroU64 {
        self.bundle_version
    }

    #[must_use]
    pub const fn metadata_sha256(&self) -> Sha256Digest {
        self.metadata_sha256
    }

    #[must_use]
    pub const fn artifact_sha256(&self) -> Sha256Digest {
        self.artifact_sha256
    }

    #[must_use]
    pub const fn dataset_manifest(&self) -> &DatasetManifestRef {
        &self.dataset_manifest
    }

    #[must_use]
    pub const fn dataset_export_sha256(&self) -> Sha256Digest {
        self.dataset_export_sha256
    }

    #[must_use]
    pub const fn dataset_policy_sha256(&self) -> Sha256Digest {
        self.dataset_policy_sha256
    }

    #[must_use]
    pub const fn feature_count(&self) -> usize {
        self.feature_count
    }

    #[must_use]
    pub const fn has_calibrated_intervals(&self) -> bool {
        matches!(
            self.product_evidence.calibration(),
            ForecastModelCalibrationState::Calibrated
        )
    }

    #[must_use]
    pub(crate) const fn product_evidence(&self) -> &ForecastModelEvidenceProjection {
        &self.product_evidence
    }

    #[must_use]
    pub const fn format(&self) -> ModelFormat {
        self.format
    }

    #[must_use]
    pub const fn output_semantics(&self) -> ModelOutputSemantics {
        self.output_semantics
    }

    #[must_use]
    pub const fn output_binding(&self) -> &ForecastOutputBinding {
        &self.output_binding
    }

    #[must_use]
    pub fn intended_use(&self) -> &str {
        &self.intended_use
    }

    #[must_use]
    pub fn limitations(&self) -> &[Box<str>] {
        &self.limitations
    }

    #[must_use]
    pub fn fallback_reason(&self) -> &str {
        &self.fallback_reason
    }
}

/// Opaque process-local one-use capability for one exact prepared forecast request.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ForecastPreparationReceipt {
    receipt_id: Uuid,
    receipt_sha256: Sha256Digest,
    expires_at: Timestamp,
}

impl ForecastPreparationReceipt {
    /// Reconstructs an untrusted transport receipt for exact registry comparison.
    pub(crate) fn try_from_wire(
        receipt_id: Uuid,
        receipt_sha256: [u8; 32],
        expires_at: Timestamp,
    ) -> Result<Self, ForecastPreparationError> {
        if receipt_id.is_nil() || receipt_sha256 == [0; 32] {
            return Err(ForecastPreparationError::ReceiptMismatch);
        }
        Ok(Self {
            receipt_id,
            receipt_sha256: Sha256Digest::new(receipt_sha256),
            expires_at,
        })
    }

    #[must_use]
    pub const fn receipt_id(self) -> Uuid {
        self.receipt_id
    }

    #[must_use]
    pub const fn receipt_sha256(self) -> Sha256Digest {
        self.receipt_sha256
    }

    #[must_use]
    pub const fn expires_at(self) -> Timestamp {
        self.expires_at
    }

    #[cfg(test)]
    const fn fixture(receipt_id: Uuid, receipt_sha256: [u8; 32], expires_at: Timestamp) -> Self {
        Self {
            receipt_id,
            receipt_sha256: Sha256Digest::new(receipt_sha256),
            expires_at,
        }
    }
}

/// Human-readable evidence preview paired with a one-use opaque receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForecastPreparationPreview {
    model: ForecastModelSummary,
    instrument_id: InstrumentId,
    observed_from: Timestamp,
    observed_through: Timestamp,
    available_at: Timestamp,
    observed_points: usize,
    horizon: ForecastHorizon,
    validity_nanos: u64,
    evidence_sha256: Sha256Digest,
    request_sha256: Sha256Digest,
    runtime_generation_sha256: Sha256Digest,
}

impl ForecastPreparationPreview {
    #[must_use]
    pub const fn model(&self) -> &ForecastModelSummary {
        &self.model
    }

    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[must_use]
    pub const fn observed_from(&self) -> Timestamp {
        self.observed_from
    }

    #[must_use]
    pub const fn observed_through(&self) -> Timestamp {
        self.observed_through
    }

    #[must_use]
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    #[must_use]
    pub const fn observed_points(&self) -> usize {
        self.observed_points
    }

    #[must_use]
    pub const fn horizon(&self) -> ForecastHorizon {
        self.horizon
    }

    #[must_use]
    pub const fn validity_nanos(&self) -> u64 {
        self.validity_nanos
    }

    #[must_use]
    pub const fn evidence_sha256(&self) -> Sha256Digest {
        self.evidence_sha256
    }

    #[must_use]
    pub const fn request_sha256(&self) -> Sha256Digest {
        self.request_sha256
    }

    #[must_use]
    pub const fn runtime_generation_sha256(&self) -> Sha256Digest {
        self.runtime_generation_sha256
    }
}

/// Prepared preview and its matching opaque confirmation capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedForecast {
    preview: ForecastPreparationPreview,
    receipt: ForecastPreparationReceipt,
    product_identity: ForecastProductIdentity,
}

impl PreparedForecast {
    #[must_use]
    pub const fn preview(&self) -> &ForecastPreparationPreview {
        &self.preview
    }

    #[must_use]
    pub const fn receipt(&self) -> ForecastPreparationReceipt {
        self.receipt
    }

    pub(crate) const fn product_identity(&self) -> &ForecastProductIdentity {
        &self.product_identity
    }
}

struct StoredForecastPreparation {
    owner: RequestOrigin,
    workspace: WorkspaceRuntimeIdentity,
    expires_at: Instant,
    runtime_generation_sha256: Sha256Digest,
    revalidation: Option<ForecastEvidenceRevalidation>,
    request: TypedToolRequest,
    retained_request_bytes: usize,
}

impl StoredForecastPreparation {
    #[cfg(test)]
    fn fixture(
        owner: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        expires_at: Instant,
        request: TypedToolRequest,
    ) -> Self {
        Self {
            owner,
            workspace,
            expires_at,
            runtime_generation_sha256: Sha256Digest::new([1; 32]),
            revalidation: None,
            request,
            retained_request_bytes: 1,
        }
    }
}

impl fmt::Debug for StoredForecastPreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredForecastPreparation")
            .field("owner", &self.owner)
            .field("workspace", &self.workspace)
            .field("expires_at", &self.expires_at)
            .field("runtime_generation_sha256", &self.runtime_generation_sha256)
            .field("revalidation", &self.revalidation)
            .field("request", &"[TYPED FORECAST REQUEST]")
            .finish()
    }
}

struct ReceiptRegistry {
    maximum: usize,
    maximum_retained_request_bytes: usize,
    entries: BTreeMap<Uuid, (ForecastPreparationReceipt, StoredForecastPreparation)>,
}

impl ReceiptRegistry {
    fn new(maximum: usize, maximum_retained_request_bytes: usize) -> Self {
        Self {
            maximum,
            maximum_retained_request_bytes,
            entries: BTreeMap::new(),
        }
    }

    fn insert(
        &mut self,
        receipt: ForecastPreparationReceipt,
        stored: StoredForecastPreparation,
    ) -> Result<(), ForecastPreparationError> {
        self.prune(Instant::now());
        let retained_request_bytes = self
            .entries
            .values()
            .try_fold(stored.retained_request_bytes, |total, (_, entry)| {
                total.checked_add(entry.retained_request_bytes)
            });
        if self.entries.len() >= self.maximum
            || retained_request_bytes
                .is_none_or(|total| total > self.maximum_retained_request_bytes)
            || self.entries.contains_key(&receipt.receipt_id)
        {
            return Err(ForecastPreparationError::Capacity);
        }
        self.entries.insert(receipt.receipt_id, (receipt, stored));
        Ok(())
    }

    fn consume(
        &mut self,
        receipt: ForecastPreparationReceipt,
        owner: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        now: Instant,
    ) -> Result<StoredForecastPreparation, ForecastPreparationError> {
        self.prune(now);
        let (retained_receipt, stored) = self
            .entries
            .get(&receipt.receipt_id)
            .ok_or(ForecastPreparationError::ReceiptUnavailable)?;
        if *retained_receipt != receipt || stored.owner != owner || stored.workspace != workspace {
            return Err(ForecastPreparationError::ReceiptMismatch);
        }
        self.entries
            .remove(&receipt.receipt_id)
            .map(|(_, stored)| stored)
            .ok_or(ForecastPreparationError::ReceiptUnavailable)
    }

    fn consume_token(
        &mut self,
        receipt_id: Uuid,
        owner: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        now: Instant,
    ) -> Result<StoredForecastPreparation, ForecastPreparationError> {
        self.prune(now);
        let (_, stored) = self
            .entries
            .get(&receipt_id)
            .ok_or(ForecastPreparationError::ReceiptUnavailable)?;
        if stored.owner != owner || stored.workspace != workspace {
            return Err(ForecastPreparationError::ReceiptMismatch);
        }
        self.entries
            .remove(&receipt_id)
            .map(|(_, stored)| stored)
            .ok_or(ForecastPreparationError::ReceiptUnavailable)
    }

    fn prune(&mut self, now: Instant) {
        self.entries
            .retain(|_, (_, stored)| stored.expires_at > now);
    }
}

/// Sole model-owned authority for previewing and admitting evidence-derived forecast jobs.
pub(crate) struct ForecastPreparationAuthority {
    runtime: Arc<ProductionModelRuntime>,
    evidence: Arc<dyn ForecastEvidenceReader>,
    generate_descriptor: ToolDescriptor,
    limits: ForecastPreparationLimits,
    receipts: Mutex<ReceiptRegistry>,
}

impl ForecastPreparationAuthority {
    /// Binds the admitted model runtime, analytical reader, and existing terminal operation.
    pub(crate) fn try_new(
        runtime: Arc<ProductionModelRuntime>,
        evidence: Arc<dyn ForecastEvidenceReader>,
        generate_descriptor: ToolDescriptor,
        limits: ForecastPreparationLimits,
    ) -> Result<Self, ForecastPreparationError> {
        if generate_descriptor.name() != GENERATE_FORECAST
            || generate_descriptor.contract().domain() != ServiceDomain::Model
        {
            return Err(ForecastPreparationError::InvalidDescriptor);
        }
        Ok(Self {
            runtime,
            evidence,
            generate_descriptor,
            limits,
            receipts: Mutex::new(ReceiptRegistry::new(
                limits.maximum_receipts.get(),
                limits.maximum_retained_request_bytes.get(),
            )),
        })
    }

    /// Enumerates admitted model bundles and only data-owner-qualified compatible choices.
    pub(crate) async fn catalog(
        &self,
        origin: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<ForecastPreparationCatalog, ForecastPreparationError> {
        validate_origin(origin, workspace)?;
        check_control(deadline, &cancellation)?;
        let retained = self.runtime.retain_forecast_runtime()?;
        let backup = self.runtime.retain_backup()?;
        let request = catalog_request(&retained, &backup)?;
        let models = request
            .models
            .iter()
            .map(ForecastModelSummary::from_requirement)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let evidence = self
            .evidence
            .catalog(request.clone(), deadline, cancellation)
            .await?;
        validate_catalog(&request, &evidence)?;
        Ok(ForecastPreparationCatalog {
            runtime_generation_sha256: retained.generation_sha256,
            models,
            evidence,
        })
    }

    /// Builds a human preview and retains the exact descriptor-admitted terminal request.
    pub(crate) async fn prepare<F>(
        &self,
        origin: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        selection: ForecastPreparationSelection,
        resolve_product_identity: F,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedForecast, ForecastPreparationError>
    where
        F: FnOnce(
                InstrumentId,
                Timestamp,
                Timestamp,
            ) -> Result<ForecastProductIdentity, ForecastPreparationError>
            + Send,
    {
        validate_origin(origin, workspace)?;
        check_control(deadline, &cancellation)?;
        let retained = self.runtime.retain_forecast_runtime()?;
        let backup = self.runtime.retain_backup()?;
        let catalog_request = catalog_request(&retained, &backup)?;
        let model = catalog_request
            .models
            .iter()
            .find(|candidate| candidate.matches_coordinate(&selection))
            .cloned()
            .ok_or(ForecastPreparationError::ModelUnavailable)?;
        let catalog = self
            .evidence
            .catalog(
                catalog_request.clone(),
                deadline,
                cancellation.child_token(),
            )
            .await?;
        validate_catalog(&catalog_request, &catalog)?;
        let option = compatible_option(&catalog, &selection)?;
        let instrument = option
            .instruments
            .iter()
            .find(|candidate| candidate.instrument_id == selection.instrument_id)
            .ok_or(ForecastPreparationError::IncompatibleSelection)?;
        let policy = option
            .policies
            .iter()
            .copied()
            .find(|candidate| candidate.admits(&selection))
            .ok_or(ForecastPreparationError::IncompatibleSelection)?;
        if option.pairing.fixed_horizon_nanos() != selection.horizon.step_nanos() {
            return Err(ForecastPreparationError::IncompatibleSelection);
        }
        let model = model.bind_selected_horizon(selection.horizon)?;
        if instrument.observed_points.get() < policy.minimum_observed_points.get() {
            return Err(ForecastPreparationError::IncompatibleSelection);
        }
        let knowledge_cutoff = wall_now()?;
        let macro_effective_date_cutoff = knowledge_cutoff
            .utc_calendar_date()
            .map_err(|_| ForecastPreparationError::TimeUnavailable)?;
        let materialization = ForecastEvidenceMaterializationRequest {
            model: model.clone(),
            selection: selection.clone(),
            pairing: option.pairing.clone(),
            authority_generation_sha256: catalog.authority_generation_sha256,
            knowledge_cutoff,
            macro_effective_date_cutoff,
        };
        let evidence = self
            .evidence
            .prepare(materialization.clone(), deadline, cancellation)
            .await?;
        validate_prepared_evidence(&materialization, &evidence)?;
        let product_identity = resolve_product_identity(
            selection.instrument_id,
            evidence.serving_input.knowledge_cutoff(),
            evidence.observed_cutoff,
        )?;
        if product_identity.knowledge_at() != evidence.serving_input.knowledge_cutoff()
            || product_identity.effective_at() != evidence.observed_cutoff
        {
            return Err(ForecastPreparationError::InvalidEvidence);
        }
        let product_target =
            ForecastProductTarget::try_from_binding(model.metadata().output_binding())
                .map_err(|_| ForecastPreparationError::ModelUnavailable)?;
        if model.product_evidence.overall() == ForecastModelEvidenceState::Unavailable {
            return Err(ForecastPreparationError::ModelUnavailable);
        }
        if product_target
            .currency_code()
            .is_some_and(|currency| currency != product_identity.quote_currency().as_str())
        {
            return Err(ForecastPreparationError::IncompatibleSelection);
        }
        let request = self
            .generate_descriptor
            .admit(typed_arguments(&evidence, &product_identity)?)
            .map_err(|_| ForecastPreparationError::InvalidEvidence)?;
        let (request_sha256, retained_request_bytes) = request_digest(&request)?;
        if retained_request_bytes > MAXIMUM_SINGLE_REQUEST_BYTES {
            return Err(ForecastPreparationError::Capacity);
        }
        let issued_at = wall_now()?;
        let lifetime_nanos = i64::try_from(self.limits.receipt_lifetime.as_nanos())
            .map_err(|_| ForecastPreparationError::TimeUnavailable)?;
        let expires_at = issued_at
            .checked_add_nanos(lifetime_nanos)
            .map_err(|_| ForecastPreparationError::TimeUnavailable)?;
        let expires_instant = Instant::now()
            .checked_add(self.limits.receipt_lifetime)
            .ok_or(ForecastPreparationError::TimeUnavailable)?;
        let revalidation = ForecastEvidenceRevalidation {
            request: materialization,
            serving_input: evidence.serving_input.clone(),
            evidence_sha256: evidence.evidence_sha256,
        };
        let mut receipts = self
            .receipts
            .lock()
            .map_err(|_| ForecastPreparationError::Unavailable)?;
        let receipt_id = unique_receipt_id(&receipts)?;
        let receipt_sha256 = receipt_digest(
            receipt_id,
            origin,
            workspace,
            retained.generation_sha256,
            evidence.evidence_sha256,
            request_sha256,
            expires_at,
        );
        let receipt = ForecastPreparationReceipt {
            receipt_id,
            receipt_sha256,
            expires_at,
        };
        let observed_from = evidence
            .observed_history
            .first()
            .map(|point| point.observed_at())
            .ok_or(ForecastPreparationError::InvalidEvidence)?;
        let preview = ForecastPreparationPreview {
            model: ForecastModelSummary::from_requirement(&model),
            instrument_id: selection.instrument_id,
            observed_from,
            observed_through: evidence.observed_cutoff,
            available_at: evidence.available_at,
            observed_points: evidence.observed_history.len(),
            horizon: selection.horizon,
            validity_nanos: selection.validity_nanos,
            evidence_sha256: evidence.evidence_sha256,
            request_sha256,
            runtime_generation_sha256: retained.generation_sha256,
        };
        receipts.insert(
            receipt,
            StoredForecastPreparation {
                owner: origin,
                workspace,
                expires_at: expires_instant,
                runtime_generation_sha256: retained.generation_sha256,
                revalidation: Some(revalidation),
                request,
                retained_request_bytes,
            },
        )?;
        Ok(PreparedForecast {
            preview,
            receipt,
            product_identity,
        })
    }

    /// Consumes one matching receipt after revalidating model and analytical generations.
    pub(crate) async fn consume(
        &self,
        origin: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        receipt: ForecastPreparationReceipt,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<TypedToolRequest, ForecastPreparationError> {
        validate_origin(origin, workspace)?;
        check_control(deadline, &cancellation)?;
        let stored = self
            .receipts
            .lock()
            .map_err(|_| ForecastPreparationError::Unavailable)?
            .consume(receipt, origin, workspace, Instant::now())?;
        self.runtime
            .validate_forecast_runtime_generation(stored.runtime_generation_sha256)?;
        let revalidation = stored
            .revalidation
            .as_ref()
            .ok_or(ForecastPreparationError::InvalidEvidence)?;
        self.evidence
            .revalidate(revalidation, deadline, cancellation)
            .await?;
        Ok(stored.request)
    }

    /// Consumes one process-local opaque confirmation token without returning receipt evidence to
    /// a presentation client. Owner, workspace, expiry, model generation, and analytical evidence
    /// are still checked against the retained authority entry before the terminal request exists.
    pub(crate) async fn consume_token(
        &self,
        origin: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        receipt_id: Uuid,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<TypedToolRequest, ForecastPreparationError> {
        validate_origin(origin, workspace)?;
        check_control(deadline, &cancellation)?;
        let stored = self
            .receipts
            .lock()
            .map_err(|_| ForecastPreparationError::Unavailable)?
            .consume_token(receipt_id, origin, workspace, Instant::now())?;
        self.runtime
            .validate_forecast_runtime_generation(stored.runtime_generation_sha256)?;
        let revalidation = stored
            .revalidation
            .as_ref()
            .ok_or(ForecastPreparationError::InvalidEvidence)?;
        self.evidence
            .revalidate(revalidation, deadline, cancellation)
            .await?;
        Ok(stored.request)
    }
}

impl fmt::Debug for ForecastPreparationAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ForecastPreparationAuthority")
            .field("runtime", &"[ADMITTED MODEL RUNTIME]")
            .field("evidence", &self.evidence)
            .field("generate_descriptor", &self.generate_descriptor.name())
            .field("limits", &self.limits)
            .field("receipts", &"[ONE-USE RECEIPTS]")
            .finish()
    }
}

fn catalog_request(
    retained: &RetainedForecastRuntime,
    backup: &RetainedRuntimeBackup,
) -> Result<ForecastEvidenceCatalogRequest, ForecastPreparationError> {
    if Sha256Digest::new(Sha256::digest(backup.canonical_index.as_ref()).into())
        != retained.generation_sha256
        || backup.models.len() != retained.backends.len()
    {
        return Err(ForecastPreparationError::ModelUnavailable);
    }
    let mut models = Vec::new();
    models
        .try_reserve_exact(retained.backends.len())
        .map_err(|_| ForecastPreparationError::Capacity)?;
    for backend in &retained.backends {
        let metadata = backend.metadata();
        let mut matching = backup.models.iter().filter(|(_, bundle)| {
            let candidate = bundle.metadata();
            candidate.model_id() == metadata.model_id()
                && candidate.bundle_id() == metadata.bundle_id()
                && candidate.bundle_version() == metadata.bundle_version()
                && candidate.metadata_hash() == metadata.metadata_hash()
                && candidate.training_run_hash() == metadata.training_run_hash()
                && candidate.output_binding().identity() == metadata.output_binding().identity()
        });
        let (_, bundle) = matching
            .next()
            .ok_or(ForecastPreparationError::InvalidEvidence)?;
        if matching.next().is_some() {
            return Err(ForecastPreparationError::InvalidEvidence);
        }
        models.push(ForecastModelRequirement {
            runtime_generation_sha256: retained.generation_sha256,
            bundle: Arc::clone(bundle),
            product_evidence: forecast_model_evidence_projection(bundle)
                .map_err(|_| ForecastPreparationError::InvalidEvidence)?,
        });
    }
    Ok(ForecastEvidenceCatalogRequest {
        runtime_generation_sha256: retained.generation_sha256,
        models: models.into_boxed_slice(),
    })
}

fn validate_catalog(
    request: &ForecastEvidenceCatalogRequest,
    catalog: &ForecastEvidenceCatalogSnapshot,
) -> Result<(), ForecastPreparationError> {
    if catalog.authority_generation_sha256.bytes() == [0; 32] {
        return Err(ForecastPreparationError::InvalidEvidence);
    }
    for dataset in catalog.datasets() {
        let model = request.models.iter().find(|model| {
            let metadata = model.metadata();
            metadata.model_id() == dataset.model_id
                && metadata.bundle_id() == &dataset.bundle_id
                && metadata.bundle_version() == dataset.bundle_version
        });
        let Some(model) = model else {
            return Err(ForecastPreparationError::InvalidEvidence);
        };
        if model.runtime_generation_sha256 != request.runtime_generation_sha256 {
            return Err(ForecastPreparationError::InvalidEvidence);
        }
        if dataset.pairing.training() != model.metadata().dataset()
            || dataset.pairing.analysis_fence().as_of()
                != model.metadata().dataset().selection_as_of()
        {
            return Err(ForecastPreparationError::InvalidEvidence);
        }
    }
    Ok(())
}

fn compatible_option<'catalog>(
    catalog: &'catalog ForecastEvidenceCatalogSnapshot,
    selection: &ForecastPreparationSelection,
) -> Result<&'catalog ForecastEvidenceDataset, ForecastPreparationError> {
    catalog
        .datasets
        .iter()
        .find(|dataset| {
            dataset.model_id == selection.model_id
                && dataset.bundle_id == selection.bundle_id
                && dataset.bundle_version == selection.bundle_version
                && dataset.dataset().manifest() == &selection.dataset_manifest
                && dataset.analysis_manifest() == &selection.analysis_manifest
        })
        .ok_or(ForecastPreparationError::IncompatibleSelection)
}

fn validate_prepared_evidence(
    expected: &ForecastEvidenceMaterializationRequest,
    evidence: &PreparedForecastEvidence,
) -> Result<(), ForecastPreparationError> {
    if evidence.request.model.runtime_generation_sha256 != expected.model.runtime_generation_sha256
        || evidence.request.selection != expected.selection
        || evidence.request.pairing != expected.pairing
        || evidence.request.authority_generation_sha256 != expected.authority_generation_sha256
        || evidence.request.knowledge_cutoff != expected.knowledge_cutoff
        || evidence.request.macro_effective_date_cutoff != expected.macro_effective_date_cutoff
        || evidence.evidence_sha256
            != evidence_digest(
                &evidence.request,
                &evidence.serving_input,
                evidence.observed_cutoff,
                evidence.available_at,
                evidence.decimal_scale,
                &evidence.observed_history,
                &evidence.inputs,
            )?
    {
        return Err(ForecastPreparationError::InvalidEvidence);
    }
    validate_forecast_shape(
        expected.model.metadata(),
        &expected.selection,
        evidence.observed_cutoff,
        evidence.available_at,
        evidence.decimal_scale,
        &evidence.observed_history,
        &evidence.inputs,
    )
}

fn validate_forecast_shape(
    metadata: &ModelMetadata,
    selection: &ForecastPreparationSelection,
    observed_cutoff: Timestamp,
    available_at: Timestamp,
    decimal_scale: u8,
    observed_history: &[ForecastObservedPoint],
    inputs: &[Box<[f64]>],
) -> Result<(), ForecastPreparationError> {
    if metadata.model_id() != selection.model_id
        || metadata.bundle_id() != &selection.bundle_id
        || metadata.bundle_version() != selection.bundle_version
        || inputs.len() != usize::from(selection.horizon.points().get())
    {
        return Err(ForecastPreparationError::InvalidEvidence);
    }
    let mut rows = Vec::new();
    rows.try_reserve_exact(inputs.len())
        .map_err(|_| ForecastPreparationError::Capacity)?;
    for row in inputs {
        if row.len() != metadata.features().len() {
            return Err(ForecastPreparationError::InvalidEvidence);
        }
        let mut values = metadata
            .features()
            .iter()
            .map(ModelFeatureValue::from_binding)
            .collect::<Vec<_>>();
        for (slot, value) in values.iter_mut().zip(row.iter().copied()) {
            slot.try_set_value(value)
                .map_err(|_| ForecastPreparationError::InvalidEvidence)?;
        }
        rows.push(values.into_boxed_slice());
    }
    let model_inputs = rows
        .iter()
        .map(|row| {
            ModelInput::try_new(metadata, row)
                .map_err(|_| ForecastPreparationError::InvalidEvidence)
        })
        .collect::<Result<Vec<_>, _>>()?;
    ForecastRequest::try_new_with_observed_history(
        selection.instrument_id,
        observed_cutoff,
        available_at,
        selection.horizon,
        decimal_scale,
        observed_history,
        &model_inputs,
    )
    .map(|_| ())
    .map_err(|_| ForecastPreparationError::InvalidEvidence)
}

fn typed_arguments(
    evidence: &PreparedForecastEvidence,
    product_identity: &ForecastProductIdentity,
) -> Result<Map<String, Value>, ForecastPreparationError> {
    let selection = &evidence.request.selection;
    let observed = evidence
        .observed_history
        .iter()
        .map(|point| {
            json!({
                "observedAtUnixNanos": point.observed_at().unix_nanos(),
                "availableAtUnixNanos": point.available_at().unix_nanos(),
                "mantissa": point.value().mantissa().to_string(),
                "sourcePitHash": hex(point.source_pit_hash()),
                "quality": quality_name(point.quality()),
            })
        })
        .collect::<Vec<_>>();
    let inputs = evidence
        .inputs
        .iter()
        .map(|row| row.iter().map(|value| json!(value)).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let pairing = evidence.request.pairing();
    let analysis_manifest = pairing.analysis_fence().manifest();
    let serving = &evidence.serving_input;
    Ok(Map::from_iter([
        ("confirm".to_owned(), Value::Bool(true)),
        ("modelId".to_owned(), json!(selection.model_id.to_string())),
        (
            "resultLimits".to_owned(),
            json!({
                "maximumItems": market_squawk_modeling::MAX_FORECAST_POINTS,
                "maximumBytes": 4 * 1024 * 1024,
            }),
        ),
        (
            "request".to_owned(),
            json!({
                "instrumentId": selection.instrument_id.to_string(),
                "productIdentity": {
                    "displayName": product_identity.display_name(),
                    "canonicalSymbol": product_identity.canonical_symbol(),
                    "description": product_identity.description(),
                    "quoteCurrency": product_identity.quote_currency().as_str(),
                    "knowledgeAtUnixNanos": product_identity.knowledge_at().unix_nanos(),
                    "effectiveAtUnixNanos": product_identity.effective_at().unix_nanos(),
                },
                "modelEvidence": evidence.request.model.product_evidence().product_value(),
                "bundleId": selection.bundle_id.as_str(),
                "bundleVersion": selection.bundle_version.get(),
                "observedThroughUnixNanos": evidence.observed_cutoff.unix_nanos(),
                "availableAtUnixNanos": evidence.available_at.unix_nanos(),
                "horizonPoints": selection.horizon.points().get(),
                "horizonStepNanos": selection.horizon.step_nanos().get(),
                "decimalScale": evidence.decimal_scale,
                "validityNanos": selection.validity_nanos,
                "observedHistory": observed,
                "inputs": inputs,
                "analysisEvidence": {
                    "manifest": {
                        "dataset": analysis_manifest.dataset_id().as_str(),
                        "manifestVersion": analysis_manifest.manifest_version(),
                        "schema": {
                            "name": analysis_manifest.schema().name(),
                            "version": analysis_manifest.schema_version().get(),
                            "fingerprint": hex(Sha256Digest::new(
                                analysis_manifest.schema().fingerprint()
                            )),
                        },
                        "contentHash": hex(analysis_manifest.content_hash()),
                    },
                    "productionIdentitySha256": hex(pairing.analysis_production_identity()),
                    "productionReceiptSha256": hex(
                        pairing.analysis_production_receipt_sha256()
                    ),
                    "pairingSha256": hex(pairing.pairing_sha256()),
                },
                "servingEvidence": {
                    "manifest": {
                        "dataset": serving.manifest().dataset_id().as_str(),
                        "manifestVersion": serving.manifest().manifest_version(),
                        "schema": {
                            "name": serving.manifest().schema().name(),
                            "version": serving.manifest().schema_version().get(),
                            "fingerprint": hex(Sha256Digest::new(
                                serving.manifest().schema().fingerprint()
                            )),
                        },
                        "contentHash": hex(serving.manifest().content_hash()),
                    },
                    "sourceId": serving.source_id().as_str(),
                    "objectGraphSha256": hex(serving.object_graph_sha256()),
                    "selectionSha256": hex(serving.selection_sha256()),
                    "resultSha256": hex(serving.result_sha256()),
                    "knowledgeCutoffUnixNanos": serving.knowledge_cutoff().unix_nanos(),
                    "priorObservedAtUnixNanos": serving.prior_observed_at().unix_nanos(),
                    "observedThroughUnixNanos": serving.observed_through().unix_nanos(),
                    "featureSha256": hex(serving.feature_sha256()),
                },
            }),
        ),
    ]))
}

fn evidence_digest(
    request: &ForecastEvidenceMaterializationRequest,
    serving_input: &ForecastServingInputFence,
    observed_cutoff: Timestamp,
    available_at: Timestamp,
    decimal_scale: u8,
    observed_history: &[ForecastObservedPoint],
    inputs: &[Box<[f64]>],
) -> Result<Sha256Digest, ForecastEvidenceReadError> {
    let selection = &request.selection;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/forecast-preparation-evidence/v1\0");
    digest.update(request.model.runtime_generation_sha256.bytes());
    digest.update(request.authority_generation_sha256.bytes());
    digest.update(request.knowledge_cutoff.unix_nanos().to_be_bytes());
    digest.update(request.macro_effective_date_cutoff.year().to_be_bytes());
    digest.update([
        request.macro_effective_date_cutoff.month(),
        request.macro_effective_date_cutoff.day(),
    ]);
    digest.update(selection.model_id.as_uuid().as_bytes());
    hash_bytes(&mut digest, selection.bundle_id.as_str().as_bytes())?;
    digest.update(selection.bundle_version.get().to_be_bytes());
    hash_manifest(&mut digest, &selection.dataset_manifest)?;
    hash_manifest(&mut digest, &selection.analysis_manifest)?;
    digest.update(request.pairing.pairing_sha256().bytes());
    hash_serving_input(&mut digest, serving_input)?;
    digest.update(selection.instrument_id.as_uuid().as_bytes());
    digest.update(selection.horizon.points().get().to_be_bytes());
    digest.update(selection.horizon.step_nanos().get().to_be_bytes());
    digest.update(selection.validity_nanos.to_be_bytes());
    digest.update(observed_cutoff.unix_nanos().to_be_bytes());
    digest.update(available_at.unix_nanos().to_be_bytes());
    digest.update([decimal_scale]);
    hash_len(&mut digest, observed_history.len())?;
    for point in observed_history {
        digest.update(point.observed_at().unix_nanos().to_be_bytes());
        digest.update(point.available_at().unix_nanos().to_be_bytes());
        digest.update(point.value().mantissa().to_be_bytes());
        digest.update([point.value().scale(), quality_tag(point.quality())]);
        digest.update(point.source_pit_hash().bytes());
    }
    hash_len(&mut digest, inputs.len())?;
    for row in inputs {
        hash_len(&mut digest, row.len())?;
        for value in row {
            digest.update(value.to_bits().to_be_bytes());
        }
    }
    Ok(Sha256Digest::new(digest.finalize().into()))
}

fn hash_serving_input(
    digest: &mut Sha256,
    serving_input: &ForecastServingInputFence,
) -> Result<(), ForecastEvidenceReadError> {
    hash_manifest(digest, serving_input.manifest())?;
    hash_bytes(digest, serving_input.source_id().as_str().as_bytes())?;
    digest.update(serving_input.object_graph_sha256().bytes());
    digest.update(serving_input.selection_sha256().bytes());
    digest.update(serving_input.result_sha256().bytes());
    digest.update(serving_input.knowledge_cutoff().unix_nanos().to_be_bytes());
    digest.update(serving_input.prior_observed_at().unix_nanos().to_be_bytes());
    digest.update(serving_input.observed_through().unix_nanos().to_be_bytes());
    digest.update(serving_input.feature_sha256().bytes());
    Ok(())
}

fn pairing_digest(
    training: &TrainingDatasetIdentity,
    analysis: &TrainingDatasetIdentity,
    training_fence: &ForecastDatasetEvidenceFence,
    analysis_fence: &ForecastDatasetEvidenceFence,
    training_production_identity: Sha256Digest,
    training_production_receipt_sha256: Sha256Digest,
    analysis_production_identity: Sha256Digest,
    analysis_production_receipt_sha256: Sha256Digest,
    fixed_horizon_nanos: NonZeroU64,
    shared_compatibility_sha256: Sha256Digest,
) -> Result<Sha256Digest, ForecastEvidenceReadError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/forecast-training-analysis-pairing/v2\0");
    hash_dataset_identity(&mut digest, training)?;
    hash_dataset_identity(&mut digest, analysis)?;
    hash_evidence_fence(&mut digest, training_fence)?;
    hash_evidence_fence(&mut digest, analysis_fence)?;
    digest.update(training_production_identity.bytes());
    digest.update(training_production_receipt_sha256.bytes());
    digest.update(analysis_production_identity.bytes());
    digest.update(analysis_production_receipt_sha256.bytes());
    digest.update(fixed_horizon_nanos.get().to_be_bytes());
    digest.update(shared_compatibility_sha256.bytes());
    Ok(Sha256Digest::new(digest.finalize().into()))
}

fn hash_dataset_identity(
    digest: &mut Sha256,
    identity: &TrainingDatasetIdentity,
) -> Result<(), ForecastEvidenceReadError> {
    hash_manifest(digest, identity.manifest())?;
    digest.update(identity.build_spec_digest().digest().bytes());
    digest.update(identity.universe_digest().bytes());
    digest.update(identity.policy_digest().bytes());
    digest.update(identity.catalog_identity().bytes());
    digest.update(identity.export_digest().bytes());
    digest.update(identity.selection_digest().bytes());
    digest.update(identity.selection_as_of().unix_nanos().to_be_bytes());
    digest.update(identity.selected_component_rows().get().to_be_bytes());
    Ok(())
}

fn hash_evidence_fence(
    digest: &mut Sha256,
    fence: &ForecastDatasetEvidenceFence,
) -> Result<(), ForecastEvidenceReadError> {
    hash_manifest(digest, fence.manifest())?;
    digest.update(fence.catalog_identity().bytes());
    digest.update(fence.export_sha256().bytes());
    digest.update(fence.selection_sha256().bytes());
    digest.update(fence.selected_rows().get().to_be_bytes());
    digest.update(fence.as_of().unix_nanos().to_be_bytes());
    Ok(())
}

fn request_digest(
    request: &TypedToolRequest,
) -> Result<(Sha256Digest, usize), ForecastPreparationError> {
    let encoded = serde_json::to_vec(request.arguments())
        .map_err(|_| ForecastPreparationError::InvalidEvidence)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/descriptor-admitted-forecast-request/v1\0");
    hash_bytes(&mut digest, request.name().as_bytes())?;
    hash_bytes(&mut digest, request.version().as_bytes())?;
    hash_bytes(&mut digest, &encoded)?;
    Ok((Sha256Digest::new(digest.finalize().into()), encoded.len()))
}

#[allow(
    clippy::too_many_arguments,
    reason = "receipt identity binds every independent authority fence"
)]
fn receipt_digest(
    receipt_id: Uuid,
    origin: RequestOrigin,
    workspace: WorkspaceRuntimeIdentity,
    runtime_generation_sha256: Sha256Digest,
    evidence_sha256: Sha256Digest,
    request_sha256: Sha256Digest,
    expires_at: Timestamp,
) -> Sha256Digest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/forecast-preparation-receipt/v1\0");
    digest.update(receipt_id.as_bytes());
    digest.update(origin.workspace_id().as_bytes());
    digest.update(origin.client_id().as_bytes());
    digest.update(workspace.workspace_id().as_uuid().as_bytes());
    digest.update(workspace.generation().get().to_be_bytes());
    digest.update(runtime_generation_sha256.bytes());
    digest.update(evidence_sha256.bytes());
    digest.update(request_sha256.bytes());
    digest.update(expires_at.unix_nanos().to_be_bytes());
    Sha256Digest::new(digest.finalize().into())
}

fn unique_receipt_id(registry: &ReceiptRegistry) -> Result<Uuid, ForecastPreparationError> {
    for _ in 0..RECEIPT_ID_ATTEMPTS {
        let candidate = Uuid::new_v4();
        if !registry.entries.contains_key(&candidate) {
            return Ok(candidate);
        }
    }
    Err(ForecastPreparationError::Unavailable)
}

fn validate_origin(
    origin: RequestOrigin,
    workspace: WorkspaceRuntimeIdentity,
) -> Result<(), ForecastPreparationError> {
    if origin.workspace_id() != workspace.workspace_id().as_uuid() {
        Err(ForecastPreparationError::ReceiptMismatch)
    } else {
        Ok(())
    }
}

fn check_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ForecastPreparationError> {
    if cancellation.is_cancelled() {
        Err(ForecastPreparationError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ForecastPreparationError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn wall_now() -> Result<Timestamp, ForecastPreparationError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ForecastPreparationError::TimeUnavailable)?
        .as_nanos();
    i64::try_from(nanos)
        .map(Timestamp::from_unix_nanos)
        .map_err(|_| ForecastPreparationError::TimeUnavailable)
}

fn hash_manifest(
    digest: &mut Sha256,
    manifest: &DatasetManifestRef,
) -> Result<(), ForecastEvidenceReadError> {
    hash_bytes(digest, manifest.dataset_id().as_str().as_bytes())?;
    digest.update(manifest.manifest_version().to_be_bytes());
    hash_bytes(digest, manifest.schema().name().as_bytes())?;
    digest.update(manifest.schema_version().get().to_be_bytes());
    digest.update(manifest.schema().fingerprint());
    digest.update(manifest.content_hash().bytes());
    Ok(())
}

fn hash_bytes(digest: &mut Sha256, value: &[u8]) -> Result<(), ForecastEvidenceReadError> {
    hash_len(digest, value.len())?;
    digest.update(value);
    Ok(())
}

fn hash_len(digest: &mut Sha256, value: usize) -> Result<(), ForecastEvidenceReadError> {
    digest.update(
        u64::try_from(value)
            .map_err(|_| ForecastEvidenceReadError::Capacity)?
            .to_be_bytes(),
    );
    Ok(())
}

fn hex(value: Sha256Digest) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in value.bytes() {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

const fn quality_tag(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}

const fn quality_name(quality: DataQuality) -> &'static str {
    match quality {
        DataQuality::DirectVerified => "direct_verified",
        DataQuality::DirectUnverified => "direct_unverified",
        DataQuality::OfficialDelayed => "official_delayed",
        DataQuality::Aggregated => "aggregated",
        DataQuality::Indicative => "indicative",
        DataQuality::Modeled => "modeled",
        DataQuality::Estimated => "estimated",
        DataQuality::Stale => "stale",
        DataQuality::Quarantined => "quarantined",
    }
}

impl From<ForecastEvidenceReadError> for ForecastPreparationError {
    fn from(error: ForecastEvidenceReadError) -> Self {
        match error {
            ForecastEvidenceReadError::Cancelled => Self::Cancelled,
            ForecastEvidenceReadError::DeadlineExceeded => Self::DeadlineExceeded,
            ForecastEvidenceReadError::Capacity => Self::Capacity,
            ForecastEvidenceReadError::InvalidEvidence => Self::InvalidEvidence,
            ForecastEvidenceReadError::Unavailable => Self::Unavailable,
        }
    }
}

impl From<ProductionModelRuntimeError> for ForecastPreparationError {
    fn from(_: ProductionModelRuntimeError) -> Self {
        Self::ModelUnavailable
    }
}

/// Closed failure classes returned by the injected analytical evidence owner.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ForecastEvidenceReadError {
    #[error("forecast evidence read was cancelled")]
    Cancelled,
    #[error("forecast evidence read deadline elapsed")]
    DeadlineExceeded,
    #[error("forecast evidence resource ceiling was exceeded")]
    Capacity,
    #[error("forecast evidence violated its typed contract")]
    InvalidEvidence,
    #[error("forecast evidence authority is unavailable")]
    Unavailable,
}

/// Forecast preparation, fencing, or one-use receipt failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ForecastPreparationError {
    #[error("forecast preparation limits are invalid")]
    InvalidLimits,
    #[error("forecast generation descriptor is invalid")]
    InvalidDescriptor,
    #[error("forecast selection is invalid")]
    InvalidSelection,
    #[error("admitted model generation is unavailable")]
    ModelUnavailable,
    #[error("forecast selection is not compatible with admitted evidence")]
    IncompatibleSelection,
    #[error("forecast evidence is invalid")]
    InvalidEvidence,
    #[error("forecast preparation receipt is unavailable")]
    ReceiptUnavailable,
    #[error("forecast preparation receipt binding differs")]
    ReceiptMismatch,
    #[error("forecast preparation capacity was exceeded")]
    Capacity,
    #[error("forecast preparation was cancelled")]
    Cancelled,
    #[error("forecast preparation deadline elapsed")]
    DeadlineExceeded,
    #[error("forecast preparation clock is unavailable")]
    TimeUnavailable,
    #[error("forecast preparation authority is unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use market_squawk_domain::Timestamp;
    use market_squawk_runtime::WorkspaceId;
    use market_squawk_services::RequestOrigin;
    use serde_json::{Map, Value, json};
    use uuid::Uuid;

    use super::{ForecastPreparationReceipt, ReceiptRegistry, StoredForecastPreparation};
    use crate::application::{
        contracts::application_capabilities, lifecycle::WorkspaceRuntimeIdentity,
    };

    #[test]
    fn an_exact_forecast_preparation_can_be_consumed_only_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let workspace_id = WorkspaceId::try_from_uuid(Uuid::new_v4())?;
        let workspace = WorkspaceRuntimeIdentity::try_new(workspace_id, 7)?;
        let origin = RequestOrigin::try_new(workspace_id.as_uuid(), Uuid::new_v4())?;
        let descriptor = application_capabilities()?
            .find("Model.GenerateForecast")
            .ok_or("forecast descriptor")?
            .clone();
        let request = descriptor.admit(Map::from_iter([
            ("confirm".to_owned(), Value::Bool(true)),
            ("modelId".to_owned(), json!(Uuid::new_v4())),
            (
                "resultLimits".to_owned(),
                json!({
                    "maximumItems": market_squawk_modeling::MAX_FORECAST_POINTS,
                    "maximumBytes": 4 * 1024 * 1024,
                }),
            ),
            (
                "request".to_owned(),
                json!({
                    "instrumentId": Uuid::new_v4(),
                    "bundleId": "fixture-bundle",
                    "bundleVersion": 1,
                    "observedThroughUnixNanos": 10,
                    "availableAtUnixNanos": 10,
                    "horizonPoints": 1,
                    "horizonStepNanos": 1,
                    "decimalScale": 2,
                    "validityNanos": 1,
                    "observedHistory": [{
                        "observedAtUnixNanos": 10,
                        "availableAtUnixNanos": 10,
                        "mantissa": "100",
                        "sourcePitHash": "0101010101010101010101010101010101010101010101010101010101010101",
                        "quality": "official_delayed"
                    }],
                    "inputs": [[1.0]]
                }),
            ),
        ]))?;
        let receipt = ForecastPreparationReceipt::fixture(
            Uuid::new_v4(),
            [9; 32],
            Timestamp::from_unix_nanos(20),
        );
        let mut registry = ReceiptRegistry::new(1, 1024);
        registry.insert(
            receipt,
            StoredForecastPreparation::fixture(
                origin,
                workspace,
                Instant::now() + Duration::from_secs(1),
                request,
            ),
        )?;

        let retained = registry.consume(receipt, origin, workspace, Instant::now())?;
        assert_eq!(retained.request.name(), "Model.GenerateForecast");
        assert!(
            registry
                .consume(receipt, origin, workspace, Instant::now())
                .is_err()
        );
        Ok(())
    }
}
