//! Authority-derived forecast preparation and one-use job-admission receipts.

use std::{
    collections::BTreeMap,
    fmt,
    num::{NonZeroU16, NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use market_squawk_data::{DatasetManifestRef, Sha256Digest};
use market_squawk_domain::{DataQuality, InstrumentId, ModelId, Timestamp};
use market_squawk_modeling::{
    BundleId, ForecastHorizon, ForecastObservedPoint, ForecastRequest, ModelFeatureValue,
    ModelFormat, ModelInput, ModelMetadata, ModelOutputSemantics, TrainingDatasetIdentity,
};
use market_squawk_services::{RequestOrigin, ServiceDomain, ToolDescriptor, TypedToolRequest};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    forecast::GENERATE_FORECAST,
    runtime::{ProductionModelRuntime, ProductionModelRuntimeError, RetainedForecastRuntime},
};
use crate::application::lifecycle::WorkspaceRuntimeIdentity;

const MAXIMUM_RECEIPTS: usize = 256;
const MAXIMUM_RECEIPT_LIFETIME: Duration = Duration::from_secs(15 * 60);
const MAXIMUM_RETAINED_REQUEST_BYTES: usize = 256 * 1024 * 1024;
const MAXIMUM_SINGLE_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_FORECAST_VALIDITY_NANOS: u64 = 30 * 24 * 60 * 60 * 1_000_000_000;
const RECEIPT_ID_ATTEMPTS: usize = 16;

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
    metadata: Arc<ModelMetadata>,
}

impl ForecastModelRequirement {
    /// Returns the model-runtime generation from which this requirement was retained.
    pub(crate) const fn runtime_generation_sha256(&self) -> Sha256Digest {
        self.runtime_generation_sha256
    }

    /// Returns the complete admitted model, feature, label, and dataset contract.
    pub(crate) const fn metadata(&self) -> &Arc<ModelMetadata> {
        &self.metadata
    }

    fn matches_coordinate(&self, selection: &ForecastPreparationSelection) -> bool {
        self.metadata.model_id() == selection.model_id
            && self.metadata.bundle_id() == &selection.bundle_id
            && self.metadata.bundle_version() == selection.bundle_version
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

/// One exact admitted feature dataset and its data-owner-derived availability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForecastEvidenceDataset {
    model_id: ModelId,
    bundle_id: BundleId,
    bundle_version: NonZeroU64,
    dataset: TrainingDatasetIdentity,
    instruments: Box<[ForecastInstrumentAvailability]>,
    policies: Box<[ForecastEvidencePolicy]>,
}

impl ForecastEvidenceDataset {
    /// Constructs a canonical nonempty compatibility record.
    pub(crate) fn try_new(
        model_id: ModelId,
        bundle_id: BundleId,
        bundle_version: NonZeroU64,
        dataset: TrainingDatasetIdentity,
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
            dataset,
            instruments: instruments.into_boxed_slice(),
            policies: policies.into_boxed_slice(),
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

    pub(crate) const fn dataset(&self) -> &TrainingDatasetIdentity {
        &self.dataset
    }

    pub(crate) fn instruments(&self) -> &[ForecastInstrumentAvailability] {
        &self.instruments
    }

    pub(crate) fn policies(&self) -> &[ForecastEvidencePolicy] {
        &self.policies
    }
}

/// Data-owner result for one coherent compatible-dataset catalog read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForecastEvidenceCatalogSnapshot {
    authority_generation_sha256: Sha256Digest,
    datasets: Box<[ForecastEvidenceDataset]>,
}

impl ForecastEvidenceCatalogSnapshot {
    /// Constructs a catalog snapshot bound to a nonzero data-owner generation.
    pub(crate) fn try_new(
        authority_generation_sha256: Sha256Digest,
        datasets: Vec<ForecastEvidenceDataset>,
    ) -> Result<Self, ForecastEvidenceReadError> {
        if authority_generation_sha256.bytes() == [0; 32] {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        Ok(Self {
            authority_generation_sha256,
            datasets: datasets.into_boxed_slice(),
        })
    }

    pub(crate) fn datasets(&self) -> &[ForecastEvidenceDataset] {
        &self.datasets
    }
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
            instrument_id,
            horizon,
            validity_nanos,
        })
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
    authority_generation_sha256: Sha256Digest,
}

impl ForecastEvidenceMaterializationRequest {
    pub(crate) const fn model(&self) -> &ForecastModelRequirement {
        &self.model
    }

    pub(crate) const fn selection(&self) -> &ForecastPreparationSelection {
        &self.selection
    }

    pub(crate) const fn authority_generation_sha256(&self) -> Sha256Digest {
        self.authority_generation_sha256
    }
}

/// Typed history and feature matrix produced only by the injected analytical authority.
pub(crate) struct PreparedForecastEvidence {
    request: ForecastEvidenceMaterializationRequest,
    observed_cutoff: Timestamp,
    available_at: Timestamp,
    decimal_scale: u8,
    observed_history: Box<[ForecastObservedPoint]>,
    inputs: Box<[Box<[f64]>]>,
    evidence_sha256: Sha256Digest,
}

impl PreparedForecastEvidence {
    /// Validates and binds the exact analytical evidence returned by the data owner.
    pub(crate) fn try_new(
        request: ForecastEvidenceMaterializationRequest,
        observed_cutoff: Timestamp,
        available_at: Timestamp,
        decimal_scale: u8,
        observed_history: Vec<ForecastObservedPoint>,
        inputs: Vec<Box<[f64]>>,
    ) -> Result<Self, ForecastEvidenceReadError> {
        if request.authority_generation_sha256.bytes() == [0; 32]
            || inputs.len() != usize::from(request.selection.horizon.points().get())
            || inputs.iter().any(|row| {
                row.len() != request.model.metadata.features().len()
                    || row.iter().any(|value| !value.is_finite())
            })
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        validate_forecast_shape(
            request.model.metadata.as_ref(),
            &request.selection,
            observed_cutoff,
            available_at,
            decimal_scale,
            &observed_history,
            &inputs,
        )
        .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)?;
        let evidence_sha256 = evidence_digest(
            &request,
            observed_cutoff,
            available_at,
            decimal_scale,
            &observed_history,
            &inputs,
        )?;
        Ok(Self {
            request,
            observed_cutoff,
            available_at,
            decimal_scale,
            observed_history: observed_history.into_boxed_slice(),
            inputs: inputs.into_boxed_slice(),
            evidence_sha256,
        })
    }

    /// Returns the canonical digest over the exact prepared history and feature matrix.
    pub(crate) const fn evidence_sha256(&self) -> Sha256Digest {
        self.evidence_sha256
    }
}

impl fmt::Debug for PreparedForecastEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedForecastEvidence")
            .field("request", &self.request)
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
    evidence_sha256: Sha256Digest,
}

impl ForecastEvidenceRevalidation {
    pub(crate) const fn request(&self) -> &ForecastEvidenceMaterializationRequest {
        &self.request
    }

    pub(crate) const fn evidence_sha256(&self) -> Sha256Digest {
        self.evidence_sha256
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
    calibrated_intervals: bool,
    format: ModelFormat,
    output_semantics: ModelOutputSemantics,
    intended_use: Box<str>,
    limitations: Box<[Box<str>]>,
    fallback_reason: Box<str>,
}

impl ForecastModelSummary {
    fn from_requirement(requirement: &ForecastModelRequirement) -> Self {
        let metadata = requirement.metadata.as_ref();
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
            calibrated_intervals: metadata.forecast_calibration().is_some(),
            format: metadata.format(),
            output_semantics: metadata.output_semantics(),
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
        self.calibrated_intervals
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
        let request = catalog_request(&retained);
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
    pub(crate) async fn prepare(
        &self,
        origin: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        selection: ForecastPreparationSelection,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedForecast, ForecastPreparationError> {
        validate_origin(origin, workspace)?;
        check_control(deadline, &cancellation)?;
        let retained = self.runtime.retain_forecast_runtime()?;
        let catalog_request = catalog_request(&retained);
        let model = catalog_request
            .models
            .iter()
            .find(|candidate| candidate.matches_coordinate(&selection))
            .cloned()
            .ok_or(ForecastPreparationError::ModelUnavailable)?;
        if model.metadata.dataset().manifest() != &selection.dataset_manifest {
            return Err(ForecastPreparationError::IncompatibleSelection);
        }
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
        if instrument.observed_points.get() < policy.minimum_observed_points.get() {
            return Err(ForecastPreparationError::IncompatibleSelection);
        }
        let materialization = ForecastEvidenceMaterializationRequest {
            model: model.clone(),
            selection: selection.clone(),
            authority_generation_sha256: catalog.authority_generation_sha256,
        };
        let evidence = self
            .evidence
            .prepare(materialization.clone(), deadline, cancellation)
            .await?;
        validate_prepared_evidence(&materialization, &evidence)?;
        let request = self
            .generate_descriptor
            .admit(typed_arguments(&evidence)?)
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
        Ok(PreparedForecast { preview, receipt })
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

fn catalog_request(retained: &RetainedForecastRuntime) -> ForecastEvidenceCatalogRequest {
    let models = retained
        .backends
        .iter()
        .map(|backend| ForecastModelRequirement {
            runtime_generation_sha256: retained.generation_sha256,
            metadata: Arc::new(backend.metadata().clone()),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    ForecastEvidenceCatalogRequest {
        runtime_generation_sha256: retained.generation_sha256,
        models,
    }
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
            let metadata = model.metadata.as_ref();
            metadata.model_id() == dataset.model_id
                && metadata.bundle_id() == &dataset.bundle_id
                && metadata.bundle_version() == dataset.bundle_version
        });
        let Some(model) = model else {
            return Err(ForecastPreparationError::InvalidEvidence);
        };
        if model.runtime_generation_sha256 != request.runtime_generation_sha256
            || model.metadata.dataset() != &dataset.dataset
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
                && dataset.dataset.manifest() == &selection.dataset_manifest
        })
        .ok_or(ForecastPreparationError::IncompatibleSelection)
}

fn validate_prepared_evidence(
    expected: &ForecastEvidenceMaterializationRequest,
    evidence: &PreparedForecastEvidence,
) -> Result<(), ForecastPreparationError> {
    if evidence.request.model.runtime_generation_sha256 != expected.model.runtime_generation_sha256
        || evidence.request.selection != expected.selection
        || evidence.request.authority_generation_sha256 != expected.authority_generation_sha256
        || evidence.evidence_sha256
            != evidence_digest(
                &evidence.request,
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
        expected.model.metadata.as_ref(),
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
        || metadata.dataset().manifest() != &selection.dataset_manifest
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
            }),
        ),
    ]))
}

fn evidence_digest(
    request: &ForecastEvidenceMaterializationRequest,
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
    digest.update(selection.model_id.as_uuid().as_bytes());
    hash_bytes(&mut digest, selection.bundle_id.as_str().as_bytes())?;
    digest.update(selection.bundle_version.get().to_be_bytes());
    hash_manifest(&mut digest, &selection.dataset_manifest)?;
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
            ForecastEvidenceReadError::NotFound => Self::IncompatibleSelection,
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
    #[error("compatible forecast evidence was not found")]
    NotFound,
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
