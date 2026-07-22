//! Immutable model, feature, dataset, and decision metadata.

use std::mem::size_of;
use std::num::NonZeroU64;

use market_squawk_analytics::{FeatureInputSchemaDigest, FeatureKey, FeatureSemanticDigest};
use market_squawk_data::{
    CatalogEndpointIdentity, ComponentKind, DatasetBuildSpecDigest, DatasetManifestRef,
    FeatureLabelComponentSpec, Sha256Digest, UniverseId,
};
use market_squawk_domain::{ModelId, Timestamp};
use thiserror::Error;

/// Maximum features consumed by one native model.
pub const MAX_MODEL_FEATURES: usize = 1_024;
/// Maximum bytes in a stable bundle identity.
pub const MAX_BUNDLE_ID_BYTES: usize = 128;
/// Maximum bytes in a training-code revision.
pub const MAX_TRAINING_CODE_REVISION_BYTES: usize = 128;

/// Stable model-bundle series identity; a version selects one immutable generation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BundleId(Box<str>);

impl BundleId {
    /// Constructs a bounded lowercase identity.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, or noncanonical identities.
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, ModelMetadataError> {
        let value = value.as_ref();
        if !canonical_identifier(value, MAX_BUNDLE_ID_BYTES) {
            return Err(ModelMetadataError::InvalidBundleId);
        }
        Ok(Self(value.into()))
    }

    /// Returns the canonical bundle identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Closed native artifact family admitted by this release.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ModelFormat {
    /// One-output deterministic affine model.
    NativeLinear,
    /// One-output deterministic affine model with a logistic link.
    NativeLogistic,
}

/// Validated feature normalization performed immediately before native arithmetic.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FeatureNormalizer {
    /// Preserve the admitted finite value exactly.
    Identity,
    /// Apply `(value - mean) / scale` with a finite positive scale.
    Standard { mean: f64, scale: f64 },
}

impl FeatureNormalizer {
    pub(crate) fn standard(mean: f64, scale: f64) -> Result<Self, ModelMetadataError> {
        if !mean.is_finite() || !scale.is_finite() || scale <= 0.0 {
            return Err(ModelMetadataError::InvalidNormalizer);
        }
        Ok(Self::Standard { mean, scale })
    }

    pub(crate) fn normalize(self, value: f64) -> Option<f64> {
        let normalized = match self {
            Self::Identity => value,
            Self::Standard { mean, scale } => (value - mean) / scale,
        };
        normalized.is_finite().then_some(normalized)
    }
}

/// Exact Task 12 feature contract bound to one artifact coefficient.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelFeatureBinding {
    key: FeatureKey,
    input_schema_digest: FeatureInputSchemaDigest,
    semantic_digest: FeatureSemanticDigest,
    normalizer: FeatureNormalizer,
}

impl ModelFeatureBinding {
    pub(crate) const fn new(
        key: FeatureKey,
        input_schema_digest: FeatureInputSchemaDigest,
        semantic_digest: FeatureSemanticDigest,
        normalizer: FeatureNormalizer,
    ) -> Self {
        Self {
            key,
            input_schema_digest,
            semantic_digest,
            normalizer,
        }
    }

    /// Returns the exact feature name and version.
    #[must_use]
    pub const fn key(&self) -> &FeatureKey {
        &self.key
    }

    /// Returns the ordered Task 12 input-schema identity.
    #[must_use]
    pub const fn input_schema_digest(&self) -> FeatureInputSchemaDigest {
        self.input_schema_digest
    }

    /// Returns the complete Task 12 semantic identity.
    #[must_use]
    pub const fn semantic_digest(&self) -> FeatureSemanticDigest {
        self.semantic_digest
    }

    /// Returns the validated normalization contract.
    #[must_use]
    pub const fn normalizer(&self) -> FeatureNormalizer {
        self.normalizer
    }

    fn retained_bytes(&self) -> Option<usize> {
        size_of::<Self>().checked_add(self.key.name().len())
    }
}

/// Exact Task 11 immutable dataset generation and build-contract identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrainingDatasetIdentity {
    manifest: DatasetManifestRef,
    build_spec_digest: DatasetBuildSpecDigest,
    universe_digest: Sha256Digest,
    policy_digest: Sha256Digest,
    catalog_identity: CatalogEndpointIdentity,
    export_digest: Sha256Digest,
    selection_digest: Sha256Digest,
    selection_as_of: Timestamp,
    selected_component_rows: NonZeroU64,
}

impl TrainingDatasetIdentity {
    /// Binds a model to one immutable feature/label generation and its canonical build contracts.
    ///
    /// # Errors
    ///
    /// Rejects reserved zero identities.
    #[allow(
        clippy::too_many_arguments,
        reason = "all independent catalog, generation, selection, and temporal identities remain explicit"
    )]
    pub fn try_new(
        manifest: DatasetManifestRef,
        build_spec_digest: DatasetBuildSpecDigest,
        universe_digest: Sha256Digest,
        policy_digest: Sha256Digest,
        catalog_identity: CatalogEndpointIdentity,
        export_digest: Sha256Digest,
        selection_digest: Sha256Digest,
        selection_as_of: Timestamp,
        selected_component_rows: NonZeroU64,
    ) -> Result<Self, ModelMetadataError> {
        if manifest.content_hash().bytes() == [0; 32]
            || universe_digest.bytes() == [0; 32]
            || policy_digest.bytes() == [0; 32]
            || export_digest.bytes() == [0; 32]
            || selection_digest.bytes() == [0; 32]
        {
            return Err(ModelMetadataError::ReservedDigest);
        }
        Ok(Self {
            manifest,
            build_spec_digest,
            universe_digest,
            policy_digest,
            catalog_identity,
            export_digest,
            selection_digest,
            selection_as_of,
            selected_component_rows,
        })
    }

    /// Returns the exact immutable dataset generation.
    #[must_use]
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the complete Task 11 dataset-build specification identity.
    #[must_use]
    pub const fn build_spec_digest(&self) -> DatasetBuildSpecDigest {
        self.build_spec_digest
    }

    /// Returns the exact historical-universe contract identity.
    #[must_use]
    pub const fn universe_digest(&self) -> Sha256Digest {
        self.universe_digest
    }

    /// Returns the exact point-in-time and adjustment policy identity.
    #[must_use]
    pub const fn policy_digest(&self) -> Sha256Digest {
        self.policy_digest
    }

    /// Returns the operator-selected exact catalog endpoint identity.
    #[must_use]
    pub const fn catalog_identity(&self) -> CatalogEndpointIdentity {
        self.catalog_identity
    }

    /// Returns the producer-registered Task 11 export identity.
    #[must_use]
    pub const fn export_digest(&self) -> Sha256Digest {
        self.export_digest
    }

    /// Returns the independently rederived selected-row identity.
    #[must_use]
    pub const fn selection_digest(&self) -> Sha256Digest {
        self.selection_digest
    }

    /// Returns the exact point-in-time cutoff bound into the selected-row identity.
    #[must_use]
    pub const fn selection_as_of(&self) -> Timestamp {
        self.selection_as_of
    }

    /// Returns the exact selected component-row count.
    #[must_use]
    pub const fn selected_component_rows(&self) -> NonZeroU64 {
        self.selected_component_rows
    }

    pub(crate) fn retained_bytes(&self) -> Option<usize> {
        size_of::<Self>()
            .checked_add(self.manifest.dataset_id().as_str().len())?
            .checked_add(self.manifest.schema().name().len())
    }
}

/// Closed-open training observation interval represented by exact UTC nanoseconds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrainingPeriod {
    start: Timestamp,
    end: Timestamp,
}

impl TrainingPeriod {
    /// Constructs a nonempty ordered training period.
    ///
    /// # Errors
    ///
    /// Rejects an end at or before the start.
    pub fn try_new(start: Timestamp, end: Timestamp) -> Result<Self, ModelMetadataError> {
        if end <= start {
            return Err(ModelMetadataError::InvalidTrainingPeriod);
        }
        Ok(Self { start, end })
    }

    /// Returns the inclusive training start.
    #[must_use]
    pub const fn start(self) -> Timestamp {
        self.start
    }

    /// Returns the exclusive training end.
    #[must_use]
    pub const fn end(self) -> Timestamp {
        self.end
    }
}

/// Trusted, caller-supplied relationships against which untrusted bundle bytes are admitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BundleExpectations {
    model_id: ModelId,
    bundle_id: BundleId,
    bundle_version: NonZeroU64,
    dataset: TrainingDatasetIdentity,
    universe_id: UniverseId,
    training_period: TrainingPeriod,
    label: FeatureLabelComponentSpec,
    training_code_revision: Box<str>,
    bundle_metadata_hash: Sha256Digest,
    artifact_hash: Sha256Digest,
    training_run_hash: Sha256Digest,
}

impl BundleExpectations {
    /// Constructs complete independent admission expectations.
    ///
    /// # Errors
    ///
    /// Rejects a non-label component or invalid code revision.
    #[allow(
        clippy::too_many_arguments,
        reason = "independent reproducibility identities remain explicit"
    )]
    pub fn try_new(
        model_id: ModelId,
        bundle_id: BundleId,
        bundle_version: NonZeroU64,
        dataset: TrainingDatasetIdentity,
        universe_id: UniverseId,
        training_period: TrainingPeriod,
        label: FeatureLabelComponentSpec,
        training_code_revision: impl AsRef<str>,
        bundle_metadata_hash: Sha256Digest,
        artifact_hash: Sha256Digest,
        training_run_hash: Sha256Digest,
    ) -> Result<Self, ModelMetadataError> {
        let training_code_revision = training_code_revision.as_ref();
        if label.kind() != ComponentKind::Label
            || !valid_revision(training_code_revision)
            || bundle_metadata_hash.bytes() == [0; 32]
            || artifact_hash.bytes() == [0; 32]
            || training_run_hash.bytes() == [0; 32]
        {
            return Err(ModelMetadataError::InvalidExpectations);
        }
        Ok(Self {
            model_id,
            bundle_id,
            bundle_version,
            dataset,
            universe_id,
            training_period,
            label,
            training_code_revision: training_code_revision.into(),
            bundle_metadata_hash,
            artifact_hash,
            training_run_hash,
        })
    }

    /// Returns the expected stable model identity.
    #[must_use]
    pub const fn model_id(&self) -> ModelId {
        self.model_id
    }

    /// Returns the expected bundle series.
    #[must_use]
    pub const fn bundle_id(&self) -> &BundleId {
        &self.bundle_id
    }

    /// Returns the expected immutable bundle version.
    #[must_use]
    pub const fn bundle_version(&self) -> NonZeroU64 {
        self.bundle_version
    }

    /// Returns the exact expected Task 11 dataset identity.
    #[must_use]
    pub const fn dataset(&self) -> &TrainingDatasetIdentity {
        &self.dataset
    }

    /// Returns the human-stable expected universe identity.
    #[must_use]
    pub const fn universe_id(&self) -> &UniverseId {
        &self.universe_id
    }

    /// Returns the expected training period.
    #[must_use]
    pub const fn training_period(&self) -> TrainingPeriod {
        self.training_period
    }

    /// Returns the expected Task 11 label component.
    #[must_use]
    pub const fn label(&self) -> &FeatureLabelComponentSpec {
        &self.label
    }

    /// Returns the expected training-code revision.
    #[must_use]
    pub fn training_code_revision(&self) -> &str {
        &self.training_code_revision
    }

    /// Returns the independently approved exact final bundle-metadata identity.
    #[must_use]
    pub const fn bundle_metadata_hash(&self) -> Sha256Digest {
        self.bundle_metadata_hash
    }

    /// Returns the independently approved exact native-artifact identity.
    #[must_use]
    pub const fn artifact_hash(&self) -> Sha256Digest {
        self.artifact_hash
    }

    /// Returns the independently approved exact training-run provenance identity.
    #[must_use]
    pub const fn training_run_hash(&self) -> Sha256Digest {
        self.training_run_hash
    }
}

/// Closed validation metric name retained with a model generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ValidationMetricName {
    /// Mean squared validation error.
    MeanSquaredError,
    /// Correct-class fraction.
    Accuracy,
    /// Binary logarithmic validation loss.
    LogLoss,
    /// Area under the receiver operating characteristic.
    AreaUnderRoc,
}

/// One finite, named validation metric.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ValidationMetric {
    name: ValidationMetricName,
    value: f64,
}

impl ValidationMetric {
    pub(crate) const fn new(name: ValidationMetricName, value: f64) -> Self {
        Self { name, value }
    }

    /// Returns the closed metric name.
    #[must_use]
    pub const fn name(self) -> ValidationMetricName {
        self.name
    }

    /// Returns the admitted finite value.
    #[must_use]
    pub const fn value(self) -> f64 {
        self.value
    }
}

/// Complete deterministic decision and confidence policy.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DecisionThresholds {
    negative_max: f64,
    positive_min: f64,
    minimum_confidence: f64,
}

impl DecisionThresholds {
    pub(crate) const fn new(negative_max: f64, positive_min: f64, minimum_confidence: f64) -> Self {
        Self {
            negative_max,
            positive_min,
            minimum_confidence,
        }
    }

    /// Returns the inclusive negative decision ceiling.
    #[must_use]
    pub const fn negative_max(self) -> f64 {
        self.negative_max
    }

    /// Returns the inclusive positive decision floor.
    #[must_use]
    pub const fn positive_min(self) -> f64 {
        self.positive_min
    }

    /// Returns the confidence floor below which the decision is no-action.
    #[must_use]
    pub const fn minimum_confidence(self) -> f64 {
        self.minimum_confidence
    }
}

/// Complete validated metadata for one immutable model-bundle generation.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelMetadata {
    model_id: ModelId,
    bundle_id: BundleId,
    bundle_version: NonZeroU64,
    metadata_hash: Sha256Digest,
    artifact_hash: Sha256Digest,
    training_run_hash: Sha256Digest,
    format: ModelFormat,
    format_version: u32,
    features: Box<[ModelFeatureBinding]>,
    feature_semantic_digests: Box<[FeatureSemanticDigest]>,
    dataset: TrainingDatasetIdentity,
    universe_id: UniverseId,
    training_period: TrainingPeriod,
    label: FeatureLabelComponentSpec,
    training_code_revision: Box<str>,
    validation_metrics: Box<[ValidationMetric]>,
    decision_thresholds: DecisionThresholds,
    intended_use: Box<str>,
    limitations: Box<[Box<str>]>,
    fallback_reason: Box<str>,
}

impl ModelMetadata {
    #[allow(
        clippy::too_many_arguments,
        reason = "validated bundle fields remain separately bound"
    )]
    pub(crate) fn new(
        expectations: &BundleExpectations,
        metadata_hash: Sha256Digest,
        artifact_hash: Sha256Digest,
        format: ModelFormat,
        format_version: u32,
        features: Vec<ModelFeatureBinding>,
        validation_metrics: Vec<ValidationMetric>,
        decision_thresholds: DecisionThresholds,
        intended_use: String,
        limitations: Vec<String>,
        fallback_reason: String,
    ) -> Self {
        let feature_semantic_digests = features
            .iter()
            .map(ModelFeatureBinding::semantic_digest)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            model_id: expectations.model_id,
            bundle_id: expectations.bundle_id.clone(),
            bundle_version: expectations.bundle_version,
            metadata_hash,
            artifact_hash,
            training_run_hash: expectations.training_run_hash,
            format,
            format_version,
            features: features.into_boxed_slice(),
            feature_semantic_digests,
            dataset: expectations.dataset.clone(),
            universe_id: expectations.universe_id.clone(),
            training_period: expectations.training_period,
            label: expectations.label.clone(),
            training_code_revision: expectations.training_code_revision.clone(),
            validation_metrics: validation_metrics.into_boxed_slice(),
            decision_thresholds,
            intended_use: intended_use.into_boxed_str(),
            limitations: limitations
                .into_iter()
                .map(String::into_boxed_str)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            fallback_reason: fallback_reason.into_boxed_str(),
        }
    }

    /// Returns the stable model identity.
    #[must_use]
    pub const fn model_id(&self) -> ModelId {
        self.model_id
    }

    /// Returns the immutable bundle series identity.
    #[must_use]
    pub const fn bundle_id(&self) -> &BundleId {
        &self.bundle_id
    }

    /// Returns the immutable bundle generation.
    #[must_use]
    pub const fn bundle_version(&self) -> NonZeroU64 {
        self.bundle_version
    }

    /// Returns the exact metadata-byte identity.
    #[must_use]
    pub const fn metadata_hash(&self) -> Sha256Digest {
        self.metadata_hash
    }

    /// Returns the exact artifact-byte identity.
    #[must_use]
    pub const fn artifact_hash(&self) -> Sha256Digest {
        self.artifact_hash
    }

    /// Returns the exact admitted training-run provenance identity.
    #[must_use]
    pub const fn training_run_hash(&self) -> Sha256Digest {
        self.training_run_hash
    }

    /// Returns the closed native format.
    #[must_use]
    pub const fn format(&self) -> ModelFormat {
        self.format
    }

    /// Returns the exact supported format version.
    #[must_use]
    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    /// Returns coefficient-ordered Task 12 feature bindings.
    #[must_use]
    pub fn features(&self) -> &[ModelFeatureBinding] {
        &self.features
    }

    /// Returns coefficient-ordered semantic feature identities.
    #[must_use]
    pub fn feature_semantic_digests(&self) -> &[FeatureSemanticDigest] {
        &self.feature_semantic_digests
    }

    /// Returns the exact Task 11 training generation.
    #[must_use]
    pub const fn dataset(&self) -> &TrainingDatasetIdentity {
        &self.dataset
    }

    /// Returns the stable training universe identity.
    #[must_use]
    pub const fn universe_id(&self) -> &UniverseId {
        &self.universe_id
    }

    /// Returns the admitted training interval.
    #[must_use]
    pub const fn training_period(&self) -> TrainingPeriod {
        self.training_period
    }

    /// Returns the exact label component contract.
    #[must_use]
    pub const fn label(&self) -> &FeatureLabelComponentSpec {
        &self.label
    }

    /// Returns the exact expected training implementation revision.
    #[must_use]
    pub fn training_code_revision(&self) -> &str {
        &self.training_code_revision
    }

    /// Returns the bounded finite validation evidence.
    #[must_use]
    pub fn validation_metrics(&self) -> &[ValidationMetric] {
        &self.validation_metrics
    }

    /// Returns the deterministic decision policy.
    #[must_use]
    pub const fn decision_thresholds(&self) -> DecisionThresholds {
        self.decision_thresholds
    }

    /// Returns the bounded declared use.
    #[must_use]
    pub fn intended_use(&self) -> &str {
        &self.intended_use
    }

    /// Returns nonempty bounded limitations.
    #[must_use]
    pub fn limitations(&self) -> &[Box<str>] {
        &self.limitations
    }

    /// Returns the mandatory no-action fallback reason.
    #[must_use]
    pub fn fallback_reason(&self) -> &str {
        &self.fallback_reason
    }

    pub(crate) fn retained_bytes(&self) -> Option<usize> {
        let mut retained = size_of::<Self>()
            .checked_add(self.bundle_id.as_str().len())?
            .checked_add(self.dataset.retained_bytes()?)?
            .checked_add(self.universe_id.as_str().len())?
            .checked_add(self.label.name().len())?
            .checked_add(self.training_code_revision.len())?
            .checked_add(self.intended_use.len())?
            .checked_add(self.fallback_reason.len())?
            .checked_add(
                size_of::<FeatureSemanticDigest>()
                    .checked_mul(self.feature_semantic_digests.len())?,
            )?
            .checked_add(size_of::<ValidationMetric>().checked_mul(self.validation_metrics.len())?)?
            .checked_add(size_of::<Box<str>>().checked_mul(self.limitations.len())?)?;
        for feature in &self.features {
            retained = retained.checked_add(feature.retained_bytes()?)?;
        }
        for limitation in &self.limitations {
            retained = retained.checked_add(limitation.len())?;
        }
        Some(retained)
    }
}

/// Model metadata or independent expectation construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ModelMetadataError {
    /// A bundle identity was not canonical or bounded.
    #[error("model bundle identity is invalid")]
    InvalidBundleId,
    /// An exact training identity used a reserved zero digest.
    #[error("model training identity contains a reserved digest")]
    ReservedDigest,
    /// The training period was empty or reversed.
    #[error("model training period is invalid")]
    InvalidTrainingPeriod,
    /// Independent bundle expectations were incomplete or invalid.
    #[error("model bundle expectations are invalid")]
    InvalidExpectations,
    /// A feature normalizer was nonfinite or had a nonpositive scale.
    #[error("model feature normalizer is invalid")]
    InvalidNormalizer,
}

pub(crate) fn valid_revision(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_TRAINING_CODE_REVISION_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn canonical_identifier(value: &str, maximum: usize) -> bool {
    let bytes = value.as_bytes();
    let edge = |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    !bytes.is_empty()
        && bytes.len() <= maximum
        && bytes.first().is_some_and(edge)
        && bytes.last().is_some_and(edge)
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}
