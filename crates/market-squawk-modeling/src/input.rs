//! Pure, bounded, identity-checked inference inputs and outputs.

use std::num::NonZeroU64;
use std::sync::Arc;

use market_squawk_analytics::{
    FeatureInputSchemaDigest, FeatureKey, FeatureMetadata, FeatureSemanticDigest,
};
use market_squawk_data::Sha256Digest;
use market_squawk_domain::ModelId;
use thiserror::Error;

use crate::{
    BundleId, MAX_MODEL_FEATURES, ModelFeatureBinding, ModelMetadata, TrainingDatasetIdentity,
};

/// One finite feature value carrying its exact Task 12 identities.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelFeatureValue {
    key: FeatureKey,
    input_schema_digest: FeatureInputSchemaDigest,
    semantic_digest: FeatureSemanticDigest,
    value: f64,
}

impl ModelFeatureValue {
    /// Binds a finite value to exact registered feature metadata.
    ///
    /// # Errors
    ///
    /// Rejects nonfinite values.
    pub fn try_new(metadata: &FeatureMetadata, value: f64) -> Result<Self, ModelInputError> {
        if !value.is_finite() {
            return Err(ModelInputError::NonFiniteValue);
        }
        Ok(Self {
            key: metadata.key().clone(),
            input_schema_digest: metadata.input_schema_digest(),
            semantic_digest: metadata.semantic_digest(),
            value,
        })
    }

    /// Creates one reusable live slot from an already validated bundle binding.
    #[must_use]
    pub fn from_binding(binding: &ModelFeatureBinding) -> Self {
        Self {
            key: binding.key().clone(),
            input_schema_digest: binding.input_schema_digest(),
            semantic_digest: binding.semantic_digest(),
            value: 0.0,
        }
    }

    /// Returns the exact feature key.
    #[must_use]
    pub const fn key(&self) -> &FeatureKey {
        &self.key
    }

    /// Returns the finite raw value.
    #[must_use]
    pub const fn value(&self) -> f64 {
        self.value
    }

    /// Reuses this admitted feature slot with a new finite live value.
    ///
    /// # Errors
    ///
    /// Rejects nonfinite values without changing the prior value.
    pub fn try_set_value(&mut self, value: f64) -> Result<(), ModelInputError> {
        if !value.is_finite() {
            return Err(ModelInputError::NonFiniteValue);
        }
        self.value = value;
        Ok(())
    }

    fn matches(&self, binding: &ModelFeatureBinding) -> bool {
        self.key == *binding.key()
            && self.input_schema_digest == binding.input_schema_digest()
            && self.semantic_digest == binding.semantic_digest()
    }
}

/// Complete coefficient-ordered input for exactly one bundle generation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ModelInput<'input> {
    bundle_id: &'input BundleId,
    bundle_version: NonZeroU64,
    metadata_hash: Sha256Digest,
    values: &'input [ModelFeatureValue],
}

impl<'input> ModelInput<'input> {
    /// Validates finite values, shape, order, versions, schemas, and semantic identities.
    ///
    /// # Errors
    ///
    /// Rejects empty/oversized or contract-mismatched values before inference.
    pub fn try_new(
        metadata: &'input ModelMetadata,
        values: &'input [ModelFeatureValue],
    ) -> Result<Self, ModelInputError> {
        if values.is_empty()
            || values.len() > MAX_MODEL_FEATURES
            || values.len() != metadata.features().len()
        {
            return Err(ModelInputError::FeatureShapeMismatch);
        }
        if values
            .iter()
            .zip(metadata.features())
            .any(|(value, binding)| !value.matches(binding))
        {
            return Err(ModelInputError::FeatureIdentityMismatch);
        }
        Ok(Self {
            bundle_id: metadata.bundle_id(),
            bundle_version: metadata.bundle_version(),
            metadata_hash: metadata.metadata_hash(),
            values,
        })
    }

    pub(crate) fn matches(&self, metadata: &ModelMetadata) -> bool {
        self.bundle_id == metadata.bundle_id()
            && self.bundle_version == metadata.bundle_version()
            && self.metadata_hash == metadata.metadata_hash()
    }

    pub(crate) fn values(&self) -> &[ModelFeatureValue] {
        self.values
    }
}

/// Inference input admission failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ModelInputError {
    /// An input feature value was NaN or infinite.
    #[error("model input feature value is nonfinite")]
    NonFiniteValue,
    /// Feature count was empty, oversized, or unequal to the artifact shape.
    #[error("model input feature shape does not match the bundle")]
    FeatureShapeMismatch,
    /// Feature order, version, schema, or semantic identity did not match.
    #[error("model input feature identity does not match the bundle")]
    FeatureIdentityMismatch,
    /// A required live feature was absent or not ready.
    #[error("required model input feature is unavailable")]
    FeatureUnavailable,
    /// A live feature scalar could not be represented at the model boundary.
    #[error("model input feature scalar is unsupported")]
    UnsupportedFeatureScalar,
}

/// Closed model decision emitted by native inference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelDecision {
    /// Score met the negative decision threshold.
    Negative,
    /// Score remained in the abstention region or below the confidence floor.
    NoAction,
    /// Score met the positive decision threshold.
    Positive,
}

/// Immutable exact identities shared by every output of one admitted backend generation.
#[derive(Debug, PartialEq)]
pub struct ModelOutputIdentity {
    model_id: ModelId,
    bundle_id: BundleId,
    bundle_version: NonZeroU64,
    dataset: TrainingDatasetIdentity,
    feature_semantic_digests: Box<[FeatureSemanticDigest]>,
    retained_bytes: usize,
}

impl ModelOutputIdentity {
    pub(crate) fn from_metadata(metadata: &ModelMetadata) -> Self {
        let dataset = metadata.dataset().clone();
        let feature_semantic_digests: Box<[_]> = metadata.feature_semantic_digests().into();
        let retained_bytes = std::mem::size_of::<Self>()
            .saturating_add(metadata.bundle_id().as_str().len())
            .saturating_add(dataset.retained_bytes().unwrap_or(usize::MAX))
            .saturating_add(
                std::mem::size_of::<FeatureSemanticDigest>()
                    .saturating_mul(feature_semantic_digests.len()),
            );
        Self {
            model_id: metadata.model_id(),
            bundle_id: metadata.bundle_id().clone(),
            bundle_version: metadata.bundle_version(),
            dataset,
            feature_semantic_digests,
            retained_bytes,
        }
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// Finite deterministic inference result with shared exact reproducibility identities.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelOutput {
    identity: Arc<ModelOutputIdentity>,
    score: f64,
    confidence: f64,
    decision: ModelDecision,
}

impl ModelOutput {
    #[allow(
        clippy::too_many_arguments,
        reason = "output binds every exact reproducibility identity"
    )]
    pub(crate) fn new(
        identity: Arc<ModelOutputIdentity>,
        score: f64,
        confidence: f64,
        decision: ModelDecision,
    ) -> Self {
        Self {
            identity,
            score,
            confidence,
            decision,
        }
    }

    /// Returns the precomputed identity block shared by this backend generation's outputs.
    #[must_use]
    pub fn identity(&self) -> &ModelOutputIdentity {
        &self.identity
    }

    /// Returns the exact producing model identity.
    #[must_use]
    pub fn model_id(&self) -> ModelId {
        self.identity.model_id
    }

    /// Returns the exact producing bundle series.
    #[must_use]
    pub fn bundle_id(&self) -> &BundleId {
        &self.identity.bundle_id
    }

    /// Returns the exact producing immutable generation.
    #[must_use]
    pub fn bundle_version(&self) -> NonZeroU64 {
        self.identity.bundle_version
    }

    /// Returns the exact Task 11 training dataset identity.
    #[must_use]
    pub fn dataset(&self) -> &TrainingDatasetIdentity {
        &self.identity.dataset
    }

    /// Returns coefficient-ordered Task 12 semantic identities.
    #[must_use]
    pub fn feature_semantic_digests(&self) -> &[FeatureSemanticDigest] {
        &self.identity.feature_semantic_digests
    }

    /// Returns the finite native score.
    #[must_use]
    pub const fn score(&self) -> f64 {
        self.score
    }

    /// Returns finite confidence in the closed interval `[0, 1]`.
    #[must_use]
    pub const fn confidence(&self) -> f64 {
        self.confidence
    }

    /// Returns the thresholded decision, including explicit abstention.
    #[must_use]
    pub const fn decision(&self) -> ModelDecision {
        self.decision
    }
}
