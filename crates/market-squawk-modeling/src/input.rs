//! Pure, bounded, identity-checked inference inputs and outputs.

use std::num::NonZeroU64;

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

    fn matches(&self, binding: &ModelFeatureBinding) -> bool {
        self.key == *binding.key()
            && self.input_schema_digest == binding.input_schema_digest()
            && self.semantic_digest == binding.semantic_digest()
    }
}

/// Complete coefficient-ordered input for exactly one bundle generation.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelInput {
    bundle_id: BundleId,
    bundle_version: NonZeroU64,
    metadata_hash: Sha256Digest,
    values: Box<[ModelFeatureValue]>,
}

impl ModelInput {
    /// Validates finite values, shape, order, versions, schemas, and semantic identities.
    ///
    /// # Errors
    ///
    /// Rejects empty/oversized or contract-mismatched values before inference.
    pub fn try_new(
        metadata: &ModelMetadata,
        values: Vec<ModelFeatureValue>,
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
            bundle_id: metadata.bundle_id().clone(),
            bundle_version: metadata.bundle_version(),
            metadata_hash: metadata.metadata_hash(),
            values: values.into_boxed_slice(),
        })
    }

    pub(crate) fn matches(&self, metadata: &ModelMetadata) -> bool {
        self.bundle_id == *metadata.bundle_id()
            && self.bundle_version == metadata.bundle_version()
            && self.metadata_hash == metadata.metadata_hash()
    }

    pub(crate) fn values(&self) -> &[ModelFeatureValue] {
        &self.values
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

/// Finite deterministic inference result with exact reproducibility identities.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelOutput {
    model_id: ModelId,
    bundle_id: BundleId,
    bundle_version: NonZeroU64,
    dataset: TrainingDatasetIdentity,
    feature_semantic_digests: Box<[FeatureSemanticDigest]>,
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
        metadata: &ModelMetadata,
        score: f64,
        confidence: f64,
        decision: ModelDecision,
    ) -> Self {
        Self {
            model_id: metadata.model_id(),
            bundle_id: metadata.bundle_id().clone(),
            bundle_version: metadata.bundle_version(),
            dataset: metadata.dataset().clone(),
            feature_semantic_digests: metadata.feature_semantic_digests().into(),
            score,
            confidence,
            decision,
        }
    }

    /// Returns the exact producing model identity.
    #[must_use]
    pub const fn model_id(&self) -> ModelId {
        self.model_id
    }

    /// Returns the exact producing bundle series.
    #[must_use]
    pub const fn bundle_id(&self) -> &BundleId {
        &self.bundle_id
    }

    /// Returns the exact producing immutable generation.
    #[must_use]
    pub const fn bundle_version(&self) -> NonZeroU64 {
        self.bundle_version
    }

    /// Returns the exact Task 11 training dataset identity.
    #[must_use]
    pub const fn dataset(&self) -> &TrainingDatasetIdentity {
        &self.dataset
    }

    /// Returns coefficient-ordered Task 12 semantic identities.
    #[must_use]
    pub fn feature_semantic_digests(&self) -> &[FeatureSemanticDigest] {
        &self.feature_semantic_digests
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
