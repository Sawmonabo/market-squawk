//! Deterministic, allocation-free native inference after bundle admission.

use std::mem::size_of;
use std::sync::Arc;

use market_squawk_analytics::FeatureSemanticDigest;
use thiserror::Error;

use crate::{
    DecisionThresholds, ModelBundle, ModelDecision, ModelFormat, ModelInput, ModelMetadata,
    ModelOutput,
};

/// Immutable native tensor admitted together with a complete bundle.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NativeArtifact {
    format: ModelFormat,
    feature_semantic_digests: Box<[FeatureSemanticDigest]>,
    weights: Box<[f64]>,
    bias: f64,
}

impl NativeArtifact {
    pub(crate) fn new(
        format: ModelFormat,
        feature_semantic_digests: Vec<FeatureSemanticDigest>,
        weights: Vec<f64>,
        bias: f64,
    ) -> Self {
        Self {
            format,
            feature_semantic_digests: feature_semantic_digests.into_boxed_slice(),
            weights: weights.into_boxed_slice(),
            bias,
        }
    }

    pub(crate) const fn format(&self) -> ModelFormat {
        self.format
    }

    pub(crate) fn weights(&self) -> &[f64] {
        &self.weights
    }

    pub(crate) const fn bias(&self) -> f64 {
        self.bias
    }

    pub(crate) fn retained_bytes(&self) -> Option<usize> {
        size_of::<Self>()
            .checked_add(
                size_of::<FeatureSemanticDigest>()
                    .checked_mul(self.feature_semantic_digests.len())?,
            )?
            .checked_add(size_of::<f64>().checked_mul(self.weights.len())?)
    }
}

/// Pure native inference contract.
pub trait InferenceBackend: Send + Sync {
    /// Returns complete immutable metadata for this exact backend generation.
    fn metadata(&self) -> &ModelMetadata;

    /// Evaluates one exact, bounded input without I/O, registry access, or allocation.
    ///
    /// # Errors
    ///
    /// Returns a typed contract or finite-arithmetic failure and never substitutes a score.
    fn infer(&self, input: &ModelInput) -> Result<ModelOutput, InferenceError>;
}

/// Native affine backend supporting the closed linear and logistic link families.
#[derive(Clone, Debug)]
pub struct NativeLinearBackend {
    bundle: Arc<ModelBundle>,
}

impl NativeLinearBackend {
    /// Constructs a native backend from one already admitted immutable bundle.
    ///
    /// # Errors
    ///
    /// Rejects any future bundle format not implemented by this backend.
    pub fn try_from_bundle(bundle: Arc<ModelBundle>) -> Result<Self, NativeBackendError> {
        if !matches!(
            bundle.metadata().format(),
            ModelFormat::NativeLinear | ModelFormat::NativeLogistic
        ) {
            return Err(NativeBackendError::UnsupportedBundleFormat);
        }
        Ok(Self { bundle })
    }
}

impl InferenceBackend for NativeLinearBackend {
    fn metadata(&self) -> &ModelMetadata {
        self.bundle.metadata()
    }

    fn infer(&self, input: &ModelInput) -> Result<ModelOutput, InferenceError> {
        let metadata = self.bundle.metadata();
        if !input.matches(metadata) {
            return Err(InferenceError::BundleMismatch);
        }
        let artifact = self.bundle.native_artifact();
        if input.values().len() != artifact.weights().len()
            || input.values().len() != metadata.features().len()
        {
            return Err(InferenceError::FeatureShapeMismatch);
        }

        let mut score = artifact.bias();
        for ((value, binding), weight) in input
            .values()
            .iter()
            .zip(metadata.features())
            .zip(artifact.weights())
        {
            let normalized = binding
                .normalizer()
                .normalize(value.value())
                .ok_or(InferenceError::NonFiniteComputation)?;
            let contribution = normalized * weight;
            if !contribution.is_finite() {
                return Err(InferenceError::NonFiniteComputation);
            }
            score += contribution;
            if !score.is_finite() {
                return Err(InferenceError::NonFiniteComputation);
            }
        }

        if artifact.format() == ModelFormat::NativeLogistic {
            score = stable_logistic(score).ok_or(InferenceError::NonFiniteComputation)?;
        }
        let (decision, confidence) = decide(score, metadata.decision_thresholds())?;
        Ok(ModelOutput::new(metadata, score, confidence, decision))
    }
}

/// Backend construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum NativeBackendError {
    /// The admitted bundle belongs to a future non-native format.
    #[error("bundle format is unsupported by the native backend")]
    UnsupportedBundleFormat,
}

/// Native inference failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InferenceError {
    /// Input was built for a different immutable bundle generation.
    #[error("model input belongs to a different bundle generation")]
    BundleMismatch,
    /// Input and tensor shapes differed despite admission checks.
    #[error("model input and native tensor shapes differ")]
    FeatureShapeMismatch,
    /// Normalization, affine arithmetic, link, or confidence became nonfinite.
    #[error("native inference produced a nonfinite intermediate")]
    NonFiniteComputation,
}

fn stable_logistic(value: f64) -> Option<f64> {
    let result = if value >= 0.0 {
        let exponential = (-value).exp();
        1.0 / (1.0 + exponential)
    } else {
        let exponential = value.exp();
        exponential / (1.0 + exponential)
    };
    result.is_finite().then_some(result)
}

fn decide(
    score: f64,
    thresholds: DecisionThresholds,
) -> Result<(ModelDecision, f64), InferenceError> {
    let (candidate, distance) = if score <= thresholds.negative_max() {
        (ModelDecision::Negative, thresholds.negative_max() - score)
    } else if score >= thresholds.positive_min() {
        (ModelDecision::Positive, score - thresholds.positive_min())
    } else {
        (ModelDecision::NoAction, 0.0)
    };
    let confidence = if candidate == ModelDecision::NoAction {
        0.0
    } else {
        distance / (1.0 + distance)
    };
    if !confidence.is_finite() || !(0.0..=1.0).contains(&confidence) {
        return Err(InferenceError::NonFiniteComputation);
    }
    let decision = if confidence < thresholds.minimum_confidence() {
        ModelDecision::NoAction
    } else {
        candidate
    };
    Ok((decision, confidence))
}
