//! Deterministic, allocation-free native inference after bundle admission.

use std::mem::size_of;
use std::sync::Arc;

use market_squawk_analytics::FeatureSemanticDigest;
use thiserror::Error;

use crate::{
    DecisionThresholds, ModelBundle, ModelDecision, ModelFormat, ModelInput, ModelMetadata,
    ModelOutput, ModelOutputIdentity,
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

/// Bounded inference contract shared by native and admitted graph runtimes.
pub trait InferenceBackend: Send + Sync {
    /// Returns complete immutable metadata for this exact backend generation.
    fn metadata(&self) -> &ModelMetadata;

    /// Evaluates one exact, bounded input without filesystem, network, or registry access.
    /// Pure native implementations remain allocation-free; admitted graph runtimes may use only
    /// their documented bounded, preallocated, or request-owned memory.
    ///
    /// # Errors
    ///
    /// Returns a typed contract or finite-arithmetic failure and never substitutes a score.
    fn infer(&self, input: &ModelInput<'_>) -> Result<ModelOutput, InferenceError>;

    /// Evaluates the distinct lagged research tensor contract without creating trading signals.
    fn infer_research(
        &self,
        _input: &ResearchForecastInput<'_>,
    ) -> Result<ResearchForecastOutput, InferenceError> {
        Err(InferenceError::FeatureShapeMismatch)
    }

    /// Releases active runtime resources after the last application lease ends.
    fn retire(&self) -> Result<(), InferenceError> {
        Ok(())
    }
}

/// One research origin with explicit lag-column order and exact exogenous feature identities.
#[derive(Clone, Copy, Debug)]
pub struct ResearchForecastInput<'a> {
    exogenous: ModelInput<'a>,
    lag_offsets: &'a [u32],
    lag_values: &'a [f64],
}

impl<'a> ResearchForecastInput<'a> {
    /// Binds finite raw lag values to their increasing positive observation offsets.
    pub fn try_new(
        exogenous: ModelInput<'a>,
        lag_offsets: &'a [u32],
        lag_values: &'a [f64],
    ) -> Result<Self, InferenceError> {
        if lag_offsets.is_empty()
            || lag_offsets.len() != lag_values.len()
            || lag_offsets.contains(&0)
            || lag_offsets.windows(2).any(|pair| pair[0] >= pair[1])
            || lag_values.iter().any(|value| !value.is_finite())
        {
            return Err(InferenceError::FeatureShapeMismatch);
        }
        Ok(Self {
            exogenous,
            lag_offsets,
            lag_values,
        })
    }

    pub(crate) fn exogenous(&self) -> &ModelInput<'a> {
        &self.exogenous
    }
    pub(crate) fn lag_offsets(&self) -> &[u32] {
        self.lag_offsets
    }
    pub(crate) fn lag_values(&self) -> &[f64] {
        self.lag_values
    }
}

/// Finite research values indexed by observation offsets, never economic timestamps or probabilities.
#[derive(Clone, Debug, PartialEq)]
pub struct ResearchForecastOutput {
    metadata_hash: market_squawk_data::Sha256Digest,
    artifact_hash: market_squawk_data::Sha256Digest,
    horizon_offsets: Box<[u32]>,
    values: Box<[f64]>,
}

impl ResearchForecastOutput {
    pub(crate) fn new(
        metadata_hash: market_squawk_data::Sha256Digest,
        artifact_hash: market_squawk_data::Sha256Digest,
        horizon_offsets: &[u32],
        values: Vec<f64>,
    ) -> Self {
        Self {
            metadata_hash,
            artifact_hash,
            horizon_offsets: horizon_offsets.into(),
            values: values.into_boxed_slice(),
        }
    }

    /// Exact admitted metadata identity binding this output to model, dataset, and training evidence.
    #[must_use]
    pub const fn metadata_hash(&self) -> market_squawk_data::Sha256Digest {
        self.metadata_hash
    }

    /// Exact graph identity which binds lag order, strategy, and output-column mapping.
    #[must_use]
    pub const fn artifact_hash(&self) -> market_squawk_data::Sha256Digest {
        self.artifact_hash
    }
    /// Increasing positive observation offsets; recursive models return their fitted one-step offset.
    #[must_use]
    pub fn horizon_offsets(&self) -> &[u32] {
        &self.horizon_offsets
    }
    /// Finite values in the same order as the explicit horizon offsets.
    #[must_use]
    pub fn values(&self) -> &[f64] {
        &self.values
    }
}

/// Native affine backend supporting the closed linear and logistic link families.
#[derive(Clone, Debug)]
pub struct NativeLinearBackend {
    bundle: Arc<ModelBundle>,
    output_identity: Arc<ModelOutputIdentity>,
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
        let output_identity = Arc::new(ModelOutputIdentity::from_metadata(bundle.metadata()));
        Ok(Self {
            bundle,
            output_identity,
        })
    }

    /// Returns the complete retained graph charge for one owned backend path.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.bundle.retained_bytes())
            .saturating_add(self.output_identity.retained_bytes())
    }
}

impl InferenceBackend for NativeLinearBackend {
    fn metadata(&self) -> &ModelMetadata {
        self.bundle.metadata()
    }

    fn infer(&self, input: &ModelInput<'_>) -> Result<ModelOutput, InferenceError> {
        let metadata = self.bundle.metadata();
        if !input.matches(metadata) {
            return Err(InferenceError::BundleMismatch);
        }
        let artifact = self
            .bundle
            .native_artifact()
            .ok_or(InferenceError::ArtifactUnavailable)?;
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
        Ok(ModelOutput::new(
            Arc::clone(&self.output_identity),
            score,
            confidence,
            decision,
        ))
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
    #[error("model inference produced a nonfinite intermediate")]
    NonFiniteComputation,
    /// The admitted bundle did not retain the artifact required by this backend.
    #[error("model artifact is unavailable for this backend")]
    ArtifactUnavailable,
    /// A bounded ONNX worker was unavailable or already occupied.
    #[error("ONNX inference worker is unavailable")]
    OnnxWorkerUnavailable,
    /// The bounded ONNX inference deadline elapsed.
    #[error("ONNX inference exceeded its deadline")]
    OnnxDeadlineExceeded,
    /// The admitted ONNX runtime failed or returned an invalid tensor.
    #[error("ONNX runtime failed closed")]
    OnnxRuntimeFailure,
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

pub(crate) fn decide(
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
