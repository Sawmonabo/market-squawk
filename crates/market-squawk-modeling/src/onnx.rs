//! Exact, self-contained ONNX inference through a serialized tract worker.

use std::mem::size_of;
use std::sync::Arc;
use std::time::Instant;

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::native::decide;
use crate::{
    InferenceBackend, InferenceError, ModelBundle, ModelFormat, ModelInput, ModelMetadata,
    ModelOutput, ModelOutputIdentity, ModelOutputSemantics,
};

#[cfg(feature = "onnx-runtime")]
mod external;
mod policy;
mod wire;
mod worker;

#[cfg(feature = "onnx-runtime")]
pub use external::{
    ControlledOnnxRuntimeRoot, ExternalOnnxRuntimeAdmission, ExternalOnnxRuntimeBackend,
    ExternalOnnxRuntimeError, ExternalOnnxRuntimeReference, ExternalRuntimePlatform,
    OPTIONAL_ONNX_RUNTIME_VERSION, optional_onnx_runtime_policy_digest,
};
pub use policy::{
    MAX_ONNX_MODEL_BYTES, MAX_ONNX_NODES, MAX_ONNX_REQUEST_ELEMENTS, MAX_ONNX_TENSORS,
    OnnxFallbackPolicy, OnnxModelPolicy, OnnxPolicyError, ValidatedOnnxModel,
};
use worker::{OnnxWorker, WorkerError};
pub use worker::{
    OnnxWorkerProcessError, OnnxWorkerProgram, OnnxWorkerProgramError, run_onnx_worker_process,
};

/// Immutable tract runtime admission evidence for one exact bundle and policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OnnxRuntimeEvidence {
    model_digest: [u8; 32],
    forecast_policy_digest: Option<[u8; 32]>,
    forecast_residuals_digest: Option<[u8; 32]>,
    policy_digest: [u8; 32],
    worker_runtime_semantics_digest: [u8; 32],
    warm_up_digest: [u8; 32],
}

impl OnnxRuntimeEvidence {
    /// Returns the exact admitted model digest.
    #[must_use]
    pub const fn model_digest(self) -> [u8; 32] {
        self.model_digest
    }

    /// Returns the exact first-class interval-policy member digest for forecast bundles.
    #[must_use]
    pub const fn forecast_policy_digest(self) -> Option<[u8; 32]> {
        self.forecast_policy_digest
    }

    /// Returns the exact first-class calibration-residual member digest for forecast bundles.
    #[must_use]
    pub const fn forecast_residuals_digest(self) -> Option<[u8; 32]> {
        self.forecast_residuals_digest
    }

    /// Returns the exact graph-policy digest.
    #[must_use]
    pub const fn policy_digest(self) -> [u8; 32] {
        self.policy_digest
    }

    /// Returns the versioned helper, protocol, limit, containment, and deadline semantics digest.
    #[must_use]
    pub const fn worker_runtime_semantics_digest(self) -> [u8; 32] {
        self.worker_runtime_semantics_digest
    }

    /// Returns the exact finite warm-up result identity.
    #[must_use]
    pub const fn warm_up_digest(self) -> [u8; 32] {
        self.warm_up_digest
    }
}

/// Required self-contained ONNX backend with one bounded model-owned worker.
#[derive(Debug)]
pub struct TractOnnxBackend {
    bundle: Arc<ModelBundle>,
    policy: OnnxModelPolicy,
    worker: OnnxWorker,
    output_identity: Arc<ModelOutputIdentity>,
    evidence: OnnxRuntimeEvidence,
}

impl TractOnnxBackend {
    /// Preflights, compiles, warms, and atomically constructs one exact ONNX generation.
    ///
    /// # Errors
    ///
    /// Returns a typed format, digest, graph, shape, runtime-load, or warm-up failure. No partial
    /// backend is published.
    pub fn try_from_bundle(
        bundle: Arc<ModelBundle>,
        policy: OnnxModelPolicy,
        program: &OnnxWorkerProgram,
    ) -> Result<Self, OnnxBackendError> {
        if bundle.metadata().format() != ModelFormat::Onnx {
            return Err(OnnxBackendError::UnsupportedBundleFormat);
        }
        if bundle.metadata().output_semantics() != policy.output_semantics()
            || bundle.metadata().output_semantics_bound() != policy.output_semantics_bound()
        {
            return Err(OnnxBackendError::OutputSemanticsMismatch);
        }
        let artifact = bundle
            .onnx_artifact_bytes()
            .ok_or(OnnxBackendError::UnsupportedBundleFormat)?;
        let preflight = policy
            .preflight(artifact)
            .map_err(OnnxBackendError::Policy)?;
        let lag_count = match (
            bundle.research_forecast_layout(),
            policy.forecast_horizons(),
        ) {
            (Some((lags, horizons, strategy)), Some(admitted)) if horizons == admitted => {
                policy
                    .validate_research_layout(artifact, lags, strategy)
                    .map_err(OnnxBackendError::Policy)?;
                // The sealed research exporter fits raw lag and exogenous columns.
                if bundle.metadata().features().iter().any(|feature| {
                    !matches!(feature.normalizer(), crate::FeatureNormalizer::Identity)
                }) {
                    return Err(OnnxBackendError::FeatureShapeMismatch);
                }
                lags.len()
            }
            (None, None) => 0,
            _ => return Err(OnnxBackendError::OutputSemanticsMismatch),
        };
        if preflight.input_elements() != bundle.metadata().features().len() + lag_count {
            return Err(OnnxBackendError::FeatureShapeMismatch);
        }
        let (worker, warm_up) = OnnxWorker::start_tract(
            program,
            artifact,
            policy.input_shape(),
            preflight.input_elements(),
            preflight.output_elements(),
            policy.inference_deadline(),
        )
        .map_err(|error| match error {
            WorkerError::Load => OnnxBackendError::RuntimeLoad,
            WorkerError::Resource => OnnxBackendError::IntermediateLimit,
            WorkerError::Unavailable | WorkerError::Deadline | WorkerError::Runtime => {
                OnnxBackendError::WarmUp
            }
            WorkerError::TerminationUncertain => OnnxBackendError::TerminationUncertain,
        })?;
        if warm_up.len() != preflight.output_elements()
            || warm_up.iter().any(|value| {
                !value.is_finite()
                    || (policy.output_semantics() == ModelOutputSemantics::BinaryProbability
                        && !(0.0..=1.0).contains(value))
            })
        {
            worker
                .retire()
                .map_err(|_| OnnxBackendError::TerminationUncertain)?;
            return Err(OnnxBackendError::WarmUp);
        }
        let worker_runtime_semantics_digest = worker.runtime_semantics_digest();
        let mut warm_up_digest = Sha256::new();
        warm_up_digest.update(b"market-squawk/onnx-warm-up/v4");
        warm_up_digest.update(policy.policy_digest());
        warm_up_digest.update(worker_runtime_semantics_digest);
        warm_up_digest.update((warm_up.len() as u64).to_be_bytes());
        for value in &warm_up {
            warm_up_digest.update(value.to_bits().to_be_bytes());
        }
        let evidence = OnnxRuntimeEvidence {
            model_digest: bundle.metadata().artifact_hash().bytes(),
            forecast_policy_digest: bundle
                .metadata()
                .forecast_calibration()
                .map(|value| value.policy_hash().bytes()),
            forecast_residuals_digest: bundle
                .metadata()
                .forecast_calibration()
                .map(|value| value.residuals_hash().bytes()),
            policy_digest: policy.policy_digest(),
            worker_runtime_semantics_digest,
            warm_up_digest: warm_up_digest.finalize().into(),
        };
        let output_identity = Arc::new(ModelOutputIdentity::from_metadata(bundle.metadata()));
        Ok(Self {
            bundle,
            policy,
            worker,
            output_identity,
            evidence,
        })
    }

    /// Returns the exact preflight and warm-up evidence.
    #[must_use]
    pub const fn runtime_evidence(&self) -> OnnxRuntimeEvidence {
        self.evidence
    }

    /// Returns the exact graph policy retained by this runtime generation.
    #[must_use]
    pub const fn policy(&self) -> &OnnxModelPolicy {
        &self.policy
    }

    /// Returns the bounded retained Rust-side graph charge.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.bundle.retained_bytes())
            .saturating_add(self.output_identity.retained_bytes())
    }

    pub(crate) fn infer_normalized_until(
        &self,
        normalized: Vec<f32>,
        absolute_deadline: Instant,
    ) -> Result<ModelOutput, InferenceError> {
        let metadata = self.bundle.metadata();
        if self.policy.forecast_horizons().is_some() {
            return Err(InferenceError::OnnxRuntimeFailure);
        }
        let values = self
            .worker
            .execute_until(normalized, absolute_deadline)
            .map_err(worker_inference_error)?;
        let [score] = values.as_slice() else {
            return Err(InferenceError::OnnxRuntimeFailure);
        };
        let score = f64::from(*score);
        validate_output_score(metadata, score)?;
        let (decision, confidence) = decide(score, metadata.decision_thresholds())?;
        Ok(ModelOutput::new(
            Arc::clone(&self.output_identity),
            score,
            confidence,
            decision,
        ))
    }
}

impl InferenceBackend for TractOnnxBackend {
    fn retire(&self) -> Result<(), InferenceError> {
        self.worker.retire().map_err(worker_inference_error)
    }

    fn infer_research(
        &self,
        input: &crate::native::ResearchForecastInput<'_>,
    ) -> Result<crate::native::ResearchForecastOutput, InferenceError> {
        let (lags, horizons, _) = self
            .bundle
            .research_forecast_layout()
            .ok_or(InferenceError::FeatureShapeMismatch)?;
        if lags != input.lag_offsets() {
            return Err(InferenceError::FeatureShapeMismatch);
        }
        let exogenous = normalize_input(self.bundle.metadata(), input.exogenous())?;
        let mut normalized = Vec::new();
        normalized
            .try_reserve_exact(lags.len() + exogenous.len())
            .map_err(|_| InferenceError::OnnxWorkerUnavailable)?;
        for value in input.lag_values() {
            let value = *value as f32;
            if !value.is_finite() {
                return Err(InferenceError::NonFiniteComputation);
            }
            normalized.push(value);
        }
        normalized.extend(exogenous);
        let deadline = Instant::now()
            .checked_add(self.worker.deadline())
            .ok_or(InferenceError::OnnxDeadlineExceeded)?;
        let values = self
            .worker
            .execute_until(normalized, deadline)
            .map_err(worker_inference_error)?;
        if values.len() != horizons.len() {
            return Err(InferenceError::OnnxRuntimeFailure);
        }
        Ok(crate::native::ResearchForecastOutput::new(
            self.metadata().metadata_hash(),
            self.metadata().artifact_hash(),
            horizons,
            values.into_iter().map(f64::from).collect(),
        ))
    }

    fn metadata(&self) -> &ModelMetadata {
        self.bundle.metadata()
    }

    fn infer(&self, input: &ModelInput<'_>) -> Result<ModelOutput, InferenceError> {
        let normalized = normalize_input(self.bundle.metadata(), input)?;
        let deadline = Instant::now()
            .checked_add(self.worker.deadline())
            .ok_or(InferenceError::OnnxDeadlineExceeded)?;
        self.infer_normalized_until(normalized, deadline)
    }
}

fn validate_output_score(metadata: &ModelMetadata, score: f64) -> Result<(), InferenceError> {
    if !score.is_finite()
        || (metadata.output_semantics() == ModelOutputSemantics::BinaryProbability
            && !(0.0..=1.0).contains(&score))
    {
        return Err(InferenceError::OnnxRuntimeFailure);
    }
    Ok(())
}

fn worker_inference_error(error: WorkerError) -> InferenceError {
    match error {
        WorkerError::Unavailable | WorkerError::Load => InferenceError::OnnxWorkerUnavailable,
        WorkerError::Resource | WorkerError::Runtime | WorkerError::TerminationUncertain => {
            InferenceError::OnnxRuntimeFailure
        }
        WorkerError::Deadline => InferenceError::OnnxDeadlineExceeded,
    }
}

fn normalize_input(
    metadata: &ModelMetadata,
    input: &ModelInput<'_>,
) -> Result<Vec<f32>, InferenceError> {
    if !input.matches(metadata) {
        return Err(InferenceError::BundleMismatch);
    }
    let mut normalized = Vec::new();
    normalized
        .try_reserve_exact(input.values().len())
        .map_err(|_| InferenceError::OnnxWorkerUnavailable)?;
    for (value, binding) in input.values().iter().zip(metadata.features()) {
        let value = binding
            .normalizer()
            .normalize(value.value())
            .ok_or(InferenceError::NonFiniteComputation)? as f32;
        if !value.is_finite() {
            return Err(InferenceError::NonFiniteComputation);
        }
        normalized.push(value);
    }
    Ok(normalized)
}

/// Required-backend construction failure before publication.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OnnxBackendError {
    #[error("ONNX bundle format is unsupported")]
    UnsupportedBundleFormat,
    #[error("ONNX runtime policy output semantics differ from the admitted bundle")]
    OutputSemanticsMismatch,
    #[error("ONNX graph failed common preflight: {0}")]
    Policy(OnnxPolicyError),
    #[error("ONNX input shape differs from the Task 13 feature contract")]
    FeatureShapeMismatch,
    #[error("tract could not compile the admitted ONNX graph")]
    RuntimeLoad,
    #[error("tract inferred an intermediate graph beyond the tensor or element ceiling")]
    IntermediateLimit,
    #[error("tract ONNX warm-up failed")]
    WarmUp,
    #[error("ONNX worker termination could not be confirmed")]
    TerminationUncertain,
}
