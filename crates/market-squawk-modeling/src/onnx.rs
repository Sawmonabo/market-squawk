//! Exact, self-contained ONNX inference through a serialized tract worker.

use std::mem::size_of;
use std::sync::Arc;
use std::time::Instant;

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::native::decide;
use crate::{
    InferenceBackend, InferenceError, ModelBundle, ModelFormat, ModelInput, ModelMetadata,
    ModelOutput, ModelOutputIdentity,
};

#[cfg(feature = "onnx-runtime")]
mod external;
mod policy;
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
    policy_digest: [u8; 32],
    warm_up_digest: [u8; 32],
    warm_up_score_bits: u32,
}

impl OnnxRuntimeEvidence {
    /// Returns the exact admitted model digest.
    #[must_use]
    pub const fn model_digest(self) -> [u8; 32] {
        self.model_digest
    }

    /// Returns the exact graph-policy digest.
    #[must_use]
    pub const fn policy_digest(self) -> [u8; 32] {
        self.policy_digest
    }

    /// Returns the exact finite warm-up result identity.
    #[must_use]
    pub const fn warm_up_digest(self) -> [u8; 32] {
        self.warm_up_digest
    }

    #[cfg(feature = "onnx-runtime")]
    const fn warm_up_score(self) -> f32 {
        f32::from_bits(self.warm_up_score_bits)
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
        let artifact = bundle
            .onnx_artifact_bytes()
            .ok_or(OnnxBackendError::UnsupportedBundleFormat)?;
        let preflight = policy
            .preflight(artifact)
            .map_err(OnnxBackendError::Policy)?;
        if preflight.input_elements() != bundle.metadata().features().len() {
            return Err(OnnxBackendError::FeatureShapeMismatch);
        }
        let (worker, warm_up) = OnnxWorker::start_tract(
            program,
            artifact,
            policy.input_shape(),
            preflight.input_elements(),
            policy.inference_deadline(),
        )
        .map_err(|error| match error {
            WorkerError::Load => OnnxBackendError::RuntimeLoad,
            WorkerError::Resource => OnnxBackendError::IntermediateLimit,
            WorkerError::Unavailable | WorkerError::Deadline | WorkerError::Runtime => {
                OnnxBackendError::WarmUp
            }
        })?;
        if !warm_up.is_finite() {
            return Err(OnnxBackendError::WarmUp);
        }
        let mut warm_up_digest = Sha256::new();
        warm_up_digest.update(b"market-squawk/onnx-warm-up/v1");
        warm_up_digest.update(policy.policy_digest());
        warm_up_digest.update(warm_up.to_bits().to_be_bytes());
        let evidence = OnnxRuntimeEvidence {
            model_digest: bundle.metadata().artifact_hash().bytes(),
            policy_digest: policy.policy_digest(),
            warm_up_digest: warm_up_digest.finalize().into(),
            warm_up_score_bits: warm_up.to_bits(),
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
        let score = f64::from(
            self.worker
                .execute_until(normalized, absolute_deadline)
                .map_err(worker_inference_error)?,
        );
        if !score.is_finite() {
            return Err(InferenceError::OnnxRuntimeFailure);
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

impl InferenceBackend for TractOnnxBackend {
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

fn worker_inference_error(error: WorkerError) -> InferenceError {
    match error {
        WorkerError::Unavailable | WorkerError::Load => InferenceError::OnnxWorkerUnavailable,
        WorkerError::Resource | WorkerError::Runtime => InferenceError::OnnxRuntimeFailure,
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
}
