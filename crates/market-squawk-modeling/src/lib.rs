//! Closed model bundles, immutable generations, and bounded native inference.
//!
//! Bundle and registry operations are control-plane work. [`InferenceBackend::infer`] consumes
//! already admitted in-memory state and performs no filesystem, database, network, plugin,
//! process, Python, MCP, LLM, or arbitrary-code work.

use std::num::NonZeroU16;

use sha2::{Digest, Sha256};
use thiserror::Error;

mod bundle;
mod input;
mod metadata;
mod native;
mod registry;

pub use bundle::{
    BundleError, BundleMetadataRef, ControlledModelRoot, MAX_ARTIFACT_BYTES,
    MAX_CONTROLLED_MODEL_PATH_BYTES, MAX_METADATA_BYTES, ModelBundle,
};
pub use input::{
    ModelDecision, ModelFeatureValue, ModelInput, ModelInputError, ModelOutput, ModelOutputIdentity,
};
pub use metadata::{
    BundleExpectations, BundleId, DecisionThresholds, FeatureNormalizer, MAX_BUNDLE_ID_BYTES,
    MAX_MODEL_FEATURES, MAX_TRAINING_CODE_REVISION_BYTES, ModelFeatureBinding, ModelFormat,
    ModelMetadata, ModelMetadataError, TrainingDatasetIdentity, TrainingPeriod, ValidationMetric,
    ValidationMetricName,
};
pub use native::{InferenceBackend, InferenceError, NativeBackendError, NativeLinearBackend};
pub use registry::{
    BundleRegistration, MAX_MODEL_REGISTRY_GENERATIONS, MAX_MODEL_REGISTRY_RETAINED_BYTES,
    ModelRegistry, ModelRegistryError,
};

/// Exact typed model failure before an execution strategy can create an order intent.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ModelFailure {
    /// Bundle read or validation failed.
    #[error("model bundle failed closed: {0}")]
    Bundle(BundleError),
    /// Immutable registry load or registration failed.
    #[error("model registry failed closed: {0}")]
    Registry(ModelRegistryError),
    /// Exact input construction failed.
    #[error("model input failed closed: {0}")]
    Input(ModelInputError),
    /// Native backend construction failed.
    #[error("native model backend failed closed: {0}")]
    Backend(NativeBackendError),
    /// Pure native inference failed.
    #[error("native model inference failed closed: {0}")]
    Inference(InferenceError),
}

/// Closed lifecycle phase for a model failure before the execution boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelFailurePhase {
    /// Untrusted persisted relationships failed validation.
    Validation,
    /// A controlled read, immutable registry operation, or backend load failed.
    Load,
    /// Finite input construction or pure native inference failed.
    Inference,
}

/// Immutable model-owned evidence that an execution adapter can audit as no-action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelFailureAudit {
    phase: ModelFailurePhase,
    source_code: NonZeroU16,
    source_digest: [u8; 32],
}

impl ModelFailureAudit {
    /// Returns the closed model lifecycle phase.
    #[must_use]
    pub const fn phase(self) -> ModelFailurePhase {
        self.phase
    }

    /// Returns the stable nonzero model failure code.
    #[must_use]
    pub const fn source_code(self) -> NonZeroU16 {
        self.source_code
    }

    /// Returns the exact model-owned error evidence identity.
    #[must_use]
    pub const fn source_digest(self) -> [u8; 32] {
        self.source_digest
    }
}

impl ModelFailure {
    /// Returns the lifecycle phase for this exact typed failure.
    #[must_use]
    pub const fn phase(self) -> ModelFailurePhase {
        match self {
            Self::Bundle(
                BundleError::ControlledRootUnavailable
                | BundleError::ReadFailure
                | BundleError::MetadataTooLarge
                | BundleError::ArtifactTooLarge,
            )
            | Self::Registry(_)
            | Self::Backend(_) => ModelFailurePhase::Load,
            Self::Bundle(_) => ModelFailurePhase::Validation,
            Self::Input(_) | Self::Inference(_) => ModelFailurePhase::Inference,
        }
    }

    /// Returns typed, non-authoritative evidence for the execution no-action adapter.
    #[must_use]
    pub fn audit(self) -> ModelFailureAudit {
        let code = self.source_code();
        let source_code = match NonZeroU16::new(code) {
            Some(value) => value,
            None => NonZeroU16::MIN,
        };
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/model-failure/v1");
        hash.update(code.to_be_bytes());
        let source_digest = hash.finalize().into();
        ModelFailureAudit {
            phase: self.phase(),
            source_code,
            source_digest,
        }
    }

    const fn source_code(self) -> u16 {
        match self {
            Self::Bundle(error) => bundle_error_code(error),
            Self::Registry(error) => registry_error_code(error),
            Self::Input(error) => input_error_code(error),
            Self::Backend(NativeBackendError::UnsupportedBundleFormat) => 301,
            Self::Inference(InferenceError::BundleMismatch) => 401,
            Self::Inference(InferenceError::FeatureShapeMismatch) => 402,
            Self::Inference(InferenceError::NonFiniteComputation) => 403,
        }
    }
}

impl From<BundleError> for ModelFailure {
    fn from(value: BundleError) -> Self {
        Self::Bundle(value)
    }
}

impl From<ModelRegistryError> for ModelFailure {
    fn from(value: ModelRegistryError) -> Self {
        Self::Registry(value)
    }
}

impl From<ModelInputError> for ModelFailure {
    fn from(value: ModelInputError) -> Self {
        Self::Input(value)
    }
}

impl From<NativeBackendError> for ModelFailure {
    fn from(value: NativeBackendError) -> Self {
        Self::Backend(value)
    }
}

impl From<InferenceError> for ModelFailure {
    fn from(value: InferenceError) -> Self {
        Self::Inference(value)
    }
}

const fn bundle_error_code(error: BundleError) -> u16 {
    match error {
        BundleError::InvalidControlledPath => 1,
        BundleError::ControlledRootUnavailable => 2,
        BundleError::ReadFailure => 3,
        BundleError::MetadataTooLarge => 4,
        BundleError::ArtifactTooLarge => 5,
        BundleError::MetadataHashMismatch => 6,
        BundleError::MetadataStructureLimit => 7,
        BundleError::MetadataSyntax => 8,
        BundleError::UnsupportedMetadataVersion => 9,
        BundleError::ModelIdentityMismatch => 10,
        BundleError::BundleIdentityMismatch => 11,
        BundleError::UnsupportedFormat => 12,
        BundleError::UnsupportedFormatVersion => 13,
        BundleError::InvalidDigest => 14,
        BundleError::InvalidFeatureCount => 15,
        BundleError::FeatureIdentityMismatch => 16,
        BundleError::FeatureSchemaMismatch => 17,
        BundleError::FeatureSemanticMismatch => 18,
        BundleError::FeatureOrderMismatch => 19,
        BundleError::InvalidNormalizer => 20,
        BundleError::DatasetMismatch => 21,
        BundleError::UniverseMismatch => 22,
        BundleError::TrainingPeriodMismatch => 23,
        BundleError::LabelMismatch => 24,
        BundleError::TrainingCodeRevisionMismatch => 25,
        BundleError::InvalidValidationMetrics => 26,
        BundleError::InvalidDecisionThresholds => 27,
        BundleError::InvalidIntendedUse => 28,
        BundleError::InvalidLimitations => 29,
        BundleError::InvalidFallback => 30,
        BundleError::ArtifactSizeMismatch => 31,
        BundleError::ArtifactHashMismatch => 32,
        BundleError::ArtifactStructureLimit => 33,
        BundleError::ArtifactSyntax => 34,
        BundleError::InvalidTensorShape => 35,
        BundleError::UnsupportedOutputShape => 36,
        BundleError::NonFiniteArtifact => 37,
        BundleError::RetainedSizeOverflow => 38,
    }
}

const fn registry_error_code(error: ModelRegistryError) -> u16 {
    match error {
        ModelRegistryError::RegistryCapacityTooLarge => 101,
        ModelRegistryError::RetainedByteLimitTooLarge => 102,
        ModelRegistryError::RetainedByteLimitTooSmall => 103,
        ModelRegistryError::RegistryFull => 104,
        ModelRegistryError::RetainedByteLimitExceeded => 105,
        ModelRegistryError::GenerationConflict => 106,
        ModelRegistryError::BundleSeriesConflict => 107,
        ModelRegistryError::ModelSeriesConflict => 108,
        ModelRegistryError::RetainedSizeOverflow => 109,
        ModelRegistryError::RegistryUnavailable => 110,
    }
}

const fn input_error_code(error: ModelInputError) -> u16 {
    match error {
        ModelInputError::NonFiniteValue => 201,
        ModelInputError::FeatureShapeMismatch => 202,
        ModelInputError::FeatureIdentityMismatch => 203,
        ModelInputError::FeatureUnavailable => 204,
        ModelInputError::UnsupportedFeatureScalar => 205,
    }
}
