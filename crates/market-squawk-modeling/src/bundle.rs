//! Capability-rooted bundle reads and closed fail-closed validation.

use std::mem::size_of;
use std::num::NonZeroU64;
use std::path::Path;
use std::str::FromStr;

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use market_squawk_analytics::FeatureRegistry;
use market_squawk_data::Sha256Digest;
use market_squawk_domain::ModelId;
use thiserror::Error;

use self::io::{
    is_controlled_relative_path, read_exact_bounded, sha256_digest, validate_json_structure,
};
use self::validation::{
    METADATA_SCHEMA_VERSION, MetadataWire, NATIVE_FORMAT_VERSION, NativeArtifactWire,
    TrainingRunWire, parse_digest, parse_format, validate_artifact, validate_dataset,
    validate_features, validate_label, validate_metrics, validate_prose, validate_thresholds,
    validate_training_run,
};
use crate::metadata::valid_revision;
use crate::native::NativeArtifact;
use crate::{BundleExpectations, ModelMetadata, ModelMetadataError};

mod io;
mod validation;

/// Maximum UTF-8 bytes in one path relative to a controlled model root.
pub const MAX_CONTROLLED_MODEL_PATH_BYTES: usize = 256;
/// Maximum exact metadata bytes admitted before parsing.
pub const MAX_METADATA_BYTES: usize = 256 * 1024;
/// Maximum exact native artifact bytes admitted before parsing.
pub const MAX_ARTIFACT_BYTES: usize = 1024 * 1024;
/// Maximum exact training-run provenance bytes admitted before parsing.
pub const MAX_TRAINING_RUN_BYTES: usize = 256 * 1024;

/// Exact metadata object expected beneath a controlled model root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BundleMetadataRef {
    relative_path: Box<str>,
    content_hash: Sha256Digest,
}

impl BundleMetadataRef {
    /// Constructs an exact local metadata reference.
    ///
    /// # Errors
    ///
    /// Rejects empty, absolute, URL-like, traversal, platform-ambiguous, or oversized paths.
    pub fn try_new(
        relative_path: impl AsRef<str>,
        content_hash: Sha256Digest,
    ) -> Result<Self, BundleError> {
        let relative_path = relative_path.as_ref();
        if !is_controlled_relative_path(relative_path) {
            return Err(BundleError::InvalidControlledPath);
        }
        Ok(Self {
            relative_path: relative_path.into(),
            content_hash,
        })
    }

    /// Returns the validated path relative to the controlled model root.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    /// Returns the exact expected SHA-256 of the metadata bytes.
    #[must_use]
    pub const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }
}

/// Process-composition-owned capability root for model artifacts.
#[derive(Debug)]
pub struct ControlledModelRoot {
    directory: Dir,
}

impl ControlledModelRoot {
    /// Opens one ambient path exactly once to establish a bounded capability root.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the configured root cannot be opened.
    pub fn open_ambient(path: impl AsRef<Path>) -> Result<Self, BundleError> {
        let directory = Dir::open_ambient_dir(path, ambient_authority())
            .map_err(|_| BundleError::ControlledRootUnavailable)?;
        Ok(Self { directory })
    }
}

/// Complete immutable admitted bundle retaining exact bytes and parsed native tensors.
#[derive(Debug)]
pub struct ModelBundle {
    metadata: ModelMetadata,
    native_artifact: NativeArtifact,
    metadata_bytes: Box<[u8]>,
    artifact_bytes: Box<[u8]>,
    training_run_bytes: Box<[u8]>,
    retained_bytes: usize,
}

impl ModelBundle {
    /// Reads and admits one exact metadata/artifact pair beneath a controlled local root.
    ///
    /// Reads are byte-bounded before allocation. Both JSON objects are structurally bounded before
    /// deserialization, deny unknown fields, and must match independent Task 11/12 expectations.
    ///
    /// # Errors
    ///
    /// Returns a typed read, hash, resource, syntax, or relationship error without producing a
    /// partial bundle.
    pub fn load(
        root: &ControlledModelRoot,
        reference: &BundleMetadataRef,
        expectations: &BundleExpectations,
        feature_registry: &FeatureRegistry,
    ) -> Result<Self, BundleError> {
        let metadata_bytes = read_exact_bounded(
            &root.directory,
            reference.relative_path(),
            MAX_METADATA_BYTES,
            BundleError::MetadataTooLarge,
        )?;
        let metadata_hash = sha256_digest(&metadata_bytes);
        if metadata_hash != reference.content_hash()
            || metadata_hash != expectations.bundle_metadata_hash()
        {
            return Err(BundleError::MetadataHashMismatch);
        }
        validate_json_structure(&metadata_bytes)
            .map_err(|_| BundleError::MetadataStructureLimit)?;
        let wire: MetadataWire =
            serde_json::from_slice(&metadata_bytes).map_err(|_| BundleError::MetadataSyntax)?;
        if wire.schema_version != METADATA_SCHEMA_VERSION {
            return Err(BundleError::UnsupportedMetadataVersion);
        }

        let model_id =
            ModelId::from_str(&wire.model_id).map_err(|_| BundleError::ModelIdentityMismatch)?;
        let bundle_id = crate::BundleId::try_new(&wire.bundle_id)
            .map_err(|_| BundleError::BundleIdentityMismatch)?;
        let bundle_version =
            NonZeroU64::new(wire.bundle_version).ok_or(BundleError::BundleIdentityMismatch)?;
        if model_id != expectations.model_id()
            || bundle_id != *expectations.bundle_id()
            || bundle_version != expectations.bundle_version()
        {
            return Err(BundleError::BundleIdentityMismatch);
        }

        let format = parse_format(&wire.artifact.format)?;
        if wire.artifact.format_version != NATIVE_FORMAT_VERSION {
            return Err(BundleError::UnsupportedFormatVersion);
        }
        let artifact_hash = parse_digest(&wire.artifact.sha256)?;
        if artifact_hash != expectations.artifact_hash() {
            return Err(BundleError::ArtifactHashMismatch);
        }
        let artifact_reference = BundleMetadataRef::try_new(&wire.artifact.path, artifact_hash)?;
        let expected_artifact_size =
            usize::try_from(wire.artifact.size_bytes).map_err(|_| BundleError::ArtifactTooLarge)?;
        if expected_artifact_size > MAX_ARTIFACT_BYTES {
            return Err(BundleError::ArtifactTooLarge);
        }
        let training_run_hash = parse_digest(&wire.training_run.sha256)?;
        if training_run_hash != expectations.training_run_hash() {
            return Err(BundleError::TrainingRunHashMismatch);
        }
        let training_run_reference =
            BundleMetadataRef::try_new(&wire.training_run.path, training_run_hash)?;
        let expected_training_run_size = usize::try_from(wire.training_run.size_bytes)
            .map_err(|_| BundleError::TrainingRunTooLarge)?;
        if expected_training_run_size > MAX_TRAINING_RUN_BYTES {
            return Err(BundleError::TrainingRunTooLarge);
        }

        let features = validate_features(&wire.features, feature_registry)?;
        validate_dataset(&wire.training_dataset, expectations)?;
        if wire.training_universe_id != expectations.universe_id().as_str() {
            return Err(BundleError::UniverseMismatch);
        }
        if wire.training_period.start_unix_nanos
            != expectations.training_period().start().unix_nanos()
            || wire.training_period.end_unix_nanos
                != expectations.training_period().end().unix_nanos()
        {
            return Err(BundleError::TrainingPeriodMismatch);
        }
        validate_label(&wire.label, expectations)?;
        if wire.training_code_revision != expectations.training_code_revision()
            || !valid_revision(&wire.training_code_revision)
        {
            return Err(BundleError::TrainingCodeRevisionMismatch);
        }
        let validation_metrics = validate_metrics(&wire.validation_metrics, format)?;
        let thresholds = validate_thresholds(wire.decision_thresholds, format)?;
        validate_prose(&wire.intended_use).map_err(|_| BundleError::InvalidIntendedUse)?;
        validation::validate_limitations(&wire.limitations)?;
        validation::validate_fallback(&wire.fallback)?;

        let training_run_bytes = read_exact_bounded(
            &root.directory,
            training_run_reference.relative_path(),
            MAX_TRAINING_RUN_BYTES,
            BundleError::TrainingRunTooLarge,
        )?;
        if training_run_bytes.len() != expected_training_run_size {
            return Err(BundleError::TrainingRunSizeMismatch);
        }
        if sha256_digest(&training_run_bytes) != training_run_hash {
            return Err(BundleError::TrainingRunHashMismatch);
        }
        validate_json_structure(&training_run_bytes)
            .map_err(|_| BundleError::TrainingRunStructureLimit)?;
        let run: TrainingRunWire = serde_json::from_slice(&training_run_bytes)
            .map_err(|_| BundleError::TrainingRunSyntax)?;
        validate_training_run(&run, &wire, expectations, format)?;

        let artifact_bytes = read_exact_bounded(
            &root.directory,
            artifact_reference.relative_path(),
            MAX_ARTIFACT_BYTES,
            BundleError::ArtifactTooLarge,
        )?;
        if artifact_bytes.len() != expected_artifact_size {
            return Err(BundleError::ArtifactSizeMismatch);
        }
        if sha256_digest(&artifact_bytes) != artifact_hash {
            return Err(BundleError::ArtifactHashMismatch);
        }
        validate_json_structure(&artifact_bytes)
            .map_err(|_| BundleError::ArtifactStructureLimit)?;
        let artifact_wire: NativeArtifactWire =
            serde_json::from_slice(&artifact_bytes).map_err(|_| BundleError::ArtifactSyntax)?;
        let artifact = validate_artifact(artifact_wire, format, &features)?;

        let metadata = ModelMetadata::new(
            expectations,
            metadata_hash,
            artifact_hash,
            format,
            wire.artifact.format_version,
            features,
            validation_metrics,
            thresholds,
            wire.intended_use,
            wire.limitations,
            wire.fallback.reason,
        );
        let retained_bytes = size_of::<Self>()
            .checked_add(
                metadata
                    .retained_bytes()
                    .ok_or(BundleError::RetainedSizeOverflow)?,
            )
            .and_then(|bytes| bytes.checked_add(artifact.retained_bytes()?))
            .and_then(|bytes| bytes.checked_add(metadata_bytes.len()))
            .and_then(|bytes| bytes.checked_add(artifact_bytes.len()))
            .and_then(|bytes| bytes.checked_add(training_run_bytes.len()))
            .ok_or(BundleError::RetainedSizeOverflow)?;
        Ok(Self {
            metadata,
            native_artifact: artifact,
            metadata_bytes: metadata_bytes.into_boxed_slice(),
            artifact_bytes: artifact_bytes.into_boxed_slice(),
            training_run_bytes: training_run_bytes.into_boxed_slice(),
            retained_bytes,
        })
    }

    /// Returns complete validated metadata.
    #[must_use]
    pub const fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    /// Returns the exact retained footprint used by registry admission.
    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Returns the exact admitted metadata bytes for reproducibility export.
    #[must_use]
    pub fn metadata_bytes(&self) -> &[u8] {
        &self.metadata_bytes
    }

    /// Returns the exact admitted artifact bytes for reproducibility export.
    #[must_use]
    pub fn artifact_bytes(&self) -> &[u8] {
        &self.artifact_bytes
    }

    /// Returns the exact admitted training-run provenance bytes.
    #[must_use]
    pub fn training_run_bytes(&self) -> &[u8] {
        &self.training_run_bytes
    }

    pub(crate) const fn native_artifact(&self) -> &NativeArtifact {
        &self.native_artifact
    }
}

/// Model-bundle admission or validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BundleError {
    #[error("model bundle path is outside the controlled relative-path grammar")]
    InvalidControlledPath,
    #[error("controlled model root is unavailable")]
    ControlledRootUnavailable,
    #[error("model bundle local read failed")]
    ReadFailure,
    #[error("model bundle metadata exceeds its byte bound")]
    MetadataTooLarge,
    #[error("model artifact exceeds its byte bound")]
    ArtifactTooLarge,
    #[error("model training-run provenance exceeds its byte bound")]
    TrainingRunTooLarge,
    #[error("model bundle metadata hash mismatch")]
    MetadataHashMismatch,
    #[error("model metadata exceeds JSON structural bounds")]
    MetadataStructureLimit,
    #[error("model bundle metadata syntax is invalid")]
    MetadataSyntax,
    #[error("model bundle metadata version is unsupported")]
    UnsupportedMetadataVersion,
    #[error("model artifact schema version is unsupported")]
    UnsupportedArtifactSchemaVersion,
    #[error("model identity differs from independent expectations")]
    ModelIdentityMismatch,
    #[error("bundle identity differs from independent expectations")]
    BundleIdentityMismatch,
    #[error("model artifact format is unsupported")]
    UnsupportedFormat,
    #[error("model artifact format version is unsupported")]
    UnsupportedFormatVersion,
    #[error("model bundle digest encoding is invalid")]
    InvalidDigest,
    #[error("model feature count is invalid")]
    InvalidFeatureCount,
    #[error("model feature identity does not resolve exactly")]
    FeatureIdentityMismatch,
    #[error("model feature schema digest mismatch")]
    FeatureSchemaMismatch,
    #[error("model feature semantic digest mismatch")]
    FeatureSemanticMismatch,
    #[error("model feature order differs from artifact coefficient order")]
    FeatureOrderMismatch,
    #[error("model feature normalizer is invalid")]
    InvalidNormalizer,
    #[error("model training dataset identity mismatch")]
    DatasetMismatch,
    #[error("model training universe identity mismatch")]
    UniverseMismatch,
    #[error("model training period mismatch")]
    TrainingPeriodMismatch,
    #[error("model label identity mismatch")]
    LabelMismatch,
    #[error("model training code revision mismatch")]
    TrainingCodeRevisionMismatch,
    #[error("model validation metrics are invalid")]
    InvalidValidationMetrics,
    #[error("model decision thresholds are invalid")]
    InvalidDecisionThresholds,
    #[error("model intended use is invalid")]
    InvalidIntendedUse,
    #[error("model limitations are invalid")]
    InvalidLimitations,
    #[error("model fallback contract is invalid")]
    InvalidFallback,
    #[error("model artifact size mismatch")]
    ArtifactSizeMismatch,
    #[error("model artifact hash mismatch")]
    ArtifactHashMismatch,
    #[error("model training-run provenance size mismatch")]
    TrainingRunSizeMismatch,
    #[error("model training-run provenance hash mismatch")]
    TrainingRunHashMismatch,
    #[error("model training-run provenance exceeds JSON structural bounds")]
    TrainingRunStructureLimit,
    #[error("model training-run provenance syntax is invalid")]
    TrainingRunSyntax,
    #[error("model training-run provenance version is unsupported")]
    UnsupportedTrainingRunVersion,
    #[error("model training-run trial identity hash mismatch")]
    TrainingRunTrialHashMismatch,
    #[error("model training-run provenance contradicts bundle authority")]
    TrainingRunRelationshipMismatch,
    #[error("model artifact exceeds JSON structural bounds")]
    ArtifactStructureLimit,
    #[error("model artifact syntax is invalid")]
    ArtifactSyntax,
    #[error("model artifact tensor shape is invalid")]
    InvalidTensorShape,
    #[error("model artifact must produce exactly one output")]
    UnsupportedOutputShape,
    #[error("model artifact contains a nonfinite tensor value")]
    NonFiniteArtifact,
    #[error("model bundle retained-byte accounting overflowed")]
    RetainedSizeOverflow,
}

impl From<ModelMetadataError> for BundleError {
    fn from(value: ModelMetadataError) -> Self {
        match value {
            ModelMetadataError::InvalidNormalizer => Self::InvalidNormalizer,
            ModelMetadataError::InvalidBundleId
            | ModelMetadataError::ReservedDigest
            | ModelMetadataError::InvalidTrainingPeriod
            | ModelMetadataError::InvalidExpectations => Self::BundleIdentityMismatch,
        }
    }
}
