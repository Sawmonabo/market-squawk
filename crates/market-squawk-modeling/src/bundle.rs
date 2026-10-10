//! Capability-rooted bundle reads and closed fail-closed validation.

#[cfg(feature = "release-evidence")]
#[path = "benchmark_support.rs"]
pub(crate) mod benchmark_support;

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
    FORECAST_POLICY_PATH, FORECAST_RESIDUALS_PATH, ForecastPolicyWire, METADATA_SCHEMA_VERSION,
    MetadataWire, NATIVE_FORMAT_VERSION, NativeArtifactWire, TrainingRunWire, parse_digest,
    parse_format, validate_artifact, validate_dataset, validate_features,
    validate_forecast_calibration, validate_label, validate_metrics, validate_output_measurement,
    validate_output_semantics, validate_output_statistic, validate_prose, validate_thresholds,
    validate_training_run,
};
use crate::metadata::valid_revision;
use crate::native::NativeArtifact;
use crate::{BundleExpectations, ModelMetadata, ModelMetadataError};

mod io;
mod probability;
pub use probability::{ProbabilityCalibrationArtifacts, ProbabilityReliabilityBin};
mod validation;

/// Maximum UTF-8 bytes in one path relative to a controlled model root.
pub const MAX_CONTROLLED_MODEL_PATH_BYTES: usize = 256;
/// Maximum exact metadata bytes admitted before parsing.
pub const MAX_METADATA_BYTES: usize = 256 * 1024;
/// Maximum exact native artifact bytes admitted before parsing.
pub const MAX_ARTIFACT_BYTES: usize = 1024 * 1024;
/// Maximum exact ONNX protobuf bytes admitted before parsing.
pub const MAX_ONNX_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
/// Maximum exact training-run provenance bytes admitted before parsing.
pub const MAX_TRAINING_RUN_BYTES: usize = 256 * 1024;
/// Maximum retained little-endian finite calibration residual bytes.
pub const MAX_FORECAST_RESIDUAL_BYTES: usize = 16 * 1024 * 1024;
/// Maximum exact forecast interval-policy JSON bytes.
pub const MAX_FORECAST_POLICY_BYTES: usize = 64 * 1024;

/// Exercises the production bundle-metadata structural and wire decoders.
///
/// Invalid metadata is a normal fuzz input. This entry point retains no decoded value and exists
/// only with the `fuzzing` feature.
#[cfg(feature = "fuzzing")]
pub fn fuzz_parse_bundle_metadata(bytes: &[u8]) {
    if bytes.len() > MAX_METADATA_BYTES || validate_json_structure(bytes).is_err() {
        return;
    }
    let _metadata = serde_json::from_slice::<MetadataWire>(bytes);
}

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
    /// Retains an already capability-confined directory without reopening an ambient path.
    #[must_use]
    pub fn from_directory(directory: Dir) -> Self {
        Self { directory }
    }

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
    artifact: BundleArtifact,
    metadata_path: Box<str>,
    artifact_path: Box<str>,
    training_run_path: Box<str>,
    metadata_bytes: Box<[u8]>,
    artifact_bytes: Box<[u8]>,
    training_run_bytes: Box<[u8]>,
    forecast_residuals_bytes: Option<Box<[u8]>>,
    forecast_policy_bytes: Option<Box<[u8]>>,
    probability_outcomes_bytes: Option<Box<[u8]>>,
    probability_policy_bytes: Option<Box<[u8]>>,
    forecast_residual_distribution: Option<crate::ForecastResidualDistribution>,
    retained_bytes: usize,
    forecast_tensor_layout: Option<validation::ForecastTensorLayout>,
}

#[derive(Debug)]
enum BundleArtifact {
    Native(NativeArtifact),
    Onnx,
}

/// Exact immutable selection evidence; deliberately contains no weight bytes or execution authority.
#[derive(Clone, Debug)]
pub struct ModelSelectionMetadata {
    metadata: ModelMetadata,
    training_run_bytes: Box<[u8]>,
    residual_distribution_available: bool,
}
impl ModelSelectionMetadata {
    /// Admitted model, dataset, and output contract.
    #[must_use]
    pub const fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }
    /// Exact independently hashed training provenance.
    #[must_use]
    pub fn training_run_bytes(&self) -> &[u8] {
        &self.training_run_bytes
    }
    /// Whether initial evidence includes the native admitted residual distribution.
    #[must_use]
    pub const fn residual_distribution_available(&self) -> bool {
        self.residual_distribution_available
    }
}

struct LoadedModelMetadata {
    metadata: ModelMetadata,
    metadata_bytes: Vec<u8>,
    training_run_bytes: Vec<u8>,
    artifact_reference: BundleMetadataRef,
    training_run_reference: BundleMetadataRef,
    expected_artifact_size: usize,
    artifact_byte_limit: usize,
    forecast_residuals_bytes: Option<Box<[u8]>>,
    forecast_policy_bytes: Option<Box<[u8]>>,
    probability_outcomes_bytes: Option<Box<[u8]>>,
    probability_policy_bytes: Option<Box<[u8]>>,
    forecast_residual_distribution: Option<crate::ForecastResidualDistribution>,
    forecast_tensor_layout: Option<validation::ForecastTensorLayout>,
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
        let LoadedModelMetadata {
            metadata,
            metadata_bytes,
            training_run_bytes,
            artifact_reference,
            training_run_reference,
            expected_artifact_size,
            artifact_byte_limit,
            forecast_residuals_bytes,
            forecast_policy_bytes,
            probability_outcomes_bytes,
            probability_policy_bytes,
            forecast_residual_distribution,
            forecast_tensor_layout,
        } = Self::read_metadata(root, reference, expectations, feature_registry)?;
        let format = metadata.format();
        let artifact_hash = metadata.artifact_hash();
        let artifact_bytes = read_exact_bounded(
            &root.directory,
            artifact_reference.relative_path(),
            artifact_byte_limit,
            BundleError::ArtifactTooLarge,
        )?;
        if artifact_bytes.len() != expected_artifact_size {
            return Err(BundleError::ArtifactSizeMismatch);
        }
        if sha256_digest(&artifact_bytes) != artifact_hash {
            return Err(BundleError::ArtifactHashMismatch);
        }
        let artifact = if format == crate::ModelFormat::Onnx {
            BundleArtifact::Onnx
        } else {
            validate_json_structure(&artifact_bytes)
                .map_err(|_| BundleError::ArtifactStructureLimit)?;
            let artifact_wire: NativeArtifactWire =
                serde_json::from_slice(&artifact_bytes).map_err(|_| BundleError::ArtifactSyntax)?;
            BundleArtifact::Native(validate_artifact(
                artifact_wire,
                format,
                metadata.features(),
            )?)
        };

        let layout_bytes = forecast_tensor_layout.as_ref().map_or(0, |layout| {
            (layout.lags.len() + layout.horizons.len()) * size_of::<u32>() + layout.strategy.len()
        });
        let retained_bytes = size_of::<Self>()
            .checked_add(layout_bytes)
            .ok_or(BundleError::RetainedSizeOverflow)?
            .checked_add(
                metadata
                    .retained_bytes()
                    .ok_or(BundleError::RetainedSizeOverflow)?,
            )
            .and_then(|bytes| match &artifact {
                BundleArtifact::Native(artifact) => bytes.checked_add(artifact.retained_bytes()?),
                BundleArtifact::Onnx => Some(bytes),
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    probability_outcomes_bytes
                        .as_ref()
                        .map_or(0, |value| value.len()),
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    probability_policy_bytes
                        .as_ref()
                        .map_or(0, |value| value.len()),
                )
            })
            .and_then(|bytes| bytes.checked_add(metadata_bytes.len()))
            .and_then(|bytes| bytes.checked_add(artifact_bytes.len()))
            .and_then(|bytes| bytes.checked_add(training_run_bytes.len()))
            .and_then(|bytes| bytes.checked_add(reference.relative_path().len()))
            .and_then(|bytes| bytes.checked_add(artifact_reference.relative_path().len()))
            .and_then(|bytes| bytes.checked_add(training_run_reference.relative_path().len()))
            .and_then(|bytes| {
                bytes.checked_add(
                    forecast_residuals_bytes
                        .as_ref()
                        .map_or(0, |value| value.len()),
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    forecast_policy_bytes
                        .as_ref()
                        .map_or(0, |value| value.len()),
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    forecast_residual_distribution
                        .as_ref()
                        .map_or(0, crate::ForecastResidualDistribution::retained_bytes),
                )
            })
            .ok_or(BundleError::RetainedSizeOverflow)?;
        Ok(Self {
            metadata,
            artifact,
            metadata_path: reference.relative_path().into(),
            artifact_path: artifact_reference.relative_path().into(),
            training_run_path: training_run_reference.relative_path().into(),
            metadata_bytes: metadata_bytes.into_boxed_slice(),
            artifact_bytes: artifact_bytes.into_boxed_slice(),
            training_run_bytes: training_run_bytes.into_boxed_slice(),
            forecast_residuals_bytes,
            forecast_policy_bytes,
            probability_outcomes_bytes,
            probability_policy_bytes,
            forecast_residual_distribution,
            forecast_tensor_layout,
            retained_bytes,
        })
    }

    /// Reopens admitted selection evidence without reading or compiling the model weight artifact.
    /// The returned value cannot be used as an inference backend or a newly admitted bundle.
    pub(crate) fn load_selection_metadata(
        root: &ControlledModelRoot,
        reference: &BundleMetadataRef,
        expectations: &BundleExpectations,
        feature_registry: &FeatureRegistry,
    ) -> Result<ModelSelectionMetadata, BundleError> {
        let loaded = Self::read_metadata(root, reference, expectations, feature_registry)?;
        Ok(ModelSelectionMetadata {
            metadata: loaded.metadata,
            training_run_bytes: loaded.training_run_bytes.into_boxed_slice(),
            residual_distribution_available: loaded.forecast_residual_distribution.is_some(),
        })
    }

    fn read_metadata(
        root: &ControlledModelRoot,
        reference: &BundleMetadataRef,
        expectations: &BundleExpectations,
        feature_registry: &FeatureRegistry,
    ) -> Result<LoadedModelMetadata, BundleError> {
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
        let output_semantics = validate_output_semantics(
            &wire.output_semantics,
            format,
            expectations.output_semantics(),
        )?;
        validate_output_measurement(&wire.output_measurement, expectations)?;
        validate_output_statistic(&wire.output_statistic, expectations)?;
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
        let artifact_byte_limit = match format {
            crate::ModelFormat::Onnx => MAX_ONNX_ARTIFACT_BYTES,
            crate::ModelFormat::NativeLinear | crate::ModelFormat::NativeLogistic => {
                MAX_ARTIFACT_BYTES
            }
        };
        if expected_artifact_size > artifact_byte_limit {
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
        if wire
            .training_period
            .decode()
            .map_err(|_| BundleError::TrainingPeriodMismatch)?
            != expectations.training_period()
        {
            return Err(BundleError::TrainingPeriodMismatch);
        }
        validate_label(&wire.label, expectations)?;
        if wire.training_code_revision != expectations.training_code_revision()
            || !valid_revision(&wire.training_code_revision)
        {
            return Err(BundleError::TrainingCodeRevisionMismatch);
        }
        if parse_digest(&wire.training_environment_sha256)?
            != expectations.training_environment_hash()
        {
            return Err(BundleError::TrainingRunRelationshipMismatch);
        }
        let validation_metrics = validate_metrics(&wire.validation_metrics, output_semantics)?;
        let thresholds = validate_thresholds(wire.decision_thresholds, output_semantics)?;
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
        validate_training_run(&run, &wire, expectations, format, output_semantics)?;
        let forecast_tensor_layout = run.forecast_tensor_layout();

        let (
            forecast_calibration,
            forecast_residuals_bytes,
            forecast_policy_bytes,
            forecast_residual_distribution,
        ) = match wire.forecast_calibration.as_ref() {
            Some(reference) => {
                if !matches!(
                    format,
                    crate::ModelFormat::NativeLinear | crate::ModelFormat::Onnx
                ) || output_semantics != crate::ModelOutputSemantics::Regression
                    || reference.residuals.path != FORECAST_RESIDUALS_PATH
                    || reference.policy.path != FORECAST_POLICY_PATH
                    || reference.residuals.path == wire.artifact.path
                    || reference.residuals.path == wire.training_run.path
                    || reference.policy.path == wire.artifact.path
                    || reference.policy.path == wire.training_run.path
                    || reference.policy.path == reference.residuals.path
                {
                    return Err(BundleError::InvalidForecastCalibration);
                }
                let residuals_hash = parse_digest(&reference.residuals.sha256)?;
                let policy_hash = parse_digest(&reference.policy.sha256)?;
                let residuals_reference =
                    BundleMetadataRef::try_new(&reference.residuals.path, residuals_hash)?;
                let policy_reference =
                    BundleMetadataRef::try_new(&reference.policy.path, policy_hash)?;
                let residuals_size = usize::try_from(reference.residuals.size_bytes)
                    .map_err(|_| BundleError::ForecastCalibrationTooLarge)?;
                let policy_size = usize::try_from(reference.policy.size_bytes)
                    .map_err(|_| BundleError::ForecastCalibrationTooLarge)?;
                if residuals_size == 0
                    || residuals_size > MAX_FORECAST_RESIDUAL_BYTES
                    || policy_size == 0
                    || policy_size > MAX_FORECAST_POLICY_BYTES
                {
                    return Err(BundleError::ForecastCalibrationTooLarge);
                }
                let residuals = read_exact_bounded(
                    &root.directory,
                    residuals_reference.relative_path(),
                    MAX_FORECAST_RESIDUAL_BYTES,
                    BundleError::ForecastCalibrationTooLarge,
                )?;
                let policy = read_exact_bounded(
                    &root.directory,
                    policy_reference.relative_path(),
                    MAX_FORECAST_POLICY_BYTES,
                    BundleError::ForecastCalibrationTooLarge,
                )?;
                if residuals.len() != residuals_size || policy.len() != policy_size {
                    return Err(BundleError::ForecastCalibrationSizeMismatch);
                }
                if sha256_digest(&residuals) != residuals_hash
                    || sha256_digest(&policy) != policy_hash
                {
                    return Err(BundleError::ForecastCalibrationHashMismatch);
                }
                validate_json_structure(&policy)
                    .map_err(|_| BundleError::ForecastCalibrationStructureLimit)?;
                let policy_wire: ForecastPolicyWire = serde_json::from_slice(&policy)
                    .map_err(|_| BundleError::ForecastCalibrationSyntax)?;
                let calibration =
                    validate_forecast_calibration(reference, policy_wire, &residuals, &run)?;
                let distribution = validation::admitted_residual_distribution(
                    &residuals,
                    residuals_hash,
                    training_run_hash,
                    &run,
                )?;
                (
                    Some(calibration),
                    Some(residuals.into_boxed_slice()),
                    Some(policy.into_boxed_slice()),
                    distribution,
                )
            }
            None => (None, None, None, None),
        };

        let (probability_calibration, probability_outcomes_bytes, probability_policy_bytes) =
            match wire.probability_calibration.as_ref() {
                Some(reference) => {
                    let (proof, outcomes, policy) =
                        probability::load(root, reference, &run, expectations, format)?;
                    (Some(proof), Some(outcomes), Some(policy))
                }
                None if output_semantics != crate::ModelOutputSemantics::BinaryProbability => {
                    (None, None, None)
                }
                None => return Err(BundleError::InvalidProbabilityCalibration),
            };

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
        )
        .with_forecast_calibration(forecast_calibration)
        .with_probability_calibration(probability_calibration);
        Ok(LoadedModelMetadata {
            metadata,
            metadata_bytes,
            training_run_bytes,
            artifact_reference,
            training_run_reference,
            expected_artifact_size,
            artifact_byte_limit,
            forecast_residuals_bytes,
            forecast_policy_bytes,
            probability_outcomes_bytes,
            probability_policy_bytes,
            forecast_residual_distribution,
            forecast_tensor_layout,
        })
    }

    /// Copies the already admitted compact selection evidence without retaining weights.
    #[must_use]
    pub fn selection_metadata(&self) -> ModelSelectionMetadata {
        ModelSelectionMetadata {
            metadata: self.metadata.clone(),
            training_run_bytes: self.training_run_bytes.clone(),
            residual_distribution_available: self.forecast_residual_distribution.is_some(),
        }
    }

    /// Exact research tensor layout admitted from the hashed training trial:
    /// raw lag columns, output observation offsets, and fitted strategy.
    #[must_use]
    pub fn research_forecast_layout(&self) -> Option<(&[u32], &[u32], &str)> {
        self.forecast_tensor_layout.as_ref().map(|layout| {
            (
                layout.lags.as_ref(),
                layout.horizons.as_ref(),
                layout.strategy.as_ref(),
            )
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

    /// Returns the first-class admitted residual member for a forecast bundle.
    #[must_use]
    pub fn forecast_residuals_bytes(&self) -> Option<&[u8]> {
        self.forecast_residuals_bytes.as_deref()
    }

    /// Returns the first-class admitted interval-policy member for a forecast bundle.
    #[must_use]
    pub fn forecast_policy_bytes(&self) -> Option<&[u8]> {
        self.forecast_policy_bytes.as_deref()
    }

    /// Empirical residual frequencies produced from this exact frozen direct estimator.
    ///
    /// Interval-only and autoregressive forecast bundles have no admitted distribution.
    #[must_use]
    pub const fn forecast_residual_distribution(
        &self,
    ) -> Option<&crate::ForecastResidualDistribution> {
        self.forecast_residual_distribution.as_ref()
    }

    /// Derives one native-unit terminal distribution from this exact bundle and its vintage.
    ///
    /// The bounded support uses frozen validation residuals, never calibrated interval endpoints.
    pub fn terminal_distribution(
        &self,
        vintage: &crate::ForecastVintage,
    ) -> Result<Option<crate::ForecastTerminalDistribution>, crate::ForecastError> {
        crate::ForecastTerminalDistribution::try_from_bundle(self, vintage)
    }

    /// Iterates every exact admitted member in stable semantic order.
    ///
    /// Paths were validated by the same controlled-path grammar used during admission. Digests
    /// name the exact retained bytes and optional forecast members are present as an inseparable
    /// pair.
    pub fn retained_members(
        &self,
    ) -> impl Iterator<Item = (&'static str, &str, &[u8], Sha256Digest)> {
        let metadata = self.metadata();
        [
            Some((
                "metadata",
                self.metadata_path.as_ref(),
                self.metadata_bytes.as_ref(),
                metadata.metadata_hash(),
            )),
            Some((
                "artifact",
                self.artifact_path.as_ref(),
                self.artifact_bytes.as_ref(),
                metadata.artifact_hash(),
            )),
            Some((
                "training_run",
                self.training_run_path.as_ref(),
                self.training_run_bytes.as_ref(),
                metadata.training_run_hash(),
            )),
            self.forecast_residuals_bytes.as_deref().map(|bytes| {
                (
                    "forecast_residuals",
                    FORECAST_RESIDUALS_PATH,
                    bytes,
                    sha256_digest(bytes),
                )
            }),
            self.probability_outcomes_bytes.as_deref().map(|bytes| {
                (
                    "probability_outcomes",
                    probability::OUTCOMES_PATH,
                    bytes,
                    sha256_digest(bytes),
                )
            }),
            self.probability_policy_bytes.as_deref().map(|bytes| {
                (
                    "probability_policy",
                    probability::POLICY_PATH,
                    bytes,
                    sha256_digest(bytes),
                )
            }),
            self.forecast_policy_bytes.as_deref().map(|bytes| {
                (
                    "forecast_policy",
                    FORECAST_POLICY_PATH,
                    bytes,
                    sha256_digest(bytes),
                )
            }),
        ]
        .into_iter()
        .flatten()
    }

    pub(crate) fn verify_probability_sources(
        &self,
        selection: &market_squawk_data::PythonDatasetSelection,
    ) -> Result<(), BundleError> {
        probability::verify_sources(self, selection)
    }

    pub(crate) const fn native_artifact(&self) -> Option<&NativeArtifact> {
        match &self.artifact {
            BundleArtifact::Native(artifact) => Some(artifact),
            BundleArtifact::Onnx => None,
        }
    }

    #[cfg(feature = "onnx-tract")]
    pub(crate) fn onnx_artifact_bytes(&self) -> Option<&[u8]> {
        match self.artifact {
            BundleArtifact::Onnx => Some(&self.artifact_bytes),
            BundleArtifact::Native(_) => None,
        }
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
    #[error("forecast calibration member exceeds its byte bound")]
    ForecastCalibrationTooLarge,
    #[error("model bundle metadata hash mismatch")]
    MetadataHashMismatch,
    #[error("model metadata exceeds JSON structural bounds")]
    MetadataStructureLimit,
    #[error("model bundle metadata syntax is invalid")]
    MetadataSyntax,
    #[error("model bundle metadata version is unsupported")]
    UnsupportedMetadataVersion,
    #[error("model output semantics are invalid or differ from independent authority")]
    InvalidOutputSemantics,
    #[error("model output measurement is invalid or differs from admitted label rows")]
    InvalidOutputMeasurement,
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
    #[error("forecast calibration member size mismatch")]
    ForecastCalibrationSizeMismatch,
    #[error("forecast calibration member hash mismatch")]
    ForecastCalibrationHashMismatch,
    #[error("forecast calibration policy exceeds JSON structural bounds")]
    ForecastCalibrationStructureLimit,
    #[error("forecast calibration policy syntax is invalid")]
    ForecastCalibrationSyntax,
    #[error("forecast calibration members or decoded policy are invalid")]
    InvalidForecastCalibration,
    #[error("binary event calibration or original outcome evidence is invalid")]
    InvalidProbabilityCalibration,
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
