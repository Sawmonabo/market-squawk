//! Closed production inference fixture used by the release-evidence command.

use std::mem::size_of;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use market_squawk_analytics::{LiveFeatureCatalog, LiveFeatureCatalogConfig};
use market_squawk_data::{
    CatalogEndpointIdentity, ComponentKind, ComponentScope, CorporateActionSensitivity,
    DatasetBuildSpecDigest, DatasetId, DatasetManifestRef, DatasetSchemaRegistry,
    FeatureLabelComponentSpec, Sha256Digest, UniverseId,
};
use market_squawk_domain::{ModelId, Timestamp};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{BundleArtifact, ModelBundle};
use crate::native::NativeArtifact;
use crate::{
    BundleExpectations, BundleId, DecisionThresholds, FeatureNormalizer, InferenceBackend,
    InferenceError, ModelFeatureBinding, ModelFeatureValue, ModelFormat, ModelInput,
    ModelInputError, ModelMetadata, ModelOutput, ModelOutputSemantics, NativeBackendError,
    NativeLinearBackend, OnnxBackendError, OnnxFallbackPolicy, OnnxModelPolicy, OnnxWorkerProgram,
    OnnxWorkerProgramError, TractOnnxBackend, TrainingDatasetIdentity, TrainingPeriod,
    ValidationMetric, ValidationMetricName,
};

const ONNX_MANIFEST: &str = include_str!("../fixtures/onnx/manifest.json");
const FORMAT_VERSION: u32 = 1;

/// Immutable identities for the exact native and ONNX release-evidence fixtures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReleaseEvidenceInferenceIdentity {
    native_artifact_digest: [u8; 32],
    onnx_artifact_digest: [u8; 32],
    onnx_policy_digest: [u8; 32],
    onnx_worker_digest: [u8; 32],
    onnx_runtime_semantics_digest: [u8; 32],
    onnx_warm_up_digest: [u8; 32],
    native_retained_bytes: usize,
    onnx_retained_bytes: usize,
}

impl ReleaseEvidenceInferenceIdentity {
    /// Returns the exact native fixture artifact digest.
    #[must_use]
    pub const fn native_artifact_digest(self) -> [u8; 32] {
        self.native_artifact_digest
    }
    /// Returns the exact ONNX fixture artifact digest.
    #[must_use]
    pub const fn onnx_artifact_digest(self) -> [u8; 32] {
        self.onnx_artifact_digest
    }
    /// Returns the exact ONNX graph-policy digest.
    #[must_use]
    pub const fn onnx_policy_digest(self) -> [u8; 32] {
        self.onnx_policy_digest
    }
    /// Returns the exact admitted ONNX worker executable digest.
    #[must_use]
    pub const fn onnx_worker_digest(self) -> [u8; 32] {
        self.onnx_worker_digest
    }
    /// Returns the versioned helper-process runtime semantics digest.
    #[must_use]
    pub const fn onnx_runtime_semantics_digest(self) -> [u8; 32] {
        self.onnx_runtime_semantics_digest
    }
    /// Returns the exact ONNX warm-up result digest.
    #[must_use]
    pub const fn onnx_warm_up_digest(self) -> [u8; 32] {
        self.onnx_warm_up_digest
    }
    /// Returns the bounded native backend retained-byte charge.
    #[must_use]
    pub const fn native_retained_bytes(self) -> usize {
        self.native_retained_bytes
    }
    /// Returns the bounded ONNX backend retained-byte charge.
    #[must_use]
    pub const fn onnx_retained_bytes(self) -> usize {
        self.onnx_retained_bytes
    }
}

/// Closed, pre-admitted native and ONNX inference generation.
#[derive(Debug)]
pub struct ReleaseEvidenceInferenceFixture {
    native: NativeLinearBackend,
    native_values: Box<[ModelFeatureValue]>,
    onnx: TractOnnxBackend,
    onnx_values: Box<[ModelFeatureValue]>,
    identity: ReleaseEvidenceInferenceIdentity,
}

impl ReleaseEvidenceInferenceFixture {
    /// Constructs and warms both production inference backends.
    ///
    /// The ONNX artifact is the checked-in, digest-verified bounded graph fixture. The worker is
    /// admitted by exact executable digest before a private helper generation is started.
    ///
    /// # Errors
    ///
    /// Returns a typed failure if fixture identities, model policy, worker admission, or backend
    /// warm-up fail closed.
    pub fn try_new(
        worker_path: &Path,
        worker_digest: [u8; 32],
    ) -> Result<Self, ReleaseEvidenceInferenceError> {
        let bindings = feature_bindings()?;
        let dataset = training_dataset()?;
        let native_bytes = b"market-squawk/release-evidence/native-linear/v1";
        let native_digest = sha256(native_bytes);
        let native_bundle = Arc::new(build_bundle(
            BundleSeed {
                model_id: "018f3c2a-91ab-7ccd-b3de-123456789abc",
                bundle_id: "release-evidence-native",
                artifact_digest: native_digest,
                metadata_digest: sha256(b"release-evidence-native-metadata-v1"),
                training_run_digest: sha256(b"release-evidence-native-training-run-v1"),
            },
            dataset.clone(),
            ModelFormat::NativeLinear,
            native_bytes,
            bindings.clone(),
            Some(NativeArtifact::new(
                ModelFormat::NativeLinear,
                bindings
                    .iter()
                    .map(ModelFeatureBinding::semantic_digest)
                    .collect(),
                vec![2.0, -1.0],
                0.5,
            )),
        )?);
        let native = NativeLinearBackend::try_from_bundle(native_bundle)
            .map_err(ReleaseEvidenceInferenceError::NativeBackend)?;
        let native_values = input_values(native.metadata())?;

        let onnx_fixture = decode_onnx_fixture()?;
        let onnx_digest = sha256(&onnx_fixture.bytes);
        if onnx_digest != onnx_fixture.digest {
            return Err(ReleaseEvidenceInferenceError::InvalidFixture);
        }
        let onnx_bundle = Arc::new(build_bundle(
            BundleSeed {
                model_id: "018f3c2a-91ab-7ccd-b3de-123456789abe",
                bundle_id: "release-evidence-onnx",
                artifact_digest: onnx_digest,
                metadata_digest: sha256(b"release-evidence-onnx-metadata-v1"),
                training_run_digest: sha256(b"release-evidence-onnx-training-run-v1"),
            },
            dataset,
            ModelFormat::Onnx,
            &onnx_fixture.bytes,
            bindings,
            None,
        )?);
        let policy = OnnxModelPolicy::try_new(
            Sha256Digest::new(onnx_digest),
            onnx_fixture.opset,
            &onnx_fixture.input_shape,
            &onnx_fixture.output_shape,
            Duration::from_millis(250),
            OnnxFallbackPolicy::NoAction,
        )
        .map_err(ReleaseEvidenceInferenceError::OnnxPolicy)?;
        let program = OnnxWorkerProgram::admit(worker_path, worker_digest)
            .map_err(ReleaseEvidenceInferenceError::WorkerAdmission)?;
        let onnx = TractOnnxBackend::try_from_bundle(onnx_bundle, policy, &program)
            .map_err(ReleaseEvidenceInferenceError::OnnxBackend)?;
        let onnx_values = input_values(onnx.metadata())?;
        let runtime = onnx.runtime_evidence();
        let identity = ReleaseEvidenceInferenceIdentity {
            native_artifact_digest: native_digest,
            onnx_artifact_digest: onnx_digest,
            onnx_policy_digest: runtime.policy_digest(),
            onnx_worker_digest: program.digest(),
            onnx_runtime_semantics_digest: runtime.worker_runtime_semantics_digest(),
            onnx_warm_up_digest: runtime.warm_up_digest(),
            native_retained_bytes: native.retained_bytes(),
            onnx_retained_bytes: onnx.retained_bytes(),
        };
        Ok(Self {
            native,
            native_values,
            onnx,
            onnx_values,
            identity,
        })
    }

    /// Runs one production native inference with exact input-identity validation.
    ///
    /// # Errors
    ///
    /// Returns a typed input or inference failure without substituting an action.
    pub fn infer_native(&self) -> Result<ModelOutput, ReleaseEvidenceInferenceError> {
        let input = ModelInput::try_new(self.native.metadata(), &self.native_values)
            .map_err(ReleaseEvidenceInferenceError::Input)?;
        self.native
            .infer(&input)
            .map_err(ReleaseEvidenceInferenceError::Inference)
    }

    /// Runs one production worker-isolated ONNX inference with exact input-identity validation.
    ///
    /// # Errors
    ///
    /// Returns a typed input or inference failure without substituting an action.
    pub fn infer_onnx(&self) -> Result<ModelOutput, ReleaseEvidenceInferenceError> {
        let input = ModelInput::try_new(self.onnx.metadata(), &self.onnx_values)
            .map_err(ReleaseEvidenceInferenceError::Input)?;
        self.onnx
            .infer(&input)
            .map_err(ReleaseEvidenceInferenceError::Inference)
    }

    /// Returns immutable fixture, policy, worker, and warm-up identities.
    #[must_use]
    pub const fn identity(&self) -> ReleaseEvidenceInferenceIdentity {
        self.identity
    }
}

/// Closed inference-fixture or execution failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ReleaseEvidenceInferenceError {
    /// A compile-time fixture or trusted benchmark contract was internally inconsistent.
    #[error("release-evidence inference fixture is invalid")]
    InvalidFixture,
    /// Native backend admission rejected the fixture.
    #[error("release-evidence native backend failed: {0}")]
    NativeBackend(NativeBackendError),
    /// ONNX graph policy rejected the fixture.
    #[error("release-evidence ONNX policy failed: {0}")]
    OnnxPolicy(crate::OnnxPolicyError),
    /// Exact ONNX worker admission failed.
    #[error("release-evidence ONNX worker admission failed: {0}")]
    WorkerAdmission(OnnxWorkerProgramError),
    /// ONNX backend construction or warm-up failed.
    #[error("release-evidence ONNX backend failed: {0}")]
    OnnxBackend(OnnxBackendError),
    /// Exact model input admission failed.
    #[error("release-evidence model input failed: {0}")]
    Input(ModelInputError),
    /// Production inference failed closed.
    #[error("release-evidence inference failed: {0}")]
    Inference(InferenceError),
}

#[derive(Clone, Copy)]
struct BundleSeed {
    model_id: &'static str,
    bundle_id: &'static str,
    artifact_digest: [u8; 32],
    metadata_digest: [u8; 32],
    training_run_digest: [u8; 32],
}

fn build_bundle(
    seed: BundleSeed,
    dataset: TrainingDatasetIdentity,
    format: ModelFormat,
    artifact_bytes: &[u8],
    features: Vec<ModelFeatureBinding>,
    native: Option<NativeArtifact>,
) -> Result<ModelBundle, ReleaseEvidenceInferenceError> {
    let expectations = BundleExpectations::try_new(
        ModelId::from_str(seed.model_id)
            .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?,
        BundleId::try_new(seed.bundle_id)
            .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?,
        NonZeroU64::MIN,
        dataset,
        UniverseId::try_from("release-evidence-universe")
            .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?,
        TrainingPeriod::try_new(Timestamp::from_unix_nanos(1), Timestamp::from_unix_nanos(2))
            .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?,
        FeatureLabelComponentSpec::try_new(
            ComponentKind::Label,
            ComponentScope::Instrument,
            CorporateActionSensitivity::RequiresAdjustment,
            "forward-return",
            NonZeroU32::MIN,
        )
        .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?,
        "release-evidence-v1",
        Sha256Digest::new([31; 32]),
        Sha256Digest::new(seed.metadata_digest),
        Sha256Digest::new(seed.artifact_digest),
        Sha256Digest::new(seed.training_run_digest),
    )
    .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?;
    let metadata = ModelMetadata::new(
        &expectations,
        Sha256Digest::new(seed.metadata_digest),
        Sha256Digest::new(seed.artifact_digest),
        format,
        FORMAT_VERSION,
        ModelOutputSemantics::Regression,
        false,
        features,
        vec![ValidationMetric::new(
            ValidationMetricName::MeanSquaredError,
            0.12,
        )],
        DecisionThresholds::new(-0.5, 0.5, 0.0),
        "bounded release performance evidence".to_owned(),
        vec!["synthetic fixture; not provider qualification".to_owned()],
        "no action on inference failure".to_owned(),
    );
    let artifact = match (format, native) {
        (ModelFormat::NativeLinear | ModelFormat::NativeLogistic, Some(artifact)) => {
            BundleArtifact::Native(artifact)
        }
        (ModelFormat::Onnx, None) => BundleArtifact::Onnx,
        _ => return Err(ReleaseEvidenceInferenceError::InvalidFixture),
    };
    let metadata_bytes: Box<[u8]> = seed.metadata_digest.into();
    let artifact_bytes: Box<[u8]> = artifact_bytes.into();
    let training_run_bytes: Box<[u8]> = seed.training_run_digest.into();
    let retained_bytes = size_of::<ModelBundle>()
        .checked_add(
            metadata
                .retained_bytes()
                .ok_or(ReleaseEvidenceInferenceError::InvalidFixture)?,
        )
        .and_then(|bytes| match &artifact {
            BundleArtifact::Native(artifact) => bytes.checked_add(artifact.retained_bytes()?),
            BundleArtifact::Onnx => Some(bytes),
        })
        .and_then(|bytes| bytes.checked_add(metadata_bytes.len()))
        .and_then(|bytes| bytes.checked_add(artifact_bytes.len()))
        .and_then(|bytes| bytes.checked_add(training_run_bytes.len()))
        .ok_or(ReleaseEvidenceInferenceError::InvalidFixture)?;
    Ok(ModelBundle {
        metadata,
        artifact,
        metadata_bytes,
        artifact_bytes,
        training_run_bytes,
        forecast_residuals_bytes: None,
        forecast_policy_bytes: None,
        retained_bytes,
    })
}

fn feature_bindings() -> Result<Vec<ModelFeatureBinding>, ReleaseEvidenceInferenceError> {
    let config = LiveFeatureCatalogConfig::try_new(
        NonZeroU32::new(50).ok_or(ReleaseEvidenceInferenceError::InvalidFixture)?,
        NonZeroU32::new(1_024).ok_or(ReleaseEvidenceInferenceError::InvalidFixture)?,
        NonZeroU32::new(4_096).ok_or(ReleaseEvidenceInferenceError::InvalidFixture)?,
        NonZeroU32::new(3).ok_or(ReleaseEvidenceInferenceError::InvalidFixture)?,
        NonZeroU64::new(60_000_000_000).ok_or(ReleaseEvidenceInferenceError::InvalidFixture)?,
        NonZeroU32::new(8).ok_or(ReleaseEvidenceInferenceError::InvalidFixture)?,
        NonZeroU64::new(250_000_000).ok_or(ReleaseEvidenceInferenceError::InvalidFixture)?,
    )
    .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?;
    let catalog = LiveFeatureCatalog::try_new(config, "release-evidence-v1")
        .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?;
    catalog
        .entries()
        .iter()
        .take(2)
        .enumerate()
        .map(|(index, metadata)| {
            let normalizer = if index == 0 {
                FeatureNormalizer::Identity
            } else {
                FeatureNormalizer::standard(10.0, 2.0)
                    .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?
            };
            Ok(ModelFeatureBinding::new(
                metadata.key().clone(),
                metadata.input_schema_digest(),
                metadata.semantic_digest(),
                normalizer,
            ))
        })
        .collect()
}

fn training_dataset() -> Result<TrainingDatasetIdentity, ReleaseEvidenceInferenceError> {
    let schema = DatasetSchemaRegistry::local()
        .canonical_feature_labels()
        .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?;
    let manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("release-evidence-feature-labels")
            .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?,
        1,
        schema,
        Sha256Digest::new([21; 32]),
    )
    .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?;
    TrainingDatasetIdentity::try_new(
        manifest,
        DatasetBuildSpecDigest::try_new([22; 32])
            .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?,
        Sha256Digest::new([23; 32]),
        Sha256Digest::new([24; 32]),
        CatalogEndpointIdentity::try_from_bytes([25; 32])
            .ok_or(ReleaseEvidenceInferenceError::InvalidFixture)?,
        Sha256Digest::new([26; 32]),
        Sha256Digest::new([27; 32]),
        Timestamp::from_unix_nanos(3),
        NonZeroU64::MIN,
    )
    .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)
}

fn input_values(
    metadata: &ModelMetadata,
) -> Result<Box<[ModelFeatureValue]>, ReleaseEvidenceInferenceError> {
    let mut values = metadata
        .features()
        .iter()
        .map(ModelFeatureValue::from_binding)
        .collect::<Vec<_>>();
    if values.len() != 2 {
        return Err(ReleaseEvidenceInferenceError::InvalidFixture);
    }
    values[0]
        .try_set_value(3.0)
        .map_err(ReleaseEvidenceInferenceError::Input)?;
    values[1]
        .try_set_value(14.0)
        .map_err(ReleaseEvidenceInferenceError::Input)?;
    Ok(values.into_boxed_slice())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureManifest {
    schema_version: u32,
    models: Vec<FixtureModel>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureModel {
    artifact_sha256: String,
    id: String,
    input_shape: Vec<usize>,
    model_hex: String,
    opset: u32,
    output_shape: Vec<usize>,
}

struct DecodedOnnxFixture {
    bytes: Vec<u8>,
    digest: [u8; 32],
    input_shape: Vec<usize>,
    output_shape: Vec<usize>,
    opset: u32,
}

fn decode_onnx_fixture() -> Result<DecodedOnnxFixture, ReleaseEvidenceInferenceError> {
    let manifest: FixtureManifest = serde_json::from_str(ONNX_MANIFEST)
        .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?;
    if manifest.schema_version != 1 || manifest.models.len() != 1 {
        return Err(ReleaseEvidenceInferenceError::InvalidFixture);
    }
    let model = manifest
        .models
        .into_iter()
        .next()
        .ok_or(ReleaseEvidenceInferenceError::InvalidFixture)?;
    if model.id != "bounded-gemm-v1"
        || model.opset != 13
        || model.input_shape != [1, 2]
        || model.output_shape != [1, 1]
    {
        return Err(ReleaseEvidenceInferenceError::InvalidFixture);
    }
    Ok(DecodedOnnxFixture {
        bytes: decode_hex(&model.model_hex)?,
        digest: decode_digest(&model.artifact_sha256)?,
        input_shape: model.input_shape,
        output_shape: model.output_shape,
        opset: model.opset,
    })
}

fn decode_digest(value: &str) -> Result<[u8; 32], ReleaseEvidenceInferenceError> {
    decode_hex(value)?
        .try_into()
        .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)
}

fn decode_hex(value: &str) -> Result<Vec<u8>, ReleaseEvidenceInferenceError> {
    if !value.len().is_multiple_of(2) {
        return Err(ReleaseEvidenceInferenceError::InvalidFixture);
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair)
                .map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)?;
            u8::from_str_radix(text, 16).map_err(|_| ReleaseEvidenceInferenceError::InvalidFixture)
        })
        .collect()
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
