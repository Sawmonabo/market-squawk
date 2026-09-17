use std::error::Error;
use std::fs;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::str::FromStr;

use crate::{
    BundleError, BundleExpectations, BundleId, BundleMetadataRef, BundleRegistration,
    ControlledModelRoot, ForecastCentralStatistic, ForecastEstimatorProfile, ForecastMeasurement,
    ForecastOutputBinding, ForecastTargetMeaning, ForecastTrainingObjective, ForecastTransform,
    MAX_ARTIFACT_BYTES, ModelBundle, ModelOutputSemantics, ModelRegistry, ModelRegistryError,
    TrainingDatasetIdentity, TrainingPeriod,
};
use market_squawk_analytics::{
    FeatureKey, FeatureMetadata, FeatureRegistry, LiveFeatureCatalog, LiveFeatureCatalogConfig,
};
use market_squawk_data::{
    CatalogEndpointIdentity, ComponentKind, ComponentScope, CorporateActionSensitivity,
    DatasetBuildSpecDigest, DatasetId, DatasetManifestRef, DatasetSchemaRegistry,
    FeatureLabelComponentSpec, Sha256Digest, UniverseId,
};
use market_squawk_domain::{ModelId, Timestamp};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn Error>>;

pub(crate) struct Fixture {
    temporary: TempDir,
    root: ControlledModelRoot,
    reference: BundleMetadataRef,
    expectations: BundleExpectations,
    registry: FeatureRegistry,
    feature_keys: [FeatureKey; 2],
}

impl Fixture {
    pub(crate) fn load(&self) -> Result<ModelBundle, BundleError> {
        ModelBundle::load(
            &self.root,
            &self.reference,
            &self.expectations,
            &self.registry,
        )
    }

    pub(crate) fn feature(&self, index: usize) -> TestResult<&FeatureMetadata> {
        let key = self
            .feature_keys
            .get(index)
            .ok_or_else(|| std::io::Error::other("fixture feature index is invalid"))?;
        self.registry
            .metadata(key)
            .ok_or_else(|| std::io::Error::other("fixture feature metadata is absent").into())
    }

    fn artifact_path(&self) -> PathBuf {
        self.temporary.path().join("artifact.json")
    }

    fn training_run_path(&self) -> PathBuf {
        self.temporary.path().join("training-run.json")
    }

    fn expectations_with_hashes(
        &self,
        bundle_metadata_hash: Sha256Digest,
        artifact_hash: Sha256Digest,
    ) -> TestResult<BundleExpectations> {
        Ok(BundleExpectations::try_new_with_output_binding(
            self.expectations.model_id(),
            self.expectations.bundle_id().clone(),
            self.expectations.bundle_version(),
            self.expectations.dataset().clone(),
            self.expectations.universe_id().clone(),
            self.expectations.training_period(),
            self.expectations.label().clone(),
            self.expectations.training_code_revision(),
            self.expectations.training_environment_hash(),
            bundle_metadata_hash,
            artifact_hash,
            self.expectations.training_run_hash(),
            self.expectations.output_binding().clone(),
        )?)
    }
}

#[test]
fn bundle_metadata_reference_rejects_non_local_paths() {
    let digest = Sha256Digest::new([7; 32]);

    assert_eq!(
        BundleMetadataRef::try_new("https://models.invalid/bundle.json", digest),
        Err(BundleError::InvalidControlledPath)
    );
    assert_eq!(
        BundleMetadataRef::try_new("../bundle.json", digest),
        Err(BundleError::InvalidControlledPath)
    );
}

#[test]
fn exact_bundle_load_and_registry_retain_immutable_generations() -> TestResult {
    let first_fixture = valid_fixture("native_linear", 1, 1, |_, _| {})?;
    let first = first_fixture.load()?;
    let first_retained_bytes = first.retained_bytes();
    assert_eq!(first.metadata().bundle_id().as_str(), "alpha-signal");
    assert_eq!(first.metadata().bundle_version().get(), 1);
    assert_eq!(first.metadata().features().len(), 2);
    assert_eq!(
        first.metadata().dataset().manifest().dataset_id().as_str(),
        "feature-label-training"
    );

    let byte_probe = ModelRegistry::try_new(nonzero_usize(1)?, nonzero_usize(4 * 1024 * 1024)?)?;
    let byte_limit = byte_probe
        .retained_bytes()?
        .checked_add(first_retained_bytes)
        .and_then(|bytes| bytes.checked_sub(1))
        .ok_or_else(|| std::io::Error::other("test registry byte limit overflowed"))?;
    let byte_bounded = ModelRegistry::try_new(nonzero_usize(1)?, nonzero_usize(byte_limit)?)?;
    assert_eq!(
        byte_bounded.try_register(first_fixture.load()?),
        Err(ModelRegistryError::RetainedByteLimitExceeded)
    );
    assert_eq!(byte_bounded.len()?, 0);

    let registry = ModelRegistry::try_new(nonzero_usize(2)?, nonzero_usize(4 * 1024 * 1024)?)?;
    assert_eq!(registry.try_register(first)?, BundleRegistration::Inserted);
    assert_eq!(
        registry.try_register(first_fixture.load()?)?,
        BundleRegistration::AlreadyRegistered
    );
    let conflict_fixture = valid_fixture("native_linear", 1, 1, |metadata, _| {
        metadata["intended_use"] = json!("a different but individually valid intended use");
    })?;
    assert_eq!(
        registry.try_register(conflict_fixture.load()?),
        Err(ModelRegistryError::GenerationConflict)
    );
    assert_eq!(registry.len()?, 1);

    let second_fixture = valid_fixture("native_linear", 1, 2, |_, _| {})?;
    assert_eq!(
        registry.try_register(second_fixture.load()?)?,
        BundleRegistration::Inserted
    );
    let first_retained = registry
        .get(&BundleId::try_new("alpha-signal")?, NonZeroU64::MIN)?
        .ok_or_else(|| std::io::Error::other("first model generation was not retained"))?;
    let latest = registry
        .latest(first_retained.metadata().model_id())?
        .ok_or_else(|| std::io::Error::other("latest model generation was not retained"))?;
    assert_eq!(first_retained.metadata().bundle_version().get(), 1);
    assert_eq!(latest.metadata().bundle_version().get(), 2);
    assert_eq!(registry.len()?, 2);
    let retained_before_series_conflicts = registry.retained_bytes()?;
    let wrong_model_same_series = valid_fixture_with_identity(
        "native_linear",
        1,
        3,
        "alpha-signal",
        "018f3c2a-91ab-7ccd-b3de-123456789abd",
        |_, _| {},
        None,
    )?;
    assert_eq!(
        registry.try_register(wrong_model_same_series.load()?),
        Err(ModelRegistryError::BundleSeriesConflict)
    );
    assert_eq!(registry.len()?, 2);
    assert_eq!(registry.retained_bytes()?, retained_before_series_conflicts);
    let competing_series_same_model = valid_fixture_with_identity(
        "native_linear",
        1,
        99,
        "competing-alpha-signal",
        "018f3c2a-91ab-7ccd-b3de-123456789abc",
        |_, _| {},
        None,
    )?;
    assert_eq!(
        registry.try_register(competing_series_same_model.load()?),
        Err(ModelRegistryError::ModelSeriesConflict)
    );
    assert_eq!(registry.len()?, 2);
    assert_eq!(registry.retained_bytes()?, retained_before_series_conflicts);
    let third_fixture = valid_fixture("native_linear", 1, 3, |_, _| {})?;
    assert_eq!(
        registry.try_register(third_fixture.load()?),
        Err(ModelRegistryError::RegistryFull)
    );
    assert_eq!(registry.len()?, 2);
    assert!(registry.retained_bytes()? <= registry.retained_byte_limit().get());
    Ok(())
}

#[test]
fn bundle_admission_fails_closed_across_complete_relationships() -> TestResult {
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["unexpected"] = json!(true);
        })?,
        BundleError::MetadataSyntax,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["schema_version"] = json!(3);
        })?,
        BundleError::UnsupportedMetadataVersion,
    )?;
    assert_fixture_error(
        valid_fixture("pickle", 1, 1, |_, _| {})?,
        BundleError::UnsupportedFormat,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 2, 1, |_, _| {})?,
        BundleError::UnsupportedFormatVersion,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            if let Some(features) = metadata["features"].as_array_mut() {
                features.swap(0, 1);
            }
        })?,
        BundleError::FeatureOrderMismatch,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["features"][0]["version"] = json!(99);
        })?,
        BundleError::FeatureIdentityMismatch,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["features"][0]["input_schema_sha256"] = json!(hex([81; 32]));
        })?,
        BundleError::FeatureSchemaMismatch,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["features"][0]["normalizer"] =
                json!({"kind": "standard", "mean": 0.0, "scale": 0.0});
        })?,
        BundleError::InvalidNormalizer,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["training_dataset"]["manifest_version"] = json!(8);
        })?,
        BundleError::DatasetMismatch,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["training_dataset"]["universe_sha256"] = json!(hex([82; 32]));
        })?,
        BundleError::DatasetMismatch,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["training_universe_id"] = json!("other-universe");
        })?,
        BundleError::UniverseMismatch,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["training_period"]["end_unix_nanos"] = json!(99);
        })?,
        BundleError::TrainingPeriodMismatch,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["label"]["version"] = json!(2);
        })?,
        BundleError::LabelMismatch,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["label"]["scope"] = json!("global");
        })?,
        BundleError::LabelMismatch,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["training_code_revision"] = json!("changed-revision");
        })?,
        BundleError::TrainingCodeRevisionMismatch,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["validation_metrics"] = json!([]);
        })?,
        BundleError::InvalidValidationMetrics,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["decision_thresholds"]["negative_max"] = json!(0.75);
            metadata["decision_thresholds"]["positive_min"] = json!(0.25);
        })?,
        BundleError::InvalidDecisionThresholds,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["intended_use"] = json!("");
        })?,
        BundleError::InvalidIntendedUse,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            if let Some(object) = metadata.as_object_mut() {
                object.remove("intended_use");
            }
        })?,
        BundleError::MetadataSyntax,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["limitations"] = json!([]);
        })?,
        BundleError::InvalidLimitations,
    )?;
    assert_fixture_error(
        valid_fixture("native_linear", 1, 1, |metadata, _| {
            metadata["fallback"]["reason"] = json!("");
        })?,
        BundleError::InvalidFallback,
    )?;
    Ok(())
}

#[test]
fn bundle_hashes_and_resource_bounds_are_checked_before_use() -> TestResult {
    let fixture = valid_fixture("native_linear", 1, 1, |_, _| {})?;
    let wrong_reference = BundleMetadataRef::try_new("bundle.json", Sha256Digest::new([9; 32]))?;
    let error = ModelBundle::load(
        &fixture.root,
        &wrong_reference,
        &fixture.expectations,
        &fixture.registry,
    )
    .err()
    .ok_or_else(|| std::io::Error::other("wrong metadata hash was accepted"))?;
    assert_eq!(error, BundleError::MetadataHashMismatch);

    let wrong_metadata_expectations = fixture.expectations_with_hashes(
        Sha256Digest::new([9; 32]),
        fixture.expectations.artifact_hash(),
    )?;
    let error = ModelBundle::load(
        &fixture.root,
        &fixture.reference,
        &wrong_metadata_expectations,
        &fixture.registry,
    )
    .err()
    .ok_or_else(|| std::io::Error::other("unapproved metadata hash was accepted"))?;
    assert_eq!(error, BundleError::MetadataHashMismatch);

    let wrong_artifact_expectations = fixture.expectations_with_hashes(
        fixture.expectations.bundle_metadata_hash(),
        Sha256Digest::new([9; 32]),
    )?;
    let error = ModelBundle::load(
        &fixture.root,
        &fixture.reference,
        &wrong_artifact_expectations,
        &fixture.registry,
    )
    .err()
    .ok_or_else(|| std::io::Error::other("unapproved artifact hash was accepted"))?;
    assert_eq!(error, BundleError::ArtifactHashMismatch);

    let fixture = valid_fixture("native_linear", 1, 1, |_, _| {})?;
    let mut bytes = fs::read(fixture.artifact_path())?;
    let byte = bytes
        .iter_mut()
        .find(|byte| **byte == b'2')
        .ok_or_else(|| std::io::Error::other("artifact had no mutable numeric byte"))?;
    *byte = b'3';
    fs::write(fixture.artifact_path(), bytes)?;
    assert_fixture_error(fixture, BundleError::ArtifactHashMismatch)?;

    let fixture = valid_fixture("native_linear", 1, 1, |_, _| {})?;
    fs::write(fixture.artifact_path(), vec![b' '; MAX_ARTIFACT_BYTES + 1])?;
    assert_fixture_error(fixture, BundleError::ArtifactTooLarge)?;

    let fixture = valid_fixture("native_linear", 1, 1, |_, _| {})?;
    let mut bytes = fs::read(fixture.training_run_path())?;
    let byte = bytes
        .iter_mut()
        .find(|byte| **byte == b'7')
        .ok_or_else(|| std::io::Error::other("training run had no mutable numeric byte"))?;
    *byte = b'8';
    fs::write(fixture.training_run_path(), bytes)?;
    assert_fixture_error(fixture, BundleError::TrainingRunHashMismatch)?;
    Ok(())
}

fn assert_fixture_error(fixture: Fixture, expected: BundleError) -> TestResult {
    let observed = fixture
        .load()
        .err()
        .ok_or_else(|| std::io::Error::other("invalid model bundle was accepted"))?;
    assert_eq!(observed, expected);
    Ok(())
}

pub(crate) fn valid_fixture(
    format: &str,
    format_version: u32,
    bundle_version: u64,
    mutate: impl FnOnce(&mut Value, &mut Value),
) -> TestResult<Fixture> {
    valid_fixture_with_identity(
        format,
        format_version,
        bundle_version,
        "alpha-signal",
        "018f3c2a-91ab-7ccd-b3de-123456789abc",
        mutate,
        None,
    )
}

#[cfg(feature = "onnx-tract")]
pub(crate) fn valid_onnx_fixture(artifact_bytes: &[u8]) -> TestResult<Fixture> {
    valid_fixture_with_identity(
        "onnx",
        1,
        1,
        "alpha-signal-onnx",
        "018f3c2a-91ab-7ccd-b3de-123456789abe",
        |_, _| {},
        Some(artifact_bytes),
    )
}

fn valid_fixture_with_identity(
    format: &str,
    format_version: u32,
    bundle_version: u64,
    bundle_id: &str,
    model_id: &str,
    mutate: impl FnOnce(&mut Value, &mut Value),
    artifact_override: Option<&[u8]>,
) -> TestResult<Fixture> {
    let catalog = LiveFeatureCatalog::try_new(live_catalog_config()?, "task12-live-revision")?;
    let feature_keys = [
        catalog.entries()[0].key().clone(),
        catalog.entries()[1].key().clone(),
    ];
    let mut registry =
        FeatureRegistry::try_new(nonzero_usize(32)?, nonzero_usize(2 * 1024 * 1024)?)?;
    catalog.try_register(&mut registry)?;

    let schema = DatasetSchemaRegistry::local().canonical_feature_labels()?;
    let manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("feature-label-training")?,
        7,
        schema,
        Sha256Digest::new([31; 32]),
    )?;
    let dataset = TrainingDatasetIdentity::try_new(
        manifest,
        DatasetBuildSpecDigest::try_new([32; 32])?,
        Sha256Digest::new([33; 32]),
        Sha256Digest::new([34; 32]),
        CatalogEndpointIdentity::try_from_bytes([38; 32])
            .ok_or_else(|| std::io::Error::other("catalog identity must be nonzero"))?,
        Sha256Digest::new([35; 32]),
        Sha256Digest::new([39; 32]),
        Timestamp::from_unix_nanos(600),
        NonZeroU64::new(30)
            .ok_or_else(|| std::io::Error::other("selected rows must be nonzero"))?,
    )?;
    let universe = UniverseId::try_from("liquid-us-equities")?;
    let period = TrainingPeriod::try_new(
        Timestamp::from_unix_nanos(10),
        Timestamp::from_unix_nanos(20),
    )?;
    let label = FeatureLabelComponentSpec::try_new(
        ComponentKind::Label,
        ComponentScope::Instrument,
        CorporateActionSensitivity::RequiresAdjustment,
        "forward-return",
        NonZeroU32::MIN,
    )?;
    let model_id = ModelId::from_str(model_id)?;
    let output_semantics = if format == "native_logistic" {
        ModelOutputSemantics::BinaryProbability
    } else {
        ModelOutputSemantics::Regression
    };
    let output_measurement = if format == "native_logistic" {
        ForecastMeasurement::Probability
    } else {
        ForecastMeasurement::Return
    };
    let output_binding = ForecastOutputBinding::try_from_admitted_model(
        output_semantics,
        output_measurement,
        ForecastCentralStatistic::Unavailable,
        ForecastTargetMeaning::Unsupported,
        ForecastTransform::Identity,
        if format == "native_logistic" {
            ForecastTransform::Logistic
        } else {
            ForecastTransform::Identity
        },
        if format == "native_logistic" {
            ForecastTrainingObjective::BinaryCrossEntropy
        } else {
            ForecastTrainingObjective::SquaredError
        },
        if format == "native_logistic" {
            ForecastEstimatorProfile::SealedBinaryLogisticV1
        } else {
            ForecastEstimatorProfile::SealedDirectLeastSquaresV1
        },
        label.clone(),
    )?;
    let expectations = BundleExpectations::try_new_with_output_binding(
        model_id,
        BundleId::try_new(bundle_id)?,
        NonZeroU64::new(bundle_version)
            .ok_or_else(|| std::io::Error::other("bundle version must be nonzero"))?,
        dataset,
        universe,
        period,
        label,
        "train-code-abc123",
        Sha256Digest::new([37; 32]),
        Sha256Digest::new([3; 32]),
        Sha256Digest::new([4; 32]),
        Sha256Digest::new([2; 32]),
        output_binding,
    )?;

    let feature_json = feature_keys
        .iter()
        .enumerate()
        .map(|(index, key)| {
            let metadata = registry
                .metadata(key)
                .ok_or_else(|| std::io::Error::other("registered feature is absent"))?;
            let normalizer = if index == 0 {
                json!({"kind": "identity"})
            } else {
                json!({"kind": "standard", "mean": 10.0, "scale": 2.0})
            };
            Ok::<_, std::io::Error>(json!({
                "name": key.name(),
                "version": key.version().get(),
                "input_schema_sha256": hex(metadata.input_schema_digest().as_bytes()),
                "semantic_sha256": hex(metadata.semantic_digest().as_bytes()),
                "normalizer": normalizer
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let semantic_digests = feature_keys
        .iter()
        .map(|key| {
            registry
                .metadata(key)
                .map(|metadata| hex(metadata.semantic_digest().as_bytes()))
                .ok_or_else(|| std::io::Error::other("registered feature is absent"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let weights = if format == "native_logistic" {
        json!([1.0, -1.0])
    } else {
        json!([2.0, -1.0])
    };
    let mut artifact = json!({
        "schema_version": 1,
        "format": format,
        "format_version": format_version,
        "feature_semantic_sha256": semantic_digests,
        "weights": weights,
        "bias": 0.5,
        "output_count": 1
    });
    let metrics = if format == "native_logistic" {
        json!([{"name": "accuracy", "value": 0.81}])
    } else {
        json!([{"name": "mean_squared_error", "value": 0.12}])
    };
    let thresholds = if format == "native_logistic" {
        json!({"negative_max": 0.4, "positive_min": 0.6, "minimum_confidence": 0.0})
    } else {
        json!({"negative_max": -0.5, "positive_min": 0.5, "minimum_confidence": 0.0})
    };
    let mut metadata = json!({
        "schema_version": 9,
        "bundle_id": bundle_id,
        "bundle_version": bundle_version,
        "model_id": model_id.to_string(),
        "artifact": {
            "path": if artifact_override.is_some() { "model.onnx" } else { "artifact.json" },
            "sha256": hex([1; 32]),
            "size_bytes": 1,
            "format": format,
            "format_version": format_version
        },
        "training_run": {
            "path": "training-run.json",
            "sha256": hex([2; 32]),
            "size_bytes": 1
        },
        "features": feature_json,
        "training_dataset": {
            "dataset_id": expectations.dataset().manifest().dataset_id().as_str(),
            "manifest_version": expectations.dataset().manifest().manifest_version(),
            "schema_name": expectations.dataset().manifest().schema().name(),
            "schema_version": expectations.dataset().manifest().schema().version().get(),
            "schema_sha256": hex(expectations.dataset().manifest().schema().fingerprint()),
            "manifest_sha256": hex(expectations.dataset().manifest().content_hash().bytes()),
            "build_spec_sha256": hex(expectations.dataset().build_spec_digest().digest().bytes()),
            "universe_sha256": hex(expectations.dataset().universe_digest().bytes()),
            "policy_sha256": hex(expectations.dataset().policy_digest().bytes()),
            "catalog_identity_sha256": hex(expectations.dataset().catalog_identity().bytes()),
            "export_sha256": hex(expectations.dataset().export_digest().bytes()),
            "selection_sha256": hex(expectations.dataset().selection_digest().bytes()),
            "selection_as_of_unix_nanos": expectations.dataset().selection_as_of().unix_nanos(),
            "selected_component_rows": expectations.dataset().selected_component_rows().get()
        },
        "training_universe_id": expectations.universe_id().as_str(),
        "training_period": {
            "start_unix_nanos": expectations.training_period().start().unix_nanos(),
            "end_unix_nanos": expectations.training_period().end().unix_nanos()
        },
        "label": {
            "kind": "label",
            "scope": "instrument",
            "corporate_action_sensitivity": "requires_adjustment",
            "name": "forward-return",
            "version": 1
        },
        "training_code_revision": "train-code-abc123",
        "training_environment_sha256": hex(expectations.training_environment_hash().bytes()),
        "validation_metrics": metrics,
        "decision_thresholds": thresholds,
        "intended_use": "bounded directional ranking for verified market features",
        "limitations": ["not calibrated for unverified or stale features"],
        "fallback": {"policy": "no_action", "reason": "model contract unavailable"},
        "output_measurement": if format == "native_logistic" {
            json!({"kind": "probability"})
        } else {
            json!({"kind": "return"})
        },
        "output_statistic": if format == "native_logistic" {
            json!({
                "estimator": {"kind": "sealed_binary_logistic_v1"},
                "objective": "binary_cross_entropy",
                "output_transform": "logistic",
                "statistic": "unavailable",
                "target": {"kind": "unsupported"},
                "target_transform": "identity"
            })
        } else {
            json!({
                "estimator": {"kind": "sealed_direct_least_squares_v1"},
                "objective": "squared_error",
                "output_transform": "identity",
                "statistic": "unavailable",
                "target": {"kind": "unsupported"},
                "target_transform": "identity"
            })
        },
        "output_semantics": if format == "native_logistic" {
            "binary_probability"
        } else {
            "regression"
        }
    });
    mutate(&mut metadata, &mut artifact);

    let temporary = TempDir::new()?;
    let artifact_bytes = match artifact_override {
        Some(bytes) => bytes.to_vec(),
        None => serde_json::to_vec(&artifact)?,
    };
    let run_features = metadata["features"]
        .as_array()
        .ok_or_else(|| std::io::Error::other("fixture features are not an array"))?
        .iter()
        .map(|feature| {
            json!({
                "input_schema_sha256": feature["input_schema_sha256"],
                "name": feature["name"],
                "semantic_sha256": feature["semantic_sha256"],
                "version": feature["version"]
            })
        })
        .collect::<Vec<_>>();
    let trial = json!({
        "bundle_id": metadata["bundle_id"],
        "bundle_version": metadata["bundle_version"],
        "dataset": metadata["training_dataset"],
        "dataset_export_sha256": hex(expectations.dataset().export_digest().bytes()),
        "environment_sha256": hex([37; 32]),
        "features": run_features,
        "label": metadata["label"],
        "missing_policy": "reject",
        "model_id": metadata["model_id"],
        "model_kind": if format == "onnx" {
            "linear"
        } else {
            format
        },
        "output_measurement": metadata["output_measurement"],
        "output_statistic": metadata["output_statistic"],
        "output_semantics": metadata["output_semantics"],
        "seed": 17,
        "split_counts": {"test": 1, "train": 7, "validation": 2},
        "split_sha256": hex([36; 32]),
        "training_code_revision": metadata["training_code_revision"],
        "training_period": metadata["training_period"],
        "universe_id": metadata["training_universe_id"]
    });
    let trial_sha256 = hex(sha256(&serde_json::to_vec(&trial)?));
    let training_run_bytes = serde_json::to_vec(&json!({
        "schema_version": 7,
        "trial": trial,
        "trial_sha256": trial_sha256,
        "validation_metrics": metadata["validation_metrics"]
    }))?;
    let artifact_sha256 = sha256(&artifact_bytes);
    metadata["artifact"]["sha256"] = json!(hex(artifact_sha256));
    metadata["artifact"]["size_bytes"] = json!(artifact_bytes.len());
    let training_run_sha256 = sha256(&training_run_bytes);
    metadata["training_run"]["sha256"] = json!(hex(training_run_sha256));
    metadata["training_run"]["size_bytes"] = json!(training_run_bytes.len());
    let metadata_bytes = serde_json::to_vec(&metadata)?;
    let bundle_metadata_sha256 = sha256(&metadata_bytes);
    let expectations = BundleExpectations::try_new_with_output_binding(
        expectations.model_id(),
        expectations.bundle_id().clone(),
        expectations.bundle_version(),
        expectations.dataset().clone(),
        expectations.universe_id().clone(),
        expectations.training_period(),
        expectations.label().clone(),
        expectations.training_code_revision(),
        expectations.training_environment_hash(),
        Sha256Digest::new(bundle_metadata_sha256),
        Sha256Digest::new(artifact_sha256),
        Sha256Digest::new(training_run_sha256),
        expectations.output_binding().clone(),
    )?;
    let artifact_name = if artifact_override.is_some() {
        "model.onnx"
    } else {
        "artifact.json"
    };
    fs::write(temporary.path().join(artifact_name), artifact_bytes)?;
    fs::write(
        temporary.path().join("training-run.json"),
        training_run_bytes,
    )?;
    fs::write(temporary.path().join("bundle.json"), &metadata_bytes)?;
    let reference =
        BundleMetadataRef::try_new("bundle.json", Sha256Digest::new(sha256(&metadata_bytes)))?;
    let root = ControlledModelRoot::open_ambient(temporary.path())?;
    Ok(Fixture {
        temporary,
        root,
        reference,
        expectations,
        registry,
        feature_keys,
    })
}

fn live_catalog_config() -> TestResult<LiveFeatureCatalogConfig> {
    Ok(LiveFeatureCatalogConfig::try_new(
        nonzero_u32(50)?,
        nonzero_u32(1_024)?,
        nonzero_u32(4_096)?,
        nonzero_u32(3)?,
        NonZeroU64::new(60_000_000_000)
            .ok_or_else(|| std::io::Error::other("duration must be nonzero"))?,
        nonzero_u32(8)?,
        NonZeroU64::new(250_000_000)
            .ok_or_else(|| std::io::Error::other("skew must be nonzero"))?,
    )?)
}

fn nonzero_u32(value: u32) -> TestResult<NonZeroU32> {
    NonZeroU32::new(value).ok_or_else(|| std::io::Error::other("test value must be nonzero").into())
}

fn nonzero_usize(value: usize) -> TestResult<NonZeroUsize> {
    NonZeroUsize::new(value)
        .ok_or_else(|| std::io::Error::other("test value must be nonzero").into())
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn hex(bytes: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}
