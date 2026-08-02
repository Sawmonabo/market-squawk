//! Closed bundle grammar and exact Task 11/12 relationship validation.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::num::{NonZeroU32, NonZeroU64};

use market_squawk_analytics::{FeatureKey, FeatureRegistry};
use market_squawk_data::{ComponentKind, ComponentScope, CorporateActionSensitivity, Sha256Digest};
use market_squawk_domain::Timestamp;
use serde::{Deserialize, Serialize};

use super::BundleError;
use crate::native::NativeArtifact;
use crate::{
    BundleExpectations, CalibrationBand, CalibrationMethod, CalibrationWindow, DecisionThresholds,
    FeatureNormalizer, ForecastCalibrationArtifacts, ForecastCoverage, MAX_MODEL_FEATURES,
    ModelFeatureBinding, ModelFormat, ModelOutputSemantics, RealizedCoverage, ValidationMetric,
    ValidationMetricName,
};

pub(super) const LEGACY_METADATA_SCHEMA_VERSION: u32 = 4;
pub(super) const METADATA_SCHEMA_VERSION: u32 = 5;
pub(super) const FORECAST_METADATA_SCHEMA_VERSION: u32 = 6;
pub(super) const NATIVE_FORMAT_VERSION: u32 = 1;
pub(super) const NATIVE_ARTIFACT_SCHEMA_VERSION: u32 = 1;
pub(super) const TRAINING_RUN_SCHEMA_VERSION: u32 = 2;
pub(super) const OUTPUT_BOUND_TRAINING_RUN_SCHEMA_VERSION: u32 = 3;
pub(super) const FORECAST_TRAINING_RUN_SCHEMA_VERSION: u32 = 4;
pub(super) const FORECAST_POLICY_SCHEMA_VERSION: u32 = 1;
pub(super) const FORECAST_RESIDUALS_PATH: &str = "calibration/residuals.f64le";
pub(super) const FORECAST_POLICY_PATH: &str = "calibration/policy.json";
const MAX_VALIDATION_METRICS: usize = 32;
const MAX_LIMITATIONS: usize = 32;
const MAX_PROSE_BYTES: usize = 512;
const MAX_TRAINING_RUN_EXAMPLES: usize = 100_000;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MetadataWire {
    pub(super) schema_version: u32,
    pub(super) bundle_id: String,
    pub(super) bundle_version: u64,
    pub(super) model_id: String,
    pub(super) artifact: ArtifactRefWire,
    pub(super) output_semantics: Option<String>,
    pub(super) training_run: FileRefWire,
    pub(super) forecast_calibration: Option<ForecastCalibrationRefWire>,
    pub(super) features: Vec<FeatureWire>,
    pub(super) training_dataset: DatasetWire,
    pub(super) training_universe_id: String,
    pub(super) training_period: TrainingPeriodWire,
    pub(super) label: LabelWire,
    pub(super) training_code_revision: String,
    pub(super) training_environment_sha256: String,
    pub(super) validation_metrics: Vec<MetricWire>,
    pub(super) decision_thresholds: ThresholdWire,
    pub(super) intended_use: String,
    pub(super) limitations: Vec<String>,
    pub(super) fallback: FallbackWire,
}

#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct FileRefWire {
    pub(super) path: String,
    pub(super) sha256: String,
    pub(super) size_bytes: u64,
}

#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct ForecastCalibrationRefWire {
    pub(super) residuals: FileRefWire,
    pub(super) policy: FileRefWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ArtifactRefWire {
    pub(super) path: String,
    pub(super) sha256: String,
    pub(super) size_bytes: u64,
    pub(super) format: String,
    pub(super) format_version: u32,
}

#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct FeatureWire {
    name: String,
    version: u32,
    input_schema_sha256: String,
    semantic_sha256: String,
    normalizer: NormalizerWire,
}

#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct NormalizerWire {
    kind: String,
    mean: Option<f64>,
    scale: Option<f64>,
}

#[derive(Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DatasetWire {
    build_spec_sha256: String,
    catalog_identity_sha256: String,
    dataset_id: String,
    export_sha256: String,
    manifest_sha256: String,
    manifest_version: u64,
    policy_sha256: String,
    schema_name: String,
    schema_sha256: String,
    schema_version: u16,
    selected_component_rows: u64,
    selection_as_of_unix_nanos: i64,
    selection_sha256: String,
    universe_sha256: String,
}

#[derive(Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TrainingPeriodWire {
    pub(super) end_unix_nanos: i64,
    pub(super) start_unix_nanos: i64,
}

#[derive(Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LabelWire {
    corporate_action_sensitivity: String,
    kind: String,
    name: String,
    scope: String,
    version: u32,
}

#[derive(Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MetricWire {
    name: String,
    value: f64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TrainingRunWire {
    pub(super) schema_version: u32,
    pub(super) trial: TrainingTrialWire,
    pub(super) trial_sha256: String,
    pub(super) validation_metrics: Vec<MetricWire>,
    pub(super) forecast_calibration: Option<ForecastCalibrationRefWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ForecastPolicyWire {
    schema_version: u32,
    kind: String,
    method: String,
    calibration_window: ForecastCalibrationWindowWire,
    dependence_assumptions: String,
    residuals_sha256: String,
    bands: Vec<ForecastBandWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ForecastCalibrationWindowWire {
    start_unix_nanos: i64,
    end_unix_nanos: i64,
    observations: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ForecastBandWire {
    target_coverage_basis_points: u16,
    lower_offset: f64,
    upper_offset: f64,
    realized_covered: u64,
    realized_total: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TrainingTrialWire {
    bundle_id: String,
    bundle_version: u64,
    dataset: DatasetWire,
    dataset_export_sha256: String,
    environment_sha256: String,
    features: Vec<TrainingFeatureWire>,
    label: LabelWire,
    missing_policy: String,
    model_id: String,
    model_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_semantics: Option<String>,
    seed: u64,
    split_counts: SplitCountsWire,
    split_sha256: String,
    training_code_revision: String,
    training_period: TrainingPeriodWire,
    universe_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    forecast: Option<ForecastTrialWire>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ForecastTrialWire {
    strategy: String,
    horizons: Vec<u32>,
    lags: Vec<u32>,
    observed_cutoff_unix_nanos: i64,
    rolling_splits: u32,
    ridge_alpha: f64,
    selection_sha256: String,
    package_versions: BTreeMap<String, String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TrainingFeatureWire {
    input_schema_sha256: String,
    name: String,
    semantic_sha256: String,
    version: u32,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SplitCountsWire {
    test: usize,
    train: usize,
    validation: usize,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ThresholdWire {
    negative_max: f64,
    positive_min: f64,
    minimum_confidence: f64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FallbackWire {
    pub(super) policy: String,
    pub(super) reason: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeArtifactWire {
    schema_version: u32,
    format: String,
    format_version: u32,
    feature_semantic_sha256: Vec<String>,
    weights: Vec<f64>,
    bias: f64,
    output_count: usize,
}

pub(super) fn validate_features(
    values: &[FeatureWire],
    registry: &FeatureRegistry,
) -> Result<Vec<ModelFeatureBinding>, BundleError> {
    if values.is_empty() || values.len() > MAX_MODEL_FEATURES {
        return Err(BundleError::InvalidFeatureCount);
    }
    let mut features = Vec::new();
    features
        .try_reserve_exact(values.len())
        .map_err(|_| BundleError::RetainedSizeOverflow)?;
    for value in values {
        let version = NonZeroU32::new(value.version).ok_or(BundleError::FeatureIdentityMismatch)?;
        let key = FeatureKey::try_new(&value.name, version)
            .map_err(|_| BundleError::FeatureIdentityMismatch)?;
        let metadata = registry
            .metadata(&key)
            .ok_or(BundleError::FeatureIdentityMismatch)?;
        if parse_digest_bytes(&value.input_schema_sha256)?
            != metadata.input_schema_digest().as_bytes()
        {
            return Err(BundleError::FeatureSchemaMismatch);
        }
        if parse_digest_bytes(&value.semantic_sha256)? != metadata.semantic_digest().as_bytes() {
            return Err(BundleError::FeatureSemanticMismatch);
        }
        let normalizer = match (
            value.normalizer.kind.as_str(),
            value.normalizer.mean,
            value.normalizer.scale,
        ) {
            ("identity", None, None) => FeatureNormalizer::Identity,
            ("standard", Some(mean), Some(scale)) => FeatureNormalizer::standard(mean, scale)
                .map_err(|_| BundleError::InvalidNormalizer)?,
            _ => return Err(BundleError::InvalidNormalizer),
        };
        features.push(ModelFeatureBinding::new(
            key,
            metadata.input_schema_digest(),
            metadata.semantic_digest(),
            normalizer,
        ));
    }
    Ok(features)
}

pub(super) fn validate_dataset(
    wire: &DatasetWire,
    expectations: &BundleExpectations,
) -> Result<(), BundleError> {
    let manifest = expectations.dataset().manifest();
    let matches = wire.dataset_id == manifest.dataset_id().as_str()
        && wire.manifest_version == manifest.manifest_version()
        && wire.schema_name == manifest.schema().name()
        && wire.schema_version == manifest.schema().version().get()
        && parse_digest_bytes(&wire.schema_sha256)? == manifest.schema().fingerprint()
        && parse_digest_bytes(&wire.manifest_sha256)? == manifest.content_hash().bytes()
        && parse_digest_bytes(&wire.build_spec_sha256)?
            == expectations.dataset().build_spec_digest().digest().bytes()
        && parse_digest_bytes(&wire.universe_sha256)?
            == expectations.dataset().universe_digest().bytes()
        && parse_digest_bytes(&wire.policy_sha256)?
            == expectations.dataset().policy_digest().bytes()
        && parse_digest_bytes(&wire.catalog_identity_sha256)?
            == expectations.dataset().catalog_identity().bytes()
        && parse_digest_bytes(&wire.export_sha256)?
            == expectations.dataset().export_digest().bytes()
        && parse_digest_bytes(&wire.selection_sha256)?
            == expectations.dataset().selection_digest().bytes()
        && wire.selection_as_of_unix_nanos == expectations.dataset().selection_as_of().unix_nanos()
        && wire.selected_component_rows == expectations.dataset().selected_component_rows().get();
    if matches {
        Ok(())
    } else {
        Err(BundleError::DatasetMismatch)
    }
}

pub(super) fn validate_label(
    wire: &LabelWire,
    expectations: &BundleExpectations,
) -> Result<(), BundleError> {
    let expected = expectations.label();
    let kind = match expected.kind() {
        ComponentKind::Feature => "feature",
        ComponentKind::Label => "label",
    };
    let scope = match expected.scope() {
        ComponentScope::Instrument => "instrument",
        ComponentScope::Account => "account",
        ComponentScope::Global => "global",
    };
    let corporate_actions = match expected.corporate_actions() {
        CorporateActionSensitivity::NotApplicable => "not_applicable",
        CorporateActionSensitivity::RequiresAdjustment => "requires_adjustment",
    };
    if wire.kind == kind
        && wire.scope == scope
        && wire.corporate_action_sensitivity == corporate_actions
        && wire.name == expected.name()
        && NonZeroU32::new(wire.version) == Some(expected.version())
    {
        Ok(())
    } else {
        Err(BundleError::LabelMismatch)
    }
}

pub(super) fn validate_training_run(
    run: &TrainingRunWire,
    metadata: &MetadataWire,
    expectations: &BundleExpectations,
    format: ModelFormat,
    output_semantics: ModelOutputSemantics,
    output_semantics_bound: bool,
) -> Result<(), BundleError> {
    if !matches!(
        run.schema_version,
        TRAINING_RUN_SCHEMA_VERSION
            | OUTPUT_BOUND_TRAINING_RUN_SCHEMA_VERSION
            | FORECAST_TRAINING_RUN_SCHEMA_VERSION
    ) {
        return Err(BundleError::UnsupportedTrainingRunVersion);
    }
    let trial_bytes = serde_json::to_vec(&run.trial).map_err(|_| BundleError::TrainingRunSyntax)?;
    if super::io::sha256_digest(&trial_bytes) != parse_digest(&run.trial_sha256)? {
        return Err(BundleError::TrainingRunTrialHashMismatch);
    }

    let trial = &run.trial;
    validate_dataset(&trial.dataset, expectations)?;
    validate_label(&trial.label, expectations)?;
    let (expected_kind, expected_output_semantics) = match run.schema_version {
        TRAINING_RUN_SCHEMA_VERSION => (
            match format {
                ModelFormat::NativeLinear => "native_linear",
                ModelFormat::NativeLogistic => "native_logistic",
                ModelFormat::Onnx => "onnx",
            },
            None,
        ),
        OUTPUT_BOUND_TRAINING_RUN_SCHEMA_VERSION => (
            match output_semantics {
                ModelOutputSemantics::Regression => "linear",
                ModelOutputSemantics::BinaryProbability => "logistic",
            },
            Some(output_semantics_name(output_semantics)),
        ),
        FORECAST_TRAINING_RUN_SCHEMA_VERSION => (
            "linear",
            Some(output_semantics_name(ModelOutputSemantics::Regression)),
        ),
        _ => return Err(BundleError::UnsupportedTrainingRunVersion),
    };
    let relationships_match =
        trial.bundle_id == metadata.bundle_id
            && trial.bundle_version == metadata.bundle_version
            && trial.dataset == metadata.training_dataset
            && trial.features.len() == metadata.features.len()
            && trial.features.iter().zip(&metadata.features).all(
                |(run_feature, metadata_feature)| {
                    run_feature.name == metadata_feature.name
                        && run_feature.version == metadata_feature.version
                        && run_feature.input_schema_sha256 == metadata_feature.input_schema_sha256
                        && run_feature.semantic_sha256 == metadata_feature.semantic_sha256
                },
            )
            && trial.label == metadata.label
            && trial.model_id == metadata.model_id
            && trial.model_kind == expected_kind
            && trial.output_semantics.as_deref() == expected_output_semantics
            && output_semantics_bound
                == matches!(
                    run.schema_version,
                    OUTPUT_BOUND_TRAINING_RUN_SCHEMA_VERSION | FORECAST_TRAINING_RUN_SCHEMA_VERSION
                )
            && match run.schema_version {
                FORECAST_TRAINING_RUN_SCHEMA_VERSION => {
                    metadata.schema_version == FORECAST_METADATA_SCHEMA_VERSION
                        && format == ModelFormat::Onnx
                        && output_semantics == ModelOutputSemantics::Regression
                        && run.forecast_calibration == metadata.forecast_calibration
                        && run.forecast_calibration.is_some()
                        && trial.forecast.is_some()
                }
                _ => {
                    metadata.schema_version != FORECAST_METADATA_SCHEMA_VERSION
                        && run.forecast_calibration.is_none()
                        && metadata.forecast_calibration.is_none()
                        && trial.forecast.is_none()
                }
            }
            && trial.training_code_revision == metadata.training_code_revision
            && trial.environment_sha256 == metadata.training_environment_sha256
            && trial.training_period == metadata.training_period
            && trial.universe_id == metadata.training_universe_id
            && run.validation_metrics == metadata.validation_metrics;
    if !relationships_match {
        return Err(BundleError::TrainingRunRelationshipMismatch);
    }
    for feature in &trial.features {
        let input = parse_digest(&feature.input_schema_sha256)?;
        let semantic = parse_digest(&feature.semantic_sha256)?;
        if input.bytes() == [0; 32] || semantic.bytes() == [0; 32] {
            return Err(BundleError::TrainingRunRelationshipMismatch);
        }
    }
    if parse_digest(&trial.dataset_export_sha256)? != expectations.dataset().export_digest() {
        return Err(BundleError::TrainingRunRelationshipMismatch);
    }
    if parse_digest(&trial.environment_sha256)? != expectations.training_environment_hash()
        || parse_digest(&trial.split_sha256)?.bytes() == [0; 32]
    {
        return Err(BundleError::TrainingRunRelationshipMismatch);
    }
    if !matches!(trial.missing_policy.as_str(), "reject" | "drop_row") {
        return Err(BundleError::TrainingRunRelationshipMismatch);
    }
    let counts = &trial.split_counts;
    let total = counts
        .train
        .checked_add(counts.validation)
        .and_then(|value| value.checked_add(counts.test))
        .ok_or(BundleError::TrainingRunRelationshipMismatch)?;
    if counts.train <= trial.features.len()
        || counts.validation == 0
        || total > MAX_TRAINING_RUN_EXAMPLES
    {
        return Err(BundleError::TrainingRunRelationshipMismatch);
    }
    if let Some(forecast) = &trial.forecast {
        let ordered_positive = |values: &[u32], maximum: usize| {
            !values.is_empty()
                && values.len() <= maximum
                && values.iter().all(|value| *value > 0)
                && values.windows(2).all(|pair| pair[0] < pair[1])
        };
        if !matches!(
            forecast.strategy.as_str(),
            "direct" | "recursive" | "multi_output" | "chained"
        ) || !ordered_positive(&forecast.horizons, 512)
            || !ordered_positive(&forecast.lags, 1_024)
            || forecast.observed_cutoff_unix_nanos
                > metadata.training_dataset.selection_as_of_unix_nanos
            || !(2..=32).contains(&forecast.rolling_splits)
            || !forecast.ridge_alpha.is_finite()
            || forecast.ridge_alpha < 0.0
            || parse_digest(&forecast.selection_sha256)?.bytes() == [0; 32]
            || forecast.package_versions.len() != 5
            || forecast.package_versions.iter().any(|(name, version)| {
                !matches!(
                    name.as_str(),
                    "numpy" | "scikit-learn" | "mapie" | "skl2onnx" | "onnx"
                ) || version.is_empty()
                    || version.len() > 64
                    || version.bytes().any(|byte| byte.is_ascii_control())
            })
        {
            return Err(BundleError::TrainingRunRelationshipMismatch);
        }
    }
    Ok(())
}

pub(super) fn validate_forecast_calibration(
    reference: &ForecastCalibrationRefWire,
    policy: ForecastPolicyWire,
    residuals: &[u8],
) -> Result<ForecastCalibrationArtifacts, BundleError> {
    if policy.schema_version != FORECAST_POLICY_SCHEMA_VERSION
        || reference.residuals.path != FORECAST_RESIDUALS_PATH
        || reference.policy.path != FORECAST_POLICY_PATH
        || residuals.is_empty()
        || !residuals.len().is_multiple_of(size_of::<f64>())
    {
        return Err(BundleError::InvalidForecastCalibration);
    }
    let method = match (policy.kind.as_str(), policy.method.as_str()) {
        ("mapie_time_series_conformal", "mapie_enbpi") => CalibrationMethod::MapieEnbpi,
        ("mapie_time_series_conformal", "mapie_aci") => CalibrationMethod::MapieAci,
        ("residual_quantile", "residual_quantile") => CalibrationMethod::ResidualQuantile,
        _ => return Err(BundleError::InvalidForecastCalibration),
    };
    let observations = NonZeroU32::new(policy.calibration_window.observations)
        .ok_or(BundleError::InvalidForecastCalibration)?;
    if usize::try_from(observations.get()).ok() != Some(residuals.len() / size_of::<f64>())
        || policy.dependence_assumptions.is_empty()
        || policy.dependence_assumptions.len() > 512
        || policy
            .dependence_assumptions
            .bytes()
            .any(|byte| byte.is_ascii_control())
        || parse_digest(&policy.residuals_sha256)? != parse_digest(&reference.residuals.sha256)?
    {
        return Err(BundleError::InvalidForecastCalibration);
    }
    for chunk in residuals.chunks_exact(size_of::<f64>()) {
        let bytes: [u8; 8] = chunk
            .try_into()
            .map_err(|_| BundleError::InvalidForecastCalibration)?;
        if !f64::from_le_bytes(bytes).is_finite() {
            return Err(BundleError::InvalidForecastCalibration);
        }
    }
    let window = CalibrationWindow::try_new(
        Timestamp::from_unix_nanos(policy.calibration_window.start_unix_nanos),
        Timestamp::from_unix_nanos(policy.calibration_window.end_unix_nanos),
        observations,
    )
    .map_err(|_| BundleError::InvalidForecastCalibration)?;
    let wires: [ForecastBandWire; 3] = policy
        .bands
        .try_into()
        .map_err(|_| BundleError::InvalidForecastCalibration)?;
    let coverages = [
        ForecastCoverage::Fifty,
        ForecastCoverage::Eighty,
        ForecastCoverage::NinetyFive,
    ];
    let mut decoded = Vec::with_capacity(3);
    for (index, wire) in wires.iter().enumerate() {
        if wire.target_coverage_basis_points != coverages[index].basis_points() {
            return Err(BundleError::InvalidForecastCalibration);
        }
        let total =
            NonZeroU64::new(wire.realized_total).ok_or(BundleError::InvalidForecastCalibration)?;
        let realized = RealizedCoverage::try_new(wire.realized_covered, total)
            .map_err(|_| BundleError::InvalidForecastCalibration)?;
        decoded.push(
            CalibrationBand::try_new(
                coverages[index],
                wire.lower_offset,
                wire.upper_offset,
                realized,
            )
            .map_err(|_| BundleError::InvalidForecastCalibration)?,
        );
    }
    let bands: [CalibrationBand; 3] = decoded
        .try_into()
        .map_err(|_| BundleError::InvalidForecastCalibration)?;
    if bands[2].lower_offset() > bands[1].lower_offset()
        || bands[1].lower_offset() > bands[0].lower_offset()
        || bands[0].upper_offset() > bands[1].upper_offset()
        || bands[1].upper_offset() > bands[2].upper_offset()
    {
        return Err(BundleError::InvalidForecastCalibration);
    }
    Ok(ForecastCalibrationArtifacts::new(
        method,
        window,
        parse_digest(&reference.policy.sha256)?,
        reference.policy.size_bytes,
        parse_digest(&reference.residuals.sha256)?,
        reference.residuals.size_bytes,
        bands,
        policy.dependence_assumptions,
    ))
}

pub(super) fn validate_metrics(
    values: &[MetricWire],
    output_semantics: ModelOutputSemantics,
) -> Result<Vec<ValidationMetric>, BundleError> {
    if values.is_empty() || values.len() > MAX_VALIDATION_METRICS {
        return Err(BundleError::InvalidValidationMetrics);
    }
    let mut metrics = Vec::new();
    metrics
        .try_reserve_exact(values.len())
        .map_err(|_| BundleError::RetainedSizeOverflow)?;
    for value in values {
        let name = match value.name.as_str() {
            "mean_squared_error" => ValidationMetricName::MeanSquaredError,
            "accuracy" => ValidationMetricName::Accuracy,
            "log_loss" => ValidationMetricName::LogLoss,
            "area_under_roc" => ValidationMetricName::AreaUnderRoc,
            _ => return Err(BundleError::InvalidValidationMetrics),
        };
        let is_fraction = matches!(
            name,
            ValidationMetricName::Accuracy | ValidationMetricName::AreaUnderRoc
        );
        if !value.value.is_finite()
            || value.value < 0.0
            || (is_fraction && value.value > 1.0)
            || metrics
                .iter()
                .any(|metric: &ValidationMetric| metric.name() == name)
        {
            return Err(BundleError::InvalidValidationMetrics);
        }
        metrics.push(ValidationMetric::new(name, value.value));
    }
    let required = match output_semantics {
        ModelOutputSemantics::Regression => ValidationMetricName::MeanSquaredError,
        ModelOutputSemantics::BinaryProbability => ValidationMetricName::Accuracy,
    };
    if !metrics.iter().any(|metric| metric.name() == required) {
        return Err(BundleError::InvalidValidationMetrics);
    }
    Ok(metrics)
}

pub(super) fn validate_thresholds(
    wire: ThresholdWire,
    output_semantics: ModelOutputSemantics,
) -> Result<DecisionThresholds, BundleError> {
    if !wire.negative_max.is_finite()
        || !wire.positive_min.is_finite()
        || !wire.minimum_confidence.is_finite()
        || wire.negative_max >= wire.positive_min
        || !(0.0..=1.0).contains(&wire.minimum_confidence)
        || (output_semantics == ModelOutputSemantics::BinaryProbability
            && (!(0.0..=1.0).contains(&wire.negative_max)
                || !(0.0..=1.0).contains(&wire.positive_min)))
    {
        return Err(BundleError::InvalidDecisionThresholds);
    }
    Ok(DecisionThresholds::new(
        wire.negative_max,
        wire.positive_min,
        wire.minimum_confidence,
    ))
}

pub(super) fn validate_output_semantics(
    metadata_schema_version: u32,
    value: Option<&str>,
    format: ModelFormat,
    expected: Option<ModelOutputSemantics>,
) -> Result<(ModelOutputSemantics, bool), BundleError> {
    let (semantics, bound) = match metadata_schema_version {
        LEGACY_METADATA_SCHEMA_VERSION if value.is_none() && expected.is_none() => (
            match format {
                ModelFormat::NativeLinear | ModelFormat::Onnx => ModelOutputSemantics::Regression,
                ModelFormat::NativeLogistic => ModelOutputSemantics::BinaryProbability,
            },
            false,
        ),
        METADATA_SCHEMA_VERSION | FORECAST_METADATA_SCHEMA_VERSION => {
            let semantics = match value {
                Some("regression") => ModelOutputSemantics::Regression,
                Some("binary_probability") => ModelOutputSemantics::BinaryProbability,
                _ => return Err(BundleError::InvalidOutputSemantics),
            };
            if expected != Some(semantics) {
                return Err(BundleError::InvalidOutputSemantics);
            }
            (semantics, true)
        }
        _ => return Err(BundleError::InvalidOutputSemantics),
    };
    if matches!(
        (format, semantics),
        (
            ModelFormat::NativeLinear,
            ModelOutputSemantics::BinaryProbability
        ) | (
            ModelFormat::NativeLogistic,
            ModelOutputSemantics::Regression
        )
    ) {
        return Err(BundleError::InvalidOutputSemantics);
    }
    Ok((semantics, bound))
}

const fn output_semantics_name(value: ModelOutputSemantics) -> &'static str {
    match value {
        ModelOutputSemantics::Regression => "regression",
        ModelOutputSemantics::BinaryProbability => "binary_probability",
    }
}

pub(super) fn validate_artifact(
    wire: NativeArtifactWire,
    expected_format: ModelFormat,
    features: &[ModelFeatureBinding],
) -> Result<NativeArtifact, BundleError> {
    if expected_format == ModelFormat::Onnx {
        return Err(BundleError::UnsupportedFormat);
    }
    if wire.schema_version != NATIVE_ARTIFACT_SCHEMA_VERSION {
        return Err(BundleError::UnsupportedArtifactSchemaVersion);
    }
    let format = parse_format(&wire.format)?;
    if format != expected_format {
        return Err(BundleError::UnsupportedFormat);
    }
    if wire.format_version != NATIVE_FORMAT_VERSION {
        return Err(BundleError::UnsupportedFormatVersion);
    }
    if wire.output_count != 1 {
        return Err(BundleError::UnsupportedOutputShape);
    }
    if wire.weights.len() != features.len()
        || wire.feature_semantic_sha256.len() != features.len()
        || wire.weights.is_empty()
        || wire.weights.len() > MAX_MODEL_FEATURES
    {
        return Err(BundleError::InvalidTensorShape);
    }
    let mut semantic_digests = Vec::new();
    semantic_digests
        .try_reserve_exact(features.len())
        .map_err(|_| BundleError::RetainedSizeOverflow)?;
    for (encoded, feature) in wire.feature_semantic_sha256.iter().zip(features) {
        let digest = parse_digest_bytes(encoded)?;
        if digest != feature.semantic_digest().as_bytes() {
            return Err(BundleError::FeatureOrderMismatch);
        }
        semantic_digests.push(feature.semantic_digest());
    }
    if !wire.bias.is_finite() || wire.weights.iter().any(|weight| !weight.is_finite()) {
        return Err(BundleError::NonFiniteArtifact);
    }
    Ok(NativeArtifact::new(
        format,
        semantic_digests,
        wire.weights,
        wire.bias,
    ))
}

pub(super) fn validate_limitations(values: &[String]) -> Result<(), BundleError> {
    if values.is_empty() || values.len() > MAX_LIMITATIONS {
        return Err(BundleError::InvalidLimitations);
    }
    for value in values {
        validate_prose(value).map_err(|_| BundleError::InvalidLimitations)?;
    }
    Ok(())
}

pub(super) fn validate_fallback(value: &FallbackWire) -> Result<(), BundleError> {
    if value.policy != "no_action" {
        return Err(BundleError::InvalidFallback);
    }
    validate_prose(&value.reason).map_err(|_| BundleError::InvalidFallback)
}

pub(super) fn parse_format(value: &str) -> Result<ModelFormat, BundleError> {
    match value {
        "native_linear" => Ok(ModelFormat::NativeLinear),
        "native_logistic" => Ok(ModelFormat::NativeLogistic),
        "onnx" => Ok(ModelFormat::Onnx),
        _ => Err(BundleError::UnsupportedFormat),
    }
}

pub(super) fn parse_digest(value: &str) -> Result<Sha256Digest, BundleError> {
    parse_digest_bytes(value).map(Sha256Digest::new)
}

pub(super) fn validate_prose(value: &str) -> Result<(), ()> {
    if value.is_empty()
        || value.len() > MAX_PROSE_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        Err(())
    } else {
        Ok(())
    }
}

fn parse_digest_bytes(value: &str) -> Result<[u8; 32], BundleError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(BundleError::InvalidDigest);
    }
    let mut bytes = [0_u8; 32];
    for (target, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let high = hex_nibble(pair[0]).ok_or(BundleError::InvalidDigest)?;
        let low = hex_nibble(pair[1]).ok_or(BundleError::InvalidDigest)?;
        *target = (high << 4) | low;
    }
    Ok(bytes)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}
