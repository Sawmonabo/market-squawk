//! Closed bundle grammar and exact Task 11/12 relationship validation.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use market_squawk_analytics::{FeatureKey, FeatureRegistry};
use market_squawk_data::{
    ComponentKind, ComponentScope, CorporateActionSensitivity, FinancialAmountBasis,
    FinancialAmountRole, FinancialShareConvention, FixedHorizonOriginBasis, Sha256Digest,
};
use market_squawk_domain::{CalendarDate, Currency, FundamentalCadence, Timestamp};
use serde::{Deserialize, Serialize, ser::SerializeMap as _};

use super::BundleError;
use crate::native::NativeArtifact;
use crate::{
    BundleExpectations, CalibrationBand, CalibrationMethod, CalibrationWindow, DecisionThresholds,
    FeatureNormalizer, ForecastCalibrationArtifacts, ForecastCentralStatistic, ForecastCoverage,
    ForecastEstimatorProfile, ForecastMeasurement, ForecastTargetMeaning,
    ForecastTrainingObjective, ForecastTransform, MAX_MODEL_FEATURES, ModelFeatureBinding,
    ModelFormat, ModelOutputSemantics, RealizedCoverage, ValidationMetric, ValidationMetricName,
};

pub(super) const METADATA_SCHEMA_VERSION: u32 = 9;
pub(super) const NATIVE_FORMAT_VERSION: u32 = 1;
pub(super) const NATIVE_ARTIFACT_SCHEMA_VERSION: u32 = 1;
pub(super) const TRAINING_RUN_SCHEMA_VERSION: u32 = 7;
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
    pub(super) output_semantics: String,
    pub(super) output_measurement: OutputMeasurementWire,
    pub(super) output_statistic: OutputStatisticWire,
    pub(super) training_run: FileRefWire,
    pub(super) forecast_calibration: Option<ForecastCalibrationRefWire>,
    pub(super) probability_calibration: Option<super::probability::ProbabilityCalibrationRefWire>,
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

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OutputStatisticWire {
    estimator: ForecastEstimatorWire,
    objective: String,
    output_transform: String,
    statistic: String,
    pub(super) target: ForecastTargetWire,
    target_transform: String,
}

#[derive(Clone, Copy, Deserialize, PartialEq)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(super) enum ForecastTargetWire {
    #[serde(rename = "financial_period")]
    FinancialPeriod {
        cadence: FundamentalCadence,
        periods_ahead: NonZeroU16,
    },
    #[serde(rename = "fixed_horizon_terminal")]
    FixedHorizonTerminal {
        horizon_nanos: u64,
        origin_basis: FixedHorizonOriginBasis,
    },
    #[serde(rename = "fixed_horizon_event")]
    FixedHorizonEvent {
        horizon_nanos: u64,
        origin_basis: FixedHorizonOriginBasis,
        event: market_squawk_data::ProbabilityEventTarget,
    },
    #[serde(rename = "unsupported")]
    Unsupported,
}

impl Serialize for ForecastTargetWire {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(match self {
            Self::FixedHorizonTerminal { .. } => 3,
            Self::FixedHorizonEvent { .. } => 4,
            Self::FinancialPeriod { .. } => 3,
            Self::Unsupported => 1,
        }))?;
        match self {
            Self::FinancialPeriod {
                cadence,
                periods_ahead,
            } => {
                map.serialize_entry("cadence", cadence)?;
                map.serialize_entry("kind", "financial_period")?;
                map.serialize_entry("periods_ahead", periods_ahead)?;
            }
            Self::FixedHorizonTerminal {
                horizon_nanos,
                origin_basis,
            } => {
                map.serialize_entry("horizon_nanos", horizon_nanos)?;
                map.serialize_entry("kind", "fixed_horizon_terminal")?;
                map.serialize_entry("origin_basis", origin_basis)?;
            }
            Self::FixedHorizonEvent {
                horizon_nanos,
                origin_basis,
                event,
            } => {
                // Value uses its sorted map representation recursively, matching canonical Python JSON.
                let event = serde_json::to_value(event).map_err(serde::ser::Error::custom)?;
                map.serialize_entry("event", &event)?;
                map.serialize_entry("horizon_nanos", horizon_nanos)?;
                map.serialize_entry("kind", "fixed_horizon_event")?;
                map.serialize_entry("origin_basis", origin_basis)?;
            }
            Self::Unsupported => map.serialize_entry("kind", "unsupported")?,
        }
        map.end()
    }
}

#[derive(Clone, Copy, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum ForecastEstimatorWire {
    #[serde(rename = "sealed_direct_least_squares_v1")]
    SealedDirectLeastSquaresV1,
    #[serde(rename = "sealed_direct_ridge_v1")]
    SealedDirectRidgeV1 { ridge_alpha: f64 },
    #[serde(rename = "sealed_oob_mean_block_bootstrap_ridge_v1")]
    SealedOobMeanBlockBootstrapRidgeV1 {
        resampling_block_length: u32,
        resampling_count: u16,
        resampling_seed: u32,
        ridge_alpha: f64,
    },
    #[serde(rename = "sealed_binary_logistic_v1")]
    SealedBinaryLogisticV1,
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(super) enum OutputMeasurementWire {
    #[serde(rename = "financial_amount")]
    FinancialAmount {
        currency: String,
        role: FinancialAmountRole,
        basis: FinancialAmountBasis,
        share_convention: Option<FinancialShareConvention>,
    },
    #[serde(rename = "price")]
    Price { currency: String },
    #[serde(rename = "return")]
    Return,
    #[serde(rename = "probability")]
    Probability,
    #[serde(rename = "other_regression")]
    OtherRegression,
}

impl Serialize for OutputMeasurementWire {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(match self {
            Self::Price { .. } => 2,
            Self::FinancialAmount { .. } => 5,
            Self::Return | Self::Probability | Self::OtherRegression => 1,
        }))?;
        match self {
            Self::FinancialAmount {
                currency,
                role,
                basis,
                share_convention,
            } => {
                map.serialize_entry("basis", basis)?;
                map.serialize_entry("currency", currency)?;
                map.serialize_entry("kind", "financial_amount")?;
                map.serialize_entry("role", role)?;
                map.serialize_entry("share_convention", share_convention)?;
            }
            Self::Price { currency } => {
                map.serialize_entry("currency", currency)?;
                map.serialize_entry("kind", "price")?;
            }
            Self::Return => map.serialize_entry("kind", "return")?,
            Self::Probability => map.serialize_entry("kind", "probability")?,
            Self::OtherRegression => map.serialize_entry("kind", "other_regression")?,
        }
        map.end()
    }
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
    split_policy: crate::metadata::TrainingSplitWire,
    study: Option<crate::metadata::TrainingStudyWire>,
    universe_sha256: String,
}

use crate::metadata::TrainingPeriodWire;

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
    pub(super) name: String,
    pub(super) value: f64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TrainingRunWire {
    pub(super) schema_version: u32,
    pub(super) trial: TrainingTrialWire,
    pub(super) trial_sha256: String,
    pub(super) validation_metrics: Vec<MetricWire>,
    pub(super) forecast_calibration: Option<ForecastCalibrationRefWire>,
    pub(super) probability_calibration: Option<super::probability::ProbabilityCalibrationRefWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ForecastPolicyWire {
    schema_version: u32,
    kind: String,
    method: String,
    fit_window: ForecastCalibrationWindowWire,
    coverage_evaluation: Option<ForecastCoverageEvaluationWire>,
    dependence_assumptions: String,
    residuals_sha256: String,
    bands: Vec<ForecastBandWire>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ForecastCalibrationWindowWire {
    ExactTime {
        start_unix_nanos: i64,
        end_unix_nanos: i64,
        observations: u32,
    },
    FiscalDates {
        start: CalendarDate,
        end: CalendarDate,
        observations: u32,
    },
}
impl ForecastCalibrationWindowWire {
    const fn observations(&self) -> u32 {
        match *self {
            Self::ExactTime { observations, .. } | Self::FiscalDates { observations, .. } => {
                observations
            }
        }
    }
    fn native_numeric(&self) -> (u8, [i64; 2]) {
        match *self {
            Self::ExactTime {
                start_unix_nanos,
                end_unix_nanos,
                ..
            } => (1, [start_unix_nanos, end_unix_nanos]),
            Self::FiscalDates { start, end, .. } => (
                2,
                [
                    i64::from(start.days_since_unix_epoch()),
                    i64::from(end.days_since_unix_epoch()),
                ],
            ),
        }
    }
    fn decode(&self) -> Result<CalibrationWindow, BundleError> {
        let observations =
            NonZeroU32::new(self.observations()).ok_or(BundleError::InvalidForecastCalibration)?;
        match *self {
            Self::ExactTime {
                start_unix_nanos,
                end_unix_nanos,
                ..
            } => CalibrationWindow::try_new(
                Timestamp::from_unix_nanos(start_unix_nanos),
                Timestamp::from_unix_nanos(end_unix_nanos),
                observations,
            ),
            Self::FiscalDates { start, end, .. } => {
                CalibrationWindow::try_fiscal(start, end, observations)
            }
        }
        .map_err(|_| BundleError::InvalidForecastCalibration)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ForecastBandWire {
    target_coverage_basis_points: u16,
    lower_offset: f64,
    upper_offset: f64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ForecastCoverageEvaluationWire {
    window: ForecastCalibrationWindowWire,
    realized: [ForecastRealizedCoverageWire; 3],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ForecastRealizedCoverageWire {
    covered: u64,
    total: u64,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    forecast: Option<ForecastTrialWire>,
    label: LabelWire,
    missing_policy: String,
    model_id: String,
    model_kind: String,
    output_measurement: OutputMeasurementWire,
    output_semantics: String,
    pub(super) output_statistic: OutputStatisticWire,
    seed: u64,
    pub(super) split_counts: SplitCountsWire,
    pub(super) split_sha256: String,
    training_code_revision: String,
    pub(super) training_period: TrainingPeriodWire,
    universe_id: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ForecastTrialWire {
    estimator_parameters: ForecastEstimatorParametersWire,
    horizons: Vec<u32>,
    lags: Vec<u32>,
    observed_cutoff_unix_nanos: i64,
    package_versions: BTreeMap<String, String>,
    ridge_alpha: f64,
    rolling_splits: u32,
    selection_sha256: String,
    strategy: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ForecastEstimatorParametersWire {
    bootstrap_aggregation: Option<String>,
    conformal_center: String,
    conformal_method: Option<String>,
    horizon_origin: String,
    horizons: Vec<u32>,
    lags: Vec<u32>,
    partition_ends: [i64; 3],
    resampling_block_length: Option<u32>,
    resampling_count: Option<u16>,
    resampling_overlapping: Option<bool>,
    resampling_seed: Option<u32>,
    ridge_alpha: f64,
    rolling_splits: u32,
    seed: u32,
    strategy: String,
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
pub(super) struct SplitCountsWire {
    pub(super) test: usize,
    pub(super) train: usize,
    pub(super) validation: usize,
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
        && wire.selected_component_rows == expectations.dataset().selected_component_rows().get()
        && wire
            .split_policy
            .matches(expectations.dataset().split_policy())
        && crate::metadata::study_matches(
            wire.study.as_ref(),
            expectations.dataset().study_policy(),
            expectations.dataset().source_snapshot_digest(),
        );
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
) -> Result<(), BundleError> {
    if run.schema_version != TRAINING_RUN_SCHEMA_VERSION {
        return Err(BundleError::UnsupportedTrainingRunVersion);
    }
    let trial_bytes = serde_json::to_vec(&run.trial).map_err(|_| BundleError::TrainingRunSyntax)?;
    if super::io::sha256_digest(&trial_bytes) != parse_digest(&run.trial_sha256)? {
        return Err(BundleError::TrainingRunTrialHashMismatch);
    }

    let trial = &run.trial;
    validate_dataset(&trial.dataset, expectations)?;
    validate_label(&trial.label, expectations)?;
    let expected_kind = match format {
        ModelFormat::NativeLinear => "native_linear",
        ModelFormat::NativeLogistic => "native_logistic",
        ModelFormat::Onnx => match output_semantics {
            ModelOutputSemantics::Regression => "linear",
            ModelOutputSemantics::BinaryProbability => "logistic",
        },
    };
    let forecast_relationships_match = match (
        &run.forecast_calibration,
        &metadata.forecast_calibration,
        &trial.forecast,
    ) {
        (None, None, None) => true,
        (Some(run_calibration), Some(metadata_calibration), None) => {
            run_calibration == metadata_calibration
                && matches!(format, ModelFormat::NativeLinear | ModelFormat::Onnx)
                && output_semantics == ModelOutputSemantics::Regression
                && matches!(
                    trial.output_statistic.estimator,
                    ForecastEstimatorWire::SealedDirectLeastSquaresV1
                )
                && (matches!(
                    trial.output_measurement,
                    OutputMeasurementWire::Price { .. }
                        | OutputMeasurementWire::FinancialAmount { .. }
                ) || (trial.label.name == "research.fixed-horizon-forward-return"
                    && trial.label.version == 1
                    && trial.label.corporate_action_sensitivity == "requires_adjustment"
                    && trial.output_measurement == OutputMeasurementWire::Return
                    && matches!(
                        trial.output_statistic.target,
                        ForecastTargetWire::FixedHorizonTerminal {
                            origin_basis: FixedHorizonOriginBasis::CompletedBarClose
                                | FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar,
                            ..
                        }
                    )))
                && trial.output_statistic.statistic == "model_estimated_conditional_mean"
                && trial.output_statistic.target_transform == "identity"
                && trial.output_statistic.output_transform == "identity"
                && trial.output_statistic.objective == "squared_error"
                && match trial.output_statistic.target {
                    ForecastTargetWire::FixedHorizonTerminal { horizon_nanos, .. } => {
                        horizon_nanos > 0
                    }
                    ForecastTargetWire::FinancialPeriod {
                        cadence: FundamentalCadence::Annual | FundamentalCadence::Quarterly,
                        ..
                    } => true,
                    _ => false,
                }
        }
        (Some(run_calibration), Some(metadata_calibration), Some(_)) => {
            run_calibration == metadata_calibration
                && format == ModelFormat::Onnx
                && output_semantics == ModelOutputSemantics::Regression
        }
        _ => false,
    };
    let relationships_match = trial.bundle_id == metadata.bundle_id
        && trial.bundle_version == metadata.bundle_version
        && trial.dataset == metadata.training_dataset
        && trial.features.len() == metadata.features.len()
        && trial
            .features
            .iter()
            .zip(&metadata.features)
            .all(|(run_feature, metadata_feature)| {
                run_feature.name == metadata_feature.name
                    && run_feature.version == metadata_feature.version
                    && run_feature.input_schema_sha256 == metadata_feature.input_schema_sha256
                    && run_feature.semantic_sha256 == metadata_feature.semantic_sha256
            })
        && trial.label == metadata.label
        && trial.model_id == metadata.model_id
        && trial.model_kind == expected_kind
        && trial.output_semantics == output_semantics_name(output_semantics)
        && trial.output_measurement == metadata.output_measurement
        && trial.output_statistic == metadata.output_statistic
        && forecast_relationships_match
        && run.probability_calibration == metadata.probability_calibration
        && (metadata.probability_calibration.is_some()
            == (output_semantics == ModelOutputSemantics::BinaryProbability))
        && !(metadata.probability_calibration.is_some() && metadata.forecast_calibration.is_some())
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
        if trial.output_statistic.statistic != "unavailable" {
            return Err(BundleError::TrainingRunRelationshipMismatch);
        }
        let ordered_positive = |values: &[u32], maximum: usize| {
            !values.is_empty()
                && values.len() <= maximum
                && values.iter().all(|value| *value > 0)
                && values.windows(2).all(|pair| pair[0] < pair[1])
        };
        let parameters = &forecast.estimator_parameters;
        let method = parameters.conformal_method.as_deref();
        let sampling_valid = if method.is_none() {
            parameters.resampling_block_length.is_none()
                && parameters.resampling_count.is_none()
                && parameters.resampling_overlapping.is_none()
                && parameters.resampling_seed.is_none()
        } else {
            parameters
                .resampling_block_length
                .is_some_and(|length| (1..=100_000).contains(&length))
                && parameters
                    .resampling_count
                    .is_some_and(|count| (2..=30).contains(&count))
                && parameters.resampling_overlapping == Some(false)
                && parameters.resampling_seed == Some(parameters.seed)
        };
        if parameters.horizons != forecast.horizons
            || parameters.lags != forecast.lags
            || parameters.horizon_origin != "next_index_after_last_observed"
            || Some(parameters.partition_ends) != trial.dataset.split_policy.timestamp_boundaries()
            || parameters.ridge_alpha.to_bits() != forecast.ridge_alpha.to_bits()
            || parameters.rolling_splits != forecast.rolling_splits
            || u64::from(parameters.seed) != trial.seed
            || parameters.strategy != forecast.strategy
            || !matches!(method, None | Some("enbpi") | Some("aci"))
            || !sampling_valid
            || (method == Some("enbpi")
                && (parameters.conformal_center != "oob_weighted_bootstrap_mean"
                    || parameters.bootstrap_aggregation.as_deref() != Some("oob_weighted_mean")))
            || (method != Some("enbpi")
                && (parameters.conformal_center != "single_fitted_model"
                    || parameters.bootstrap_aggregation.is_some()))
        {
            return Err(BundleError::TrainingRunRelationshipMismatch);
        }
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
    let estimator_matches_producer = match (
        format,
        output_semantics,
        &trial.forecast,
        &trial.output_statistic.estimator,
    ) {
        (
            ModelFormat::NativeLinear | ModelFormat::Onnx,
            ModelOutputSemantics::Regression,
            None,
            ForecastEstimatorWire::SealedDirectLeastSquaresV1,
        ) => true,
        (
            ModelFormat::Onnx,
            ModelOutputSemantics::Regression,
            Some(forecast),
            ForecastEstimatorWire::SealedDirectRidgeV1 { ridge_alpha },
        ) => {
            ridge_alpha.to_bits() == forecast.ridge_alpha.to_bits()
                && forecast.estimator_parameters.conformal_method.as_deref() != Some("enbpi")
        }
        (
            ModelFormat::Onnx,
            ModelOutputSemantics::Regression,
            Some(forecast),
            ForecastEstimatorWire::SealedOobMeanBlockBootstrapRidgeV1 {
                ridge_alpha,
                resampling_block_length,
                resampling_count,
                resampling_seed,
            },
        ) => {
            let parameters = &forecast.estimator_parameters;
            ridge_alpha.to_bits() == forecast.ridge_alpha.to_bits()
                && parameters.conformal_method.as_deref() == Some("enbpi")
                && parameters.resampling_block_length == Some(*resampling_block_length)
                && parameters.resampling_count == Some(*resampling_count)
                && parameters.resampling_seed == Some(*resampling_seed)
        }
        (
            ModelFormat::NativeLogistic | ModelFormat::Onnx,
            ModelOutputSemantics::BinaryProbability,
            None,
            ForecastEstimatorWire::SealedBinaryLogisticV1,
        ) => true,
        _ => false,
    };
    if !estimator_matches_producer {
        return Err(BundleError::TrainingRunRelationshipMismatch);
    }
    Ok(())
}

pub(super) fn validate_forecast_calibration(
    reference: &ForecastCalibrationRefWire,
    policy: ForecastPolicyWire,
    residuals: &[u8],
    run: &TrainingRunWire,
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
    if let Some(forecast) = &run.trial.forecast {
        let method_matches = matches!(
            (
                forecast.estimator_parameters.conformal_method.as_deref(),
                method
            ),
            (None, CalibrationMethod::ResidualQuantile)
                | (Some("enbpi"), CalibrationMethod::MapieEnbpi)
                | (Some("aci"), CalibrationMethod::MapieAci)
        );
        if !method_matches {
            return Err(BundleError::InvalidForecastCalibration);
        }
    }
    let observations = NonZeroU32::new(policy.fit_window.observations())
        .ok_or(BundleError::InvalidForecastCalibration)?;
    let evaluation_observations = policy
        .coverage_evaluation
        .as_ref()
        .map_or(0, |evaluation| evaluation.window.observations());
    if observations
        .get()
        .checked_add(evaluation_observations)
        .and_then(|count| usize::try_from(count).ok())
        != Some(residuals.len() / size_of::<f64>())
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
    if run.trial.forecast.is_none() {
        validate_direct_calibration(&policy, residuals, run)?;
    } else {
        validate_research_calibration(&policy, residuals, run)?;
    }
    let window = policy.fit_window.decode()?;
    let coverage_evaluation = policy
        .coverage_evaluation
        .as_ref()
        .map(|evaluation| {
            let evaluation_window = evaluation.window.decode()?;
            if !evaluation_window
                .start_coordinate()
                .partial_cmp(&window.end_coordinate())
                .is_some_and(|order| !order.is_lt())
            {
                return Err(BundleError::InvalidForecastCalibration);
            }
            let mut realized = Vec::with_capacity(3);
            for item in &evaluation.realized {
                realized.push(
                    RealizedCoverage::try_new(
                        item.covered,
                        NonZeroU64::new(item.total)
                            .ok_or(BundleError::InvalidForecastCalibration)?,
                    )
                    .map_err(|_| BundleError::InvalidForecastCalibration)?,
                );
            }
            crate::CalibrationCoverageEvaluation::try_new(
                evaluation_window,
                realized
                    .try_into()
                    .map_err(|_| BundleError::InvalidForecastCalibration)?,
            )
            .map_err(|_| BundleError::InvalidForecastCalibration)
        })
        .transpose()?;
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
        decoded.push(
            CalibrationBand::try_new(coverages[index], wire.lower_offset, wire.upper_offset)
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
        coverage_evaluation,
        policy.dependence_assumptions,
    ))
}

pub(super) fn admitted_residual_distribution(
    residuals: &[u8],
    residuals_hash: Sha256Digest,
    training_run_hash: Sha256Digest,
    run: &TrainingRunWire,
) -> Result<Option<crate::ForecastResidualDistribution>, BundleError> {
    if run.trial.forecast.is_some()
        || run.trial.output_statistic.statistic != "model_estimated_conditional_mean"
    {
        return Ok(None);
    }
    crate::ForecastResidualDistribution::try_from_admitted_residuals(
        residuals,
        run.trial.split_counts.validation,
        residuals_hash,
        training_run_hash,
    )
    .map(Some)
    .map_err(|_| BundleError::InvalidForecastCalibration)
}

fn validate_direct_calibration(
    policy: &ForecastPolicyWire,
    residuals: &[u8],
    run: &TrainingRunWire,
) -> Result<(), BundleError> {
    let counts = &run.trial.split_counts;
    let evaluation = policy
        .coverage_evaluation
        .as_ref()
        .ok_or(BundleError::InvalidForecastCalibration)?;
    let (precision, [train_end, validation_end, test_end]) =
        run.trial.dataset.split_policy.native_numeric();
    let (training_precision, [_, training_end]) = run.trial.training_period.native_numeric();
    let (fit_precision, [fit_start, fit_end]) = policy.fit_window.native_numeric();
    let (evaluation_precision, [evaluation_start, evaluation_end]) =
        evaluation.window.native_numeric();
    if [training_precision, fit_precision, evaluation_precision]
        .iter()
        .any(|value| *value != precision)
    {
        return Err(BundleError::InvalidForecastCalibration);
    }
    let selection_end = if precision == 1 {
        run.trial.dataset.selection_as_of_unix_nanos
    } else {
        i64::from(
            Timestamp::from_unix_nanos(run.trial.dataset.selection_as_of_unix_nanos)
                .utc_calendar_date()
                .map_err(|_| BundleError::InvalidForecastCalibration)?
                .days_since_unix_epoch(),
        )
    };
    if policy.kind != "residual_quantile"
        || policy.method != "residual_quantile"
        || counts.validation < 2
        || counts.test == 0
        || counts.validation.checked_add(counts.test) != Some(residuals.len() / size_of::<f64>())
        || usize::try_from(policy.fit_window.observations()).ok() != Some(counts.validation)
        || usize::try_from(evaluation.window.observations()).ok() != Some(counts.test)
        || fit_start < training_end
        || training_end > train_end.saturating_add(1)
        || fit_start <= train_end
        || Some(fit_end) != validation_end.checked_add(1)
        || evaluation_start <= validation_end
        || Some(evaluation_end) != test_end.checked_add(1)
        || fit_end > evaluation_start
        || evaluation_end > selection_end.saturating_add(1)
        || policy.bands.len() != 3
    {
        return Err(BundleError::InvalidForecastCalibration);
    }
    let decode = |chunk: &[u8]| -> Result<f64, BundleError> {
        let bytes = chunk
            .try_into()
            .map_err(|_| BundleError::InvalidForecastCalibration)?;
        Ok(f64::from_le_bytes(bytes))
    };
    let boundary = counts
        .validation
        .checked_mul(size_of::<f64>())
        .ok_or(BundleError::InvalidForecastCalibration)?;
    let (calibration, held_out) = residuals.split_at(boundary);
    let mut ordered = Vec::new();
    ordered
        .try_reserve_exact(counts.validation)
        .map_err(|_| BundleError::RetainedSizeOverflow)?;
    for chunk in calibration.chunks_exact(size_of::<f64>()) {
        ordered.push(decode(chunk)?);
    }
    ordered.sort_unstable_by(f64::total_cmp);
    for ((band, coverage), realized) in policy
        .bands
        .iter()
        .zip([5_000_usize, 8_000, 9_500])
        .zip(&evaluation.realized)
    {
        let last = ordered.len() - 1;
        let lower_index = (10_000 - coverage) * last / 20_000;
        let upper_index = ((10_000 + coverage) * last).div_ceil(20_000);
        let lower = if ordered[lower_index] < 0.0 {
            ordered[lower_index]
        } else {
            0.0
        };
        let upper = if ordered[upper_index] > 0.0 {
            ordered[upper_index]
        } else {
            0.0
        };
        let mut covered = 0_u64;
        for chunk in held_out.chunks_exact(size_of::<f64>()) {
            let residual = decode(chunk)?;
            covered += u64::from(lower <= residual && residual <= upper);
        }
        if band.lower_offset.to_bits() != lower.to_bits()
            || band.upper_offset.to_bits() != upper.to_bits()
            || realized.covered != covered
            || usize::try_from(realized.total).ok() != Some(counts.test)
        {
            return Err(BundleError::InvalidForecastCalibration);
        }
    }
    Ok(())
}

fn validate_research_calibration(
    policy: &ForecastPolicyWire,
    residuals: &[u8],
    run: &TrainingRunWire,
) -> Result<(), BundleError> {
    let evaluation = policy
        .coverage_evaluation
        .as_ref()
        .ok_or(BundleError::InvalidForecastCalibration)?;
    let (precision, [train_end, validation_end, test_end]) =
        run.trial.dataset.split_policy.native_numeric();
    let (training_precision, [_, training_end]) = run.trial.training_period.native_numeric();
    let (fit_precision, [fit_start, fit_end]) = policy.fit_window.native_numeric();
    let (evaluation_precision, [evaluation_start, evaluation_end]) =
        evaluation.window.native_numeric();
    if [training_precision, fit_precision, evaluation_precision]
        .iter()
        .any(|value| *value != precision)
    {
        return Err(BundleError::InvalidForecastCalibration);
    }
    let selection_end = if precision == 1 {
        run.trial.dataset.selection_as_of_unix_nanos
    } else {
        i64::from(
            Timestamp::from_unix_nanos(run.trial.dataset.selection_as_of_unix_nanos)
                .utc_calendar_date()
                .map_err(|_| BundleError::InvalidForecastCalibration)?
                .days_since_unix_epoch(),
        )
    };
    if training_end > train_end.saturating_add(1)
        || fit_start <= train_end
        || Some(fit_end) != validation_end.checked_add(1)
        || evaluation_start <= validation_end
        || Some(evaluation_end) != test_end.checked_add(1)
        || evaluation_end > selection_end.saturating_add(1)
        || policy.bands.len() != 3
    {
        return Err(BundleError::InvalidForecastCalibration);
    }
    let boundary = usize::try_from(policy.fit_window.observations())
        .ok()
        .and_then(|count| count.checked_mul(8))
        .ok_or(BundleError::InvalidForecastCalibration)?;
    let held_out = residuals
        .get(boundary..)
        .ok_or(BundleError::InvalidForecastCalibration)?;
    for (band, realized) in policy.bands.iter().zip(&evaluation.realized) {
        let mut covered = 0_u64;
        for chunk in held_out.chunks_exact(8) {
            let value = f64::from_le_bytes(
                chunk
                    .try_into()
                    .map_err(|_| BundleError::InvalidForecastCalibration)?,
            );
            covered += u64::from(band.lower_offset <= value && value <= band.upper_offset);
        }
        if realized.covered != covered
            || usize::try_from(realized.total).ok() != Some(held_out.len() / 8)
        {
            return Err(BundleError::InvalidForecastCalibration);
        }
    }
    Ok(())
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
    value: &str,
    format: ModelFormat,
    expected: ModelOutputSemantics,
) -> Result<ModelOutputSemantics, BundleError> {
    let semantics = match value {
        "regression" => ModelOutputSemantics::Regression,
        "binary_probability" => ModelOutputSemantics::BinaryProbability,
        _ => return Err(BundleError::InvalidOutputSemantics),
    };
    if semantics != expected
        || matches!(
            (format, semantics),
            (
                ModelFormat::NativeLinear,
                ModelOutputSemantics::BinaryProbability
            ) | (
                ModelFormat::NativeLogistic,
                ModelOutputSemantics::Regression
            )
        )
    {
        return Err(BundleError::InvalidOutputSemantics);
    }
    Ok(semantics)
}

pub(super) fn validate_output_measurement(
    value: &OutputMeasurementWire,
    expectations: &BundleExpectations,
) -> Result<(), BundleError> {
    let expected = expectations.output_binding();
    if measurement(value)? == expected.measurement() {
        Ok(())
    } else {
        Err(BundleError::InvalidOutputMeasurement)
    }
}

pub(super) fn validate_output_statistic(
    value: &OutputStatisticWire,
    expectations: &BundleExpectations,
) -> Result<(), BundleError> {
    let expected = expectations.output_binding();
    let statistic = match value.statistic.as_str() {
        "model_estimated_conditional_mean" => {
            ForecastCentralStatistic::ModelEstimatedConditionalMean
        }
        "unavailable" => ForecastCentralStatistic::Unavailable,
        _ => return Err(BundleError::InvalidOutputMeasurement),
    };
    let target = match value.target {
        ForecastTargetWire::FixedHorizonTerminal {
            horizon_nanos,
            origin_basis,
        } => ForecastTargetMeaning::FixedHorizonTerminal {
            horizon_nanos: NonZeroU64::new(horizon_nanos)
                .ok_or(BundleError::InvalidOutputMeasurement)?,
            origin_basis,
        },
        ForecastTargetWire::FixedHorizonEvent {
            horizon_nanos,
            origin_basis,
            event,
        } => ForecastTargetMeaning::FixedHorizonEvent {
            horizon_nanos: NonZeroU64::new(horizon_nanos)
                .ok_or(BundleError::InvalidOutputSemantics)?,
            origin_basis,
            event,
        },
        ForecastTargetWire::Unsupported => ForecastTargetMeaning::Unsupported,
        ForecastTargetWire::FinancialPeriod {
            cadence,
            periods_ahead,
        } => ForecastTargetMeaning::FinancialPeriod {
            cadence,
            periods_ahead,
        },
    };
    let transform = |value: &str| match value {
        "identity" => Ok(ForecastTransform::Identity),
        "logistic" => Ok(ForecastTransform::Logistic),
        _ => Err(BundleError::InvalidOutputMeasurement),
    };
    let objective = match value.objective.as_str() {
        "squared_error" => ForecastTrainingObjective::SquaredError,
        "binary_cross_entropy" => ForecastTrainingObjective::BinaryCrossEntropy,
        _ => return Err(BundleError::InvalidOutputMeasurement),
    };
    let estimator = match value.estimator {
        ForecastEstimatorWire::SealedDirectLeastSquaresV1 => {
            ForecastEstimatorProfile::SealedDirectLeastSquaresV1
        }
        ForecastEstimatorWire::SealedDirectRidgeV1 { ridge_alpha } => {
            if !ridge_alpha.is_finite() || ridge_alpha < 0.0 {
                return Err(BundleError::InvalidOutputMeasurement);
            }
            ForecastEstimatorProfile::SealedDirectRidgeV1 {
                ridge_alpha_bits: ridge_alpha.to_bits(),
            }
        }
        ForecastEstimatorWire::SealedOobMeanBlockBootstrapRidgeV1 {
            ridge_alpha,
            resampling_block_length,
            resampling_count,
            resampling_seed,
        } => {
            if !ridge_alpha.is_finite()
                || ridge_alpha < 0.0
                || !(1..=100_000).contains(&resampling_block_length)
                || !(2..=30).contains(&resampling_count)
            {
                return Err(BundleError::InvalidOutputMeasurement);
            }
            ForecastEstimatorProfile::SealedOobMeanBlockBootstrapRidgeV1 {
                ridge_alpha_bits: ridge_alpha.to_bits(),
                resampling_block_length,
                resampling_count,
                resampling_seed,
            }
        }
        ForecastEstimatorWire::SealedBinaryLogisticV1 => {
            ForecastEstimatorProfile::SealedBinaryLogisticV1
        }
    };
    if statistic == expected.central_statistic()
        && target == expected.target()
        && transform(&value.target_transform)? == expected.target_transform()
        && transform(&value.output_transform)? == expected.output_transform()
        && objective == expected.objective()
        && estimator == expected.estimator()
    {
        Ok(())
    } else {
        Err(BundleError::InvalidOutputMeasurement)
    }
}

fn measurement(value: &OutputMeasurementWire) -> Result<ForecastMeasurement, BundleError> {
    match value {
        OutputMeasurementWire::Price { currency } => {
            let parsed = Currency::try_from(currency.as_str())
                .map_err(|_| BundleError::InvalidOutputMeasurement)?;
            if parsed.as_str() != currency {
                return Err(BundleError::InvalidOutputMeasurement);
            }
            Ok(ForecastMeasurement::Price { currency: parsed })
        }
        OutputMeasurementWire::Return => Ok(ForecastMeasurement::Return),
        OutputMeasurementWire::FinancialAmount {
            currency,
            role,
            basis,
            share_convention,
        } => {
            let parsed = Currency::try_from(currency.as_str())
                .map_err(|_| BundleError::InvalidOutputMeasurement)?;
            if parsed.as_str() != currency {
                return Err(BundleError::InvalidOutputMeasurement);
            }
            Ok(ForecastMeasurement::FinancialAmount {
                currency: parsed,
                role: *role,
                basis: *basis,
                share_convention: *share_convention,
            })
        }
        OutputMeasurementWire::Probability => Ok(ForecastMeasurement::Probability),
        OutputMeasurementWire::OtherRegression => Ok(ForecastMeasurement::OtherRegression),
    }
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
