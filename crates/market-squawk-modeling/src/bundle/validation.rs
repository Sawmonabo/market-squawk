//! Closed bundle grammar and exact Task 11/12 relationship validation.

use std::num::NonZeroU32;

use market_squawk_analytics::{FeatureKey, FeatureRegistry};
use market_squawk_data::{ComponentKind, ComponentScope, CorporateActionSensitivity, Sha256Digest};
use serde::Deserialize;

use super::BundleError;
use crate::native::NativeArtifact;
use crate::{
    BundleExpectations, DecisionThresholds, FeatureNormalizer, MAX_MODEL_FEATURES,
    ModelFeatureBinding, ModelFormat, ValidationMetric, ValidationMetricName,
};

pub(super) const METADATA_SCHEMA_VERSION: u32 = 1;
pub(super) const NATIVE_FORMAT_VERSION: u32 = 1;
const MAX_VALIDATION_METRICS: usize = 32;
const MAX_LIMITATIONS: usize = 32;
const MAX_PROSE_BYTES: usize = 512;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MetadataWire {
    pub(super) schema_version: u32,
    pub(super) bundle_id: String,
    pub(super) bundle_version: u64,
    pub(super) model_id: String,
    pub(super) artifact: ArtifactRefWire,
    pub(super) features: Vec<FeatureWire>,
    pub(super) training_dataset: DatasetWire,
    pub(super) training_universe_id: String,
    pub(super) training_period: TrainingPeriodWire,
    pub(super) label: LabelWire,
    pub(super) training_code_revision: String,
    pub(super) validation_metrics: Vec<MetricWire>,
    pub(super) decision_thresholds: ThresholdWire,
    pub(super) intended_use: String,
    pub(super) limitations: Vec<String>,
    pub(super) fallback: FallbackWire,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FeatureWire {
    name: String,
    version: u32,
    input_schema_sha256: String,
    semantic_sha256: String,
    normalizer: NormalizerWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NormalizerWire {
    kind: String,
    mean: Option<f64>,
    scale: Option<f64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DatasetWire {
    dataset_id: String,
    manifest_version: u64,
    schema_name: String,
    schema_version: u16,
    schema_sha256: String,
    manifest_sha256: String,
    build_spec_sha256: String,
    universe_sha256: String,
    policy_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TrainingPeriodWire {
    pub(super) start_unix_nanos: i64,
    pub(super) end_unix_nanos: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LabelWire {
    kind: String,
    scope: String,
    corporate_action_sensitivity: String,
    name: String,
    version: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MetricWire {
    name: String,
    value: f64,
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
            == expectations.dataset().policy_digest().bytes();
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

pub(super) fn validate_metrics(
    values: &[MetricWire],
    format: ModelFormat,
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
    let required = match format {
        ModelFormat::NativeLinear => ValidationMetricName::MeanSquaredError,
        ModelFormat::NativeLogistic => ValidationMetricName::Accuracy,
    };
    if !metrics.iter().any(|metric| metric.name() == required) {
        return Err(BundleError::InvalidValidationMetrics);
    }
    Ok(metrics)
}

pub(super) fn validate_thresholds(
    wire: ThresholdWire,
    format: ModelFormat,
) -> Result<DecisionThresholds, BundleError> {
    if !wire.negative_max.is_finite()
        || !wire.positive_min.is_finite()
        || !wire.minimum_confidence.is_finite()
        || wire.negative_max >= wire.positive_min
        || !(0.0..=1.0).contains(&wire.minimum_confidence)
        || (format == ModelFormat::NativeLogistic
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

pub(super) fn validate_artifact(
    wire: NativeArtifactWire,
    expected_format: ModelFormat,
    features: &[ModelFeatureBinding],
) -> Result<NativeArtifact, BundleError> {
    if wire.schema_version != METADATA_SCHEMA_VERSION {
        return Err(BundleError::UnsupportedMetadataVersion);
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
