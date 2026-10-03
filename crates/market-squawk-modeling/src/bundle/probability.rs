//! Original binary outcomes, held-out calibration, and untouched evaluation admission.

use super::io::{read_exact_bounded, sha256_digest, validate_json_structure};
use super::validation::{FileRefWire, ForecastTargetWire, TrainingRunWire, parse_digest};
use super::{BundleError, BundleExpectations, ControlledModelRoot};
use crate::{CalibrationWindow, ModelFormat, ModelOutputSemantics};
use market_squawk_data::{DatasetSplit, PythonDatasetSelection, Sha256Digest};
use market_squawk_domain::Timestamp;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;

pub(super) const OUTCOMES_PATH: &str = "calibration/probability-outcomes.bin";
pub(super) const POLICY_PATH: &str = "calibration/probability-policy.json";
const MAX_OUTCOMES: usize = 4_000_000;
const MAX_POLICY: usize = 65_536;

#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct ProbabilityCalibrationRefWire {
    pub(super) outcomes: FileRefWire,
    pub(super) policy: FileRefWire,
}

/// One untouched evaluation reliability bin; empty bins retain explicit absent means.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct ProbabilityReliabilityBin {
    count: u32,
    mean_probability: Option<f64>,
    observed_frequency: Option<f64>,
}
impl ProbabilityReliabilityBin {
    pub const fn count(&self) -> u32 {
        self.count
    }
    pub const fn mean_probability(&self) -> Option<f64> {
        self.mean_probability
    }
    pub const fn observed_frequency(&self) -> Option<f64> {
        self.observed_frequency
    }
}

/// Bundle-admitted calibration and later evaluation, never caller-authored probability authority.
#[derive(Clone, Debug, PartialEq)]
pub struct ProbabilityCalibrationArtifacts {
    policy_hash: Sha256Digest,
    outcomes_hash: Sha256Digest,
    train_window: CalibrationWindow,
    calibration_window: CalibrationWindow,
    evaluation_window: CalibrationWindow,
    slope: f64,
    intercept: f64,
    brier_score: f64,
    log_loss: f64,
    reliability_bins: [ProbabilityReliabilityBin; 10],
}
impl ProbabilityCalibrationArtifacts {
    pub const fn policy_hash(&self) -> Sha256Digest {
        self.policy_hash
    }
    pub const fn outcomes_hash(&self) -> Sha256Digest {
        self.outcomes_hash
    }
    pub const fn train_window(&self) -> CalibrationWindow {
        self.train_window
    }
    pub const fn calibration_window(&self) -> CalibrationWindow {
        self.calibration_window
    }
    pub const fn evaluation_window(&self) -> CalibrationWindow {
        self.evaluation_window
    }
    pub const fn calibration_slope(&self) -> f64 {
        self.slope
    }
    pub const fn calibration_intercept(&self) -> f64 {
        self.intercept
    }
    pub const fn brier_score(&self) -> f64 {
        self.brier_score
    }
    pub const fn log_loss(&self) -> f64 {
        self.log_loss
    }
    pub const fn reliability_bins(&self) -> &[ProbabilityReliabilityBin; 10] {
        &self.reliability_bins
    }
}
impl Serialize for ProbabilityCalibrationArtifacts {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        fn window(value: CalibrationWindow) -> serde_json::Value {
            serde_json::json!({"kind":"exact_time", "start_unix_nanos":value.start().map(Timestamp::unix_nanos),
                "end_unix_nanos":value.end().map(Timestamp::unix_nanos),"observations":value.observations().get()})
        }
        serde_json::json!({"method":"sigmoid_logit_affine_v1", "policy_sha256":hex(self.policy_hash),
            "outcomes_sha256":hex(self.outcomes_hash),"train_window":window(self.train_window),
            "calibration_window":window(self.calibration_window),"evaluation_window":window(self.evaluation_window),
            "slope":self.slope,"intercept":self.intercept,"regularization_c":1_000_000.0,
            "evaluation":{"brier_score":self.brier_score,"log_loss":self.log_loss,"reliability_bins":self.reliability_bins}}).serialize(serializer)
    }
}
fn hex(value: Sha256Digest) -> String {
    value
        .bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Policy {
    schema_version: u32,
    method: String,
    target: ForecastTargetWire,
    dataset_export_sha256: String,
    dataset_selection_sha256: String,
    split_sha256: String,
    train_window: Window,
    calibration_window: Window,
    evaluation_window: Window,
    slope: f64,
    intercept: f64,
    regularization_c: f64,
    outcomes_sha256: String,
    evaluation: Evaluation,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Window {
    kind: String,
    start_unix_nanos: i64,
    end_unix_nanos: i64,
    observations: u32,
}
impl Window {
    fn decode(&self) -> Result<CalibrationWindow, BundleError> {
        if self.kind != "exact_time" {
            return Err(BundleError::InvalidProbabilityCalibration);
        }
        CalibrationWindow::try_new(
            Timestamp::from_unix_nanos(self.start_unix_nanos),
            Timestamp::from_unix_nanos(self.end_unix_nanos),
            NonZeroU32::new(self.observations).ok_or(BundleError::InvalidProbabilityCalibration)?,
        )
        .map_err(|_| BundleError::InvalidProbabilityCalibration)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Evaluation {
    brier_score: f64,
    log_loss: f64,
    reliability_bins: [BinWire; 10],
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BinWire {
    count: u32,
    #[serde(deserialize_with = "Option::deserialize")]
    mean_probability: Option<f64>,
    #[serde(deserialize_with = "Option::deserialize")]
    observed_frequency: Option<f64>,
}

#[derive(Clone, Copy)]
struct Outcome {
    raw: f64,
    probability: f64,
    label: f64,
    origin: i64,
    maturity: i64,
}
fn outcome(bytes: &[u8]) -> Result<Outcome, BundleError> {
    let array = |index: usize| {
        bytes
            .get(index..index + 8)
            .and_then(|v| v.try_into().ok())
            .ok_or(BundleError::InvalidProbabilityCalibration)
    };
    let value = Outcome {
        raw: f64::from_le_bytes(array(0)?),
        probability: f64::from_le_bytes(array(8)?),
        label: f64::from_le_bytes(array(16)?),
        origin: i64::from_le_bytes(array(24)?),
        maturity: i64::from_le_bytes(array(32)?),
    };
    if !value.raw.is_finite()
        || !value.probability.is_finite()
        || !(0.0..=1.0).contains(&value.probability)
        || !matches!(value.label, 0.0 | 1.0)
        || value.origin >= value.maturity
    {
        return Err(BundleError::InvalidProbabilityCalibration);
    }
    Ok(value)
}
fn close(a: f64, b: f64) -> bool {
    a.is_finite() && b.is_finite() && (a - b).abs() <= 1e-12 + 1e-10 * a.abs().max(b.abs())
}
fn sigmoid(value: f64) -> f64 {
    if value >= 0.0 {
        1.0 / (1.0 + (-value).exp())
    } else {
        let exp = value.exp();
        exp / (1.0 + exp)
    }
}

pub(super) fn load(
    root: &ControlledModelRoot,
    reference: &ProbabilityCalibrationRefWire,
    run: &TrainingRunWire,
    expectations: &BundleExpectations,
    format: ModelFormat,
) -> Result<(ProbabilityCalibrationArtifacts, Box<[u8]>, Box<[u8]>), BundleError> {
    let invalid = BundleError::InvalidProbabilityCalibration;
    if expectations.output_semantics() != ModelOutputSemantics::BinaryProbability
        || reference.outcomes.path != OUTCOMES_PATH
        || reference.policy.path != POLICY_PATH
        || reference.outcomes.size_bytes == 0
        || reference.outcomes.size_bytes > MAX_OUTCOMES as u64
        || reference.policy.size_bytes == 0
        || reference.policy.size_bytes > MAX_POLICY as u64
    {
        return Err(invalid);
    }
    let outcomes = read_exact_bounded(&root.directory, OUTCOMES_PATH, MAX_OUTCOMES, invalid)?;
    let policy = read_exact_bounded(&root.directory, POLICY_PATH, MAX_POLICY, invalid)?;
    let outcomes_hash = parse_digest(&reference.outcomes.sha256)?;
    let policy_hash = parse_digest(&reference.policy.sha256)?;
    if outcomes.len() as u64 != reference.outcomes.size_bytes
        || policy.len() as u64 != reference.policy.size_bytes
        || sha256_digest(&outcomes) != outcomes_hash
        || sha256_digest(&policy) != policy_hash
        || outcomes.len() % 40 != 0
    {
        return Err(invalid);
    }
    validate_json_structure(&policy).map_err(|_| invalid)?;
    let wire: Policy = serde_json::from_slice(&policy).map_err(|_| invalid)?;
    let counts = &run.trial.split_counts;
    let boundaries = expectations
        .dataset()
        .split_policy()
        .timestamp_boundaries()
        .ok_or(invalid)?
        .map(Timestamp::unix_nanos);
    let training = run
        .trial
        .training_period
        .timestamp_bounds()
        .ok_or(invalid)?;
    if wire.schema_version != 1
        || wire.method != "sigmoid_logit_affine_v1"
        || wire.regularization_c != 1_000_000.0
        || !wire.slope.is_finite()
        || !wire.intercept.is_finite()
        || !matches!(wire.target, ForecastTargetWire::FixedHorizonEvent { .. })
        || wire.target != run.trial.output_statistic.target
        || parse_digest(&wire.dataset_export_sha256)? != expectations.dataset().export_digest()
        || parse_digest(&wire.dataset_selection_sha256)?
            != expectations.dataset().selection_digest()
        || wire.split_sha256 != run.trial.split_sha256
        || parse_digest(&wire.outcomes_sha256)? != outcomes_hash
        || wire.train_window.observations as usize != counts.train
        || wire.calibration_window.observations as usize != counts.validation
        || wire.evaluation_window.observations as usize != counts.test
        || outcomes.len() / 40 != counts.validation.checked_add(counts.test).ok_or(invalid)?
        || training
            != [
                wire.train_window.start_unix_nanos,
                wire.train_window.end_unix_nanos,
            ]
        || wire.train_window.end_unix_nanos > wire.calibration_window.start_unix_nanos
        || wire.calibration_window.end_unix_nanos > wire.evaluation_window.start_unix_nanos
        || wire.train_window.end_unix_nanos > boundaries[0].checked_add(1).ok_or(invalid)?
        || wire.calibration_window.start_unix_nanos <= boundaries[0]
        || wire.calibration_window.end_unix_nanos != boundaries[1].checked_add(1).ok_or(invalid)?
        || wire.evaluation_window.start_unix_nanos <= boundaries[1]
        || wire.evaluation_window.end_unix_nanos != boundaries[2].checked_add(1).ok_or(invalid)?
    {
        return Err(invalid);
    }
    let train_window = wire.train_window.decode()?;
    let calibration_window = wire.calibration_window.decode()?;
    let evaluation_window = wire.evaluation_window.decode()?;
    let mut sums = [(0_u32, 0.0, 0.0); 10];
    let (mut brier, mut log_loss, mut correct) = (0.0, 0.0, 0_usize);
    for (partition, (start, count, window)) in [
        (0, counts.validation, &wire.calibration_window),
        (counts.validation, counts.test, &wire.evaluation_window),
    ]
    .into_iter()
    .enumerate()
    {
        let (mut first, mut last, mut positive) = (None, None, 0_usize);
        for bytes in outcomes[start * 40..(start + count) * 40].chunks_exact(40) {
            let row = outcome(bytes)?;
            if !(wire.slope * row.raw + wire.intercept).is_finite()
                || row.origin < window.start_unix_nanos
                || row.origin >= window.end_unix_nanos
                || row.maturity >= window.end_unix_nanos
                || last.is_some_and(|previous| previous > row.origin)
                || (sigmoid(wire.slope * row.raw + wire.intercept) - row.probability).abs()
                    > if format == ModelFormat::Onnx {
                        2e-5
                    } else {
                        1e-12
                    }
            {
                return Err(invalid);
            }
            first.get_or_insert(row.origin);
            last = Some(row.origin);
            positive += usize::from(row.label == 1.0);
            if partition == 1 {
                let difference = row.probability - row.label;
                brier += difference * difference;
                let clipped = row.probability.clamp(1e-15, 1.0 - 1e-15);
                log_loss -= row.label * clipped.ln() + (1.0 - row.label) * (1.0 - clipped).ln();
                correct += usize::from((row.probability >= 0.5) == (row.label == 1.0));
                let index = ((row.probability * 10.0).floor() as usize).min(9);
                sums[index].0 += 1;
                sums[index].1 += row.probability;
                sums[index].2 += row.label;
            }
        }
        if count < 2
            || first != Some(window.start_unix_nanos)
            || first == last
            || (partition == 0 && (positive == 0 || positive == count))
        {
            return Err(invalid);
        }
    }
    brier /= counts.test as f64;
    log_loss /= counts.test as f64;
    if !close(brier, wire.evaluation.brier_score) || !close(log_loss, wire.evaluation.log_loss) {
        return Err(invalid);
    }
    let mut bins = [ProbabilityReliabilityBin {
        count: 0,
        mean_probability: None,
        observed_frequency: None,
    }; 10];
    for (index, (count, predicted, actual)) in sums.into_iter().enumerate() {
        let means = if count == 0 {
            (None, None)
        } else {
            (
                Some(predicted / f64::from(count)),
                Some(actual / f64::from(count)),
            )
        };
        let expected = &wire.evaluation.reliability_bins[index];
        if expected.count != count
            || !optional_close(expected.mean_probability, means.0)
            || !optional_close(expected.observed_frequency, means.1)
        {
            return Err(invalid);
        }
        bins[index] = ProbabilityReliabilityBin {
            count,
            mean_probability: means.0,
            observed_frequency: means.1,
        };
    }
    for metric in &run.validation_metrics {
        let expected = match metric.name.as_str() {
            "accuracy" => correct as f64 / counts.test as f64,
            "log_loss" => log_loss,
            _ => return Err(invalid),
        };
        if !close(metric.value, expected) {
            return Err(invalid);
        }
    }
    if run.validation_metrics.len() != 2 {
        return Err(invalid);
    }
    Ok((
        ProbabilityCalibrationArtifacts {
            policy_hash,
            outcomes_hash,
            train_window,
            calibration_window,
            evaluation_window,
            slope: wire.slope,
            intercept: wire.intercept,
            brier_score: brier,
            log_loss,
            reliability_bins: bins,
        },
        outcomes.into_boxed_slice(),
        policy.into_boxed_slice(),
    ))
}
fn optional_close(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => close(a, b),
        _ => false,
    }
}

/// Compare every retained complete-case outcome with original, independently verified source rows.
pub(super) fn verify_sources(
    bundle: &super::ModelBundle,
    selection: &PythonDatasetSelection,
) -> Result<(), BundleError> {
    let Some(proof) = bundle.metadata().probability_calibration() else {
        return Ok(());
    };
    let invalid = BundleError::InvalidProbabilityCalibration;
    let observations = selection
        .probability_label_observations(bundle.metadata().label())
        .ok_or(invalid)?;
    let bytes = bundle
        .probability_outcomes_bytes
        .as_deref()
        .ok_or(invalid)?;
    let expected_counts = [
        proof.train_window.observations().get() as usize,
        proof.calibration_window.observations().get() as usize,
        proof.evaluation_window.observations().get() as usize,
    ];
    let specs = selection.probability_feature_specs();
    let feature_order = bundle
        .metadata()
        .features()
        .iter()
        .map(|binding| {
            specs
                .iter()
                .position(|spec| {
                    spec.name() == binding.key().name() && spec.version() == binding.key().version()
                })
                .ok_or(invalid)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if feature_order.len() != specs.len() {
        return Err(invalid);
    }
    let mut training_start = None;
    let mut training_end = None;
    let mut counts = [0_usize; 3];
    let mut train_positive = 0_usize;
    let mut encoded = bytes.chunks_exact(40);
    for observation in observations {
        let index = match observation.split() {
            DatasetSplit::Train => 0,
            DatasetSplit::Validation => 1,
            DatasetSplit::Test => 2,
        };
        counts[index] += 1;
        if index == 0 {
            train_positive += usize::from(observation.value());
            training_start.get_or_insert(observation.partition_origin());
            training_end = Some(
                training_end.map_or(observation.label_maturity(), |prior: Timestamp| {
                    prior.max(observation.label_maturity())
                }),
            );
            continue;
        }
        let row = outcome(encoded.next().ok_or(invalid)?)?;
        if observation.features().len() != specs.len() {
            return Err(invalid);
        }
        if let Some(artifact) = bundle.native_artifact() {
            let mut score = artifact.bias();
            for ((binding, weight), index) in bundle
                .metadata()
                .features()
                .iter()
                .zip(artifact.weights())
                .zip(&feature_order)
            {
                let number = observation.features()[*index]
                    .to_string()
                    .parse::<f64>()
                    .map_err(|_| invalid)?;
                let normalized = binding.normalizer().normalize(number).ok_or(invalid)?;
                score += normalized * weight;
                if !score.is_finite() {
                    return Err(invalid);
                }
            }
            if (sigmoid(score) - row.probability).abs() > 1e-12 {
                return Err(invalid);
            }
        }
        if row.origin != observation.partition_origin().unix_nanos()
            || row.maturity != observation.label_maturity().unix_nanos()
            || (row.label == 1.0) != observation.value()
        {
            return Err(invalid);
        }
    }
    if training_start != proof.train_window.start()
        || training_end.and_then(|time| time.checked_add_nanos(1).ok()) != proof.train_window.end()
        || counts != expected_counts
        || encoded.next().is_some()
        || train_positive == 0
        || train_positive == counts[0]
    {
        return Err(invalid);
    }
    Ok(())
}
