//! Canonical bounded JSON encoding for immutable trial records.

use market_squawk_data::Sha256Digest;
use market_squawk_domain::SourceIdentifier;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::ExperimentError;
use super::diagnostics::{BacktestOverfittingDiagnostic, DeflatedPerformanceDiagnostic};
use super::model::{
    BacktestArtifact, ExperimentLimits, TrialCompletion, TrialCompletionInput,
    TrialComponentBinding, TrialFailure, TrialId, TrialMetric, TrialParameter,
    TrialSearchDimension, TrialSpec, TrialSpecInput, TrialStatus,
};

const TRIAL_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReservationWire {
    schema_version: u16,
    trial_id: String,
    spec: TrialSpecWire,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrialSpecWire {
    dataset_identity: String,
    object_graph_digest: String,
    execution_assumption_digest: String,
    model: Option<BindingWire>,
    strategy: BindingWire,
    code: BindingWire,
    configuration_digest: String,
    seed: u64,
    parameters: Vec<PairWire>,
    search_space: Vec<SearchWire>,
    selection_criterion: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingWire {
    name: String,
    digest: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PairWire {
    name: String,
    value: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchWire {
    name: String,
    candidates: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalWire {
    schema_version: u16,
    trial_id: String,
    status: TerminalStatusWire,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed: Option<CompletedWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failed: Option<FailedWire>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TerminalStatusWire {
    Completed,
    Failed,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletedWire {
    result_digest: String,
    artifact_reference: String,
    artifact_digest: String,
    artifact_bytes: u64,
    metrics: Vec<MetricWire>,
    probability_of_backtest_overfitting: f64,
    probability_fold_count: usize,
    deflated_performance_probability: f64,
    expected_maximum_sharpe: f64,
    selected: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FailedWire {
    code: String,
    evidence_digest: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricWire {
    name: String,
    value: f64,
}

pub(super) fn encode_reservation(spec: &TrialSpec) -> Result<Vec<u8>, ExperimentError> {
    serde_json::to_vec(&ReservationWire {
        schema_version: TRIAL_SCHEMA_VERSION,
        trial_id: encode_hex(spec.id().digest().bytes()),
        spec: TrialSpecWire::from(spec),
    })
    .map_err(|_| ExperimentError::Encoding)
}

pub(super) fn decode_reservation(bytes: &[u8]) -> Result<TrialSpec, ExperimentError> {
    let wire: ReservationWire =
        serde_json::from_slice(bytes).map_err(|_| ExperimentError::CorruptRecord)?;
    if wire.schema_version != TRIAL_SCHEMA_VERSION {
        return Err(ExperimentError::CorruptRecord);
    }
    let expected = TrialId(decode_hex(&wire.trial_id)?);
    let spec = TrialSpec::try_from(wire.spec)?;
    if spec.id() != expected {
        return Err(ExperimentError::CorruptRecord);
    }
    Ok(spec)
}

pub(super) fn encode_terminal(
    id: TrialId,
    status: &TrialStatus,
) -> Result<Vec<u8>, ExperimentError> {
    let wire = match status {
        TrialStatus::Reserved => return Err(ExperimentError::InvalidCompletion),
        TrialStatus::Completed(value) => TerminalWire {
            schema_version: TRIAL_SCHEMA_VERSION,
            trial_id: encode_hex(id.digest().bytes()),
            status: TerminalStatusWire::Completed,
            completed: Some(CompletedWire {
                result_digest: encode_hex(value.result_digest.bytes()),
                artifact_reference: value.artifact.reference.to_string(),
                artifact_digest: encode_hex(value.artifact.digest.bytes()),
                artifact_bytes: value.artifact.byte_count,
                metrics: value
                    .metrics
                    .iter()
                    .map(|metric| MetricWire {
                        name: metric.name.as_str().to_owned(),
                        value: metric.value,
                    })
                    .collect(),
                probability_of_backtest_overfitting: value
                    .probability_of_backtest_overfitting
                    .probability,
                probability_fold_count: value.probability_of_backtest_overfitting.folds,
                deflated_performance_probability: value.deflated_performance.probability,
                expected_maximum_sharpe: value.deflated_performance.expected_maximum_sharpe,
                selected: value.selected,
            }),
            failed: None,
        },
        TrialStatus::Failed(value) => TerminalWire {
            schema_version: TRIAL_SCHEMA_VERSION,
            trial_id: encode_hex(id.digest().bytes()),
            status: TerminalStatusWire::Failed,
            completed: None,
            failed: Some(FailedWire {
                code: value.code.as_str().to_owned(),
                evidence_digest: encode_hex(value.evidence_digest.bytes()),
            }),
        },
    };
    serde_json::to_vec(&wire).map_err(|_| ExperimentError::Encoding)
}

pub(super) fn decode_terminal(
    bytes: &[u8],
    expected_id: TrialId,
    limits: ExperimentLimits,
) -> Result<TrialStatus, ExperimentError> {
    let wire: TerminalWire =
        serde_json::from_slice(bytes).map_err(|_| ExperimentError::CorruptRecord)?;
    require_terminal_header(wire.schema_version, &wire.trial_id, expected_id)?;
    match (wire.status, wire.completed, wire.failed) {
        (
            TerminalStatusWire::Completed,
            Some(CompletedWire {
                result_digest,
                artifact_reference,
                artifact_digest,
                artifact_bytes,
                metrics,
                probability_of_backtest_overfitting,
                probability_fold_count,
                deflated_performance_probability,
                expected_maximum_sharpe,
                selected,
            }),
            None,
        ) => {
            if artifact_reference.is_empty()
                || artifact_bytes == 0
                || !probability_of_backtest_overfitting.is_finite()
                || !(0.0..=1.0).contains(&probability_of_backtest_overfitting)
                || probability_fold_count < 2
                || !deflated_performance_probability.is_finite()
                || !(0.0..=1.0).contains(&deflated_performance_probability)
                || !expected_maximum_sharpe.is_finite()
            {
                return Err(ExperimentError::CorruptRecord);
            }
            let metrics = metrics
                .into_iter()
                .map(|metric| {
                    TrialMetric::try_new(parse_identifier(metric.name)?, metric.value)
                        .map_err(|_| ExperimentError::CorruptRecord)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let completion = TrialCompletion::try_new(
                TrialCompletionInput {
                    result_digest: decode_hex(&result_digest)?,
                    artifact: BacktestArtifact {
                        reference: artifact_reference.into_boxed_str(),
                        digest: decode_hex(&artifact_digest)?,
                        byte_count: artifact_bytes,
                    },
                    metrics,
                    probability_of_backtest_overfitting: BacktestOverfittingDiagnostic {
                        probability: probability_of_backtest_overfitting,
                        folds: probability_fold_count,
                    },
                    deflated_performance: DeflatedPerformanceDiagnostic {
                        probability: deflated_performance_probability,
                        expected_maximum_sharpe,
                    },
                    selected,
                },
                limits,
            )
            .map_err(|_| ExperimentError::CorruptRecord)?;
            Ok(TrialStatus::Completed(completion))
        }
        (
            TerminalStatusWire::Failed,
            None,
            Some(FailedWire {
                code,
                evidence_digest,
            }),
        ) => Ok(TrialStatus::Failed(
            TrialFailure::try_new(parse_identifier(code)?, decode_hex(&evidence_digest)?)
                .map_err(|_| ExperimentError::CorruptRecord)?,
        )),
        _ => Err(ExperimentError::CorruptRecord),
    }
}

fn require_terminal_header(
    schema_version: u16,
    trial_id: &str,
    expected_id: TrialId,
) -> Result<(), ExperimentError> {
    if schema_version != TRIAL_SCHEMA_VERSION || TrialId(decode_hex(trial_id)?) != expected_id {
        return Err(ExperimentError::CorruptRecord);
    }
    Ok(())
}

impl From<&TrialSpec> for TrialSpecWire {
    fn from(value: &TrialSpec) -> Self {
        Self {
            dataset_identity: encode_hex(value.dataset_identity.bytes()),
            object_graph_digest: encode_hex(value.object_graph_digest.bytes()),
            execution_assumption_digest: encode_hex(value.execution_assumption_digest.bytes()),
            model: value.model.as_ref().map(BindingWire::from),
            strategy: BindingWire::from(&value.strategy),
            code: BindingWire::from(&value.code),
            configuration_digest: encode_hex(value.configuration_digest.bytes()),
            seed: value.seed,
            parameters: value
                .parameters
                .iter()
                .map(|parameter| PairWire {
                    name: parameter.name.as_str().to_owned(),
                    value: parameter.value.as_str().to_owned(),
                })
                .collect(),
            search_space: value
                .search_space
                .iter()
                .map(|dimension| SearchWire {
                    name: dimension.name.as_str().to_owned(),
                    candidates: dimension
                        .candidates
                        .iter()
                        .map(|candidate| candidate.as_str().to_owned())
                        .collect(),
                })
                .collect(),
            selection_criterion: value.selection_criterion.as_str().to_owned(),
        }
    }
}

impl From<&TrialComponentBinding> for BindingWire {
    fn from(value: &TrialComponentBinding) -> Self {
        Self {
            name: value.name.as_str().to_owned(),
            digest: encode_hex(value.digest.bytes()),
        }
    }
}

impl TryFrom<TrialSpecWire> for TrialSpec {
    type Error = ExperimentError;

    fn try_from(value: TrialSpecWire) -> Result<Self, Self::Error> {
        TrialSpec::try_new(TrialSpecInput {
            dataset_identity: decode_hex(&value.dataset_identity)?,
            object_graph_digest: decode_hex(&value.object_graph_digest)?,
            execution_assumption_digest: decode_hex(&value.execution_assumption_digest)?,
            model: value
                .model
                .map(TrialComponentBinding::try_from)
                .transpose()?,
            strategy: TrialComponentBinding::try_from(value.strategy)?,
            code: TrialComponentBinding::try_from(value.code)?,
            configuration_digest: decode_hex(&value.configuration_digest)?,
            seed: value.seed,
            parameters: value
                .parameters
                .into_iter()
                .map(|parameter| {
                    Ok(TrialParameter::new(
                        parse_identifier(parameter.name)?,
                        parse_identifier(parameter.value)?,
                    ))
                })
                .collect::<Result<Vec<_>, ExperimentError>>()?,
            search_space: value
                .search_space
                .into_iter()
                .map(|dimension| {
                    TrialSearchDimension::try_new(
                        parse_identifier(dimension.name)?,
                        dimension
                            .candidates
                            .into_iter()
                            .map(parse_identifier)
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            selection_criterion: parse_identifier(value.selection_criterion)?,
        })
        .map_err(|_| ExperimentError::CorruptRecord)
    }
}

impl TryFrom<BindingWire> for TrialComponentBinding {
    type Error = ExperimentError;

    fn try_from(value: BindingWire) -> Result<Self, Self::Error> {
        Self::try_new(parse_identifier(value.name)?, decode_hex(&value.digest)?)
            .map_err(|_| ExperimentError::CorruptRecord)
    }
}

fn parse_identifier(value: String) -> Result<SourceIdentifier, ExperimentError> {
    SourceIdentifier::try_from(value).map_err(|_| ExperimentError::CorruptRecord)
}

pub(super) fn digest_bytes(bytes: &[u8]) -> Sha256Digest {
    Sha256Digest::new(Sha256::digest(bytes).into())
}

pub(super) fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn decode_hex(value: &str) -> Result<Sha256Digest, ExperimentError> {
    if value.len() != 64 {
        return Err(ExperimentError::CorruptRecord);
    }
    let mut bytes = [0_u8; 32];
    for (index, slot) in bytes.iter_mut().enumerate() {
        let start = index.checked_mul(2).ok_or(ExperimentError::CorruptRecord)?;
        let pair = value
            .get(start..start + 2)
            .ok_or(ExperimentError::CorruptRecord)?;
        *slot = u8::from_str_radix(pair, 16).map_err(|_| ExperimentError::CorruptRecord)?;
    }
    Ok(Sha256Digest::new(bytes))
}
