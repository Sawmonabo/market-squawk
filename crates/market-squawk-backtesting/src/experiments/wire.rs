//! Canonical bounded JSON encoding for immutable trial records.

use market_squawk_data::Sha256Digest;
use market_squawk_domain::SourceIdentifier;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::ExperimentError;
use super::model::{
    BacktestArtifact, ExperimentLimits, TrialCompletion, TrialCompletionInput,
    TrialComponentBinding, TrialDatasetPartition, TrialFailure, TrialId, TrialMetric,
    TrialParameter, TrialSearchDimension, TrialSpec, TrialSpecInput, TrialStatus,
};

const RESERVATION_SCHEMA_VERSION: u16 = 3;
const TERMINAL_SCHEMA_VERSION: u16 = 3;

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
    run_input_digest: String,
    cohort_authority_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cohort_universe_digest: Option<String>,
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
    dataset_partition_start: i64,
    dataset_partition_end: i64,
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
        schema_version: RESERVATION_SCHEMA_VERSION,
        trial_id: encode_hex(spec.id().digest().bytes()),
        spec: TrialSpecWire::from(spec),
    })
    .map_err(|_| ExperimentError::Encoding)
}

pub(super) fn decode_reservation(bytes: &[u8]) -> Result<TrialSpec, ExperimentError> {
    let wire: ReservationWire =
        serde_json::from_slice(bytes).map_err(|_| ExperimentError::CorruptRecord)?;
    if wire.schema_version != RESERVATION_SCHEMA_VERSION {
        return Err(ExperimentError::CorruptRecord);
    }
    let expected = TrialId(decode_hex(&wire.trial_id)?);
    let spec = wire.spec.try_into_spec()?;
    if spec.id() != expected {
        return Err(ExperimentError::CorruptRecord);
    }
    Ok(spec)
}

pub(super) struct DecodedTerminal {
    pub status: TrialStatus,
}

pub(super) fn encode_terminal(
    id: TrialId,
    status: &TrialStatus,
) -> Result<Vec<u8>, ExperimentError> {
    let wire = match status {
        TrialStatus::Reserved => return Err(ExperimentError::InvalidCompletion),
        TrialStatus::Completed(value) => TerminalWire {
            schema_version: TERMINAL_SCHEMA_VERSION,
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
                dataset_partition_start: value.dataset_partition.starts_at().unix_nanos(),
                dataset_partition_end: value.dataset_partition.ends_at().unix_nanos(),
            }),
            failed: None,
        },
        TrialStatus::Failed(value) => TerminalWire {
            schema_version: TERMINAL_SCHEMA_VERSION,
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
) -> Result<DecodedTerminal, ExperimentError> {
    let wire: TerminalWire =
        serde_json::from_slice(bytes).map_err(|_| ExperimentError::CorruptRecord)?;
    let schema_version = wire.schema_version;
    require_terminal_header(schema_version, &wire.trial_id, expected_id)?;
    match (wire.status, wire.completed, wire.failed) {
        (
            TerminalStatusWire::Completed,
            Some(CompletedWire {
                result_digest,
                artifact_reference,
                artifact_digest,
                artifact_bytes,
                metrics,
                dataset_partition_start,
                dataset_partition_end,
            }),
            None,
        ) => {
            if artifact_reference.is_empty()
                || artifact_bytes == 0
                || dataset_partition_start >= dataset_partition_end
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
                    dataset_partition: TrialDatasetPartition::try_new(
                        market_squawk_domain::Timestamp::from_unix_nanos(dataset_partition_start),
                        market_squawk_domain::Timestamp::from_unix_nanos(dataset_partition_end),
                    )
                    .map_err(|_| ExperimentError::CorruptRecord)?,
                },
                limits,
            )
            .map_err(|_| ExperimentError::CorruptRecord)?;
            Ok(DecodedTerminal {
                status: TrialStatus::Completed(completion),
            })
        }
        (
            TerminalStatusWire::Failed,
            None,
            Some(FailedWire {
                code,
                evidence_digest,
            }),
        ) => Ok(DecodedTerminal {
            status: TrialStatus::Failed(
                TrialFailure::try_new(parse_identifier(code)?, decode_hex(&evidence_digest)?)
                    .map_err(|_| ExperimentError::CorruptRecord)?,
            ),
        }),
        _ => Err(ExperimentError::CorruptRecord),
    }
}

fn require_terminal_header(
    schema_version: u16,
    trial_id: &str,
    expected_id: TrialId,
) -> Result<(), ExperimentError> {
    if schema_version != TERMINAL_SCHEMA_VERSION || TrialId(decode_hex(trial_id)?) != expected_id {
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
            run_input_digest: encode_hex(value.run_input_digest.bytes()),
            cohort_authority_digest: encode_hex(value.cohort_authority_digest.bytes()),
            cohort_universe_digest: value
                .cohort_universe_digest
                .map(|digest| encode_hex(digest.bytes())),
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

impl TrialSpecWire {
    fn try_into_spec(self) -> Result<TrialSpec, ExperimentError> {
        let cohort_universe_digest = self
            .cohort_universe_digest
            .as_deref()
            .map(decode_hex)
            .transpose()?;
        TrialSpec::try_new(TrialSpecInput {
            dataset_identity: decode_hex(&self.dataset_identity)?,
            object_graph_digest: decode_hex(&self.object_graph_digest)?,
            execution_assumption_digest: decode_hex(&self.execution_assumption_digest)?,
            run_input_digest: decode_hex(&self.run_input_digest)?,
            cohort_authority_digest: decode_hex(&self.cohort_authority_digest)?,
            cohort_universe_digest,
            model: self
                .model
                .map(TrialComponentBinding::try_from)
                .transpose()?,
            strategy: TrialComponentBinding::try_from(self.strategy)?,
            code: TrialComponentBinding::try_from(self.code)?,
            configuration_digest: decode_hex(&self.configuration_digest)?,
            seed: self.seed,
            parameters: self
                .parameters
                .into_iter()
                .map(|parameter| {
                    Ok(TrialParameter::new(
                        parse_identifier(parameter.name)?,
                        parse_identifier(parameter.value)?,
                    ))
                })
                .collect::<Result<Vec<_>, ExperimentError>>()?,
            search_space: self
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
            selection_criterion: parse_identifier(self.selection_criterion)?,
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

pub(super) fn decode_hex(value: &str) -> Result<Sha256Digest, ExperimentError> {
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
