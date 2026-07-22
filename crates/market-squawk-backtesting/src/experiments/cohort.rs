//! Append-only cohort diagnostics and selection evidence bound to completed trial results.

use std::collections::BTreeSet;

use market_squawk_data::Sha256Digest;
use market_squawk_domain::SourceIdentifier;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::ExperimentError;
use super::diagnostics::{BacktestOverfittingDiagnostic, DeflatedPerformanceDiagnostic};
use super::model::{TrialComponentBinding, TrialDatasetPartition, TrialId};
use super::wire::{decode_hex, encode_hex};

/// Exact in-sample/out-of-sample completed-trial pair for one fold candidate.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BacktestCohortCandidate {
    in_sample: TrialId,
    out_of_sample: TrialId,
}

impl BacktestCohortCandidate {
    /// Binds two independently completed trial results used as paired fold scores.
    #[must_use]
    pub const fn new(in_sample: TrialId, out_of_sample: TrialId) -> Self {
        Self {
            in_sample,
            out_of_sample,
        }
    }

    /// Returns the in-sample trial identity.
    #[must_use]
    pub const fn in_sample(self) -> TrialId {
        self.in_sample
    }

    /// Returns the out-of-sample trial identity.
    #[must_use]
    pub const fn out_of_sample(self) -> TrialId {
        self.out_of_sample
    }
}

/// One canonical candidate set used by the PBO evaluator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BacktestCohortFold {
    candidates: Box<[BacktestCohortCandidate]>,
}

impl BacktestCohortFold {
    /// Requires at least two duplicate-free candidate pairs.
    pub fn try_new(mut candidates: Vec<BacktestCohortCandidate>) -> Result<Self, ExperimentError> {
        candidates.sort_unstable();
        if candidates.len() < 2 || candidates.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        Ok(Self {
            candidates: candidates.into_boxed_slice(),
        })
    }

    /// Returns exact paired trial identities in canonical order.
    #[must_use]
    pub fn candidates(&self) -> &[BacktestCohortCandidate] {
        &self.candidates
    }
}

/// Bounded cohort design; it contains identities and criterion but no caller-authored scores.
#[derive(Clone, Debug)]
pub struct BacktestCohortPlan {
    folds: Box<[BacktestCohortFold]>,
    selection_candidates: Box<[TrialId]>,
    selection_criterion: SourceIdentifier,
}

impl BacktestCohortPlan {
    /// Canonicalizes the candidate set and rejects an underidentified cohort.
    pub fn try_new(
        folds: Vec<BacktestCohortFold>,
        mut selection_candidates: Vec<TrialId>,
        selection_criterion: SourceIdentifier,
    ) -> Result<Self, ExperimentError> {
        selection_candidates.sort_unstable();
        if folds.len() < 2
            || selection_candidates.len() < 2
            || selection_candidates
                .windows(2)
                .any(|pair| pair[0] == pair[1])
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        let members = folds
            .iter()
            .flat_map(|fold| fold.candidates.iter())
            .flat_map(|candidate| [candidate.in_sample, candidate.out_of_sample])
            .collect::<BTreeSet<_>>();
        if selection_candidates
            .iter()
            .any(|candidate| !members.contains(candidate))
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        Ok(Self {
            folds: folds.into_boxed_slice(),
            selection_candidates: selection_candidates.into_boxed_slice(),
            selection_criterion,
        })
    }

    pub(crate) fn folds(&self) -> &[BacktestCohortFold] {
        &self.folds
    }

    pub(crate) fn selection_candidates(&self) -> &[TrialId] {
        &self.selection_candidates
    }

    pub(crate) const fn selection_criterion(&self) -> &SourceIdentifier {
        &self.selection_criterion
    }
}

/// Exact completed result admitted as a cohort member.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CohortMemberBinding {
    trial_id: TrialId,
    result_digest: Sha256Digest,
    dataset_identity: Sha256Digest,
    dataset_partition: TrialDatasetPartition,
    parameter_digest: Sha256Digest,
}

impl CohortMemberBinding {
    pub(crate) const fn new(
        trial_id: TrialId,
        result_digest: Sha256Digest,
        dataset_identity: Sha256Digest,
        dataset_partition: TrialDatasetPartition,
        parameter_digest: Sha256Digest,
    ) -> Self {
        Self {
            trial_id,
            result_digest,
            dataset_identity,
            dataset_partition,
            parameter_digest,
        }
    }

    /// Returns the immutable trial identity.
    #[must_use]
    pub const fn trial_id(self) -> TrialId {
        self.trial_id
    }

    /// Returns the exact completed backtest result identity.
    #[must_use]
    pub const fn result_digest(self) -> Sha256Digest {
        self.result_digest
    }

    /// Returns the exact partition dataset content identity.
    #[must_use]
    pub const fn dataset_identity(self) -> Sha256Digest {
        self.dataset_identity
    }

    /// Returns the exact non-overlap interval used by this member.
    #[must_use]
    pub const fn dataset_partition(self) -> TrialDatasetPartition {
        self.dataset_partition
    }

    /// Returns the candidate parameter-vector identity.
    #[must_use]
    pub const fn parameter_digest(self) -> Sha256Digest {
        self.parameter_digest
    }
}

/// Content identity for one append-only cohort decision record.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BacktestCohortEvaluationId(Sha256Digest);

impl BacktestCohortEvaluationId {
    /// Returns the exact canonical SHA-256 identity.
    #[must_use]
    pub const fn digest(self) -> Sha256Digest {
        self.0
    }
}

/// Authoritative post-run PBO, deflated-performance, and selected-trial evidence.
#[derive(Clone, Debug, PartialEq)]
pub struct BacktestCohortEvaluation {
    id: BacktestCohortEvaluationId,
    evaluator: TrialComponentBinding,
    experiment_design_digest: Sha256Digest,
    selection_criterion: SourceIdentifier,
    members: Box<[CohortMemberBinding]>,
    folds: Box<[BacktestCohortFold]>,
    selection_candidates: Box<[TrialId]>,
    probability_of_backtest_overfitting: BacktestOverfittingDiagnostic,
    deflated_performance: DeflatedPerformanceDiagnostic,
    selected: CohortMemberBinding,
}

pub(crate) struct CohortEvaluationInput {
    pub evaluator: TrialComponentBinding,
    pub experiment_design_digest: Sha256Digest,
    pub selection_criterion: SourceIdentifier,
    pub members: Vec<CohortMemberBinding>,
    pub folds: Vec<BacktestCohortFold>,
    pub selection_candidates: Vec<TrialId>,
    pub probability_of_backtest_overfitting: BacktestOverfittingDiagnostic,
    pub deflated_performance: DeflatedPerformanceDiagnostic,
    pub selected: CohortMemberBinding,
}

impl BacktestCohortEvaluation {
    pub(crate) fn try_new(mut input: CohortEvaluationInput) -> Result<Self, ExperimentError> {
        input.members.sort_unstable_by_key(|member| member.trial_id);
        if input.members.len() < 2
            || input
                .members
                .windows(2)
                .any(|pair| pair[0].trial_id == pair[1].trial_id)
            || !input.members.contains(&input.selected)
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        input.selection_candidates.sort_unstable();
        let mut value = Self {
            id: BacktestCohortEvaluationId(Sha256Digest::new([0; 32])),
            evaluator: input.evaluator,
            experiment_design_digest: input.experiment_design_digest,
            selection_criterion: input.selection_criterion,
            members: input.members.into_boxed_slice(),
            folds: input.folds.into_boxed_slice(),
            selection_candidates: input.selection_candidates.into_boxed_slice(),
            probability_of_backtest_overfitting: input.probability_of_backtest_overfitting,
            deflated_performance: input.deflated_performance,
            selected: input.selected,
        };
        value.id = BacktestCohortEvaluationId(evaluation_digest(&value)?);
        Ok(value)
    }

    /// Returns the immutable cohort decision identity.
    #[must_use]
    pub const fn id(&self) -> BacktestCohortEvaluationId {
        self.id
    }

    /// Returns exact member and completed-result bindings.
    #[must_use]
    pub fn members(&self) -> &[CohortMemberBinding] {
        &self.members
    }

    /// Returns the code-selected winning completed result.
    #[must_use]
    pub const fn selected(&self) -> CohortMemberBinding {
        self.selected
    }

    /// Returns the PBO diagnostic computed from member-owned metrics.
    #[must_use]
    pub const fn probability_of_backtest_overfitting(&self) -> BacktestOverfittingDiagnostic {
        self.probability_of_backtest_overfitting
    }

    /// Returns the deflated-performance diagnostic computed from member-owned metrics.
    #[must_use]
    pub const fn deflated_performance(&self) -> DeflatedPerformanceDiagnostic {
        self.deflated_performance
    }
}

fn evaluation_digest(value: &BacktestCohortEvaluation) -> Result<Sha256Digest, ExperimentError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/backtest-cohort-evaluation/v1");
    hash_bytes(&mut hash, value.evaluator.name().as_str().as_bytes())?;
    hash.update(value.evaluator.digest().bytes());
    hash.update(value.experiment_design_digest.bytes());
    hash_bytes(&mut hash, value.selection_criterion.as_str().as_bytes())?;
    hash_length(&mut hash, value.members.len())?;
    for member in &value.members {
        hash.update(member.trial_id.digest().bytes());
        hash.update(member.result_digest.bytes());
        hash.update(member.dataset_identity.bytes());
        hash.update(
            member
                .dataset_partition
                .starts_at()
                .unix_nanos()
                .to_be_bytes(),
        );
        hash.update(
            member
                .dataset_partition
                .ends_at()
                .unix_nanos()
                .to_be_bytes(),
        );
        hash.update(member.parameter_digest.bytes());
    }
    hash_length(&mut hash, value.folds.len())?;
    for fold in &value.folds {
        hash_length(&mut hash, fold.candidates.len())?;
        for candidate in &fold.candidates {
            hash.update(candidate.in_sample.digest().bytes());
            hash.update(candidate.out_of_sample.digest().bytes());
        }
    }
    hash_length(&mut hash, value.selection_candidates.len())?;
    for trial in &value.selection_candidates {
        hash.update(trial.digest().bytes());
    }
    hash.update(
        value
            .probability_of_backtest_overfitting
            .probability
            .to_bits()
            .to_be_bytes(),
    );
    hash_length(&mut hash, value.probability_of_backtest_overfitting.folds)?;
    hash.update(
        value
            .deflated_performance
            .probability
            .to_bits()
            .to_be_bytes(),
    );
    hash.update(
        value
            .deflated_performance
            .expected_maximum_sharpe
            .to_bits()
            .to_be_bytes(),
    );
    hash.update(value.selected.trial_id.digest().bytes());
    hash.update(value.selected.result_digest.bytes());
    Ok(Sha256Digest::new(hash.finalize().into()))
}

fn hash_bytes(hash: &mut Sha256, bytes: &[u8]) -> Result<(), ExperimentError> {
    hash_length(hash, bytes.len())?;
    hash.update(bytes);
    Ok(())
}

fn hash_length(hash: &mut Sha256, length: usize) -> Result<(), ExperimentError> {
    hash.update(
        u64::try_from(length)
            .map_err(|_| ExperimentError::Encoding)?
            .to_be_bytes(),
    );
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EvaluationWire {
    schema_version: u16,
    evaluation_id: String,
    evaluator_name: String,
    evaluator_digest: String,
    experiment_design_digest: String,
    selection_criterion: String,
    members: Vec<MemberWire>,
    folds: Vec<Vec<CandidateWire>>,
    selection_candidates: Vec<String>,
    probability_of_backtest_overfitting: f64,
    probability_fold_count: usize,
    deflated_performance_probability: f64,
    expected_maximum_sharpe: f64,
    selected_trial_id: String,
    selected_result_digest: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MemberWire {
    trial_id: String,
    result_digest: String,
    dataset_identity: String,
    dataset_partition_start: i64,
    dataset_partition_end: i64,
    parameter_digest: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CandidateWire {
    in_sample: String,
    out_of_sample: String,
}

pub(super) fn encode_evaluation(
    value: &BacktestCohortEvaluation,
) -> Result<Vec<u8>, ExperimentError> {
    serde_json::to_vec(&EvaluationWire {
        schema_version: 1,
        evaluation_id: encode_hex(value.id.digest().bytes()),
        evaluator_name: value.evaluator.name().as_str().to_owned(),
        evaluator_digest: encode_hex(value.evaluator.digest().bytes()),
        experiment_design_digest: encode_hex(value.experiment_design_digest.bytes()),
        selection_criterion: value.selection_criterion.as_str().to_owned(),
        members: value
            .members
            .iter()
            .map(|member| MemberWire {
                trial_id: encode_hex(member.trial_id.digest().bytes()),
                result_digest: encode_hex(member.result_digest.bytes()),
                dataset_identity: encode_hex(member.dataset_identity.bytes()),
                dataset_partition_start: member.dataset_partition.starts_at().unix_nanos(),
                dataset_partition_end: member.dataset_partition.ends_at().unix_nanos(),
                parameter_digest: encode_hex(member.parameter_digest.bytes()),
            })
            .collect(),
        folds: value
            .folds
            .iter()
            .map(|fold| {
                fold.candidates
                    .iter()
                    .map(|candidate| CandidateWire {
                        in_sample: encode_hex(candidate.in_sample.digest().bytes()),
                        out_of_sample: encode_hex(candidate.out_of_sample.digest().bytes()),
                    })
                    .collect()
            })
            .collect(),
        selection_candidates: value
            .selection_candidates
            .iter()
            .map(|trial| encode_hex(trial.digest().bytes()))
            .collect(),
        probability_of_backtest_overfitting: value.probability_of_backtest_overfitting.probability,
        probability_fold_count: value.probability_of_backtest_overfitting.folds,
        deflated_performance_probability: value.deflated_performance.probability,
        expected_maximum_sharpe: value.deflated_performance.expected_maximum_sharpe,
        selected_trial_id: encode_hex(value.selected.trial_id.digest().bytes()),
        selected_result_digest: encode_hex(value.selected.result_digest.bytes()),
    })
    .map_err(|_| ExperimentError::Encoding)
}

pub(super) fn decode_evaluation(
    bytes: &[u8],
    expected: BacktestCohortEvaluationId,
) -> Result<BacktestCohortEvaluation, ExperimentError> {
    let wire: EvaluationWire =
        serde_json::from_slice(bytes).map_err(|_| ExperimentError::CorruptRecord)?;
    if wire.schema_version != 1
        || decode_hex(&wire.evaluation_id)? != expected.digest()
        || !wire.probability_of_backtest_overfitting.is_finite()
        || !(0.0..=1.0).contains(&wire.probability_of_backtest_overfitting)
        || wire.probability_fold_count < 2
        || !wire.deflated_performance_probability.is_finite()
        || !(0.0..=1.0).contains(&wire.deflated_performance_probability)
        || !wire.expected_maximum_sharpe.is_finite()
    {
        return Err(ExperimentError::CorruptRecord);
    }
    let members = wire
        .members
        .into_iter()
        .map(|member| {
            Ok(CohortMemberBinding::new(
                TrialId(decode_hex(&member.trial_id)?),
                decode_hex(&member.result_digest)?,
                decode_hex(&member.dataset_identity)?,
                TrialDatasetPartition::try_new(
                    market_squawk_domain::Timestamp::from_unix_nanos(
                        member.dataset_partition_start,
                    ),
                    market_squawk_domain::Timestamp::from_unix_nanos(member.dataset_partition_end),
                )
                .map_err(|_| ExperimentError::CorruptRecord)?,
                decode_hex(&member.parameter_digest)?,
            ))
        })
        .collect::<Result<Vec<_>, ExperimentError>>()?;
    let selected_id = TrialId(decode_hex(&wire.selected_trial_id)?);
    let selected_result_digest = decode_hex(&wire.selected_result_digest)?;
    let selected = members
        .iter()
        .find(|member| {
            member.trial_id == selected_id && member.result_digest == selected_result_digest
        })
        .copied()
        .ok_or(ExperimentError::CorruptRecord)?;
    let value = BacktestCohortEvaluation::try_new(CohortEvaluationInput {
        evaluator: TrialComponentBinding::try_new(
            SourceIdentifier::try_from(wire.evaluator_name)
                .map_err(|_| ExperimentError::CorruptRecord)?,
            decode_hex(&wire.evaluator_digest)?,
        )?,
        experiment_design_digest: decode_hex(&wire.experiment_design_digest)?,
        selection_criterion: SourceIdentifier::try_from(wire.selection_criterion)
            .map_err(|_| ExperimentError::CorruptRecord)?,
        members,
        folds: wire
            .folds
            .into_iter()
            .map(|fold| {
                BacktestCohortFold::try_new(
                    fold.into_iter()
                        .map(|candidate| {
                            Ok(BacktestCohortCandidate::new(
                                TrialId(decode_hex(&candidate.in_sample)?),
                                TrialId(decode_hex(&candidate.out_of_sample)?),
                            ))
                        })
                        .collect::<Result<Vec<_>, ExperimentError>>()?,
                )
            })
            .collect::<Result<Vec<_>, ExperimentError>>()?,
        selection_candidates: wire
            .selection_candidates
            .into_iter()
            .map(|trial| decode_hex(&trial).map(TrialId))
            .collect::<Result<Vec<_>, ExperimentError>>()?,
        probability_of_backtest_overfitting: BacktestOverfittingDiagnostic {
            probability: wire.probability_of_backtest_overfitting,
            folds: wire.probability_fold_count,
        },
        deflated_performance: DeflatedPerformanceDiagnostic {
            probability: wire.deflated_performance_probability,
            expected_maximum_sharpe: wire.expected_maximum_sharpe,
        },
        selected,
    })?;
    if value.id != expected {
        return Err(ExperimentError::CorruptRecord);
    }
    Ok(value)
}
