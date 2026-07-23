//! Append-only cohort diagnostics and selection evidence bound to completed trial results.

use std::collections::BTreeSet;

use market_squawk_data::Sha256Digest;
use market_squawk_domain::SourceIdentifier;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::ExperimentError;
use super::diagnostics::{BacktestOverfittingDiagnostic, DeflatedPerformanceDiagnostic};
use super::model::{
    TrialComponentBinding, TrialDatasetPartition, TrialId, TrialParameter, require_digest,
};
use super::wire::{decode_hex, encode_hex};

const HARD_MAX_COHORT_FOLDS: usize = 1_024;
const HARD_MAX_GENERATOR_PARAMETERS: usize = 1_024;
/// Maximum candidates admitted in one cohort fold.
pub const MAX_COHORT_CANDIDATES_PER_FOLD: usize = 16_384;
/// Maximum candidates admitted for final selection.
pub const MAX_COHORT_SELECTION_CANDIDATES: usize = 16_384;
/// Maximum distinct trial records materialized for one cohort evaluation.
pub const MAX_COHORT_UNIQUE_MEMBERS: usize = 131_072;
/// Maximum fold-pair and selection trial references admitted by one cohort plan.
pub const MAX_COHORT_MEMBER_REFERENCES: usize = 147_456;
const MAX_COHORT_FOLD_CANDIDATES: usize = 65_536;
type PartitionSortKey = ([u8; 32], [u8; 32], i64, i64);
type FoldPartitionSortKey = (PartitionSortKey, PartitionSortKey);

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

/// Exact dataset generation and interval admitted as one generated cohort partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BacktestCohortPartition {
    dataset_identity: Sha256Digest,
    object_graph_digest: Sha256Digest,
    interval: TrialDatasetPartition,
}

impl BacktestCohortPartition {
    /// Binds a generated partition to immutable data and object-graph identities.
    pub fn try_new(
        dataset_identity: Sha256Digest,
        object_graph_digest: Sha256Digest,
        interval: TrialDatasetPartition,
    ) -> Result<Self, ExperimentError> {
        require_digest(dataset_identity)?;
        require_digest(object_graph_digest)?;
        Ok(Self {
            dataset_identity,
            object_graph_digest,
            interval,
        })
    }

    /// Returns the exact partition dataset identity.
    #[must_use]
    pub const fn dataset_identity(self) -> Sha256Digest {
        self.dataset_identity
    }

    /// Returns the partition's pinned object graph.
    #[must_use]
    pub const fn object_graph_digest(self) -> Sha256Digest {
        self.object_graph_digest
    }

    /// Returns the exact event-time interval.
    #[must_use]
    pub const fn interval(self) -> TrialDatasetPartition {
        self.interval
    }
}

/// One exact generated in-sample/out-of-sample partition pair.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BacktestCohortFoldPartition {
    in_sample: BacktestCohortPartition,
    out_of_sample: BacktestCohortPartition,
}

impl BacktestCohortFoldPartition {
    /// Requires disjoint, time-ordered, independently identified partitions.
    pub fn try_new(
        in_sample: BacktestCohortPartition,
        out_of_sample: BacktestCohortPartition,
    ) -> Result<Self, ExperimentError> {
        if in_sample.dataset_identity == out_of_sample.dataset_identity
            || in_sample.interval.ends_at() >= out_of_sample.interval.starts_at()
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        Ok(Self {
            in_sample,
            out_of_sample,
        })
    }

    /// Returns the generated in-sample partition.
    #[must_use]
    pub const fn in_sample(self) -> BacktestCohortPartition {
        self.in_sample
    }

    /// Returns the generated out-of-sample partition.
    #[must_use]
    pub const fn out_of_sample(self) -> BacktestCohortPartition {
        self.out_of_sample
    }
}

/// Pre-run canonical fold-generation authority shared by every cohort member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BacktestCohortUniverse {
    generator_version: SourceIdentifier,
    generation_parameters: Box<[TrialParameter]>,
    expected_candidate_count: usize,
    folds: Box<[BacktestCohortFoldPartition]>,
    selection_partition: BacktestCohortPartition,
    digest: Sha256Digest,
}

impl BacktestCohortUniverse {
    /// Canonicalizes generator parameters and the complete generated partition universe.
    pub fn try_new(
        generator_version: SourceIdentifier,
        mut generation_parameters: Vec<TrialParameter>,
        expected_candidate_count: usize,
        mut folds: Vec<BacktestCohortFoldPartition>,
        selection_partition: BacktestCohortPartition,
    ) -> Result<Self, ExperimentError> {
        if generation_parameters.is_empty()
            || generation_parameters.len() > HARD_MAX_GENERATOR_PARAMETERS
            || folds.len() < 2
            || folds.len() > HARD_MAX_COHORT_FOLDS
            || !(2..=MAX_COHORT_CANDIDATES_PER_FOLD).contains(&expected_candidate_count)
            || expected_candidate_count > MAX_COHORT_SELECTION_CANDIDATES
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        cohort_member_reference_count(folds.len(), expected_candidate_count)?;
        generation_parameters.sort_unstable();
        if generation_parameters
            .windows(2)
            .any(|pair| pair[0].name() == pair[1].name())
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        folds.sort_unstable_by_key(fold_partition_key);
        if folds.windows(2).any(|pair| pair[0] == pair[1])
            || !folds
                .iter()
                .any(|fold| fold.out_of_sample == selection_partition)
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        let mut value = Self {
            generator_version,
            generation_parameters: generation_parameters.into_boxed_slice(),
            expected_candidate_count,
            folds: folds.into_boxed_slice(),
            selection_partition,
            digest: Sha256Digest::new([0; 32]),
        };
        value.digest = universe_digest(&value)?;
        Ok(value)
    }

    /// Returns the canonical universe identity bound before any member run.
    #[must_use]
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }

    /// Returns the versioned generator implementation identifier.
    #[must_use]
    pub const fn generator_version(&self) -> &SourceIdentifier {
        &self.generator_version
    }

    /// Returns the canonical generator parameters.
    #[must_use]
    pub fn generation_parameters(&self) -> &[TrialParameter] {
        &self.generation_parameters
    }

    /// Returns the complete search-space cardinality required in every fold and selection set.
    #[must_use]
    pub const fn expected_candidate_count(&self) -> usize {
        self.expected_candidate_count
    }

    /// Returns every exact generated fold partition.
    #[must_use]
    pub fn folds(&self) -> &[BacktestCohortFoldPartition] {
        &self.folds
    }

    /// Returns the exact dataset partition used for final candidate selection.
    #[must_use]
    pub const fn selection_partition(&self) -> BacktestCohortPartition {
        self.selection_partition
    }
}

fn fold_partition_key(fold: &BacktestCohortFoldPartition) -> FoldPartitionSortKey {
    (
        partition_sort_key(fold.in_sample),
        partition_sort_key(fold.out_of_sample),
    )
}

fn partition_sort_key(partition: BacktestCohortPartition) -> PartitionSortKey {
    (
        partition.dataset_identity.bytes(),
        partition.object_graph_digest.bytes(),
        partition.interval.starts_at().unix_nanos(),
        partition.interval.ends_at().unix_nanos(),
    )
}

fn universe_digest(value: &BacktestCohortUniverse) -> Result<Sha256Digest, ExperimentError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/backtest-cohort-universe/v2");
    hash_bytes(&mut hash, value.generator_version.as_str().as_bytes())?;
    hash_length(&mut hash, value.generation_parameters.len())?;
    for parameter in &value.generation_parameters {
        hash_bytes(&mut hash, parameter.name().as_str().as_bytes())?;
        hash_bytes(&mut hash, parameter.value().as_str().as_bytes())?;
    }
    hash_length(&mut hash, value.expected_candidate_count)?;
    hash_length(&mut hash, value.folds.len())?;
    for fold in &value.folds {
        hash_partition(&mut hash, fold.in_sample);
        hash_partition(&mut hash, fold.out_of_sample);
    }
    hash_partition(&mut hash, value.selection_partition);
    Ok(Sha256Digest::new(hash.finalize().into()))
}

fn hash_partition(hash: &mut Sha256, partition: BacktestCohortPartition) {
    hash.update(partition.dataset_identity.bytes());
    hash.update(partition.object_graph_digest.bytes());
    hash.update(partition.interval.starts_at().unix_nanos().to_be_bytes());
    hash.update(partition.interval.ends_at().unix_nanos().to_be_bytes());
}

/// One canonical candidate set used by the PBO evaluator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BacktestCohortFold {
    candidates: Box<[BacktestCohortCandidate]>,
}

impl BacktestCohortFold {
    /// Requires at least two duplicate-free candidate pairs.
    pub fn try_new(mut candidates: Vec<BacktestCohortCandidate>) -> Result<Self, ExperimentError> {
        if !(2..=MAX_COHORT_CANDIDATES_PER_FOLD).contains(&candidates.len()) {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        candidates.sort_unstable();
        if candidates.windows(2).any(|pair| pair[0] == pair[1]) {
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
    universe: BacktestCohortUniverse,
    folds: Box<[BacktestCohortFold]>,
    selection_candidates: Box<[TrialId]>,
    member_ids: Box<[TrialId]>,
    member_reference_count: usize,
    selection_criterion: SourceIdentifier,
}

impl BacktestCohortPlan {
    /// Canonicalizes the candidate set and rejects an underidentified cohort.
    pub fn try_new(
        universe: BacktestCohortUniverse,
        folds: Vec<BacktestCohortFold>,
        mut selection_candidates: Vec<TrialId>,
        selection_criterion: SourceIdentifier,
    ) -> Result<Self, ExperimentError> {
        let expected_candidate_count = universe.expected_candidate_count;
        if folds.len() != universe.folds.len()
            || folds.len() > HARD_MAX_COHORT_FOLDS
            || selection_candidates.len() != expected_candidate_count
            || selection_candidates.len() > MAX_COHORT_SELECTION_CANDIDATES
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        let member_reference_count =
            cohort_member_reference_count(folds.len(), expected_candidate_count)?;
        if folds
            .iter()
            .any(|fold| fold.candidates.len() != expected_candidate_count)
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        selection_candidates.sort_unstable();
        if selection_candidates
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        let mut members = BTreeSet::new();
        for candidate in folds.iter().flat_map(|fold| fold.candidates.iter()) {
            members.insert(candidate.in_sample);
            if members.len() > MAX_COHORT_UNIQUE_MEMBERS {
                return Err(ExperimentError::InvalidDiagnostic);
            }
            members.insert(candidate.out_of_sample);
            if members.len() > MAX_COHORT_UNIQUE_MEMBERS {
                return Err(ExperimentError::InvalidDiagnostic);
            }
        }
        if selection_candidates
            .iter()
            .any(|candidate| !members.contains(candidate))
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        let mut member_ids = Vec::new();
        member_ids
            .try_reserve_exact(members.len())
            .map_err(|_| ExperimentError::LimitExceeded)?;
        member_ids.extend(members);
        Ok(Self {
            universe,
            folds: folds.into_boxed_slice(),
            selection_candidates: selection_candidates.into_boxed_slice(),
            member_ids: member_ids.into_boxed_slice(),
            member_reference_count,
            selection_criterion,
        })
    }

    /// Returns the complete fold universe bound before member execution.
    #[must_use]
    pub const fn universe(&self) -> &BacktestCohortUniverse {
        &self.universe
    }

    pub(crate) fn folds(&self) -> &[BacktestCohortFold] {
        &self.folds
    }

    pub(crate) fn selection_candidates(&self) -> &[TrialId] {
        &self.selection_candidates
    }

    pub(crate) fn member_ids(&self) -> &[TrialId] {
        &self.member_ids
    }

    pub(crate) const fn member_reference_count(&self) -> usize {
        self.member_reference_count
    }

    pub(crate) const fn selection_criterion(&self) -> &SourceIdentifier {
        &self.selection_criterion
    }
}

fn cohort_member_reference_count(
    fold_count: usize,
    expected_candidate_count: usize,
) -> Result<usize, ExperimentError> {
    let fold_candidates = fold_count
        .checked_mul(expected_candidate_count)
        .filter(|count| *count <= MAX_COHORT_FOLD_CANDIDATES)
        .ok_or(ExperimentError::InvalidDiagnostic)?;
    fold_candidates
        .checked_mul(2)
        .and_then(|count| count.checked_add(expected_candidate_count))
        .filter(|count| *count <= MAX_COHORT_MEMBER_REFERENCES)
        .ok_or(ExperimentError::InvalidDiagnostic)
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
    cohort_universe_digest: Option<Sha256Digest>,
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
    pub cohort_universe_digest: Sha256Digest,
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
        require_digest(input.cohort_universe_digest)?;
        validate_cohort_materialization_counts(
            input.members.len(),
            input.folds.len(),
            input.selection_candidates.len(),
            input.folds.iter().map(|fold| fold.candidates.len()),
        )?;
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
        if input
            .selection_candidates
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        let mut value = Self {
            id: BacktestCohortEvaluationId(Sha256Digest::new([0; 32])),
            evaluator: input.evaluator,
            experiment_design_digest: input.experiment_design_digest,
            cohort_universe_digest: Some(input.cohort_universe_digest),
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

    fn try_new_legacy(mut input: CohortEvaluationInput) -> Result<Self, ExperimentError> {
        validate_cohort_materialization_counts(
            input.members.len(),
            input.folds.len(),
            input.selection_candidates.len(),
            input.folds.iter().map(|fold| fold.candidates.len()),
        )?;
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
        if input
            .selection_candidates
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        let mut value = Self {
            id: BacktestCohortEvaluationId(Sha256Digest::new([0; 32])),
            evaluator: input.evaluator,
            experiment_design_digest: input.experiment_design_digest,
            cohort_universe_digest: None,
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

    /// Returns the pre-run universe identity, or `None` for schema-v1 records.
    #[must_use]
    pub const fn cohort_universe_digest(&self) -> Option<Sha256Digest> {
        self.cohort_universe_digest
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

fn validate_cohort_materialization_counts(
    member_count: usize,
    fold_count: usize,
    selection_count: usize,
    fold_candidate_counts: impl IntoIterator<Item = usize>,
) -> Result<(), ExperimentError> {
    if !(2..=MAX_COHORT_UNIQUE_MEMBERS).contains(&member_count)
        || !(2..=HARD_MAX_COHORT_FOLDS).contains(&fold_count)
        || !(2..=MAX_COHORT_SELECTION_CANDIDATES).contains(&selection_count)
    {
        return Err(ExperimentError::InvalidDiagnostic);
    }
    let mut candidate_count = 0_usize;
    for count in fold_candidate_counts {
        if !(2..=MAX_COHORT_CANDIDATES_PER_FOLD).contains(&count) {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        candidate_count = candidate_count
            .checked_add(count)
            .filter(|value| *value <= MAX_COHORT_FOLD_CANDIDATES)
            .ok_or(ExperimentError::InvalidDiagnostic)?;
    }
    candidate_count
        .checked_mul(2)
        .and_then(|count| count.checked_add(selection_count))
        .filter(|count| *count <= MAX_COHORT_MEMBER_REFERENCES)
        .ok_or(ExperimentError::InvalidDiagnostic)?;
    Ok(())
}

fn evaluation_digest(value: &BacktestCohortEvaluation) -> Result<Sha256Digest, ExperimentError> {
    let mut hash = Sha256::new();
    match value.cohort_universe_digest {
        Some(digest) => {
            hash.update(b"market-squawk/backtest-cohort-evaluation/v2");
            hash.update(digest.bytes());
        }
        None => hash.update(b"market-squawk/backtest-cohort-evaluation/v1"),
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cohort_universe_digest: Option<String>,
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
        schema_version: 2,
        evaluation_id: encode_hex(value.id.digest().bytes()),
        evaluator_name: value.evaluator.name().as_str().to_owned(),
        evaluator_digest: encode_hex(value.evaluator.digest().bytes()),
        experiment_design_digest: encode_hex(value.experiment_design_digest.bytes()),
        cohort_universe_digest: value
            .cohort_universe_digest
            .map(|digest| encode_hex(digest.bytes())),
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
    if !matches!(wire.schema_version, 1 | 2)
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
    validate_cohort_materialization_counts(
        wire.members.len(),
        wire.folds.len(),
        wire.selection_candidates.len(),
        wire.folds.iter().map(Vec::len),
    )
    .map_err(|_| ExperimentError::CorruptRecord)?;
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
    let universe_digest = wire
        .cohort_universe_digest
        .as_deref()
        .map(decode_hex)
        .transpose()?;
    if (wire.schema_version == 1) != universe_digest.is_none() {
        return Err(ExperimentError::CorruptRecord);
    }
    let input = CohortEvaluationInput {
        evaluator: TrialComponentBinding::try_new(
            SourceIdentifier::try_from(wire.evaluator_name)
                .map_err(|_| ExperimentError::CorruptRecord)?,
            decode_hex(&wire.evaluator_digest)?,
        )?,
        experiment_design_digest: decode_hex(&wire.experiment_design_digest)?,
        cohort_universe_digest: universe_digest.unwrap_or(Sha256Digest::new([0; 32])),
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
    };
    let value = if wire.schema_version == 1 {
        BacktestCohortEvaluation::try_new_legacy(input)?
    } else {
        BacktestCohortEvaluation::try_new(input)?
    };
    if value.id != expected {
        return Err(ExperimentError::CorruptRecord);
    }
    Ok(value)
}
