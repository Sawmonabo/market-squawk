//! Reserve-before-run application service for governed point-in-time experiments.

use std::collections::{BTreeMap, BTreeSet};

use market_squawk_data::Sha256Digest;
use market_squawk_domain::SourceIdentifier;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::experiments::{
    BacktestCohortEvaluation, BacktestCohortPlan, BacktestCohortUniverse,
    BacktestOverfittingDiagnostic, BacktestOverfittingFold, BacktestOverfittingInput,
    BacktestOverfittingScore, CohortEvaluationInput, CohortMemberBinding,
    DeflatedPerformanceDiagnostic, DeflatedPerformanceInput, ExperimentError, ExperimentInventory,
    MAX_COHORT_MEMBER_REFERENCES, TrialCompletionInput, TrialDatasetPartition, TrialFailure,
    TrialId, TrialMetric, TrialParameter, TrialRecord, TrialSearchDimension, TrialSpec,
    TrialSpecInput, TrialStatus,
};
use crate::{
    AdmittedBacktestStrategy, BacktestEngine, BacktestError, BacktestRequest, BacktestRun,
};

mod artifact;

/// Search and selection contract combined with strategy-owned executable identity by the service.
#[derive(Clone, Debug)]
pub struct BacktestTrialPlan {
    parameters: Vec<TrialParameter>,
    search_space: Vec<TrialSearchDimension>,
    selection_criterion: SourceIdentifier,
    cohort_universe: Option<BacktestCohortUniverse>,
}

impl BacktestTrialPlan {
    /// Owns the bounded experiment dimensions that are independent of executable identity.
    #[must_use]
    pub const fn new(
        parameters: Vec<TrialParameter>,
        search_space: Vec<TrialSearchDimension>,
        selection_criterion: SourceIdentifier,
    ) -> Self {
        Self {
            parameters,
            search_space,
            selection_criterion,
            cohort_universe: None,
        }
    }

    /// Binds a complete code-generated cohort universe before this member is reserved.
    #[must_use]
    pub fn with_cohort_universe(mut self, universe: BacktestCohortUniverse) -> Self {
        self.cohort_universe = Some(universe);
        self
    }
}

/// Successful governed run and its immutable terminal inventory record.
#[derive(Clone, Debug)]
pub struct BacktestResult {
    run: BacktestRun,
    trial: TrialRecord,
}

impl BacktestResult {
    /// Returns reconciled fills, portfolio state, and deterministic result identity.
    #[must_use]
    pub const fn run(&self) -> &BacktestRun {
        &self.run
    }

    /// Returns the immutable completed trial record.
    #[must_use]
    pub const fn trial(&self) -> &TrialRecord {
        &self.trial
    }
}

/// Audited engine failure whose immutable terminal was committed before returning.
#[derive(Debug)]
pub struct BacktestFailure {
    error: BacktestError,
    trial: TrialRecord,
}

impl BacktestFailure {
    /// Returns the typed backtest failure.
    #[must_use]
    pub const fn error(&self) -> &BacktestError {
        &self.error
    }

    /// Returns the immutable failed trial record.
    #[must_use]
    pub const fn trial(&self) -> &TrialRecord {
        &self.trial
    }
}

/// Domain outcome after durable reservation and exactly one terminal transition.
#[derive(Debug)]
pub enum BacktestOutcome {
    /// The run reconciled and its bounded artifact and completion were committed.
    Completed(Box<BacktestResult>),
    /// The engine failed closed and an immutable failure terminal was committed.
    Failed(Box<BacktestFailure>),
}

/// Sole orchestrator for reservation, execution, artifact publication, and terminal commit.
#[derive(Debug)]
pub struct BacktestService {
    inventory: ExperimentInventory,
}

impl BacktestService {
    /// Owns one capability-confined experiment inventory.
    #[must_use]
    pub const fn new(inventory: ExperimentInventory) -> Self {
        Self { inventory }
    }

    /// Derives one exact trial from the request and strategy capability before durable reservation.
    pub fn run(
        &self,
        request: BacktestRequest,
        strategy: &mut AdmittedBacktestStrategy,
        plan: BacktestTrialPlan,
        cancellation: &CancellationToken,
    ) -> Result<BacktestOutcome, BacktestServiceError> {
        let dataset_partition = TrialDatasetPartition::try_new(
            request
                .dataset
                .observations
                .first()
                .ok_or(BacktestServiceError::InvalidCohort)?
                .decision_at,
            request
                .dataset
                .observations
                .last()
                .ok_or(BacktestServiceError::InvalidCohort)?
                .decision_at,
        )?;
        let executable = strategy.identity();
        let cohort_universe_digest = plan
            .cohort_universe
            .as_ref()
            .map(BacktestCohortUniverse::digest);
        let expected_cohort_candidates = plan
            .cohort_universe
            .as_ref()
            .map(BacktestCohortUniverse::expected_candidate_count);
        if expected_cohort_candidates
            .is_some_and(|expected| expected > self.inventory.limits().max_trials())
        {
            return Err(ExperimentError::LimitExceeded.into());
        }
        let spec = TrialSpec::try_new(TrialSpecInput {
            dataset_identity: request.dataset_identity(),
            object_graph_digest: request.object_graph_digest(),
            execution_assumption_digest: request.assumption_digest(),
            run_input_digest: request.run_input_digest(),
            cohort_authority_digest: request.cohort_authority_digest(),
            cohort_universe_digest,
            model: executable.model().cloned(),
            strategy: executable.strategy().clone(),
            code: executable.code().clone(),
            configuration_digest: executable.configuration_digest(),
            seed: request.seed(),
            parameters: plan.parameters,
            search_space: plan.search_space,
            selection_criterion: plan.selection_criterion,
        })?;
        if let Some(expected) = expected_cohort_candidates
            && spec.search_space_cardinality()? != expected
        {
            return Err(BacktestServiceError::InvalidCohort);
        }
        let reservation = self.inventory.reserve(spec)?;
        let run = match BacktestEngine::run(&request, strategy, cancellation) {
            Ok(run) => run,
            Err(error) => {
                let trial =
                    self.commit_failure(reservation, "backtest-engine", &error.to_string())?;
                return Ok(BacktestOutcome::Failed(Box::new(BacktestFailure {
                    error,
                    trial,
                })));
            }
        };
        let artifact_bytes =
            match artifact::encode(&request, &run, self.inventory.limits().max_artifact_bytes()) {
                Ok(bytes) => bytes,
                Err(error) => {
                    self.commit_failure(
                        reservation,
                        "backtest-artifact-encoding",
                        "bounded encoding",
                    )?;
                    return Err(error);
                }
            };
        let metrics = match run_metrics(&request, &run) {
            Ok(metrics) => metrics,
            Err(error) => {
                self.commit_failure(reservation, "backtest-terminal-metrics", &error.to_string())?;
                return Err(error);
            }
        };
        let artifact = match self.inventory.prepare_artifact(&artifact_bytes) {
            Ok(artifact) => artifact,
            Err(error) => {
                self.commit_failure(
                    reservation,
                    "backtest-artifact-validation",
                    &error.to_string(),
                )?;
                return Err(error.into());
            }
        };
        let completion = match self.inventory.prepare_completion(
            &reservation,
            TrialCompletionInput {
                result_digest: run.result_digest(),
                artifact,
                metrics,
                dataset_partition: Some(dataset_partition),
            },
        ) {
            Ok(completion) => completion,
            Err(error) => {
                self.commit_failure(
                    reservation,
                    "backtest-terminal-validation",
                    &error.to_string(),
                )?;
                return Err(error.into());
            }
        };
        let trial = self
            .inventory
            .complete(reservation, completion, &artifact_bytes)?;
        Ok(BacktestOutcome::Completed(Box::new(BacktestResult {
            run,
            trial,
        })))
    }

    /// Computes and appends authoritative diagnostics from immutable completed-trial metrics.
    pub fn evaluate_cohort(
        &self,
        plan: BacktestCohortPlan,
    ) -> Result<BacktestCohortEvaluation, BacktestServiceError> {
        let expected_candidates = plan.universe().expected_candidate_count();
        if plan.member_ids().len() > self.inventory.limits().max_trials()
            || expected_candidates > self.inventory.limits().max_trials()
            || plan.member_reference_count() > MAX_COHORT_MEMBER_REFERENCES
        {
            return Err(ExperimentError::LimitExceeded.into());
        }
        let mut records = BTreeMap::new();
        let mut design = None;
        let mut cohort_authority = None;
        for id in plan.member_ids() {
            let record = self.inventory.trial(*id)?;
            if record.spec().selection_criterion() != plan.selection_criterion()
                || record.spec().cohort_universe_digest() != Some(plan.universe().digest())
                || !matches!(record.status(), TrialStatus::Completed(_))
            {
                return Err(BacktestServiceError::InvalidCohort);
            }
            let candidate_authority = record
                .spec()
                .cohort_authority_digest()
                .ok_or(BacktestServiceError::InvalidCohort)?;
            if cohort_authority.is_some_and(|expected| expected != candidate_authority) {
                return Err(BacktestServiceError::InvalidCohort);
            }
            cohort_authority = Some(candidate_authority);
            let candidate_design = record.spec().experiment_design_digest()?;
            if design.is_some_and(|expected| expected != candidate_design) {
                return Err(BacktestServiceError::InvalidCohort);
            }
            let candidate_cardinality = record.spec().search_space_cardinality()?;
            if candidate_cardinality != expected_candidates {
                return Err(BacktestServiceError::InvalidCohort);
            }
            design = Some(candidate_design);
            records.insert(*id, record);
        }
        let independent_trials = expected_candidates;
        validate_cohort_folds(&plan, &records, independent_trials)?;
        validate_selection_candidates(&plan, &records, independent_trials)?;
        let mut diagnostic_folds = Vec::new();
        diagnostic_folds
            .try_reserve_exact(plan.folds().len())
            .map_err(|_| ExperimentError::LimitExceeded)?;
        for fold in plan.folds() {
            let mut candidates = Vec::new();
            candidates
                .try_reserve_exact(expected_candidates)
                .map_err(|_| ExperimentError::LimitExceeded)?;
            for candidate in fold.candidates() {
                candidates.push(BacktestOverfittingScore {
                    in_sample: trial_metric(
                        &records,
                        candidate.in_sample(),
                        plan.selection_criterion(),
                    )?,
                    out_of_sample: trial_metric(
                        &records,
                        candidate.out_of_sample(),
                        plan.selection_criterion(),
                    )?,
                });
            }
            diagnostic_folds.push(BacktestOverfittingFold { candidates });
        }
        let overfitting_input = BacktestOverfittingInput {
            folds: diagnostic_folds,
        };
        let probability_of_backtest_overfitting =
            BacktestOverfittingDiagnostic::try_compute(&overfitting_input)?;
        let mut selection_scores = Vec::new();
        selection_scores
            .try_reserve_exact(expected_candidates)
            .map_err(|_| ExperimentError::LimitExceeded)?;
        for id in plan.selection_candidates() {
            selection_scores.push((
                *id,
                trial_metric(&records, *id, plan.selection_criterion())?,
            ));
        }
        let selected_id = selection_scores
            .iter()
            .max_by(|(left_id, left_score), (right_id, right_score)| {
                left_score
                    .total_cmp(right_score)
                    .then_with(|| right_id.cmp(left_id))
            })
            .map(|(id, _)| *id)
            .ok_or(BacktestServiceError::InvalidCohort)?;
        let selected = member_binding(&records, selected_id)?;
        let sharpe_metric = SourceIdentifier::try_from("sharpe")?;
        let mut sharpes = Vec::new();
        sharpes
            .try_reserve_exact(expected_candidates)
            .map_err(|_| ExperimentError::LimitExceeded)?;
        for id in plan.selection_candidates() {
            sharpes.push(trial_metric(&records, *id, &sharpe_metric)?);
        }
        let sharpe_count = sharpes.len() as f64;
        let sharpe_mean = sharpes.iter().sum::<f64>() / sharpe_count;
        let trial_sharpe_variance = sharpes
            .iter()
            .map(|value| {
                let deviation = value - sharpe_mean;
                deviation * deviation
            })
            .sum::<f64>()
            / (sharpe_count - 1.0);
        let observations = trial_metric(
            &records,
            selected_id,
            &SourceIdentifier::try_from("return-observations")?,
        )?;
        if observations.fract() != 0.0 || observations < 3.0 || observations > usize::MAX as f64 {
            return Err(BacktestServiceError::InvalidCohort);
        }
        let deflated_performance =
            DeflatedPerformanceDiagnostic::try_compute(DeflatedPerformanceInput {
                observed_sharpe: trial_metric(
                    &records,
                    selected_id,
                    &SourceIdentifier::try_from("sharpe")?,
                )?,
                independent_trials,
                observations: observations as usize,
                trial_sharpe_variance,
                return_skewness: trial_metric(
                    &records,
                    selected_id,
                    &SourceIdentifier::try_from("return-skewness")?,
                )?,
                return_excess_kurtosis: trial_metric(
                    &records,
                    selected_id,
                    &SourceIdentifier::try_from("return-excess-kurtosis")?,
                )?,
            })?;
        let evaluator = cohort_evaluator_binding()?;
        let mut members = Vec::new();
        members
            .try_reserve_exact(plan.member_ids().len())
            .map_err(|_| ExperimentError::LimitExceeded)?;
        for id in plan.member_ids() {
            members.push(member_binding(&records, *id)?);
        }
        let evaluation = BacktestCohortEvaluation::try_new(CohortEvaluationInput {
            evaluator,
            experiment_design_digest: design.ok_or(BacktestServiceError::InvalidCohort)?,
            cohort_universe_digest: plan.universe().digest(),
            selection_criterion: plan.selection_criterion().clone(),
            members,
            folds: plan.folds().to_vec(),
            selection_candidates: plan.selection_candidates().to_vec(),
            probability_of_backtest_overfitting,
            deflated_performance,
            selected,
        })?;
        self.inventory.publish_cohort_evaluation(&evaluation)?;
        Ok(evaluation)
    }

    fn commit_failure(
        &self,
        reservation: crate::TrialReservation,
        code: &'static str,
        evidence: &str,
    ) -> Result<TrialRecord, BacktestServiceError> {
        let code =
            SourceIdentifier::try_from(code).map_err(|_| BacktestServiceError::FailureEncoding)?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/backtest-failure/v1");
        hash.update((code.as_str().len() as u64).to_be_bytes());
        hash.update(code.as_str().as_bytes());
        hash.update((evidence.len() as u64).to_be_bytes());
        hash.update(evidence.as_bytes());
        let failure = TrialFailure::try_new(code, Sha256Digest::new(hash.finalize().into()))?;
        self.inventory
            .fail(reservation, failure)
            .map_err(Into::into)
    }
}

fn run_metrics(
    request: &BacktestRequest,
    run: &BacktestRun,
) -> Result<Vec<TrialMetric>, BacktestServiceError> {
    let initial = request.portfolio.initial_cash.amount();
    let ending = run.portfolio().marked_equity().amount();
    let total_return = ending
        .checked_sub(initial)
        .and_then(|value| value.checked_div(initial))
        .and_then(|value| rust_decimal::prelude::ToPrimitive::to_f64(&value))
        .ok_or(BacktestServiceError::MetricEncoding)?;
    let performance = run.performance();
    Ok(vec![
        TrialMetric::try_new(
            SourceIdentifier::try_from("ending-equity")?,
            rust_decimal::prelude::ToPrimitive::to_f64(&ending)
                .ok_or(BacktestServiceError::MetricEncoding)?,
        )?,
        TrialMetric::try_new(
            SourceIdentifier::try_from("fill-count")?,
            run.fills().len() as f64,
        )?,
        TrialMetric::try_new(SourceIdentifier::try_from("total-return")?, total_return)?,
        TrialMetric::try_new(SourceIdentifier::try_from("sharpe")?, performance.sharpe)?,
        TrialMetric::try_new(
            SourceIdentifier::try_from("return-observations")?,
            performance.observations as f64,
        )?,
        TrialMetric::try_new(
            SourceIdentifier::try_from("return-skewness")?,
            performance.skewness,
        )?,
        TrialMetric::try_new(
            SourceIdentifier::try_from("return-excess-kurtosis")?,
            performance.excess_kurtosis,
        )?,
    ])
}

fn trial_metric(
    records: &BTreeMap<TrialId, TrialRecord>,
    id: TrialId,
    name: &SourceIdentifier,
) -> Result<f64, BacktestServiceError> {
    let record = records
        .get(&id)
        .ok_or(BacktestServiceError::InvalidCohort)?;
    let TrialStatus::Completed(completion) = record.status() else {
        return Err(BacktestServiceError::InvalidCohort);
    };
    completion
        .metrics()
        .iter()
        .find(|metric| metric.name() == name)
        .map(TrialMetric::value)
        .ok_or(BacktestServiceError::InvalidCohort)
}

fn member_binding(
    records: &BTreeMap<TrialId, TrialRecord>,
    id: TrialId,
) -> Result<CohortMemberBinding, BacktestServiceError> {
    let record = records
        .get(&id)
        .ok_or(BacktestServiceError::InvalidCohort)?;
    let TrialStatus::Completed(completion) = record.status() else {
        return Err(BacktestServiceError::InvalidCohort);
    };
    Ok(CohortMemberBinding::new(
        id,
        completion.result_digest(),
        record.spec().dataset_identity(),
        completion
            .dataset_partition()
            .ok_or(BacktestServiceError::InvalidCohort)?,
        record.spec().parameter_digest()?,
    ))
}

fn validate_cohort_folds(
    plan: &BacktestCohortPlan,
    records: &BTreeMap<TrialId, TrialRecord>,
    expected_candidates: usize,
) -> Result<(), BacktestServiceError> {
    let mut expected_parameters = None::<BTreeSet<[u8; 32]>>;
    let mut seen_partitions = BTreeSet::new();
    for fold in plan.folds() {
        if fold.candidates().len() != expected_candidates {
            return Err(BacktestServiceError::InvalidCohort);
        }
        let mut fold_parameters = BTreeSet::new();
        let mut fold_identity = None;
        for candidate in fold.candidates() {
            let in_record = records
                .get(&candidate.in_sample())
                .ok_or(BacktestServiceError::InvalidCohort)?;
            let out_record = records
                .get(&candidate.out_of_sample())
                .ok_or(BacktestServiceError::InvalidCohort)?;
            let TrialStatus::Completed(in_completion) = in_record.status() else {
                return Err(BacktestServiceError::InvalidCohort);
            };
            let TrialStatus::Completed(out_completion) = out_record.status() else {
                return Err(BacktestServiceError::InvalidCohort);
            };
            let in_partition = in_completion
                .dataset_partition()
                .ok_or(BacktestServiceError::InvalidCohort)?;
            let out_partition = out_completion
                .dataset_partition()
                .ok_or(BacktestServiceError::InvalidCohort)?;
            let parameter_digest = in_record.spec().parameter_digest()?;
            if parameter_digest != out_record.spec().parameter_digest()?
                || in_record.spec().dataset_identity() == out_record.spec().dataset_identity()
                || in_partition.ends_at() >= out_partition.starts_at()
            {
                return Err(BacktestServiceError::InvalidCohort);
            }
            if !fold_parameters.insert(parameter_digest.bytes()) {
                return Err(BacktestServiceError::InvalidCohort);
            }
            let identity = (
                in_record.spec().dataset_identity().bytes(),
                in_record.spec().object_graph_digest().bytes(),
                in_partition.starts_at().unix_nanos(),
                in_partition.ends_at().unix_nanos(),
                out_record.spec().dataset_identity().bytes(),
                out_record.spec().object_graph_digest().bytes(),
                out_partition.starts_at().unix_nanos(),
                out_partition.ends_at().unix_nanos(),
            );
            if fold_identity.is_some_and(|expected| expected != identity) {
                return Err(BacktestServiceError::InvalidCohort);
            }
            fold_identity = Some(identity);
        }
        if expected_parameters
            .as_ref()
            .is_some_and(|expected| expected != &fold_parameters)
            || !seen_partitions.insert(fold_identity.ok_or(BacktestServiceError::InvalidCohort)?)
        {
            return Err(BacktestServiceError::InvalidCohort);
        }
        expected_parameters = Some(fold_parameters);
    }
    let expected_partitions = plan
        .universe()
        .folds()
        .iter()
        .map(|fold| {
            let in_sample = fold.in_sample();
            let out_of_sample = fold.out_of_sample();
            (
                in_sample.dataset_identity().bytes(),
                in_sample.object_graph_digest().bytes(),
                in_sample.interval().starts_at().unix_nanos(),
                in_sample.interval().ends_at().unix_nanos(),
                out_of_sample.dataset_identity().bytes(),
                out_of_sample.object_graph_digest().bytes(),
                out_of_sample.interval().starts_at().unix_nanos(),
                out_of_sample.interval().ends_at().unix_nanos(),
            )
        })
        .collect::<BTreeSet<_>>();
    if seen_partitions != expected_partitions {
        return Err(BacktestServiceError::InvalidCohort);
    }
    Ok(())
}

fn validate_selection_candidates(
    plan: &BacktestCohortPlan,
    records: &BTreeMap<TrialId, TrialRecord>,
    expected_candidates: usize,
) -> Result<(), BacktestServiceError> {
    if plan.selection_candidates().len() != expected_candidates {
        return Err(BacktestServiceError::InvalidCohort);
    }
    let mut expected_dataset = None;
    let mut parameter_digests = BTreeSet::new();
    for candidate in plan.selection_candidates() {
        let record = records
            .get(candidate)
            .ok_or(BacktestServiceError::InvalidCohort)?;
        let TrialStatus::Completed(completion) = record.status() else {
            return Err(BacktestServiceError::InvalidCohort);
        };
        let dataset = (
            record.spec().dataset_identity(),
            record.spec().object_graph_digest(),
            completion
                .dataset_partition()
                .ok_or(BacktestServiceError::InvalidCohort)?,
        );
        if expected_dataset.is_some_and(|expected| expected != dataset)
            || !parameter_digests.insert(record.spec().parameter_digest()?.bytes())
        {
            return Err(BacktestServiceError::InvalidCohort);
        }
        expected_dataset = Some(dataset);
    }
    let expected_dataset = expected_dataset.ok_or(BacktestServiceError::InvalidCohort)?;
    let selection = plan.universe().selection_partition();
    if expected_dataset
        != (
            selection.dataset_identity(),
            selection.object_graph_digest(),
            selection.interval(),
        )
    {
        return Err(BacktestServiceError::InvalidCohort);
    }
    Ok(())
}

fn cohort_evaluator_binding() -> Result<crate::TrialComponentBinding, BacktestServiceError> {
    let name = SourceIdentifier::try_from("backtest-cohort-evaluator-v1")?;
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/backtest-cohort-evaluator/v1");
    Ok(crate::TrialComponentBinding::try_new(
        name,
        Sha256Digest::new(hash.finalize().into()),
    )?)
}

/// Binding, evaluation, inventory, or bounded artifact failure outside engine domain outcomes.
#[derive(Debug, Error)]
pub enum BacktestServiceError {
    /// Exact post-run values could not be represented as finite analytical metrics.
    #[error("backtest post-run metrics could not be encoded")]
    MetricEncoding,
    /// Completed trials did not form one consistent bounded cohort.
    #[error("backtest cohort evidence is incomplete or inconsistent")]
    InvalidCohort,
    /// A fixed internal failure identity could not be encoded.
    #[error("backtest failure evidence could not be encoded")]
    FailureEncoding,
    /// The detailed result exceeded its bound or could not be encoded.
    #[error("backtest result artifact encoding failed")]
    ArtifactEncoding,
    /// Immutable inventory or artifact publication failed.
    #[error("backtest experiment inventory failed: {0}")]
    Experiment(#[from] ExperimentError),
    /// A fixed metric identity could not be represented.
    #[error("backtest metric identity could not be encoded: {0}")]
    MetricIdentity(#[from] market_squawk_domain::IdentityError),
}
