//! Application-owned authority composition for governed point-in-time backtests.

use market_squawk_backtesting::{
    BacktestAdmissionError, BacktestCohortCandidate, BacktestCohortEvaluation, BacktestCohortFold,
    BacktestCohortFoldPartition, BacktestCohortPartition, BacktestCohortPlan,
    BacktestCohortUniverse, BacktestDataset, BacktestLimits, BacktestOutcome, BacktestRequest,
    BacktestService, BacktestServiceError, BacktestStrategyRegistry, BacktestTrialPlan,
    ExperimentInventory, ExperimentLimits, PortfolioSeed, ResearchExecutionAssumptions, TrialId,
    TrialParameter, TrialSearchDimension,
};
use market_squawk_data::Sha256Digest;
use market_squawk_data::{CorporateActionPlan, PinnedInstrumentDefinitions, PinnedQueryOutput};
use market_squawk_domain::SourceIdentifier;
use market_squawk_platform::{LocalPaths, PathError};
use std::collections::BTreeMap;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// Parameter, search-space, and selection bindings combined with strategy-owned identity.
#[derive(Clone, Debug)]
pub struct BacktestExperimentPlan {
    pub parameters: Vec<TrialParameter>,
    pub search_space: Vec<TrialSearchDimension>,
    pub selection_criterion: SourceIdentifier,
}

/// Owned production input whose data and definition authority are non-forgeable receipts.
#[derive(Debug)]
pub struct PinnedBacktestInput {
    pub query: PinnedQueryOutput,
    pub instrument_definitions: PinnedInstrumentDefinitions,
    pub execution_assumptions: ResearchExecutionAssumptions,
    pub portfolio: PortfolioSeed,
    pub corporate_actions: Option<CorporateActionPlan>,
    pub sources: Vec<SourceIdentifier>,
    pub seed: u64,
    pub limits: BacktestLimits,
    pub experiment: BacktestExperimentPlan,
    /// Freshly materialized cohort members, when this registration carries the V1 evidence recipe.
    pub cohort: Option<PinnedBacktestCohort>,
}

/// One independently minted member input in a predeclared cohort.
///
/// `member_id` is a recipe-local mapping key, not a trial identity. The immutable `TrialId` is
/// deliberately derived only after the pinned receipt, executable identity, and complete cohort
/// universe have all been admitted.
#[derive(Debug)]
pub struct PinnedBacktestCohortMember {
    pub member_id: SourceIdentifier,
    pub input: PinnedBacktestInput,
}

/// One predeclared in-sample/out-of-sample member pair in a cohort fold.
#[derive(Clone, Debug)]
pub struct PinnedBacktestCohortCandidate {
    pub in_sample_member_id: SourceIdentifier,
    pub out_of_sample_member_id: SourceIdentifier,
}

/// Complete execution-ready cohort built exclusively from freshly pinned member receipts.
#[derive(Debug)]
pub struct PinnedBacktestCohort {
    pub generator_version: SourceIdentifier,
    pub generator_parameters: Vec<TrialParameter>,
    pub members: Vec<PinnedBacktestCohortMember>,
    pub folds: Vec<Vec<PinnedBacktestCohortCandidate>>,
    pub selection_member_ids: Vec<SourceIdentifier>,
}

/// The selected completed run and its append-only cohort diagnostic evidence.
#[derive(Debug)]
pub struct ProductionBacktestCohortOutcome {
    pub outcome: BacktestOutcome,
    pub evaluation: BacktestCohortEvaluation,
}

/// Sole application composition for controlled local backtesting artifacts and inventory.
#[derive(Debug)]
pub struct ProductionBacktestService {
    inner: BacktestService,
    strategies: BacktestStrategyRegistry,
}

impl ProductionBacktestService {
    /// Opens the configured artifact capability and initializes the immutable experiment namespace.
    pub fn initialize(
        paths: &LocalPaths,
        limits: ExperimentLimits,
        strategies: BacktestStrategyRegistry,
    ) -> Result<Self, ProductionBacktestServiceError> {
        let root = paths.artifacts()?.try_clone_directory()?;
        let inventory = ExperimentInventory::try_new(root, limits)?;
        Ok(Self {
            inner: BacktestService::new(inventory),
            strategies,
        })
    }

    /// Resolves the registered build, admits the pinned query, and runs the governed service.
    pub fn run(
        &self,
        input: PinnedBacktestInput,
        build_id: &SourceIdentifier,
        cancellation: &CancellationToken,
    ) -> Result<BacktestOutcome, ProductionBacktestServiceError> {
        if input.cohort.is_some() {
            return Err(market_squawk_backtesting::ExperimentError::InvalidDiagnostic.into());
        }
        let mut strategy = self.strategies.admit(build_id)?;
        let dataset = BacktestDataset::try_from_pinned_query(
            input.query,
            input.instrument_definitions,
            input.limits,
        )?;
        let experiment = input.experiment;
        let request = BacktestRequest::try_new(
            dataset,
            input.execution_assumptions,
            input.portfolio,
            input.corporate_actions,
            input.sources,
            input.seed,
            input.limits,
        )?;
        self.inner
            .run(
                request,
                &mut strategy,
                BacktestTrialPlan::new(
                    experiment.parameters,
                    experiment.search_space,
                    experiment.selection_criterion,
                ),
                cancellation,
            )
            .map_err(Into::into)
    }

    /// Runs every freshly pinned cohort member, then derives diagnostics from their immutable
    /// terminal records before returning only the code-selected member for application publication.
    pub fn run_cohort(
        &self,
        cohort: PinnedBacktestCohort,
        build_id: &SourceIdentifier,
        cancellation: &CancellationToken,
    ) -> Result<ProductionBacktestCohortOutcome, ProductionBacktestServiceError> {
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(cohort.members.len())
            .map_err(|_| market_squawk_backtesting::ExperimentError::LimitExceeded)?;
        let mut member_partitions = BTreeMap::new();
        for member in cohort.members {
            if member_partitions.contains_key(&member.member_id) {
                return Err(market_squawk_backtesting::ExperimentError::InvalidDiagnostic.into());
            }
            let input = member.input;
            let dataset = BacktestDataset::try_from_pinned_query(
                input.query,
                input.instrument_definitions,
                input.limits,
            )?;
            let request = BacktestRequest::try_new(
                dataset,
                input.execution_assumptions,
                input.portfolio,
                input.corporate_actions,
                input.sources,
                input.seed,
                input.limits,
            )?;
            let partition = request
                .dataset_partition()
                .ok_or(market_squawk_backtesting::BacktestError::InvalidRequest)?;
            member_partitions.insert(
                member.member_id.clone(),
                BacktestCohortPartition::try_new(
                    request.dataset_identity(),
                    request.object_graph_digest(),
                    partition,
                )?,
            );
            prepared.push(PreparedCohortMember {
                member_id: member.member_id,
                request,
                experiment: input.experiment,
            });
        }
        let expected_candidate_count = cohort.selection_member_ids.len();
        let mut fold_partitions = Vec::new();
        fold_partitions
            .try_reserve_exact(cohort.folds.len())
            .map_err(|_| market_squawk_backtesting::ExperimentError::LimitExceeded)?;
        for fold in &cohort.folds {
            let representative = fold
                .first()
                .ok_or(market_squawk_backtesting::ExperimentError::InvalidDiagnostic)?;
            let in_sample = *member_partitions
                .get(&representative.in_sample_member_id)
                .ok_or(market_squawk_backtesting::ExperimentError::InvalidDiagnostic)?;
            let out_of_sample = *member_partitions
                .get(&representative.out_of_sample_member_id)
                .ok_or(market_squawk_backtesting::ExperimentError::InvalidDiagnostic)?;
            fold_partitions.push(BacktestCohortFoldPartition::try_new(
                in_sample,
                out_of_sample,
            )?);
        }
        let selection_member = cohort
            .selection_member_ids
            .first()
            .ok_or(market_squawk_backtesting::ExperimentError::InvalidDiagnostic)?;
        let selection_partition = *member_partitions
            .get(selection_member)
            .ok_or(market_squawk_backtesting::ExperimentError::InvalidDiagnostic)?;
        let universe = BacktestCohortUniverse::try_new(
            cohort.generator_version,
            cohort.generator_parameters,
            expected_candidate_count,
            fold_partitions,
            selection_partition,
        )?;

        let mut completed = Vec::new();
        let mut trial_ids = BTreeMap::new();
        for member in prepared {
            let mut strategy = self.strategies.admit(build_id)?;
            let outcome = self.inner.run(
                member.request,
                &mut strategy,
                BacktestTrialPlan::new(
                    member.experiment.parameters,
                    member.experiment.search_space,
                    member.experiment.selection_criterion,
                )
                .with_cohort_universe(universe.clone()),
                cancellation,
            )?;
            let BacktestOutcome::Completed(result) = &outcome else {
                return Err(market_squawk_backtesting::ExperimentError::InvalidDiagnostic.into());
            };
            trial_ids.insert(member.member_id, result.trial().spec().id());
            completed.push(outcome);
        }
        let mut folds = Vec::new();
        for fold in cohort.folds {
            let mut candidates = Vec::new();
            for candidate in fold {
                candidates.push(BacktestCohortCandidate::new(
                    cohort_trial_id(&trial_ids, &candidate.in_sample_member_id)?,
                    cohort_trial_id(&trial_ids, &candidate.out_of_sample_member_id)?,
                ));
            }
            folds.push(BacktestCohortFold::try_new(candidates)?);
        }
        let selection_candidates = cohort
            .selection_member_ids
            .iter()
            .map(|member_id| cohort_trial_id(&trial_ids, member_id))
            .collect::<Result<Vec<_>, _>>()?;
        let selection_criterion = completed
            .first()
            .and_then(|outcome| match outcome {
                BacktestOutcome::Completed(result) => {
                    Some(result.trial().spec().selection_criterion().clone())
                }
                BacktestOutcome::Failed(_) => None,
            })
            .ok_or(market_squawk_backtesting::ExperimentError::InvalidDiagnostic)?;
        let plan = BacktestCohortPlan::try_new(
            universe,
            folds,
            selection_candidates,
            selection_criterion,
        )?;
        let evaluation = self.inner.evaluate_cohort(plan)?;
        let selected_id = evaluation.selected().trial_id();
        let selected_index = completed
            .iter()
            .position(|outcome| matches!(outcome, BacktestOutcome::Completed(result) if result.trial().spec().id() == selected_id))
            .ok_or(market_squawk_backtesting::ExperimentError::InvalidDiagnostic)?;
        Ok(ProductionBacktestCohortOutcome {
            outcome: completed.swap_remove(selected_index),
            evaluation,
        })
    }

    /// Resolves one immutable report from the same confined backtest inventory that published it.
    ///
    /// The public application layer validates the path-free report identity before calling this
    /// method; this service still derives the artifact location solely from its digest.
    pub fn read_report(
        &self,
        digest: Sha256Digest,
        byte_count: u64,
    ) -> Result<Vec<u8>, ProductionBacktestServiceError> {
        self.inner
            .read_artifact(digest, byte_count)
            .map_err(Into::into)
    }
}

#[derive(Debug)]
struct PreparedCohortMember {
    member_id: SourceIdentifier,
    request: BacktestRequest,
    experiment: BacktestExperimentPlan,
}

fn cohort_trial_id(
    trial_ids: &BTreeMap<SourceIdentifier, TrialId>,
    member_id: &SourceIdentifier,
) -> Result<TrialId, market_squawk_backtesting::ExperimentError> {
    trial_ids
        .get(member_id)
        .copied()
        .ok_or(market_squawk_backtesting::ExperimentError::InvalidDiagnostic)
}

/// Local-path, admission, trial, and governed-service composition failure.
#[derive(Debug, Error)]
pub enum ProductionBacktestServiceError {
    /// The controlled artifact capability is unavailable or changed identity.
    #[error("backtest local path failed: {0}")]
    Path(#[from] PathError),
    /// Point-in-time dataset or request admission failed.
    #[error("backtest request admission failed: {0}")]
    Backtest(#[from] market_squawk_backtesting::BacktestError),
    /// Trial construction or inventory initialization failed.
    #[error("backtest experiment governance failed: {0}")]
    Experiment(#[from] market_squawk_backtesting::ExperimentError),
    /// Governed execution or terminal publication failed.
    #[error("backtest service failed: {0}")]
    Service(#[from] BacktestServiceError),
    /// Application-owned build registration or admission failed.
    #[error("backtest strategy admission failed: {0}")]
    Admission(#[from] BacktestAdmissionError),
}
