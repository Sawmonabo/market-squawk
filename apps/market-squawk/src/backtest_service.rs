//! Application-owned authority composition for governed point-in-time backtests.

use market_squawk_backtesting::{
    BacktestDataset, BacktestEvaluation, BacktestLimits, BacktestOutcome, BacktestRequest,
    BacktestService, BacktestServiceError, BacktestStrategy, ExperimentInventory, ExperimentLimits,
    PortfolioSeed, ResearchExecutionAssumptions, TrialComponentBinding, TrialParameter,
    TrialSearchDimension, TrialSpec, TrialSpecInput,
};
use market_squawk_data::{CorporateActionPlan, PinnedQueryOutput};
use market_squawk_domain::{InstrumentExecutionTerms, SourceIdentifier};
use market_squawk_platform::{LocalPaths, PathError};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// Model, strategy, code, configuration, parameter, and selection bindings for one trial.
#[derive(Clone, Debug)]
pub struct BacktestGovernanceBindings {
    pub model: Option<TrialComponentBinding>,
    pub strategy: TrialComponentBinding,
    pub code: TrialComponentBinding,
    pub configuration_digest: market_squawk_data::Sha256Digest,
    pub parameters: Vec<TrialParameter>,
    pub search_space: Vec<TrialSearchDimension>,
    pub selection_criterion: SourceIdentifier,
}

/// Owned production input whose data authority is a non-forgeable Task 11 query receipt.
#[derive(Debug)]
pub struct PinnedBacktestInput {
    pub query: PinnedQueryOutput,
    pub execution_terms: Vec<InstrumentExecutionTerms>,
    pub execution_assumptions: ResearchExecutionAssumptions,
    pub portfolio: PortfolioSeed,
    pub corporate_actions: Option<CorporateActionPlan>,
    pub sources: Vec<SourceIdentifier>,
    pub seed: u64,
    pub limits: BacktestLimits,
    pub governance: BacktestGovernanceBindings,
    pub evaluation: BacktestEvaluation,
}

/// Sole application composition for controlled local backtesting artifacts and inventory.
#[derive(Debug)]
pub struct ProductionBacktestService {
    inner: BacktestService,
}

impl ProductionBacktestService {
    /// Opens the configured artifact capability and initializes the immutable experiment namespace.
    pub fn initialize(
        paths: &LocalPaths,
        limits: ExperimentLimits,
    ) -> Result<Self, ProductionBacktestServiceError> {
        let root = paths.artifacts()?.try_clone_directory()?;
        let inventory = ExperimentInventory::try_new(root, limits)?;
        Ok(Self {
            inner: BacktestService::new(inventory),
        })
    }

    /// Admits the pinned query, derives the trial identity, then runs the governed service.
    pub fn run(
        &self,
        input: PinnedBacktestInput,
        strategy: &mut dyn BacktestStrategy,
        cancellation: &CancellationToken,
    ) -> Result<BacktestOutcome, ProductionBacktestServiceError> {
        let dataset = BacktestDataset::try_from_pinned_query(
            input.query,
            input.execution_terms,
            input.limits,
        )?;
        let governance = input.governance;
        let spec = TrialSpec::try_new(TrialSpecInput {
            dataset_identity: dataset.identity(),
            object_graph_digest: dataset.object_graph_digest(),
            execution_assumption_digest: input.execution_assumptions.digest(),
            model: governance.model,
            strategy: governance.strategy,
            code: governance.code,
            configuration_digest: governance.configuration_digest,
            seed: input.seed,
            parameters: governance.parameters,
            search_space: governance.search_space,
            selection_criterion: governance.selection_criterion,
        })?;
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
            .run(spec, request, strategy, input.evaluation, cancellation)
            .map_err(Into::into)
    }
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
}
