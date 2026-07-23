//! Application-owned authority composition for governed point-in-time backtests.

use market_squawk_backtesting::{
    BacktestAdmissionError, BacktestDataset, BacktestLimits, BacktestOutcome, BacktestRequest,
    BacktestService, BacktestServiceError, BacktestStrategyRegistry, BacktestTrialPlan,
    ExperimentInventory, ExperimentLimits, PortfolioSeed, ResearchExecutionAssumptions,
    TrialParameter, TrialSearchDimension,
};
use market_squawk_data::{CorporateActionPlan, PinnedInstrumentDefinitions, PinnedQueryOutput};
use market_squawk_domain::SourceIdentifier;
use market_squawk_platform::{LocalPaths, PathError};
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
