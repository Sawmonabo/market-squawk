#![forbid(unsafe_code)]
//! Point-in-time research backtesting and immutable experiment governance.

mod clock;
mod dataset;
mod engine;
mod experiments;
mod fills;
mod model_strategy;
mod service;

pub use clock::EventTimeClock;
pub use dataset::{
    AVAILABLE_AT_COMPONENT, BacktestDataset, BacktestLimits, BacktestLimitsInput,
    BacktestObservation, DEPTH_COMPONENT, EVENT_AT_COMPONENT, HistoricalUniverseStatus,
    MID_PRICE_COMPONENT, ResearchFeatureValue, SPREAD_COMPONENT, STALE_AT_COMPONENT,
    UNIVERSE_COMPONENT,
};
pub use engine::{
    AccountingReconciliation, BacktestContext, BacktestEngine, BacktestError, BacktestRequest,
    BacktestRun, BacktestStrategy, PortfolioSeed,
};
pub use experiments::{
    BacktestArtifact, BacktestOverfittingDiagnostic, BacktestOverfittingFold,
    BacktestOverfittingInput, BacktestOverfittingScore, DeflatedPerformanceDiagnostic,
    DeflatedPerformanceInput, ExperimentError, ExperimentInventory, ExperimentLimits,
    ExperimentLimitsInput, TrialCompletion, TrialCompletionInput, TrialComponentBinding,
    TrialFailure, TrialId, TrialMetric, TrialParameter, TrialRecord, TrialReservation,
    TrialSearchDimension, TrialSpec, TrialSpecInput, TrialStatus,
};
pub use fills::{ResearchExecutionAssumptions, ResearchExecutionAssumptionsInput, ResearchFill};
pub use model_strategy::{BacktestModelDecisionMapper, BacktestModelStrategy};
pub use service::{
    BacktestEvaluation, BacktestFailure, BacktestOutcome, BacktestResult, BacktestService,
    BacktestServiceError,
};

#[cfg(test)]
mod tests;
