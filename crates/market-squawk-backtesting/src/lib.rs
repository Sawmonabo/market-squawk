#![forbid(unsafe_code)]
//! Point-in-time research backtesting and immutable experiment governance.

mod clock;
mod dataset;
mod engine;
mod experiments;
mod fills;
mod model_strategy;
mod service;
mod strategy;

pub use clock::EventTimeClock;
pub use dataset::{
    AVAILABLE_AT_COMPONENT, BacktestDataset, BacktestLimits, BacktestLimitsInput,
    BacktestObservation, DEPTH_COMPONENT, EVENT_AT_COMPONENT, HistoricalUniverseStatus,
    MID_PRICE_COMPONENT, ResearchFeatureValue, SPREAD_COMPONENT, STALE_AT_COMPONENT,
    UNIVERSE_COMPONENT,
};
pub use engine::{
    AccountingReconciliation, BacktestContext, BacktestEngine, BacktestError, BacktestRequest,
    BacktestRun, PortfolioSeed,
};
pub use experiments::{
    BacktestArtifact, BacktestCohortCandidate, BacktestCohortEvaluation,
    BacktestCohortEvaluationId, BacktestCohortFold, BacktestCohortFoldPartition,
    BacktestCohortPartition, BacktestCohortPlan, BacktestCohortUniverse,
    BacktestExecutableIdentity, BacktestOverfittingDiagnostic, BacktestOverfittingFold,
    BacktestOverfittingInput, BacktestOverfittingScore, CohortMemberBinding,
    DeflatedPerformanceDiagnostic, DeflatedPerformanceInput, ExperimentError, ExperimentInventory,
    ExperimentLimits, ExperimentLimitsInput, MAX_COHORT_CANDIDATES_PER_FOLD,
    MAX_COHORT_MEMBER_REFERENCES, MAX_COHORT_SELECTION_CANDIDATES, MAX_COHORT_UNIQUE_MEMBERS,
    TrialCompletion, TrialComponentBinding, TrialDatasetPartition, TrialFailure, TrialId,
    TrialMetric, TrialParameter, TrialRecord, TrialReservation, TrialSearchDimension, TrialSpec,
    TrialSpecInput, TrialStatus,
};
pub use fills::{
    RESEARCH_EXECUTION_POLICY_VERSION, ResearchExecutionAssumptions,
    ResearchExecutionAssumptionsInput, ResearchFill, ResearchLiquidityPriority,
};
pub use model_strategy::{BacktestModelDecisionMapper, BacktestModelStrategy};
pub use service::{
    BacktestFailure, BacktestOutcome, BacktestResult, BacktestService, BacktestServiceError,
    BacktestTrialPlan,
};
pub use strategy::{
    AdmittedBacktestStrategy, BacktestAdmissionError, BacktestBuildReceipt,
    BacktestBuildRegistration, BacktestStrategy, BacktestStrategyClass, BacktestStrategyFactory,
    BacktestStrategyInstance, BacktestStrategyRegistry,
};

#[cfg(test)]
mod tests;
