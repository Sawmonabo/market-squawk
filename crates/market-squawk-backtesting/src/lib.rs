#![forbid(unsafe_code)]
//! Point-in-time research backtesting and immutable experiment governance.

mod clock;
mod dataset;
mod engine;
mod experiments;
mod fills;
mod model_strategy;
mod recommendation;
#[cfg(feature = "release-evidence")]
mod release_evidence;
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
pub use recommendation::{
    COST_ADJUSTED_TOTAL_RETURN_METRIC, MAXIMUM_DRAWDOWN_METRIC,
    MaterializedRecommendationSignalPlanV1, POSITIVE_FOLD_STABILITY_METRIC,
    RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1, RECOMMENDATION_OOS_FOLD_COUNT_V1,
    RECOMMENDATION_OOS_FOLD_HORIZON_NANOS_V1, RECOMMENDATION_TARGET_HORIZON_NANOS_V1,
    RecommendationAggregateEvidenceV1, RecommendationAggregateUnavailableV1,
    RecommendationAggregateV1, RecommendationBacktestError, RecommendationBacktestEvidenceV1,
    RecommendationBacktestKernelV1, RecommendationBacktestLimits,
    RecommendationBacktestLimitsInput, RecommendationBacktestPolicyV1,
    RecommendationBacktestPolicyV1Input, RecommendationBacktestPublicationV1,
    RecommendationBenchmarkAggregateV1, RecommendationBenchmarkGapV1,
    RecommendationBenchmarkPolicyV1, RecommendationEquityPointV1, RecommendationExecutionGapV1,
    RecommendationMaterializedBacktestErrorV1, RecommendationOosFoldV1,
    RecommendationPreauthorizedSignalV1, RecommendationRoundTripOutcomeV1,
    RecommendationSignalCensorReasonV1, RecommendationSignalDispositionV1,
    RecommendationSignalInstructionV1, RecommendationSignalPlanCompletenessV1,
    RecommendationSignalPlanMaterializationErrorV1, RecommendationSignalPlanMaterializationInputV1,
    RecommendationSignalPlanMaterializerV1, RecommendationSignalPlanV1,
    RecommendationSignalResultV1, RecommendationSignalUnavailableReasonV1, RecommendationSignalV1,
    recommendation_conservative_execution_assumptions_v1,
};
#[cfg(feature = "release-evidence")]
pub use release_evidence::{
    ReleaseEvidenceBacktestError, ReleaseEvidenceBacktestResult, run_release_evidence_backtest,
};
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
