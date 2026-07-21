//! Risk-enforced execution contracts with no public authority or dispatch bypass.

mod account;
mod adapter;
mod approval;
mod audit;
mod clock;
mod dispatcher;
mod intent;
mod limits;
mod live_hook;
mod risk;
mod strategy;
mod task_reaper;

pub use account::{
    AccountBootstrap, AccountCoordinatorConfig, AccountCoordinatorError,
    AccountIdempotencyBootstrap, AccountIdempotencyBootstrapError, AccountIdempotencySnapshotError,
    AccountIdempotencyTombstone, AccountReservationError, AccountReservationStateError,
    AccountRiskCoordinator, AccountRiskReservation,
};
pub use adapter::{
    ACCOUNT_REPLACEMENT_SCHEMA_VERSION, CancelOrder, CancelReceipt, CancelStatus, DispatchOrder,
    ExecutionAdapter, ExecutionAdapterError, ExecutionAdapterFuture, ExecutionMarketSink,
    ExecutionMarketSinkError, ExecutionMarketUpdate, ExecutionOperation, ExecutionReceipt,
    ExecutionState, ExecutionStateError, ExecutionStateSourceBinding, MAX_RECONCILED_ACCOUNTS,
    MAX_RECONCILED_ORDERS, MAX_RECONCILED_POSITIONS_PER_ACCOUNT, PersistenceAcknowledgement,
    ReconcileOrders, ReconciledAccountState, ReconciledAccountStateError, ReconciledOrder,
    ReconciledOrderStatus, ReconciliationAcknowledgement, ReconciliationBatchBinding,
    ReconciliationBatchBindingError, ReconciliationBatchId,
};
pub use approval::{
    ApprovedOrder, ExecutionMarketReference, ExecutionPriceBound, ExecutionPriceBoundError,
    MAX_EXECUTION_MARKET_LEVELS_PER_SIDE, RiskPolicyIdentity,
};
pub use audit::{
    ExecutionAuditConfig, ExecutionAuditError, ExecutionAuditEvent, ExecutionAuditKind,
    ExecutionAuditReader, ExecutionAuditReason, ExecutionAuditWriter, MAX_EXECUTION_AUDIT_REASONS,
};
pub use dispatcher::{
    ExecutionDispatchError, ExecutionDispatcher, ExecutionDispatcherConfig,
    ExecutionDispatcherError, ExecutionDispatcherHandle, ExecutionDispatcherShutdown,
};
pub use intent::{
    MAX_INTENT_SLIPPAGE_BASIS_POINTS, MAX_ORDER_REASON_CODES, OrderIntent, OrderIntentDigest,
    OrderIntentError, OrderIntentInput,
};
pub use limits::{
    AccountRiskViolation, MAX_RISK_INSTRUMENTS, RiskLimits, RiskLimitsError, RiskLimitsInput,
};
pub use live_hook::{ExecutionLiveActionHook, ExecutionLiveActionHookError};
pub use risk::{
    MarketRiskInput, MarketRiskInputError, PreAuthorityRiskOutcome, RiskOutcome, RiskRejection,
    RiskRejectionCode, RiskService, RiskServiceConfig, RiskServiceError,
};
pub use strategy::{
    BoundedOrderIntentIterator, BoundedOrderIntents, MAX_STRATEGY_ORDER_INTENTS, Strategy,
    StrategyContext, StrategyError,
};
pub use task_reaper::{
    ExecutionTask, ExecutionTaskDrain, ExecutionTaskPermit, ExecutionTaskReaper,
    ExecutionTaskReaperError,
};
