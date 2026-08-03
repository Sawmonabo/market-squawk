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
mod portfolio;
mod risk;
mod strategy;
mod task_reaper;

pub use account::{
    AccountBootstrap, AccountCoordinatorConfig, AccountCoordinatorError,
    AccountIdempotencyBootstrap, AccountIdempotencyBootstrapError, AccountIdempotencySnapshotError,
    AccountIdempotencyTombstone, AccountReconciliationFenceError, AccountRecoveryBootstrap,
    AccountRecoverySnapshotError, AccountReservationError, AccountReservationStateError,
    AccountRiskCoordinator, AccountRiskReconciliationFence, AccountRiskReservation,
};
pub use adapter::{
    ACCOUNT_REPLACEMENT_SCHEMA_VERSION, CancelOrder, CancelReceipt, CancelStatus, DispatchOrder,
    ExecutionAdapter, ExecutionAdapterError, ExecutionAdapterFuture, ExecutionMarketSink,
    ExecutionMarketSinkError, ExecutionMarketUpdate, ExecutionOperation, ExecutionReceipt,
    ExecutionState, ExecutionStateError, ExecutionStateSourceBinding, MAX_RECONCILED_ACCOUNTS,
    MAX_RECONCILED_ORDERS, MAX_RECONCILED_POSITIONS_PER_ACCOUNT, PersistenceAcknowledgement,
    ReconcileOrders, ReconciledAccountState, ReconciledAccountStateError, ReconciledOrder,
    ReconciledOrderStatus, ReconciliationAcknowledgement, ReconciliationBatchBinding,
    ReconciliationBatchBindingError, ReconciliationBatchId, RecoverExecutionState,
    RecoveredDispatchOrder, RecoveredDispatchOrderError,
};
pub use approval::{
    ApprovedOrder, ExecutionMarketReference, ExecutionPriceBound, ExecutionPriceBoundError,
    MAX_EXECUTION_MARKET_LEVELS_PER_SIDE, RiskPolicyIdentity, RiskPolicyIdentityError,
};
pub use audit::{
    ExecutionAuditConfig, ExecutionAuditError, ExecutionAuditEvent, ExecutionAuditKind,
    ExecutionAuditReader, ExecutionAuditReason, ExecutionAuditRecord, ExecutionAuditWriter,
    MAX_EXECUTION_AUDIT_REASONS, StrategyNoActionAuditEvent,
};
pub use dispatcher::{
    ExecutionDispatchError, ExecutionDispatcher, ExecutionDispatcherConfig,
    ExecutionDispatcherError, ExecutionDispatcherHandle, ExecutionDispatcherQuiesce,
    ExecutionDispatcherShutdown,
};
pub use intent::{
    MAX_INTENT_SLIPPAGE_BASIS_POINTS, MAX_ORDER_REASON_CODES, MAX_ORDER_TARGET_ID_BYTES,
    OrderIntent, OrderIntentDigest, OrderIntentError, OrderIntentInput, OrderTargetReference,
    OrderTargetReferenceError,
};
pub use limits::{
    AccountRiskViolation, MAX_PAPER_FEE_BASIS_POINTS, MAX_RISK_INSTRUMENTS, RiskLimits,
    RiskLimitsError, RiskLimitsInput, RiskLimitsSnapshot,
};
pub use live_hook::{ExecutionLiveActionHook, ExecutionLiveActionHookError};
pub use portfolio::{
    PortfolioReadCapability, PortfolioReadError, PortfolioReadLimits, PortfolioRiskBinding,
    PortfolioServicePublisher, portfolio_execution_state,
};
pub use risk::{
    MarketRiskInput, MarketRiskInputError, PreAuthorityRiskOutcome, RiskOutcome, RiskRejection,
    RiskRejectionCode, RiskService, RiskServiceConfig, RiskServiceError,
};
pub use strategy::{
    BookImbalancePaperStrategy, BookImbalancePaperStrategyConfig,
    BookImbalancePaperStrategyConfigError, BookImbalancePaperStrategyConfigInput,
    BoundedOrderIntentIterator, BoundedOrderIntents, MAX_STRATEGY_ORDER_INTENTS, ManualPaperDraft,
    ManualPaperDraftError, ManualPaperDraftIngress, ManualPaperDraftInput, ManualPaperIngressError,
    ManualPaperStrategy, ModelDecisionMapper, ModelInferencePath, ModelStrategy,
    NativeModelInferencePath, PAPER_BOOK_IMBALANCE_INTENT_LIFETIME_NANOS,
    PAPER_BOOK_IMBALANCE_MAXIMUM_SLIPPAGE_BASIS_POINTS, PAPER_BOOK_IMBALANCE_ORDER_QUANTITY_LOTS,
    Strategy, StrategyContext, StrategyError, StrategyNoAction, StrategyNoActionDomain,
    StrategyNoActionPhase,
};
pub use task_reaper::{
    ExecutionTask, ExecutionTaskDrain, ExecutionTaskPermit, ExecutionTaskReaper,
    ExecutionTaskReaperError,
};
