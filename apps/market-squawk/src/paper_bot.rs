//! Production ownership boundary for risk-enforced local paper execution.
//!
//! This module composes the sealed production source, action-enabled live runtime, canonical
//! account coordinator, execution dispatcher, and realistic paper worker. It owns every spawned
//! worker and defines the only supported shutdown order: source, live actors, dispatcher, then
//! paper execution.

mod defaults;
mod supervisor;

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use market_squawk_adapter_paper::{
    PaperAccountBootstrap, PaperAccountReplaySnapshot, PaperAuditReader, PaperCheckpointRepository,
    PaperCheckpointRepositoryError, PaperControlContext, PaperControlError, PaperExecutionConfig,
    PaperExecutionRuntime, PaperExecutionSnapshot, PaperStartError,
};
use market_squawk_analytics::RequiredLiveFeature;
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountCoordinatorError, AccountRecoveryBootstrap,
    AccountRiskCoordinator, ExecutionAdapter, ExecutionAuditConfig, ExecutionAuditError,
    ExecutionAuditReader, ExecutionAuditWriter, ExecutionDispatcher, ExecutionDispatcherConfig,
    ExecutionDispatcherError, ExecutionDispatcherQuiesce, ExecutionDispatcherShutdown,
    ExecutionLiveActionHook, ExecutionLiveActionHookError, ExecutionMarketSink, ExecutionTaskDrain,
    ExecutionTaskReaper, ExecutionTaskReaperError, ReconciledOrderStatus, RiskLimits, RiskService,
    RiskServiceConfig, RiskServiceError, Strategy,
};
use market_squawk_live::{
    ActionAuthorityIssueLimit, LiveRouteConfig, LiveRuntimeConfig, LiveSnapshotReader,
    RouteActionHook, RouteActionHookError, ShardKey,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    ProductionLiveSourceComposition, ProductionLiveSourceRuntime, ProductionLiveSourceRuntimeError,
};
use supervisor::{PaperFinancialSupervisor, PaperFinancialSupervisorShutdown};

const PRODUCTION_EXECUTION_TASK_CAPACITY: usize = 3;

#[cfg(test)]
pub(crate) use defaults::local_kraken_paper_bot_with_strategy_for_test;
pub use defaults::{local_coinbase_paper_bot, local_paper_bot};

/// Frozen bounded account, risk, dispatch, and paper-worker inputs for one production run.
#[derive(Debug)]
pub struct ProductionPaperBotExecutionConfig {
    pub account_coordinator: AccountCoordinatorConfig,
    pub accounts: Vec<AccountBootstrap>,
    pub risk_limits: RiskLimits,
    pub risk_service: RiskServiceConfig,
    pub execution_audit: ExecutionAuditConfig,
    pub dispatcher: ExecutionDispatcherConfig,
    pub paper: PaperExecutionConfig,
    pub paper_checkpoint_repository: PaperCheckpointRepository,
    pub paper_accounts: Vec<PaperAccountBootstrap>,
    pub paper_control_timeout: Duration,
}

/// One route-owned strategy and its exact live readiness requirements.
#[derive(Debug)]
pub struct ProductionPaperBotRoute {
    route: ShardKey,
    strategy: Box<dyn Strategy>,
    required_features: Vec<RequiredLiveFeature>,
    maximum_intents: ActionAuthorityIssueLimit,
}

impl ProductionPaperBotRoute {
    /// Transfers one non-shareable strategy into its exact production route.
    pub fn new(
        route: ShardKey,
        strategy: Box<dyn Strategy>,
        required_features: Vec<RequiredLiveFeature>,
        maximum_intents: ActionAuthorityIssueLimit,
    ) -> Self {
        Self {
            route,
            strategy,
            required_features,
            maximum_intents,
        }
    }

    /// Returns the exact route that will own this strategy.
    pub const fn route(&self) -> &ShardKey {
        &self.route
    }
}

/// Validated, pre-network production paper-bot composition.
#[derive(Debug)]
pub struct ProductionPaperBotComposition {
    source: ProductionLiveSourceComposition,
    runtime_config: LiveRuntimeConfig,
    execution: ProductionPaperBotExecutionConfig,
    strategies: Vec<ProductionPaperBotRoute>,
}

impl ProductionPaperBotComposition {
    /// Validates route ownership and canonical paper/risk account parity before spawning workers.
    ///
    /// # Errors
    ///
    /// Rejects incomplete or duplicate route ownership, a zero control deadline, or divergent
    /// paper and risk account bootstraps.
    pub fn try_new(
        source: ProductionLiveSourceComposition,
        runtime_config: LiveRuntimeConfig,
        execution: ProductionPaperBotExecutionConfig,
        strategies: Vec<ProductionPaperBotRoute>,
    ) -> Result<Self, ProductionPaperBotCompositionError> {
        validate_strategy_routes(source.routes(), &strategies)?;
        if execution.paper_control_timeout.is_zero() {
            return Err(ProductionPaperBotCompositionError::ZeroPaperControlTimeout);
        }
        validate_canonical_accounts(&execution.accounts, &execution.paper_accounts)?;
        Ok(Self {
            source,
            runtime_config,
            execution,
            strategies,
        })
    }

    #[cfg(all(test, debug_assertions))]
    pub(crate) fn with_local_kraken_endpoint_for_test(
        mut self,
        endpoint: &str,
    ) -> Result<Self, crate::ProductionLiveSourceCompositionError> {
        self.source = self.source.with_local_kraken_endpoint_for_test(endpoint)?;
        Ok(self)
    }

    /// Starts paper state, dispatch, per-route risk hooks, live actors, and only then the source.
    ///
    /// # Errors
    ///
    /// Returns the exact startup failure. If a worker had already started, incomplete rollback is
    /// retained in [`ProductionPaperBotStartError::Rollback`].
    pub async fn start(
        self,
        cancellation: CancellationToken,
    ) -> Result<ProductionPaperBotRuntime, ProductionPaperBotStartError> {
        let Self {
            source,
            runtime_config,
            execution,
            strategies,
        } = self;
        let (execution_audit, execution_audit_reader) =
            ExecutionAuditWriter::try_new(execution.execution_audit)
                .map_err(ProductionPaperBotStartError::ExecutionAudit)?;
        let task_capacity = NonZeroUsize::new(PRODUCTION_EXECUTION_TASK_CAPACITY)
            .ok_or(ProductionPaperBotStartError::Allocation)?;
        let task_reaper = ExecutionTaskReaper::try_new(task_capacity)
            .map_err(ProductionPaperBotStartError::TaskOwnership)?;
        let mut account_ids = Vec::new();
        account_ids
            .try_reserve_exact(execution.accounts.len())
            .map_err(|_| ProductionPaperBotStartError::Allocation)?;
        account_ids.extend(execution.accounts.iter().map(|account| account.account_id));
        let mut checkpoint_repository = execution.paper_checkpoint_repository;
        let recovery = checkpoint_repository.take_recovery();
        let (accounts, mut paper, recovered_nonterminal_orders) = match recovery {
            Some(recovery) => {
                let (checkpoint, recovered_accounts) = recovery.into_parts();
                if recovered_accounts.len() != execution.accounts.len()
                    || recovered_accounts.iter().any(|recovered| {
                        !execution.accounts.iter().any(|configured| {
                            configured.account_id == recovered.state().account_id()
                        })
                    })
                {
                    return Err(ProductionPaperBotStartError::InvalidRecoveryOwnership);
                }
                let recovered_nonterminal_orders = checkpoint.has_nonterminal_orders();
                let sequence = checkpoint.sequence();
                let account_bootstraps = recovered_accounts.into_vec().into_iter().map(|account| {
                    let (state, idempotency) = account.into_parts();
                    AccountRecoveryBootstrap { state, idempotency }
                });
                let accounts = Arc::new(
                    AccountRiskCoordinator::try_new_from_recovery(
                        execution.account_coordinator,
                        account_bootstraps,
                        sequence,
                    )
                    .map_err(ProductionPaperBotStartError::Accounts)?,
                );
                let paper =
                    PaperExecutionRuntime::try_start_from_checkpoint_with_reconciliation_fence(
                        execution.paper,
                        checkpoint,
                        &checkpoint_repository,
                        task_reaper.clone(),
                        accounts.reconciliation_fence(),
                    )
                    .map_err(ProductionPaperBotStartError::Paper)?;
                (accounts, paper, recovered_nonterminal_orders)
            }
            None => {
                let accounts = Arc::new(
                    AccountRiskCoordinator::try_new(
                        execution.account_coordinator,
                        execution.accounts,
                    )
                    .map_err(ProductionPaperBotStartError::Accounts)?,
                );
                let paper = PaperExecutionRuntime::try_start_with_reconciliation_fence(
                    execution.paper,
                    execution.paper_accounts,
                    &checkpoint_repository,
                    task_reaper.clone(),
                    accounts.reconciliation_fence(),
                )
                .map_err(ProductionPaperBotStartError::Paper)?;
                (accounts, paper, false)
            }
        };
        if recovered_nonterminal_orders {
            let terminalization = match PaperControlContext::try_new(
                execution.paper_control_timeout,
                CancellationToken::new(),
            ) {
                Ok(control) => paper.terminalize_recovered_orders(control).await,
                Err(error) => Err(error),
            };
            if let Err(error) = terminalization {
                let startup = ProductionPaperBotStartError::RecoveryControl(error);
                let rollback = rollback_execution(
                    None,
                    None,
                    paper,
                    task_reaper,
                    execution.paper_control_timeout,
                )
                .await;
                return Err(with_rollback(startup, rollback));
            }
        }
        let paper_audit_reader = match paper.take_audit_reader() {
            Some(reader) => reader,
            None => {
                let startup = ProductionPaperBotStartError::MissingPaperAuditReader;
                let rollback = rollback_execution(
                    None,
                    None,
                    paper,
                    task_reaper,
                    execution.paper_control_timeout,
                )
                .await;
                return Err(with_rollback(startup, rollback));
            }
        };
        let financial_changes = match paper.take_financial_change_reader() {
            Some(reader) => reader,
            None => {
                let startup = ProductionPaperBotStartError::MissingPaperFinancialChangeReader;
                let rollback = rollback_execution(
                    None,
                    None,
                    paper,
                    task_reaper,
                    execution.paper_control_timeout,
                )
                .await;
                return Err(with_rollback(startup, rollback));
            }
        };
        let paper_adapter = paper.adapter();
        let paper_market = paper.market_ingress();
        let dispatcher = match ExecutionDispatcher::try_start(
            paper_adapter as Arc<dyn ExecutionAdapter>,
            Arc::clone(&accounts),
            execution_audit.clone(),
            execution.dispatcher,
            task_reaper.clone(),
        ) {
            Ok(dispatcher) => Arc::new(dispatcher),
            Err(error) => {
                let startup = ProductionPaperBotStartError::Dispatcher(error);
                let rollback = rollback_execution(
                    None,
                    None,
                    paper,
                    task_reaper,
                    execution.paper_control_timeout,
                )
                .await;
                return Err(with_rollback(startup, rollback));
            }
        };
        if recovered_nonterminal_orders
            && let Err(error) = persist_paper_checkpoint(
                &dispatcher,
                &accounts,
                &account_ids,
                &paper,
                &mut checkpoint_repository,
                execution.paper_control_timeout,
            )
            .await
        {
            let startup = ProductionPaperBotStartError::RecoveryFinalization(error);
            let rollback = rollback_execution(
                Some(dispatcher),
                None,
                paper,
                task_reaper,
                execution.paper_control_timeout,
            )
            .await;
            return Err(with_rollback(startup, rollback));
        }
        let supervisor = match PaperFinancialSupervisor::try_start(
            financial_changes,
            Arc::clone(&dispatcher),
            accounts.reconciliation_fence(),
            &task_reaper,
        ) {
            Ok(supervisor) => supervisor,
            Err(error) => {
                let startup = ProductionPaperBotStartError::TaskOwnership(error);
                let rollback = rollback_execution(
                    Some(dispatcher),
                    None,
                    paper,
                    task_reaper,
                    execution.paper_control_timeout,
                )
                .await;
                return Err(with_rollback(startup, rollback));
            }
        };
        let mut action_hooks = Vec::new();
        if action_hooks.try_reserve_exact(strategies.len()).is_err() {
            let startup = ProductionPaperBotStartError::Allocation;
            let rollback = rollback_execution(
                Some(dispatcher),
                Some(supervisor),
                paper,
                task_reaper,
                execution.paper_control_timeout,
            )
            .await;
            return Err(with_rollback(startup, rollback));
        }
        for route in strategies {
            let risk = match RiskService::try_new(
                Arc::clone(&accounts),
                execution.risk_limits.clone(),
                execution_audit.clone(),
                execution.risk_service,
            ) {
                Ok(risk) => risk,
                Err(error) => {
                    let startup = ProductionPaperBotStartError::Risk(error);
                    let rollback = rollback_execution(
                        Some(dispatcher),
                        Some(supervisor),
                        paper,
                        task_reaper,
                        execution.paper_control_timeout,
                    )
                    .await;
                    return Err(with_rollback(startup, rollback));
                }
            };
            let hook = match ExecutionLiveActionHook::try_new(
                route.strategy,
                risk,
                dispatcher.handle(),
                Arc::clone(&paper_market) as Arc<dyn ExecutionMarketSink>,
                route.maximum_intents,
            ) {
                Ok(hook) => hook,
                Err(error) => {
                    let startup = ProductionPaperBotStartError::ExecutionHook(error);
                    let rollback = rollback_execution(
                        Some(dispatcher),
                        Some(supervisor),
                        paper,
                        task_reaper,
                        execution.paper_control_timeout,
                    )
                    .await;
                    return Err(with_rollback(startup, rollback));
                }
            };
            let route_hook = match RouteActionHook::try_new(
                route.route,
                Box::new(hook),
                route.required_features,
            ) {
                Ok(hook) => hook,
                Err(error) => {
                    let startup = ProductionPaperBotStartError::RouteHook(error);
                    let rollback = rollback_execution(
                        Some(dispatcher),
                        Some(supervisor),
                        paper,
                        task_reaper,
                        execution.paper_control_timeout,
                    )
                    .await;
                    return Err(with_rollback(startup, rollback));
                }
            };
            action_hooks.push(route_hook);
        }
        let live = match source
            .start_with_action_hooks(runtime_config, action_hooks, cancellation)
            .await
        {
            Ok(live) => live,
            Err(error) => {
                let startup = ProductionPaperBotStartError::Source(error);
                let rollback = rollback_execution(
                    Some(dispatcher),
                    Some(supervisor),
                    paper,
                    task_reaper,
                    execution.paper_control_timeout,
                )
                .await;
                return Err(with_rollback(startup, rollback));
            }
        };
        Ok(ProductionPaperBotRuntime {
            live,
            dispatcher,
            accounts,
            account_ids,
            supervisor,
            paper,
            checkpoint_repository,
            task_reaper,
            paper_control_timeout: execution.paper_control_timeout,
            execution_audit_reader,
            paper_audit_reader,
        })
    }
}

/// Sole owner of all workers in one production paper-bot run.
#[derive(Debug)]
pub struct ProductionPaperBotRuntime {
    live: ProductionLiveSourceRuntime,
    dispatcher: Arc<ExecutionDispatcher>,
    accounts: Arc<AccountRiskCoordinator>,
    account_ids: Vec<market_squawk_domain::AccountId>,
    supervisor: PaperFinancialSupervisor,
    paper: PaperExecutionRuntime,
    checkpoint_repository: PaperCheckpointRepository,
    task_reaper: ExecutionTaskReaper,
    paper_control_timeout: Duration,
    execution_audit_reader: ExecutionAuditReader,
    paper_audit_reader: PaperAuditReader,
}

impl ProductionPaperBotRuntime {
    /// Returns authority-free immutable market snapshots.
    pub fn snapshots(&self) -> LiveSnapshotReader {
        self.live.snapshots()
    }

    /// Returns the sole bounded execution-audit consumer for out-of-hot-path persistence.
    pub const fn execution_audit_reader(&mut self) -> &mut ExecutionAuditReader {
        &mut self.execution_audit_reader
    }

    /// Returns the sole bounded paper-audit consumer for out-of-hot-path persistence.
    pub const fn paper_audit_reader(&mut self) -> &mut PaperAuditReader {
        &mut self.paper_audit_reader
    }

    /// Stops the source and live actors before dispatch and paper execution, attempting every
    /// barrier even when an earlier barrier fails.
    pub async fn shutdown(self) -> ProductionPaperBotShutdown {
        let Self {
            live,
            dispatcher,
            accounts,
            account_ids,
            supervisor,
            paper,
            mut checkpoint_repository,
            task_reaper,
            paper_control_timeout,
            execution_audit_reader,
            paper_audit_reader,
        } = self;
        let source_and_live = live.shutdown().await;
        let supervisor = supervisor.shutdown().await;
        let (checkpoint, dispatcher_quiesce, dispatcher) = match Arc::try_unwrap(dispatcher) {
            Ok(mut dispatcher) => {
                let quiesce = dispatcher.quiesce().await;
                let checkpoint = if matches!(
                    quiesce,
                    ExecutionDispatcherQuiesce::Complete
                        | ExecutionDispatcherQuiesce::AlreadyQuiesced
                ) {
                    persist_paper_checkpoint(
                        &dispatcher,
                        &accounts,
                        &account_ids,
                        &paper,
                        &mut checkpoint_repository,
                        paper_control_timeout,
                    )
                    .await
                } else {
                    Err(ProductionPaperCheckpointError::DispatcherNotQuiescent)
                };
                let shutdown = dispatcher.shutdown().await;
                (checkpoint, quiesce, shutdown)
            }
            Err(_) => (
                Err(ProductionPaperCheckpointError::DispatcherOwnership),
                ExecutionDispatcherQuiesce::Incomplete,
                ExecutionDispatcherShutdown::Incomplete,
            ),
        };
        let paper = shutdown_paper(paper, paper_control_timeout).await;
        let task_drain = drain_execution_tasks(&task_reaper, paper_control_timeout).await;
        ProductionPaperBotShutdown {
            source_and_live,
            supervisor,
            dispatcher_quiesce,
            checkpoint,
            dispatcher,
            paper,
            task_drain,
            execution_audit_reader,
            paper_audit_reader,
        }
    }
}

/// Fully inspected shutdown result, including sole ownership of both audit consumers.
#[derive(Debug)]
pub struct ProductionPaperBotShutdown {
    source_and_live: Result<(), ProductionLiveSourceRuntimeError>,
    supervisor: PaperFinancialSupervisorShutdown,
    dispatcher_quiesce: ExecutionDispatcherQuiesce,
    checkpoint: Result<u64, ProductionPaperCheckpointError>,
    dispatcher: ExecutionDispatcherShutdown,
    paper: Result<PaperExecutionSnapshot, PaperControlError>,
    task_drain: ExecutionTaskDrain,
    execution_audit_reader: ExecutionAuditReader,
    paper_audit_reader: PaperAuditReader,
}

impl ProductionPaperBotShutdown {
    /// Reports whether every lifecycle barrier completed and the paper snapshot is complete.
    pub fn is_complete(&self) -> bool {
        self.source_and_live.is_ok()
            && self.supervisor.is_complete()
            && matches!(
                self.dispatcher_quiesce,
                ExecutionDispatcherQuiesce::Complete | ExecutionDispatcherQuiesce::AlreadyQuiesced
            )
            && self.dispatcher == ExecutionDispatcherShutdown::Complete
            && self.paper.as_ref().is_ok_and(|paper| {
                paper.complete()
                    && self
                        .checkpoint
                        .as_ref()
                        .is_ok_and(|sequence| *sequence == paper.sequence())
            })
            && self.task_drain.is_complete()
    }

    pub const fn source_and_live(&self) -> &Result<(), ProductionLiveSourceRuntimeError> {
        &self.source_and_live
    }

    pub const fn dispatcher(&self) -> ExecutionDispatcherShutdown {
        self.dispatcher
    }

    pub const fn dispatcher_quiesce(&self) -> ExecutionDispatcherQuiesce {
        self.dispatcher_quiesce
    }

    pub const fn supervisor_last_error(
        &self,
    ) -> Option<market_squawk_execution::ExecutionDispatchError> {
        self.supervisor.last_error()
    }

    pub const fn supervisor_reader_closed(&self) -> bool {
        self.supervisor.reader_closed()
    }

    pub const fn checkpoint(&self) -> &Result<u64, ProductionPaperCheckpointError> {
        &self.checkpoint
    }

    pub const fn paper(&self) -> &Result<PaperExecutionSnapshot, PaperControlError> {
        &self.paper
    }

    pub const fn task_drain(&self) -> ExecutionTaskDrain {
        self.task_drain
    }

    /// Returns both sole audit consumers after every producer has stopped.
    pub fn into_audit_readers(self) -> (ExecutionAuditReader, PaperAuditReader) {
        (self.execution_audit_reader, self.paper_audit_reader)
    }
}

/// Production paper-bot validation failure before any worker or network activity starts.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProductionPaperBotCompositionError {
    #[error("production paper bot requires exactly one strategy owner for every source route")]
    StrategyRouteSetMismatch,
    #[error("production paper bot contains duplicate strategy route ownership")]
    DuplicateStrategyRoute,
    #[error("paper control timeout must be positive")]
    ZeroPaperControlTimeout,
    #[error("risk and paper account bootstraps do not describe the same canonical state")]
    AccountBootstrapMismatch,
}

/// Production paper-bot startup failure with inspected rollback when workers had started.
#[derive(Debug, Error)]
pub enum ProductionPaperBotStartError {
    #[error(transparent)]
    Accounts(AccountCoordinatorError),
    #[error(transparent)]
    ExecutionAudit(ExecutionAuditError),
    #[error(transparent)]
    TaskOwnership(ExecutionTaskReaperError),
    #[error(transparent)]
    Paper(PaperStartError),
    #[error("fresh paper runtime did not transfer its sole audit reader")]
    MissingPaperAuditReader,
    #[error("paper runtime did not transfer its sole financial-change reader")]
    MissingPaperFinancialChangeReader,
    #[error("current recovery image cannot restore exact dispatcher order ownership")]
    InvalidRecoveryOwnership,
    #[error("recovered paper orders could not be terminalized before live admission")]
    RecoveryControl(#[source] PaperControlError),
    #[error("terminal recovered paper state could not be published before live admission")]
    RecoveryFinalization(#[source] ProductionPaperCheckpointError),
    #[error(transparent)]
    Dispatcher(ExecutionDispatcherError),
    #[error(transparent)]
    Risk(RiskServiceError),
    #[error(transparent)]
    ExecutionHook(ExecutionLiveActionHookError),
    #[error(transparent)]
    RouteHook(RouteActionHookError),
    #[error("production paper-bot bounded allocation failed")]
    Allocation,
    #[error(transparent)]
    Source(ProductionLiveSourceRuntimeError),
    #[error("production paper-bot startup failed and worker rollback was incomplete")]
    Rollback {
        startup: Box<ProductionPaperBotStartError>,
        rollback: ProductionPaperBotRollback,
    },
}

/// Inspected dispatcher and paper-worker rollback outcome after startup failure.
#[derive(Debug)]
pub struct ProductionPaperBotRollback {
    dispatcher: Option<ExecutionDispatcherShutdown>,
    supervisor: Option<PaperFinancialSupervisorShutdown>,
    paper: Result<PaperExecutionSnapshot, PaperControlError>,
    task_drain: ExecutionTaskDrain,
}

impl ProductionPaperBotRollback {
    pub fn is_complete(&self) -> bool {
        self.dispatcher
            .is_none_or(|status| status == ExecutionDispatcherShutdown::Complete)
            && self
                .supervisor
                .is_none_or(PaperFinancialSupervisorShutdown::is_complete)
            && self
                .paper
                .as_ref()
                .is_ok_and(PaperExecutionSnapshot::complete)
            && self.task_drain.is_complete()
    }

    pub const fn dispatcher(&self) -> Option<ExecutionDispatcherShutdown> {
        self.dispatcher
    }

    pub const fn paper(&self) -> &Result<PaperExecutionSnapshot, PaperControlError> {
        &self.paper
    }

    pub const fn task_drain(&self) -> ExecutionTaskDrain {
        self.task_drain
    }
}

/// Durable paper-checkpoint publication or acknowledgement failure.
#[derive(Debug, Error)]
pub enum ProductionPaperCheckpointError {
    #[error("execution dispatcher did not become quiescent before final reconciliation")]
    DispatcherNotQuiescent,
    #[error("execution dispatcher retained an unexpected owner during final shutdown")]
    DispatcherOwnership,
    #[error("paper financial state did not become terminal and reconciled")]
    UnsettledFinancialState,
    #[error("paper checkpoint bounded allocation failed")]
    Allocation,
    #[error("persisted checkpoint sequence did not match the final paper checkpoint")]
    FinalSequenceMismatch,
    #[error(transparent)]
    Control(#[from] PaperControlError),
    #[error(transparent)]
    Repository(#[from] PaperCheckpointRepositoryError),
    #[error(transparent)]
    Dispatch(#[from] market_squawk_execution::ExecutionDispatchError),
    #[error(transparent)]
    Idempotency(#[from] market_squawk_execution::AccountIdempotencySnapshotError),
    #[error(transparent)]
    AccountRecovery(#[from] market_squawk_execution::AccountRecoverySnapshotError),
}

fn validate_strategy_routes(
    routes: &[LiveRouteConfig],
    strategies: &[ProductionPaperBotRoute],
) -> Result<(), ProductionPaperBotCompositionError> {
    if routes.len() != strategies.len()
        || routes.iter().any(|route| {
            !strategies
                .iter()
                .any(|strategy| strategy.route() == route.route())
        })
    {
        return Err(ProductionPaperBotCompositionError::StrategyRouteSetMismatch);
    }
    for (index, strategy) in strategies.iter().enumerate() {
        if strategies[index.saturating_add(1)..]
            .iter()
            .any(|other| other.route() == strategy.route())
        {
            return Err(ProductionPaperBotCompositionError::DuplicateStrategyRoute);
        }
    }
    Ok(())
}

fn validate_canonical_accounts(
    accounts: &[AccountBootstrap],
    paper_accounts: &[PaperAccountBootstrap],
) -> Result<(), ProductionPaperBotCompositionError> {
    if accounts.len() != paper_accounts.len() {
        return Err(ProductionPaperBotCompositionError::AccountBootstrapMismatch);
    }
    for account in accounts {
        let Some(paper) = paper_accounts
            .iter()
            .find(|candidate| candidate.account_id == account.account_id)
        else {
            return Err(ProductionPaperBotCompositionError::AccountBootstrapMismatch);
        };
        if paper.revision != account.revision
            || paper.eligible != account.eligible
            || paper.cash.as_slice() != [account.cash]
            || paper.capital != account.capital
            || paper.peak_capital != account.peak_capital
            || paper.gross_exposure != account.gross_exposure
            || paper.realized_loss != account.realized_loss
            || paper.realized_pnl != account.realized_pnl
            || paper.positions != account.positions
            || paper.position_cost_basis != account.position_cost_basis
        {
            return Err(ProductionPaperBotCompositionError::AccountBootstrapMismatch);
        }
    }
    Ok(())
}

async fn rollback_execution(
    dispatcher: Option<Arc<ExecutionDispatcher>>,
    supervisor: Option<PaperFinancialSupervisor>,
    paper: PaperExecutionRuntime,
    task_reaper: ExecutionTaskReaper,
    paper_control_timeout: Duration,
) -> ProductionPaperBotRollback {
    let supervisor = match supervisor {
        Some(supervisor) => Some(supervisor.shutdown().await),
        None => None,
    };
    let dispatcher = match dispatcher {
        Some(dispatcher) => Some(match Arc::try_unwrap(dispatcher) {
            Ok(dispatcher) => dispatcher.shutdown().await,
            Err(_) => ExecutionDispatcherShutdown::Incomplete,
        }),
        None => None,
    };
    let paper = shutdown_paper(paper, paper_control_timeout).await;
    let task_drain = drain_execution_tasks(&task_reaper, paper_control_timeout).await;
    ProductionPaperBotRollback {
        dispatcher,
        supervisor,
        paper,
        task_drain,
    }
}

async fn persist_paper_checkpoint(
    dispatcher: &ExecutionDispatcher,
    accounts: &AccountRiskCoordinator,
    account_ids: &[market_squawk_domain::AccountId],
    paper: &PaperExecutionRuntime,
    repository: &mut PaperCheckpointRepository,
    timeout: Duration,
) -> Result<u64, ProductionPaperCheckpointError> {
    settle_paper_accounts(dispatcher, accounts.reconciliation_fence()).await?;
    let control = PaperControlContext::try_new(timeout, CancellationToken::new())?;
    let adapter = paper.adapter();
    let checkpoint = adapter.checkpoint(control).await?;
    if checkpoint.has_nonterminal_orders() || !accounts.reconciliation_fence().is_current() {
        return Err(ProductionPaperCheckpointError::UnsettledFinancialState);
    }
    let mut replay = Vec::new();
    replay
        .try_reserve_exact(account_ids.len())
        .map_err(|_| ProductionPaperCheckpointError::Allocation)?;
    for account_id in account_ids {
        replay.push(PaperAccountReplaySnapshot::from_reconciled_state(
            accounts.snapshot_recovery_state(*account_id)?,
            accounts.snapshot_idempotency(*account_id)?,
        ));
    }
    let receipt = repository.persist_with_replay(&checkpoint, &replay)?;
    let persisted_sequence = receipt.sequence();
    if persisted_sequence != checkpoint.sequence() {
        return Err(ProductionPaperCheckpointError::FinalSequenceMismatch);
    }
    let authority = dispatcher.persistence_acknowledgement()?;
    adapter.acknowledge_persistence(authority, receipt).await?;
    Ok(persisted_sequence)
}

async fn settle_paper_accounts(
    dispatcher: &ExecutionDispatcher,
    fence: market_squawk_execution::AccountRiskReconciliationFence,
) -> Result<(), ProductionPaperCheckpointError> {
    let initial = match dispatcher.reconcile().await {
        Ok(state) => Some(state),
        Err(market_squawk_execution::ExecutionDispatchError::OrderNotTracked) => None,
        Err(error) => return Err(error.into()),
    };
    if let Some(state) = initial {
        let mut open_orders = Vec::new();
        open_orders
            .try_reserve_exact(state.orders().len())
            .map_err(|_| ProductionPaperCheckpointError::Allocation)?;
        open_orders.extend(state.orders().iter().filter_map(|order| {
            matches!(
                order.status(),
                ReconciledOrderStatus::Open | ReconciledOrderStatus::PartiallyFilled
            )
            .then_some(order.order_id())
        }));
        for order_id in open_orders {
            dispatcher.cancel(order_id).await?;
        }
        if !state.orders().is_empty() {
            let terminal = dispatcher.reconcile().await?;
            if terminal.orders().iter().any(|order| {
                matches!(
                    order.status(),
                    ReconciledOrderStatus::Open | ReconciledOrderStatus::PartiallyFilled
                )
            }) {
                return Err(ProductionPaperCheckpointError::UnsettledFinancialState);
            }
        }
    }
    if !fence.is_current() {
        dispatcher.reconcile_accounts().await?;
    }
    if !fence.is_current() {
        return Err(ProductionPaperCheckpointError::UnsettledFinancialState);
    }
    Ok(())
}

async fn drain_execution_tasks(
    task_reaper: &ExecutionTaskReaper,
    timeout: Duration,
) -> ExecutionTaskDrain {
    let now = tokio::time::Instant::now();
    let deadline = match now.checked_add(timeout) {
        Some(deadline) => deadline,
        None => now,
    };
    task_reaper.drain(deadline).await
}

async fn shutdown_paper(
    paper: PaperExecutionRuntime,
    timeout: Duration,
) -> Result<PaperExecutionSnapshot, PaperControlError> {
    let control = PaperControlContext::try_new(timeout, CancellationToken::new())?;
    paper.shutdown(control).await
}

fn with_rollback(
    startup: ProductionPaperBotStartError,
    rollback: ProductionPaperBotRollback,
) -> ProductionPaperBotStartError {
    if rollback.is_complete() {
        startup
    } else {
        ProductionPaperBotStartError::Rollback {
            startup: Box::new(startup),
            rollback,
        }
    }
}
