//! Production ownership boundary for risk-enforced local paper execution.
//!
//! This module composes the sealed production source, action-enabled live runtime, canonical
//! account coordinator, execution dispatcher, and realistic paper worker. It owns every spawned
//! worker and defines the only supported shutdown order: source, live actors, dispatcher, then
//! paper execution.

mod audit;
#[cfg(feature = "release-evidence")]
mod benchmark_support;
mod defaults;
mod supervisor;

use audit::ProductionAuditService;
pub use audit::{
    ProductionAuditBarrierError, ProductionAuditError, ProductionAuditEvidence,
    ProductionAuditShutdown, ProductionAuditShutdownStatus,
};

use std::{
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk_adapter_paper::{
    PaperAccountBootstrap, PaperAccountReplaySnapshot, PaperCheckpointError,
    PaperCheckpointRepository, PaperCheckpointRepositoryError, PaperControlContext,
    PaperControlError, PaperExecutionConfig, PaperExecutionRuntime, PaperExecutionSnapshot,
    PaperStartError,
};
use market_squawk_analytics::RequiredLiveFeature;
use market_squawk_domain::OrderId;
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountCoordinatorError, AccountRecoveryBootstrap,
    AccountRiskCoordinator, CancelReceipt, ExecutionAdapter, ExecutionAuditConfig,
    ExecutionAuditError, ExecutionAuditWriter, ExecutionDispatchError, ExecutionDispatcher,
    ExecutionDispatcherConfig, ExecutionDispatcherError, ExecutionDispatcherQuiesce,
    ExecutionDispatcherShutdown, ExecutionLiveActionHook, ExecutionLiveActionHookError,
    ExecutionMarketSink, ExecutionState, ExecutionTaskDrain, ExecutionTaskReaper,
    ExecutionTaskReaperError, PortfolioReadCapability, ReconciledOrderStatus,
    RecoveredDispatchOrder, RiskLimits, RiskService, RiskServiceConfig, RiskServiceError, Strategy,
};
use market_squawk_live::{
    ActionAuthorityIssueLimit, LiveActionHook, LiveRouteConfig, LiveRuntimeConfig,
    LiveSnapshotReader, RouteActionHook, RouteActionHookError, RouteQualifiedMarketExport,
    ShardKey,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    ProductionLiveSourceComposition, ProductionLiveSourceRuntime, ProductionLiveSourceRuntimeError,
};
use supervisor::{PaperFinancialSupervisor, PaperFinancialSupervisorShutdown};

#[cfg(feature = "release-evidence")]
pub(crate) use benchmark_support::{
    ReleaseLatencyDistribution, ReleaseMeasuredOutcomeLedger, ReleasePaperBotBenchmarkComposition,
    ReleasePaperBotBenchmarkResult,
};

const PRODUCTION_EXECUTION_TASK_CAPACITY: usize = 3;

#[derive(Debug)]
struct ProductionPaperRecovery {
    orders: Vec<RecoveredDispatchOrder>,
    quarantined: bool,
}

pub(crate) use defaults::local_paper_bot_with_provider_rate;
pub use defaults::{local_coinbase_paper_bot, local_paper_bot};
#[cfg(test)]
pub(crate) use defaults::{
    local_kraken_paper_bot_with_strategy_for_test, local_paper_portfolio_capability_for_test,
};

/// Frozen bounded account, risk, dispatch, and paper-worker inputs for one production run.
#[derive(Debug)]
pub struct ProductionPaperBotExecutionConfig {
    pub account_coordinator: AccountCoordinatorConfig,
    pub accounts: Vec<AccountBootstrap>,
    pub portfolio: PortfolioReadCapability,
    pub risk_limits: RiskLimits,
    pub risk_service: RiskServiceConfig,
    pub execution_audit: ExecutionAuditConfig,
    pub dispatcher: ExecutionDispatcherConfig,
    pub paper: PaperExecutionConfig,
    pub paper_checkpoint_repository: PaperCheckpointRepository,
    pub audit_directory: cap_std::fs::Dir,
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
    source: PaperBotSourceComposition,
    runtime_config: LiveRuntimeConfig,
    execution: ProductionPaperBotExecutionConfig,
    strategies: Vec<ProductionPaperBotRoute>,
}

#[derive(Debug)]
enum PaperBotSourceComposition {
    Production(Box<ProductionLiveSourceComposition>),
    #[cfg(feature = "release-evidence")]
    ReleaseBenchmark(Vec<LiveRouteConfig>),
}

impl PaperBotSourceComposition {
    fn routes(&self) -> &[LiveRouteConfig] {
        match self {
            Self::Production(source) => source.routes(),
            #[cfg(feature = "release-evidence")]
            Self::ReleaseBenchmark(routes) => routes,
        }
    }

    fn production(&self) -> Option<&ProductionLiveSourceComposition> {
        #[cfg(feature = "release-evidence")]
        {
            match self {
                Self::Production(source) => Some(source),
                Self::ReleaseBenchmark(_) => None,
            }
        }
        #[cfg(not(feature = "release-evidence"))]
        {
            let Self::Production(source) = self;
            Some(source)
        }
    }

    #[cfg(all(test, debug_assertions))]
    fn into_production(self) -> Option<Box<ProductionLiveSourceComposition>> {
        #[cfg(feature = "release-evidence")]
        {
            match self {
                Self::Production(source) => Some(source),
                Self::ReleaseBenchmark(_) => None,
            }
        }
        #[cfg(not(feature = "release-evidence"))]
        {
            let Self::Production(source) = self;
            Some(source)
        }
    }
}

enum PaperBotStartMode {
    Production(Option<Vec<RouteQualifiedMarketExport>>),
    #[cfg(feature = "release-evidence")]
    ReleaseBenchmark(Arc<benchmark_support::ReleaseBenchmarkObserver>),
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
        Self::try_new_inner(
            PaperBotSourceComposition::Production(Box::new(source)),
            runtime_config,
            execution,
            strategies,
        )
    }

    #[cfg(feature = "release-evidence")]
    fn try_new_release_benchmark(
        routes: Vec<LiveRouteConfig>,
        runtime_config: LiveRuntimeConfig,
        execution: ProductionPaperBotExecutionConfig,
        strategies: Vec<ProductionPaperBotRoute>,
    ) -> Result<Self, ProductionPaperBotCompositionError> {
        Self::try_new_inner(
            PaperBotSourceComposition::ReleaseBenchmark(routes),
            runtime_config,
            execution,
            strategies,
        )
    }

    fn try_new_inner(
        source: PaperBotSourceComposition,
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

    /// Returns the complete immutable route set used to create bounded post-action exports.
    pub fn live_routes(&self) -> &[LiveRouteConfig] {
        self.source.routes()
    }

    /// Returns the admitted conservative retained-byte charge ceiling for one source message.
    pub const fn maximum_message_bytes(&self) -> NonZeroU32 {
        self.runtime_config.maximum_message_bytes()
    }

    #[cfg(all(test, debug_assertions))]
    pub(crate) fn with_local_kraken_endpoint_for_test(
        mut self,
        endpoint: &str,
    ) -> Result<Self, crate::ProductionLiveSourceCompositionError> {
        let source = self
            .source
            .into_production()
            .ok_or(crate::ProductionLiveSourceCompositionError::RouteSetMismatch)?;
        self.source = PaperBotSourceComposition::Production(Box::new(
            (*source).with_local_kraken_endpoint_for_test(endpoint)?,
        ));
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
        Ok(self
            .start_inner(PaperBotStartMode::Production(None), cancellation)
            .await?
            .runtime)
    }

    /// Starts the complete paper path with one bounded qualified-market export per live route.
    ///
    /// Export route ownership is validated before any paper, audit, dispatcher, supervisor, live,
    /// or provider worker starts. Receiver ownership remains with the caller that created each
    /// route export; this composition transfers only the sender side into the live runtime.
    ///
    /// # Errors
    ///
    /// Returns an exact route-set or ordinary startup failure. If a later worker has already
    /// started, the existing inspected rollback contract remains in force.
    pub async fn start_with_qualified_market_exports(
        self,
        qualified_market_exports: Vec<RouteQualifiedMarketExport>,
        cancellation: CancellationToken,
    ) -> Result<ProductionPaperBotRuntime, ProductionPaperBotStartError> {
        let source = self
            .source
            .production()
            .ok_or(ProductionPaperBotStartError::InvalidRecoveryOwnership)?;
        source
            .validate_qualified_market_export_routes(&qualified_market_exports)
            .map_err(ProductionPaperBotStartError::Source)?;
        Ok(self
            .start_inner(
                PaperBotStartMode::Production(Some(qualified_market_exports)),
                cancellation,
            )
            .await?
            .runtime)
    }

    async fn start_inner(
        self,
        mode: PaperBotStartMode,
        cancellation: CancellationToken,
    ) -> Result<StartedPaperBotRuntime, ProductionPaperBotStartError> {
        let Self {
            source,
            runtime_config,
            execution,
            strategies,
        } = self;
        let startup_deadline = tokio::time::Instant::now()
            .checked_add(execution.paper_control_timeout)
            .ok_or(ProductionPaperBotStartError::InvalidRecoveryOwnership)?;
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
        let (accounts, mut paper, dispatcher_recovery) = match recovery {
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
                let recovered_orders = checkpoint
                    .recovered_dispatch_orders()
                    .map_err(ProductionPaperBotStartError::RecoveryCheckpoint)?;
                let quarantined = checkpoint.reconciliation_required();
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
                (
                    accounts,
                    paper,
                    Some(ProductionPaperRecovery {
                        orders: recovered_orders,
                        quarantined,
                    }),
                )
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
                (accounts, paper, None)
            }
        };
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
        let audit_service = match ProductionAuditService::try_start(
            execution.audit_directory,
            execution_audit_reader,
            paper_audit_reader,
            execution.paper_control_timeout,
        ) {
            Ok(service) => service,
            Err(error) => {
                let startup = ProductionPaperBotStartError::Audit(error);
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
        if let Err(error) = checkpoint_repository.mark_run_dirty() {
            let startup = ProductionPaperBotStartError::CheckpointRepository(error);
            drop(execution_audit);
            let rollback = rollback_execution_with_audit(
                None,
                None,
                paper,
                task_reaper,
                execution.paper_control_timeout,
                audit_service,
            )
            .await;
            return Err(with_rollback(startup, rollback));
        }
        let dispatcher_recovery = match dispatcher_recovery {
            Some(recovery) => {
                let control = match paper_control_before(startup_deadline) {
                    Ok(control) => control,
                    Err(error) => {
                        let startup = ProductionPaperBotStartError::RecoveryInitialization(error);
                        drop(execution_audit);
                        let rollback = rollback_execution_with_audit(
                            None,
                            None,
                            paper,
                            task_reaper,
                            execution.paper_control_timeout,
                            audit_service,
                        )
                        .await;
                        return Err(with_rollback(startup, rollback));
                    }
                };
                let initialization = match paper.initialize_recovery(control).await {
                    Ok(initialization) => initialization,
                    Err(error) => {
                        let startup = ProductionPaperBotStartError::RecoveryInitialization(
                            ProductionPaperCheckpointError::Control(error),
                        );
                        drop(execution_audit);
                        let rollback = rollback_execution_with_audit(
                            None,
                            None,
                            paper,
                            task_reaper,
                            execution.paper_control_timeout,
                            audit_service,
                        )
                        .await;
                        return Err(with_rollback(startup, rollback));
                    }
                };
                if let Err(error) = audit_service.flush(startup_deadline).await {
                    let startup = ProductionPaperBotStartError::AuditBarrier(error);
                    drop(execution_audit);
                    let rollback = rollback_execution_with_audit(
                        None,
                        None,
                        paper,
                        task_reaper,
                        execution.paper_control_timeout,
                        audit_service,
                    )
                    .await;
                    return Err(with_rollback(startup, rollback));
                }
                Some((recovery, initialization.sequence()))
            }
            None => None,
        };
        let (dispatcher_result, recovered_quarantine, recovered_order_ownership) =
            match dispatcher_recovery {
                Some((recovery, runtime_sequence))
                    if recovery.quarantined || !recovery.orders.is_empty() =>
                {
                    let recovered_order_ownership = !recovery.orders.is_empty();
                    (
                        ExecutionDispatcher::try_start_with_recovery(
                            paper_adapter as Arc<dyn ExecutionAdapter>,
                            Arc::clone(&accounts),
                            execution_audit.clone(),
                            execution.dispatcher,
                            task_reaper.clone(),
                            runtime_sequence,
                            recovery.orders,
                        ),
                        recovery.quarantined,
                        recovered_order_ownership,
                    )
                }
                Some((_recovery, _runtime_sequence)) => (
                    ExecutionDispatcher::try_start(
                        paper_adapter as Arc<dyn ExecutionAdapter>,
                        Arc::clone(&accounts),
                        execution_audit.clone(),
                        execution.dispatcher,
                        task_reaper.clone(),
                    ),
                    false,
                    false,
                ),
                None => (
                    ExecutionDispatcher::try_start(
                        paper_adapter as Arc<dyn ExecutionAdapter>,
                        Arc::clone(&accounts),
                        execution_audit.clone(),
                        execution.dispatcher,
                        task_reaper.clone(),
                    ),
                    false,
                    false,
                ),
            };
        let dispatcher = match dispatcher_result {
            Ok(dispatcher) => Arc::new(dispatcher),
            Err(error) => {
                let startup = ProductionPaperBotStartError::Dispatcher(error);
                drop(execution_audit);
                let rollback = rollback_execution_with_audit(
                    None,
                    None,
                    paper,
                    task_reaper,
                    execution.paper_control_timeout,
                    audit_service,
                )
                .await;
                return Err(with_rollback(startup, rollback));
            }
        };
        if recovered_quarantine {
            let recovery =
                tokio::time::timeout_at(startup_deadline, dispatcher.recover_quarantined())
                    .await
                    .map_err(|_| {
                        market_squawk_execution::ExecutionDispatchError::OperationDeadlineExceeded
                    })
                    .and_then(|result| result);
            if let Err(error) = recovery {
                let startup = ProductionPaperBotStartError::RecoveryRevalidation(error);
                drop(execution_audit);
                let rollback = rollback_execution_with_audit(
                    Some(dispatcher),
                    None,
                    paper,
                    task_reaper,
                    execution.paper_control_timeout,
                    audit_service,
                )
                .await;
                return Err(with_rollback(startup, rollback));
            }
            if let Err(error) = audit_service.flush(startup_deadline).await {
                let startup = ProductionPaperBotStartError::AuditBarrier(error);
                drop(execution_audit);
                let rollback = rollback_execution_with_audit(
                    Some(dispatcher),
                    None,
                    paper,
                    task_reaper,
                    execution.paper_control_timeout,
                    audit_service,
                )
                .await;
                return Err(with_rollback(startup, rollback));
            }
        }
        if (recovered_order_ownership || recovered_quarantine)
            && let Err(error) = persist_paper_checkpoint(
                &dispatcher,
                &accounts,
                &account_ids,
                &paper,
                &mut checkpoint_repository,
                &audit_service,
                startup_deadline,
            )
            .await
        {
            let startup = ProductionPaperBotStartError::RecoveryFinalization(error);
            drop(execution_audit);
            let rollback = rollback_execution_with_audit(
                Some(dispatcher),
                None,
                paper,
                task_reaper,
                execution.paper_control_timeout,
                audit_service,
            )
            .await;
            return Err(with_rollback(startup, rollback));
        }
        if (recovered_order_ownership || recovered_quarantine)
            && let Err(error) = checkpoint_repository.mark_run_dirty()
        {
            let startup = ProductionPaperBotStartError::CheckpointRepository(error);
            drop(execution_audit);
            let rollback = rollback_execution_with_audit(
                Some(dispatcher),
                None,
                paper,
                task_reaper,
                execution.paper_control_timeout,
                audit_service,
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
                drop(execution_audit);
                let rollback = rollback_execution_with_audit(
                    Some(dispatcher),
                    None,
                    paper,
                    task_reaper,
                    execution.paper_control_timeout,
                    audit_service,
                )
                .await;
                return Err(with_rollback(startup, rollback));
            }
        };
        #[cfg(feature = "release-evidence")]
        let benchmark_observer = match &mode {
            PaperBotStartMode::Production(_) => None,
            PaperBotStartMode::ReleaseBenchmark(observer) => Some(Arc::clone(observer)),
        };
        let mut action_hooks = Vec::new();
        if action_hooks.try_reserve_exact(strategies.len()).is_err() {
            let startup = ProductionPaperBotStartError::Allocation;
            drop(execution_audit);
            let rollback = rollback_execution_with_audit(
                Some(dispatcher),
                Some(supervisor),
                paper,
                task_reaper,
                execution.paper_control_timeout,
                audit_service,
            )
            .await;
            return Err(with_rollback(startup, rollback));
        }
        for route in strategies {
            let risk = match RiskService::try_new(
                Arc::clone(&accounts),
                execution.portfolio.clone(),
                execution.risk_limits.clone(),
                execution_audit.clone(),
                execution.risk_service,
            ) {
                Ok(risk) => risk,
                Err(error) => {
                    let startup = ProductionPaperBotStartError::Risk(error);
                    drop(execution_audit);
                    let rollback = rollback_execution_with_audit(
                        Some(dispatcher),
                        Some(supervisor),
                        paper,
                        task_reaper,
                        execution.paper_control_timeout,
                        audit_service,
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
                    drop(execution_audit);
                    let rollback = rollback_execution_with_audit(
                        Some(dispatcher),
                        Some(supervisor),
                        paper,
                        task_reaper,
                        execution.paper_control_timeout,
                        audit_service,
                    )
                    .await;
                    return Err(with_rollback(startup, rollback));
                }
            };
            let hook: Box<dyn LiveActionHook> = {
                #[cfg(feature = "release-evidence")]
                if let Some(observer) = benchmark_observer.as_ref() {
                    Box::new(benchmark_support::ObservedExecutionHook::new(
                        hook,
                        Arc::clone(observer),
                    ))
                } else {
                    Box::new(hook)
                }
                #[cfg(not(feature = "release-evidence"))]
                {
                    Box::new(hook)
                }
            };
            let route_hook =
                match RouteActionHook::try_new(route.route, hook, route.required_features) {
                    Ok(hook) => hook,
                    Err(error) => {
                        let startup = ProductionPaperBotStartError::RouteHook(error);
                        drop(execution_audit);
                        let rollback = rollback_execution_with_audit(
                            Some(dispatcher),
                            Some(supervisor),
                            paper,
                            task_reaper,
                            execution.paper_control_timeout,
                            audit_service,
                        )
                        .await;
                        return Err(with_rollback(startup, rollback));
                    }
                };
            action_hooks.push(route_hook);
        }
        let live_result = match (source, mode) {
            (
                PaperBotSourceComposition::Production(source),
                PaperBotStartMode::Production(Some(exports)),
            ) => (*source)
                .start_with_action_hooks_and_qualified_market_exports(
                    runtime_config,
                    action_hooks,
                    exports,
                    cancellation,
                )
                .await
                .map(StartedPaperBotLiveRuntime::production),
            (
                PaperBotSourceComposition::Production(source),
                PaperBotStartMode::Production(None),
            ) => (*source)
                .start_with_action_hooks(runtime_config, action_hooks, cancellation)
                .await
                .map(StartedPaperBotLiveRuntime::production),
            #[cfg(feature = "release-evidence")]
            (
                PaperBotSourceComposition::ReleaseBenchmark(routes),
                PaperBotStartMode::ReleaseBenchmark(observer),
            ) => benchmark_support::ReleaseBenchmarkLiveRuntime::start(
                runtime_config,
                routes,
                action_hooks,
                observer,
                cancellation,
            )
            .await
            .map(StartedPaperBotLiveRuntime::release_benchmark),
            #[allow(unreachable_patterns)]
            _ => Err(ProductionLiveSourceRuntimeError::QualifiedMarketExportRouteSetMismatch),
        };
        let live = match live_result {
            Ok(live) => live,
            Err(error) => {
                let startup = ProductionPaperBotStartError::Source(error);
                drop(execution_audit);
                let rollback = rollback_execution_with_audit(
                    Some(dispatcher),
                    Some(supervisor),
                    paper,
                    task_reaper,
                    execution.paper_control_timeout,
                    audit_service,
                )
                .await;
                return Err(with_rollback(startup, rollback));
            }
        };
        #[cfg(feature = "release-evidence")]
        let benchmark_producer = live.benchmark_producer;
        let live = live.live;
        Ok(StartedPaperBotRuntime {
            runtime: ProductionPaperBotRuntime {
                live,
                dispatcher,
                accounts,
                account_ids,
                supervisor,
                paper,
                checkpoint_repository,
                task_reaper,
                paper_control_timeout: execution.paper_control_timeout,
                audit_service,
            },
            #[cfg(feature = "release-evidence")]
            benchmark_producer,
        })
    }
}

#[derive(Debug)]
struct StartedPaperBotRuntime {
    runtime: ProductionPaperBotRuntime,
    #[cfg(feature = "release-evidence")]
    benchmark_producer: Option<benchmark_support::ReleaseBenchmarkProducer>,
}

/// Sole owner of all workers in one production paper-bot run.
#[derive(Debug)]
pub struct ProductionPaperBotRuntime {
    live: PaperBotLiveRuntime,
    dispatcher: Arc<ExecutionDispatcher>,
    accounts: Arc<AccountRiskCoordinator>,
    account_ids: Vec<market_squawk_domain::AccountId>,
    supervisor: PaperFinancialSupervisor,
    paper: PaperExecutionRuntime,
    checkpoint_repository: PaperCheckpointRepository,
    task_reaper: ExecutionTaskReaper,
    paper_control_timeout: Duration,
    audit_service: ProductionAuditService,
}

#[derive(Debug)]
enum PaperBotLiveRuntime {
    Production(ProductionLiveSourceRuntime),
    #[cfg(feature = "release-evidence")]
    ReleaseBenchmark(benchmark_support::ReleaseBenchmarkLiveRuntime),
}

#[derive(Debug)]
struct StartedPaperBotLiveRuntime {
    live: PaperBotLiveRuntime,
    #[cfg(feature = "release-evidence")]
    benchmark_producer: Option<benchmark_support::ReleaseBenchmarkProducer>,
}

impl StartedPaperBotLiveRuntime {
    fn production(runtime: ProductionLiveSourceRuntime) -> Self {
        Self {
            live: PaperBotLiveRuntime::Production(runtime),
            #[cfg(feature = "release-evidence")]
            benchmark_producer: None,
        }
    }

    #[cfg(feature = "release-evidence")]
    fn release_benchmark(
        (runtime, producer): (
            benchmark_support::ReleaseBenchmarkLiveRuntime,
            benchmark_support::ReleaseBenchmarkProducer,
        ),
    ) -> Self {
        Self {
            live: PaperBotLiveRuntime::ReleaseBenchmark(runtime),
            benchmark_producer: Some(producer),
        }
    }
}

impl PaperBotLiveRuntime {
    fn snapshots(&self) -> LiveSnapshotReader {
        match self {
            Self::Production(runtime) => runtime.snapshots(),
            #[cfg(feature = "release-evidence")]
            Self::ReleaseBenchmark(runtime) => runtime.snapshots(),
        }
    }

    async fn shutdown(self) -> Result<(), ProductionLiveSourceRuntimeError> {
        match self {
            Self::Production(runtime) => runtime.shutdown().await,
            #[cfg(feature = "release-evidence")]
            Self::ReleaseBenchmark(runtime) => runtime.shutdown().await,
        }
    }
}

impl ProductionPaperBotRuntime {
    /// Returns authority-free immutable market snapshots.
    pub fn snapshots(&self) -> LiveSnapshotReader {
        self.live.snapshots()
    }

    /// Reports whether durable paper state and account-risk authority share one current sequence.
    pub fn financial_reconciliation_current(&self) -> bool {
        self.accounts.reconciliation_fence().is_current()
    }

    /// Returns a complete paper state image without exposing the paper adapter.
    ///
    /// The effective deadline is the earlier of the caller deadline and the startup-configured
    /// paper control bound.
    ///
    /// # Errors
    ///
    /// Returns [`ProductionPaperControlError::Cancelled`] when caller cancellation wins,
    /// [`ProductionPaperControlError::DeadlineExceeded`] when either deadline expires, or the
    /// bounded paper-worker failure otherwise.
    pub async fn paper_snapshot(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PaperExecutionSnapshot, ProductionPaperControlError> {
        let deadline =
            bounded_paper_control_deadline(self.paper_control_timeout, deadline, cancellation)?;
        let control = PaperControlContext::try_new_before(
            tokio::time::Instant::from_std(deadline),
            cancellation.child_token(),
        )?;
        let adapter = self.paper.adapter();
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ProductionPaperControlError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                Err(ProductionPaperControlError::DeadlineExceeded)
            }
            result = adapter.snapshot(control) => result.map_err(Into::into),
        }
    }

    /// Cancels one accepted order already tracked by the risk-enforced dispatcher.
    ///
    /// Caller cancellation or deadline expiry never transfers execution authority. If either wins
    /// after lifecycle admission, the dispatcher's armed fail-safe marks the order for
    /// reconciliation before this future returns.
    ///
    /// # Errors
    ///
    /// Returns a bounded control error or the exact dispatcher rejection. An untracked order is
    /// rejected by the dispatcher and no adapter-specific cancellation surface is available.
    pub async fn cancel_tracked_order(
        &self,
        order_id: OrderId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CancelReceipt, ProductionPaperControlError> {
        let deadline =
            bounded_paper_control_deadline(self.paper_control_timeout, deadline, cancellation)?;
        await_paper_dispatch(
            self.dispatcher.cancel_before(
                order_id,
                tokio::time::Instant::from_std(deadline),
                cancellation,
            ),
            deadline,
            cancellation,
        )
        .await
    }

    /// Reconciles the authoritative backend state for dispatcher-tracked paper orders.
    ///
    /// # Errors
    ///
    /// Returns a bounded control error or the exact dispatcher reconciliation failure. In-flight
    /// work remains owned by the dispatcher's fail-safe when caller cancellation or expiry wins.
    pub async fn reconcile_tracked_orders(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ExecutionState, ProductionPaperControlError> {
        let deadline =
            bounded_paper_control_deadline(self.paper_control_timeout, deadline, cancellation)?;
        await_paper_dispatch(
            self.dispatcher
                .reconcile_before(tokio::time::Instant::from_std(deadline), cancellation),
            deadline,
            cancellation,
        )
        .await
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
            audit_service,
        } = self;
        let source_and_live = live.shutdown().await;
        let supervisor = supervisor.shutdown().await;
        let financial_deadline = tokio::time::Instant::now().checked_add(paper_control_timeout);
        let (checkpoint, dispatcher_quiesce, dispatcher) = match Arc::try_unwrap(dispatcher) {
            Ok(mut dispatcher) => {
                let quiesce = dispatcher.quiesce().await;
                let checkpoint = match (quiesce, financial_deadline) {
                    (
                        ExecutionDispatcherQuiesce::Complete
                        | ExecutionDispatcherQuiesce::AlreadyQuiesced,
                        Some(deadline),
                    ) => {
                        persist_paper_checkpoint(
                            &dispatcher,
                            &accounts,
                            &account_ids,
                            &paper,
                            &mut checkpoint_repository,
                            &audit_service,
                            deadline,
                        )
                        .await
                    }
                    (
                        ExecutionDispatcherQuiesce::Complete
                        | ExecutionDispatcherQuiesce::AlreadyQuiesced,
                        None,
                    ) => Err(ProductionPaperCheckpointError::SettlementDeadlineExceeded),
                    _ => Err(ProductionPaperCheckpointError::DispatcherNotQuiescent),
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
        let recovery_content = match checkpoint.as_ref() {
            Ok(checkpoint) => {
                verify_stabilized_checkpoint(&paper, paper_control_timeout, *checkpoint).await
            }
            Err(_) => Err(ProductionPaperCheckpointError::FinalContentMismatch),
        };
        let paper = shutdown_paper(paper, paper_control_timeout).await;
        let task_drain = drain_execution_tasks(&task_reaper, paper_control_timeout).await;
        let producers_complete = supervisor.is_complete()
            && dispatcher == ExecutionDispatcherShutdown::Complete
            && paper.is_ok()
            && task_drain.is_complete();
        let audit_deadline = tokio::time::Instant::now()
            .checked_add(paper_control_timeout)
            .unwrap_or_else(tokio::time::Instant::now);
        let audit = audit_service
            .shutdown(audit_deadline, producers_complete)
            .await;
        ProductionPaperBotShutdown {
            source_and_live,
            supervisor,
            dispatcher_quiesce,
            checkpoint,
            recovery_content,
            dispatcher,
            paper,
            task_drain,
            audit,
        }
    }
}

/// Bounded production paper snapshot or dispatcher-control failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProductionPaperControlError {
    #[error("production paper control operation was cancelled")]
    Cancelled,
    #[error("production paper control operation exceeded its deadline")]
    DeadlineExceeded,
    #[error(transparent)]
    Paper(PaperControlError),
    #[error(transparent)]
    Dispatch(ExecutionDispatchError),
}

impl From<PaperControlError> for ProductionPaperControlError {
    fn from(error: PaperControlError) -> Self {
        match error {
            PaperControlError::Cancelled => Self::Cancelled,
            PaperControlError::InvalidDeadline | PaperControlError::DeadlineExceeded => {
                Self::DeadlineExceeded
            }
            error => Self::Paper(error),
        }
    }
}

impl From<ExecutionDispatchError> for ProductionPaperControlError {
    fn from(error: ExecutionDispatchError) -> Self {
        match error {
            ExecutionDispatchError::OperationDeadlineExceeded => Self::DeadlineExceeded,
            ExecutionDispatchError::OperationCancelled => Self::Cancelled,
            error => Self::Dispatch(error),
        }
    }
}

/// Fully inspected shutdown result, including durable drain of both audit streams.
#[derive(Debug)]
pub struct ProductionPaperBotShutdown {
    source_and_live: Result<(), ProductionLiveSourceRuntimeError>,
    supervisor: PaperFinancialSupervisorShutdown,
    dispatcher_quiesce: ExecutionDispatcherQuiesce,
    checkpoint: Result<ProductionPaperCheckpointEvidence, ProductionPaperCheckpointError>,
    recovery_content: Result<[u8; 32], ProductionPaperCheckpointError>,
    dispatcher: ExecutionDispatcherShutdown,
    paper: Result<PaperExecutionSnapshot, PaperControlError>,
    task_drain: ExecutionTaskDrain,
    audit: ProductionAuditShutdown,
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
            && matches!(
                (&self.checkpoint, &self.recovery_content),
                (Ok(checkpoint), Ok(recovery_digest))
                    if checkpoint.recovery_digest() == *recovery_digest
            )
            && self.paper.as_ref().is_ok_and(|paper| {
                paper.complete()
                    && self
                        .checkpoint
                        .as_ref()
                        .is_ok_and(|checkpoint| checkpoint.sequence() == paper.sequence())
            })
            && self.task_drain.is_complete()
            && self.audit.is_complete()
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

    pub const fn checkpoint(
        &self,
    ) -> &Result<ProductionPaperCheckpointEvidence, ProductionPaperCheckpointError> {
        &self.checkpoint
    }

    pub const fn recovery_content(&self) -> &Result<[u8; 32], ProductionPaperCheckpointError> {
        &self.recovery_content
    }

    pub const fn paper(&self) -> &Result<PaperExecutionSnapshot, PaperControlError> {
        &self.paper
    }

    pub const fn task_drain(&self) -> ExecutionTaskDrain {
        self.task_drain
    }

    pub const fn audit(&self) -> &ProductionAuditShutdown {
        &self.audit
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
    Audit(ProductionAuditError),
    #[error(transparent)]
    AuditBarrier(ProductionAuditBarrierError),
    #[error(transparent)]
    CheckpointRepository(PaperCheckpointRepositoryError),
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
    #[error(transparent)]
    RecoveryCheckpoint(PaperCheckpointError),
    #[error("paper recovery evidence could not be admitted after audit ownership")]
    RecoveryInitialization(#[source] ProductionPaperCheckpointError),
    #[error("dispatcher-owned paper recovery could not clear durable quarantine")]
    RecoveryRevalidation(#[source] market_squawk_execution::ExecutionDispatchError),
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
    audit: Option<ProductionAuditShutdown>,
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
                .is_ok_and(|paper| paper.complete() && !paper.reconciliation_required())
            && self.task_drain.is_complete()
            && self
                .audit
                .as_ref()
                .is_none_or(ProductionAuditShutdown::is_complete)
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

    pub const fn audit(&self) -> Option<&ProductionAuditShutdown> {
        self.audit.as_ref()
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
    #[error("paper settlement did not complete before the shutdown deadline")]
    SettlementDeadlineExceeded,
    #[error("paper checkpoint bounded allocation failed")]
    Allocation,
    #[error("persisted checkpoint sequence did not match the final paper checkpoint")]
    FinalSequenceMismatch,
    #[error("durable paper checkpoint content did not match the stabilized runtime recovery image")]
    FinalContentMismatch,
    #[error(transparent)]
    Control(#[from] PaperControlError),
    #[error(transparent)]
    Repository(#[from] PaperCheckpointRepositoryError),
    #[error(transparent)]
    AuditBarrier(#[from] ProductionAuditBarrierError),
    #[error(transparent)]
    Checkpoint(#[from] market_squawk_adapter_paper::PaperCheckpointError),
    #[error(transparent)]
    Dispatch(#[from] market_squawk_execution::ExecutionDispatchError),
    #[error(transparent)]
    Idempotency(#[from] market_squawk_execution::AccountIdempotencySnapshotError),
    #[error(transparent)]
    AccountRecovery(#[from] market_squawk_execution::AccountRecoverySnapshotError),
}

/// Exact durable identity of the post-acknowledgement paper recovery image.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProductionPaperCheckpointEvidence {
    generation: std::num::NonZeroU64,
    sequence: u64,
    recovery_digest: [u8; 32],
    artifact_digest: [u8; 32],
}

impl ProductionPaperCheckpointEvidence {
    pub const fn generation(self) -> std::num::NonZeroU64 {
        self.generation
    }

    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    pub const fn recovery_digest(self) -> [u8; 32] {
        self.recovery_digest
    }

    pub const fn artifact_digest(self) -> [u8; 32] {
        self.artifact_digest
    }
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
        audit: None,
    }
}

async fn rollback_execution_with_audit(
    dispatcher: Option<Arc<ExecutionDispatcher>>,
    supervisor: Option<PaperFinancialSupervisor>,
    paper: PaperExecutionRuntime,
    task_reaper: ExecutionTaskReaper,
    paper_control_timeout: Duration,
    audit: ProductionAuditService,
) -> ProductionPaperBotRollback {
    let mut rollback = rollback_execution(
        dispatcher,
        supervisor,
        paper,
        task_reaper,
        paper_control_timeout,
    )
    .await;
    let producers_complete = rollback
        .dispatcher
        .is_none_or(|status| status == ExecutionDispatcherShutdown::Complete)
        && rollback
            .supervisor
            .is_none_or(PaperFinancialSupervisorShutdown::is_complete)
        && rollback.paper.is_ok()
        && rollback.task_drain.is_complete();
    let deadline = tokio::time::Instant::now()
        .checked_add(paper_control_timeout)
        .unwrap_or_else(tokio::time::Instant::now);
    rollback.audit = Some(audit.shutdown(deadline, producers_complete).await);
    rollback
}

async fn persist_paper_checkpoint(
    dispatcher: &ExecutionDispatcher,
    accounts: &AccountRiskCoordinator,
    account_ids: &[market_squawk_domain::AccountId],
    paper: &PaperExecutionRuntime,
    repository: &mut PaperCheckpointRepository,
    audit: &ProductionAuditService,
    deadline: tokio::time::Instant,
) -> Result<ProductionPaperCheckpointEvidence, ProductionPaperCheckpointError> {
    settle_paper_accounts(dispatcher, accounts.reconciliation_fence(), deadline).await?;
    let control = paper_control_before(deadline)?;
    let adapter = paper.adapter();
    let checkpoint = adapter.checkpoint(control).await?;
    if checkpoint.has_nonterminal_orders()
        || checkpoint.reconciliation_required()
        || !accounts.reconciliation_fence().is_current()
    {
        return Err(ProductionPaperCheckpointError::UnsettledFinancialState);
    }
    let _audit_evidence = audit.flush(deadline).await?;
    let replay = account_replay(accounts, account_ids)?;
    let receipt = repository.persist_with_replay(&checkpoint, &replay)?;
    let persisted_sequence = receipt.sequence();
    if persisted_sequence != checkpoint.sequence() {
        return Err(ProductionPaperCheckpointError::FinalSequenceMismatch);
    }
    let authority = dispatcher.persistence_acknowledgement()?;
    tokio::time::timeout_at(
        deadline,
        adapter.acknowledge_persistence(authority, receipt),
    )
    .await
    .map_err(|_| ProductionPaperCheckpointError::SettlementDeadlineExceeded)??;

    let control = paper_control_before(deadline)?;
    let stabilized = adapter.checkpoint(control).await?;
    if stabilized.has_nonterminal_orders()
        || stabilized.reconciliation_required()
        || !accounts.reconciliation_fence().is_current()
    {
        return Err(ProductionPaperCheckpointError::UnsettledFinancialState);
    }
    let _audit_evidence = audit.flush(deadline).await?;
    let replay = account_replay(accounts, account_ids)?;
    let receipt = repository.persist_stabilized_with_replay(&stabilized, &replay)?;
    if tokio::time::Instant::now() >= deadline {
        return Err(ProductionPaperCheckpointError::SettlementDeadlineExceeded);
    }
    let recovery_digest = stabilized.recovery_digest()?;
    if receipt.sequence() != stabilized.sequence()
        || receipt.recovery_digest() != recovery_digest
        || receipt.artifact_digest() != recovery_digest
    {
        return Err(ProductionPaperCheckpointError::FinalContentMismatch);
    }
    Ok(ProductionPaperCheckpointEvidence {
        generation: receipt.generation(),
        sequence: receipt.sequence(),
        recovery_digest,
        artifact_digest: receipt.artifact_digest(),
    })
}

fn account_replay(
    accounts: &AccountRiskCoordinator,
    account_ids: &[market_squawk_domain::AccountId],
) -> Result<Vec<PaperAccountReplaySnapshot>, ProductionPaperCheckpointError> {
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
    Ok(replay)
}

async fn verify_stabilized_checkpoint(
    paper: &PaperExecutionRuntime,
    timeout: Duration,
    expected: ProductionPaperCheckpointEvidence,
) -> Result<[u8; 32], ProductionPaperCheckpointError> {
    let control = PaperControlContext::try_new(timeout, CancellationToken::new())?;
    let checkpoint = paper.adapter().checkpoint(control).await?;
    let digest = checkpoint.recovery_digest()?;
    if checkpoint.has_nonterminal_orders()
        || checkpoint.reconciliation_required()
        || checkpoint.sequence() != expected.sequence()
        || digest != expected.recovery_digest()
        || digest != expected.artifact_digest()
    {
        return Err(ProductionPaperCheckpointError::FinalContentMismatch);
    }
    Ok(digest)
}

async fn settle_paper_accounts(
    dispatcher: &ExecutionDispatcher,
    fence: market_squawk_execution::AccountRiskReconciliationFence,
    deadline: tokio::time::Instant,
) -> Result<(), ProductionPaperCheckpointError> {
    let initial = match tokio::time::timeout_at(deadline, dispatcher.reconcile())
        .await
        .map_err(|_| ProductionPaperCheckpointError::SettlementDeadlineExceeded)?
    {
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
            tokio::time::timeout_at(deadline, dispatcher.cancel(order_id))
                .await
                .map_err(|_| ProductionPaperCheckpointError::SettlementDeadlineExceeded)??;
        }
        if !state.orders().is_empty() {
            loop {
                let terminal = tokio::time::timeout_at(deadline, dispatcher.reconcile())
                    .await
                    .map_err(|_| ProductionPaperCheckpointError::SettlementDeadlineExceeded)??;
                if !terminal.orders().iter().any(|order| {
                    matches!(
                        order.status(),
                        ReconciledOrderStatus::Open | ReconciledOrderStatus::PartiallyFilled
                    )
                }) {
                    break;
                }
                let now = tokio::time::Instant::now();
                let wake = now
                    .checked_add(Duration::from_millis(1))
                    .map_or(deadline, |wake| wake.min(deadline));
                if wake >= deadline {
                    return Err(ProductionPaperCheckpointError::SettlementDeadlineExceeded);
                }
                tokio::time::sleep_until(wake).await;
            }
        }
    }
    if !fence.is_current() {
        tokio::time::timeout_at(deadline, dispatcher.reconcile_accounts())
            .await
            .map_err(|_| ProductionPaperCheckpointError::SettlementDeadlineExceeded)??;
    }
    if !fence.is_current() {
        return Err(ProductionPaperCheckpointError::UnsettledFinancialState);
    }
    Ok(())
}

fn paper_control_before(
    deadline: tokio::time::Instant,
) -> Result<PaperControlContext, ProductionPaperCheckpointError> {
    let remaining = deadline
        .checked_duration_since(tokio::time::Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(ProductionPaperCheckpointError::SettlementDeadlineExceeded)?;
    Ok(PaperControlContext::try_new(
        remaining,
        CancellationToken::new(),
    )?)
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

fn bounded_paper_control_deadline(
    maximum: Duration,
    caller_deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Instant, ProductionPaperControlError> {
    if cancellation.is_cancelled() {
        return Err(ProductionPaperControlError::Cancelled);
    }
    let now = Instant::now();
    if caller_deadline <= now {
        return Err(ProductionPaperControlError::DeadlineExceeded);
    }
    let configured_deadline = now
        .checked_add(maximum)
        .ok_or(ProductionPaperControlError::DeadlineExceeded)?;
    Ok(caller_deadline.min(configured_deadline))
}

async fn await_paper_dispatch<T, F>(
    operation: F,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<T, ProductionPaperControlError>
where
    F: Future<Output = Result<T, ExecutionDispatchError>>,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ProductionPaperControlError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(ProductionPaperControlError::DeadlineExceeded)
        }
        result = operation => result.map_err(Into::into),
    }
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
