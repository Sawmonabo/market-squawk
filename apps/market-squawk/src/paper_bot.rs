//! Production ownership boundary for risk-enforced local paper execution.
//!
//! This module composes the sealed production source, action-enabled live runtime, canonical
//! account coordinator, execution dispatcher, and realistic paper worker. It owns every spawned
//! worker and defines the only supported shutdown order: source, live actors, dispatcher, then
//! paper execution.

mod defaults;

use std::{sync::Arc, time::Duration};

use market_squawk_adapter_paper::{
    PaperAccountBootstrap, PaperAuditReader, PaperControlContext, PaperControlError,
    PaperExecutionConfig, PaperExecutionRuntime, PaperExecutionSnapshot, PaperStartError,
};
use market_squawk_analytics::RequiredLiveFeature;
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountCoordinatorError, AccountRiskCoordinator,
    ExecutionAdapter, ExecutionAuditConfig, ExecutionAuditError, ExecutionAuditReader,
    ExecutionAuditWriter, ExecutionDispatcher, ExecutionDispatcherConfig, ExecutionDispatcherError,
    ExecutionDispatcherShutdown, ExecutionLiveActionHook, ExecutionLiveActionHookError,
    ExecutionMarketSink, RiskLimits, RiskService, RiskServiceConfig, RiskServiceError, Strategy,
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

pub use defaults::local_coinbase_paper_bot;

/// Frozen bounded account, risk, dispatch, and paper-worker inputs for one production run.
#[derive(Clone, Debug)]
pub struct ProductionPaperBotExecutionConfig {
    pub account_coordinator: AccountCoordinatorConfig,
    pub accounts: Vec<AccountBootstrap>,
    pub risk_limits: RiskLimits,
    pub risk_service: RiskServiceConfig,
    pub execution_audit: ExecutionAuditConfig,
    pub dispatcher: ExecutionDispatcherConfig,
    pub paper: PaperExecutionConfig,
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
        let accounts = Arc::new(
            AccountRiskCoordinator::try_new(execution.account_coordinator, execution.accounts)
                .map_err(ProductionPaperBotStartError::Accounts)?,
        );
        let (execution_audit, execution_audit_reader) =
            ExecutionAuditWriter::try_new(execution.execution_audit)
                .map_err(ProductionPaperBotStartError::ExecutionAudit)?;
        let mut paper = PaperExecutionRuntime::try_start(execution.paper, execution.paper_accounts)
            .map_err(ProductionPaperBotStartError::Paper)?;
        let paper_audit_reader = match paper.take_audit_reader() {
            Some(reader) => reader,
            None => {
                let startup = ProductionPaperBotStartError::MissingPaperAuditReader;
                let rollback =
                    rollback_execution(None, paper, execution.paper_control_timeout).await;
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
        ) {
            Ok(dispatcher) => dispatcher,
            Err(error) => {
                let startup = ProductionPaperBotStartError::Dispatcher(error);
                let rollback =
                    rollback_execution(None, paper, execution.paper_control_timeout).await;
                return Err(with_rollback(startup, rollback));
            }
        };
        let mut action_hooks = Vec::new();
        if action_hooks.try_reserve_exact(strategies.len()).is_err() {
            let startup = ProductionPaperBotStartError::Allocation;
            let rollback =
                rollback_execution(Some(dispatcher), paper, execution.paper_control_timeout).await;
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
                        paper,
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
                        paper,
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
                        paper,
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
                let rollback =
                    rollback_execution(Some(dispatcher), paper, execution.paper_control_timeout)
                        .await;
                return Err(with_rollback(startup, rollback));
            }
        };
        Ok(ProductionPaperBotRuntime {
            live,
            dispatcher,
            paper,
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
    dispatcher: ExecutionDispatcher,
    paper: PaperExecutionRuntime,
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
            paper,
            paper_control_timeout,
            execution_audit_reader,
            paper_audit_reader,
        } = self;
        let source_and_live = live.shutdown().await;
        let dispatcher = dispatcher.shutdown().await;
        let paper = shutdown_paper(paper, paper_control_timeout).await;
        ProductionPaperBotShutdown {
            source_and_live,
            dispatcher,
            paper,
            execution_audit_reader,
            paper_audit_reader,
        }
    }
}

/// Fully inspected shutdown result, including sole ownership of both audit consumers.
#[derive(Debug)]
pub struct ProductionPaperBotShutdown {
    source_and_live: Result<(), ProductionLiveSourceRuntimeError>,
    dispatcher: ExecutionDispatcherShutdown,
    paper: Result<PaperExecutionSnapshot, PaperControlError>,
    execution_audit_reader: ExecutionAuditReader,
    paper_audit_reader: PaperAuditReader,
}

impl ProductionPaperBotShutdown {
    /// Reports whether every lifecycle barrier completed and the paper snapshot is complete.
    pub fn is_complete(&self) -> bool {
        self.source_and_live.is_ok()
            && self.dispatcher == ExecutionDispatcherShutdown::Complete
            && self
                .paper
                .as_ref()
                .is_ok_and(PaperExecutionSnapshot::complete)
    }

    pub const fn source_and_live(&self) -> &Result<(), ProductionLiveSourceRuntimeError> {
        &self.source_and_live
    }

    pub const fn dispatcher(&self) -> ExecutionDispatcherShutdown {
        self.dispatcher
    }

    pub const fn paper(&self) -> &Result<PaperExecutionSnapshot, PaperControlError> {
        &self.paper
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
    Paper(PaperStartError),
    #[error("fresh paper runtime did not transfer its sole audit reader")]
    MissingPaperAuditReader,
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
    paper: Result<PaperExecutionSnapshot, PaperControlError>,
}

impl ProductionPaperBotRollback {
    pub fn is_complete(&self) -> bool {
        self.dispatcher
            .is_none_or(|status| status == ExecutionDispatcherShutdown::Complete)
            && self
                .paper
                .as_ref()
                .is_ok_and(PaperExecutionSnapshot::complete)
    }

    pub const fn dispatcher(&self) -> Option<ExecutionDispatcherShutdown> {
        self.dispatcher
    }

    pub const fn paper(&self) -> &Result<PaperExecutionSnapshot, PaperControlError> {
        &self.paper
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
    dispatcher: Option<ExecutionDispatcher>,
    paper: PaperExecutionRuntime,
    paper_control_timeout: Duration,
) -> ProductionPaperBotRollback {
    let dispatcher = match dispatcher {
        Some(dispatcher) => Some(dispatcher.shutdown().await),
        None => None,
    };
    let paper = shutdown_paper(paper, paper_control_timeout).await;
    ProductionPaperBotRollback { dispatcher, paper }
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
