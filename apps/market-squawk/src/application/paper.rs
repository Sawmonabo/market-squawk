//! Lifecycle-owned paper bot and execution application services.

mod market;
mod source_lifecycle;
mod source_runtime;

use std::{
    fmt,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use market_squawk_adapter_paper::{
    PaperAccountRiskSnapshot, PaperCashBalance, PaperExecutionSnapshot, PaperFillSnapshot,
    PaperOrderSnapshot, PaperPosition,
};
use market_squawk_decisions::{InvestmentTargetSetId, TargetState, TargetStatus};
use market_squawk_domain::{
    BasisPoints, DigestAlgorithm, Money, OrderId, OrderSide, OrderType, PriceTicks, QuantityLots,
    RevisionNumber, TimeInForce, Timestamp,
};
use market_squawk_execution::{
    CancelReceipt, CancelStatus, ExecutionAdapterError, ExecutionDispatchError, ExecutionState,
    ManualPaperDraft, ManualPaperDraftInput, OrderTargetReference, RiskLimitsSnapshot,
};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use market_squawk_sources::ProviderRateAuthority;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::decision::DecisionApplication;
use super::live_fair_value::{LiveFairValueExportDrains, LiveFairValueObservationBuffer};
use super::source::SourceRuntimeView;
use super::{ApplicationDomainService, effective_service_limits};
use crate::{
    AppConfig, ProductionLiveSourceRuntime, ProductionSourceProvider, ProviderAdapterActivation,
    paper_bot::{
        PaperStrategyMode, ProductionExecutionAuditRecord, ProductionExecutionAuditSnapshot,
        ProductionManualPaperIngressError, ProductionPaperBotRuntime,
        local_coinbase_direct_paper_bot_with_activation_and_strategy_mode,
        local_live_market_with_provider_rate, local_paper_bot_with_provider_rate_and_strategy_mode,
        manual_paper_account_id, manual_paper_reason_code, manual_paper_strategy_id,
    },
};

pub(crate) use source_lifecycle::PaperSourceLifecycleControl;

const BOT_GET_STATUS: &str = "Bot.GetStatus";
const BOT_START: &str = "Bot.Start";
const BOT_STOP: &str = "Bot.Stop";
const RISK_TRIGGER_KILL_SWITCH: &str = "Risk.TriggerKillSwitch";
const EXECUTION_GET_ORDERS: &str = "Execution.GetOrders";
const EXECUTION_GET_FILLS: &str = "Execution.GetFills";
const EXECUTION_CANCEL: &str = "Execution.Cancel";
const EXECUTION_RECONCILE: &str = "Execution.Reconcile";
const EXECUTION_GET_MANUAL_PAPER_TARGETS: &str = "Execution.GetManualPaperTargets";
const EXECUTION_SUBMIT_MANUAL_PAPER_DRAFT: &str = "Execution.SubmitManualPaperDraft";
const MANUAL_PAPER_DRAFT_LIFETIME: Duration = Duration::from_secs(60);
/// Upper bound inherited from the local decision catalog's installed-product capacity.
const MAXIMUM_MANUAL_PAPER_TARGET_INDEX_ENTRIES: usize = 4_096;

/// Shared paper lifecycle exposed as distinct Bot and Execution domain services.
pub struct PaperApplicationServices {
    controller: Arc<PaperController>,
}

/// Exact synchronous paper/execution facts accepted by lifecycle preflight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PaperRuntimeActivitySnapshot {
    paper_execution_active: bool,
    reconciliation_pending: bool,
}

impl PaperRuntimeActivitySnapshot {
    /// Returns whether the paper execution runtime currently owns execution authority.
    pub(crate) const fn paper_execution_active(self) -> bool {
        self.paper_execution_active
    }

    /// Returns whether execution state requires reconciliation.
    pub(crate) const fn reconciliation_pending(self) -> bool {
        self.reconciliation_pending
    }
}

/// Read-only synchronous paper runtime authority used by installed lifecycle preflight.
pub(crate) trait PaperRuntimeActivityAuthority: fmt::Debug + Send + Sync + 'static {
    /// Samples one coherent activity state or fails closed while the owner cannot do so.
    fn activity(&self) -> Result<PaperRuntimeActivitySnapshot, ServiceError>;
}

#[derive(Clone, Debug)]
struct PaperRuntimeActivityControl {
    controller: Arc<PaperController>,
}

impl PaperRuntimeActivityAuthority for PaperRuntimeActivityControl {
    fn activity(&self) -> Result<PaperRuntimeActivitySnapshot, ServiceError> {
        let state = self
            .controller
            .state
            .try_lock()
            .map_err(|_busy| ServiceError::Unavailable)?;
        match &*state {
            PaperState::Stopped { .. } | PaperState::LiveOnly { .. } => {
                Ok(PaperRuntimeActivitySnapshot {
                    paper_execution_active: false,
                    reconciliation_pending: false,
                })
            }
            PaperState::LiveStarting { .. }
            | PaperState::Starting { .. }
            | PaperState::Running { .. }
            | PaperState::Stopping => {
                // The paper adapter's authoritative reconciliation fact is asynchronous. A
                // synchronous preflight must not guess it while a runtime or transition exists.
                Err(ServiceError::Unavailable)
            }
        }
    }
}

impl PaperApplicationServices {
    /// Creates a stopped paper controller from validated effective configuration.
    #[must_use]
    pub fn new(
        config: AppConfig,
        live_fair_value: Arc<LiveFairValueObservationBuffer>,
        provider_rate: ProviderRateAuthority,
        provider_activation: Arc<ProviderAdapterActivation>,
        decisions: Arc<DecisionApplication>,
    ) -> Self {
        Self {
            controller: Arc::new(PaperController::new(
                config,
                live_fair_value,
                provider_rate,
                provider_activation,
                decisions,
            )),
        }
    }

    /// Returns the Bot-domain implementation sharing this sole runtime owner.
    pub fn bot(&self) -> Arc<dyn ApplicationDomainService> {
        Arc::new(BotDomainService {
            controller: Arc::clone(&self.controller),
        })
    }

    /// Returns the Execution-domain implementation sharing this sole runtime owner.
    pub fn execution(&self) -> Arc<dyn ApplicationDomainService> {
        Arc::new(ExecutionDomainService {
            controller: Arc::clone(&self.controller),
        })
    }

    /// Returns the Market-domain implementation sharing this sole runtime owner.
    pub fn market(&self) -> Arc<dyn ApplicationDomainService> {
        Arc::new(market::MarketDomainService::new(Arc::clone(
            &self.controller,
        )))
    }

    /// Returns an authority-free Source-domain view sharing this sole live-runtime owner.
    pub fn source_runtime_view(&self) -> Arc<dyn SourceRuntimeView> {
        Arc::new(source_runtime::PaperSourceRuntimeView::new(Arc::clone(
            &self.controller,
        )))
    }

    /// Returns lifecycle control sharing this exact paper/live runtime owner.
    pub(crate) fn source_lifecycle_control(&self) -> PaperSourceLifecycleControl {
        PaperSourceLifecycleControl::new(Arc::clone(&self.controller))
    }

    /// Returns read-only exact paper/execution activity for installed lifecycle preflight.
    pub(crate) fn runtime_activity_authority(&self) -> Arc<dyn PaperRuntimeActivityAuthority> {
        Arc::new(PaperRuntimeActivityControl {
            controller: Arc::clone(&self.controller),
        })
    }
}

impl fmt::Debug for PaperApplicationServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PaperApplicationServices")
            .field("controller", &self.controller)
            .finish()
    }
}

struct BotDomainService {
    controller: Arc<PaperController>,
}

struct ExecutionDomainService {
    controller: Arc<PaperController>,
}

impl fmt::Debug for BotDomainService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BotDomainService")
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for ExecutionDomainService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionDomainService")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ApplicationDomainService for BotDomainService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Bot
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let limits = effective_service_limits(&request, &context)?;
        let content = match request.name() {
            BOT_GET_STATUS => {
                self.controller
                    .status(&context, limits.maximum_result_items())
                    .await?
            }
            BOT_START => self.controller.start(&request, &context).await?,
            BOT_STOP | RISK_TRIGGER_KILL_SWITCH => {
                let reason = required_string(&request, "reason")?;
                self.controller.stop(reason, &context).await?
            }
            _ => return Err(ServiceError::NotFound),
        };
        TypedToolResult::try_new(
            content,
            1,
            ToolResultMetadata::complete_not_applicable(),
            limits,
        )
        .map_err(Into::into)
    }

    fn begin_shutdown(&self) {
        self.controller.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.controller.finish_shutdown(deadline).await
    }
}

#[async_trait]
impl ApplicationDomainService for ExecutionDomainService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Execution
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let limits = effective_service_limits(&request, &context)?;
        let (content, returned, available) = match request.name() {
            EXECUTION_GET_ORDERS => {
                self.controller
                    .orders(&context, limits.maximum_result_items())
                    .await?
            }
            EXECUTION_GET_FILLS => {
                self.controller
                    .fills(&context, limits.maximum_result_items())
                    .await?
            }
            EXECUTION_GET_MANUAL_PAPER_TARGETS => {
                self.controller
                    .manual_paper_targets(&context, limits.maximum_result_items())
                    .await?
            }
            EXECUTION_SUBMIT_MANUAL_PAPER_DRAFT => {
                self.controller
                    .submit_manual_paper_order(&request, &context)
                    .await?
            }
            EXECUTION_CANCEL => {
                let order = required_string(&request, "orderId")?;
                let order =
                    OrderId::from_str(order).map_err(|_error| ServiceError::InvalidRequest)?;
                let receipt = self.controller.cancel(order, &context).await?;
                (cancel_receipt_value(receipt), 1, 1)
            }
            EXECUTION_RECONCILE => {
                let state = self.controller.reconcile(&context).await?;
                (execution_state_value(&state), 1, 1)
            }
            _ => return Err(ServiceError::NotFound),
        };
        let metadata = if returned < available {
            ToolResultMetadata::try_truncated_not_applicable(available)
                .map_err(|_error| ServiceError::InvalidResult)?
        } else {
            ToolResultMetadata::complete_not_applicable()
        };
        TypedToolResult::try_new(content, returned, metadata, limits).map_err(Into::into)
    }

    fn begin_shutdown(&self) {
        self.controller.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.controller.finish_shutdown(deadline).await
    }
}

struct PaperController {
    config: AppConfig,
    live_fair_value: Arc<LiveFairValueObservationBuffer>,
    provider_rate: ProviderRateAuthority,
    provider_activation: Arc<ProviderAdapterActivation>,
    decisions: Arc<DecisionApplication>,
    accepting: AtomicBool,
    lifecycle: CancellationToken,
    // Serializes every mutation that can replace the sole live/paper runtime owner.
    owner_gate: Mutex<()>,
    state: Mutex<PaperState>,
    restart_request: Mutex<Option<TypedToolRequest>>,
    changed: watch::Sender<u64>,
}

impl PaperController {
    fn new(
        config: AppConfig,
        live_fair_value: Arc<LiveFairValueObservationBuffer>,
        provider_rate: ProviderRateAuthority,
        provider_activation: Arc<ProviderAdapterActivation>,
        decisions: Arc<DecisionApplication>,
    ) -> Self {
        let (changed, _initial_receiver) = watch::channel(0);
        Self {
            config,
            live_fair_value,
            provider_rate,
            provider_activation,
            decisions,
            accepting: AtomicBool::new(true),
            lifecycle: CancellationToken::new(),
            owner_gate: Mutex::new(()),
            state: Mutex::new(PaperState::Stopped {
                last_complete: None,
            }),
            restart_request: Mutex::new(None),
            changed,
        }
    }

    async fn status(
        &self,
        context: &RequestContext,
        maximum_items: usize,
    ) -> Result<Value, ServiceError> {
        ensure_live(context)?;
        let state = bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
        match &*state {
            PaperState::Stopped { last_complete } => Ok(json!({
                "state": "stopped",
                "lastShutdownComplete": last_complete,
            })),
            PaperState::LiveOnly {
                last_paper_complete,
                ..
            } => Ok(json!({
                "state": "stopped",
                "lastShutdownComplete": last_paper_complete,
            })),
            PaperState::Starting { .. } => Ok(json!({"state": "starting"})),
            PaperState::LiveStarting { .. } => Ok(json!({"state": "stopped"})),
            PaperState::Stopping => Ok(json!({"state": "stopping"})),
            PaperState::Running {
                provider,
                strategy_mode,
                runtime,
                exports,
                cancellation,
                ..
            } => {
                if cancellation.is_cancelled() || !runtime.source_is_healthy() {
                    return Ok(json!({
                        "state": "failed",
                        "provider": provider.name(),
                        "requiresStop": true,
                    }));
                }
                if !exports.is_healthy() {
                    return Err(ServiceError::Unavailable);
                }
                let financial_reconciliation_current = runtime.financial_reconciliation_current();
                let snapshot = runtime
                    .paper_snapshot(context.deadline(), context.cancellation())
                    .await
                    .map_err(map_control_error)?;
                let audit = runtime
                    .execution_audit_snapshot(None, maximum_items)
                    .map_err(|_error| ServiceError::Unavailable)?;
                Ok(json!({
                    "state": "running",
                    "strategyMode": strategy_mode.as_str(),
                    "sequence": snapshot.sequence(),
                    "complete": snapshot.complete(),
                    "reconciliationRequired": snapshot.reconciliation_required(),
                    "financialReconciliationCurrent": financial_reconciliation_current,
                    "orders": snapshot.orders().len(),
                    "fills": snapshot.fills().len(),
                    "positions": snapshot.positions().len(),
                    "accounts": bounded_evidence(snapshot.accounts(), maximum_items, account_value)?,
                    "cash": bounded_evidence(snapshot.cash(), maximum_items, cash_value)?,
                    "positionEvidence": bounded_evidence(snapshot.positions(), maximum_items, position_value)?,
                    "configurationDigestSha256": hex(snapshot.configuration_digest()),
                    "simulation": simulation_value(snapshot.simulation()),
                    "reconciliation": reconciliation_value(&snapshot, financial_reconciliation_current),
                    "riskLimits": risk_limits_value(runtime.risk_limits(), maximum_items)?,
                    "riskDecisions": audit_snapshot_value(&audit)?,
                }))
            }
        }
    }

    /// Starts market data without installing strategy, risk, or execution authority.
    async fn start_public_source_owned(
        &self,
        provider: ProductionSourceProvider,
        deadline: Instant,
        request_cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::Unavailable);
        }
        let composition = local_live_market_with_provider_rate(
            self.config.clone(),
            provider,
            self.provider_rate.clone(),
        )
        .map_err(|error| {
            tracing::error!(
                provider = ?provider,
                error = %error,
                "public market source composition failed"
            );
            ServiceError::Unavailable
        })?;
        let run_id = Uuid::new_v4();
        let run_cancellation = self.lifecycle.child_token();
        let last_paper_complete = {
            let mut state = bounded_lock(&self.state, deadline, request_cancellation).await?;
            let last_paper_complete = match &*state {
                PaperState::Stopped { last_complete } => *last_complete,
                PaperState::LiveOnly {
                    provider: current,
                    runtime,
                    exports,
                    cancellation,
                    ..
                } if *current == provider
                    && !cancellation.is_cancelled()
                    && runtime.is_healthy()
                    && exports.is_healthy() =>
                {
                    return Ok(());
                }
                _ => return Err(ServiceError::InvalidRequest),
            };
            *state = PaperState::LiveStarting {
                run_id,
                cancellation: run_cancellation.clone(),
            };
            last_paper_complete
        };
        self.signal_change();

        let (qualified_market_exports, export_drains) = match LiveFairValueExportDrains::try_start(
            composition.live_routes(),
            composition.maximum_message_bytes(),
            Arc::clone(&self.live_fair_value),
            run_cancellation.clone(),
            deadline,
        )
        .await
        {
            Ok(exports) => exports,
            Err(error) => {
                tracing::error!(
                    provider = ?provider,
                    error = %error,
                    "public market export startup failed"
                );
                run_cancellation.cancel();
                self.set_stopped(last_paper_complete).await;
                return Err(ServiceError::Unavailable);
            }
        };
        let result = tokio::select! {
            biased;
            () = request_cancellation.cancelled() => {
                run_cancellation.cancel();
                Err(ServiceError::Cancelled)
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                run_cancellation.cancel();
                Err(ServiceError::DeadlineExceeded)
            }
            result = composition.start_with_qualified_market_exports(
                qualified_market_exports,
                run_cancellation.clone(),
            ) => result.map_err(|error| {
                tracing::error!(
                    provider = ?provider,
                    error = %error,
                    "public market source runtime failed to start"
                );
                ServiceError::Unavailable
            }),
        };
        let exports = PaperRuntimeExports::QualifiedMarket(export_drains);
        match result {
            Ok(runtime) => {
                let mut state = self.state.lock().await;
                let current_start = matches!(
                    &*state,
                    PaperState::LiveStarting {
                        run_id: current,
                        ..
                    } if *current == run_id
                );
                if current_start
                    && self.accepting.load(Ordering::Acquire)
                    && !run_cancellation.is_cancelled()
                    && runtime.is_healthy()
                    && exports.is_healthy()
                {
                    *state = PaperState::LiveOnly {
                        provider,
                        runtime: Box::new(runtime),
                        exports,
                        cancellation: run_cancellation,
                        last_paper_complete,
                    };
                    drop(state);
                    self.signal_change();
                    return Ok(());
                }
                if current_start {
                    *state = PaperState::Stopping;
                }
                drop(state);
                let complete =
                    bounded_live_runtime_shutdown(runtime, exports, deadline, request_cancellation)
                        .await;
                self.set_stopped(last_paper_complete).await;
                if !complete? {
                    return Err(ServiceError::Unavailable);
                }
                Err(ServiceError::Unavailable)
            }
            Err(error) => {
                exports.begin_shutdown();
                run_cancellation.cancel();
                let cleanup = CancellationToken::new();
                let cleanup_deadline = Instant::now()
                    .checked_add(self.config.source_shutdown())
                    .ok_or(ServiceError::Unavailable)?;
                let drains_complete = exports.finish_before(cleanup_deadline, &cleanup).await;
                self.set_stopped(last_paper_complete).await;
                if drains_complete {
                    Err(error)
                } else {
                    Err(ServiceError::Unavailable)
                }
            }
        }
    }

    async fn restore_public_source_owned(
        &self,
        provider: Option<ProductionSourceProvider>,
        last_paper_complete: Option<bool>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        self.set_stopped(last_paper_complete).await;
        if let Some(provider) = provider {
            self.start_public_source_owned(provider, deadline, cancellation)
                .await?;
        }
        Ok(())
    }

    async fn start(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let _owner =
            bounded_lock(&self.owner_gate, context.deadline(), context.cancellation()).await?;
        self.start_paper_before_owned(request, context.deadline(), context.cancellation())
            .await
    }

    async fn start_paper_before_owned(
        &self,
        request: &TypedToolRequest,
        deadline: Instant,
        request_cancellation: &CancellationToken,
    ) -> Result<Value, ServiceError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::Unavailable);
        }
        let provider = PaperProvider::from_request(request)?;
        let strategy_mode = paper_strategy_mode(request)?;
        let initial_cash = required_string(request, "initialCash")?
            .parse::<Decimal>()
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let fee_basis_points = request
            .arguments()
            .get("feeBasisPoints")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(ServiceError::InvalidRequest)?;
        let run_id = Uuid::new_v4();
        let run_cancellation = self.lifecycle.child_token();
        let (restore_public_source, last_paper_complete, retired_live) = {
            let mut state = bounded_lock(&self.state, deadline, request_cancellation).await?;
            let previous = std::mem::replace(
                &mut *state,
                PaperState::Starting {
                    run_id,
                    cancellation: run_cancellation.clone(),
                },
            );
            let (restore_public_source, last_paper_complete, retired_live) = match previous {
                PaperState::Stopped { last_complete } => (None, last_complete, None),
                PaperState::LiveOnly {
                    provider,
                    runtime,
                    exports,
                    cancellation,
                    last_paper_complete,
                } => {
                    cancellation.cancel();
                    (
                        Some(provider),
                        last_paper_complete,
                        Some((*runtime, exports, cancellation)),
                    )
                }
                other => {
                    *state = other;
                    return Err(ServiceError::InvalidRequest);
                }
            };
            *state = PaperState::Starting {
                run_id,
                cancellation: run_cancellation.clone(),
            };
            (restore_public_source, last_paper_complete, retired_live)
        };
        self.signal_change();
        if let Some((runtime, exports, cancellation)) = retired_live {
            match bounded_live_runtime_shutdown(runtime, exports, deadline, request_cancellation)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    self.set_stopped(last_paper_complete).await;
                    return Err(ServiceError::Unavailable);
                }
                Err(error) => {
                    cancellation.cancel();
                    self.set_stopped(last_paper_complete).await;
                    return Err(error);
                }
            }
        }
        *self.restart_request.lock().await = Some(request.clone());

        let composition = match provider {
            PaperProvider::Public(provider) => {
                local_paper_bot_with_provider_rate_and_strategy_mode(
                    self.config.clone(),
                    provider,
                    initial_cash,
                    fee_basis_points,
                    self.provider_rate.clone(),
                    strategy_mode,
                )
                .map_err(|_error| ServiceError::Unavailable)
            }
            PaperProvider::CoinbaseDirect {
                provider_session_id,
            } => {
                tokio::select! {
                    biased;
                    () = request_cancellation.cancelled() => Err(ServiceError::Cancelled),
                    () = tokio::time::sleep_until(
                        tokio::time::Instant::from_std(deadline)
                    ) => Err(ServiceError::DeadlineExceeded),
                    result = local_coinbase_direct_paper_bot_with_activation_and_strategy_mode(
                        self.config.clone(),
                        provider_session_id,
                        initial_cash,
                        fee_basis_points,
                        self.provider_activation.as_ref(),
                        run_cancellation.clone(),
                        strategy_mode,
                    ) => result.map_err(|_error| ServiceError::Unavailable),
                }
            }
        };
        let composition = match composition {
            Ok(composition) => composition,
            Err(error) => {
                run_cancellation.cancel();
                self.restore_public_source_owned(
                    restore_public_source,
                    last_paper_complete,
                    deadline,
                    request_cancellation,
                )
                .await?;
                return Err(error);
            }
        };
        let (result, exports) = match provider {
            PaperProvider::Public(_) => {
                let (qualified_market_exports, export_drains) =
                    match LiveFairValueExportDrains::try_start(
                        composition.live_routes(),
                        composition.maximum_message_bytes(),
                        Arc::clone(&self.live_fair_value),
                        run_cancellation.clone(),
                        deadline,
                    )
                    .await
                    {
                        Ok(exports) => exports,
                        Err(_error) => {
                            run_cancellation.cancel();
                            self.restore_public_source_owned(
                                restore_public_source,
                                last_paper_complete,
                                deadline,
                                request_cancellation,
                            )
                            .await?;
                            return Err(ServiceError::Unavailable);
                        }
                    };
                let result = tokio::select! {
                    biased;
                    () = request_cancellation.cancelled() => {
                        run_cancellation.cancel();
                        Err(ServiceError::Cancelled)
                    }
                    () = tokio::time::sleep_until(
                        tokio::time::Instant::from_std(deadline)
                    ) => {
                        run_cancellation.cancel();
                        Err(ServiceError::DeadlineExceeded)
                    }
                    result = composition.start_with_qualified_market_exports(
                        qualified_market_exports,
                        run_cancellation.clone(),
                    ) => result.map_err(|_error| ServiceError::Unavailable)
                };
                (result, PaperRuntimeExports::QualifiedMarket(export_drains))
            }
            PaperProvider::CoinbaseDirect { .. } => {
                let result = tokio::select! {
                    biased;
                    () = request_cancellation.cancelled() => {
                        run_cancellation.cancel();
                        Err(ServiceError::Cancelled)
                    }
                    () = tokio::time::sleep_until(
                        tokio::time::Instant::from_std(deadline)
                    ) => {
                        run_cancellation.cancel();
                        Err(ServiceError::DeadlineExceeded)
                    }
                    result = composition.start(run_cancellation.clone()) => {
                        result.map_err(|_error| ServiceError::Unavailable)
                    }
                };
                (result, PaperRuntimeExports::DirectExecutionOnly)
            }
        };
        match result {
            Ok(runtime) => {
                let mut state = self.state.lock().await;
                let current_start = matches!(
                    &*state,
                    PaperState::Starting {
                        run_id: current,
                        ..
                    } if *current == run_id
                );
                if current_start
                    && self.accepting.load(Ordering::Acquire)
                    && !run_cancellation.is_cancelled()
                    && runtime.source_is_healthy()
                {
                    *state = PaperState::Running {
                        provider,
                        strategy_mode,
                        runtime: Box::new(runtime),
                        exports,
                        cancellation: run_cancellation,
                        restore_public_source,
                    };
                    drop(state);
                    self.signal_change();
                    return Ok(json!({
                        "state": "running",
                        "provider": provider.name(),
                        "strategyMode": strategy_mode.as_str(),
                    }));
                }
                if current_start {
                    *state = PaperState::Stopping;
                }
                drop(state);
                let shutdown =
                    bounded_runtime_shutdown(runtime, exports, deadline, request_cancellation)
                        .await;
                match shutdown {
                    Ok(true) => {
                        self.restore_public_source_owned(
                            restore_public_source,
                            Some(true),
                            deadline,
                            request_cancellation,
                        )
                        .await?;
                        Err(ServiceError::Unavailable)
                    }
                    Ok(false) => {
                        self.set_stopped(Some(false)).await;
                        Err(ServiceError::Unavailable)
                    }
                    Err(error) => {
                        self.set_stopped(Some(false)).await;
                        Err(error)
                    }
                }
            }
            Err(error) => {
                run_cancellation.cancel();
                exports.begin_shutdown();
                let cleanup = CancellationToken::new();
                let cleanup_deadline = Instant::now()
                    .checked_add(self.config.source_shutdown())
                    .ok_or(ServiceError::Unavailable)?;
                let drains_complete = exports.finish_before(cleanup_deadline, &cleanup).await;
                if drains_complete {
                    self.restore_public_source_owned(
                        restore_public_source,
                        last_paper_complete,
                        deadline,
                        request_cancellation,
                    )
                    .await?;
                    Err(error)
                } else {
                    self.set_stopped(last_paper_complete).await;
                    Err(ServiceError::Unavailable)
                }
            }
        }
    }

    async fn stop(&self, reason: &str, context: &RequestContext) -> Result<Value, ServiceError> {
        let _owner =
            bounded_lock(&self.owner_gate, context.deadline(), context.cancellation()).await?;
        let complete = self
            .stop_paper_before_owned(context.deadline(), context.cancellation())
            .await?;
        if !complete {
            return Err(ServiceError::Unavailable);
        }
        Ok(json!({
            "state": "stopped",
            "shutdownComplete": complete,
            "reason": reason,
        }))
    }

    async fn orders(
        &self,
        context: &RequestContext,
        maximum_items: usize,
    ) -> Result<(Value, usize, usize), ServiceError> {
        let Some(snapshot) = self.read_snapshot(context).await? else {
            return Ok((Value::Null, 0, 0));
        };
        let available = snapshot.orders().len();
        let returned = available.min(maximum_items);
        if returned == 0 {
            return Ok((Value::Null, 0, 0));
        }
        let mut values = Vec::new();
        values
            .try_reserve_exact(returned)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        values.extend(
            snapshot.orders()[..returned]
                .iter()
                .map(|order| order_value(order, snapshot.fills())),
        );
        Ok((Value::Array(values), returned, available))
    }

    async fn fills(
        &self,
        context: &RequestContext,
        maximum_items: usize,
    ) -> Result<(Value, usize, usize), ServiceError> {
        let Some(snapshot) = self.read_snapshot(context).await? else {
            return Ok((Value::Null, 0, 0));
        };
        let available = snapshot.fills().len();
        let returned = available.min(maximum_items);
        if returned == 0 {
            return Ok((Value::Null, 0, 0));
        }
        let mut values = Vec::new();
        values
            .try_reserve_exact(returned)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        values.extend(snapshot.fills()[..returned].iter().copied().map(fill_value));
        Ok((Value::Array(values), returned, available))
    }

    async fn manual_paper_targets(
        &self,
        context: &RequestContext,
        maximum_items: usize,
    ) -> Result<(Value, usize, usize), ServiceError> {
        ensure_live(context)?;
        let state = bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
        let PaperState::Running {
            runtime,
            exports,
            cancellation,
            ..
        } = &*state
        else {
            return Err(ServiceError::Unavailable);
        };
        if cancellation.is_cancelled() || !runtime.source_is_healthy() || !exports.is_healthy() {
            return Err(ServiceError::Unavailable);
        }
        let now = current_timestamp()?;
        let entries = self
            .decisions
            .list_target_index(MAXIMUM_MANUAL_PAPER_TARGET_INDEX_ENTRIES)
            .map_err(map_decision_error)?;
        let mut targets = Vec::new();
        targets
            .try_reserve_exact(entries.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for entry in entries {
            if entry.status() != TargetStatus::Active {
                continue;
            }
            let target = self
                .decisions
                .get_target(entry.id(), entry.revision())
                .map_err(map_decision_error)?;
            if !target_currently_usable(&target, now) {
                continue;
            }
            let Ok(route) = sole_compatible_manual_route(runtime, &target) else {
                continue;
            };
            targets.push(manual_paper_target_value(&target, route)?);
        }
        if targets.len() > maximum_items {
            return Err(ServiceError::ResourceExhausted);
        }
        let count = targets.len();
        Ok((json!({"targets": targets}), count, count))
    }

    async fn submit_manual_paper_order(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<(Value, usize, usize), ServiceError> {
        ensure_live(context)?;
        let target_id = InvestmentTargetSetId::try_new(required_string(request, "targetId")?)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let target_revision = request
            .arguments()
            .get("targetRevision")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .and_then(|value| RevisionNumber::new(value).ok())
            .ok_or(ServiceError::InvalidRequest)?;
        let side = manual_order_side(required_string(request, "side")?)?;
        let order_type = manual_order_type(required_string(request, "orderType")?)?;
        let time_in_force = manual_time_in_force(required_string(request, "timeInForce")?)?;
        let quantity = required_string(request, "quantityLots")?
            .parse::<i64>()
            .map_err(|_error| ServiceError::InvalidRequest)
            .and_then(|lots| {
                QuantityLots::new(lots).map_err(|_error| ServiceError::InvalidRequest)
            })?;
        let state = bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
        let PaperState::Running {
            runtime,
            exports,
            cancellation,
            ..
        } = &*state
        else {
            return Err(ServiceError::Unavailable);
        };
        if cancellation.is_cancelled() || !runtime.source_is_healthy() || !exports.is_healthy() {
            return Err(ServiceError::Unavailable);
        }
        let now = current_timestamp()?;
        let target = self.current_active_target(&target_id, target_revision, now)?;
        let manual_route = sole_compatible_manual_route(runtime, &target)?;
        let route = manual_route.route();
        let terms = manual_route.execution_terms();
        let target_core = target.target().target();
        if target_core.reference_mark().price().currency() != terms.quote_currency() {
            return Err(ServiceError::InvalidRequest);
        }
        let limit_price =
            selected_target_price(request, "limitTargetLevel", &target, terms, order_type)?;
        let stop_price =
            selected_target_price(request, "stopTargetLevel", &target, terms, order_type)?;
        let content_digest = target_core.content_identity().evidence_digest();
        if content_digest.algorithm() != DigestAlgorithm::Sha256 {
            return Err(ServiceError::Unavailable);
        }
        let target_reference = OrderTargetReference::try_new(
            target_core.id().as_str(),
            std::num::NonZeroU64::new(u64::from(target_core.revision().get()))
                .ok_or(ServiceError::Unavailable)?,
            content_digest.bytes(),
        )
        .map_err(|_error| ServiceError::Unavailable)?;
        let expires_at = now
            .checked_add_nanos(
                i64::try_from(MANUAL_PAPER_DRAFT_LIFETIME.as_nanos())
                    .map_err(|_error| ServiceError::Unavailable)?,
            )
            .map_err(|_error| ServiceError::Unavailable)?;
        let order_id =
            OrderId::try_from(Uuid::new_v4()).map_err(|_error| ServiceError::Unavailable)?;
        let client_order_id =
            market_squawk_domain::ClientOrderId::try_from(format!("paper-manual-{order_id}"))
                .map_err(|_error| ServiceError::Unavailable)?;
        let draft = ManualPaperDraft::try_new(ManualPaperDraftInput {
            order_id,
            client_order_id,
            strategy_id: manual_paper_strategy_id().map_err(|_error| ServiceError::Unavailable)?,
            account_id: manual_paper_account_id().map_err(|_error| ServiceError::Unavailable)?,
            side,
            order_type,
            quantity,
            limit_price,
            stop_price,
            time_in_force,
            expires_at,
            reason_code: manual_paper_reason_code().map_err(|_error| ServiceError::Unavailable)?,
            maximum_slippage: BasisPoints::new(100),
            target_reference,
        })
        .map_err(|_error| ServiceError::InvalidRequest)?;
        runtime
            .try_submit_manual_paper_draft(route, draft)
            .map_err(map_manual_paper_ingress_error)?;
        Ok((
            json!({
                "state": "accepted",
                "targetId": target_core.id().as_str(),
                "targetRevision": target_core.revision().get(),
            }),
            1,
            1,
        ))
    }

    fn current_active_target(
        &self,
        target_id: &InvestmentTargetSetId,
        requested_revision: RevisionNumber,
        now: Timestamp,
    ) -> Result<TargetState, ServiceError> {
        let entries = self
            .decisions
            .list_target_index(MAXIMUM_MANUAL_PAPER_TARGET_INDEX_ENTRIES)
            .map_err(map_decision_error)?;
        let entry = entries
            .iter()
            .find(|candidate| {
                candidate.id() == target_id
                    && candidate.revision() == requested_revision
                    && candidate.status() == TargetStatus::Active
            })
            .ok_or(ServiceError::NotFound)?;
        let target = self
            .decisions
            .get_target(entry.id(), entry.revision())
            .map_err(map_decision_error)?;
        if target_currently_usable(&target, now) {
            Ok(target)
        } else {
            Err(ServiceError::InvalidRequest)
        }
    }

    async fn read_snapshot(
        &self,
        context: &RequestContext,
    ) -> Result<Option<PaperExecutionSnapshot>, ServiceError> {
        ensure_live(context)?;
        let state = bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
        let (runtime, exports, cancellation) = match &*state {
            PaperState::Stopped { .. } | PaperState::LiveOnly { .. } => return Ok(None),
            PaperState::Running {
                runtime,
                exports,
                cancellation,
                ..
            } => (runtime, exports, cancellation),
            PaperState::LiveStarting { .. }
            | PaperState::Starting { .. }
            | PaperState::Stopping => {
                return Err(ServiceError::Unavailable);
            }
        };
        if cancellation.is_cancelled() || !runtime.source_is_healthy() || !exports.is_healthy() {
            return Err(ServiceError::Unavailable);
        }
        runtime
            .paper_snapshot(context.deadline(), context.cancellation())
            .await
            .map_err(map_control_error)
            .map(Some)
    }

    async fn cancel(
        &self,
        order_id: OrderId,
        context: &RequestContext,
    ) -> Result<CancelReceipt, ServiceError> {
        ensure_live(context)?;
        let state = bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
        let PaperState::Running {
            runtime,
            exports,
            cancellation,
            ..
        } = &*state
        else {
            return Err(ServiceError::Unavailable);
        };
        if cancellation.is_cancelled() || !runtime.source_is_healthy() || !exports.is_healthy() {
            return Err(ServiceError::Unavailable);
        }
        runtime
            .cancel_tracked_order(order_id, context.deadline(), context.cancellation())
            .await
            .map_err(map_control_error)
    }

    async fn reconcile(&self, context: &RequestContext) -> Result<ExecutionState, ServiceError> {
        ensure_live(context)?;
        let state = bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
        let PaperState::Running {
            runtime,
            exports,
            cancellation,
            ..
        } = &*state
        else {
            return Err(ServiceError::Unavailable);
        };
        if cancellation.is_cancelled() || !runtime.source_is_healthy() || !exports.is_healthy() {
            return Err(ServiceError::Unavailable);
        }
        runtime
            .reconcile_tracked_orders(context.deadline(), context.cancellation())
            .await
            .map_err(map_control_error)
    }

    fn begin_shutdown(&self) {
        if self.accepting.swap(false, Ordering::AcqRel) {
            self.lifecycle.cancel();
        }
    }

    async fn stop_paper_before_owned(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<bool, ServiceError> {
        ensure_before(deadline, cancellation)?;
        let mut state = bounded_lock(&self.state, deadline, cancellation).await?;
        match std::mem::replace(&mut *state, PaperState::Stopping) {
            PaperState::Stopped { last_complete } => {
                *state = PaperState::Stopped { last_complete };
                drop(state);
                *self.restart_request.lock().await = None;
                Ok(last_complete.unwrap_or(true))
            }
            live @ PaperState::LiveOnly { .. } => {
                // `Bot.Stop` owns paper authority only. A source-only runtime is already in the
                // requested paper state and must remain the sole live owner.
                *state = live;
                drop(state);
                *self.restart_request.lock().await = None;
                Ok(true)
            }
            PaperState::Running {
                runtime,
                exports,
                cancellation: run_cancellation,
                restore_public_source,
                ..
            } => {
                run_cancellation.cancel();
                drop(state);
                // Once an explicit paper stop owns the transition, no source lifecycle retry may
                // resurrect paper execution from the previously retained request.
                *self.restart_request.lock().await = None;
                let complete =
                    bounded_runtime_shutdown(*runtime, exports, deadline, cancellation).await;
                match complete {
                    Ok(true) => {
                        self.restore_public_source_owned(
                            restore_public_source,
                            Some(true),
                            deadline,
                            cancellation,
                        )
                        .await?;
                        Ok(true)
                    }
                    Ok(false) => {
                        self.set_stopped(Some(false)).await;
                        Ok(false)
                    }
                    Err(error) => {
                        self.set_stopped(Some(false)).await;
                        Err(error)
                    }
                }
            }
            other @ (PaperState::LiveStarting { .. }
            | PaperState::Starting { .. }
            | PaperState::Stopping) => {
                *state = other;
                Err(ServiceError::Unavailable)
            }
        }
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        let cleanup = CancellationToken::new();
        let _owner = bounded_lock(&self.owner_gate, deadline, &cleanup).await?;
        match self.stop_runtime_before_owned(deadline, &cleanup).await? {
            true => Ok(()),
            false => Err(ServiceError::Unavailable),
        }
    }

    async fn stop_runtime_before_owned(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<bool, ServiceError> {
        loop {
            ensure_before(deadline, cancellation)?;
            let mut changes = self.changed.subscribe();
            let mut state = bounded_lock(&self.state, deadline, cancellation).await?;
            match std::mem::replace(&mut *state, PaperState::Stopping) {
                PaperState::Stopped { last_complete } => {
                    *state = PaperState::Stopped { last_complete };
                    return Ok(last_complete.unwrap_or(true));
                }
                PaperState::LiveOnly {
                    runtime,
                    exports,
                    cancellation: run_cancellation,
                    last_paper_complete,
                    ..
                } => {
                    exports.begin_shutdown();
                    run_cancellation.cancel();
                    drop(state);
                    let complete =
                        bounded_live_runtime_shutdown(*runtime, exports, deadline, cancellation)
                            .await;
                    self.set_stopped(last_paper_complete).await;
                    return complete;
                }
                PaperState::Running {
                    runtime,
                    exports,
                    cancellation: run_cancellation,
                    ..
                } => {
                    exports.begin_shutdown();
                    run_cancellation.cancel();
                    drop(state);
                    let complete =
                        bounded_runtime_shutdown(*runtime, exports, deadline, cancellation).await;
                    self.set_stopped(Some(complete.as_ref().copied().unwrap_or(false)))
                        .await;
                    return complete;
                }
                PaperState::LiveStarting {
                    cancellation: run_cancellation,
                    ..
                }
                | PaperState::Starting {
                    cancellation: run_cancellation,
                    ..
                } => {
                    run_cancellation.cancel();
                    *state = PaperState::Stopping;
                    drop(state);
                    self.signal_change();
                    wait_changed(&mut changes, deadline, cancellation).await?;
                }
                PaperState::Stopping => {
                    *state = PaperState::Stopping;
                    drop(state);
                    wait_changed(&mut changes, deadline, cancellation).await?;
                }
            }
        }
    }

    async fn set_stopped(&self, complete: Option<bool>) {
        *self.state.lock().await = PaperState::Stopped {
            last_complete: complete,
        };
        self.signal_change();
    }

    fn signal_change(&self) {
        self.changed.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TargetLadderSelector {
    Downside,
    Add,
    EntryLower,
    EntryUpper,
    Base,
    TrimLower,
    TrimUpper,
    ExitLower,
    ExitUpper,
    Upside,
}

impl TargetLadderSelector {
    fn parse(value: &str) -> Result<Self, ServiceError> {
        match value {
            "downside" => Ok(Self::Downside),
            "add" => Ok(Self::Add),
            "entry_lower" => Ok(Self::EntryLower),
            "entry_upper" => Ok(Self::EntryUpper),
            "base" => Ok(Self::Base),
            "trim_lower" => Ok(Self::TrimLower),
            "trim_upper" => Ok(Self::TrimUpper),
            "exit_lower" => Ok(Self::ExitLower),
            "exit_upper" => Ok(Self::ExitUpper),
            "upside" => Ok(Self::Upside),
            _ => Err(ServiceError::InvalidRequest),
        }
    }

    const fn level(self) -> &'static str {
        match self {
            Self::Downside => "downside",
            Self::Add => "add",
            Self::EntryLower => "entry_lower",
            Self::EntryUpper => "entry_upper",
            Self::Base => "base",
            Self::TrimLower => "trim_lower",
            Self::TrimUpper => "trim_upper",
            Self::ExitLower => "exit_lower",
            Self::ExitUpper => "exit_upper",
            Self::Upside => "upside",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Downside => "Downside",
            Self::Add => "Add",
            Self::EntryLower => "Entry lower",
            Self::EntryUpper => "Entry upper",
            Self::Base => "Base",
            Self::TrimLower => "Trim lower",
            Self::TrimUpper => "Trim upper",
            Self::ExitLower => "Exit lower",
            Self::ExitUpper => "Exit upper",
            Self::Upside => "Upside",
        }
    }

    fn price(self, state: &TargetState) -> Money {
        let target = state.target().target();
        match self {
            Self::Downside => target.cases().downside(),
            Self::Add => state.target().add_case(),
            Self::EntryLower => target.entry_range().lower(),
            Self::EntryUpper => target.entry_range().upper(),
            Self::Base => target.cases().base(),
            Self::TrimLower => target.trim_range().lower(),
            Self::TrimUpper => target.trim_range().upper(),
            Self::ExitLower => target.exit_range().lower(),
            Self::ExitUpper => target.exit_range().upper(),
            Self::Upside => target.cases().upside(),
        }
    }
}

fn manual_order_side(value: &str) -> Result<OrderSide, ServiceError> {
    match value {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(ServiceError::InvalidRequest),
    }
}

fn manual_order_type(value: &str) -> Result<OrderType, ServiceError> {
    match value {
        "market" => Ok(OrderType::Market),
        "limit" => Ok(OrderType::Limit),
        "stop" => Ok(OrderType::Stop),
        "stop_limit" => Ok(OrderType::StopLimit),
        _ => Err(ServiceError::InvalidRequest),
    }
}

fn manual_time_in_force(value: &str) -> Result<TimeInForce, ServiceError> {
    match value {
        "day" => Ok(TimeInForce::Day),
        "good_til_cancelled" => Ok(TimeInForce::GoodTilCancelled),
        "immediate_or_cancel" => Ok(TimeInForce::ImmediateOrCancel),
        "fill_or_kill" => Ok(TimeInForce::FillOrKill),
        _ => Err(ServiceError::InvalidRequest),
    }
}

fn selected_target_price(
    request: &TypedToolRequest,
    field: &str,
    target: &TargetState,
    terms: market_squawk_domain::InstrumentExecutionTerms,
    order_type: OrderType,
) -> Result<Option<PriceTicks>, ServiceError> {
    let required = match field {
        "limitTargetLevel" => matches!(order_type, OrderType::Limit | OrderType::StopLimit),
        "stopTargetLevel" => matches!(order_type, OrderType::Stop | OrderType::StopLimit),
        _ => return Err(ServiceError::Internal),
    };
    let selector = request.arguments().get(field).and_then(Value::as_str);
    if required != selector.is_some() {
        return Err(ServiceError::InvalidRequest);
    }
    let Some(selector) = selector else {
        return Ok(None);
    };
    let price = TargetLadderSelector::parse(selector)?.price(target);
    if price.currency() != terms.quote_currency() {
        return Err(ServiceError::InvalidRequest);
    }
    PriceTicks::try_from_decimal(price.amount(), terms.price_tick())
        .map(Some)
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn target_currently_usable(target: &TargetState, now: Timestamp) -> bool {
    target.status() == TargetStatus::Active
        && target.target().effective_at() <= now
        && now < target.target().target().expires_at()
}

fn manual_paper_target_value(
    target: &TargetState,
    route: &crate::paper_bot::ManualPaperRoute,
) -> Result<Value, ServiceError> {
    let target_core = target.target().target();
    let mut ladder = Vec::new();
    ladder
        .try_reserve_exact(10)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for level in [
        TargetLadderSelector::Downside,
        TargetLadderSelector::Add,
        TargetLadderSelector::EntryLower,
        TargetLadderSelector::EntryUpper,
        TargetLadderSelector::Base,
        TargetLadderSelector::TrimLower,
        TargetLadderSelector::TrimUpper,
        TargetLadderSelector::ExitLower,
        TargetLadderSelector::ExitUpper,
        TargetLadderSelector::Upside,
    ] {
        ladder.push(json!({
            "level": level.level(),
            "label": level.label(),
            "value": level.price(target),
        }));
    }
    Ok(json!({
        "targetId": target_core.id().as_str(),
        "targetRevision": target_core.revision().get(),
        "instrumentId": target_core.instrument_id(),
        "status": "active",
        "thesis": target.target().thesis().as_str(),
        "expiresAt": target_core.expires_at(),
        "reviewDueAt": target.target().review_due_at(),
        "route": {
            "venueId": route.route().venue(),
            "instrumentId": route.route().instrument(),
        },
        "ladder": ladder,
    }))
}

/// Resolves the only active manual route that can trade an exact governed target.
///
/// A target never chooses a venue. More than one compatible route is ambiguous and therefore
/// rejected rather than resolved by an incidental route order.
fn sole_compatible_manual_route<'a>(
    runtime: &'a ProductionPaperBotRuntime,
    target: &TargetState,
) -> Result<&'a crate::paper_bot::ManualPaperRoute, ServiceError> {
    let target_core = target.target().target();
    let reference_currency = target_core.reference_mark().price().currency();
    let mut compatible = None;
    for route in runtime.manual_paper_routes() {
        let terms = route.execution_terms();
        if route.route().instrument() != target_core.instrument_id()
            || terms.quote_currency() != reference_currency
        {
            continue;
        }
        if compatible.replace(route).is_some() {
            return Err(ServiceError::InvalidRequest);
        }
    }
    compatible.ok_or(ServiceError::InvalidRequest)
}

fn current_timestamp() -> Result<Timestamp, ServiceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ServiceError::Unavailable)?
        .as_nanos();
    let nanos = i64::try_from(nanos).map_err(|_error| ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn map_decision_error(
    error: crate::application::decision::DecisionApplicationError,
) -> ServiceError {
    use crate::application::decision::DecisionApplicationError;
    use market_squawk_decisions::DecisionRepositoryError;

    match error {
        DecisionApplicationError::Repository(DecisionRepositoryError::NotFound) => {
            ServiceError::NotFound
        }
        DecisionApplicationError::Repository(
            DecisionRepositoryError::Capacity | DecisionRepositoryError::InvalidLimits,
        )
        | DecisionApplicationError::Allocation
        | DecisionApplicationError::Capacity => ServiceError::ResourceExhausted,
        DecisionApplicationError::Repository(_)
        | DecisionApplicationError::Unavailable
        | DecisionApplicationError::Persistence => ServiceError::Unavailable,
        DecisionApplicationError::InvalidPersistentState => ServiceError::Internal,
    }
}

const fn map_manual_paper_ingress_error(error: ProductionManualPaperIngressError) -> ServiceError {
    match error {
        ProductionManualPaperIngressError::RouteUnavailable => ServiceError::InvalidRequest,
        ProductionManualPaperIngressError::Occupied => ServiceError::ResourceExhausted,
        ProductionManualPaperIngressError::Closed => ServiceError::Unavailable,
    }
}

impl fmt::Debug for PaperController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PaperController")
            .field("config", &"[REDACTED EFFECTIVE CONFIG]")
            .field("accepting", &self.accepting.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl Drop for PaperController {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

enum PaperState {
    Stopped {
        last_complete: Option<bool>,
    },
    LiveStarting {
        run_id: Uuid,
        cancellation: CancellationToken,
    },
    LiveOnly {
        provider: ProductionSourceProvider,
        runtime: Box<ProductionLiveSourceRuntime>,
        exports: PaperRuntimeExports,
        cancellation: CancellationToken,
        last_paper_complete: Option<bool>,
    },
    Starting {
        run_id: Uuid,
        cancellation: CancellationToken,
    },
    Running {
        provider: PaperProvider,
        strategy_mode: PaperStrategyMode,
        runtime: Box<ProductionPaperBotRuntime>,
        exports: PaperRuntimeExports,
        cancellation: CancellationToken,
        restore_public_source: Option<ProductionSourceProvider>,
    },
    Stopping,
}

async fn bounded_runtime_shutdown(
    runtime: ProductionPaperBotRuntime,
    exports: PaperRuntimeExports,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<bool, ServiceError> {
    exports.begin_shutdown();
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ServiceError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(ServiceError::DeadlineExceeded)
        }
        shutdown = runtime.shutdown() => {
            let runtime_complete = shutdown.is_complete();
            let exports_complete = exports.finish_before(deadline, cancellation).await;
            Ok(runtime_complete && exports_complete)
        },
    }
}

async fn bounded_live_runtime_shutdown(
    runtime: ProductionLiveSourceRuntime,
    exports: PaperRuntimeExports,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<bool, ServiceError> {
    exports.begin_shutdown();
    let runtime_complete = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            return Err(ServiceError::DeadlineExceeded);
        }
        result = runtime.shutdown() => match result {
            Ok(()) => true,
            Err(error) => {
                tracing::error!(%error, "public market source shutdown did not complete");
                false
            }
        },
    };
    let exports_complete = exports.finish_before(deadline, cancellation).await;
    Ok(runtime_complete && exports_complete)
}

/// Parses the only two supported paper strategy modes; absence preserves manual operation.
fn paper_strategy_mode(request: &TypedToolRequest) -> Result<PaperStrategyMode, ServiceError> {
    match request.arguments().get("strategyMode") {
        None => Ok(PaperStrategyMode::Manual),
        Some(Value::String(value)) if value == "manual" => Ok(PaperStrategyMode::Manual),
        Some(Value::String(value)) if value == "book_imbalance" => {
            Ok(PaperStrategyMode::BookImbalance)
        }
        Some(_) => Err(ServiceError::InvalidRequest),
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum PaperProvider {
    Public(ProductionSourceProvider),
    CoinbaseDirect { provider_session_id: Uuid },
}

impl PaperProvider {
    fn from_request(request: &TypedToolRequest) -> Result<Self, ServiceError> {
        let session = request
            .arguments()
            .get("providerSessionId")
            .and_then(Value::as_str);
        match required_string(request, "provider")? {
            "coinbase" if session.is_none() => Ok(Self::Public(ProductionSourceProvider::Coinbase)),
            "kraken" if session.is_none() => Ok(Self::Public(ProductionSourceProvider::Kraken)),
            "coinbase-direct" => {
                let provider_session_id = session
                    .ok_or(ServiceError::InvalidRequest)?
                    .parse()
                    .map_err(|_error| ServiceError::InvalidRequest)?;
                Ok(Self::CoinbaseDirect {
                    provider_session_id,
                })
            }
            _ => Err(ServiceError::InvalidRequest),
        }
    }

    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Public(ProductionSourceProvider::Coinbase) => "coinbase",
            Self::Public(ProductionSourceProvider::Kraken) => "kraken",
            Self::CoinbaseDirect { .. } => "coinbase-direct",
        }
    }
}

enum PaperRuntimeExports {
    QualifiedMarket(LiveFairValueExportDrains),
    DirectExecutionOnly,
}

impl PaperRuntimeExports {
    fn is_healthy(&self) -> bool {
        match self {
            Self::QualifiedMarket(exports) => exports.is_healthy(),
            Self::DirectExecutionOnly => true,
        }
    }

    fn begin_shutdown(&self) {
        if let Self::QualifiedMarket(exports) = self {
            exports.begin_shutdown();
        }
    }

    async fn finish_before(self, deadline: Instant, cancellation: &CancellationToken) -> bool {
        match self {
            Self::QualifiedMarket(exports) => {
                match exports.finish_before(deadline, cancellation).await {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::error!(%error, "public market export shutdown did not complete");
                        false
                    }
                }
            }
            Self::DirectExecutionOnly => true,
        }
    }
}

async fn bounded_lock<'state, State>(
    state: &'state Mutex<State>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<tokio::sync::MutexGuard<'state, State>, ServiceError> {
    ensure_before(deadline, cancellation)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ServiceError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(ServiceError::DeadlineExceeded)
        }
        guard = state.lock() => Ok(guard),
    }
}

async fn wait_changed(
    changes: &mut watch::Receiver<u64>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ServiceError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(ServiceError::DeadlineExceeded)
        }
        result = changes.changed() => result.map_err(|_closed| ServiceError::Unavailable),
    }
}

fn required_string<'request>(
    request: &'request TypedToolRequest,
    field: &str,
) -> Result<&'request str, ServiceError> {
    request
        .arguments()
        .get(field)
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    ensure_before(context.deadline(), context.cancellation())
}

fn ensure_before(deadline: Instant, cancellation: &CancellationToken) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_control_error(error: crate::paper_bot::ProductionPaperControlError) -> ServiceError {
    match error {
        crate::paper_bot::ProductionPaperControlError::Cancelled => ServiceError::Cancelled,
        crate::paper_bot::ProductionPaperControlError::DeadlineExceeded => {
            ServiceError::DeadlineExceeded
        }
        crate::paper_bot::ProductionPaperControlError::Dispatch(error) => map_dispatch_error(error),
        crate::paper_bot::ProductionPaperControlError::Paper(error) => match error {
            market_squawk_adapter_paper::PaperControlError::Cancelled => ServiceError::Cancelled,
            market_squawk_adapter_paper::PaperControlError::InvalidDeadline
            | market_squawk_adapter_paper::PaperControlError::DeadlineExceeded => {
                ServiceError::DeadlineExceeded
            }
            market_squawk_adapter_paper::PaperControlError::Adapter(error) => {
                map_adapter_error(error)
            }
            market_squawk_adapter_paper::PaperControlError::Closed
            | market_squawk_adapter_paper::PaperControlError::WorkerFailed
            | market_squawk_adapter_paper::PaperControlError::ShutdownIncomplete
            | market_squawk_adapter_paper::PaperControlError::RecoveryInitializationUnavailable => {
                ServiceError::Unavailable
            }
        },
    }
}

const fn map_dispatch_error(error: ExecutionDispatchError) -> ServiceError {
    match error {
        ExecutionDispatchError::OrderNotTracked => ServiceError::NotFound,
        ExecutionDispatchError::OrderNotCancelable
        | ExecutionDispatchError::DuplicateApproval
        | ExecutionDispatchError::ReceiptMismatch
        | ExecutionDispatchError::AccountReplacementRejected
        | ExecutionDispatchError::ReconciliationAcknowledgementPending => {
            ServiceError::InvalidRequest
        }
        ExecutionDispatchError::Allocation
        | ExecutionDispatchError::QueueCountSaturated
        | ExecutionDispatchError::QueueBytesSaturated
        | ExecutionDispatchError::RegistryCapacity
        | ExecutionDispatchError::RegistryBusy
        | ExecutionDispatchError::PendingReconciliationCapacity
        | ExecutionDispatchError::TaskOwnershipUnavailable => ServiceError::ResourceExhausted,
        ExecutionDispatchError::OperationCancelled => ServiceError::Cancelled,
        ExecutionDispatchError::OperationDeadlineExceeded => ServiceError::DeadlineExceeded,
        ExecutionDispatchError::Adapter(error) => map_adapter_error(error),
        ExecutionDispatchError::AuditUnavailable
        | ExecutionDispatchError::Closed
        | ExecutionDispatchError::CommandSizeUnsupported
        | ExecutionDispatchError::RegistryPoisoned
        | ExecutionDispatchError::RegistryInvariant
        | ExecutionDispatchError::ClockUnavailable => ServiceError::Unavailable,
    }
}

const fn map_adapter_error(error: ExecutionAdapterError) -> ServiceError {
    match error {
        ExecutionAdapterError::Rejected => ServiceError::InvalidRequest,
        ExecutionAdapterError::NotAttemptedBusy => ServiceError::ResourceExhausted,
        ExecutionAdapterError::KnownFailure
        | ExecutionAdapterError::UncertainOutcome
        | ExecutionAdapterError::ReconciliationRequired => ServiceError::Unavailable,
    }
}

fn order_value(order: &PaperOrderSnapshot, fills: &[PaperFillSnapshot]) -> Value {
    json!({
        "orderId": order.order_id(),
        "accountId": order.account_id(),
        "state": order.state(),
        "requestedLots": order.requested(),
        "filledLots": order.cumulative_filled(),
        "averageFillPriceTicks": order.average_fill_price(),
        "maximumFillPriceTicks": order.maximum_fill_price(),
        "maximumExecutionPriceTicks": order.maximum_execution_price(),
        "side": order.side(),
        "referencePriceTicks": order.reference_price(),
        "maximumSlippageBasisPoints": order.maximum_slippage().get(),
        "cumulativeFees": order.cumulative_fees(),
        "acceptedAt": order.accepted_at(),
        "eligibleAt": order.eligible_at(),
        "expiresAt": order.expires_at(),
        "revision": order.revision(),
        "targetReference": order.target_reference().map(|target| json!({
            "targetId": target.target_id(),
            "revision": target.revision().get(),
            "contentSha256": hex(target.content_sha256()),
        })),
        "observed": observed_order_evidence(order, fills),
    })
}

fn observed_order_evidence(order: &PaperOrderSnapshot, fills: &[PaperFillSnapshot]) -> Value {
    let first_fill = fills
        .iter()
        .filter(|fill| fill.order_id() == order.order_id())
        .min_by_key(|fill| fill.event_at());
    let first_fill_at = first_fill.map(|fill| fill.event_at());
    let observed_latency_nanos = first_fill_at.and_then(|filled_at| {
        filled_at
            .unix_nanos()
            .checked_sub(order.eligible_at().unix_nanos())
    });
    let observed_slippage_ticks =
        order
            .average_fill_price()
            .and_then(|average| match order.side() {
                OrderSide::Buy => average.get().checked_sub(order.reference_price().get()),
                OrderSide::Sell => order.reference_price().get().checked_sub(average.get()),
            });
    let observed_slippage_basis_points = observed_slippage_ticks.and_then(|ticks| {
        i128::from(ticks)
            .checked_mul(10_000)
            .and_then(|scaled| scaled.checked_div(i128::from(order.reference_price().get())))
            .and_then(|basis_points| i64::try_from(basis_points).ok())
    });
    json!({
        "firstFillAt": first_fill_at,
        "firstFillAfterEligibilityNanos": observed_latency_nanos,
        "averageFillSlippageTicks": observed_slippage_ticks,
        "averageFillSlippageBasisPoints": observed_slippage_basis_points,
    })
}

fn simulation_value(simulation: market_squawk_adapter_paper::PaperSimulationSnapshot) -> Value {
    json!({
        "configurationVersion": simulation.configuration_version(),
        "minimumLatencyNanos": simulation.minimum_latency_nanos(),
        "maximumLatencyNanos": simulation.maximum_latency_nanos(),
        "cancelLatencyNanos": simulation.cancel_latency_nanos(),
        "maximumMarkAgeNanos": simulation.maximum_mark_age_nanos(),
        "maximumParticipationBasisPoints": simulation.maximum_participation_basis_points(),
        "impactBasisPointsPerLevel": simulation.impact_basis_points_per_level(),
        "makerFeeBasisPoints": simulation.maker_fee_basis_points(),
        "takerFeeBasisPoints": simulation.taker_fee_basis_points(),
        "minimumFee": simulation.minimum_fee(),
        "maximumFee": simulation.maximum_fee(),
    })
}

fn reconciliation_value(
    snapshot: &PaperExecutionSnapshot,
    financial_reconciliation_current: bool,
) -> Value {
    json!({
        "snapshotSequence": snapshot.sequence(),
        "snapshotComplete": snapshot.complete(),
        "configurationDigestSha256": hex(snapshot.configuration_digest()),
        "reconciliationRequired": snapshot.reconciliation_required(),
        "financialReconciliationCurrent": financial_reconciliation_current,
        "activeOrderCount": snapshot.active_orders().len(),
        "archivedOrderCount": snapshot.archived_orders().len(),
        "fillCount": snapshot.fills().len(),
        "accountCount": snapshot.accounts().len(),
        "cashBalanceCount": snapshot.cash().len(),
        "positionCount": snapshot.positions().len(),
    })
}

fn bounded_evidence<T: Copy>(
    values: &[T],
    maximum_items: usize,
    value: fn(T) -> Value,
) -> Result<Value, ServiceError> {
    let returned = values.len().min(maximum_items);
    let mut rows = Vec::new();
    rows.try_reserve_exact(returned)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    rows.extend(values[..returned].iter().copied().map(value));
    Ok(json!({
        "rows": rows,
        "returnedItems": returned,
        "availableItems": values.len(),
    }))
}

fn account_value(account: PaperAccountRiskSnapshot) -> Value {
    json!({
        "accountId": account.account_id(),
        "revision": account.revision().get(),
        "eligible": account.eligible(),
        "currency": account.currency(),
        "settledCapital": account.settled_capital(),
        "markedEquity": account.marked_equity(),
        "peakMarkedEquity": account.peak_marked_equity(),
        "grossExposure": account.marked_gross_exposure(),
        "unrealizedPnl": account.unrealized_pnl(),
        "realizedPnl": account.realized_pnl(),
        "realizedLoss": account.realized_loss(),
        "drawdown": account.drawdown(),
        "markDigestSha256": hex(account.mark_digest()),
    })
}

fn cash_value(cash: PaperCashBalance) -> Value {
    json!({"accountId": cash.account_id(), "balance": cash.balance()})
}

fn position_value(position: PaperPosition) -> Value {
    json!({
        "accountId": position.account_id(),
        "instrumentId": position.instrument_id(),
        "lots": position.lots(),
        "costBasis": position.cost_basis(),
    })
}

fn risk_limits_value(
    limits: &RiskLimitsSnapshot,
    maximum_items: usize,
) -> Result<Value, ServiceError> {
    let eligible = bounded_evidence(limits.eligible_instruments(), maximum_items, |instrument| {
        json!(instrument)
    })?;
    Ok(json!({
        "currency": limits.currency(),
        "eligibleInstruments": eligible,
        "maximumPositionLots": limits.maximum_position_lots(),
        "maximumOrderNotional": limits.maximum_order_notional(),
        "maximumGrossExposure": limits.maximum_gross_exposure(),
        "maximumLeverageBasisPoints": limits.maximum_leverage().get(),
        "minimumCapital": limits.minimum_capital(),
        "maximumLoss": limits.maximum_loss(),
        "maximumDrawdown": limits.maximum_drawdown(),
        "maximumFeeBasisPoints": limits.maximum_fee().get(),
        "maximumPriceDeviationBasisPoints": limits.maximum_price_deviation().get(),
        "maximumSlippageBasisPoints": limits.maximum_slippage().get(),
        "maximumOrdersPerWindow": limits.maximum_orders_per_window().get(),
        "orderRateWindowNanos": limits.order_rate_window_nanos(),
        "reservationTtlNanos": limits.reservation_ttl_nanos(),
        "allowShort": limits.allow_short(),
        "killSwitch": limits.kill_switch(),
    }))
}

fn audit_snapshot_value(
    snapshot: &ProductionExecutionAuditSnapshot,
) -> Result<Value, ServiceError> {
    let mut records = Vec::new();
    records
        .try_reserve_exact(snapshot.records().len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    records.extend(snapshot.records().iter().copied().map(audit_record_value));
    Ok(json!({
        "records": records,
        "returnedItems": snapshot.returned_items(),
        "availableItems": snapshot.available_items(),
        "totalPublished": snapshot.total_published(),
        "oldestSequence": snapshot.oldest_sequence(),
        "latestSequence": snapshot.latest_sequence(),
        "cursorExpired": snapshot.cursor_expired(),
        "nextCursor": snapshot.next_cursor(),
    }))
}

fn audit_record_value(record: ProductionExecutionAuditRecord) -> Value {
    let event = record.event();
    json!({
        "sequence": record.sequence(),
        "kind": event.kind(),
        "approvalId": event.approval_id(),
        "orderId": event.order_id(),
        "accountId": event.account_id(),
        "instrumentId": event.instrument_id(),
        "strategyId": event.strategy_id(),
        "modelId": event.model_id(),
        "intentDigestSha256": hex(event.intent_digest().as_bytes()),
        "assessmentDigestSha256": event.assessment_digest().map(hex),
        "evidenceBindingDigestSha256": event.evidence_binding_digest().map(hex),
        "executionIdentityDigestSha256": event.execution_identity_digest().map(hex),
        "portfolioContentDigestSha256": event.portfolio_content_digest().map(hex),
        "maximumExecutionPriceTicks": event.execution_price_bound().map(|bound| bound.maximum_price()),
        "riskPolicyDigestSha256": hex(event.risk_policy().digest()),
        "riskPolicyRulesetVersion": event.risk_policy().ruleset_version().get(),
        "marketObservedAt": event.market_observed_at(),
        "validUntil": event.valid_until(),
        "observedAt": event.observed_at(),
        "reasons": event.reasons().collect::<Vec<_>>(),
    })
}

fn hex(bytes: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn fill_value(fill: PaperFillSnapshot) -> Value {
    json!({
        "sequence": fill.sequence(),
        "orderId": fill.order_id(),
        "eventAt": fill.event_at(),
        "quantityLots": fill.quantity(),
        "averagePriceTicks": fill.average_price(),
        "maximumPriceTicks": fill.maximum_price(),
        "notional": fill.notional(),
        "fee": fill.fee(),
        "liquidity": fill.liquidity(),
    })
}

fn cancel_receipt_value(receipt: CancelReceipt) -> Value {
    json!({
        "orderId": receipt.order_id(),
        "status": cancel_status(receipt.status()),
        "observedAt": receipt.observed_at(),
        "cumulativeFilledLots": receipt.cumulative_filled(),
        "averageFillPriceTicks": receipt.average_fill_price(),
        "maximumFillPriceTicks": receipt.maximum_fill_price(),
        "cumulativeFees": receipt.cumulative_fees(),
    })
}

fn execution_state_value(state: &ExecutionState) -> Value {
    json!({
        "observedAt": state.observed_at(),
        "orderCount": state.orders().len(),
        "accountCount": state.accounts().len(),
        "sourceBound": state.source_binding().is_some(),
        "reconciliationRequired": state.reconciliation_required(),
    })
}

const fn cancel_status(status: CancelStatus) -> &'static str {
    match status {
        CancelStatus::Pending => "pending",
        CancelStatus::Canceled => "canceled",
        CancelStatus::AlreadyTerminal => "already_terminal",
    }
}
