//! Lifecycle-owned paper bot and execution application services.

mod market;
mod source_runtime;

use std::{
    fmt,
    num::NonZeroU64,
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
use market_squawk_data::{InstrumentDefinitionReadCapability, MarketDataInstrumentReadCapability};
use market_squawk_decisions::{InvestmentTargetSetId, TargetState, TargetStatus};
use market_squawk_domain::{
    BasisPoints, DigestAlgorithm, Money, OrderId, OrderSide, OrderType, PriceTicks, QuantityLots,
    RevisionNumber, SourceIdentifier, TimeInForce, Timestamp,
};
use market_squawk_execution::{
    AccountRiskViolation, CancelReceipt, CancelStatus, ExecutionAdapterError, ExecutionAuditKind,
    ExecutionAuditReason, ExecutionDispatchError, ExecutionState, ManualPaperDraft,
    ManualPaperDraftInput, OrderTargetReference, RiskLimitsSnapshot, RiskRejectionCode,
};
use market_squawk_live::{ActiveLiveActionHookGroup, LiveActionHookGeneration};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::decision::DecisionApplication;
use super::market_runtime::{
    COINBASE_DIRECT_SURFACE_ID, COINBASE_PUBLIC_SURFACE_ID, KRAKEN_PUBLIC_SURFACE_ID,
    MarketRuntimeRegistry, PaperMarketSurfaceSelection,
};
use super::recommendation::RecommendationSetupAuthority;
use super::source::SourceRuntimeView;
use super::{ApplicationDomainService, effective_service_limits};
use crate::{
    AppConfig, ProductionSourceProvider,
    paper_bot::{
        PaperStrategyMode, ProductionExecutionAuditRecord, ProductionExecutionAuditSnapshot,
        ProductionManualPaperIngressError, ProductionPaperBotRuntime,
        local_coinbase_direct_paper_bot_on_existing_market_with_strategy_mode,
        local_paper_bot_on_existing_public_market_with_strategy_mode, manual_paper_account_id,
        manual_paper_reason_code, manual_paper_strategy_id,
    },
    portfolio_application::{
        PortfolioAccountCatalogReadCapability, PortfolioCandidateResolutionAuthority,
    },
};

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
const MAXIMUM_PRODUCT_ORDER_TOKENS: usize = 4_096;

/// Shared paper lifecycle exposed as distinct Bot and Execution domain services.
pub struct PaperApplicationServices {
    controller: Arc<PaperController>,
    market_runtime: Arc<MarketRuntimeRegistry>,
    instrument_definitions: InstrumentDefinitionReadCapability,
    market_data_instruments: MarketDataInstrumentReadCapability,
    reference_search: Arc<dyn market::MarketReferenceSearchAuthority>,
}

/// Market-only candidate factory retained until durable workspace setup is available.
#[derive(Clone)]
pub(crate) struct PortfolioCandidateResolutionFactory {
    inner: market::ProductionPortfolioCandidateResolutionFactory,
}

impl PortfolioCandidateResolutionFactory {
    /// Binds the exact workspace setup and immutable imported-portfolio catalog reader.
    pub(crate) fn bind(
        &self,
        setup: Arc<RecommendationSetupAuthority>,
        catalog: PortfolioAccountCatalogReadCapability,
    ) -> Arc<dyn PortfolioCandidateResolutionAuthority> {
        self.inner.bind(setup, catalog)
    }
}

impl fmt::Debug for PortfolioCandidateResolutionFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PortfolioCandidateResolutionFactory")
            .field("authority", &"[CURRENT MARKET READS ONLY]")
            .finish()
    }
}

pub(crate) use market::{
    MarketReferenceMatchKind, MarketReferenceRecord, MarketReferenceSearchAuthority,
    MarketReferenceSearchPage,
};

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
            PaperState::Stopped { .. } => Ok(PaperRuntimeActivitySnapshot {
                paper_execution_active: false,
                reconciliation_pending: false,
            }),
            PaperState::CleanupRequired { .. } => Ok(PaperRuntimeActivitySnapshot {
                paper_execution_active: false,
                reconciliation_pending: true,
            }),
            PaperState::Starting { .. } | PaperState::Running { .. } | PaperState::Stopping => {
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
    pub(crate) fn new(
        config: AppConfig,
        decisions: Arc<DecisionApplication>,
        market_runtime: Arc<MarketRuntimeRegistry>,
        instrument_definitions: InstrumentDefinitionReadCapability,
        market_data_instruments: MarketDataInstrumentReadCapability,
        reference_search: Arc<dyn MarketReferenceSearchAuthority>,
    ) -> Self {
        Self {
            controller: Arc::new(PaperController::new(
                config,
                decisions,
                Arc::clone(&market_runtime),
            )),
            market_runtime,
            instrument_definitions,
            market_data_instruments,
            reference_search,
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
        Arc::new(market::MarketDomainService::new(
            Arc::clone(&self.market_runtime),
            self.instrument_definitions.clone(),
            self.market_data_instruments.clone(),
            Arc::clone(&self.reference_search),
        ))
    }

    /// Returns an authority-free Source-domain view sharing this sole live-runtime owner.
    pub fn source_runtime_view(&self) -> Arc<dyn SourceRuntimeView> {
        Arc::new(source_runtime::MarketSourceRuntimeView::new(Arc::clone(
            &self.market_runtime,
        )))
    }

    /// Returns read-only exact paper/execution activity for installed lifecycle preflight.
    pub(crate) fn runtime_activity_authority(&self) -> Arc<dyn PaperRuntimeActivityAuthority> {
        Arc::new(PaperRuntimeActivityControl {
            controller: Arc::clone(&self.controller),
        })
    }

    /// Returns a read-only factory without paper state, action hooks, risk, or order authority.
    pub(crate) fn candidate_resolution_factory(
        &self,
    ) -> Result<PortfolioCandidateResolutionFactory, ServiceError> {
        let maximum_mark_age_nanos = u64::try_from(self.controller.config.stale_after().as_nanos())
            .map_err(|_error| ServiceError::Internal)?;
        market::ProductionPortfolioCandidateResolutionFactory::try_new(
            Arc::clone(&self.market_runtime),
            self.instrument_definitions.clone(),
            self.market_data_instruments.clone(),
            maximum_mark_age_nanos,
        )
        .map(|inner| PortfolioCandidateResolutionFactory { inner })
    }
}

impl fmt::Debug for PaperApplicationServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PaperApplicationServices")
            .field("controller", &self.controller)
            .field("market_runtime", &self.market_runtime)
            .field(
                "instrument_definitions",
                &"[SEALED INSTRUMENT-DEFINITION READ AUTHORITY]",
            )
            .field(
                "market_data_instruments",
                &"[SEALED MARKET-DATA INSTRUMENT READ AUTHORITY]",
            )
            .field("reference_search", &self.reference_search)
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
                let order_token = required_string(&request, "orderToken")?
                    .parse::<Uuid>()
                    .map_err(|_error| ServiceError::InvalidRequest)?;
                let receipt = self.controller.cancel(order_token, &context).await?;
                (cancel_receipt_value(receipt, order_token), 1, 1)
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
    decisions: Arc<DecisionApplication>,
    market_runtime: Arc<MarketRuntimeRegistry>,
    accepting: AtomicBool,
    lifecycle: CancellationToken,
    // Serializes every mutation that can replace the sole live/paper runtime owner.
    owner_gate: Mutex<()>,
    state: Mutex<PaperState>,
    // Session-scoped opaque handles keep execution and decision authority out of ordinary product
    // reads while still allowing an explicit follow-up cancellation or manual paper draft.
    product_tokens: Mutex<ProductAuthorityTokens>,
    // Bot and Execution are separate facades over this shared controller. Retain one terminal
    // shutdown result so the second facade cannot repeat destruction of the same runtime owner.
    shutdown: Mutex<Option<Result<(), ServiceError>>>,
}

impl PaperController {
    fn new(
        config: AppConfig,
        decisions: Arc<DecisionApplication>,
        market_runtime: Arc<MarketRuntimeRegistry>,
    ) -> Self {
        Self {
            config,
            decisions,
            market_runtime,
            accepting: AtomicBool::new(true),
            lifecycle: CancellationToken::new(),
            owner_gate: Mutex::new(()),
            state: Mutex::new(PaperState::Stopped {
                last_complete: None,
            }),
            product_tokens: Mutex::new(ProductAuthorityTokens::default()),
            shutdown: Mutex::new(None),
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
            PaperState::CleanupRequired { .. } => Ok(json!({
                "state": "failed",
                "requiresStop": true,
                "recoveryAction": "stop_before_restart",
            })),
            PaperState::Starting { .. } => Ok(json!({"state": "starting"})),
            PaperState::Stopping => Ok(json!({"state": "stopping"})),
            PaperState::Running {
                provider: _,
                strategy_mode,
                runtime,
                surface_id,
                cancellation,
                ..
            } => {
                let source_healthy = self
                    .market_runtime
                    .verify(surface_id, context.deadline(), context.cancellation())
                    .await?
                    .is_some();
                if cancellation.is_cancelled() || !runtime.source_is_healthy() || !source_healthy {
                    return Ok(json!({
                        "state": "failed",
                        "requiresStop": true,
                    }));
                }
                let financial_reconciliation_current = runtime.financial_reconciliation_current();
                let snapshot = runtime
                    .paper_snapshot(context.deadline(), context.cancellation())
                    .await
                    .map_err(map_control_error)?;
                let audit = runtime
                    .execution_audit_snapshot(None, maximum_items)
                    .map_err(|_error| ServiceError::Unavailable)?;
                let mut product_tokens = bounded_lock(
                    &self.product_tokens,
                    context.deadline(),
                    context.cancellation(),
                )
                .await?;
                Ok(json!({
                    "state": "running",
                    "strategyMode": strategy_mode.as_str(),
                    "complete": snapshot.complete(),
                    "reconciliationRequired": snapshot.reconciliation_required(),
                    "financialReconciliationCurrent": financial_reconciliation_current,
                    "orders": snapshot.orders().len(),
                    "fills": snapshot.fills().len(),
                    "positions": snapshot.positions().len(),
                    "accounts": bounded_evidence(snapshot.accounts(), maximum_items, account_value)?,
                    "cash": bounded_evidence(snapshot.cash(), maximum_items, cash_value)?,
                    "positionRecords": bounded_evidence(snapshot.positions(), maximum_items, position_value)?,
                    "simulation": simulation_value(snapshot.simulation()),
                    "reconciliation": reconciliation_value(&snapshot, financial_reconciliation_current),
                    "riskLimits": risk_limits_value(runtime.risk_limits(), maximum_items)?,
                    "riskDecisions": audit_snapshot_value(&audit, &mut product_tokens)?,
                }))
            }
        }
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
        let market_selection = self
            .market_runtime
            .select_paper_market_surface(deadline, request_cancellation)
            .await?;
        let provider = PaperProvider::from_selection(&market_selection)?;
        let surface_id = provider.surface_id()?;
        let onboarding_session_id = provider.onboarding_session_id();
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
        let last_complete = {
            let mut state = bounded_lock(&self.state, deadline, request_cancellation).await?;
            let PaperState::Stopped { last_complete } = &*state else {
                return Err(ServiceError::InvalidRequest);
            };
            let last_complete = *last_complete;
            *state = PaperState::Starting { run_id };
            last_complete
        };
        match bounded_lock(&self.product_tokens, deadline, request_cancellation).await {
            Ok(mut tokens) => tokens.clear(),
            Err(error) => {
                run_cancellation.cancel();
                self.set_stopped(last_complete).await;
                return Err(error);
            }
        }
        let composition = match provider {
            PaperProvider::Public { provider, .. } => {
                local_paper_bot_on_existing_public_market_with_strategy_mode(
                    self.config.clone(),
                    provider,
                    initial_cash,
                    fee_basis_points,
                    strategy_mode,
                )
            }
            PaperProvider::CoinbaseDirect { .. } => {
                local_coinbase_direct_paper_bot_on_existing_market_with_strategy_mode(
                    self.config.clone(),
                    initial_cash,
                    fee_basis_points,
                    strategy_mode,
                )
            }
        }
        .map_err(|error| {
            tracing::error!(%error, "paper execution composition failed");
            ServiceError::Unavailable
        });
        let composition = match composition {
            Ok(composition) => composition,
            Err(error) => {
                run_cancellation.cancel();
                self.set_stopped(last_complete).await;
                return Err(error);
            }
        };
        let snapshots = match self
            .market_runtime
            .snapshot_reader(
                &surface_id,
                onboarding_session_id,
                deadline,
                request_cancellation,
            )
            .await
        {
            Ok(snapshots) => snapshots,
            Err(error) => {
                run_cancellation.cancel();
                self.set_stopped(last_complete).await;
                return Err(error);
            }
        };
        if let Err(error) = ensure_before(deadline, request_cancellation) {
            run_cancellation.cancel();
            self.set_stopped(last_complete).await;
            return Err(error);
        }
        let prepared = composition
            .prepare_on_existing_live(snapshots, run_cancellation.clone())
            .await
            .map_err(|error| {
                tracing::error!(%error, "paper execution graph failed to start");
                ServiceError::Unavailable
            });
        let (runtime, action_hooks) = match prepared {
            Ok(prepared) => {
                let (runtime, action_hooks) = prepared.into_parts();
                if let Err(error) = ensure_before(deadline, request_cancellation) {
                    run_cancellation.cancel();
                    drop(action_hooks);
                    let complete = bounded_runtime_shutdown(runtime).await;
                    self.set_stopped(Some(complete)).await;
                    return Err(error);
                }
                (runtime, action_hooks)
            }
            Err(error) => {
                run_cancellation.cancel();
                self.set_stopped(Some(false)).await;
                return Err(error);
            }
        };
        let prepared_hooks = match self
            .market_runtime
            .prepare_action_hooks(
                &surface_id,
                onboarding_session_id,
                action_hooks,
                deadline,
                request_cancellation,
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                run_cancellation.cancel();
                let complete = bounded_runtime_shutdown(runtime).await;
                self.set_stopped(Some(complete)).await;
                return Err(error);
            }
        };
        let hook_runtime_incarnation = prepared_hooks.runtime_incarnation();
        let hook_generation = prepared_hooks.generation();
        if let Err(error) = ensure_before(deadline, request_cancellation) {
            drop(prepared_hooks);
            run_cancellation.cancel();
            let complete = bounded_runtime_shutdown(runtime).await;
            let reap = self
                .reap_action_hooks_for_cleanup(
                    &surface_id,
                    hook_runtime_incarnation,
                    hook_generation,
                )
                .await;
            if reap.is_err() {
                self.set_cleanup_required(
                    provider,
                    surface_id,
                    hook_runtime_incarnation,
                    hook_generation,
                )
                .await;
            } else {
                self.set_stopped(Some(complete)).await;
            }
            return Err(error);
        }
        let active_hooks = match prepared_hooks.activate() {
            Ok(active) => active,
            Err(error) => {
                run_cancellation.cancel();
                let complete = bounded_runtime_shutdown(runtime).await;
                let reap = self
                    .reap_action_hooks_for_cleanup(
                        &surface_id,
                        hook_runtime_incarnation,
                        hook_generation,
                    )
                    .await;
                if reap.is_err() {
                    self.set_cleanup_required(
                        provider,
                        surface_id,
                        hook_runtime_incarnation,
                        hook_generation,
                    )
                    .await;
                } else {
                    self.set_stopped(Some(complete)).await;
                }
                return Err(error);
            }
        };

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
                surface_id,
                strategy_mode,
                runtime: Box::new(runtime),
                action_hooks: active_hooks,
                cancellation: run_cancellation,
            };
            drop(state);
            return Ok(json!({
                "state": "running",
                "strategyMode": strategy_mode.as_str(),
            }));
        }
        if current_start {
            *state = PaperState::Stopping;
        }
        drop(state);

        let disabled = active_hooks.disable();
        let hook_runtime_incarnation = disabled.runtime_incarnation();
        let hook_generation = disabled.generation();
        run_cancellation.cancel();
        let complete = bounded_runtime_shutdown(runtime).await;
        let reap = self
            .reap_action_hooks_for_cleanup(&surface_id, hook_runtime_incarnation, hook_generation)
            .await;
        if reap.is_err() {
            self.set_cleanup_required(
                provider,
                surface_id,
                hook_runtime_incarnation,
                hook_generation,
            )
            .await;
        } else {
            self.set_stopped(Some(complete)).await;
        }
        Err(ServiceError::Unavailable)
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
        let mut tokens = bounded_lock(
            &self.product_tokens,
            context.deadline(),
            context.cancellation(),
        )
        .await?;
        for order in &snapshot.orders()[..returned] {
            values.push(order_value(order, snapshot.fills(), &mut tokens)?);
        }
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
        let mut tokens = bounded_lock(
            &self.product_tokens,
            context.deadline(),
            context.cancellation(),
        )
        .await?;
        for fill in snapshot.fills()[..returned].iter().copied() {
            values.push(fill_value(fill, &mut tokens)?);
        }
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
            surface_id,
            cancellation,
            ..
        } = &*state
        else {
            return Err(ServiceError::Unavailable);
        };
        if cancellation.is_cancelled()
            || !runtime.source_is_healthy()
            || self
                .market_runtime
                .verify(surface_id, context.deadline(), context.cancellation())
                .await?
                .is_none()
        {
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
        let mut product_tokens = bounded_lock(
            &self.product_tokens,
            context.deadline(),
            context.cancellation(),
        )
        .await?;
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
            if sole_compatible_manual_route(runtime, &target).is_err() {
                continue;
            }
            let target_token = product_tokens.target_token(entry.id(), entry.revision())?;
            targets.push(manual_paper_target_value(&target, target_token)?);
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
        let target_token = required_string(request, "targetToken")?
            .parse::<Uuid>()
            .map_err(|_error| ServiceError::InvalidRequest)?;
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
            surface_id,
            cancellation,
            ..
        } = &*state
        else {
            return Err(ServiceError::Unavailable);
        };
        if cancellation.is_cancelled()
            || !runtime.source_is_healthy()
            || self
                .market_runtime
                .verify(surface_id, context.deadline(), context.cancellation())
                .await?
                .is_none()
        {
            return Err(ServiceError::Unavailable);
        }
        let now = current_timestamp()?;
        let (target_id, target_revision) = {
            let product_tokens = bounded_lock(
                &self.product_tokens,
                context.deadline(),
                context.cancellation(),
            )
            .await?;
            let (target_id, target_revision) = product_tokens.resolve_target(target_token)?;
            (target_id.clone(), target_revision)
        };
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
                "accepted": true,
                "targetToken": target_token,
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
        let (runtime, surface_id, cancellation) = match &*state {
            PaperState::Stopped { .. } => return Ok(None),
            PaperState::Running {
                runtime,
                surface_id,
                cancellation,
                ..
            } => (runtime, surface_id, cancellation),
            PaperState::CleanupRequired { .. }
            | PaperState::Starting { .. }
            | PaperState::Stopping => {
                return Err(ServiceError::Unavailable);
            }
        };
        if cancellation.is_cancelled()
            || !runtime.source_is_healthy()
            || self
                .market_runtime
                .verify(surface_id, context.deadline(), context.cancellation())
                .await?
                .is_none()
        {
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
        order_token: Uuid,
        context: &RequestContext,
    ) -> Result<CancelReceipt, ServiceError> {
        ensure_live(context)?;
        let state = bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
        let PaperState::Running {
            runtime,
            surface_id,
            cancellation,
            ..
        } = &*state
        else {
            return Err(ServiceError::Unavailable);
        };
        if cancellation.is_cancelled()
            || !runtime.source_is_healthy()
            || self
                .market_runtime
                .verify(surface_id, context.deadline(), context.cancellation())
                .await?
                .is_none()
        {
            return Err(ServiceError::Unavailable);
        }
        let order_id = bounded_lock(
            &self.product_tokens,
            context.deadline(),
            context.cancellation(),
        )
        .await?
        .resolve_order(order_token)?;
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
            surface_id,
            cancellation,
            ..
        } = &*state
        else {
            return Err(ServiceError::Unavailable);
        };
        if cancellation.is_cancelled()
            || !runtime.source_is_healthy()
            || self
                .market_runtime
                .verify(surface_id, context.deadline(), context.cancellation())
                .await?
                .is_none()
        {
            return Err(ServiceError::Unavailable);
        }
        runtime
            .reconcile_tracked_orders(context.deadline(), context.cancellation())
            .await
            .map_err(map_control_error)
    }

    fn begin_shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
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
                Ok(last_complete.unwrap_or(true))
            }
            PaperState::CleanupRequired {
                provider,
                surface_id,
                runtime_incarnation,
                hook_generation,
            } => {
                drop(state);
                match self
                    .reap_action_hooks_for_cleanup(
                        &surface_id,
                        runtime_incarnation,
                        hook_generation,
                    )
                    .await
                {
                    Ok(_receipt) => {
                        self.set_stopped(Some(true)).await;
                        Ok(true)
                    }
                    Err(error) => {
                        self.set_cleanup_required(
                            provider,
                            surface_id,
                            runtime_incarnation,
                            hook_generation,
                        )
                        .await;
                        Err(error)
                    }
                }
            }
            PaperState::Running {
                provider,
                surface_id,
                runtime,
                action_hooks,
                cancellation: run_cancellation,
                ..
            } => {
                let disabled = action_hooks.disable();
                let runtime_incarnation = disabled.runtime_incarnation();
                let hook_generation = disabled.generation();
                run_cancellation.cancel();
                drop(state);
                let complete = bounded_runtime_shutdown(*runtime).await;
                let reap = self
                    .reap_action_hooks_for_cleanup(
                        &surface_id,
                        runtime_incarnation,
                        hook_generation,
                    )
                    .await;
                if let Err(error) = reap {
                    self.set_cleanup_required(
                        provider,
                        surface_id,
                        runtime_incarnation,
                        hook_generation,
                    )
                    .await;
                    return Err(error);
                }
                self.set_stopped(Some(complete)).await;
                Ok(complete)
            }
            other @ (PaperState::Starting { .. } | PaperState::Stopping) => {
                *state = other;
                Err(ServiceError::Unavailable)
            }
        }
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        let cleanup = CancellationToken::new();
        let mut shutdown = bounded_lock(&self.shutdown, deadline, &cleanup).await?;
        if let Some(result) = *shutdown {
            return result;
        }
        let _owner = bounded_lock(&self.owner_gate, deadline, &cleanup).await?;
        let paper = self.stop_paper_before_owned(deadline, &cleanup).await;
        let sources = self.market_runtime.finish_shutdown(deadline).await;
        let result = match (paper, sources) {
            (Ok(true), Ok(())) => Ok(()),
            (Ok(false), Ok(())) => Err(ServiceError::Unavailable),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(_paper), Err(_sources)) => Err(ServiceError::Unavailable),
        };
        *shutdown = Some(result);
        result
    }

    async fn set_cleanup_required(
        &self,
        provider: PaperProvider,
        surface_id: SourceIdentifier,
        runtime_incarnation: NonZeroU64,
        hook_generation: LiveActionHookGeneration,
    ) {
        *self.state.lock().await = PaperState::CleanupRequired {
            provider,
            surface_id,
            runtime_incarnation,
            hook_generation,
        };
    }

    async fn set_stopped(&self, complete: Option<bool>) {
        *self.state.lock().await = PaperState::Stopped {
            last_complete: complete,
        };
    }

    fn cleanup_deadline(&self) -> Result<Instant, ServiceError> {
        Instant::now()
            .checked_add(self.config.source_shutdown())
            .ok_or(ServiceError::Unavailable)
    }

    async fn reap_action_hooks_for_cleanup(
        &self,
        surface_id: &SourceIdentifier,
        runtime_incarnation: NonZeroU64,
        hook_generation: LiveActionHookGeneration,
    ) -> Result<(), ServiceError> {
        let cleanup = CancellationToken::new();
        self.market_runtime
            .reap_action_hooks(
                surface_id,
                runtime_incarnation,
                hook_generation,
                self.cleanup_deadline()?,
                &cleanup,
            )
            .await
            .map(|_receipt| ())
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
    target_token: Uuid,
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
        "targetToken": target_token,
        "instrumentId": target_core.instrument_id(),
        "thesis": target.target().thesis().as_str(),
        "expiresAt": target_core.expires_at(),
        "reviewDueAt": target.target().review_due_at(),
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
        self.lifecycle.cancel();
    }
}

enum PaperState {
    Stopped {
        last_complete: Option<bool>,
    },
    CleanupRequired {
        provider: PaperProvider,
        surface_id: SourceIdentifier,
        runtime_incarnation: NonZeroU64,
        hook_generation: LiveActionHookGeneration,
    },
    Starting {
        run_id: Uuid,
    },
    Running {
        provider: PaperProvider,
        surface_id: SourceIdentifier,
        strategy_mode: PaperStrategyMode,
        runtime: Box<ProductionPaperBotRuntime>,
        action_hooks: ActiveLiveActionHookGroup,
        cancellation: CancellationToken,
    },
    Stopping,
}

#[derive(Default)]
struct ProductAuthorityTokens {
    orders: Vec<ProductOrderToken>,
    targets: Vec<ProductTargetToken>,
}

struct ProductOrderToken {
    token: Uuid,
    order_id: OrderId,
}

struct ProductTargetToken {
    token: Uuid,
    target_id: InvestmentTargetSetId,
    revision: RevisionNumber,
}

impl ProductAuthorityTokens {
    fn clear(&mut self) {
        self.orders.clear();
        self.targets.clear();
    }

    fn order_token(&mut self, order_id: OrderId) -> Result<Uuid, ServiceError> {
        if let Some(existing) = self
            .orders
            .iter()
            .find(|binding| binding.order_id == order_id)
        {
            return Ok(existing.token);
        }
        if self.orders.len() >= MAXIMUM_PRODUCT_ORDER_TOKENS {
            return Err(ServiceError::ResourceExhausted);
        }
        self.orders
            .try_reserve(1)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let token = Uuid::new_v4();
        self.orders.push(ProductOrderToken { token, order_id });
        Ok(token)
    }

    fn resolve_order(&self, token: Uuid) -> Result<OrderId, ServiceError> {
        self.orders
            .iter()
            .find(|binding| binding.token == token)
            .map(|binding| binding.order_id)
            .ok_or(ServiceError::NotFound)
    }

    fn target_token(
        &mut self,
        target_id: &InvestmentTargetSetId,
        revision: RevisionNumber,
    ) -> Result<Uuid, ServiceError> {
        if let Some(existing) = self
            .targets
            .iter()
            .find(|binding| binding.target_id == *target_id && binding.revision == revision)
        {
            return Ok(existing.token);
        }
        if self.targets.len() >= MAXIMUM_MANUAL_PAPER_TARGET_INDEX_ENTRIES {
            return Err(ServiceError::ResourceExhausted);
        }
        self.targets
            .try_reserve(1)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let token = Uuid::new_v4();
        self.targets.push(ProductTargetToken {
            token,
            target_id: target_id.clone(),
            revision,
        });
        Ok(token)
    }

    fn resolve_target(
        &self,
        token: Uuid,
    ) -> Result<(&InvestmentTargetSetId, RevisionNumber), ServiceError> {
        self.targets
            .iter()
            .find(|binding| binding.token == token)
            .map(|binding| (&binding.target_id, binding.revision))
            .ok_or(ServiceError::NotFound)
    }
}

async fn bounded_runtime_shutdown(runtime: ProductionPaperBotRuntime) -> bool {
    runtime.shutdown().await.is_complete()
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
    Public {
        provider: ProductionSourceProvider,
        onboarding_session_id: Uuid,
    },
    CoinbaseDirect {
        provider_session_id: Uuid,
    },
}

impl PaperProvider {
    fn from_selection(selection: &PaperMarketSurfaceSelection) -> Result<Self, ServiceError> {
        let session_id = selection
            .onboarding_session_id()
            .ok_or(ServiceError::Unavailable)?;
        match selection.surface_id().as_str() {
            COINBASE_PUBLIC_SURFACE_ID => Ok(Self::Public {
                provider: ProductionSourceProvider::Coinbase,
                onboarding_session_id: session_id,
            }),
            KRAKEN_PUBLIC_SURFACE_ID => Ok(Self::Public {
                provider: ProductionSourceProvider::Kraken,
                onboarding_session_id: session_id,
            }),
            COINBASE_DIRECT_SURFACE_ID => Ok(Self::CoinbaseDirect {
                provider_session_id: session_id,
            }),
            _ => Err(ServiceError::Unavailable),
        }
    }

    fn surface_id(self) -> Result<SourceIdentifier, ServiceError> {
        let value = match self {
            Self::Public {
                provider: ProductionSourceProvider::Coinbase,
                ..
            } => COINBASE_PUBLIC_SURFACE_ID,
            Self::Public {
                provider: ProductionSourceProvider::Kraken,
                ..
            } => KRAKEN_PUBLIC_SURFACE_ID,
            Self::CoinbaseDirect { .. } => COINBASE_DIRECT_SURFACE_ID,
        };
        SourceIdentifier::try_from(value).map_err(|_error| ServiceError::Internal)
    }

    const fn onboarding_session_id(self) -> Option<Uuid> {
        match self {
            Self::Public {
                onboarding_session_id,
                ..
            } => Some(onboarding_session_id),
            Self::CoinbaseDirect {
                provider_session_id,
            } => Some(provider_session_id),
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

fn order_value(
    order: &PaperOrderSnapshot,
    fills: &[PaperFillSnapshot],
    tokens: &mut ProductAuthorityTokens,
) -> Result<Value, ServiceError> {
    let order_token = tokens.order_token(order.order_id())?;
    let target_token = order
        .target_reference()
        .map(|target| {
            let target_id = InvestmentTargetSetId::try_new(target.target_id())
                .map_err(|_error| ServiceError::Unavailable)?;
            let revision = u32::try_from(target.revision().get())
                .ok()
                .and_then(|revision| RevisionNumber::new(revision).ok())
                .ok_or(ServiceError::Unavailable)?;
            tokens.target_token(&target_id, revision)
        })
        .transpose()?;
    Ok(json!({
        "orderToken": order_token,
        "status": order.state(),
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
        "targetToken": target_token,
        "observed": observed_order_evidence(order, fills),
    }))
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
        "snapshotComplete": snapshot.complete(),
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
        "allowShort": limits.allow_short(),
        "killSwitch": limits.kill_switch(),
    }))
}

fn audit_snapshot_value(
    snapshot: &ProductionExecutionAuditSnapshot,
    tokens: &mut ProductAuthorityTokens,
) -> Result<Value, ServiceError> {
    let mut records = Vec::new();
    records
        .try_reserve_exact(snapshot.records().len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for record in snapshot.records().iter().copied() {
        records.push(audit_record_value(record, tokens)?);
    }
    Ok(json!({
        "records": records,
        "returnedItems": snapshot.returned_items(),
        "availableItems": snapshot.available_items(),
    }))
}

fn audit_record_value(
    record: ProductionExecutionAuditRecord,
    tokens: &mut ProductAuthorityTokens,
) -> Result<Value, ServiceError> {
    let event = record.event();
    let order_token = tokens.order_token(event.order_id())?;
    let mut reasons = Vec::new();
    let reason_count = event.reasons().count();
    reasons
        .try_reserve_exact(reason_count)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for raw_reason in event.reasons() {
        let reason = product_risk_reason(raw_reason);
        if !reasons.contains(&reason) {
            reasons.push(reason);
        }
    }
    Ok(json!({
        "outcome": product_risk_outcome(event.kind()),
        "orderToken": order_token,
        "instrumentId": event.instrument_id(),
        "maximumPriceTicks": event.execution_price_bound().map(|bound| bound.maximum_price()),
        "marketObservedAt": event.market_observed_at(),
        "validUntil": event.valid_until(),
        "observedAt": event.observed_at(),
        "reasons": reasons,
    }))
}

const fn product_risk_outcome(kind: ExecutionAuditKind) -> &'static str {
    match kind {
        ExecutionAuditKind::RiskRejected
        | ExecutionAuditKind::DispatchRejected
        | ExecutionAuditKind::DispatchKnownFailure => "declined",
        ExecutionAuditKind::RiskApproved => "approved",
        ExecutionAuditKind::DispatchAccepted => "accepted",
        ExecutionAuditKind::DispatchUncertain => "needs_review",
        ExecutionAuditKind::CancelAccepted => "cancel_requested",
        ExecutionAuditKind::CancelTerminal => "cancelled",
        ExecutionAuditKind::ReconciliationObserved => "reconciled",
    }
}

const fn product_risk_reason(reason: ExecutionAuditReason) -> &'static str {
    match reason {
        ExecutionAuditReason::Risk(reason) => product_rejection_reason(reason),
        ExecutionAuditReason::AdapterRejected | ExecutionAuditReason::AdapterKnownFailure => {
            "The virtual order was declined."
        }
        ExecutionAuditReason::AdapterUncertain => {
            "The order result needs review before continuing."
        }
        ExecutionAuditReason::ReconciliationRequired => {
            "The account needs reconciliation before another order."
        }
        ExecutionAuditReason::DuplicateApproval
        | ExecutionAuditReason::ApprovalInvalid
        | ExecutionAuditReason::PortfolioRevisionInvalid
        | ExecutionAuditReason::ReceiptMismatch
        | ExecutionAuditReason::ObservationTimestampInvalid
        | ExecutionAuditReason::UnexpectedReconciliationOrder
        | ExecutionAuditReason::AccountReplacementRejected => {
            "The order or account changed before the check completed."
        }
        ExecutionAuditReason::QueueCountSaturated
        | ExecutionAuditReason::QueueBytesSaturated
        | ExecutionAuditReason::TaskOwnershipSaturated
        | ExecutionAuditReason::RegistryCapacity
        | ExecutionAuditReason::RegistryUnavailable
        | ExecutionAuditReason::ClockFailure
        | ExecutionAuditReason::PendingReconciliationCapacity
        | ExecutionAuditReason::OperationDeadlineExceeded
        | ExecutionAuditReason::AuditReasonOverflow => {
            "Paper trading is temporarily unavailable. Try again."
        }
    }
}

const fn product_rejection_reason(reason: RiskRejectionCode) -> &'static str {
    match reason {
        RiskRejectionCode::MarketDepthUnavailable
        | RiskRejectionCode::SourceQuality
        | RiskRejectionCode::SourceIneligible
        | RiskRejectionCode::SourceStale
        | RiskRejectionCode::MarketTimestampInFuture
        | RiskRejectionCode::MarketPredatesSignal
        | RiskRejectionCode::InstrumentDefinitionMismatch => {
            "Market data is unavailable or too old."
        }
        RiskRejectionCode::InstrumentNotTrading => "The investment cannot be traded right now.",
        RiskRejectionCode::PolicyExpired
        | RiskRejectionCode::IntentExpired
        | RiskRejectionCode::StopNotTriggered => {
            "The order is no longer valid at current conditions."
        }
        RiskRejectionCode::InvalidReferencePrice
        | RiskRejectionCode::OrderPriceLimit
        | RiskRejectionCode::IntentSlippageLimit
        | RiskRejectionCode::PolicySlippageLimit
        | RiskRejectionCode::PriceDeviationLimit => {
            "The order is outside the active price and slippage limits."
        }
        RiskRejectionCode::Account(reason) => product_account_risk_reason(reason),
        RiskRejectionCode::Portfolio(_) => "The account needs reconciliation before another order.",
        RiskRejectionCode::ClockFailure
        | RiskRejectionCode::ClockRollback
        | RiskRejectionCode::Authority
        | RiskRejectionCode::ApprovalIdentity
        | RiskRejectionCode::AuditUnavailable => {
            "Paper trading is temporarily unavailable. Try again."
        }
    }
}

const fn product_account_risk_reason(reason: AccountRiskViolation) -> &'static str {
    match reason {
        AccountRiskViolation::KillSwitch => "Paper trading is paused by the emergency stop.",
        AccountRiskViolation::InsufficientPosition | AccountRiskViolation::InsufficientCash => {
            "Available cash or holdings are insufficient."
        }
        AccountRiskViolation::InstrumentIneligible
        | AccountRiskViolation::CurrencyMismatch
        | AccountRiskViolation::UnsupportedSettlement => {
            "The investment is not eligible for paper trading."
        }
        AccountRiskViolation::ReconciliationRequired
        | AccountRiskViolation::PortfolioStateMismatch => {
            "The account needs reconciliation before another order."
        }
        AccountRiskViolation::IntentExpired
        | AccountRiskViolation::IntentLifetimeExceeded
        | AccountRiskViolation::DuplicateClientOrder
        | AccountRiskViolation::DuplicateOrder => {
            "The order is no longer valid at current conditions."
        }
        AccountRiskViolation::OrderRateLimit
        | AccountRiskViolation::OrderNotionalLimit
        | AccountRiskViolation::PositionLimit
        | AccountRiskViolation::ExposureLimit
        | AccountRiskViolation::LeverageLimit
        | AccountRiskViolation::CapitalLimit
        | AccountRiskViolation::LossLimit
        | AccountRiskViolation::DrawdownLimit => "The order is outside the active safety limits.",
        AccountRiskViolation::AccountNotFound | AccountRiskViolation::AccountIneligible => {
            "The virtual account is not eligible for paper trading."
        }
        AccountRiskViolation::IdempotencyCapacity
        | AccountRiskViolation::IdempotencyRevisionExhausted
        | AccountRiskViolation::ReservationCapacity
        | AccountRiskViolation::ArithmeticOverflow
        | AccountRiskViolation::AccountCoordinatorBusy
        | AccountRiskViolation::AccountCoordinatorPoisoned
        | AccountRiskViolation::ClockFailure => {
            "Paper trading is temporarily unavailable. Try again."
        }
    }
}

fn fill_value(
    fill: PaperFillSnapshot,
    tokens: &mut ProductAuthorityTokens,
) -> Result<Value, ServiceError> {
    let order_token = tokens.order_token(fill.order_id())?;
    Ok(json!({
        "orderToken": order_token,
        "eventAt": fill.event_at(),
        "quantityLots": fill.quantity(),
        "averagePriceTicks": fill.average_price(),
        "maximumPriceTicks": fill.maximum_price(),
        "notional": fill.notional(),
        "fee": fill.fee(),
        "liquidity": fill.liquidity(),
    }))
}

fn cancel_receipt_value(receipt: CancelReceipt, order_token: Uuid) -> Value {
    json!({
        "orderToken": order_token,
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
        "ordersChecked": state.orders().len(),
        "accountsChecked": state.accounts().len(),
        "marketDataReady": state.source_binding().is_some(),
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
