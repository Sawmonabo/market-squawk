//! Lifecycle-owned paper bot and execution application services.

mod market;
mod product;
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
use market_squawk_adapter_paper::PaperExecutionSnapshot;
use market_squawk_data::{InstrumentDefinitionReadCapability, MarketDataInstrumentReadCapability};
use market_squawk_decisions::{InvestmentTargetSetId, TargetState, TargetStatus};
use market_squawk_domain::{
    BasisPoints, Currency, DigestAlgorithm, InstrumentExecutionTerms, Money, OrderId, OrderSide,
    OrderType, PriceTicks, QuantityLots, RevisionNumber, SourceIdentifier, TimeInForce, Timestamp,
};
use market_squawk_execution::{
    AccountRiskViolation, ExecutionAdapterError, ExecutionAuditKind, ExecutionAuditReason,
    ExecutionDispatchError, ExecutionState, ManualPaperDraft, ManualPaperDraftInput,
    OrderTargetReference, RiskRejectionCode,
};
use market_squawk_live::{ActiveLiveActionHookGroup, LiveActionHookGeneration, ShardKey};
use market_squawk_services::{
    RequestContext, RequestOrigin, ServiceDomain, ServiceError, ToolResultMetadata,
    TypedToolRequest, TypedToolResult,
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
use super::research::MarketHistoryReadCapability;
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
const BOT_GET_START_PREPARATION: &str = "Bot.GetStartPreparation";
const BOT_PREPARE_START: &str = "Bot.PrepareStart";
const BOT_START: &str = "Bot.Start";
const BOT_STOP: &str = "Bot.Stop";
const RISK_TRIGGER_KILL_SWITCH: &str = "Risk.TriggerKillSwitch";
const EXECUTION_GET_ORDERS: &str = "Execution.GetOrders";
const EXECUTION_GET_FILLS: &str = "Execution.GetFills";
const EXECUTION_CANCEL: &str = "Execution.Cancel";
const EXECUTION_RECONCILE: &str = "Execution.Reconcile";
const EXECUTION_GET_MANUAL_PAPER_TARGETS: &str = "Execution.GetManualPaperTargets";
const EXECUTION_PREPARE_MANUAL_PAPER_DRAFT: &str = "Execution.PrepareManualPaperDraft";
const EXECUTION_SUBMIT_MANUAL_PAPER_DRAFT: &str = "Execution.SubmitManualPaperDraft";
const MANUAL_PAPER_DRAFT_LIFETIME: Duration = Duration::from_secs(60);
const PAPER_START_PREPARATION_LIFETIME: Duration = Duration::from_secs(60);
const MAXIMUM_PENDING_PAPER_PREPARATIONS: usize = 64;
/// Upper bound inherited from the local decision catalog's installed-product capacity.
const MAXIMUM_MANUAL_PAPER_TARGET_INDEX_ENTRIES: usize = 4_096;
const MAXIMUM_PRODUCT_MANUAL_PAPER_TARGETS: usize = 100;
const MAXIMUM_PRODUCT_ORDER_TOKENS: usize = 4_096;

/// Shared paper lifecycle exposed as distinct Bot and Execution domain services.
pub struct PaperApplicationServices {
    controller: Arc<PaperController>,
    market_runtime: Arc<MarketRuntimeRegistry>,
    instrument_definitions: InstrumentDefinitionReadCapability,
    market_data_instruments: MarketDataInstrumentReadCapability,
    reference_search: Arc<dyn market::MarketReferenceSearchAuthority>,
    market_history: MarketHistoryReadCapability,
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
        market_history: MarketHistoryReadCapability,
    ) -> Self {
        Self {
            controller: Arc::new(PaperController::new(
                config,
                decisions,
                Arc::clone(&market_runtime),
                instrument_definitions.clone(),
                market_data_instruments.clone(),
            )),
            market_runtime,
            instrument_definitions,
            market_data_instruments,
            reference_search,
            market_history,
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
            self.market_history.clone(),
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
            BOT_GET_START_PREPARATION => self.controller.start_preparation(&context).await?,
            BOT_PREPARE_START => self.controller.prepare_start(&request, &context).await?,
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
            EXECUTION_PREPARE_MANUAL_PAPER_DRAFT => {
                self.controller
                    .prepare_manual_paper_order(&request, &context)
                    .await?
            }
            EXECUTION_SUBMIT_MANUAL_PAPER_DRAFT => {
                self.controller
                    .submit_manual_paper_order(&request, &context)
                    .await?
            }
            EXECUTION_CANCEL => {
                let order_token = required_string(&request, "actionToken")?.to_owned();
                let result = self.controller.cancel(&order_token, &context).await?;
                (result, 1, 1)
            }
            EXECUTION_RECONCILE => {
                let state = self.controller.reconcile(&context).await?;
                (
                    json!({
                        "observedAt": state.observed_at().unix_nanos(),
                        "ordersChecked": state.orders().len(),
                        "accountsChecked": state.accounts().len(),
                        "marketDataReady": state.source_binding().is_some(),
                        "reconciliationRequired": state.reconciliation_required(),
                    }),
                    1,
                    1,
                )
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
    instrument_definitions: InstrumentDefinitionReadCapability,
    market_data_instruments: MarketDataInstrumentReadCapability,
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
        instrument_definitions: InstrumentDefinitionReadCapability,
        market_data_instruments: MarketDataInstrumentReadCapability,
    ) -> Self {
        Self {
            config,
            decisions,
            market_runtime,
            instrument_definitions,
            market_data_instruments,
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
            PaperState::Stopped { .. } => Ok(json!({
                "sessionAvailability": "ready",
                "safeguards": "active",
            })),
            PaperState::CleanupRequired { .. } => Ok(json!({
                "sessionAvailability": "unavailable",
                "safeguards": "action_needed",
            })),
            PaperState::Starting { .. } | PaperState::Stopping => Ok(json!({
                "sessionAvailability": "unavailable",
                "safeguards": "active",
            })),
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
                        "sessionAvailability": "unavailable",
                        "safeguards": "action_needed",
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
                let required =
                    product::required_instruments(&snapshot, &audit, runtime.risk_limits());
                let instruments = product::load_instruments(
                    &required,
                    &self.instrument_definitions,
                    &self.market_data_instruments,
                    context.deadline(),
                    context.cancellation(),
                )?;
                product::status(
                    *strategy_mode,
                    &snapshot,
                    &audit,
                    runtime.risk_limits(),
                    &instruments,
                    maximum_items,
                    financial_reconciliation_current,
                )
            }
        }
    }

    async fn start_preparation(&self, context: &RequestContext) -> Result<Value, ServiceError> {
        ensure_live(context)?;
        {
            let state =
                bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
            if !matches!(&*state, PaperState::Stopped { .. }) {
                return Err(ServiceError::InvalidRequest);
            }
        }
        self.market_runtime
            .select_paper_market_surface(context.deadline(), context.cancellation())
            .await?;
        let currency = configured_paper_currency(&self.config)?;
        Ok(paper_start_preparation(currency)?)
    }

    async fn prepare_start(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        ensure_live(context)?;
        let origin = required_origin(context)?;
        {
            let state =
                bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
            if !matches!(&*state, PaperState::Stopped { .. }) {
                return Err(ServiceError::InvalidRequest);
            }
        }
        self.market_runtime
            .select_paper_market_surface(context.deadline(), context.cancellation())
            .await?;
        let currency = configured_paper_currency(&self.config)?;
        let cash = resolve_paper_cash_choice(required_string(request, "cashChoice")?)?;
        let cost = resolve_paper_cost_choice(required_string(request, "costChoice")?)?;
        let mode = resolve_paper_mode_choice(required_string(request, "modeChoice")?)?;
        let now = current_timestamp()?;
        let expires_at = now
            .checked_add_nanos(
                i64::try_from(PAPER_START_PREPARATION_LIFETIME.as_nanos())
                    .map_err(|_error| ServiceError::Unavailable)?,
            )
            .map_err(|_error| ServiceError::Unavailable)?;
        let expires_at_instant = Instant::now()
            .checked_add(PAPER_START_PREPARATION_LIFETIME)
            .ok_or(ServiceError::Unavailable)?;
        let initial_cash = cash
            .amount
            .parse::<Decimal>()
            .map_err(|_error| ServiceError::Internal)?;
        let prepared = PreparedPaperStart {
            origin,
            expires_at: expires_at_instant,
            initial_cash,
            fee_basis_points: cost.basis_points,
            strategy_mode: mode.mode,
        };
        let confirmation_token = bounded_lock(
            &self.product_tokens,
            context.deadline(),
            context.cancellation(),
        )
        .await?
        .insert_start(prepared)?;
        Ok(json!({
            "confirmationToken": confirmation_token,
            "expiresAt": product::timestamp(expires_at),
            "virtualCash": product::money(Money::new(initial_cash, currency)),
            "estimatedTradingCost": product::percentage(BasisPoints::new(i32::try_from(cost.basis_points).map_err(|_error| ServiceError::Internal)?)),
            "modeLabel": mode.label,
            "safeguards": [
                "This session uses virtual cash and cannot place brokerage orders.",
                "Every virtual order remains subject to the active account and price safeguards.",
                "Starting the session does not place a virtual order.",
            ],
        }))
    }

    async fn start(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let origin = required_origin(context)?;
        let confirmation_token = required_string(request, "confirmationToken")?;
        let prepared = bounded_lock(
            &self.product_tokens,
            context.deadline(),
            context.cancellation(),
        )
        .await?
        .consume_start(confirmation_token, origin, Instant::now())?;
        let _owner =
            bounded_lock(&self.owner_gate, context.deadline(), context.cancellation()).await?;
        self.start_paper_before_owned(prepared, context.deadline(), context.cancellation())
            .await
    }

    async fn start_paper_before_owned(
        &self,
        prepared: PreparedPaperStart,
        deadline: Instant,
        request_cancellation: &CancellationToken,
    ) -> Result<Value, ServiceError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::Unavailable);
        }
        if Instant::now() >= prepared.expires_at {
            return Err(ServiceError::InvalidRequest);
        }
        let market_selection = self
            .market_runtime
            .select_paper_market_surface(deadline, request_cancellation)
            .await?;
        let provider = PaperProvider::from_selection(&market_selection)?;
        let surface_id = provider.surface_id()?;
        let onboarding_session_id = provider.onboarding_session_id();
        let strategy_mode = prepared.strategy_mode;
        let initial_cash = prepared.initial_cash;
        let fee_basis_points = prepared.fee_basis_points;
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
                "sessionAvailability": "active",
                "safeguards": "active",
                "modeLabel": paper_mode_label(strategy_mode),
                "message": "The virtual paper session is active. No brokerage order was placed.",
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
    async fn stop(&self, _reason: &str, context: &RequestContext) -> Result<Value, ServiceError> {
        let _owner =
            bounded_lock(&self.owner_gate, context.deadline(), context.cancellation()).await?;
        let complete = self
            .stop_paper_before_owned(context.deadline(), context.cancellation())
            .await?;
        if !complete {
            return Err(ServiceError::Unavailable);
        }
        Ok(json!({
            "sessionAvailability": "ready",
            "safeguards": "active",
            "message": "The paper session is stopped and no new virtual orders will be placed.",
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
        let ids = product::execution_instruments(&snapshot);
        let instruments = product::load_instruments(
            &ids,
            &self.instrument_definitions,
            &self.market_data_instruments,
            context.deadline(),
            context.cancellation(),
        )?;
        for order in &snapshot.orders()[..returned] {
            values.push(product::order(order, &instruments, &mut tokens)?);
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
        let ids = product::execution_instruments(&snapshot);
        let instruments = product::load_instruments(
            &ids,
            &self.instrument_definitions,
            &self.market_data_instruments,
            context.deadline(),
            context.cancellation(),
        )?;
        for fill in snapshot.fills()[..returned].iter().copied() {
            values.push(product::fill(fill, snapshot.orders(), &instruments)?);
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
        let paper_snapshot = runtime
            .paper_snapshot(context.deadline(), context.cancellation())
            .await
            .map_err(map_control_error)?;
        let entries = self
            .decisions
            .list_target_index(MAXIMUM_MANUAL_PAPER_TARGET_INDEX_ENTRIES)
            .map_err(map_decision_error)?;
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(entries.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        {
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
                let instrument_id = target.target().target().instrument_id();
                let can_sell = runtime.risk_limits().allow_short()
                    || paper_snapshot.positions().iter().any(|position| {
                        position.instrument_id() == instrument_id && position.lots() > 0
                    });
                prepared.push((target, target_token, instrument_id, can_sell));
                if prepared.len() > MAXIMUM_PRODUCT_MANUAL_PAPER_TARGETS {
                    return Err(ServiceError::ResourceExhausted);
                }
            }
        }
        if prepared.len() > maximum_items {
            return Err(ServiceError::ResourceExhausted);
        }
        let mut instrument_ids = Vec::new();
        instrument_ids
            .try_reserve_exact(prepared.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        instrument_ids.extend(
            prepared
                .iter()
                .map(|(_, _, instrument_id, _)| *instrument_id),
        );
        instrument_ids.sort_unstable();
        instrument_ids.dedup();
        let instruments = product::load_instruments(
            &instrument_ids,
            &self.instrument_definitions,
            &self.market_data_instruments,
            context.deadline(),
            context.cancellation(),
        )?;
        let mut targets = Vec::new();
        targets
            .try_reserve_exact(prepared.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for (target, target_token, instrument_id, can_sell) in prepared {
            targets.push(product::manual_target(
                &target,
                &target_token,
                product::instrument(&instruments, instrument_id)?,
                can_sell,
            )?);
        }
        let count = targets.len();
        Ok((json!({"targets": targets}), count, count))
    }

    async fn prepare_manual_paper_order(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<(Value, usize, usize), ServiceError> {
        ensure_live(context)?;
        let origin = required_origin(context)?;
        let target_token = required_string(request, "targetToken")?;
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
            let entries = self
                .decisions
                .list_target_index(MAXIMUM_MANUAL_PAPER_TARGET_INDEX_ENTRIES)
                .map_err(map_decision_error)?;
            let mut product_tokens = bounded_lock(
                &self.product_tokens,
                context.deadline(),
                context.cancellation(),
            )
            .await?;
            let mut resolved = None;
            for entry in &entries {
                let candidate = product_tokens.target_token(entry.id(), entry.revision())?;
                if candidate.as_ref() != target_token {
                    continue;
                }
                if resolved
                    .replace((entry.id().clone(), entry.revision()))
                    .is_some()
                {
                    return Err(ServiceError::Unavailable);
                }
            }
            resolved.ok_or(ServiceError::NotFound)?
        };
        let target = self.current_active_target(&target_id, target_revision, now)?;
        let manual_route = sole_compatible_manual_route(runtime, &target)?;
        if !manual_choice_is_compatible(order_type, time_in_force) {
            return Err(ServiceError::InvalidRequest);
        }
        if side == OrderSide::Sell && !runtime.risk_limits().allow_short() {
            let snapshot = runtime
                .paper_snapshot(context.deadline(), context.cancellation())
                .await
                .map_err(map_control_error)?;
            if !snapshot.positions().iter().any(|position| {
                position.instrument_id() == target.target().target().instrument_id()
                    && position.lots() > 0
            }) {
                return Err(ServiceError::InvalidRequest);
            }
        }
        let route = manual_route.route();
        let terms = manual_route.execution_terms();
        let target_core = target.target().target();
        if target_core.reference_mark().price().currency() != terms.quote_currency() {
            return Err(ServiceError::InvalidRequest);
        }
        let limit_selection =
            selected_target_price(request, "limitTargetLevel", &target, terms, order_type)?;
        let stop_selection =
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
            limit_price: limit_selection.map(|(_, price)| price),
            stop_price: stop_selection.map(|(_, price)| price),
            time_in_force,
            expires_at,
            reason_code: manual_paper_reason_code().map_err(|_error| ServiceError::Unavailable)?,
            maximum_slippage: runtime.risk_limits().maximum_slippage(),
            target_reference,
        })
        .map_err(|_error| ServiceError::InvalidRequest)?;
        let instrument_id = target_core.instrument_id();
        let instruments = product::load_instruments(
            &[instrument_id],
            &self.instrument_definitions,
            &self.market_data_instruments,
            context.deadline(),
            context.cancellation(),
        )?;
        let instrument = product::instrument(&instruments, instrument_id)?;
        let prepared = PreparedManualPaper {
            origin,
            expires_at: Instant::now()
                .checked_add(MANUAL_PAPER_DRAFT_LIFETIME)
                .ok_or(ServiceError::Unavailable)?,
            target_id,
            target_revision,
            target_sha256: content_digest.bytes(),
            route: route.clone(),
            terms,
            maximum_order_notional: runtime.risk_limits().maximum_order_notional(),
            maximum_slippage: runtime.risk_limits().maximum_slippage(),
            allow_short: runtime.risk_limits().allow_short(),
            draft,
        };
        let confirmation_token = bounded_lock(
            &self.product_tokens,
            context.deadline(),
            context.cancellation(),
        )
        .await?
        .insert_manual(prepared)?;
        Ok((
            manual_paper_preview(
                &confirmation_token,
                expires_at,
                instrument,
                side,
                order_type,
                quantity,
                terms,
                limit_selection,
                stop_selection,
                time_in_force,
                runtime.risk_limits().maximum_order_notional(),
                runtime.risk_limits().maximum_slippage(),
                runtime.risk_limits().allow_short(),
            )?,
            1,
            1,
        ))
    }

    async fn submit_manual_paper_order(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<(Value, usize, usize), ServiceError> {
        ensure_live(context)?;
        let origin = required_origin(context)?;
        let confirmation_token = required_string(request, "confirmationToken")?;
        let prepared = bounded_lock(
            &self.product_tokens,
            context.deadline(),
            context.cancellation(),
        )
        .await?
        .consume_manual(confirmation_token, origin, Instant::now())?;
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
        let target = self.current_active_target(
            &prepared.target_id,
            prepared.target_revision,
            current_timestamp()?,
        )?;
        let digest = target
            .target()
            .target()
            .content_identity()
            .evidence_digest();
        if digest.algorithm() != DigestAlgorithm::Sha256
            || digest.bytes() != prepared.target_sha256
            || runtime.risk_limits().maximum_order_notional() != prepared.maximum_order_notional
            || runtime.risk_limits().maximum_slippage() != prepared.maximum_slippage
            || runtime.risk_limits().allow_short() != prepared.allow_short
        {
            return Err(ServiceError::InvalidRequest);
        }
        let mut matched_route = None;
        for candidate in runtime.manual_paper_routes() {
            if candidate.route() != &prepared.route || candidate.execution_terms() != prepared.terms
            {
                continue;
            }
            if matched_route.replace(candidate).is_some() {
                return Err(ServiceError::Unavailable);
            }
        }
        let route = matched_route.ok_or(ServiceError::InvalidRequest)?;
        runtime
            .try_submit_manual_paper_draft(route.route(), prepared.draft)
            .map_err(map_manual_paper_ingress_error)?;
        Ok((
            json!({
                "accepted": true,
                "message": "The virtual order was accepted and remains subject to the active safeguards.",
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
        let snapshot = runtime
            .paper_snapshot(context.deadline(), context.cancellation())
            .await
            .map_err(map_control_error)?;
        Ok(Some(snapshot))
    }

    async fn cancel(
        &self,
        order_token: &str,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
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
        let before_cancel = runtime
            .paper_snapshot(context.deadline(), context.cancellation())
            .await
            .map_err(map_control_error)?;
        let order_id = {
            let mut tokens = bounded_lock(
                &self.product_tokens,
                context.deadline(),
                context.cancellation(),
            )
            .await?;
            let mut resolved = None;
            for order in before_cancel.orders() {
                if tokens.order_token(order.order_id())?.as_ref() != order_token {
                    continue;
                }
                if resolved.replace(order.order_id()).is_some() {
                    return Err(ServiceError::Unavailable);
                }
            }
            resolved.ok_or(ServiceError::NotFound)?
        };
        let receipt = runtime
            .cancel_tracked_order(order_id, context.deadline(), context.cancellation())
            .await
            .map_err(map_control_error)?;
        let snapshot = runtime
            .paper_snapshot(context.deadline(), context.cancellation())
            .await
            .map_err(map_control_error)?;
        let order = snapshot
            .orders()
            .iter()
            .find(|order| order.order_id() == order_id)
            .ok_or(ServiceError::Unavailable)?;
        product::cancel(receipt, order_token, order)
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

const fn manual_choice_is_compatible(order_type: OrderType, time_in_force: TimeInForce) -> bool {
    match order_type {
        OrderType::Market => matches!(
            time_in_force,
            TimeInForce::Day | TimeInForce::ImmediateOrCancel | TimeInForce::FillOrKill
        ),
        OrderType::Limit => true,
        OrderType::Stop | OrderType::StopLimit => {
            matches!(
                time_in_force,
                TimeInForce::Day | TimeInForce::GoodTilCancelled
            )
        }
    }
}

fn selected_target_price(
    request: &TypedToolRequest,
    field: &str,
    target: &TargetState,
    terms: market_squawk_domain::InstrumentExecutionTerms,
    order_type: OrderType,
) -> Result<Option<(TargetLadderSelector, PriceTicks)>, ServiceError> {
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
    let selector = TargetLadderSelector::parse(selector)?;
    let price = selector.price(target);
    if price.currency() != terms.quote_currency() {
        return Err(ServiceError::InvalidRequest);
    }
    PriceTicks::try_from_decimal(price.amount(), terms.price_tick())
        .map(|price| Some((selector, price)))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn target_currently_usable(target: &TargetState, now: Timestamp) -> bool {
    target.status() == TargetStatus::Active
        && target.target().effective_at() <= now
        && now < target.target().target().expires_at()
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

fn required_origin(context: &RequestContext) -> Result<RequestOrigin, ServiceError> {
    context.origin().ok_or(ServiceError::Unauthorized)
}

#[derive(Clone, Copy)]
struct PaperCashChoice {
    id: &'static str,
    label: &'static str,
    amount: &'static str,
    explanation: &'static str,
}

#[derive(Clone, Copy)]
struct PaperCostChoice {
    id: &'static str,
    label: &'static str,
    basis_points: u32,
    explanation: &'static str,
}

#[derive(Clone, Copy)]
struct PaperModeChoice {
    id: &'static str,
    label: &'static str,
    mode: PaperStrategyMode,
    explanation: &'static str,
}

const PAPER_CASH_CHOICES: [PaperCashChoice; 3] = [
    PaperCashChoice {
        id: "starter",
        label: "Starter virtual portfolio",
        amount: "25000",
        explanation: "Practice with a smaller virtual balance.",
    },
    PaperCashChoice {
        id: "standard",
        label: "Standard virtual portfolio",
        amount: "100000",
        explanation: "Practice with a mid-sized virtual balance.",
    },
    PaperCashChoice {
        id: "expanded",
        label: "Expanded virtual portfolio",
        amount: "250000",
        explanation: "Practice with a larger virtual balance while keeping the same safeguards.",
    },
];

const PAPER_COST_CHOICES: [PaperCostChoice; 3] = [
    PaperCostChoice {
        id: "low",
        label: "Lower estimated costs",
        basis_points: 10,
        explanation: "Estimate trading costs at 0.1% of each filled amount.",
    },
    PaperCostChoice {
        id: "moderate",
        label: "Moderate estimated costs",
        basis_points: 25,
        explanation: "Estimate trading costs at 0.25% of each filled amount.",
    },
    PaperCostChoice {
        id: "high",
        label: "Higher estimated costs",
        basis_points: 50,
        explanation: "Estimate trading costs at 0.5% of each filled amount.",
    },
];

const PAPER_MODE_CHOICES: [PaperModeChoice; 2] = [
    PaperModeChoice {
        id: "manual",
        label: "Manual practice",
        mode: PaperStrategyMode::Manual,
        explanation: "Only virtual orders that you explicitly prepare and confirm can proceed.",
    },
    PaperModeChoice {
        id: "guided",
        label: "Guided practice",
        mode: PaperStrategyMode::BookImbalance,
        explanation: "Practice the installed guided strategy within the same virtual-account safeguards.",
    },
];

fn configured_paper_currency(config: &AppConfig) -> Result<Currency, ServiceError> {
    let mut currency = None;
    for product in config.products() {
        let code = product
            .rsplit_once('-')
            .map(|(_, code)| code)
            .ok_or(ServiceError::Unavailable)?;
        let candidate = Currency::try_from(code).map_err(|_error| ServiceError::Unavailable)?;
        if currency
            .replace(candidate)
            .is_some_and(|current| current != candidate)
        {
            return Err(ServiceError::Unavailable);
        }
    }
    currency.ok_or(ServiceError::Unavailable)
}

fn paper_choice_token(kind: &str, id: &str) -> Result<Box<str>, ServiceError> {
    super::opaque_product_text_token(
        "paper_choice_",
        b"market-squawk/product-paper-start-choice/v1\0",
        &[kind.as_bytes(), id.as_bytes()],
        512,
    )
    .map_err(|_| ServiceError::ResourceExhausted)
}

fn paper_start_preparation(currency: Currency) -> Result<Value, ServiceError> {
    let mut tokens = Vec::new();
    tokens
        .try_reserve_exact(
            PAPER_CASH_CHOICES.len() + PAPER_COST_CHOICES.len() + PAPER_MODE_CHOICES.len(),
        )
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    let cash = PAPER_CASH_CHOICES
        .iter()
        .map(|choice| {
            let token = paper_choice_token("cash", choice.id)?;
            tokens.push(token.clone());
            let amount = choice
                .amount
                .parse::<Decimal>()
                .map_err(|_error| ServiceError::Internal)?;
            Ok(json!({
                "choiceToken": token,
                "label": choice.label,
                "amount": product::money(Money::new(amount, currency)),
                "explanation": choice.explanation,
            }))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    let costs = PAPER_COST_CHOICES
        .iter()
        .map(|choice| {
            let token = paper_choice_token("cost", choice.id)?;
            tokens.push(token.clone());
            Ok(json!({
                "choiceToken": token,
                "label": choice.label,
                "estimatedTradingCost": product::percentage(BasisPoints::new(i32::try_from(choice.basis_points).map_err(|_error| ServiceError::Internal)?)),
                "explanation": choice.explanation,
            }))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    let modes = PAPER_MODE_CHOICES
        .iter()
        .map(|choice| {
            let token = paper_choice_token("mode", choice.id)?;
            tokens.push(token.clone());
            Ok(json!({
                "choiceToken": token,
                "label": choice.label,
                "explanation": choice.explanation,
            }))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    tokens.sort_unstable();
    if tokens.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ServiceError::Unavailable);
    }
    Ok(json!({
        "virtualCashChoices": cash,
        "costChoices": costs,
        "modeChoices": modes,
    }))
}

fn resolve_paper_cash_choice(token: &str) -> Result<PaperCashChoice, ServiceError> {
    resolve_paper_choice(token, "cash", &PAPER_CASH_CHOICES, |choice| choice.id)
}

fn resolve_paper_cost_choice(token: &str) -> Result<PaperCostChoice, ServiceError> {
    resolve_paper_choice(token, "cost", &PAPER_COST_CHOICES, |choice| choice.id)
}

fn resolve_paper_mode_choice(token: &str) -> Result<PaperModeChoice, ServiceError> {
    resolve_paper_choice(token, "mode", &PAPER_MODE_CHOICES, |choice| choice.id)
}

fn resolve_paper_choice<Choice: Copy>(
    token: &str,
    kind: &str,
    choices: &[Choice],
    id: impl Fn(Choice) -> &'static str,
) -> Result<Choice, ServiceError> {
    let mut resolved = None;
    for choice in choices.iter().copied() {
        if paper_choice_token(kind, id(choice))?.as_ref() != token {
            continue;
        }
        if resolved.replace(choice).is_some() {
            return Err(ServiceError::Unavailable);
        }
    }
    resolved.ok_or(ServiceError::NotFound)
}

const fn paper_mode_label(mode: PaperStrategyMode) -> &'static str {
    match mode {
        PaperStrategyMode::Manual => "Manual practice",
        PaperStrategyMode::BookImbalance => "Guided practice",
    }
}

#[allow(clippy::too_many_arguments)]
fn manual_paper_preview(
    confirmation_token: &str,
    expires_at: Timestamp,
    instrument: &product::ProductInstrument,
    side: OrderSide,
    order_type: OrderType,
    quantity: QuantityLots,
    terms: InstrumentExecutionTerms,
    limit: Option<(TargetLadderSelector, PriceTicks)>,
    stop: Option<(TargetLadderSelector, PriceTicks)>,
    time_in_force: TimeInForce,
    maximum_order_notional: Money,
    maximum_slippage: BasisPoints,
    allow_short: bool,
) -> Result<Value, ServiceError> {
    let condition =
        |selection: Option<(TargetLadderSelector, PriceTicks)>| -> Result<Value, ServiceError> {
            selection.map_or(Ok(Value::Null), |(level, price)| {
                Ok(json!({
                    "label": level.label(),
                    "value": product::price(price, terms)?,
                }))
            })
        };
    Ok(json!({
        "confirmationToken": confirmation_token,
        "expiresAt": product::timestamp(expires_at),
        "investment": product::investment(instrument),
        "direction": match side { OrderSide::Buy => "Buy", OrderSide::Sell => "Sell" },
        "orderApproach": match order_type { OrderType::Market => "Market", OrderType::Limit => "Limit", OrderType::Stop => "Stop", OrderType::StopLimit => "Stop limit" },
        "quantity": product::quantity(quantity, terms)?,
        "duration": match time_in_force { TimeInForce::Day => "Today", TimeInForce::GoodTilCancelled => "Until cancelled", TimeInForce::ImmediateOrCancel => "Fill now or cancel", TimeInForce::FillOrKill => "All now or cancel" },
        "limitCondition": condition(limit)?,
        "stopCondition": condition(stop)?,
        "safeguards": {
            "maximumOrderValue": product::money(maximum_order_notional),
            "maximumSlippage": product::percentage(maximum_slippage),
            "shorting": if allow_short { "allowed" } else { "disabled" },
        },
        "simulationWarning": "This is a virtual trade. It cannot place a brokerage order or use real money.",
    }))
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
    start_preparations: Vec<ProductStartPreparation>,
    manual_preparations: Vec<ProductManualPreparation>,
}

struct ProductOrderToken {
    token: Box<str>,
    order_id: OrderId,
}

struct ProductTargetToken {
    token: Box<str>,
    target_id: InvestmentTargetSetId,
    revision: RevisionNumber,
}

struct PreparedPaperStart {
    origin: RequestOrigin,
    expires_at: Instant,
    initial_cash: Decimal,
    fee_basis_points: u32,
    strategy_mode: PaperStrategyMode,
}

struct ProductStartPreparation {
    token: Box<str>,
    prepared: PreparedPaperStart,
}

struct PreparedManualPaper {
    origin: RequestOrigin,
    expires_at: Instant,
    target_id: InvestmentTargetSetId,
    target_revision: RevisionNumber,
    target_sha256: [u8; 32],
    route: ShardKey,
    terms: InstrumentExecutionTerms,
    maximum_order_notional: Money,
    maximum_slippage: BasisPoints,
    allow_short: bool,
    draft: ManualPaperDraft,
}

struct ProductManualPreparation {
    token: Box<str>,
    prepared: PreparedManualPaper,
}

impl ProductAuthorityTokens {
    fn clear(&mut self) {
        self.orders.clear();
        self.targets.clear();
        self.start_preparations.clear();
        self.manual_preparations.clear();
    }

    fn insert_start(&mut self, prepared: PreparedPaperStart) -> Result<Box<str>, ServiceError> {
        self.prune_preparations(Instant::now());
        if self.start_preparations.len() >= MAXIMUM_PENDING_PAPER_PREPARATIONS {
            return Err(ServiceError::ResourceExhausted);
        }
        self.start_preparations
            .try_reserve(1)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let token = self.unique_confirmation_token(
            b"market-squawk/product-paper-start-confirmation/v1\0",
            prepared.origin,
        )?;
        self.start_preparations.push(ProductStartPreparation {
            token: token.clone(),
            prepared,
        });
        Ok(token)
    }

    fn consume_start(
        &mut self,
        token: &str,
        origin: RequestOrigin,
        now: Instant,
    ) -> Result<PreparedPaperStart, ServiceError> {
        self.prune_preparations(now);
        let index = unique_preparation_index(
            self.start_preparations
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.token.as_ref() == token)
                .map(|(index, entry)| (index, entry.prepared.origin)),
            origin,
        )?;
        Ok(self.start_preparations.remove(index).prepared)
    }

    fn insert_manual(&mut self, prepared: PreparedManualPaper) -> Result<Box<str>, ServiceError> {
        self.prune_preparations(Instant::now());
        if self.manual_preparations.len() >= MAXIMUM_PENDING_PAPER_PREPARATIONS {
            return Err(ServiceError::ResourceExhausted);
        }
        self.manual_preparations
            .try_reserve(1)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let token = self.unique_confirmation_token(
            b"market-squawk/product-paper-manual-confirmation/v1\0",
            prepared.origin,
        )?;
        self.manual_preparations.push(ProductManualPreparation {
            token: token.clone(),
            prepared,
        });
        Ok(token)
    }

    fn consume_manual(
        &mut self,
        token: &str,
        origin: RequestOrigin,
        now: Instant,
    ) -> Result<PreparedManualPaper, ServiceError> {
        self.prune_preparations(now);
        let index = unique_preparation_index(
            self.manual_preparations
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.token.as_ref() == token)
                .map(|(index, entry)| (index, entry.prepared.origin)),
            origin,
        )?;
        Ok(self.manual_preparations.remove(index).prepared)
    }

    fn prune_preparations(&mut self, now: Instant) {
        self.start_preparations
            .retain(|entry| entry.prepared.expires_at > now);
        self.manual_preparations
            .retain(|entry| entry.prepared.expires_at > now);
    }

    fn unique_confirmation_token(
        &self,
        domain: &[u8],
        origin: RequestOrigin,
    ) -> Result<Box<str>, ServiceError> {
        for _ in 0..16 {
            let nonce = Uuid::new_v4();
            let token = super::opaque_product_text_token(
                "paper_confirm_",
                domain,
                &[
                    origin.workspace_id().as_bytes(),
                    origin.client_id().as_bytes(),
                    nonce.as_bytes(),
                ],
                512,
            )
            .map_err(|_| ServiceError::ResourceExhausted)?;
            if !self
                .start_preparations
                .iter()
                .any(|entry| entry.token == token)
                && !self
                    .manual_preparations
                    .iter()
                    .any(|entry| entry.token == token)
            {
                return Ok(token);
            }
        }
        Err(ServiceError::Unavailable)
    }

    fn order_token(&mut self, order_id: OrderId) -> Result<Box<str>, ServiceError> {
        if let Some(existing) = self
            .orders
            .iter()
            .find(|binding| binding.order_id == order_id)
        {
            return Ok(existing.token.clone());
        }
        if self.orders.len() >= MAXIMUM_PRODUCT_ORDER_TOKENS {
            return Err(ServiceError::ResourceExhausted);
        }
        self.orders
            .try_reserve(1)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let identity = order_id.to_string();
        let token = super::opaque_product_text_token(
            "paper_action_",
            b"market-squawk/product-paper-order/v1\0",
            &[identity.as_bytes()],
            512,
        )
        .map_err(|_| ServiceError::ResourceExhausted)?;
        if self
            .orders
            .iter()
            .any(|binding| binding.token == token && binding.order_id != order_id)
        {
            return Err(ServiceError::Unavailable);
        }
        self.orders.push(ProductOrderToken {
            token: token.clone(),
            order_id,
        });
        Ok(token)
    }

    fn target_token(
        &mut self,
        target_id: &InvestmentTargetSetId,
        revision: RevisionNumber,
    ) -> Result<Box<str>, ServiceError> {
        if let Some(existing) = self
            .targets
            .iter()
            .find(|binding| binding.target_id == *target_id && binding.revision == revision)
        {
            return Ok(existing.token.clone());
        }
        if self.targets.len() >= MAXIMUM_MANUAL_PAPER_TARGET_INDEX_ENTRIES {
            return Err(ServiceError::ResourceExhausted);
        }
        self.targets
            .try_reserve(1)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let revision_text = revision.get().to_string();
        let token = super::opaque_product_text_token(
            "paper_target_",
            b"market-squawk/product-paper-target/v1\0",
            &[target_id.as_str().as_bytes(), revision_text.as_bytes()],
            512,
        )
        .map_err(|_| ServiceError::ResourceExhausted)?;
        if self.targets.iter().any(|binding| {
            binding.token == token
                && (binding.target_id != *target_id || binding.revision != revision)
        }) {
            return Err(ServiceError::Unavailable);
        }
        self.targets.push(ProductTargetToken {
            token: token.clone(),
            target_id: target_id.clone(),
            revision,
        });
        Ok(token)
    }
}

fn unique_preparation_index(
    matches: impl Iterator<Item = (usize, RequestOrigin)>,
    origin: RequestOrigin,
) -> Result<usize, ServiceError> {
    let mut resolved = None;
    for (index, retained_origin) in matches {
        if retained_origin != origin {
            return Err(ServiceError::Unauthorized);
        }
        if resolved.replace(index).is_some() {
            return Err(ServiceError::Unavailable);
        }
    }
    resolved.ok_or(ServiceError::NotFound)
}

async fn bounded_runtime_shutdown(runtime: ProductionPaperBotRuntime) -> bool {
    runtime.shutdown().await.is_complete()
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
