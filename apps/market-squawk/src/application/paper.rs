//! Lifecycle-owned paper bot and execution application services.

mod market;
mod source_runtime;

use std::{
    fmt,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use market_squawk_adapter_paper::{PaperExecutionSnapshot, PaperFillSnapshot, PaperOrderSnapshot};
use market_squawk_domain::OrderId;
use market_squawk_execution::{
    CancelReceipt, CancelStatus, ExecutionAdapterError, ExecutionDispatchError, ExecutionState,
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

use super::live_fair_value::{LiveFairValueExportDrains, LiveFairValueObservationBuffer};
use super::source::SourceRuntimeView;
use super::{ApplicationDomainService, effective_service_limits};
use crate::{
    AppConfig, ProductionSourceProvider,
    paper_bot::{ProductionPaperBotRuntime, local_paper_bot_with_provider_rate},
};

const BOT_GET_STATUS: &str = "Bot.GetStatus";
const BOT_START: &str = "Bot.Start";
const BOT_STOP: &str = "Bot.Stop";
const RISK_TRIGGER_KILL_SWITCH: &str = "Risk.TriggerKillSwitch";
const EXECUTION_GET_ORDERS: &str = "Execution.GetOrders";
const EXECUTION_GET_FILLS: &str = "Execution.GetFills";
const EXECUTION_CANCEL: &str = "Execution.Cancel";
const EXECUTION_RECONCILE: &str = "Execution.Reconcile";

/// Shared paper lifecycle exposed as distinct Bot and Execution domain services.
pub struct PaperApplicationServices {
    controller: Arc<PaperController>,
}

impl PaperApplicationServices {
    /// Creates a stopped paper controller from validated effective configuration.
    #[must_use]
    pub fn new(
        config: AppConfig,
        live_fair_value: Arc<LiveFairValueObservationBuffer>,
        provider_rate: ProviderRateAuthority,
    ) -> Self {
        Self {
            controller: Arc::new(PaperController::new(config, live_fair_value, provider_rate)),
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
            BOT_GET_STATUS => self.controller.status(&context).await?,
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
    accepting: AtomicBool,
    lifecycle: CancellationToken,
    state: Mutex<PaperState>,
    changed: watch::Sender<u64>,
}

impl PaperController {
    fn new(
        config: AppConfig,
        live_fair_value: Arc<LiveFairValueObservationBuffer>,
        provider_rate: ProviderRateAuthority,
    ) -> Self {
        let (changed, _initial_receiver) = watch::channel(0);
        Self {
            config,
            live_fair_value,
            provider_rate,
            accepting: AtomicBool::new(true),
            lifecycle: CancellationToken::new(),
            state: Mutex::new(PaperState::Stopped {
                last_complete: None,
            }),
            changed,
        }
    }

    async fn status(&self, context: &RequestContext) -> Result<Value, ServiceError> {
        ensure_live(context)?;
        let state = bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
        match &*state {
            PaperState::Stopped { last_complete } => Ok(json!({
                "state": "stopped",
                "lastShutdownComplete": last_complete,
            })),
            PaperState::Starting => Ok(json!({"state": "starting"})),
            PaperState::Stopping => Ok(json!({"state": "stopping"})),
            PaperState::Running {
                runtime, exports, ..
            } => {
                if !exports.is_healthy() {
                    return Err(ServiceError::Unavailable);
                }
                let financial_reconciliation_current = runtime.financial_reconciliation_current();
                let snapshot = runtime
                    .paper_snapshot(context.deadline(), context.cancellation())
                    .await
                    .map_err(map_control_error)?;
                Ok(json!({
                    "state": "running",
                    "sequence": snapshot.sequence(),
                    "complete": snapshot.complete(),
                    "reconciliationRequired": snapshot.reconciliation_required(),
                    "financialReconciliationCurrent": financial_reconciliation_current,
                    "orders": snapshot.orders().len(),
                    "fills": snapshot.fills().len(),
                    "positions": snapshot.positions().len(),
                }))
            }
        }
    }

    async fn start(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::Unavailable);
        }
        let provider = match required_string(request, "provider")? {
            "coinbase" => ProductionSourceProvider::Coinbase,
            "kraken" => ProductionSourceProvider::Kraken,
            _ => return Err(ServiceError::InvalidRequest),
        };
        let initial_cash = required_string(request, "initialCash")?
            .parse::<Decimal>()
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let fee_basis_points = request
            .arguments()
            .get("feeBasisPoints")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(ServiceError::InvalidRequest)?;
        {
            let mut state =
                bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
            if !matches!(*state, PaperState::Stopped { .. }) {
                return Err(ServiceError::InvalidRequest);
            }
            *state = PaperState::Starting;
        }
        self.signal_change();

        let composition = match local_paper_bot_with_provider_rate(
            self.config.clone(),
            provider,
            initial_cash,
            fee_basis_points,
            self.provider_rate.clone(),
        ) {
            Ok(composition) => composition,
            Err(_error) => {
                self.set_stopped(None).await;
                return Err(ServiceError::Unavailable);
            }
        };
        let run_cancellation = self.lifecycle.child_token();
        let (qualified_market_exports, export_drains) = match LiveFairValueExportDrains::try_start(
            composition.live_routes(),
            composition.maximum_message_bytes(),
            Arc::clone(&self.live_fair_value),
            run_cancellation.clone(),
            context.deadline(),
        )
        .await
        {
            Ok(exports) => exports,
            Err(_error) => {
                run_cancellation.cancel();
                self.set_stopped(None).await;
                return Err(ServiceError::Unavailable);
            }
        };
        let result = tokio::select! {
            biased;
            () = context.cancellation().cancelled() => {
                run_cancellation.cancel();
                Err(ServiceError::Cancelled)
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(context.deadline())) => {
                run_cancellation.cancel();
                Err(ServiceError::DeadlineExceeded)
            }
            result = composition.start_with_qualified_market_exports(
                qualified_market_exports,
                run_cancellation.clone(),
            ) => {
                result.map_err(|_error| ServiceError::Unavailable)
            }
        };
        match result {
            Ok(runtime) if self.accepting.load(Ordering::Acquire) => {
                let mut state = self.state.lock().await;
                *state = PaperState::Running {
                    provider,
                    runtime: Box::new(runtime),
                    exports: export_drains,
                };
                self.signal_change();
                Ok(json!({
                    "state": "running",
                    "provider": provider_name(provider),
                }))
            }
            Ok(runtime) => {
                let mut state = self.state.lock().await;
                *state = PaperState::Stopping;
                drop(state);
                let shutdown = bounded_runtime_shutdown(
                    runtime,
                    export_drains,
                    context.deadline(),
                    context.cancellation(),
                )
                .await;
                self.set_stopped(Some(shutdown.as_ref().copied().unwrap_or(false)))
                    .await;
                let _complete = shutdown?;
                Err(ServiceError::Unavailable)
            }
            Err(error) => {
                run_cancellation.cancel();
                export_drains.begin_shutdown();
                let cleanup = CancellationToken::new();
                let cleanup_deadline = Instant::now()
                    .checked_add(self.config.source_shutdown())
                    .ok_or(ServiceError::Unavailable)?;
                let drains_complete = export_drains
                    .finish_before(cleanup_deadline, &cleanup)
                    .await
                    .is_ok();
                self.set_stopped(Some(drains_complete)).await;
                if drains_complete {
                    Err(error)
                } else {
                    Err(ServiceError::Unavailable)
                }
            }
        }
    }

    async fn stop(&self, reason: &str, context: &RequestContext) -> Result<Value, ServiceError> {
        let complete = self
            .stop_before(context.deadline(), context.cancellation())
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
        let snapshot = self.snapshot(context).await?;
        let available = snapshot.orders().len();
        let returned = available.min(maximum_items);
        if returned == 0 {
            return Ok((Value::Null, 0, 0));
        }
        let mut values = Vec::new();
        values
            .try_reserve_exact(returned)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        values.extend(snapshot.orders()[..returned].iter().map(order_value));
        Ok((Value::Array(values), returned, available))
    }

    async fn fills(
        &self,
        context: &RequestContext,
        maximum_items: usize,
    ) -> Result<(Value, usize, usize), ServiceError> {
        let snapshot = self.snapshot(context).await?;
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

    async fn snapshot(
        &self,
        context: &RequestContext,
    ) -> Result<PaperExecutionSnapshot, ServiceError> {
        ensure_live(context)?;
        let state = bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
        let PaperState::Running {
            runtime, exports, ..
        } = &*state
        else {
            return Err(ServiceError::Unavailable);
        };
        if !exports.is_healthy() {
            return Err(ServiceError::Unavailable);
        }
        runtime
            .paper_snapshot(context.deadline(), context.cancellation())
            .await
            .map_err(map_control_error)
    }

    async fn cancel(
        &self,
        order_id: OrderId,
        context: &RequestContext,
    ) -> Result<CancelReceipt, ServiceError> {
        ensure_live(context)?;
        let state = bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
        let PaperState::Running {
            runtime, exports, ..
        } = &*state
        else {
            return Err(ServiceError::Unavailable);
        };
        if !exports.is_healthy() {
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
            runtime, exports, ..
        } = &*state
        else {
            return Err(ServiceError::Unavailable);
        };
        if !exports.is_healthy() {
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

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        let cleanup = CancellationToken::new();
        match self.stop_before(deadline, &cleanup).await? {
            true => Ok(()),
            false => Err(ServiceError::Unavailable),
        }
    }

    async fn stop_before(
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
                PaperState::Running {
                    runtime, exports, ..
                } => {
                    drop(state);
                    let complete =
                        bounded_runtime_shutdown(*runtime, exports, deadline, cancellation).await;
                    self.set_stopped(Some(complete.as_ref().copied().unwrap_or(false)))
                        .await;
                    return complete;
                }
                PaperState::Starting => {
                    *state = PaperState::Starting;
                    drop(state);
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
    Starting,
    Running {
        provider: ProductionSourceProvider,
        runtime: Box<ProductionPaperBotRuntime>,
        exports: LiveFairValueExportDrains,
    },
    Stopping,
}

async fn bounded_runtime_shutdown(
    runtime: ProductionPaperBotRuntime,
    exports: LiveFairValueExportDrains,
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
            let exports_complete = exports
                .finish_before(deadline, cancellation)
                .await
                .is_ok();
            Ok(runtime_complete && exports_complete)
        },
    }
}

async fn bounded_lock<'state>(
    state: &'state Mutex<PaperState>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<tokio::sync::MutexGuard<'state, PaperState>, ServiceError> {
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

const fn provider_name(provider: ProductionSourceProvider) -> &'static str {
    match provider {
        ProductionSourceProvider::Coinbase => "coinbase",
        ProductionSourceProvider::Kraken => "kraken",
    }
}

fn order_value(order: &PaperOrderSnapshot) -> Value {
    json!({
        "orderId": order.order_id(),
        "accountId": order.account_id(),
        "state": order.state(),
        "requestedLots": order.requested(),
        "filledLots": order.cumulative_filled(),
        "averageFillPriceTicks": order.average_fill_price(),
        "maximumFillPriceTicks": order.maximum_fill_price(),
        "maximumExecutionPriceTicks": order.maximum_execution_price(),
        "cumulativeFees": order.cumulative_fees(),
        "acceptedAt": order.accepted_at(),
        "eligibleAt": order.eligible_at(),
        "expiresAt": order.expires_at(),
        "revision": order.revision(),
    })
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
