#![allow(
    clippy::panic,
    reason = "invalid fixed fixtures and failed assertions must terminate this test binary"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use market_squawk_adapter_paper::{
    FeeSchedule, PaperAccountBootstrap, PaperAuditKind, PaperExecutionConfig,
    PaperExecutionConfigInput, PaperExecutionRuntime, PaperVenueSession, PaperVenueSessionCalendar,
};
use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, Currency, DataQuality, Money, OrderId, OrderReasonCode,
    OrderSide, OrderType, PriceTicks, QuantityLots, RuleVersion, SourceIdentifier, StrategyId,
    TimeInForce, Timestamp, VenueId,
};
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountIdempotencyBootstrap,
    AccountRiskCoordinator, BoundedOrderIntents, CancelReceipt, CancelStatus, DispatchOrder,
    ExecutionAdapter, ExecutionAdapterError, ExecutionAdapterFuture, ExecutionAuditConfig,
    ExecutionAuditEvent, ExecutionAuditKind, ExecutionAuditReader, ExecutionAuditReason,
    ExecutionAuditWriter, ExecutionDispatcher, ExecutionDispatcherConfig,
    ExecutionDispatcherShutdown, ExecutionLiveActionHook, ExecutionMarketSink,
    ExecutionMarketSinkError, ExecutionMarketUpdate, ExecutionReceipt, ExecutionState, OrderIntent,
    OrderIntentInput, ReconciledOrder, ReconciledOrderStatus, RiskLimits, RiskLimitsInput,
    RiskPolicyIdentity, RiskService, RiskServiceConfig, Strategy, StrategyContext, StrategyError,
};
use market_squawk_live::{ActionAuthorityIssueLimit, LiveRuntime, RouteActionHook};
pub(crate) use market_squawk_live::{
    DepthLimit, LiveRouteConfig, LiveRouteConfigInput, LiveRuntimeConfig, LiveRuntimeConfigInput,
    ShardKey, ShardRoutingVersion, SnapshotLimits,
};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

#[allow(
    dead_code,
    reason = "the deterministic shared live-source fixture exposes helpers for several test binaries"
)]
#[path = "../../../crates/market-squawk-live/tests/support/current_source.rs"]
mod current_source;

use current_source::{
    INSTRUMENT_ONE, SourceHarness, TestResult, route, route_config, runtime_config,
};

#[derive(Debug)]
struct SnapshotStrategy {
    account_ids: [AccountId; 4],
    strategy_id: StrategyId,
    terms: market_squawk_domain::InstrumentExecutionTerms,
    order_ids: [OrderId; 4],
    client_ids: [ClientOrderId; 4],
    reason: OrderReasonCode,
    emitted: usize,
}

#[path = "risk_dispatch_pipeline/paper.rs"]
mod paper;

impl Strategy for SnapshotStrategy {
    fn on_market_event(
        &mut self,
        context: &StrategyContext<'_>,
        event: &market_squawk_domain::MarketEvent,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        if !matches!(
            event,
            market_squawk_domain::MarketEvent::BookSnapshot(_)
                | market_squawk_domain::MarketEvent::Trade(_)
        ) || self.emitted >= self.order_ids.len()
        {
            return Ok(BoundedOrderIntents::new());
        }
        let index = self.emitted;
        self.emitted += 1;
        let expires_at = context
            .market()
            .observed_at()
            .checked_add_nanos(30_000_000_000)
            .map_err(|_| StrategyError::Evaluation)?;
        let intent = OrderIntent::try_new(OrderIntentInput {
            order_id: self.order_ids[index],
            client_order_id: self.client_ids[index].clone(),
            strategy_id: self.strategy_id,
            model_id: None,
            account_id: self.account_ids[index],
            execution_terms: self.terms,
            side: OrderSide::Sell,
            order_type: OrderType::Market,
            quantity: QuantityLots::new(2).map_err(|_| StrategyError::Evaluation)?,
            limit_price: None,
            stop_price: None,
            time_in_force: TimeInForce::ImmediateOrCancel,
            signal_at: context.market().observed_at(),
            expires_at,
            reason_codes: vec![self.reason.clone()],
            maximum_slippage: BasisPoints::new(100),
            required_quality: DataQuality::DirectVerified,
        })
        .map_err(|_| StrategyError::Evaluation)?;
        let mut intents = BoundedOrderIntents::new();
        intents.try_push(intent)?;
        Ok(intents)
    }

    fn retained_bytes(&self) -> Result<usize, StrategyError> {
        Ok(size_of::<Self>()
            + self
                .client_ids
                .iter()
                .map(ClientOrderId::retained_bytes)
                .sum::<usize>()
            + self.reason.as_str().len())
    }
}

#[derive(Debug)]
struct CountingMarketSink {
    updates: AtomicUsize,
    valid: AtomicBool,
}

impl ExecutionMarketSink for CountingMarketSink {
    fn try_publish(&self, update: ExecutionMarketUpdate) -> Result<(), ExecutionMarketSinkError> {
        let market = update.market();
        if market.quality() != DataQuality::DirectVerified
            || market.best_bid().is_none()
            || update.assessment_digest() == [0; 32]
        {
            self.valid.store(false, Ordering::Release);
        }
        self.updates.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn retained_bytes(&self) -> Result<usize, ExecutionMarketSinkError> {
        Ok(size_of::<Self>())
    }
}

#[derive(Debug)]
struct ScriptedAdapter {
    calls: AtomicUsize,
    evidence_valid: AtomicBool,
    accepted: Mutex<Option<OrderId>>,
    usd: Currency,
}

impl ExecutionAdapter for ScriptedAdapter {
    fn submit(
        &self,
        order: DispatchOrder,
    ) -> ExecutionAdapterFuture<'_, Result<ExecutionReceipt, ExecutionAdapterError>> {
        let invocation = self.calls.fetch_add(1, Ordering::AcqRel);
        if order.market().quality() != DataQuality::DirectVerified
            || order.market().best_bid().is_none()
            || order
                .assessment_id()
                .as_source_identifier()
                .as_str()
                .is_empty()
            || order.evidence_binding_digest() == [0; 32]
            || order.evidence_binding().instrument_id() != order.execution_terms().instrument_id()
            || order.reason_codes().len() != 1
            || order.valid_until() < order.submitted_at()
        {
            self.evidence_valid.store(false, Ordering::Release);
        }
        if invocation >= 3 {
            return Box::pin(std::future::pending());
        }
        let result = if invocation == 0 {
            Err(ExecutionAdapterError::KnownFailure)
        } else {
            match self.accepted.try_lock() {
                Ok(mut accepted) => {
                    *accepted = Some(order.order_id());
                    Ok(ExecutionReceipt::new(
                        order.order_id(),
                        order.submitted_at(),
                    ))
                }
                Err(_) => Err(ExecutionAdapterError::KnownFailure),
            }
        };
        Box::pin(async move { result })
    }

    fn cancel(
        &self,
        order_id: &OrderId,
    ) -> ExecutionAdapterFuture<'_, Result<CancelReceipt, ExecutionAdapterError>> {
        let result = current_timestamp().and_then(|observed_at| {
            CancelReceipt::try_new(
                *order_id,
                CancelStatus::Canceled,
                observed_at,
                QuantityLots::new(1).unwrap_or_else(|error| panic!("valid partial fill: {error}")),
                Some(PriceTicks::new(10_000)),
                Money::new(Decimal::new(1, 2), self.usd),
            )
            .map_err(|_| ExecutionAdapterError::KnownFailure)
        });
        Box::pin(async move { result })
    }

    fn reconcile<'adapter>(
        &'adapter self,
        order_ids: &'adapter [OrderId],
    ) -> ExecutionAdapterFuture<'adapter, Result<ExecutionState, ExecutionAdapterError>> {
        let accepted = self.accepted.try_lock().ok().and_then(|accepted| *accepted);
        let result = match accepted {
            Some(order_id) if order_ids == [order_id] => {
                current_timestamp().and_then(|observed_at| {
                    ReconciledOrder::try_new(
                        order_id,
                        ReconciledOrderStatus::Filled,
                        QuantityLots::new(2)
                            .unwrap_or_else(|error| panic!("valid cumulative fill: {error}")),
                        Some(PriceTicks::new(10_000)),
                        Money::new(Decimal::new(2, 2), self.usd),
                    )
                    .and_then(|order| ExecutionState::try_new(observed_at, vec![order], false))
                    .map_err(|_| ExecutionAdapterError::KnownFailure)
                })
            }
            _ => Err(ExecutionAdapterError::KnownFailure),
        };
        Box::pin(async move { result })
    }
}

fn current_timestamp() -> Result<Timestamp, ExecutionAdapterError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ExecutionAdapterError::KnownFailure)?;
    let nanos = i128::from(elapsed.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i128::from(elapsed.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(ExecutionAdapterError::KnownFailure)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

#[tokio::test(flavor = "current_thread")]
async fn committed_market_risk_and_dispatch_are_one_use_bounded_and_fill_safe() -> TestResult {
    let route_config = route_config(INSTRUMENT_ONE)?;
    let terms = route_config.definition().execution_terms();
    let usd = Currency::try_from("USD")?;
    let account_id = AccountId::from_str("50000000-0000-0000-0000-000000000001")?;
    let submitted_shutdown_account_id =
        AccountId::from_str("50000000-0000-0000-0000-000000000002")?;
    let accepted_shutdown_account_id = AccountId::from_str("50000000-0000-0000-0000-000000000003")?;
    let strategy_id = StrategyId::from_str("30000000-0000-0000-0000-000000000001")?;
    let accounts = Arc::new(AccountRiskCoordinator::try_new(
        AccountCoordinatorConfig::default(),
        [
            account_id,
            submitted_shutdown_account_id,
            accepted_shutdown_account_id,
        ]
        .map(|account_id| AccountBootstrap {
            account_id,
            revision: NonZeroU64::MIN,
            eligible: true,
            cash: Money::new(Decimal::new(10_000, 0), usd),
            capital: Money::new(Decimal::new(10_000, 0), usd),
            peak_capital: Money::new(Decimal::new(10_000, 0), usd),
            gross_exposure: Money::new(Decimal::ZERO, usd),
            realized_loss: Money::new(Decimal::ZERO, usd),
            positions: vec![(terms.instrument_id(), 0)],
            idempotency: AccountIdempotencyBootstrap::empty(),
        }),
    )?);
    let limits = RiskLimits::try_new(RiskLimitsInput {
        currency: usd,
        eligible_instruments: BTreeSet::from([terms.instrument_id()]),
        maximum_position_lots: 100,
        maximum_order_notional: Money::new(Decimal::new(1_000_000, 0), usd),
        maximum_gross_exposure: Money::new(Decimal::new(1_000_000, 0), usd),
        maximum_leverage: BasisPoints::new(100_000),
        minimum_capital: Money::new(Decimal::ONE, usd),
        maximum_loss: Money::new(Decimal::new(1_000_000, 0), usd),
        maximum_drawdown: Money::new(Decimal::new(1_000_000, 0), usd),
        maximum_fee: BasisPoints::new(100),
        maximum_price_deviation: BasisPoints::new(1_000),
        maximum_slippage: BasisPoints::new(1_000),
        maximum_orders_per_window: NonZeroU32::new(8).ok_or("zero order rate")?,
        order_rate_window_nanos: 60_000_000_000,
        reservation_ttl_nanos: 5_000_000_000,
        allow_short: true,
        kill_switch: false,
    })?;
    let audit_bytes = NonZeroU32::new(2 * 1024 * 1024).ok_or("zero audit bytes")?;
    let (audit, mut audit_reader) = ExecutionAuditWriter::try_new(ExecutionAuditConfig {
        maximum_records: NonZeroUsize::new(32).ok_or("zero audit records")?,
        maximum_bytes: audit_bytes,
    })?;
    let adapter = Arc::new(ScriptedAdapter {
        calls: AtomicUsize::new(0),
        evidence_valid: AtomicBool::new(true),
        accepted: Mutex::new(None),
        usd,
    });
    let dispatcher = ExecutionDispatcher::try_start(
        Arc::clone(&adapter) as Arc<dyn ExecutionAdapter>,
        Arc::clone(&accounts),
        audit.clone(),
        ExecutionDispatcherConfig {
            maximum_queued_commands: NonZeroUsize::MIN,
            maximum_queued_bytes: NonZeroU32::new(64 * 1024).ok_or("zero dispatch bytes")?,
            maximum_registry_entries: NonZeroUsize::new(4).ok_or("zero registry entries")?,
            shutdown_deadline: Duration::from_millis(10),
        },
    )?;
    let market_sink = Arc::new(CountingMarketSink {
        updates: AtomicUsize::new(0),
        valid: AtomicBool::new(true),
    });
    let policy = RiskPolicyIdentity::new(
        &SourceIdentifier::try_from("risk-default")?,
        RuleVersion::new(1)?,
    );
    let risk = RiskService::try_new(
        Arc::clone(&accounts),
        limits.clone(),
        audit,
        RiskServiceConfig {
            policy,
            policy_valid_until: Timestamp::from_unix_nanos(i64::MAX),
            maximum_approval_lifetime: Duration::from_secs(60),
        },
    )?;
    let strategy = SnapshotStrategy {
        account_ids: [
            account_id,
            account_id,
            accepted_shutdown_account_id,
            submitted_shutdown_account_id,
        ],
        strategy_id,
        terms,
        order_ids: [
            OrderId::from_str("20000000-0000-0000-0000-000000000001")?,
            OrderId::from_str("20000000-0000-0000-0000-000000000002")?,
            OrderId::from_str("20000000-0000-0000-0000-000000000003")?,
            OrderId::from_str("20000000-0000-0000-0000-000000000004")?,
        ],
        client_ids: [
            ClientOrderId::try_from("dispatch-1")?,
            ClientOrderId::try_from("dispatch-2")?,
            ClientOrderId::try_from("dispatch-3")?,
            ClientOrderId::try_from("dispatch-4")?,
        ],
        reason: OrderReasonCode::try_from("dispatch.integration")?,
        emitted: 0,
    };
    let hook = ExecutionLiveActionHook::try_new(
        Box::new(strategy),
        risk,
        dispatcher.handle(),
        Arc::clone(&market_sink) as Arc<dyn ExecutionMarketSink>,
        ActionAuthorityIssueLimit::MIN,
    )?;
    let route_hook = RouteActionHook::try_new(route(INSTRUMENT_ONE)?, Box::new(hook), Vec::new())?;
    let runtime = LiveRuntime::start_with_action_hooks(
        runtime_config(8, 8 * 1024 * 1024, 4 * 1024 * 1024)?,
        vec![route_config],
        vec![route_hook],
    )
    .await?;
    let mut source = SourceHarness::try_new("dispatch-source", 1, INSTRUMENT_ONE)?;
    let ingress = runtime
        .ingress()
        .bind_generation(
            route(INSTRUMENT_ONE)?,
            source.current_lease()?,
            CancellationToken::new(),
        )
        .await?;

    let (_, first) = source.two_sided_book_snapshot_batch("book-1", 1)?;
    ingress.try_publish(first)?;
    let first_audit =
        wait_for_audit(&mut audit_reader, ExecutionAuditKind::DispatchKnownFailure).await?;
    assert_eq!(first_audit.strategy_id(), strategy_id);

    let (_, second) = source.batch("trade-2", 2)?;
    ingress.try_publish(second)?;
    let accepted = wait_for_audit(&mut audit_reader, ExecutionAuditKind::DispatchAccepted).await?;
    assert_eq!(accepted.strategy_id(), strategy_id);
    assert!(accepted.assessment_digest().is_some());
    assert!(accepted.evidence_binding_digest().is_some());
    assert_eq!(accepted.risk_policy(), policy);

    let accepted_order = adapter
        .accepted
        .try_lock()
        .ok()
        .and_then(|order| *order)
        .ok_or("adapter did not retain accepted order")?;
    let canceled = dispatcher.cancel(accepted_order).await?;
    assert_eq!(canceled.cumulative_filled().get(), 1);
    let cancel_audit =
        wait_for_audit(&mut audit_reader, ExecutionAuditKind::DispatchUncertain).await?;
    assert!(
        cancel_audit
            .reasons()
            .any(|reason| reason == ExecutionAuditReason::ReconciliationRequired)
    );
    let state = dispatcher.reconcile().await?;
    assert_eq!(state.orders().len(), 1);
    assert_eq!(state.orders()[0].cumulative_filled().get(), 2);
    assert_eq!(
        state.orders()[0].cumulative_fees().amount(),
        Decimal::new(2, 2)
    );
    let reconciliation = wait_for_audit(
        &mut audit_reader,
        ExecutionAuditKind::ReconciliationObserved,
    )
    .await?;
    assert!(
        reconciliation
            .reasons()
            .any(|reason| reason == ExecutionAuditReason::ReconciliationRequired)
    );

    let (_, third) = source.batch("trade-3", 3)?;
    ingress.try_publish(third)?;
    let accepted_for_shutdown =
        wait_for_audit(&mut audit_reader, ExecutionAuditKind::DispatchAccepted).await?;
    assert_eq!(
        accepted_for_shutdown.account_id(),
        accepted_shutdown_account_id
    );

    let (_, fourth) = source.batch("trade-4", 4)?;
    ingress.try_publish(fourth)?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while adapter.calls.load(Ordering::Acquire) != 4 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(runtime.shutdown().await.is_complete());
    assert_eq!(
        dispatcher.shutdown().await,
        ExecutionDispatcherShutdown::DeadlineAborted
    );
    for (suffix, account_id) in [
        (5_u8, submitted_shutdown_account_id),
        (6_u8, accepted_shutdown_account_id),
    ] {
        let shutdown_probe = OrderIntent::try_new(OrderIntentInput {
            order_id: OrderId::from_str(&format!("20000000-0000-0000-0000-{suffix:012}"))?,
            client_order_id: ClientOrderId::try_from(format!("dispatch-probe-{suffix}"))?,
            strategy_id,
            model_id: None,
            account_id,
            execution_terms: terms,
            side: OrderSide::Sell,
            order_type: OrderType::Market,
            quantity: QuantityLots::new(1)?,
            limit_price: None,
            stop_price: None,
            time_in_force: TimeInForce::ImmediateOrCancel,
            signal_at: Timestamp::from_unix_nanos(0),
            expires_at: Timestamp::from_unix_nanos(i64::MAX),
            reason_codes: vec![OrderReasonCode::try_from("dispatch.shutdown.probe")?],
            maximum_slippage: BasisPoints::new(100),
            required_quality: DataQuality::DirectVerified,
        })?;
        let shutdown_rejection = accounts
            .assess(&shutdown_probe, PriceTicks::new(10_000), &limits)
            .err()
            .ok_or("shutdown order did not block its account for reconciliation")?;
        assert!(
            shutdown_rejection
                .reasons()
                .contains(&market_squawk_execution::AccountRiskViolation::ReconciliationRequired)
        );
    }
    assert_eq!(adapter.calls.load(Ordering::Acquire), 4);
    assert!(adapter.evidence_valid.load(Ordering::Acquire));
    assert_eq!(market_sink.updates.load(Ordering::Acquire), 4);
    assert!(market_sink.valid.load(Ordering::Acquire));
    Ok(())
}

async fn wait_for_audit(
    reader: &mut ExecutionAuditReader,
    kind: ExecutionAuditKind,
) -> TestResult<ExecutionAuditEvent> {
    let mut seen = Vec::new();
    let event = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            while let Some(event) = reader.try_next()? {
                if event.kind() == kind {
                    return Ok::<_, market_squawk_execution::ExecutionAuditError>(event);
                }
                seen.push(format!(
                    "{:?}:{:?}",
                    event.kind(),
                    event.reasons().collect::<Vec<_>>()
                ));
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    match event {
        Ok(event) => Ok(event?),
        Err(_) => Err(std::io::Error::other(format!(
            "timed out waiting for {kind:?}; observed {seen:?}"
        ))
        .into()),
    }
}
