use super::*;

const PAPER_ORDER_COUNT: usize = 6;

#[derive(Debug)]
struct PaperMarketProbe {
    sink: Arc<market_squawk_adapter_paper::PaperMarketIngress>,
    published: AtomicUsize,
    notification: tokio::sync::Notify,
}

impl PaperMarketProbe {
    async fn wait_for(&self, expected: usize) -> TestResult {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let notified = self.notification.notified();
                if self.published.load(Ordering::Acquire) >= expected {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| {
            std::io::Error::other(format!(
                "timed out waiting for {expected} paper market updates; observed {}",
                self.published.load(Ordering::Acquire)
            ))
        })?;
        Ok(())
    }
}

impl ExecutionMarketSink for PaperMarketProbe {
    fn try_publish(&self, update: ExecutionMarketUpdate) -> Result<(), ExecutionMarketSinkError> {
        self.sink.try_publish(update)?;
        self.published.fetch_add(1, Ordering::AcqRel);
        self.notification.notify_waiters();
        Ok(())
    }

    fn retained_bytes(&self) -> Result<usize, ExecutionMarketSinkError> {
        self.sink
            .retained_bytes()?
            .checked_add(size_of::<Self>())
            .ok_or(ExecutionMarketSinkError::RetainedSize)
    }
}

#[derive(Debug)]
pub(super) struct PaperScenarioStrategy {
    account_id: AccountId,
    strategy_id: StrategyId,
    terms: market_squawk_domain::InstrumentExecutionTerms,
    order_ids: [OrderId; PAPER_ORDER_COUNT],
    client_order_ids: [ClientOrderId; PAPER_ORDER_COUNT],
    reason: OrderReasonCode,
    emitted: usize,
}

impl Strategy for PaperScenarioStrategy {
    fn on_market_event(
        &mut self,
        context: &StrategyContext<'_>,
        event: &market_squawk_domain::MarketEvent,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        if self.emitted >= self.order_ids.len()
            || !matches!(
                event,
                market_squawk_domain::MarketEvent::BookSnapshot(_)
                    | market_squawk_domain::MarketEvent::Trade(_)
            )
        {
            return Ok(BoundedOrderIntents::new());
        }
        if self.emitted == 5
            && !matches!(
                event,
                market_squawk_domain::MarketEvent::Trade(trade)
                    if trade.price() == PriceTicks::new(9_700)
            )
        {
            return Ok(BoundedOrderIntents::new());
        }
        let index = self.emitted;
        self.emitted += 1;
        let (order_type, quantity, limit_price, stop_price, time_in_force) = match index {
            0 => (
                OrderType::Market,
                2,
                None,
                None,
                TimeInForce::ImmediateOrCancel,
            ),
            1 => (
                OrderType::Stop,
                2,
                None,
                Some(PriceTicks::new(10_000)),
                TimeInForce::GoodTilCancelled,
            ),
            2 => (OrderType::Market, 150, None, None, TimeInForce::FillOrKill),
            3 => (
                OrderType::Limit,
                150,
                Some(PriceTicks::new(9_900)),
                None,
                TimeInForce::GoodTilCancelled,
            ),
            4 => (
                OrderType::StopLimit,
                2,
                Some(PriceTicks::new(9_900)),
                Some(PriceTicks::new(10_000)),
                TimeInForce::Day,
            ),
            _ => (
                OrderType::Market,
                1,
                None,
                None,
                TimeInForce::ImmediateOrCancel,
            ),
        };
        let intent = OrderIntent::try_new(OrderIntentInput {
            order_id: self.order_ids[index],
            client_order_id: self.client_order_ids[index].clone(),
            strategy_id: self.strategy_id,
            model_id: None,
            account_id: self.account_id,
            execution_terms: self.terms,
            side: OrderSide::Sell,
            order_type,
            quantity: QuantityLots::new(quantity).map_err(|_| StrategyError::Evaluation)?,
            limit_price,
            stop_price,
            time_in_force,
            signal_at: context.market().observed_at(),
            expires_at: context
                .market()
                .observed_at()
                .checked_add_nanos(30_000_000_000)
                .map_err(|_| StrategyError::Evaluation)?,
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
                .client_order_ids
                .iter()
                .map(ClientOrderId::retained_bytes)
                .sum::<usize>()
            + self.reason.as_str().len())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn committed_live_authority_reaches_realistic_paper_fill_and_reconcile() -> TestResult {
    let route_config = route_config(INSTRUMENT_ONE)?;
    let terms = route_config.definition().execution_terms();
    let usd = Currency::try_from("USD")?;
    let account_id = AccountId::from_str("51000000-0000-0000-0000-000000000001")?;
    let strategy_id = StrategyId::from_str("31000000-0000-0000-0000-000000000001")?;
    let order_ids = [
        OrderId::from_str("21000000-0000-0000-0000-000000000001")?,
        OrderId::from_str("21000000-0000-0000-0000-000000000002")?,
        OrderId::from_str("21000000-0000-0000-0000-000000000003")?,
        OrderId::from_str("21000000-0000-0000-0000-000000000004")?,
        OrderId::from_str("21000000-0000-0000-0000-000000000005")?,
        OrderId::from_str("21000000-0000-0000-0000-000000000006")?,
    ];
    let accounts = Arc::new(AccountRiskCoordinator::try_new(
        AccountCoordinatorConfig::default(),
        [AccountBootstrap {
            account_id,
            revision: NonZeroU64::MIN,
            eligible: true,
            cash: Money::new(Decimal::new(10_000, 0), usd),
            capital: Money::new(Decimal::new(10_500, 0), usd),
            peak_capital: Money::new(Decimal::new(10_500, 0), usd),
            gross_exposure: Money::new(Decimal::new(50_000, 0), usd),
            realized_loss: Money::new(Decimal::ZERO, usd),
            positions: vec![(terms.instrument_id(), 500)],
            idempotency: AccountIdempotencyBootstrap::empty(),
        }],
    )?);
    let limits = RiskLimits::try_new(RiskLimitsInput {
        currency: usd,
        eligible_instruments: BTreeSet::from([terms.instrument_id()]),
        maximum_position_lots: 1_000,
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
        allow_short: false,
        kill_switch: false,
    })?;
    let (execution_audit, mut execution_audit_reader) =
        ExecutionAuditWriter::try_new(ExecutionAuditConfig {
            maximum_records: NonZeroUsize::new(64).ok_or("zero audit records")?,
            maximum_bytes: NonZeroU32::new(4 * 1024 * 1024).ok_or("zero audit bytes")?,
        })?;
    let fees = FeeSchedule::try_new(
        0,
        10,
        Money::new(Decimal::ZERO, usd),
        Some(Money::new(Decimal::new(100, 0), usd)),
        2,
    )?;
    let calendar_observed_at = current_timestamp()?;
    let day_session_close = calendar_observed_at.checked_add_nanos(20_000_000_000)?;
    let day_expires_at = day_session_close.checked_add_nanos(-1)?;
    let session_calendar = PaperVenueSessionCalendar::try_new(
        SourceIdentifier::try_from("coinbase-session-calendar")?,
        RuleVersion::new(1)?,
        VenueId::try_from("coinbase")?,
        "UTC",
        vec![PaperVenueSession::try_new(
            SourceIdentifier::try_from("coinbase-current-session")?,
            calendar_observed_at.checked_add_nanos(-1_000_000_000)?,
            day_session_close,
        )?],
    )?;
    let paper_config = PaperExecutionConfig::try_new(PaperExecutionConfigInput {
        configuration_version: NonZeroU64::MIN,
        deterministic_seed: [7; 32],
        command_capacity: NonZeroUsize::new(16).ok_or("zero command capacity")?,
        command_maximum_bytes: NonZeroU32::new(16 * 64 * 1024).ok_or("zero command bytes")?,
        market_capacity: NonZeroUsize::new(16).ok_or("zero market capacity")?,
        market_maximum_bytes: NonZeroU32::new(512 * 1024).ok_or("zero market bytes")?,
        audit_capacity: NonZeroUsize::new(64).ok_or("zero paper audit capacity")?,
        audit_maximum_bytes: NonZeroU32::new(2 * 1024 * 1024).ok_or("zero paper audit bytes")?,
        maximum_orders: NonZeroUsize::new(8).ok_or("zero maximum orders")?,
        maximum_fills: NonZeroUsize::new(16).ok_or("zero maximum fills")?,
        maximum_idempotency_keys: NonZeroUsize::new(8).ok_or("zero idempotency")?,
        minimum_latency_nanos: 0,
        maximum_latency_nanos: 0,
        cancel_latency_nanos: 1_000_000,
        day_session_calendar: session_calendar,
        maximum_participation_basis_points: 10_000,
        impact_basis_points_per_level: 0,
        reporting_currency: usd,
        ledger_maximum_accounts: NonZeroUsize::MIN,
        ledger_maximum_balances: NonZeroUsize::MIN,
        ledger_maximum_positions: NonZeroUsize::MIN,
        allow_short: false,
        fee_schedule: fees,
    })?;
    let mut paper = PaperExecutionRuntime::try_start(
        paper_config.clone(),
        [PaperAccountBootstrap {
            account_id,
            revision: NonZeroU64::MIN,
            eligible: true,
            cash: vec![Money::new(Decimal::new(10_000, 0), usd)],
            capital: Money::new(Decimal::new(10_500, 0), usd),
            peak_capital: Money::new(Decimal::new(10_500, 0), usd),
            gross_exposure: Money::new(Decimal::new(50_000, 0), usd),
            realized_loss: Money::new(Decimal::ZERO, usd),
            positions: vec![(terms.instrument_id(), 500)],
        }],
    )?;
    let mut paper_audit = paper
        .take_audit_reader()
        .ok_or("paper audit reader was already transferred")?;
    let paper_audit_task = tokio::spawn(async move {
        let mut records = Vec::new();
        while let Some(record) = paper_audit.recv().await {
            records.push(record);
        }
        records
    });
    let paper_adapter = paper.adapter();
    let paper_market = Arc::new(PaperMarketProbe {
        sink: paper.market_ingress(),
        published: AtomicUsize::new(0),
        notification: tokio::sync::Notify::new(),
    });
    let dispatcher = ExecutionDispatcher::try_start(
        Arc::clone(&paper_adapter) as Arc<dyn ExecutionAdapter>,
        Arc::clone(&accounts),
        execution_audit.clone(),
        ExecutionDispatcherConfig {
            maximum_queued_commands: NonZeroUsize::new(8).ok_or("zero dispatch queue")?,
            maximum_queued_bytes: NonZeroU32::new(512 * 1024).ok_or("zero dispatch bytes")?,
            maximum_registry_entries: NonZeroUsize::new(8).ok_or("zero registry")?,
            shutdown_deadline: Duration::from_secs(1),
        },
    )?;
    let policy = RiskPolicyIdentity::new(
        &SourceIdentifier::try_from("risk-paper-production")?,
        RuleVersion::new(1)?,
    );
    let risk = RiskService::try_new(
        Arc::clone(&accounts),
        limits,
        execution_audit,
        RiskServiceConfig {
            policy,
            policy_valid_until: Timestamp::from_unix_nanos(i64::MAX),
            maximum_approval_lifetime: Duration::from_secs(60),
        },
    )?;
    let strategy = PaperScenarioStrategy {
        account_id,
        strategy_id,
        terms,
        order_ids,
        client_order_ids: [
            ClientOrderId::try_from("real-paper-1")?,
            ClientOrderId::try_from("real-paper-2")?,
            ClientOrderId::try_from("real-paper-3")?,
            ClientOrderId::try_from("real-paper-4")?,
            ClientOrderId::try_from("real-paper-5")?,
            ClientOrderId::try_from("real-paper-6")?,
        ],
        reason: OrderReasonCode::try_from("paper.integration")?,
        emitted: 0,
    };
    let hook = ExecutionLiveActionHook::try_new(
        Box::new(strategy),
        risk,
        dispatcher.handle(),
        Arc::clone(&paper_market) as Arc<dyn ExecutionMarketSink>,
        ActionAuthorityIssueLimit::MIN,
    )?;
    let runtime = LiveRuntime::start_with_action_hooks(
        runtime_config(16, 8 * 1024 * 1024, 4 * 1024 * 1024)?,
        vec![route_config],
        vec![RouteActionHook::try_new(
            route(INSTRUMENT_ONE)?,
            Box::new(hook),
            Vec::new(),
        )?],
    )
    .await?;
    let mut source = SourceHarness::try_new("paper-source", 1, INSTRUMENT_ONE)?;
    let ingress = runtime
        .ingress()
        .bind_generation(
            route(INSTRUMENT_ONE)?,
            source.current_lease()?,
            CancellationToken::new(),
        )
        .await?;
    let (_, snapshot) = source.two_sided_book_snapshot_batch("paper-book", 1)?;
    ingress.try_publish(snapshot)?;
    let mut accepted_digests = BTreeMap::new();
    accepted_digests.insert(
        order_ids[0],
        assert_accepted(&mut execution_audit_reader, order_ids[0]).await?,
    );
    for (index, price) in [
        (1_usize, "100.00"),
        (2, "100.00"),
        (3, "98.00"),
        (4, "98.00"),
    ] {
        let (_, trade) = source.batch_with_price(
            &format!("paper-trade-{index}"),
            u64::try_from(index + 1)?,
            price,
        )?;
        ingress.try_publish(trade)?;
        accepted_digests.insert(
            order_ids[index],
            assert_accepted(&mut execution_audit_reader, order_ids[index]).await?,
        );
    }
    let cancel = dispatcher.cancel(order_ids[4]).await?;
    assert_eq!(cancel.status(), CancelStatus::Pending);
    tokio::time::sleep(Duration::from_millis(2)).await;
    let canceled_before_next_market = dispatcher
        .reconcile()
        .await
        .map_err(|error| std::io::Error::other(format!("cancel reconciliation: {error}")))?;
    let canceled = canceled_before_next_market
        .orders()
        .iter()
        .find(|order| order.order_id() == order_ids[4])
        .ok_or("paper cancel probe missing from reconciliation")?;
    assert_eq!(canceled.status(), ReconciledOrderStatus::Canceled);
    let (_, continuation) = source.batch_with_price("paper-trade-5", 6, "98.00")?;
    ingress.try_publish(continuation)?;
    paper_market.wait_for(6).await?;
    let barrier_snapshot = paper_adapter.snapshot().await?;
    let continued = barrier_snapshot
        .orders()
        .iter()
        .find(|order| order.order_id() == order_ids[3])
        .ok_or("continued paper order missing from barrier snapshot")?;
    assert_eq!(
        continued.state(),
        market_squawk_adapter_paper::PaperOrderState::Filled
    );
    let dispatcher_reconciled = dispatcher
        .reconcile()
        .await
        .map_err(|error| std::io::Error::other(format!("initial fill reconciliation: {error}")))?;
    assert!(!dispatcher_reconciled.reconciliation_required());
    let reconciled = paper_adapter.reconcile(&order_ids).await?;
    let initial_expected = [
        ReconciledOrderStatus::Filled,
        ReconciledOrderStatus::Filled,
        ReconciledOrderStatus::Canceled,
        ReconciledOrderStatus::Filled,
        ReconciledOrderStatus::Canceled,
    ];
    for (order_id, expected_status) in order_ids.into_iter().zip(initial_expected) {
        let observed = reconciled
            .orders()
            .iter()
            .find(|order| order.order_id() == order_id)
            .ok_or("paper order missing from reconciliation")?;
        assert_eq!(observed.status(), expected_status);
    }
    assert!(!reconciled.reconciliation_required());
    let (_, subsequent) = source.batch_with_price("paper-subsequent-order", 7, "97.00")?;
    ingress.try_publish(subsequent)?;
    accepted_digests.insert(
        order_ids[5],
        assert_accepted(&mut execution_audit_reader, order_ids[5]).await?,
    );
    let (_, subsequent_fill) = source.batch_with_price("paper-subsequent-fill", 8, "97.00")?;
    ingress.try_publish(subsequent_fill)?;
    paper_market.wait_for(8).await?;
    let subsequent_barrier = paper_adapter.snapshot().await?;
    let subsequent_order = subsequent_barrier
        .orders()
        .iter()
        .find(|order| order.order_id() == order_ids[5])
        .ok_or("subsequent paper order missing after risk-state replacement")?;
    assert_eq!(
        subsequent_order.state(),
        market_squawk_adapter_paper::PaperOrderState::Filled
    );
    assert!(
        !dispatcher
            .reconcile()
            .await
            .map_err(|error| std::io::Error::other(format!(
                "subsequent fill reconciliation: {error}"
            )))?
            .reconciliation_required()
    );
    let final_active_snapshot = paper_adapter.snapshot().await?;
    assert_eq!(final_active_snapshot.fills().len(), 5);
    let continuation_fills = final_active_snapshot
        .fills()
        .iter()
        .filter(|fill| fill.order_id() == order_ids[3])
        .collect::<Vec<_>>();
    assert_eq!(continuation_fills.len(), 2);
    assert_eq!(continuation_fills[0].quantity().get(), 100);
    assert_eq!(continuation_fills[1].quantity().get(), 50);
    assert_eq!(
        continuation_fills[0].liquidity(),
        market_squawk_adapter_paper::LiquidityRole::Taker
    );
    assert_eq!(
        continuation_fills[1].liquidity(),
        market_squawk_adapter_paper::LiquidityRole::Maker
    );
    let gtc_order = final_active_snapshot
        .orders()
        .iter()
        .find(|order| order.order_id() == order_ids[3])
        .ok_or("GTC paper order missing from final snapshot")?;
    assert!(
        gtc_order
            .expires_at()
            .unix_nanos()
            .checked_sub(gtc_order.accepted_at().unix_nanos())
            .is_some_and(|lifetime| lifetime > 20_000_000_000)
    );
    let day_order = final_active_snapshot
        .orders()
        .iter()
        .find(|order| order.order_id() == order_ids[4])
        .ok_or("Day paper order missing from final snapshot")?;
    assert_eq!(day_order.expires_at(), day_expires_at);

    let checkpoint = paper_adapter.checkpoint().await?;
    assert!(checkpoint.complete());
    assert!(checkpoint.encode(1).is_err());
    let encoded_checkpoint = checkpoint.encode(1024 * 1024)?;
    let mut incompatible_input = paper_config.input().clone();
    incompatible_input.fee_schedule = FeeSchedule::try_new(
        0,
        11,
        Money::new(Decimal::ZERO, usd),
        Some(Money::new(Decimal::new(100, 0), usd)),
        2,
    )?;
    assert!(
        market_squawk_adapter_paper::PaperExecutionCheckpoint::decode(
            PaperExecutionConfig::try_new(incompatible_input)?,
            &encoded_checkpoint,
            1024 * 1024,
        )
        .is_err()
    );
    let checkpoint = market_squawk_adapter_paper::PaperExecutionCheckpoint::decode(
        paper_config.clone(),
        &encoded_checkpoint,
        1024 * 1024,
    )?;

    assert!(runtime.shutdown().await.is_complete());
    assert_eq!(
        dispatcher.shutdown().await,
        ExecutionDispatcherShutdown::Complete
    );
    let final_snapshot = paper.shutdown().await?;
    let paper_audit_records = paper_audit_task.await?;
    let fill_audits = paper_audit_records
        .iter()
        .filter(|record| record.kind() == PaperAuditKind::Filled)
        .collect::<Vec<_>>();
    assert_eq!(fill_audits.len(), final_snapshot.fills().len());
    for (audit, fill) in fill_audits.into_iter().zip(final_snapshot.fills()) {
        assert_eq!(audit.sequence(), fill.sequence());
        assert_eq!(audit.order_id(), Some(fill.order_id()));
    }
    for kind in [
        PaperAuditKind::Accepted,
        PaperAuditKind::Filled,
        PaperAuditKind::CancelRequested,
        PaperAuditKind::Canceled,
    ] {
        assert!(
            paper_audit_records
                .iter()
                .any(|record| record.kind() == kind)
        );
    }
    for (order_id, expected_digest) in &accepted_digests {
        let accepted = paper_audit_records
            .iter()
            .find(|record| {
                record.kind() == PaperAuditKind::Accepted && record.order_id() == Some(*order_id)
            })
            .ok_or("accepted paper audit record missing")?;
        assert_eq!(accepted.input_digest(), *expected_digest);
    }

    let mut recovered =
        PaperExecutionRuntime::try_start_from_checkpoint(paper_config.clone(), checkpoint)?;
    let recovered_adapter = recovered.adapter();
    let mut recovered_audit = recovered
        .take_audit_reader()
        .ok_or("recovered audit reader was already transferred")?;
    let recovery_loaded = tokio::time::timeout(Duration::from_secs(1), recovered_audit.recv())
        .await?
        .ok_or("recovery audit stream closed")?;
    assert_eq!(recovery_loaded.kind(), PaperAuditKind::RecoveryLoaded);
    assert_eq!(
        recovery_loaded.sequence(),
        final_snapshot
            .sequence()
            .checked_add(1)
            .ok_or("sequence overflow")?
    );
    let recovered_snapshot = recovered_adapter.snapshot().await?;
    assert_eq!(recovered_snapshot.accounts(), final_snapshot.accounts());
    assert_eq!(recovered_snapshot.orders(), final_snapshot.orders());
    assert_eq!(recovered_snapshot.fills(), final_snapshot.fills());
    assert_eq!(recovered_snapshot.cash(), final_snapshot.cash());
    assert_eq!(recovered_snapshot.positions(), final_snapshot.positions());
    let recovered_state = recovered_adapter.reconcile(&order_ids).await?;
    let final_expected = [
        ReconciledOrderStatus::Filled,
        ReconciledOrderStatus::Filled,
        ReconciledOrderStatus::Canceled,
        ReconciledOrderStatus::Filled,
        ReconciledOrderStatus::Canceled,
        ReconciledOrderStatus::Filled,
    ];
    for (observed, expected_status) in recovered_state.orders().iter().zip(final_expected) {
        assert_eq!(observed.status(), expected_status);
    }
    recovered_audit.report_persistence_failure();
    let failed_snapshot = recovered_adapter.snapshot().await?;
    assert!(failed_snapshot.complete());
    assert!(failed_snapshot.reconciliation_required());
    let reconciliation_checkpoint = recovered_adapter.checkpoint().await?;
    assert!(recovered.shutdown().await?.complete());

    let mut reconciled_recovery =
        PaperExecutionRuntime::try_start_from_checkpoint(paper_config, reconciliation_checkpoint)?;
    let mut reconciled_recovery_audit = reconciled_recovery
        .take_audit_reader()
        .ok_or("reconciliation recovery audit reader was already transferred")?;
    let recovery_loaded =
        tokio::time::timeout(Duration::from_secs(1), reconciled_recovery_audit.recv())
            .await?
            .ok_or("reconciliation recovery audit stream closed")?;
    let reconciliation_required =
        tokio::time::timeout(Duration::from_secs(1), reconciled_recovery_audit.recv())
            .await?
            .ok_or("reconciliation-required audit stream closed")?;
    assert_eq!(recovery_loaded.kind(), PaperAuditKind::RecoveryLoaded);
    assert_eq!(
        reconciliation_required.kind(),
        PaperAuditKind::ReconciliationRequired
    );
    assert_eq!(
        reconciliation_required.sequence(),
        recovery_loaded
            .sequence()
            .checked_add(1)
            .ok_or("sequence overflow")?
    );
    assert!(
        reconciled_recovery
            .shutdown()
            .await?
            .reconciliation_required()
    );
    Ok(())
}

async fn assert_accepted(
    reader: &mut ExecutionAuditReader,
    order_id: OrderId,
) -> TestResult<[u8; 32]> {
    let accepted = wait_for_audit(reader, ExecutionAuditKind::DispatchAccepted)
        .await
        .map_err(|error| {
            std::io::Error::other(format!("order {order_id} was not accepted: {error}"))
        })?;
    if accepted.order_id() != order_id {
        return Err(std::io::Error::other(format!(
            "accepted order mismatch: expected {order_id}, observed {}",
            accepted.order_id()
        ))
        .into());
    }
    Ok(accepted.intent_digest().as_bytes())
}
