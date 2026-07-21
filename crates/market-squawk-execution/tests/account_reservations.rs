#![allow(
    clippy::panic,
    reason = "invalid fixed fixtures, worker failure, and failed assertions must terminate tests"
)]

use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, Currency, DataQuality, Denomination,
    InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId, LotSize, Money, OrderId,
    OrderReasonCode, OrderSide, OrderType, PriceTicks, QuantityLots, StrategyId, TickSize,
    TimeInForce, Timestamp,
};
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountIdempotencyBootstrap,
    AccountRiskCoordinator, AccountRiskViolation, OrderIntent, OrderIntentInput, RiskLimits,
    RiskLimitsInput,
};
use rust_decimal::Decimal;

#[test]
fn concurrent_reservations_cannot_jointly_exceed_account_limits() {
    let fixture = Fixture::new();
    let coordinator = Arc::new(
        AccountRiskCoordinator::try_new(
            AccountCoordinatorConfig {
                partition_count: NonZeroUsize::new(2)
                    .unwrap_or_else(|| panic!("fixture partition count is nonzero")),
                max_accounts_per_partition: NonZeroUsize::new(4)
                    .unwrap_or_else(|| panic!("fixture account capacity is nonzero")),
                max_reservations_per_account: NonZeroUsize::new(4)
                    .unwrap_or_else(|| panic!("fixture reservation capacity is nonzero")),
                max_positions_per_account: NonZeroUsize::new(8)
                    .unwrap_or_else(|| panic!("fixture position capacity is nonzero")),
                max_idempotency_keys_per_account: NonZeroUsize::new(8)
                    .unwrap_or_else(|| panic!("fixture idempotency capacity is nonzero")),
                maximum_intent_lifetime_nanos: NonZeroU64::new(i64::MAX as u64)
                    .unwrap_or_else(|| panic!("fixture intent lifetime is nonzero")),
                max_rate_events_per_account: NonZeroUsize::new(8)
                    .unwrap_or_else(|| panic!("fixture rate capacity is nonzero")),
            },
            [fixture.account()],
        )
        .unwrap_or_else(|error| panic!("valid coordinator: {error}")),
    );
    let limits = Arc::new(fixture.limits(Decimal::new(150, 0)));
    let start = Arc::new(Barrier::new(3));
    let finish = Arc::new(Barrier::new(3));

    let mut workers = Vec::new();
    for suffix in [1_u8, 2] {
        let coordinator = Arc::clone(&coordinator);
        let limits = Arc::clone(&limits);
        let start = Arc::clone(&start);
        let finish = Arc::clone(&finish);
        let intent = fixture.intent(suffix, 1);
        workers.push(std::thread::spawn(move || {
            start.wait();
            let reservation = coordinator.try_reserve(&intent, PriceTicks::new(100), &limits);
            let accepted = reservation.is_ok();
            finish.wait();
            (accepted, reservation)
        }));
    }

    start.wait();
    finish.wait();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| {
            worker
                .join()
                .unwrap_or_else(|_| panic!("reservation worker must not panic"))
        })
        .collect();
    assert_eq!(results.iter().filter(|(accepted, _)| *accepted).count(), 1);
    let rejection = results
        .iter()
        .find_map(|(_, result)| result.as_ref().err())
        .unwrap_or_else(|| panic!("one reservation must be rejected"));
    assert!(
        rejection
            .reasons()
            .contains(&AccountRiskViolation::InsufficientCash)
            || rejection
                .reasons()
                .contains(&AccountRiskViolation::ExposureLimit)
            || rejection
                .reasons()
                .contains(&AccountRiskViolation::AccountCoordinatorBusy)
    );
}

#[test]
fn drop_releases_capacity_but_idempotency_identity_remains_consumed() {
    let fixture = Fixture::new();
    let account_config = AccountCoordinatorConfig {
        maximum_intent_lifetime_nanos: NonZeroU64::new(i64::MAX as u64)
            .unwrap_or_else(|| panic!("fixture intent lifetime is nonzero")),
        ..AccountCoordinatorConfig::default()
    };
    let coordinator = AccountRiskCoordinator::try_new(account_config, [fixture.account()])
        .unwrap_or_else(|error| panic!("valid coordinator: {error}"));
    let limits = fixture.limits(Decimal::new(150, 0));
    let first = fixture.intent(1, 1);
    let reservation = coordinator
        .try_reserve(&first, PriceTicks::new(100), &limits)
        .unwrap_or_else(|error| panic!("first reservation succeeds: {error}"));
    assert!(reservation.validate_current().is_ok());
    drop(reservation);

    let replacement = fixture.intent(2, 1);
    coordinator
        .try_reserve(&replacement, PriceTicks::new(100), &limits)
        .unwrap_or_else(|error| panic!("released exposure is reusable: {error}"));
    let duplicate = match coordinator.try_reserve(&first, PriceTicks::new(100), &limits) {
        Ok(_) => panic!("a consumed client-order identity must remain a duplicate"),
        Err(rejection) => rejection,
    };
    assert!(
        duplicate
            .reasons()
            .contains(&AccountRiskViolation::DuplicateClientOrder)
    );

    let persisted = coordinator
        .snapshot_idempotency(fixture.account_id)
        .unwrap_or_else(|error| panic!("snapshot replay fence: {error}"));
    let mut restarted_account = fixture.account();
    restarted_account.idempotency = persisted;
    let restarted = AccountRiskCoordinator::try_new(account_config, [restarted_account])
        .unwrap_or_else(|error| panic!("restart loads replay fence: {error}"));
    let restart_duplicate = restarted
        .try_reserve(&first, PriceTicks::new(100), &limits)
        .err()
        .unwrap_or_else(|| panic!("restart must not reauthorize a non-expired replay"));
    assert!(
        restart_duplicate
            .reasons()
            .contains(&AccountRiskViolation::DuplicateOrder)
    );
}

#[test]
fn expired_tombstone_compaction_restores_capacity_and_snapshot_preserves_the_successor() {
    let fixture = Fixture::new();
    let config = AccountCoordinatorConfig {
        max_idempotency_keys_per_account: NonZeroUsize::MIN,
        maximum_intent_lifetime_nanos: NonZeroU64::new(1_000_000_000)
            .unwrap_or_else(|| panic!("fixture intent lifetime is nonzero")),
        ..AccountCoordinatorConfig::default()
    };
    let coordinator = AccountRiskCoordinator::try_new(config, [fixture.account()])
        .unwrap_or_else(|error| panic!("valid coordinator: {error}"));
    let limits = fixture.limits(Decimal::new(150, 0));
    let first_signal = current_timestamp();
    let first_expiry = first_signal
        .checked_add_nanos(10_000_000)
        .unwrap_or_else(|error| panic!("bounded first expiry: {error}"));
    let first = fixture.intent_with_times(1, 1, first_signal, first_expiry);
    drop(
        coordinator
            .try_reserve(&first, PriceTicks::new(100), &limits)
            .unwrap_or_else(|error| panic!("first reservation succeeds: {error}")),
    );
    let stale_snapshot = coordinator
        .snapshot_idempotency(fixture.account_id)
        .unwrap_or_else(|error| panic!("snapshot first replay fence: {error}"));
    let stale_revision = stale_snapshot.revision();

    let blocked_signal = current_timestamp();
    let blocked = fixture.intent_with_times(
        2,
        1,
        blocked_signal,
        blocked_signal
            .checked_add_nanos(100_000_000)
            .unwrap_or_else(|error| panic!("bounded blocked expiry: {error}")),
    );
    let capacity = coordinator
        .try_reserve(&blocked, PriceTicks::new(100), &limits)
        .err()
        .unwrap_or_else(|| panic!("non-expired tombstone must retain the sole slot"));
    assert!(
        capacity
            .reasons()
            .contains(&AccountRiskViolation::IdempotencyCapacity)
    );

    let deadline = Instant::now() + Duration::from_secs(1);
    while current_timestamp() <= first_expiry {
        assert!(
            Instant::now() < deadline,
            "trusted wall clock did not advance beyond the fixed tombstone expiry"
        );
        std::thread::yield_now();
    }
    let mut expired_bootstrap = fixture.account();
    expired_bootstrap.idempotency = stale_snapshot;
    assert_eq!(
        AccountRiskCoordinator::try_new(config, [expired_bootstrap]).err(),
        Some(market_squawk_execution::AccountCoordinatorError::InvalidIdempotencyBootstrap)
    );
    let compacted = coordinator
        .snapshot_idempotency(fixture.account_id)
        .unwrap_or_else(|error| panic!("idle snapshot compacts expired identities: {error}"));
    assert!(compacted.tombstones().is_empty());
    assert!(compacted.revision() > stale_revision);
    let successor_signal = current_timestamp();
    let successor = fixture.intent_with_times(
        2,
        1,
        successor_signal,
        successor_signal
            .checked_add_nanos(100_000_000)
            .unwrap_or_else(|error| panic!("bounded successor expiry: {error}")),
    );
    drop(
        coordinator
            .try_reserve(&successor, PriceTicks::new(100), &limits)
            .unwrap_or_else(|error| panic!("expired tombstone capacity is reusable: {error}")),
    );
    let snapshot = coordinator
        .snapshot_idempotency(fixture.account_id)
        .unwrap_or_else(|error| panic!("snapshot successor replay fence: {error}"));
    assert_eq!(snapshot.tombstones().len(), 1);
    assert_eq!(snapshot.tombstones()[0].order_id(), successor.order_id());

    let mut restarted_account = fixture.account();
    restarted_account.idempotency = snapshot;
    let restarted = AccountRiskCoordinator::try_new(config, [restarted_account])
        .unwrap_or_else(|error| panic!("restart loads compacted fence: {error}"));
    let replay = restarted
        .try_reserve(&successor, PriceTicks::new(100), &limits)
        .err()
        .unwrap_or_else(|| panic!("restart must reject the successor replay"));
    assert!(
        replay
            .reasons()
            .contains(&AccountRiskViolation::DuplicateOrder)
    );
}

#[test]
fn deadline_construction_failure_does_not_mutate_the_replay_fence() {
    let fixture = Fixture::new();
    let config = AccountCoordinatorConfig {
        maximum_intent_lifetime_nanos: NonZeroU64::new(i64::MAX as u64)
            .unwrap_or_else(|| panic!("fixture intent lifetime is nonzero")),
        ..AccountCoordinatorConfig::default()
    };
    let coordinator = AccountRiskCoordinator::try_new(config, [fixture.account()])
        .unwrap_or_else(|error| panic!("valid coordinator: {error}"));
    let before = coordinator
        .snapshot_idempotency(fixture.account_id)
        .unwrap_or_else(|error| panic!("snapshot initial replay fence: {error}"));
    let limits = fixture.limits_with_ttl(Decimal::new(150, 0), i64::MAX);
    let rejection = coordinator
        .try_reserve(&fixture.intent(1, 1), PriceTicks::new(100), &limits)
        .err()
        .unwrap_or_else(|| panic!("unrepresentable deadline must fail closed"));
    assert!(
        rejection
            .reasons()
            .contains(&AccountRiskViolation::ArithmeticOverflow)
    );
    let after = coordinator
        .snapshot_idempotency(fixture.account_id)
        .unwrap_or_else(|error| panic!("snapshot unchanged replay fence: {error}"));
    assert_eq!(after, before);
}

fn current_timestamp() -> Timestamp {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|error| panic!("system clock after epoch: {error}"));
    let nanos = i128::from(elapsed.as_secs()) * 1_000_000_000 + i128::from(elapsed.subsec_nanos());
    Timestamp::from_unix_nanos(
        i64::try_from(nanos).unwrap_or_else(|error| panic!("timestamp range: {error}")),
    )
}

struct Fixture {
    account_id: AccountId,
    instrument_id: InstrumentId,
    terms: InstrumentExecutionTerms,
    usd: Currency,
}

impl Fixture {
    fn new() -> Self {
        let account_id = AccountId::from_str("50000000-0000-0000-0000-000000000001")
            .unwrap_or_else(|error| panic!("valid account fixture: {error}"));
        let instrument_id = InstrumentId::from_str("10000000-0000-0000-0000-000000000001")
            .unwrap_or_else(|error| panic!("valid instrument fixture: {error}"));
        let usd = Currency::try_from("USD")
            .unwrap_or_else(|error| panic!("valid currency fixture: {error}"));
        let terms = InstrumentExecutionTerms::try_new(
            instrument_id,
            InstrumentDefinitionRevision::try_from(1)
                .unwrap_or_else(|error| panic!("valid revision fixture: {error}")),
            TickSize::try_from_decimal(Decimal::new(1, 0))
                .unwrap_or_else(|error| panic!("valid tick fixture: {error}")),
            LotSize::try_from_decimal(Decimal::new(1, 0))
                .unwrap_or_else(|error| panic!("valid lot fixture: {error}")),
            usd,
            Denomination::Currency(usd),
            Decimal::ONE,
        )
        .unwrap_or_else(|error| panic!("valid terms fixture: {error}"));
        Self {
            account_id,
            instrument_id,
            terms,
            usd,
        }
    }

    fn account(&self) -> AccountBootstrap {
        AccountBootstrap {
            account_id: self.account_id,
            revision: NonZeroU64::new(1).unwrap_or_else(|| panic!("fixture revision is nonzero")),
            eligible: true,
            cash: Money::new(Decimal::new(150, 0), self.usd),
            capital: Money::new(Decimal::new(150, 0), self.usd),
            peak_capital: Money::new(Decimal::new(150, 0), self.usd),
            gross_exposure: Money::new(Decimal::ZERO, self.usd),
            realized_pnl: Money::new(Decimal::ZERO, self.usd),
            realized_loss: Money::new(Decimal::ZERO, self.usd),
            positions: vec![(self.instrument_id, 0)],
            position_cost_basis: vec![(self.instrument_id, Money::new(Decimal::ZERO, self.usd))],
            idempotency: AccountIdempotencyBootstrap::empty(),
        }
    }

    fn limits(&self, account_ceiling: Decimal) -> RiskLimits {
        self.limits_with_ttl(account_ceiling, 1_000_000_000)
    }

    fn limits_with_ttl(&self, account_ceiling: Decimal, reservation_ttl_nanos: i64) -> RiskLimits {
        RiskLimits::try_new(RiskLimitsInput {
            currency: self.usd,
            eligible_instruments: BTreeSet::from([self.instrument_id]),
            maximum_position_lots: 1_000,
            maximum_order_notional: Money::new(account_ceiling, self.usd),
            maximum_gross_exposure: Money::new(account_ceiling, self.usd),
            maximum_leverage: BasisPoints::new(10_000),
            minimum_capital: Money::new(Decimal::ONE, self.usd),
            maximum_loss: Money::new(account_ceiling, self.usd),
            maximum_drawdown: Money::new(account_ceiling, self.usd),
            maximum_fee: BasisPoints::new(0),
            maximum_price_deviation: BasisPoints::new(100),
            maximum_slippage: BasisPoints::new(100),
            maximum_orders_per_window: NonZeroU32::new(8)
                .unwrap_or_else(|| panic!("fixture rate count is nonzero")),
            order_rate_window_nanos: 1_000_000_000,
            reservation_ttl_nanos,
            allow_short: false,
            kill_switch: false,
        })
        .unwrap_or_else(|error| panic!("valid limits: {error}"))
    }

    fn intent(&self, suffix: u8, quantity: i64) -> OrderIntent {
        self.intent_with_times(
            suffix,
            quantity,
            Timestamp::from_unix_nanos(1),
            Timestamp::from_unix_nanos(i64::MAX),
        )
    }

    fn intent_with_times(
        &self,
        suffix: u8,
        quantity: i64,
        signal_at: Timestamp,
        expires_at: Timestamp,
    ) -> OrderIntent {
        let order_id = format!("20000000-0000-0000-0000-{suffix:012}");
        OrderIntent::try_new(OrderIntentInput {
            order_id: OrderId::from_str(&order_id)
                .unwrap_or_else(|error| panic!("valid order fixture: {error}")),
            client_order_id: ClientOrderId::try_from(format!("client-{suffix}"))
                .unwrap_or_else(|error| panic!("valid client-order fixture: {error}")),
            strategy_id: StrategyId::from_str("30000000-0000-0000-0000-000000000001")
                .unwrap_or_else(|error| panic!("valid strategy fixture: {error}")),
            model_id: None,
            account_id: self.account_id,
            execution_terms: self.terms,
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            quantity: QuantityLots::new(quantity)
                .unwrap_or_else(|error| panic!("valid quantity fixture: {error}")),
            limit_price: Some(PriceTicks::new(100)),
            stop_price: None,
            time_in_force: TimeInForce::Day,
            signal_at,
            expires_at,
            reason_codes: vec![
                OrderReasonCode::try_from("test")
                    .unwrap_or_else(|error| panic!("valid reason fixture: {error}")),
            ],
            maximum_slippage: BasisPoints::new(10),
            required_quality: DataQuality::DirectVerified,
        })
        .unwrap_or_else(|error| panic!("valid intent fixture: {error}"))
    }
}
