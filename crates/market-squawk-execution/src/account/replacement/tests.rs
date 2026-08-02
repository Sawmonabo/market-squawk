use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroU64};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, Currency, DataQuality, Denomination,
    InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId, LotSize, Money, OrderId,
    OrderReasonCode, OrderSide, OrderType, PriceTicks, QuantityLots, StrategyId, TickSize,
    TimeInForce, Timestamp,
};
use rust_decimal::Decimal;

use super::{
    AccountReplacementCandidate, AccountReplacementError, AccountReplacementReservationBinding,
    AccountReplacementSource, AccountStateReplacementBatch, prepare_candidate,
};
use crate::account::{
    AccountBootstrap, AccountCoordinatorConfig, AccountIdempotencyBootstrap,
    AccountRiskCoordinator, ReservationRecord, partition_index,
};
use crate::clock::{AccountReservationLease, ClockReading};
use crate::{
    ACCOUNT_REPLACEMENT_SCHEMA_VERSION, AccountReservationStateError, AccountRiskViolation,
    ExecutionStateSourceBinding, OrderIntent, OrderIntentDigest, OrderIntentInput,
    ReconciledAccountState, RiskLimits, RiskLimitsInput,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn replacement_validation_rejects_stale_partial_mismatched_and_rollback_images() -> TestResult {
    assert_eq!(ACCOUNT_REPLACEMENT_SCHEMA_VERSION, 3);
    assert!(ExecutionStateSourceBinding::try_new(1, [1; 32], NonZeroU64::MIN, [2; 32]).is_err());
    assert!(ExecutionStateSourceBinding::try_new(3, [1; 32], NonZeroU64::MIN, [2; 32]).is_ok());
    let fixture = Fixture::new()?;
    let mut current = crate::account::AccountState::try_from_bootstrap(
        fixture.bootstrap(),
        AccountCoordinatorConfig::default(),
    )?;
    current
        .reconciliation_required
        .store(true, Ordering::Release);
    current.last_reconciliation = Some(AccountReplacementSource {
        configuration_digest: [1; 32],
        snapshot_sequence: 10,
        snapshot_digest: [2; 32],
        invocation_digest: [3; 32],
    });
    let binding = fixture.binding();
    current.reservations.push(fixture.reservation(&current));

    let stale = fixture.candidate(1, vec![binding], 100, 1)?;
    assert_eq!(
        prepare_candidate(
            &current,
            &stale,
            fixture.source([1; 32], 11, [4; 32])?,
            [5; 32],
            8
        )
        .err(),
        Some(AccountReplacementError::RevisionRollback)
    );
    assert_eq!(
        AccountReplacementCandidate::try_new(fixture.image(2, 100, 1)?, 1, Vec::new()).err(),
        Some(AccountReplacementError::InvalidReservationClosure)
    );
    let valid = fixture.candidate(2, vec![binding], 100, 1)?;
    assert_eq!(
        prepare_candidate(
            &current,
            &valid,
            fixture.source([9; 32], 11, [4; 32])?,
            [5; 32],
            8
        )
        .err(),
        Some(AccountReplacementError::SourceRollbackOrMismatch)
    );
    assert_eq!(
        prepare_candidate(
            &current,
            &valid,
            fixture.source([1; 32], 10, [4; 32])?,
            [5; 32],
            8
        )
        .err(),
        Some(AccountReplacementError::SourceRollbackOrMismatch)
    );
    let financial_rollback = fixture.candidate(2, vec![binding], 99, 0)?;
    assert_eq!(
        prepare_candidate(
            &current,
            &financial_rollback,
            fixture.source([1; 32], 11, [4; 32])?,
            [5; 32],
            8,
        )
        .err(),
        Some(AccountReplacementError::FinancialRollback)
    );
    assert_eq!(current.account_revision.load(Ordering::Acquire), 1);
    assert!(current.reconciliation_required.load(Ordering::Acquire));
    assert_eq!(current.reservations.len(), 1);
    Ok(())
}

#[test]
fn complete_replacement_invalidates_old_lease_and_publishes_clear_new_revision() -> TestResult {
    let fixture = Fixture::new()?;
    let coordinator = AccountRiskCoordinator::try_new(
        AccountCoordinatorConfig::default(),
        [fixture.bootstrap()],
    )?;
    let index = partition_index(fixture.account_id, coordinator.config.partition_count.get());
    let lease = {
        let mut partition = coordinator.partitions[index]
            .lock()
            .map_err(|_| "account partition poisoned")?;
        let account = partition
            .accounts
            .get_mut(&fixture.account_id)
            .ok_or("account missing")?;
        account
            .reconciliation_required
            .store(true, Ordering::Release);
        let reservation = fixture.reservation(account);
        let lease = Arc::clone(&reservation.lease);
        account.reservations.push(reservation);
        lease
    };
    let batch = AccountStateReplacementBatch::try_new(
        fixture.source([1; 32], 11, [4; 32])?,
        [5; 32],
        vec![fixture.candidate(2, vec![fixture.binding()], 100, 1)?],
    )?;

    coordinator.replace_reconciled_accounts(batch)?;

    assert_eq!(
        lease.validate(ClockReading {
            wall: Timestamp::from_unix_nanos(1),
            monotonic: Instant::now(),
        }),
        Err(AccountReservationStateError::ReconciliationRequired)
    );
    let partition = coordinator.partitions[index]
        .lock()
        .map_err(|_| "account partition poisoned")?;
    let account = partition
        .accounts
        .get(&fixture.account_id)
        .ok_or("account missing")?;
    assert_eq!(account.account_revision.load(Ordering::Acquire), 2);
    assert!(!account.reconciliation_required.load(Ordering::Acquire));
    assert!(account.reservations.is_empty());
    assert_eq!(account.idempotency_revision, NonZeroU64::MIN);
    assert_eq!(account.settled_capital, fixture.money(100));
    assert_eq!(account.capital, fixture.money(99));
    assert_eq!(account.unrealized_pnl, fixture.money(-1));
    assert_eq!(account.gross_exposure, fixture.money(9));
    assert_eq!(account.drawdown, fixture.money(1));
    assert_eq!(account.mark_digest, [6; 32]);
    drop(partition);

    let rejection =
        match coordinator.assess(&fixture.intent()?, PriceTicks::new(10), &fixture.limits()?) {
            Ok(()) => return Err("marked equity below the minimum accepted the next order".into()),
            Err(rejection) => rejection,
        };
    assert!(
        rejection
            .reasons()
            .contains(&AccountRiskViolation::CapitalLimit)
    );
    Ok(())
}

#[test]
fn complete_account_replacement_rejects_a_partial_configured_account_set() -> TestResult {
    let fixture = Fixture::new()?;
    let mut second = fixture.bootstrap();
    second.account_id = AccountId::from_str("51000000-0000-0000-0000-000000000100")?;
    let coordinator = AccountRiskCoordinator::try_new(
        AccountCoordinatorConfig::default(),
        [fixture.bootstrap(), second],
    )?;
    let source = fixture.source([1; 32], 11, [4; 32])?;
    let state = fixture.image(2, 100, 1)?;

    assert_eq!(
        coordinator
            .replace_unreserved_reconciled_accounts(source, [5; 32], &[state])
            .err(),
        Some(AccountReplacementError::InvalidAccountClosure)
    );
    Ok(())
}

struct Fixture {
    account_id: AccountId,
    instrument_id: InstrumentId,
    order_id: OrderId,
    currency: Currency,
    digest: OrderIntentDigest,
}

impl Fixture {
    fn new() -> TestResult<Self> {
        Ok(Self {
            account_id: AccountId::from_str("51000000-0000-0000-0000-000000000099")?,
            instrument_id: InstrumentId::from_str("11000000-0000-0000-0000-000000000099")?,
            order_id: OrderId::from_str("21000000-0000-0000-0000-000000000099")?,
            currency: Currency::try_from("USD")?,
            digest: OrderIntentDigest::from_bytes([7; 32]),
        })
    }

    fn bootstrap(&self) -> AccountBootstrap {
        AccountBootstrap {
            account_id: self.account_id,
            revision: NonZeroU64::MIN,
            eligible: true,
            cash: self.money(100),
            capital: self.money(100),
            peak_capital: self.money(100),
            gross_exposure: self.money(10),
            realized_pnl: self.money(0),
            realized_loss: self.money(1),
            positions: vec![(self.instrument_id, 1)],
            position_cost_basis: vec![(self.instrument_id, self.money(10))],
            idempotency: AccountIdempotencyBootstrap::empty(),
        }
    }

    fn binding(&self) -> AccountReplacementReservationBinding {
        AccountReplacementReservationBinding::new(self.order_id, self.digest, 1)
    }

    fn reservation(&self, account: &crate::account::AccountState) -> ReservationRecord {
        ReservationRecord {
            order_id: self.order_id,
            intent_digest: self.digest,
            lease: Arc::new(AccountReservationLease::new(
                Arc::clone(&account.account_revision),
                Arc::clone(&account.reconciliation_required),
                1,
                Timestamp::from_unix_nanos(i64::MAX),
                Instant::now() + Duration::from_secs(60),
            )),
            cash: self.money(0),
            exposure: self.money(0),
            instrument_id: self.instrument_id,
            signed_quantity: 0,
        }
    }

    fn candidate(
        &self,
        revision: u64,
        reservations: Vec<AccountReplacementReservationBinding>,
        peak_capital: i64,
        realized_loss: i64,
    ) -> Result<AccountReplacementCandidate, Box<dyn std::error::Error>> {
        Ok(AccountReplacementCandidate::try_new(
            self.image(revision, peak_capital, realized_loss)?,
            1,
            reservations,
        )?)
    }

    fn image(
        &self,
        revision: u64,
        peak_capital: i64,
        realized_loss: i64,
    ) -> Result<ReconciledAccountState, Box<dyn std::error::Error>> {
        Ok(ReconciledAccountState::try_new(
            self.account_id,
            NonZeroU64::new(revision).ok_or("zero revision")?,
            true,
            self.currency,
            self.money(90),
            self.money(100),
            self.money(99),
            self.money(peak_capital),
            self.money(9),
            self.money(-1),
            self.money(peak_capital - 99),
            [6; 32],
            self.money(0),
            self.money(realized_loss),
            vec![(self.instrument_id, 1)],
            vec![(self.instrument_id, self.money(10))],
        )?)
    }

    fn source(
        &self,
        configuration_digest: [u8; 32],
        sequence: u64,
        snapshot_digest: [u8; 32],
    ) -> Result<ExecutionStateSourceBinding, Box<dyn std::error::Error>> {
        Ok(ExecutionStateSourceBinding::try_new(
            ACCOUNT_REPLACEMENT_SCHEMA_VERSION,
            configuration_digest,
            NonZeroU64::new(sequence).ok_or("zero source sequence")?,
            snapshot_digest,
        )?)
    }

    fn money(&self, amount: i64) -> Money {
        Money::new(Decimal::new(amount, 0), self.currency)
    }

    fn terms(&self) -> TestResult<InstrumentExecutionTerms> {
        Ok(InstrumentExecutionTerms::try_new(
            self.instrument_id,
            InstrumentDefinitionRevision::try_from(1)?,
            TickSize::try_from_decimal(Decimal::ONE)?,
            LotSize::try_from_decimal(Decimal::ONE)?,
            self.currency,
            Denomination::Currency(self.currency),
            Decimal::ONE,
        )?)
    }

    fn intent(&self) -> TestResult<OrderIntent> {
        Ok(OrderIntent::try_new(OrderIntentInput {
            order_id: OrderId::from_str("21000000-0000-0000-0000-000000000100")?,
            client_order_id: ClientOrderId::try_from("marked-risk-next")?,
            strategy_id: StrategyId::from_str("31000000-0000-0000-0000-000000000099")?,
            model_id: None,
            account_id: self.account_id,
            execution_terms: self.terms()?,
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            quantity: QuantityLots::new(1)?,
            limit_price: Some(PriceTicks::new(10)),
            stop_price: None,
            time_in_force: TimeInForce::Day,
            signal_at: Timestamp::from_unix_nanos(1),
            expires_at: Timestamp::from_unix_nanos(i64::MAX),
            reason_codes: vec![OrderReasonCode::try_from("marked.risk")?],
            maximum_slippage: BasisPoints::new(100),
            required_quality: DataQuality::DirectVerified,
        })?)
    }

    fn limits(&self) -> TestResult<RiskLimits> {
        Ok(RiskLimits::try_new(RiskLimitsInput {
            currency: self.currency,
            eligible_instruments: BTreeSet::from([self.instrument_id]),
            maximum_position_lots: 100,
            maximum_order_notional: self.money(10_000),
            maximum_gross_exposure: self.money(10_000),
            maximum_leverage: BasisPoints::new(100_000),
            minimum_capital: self.money(100),
            maximum_loss: self.money(10_000),
            maximum_drawdown: self.money(10_000),
            maximum_fee: BasisPoints::new(0),
            maximum_price_deviation: BasisPoints::new(100),
            maximum_slippage: BasisPoints::new(100),
            maximum_orders_per_window: NonZeroU32::MIN,
            order_rate_window_nanos: 1_000_000_000,
            reservation_ttl_nanos: 1_000_000_000,
            allow_short: false,
            kill_switch: false,
        })?)
    }
}
