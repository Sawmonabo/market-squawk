use std::num::NonZeroU64;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use market_squawk_domain::{AccountId, Currency, InstrumentId, Money, OrderId, Timestamp};
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
    ACCOUNT_REPLACEMENT_SCHEMA_VERSION, AccountReservationStateError, ExecutionStateSourceBinding,
    OrderIntentDigest, ReconciledAccountState,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn replacement_validation_rejects_stale_partial_mismatched_and_rollback_images() -> TestResult {
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
        Err(AccountReservationStateError::AccountStateChanged)
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
            realized_loss: self.money(1),
            positions: vec![(self.instrument_id, 1)],
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
            self.money(99),
            self.money(peak_capital),
            self.money(9),
            self.money(realized_loss),
            vec![(self.instrument_id, 1)],
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
}
