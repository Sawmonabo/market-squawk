//! Fixed-capacity account ownership and atomic risk reservations.

mod contracts;
mod replacement;
mod reservation;

pub use contracts::{
    AccountBootstrap, AccountCoordinatorConfig, AccountCoordinatorError,
    AccountIdempotencyBootstrap, AccountIdempotencyBootstrapError, AccountIdempotencySnapshotError,
    AccountIdempotencyTombstone,
};
pub(crate) use replacement::{
    AccountReplacementCandidate, AccountReplacementReservationBinding, AccountStateReplacementBatch,
};
#[cfg(test)]
pub(crate) use reservation::accepted_reservation_for_test;
pub(crate) use reservation::{AccountOutcomeFailSafe, AccountSubmissionFailSafe};
pub use reservation::{
    AccountReservationError, AccountReservationStateError, AccountRiskReservation,
};

use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

use market_squawk_domain::{
    AccountId, Currency, InstrumentId, Money, OrderSide, PriceTicks, Timestamp,
};
use rust_decimal::Decimal;

use crate::clock::{AccountReservationLease, ClockReading, monotonic_deadline, system_now};
use crate::limits::{AccountRiskViolation, ReservationCalculation};
use crate::{OrderIntent, RiskLimits};

/// Fixed-partition authoritative account owner.
#[derive(Debug)]
pub struct AccountRiskCoordinator {
    config: AccountCoordinatorConfig,
    partitions: Box<[Mutex<AccountPartition>]>,
}

impl AccountRiskCoordinator {
    /// Takes ownership of bounded startup account state.
    ///
    /// # Errors
    ///
    /// Rejects duplicate, invalid, or over-capacity account inputs atomically before use.
    pub fn try_new(
        config: AccountCoordinatorConfig,
        accounts: impl IntoIterator<Item = AccountBootstrap>,
    ) -> Result<Self, AccountCoordinatorError> {
        let now = system_now().map_err(|_| AccountCoordinatorError::ClockFailure)?;
        if config.maximum_intent_lifetime_nanos.get() > i64::MAX as u64 {
            return Err(AccountCoordinatorError::InvalidIdempotencyBootstrap);
        }
        let mut partitions = Vec::with_capacity(config.partition_count.get());
        for _ in 0..config.partition_count.get() {
            partitions.push(AccountPartition {
                accounts: HashMap::with_capacity(config.max_accounts_per_partition.get()),
            });
        }
        for mut account in accounts {
            validate_bootstrap(&mut account, config, now)?;
            let index = partition_index(account.account_id, config.partition_count.get());
            let partition = &mut partitions[index];
            if partition.accounts.contains_key(&account.account_id) {
                return Err(AccountCoordinatorError::DuplicateAccount);
            }
            if partition.accounts.len() >= config.max_accounts_per_partition.get() {
                return Err(AccountCoordinatorError::AccountCapacity);
            }
            let account_id = account.account_id;
            partition.accounts.insert(
                account_id,
                AccountState::try_from_bootstrap(account, config)?,
            );
        }
        Ok(Self {
            config,
            partitions: partitions
                .into_iter()
                .map(Mutex::new)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    /// Evaluates current authoritative account state without consuming rate or idempotency space.
    ///
    /// This nonblocking preflight cannot authorize or dispatch an order. A later reservation still
    /// reruns every check atomically under the partition owner.
    pub fn assess(
        &self,
        intent: &OrderIntent,
        reservation_price: PriceTicks,
        limits: &RiskLimits,
    ) -> Result<(), AccountReservationError> {
        let now = system_now().map_err(|_| {
            AccountReservationError::from_reason(AccountRiskViolation::ClockFailure)
        })?;
        let index = partition_index(intent.account_id(), self.config.partition_count.get());
        let partition = match self.partitions[index].try_lock() {
            Ok(partition) => partition,
            Err(TryLockError::WouldBlock) => {
                return Err(AccountReservationError::from_reason(
                    AccountRiskViolation::AccountCoordinatorBusy,
                ));
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AccountReservationError::from_reason(
                    AccountRiskViolation::AccountCoordinatorPoisoned,
                ));
            }
        };
        let Some(account) = partition.accounts.get(&intent.account_id()) else {
            return Err(AccountReservationError::from_reason(
                AccountRiskViolation::AccountNotFound,
            ));
        };
        account
            .assess(intent, reservation_price, limits, now, self.config)
            .map(|_| ())
    }

    /// Atomically reserves account cash, position, exposure, rate, and idempotency capacity.
    ///
    /// The method uses nonblocking partition acquisition. It accepts no caller-authored account
    /// snapshot or time and returns no execution/adapter authority.
    pub fn try_reserve(
        &self,
        intent: &OrderIntent,
        reservation_price: PriceTicks,
        limits: &RiskLimits,
    ) -> Result<AccountRiskReservation, AccountReservationError> {
        let now = system_now().map_err(|_| {
            AccountReservationError::from_reason(AccountRiskViolation::ClockFailure)
        })?;
        let index = partition_index(intent.account_id(), self.config.partition_count.get());
        let mut partition = match self.partitions[index].try_lock() {
            Ok(partition) => partition,
            Err(TryLockError::WouldBlock) => {
                return Err(AccountReservationError::from_reason(
                    AccountRiskViolation::AccountCoordinatorBusy,
                ));
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AccountReservationError::from_reason(
                    AccountRiskViolation::AccountCoordinatorPoisoned,
                ));
            }
        };
        let Some(account) = partition.accounts.get_mut(&intent.account_id()) else {
            return Err(AccountReservationError::from_reason(
                AccountRiskViolation::AccountNotFound,
            ));
        };
        account.try_reserve(intent, reservation_price, limits, now, self.config)
    }

    /// Returns one bounded, restart-loadable replay-fence snapshot for durable persistence.
    ///
    /// The trusted-time boundary evicts only tombstones whose inclusive intent deadline has passed
    /// and advances the replay revision before copying the snapshot. This method is control-plane
    /// only and performs no filesystem or network I/O.
    pub fn snapshot_idempotency(
        &self,
        account_id: AccountId,
    ) -> Result<AccountIdempotencyBootstrap, AccountIdempotencySnapshotError> {
        let now = system_now().map_err(|_| AccountIdempotencySnapshotError::ClockFailure)?;
        let index = partition_index(account_id, self.config.partition_count.get());
        let mut partition = match self.partitions[index].try_lock() {
            Ok(partition) => partition,
            Err(TryLockError::WouldBlock) => return Err(AccountIdempotencySnapshotError::Busy),
            Err(TryLockError::Poisoned(_)) => {
                return Err(AccountIdempotencySnapshotError::Poisoned);
            }
        };
        let account = partition
            .accounts
            .get_mut(&account_id)
            .ok_or(AccountIdempotencySnapshotError::AccountNotFound)?;
        account
            .compact_expired_idempotency(now.wall)
            .map_err(|()| AccountIdempotencySnapshotError::RevisionExhausted)?;
        Ok(AccountIdempotencyBootstrap {
            revision: account.idempotency_revision,
            tombstones: account.idempotency_tombstones.clone().into_boxed_slice(),
        })
    }
}

#[derive(Debug)]
struct AccountPartition {
    accounts: HashMap<AccountId, AccountState>,
}

#[derive(Debug)]
struct AccountState {
    eligible: bool,
    currency: Currency,
    cash: Money,
    settled_capital: Money,
    capital: Money,
    peak_capital: Money,
    gross_exposure: Money,
    unrealized_pnl: Money,
    drawdown: Money,
    mark_digest: [u8; 32],
    realized_pnl: Money,
    realized_loss: Money,
    positions: HashMap<InstrumentId, i64>,
    position_cost_basis: HashMap<InstrumentId, Money>,
    account_revision: Arc<AtomicU64>,
    reconciliation_required: Arc<AtomicBool>,
    reservations: Vec<ReservationRecord>,
    idempotency_revision: NonZeroU64,
    idempotency_tombstones: Vec<AccountIdempotencyTombstone>,
    rate_events: VecDeque<i64>,
    last_reconciliation: Option<replacement::AccountReplacementSource>,
}

impl AccountState {
    fn try_from_bootstrap(
        bootstrap: AccountBootstrap,
        config: AccountCoordinatorConfig,
    ) -> Result<Self, AccountCoordinatorError> {
        let mut positions = HashMap::new();
        positions
            .try_reserve(config.max_positions_per_account.get())
            .map_err(|_| AccountCoordinatorError::Allocation)?;
        positions.extend(bootstrap.positions);
        let mut position_cost_basis = HashMap::new();
        position_cost_basis
            .try_reserve(config.max_positions_per_account.get())
            .map_err(|_| AccountCoordinatorError::Allocation)?;
        position_cost_basis.extend(bootstrap.position_cost_basis);
        let mut reservations = Vec::new();
        reservations
            .try_reserve_exact(config.max_reservations_per_account.get())
            .map_err(|_| AccountCoordinatorError::Allocation)?;
        let mut idempotency_tombstones = Vec::new();
        idempotency_tombstones
            .try_reserve_exact(config.max_idempotency_keys_per_account.get())
            .map_err(|_| AccountCoordinatorError::Allocation)?;
        idempotency_tombstones.extend(bootstrap.idempotency.tombstones.iter().cloned());
        let mut rate_events = VecDeque::new();
        rate_events
            .try_reserve_exact(config.max_rate_events_per_account.get())
            .map_err(|_| AccountCoordinatorError::Allocation)?;
        let drawdown = bootstrap
            .peak_capital
            .checked_sub(bootstrap.capital)
            .map_err(|_| AccountCoordinatorError::InvalidBootstrap)?;
        Ok(Self {
            eligible: bootstrap.eligible,
            currency: bootstrap.cash.currency(),
            cash: bootstrap.cash,
            settled_capital: bootstrap.capital,
            capital: bootstrap.capital,
            peak_capital: bootstrap.peak_capital,
            gross_exposure: bootstrap.gross_exposure,
            unrealized_pnl: Money::new(Decimal::ZERO, bootstrap.cash.currency()),
            drawdown,
            mark_digest: [0; 32],
            realized_pnl: bootstrap.realized_pnl,
            realized_loss: bootstrap.realized_loss,
            positions,
            position_cost_basis,
            account_revision: Arc::new(AtomicU64::new(bootstrap.revision.get())),
            reconciliation_required: Arc::new(AtomicBool::new(false)),
            reservations,
            idempotency_revision: bootstrap.idempotency.revision,
            idempotency_tombstones,
            rate_events,
            last_reconciliation: None,
        })
    }

    fn try_reserve(
        &mut self,
        intent: &OrderIntent,
        reservation_price: PriceTicks,
        limits: &RiskLimits,
        now: ClockReading,
        config: AccountCoordinatorConfig,
    ) -> Result<AccountRiskReservation, AccountReservationError> {
        let oldest = now
            .wall
            .unix_nanos()
            .checked_sub(limits.order_rate_window_nanos())
            .unwrap_or(i64::MIN);
        let calculation = self.assess(intent, reservation_price, limits, now, config)?;
        let removes_expired = self
            .idempotency_tombstones
            .iter()
            .any(|tombstone| now.wall > tombstone.intent_expires_at);
        let revision_steps = 1_u64 + u64::from(removes_expired);
        let next_idempotency_revision = self
            .idempotency_revision
            .get()
            .checked_add(revision_steps)
            .and_then(NonZeroU64::new)
            .ok_or_else(|| {
                AccountReservationError::from_reason(
                    AccountRiskViolation::IdempotencyRevisionExhausted,
                )
            })?;
        let terms = intent.execution_terms();
        let wall_expiry = intent.expires_at().min(
            now.wall
                .checked_add_nanos(limits.reservation_ttl_nanos())
                .map_err(|_| {
                    AccountReservationError::from_reason(AccountRiskViolation::ArithmeticOverflow)
                })?,
        );
        let monotonic_expiry =
            monotonic_deadline(now, limits.reservation_ttl_nanos()).map_err(|_| {
                AccountReservationError::from_reason(AccountRiskViolation::ClockFailure)
            })?;
        let lease = Arc::new(AccountReservationLease::new(
            Arc::clone(&self.account_revision),
            Arc::clone(&self.reconciliation_required),
            self.account_revision.load(Ordering::Acquire),
            wall_expiry,
            monotonic_expiry,
        ));

        // Every fallible calculation is complete. The preallocated collections below now publish
        // the reservation, tombstone, revision, and rate event as one infallible lock-owned step.
        if removes_expired {
            self.idempotency_tombstones
                .retain(|tombstone| now.wall <= tombstone.intent_expires_at);
        }
        self.reservations.retain(ReservationRecord::retained);
        while self
            .rate_events
            .front()
            .is_some_and(|timestamp| *timestamp <= oldest)
        {
            let _ = self.rate_events.pop_front();
        }
        self.reservations.push(ReservationRecord {
            order_id: intent.order_id(),
            intent_digest: intent.digest(),
            lease: Arc::clone(&lease),
            cash: calculation.cash,
            exposure: calculation.exposure,
            instrument_id: terms.instrument_id(),
            signed_quantity: calculation.signed_quantity,
        });
        self.idempotency_tombstones
            .push(AccountIdempotencyTombstone::new(
                intent.order_id(),
                intent.client_order_id().clone(),
                intent.digest(),
                intent.expires_at(),
            ));
        self.idempotency_revision = next_idempotency_revision;
        self.rate_events.push_back(now.wall.unix_nanos());
        Ok(AccountRiskReservation {
            account_id: intent.account_id(),
            intent_digest: intent.digest(),
            lease,
        })
    }

    fn compact_expired_idempotency(&mut self, now: Timestamp) -> Result<(), ()> {
        if !self
            .idempotency_tombstones
            .iter()
            .any(|tombstone| now > tombstone.intent_expires_at)
        {
            return Ok(());
        }
        let next_revision = self
            .idempotency_revision
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(())?;
        self.idempotency_tombstones
            .retain(|tombstone| now <= tombstone.intent_expires_at);
        self.idempotency_revision = next_revision;
        Ok(())
    }

    fn assess(
        &self,
        intent: &OrderIntent,
        reservation_price: PriceTicks,
        limits: &RiskLimits,
        now: ClockReading,
        config: AccountCoordinatorConfig,
    ) -> Result<ReservationCalculation, AccountReservationError> {
        let mut reasons = Vec::new();
        if limits.kill_switch() {
            reasons.push(AccountRiskViolation::KillSwitch);
        }
        if self.reconciliation_required.load(Ordering::Acquire) {
            reasons.push(AccountRiskViolation::ReconciliationRequired);
        }
        if !self.eligible {
            reasons.push(AccountRiskViolation::AccountIneligible);
        }
        let terms = intent.execution_terms();
        if !limits.instrument_is_eligible(terms.instrument_id()) {
            reasons.push(AccountRiskViolation::InstrumentIneligible);
        }
        if terms.settlement_currency().is_none() {
            reasons.push(AccountRiskViolation::UnsupportedSettlement);
        }
        if self.currency != limits.currency()
            || terms.quote_currency() != self.currency
            || terms
                .settlement_currency()
                .is_some_and(|value| value != self.currency)
        {
            reasons.push(AccountRiskViolation::CurrencyMismatch);
        }
        if now.wall > intent.expires_at() {
            reasons.push(AccountRiskViolation::IntentExpired);
        }
        let intent_lifetime = i128::from(intent.expires_at().unix_nanos())
            - i128::from(intent.signal_at().unix_nanos());
        if intent_lifetime > i128::from(config.maximum_intent_lifetime_nanos.get()) {
            reasons.push(AccountRiskViolation::IntentLifetimeExceeded);
        }
        let active_tombstones = self
            .idempotency_tombstones
            .iter()
            .filter(|tombstone| now.wall <= tombstone.intent_expires_at);
        if active_tombstones
            .clone()
            .any(|tombstone| tombstone.client_order_id == *intent.client_order_id())
        {
            reasons.push(AccountRiskViolation::DuplicateClientOrder);
        }
        if active_tombstones
            .clone()
            .any(|tombstone| tombstone.order_id == intent.order_id())
        {
            reasons.push(AccountRiskViolation::DuplicateOrder);
        }
        if active_tombstones.count() >= config.max_idempotency_keys_per_account.get() {
            reasons.push(AccountRiskViolation::IdempotencyCapacity);
        }
        if self
            .reservations
            .iter()
            .filter(|reservation| reservation.retained())
            .count()
            >= config.max_reservations_per_account.get()
        {
            reasons.push(AccountRiskViolation::ReservationCapacity);
        }
        let oldest = now
            .wall
            .unix_nanos()
            .checked_sub(limits.order_rate_window_nanos())
            .unwrap_or(i64::MIN);
        let recent_rate_events = self
            .rate_events
            .iter()
            .filter(|timestamp| **timestamp > oldest)
            .count();
        if recent_rate_events
            >= usize::try_from(limits.maximum_orders_per_window().get()).unwrap_or(usize::MAX)
            || recent_rate_events >= config.max_rate_events_per_account.get()
        {
            reasons.push(AccountRiskViolation::OrderRateLimit);
        }

        let calculation = ReservationCalculation::for_intent(intent, reservation_price, limits);
        match &calculation {
            Ok(calculation) => self.evaluate_calculation(intent, limits, calculation, &mut reasons),
            Err(()) => reasons.push(AccountRiskViolation::ArithmeticOverflow),
        }
        if !reasons.is_empty() {
            return Err(AccountReservationError::from_reasons(reasons));
        }
        calculation.map_err(|()| {
            AccountReservationError::from_reason(AccountRiskViolation::ArithmeticOverflow)
        })
    }

    fn evaluate_calculation(
        &self,
        intent: &OrderIntent,
        limits: &RiskLimits,
        calculation: &ReservationCalculation,
        reasons: &mut Vec<AccountRiskViolation>,
    ) {
        if calculation.exposure.amount() > limits.maximum_order_notional().amount() {
            reasons.push(AccountRiskViolation::OrderNotionalLimit);
        }
        let active = match self.active_totals(intent.execution_terms().instrument_id()) {
            Ok(active) => active,
            Err(()) => {
                reasons.push(AccountRiskViolation::ArithmeticOverflow);
                return;
            }
        };
        if intent.side() == OrderSide::Buy
            && active
                .cash
                .checked_add(calculation.cash)
                .is_ok_and(|reserved| reserved.amount() > self.cash.amount())
        {
            reasons.push(AccountRiskViolation::InsufficientCash);
        }
        let current_position = self
            .positions
            .get(&intent.execution_terms().instrument_id())
            .copied()
            .unwrap_or(0);
        let projected_position = current_position
            .checked_add(active.signed_quantity)
            .and_then(|value| value.checked_add(calculation.signed_quantity));
        match projected_position {
            Some(position) => {
                if position.unsigned_abs() > limits.maximum_position_lots().unsigned_abs() {
                    reasons.push(AccountRiskViolation::PositionLimit);
                }
                if !limits.allow_short() && position < 0 {
                    reasons.push(AccountRiskViolation::InsufficientPosition);
                }
            }
            None => reasons.push(AccountRiskViolation::ArithmeticOverflow),
        }
        let projected_exposure = self
            .gross_exposure
            .checked_add(active.exposure)
            .and_then(|value| value.checked_add(calculation.exposure));
        match projected_exposure {
            Ok(exposure) => {
                if exposure.amount() > limits.maximum_gross_exposure().amount() {
                    reasons.push(AccountRiskViolation::ExposureLimit);
                }
                if limits.leverage_exceeded(exposure, self.capital) {
                    reasons.push(AccountRiskViolation::LeverageLimit);
                }
            }
            Err(_) => reasons.push(AccountRiskViolation::ArithmeticOverflow),
        }
        if self.capital.amount() < limits.minimum_capital().amount() {
            reasons.push(AccountRiskViolation::CapitalLimit);
        }
        let unrealized_loss_amount = if self.unrealized_pnl.amount().is_sign_negative() {
            match Decimal::ZERO.checked_sub(self.unrealized_pnl.amount()) {
                Some(value) => value,
                None => {
                    reasons.push(AccountRiskViolation::ArithmeticOverflow);
                    Decimal::ZERO
                }
            }
        } else {
            Decimal::ZERO
        };
        let unrealized_loss = Money::new(unrealized_loss_amount, self.currency);
        match self.realized_loss.checked_add(unrealized_loss) {
            Ok(loss) if loss.amount() > limits.maximum_loss().amount() => {
                reasons.push(AccountRiskViolation::LossLimit);
            }
            Ok(_) => {}
            Err(_) => reasons.push(AccountRiskViolation::ArithmeticOverflow),
        }
        if self.drawdown.amount() > limits.maximum_drawdown().amount() {
            reasons.push(AccountRiskViolation::DrawdownLimit);
        }
    }

    fn active_totals(&self, instrument_id: InstrumentId) -> Result<ActiveTotals, ()> {
        let zero = Money::new(Decimal::ZERO, self.currency);
        let mut totals = ActiveTotals {
            cash: zero,
            exposure: zero,
            signed_quantity: 0,
        };
        for reservation in self
            .reservations
            .iter()
            .filter(|reservation| reservation.counts_against_limits())
        {
            totals.cash = totals.cash.checked_add(reservation.cash).map_err(|_| ())?;
            totals.exposure = totals
                .exposure
                .checked_add(reservation.exposure)
                .map_err(|_| ())?;
            if reservation.instrument_id == instrument_id {
                totals.signed_quantity = totals
                    .signed_quantity
                    .checked_add(reservation.signed_quantity)
                    .ok_or(())?;
            }
        }
        Ok(totals)
    }
}

#[derive(Clone, Copy, Debug)]
struct ActiveTotals {
    cash: Money,
    exposure: Money,
    signed_quantity: i64,
}

#[derive(Debug)]
struct ReservationRecord {
    order_id: market_squawk_domain::OrderId,
    intent_digest: crate::OrderIntentDigest,
    lease: Arc<AccountReservationLease>,
    cash: Money,
    exposure: Money,
    instrument_id: InstrumentId,
    signed_quantity: i64,
}

impl ReservationRecord {
    fn retained(&self) -> bool {
        self.lease.counts_against_limits()
    }

    fn counts_against_limits(&self) -> bool {
        self.retained()
    }
}

fn validate_bootstrap(
    account: &mut AccountBootstrap,
    config: AccountCoordinatorConfig,
    now: ClockReading,
) -> Result<(), AccountCoordinatorError> {
    let currency = account.cash.currency();
    let money = [
        account.cash,
        account.capital,
        account.peak_capital,
        account.gross_exposure,
        account.realized_loss,
    ];
    if money
        .iter()
        .any(|value| value.currency() != currency || value.amount().is_sign_negative())
        || account.realized_pnl.currency() != currency
        || account.capital.amount().is_zero()
        || account.peak_capital.amount() < account.capital.amount()
        || account.positions.len() > config.max_positions_per_account.get()
        || account.position_cost_basis.len() > config.max_positions_per_account.get()
    {
        return Err(AccountCoordinatorError::InvalidBootstrap);
    }
    account
        .positions
        .sort_unstable_by_key(|(instrument_id, _)| *instrument_id);
    if account
        .positions
        .windows(2)
        .any(|pair| pair[0].0 == pair[1].0)
    {
        return Err(AccountCoordinatorError::InvalidBootstrap);
    }
    account
        .position_cost_basis
        .sort_unstable_by_key(|(instrument_id, _)| *instrument_id);
    if account
        .position_cost_basis
        .windows(2)
        .any(|pair| pair[0].0 == pair[1].0)
        || account.positions.len() != account.position_cost_basis.len()
        || account
            .positions
            .iter()
            .zip(&account.position_cost_basis)
            .any(|((position_id, lots), (basis_id, basis))| {
                position_id != basis_id
                    || basis.currency() != currency
                    || basis.amount().is_sign_negative()
                    || (*lots == 0 && !basis.amount().is_zero())
                    || (*lots != 0 && basis.amount().is_zero())
            })
    {
        return Err(AccountCoordinatorError::InvalidBootstrap);
    }
    let maximum_expiry = now
        .wall
        .checked_add_nanos(
            i64::try_from(config.maximum_intent_lifetime_nanos.get())
                .map_err(|_| AccountCoordinatorError::InvalidIdempotencyBootstrap)?,
        )
        .unwrap_or(Timestamp::from_unix_nanos(i64::MAX));
    if account.idempotency.tombstones.len() > config.max_idempotency_keys_per_account.get()
        || account.idempotency.tombstones.iter().any(|tombstone| {
            now.wall > tombstone.intent_expires_at || tombstone.intent_expires_at > maximum_expiry
        })
    {
        return Err(AccountCoordinatorError::InvalidIdempotencyBootstrap);
    }
    Ok(())
}

fn partition_index(account_id: AccountId, partition_count: usize) -> usize {
    let hash = account_id
        .as_uuid()
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    usize::try_from(hash % partition_count as u64).unwrap_or(0)
}
