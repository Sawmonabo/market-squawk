//! Fixed-capacity account ownership and atomic risk reservations.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

use market_squawk_domain::{
    AccountId, ClientOrderId, Currency, InstrumentId, Money, OrderSide, PriceTicks,
};
use rust_decimal::Decimal;
use thiserror::Error;

use crate::clock::{AccountReservationLease, ClockReading, monotonic_deadline, system_now};
use crate::limits::{AccountRiskViolation, ReservationCalculation};
use crate::{OrderIntent, OrderIntentDigest, RiskLimits};

/// Startup-fixed memory and partition bounds for authoritative account coordination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccountCoordinatorConfig {
    /// Number of deterministic account partitions.
    pub partition_count: NonZeroUsize,
    /// Maximum accounts retained in one partition.
    pub max_accounts_per_partition: NonZeroUsize,
    /// Maximum outstanding and terminal reservation records retained before compaction.
    pub max_reservations_per_account: NonZeroUsize,
    /// Maximum positions retained for one account.
    pub max_positions_per_account: NonZeroUsize,
    /// Maximum consumed client-order identities retained for one account.
    pub max_idempotency_keys_per_account: NonZeroUsize,
    /// Maximum accepted timestamps retained for rate enforcement.
    pub max_rate_events_per_account: NonZeroUsize,
}

impl Default for AccountCoordinatorConfig {
    fn default() -> Self {
        Self {
            partition_count: nonzero_usize(16),
            max_accounts_per_partition: nonzero_usize(1_024),
            max_reservations_per_account: nonzero_usize(256),
            max_positions_per_account: nonzero_usize(4_096),
            max_idempotency_keys_per_account: nonzero_usize(4_096),
            max_rate_events_per_account: nonzero_usize(1_024),
        }
    }
}

fn nonzero_usize(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
}

/// One explicit startup account state transferred into coordinator ownership.
///
/// Evaluation never accepts this structure. Once admitted, the coordinator is the only current
/// account authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountBootstrap {
    /// Stable account identity.
    pub account_id: AccountId,
    /// Nonzero upstream account revision.
    pub revision: NonZeroU64,
    /// Whether the account is currently eligible for orders.
    pub eligible: bool,
    /// Current available cash before new reservations.
    pub cash: Money,
    /// Current risk capital.
    pub capital: Money,
    /// Highest capital used for drawdown measurement.
    pub peak_capital: Money,
    /// Current gross exposure before new reservations.
    pub gross_exposure: Money,
    /// Current nonnegative realized loss measure.
    pub realized_loss: Money,
    /// Current signed positions in instrument lots.
    pub positions: Vec<(InstrumentId, i64)>,
}

/// Atomic account coordination construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AccountCoordinatorError {
    /// More accounts hashed to a partition than its startup bound permits.
    #[error("account partition capacity exceeded")]
    AccountCapacity,
    /// The same account appeared more than once.
    #[error("duplicate account bootstrap")]
    DuplicateAccount,
    /// A bootstrap violated currency, sign, peak, position, or capacity invariants.
    #[error("invalid account bootstrap")]
    InvalidBootstrap,
}

/// Stable, nonempty account reservation rejection.
#[derive(Debug, Eq, PartialEq)]
pub struct AccountReservationError {
    reasons: Box<[AccountRiskViolation]>,
}

impl AccountReservationError {
    /// Returns every applicable reason in stable enum order.
    pub const fn reasons(&self) -> &[AccountRiskViolation] {
        &self.reasons
    }

    fn from_reason(reason: AccountRiskViolation) -> Self {
        Self {
            reasons: Box::new([reason]),
        }
    }

    fn from_reasons(mut reasons: Vec<AccountRiskViolation>) -> Self {
        reasons.sort_unstable();
        reasons.dedup();
        debug_assert!(!reasons.is_empty());
        Self {
            reasons: reasons.into_boxed_slice(),
        }
    }
}

impl fmt::Display for AccountReservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("account risk rejected order:")?;
        for reason in &self.reasons {
            write!(formatter, " {reason}")?;
        }
        Ok(())
    }
}

impl std::error::Error for AccountReservationError {}

/// Private-field, non-cloneable account reservation.
///
/// This value conveys no broker or adapter authority. Dropping an active reservation releases its
/// exposure atomically without acquiring the account partition lock.
#[derive(Debug)]
pub struct AccountRiskReservation {
    account_id: AccountId,
    intent_digest: OrderIntentDigest,
    lease: Arc<AccountReservationLease>,
}

impl AccountRiskReservation {
    /// Returns the reserved account.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the exact order-intent digest bound to this reservation.
    pub const fn intent_digest(&self) -> OrderIntentDigest {
        self.intent_digest
    }

    /// Revalidates state revision, reservation state, and both deadlines.
    ///
    /// # Errors
    ///
    /// Fails after release, commit, reconciliation transition, account replacement, clock failure,
    /// or inclusive expiration.
    pub fn validate_current(&self) -> Result<(), AccountReservationStateError> {
        let now = system_now().map_err(|_| AccountReservationStateError::ClockFailure)?;
        self.lease.validate(now)
    }

    /// Explicitly releases the active reservation. Drop has the same fail-safe effect.
    pub fn release(self) {
        self.lease.release();
    }

    /// Marks an uncertain backend outcome. Exposure remains reserved until reconciliation.
    pub fn mark_reconciliation_required(self) {
        self.lease.mark_reconciliation_required();
    }
}

impl Drop for AccountRiskReservation {
    fn drop(&mut self) {
        self.lease.release();
    }
}

/// Current reservation validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AccountReservationStateError {
    /// Reservation is released, committed, or awaiting reconciliation.
    #[error("account reservation is not active")]
    NotActive,
    /// Authoritative account state changed after reservation.
    #[error("account state revision changed")]
    AccountStateChanged,
    /// Either wall or monotonic expiry was reached.
    #[error("account reservation expired")]
    Expired,
    /// Trusted clock failure.
    #[error("trusted account-reservation clock failed")]
    ClockFailure,
}

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
        let mut partitions = Vec::with_capacity(config.partition_count.get());
        for _ in 0..config.partition_count.get() {
            partitions.push(AccountPartition {
                accounts: HashMap::with_capacity(config.max_accounts_per_partition.get()),
            });
        }
        for account in accounts {
            validate_bootstrap(&account, config.max_positions_per_account.get())?;
            let index = partition_index(account.account_id, config.partition_count.get());
            let partition = &mut partitions[index];
            if partition.accounts.contains_key(&account.account_id) {
                return Err(AccountCoordinatorError::DuplicateAccount);
            }
            if partition.accounts.len() >= config.max_accounts_per_partition.get() {
                return Err(AccountCoordinatorError::AccountCapacity);
            }
            let account_id = account.account_id;
            partition
                .accounts
                .insert(account_id, AccountState::from_bootstrap(account));
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
    capital: Money,
    peak_capital: Money,
    gross_exposure: Money,
    realized_loss: Money,
    positions: HashMap<InstrumentId, i64>,
    account_revision: Arc<AtomicU64>,
    reservations: Vec<ReservationRecord>,
    seen_client_orders: BTreeSet<ClientOrderId>,
    rate_events: VecDeque<i64>,
}

impl AccountState {
    fn from_bootstrap(bootstrap: AccountBootstrap) -> Self {
        Self {
            eligible: bootstrap.eligible,
            currency: bootstrap.cash.currency(),
            cash: bootstrap.cash,
            capital: bootstrap.capital,
            peak_capital: bootstrap.peak_capital,
            gross_exposure: bootstrap.gross_exposure,
            realized_loss: bootstrap.realized_loss,
            positions: bootstrap.positions.into_iter().collect(),
            account_revision: Arc::new(AtomicU64::new(bootstrap.revision.get())),
            reservations: Vec::new(),
            seen_client_orders: BTreeSet::new(),
            rate_events: VecDeque::new(),
        }
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
        self.reservations.retain(ReservationRecord::retained);
        while self
            .rate_events
            .front()
            .is_some_and(|timestamp| *timestamp <= oldest)
        {
            let _ = self.rate_events.pop_front();
        }

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
            self.account_revision.load(Ordering::Acquire),
            wall_expiry,
            monotonic_expiry,
        ));
        self.reservations.push(ReservationRecord {
            lease: Arc::clone(&lease),
            cash: calculation.cash,
            exposure: calculation.exposure,
            instrument_id: terms.instrument_id(),
            signed_quantity: calculation.signed_quantity,
        });
        let inserted = self
            .seen_client_orders
            .insert(intent.client_order_id().clone());
        debug_assert!(inserted);
        self.rate_events.push_back(now.wall.unix_nanos());
        Ok(AccountRiskReservation {
            account_id: intent.account_id(),
            intent_digest: intent.digest(),
            lease,
        })
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
        if now.wall >= intent.expires_at() {
            reasons.push(AccountRiskViolation::IntentExpired);
        }
        if self.seen_client_orders.contains(intent.client_order_id()) {
            reasons.push(AccountRiskViolation::DuplicateClientOrder);
        }
        if self.seen_client_orders.len() >= config.max_idempotency_keys_per_account.get() {
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
        if self.realized_loss.amount() > limits.maximum_loss().amount() {
            reasons.push(AccountRiskViolation::LossLimit);
        }
        match self.peak_capital.checked_sub(self.capital) {
            Ok(drawdown) if drawdown.amount() > limits.maximum_drawdown().amount() => {
                reasons.push(AccountRiskViolation::DrawdownLimit);
            }
            Ok(_) => {}
            Err(_) => reasons.push(AccountRiskViolation::ArithmeticOverflow),
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
    account: &AccountBootstrap,
    maximum_positions: usize,
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
        || account.capital.amount().is_zero()
        || account.peak_capital.amount() < account.capital.amount()
        || account.positions.len() > maximum_positions
    {
        return Err(AccountCoordinatorError::InvalidBootstrap);
    }
    let unique: BTreeSet<_> = account.positions.iter().map(|(id, _)| *id).collect();
    if unique.len() != account.positions.len() {
        return Err(AccountCoordinatorError::InvalidBootstrap);
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
