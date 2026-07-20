//! Checked single-writer cash, position, reservation, and fill accounting.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use market_squawk_domain::{
    AccountId, Currency, InstrumentExecutionTerms, InstrumentId, Money, OrderId, OrderSide,
    PriceTicks, QuantityLots,
};
use rust_decimal::Decimal;
use thiserror::Error;

use crate::{FeeError, FeeSchedule, LiquidityRole};

#[path = "ledger/account_state.rs"]
mod account_state;
use account_state::PaperAccountRiskState;
pub use account_state::{PaperAccountBootstrap, PaperAccountRiskSnapshot};
#[path = "ledger/recovery.rs"]
mod recovery;
pub(crate) use recovery::LedgerRecoveryWire;

/// Fixed paper-ledger policy and hard ownership bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaperLedgerConfig {
    pub allow_short: bool,
    pub maximum_accounts: usize,
    pub maximum_balances: usize,
    pub maximum_positions: usize,
    pub maximum_reservations: usize,
    pub fee_schedule: FeeSchedule,
}

/// One exact settled cash balance in a checkpoint/snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaperCashBalance {
    account_id: AccountId,
    balance: Money,
}

impl PaperCashBalance {
    pub const fn account_id(self) -> AccountId {
        self.account_id
    }
    pub const fn balance(self) -> Money {
        self.balance
    }
}

/// One signed settled instrument position in a checkpoint/snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaperPosition {
    account_id: AccountId,
    instrument_id: InstrumentId,
    lots: i64,
}

impl PaperPosition {
    pub const fn account_id(self) -> AccountId {
        self.account_id
    }
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }
    pub const fn lots(self) -> i64 {
        self.lots
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Reservation {
    account_id: AccountId,
    terms: InstrumentExecutionTerms,
    side: OrderSide,
    remaining: QuantityLots,
    reserved_cash: Money,
    reserved_position_lots: i64,
}

/// Exact result of one atomic fill application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaperFill {
    quantity: QuantityLots,
    average_price: PriceTicks,
    notional: Money,
    fee: Money,
    liquidity: LiquidityRole,
}

impl PaperFill {
    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }
    pub const fn average_price(self) -> PriceTicks {
        self.average_price
    }
    pub const fn notional(self) -> Money {
        self.notional
    }
    pub const fn fee(self) -> Money {
        self.fee
    }
    pub const fn liquidity(self) -> LiquidityRole {
        self.liquidity
    }
}

/// Bounded exact paper account state. All mutation is transactional.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperLedger {
    config: PaperLedgerConfig,
    accounts: BTreeMap<AccountId, PaperAccountRiskState>,
    cash: BTreeMap<(AccountId, Currency), Decimal>,
    positions: BTreeMap<(AccountId, InstrumentId), i64>,
    reservations: BTreeMap<OrderId, Reservation>,
}

impl PaperLedger {
    /// Validates and owns an initial bounded account image.
    pub fn try_new(
        config: PaperLedgerConfig,
        accounts: impl IntoIterator<Item = PaperAccountBootstrap>,
    ) -> Result<Self, PaperLedgerError> {
        if config.maximum_accounts == 0
            || config.maximum_balances == 0
            || config.maximum_positions == 0
            || config.maximum_reservations == 0
        {
            return Err(PaperLedgerError::InvalidConfiguration);
        }
        let mut cash = BTreeMap::new();
        let mut positions = BTreeMap::new();
        let mut account_states = BTreeMap::new();
        let mut account_count = 0_usize;
        for account in accounts {
            account_count = account_count
                .checked_add(1)
                .ok_or(PaperLedgerError::Capacity)?;
            if account_count > config.maximum_accounts {
                return Err(PaperLedgerError::Capacity);
            }
            let money = [
                account.capital,
                account.peak_capital,
                account.gross_exposure,
                account.realized_loss,
            ];
            let currency = account.capital.currency();
            if account.cash.is_empty()
                || money
                    .iter()
                    .any(|value| value.currency() != currency || value.amount().is_sign_negative())
                || account.capital.amount().is_zero()
                || account.peak_capital.amount() < account.capital.amount()
                || !account
                    .cash
                    .iter()
                    .any(|balance| balance.currency() == currency)
                || account_states
                    .insert(
                        account.account_id,
                        PaperAccountRiskState {
                            revision: account.revision,
                            eligible: account.eligible,
                            currency,
                            capital: account.capital,
                            peak_capital: account.peak_capital,
                            gross_exposure: account.gross_exposure,
                            realized_loss: account.realized_loss,
                        },
                    )
                    .is_some()
            {
                return Err(PaperLedgerError::InvalidBootstrap);
            }
            for balance in account.cash {
                if cash.len() >= config.maximum_balances {
                    return Err(PaperLedgerError::Capacity);
                }
                if balance.amount().is_sign_negative()
                    || cash
                        .insert((account.account_id, balance.currency()), balance.amount())
                        .is_some()
                {
                    return Err(PaperLedgerError::InvalidBootstrap);
                }
            }
            for (instrument, lots) in account.positions {
                if positions.len() >= config.maximum_positions {
                    return Err(PaperLedgerError::Capacity);
                }
                if (!config.allow_short && lots < 0)
                    || positions
                        .insert((account.account_id, instrument), lots)
                        .is_some()
                {
                    return Err(PaperLedgerError::InvalidBootstrap);
                }
            }
        }
        if account_count == 0 {
            return Err(PaperLedgerError::InvalidBootstrap);
        }
        Ok(Self {
            config,
            accounts: account_states,
            cash,
            positions,
            reservations: BTreeMap::new(),
        })
    }

    /// Returns settled cash.
    pub fn cash(&self, account: AccountId, currency: Currency) -> Result<Money, PaperLedgerError> {
        self.cash
            .get(&(account, currency))
            .copied()
            .map(|amount| Money::new(amount, currency))
            .ok_or(PaperLedgerError::UnknownAccountOrCurrency)
    }

    /// Returns settled cash less outstanding buy reservations.
    pub fn available_cash(
        &self,
        account: AccountId,
        currency: Currency,
    ) -> Result<Money, PaperLedgerError> {
        let settled = self.cash(account, currency)?;
        let reserved = self
            .reservations
            .values()
            .filter(|reservation| {
                reservation.account_id == account
                    && reservation.reserved_cash.currency() == currency
            })
            .try_fold(Decimal::ZERO, |sum, reservation| {
                sum.checked_add(reservation.reserved_cash.amount())
                    .ok_or(PaperLedgerError::Overflow)
            })?;
        let available = settled
            .amount()
            .checked_sub(reserved)
            .ok_or(PaperLedgerError::Overflow)?;
        Ok(Money::new(available, currency))
    }

    /// Returns the signed settled position.
    pub fn position_lots(
        &self,
        account: AccountId,
        instrument: InstrumentId,
    ) -> Result<i64, PaperLedgerError> {
        if !self.cash.keys().any(|(candidate, _)| *candidate == account) {
            return Err(PaperLedgerError::UnknownAccountOrCurrency);
        }
        Ok(self
            .positions
            .get(&(account, instrument))
            .copied()
            .unwrap_or(0))
    }

    pub(crate) fn cash_snapshot(&self) -> Vec<PaperCashBalance> {
        self.cash
            .iter()
            .map(|((account_id, currency), amount)| PaperCashBalance {
                account_id: *account_id,
                balance: Money::new(*amount, *currency),
            })
            .collect()
    }

    pub(crate) fn position_snapshot(&self) -> Vec<PaperPosition> {
        self.positions
            .iter()
            .map(|((account_id, instrument_id), lots)| PaperPosition {
                account_id: *account_id,
                instrument_id: *instrument_id,
                lots: *lots,
            })
            .collect()
    }

    /// Reserves worst-case resources before an order is accepted.
    pub fn reserve(
        &mut self,
        order_id: OrderId,
        account_id: AccountId,
        terms: InstrumentExecutionTerms,
        side: OrderSide,
        quantity: QuantityLots,
        reservation_price: PriceTicks,
    ) -> Result<(), PaperLedgerError> {
        validate_terms(terms, self.config.fee_schedule.currency())?;
        if quantity.get() == 0 || reservation_price.get() <= 0 {
            return Err(PaperLedgerError::InvalidQuantityOrPrice);
        }
        self.cash(account_id, terms.quote_currency())?;
        if !self
            .positions
            .contains_key(&(account_id, terms.instrument_id()))
            && self.positions.len() >= self.config.maximum_positions
        {
            return Err(PaperLedgerError::Capacity);
        }
        if self.reservations.contains_key(&order_id) {
            return Err(PaperLedgerError::DuplicateOrder);
        }
        if self.reservations.len() >= self.config.maximum_reservations {
            return Err(PaperLedgerError::Capacity);
        }
        let zero = Money::new(Decimal::ZERO, terms.quote_currency());
        let (reserved_cash, reserved_position_lots) = match side {
            OrderSide::Buy => {
                let notional = checked_notional(terms, reservation_price, quantity)?;
                let fee = self
                    .config
                    .fee_schedule
                    .charge(notional, LiquidityRole::Taker)?;
                let required = notional
                    .checked_add(fee)
                    .map_err(|_| PaperLedgerError::Overflow)?;
                if self
                    .available_cash(account_id, required.currency())?
                    .amount()
                    < required.amount()
                {
                    return Err(PaperLedgerError::InsufficientCash);
                }
                (required, 0)
            }
            OrderSide::Sell => {
                let already_reserved = self
                    .reservations
                    .values()
                    .filter(|reservation| {
                        reservation.account_id == account_id
                            && reservation.terms.instrument_id() == terms.instrument_id()
                            && reservation.side == OrderSide::Sell
                    })
                    .try_fold(0_i64, |sum, reservation| {
                        sum.checked_add(reservation.reserved_position_lots)
                            .ok_or(PaperLedgerError::Overflow)
                    })?;
                let available = self
                    .position_lots(account_id, terms.instrument_id())?
                    .checked_sub(already_reserved)
                    .ok_or(PaperLedgerError::Overflow)?;
                if !self.config.allow_short && available < quantity.get() {
                    return Err(PaperLedgerError::InsufficientPosition);
                }
                (zero, quantity.get())
            }
        };
        self.reservations.insert(
            order_id,
            Reservation {
                account_id,
                terms,
                side,
                remaining: quantity,
                reserved_cash,
                reserved_position_lots,
            },
        );
        Ok(())
    }

    /// Applies one multi-level fill atomically and charges one fee over its exact total notional.
    pub fn apply_fill(
        &mut self,
        order_id: OrderId,
        terms: InstrumentExecutionTerms,
        legs: &[(PriceTicks, QuantityLots)],
        liquidity: LiquidityRole,
    ) -> Result<PaperFill, PaperLedgerError> {
        let reservation = self
            .reservations
            .get(&order_id)
            .cloned()
            .ok_or(PaperLedgerError::UnknownOrder)?;
        if reservation.terms != terms {
            return Err(PaperLedgerError::TermsMismatch);
        }
        let (quantity, notional, weighted_ticks) = aggregate_legs(terms, legs)?;
        if quantity > reservation.remaining {
            return Err(PaperLedgerError::Overfill);
        }
        let fee = self.config.fee_schedule.charge(notional, liquidity)?;
        let average_price = adverse_average(weighted_ticks, quantity, reservation.side)?;
        let currency = terms.quote_currency();
        let current_cash = self.cash(reservation.account_id, currency)?.amount();
        let current_position = self.position_lots(reservation.account_id, terms.instrument_id())?;
        let current_account = self
            .accounts
            .get(&reservation.account_id)
            .copied()
            .ok_or(PaperLedgerError::UnknownAccountOrCurrency)?;
        if current_account.currency != currency {
            return Err(PaperLedgerError::FeeCurrencyMismatch);
        }
        let (next_cash, next_position, consumed_cash) = match reservation.side {
            OrderSide::Buy => {
                let debit = notional
                    .checked_add(fee)
                    .map_err(|_| PaperLedgerError::Overflow)?;
                if debit.amount() > reservation.reserved_cash.amount() {
                    return Err(PaperLedgerError::ReservationExceeded);
                }
                let cash = current_cash
                    .checked_sub(debit.amount())
                    .ok_or(PaperLedgerError::Overflow)?;
                let position = current_position
                    .checked_add(quantity.get())
                    .ok_or(PaperLedgerError::Overflow)?;
                (cash, position, debit.amount())
            }
            OrderSide::Sell => {
                let proceeds = notional
                    .checked_sub(fee)
                    .map_err(|_| PaperLedgerError::Overflow)?;
                if proceeds.amount().is_sign_negative() {
                    return Err(PaperLedgerError::FeeExceedsNotional);
                }
                let position = current_position
                    .checked_sub(quantity.get())
                    .ok_or(PaperLedgerError::Overflow)?;
                if !self.config.allow_short && position < 0 {
                    return Err(PaperLedgerError::InsufficientPosition);
                }
                let cash = current_cash
                    .checked_add(proceeds.amount())
                    .ok_or(PaperLedgerError::Overflow)?;
                (cash, position, Decimal::ZERO)
            }
        };
        let next_remaining = reservation
            .remaining
            .checked_sub(quantity)
            .map_err(|_| PaperLedgerError::Overfill)?;
        let next_reserved_cash = reservation
            .reserved_cash
            .amount()
            .checked_sub(consumed_cash)
            .ok_or(PaperLedgerError::ReservationExceeded)?;
        let next_revision = current_account
            .revision
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(PaperLedgerError::Overflow)?;
        let next_capital = current_account
            .capital
            .checked_sub(fee)
            .map_err(|_| PaperLedgerError::Overflow)?;
        if next_capital.amount().is_zero() || next_capital.amount().is_sign_negative() {
            return Err(PaperLedgerError::InsufficientCapital);
        }
        let next_realized_loss = current_account
            .realized_loss
            .checked_add(fee)
            .map_err(|_| PaperLedgerError::Overflow)?;
        let next_gross_exposure = match reservation.side {
            OrderSide::Buy => current_account
                .gross_exposure
                .checked_add(notional)
                .map_err(|_| PaperLedgerError::Overflow)?,
            OrderSide::Sell => current_account
                .gross_exposure
                .checked_sub(notional)
                .map_err(|_| PaperLedgerError::Overflow)?,
        };
        if next_gross_exposure.amount().is_sign_negative() {
            return Err(PaperLedgerError::ExposureUnderflow);
        }
        let next_account = PaperAccountRiskState {
            revision: next_revision,
            capital: next_capital,
            gross_exposure: next_gross_exposure,
            realized_loss: next_realized_loss,
            ..current_account
        };

        self.cash
            .insert((reservation.account_id, currency), next_cash);
        self.positions.insert(
            (reservation.account_id, terms.instrument_id()),
            next_position,
        );
        self.accounts.insert(reservation.account_id, next_account);
        if next_remaining.get() == 0 {
            self.reservations.remove(&order_id);
        } else if let Some(stored) = self.reservations.get_mut(&order_id) {
            stored.remaining = next_remaining;
            stored.reserved_cash = Money::new(next_reserved_cash, currency);
            stored.reserved_position_lots = match stored.side {
                OrderSide::Buy => 0,
                OrderSide::Sell => next_remaining.get(),
            };
        }
        Ok(PaperFill {
            quantity,
            average_price,
            notional,
            fee,
            liquidity,
        })
    }

    /// Releases any unconsumed reservation after cancellation, rejection, or expiry.
    pub fn release(&mut self, order_id: OrderId) -> Result<(), PaperLedgerError> {
        self.reservations
            .remove(&order_id)
            .map(|_| ())
            .ok_or(PaperLedgerError::UnknownOrder)
    }
}

fn validate_terms(
    terms: InstrumentExecutionTerms,
    fee_currency: Currency,
) -> Result<(), PaperLedgerError> {
    if terms.settlement_currency() != Some(terms.quote_currency()) {
        return Err(PaperLedgerError::UnsupportedSettlement);
    }
    if terms.quote_currency() != fee_currency {
        return Err(PaperLedgerError::FeeCurrencyMismatch);
    }
    Ok(())
}

fn checked_notional(
    terms: InstrumentExecutionTerms,
    price: PriceTicks,
    quantity: QuantityLots,
) -> Result<Money, PaperLedgerError> {
    if price.get() <= 0 || quantity.get() == 0 {
        return Err(PaperLedgerError::InvalidQuantityOrPrice);
    }
    let base = price
        .checked_mul_quantity(
            quantity,
            terms.price_tick(),
            terms.lot_size(),
            terms.quote_currency(),
        )
        .map_err(|_| PaperLedgerError::Overflow)?;
    let amount = base
        .amount()
        .checked_mul(terms.contract_multiplier())
        .ok_or(PaperLedgerError::Overflow)?;
    Ok(Money::new(amount, base.currency()))
}

fn aggregate_legs(
    terms: InstrumentExecutionTerms,
    legs: &[(PriceTicks, QuantityLots)],
) -> Result<(QuantityLots, Money, i128), PaperLedgerError> {
    if legs.is_empty() {
        return Err(PaperLedgerError::InvalidQuantityOrPrice);
    }
    let mut quantity = QuantityLots::new(0).map_err(|_| PaperLedgerError::Overflow)?;
    let mut notional = Money::new(Decimal::ZERO, terms.quote_currency());
    let mut weighted_ticks = 0_i128;
    for (price, leg_quantity) in legs {
        if price.get() <= 0 || leg_quantity.get() == 0 {
            return Err(PaperLedgerError::InvalidQuantityOrPrice);
        }
        quantity = quantity
            .checked_add(*leg_quantity)
            .map_err(|_| PaperLedgerError::Overflow)?;
        notional = notional
            .checked_add(checked_notional(terms, *price, *leg_quantity)?)
            .map_err(|_| PaperLedgerError::Overflow)?;
        weighted_ticks = weighted_ticks
            .checked_add(
                i128::from(price.get())
                    .checked_mul(i128::from(leg_quantity.get()))
                    .ok_or(PaperLedgerError::Overflow)?,
            )
            .ok_or(PaperLedgerError::Overflow)?;
    }
    Ok((quantity, notional, weighted_ticks))
}

fn adverse_average(
    weighted_ticks: i128,
    quantity: QuantityLots,
    side: OrderSide,
) -> Result<PriceTicks, PaperLedgerError> {
    let divisor = i128::from(quantity.get());
    let quotient = weighted_ticks
        .checked_div(divisor)
        .ok_or(PaperLedgerError::Overflow)?;
    let remainder = weighted_ticks
        .checked_rem(divisor)
        .ok_or(PaperLedgerError::Overflow)?;
    let rounded = if side == OrderSide::Buy && remainder != 0 {
        quotient.checked_add(1).ok_or(PaperLedgerError::Overflow)?
    } else {
        quotient
    };
    i64::try_from(rounded)
        .map(PriceTicks::new)
        .map_err(|_| PaperLedgerError::Overflow)
}

/// Transactional paper-ledger failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PaperLedgerError {
    #[error("paper ledger configuration is invalid")]
    InvalidConfiguration,
    #[error("paper account bootstrap is invalid")]
    InvalidBootstrap,
    #[error("paper ledger bounded capacity is exhausted")]
    Capacity,
    #[error("paper account or currency is unknown")]
    UnknownAccountOrCurrency,
    #[error("paper order is unknown")]
    UnknownOrder,
    #[error("paper order identity already exists")]
    DuplicateOrder,
    #[error("paper quantity or price is invalid")]
    InvalidQuantityOrPrice,
    #[error("paper account has insufficient available cash")]
    InsufficientCash,
    #[error("paper account has insufficient available position")]
    InsufficientPosition,
    #[error("paper account capital cannot absorb the assessed fee")]
    InsufficientCapital,
    #[error("paper account gross exposure would become negative")]
    ExposureUnderflow,
    #[error("paper execution terms do not match the reserved order")]
    TermsMismatch,
    #[error("paper execution requires currency settlement equal to quote currency")]
    UnsupportedSettlement,
    #[error("paper fee currency does not match instrument quote currency")]
    FeeCurrencyMismatch,
    #[error("paper fill exceeds remaining quantity")]
    Overfill,
    #[error("paper fill exceeds its admitted cash reservation")]
    ReservationExceeded,
    #[error("paper fee exceeds sell notional")]
    FeeExceedsNotional,
    #[error("paper accounting arithmetic overflowed")]
    Overflow,
    #[error("paper recovery ledger violates accounting invariants")]
    InvalidRecovery,
    #[error(transparent)]
    Fee(#[from] FeeError),
}
