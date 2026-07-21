//! Checked single-writer cash, position, reservation, and fill accounting.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use market_squawk_domain::{
    AccountId, Currency, InstrumentExecutionTerms, InstrumentId, Money, OrderId, OrderSide,
    PriceTicks, QuantityLots,
};
use rust_decimal::{Decimal, RoundingStrategy};
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
    pub exposure_valuation: PaperExposureValuation,
    pub maximum_accounts: usize,
    pub maximum_balances: usize,
    pub maximum_positions: usize,
    pub maximum_reservations: usize,
    pub fee_schedule: FeeSchedule,
}

/// Fixed valuation policy used for paper risk exposure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PaperExposureValuation {
    /// Values each signed open position at its exact retained open cost basis.
    OpenCost,
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
    cost_basis: Money,
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

    pub const fn cost_basis(self) -> Money {
        self.cost_basis
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Reservation {
    account_id: AccountId,
    terms: InstrumentExecutionTerms,
    side: OrderSide,
    original_quantity: QuantityLots,
    reservation_price: PriceTicks,
    remaining: QuantityLots,
    reserved_cash: Money,
    reserved_position_lots: i64,
    cumulative_maker_notional: Money,
    cumulative_taker_notional: Money,
    cumulative_fee: Money,
}

/// Exact result of one atomic fill application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaperFill {
    quantity: QuantityLots,
    average_price: PriceTicks,
    maximum_price: PriceTicks,
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
    pub const fn maximum_price(self) -> PriceTicks {
        self.maximum_price
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
    position_cost_basis: BTreeMap<(AccountId, InstrumentId), Decimal>,
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
        match config.exposure_valuation {
            PaperExposureValuation::OpenCost => {}
        }
        let mut cash = BTreeMap::new();
        let mut positions = BTreeMap::new();
        let mut position_cost_basis = BTreeMap::new();
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
                || account.realized_pnl.currency() != currency
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
                            realized_pnl: account.realized_pnl,
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
            for (instrument, basis) in account.position_cost_basis {
                if basis.currency() != currency
                    || basis.amount().is_sign_negative()
                    || position_cost_basis
                        .insert((account.account_id, instrument), basis.amount())
                        .is_some()
                {
                    return Err(PaperLedgerError::InvalidBootstrap);
                }
            }
        }
        if account_count == 0 {
            return Err(PaperLedgerError::InvalidBootstrap);
        }
        if positions.len() != position_cost_basis.len()
            || positions.iter().any(|(key, lots)| {
                let Some(basis) = position_cost_basis.get(key) else {
                    return true;
                };
                (*lots == 0 && !basis.is_zero()) || (*lots != 0 && basis.is_zero())
            })
        {
            return Err(PaperLedgerError::InvalidBootstrap);
        }
        for (account_id, account) in &account_states {
            let basis = position_cost_basis
                .iter()
                .filter(|((candidate, _), _)| candidate == account_id)
                .try_fold(Decimal::ZERO, |sum, (_, basis)| {
                    sum.checked_add(*basis).ok_or(PaperLedgerError::Overflow)
                })?;
            if basis != account.gross_exposure.amount() {
                return Err(PaperLedgerError::InvalidBootstrap);
            }
        }
        Ok(Self {
            config,
            accounts: account_states,
            cash,
            positions,
            position_cost_basis,
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

    /// Returns the nonnegative open-cost basis for one signed position.
    pub fn position_cost_basis(
        &self,
        account: AccountId,
        instrument: InstrumentId,
    ) -> Result<Money, PaperLedgerError> {
        let currency = self
            .accounts
            .get(&account)
            .map(|state| state.currency)
            .ok_or(PaperLedgerError::UnknownAccountOrCurrency)?;
        Ok(Money::new(
            self.position_cost_basis
                .get(&(account, instrument))
                .copied()
                .unwrap_or(Decimal::ZERO),
            currency,
        ))
    }

    /// Returns the exact current account-risk dimensions used for reconciliation.
    pub fn account_risk(
        &self,
        account_id: AccountId,
    ) -> Result<PaperAccountRiskSnapshot, PaperLedgerError> {
        let account = self
            .accounts
            .get(&account_id)
            .copied()
            .ok_or(PaperLedgerError::UnknownAccountOrCurrency)?;
        Ok(PaperAccountRiskSnapshot::new(account_id, account))
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
                cost_basis: Money::new(
                    self.position_cost_basis
                        .get(&(*account_id, *instrument_id))
                        .copied()
                        .unwrap_or(Decimal::ZERO),
                    self.accounts
                        .get(account_id)
                        .map_or(self.config.fee_schedule.currency(), |account| {
                            account.currency
                        }),
                ),
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
                let required = expected_reserved_cash(
                    self.config.fee_schedule,
                    terms,
                    side,
                    quantity,
                    reservation_price,
                    zero,
                    zero,
                    zero,
                )?;
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
                let cash_shortfall = expected_reserved_cash(
                    self.config.fee_schedule,
                    terms,
                    side,
                    quantity,
                    reservation_price,
                    zero,
                    zero,
                    zero,
                )?;
                if self
                    .available_cash(account_id, terms.quote_currency())?
                    .amount()
                    < cash_shortfall.amount()
                {
                    return Err(PaperLedgerError::InsufficientCash);
                }
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
                (cash_shortfall, quantity.get())
            }
        };
        self.reservations.insert(
            order_id,
            Reservation {
                account_id,
                terms,
                side,
                original_quantity: quantity,
                reservation_price,
                remaining: quantity,
                reserved_cash,
                reserved_position_lots,
                cumulative_maker_notional: zero,
                cumulative_taker_notional: zero,
                cumulative_fee: zero,
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
        let (quantity, notional, weighted_ticks, maximum_price) = aggregate_legs(terms, legs)?;
        if quantity > reservation.remaining {
            return Err(PaperLedgerError::Overfill);
        }
        let (next_maker_notional, next_taker_notional) = match liquidity {
            LiquidityRole::Maker => (
                reservation
                    .cumulative_maker_notional
                    .checked_add(notional)
                    .map_err(|_| PaperLedgerError::Overflow)?,
                reservation.cumulative_taker_notional,
            ),
            LiquidityRole::Taker => (
                reservation.cumulative_maker_notional,
                reservation
                    .cumulative_taker_notional
                    .checked_add(notional)
                    .map_err(|_| PaperLedgerError::Overflow)?,
            ),
        };
        let cumulative_fee = self
            .config
            .fee_schedule
            .charge_cumulative(next_maker_notional, next_taker_notional)?;
        let fee = cumulative_fee
            .checked_sub(reservation.cumulative_fee)
            .map_err(|_| PaperLedgerError::Overflow)?;
        if fee.amount().is_sign_negative() {
            return Err(PaperLedgerError::Overflow);
        }
        let average_price = adverse_average(weighted_ticks, quantity, reservation.side)?;
        let currency = terms.quote_currency();
        let current_cash = self.cash(reservation.account_id, currency)?.amount();
        let current_position = self.position_lots(reservation.account_id, terms.instrument_id())?;
        let current_cost_basis =
            self.position_cost_basis(reservation.account_id, terms.instrument_id())?;
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
                let position = current_position
                    .checked_sub(quantity.get())
                    .ok_or(PaperLedgerError::Overflow)?;
                if !self.config.allow_short && position < 0 {
                    return Err(PaperLedgerError::InsufficientPosition);
                }
                let cash = current_cash
                    .checked_add(proceeds.amount())
                    .ok_or(PaperLedgerError::Overflow)?;
                if cash.is_sign_negative() {
                    return Err(PaperLedgerError::InsufficientCash);
                }
                let consumed = (-proceeds.amount()).max(Decimal::ZERO);
                if consumed > reservation.reserved_cash.amount() {
                    return Err(PaperLedgerError::ReservationExceeded);
                }
                (cash, position, consumed)
            }
        };
        let next_remaining = reservation
            .remaining
            .checked_sub(quantity)
            .map_err(|_| PaperLedgerError::Overfill)?;
        let next_reserved_cash = if next_remaining.get() == 0 {
            Decimal::ZERO
        } else {
            expected_reserved_cash(
                self.config.fee_schedule,
                reservation.terms,
                reservation.side,
                next_remaining,
                reservation.reservation_price,
                next_maker_notional,
                next_taker_notional,
                cumulative_fee,
            )?
            .amount()
        };
        let maximum_remaining_cash = reservation
            .reserved_cash
            .amount()
            .checked_sub(consumed_cash)
            .ok_or(PaperLedgerError::ReservationExceeded)?;
        if next_reserved_cash > maximum_remaining_cash {
            return Err(PaperLedgerError::ReservationExceeded);
        }
        let next_revision = current_account
            .revision
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(PaperLedgerError::Overflow)?;
        let position_transition = transition_position(
            current_position,
            current_cost_basis,
            reservation.side,
            quantity,
            notional,
            self.config.fee_schedule.money_scale(),
        )?;
        if position_transition.next_lots != next_position {
            return Err(PaperLedgerError::Overflow);
        }
        let next_capital = current_account
            .capital
            .checked_add(position_transition.realized_pnl)
            .and_then(|capital| capital.checked_sub(fee))
            .map_err(|_| PaperLedgerError::Overflow)?;
        if next_capital.amount().is_zero() || next_capital.amount().is_sign_negative() {
            return Err(PaperLedgerError::InsufficientCapital);
        }
        let realized_loss_delta = Money::new(
            (-position_transition.realized_pnl.amount()).max(Decimal::ZERO),
            currency,
        )
        .checked_add(fee)
        .map_err(|_| PaperLedgerError::Overflow)?;
        let next_realized_loss = current_account
            .realized_loss
            .checked_add(realized_loss_delta)
            .map_err(|_| PaperLedgerError::Overflow)?;
        let next_realized_pnl = current_account
            .realized_pnl
            .checked_add(position_transition.realized_pnl)
            .map_err(|_| PaperLedgerError::Overflow)?;
        let next_gross_exposure = current_account
            .gross_exposure
            .checked_sub(current_cost_basis)
            .and_then(|exposure| exposure.checked_add(position_transition.next_cost_basis))
            .map_err(|_| PaperLedgerError::Overflow)?;
        let next_peak_capital = if next_capital.amount() > current_account.peak_capital.amount() {
            next_capital
        } else {
            current_account.peak_capital
        };
        let next_account = PaperAccountRiskState {
            revision: next_revision,
            capital: next_capital,
            peak_capital: next_peak_capital,
            gross_exposure: next_gross_exposure,
            realized_loss: next_realized_loss,
            realized_pnl: next_realized_pnl,
            ..current_account
        };

        self.cash
            .insert((reservation.account_id, currency), next_cash);
        self.positions.insert(
            (reservation.account_id, terms.instrument_id()),
            next_position,
        );
        self.position_cost_basis.insert(
            (reservation.account_id, terms.instrument_id()),
            position_transition.next_cost_basis.amount(),
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
            stored.cumulative_maker_notional = next_maker_notional;
            stored.cumulative_taker_notional = next_taker_notional;
            stored.cumulative_fee = cumulative_fee;
        }
        Ok(PaperFill {
            quantity,
            average_price,
            maximum_price,
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

#[derive(Clone, Copy, Debug)]
struct PositionTransition {
    next_lots: i64,
    next_cost_basis: Money,
    realized_pnl: Money,
}

fn transition_position(
    current_position: i64,
    current_cost_basis: Money,
    side: OrderSide,
    quantity: QuantityLots,
    fill_notional: Money,
    money_scale: u32,
) -> Result<PositionTransition, PaperLedgerError> {
    let signed_fill = match side {
        OrderSide::Buy => quantity.get(),
        OrderSide::Sell => quantity
            .get()
            .checked_neg()
            .ok_or(PaperLedgerError::Overflow)?,
    };
    let next_position = current_position
        .checked_add(signed_fill)
        .ok_or(PaperLedgerError::Overflow)?;
    let current_abs = current_position.unsigned_abs();
    let fill_quantity = quantity.get().unsigned_abs();
    let closing_lots = if current_position == 0 || current_position.signum() == signed_fill.signum()
    {
        0
    } else {
        current_abs.min(fill_quantity)
    };
    let closing_notional = if closing_lots == fill_quantity {
        fill_notional.amount()
    } else {
        proportional_amount(
            fill_notional.amount(),
            closing_lots,
            fill_quantity,
            money_scale,
        )?
    };
    let closed_basis = if closing_lots == current_abs {
        current_cost_basis.amount()
    } else {
        proportional_amount(
            current_cost_basis.amount(),
            closing_lots,
            current_abs,
            money_scale,
        )?
    };
    let opening_notional = fill_notional
        .amount()
        .checked_sub(closing_notional)
        .ok_or(PaperLedgerError::Overflow)?;
    let remaining_basis = current_cost_basis
        .amount()
        .checked_sub(closed_basis)
        .ok_or(PaperLedgerError::Overflow)?;
    let next_basis = remaining_basis
        .checked_add(opening_notional)
        .ok_or(PaperLedgerError::Overflow)?;
    let realized = if current_position > 0 {
        closing_notional.checked_sub(closed_basis)
    } else if current_position < 0 {
        closed_basis.checked_sub(closing_notional)
    } else {
        Some(Decimal::ZERO)
    }
    .ok_or(PaperLedgerError::Overflow)?;
    if next_basis.is_sign_negative() || (next_position == 0 && !next_basis.is_zero()) {
        return Err(PaperLedgerError::Overflow);
    }
    Ok(PositionTransition {
        next_lots: next_position,
        next_cost_basis: Money::new(next_basis, current_cost_basis.currency()),
        realized_pnl: Money::new(realized, current_cost_basis.currency()),
    })
}

fn proportional_amount(
    amount: Decimal,
    numerator: u64,
    denominator: u64,
    money_scale: u32,
) -> Result<Decimal, PaperLedgerError> {
    if numerator == 0 {
        return Ok(Decimal::ZERO);
    }
    if denominator == 0 {
        return Err(PaperLedgerError::Overflow);
    }
    amount
        .checked_mul(Decimal::from(numerator))
        .and_then(|value| value.checked_div(Decimal::from(denominator)))
        .map(|value| {
            value.round_dp_with_strategy(money_scale, RoundingStrategy::MidpointNearestEven)
        })
        .ok_or(PaperLedgerError::Overflow)
}

#[expect(
    clippy::too_many_arguments,
    reason = "reservation recovery validates eight independent financial dimensions as one invariant"
)]
fn expected_reserved_cash(
    fee_schedule: FeeSchedule,
    terms: InstrumentExecutionTerms,
    side: OrderSide,
    remaining: QuantityLots,
    reservation_price: PriceTicks,
    cumulative_maker_notional: Money,
    cumulative_taker_notional: Money,
    cumulative_fee: Money,
) -> Result<Money, PaperLedgerError> {
    let remaining_notional = checked_notional(terms, reservation_price, remaining)?;
    let future_fee = match side {
        OrderSide::Buy => {
            let maker_total = cumulative_maker_notional
                .checked_add(remaining_notional)
                .map_err(|_| PaperLedgerError::Overflow)?;
            let taker_total = cumulative_taker_notional
                .checked_add(remaining_notional)
                .map_err(|_| PaperLedgerError::Overflow)?;
            let all_maker =
                fee_schedule.charge_cumulative(maker_total, cumulative_taker_notional)?;
            let all_taker =
                fee_schedule.charge_cumulative(cumulative_maker_notional, taker_total)?;
            if all_maker.amount() >= all_taker.amount() {
                all_maker
            } else {
                all_taker
            }
        }
        OrderSide::Sell => {
            let one_lot =
                QuantityLots::new(1).map_err(|_| PaperLedgerError::InvalidQuantityOrPrice)?;
            // Sell proceeds can deteriorate below the admission price. Reserve against the
            // smallest valid positive execution price so a minimum fee cannot drive cash below
            // zero before cancellation or resynchronization.
            let one_lot_notional = checked_notional(terms, PriceTicks::new(1), one_lot)?;
            let maker_total = cumulative_maker_notional
                .checked_add(one_lot_notional)
                .map_err(|_| PaperLedgerError::Overflow)?;
            let taker_total = cumulative_taker_notional
                .checked_add(one_lot_notional)
                .map_err(|_| PaperLedgerError::Overflow)?;
            let maker_increment = fee_schedule
                .charge_cumulative(maker_total, cumulative_taker_notional)?
                .checked_sub(cumulative_fee)
                .map_err(|_| PaperLedgerError::Overflow)?;
            let taker_increment = fee_schedule
                .charge_cumulative(cumulative_maker_notional, taker_total)?
                .checked_sub(cumulative_fee)
                .map_err(|_| PaperLedgerError::Overflow)?;
            let worst_increment = if maker_increment.amount() >= taker_increment.amount() {
                maker_increment
            } else {
                taker_increment
            };
            let shortfall = worst_increment
                .amount()
                .checked_sub(one_lot_notional.amount())
                .ok_or(PaperLedgerError::Overflow)?
                .max(Decimal::ZERO);
            return Ok(Money::new(shortfall, terms.quote_currency()));
        }
    };
    remaining_notional
        .checked_add(
            future_fee
                .checked_sub(cumulative_fee)
                .map_err(|_| PaperLedgerError::Overflow)?,
        )
        .map_err(|_| PaperLedgerError::Overflow)
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

pub(crate) fn checked_notional(
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
) -> Result<(QuantityLots, Money, i128, PriceTicks), PaperLedgerError> {
    if legs.is_empty() {
        return Err(PaperLedgerError::InvalidQuantityOrPrice);
    }
    let mut quantity = QuantityLots::new(0).map_err(|_| PaperLedgerError::Overflow)?;
    let mut notional = Money::new(Decimal::ZERO, terms.quote_currency());
    let mut weighted_ticks = 0_i128;
    let mut maximum_price = None;
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
        maximum_price =
            Some(maximum_price.map_or(*price, |current: PriceTicks| current.max(*price)));
    }
    let maximum_price = maximum_price.ok_or(PaperLedgerError::InvalidQuantityOrPrice)?;
    Ok((quantity, notional, weighted_ticks, maximum_price))
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
