//! Strict wire conversion and invariant revalidation for paper ledger recovery.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use market_squawk_domain::{
    AccountId, Currency, InstrumentExecutionTerms, InstrumentId, Money, OrderId, OrderSide,
    QuantityLots,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{
    PaperAccountBootstrap, PaperLedger, PaperLedgerConfig, PaperLedgerError, Reservation,
    expected_reserved_cash, validate_terms,
};

type AccountRecoveryParts = (
    Vec<Money>,
    Vec<(InstrumentId, i64)>,
    Vec<(InstrumentId, Money)>,
);

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LedgerRecoveryWire {
    accounts: Vec<AccountRiskRecoveryWire>,
    cash: Vec<CashRecoveryWire>,
    positions: Vec<PositionRecoveryWire>,
    reservations: Vec<ReservationRecoveryWire>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AccountRiskRecoveryWire {
    account_id: AccountId,
    revision: NonZeroU64,
    eligible: bool,
    currency: Currency,
    capital: Money,
    peak_capital: Money,
    gross_exposure: Money,
    realized_loss: Money,
    realized_pnl: Money,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CashRecoveryWire {
    account_id: AccountId,
    balance: Money,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PositionRecoveryWire {
    account_id: AccountId,
    instrument_id: InstrumentId,
    lots: i64,
    cost_basis: Money,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReservationRecoveryWire {
    order_id: OrderId,
    account_id: AccountId,
    terms: InstrumentExecutionTerms,
    side: OrderSide,
    original_quantity: QuantityLots,
    reservation_price: market_squawk_domain::PriceTicks,
    remaining: QuantityLots,
    reserved_cash: Money,
    reserved_position_lots: i64,
    cumulative_maker_notional: Money,
    cumulative_taker_notional: Money,
    cumulative_fee: Money,
}

impl PaperLedger {
    pub(crate) fn recovery_wire(&self) -> LedgerRecoveryWire {
        LedgerRecoveryWire {
            accounts: self
                .accounts
                .iter()
                .map(|(account_id, account)| AccountRiskRecoveryWire {
                    account_id: *account_id,
                    revision: account.revision,
                    eligible: account.eligible,
                    currency: account.currency,
                    capital: account.capital,
                    peak_capital: account.peak_capital,
                    gross_exposure: account.gross_exposure,
                    realized_loss: account.realized_loss,
                    realized_pnl: account.realized_pnl,
                })
                .collect(),
            cash: self
                .cash
                .iter()
                .map(|((account_id, currency), amount)| CashRecoveryWire {
                    account_id: *account_id,
                    balance: Money::new(*amount, *currency),
                })
                .collect(),
            positions: self
                .positions
                .iter()
                .map(|((account_id, instrument_id), lots)| PositionRecoveryWire {
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
                .collect(),
            reservations: self
                .reservations
                .iter()
                .map(|(order_id, reservation)| ReservationRecoveryWire {
                    order_id: *order_id,
                    account_id: reservation.account_id,
                    terms: reservation.terms,
                    side: reservation.side,
                    original_quantity: reservation.original_quantity,
                    reservation_price: reservation.reservation_price,
                    remaining: reservation.remaining,
                    reserved_cash: reservation.reserved_cash,
                    reserved_position_lots: reservation.reserved_position_lots,
                    cumulative_maker_notional: reservation.cumulative_maker_notional,
                    cumulative_taker_notional: reservation.cumulative_taker_notional,
                    cumulative_fee: reservation.cumulative_fee,
                })
                .collect(),
        }
    }

    pub(crate) fn try_from_recovery_wire(
        config: PaperLedgerConfig,
        wire: LedgerRecoveryWire,
    ) -> Result<Self, PaperLedgerError> {
        if wire.cash.len() > config.maximum_balances
            || wire.accounts.len() > config.maximum_accounts
            || wire.positions.len() > config.maximum_positions
            || wire.reservations.len() > config.maximum_reservations
        {
            return Err(PaperLedgerError::Capacity);
        }
        let mut accounts: BTreeMap<AccountId, AccountRecoveryParts> = BTreeMap::new();
        for entry in wire.cash {
            accounts
                .entry(entry.account_id)
                .or_default()
                .0
                .push(entry.balance);
        }
        for entry in wire.positions {
            let parts = accounts.entry(entry.account_id).or_default();
            parts.1.push((entry.instrument_id, entry.lots));
            parts.2.push((entry.instrument_id, entry.cost_basis));
        }
        let mut bootstraps = Vec::new();
        bootstraps
            .try_reserve_exact(wire.accounts.len())
            .map_err(|_| PaperLedgerError::Capacity)?;
        for account in wire.accounts {
            let (cash, positions, position_cost_basis) = accounts
                .remove(&account.account_id)
                .ok_or(PaperLedgerError::InvalidRecovery)?;
            if account.capital.currency() != account.currency
                || account.peak_capital.currency() != account.currency
                || account.gross_exposure.currency() != account.currency
                || account.realized_loss.currency() != account.currency
                || account.realized_pnl.currency() != account.currency
                || !cash
                    .iter()
                    .any(|balance| balance.currency() == account.currency)
            {
                return Err(PaperLedgerError::InvalidRecovery);
            }
            bootstraps.push(PaperAccountBootstrap {
                account_id: account.account_id,
                revision: account.revision,
                eligible: account.eligible,
                cash,
                capital: account.capital,
                peak_capital: account.peak_capital,
                gross_exposure: account.gross_exposure,
                realized_loss: account.realized_loss,
                realized_pnl: account.realized_pnl,
                positions,
                position_cost_basis,
            });
        }
        if !accounts.is_empty() {
            return Err(PaperLedgerError::InvalidRecovery);
        }
        let mut ledger = Self::try_new(config, bootstraps)?;
        for entry in wire.reservations {
            validate_terms(entry.terms, ledger.config.fee_schedule.currency())?;
            let expected_cash = expected_reserved_cash(
                ledger.config.fee_schedule,
                entry.terms,
                entry.side,
                entry.remaining,
                entry.reservation_price,
                entry.cumulative_maker_notional,
                entry.cumulative_taker_notional,
                entry.cumulative_fee,
            )?;
            let filled_quantity = entry
                .original_quantity
                .get()
                .checked_sub(entry.remaining.get())
                .ok_or(PaperLedgerError::InvalidRecovery)?;
            if entry.remaining.get() == 0
                || entry.original_quantity.get() == 0
                || entry.reservation_price.get() <= 0
                || filled_quantity < 0
                || entry.reserved_cash.currency() != entry.terms.quote_currency()
                || entry.reserved_cash.amount().is_sign_negative()
                || entry.reserved_cash != expected_cash
                || entry.cumulative_maker_notional.currency() != entry.terms.quote_currency()
                || entry.cumulative_maker_notional.amount().is_sign_negative()
                || entry.cumulative_taker_notional.currency() != entry.terms.quote_currency()
                || entry.cumulative_taker_notional.amount().is_sign_negative()
                || entry.cumulative_fee.currency() != entry.terms.quote_currency()
                || entry.cumulative_fee.amount().is_sign_negative()
                || ledger.config.fee_schedule.charge_cumulative(
                    entry.cumulative_maker_notional,
                    entry.cumulative_taker_notional,
                )? != entry.cumulative_fee
                || match entry.side {
                    OrderSide::Buy => entry.reserved_position_lots != 0,
                    OrderSide::Sell => entry.reserved_position_lots != entry.remaining.get(),
                }
                || ledger
                    .reservations
                    .insert(
                        entry.order_id,
                        Reservation {
                            account_id: entry.account_id,
                            terms: entry.terms,
                            side: entry.side,
                            original_quantity: entry.original_quantity,
                            reservation_price: entry.reservation_price,
                            remaining: entry.remaining,
                            reserved_cash: entry.reserved_cash,
                            reserved_position_lots: entry.reserved_position_lots,
                            cumulative_maker_notional: entry.cumulative_maker_notional,
                            cumulative_taker_notional: entry.cumulative_taker_notional,
                            cumulative_fee: entry.cumulative_fee,
                        },
                    )
                    .is_some()
            {
                return Err(PaperLedgerError::InvalidRecovery);
            }
        }
        ledger.validate_recovered_resources()?;
        Ok(ledger)
    }

    fn validate_recovered_resources(&self) -> Result<(), PaperLedgerError> {
        for reservation in self.reservations.values() {
            self.cash(reservation.account_id, reservation.terms.quote_currency())
                .map_err(|_| PaperLedgerError::InvalidRecovery)?;
        }
        for (account_id, currency) in self.cash.keys().copied() {
            if self
                .available_cash(account_id, currency)?
                .amount()
                .is_sign_negative()
            {
                return Err(PaperLedgerError::InvalidRecovery);
            }
        }
        if self.config.allow_short {
            return Ok(());
        }
        for ((account_id, instrument_id), position) in &self.positions {
            let reserved = self
                .reservations
                .values()
                .filter(|reservation| {
                    reservation.account_id == *account_id
                        && reservation.terms.instrument_id() == *instrument_id
                        && reservation.side == OrderSide::Sell
                })
                .try_fold(0_i64, |sum, reservation| {
                    sum.checked_add(reservation.reserved_position_lots)
                        .ok_or(PaperLedgerError::Overflow)
                })?;
            if reserved > *position {
                return Err(PaperLedgerError::InvalidRecovery);
            }
        }
        Ok(())
    }

    pub(crate) fn has_reservation(&self, order_id: OrderId) -> bool {
        self.reservations.contains_key(&order_id)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "checkpoint validation binds every independent order and fee dimension exactly"
    )]
    pub(crate) fn reservation_matches(
        &self,
        order_id: OrderId,
        account_id: AccountId,
        terms: InstrumentExecutionTerms,
        side: OrderSide,
        original_quantity: QuantityLots,
        reservation_price: market_squawk_domain::PriceTicks,
        remaining: QuantityLots,
        cumulative_maker_notional: Money,
        cumulative_taker_notional: Money,
        cumulative_fee: Money,
    ) -> bool {
        self.reservations.get(&order_id).is_some_and(|reservation| {
            reservation.account_id == account_id
                && reservation.terms == terms
                && reservation.side == side
                && reservation.original_quantity == original_quantity
                && reservation.reservation_price == reservation_price
                && reservation.remaining == remaining
                && reservation.cumulative_maker_notional == cumulative_maker_notional
                && reservation.cumulative_taker_notional == cumulative_taker_notional
                && reservation.cumulative_fee == cumulative_fee
        })
    }

    pub(crate) fn reservation_count(&self) -> usize {
        self.reservations.len()
    }
}
