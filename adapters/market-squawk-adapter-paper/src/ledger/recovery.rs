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
    validate_terms,
};

type AccountRecoveryParts = (Vec<Money>, Vec<(InstrumentId, i64)>);

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
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReservationRecoveryWire {
    order_id: OrderId,
    account_id: AccountId,
    terms: InstrumentExecutionTerms,
    side: OrderSide,
    remaining: QuantityLots,
    reserved_cash: Money,
    reserved_position_lots: i64,
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
                    remaining: reservation.remaining,
                    reserved_cash: reservation.reserved_cash,
                    reserved_position_lots: reservation.reserved_position_lots,
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
            accounts
                .entry(entry.account_id)
                .or_default()
                .1
                .push((entry.instrument_id, entry.lots));
        }
        let mut bootstraps = Vec::new();
        bootstraps
            .try_reserve_exact(wire.accounts.len())
            .map_err(|_| PaperLedgerError::Capacity)?;
        for account in wire.accounts {
            let (cash, positions) = accounts
                .remove(&account.account_id)
                .ok_or(PaperLedgerError::InvalidRecovery)?;
            if !cash
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
                positions,
            });
        }
        if !accounts.is_empty() {
            return Err(PaperLedgerError::InvalidRecovery);
        }
        let mut ledger = Self::try_new(config, bootstraps)?;
        for entry in wire.reservations {
            validate_terms(entry.terms, ledger.config.fee_schedule.currency())?;
            if entry.remaining.get() == 0
                || entry.reserved_cash.currency() != entry.terms.quote_currency()
                || entry.reserved_cash.amount().is_sign_negative()
                || match entry.side {
                    OrderSide::Buy => entry.reserved_position_lots != 0,
                    OrderSide::Sell => {
                        entry.reserved_cash.amount() != Decimal::ZERO
                            || entry.reserved_position_lots != entry.remaining.get()
                    }
                }
                || ledger
                    .reservations
                    .insert(
                        entry.order_id,
                        Reservation {
                            account_id: entry.account_id,
                            terms: entry.terms,
                            side: entry.side,
                            remaining: entry.remaining,
                            reserved_cash: entry.reserved_cash,
                            reserved_position_lots: entry.reserved_position_lots,
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

    pub(crate) fn reservation_count(&self) -> usize {
        self.reservations.len()
    }
}
