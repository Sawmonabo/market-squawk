//! Checked ledger replay and Task 11 corporate-action application.

use std::collections::BTreeMap;

use market_squawk_data::{AdjustmentStep, CorporateActionPlan};
use market_squawk_domain::{Currency, InstrumentId, MergerConsideration, Money, SourceIdentifier};
use rust_decimal::Decimal;

use crate::evidence::ValuationSet;
use crate::lots::{Lot, LotDirection, dispose};
use crate::transaction::{CashFlow, CashFlowKind, LedgerEntry, LedgerEntryKind, Trade, TradeSide};
use crate::{PortfolioError, checked_decimal_add, checked_decimal_div, checked_decimal_mul};

#[derive(Clone, Debug, Default)]
pub(crate) struct CurrencyAmounts(pub(crate) BTreeMap<Currency, Decimal>);

impl CurrencyAmounts {
    fn add(&mut self, money: Money) -> Result<(), PortfolioError> {
        let current = self.0.get(&money.currency()).copied().unwrap_or_default();
        self.0.insert(
            money.currency(),
            checked_decimal_add(current, money.amount())?,
        );
        Ok(())
    }

    fn subtract(&mut self, money: Money) -> Result<(), PortfolioError> {
        self.add(Money::new(-money.amount(), money.currency()))
    }

    pub(crate) fn total(&self, valuation: &ValuationSet) -> Result<Money, PortfolioError> {
        self.0.iter().try_fold(
            Money::new(Decimal::ZERO, valuation.base_currency),
            |total, (currency, amount)| {
                total
                    .checked_add(valuation.convert(Money::new(*amount, *currency))?)
                    .map_err(|_| PortfolioError::Arithmetic)
            },
        )
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ReplayState {
    pub(crate) cash: CurrencyAmounts,
    pub(crate) lots: Vec<Lot>,
    pub(crate) realized_gain: CurrencyAmounts,
    pub(crate) income: CurrencyAmounts,
    pub(crate) withholding: CurrencyAmounts,
    pub(crate) fees: CurrencyAmounts,
    pub(crate) return_of_capital: CurrencyAmounts,
}

impl ReplayState {
    pub(crate) fn apply_entry(&mut self, entry: &LedgerEntry) -> Result<(), PortfolioError> {
        match &entry.kind {
            LedgerEntryKind::Trade(trade) => self.apply_trade(entry, trade),
            LedgerEntryKind::CashFlow(flow) => self.apply_cash_flow(*flow),
        }
    }

    fn apply_trade(&mut self, entry: &LedgerEntry, trade: &Trade) -> Result<(), PortfolioError> {
        let gross = trade
            .price
            .checked_mul_decimal(trade.quantity)
            .map_err(|_| PortfolioError::Arithmetic)?;
        self.fees.add(trade.fee)?;
        match trade.side {
            TradeSide::Buy => {
                let total = gross
                    .checked_add(trade.fee)
                    .map_err(|_| PortfolioError::Arithmetic)?;
                self.cash.subtract(total)?;
                self.lots.push(Lot {
                    id: entry.transaction.transaction_id.clone(),
                    instrument_id: trade.instrument_id,
                    direction: LotDirection::Long,
                    opened_at: entry.occurred_at,
                    quantity: trade.quantity,
                    basis: total,
                    basis_complete: true,
                });
            }
            TradeSide::Sell => {
                let proceeds = gross
                    .checked_sub(trade.fee)
                    .map_err(|_| PortfolioError::Arithmetic)?;
                let disposal = dispose(
                    &mut self.lots,
                    trade.instrument_id,
                    LotDirection::Long,
                    trade.quantity,
                    &trade.lot_selection,
                )?;
                require_complete(disposal.basis_complete)?;
                self.cash.add(proceeds)?;
                self.realized_gain.add(
                    proceeds
                        .checked_sub(disposal.basis)
                        .map_err(|_| PortfolioError::Arithmetic)?,
                )?;
            }
            TradeSide::SellShort => {
                let proceeds = gross
                    .checked_sub(trade.fee)
                    .map_err(|_| PortfolioError::Arithmetic)?;
                self.cash.add(proceeds)?;
                self.lots.push(Lot {
                    id: entry.transaction.transaction_id.clone(),
                    instrument_id: trade.instrument_id,
                    direction: LotDirection::Short,
                    opened_at: entry.occurred_at,
                    quantity: trade.quantity,
                    basis: proceeds,
                    basis_complete: true,
                });
            }
            TradeSide::BuyToCover => {
                let cost = gross
                    .checked_add(trade.fee)
                    .map_err(|_| PortfolioError::Arithmetic)?;
                let disposal = dispose(
                    &mut self.lots,
                    trade.instrument_id,
                    LotDirection::Short,
                    trade.quantity,
                    &trade.lot_selection,
                )?;
                require_complete(disposal.basis_complete)?;
                self.cash.subtract(cost)?;
                self.realized_gain.add(
                    disposal
                        .basis
                        .checked_sub(cost)
                        .map_err(|_| PortfolioError::Arithmetic)?,
                )?;
            }
        }
        Ok(())
    }

    fn apply_cash_flow(&mut self, flow: CashFlow) -> Result<(), PortfolioError> {
        match flow.kind {
            CashFlowKind::Deposit => self.cash.add(flow.amount),
            CashFlowKind::Withdrawal => self.cash.subtract(flow.amount),
            CashFlowKind::Dividend | CashFlowKind::Interest => {
                self.cash.add(flow.amount)?;
                self.income.add(flow.amount)
            }
            CashFlowKind::Withholding => {
                self.cash.subtract(flow.amount)?;
                self.withholding.add(flow.amount)
            }
            CashFlowKind::Fee => {
                self.cash.subtract(flow.amount)?;
                self.fees.add(flow.amount)
            }
        }
    }

    pub(crate) fn validate_plan(plan: &CorporateActionPlan) -> Result<(), PortfolioError> {
        if !plan.conflicts().is_empty() {
            return Err(PortfolioError::UnresolvedCorporateAction);
        }
        Ok(())
    }

    pub(crate) fn apply_step(
        &mut self,
        plan: &CorporateActionPlan,
        step: &AdjustmentStep,
    ) -> Result<(), PortfolioError> {
        let admitted_index = step_index(step);
        let record = plan
            .admitted()
            .get(admitted_index)
            .ok_or(PortfolioError::EvidenceMismatch)?;
        let subject = record
            .observation()
            .context()
            .provenance()
            .instrument_id()
            .ok_or(PortfolioError::EvidenceMismatch)?;
        match step {
            AdjustmentStep::Split {
                quantity_factor, ..
            } => self.apply_split(subject, *quantity_factor)?,
            AdjustmentStep::CashDividend { amount, .. } => {
                let quantity = self.signed_quantity(subject)?;
                let cash = amount
                    .checked_mul_decimal(quantity)
                    .map_err(|_| PortfolioError::Arithmetic)?;
                self.cash.add(cash)?;
                self.income.add(cash)?;
            }
            AdjustmentStep::ReturnOfCapital { amount, .. } => {
                self.apply_return_of_capital(subject, *amount)?;
            }
            AdjustmentStep::Spinoff {
                distributed_instrument,
                distribution_ratio,
                ..
            } => self.apply_spinoff(subject, *distributed_instrument, *distribution_ratio)?,
            AdjustmentStep::Merger {
                successor,
                consideration,
                ..
            } => self.apply_merger(subject, *successor, *consideration)?,
            AdjustmentStep::Delisting { .. } | AdjustmentStep::SymbolChange { .. } => {}
        }
        Ok(())
    }

    fn apply_split(
        &mut self,
        subject: InstrumentId,
        ratio: market_squawk_data::AdjustmentRatio,
    ) -> Result<(), PortfolioError> {
        let factor = checked_decimal_div(
            Decimal::from(ratio.numerator().get()),
            Decimal::from(ratio.denominator().get()),
        )?;
        for lot in self
            .lots
            .iter_mut()
            .filter(|lot| lot.instrument_id == subject)
        {
            lot.quantity = checked_decimal_mul(lot.quantity, factor)?;
        }
        Ok(())
    }

    fn apply_return_of_capital(
        &mut self,
        subject: InstrumentId,
        amount: Money,
    ) -> Result<(), PortfolioError> {
        let affected = self
            .lots
            .iter()
            .enumerate()
            .filter(|(_, lot)| lot.instrument_id == subject && lot.direction == LotDirection::Long)
            .collect::<Vec<_>>();
        if affected
            .iter()
            .any(|(_, lot)| !lot.basis_complete || lot.basis.currency() != amount.currency())
        {
            return Err(PortfolioError::UnresolvedCorporateAction);
        }
        let mut updates = Vec::new();
        updates
            .try_reserve_exact(affected.len())
            .map_err(|_| PortfolioError::AllocationFailed)?;
        let mut total_distribution = Money::new(Decimal::ZERO, amount.currency());
        let mut total_excess = Money::new(Decimal::ZERO, amount.currency());
        for (index, lot) in affected {
            let distribution = amount
                .checked_mul_decimal(lot.quantity)
                .map_err(|_| PortfolioError::Arithmetic)?;
            let reduction = distribution.amount().min(lot.basis.amount());
            let updated_basis = lot
                .basis
                .checked_sub(Money::new(reduction, amount.currency()))
                .map_err(|_| PortfolioError::Arithmetic)?;
            let excess = distribution
                .checked_sub(Money::new(reduction, amount.currency()))
                .map_err(|_| PortfolioError::Arithmetic)?;
            total_distribution = total_distribution
                .checked_add(distribution)
                .map_err(|_| PortfolioError::Arithmetic)?;
            total_excess = total_excess
                .checked_add(excess)
                .map_err(|_| PortfolioError::Arithmetic)?;
            updates.push((index, updated_basis));
        }
        for (index, updated_basis) in updates {
            let lot = self
                .lots
                .get_mut(index)
                .ok_or(PortfolioError::UnresolvedCorporateAction)?;
            lot.basis = updated_basis;
        }
        self.cash.add(total_distribution)?;
        self.return_of_capital.add(total_distribution)?;
        self.realized_gain.add(total_excess)?;
        Ok(())
    }

    fn apply_spinoff(
        &mut self,
        subject: InstrumentId,
        distributed: InstrumentId,
        ratio: market_squawk_data::AdjustmentRatio,
    ) -> Result<(), PortfolioError> {
        let factor = checked_decimal_div(
            Decimal::from(ratio.numerator().get()),
            Decimal::from(ratio.denominator().get()),
        )?;
        let source_lots = self
            .lots
            .iter()
            .filter(|lot| lot.instrument_id == subject)
            .cloned()
            .collect::<Vec<_>>();
        for source in source_lots {
            self.lots.push(Lot {
                id: SourceIdentifier::try_from(format!("spinoff-{}", source.id.as_str()))
                    .map_err(|_| PortfolioError::InvalidTransaction)?,
                instrument_id: distributed,
                direction: source.direction,
                opened_at: source.opened_at,
                quantity: checked_decimal_mul(source.quantity, factor)?,
                basis: Money::new(Decimal::ZERO, source.basis.currency()),
                basis_complete: false,
            });
        }
        Ok(())
    }

    fn apply_merger(
        &mut self,
        subject: InstrumentId,
        successor: InstrumentId,
        consideration: MergerConsideration,
    ) -> Result<(), PortfolioError> {
        let subject_lots = self
            .lots
            .iter()
            .filter(|lot| lot.instrument_id == subject)
            .collect::<Vec<_>>();
        if subject_lots.iter().any(|lot| !lot.basis_complete) {
            return Err(PortfolioError::UnresolvedCorporateAction);
        }
        let consideration_currency = match consideration {
            MergerConsideration::Cash { amount } => Some(amount.currency()),
            MergerConsideration::Mixed { cash, .. } => Some(cash.currency()),
            MergerConsideration::Unspecified | MergerConsideration::Stock { .. } => None,
        };
        if consideration_currency.is_some_and(|currency| {
            subject_lots
                .iter()
                .any(|lot| lot.basis.currency() != currency)
        }) {
            return Err(PortfolioError::CurrencyMismatch);
        }
        match consideration {
            MergerConsideration::Unspecified => Err(PortfolioError::UnresolvedCorporateAction),
            MergerConsideration::Stock {
                numerator,
                denominator,
            } => self.convert_merger_lots(subject, successor, numerator.get(), denominator.get()),
            MergerConsideration::Cash { amount } => self.cash_merger(subject, amount),
            MergerConsideration::Mixed {
                numerator,
                denominator,
                cash,
            } => {
                let quantity = self.signed_quantity(subject)?;
                let cash_total = cash
                    .checked_mul_decimal(quantity)
                    .map_err(|_| PortfolioError::Arithmetic)?;
                self.cash.add(cash_total)?;
                self.convert_merger_lots(subject, successor, numerator.get(), denominator.get())
            }
        }
    }

    fn convert_merger_lots(
        &mut self,
        subject: InstrumentId,
        successor: InstrumentId,
        numerator: u32,
        denominator: u32,
    ) -> Result<(), PortfolioError> {
        let factor = checked_decimal_div(Decimal::from(numerator), Decimal::from(denominator))?;
        for lot in self
            .lots
            .iter_mut()
            .filter(|lot| lot.instrument_id == subject)
        {
            lot.instrument_id = successor;
            lot.quantity = checked_decimal_mul(lot.quantity, factor)?;
        }
        Ok(())
    }

    fn cash_merger(&mut self, subject: InstrumentId, amount: Money) -> Result<(), PortfolioError> {
        let selected = self
            .lots
            .iter()
            .filter(|lot| lot.instrument_id == subject)
            .cloned()
            .collect::<Vec<_>>();
        for lot in selected {
            let proceeds = amount
                .checked_mul_decimal(lot.quantity)
                .map_err(|_| PortfolioError::Arithmetic)?;
            self.cash.add(proceeds)?;
            let gain = match lot.direction {
                LotDirection::Long => proceeds
                    .checked_sub(lot.basis)
                    .map_err(|_| PortfolioError::Arithmetic)?,
                LotDirection::Short => lot
                    .basis
                    .checked_sub(proceeds)
                    .map_err(|_| PortfolioError::Arithmetic)?,
            };
            self.realized_gain.add(gain)?;
        }
        self.lots.retain(|lot| lot.instrument_id != subject);
        Ok(())
    }

    fn signed_quantity(&self, instrument_id: InstrumentId) -> Result<Decimal, PortfolioError> {
        self.lots
            .iter()
            .filter(|lot| lot.instrument_id == instrument_id)
            .try_fold(Decimal::ZERO, |total, lot| {
                let quantity = match lot.direction {
                    LotDirection::Long => lot.quantity,
                    LotDirection::Short => -lot.quantity,
                };
                checked_decimal_add(total, quantity)
            })
    }
}

fn require_complete(value: bool) -> Result<(), PortfolioError> {
    if value {
        Ok(())
    } else {
        Err(PortfolioError::UnresolvedCorporateAction)
    }
}

pub(crate) fn step_index(step: &AdjustmentStep) -> usize {
    match step {
        AdjustmentStep::Split { admitted_index, .. }
        | AdjustmentStep::CashDividend { admitted_index, .. }
        | AdjustmentStep::ReturnOfCapital { admitted_index, .. }
        | AdjustmentStep::Spinoff { admitted_index, .. }
        | AdjustmentStep::Merger { admitted_index, .. }
        | AdjustmentStep::Delisting { admitted_index }
        | AdjustmentStep::SymbolChange { admitted_index, .. } => *admitted_index,
    }
}
