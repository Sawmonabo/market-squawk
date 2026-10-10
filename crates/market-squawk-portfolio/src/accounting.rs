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
        if money.amount().is_zero() {
            return Ok(());
        }
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
    pub(crate) cash_entitlements: BTreeMap<usize, crate::CashEntitlement>,
    pub(crate) lots: Vec<Lot>,
    pub(crate) realized_gain: CurrencyAmounts,
    pub(crate) incomplete_realized_basis: bool,
    pub(crate) realized_loss: CurrencyAmounts,
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
        let gross = trade.gross_notional()?;
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
                self.cash.add(proceeds)?;
                if disposal.basis_complete && disposal.basis.currency() == proceeds.currency() {
                    self.record_realized(
                        proceeds
                            .checked_sub(disposal.basis)
                            .map_err(|_| PortfolioError::Arithmetic)?,
                    )?;
                } else {
                    self.incomplete_realized_basis = true;
                }
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
                self.cash.subtract(cost)?;
                if disposal.basis_complete && disposal.basis.currency() == cost.currency() {
                    self.record_realized(
                        disposal
                            .basis
                            .checked_sub(cost)
                            .map_err(|_| PortfolioError::Arithmetic)?,
                    )?;
                } else {
                    self.incomplete_realized_basis = true;
                }
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
                self.accrue_cash(plan, admitted_index, subject, cash)?;
                self.income.add(cash)?;
            }
            AdjustmentStep::ReturnOfCapital { amount, .. } => {
                let cash = self.apply_return_of_capital(subject, *amount)?;
                self.accrue_cash(plan, admitted_index, subject, cash)?;
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
            } => self.apply_merger(plan, admitted_index, subject, *successor, *consideration)?,
            AdjustmentStep::Delisting { .. } | AdjustmentStep::SymbolChange { .. } => {}
        }
        Ok(())
    }

    fn accrue_cash(
        &mut self,
        plan: &CorporateActionPlan,
        index: usize,
        subject: InstrumentId,
        amount: Money,
    ) -> Result<(), PortfolioError> {
        let entitlement = crate::CashEntitlement::from_plan(plan, index, subject, amount)?;
        if self.cash_entitlements.insert(index, entitlement).is_some() {
            return Err(PortfolioError::EvidenceMismatch);
        }
        Ok(())
    }

    pub(crate) fn settle_cash(&mut self, admitted_index: usize) -> Result<(), PortfolioError> {
        let entitlement = self
            .cash_entitlements
            .get_mut(&admitted_index)
            .ok_or(PortfolioError::EvidenceMismatch)?;
        if entitlement.settled || entitlement.simulated_settlement_at.is_none() {
            return Err(PortfolioError::EvidenceMismatch);
        }
        self.cash.add(entitlement.amount)?;
        entitlement.settled = true;
        Ok(())
    }

    pub(crate) fn receivable_value(
        &self,
        valuation: &ValuationSet,
    ) -> Result<Money, PortfolioError> {
        let mut amounts = CurrencyAmounts::default();
        for entitlement in self
            .cash_entitlements
            .values()
            .filter(|value| !value.settled)
        {
            amounts.add(entitlement.amount)?;
        }
        amounts.total(valuation)
    }

    fn apply_split(
        &mut self,
        subject: InstrumentId,
        ratio: market_squawk_data::AdjustmentRatio,
    ) -> Result<(), PortfolioError> {
        for lot in self
            .lots
            .iter_mut()
            .filter(|lot| lot.instrument_id == subject)
        {
            lot.quantity = checked_decimal_div(
                checked_decimal_mul(lot.quantity, Decimal::from(ratio.numerator().get()))?,
                Decimal::from(ratio.denominator().get()),
            )?;
        }
        Ok(())
    }

    fn apply_return_of_capital(
        &mut self,
        subject: InstrumentId,
        amount: Money,
    ) -> Result<Money, PortfolioError> {
        let distribution = amount
            .checked_mul_decimal(self.signed_quantity(subject)?)
            .map_err(|_| PortfolioError::Arithmetic)?;
        let mut excess = Money::new(Decimal::ZERO, amount.currency());
        for lot in self
            .lots
            .iter_mut()
            .filter(|lot| lot.instrument_id == subject)
        {
            if lot.direction != LotDirection::Long
                || !lot.basis_complete
                || lot.basis.currency() != amount.currency()
            {
                // Inventory and the cash claim remain usable, but a tax-basis/expense result cannot
                // be invented from missing allocation or action-date FX evidence.
                lot.basis_complete = false;
                self.incomplete_realized_basis = true;
                continue;
            }
            let gross = amount
                .checked_mul_decimal(lot.quantity)
                .map_err(|_| PortfolioError::Arithmetic)?;
            let reduction = gross.amount().min(lot.basis.amount());
            lot.basis = lot
                .basis
                .checked_sub(Money::new(reduction, amount.currency()))
                .map_err(|_| PortfolioError::Arithmetic)?;
            excess = excess
                .checked_add(Money::new(gross.amount() - reduction, amount.currency()))
                .map_err(|_| PortfolioError::Arithmetic)?;
        }
        self.return_of_capital.add(distribution)?;
        self.record_realized(excess)?;
        Ok(distribution)
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
        for lot in self
            .lots
            .iter_mut()
            .filter(|lot| lot.instrument_id == subject)
        {
            lot.basis_complete = false;
        }
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
        plan: &CorporateActionPlan,
        index: usize,
        subject: InstrumentId,
        successor: InstrumentId,
        consideration: MergerConsideration,
    ) -> Result<(), PortfolioError> {
        match consideration {
            MergerConsideration::Unspecified => Err(PortfolioError::UnresolvedCorporateAction),
            MergerConsideration::Stock {
                numerator,
                denominator,
            } => {
                if self.lots.iter().any(|lot| lot.instrument_id == subject) {
                    // The exchange preserves wealth terms but supplies no taxable/non-taxable
                    // recognition or carryover-basis authority.
                    self.incomplete_realized_basis = true;
                }
                self.convert_merger_lots(
                    subject,
                    successor,
                    numerator.get(),
                    denominator.get(),
                    false,
                )
            }
            MergerConsideration::Cash { amount } => {
                let cash = self.cash_merger(subject, amount)?;
                self.accrue_cash(plan, index, subject, cash)
            }
            MergerConsideration::Mixed {
                numerator,
                denominator,
                cash,
            } => {
                let proceeds = cash
                    .checked_mul_decimal(self.signed_quantity(subject)?)
                    .map_err(|_| PortfolioError::Arithmetic)?;
                // Cash/stock consideration is fully usable for wealth. The source supplied no
                // split of historical basis between boot and successor stock, so realized basis
                // and successor basis remain explicitly incomplete.
                if self.lots.iter().any(|lot| lot.instrument_id == subject) {
                    self.incomplete_realized_basis = true;
                }
                self.convert_merger_lots(
                    subject,
                    successor,
                    numerator.get(),
                    denominator.get(),
                    false,
                )?;
                self.accrue_cash(plan, index, subject, proceeds)
            }
        }
    }

    fn convert_merger_lots(
        &mut self,
        subject: InstrumentId,
        successor: InstrumentId,
        numerator: u32,
        denominator: u32,
        preserve_basis: bool,
    ) -> Result<(), PortfolioError> {
        let factor = checked_decimal_div(Decimal::from(numerator), Decimal::from(denominator))?;
        for lot in self
            .lots
            .iter_mut()
            .filter(|lot| lot.instrument_id == subject)
        {
            lot.instrument_id = successor;
            lot.quantity = checked_decimal_mul(lot.quantity, factor)?;
            lot.basis_complete &= preserve_basis;
        }
        Ok(())
    }

    fn cash_merger(
        &mut self,
        subject: InstrumentId,
        amount: Money,
    ) -> Result<Money, PortfolioError> {
        let proceeds = amount
            .checked_mul_decimal(self.signed_quantity(subject)?)
            .map_err(|_| PortfolioError::Arithmetic)?;
        let selected = self
            .lots
            .iter()
            .filter(|lot| lot.instrument_id == subject)
            .cloned()
            .collect::<Vec<_>>();
        for lot in selected {
            if !lot.basis_complete || lot.basis.currency() != amount.currency() {
                self.incomplete_realized_basis = true;
                continue;
            }
            let gross = amount
                .checked_mul_decimal(lot.quantity)
                .map_err(|_| PortfolioError::Arithmetic)?;
            let gain = match lot.direction {
                LotDirection::Long => gross.checked_sub(lot.basis),
                LotDirection::Short => lot.basis.checked_sub(gross),
            }
            .map_err(|_| PortfolioError::Arithmetic)?;
            self.record_realized(gain)?;
        }
        self.lots.retain(|lot| lot.instrument_id != subject);
        Ok(proceeds)
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

    fn record_realized(&mut self, gain: Money) -> Result<(), PortfolioError> {
        if gain.amount().is_sign_negative() {
            let loss = Decimal::ZERO
                .checked_sub(gain.amount())
                .ok_or(PortfolioError::Arithmetic)?;
            self.realized_loss.add(Money::new(loss, gain.currency()))?;
        }
        self.realized_gain.add(gain)
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
