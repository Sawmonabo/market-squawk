//! Exact shadow constraints and Task 16 immutable-ledger reconciliation.

use std::collections::BTreeMap;

use market_squawk_data::AdjustmentStep;
use market_squawk_domain::{
    AvailabilityEvidence, InstrumentExecutionTerms, InstrumentId, MergerConsideration, Money,
    OrderSide, RevisionNumber, SourceIdentifier, Timestamp,
};
use market_squawk_portfolio::{
    CashFlow, CashFlowKind, CorporateActionBinding, LedgerEntry, LedgerEntryKind, LotSelection,
    PortfolioLedger, PortfolioRevision, PriceEvidence, RevisionEvidence, Trade, TradeSide,
    TransactionRevision, ValuationSet,
};
use rust_decimal::Decimal;

use super::{BacktestError, BacktestRequest};
use crate::ResearchFill;
use crate::dataset::BacktestObservation;

#[derive(Debug)]
pub(super) struct ShadowPortfolio {
    pub(super) cash: Money,
    positions: BTreeMap<InstrumentId, Decimal>,
    fees: Money,
}

impl ShadowPortfolio {
    pub(super) fn new(initial_cash: Money) -> Self {
        Self {
            cash: initial_cash,
            positions: BTreeMap::new(),
            fees: Money::new(Decimal::ZERO, initial_cash.currency()),
        }
    }

    pub(super) fn position(&self, instrument: InstrumentId) -> Decimal {
        self.positions
            .get(&instrument)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    pub(super) fn replay(
        request: &BacktestRequest,
        fills: &[ResearchFill],
        as_of: Timestamp,
    ) -> Result<Self, BacktestError> {
        let mut operations = Vec::new();
        operations
            .try_reserve_exact(
                fills.len().saturating_add(
                    request
                        .corporate_actions
                        .as_ref()
                        .map_or(0, |plan| plan.steps().len()),
                ),
            )
            .map_err(|_| BacktestError::LimitExceeded)?;
        for (index, fill) in fills.iter().enumerate() {
            if fill.executed_at() <= as_of {
                let terms = request
                    .dataset
                    .observations
                    .iter()
                    .find(|observation| observation.instrument_id() == fill.instrument_id())
                    .map(|observation| observation.execution_terms)
                    .ok_or(BacktestError::InvalidIntent)?;
                operations.push(ShadowOperation::Fill { index, fill, terms });
            }
        }
        if let Some(plan) = &request.corporate_actions {
            for step in plan.steps() {
                let record = plan
                    .admitted()
                    .get(step_index(step))
                    .ok_or(BacktestError::AccountingMismatch)?;
                if action_is_available(record, as_of) {
                    operations.push(ShadowOperation::Action { step, record });
                }
            }
        }
        operations.sort_unstable_by(|left, right| left.key().cmp(&right.key()));
        let mut shadow = Self::new(request.portfolio.initial_cash);
        for operation in operations {
            match operation {
                ShadowOperation::Fill { fill, terms, .. } => shadow.apply(fill, terms)?,
                ShadowOperation::Action { step, record } => {
                    shadow.apply_action(step, record)?;
                }
            }
        }
        Ok(shadow)
    }

    pub(super) fn apply(
        &mut self,
        fill: &ResearchFill,
        terms: InstrumentExecutionTerms,
    ) -> Result<(), BacktestError> {
        if fill.instrument_id() != terms.instrument_id()
            || fill.fee().currency() != self.cash.currency()
        {
            return Err(BacktestError::AccountingMismatch);
        }
        let quantity = fill.quantity().checked_to_decimal(terms.lot_size())?;
        let notional = fill
            .price()
            .checked_mul_quantity(
                fill.quantity(),
                terms.price_tick(),
                terms.lot_size(),
                terms.quote_currency(),
            )?
            .checked_mul_decimal(terms.contract_multiplier())?;
        if notional.currency() != self.cash.currency() {
            return Err(BacktestError::AccountingMismatch);
        }
        let current = self.position(terms.instrument_id());
        match fill.side() {
            OrderSide::Buy => {
                let cost = notional.checked_add(fill.fee())?;
                if self.cash.amount() < cost.amount() {
                    return Err(BacktestError::PortfolioConstraint);
                }
                self.cash = self.cash.checked_sub(cost)?;
                self.positions.insert(
                    terms.instrument_id(),
                    current
                        .checked_add(quantity)
                        .ok_or(BacktestError::AccountingMismatch)?,
                );
            }
            OrderSide::Sell => {
                if current < quantity {
                    return Err(BacktestError::PortfolioConstraint);
                }
                self.cash = self.cash.checked_add(notional.checked_sub(fill.fee())?)?;
                let remaining = current
                    .checked_sub(quantity)
                    .ok_or(BacktestError::AccountingMismatch)?;
                if remaining.is_zero() {
                    self.positions.remove(&terms.instrument_id());
                } else {
                    self.positions.insert(terms.instrument_id(), remaining);
                }
            }
        }
        self.fees = self.fees.checked_add(fill.fee())?;
        Ok(())
    }

    pub(super) fn matches_revision(&self, revision: &PortfolioRevision) -> bool {
        self.cash == revision.cash()
            && self.fees == revision.fees()
            && self.positions.len() == revision.positions().len()
            && self.positions.iter().all(|(instrument, quantity)| {
                revision
                    .position(*instrument)
                    .is_some_and(|position| position.quantity() == *quantity)
            })
    }

    pub(super) fn marked_equity(
        &self,
        prices: &BTreeMap<InstrumentId, (Money, Timestamp)>,
        as_of: Timestamp,
    ) -> Result<Option<Money>, BacktestError> {
        let mut equity = self.cash;
        for (instrument, quantity) in &self.positions {
            let Some((price, stale_at)) = prices.get(instrument) else {
                return Ok(None);
            };
            if *stale_at < as_of || price.currency() != equity.currency() {
                return Ok(None);
            }
            equity = equity.checked_add(price.checked_mul_decimal(*quantity)?)?;
        }
        Ok(Some(equity))
    }

    fn apply_action(
        &mut self,
        step: &AdjustmentStep,
        record: &market_squawk_data::CorporateActionRecord,
    ) -> Result<(), BacktestError> {
        let subject = record
            .observation()
            .context()
            .provenance()
            .instrument_id()
            .ok_or(BacktestError::AccountingMismatch)?;
        match step {
            AdjustmentStep::Split {
                quantity_factor, ..
            } => {
                let factor = ratio(
                    quantity_factor.numerator().get(),
                    quantity_factor.denominator().get(),
                )?;
                self.scale_position(subject, factor)?;
            }
            AdjustmentStep::CashDividend { amount, .. } => {
                self.add_cash_for_quantity(*amount, self.position(subject))?;
            }
            AdjustmentStep::ReturnOfCapital { amount, .. } => {
                self.add_cash_for_quantity(*amount, self.position(subject).max(Decimal::ZERO))?;
            }
            AdjustmentStep::Spinoff {
                distributed_instrument,
                distribution_ratio,
                ..
            } => {
                let factor = ratio(
                    distribution_ratio.numerator().get(),
                    distribution_ratio.denominator().get(),
                )?;
                let distributed = self
                    .position(subject)
                    .checked_mul(factor)
                    .ok_or(BacktestError::AccountingMismatch)?;
                self.add_position(*distributed_instrument, distributed)?;
            }
            AdjustmentStep::Merger {
                successor,
                consideration,
                ..
            } => self.apply_merger(subject, *successor, *consideration)?,
            AdjustmentStep::Delisting { .. } | AdjustmentStep::SymbolChange { .. } => {}
        }
        Ok(())
    }

    fn apply_merger(
        &mut self,
        subject: InstrumentId,
        successor: InstrumentId,
        consideration: MergerConsideration,
    ) -> Result<(), BacktestError> {
        let quantity = self.positions.remove(&subject).unwrap_or(Decimal::ZERO);
        match consideration {
            MergerConsideration::Unspecified => return Err(BacktestError::AccountingMismatch),
            MergerConsideration::Stock {
                numerator,
                denominator,
            } => {
                let converted = quantity
                    .checked_mul(ratio(numerator.get(), denominator.get())?)
                    .ok_or(BacktestError::AccountingMismatch)?;
                self.add_position(successor, converted)?;
            }
            MergerConsideration::Cash { amount } => {
                self.add_cash_for_quantity(amount, quantity)?;
            }
            MergerConsideration::Mixed {
                numerator,
                denominator,
                cash,
            } => {
                self.add_cash_for_quantity(cash, quantity)?;
                let converted = quantity
                    .checked_mul(ratio(numerator.get(), denominator.get())?)
                    .ok_or(BacktestError::AccountingMismatch)?;
                self.add_position(successor, converted)?;
            }
        }
        Ok(())
    }

    fn add_cash_for_quantity(
        &mut self,
        amount: Money,
        quantity: Decimal,
    ) -> Result<(), BacktestError> {
        if amount.currency() != self.cash.currency() {
            return Err(BacktestError::AccountingMismatch);
        }
        let cash = amount.checked_mul_decimal(quantity)?;
        self.cash = self.cash.checked_add(cash)?;
        Ok(())
    }

    fn scale_position(
        &mut self,
        instrument: InstrumentId,
        factor: Decimal,
    ) -> Result<(), BacktestError> {
        let current = self.position(instrument);
        let scaled = current
            .checked_mul(factor)
            .ok_or(BacktestError::AccountingMismatch)?;
        if scaled.is_zero() {
            self.positions.remove(&instrument);
        } else {
            self.positions.insert(instrument, scaled);
        }
        Ok(())
    }

    fn add_position(
        &mut self,
        instrument: InstrumentId,
        quantity: Decimal,
    ) -> Result<(), BacktestError> {
        let updated = self
            .position(instrument)
            .checked_add(quantity)
            .ok_or(BacktestError::AccountingMismatch)?;
        if updated.is_zero() {
            self.positions.remove(&instrument);
        } else {
            self.positions.insert(instrument, updated);
        }
        Ok(())
    }
}

#[derive(Debug)]
enum ShadowOperation<'a> {
    Fill {
        index: usize,
        fill: &'a ResearchFill,
        terms: InstrumentExecutionTerms,
    },
    Action {
        step: &'a AdjustmentStep,
        record: &'a market_squawk_data::CorporateActionRecord,
    },
}

impl ShadowOperation<'_> {
    fn key(&self) -> (Timestamp, u8, &str, usize) {
        match self {
            Self::Fill { index, fill, .. } => {
                (fill.executed_at(), 1, "backtest-research-fill", *index)
            }
            Self::Action { step, record } => (
                record
                    .observation()
                    .context()
                    .time()
                    .effective()
                    .exact_timestamp()
                    .unwrap_or(Timestamp::from_unix_nanos(i64::MAX)),
                0,
                record
                    .observation()
                    .context()
                    .provenance()
                    .source_identifier()
                    .as_str(),
                step_index(step),
            ),
        }
    }
}

fn action_is_available(
    record: &market_squawk_data::CorporateActionRecord,
    as_of: Timestamp,
) -> bool {
    let available = match record.observation().context().provenance().availability() {
        AvailabilityEvidence::Evidenced { available_at, .. } => *available_at <= as_of,
        AvailabilityEvidence::LocalFirstObserved { observed_at } => *observed_at <= as_of,
        AvailabilityEvidence::Inferred { .. } | AvailabilityEvidence::Unknown => false,
    };
    available
        && record
            .observation()
            .context()
            .time()
            .effective()
            .exact_timestamp()
            .is_some_and(|effective| effective <= as_of)
}

fn step_index(step: &AdjustmentStep) -> usize {
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

fn ratio(numerator: u32, denominator: u32) -> Result<Decimal, BacktestError> {
    Decimal::from(numerator)
        .checked_div(Decimal::from(denominator))
        .ok_or(BacktestError::AccountingMismatch)
}

pub(super) fn reconcile(
    request: &BacktestRequest,
    fills: &[ResearchFill],
) -> Result<PortfolioRevision, BacktestError> {
    let first = request
        .dataset
        .observations
        .first()
        .ok_or(BacktestError::InvalidDataset)?;
    let as_of = request
        .dataset
        .observations
        .last()
        .ok_or(BacktestError::InvalidDataset)?
        .decision_at;
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(fills.len().saturating_add(1))
        .map_err(|_| BacktestError::LimitExceeded)?;
    entries.push(LedgerEntry::try_new(
        request.portfolio.account_id,
        TransactionRevision::try_new(
            SourceIdentifier::try_from("backtest-initial-capital")?,
            RevisionNumber::new(1)?,
            None,
        )?,
        first.decision_at.checked_sub_nanos(1)?,
        SourceIdentifier::try_from("backtest-initial-capital")?,
        LedgerEntryKind::CashFlow(CashFlow::try_new(
            CashFlowKind::Deposit,
            request.portfolio.initial_cash,
            None,
        )?),
    )?);
    for (index, fill) in fills.iter().enumerate() {
        let terms = request
            .dataset
            .observations
            .iter()
            .find(|observation| observation.instrument_id() == fill.instrument_id())
            .map(|observation| observation.execution_terms)
            .ok_or(BacktestError::InvalidIntent)?;
        let quantity = fill.quantity().checked_to_decimal(terms.lot_size())?;
        let price = fill
            .price()
            .checked_to_decimal(terms.price_tick())?
            .checked_mul(terms.contract_multiplier())
            .ok_or(BacktestError::AccountingMismatch)?;
        let side = match fill.side() {
            OrderSide::Buy => TradeSide::Buy,
            OrderSide::Sell => TradeSide::Sell,
        };
        entries.push(LedgerEntry::try_new(
            request.portfolio.account_id,
            TransactionRevision::try_new(
                SourceIdentifier::try_from(format!("backtest-fill-{index:016x}"))?,
                RevisionNumber::new(1)?,
                None,
            )?,
            fill.executed_at(),
            SourceIdentifier::try_from("backtest-research-fill")?,
            LedgerEntryKind::Trade(Trade::try_new(
                side,
                terms.instrument_id(),
                quantity,
                Money::new(price, terms.quote_currency()),
                fill.fee(),
                LotSelection::Fifo,
            )?),
        )?);
    }
    let valuation = ValuationSet::try_new(
        request.portfolio.initial_cash.currency(),
        as_of,
        request.dataset.manifest.clone(),
        request.dataset.point_in_time_content,
        latest_prices(&request.dataset.observations, as_of)?,
        Vec::new(),
        request.portfolio.limits,
    )?;
    let evidence = RevisionEvidence::try_new(
        as_of,
        request.dataset.manifest.clone(),
        request.dataset.point_in_time_content,
        request.dataset.point_in_time_audit,
        request.sources.to_vec(),
        Vec::new(),
        request
            .corporate_actions
            .as_ref()
            .map(CorporateActionBinding::from_plan),
    )?;
    let mut ledger = PortfolioLedger::try_new(
        request.portfolio.account_id,
        request.portfolio.initial_cash.currency(),
        request.portfolio.limits,
    )?;
    ledger
        .try_apply(
            entries,
            request.corporate_actions.as_ref(),
            valuation,
            evidence,
        )
        .map_err(Into::into)
}

fn latest_prices(
    observations: &[BacktestObservation],
    as_of: Timestamp,
) -> Result<Vec<PriceEvidence>, BacktestError> {
    let mut latest = BTreeMap::<InstrumentId, &BacktestObservation>::new();
    for observation in observations {
        if observation.decision_at <= as_of
            && observation.stale_at >= as_of
            && observation.mid_price.is_some()
        {
            latest.insert(observation.instrument_id(), observation);
        }
    }
    latest
        .into_values()
        .map(|observation| {
            let terms = observation.execution_terms;
            let price = observation
                .mid_price
                .ok_or(BacktestError::MissingFinalPrice)?
                .checked_to_decimal(terms.price_tick())?
                .checked_mul(terms.contract_multiplier())
                .ok_or(BacktestError::AccountingMismatch)?;
            PriceEvidence::try_new(
                observation.instrument_id(),
                Money::new(price, terms.quote_currency()),
                as_of,
                SourceIdentifier::try_from("backtest-final-valuation")?,
            )
            .map_err(Into::into)
        })
        .collect()
}
