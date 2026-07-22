//! Exact shadow constraints and Task 16 immutable-ledger reconciliation.

use std::collections::BTreeMap;

use market_squawk_domain::{
    InstrumentExecutionTerms, InstrumentId, Money, OrderSide, RevisionNumber, SourceIdentifier,
    Timestamp,
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
