//! Exact shadow constraints and Task 16 immutable-ledger reconciliation.

use std::collections::BTreeMap;

use market_squawk_data::{AdjustmentStep, CorporateActionPlan};
use market_squawk_domain::{
    AvailabilityEvidence, EvidenceDigest, InstrumentExecutionTerms, InstrumentId,
    MergerConsideration, Money, OrderSide, RevisionNumber, SourceIdentifier, Timestamp,
};
use market_squawk_portfolio::{
    CashFlow, CashFlowKind, CorporateActionBinding, LedgerEntry, LedgerEntryKind, LotSelection,
    PortfolioError, PortfolioLedger, PortfolioRevision, PriceEvidence, RevisionEvidence, Trade,
    TradeSide, TransactionRevision, ValuationSet,
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
    entitlements: BTreeMap<usize, ShadowCashEntitlement>,
}

#[derive(Debug)]
struct ShadowCashEntitlement {
    evidence: EvidenceDigest,
    amount: Money,
    settlement_at: Option<Timestamp>,
    settled: bool,
}

impl ShadowPortfolio {
    pub(super) fn new(initial_cash: Money) -> Self {
        Self {
            cash: initial_cash,
            positions: BTreeMap::new(),
            fees: Money::new(Decimal::ZERO, initial_cash.currency()),
            entitlements: BTreeMap::new(),
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
        fills: &super::RunHistory,
        as_of: Timestamp,
    ) -> Result<Self, BacktestError> {
        // Merge the chronological fill cursor with the much smaller corporate-action schedule.
        // Ordering is identical to ShadowOperation::key: actions, fills, then cash settlement.
        let mut operations = Vec::new();
        if let Some(plan) = &request.corporate_actions {
            for step in plan.steps() {
                let record = plan
                    .admitted()
                    .get(step_index(step))
                    .ok_or(BacktestError::AccountingMismatch)?;
                if action_is_available(record, as_of, as_of) {
                    append_action_operations(&mut operations, step, record, as_of);
                }
            }
        }
        operations.sort_unstable_by(|left, right| left.key().cmp(&right.key()));
        let mut actions = operations.into_iter().peekable();
        let mut shadow = Self::new(request.portfolio.initial_cash);
        for (index, fill) in fills.iter().enumerate() {
            let fill = fill?;
            if fill.executed_at() > as_of {
                break;
            }
            let key = (fill.executed_at(), 1, "backtest-research-fill", index);
            while actions.peek().is_some_and(|action| action.key() <= key) {
                shadow.apply_operation(actions.next().ok_or(BacktestError::AccountingMismatch)?)?;
            }
            let terms = request
                .dataset
                .observations
                .first_for_instrument(fill.instrument_id())?
                .execution_terms;
            shadow.apply(&fill, terms)?;
        }
        for action in actions {
            shadow.apply_operation(action)?;
        }
        Ok(shadow)
    }

    fn apply_operation(&mut self, operation: ShadowOperation<'_>) -> Result<(), BacktestError> {
        match operation {
            ShadowOperation::Fill { fill, terms, .. } => self.apply(fill, terms)?,
            ShadowOperation::Action { step, record } => self.apply_action(step, record)?,
            ShadowOperation::CashSettlement { index, .. } => {
                let entitlement = self
                    .entitlements
                    .get_mut(&index)
                    .ok_or(BacktestError::AccountingMismatch)?;
                if entitlement.settled {
                    return Err(BacktestError::AccountingMismatch);
                }
                self.cash = self.cash.checked_add(entitlement.amount)?;
                entitlement.settled = true;
            }
        }
        Ok(())
    }

    fn replay_operations(
        initial_cash: Money,
        mut operations: Vec<ShadowOperation<'_>>,
    ) -> Result<Self, BacktestError> {
        operations.sort_unstable_by(|left, right| left.key().cmp(&right.key()));
        let mut shadow = Self::new(initial_cash);
        for operation in operations {
            match operation {
                ShadowOperation::Fill { fill, terms, .. } => shadow.apply(fill, terms)?,
                ShadowOperation::Action { step, record } => {
                    shadow.apply_action(step, record)?;
                }
                ShadowOperation::CashSettlement { index, .. } => {
                    let entitlement = shadow
                        .entitlements
                        .get_mut(&index)
                        .ok_or(BacktestError::AccountingMismatch)?;
                    if entitlement.settled {
                        return Err(BacktestError::AccountingMismatch);
                    }
                    shadow.cash = shadow.cash.checked_add(entitlement.amount)?;
                    entitlement.settled = true;
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
            && self.entitlements.len() == revision.cash_entitlements().len()
            && self
                .entitlements
                .values()
                .zip(revision.cash_entitlements())
                .all(|(left, right)| {
                    left.evidence == right.action_evidence()
                        && left.amount == right.amount()
                        && left.settlement_at == right.simulated_settlement_at()
                        && left.settled == right.settled()
                })
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
        let mut equity = self.cash.checked_add(self.receivable_value()?)?;
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
                let quantity = self
                    .position(subject)
                    .checked_mul(Decimal::from(quantity_factor.numerator().get()))
                    .and_then(|value| {
                        value.checked_div(Decimal::from(quantity_factor.denominator().get()))
                    })
                    .ok_or(BacktestError::AccountingMismatch)?;
                if quantity.is_zero() {
                    self.positions.remove(&subject);
                } else {
                    self.positions.insert(subject, quantity);
                }
            }
            AdjustmentStep::CashDividend { amount, .. } => {
                self.accrue_cash(step, record, *amount, self.position(subject))?;
            }
            AdjustmentStep::ReturnOfCapital { amount, .. } => {
                self.accrue_cash(step, record, *amount, self.position(subject))?;
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
            } => self.apply_merger(step, record, subject, *successor, *consideration)?,
            AdjustmentStep::Delisting { .. } | AdjustmentStep::SymbolChange { .. } => {}
        }
        Ok(())
    }

    fn apply_merger(
        &mut self,
        step: &AdjustmentStep,
        record: &market_squawk_data::CorporateActionRecord,
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
                self.accrue_cash(step, record, amount, quantity)?;
            }
            MergerConsideration::Mixed {
                numerator,
                denominator,
                cash,
            } => {
                self.accrue_cash(step, record, cash, quantity)?;
                let converted = quantity
                    .checked_mul(ratio(numerator.get(), denominator.get())?)
                    .ok_or(BacktestError::AccountingMismatch)?;
                self.add_position(successor, converted)?;
            }
        }
        Ok(())
    }

    fn accrue_cash(
        &mut self,
        step: &AdjustmentStep,
        record: &market_squawk_data::CorporateActionRecord,
        amount: Money,
        quantity: Decimal,
    ) -> Result<(), BacktestError> {
        if amount.currency() != self.cash.currency() {
            return Err(BacktestError::AccountingMismatch);
        }
        let cash = amount.checked_mul_decimal(quantity)?;
        if self
            .entitlements
            .insert(
                step_index(step),
                ShadowCashEntitlement {
                    evidence: record.evidence_digest(),
                    amount: cash,
                    settlement_at: record
                        .application()
                        .and_then(|value| value.simulated_cash_settlement_at()),
                    settled: false,
                },
            )
            .is_some()
        {
            return Err(BacktestError::AccountingMismatch);
        }
        Ok(())
    }

    fn receivable_value(&self) -> Result<Money, BacktestError> {
        self.entitlements
            .values()
            .filter(|value| !value.settled)
            .try_fold(
                Money::new(Decimal::ZERO, self.cash.currency()),
                |total, value| total.checked_add(value.amount).map_err(Into::into),
            )
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

/// Recommendation outcomes reuse the generic ledger's exact fill/action accounting. No pricing,
/// dividend amount, split factor, merger consideration, or delisting proceeds are inferred here.
pub(crate) struct RecommendationAccounting {
    shadow: ShadowPortfolio,
    instrument: InstrumentId,
    delisted: bool,
}

impl RecommendationAccounting {
    pub(crate) fn at(
        entry: &ResearchFill,
        terms: InstrumentExecutionTerms,
        exit: Option<&ResearchFill>,
        plan: &CorporateActionPlan,
        as_of: Timestamp,
    ) -> Result<Self, BacktestError> {
        if entry.executed_at() > as_of
            || as_of > plan.valuation_cutoff()
            || exit.is_some_and(|fill| fill.executed_at() > as_of)
        {
            return Err(BacktestError::InvalidRequest);
        }
        let entry_cost = entry
            .price()
            .checked_mul_quantity(
                entry.quantity(),
                terms.price_tick(),
                terms.lot_size(),
                terms.quote_currency(),
            )?
            .checked_mul_decimal(terms.contract_multiplier())?
            .checked_add(entry.fee())?;
        let mut operations = Vec::new();
        operations
            .try_reserve_exact(plan.steps().len().saturating_mul(2).saturating_add(2))
            .map_err(|_| BacktestError::LimitExceeded)?;
        operations.push(ShadowOperation::Fill {
            index: 0,
            fill: entry,
            terms,
        });
        if let Some(exit) = exit {
            operations.push(ShadowOperation::Fill {
                index: 1,
                fill: exit,
                terms,
            });
        }
        let mut delisted = false;
        for step in plan.steps() {
            let record = plan
                .admitted()
                .get(step_index(step))
                .ok_or(BacktestError::AccountingMismatch)?;
            if action_is_available(record, plan.knowledge_cutoff(), as_of) {
                delisted |= matches!(step, AdjustmentStep::Delisting { .. })
                    && record.observation().context().provenance().instrument_id()
                        == Some(terms.instrument_id());
                append_action_operations(&mut operations, step, record, as_of);
            }
        }
        Ok(Self {
            shadow: ShadowPortfolio::replay_operations(entry_cost, operations)?,
            instrument: terms.instrument_id(),
            delisted,
        })
    }

    /// Complete post-action units; a distinct unpriced successor or distributed asset stays a gap.
    pub(crate) fn quantity(&self) -> Result<Decimal, BacktestError> {
        if self.delisted
            || self
                .shadow
                .positions
                .keys()
                .any(|instrument| *instrument != self.instrument)
        {
            return Err(BacktestError::MissingFinalPrice);
        }
        Ok(self.shadow.position(self.instrument))
    }

    pub(crate) fn cash(&self) -> Money {
        self.shadow.cash
    }

    pub(crate) fn receivable_value(&self) -> Result<Money, BacktestError> {
        self.shadow.receivable_value()
    }
}

#[derive(Debug)]
enum ShadowOperation<'a> {
    CashSettlement {
        index: usize,
        at: Timestamp,
    },
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
            Self::CashSettlement { index, at } => (*at, 2, "", *index),
            Self::Fill { index, fill, .. } => {
                (fill.executed_at(), 1, "backtest-research-fill", *index)
            }
            Self::Action { step, record } => (
                record
                    .application_at()
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
    knowledge_cutoff: Timestamp,
    as_of: Timestamp,
) -> bool {
    let available = match record.observation().context().provenance().availability() {
        AvailabilityEvidence::Evidenced { available_at, .. } => *available_at <= knowledge_cutoff,
        AvailabilityEvidence::LocalFirstObserved { observed_at } => {
            *observed_at <= knowledge_cutoff
        }
        AvailabilityEvidence::Inferred { .. } | AvailabilityEvidence::Unknown => false,
    };
    available
        && record
            .application()
            .is_none_or(|value| value.available_at() <= knowledge_cutoff)
        && record
            .application_at()
            .is_some_and(|effective| effective <= as_of)
}

fn append_action_operations<'a>(
    operations: &mut Vec<ShadowOperation<'a>>,
    step: &'a AdjustmentStep,
    record: &'a market_squawk_data::CorporateActionRecord,
    as_of: Timestamp,
) {
    operations.push(ShadowOperation::Action { step, record });
    if matches!(
        step,
        AdjustmentStep::CashDividend { .. }
            | AdjustmentStep::ReturnOfCapital { .. }
            | AdjustmentStep::Merger {
                consideration: MergerConsideration::Cash { .. } | MergerConsideration::Mixed { .. },
                ..
            }
    ) {
        if let Some(at) = record
            .application()
            .and_then(|value| value.simulated_cash_settlement_at())
        {
            if at <= as_of {
                operations.push(ShadowOperation::CashSettlement {
                    index: step_index(step),
                    at,
                });
            }
        }
    }
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
    fills: &super::RunHistory,
    cancellation: &tokio_util::sync::CancellationToken,
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
    let initial = LedgerEntry::try_new(
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
    )?;
    let fill_entries =
        fills
            .iter()
            .enumerate()
            .map(|(index, fill)| -> Result<LedgerEntry, BacktestError> {
                let fill = fill?;
                let terms = request
                    .dataset
                    .observations
                    .first_for_instrument(fill.instrument_id())?
                    .execution_terms;
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
                Ok(LedgerEntry::try_new(
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
                )?)
            });
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
    if let Some(scratch) = request.dataset.observations.operation_scratch() {
        let mut fill_error = None;
        let entries = std::iter::once(Ok(initial))
            .chain(fill_entries)
            .map(|entry| {
                entry.map_err(|error| {
                    fill_error = Some(error);
                    PortfolioError::EvidenceMismatch
                })
            });
        let revision = ledger.try_apply_stream(
            entries,
            request.corporate_actions.as_ref(),
            valuation,
            evidence,
            scratch,
            16 * 1024 * 1024 * 1024,
            cancellation,
        );
        if let Some(error) = fill_error {
            return Err(error);
        }
        revision.map_err(Into::into)
    } else {
        let entries = std::iter::once(Ok(initial))
            .chain(fill_entries)
            .collect::<Result<Vec<_>, BacktestError>>()?;
        ledger
            .try_apply(
                entries,
                request.corporate_actions.as_ref(),
                valuation,
                evidence,
            )
            .map_err(Into::into)
    }
}

fn latest_prices(
    observations: &crate::dataset::observation_store::ObservationStore,
    as_of: Timestamp,
) -> Result<Vec<PriceEvidence>, BacktestError> {
    let mut latest = BTreeMap::<InstrumentId, BacktestObservation>::new();
    for observation in observations.iter() {
        let observation = observation?;
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
