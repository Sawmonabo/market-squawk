//! Source-admitted corporate actions replayed against the worker's original fill chronology.
use super::*;
use crate::{PaperExecutionSnapshot, PaperFillSnapshot, PaperOrderSnapshot};
use market_squawk_data::{AdjustmentStep, CorporateActionPlan};
use market_squawk_domain::{CalendarDate, Timestamp};
use serde::{Deserialize, Serialize};

pub(crate) const MAXIMUM_ACTIONS: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PaperActionOrigin {
    pub(super) opened_at: Timestamp,
    pub(super) accounts: Vec<PaperAccountBootstrap>,
}

/// Receivable or short-payment obligation, fixed at the original entitlement boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperCashEntitlement {
    pub(super) account_id: AccountId,
    pub(super) instrument_id: InstrumentId,
    pub(super) evidence: [u8; 32],
    pub(super) entitled_at: Timestamp,
    pub(super) amount: Money,
    pub(super) payable_date: Option<CalendarDate>,
    pub(super) settlement_at: Option<Timestamp>,
    pub(super) settled: bool,
}
impl PaperCashEntitlement {
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub const fn evidence(&self) -> [u8; 32] {
        self.evidence
    }
    pub const fn entitled_at(&self) -> Timestamp {
        self.entitled_at
    }
    pub const fn amount(&self) -> Money {
        self.amount
    }
    pub const fn payable_date(&self) -> Option<CalendarDate> {
        self.payable_date
    }
    pub const fn simulated_settlement_at(&self) -> Option<Timestamp> {
        self.settlement_at
    }
    pub const fn settled(&self) -> bool {
        self.settled
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PaperActionReceipt {
    pub(super) content: [u8; 32],
    pub(super) source: [u8; 32],
    pub(super) applied_economics: [u8; 32],
    pub(super) source_reference: Vec<u8>,
    pub(super) covered_instruments: Vec<InstrumentId>,
    pub(super) covered_interval: (CalendarDate, CalendarDate),
    pub(super) us_equity_dates: bool,
    pub(super) knowledge_cutoff: Timestamp,
    pub(super) valued_at: Timestamp,
}

impl PaperLedger {
    /// The originally applied source plan must cover this exact stock and original quote clock.
    /// No receipt, another instrument, a later quote or an advanced cutoff remains unavailable.
    pub(crate) fn virtual_source_covers(&self, instrument: InstrumentId, observed_at: Timestamp) -> bool {
        let (Some(receipt), Some(origin)) = (&self.action_receipt, &self.action_origin) else { return false; };
        let (Ok(origin_date), Ok(mark_date)) = (us_equity_action_date(origin.opened_at), us_equity_action_date(observed_at)) else { return false; };
        receipt.us_equity_dates && receipt.covered_instruments.contains(&instrument)
            && receipt.covered_interval.0 <= origin_date && receipt.covered_interval.1 >= mark_date
            && origin.opened_at <= observed_at && receipt.valued_at >= observed_at
            && receipt.knowledge_cutoff >= observed_at
    }

    pub(crate) fn executable_mark_snapshot(&self) -> Vec<super::PaperMarkEvidence> {
        self.marks.values().copied().collect()
    }
    pub(crate) fn action_origin_snapshot(&self) -> Option<(Timestamp, Vec<PaperAccountBootstrap>)> {
        self.action_origin
            .as_ref()
            .map(|origin| (origin.opened_at, origin.accounts.clone()))
    }
    pub(crate) fn requires_action_source_reopen(&self) -> bool {
        self.action_receipt.is_some()
    }
    pub(crate) fn verify_reopened_action_source(
        &self,
        plan: &CorporateActionPlan,
        reference: &[u8],
    ) -> Result<(), PaperLedgerError> {
        let receipt = self
            .action_receipt
            .as_ref()
            .ok_or(PaperLedgerError::InvalidActionEvidence)?;
        let coverage = plan
            .source_admission()
            .ok_or(PaperLedgerError::InvalidActionEvidence)?;
        if receipt.content != plan.content_hash().bytes()
            || receipt.source != coverage.evidence_digest().bytes()
            || receipt.applied_economics != effective_economics_digest(plan,
                self.action_origin.as_ref().ok_or(PaperLedgerError::InvalidRecovery)?.opened_at, receipt.valued_at)?
            || receipt.source_reference != reference
            || receipt.covered_instruments.as_slice() != coverage.instruments()
            || receipt.covered_interval != coverage.interval()
            || receipt.us_equity_dates != coverage.uses_us_equity_dates()
            || receipt.knowledge_cutoff != plan.knowledge_cutoff()
            || receipt.valued_at != plan.valuation_cutoff()
        {
            return Err(PaperLedgerError::InvalidActionEvidence);
        }
        Ok(())
    }
    pub(crate) fn action_source_reference(&self) -> Option<Vec<u8>> {
        self.action_receipt
            .as_ref()
            .map(|receipt| receipt.source_reference.clone())
    }
    pub(crate) fn cash_entitlement_snapshot(&self) -> Vec<PaperCashEntitlement> {
        self.cash_entitlements.clone()
    }

    /// Called only at a new worker's actual bootstrap clock, before any fills exist.
    pub(crate) fn retain_action_origin(
        &mut self,
        opened_at: Timestamp,
    ) -> Result<(), PaperLedgerError> {
        if self.action_origin.is_some() || !self.reservations.is_empty() {
            return Err(PaperLedgerError::InvalidRecovery);
        }
        let mut accounts = Vec::new();
        accounts
            .try_reserve_exact(self.accounts.len())
            .map_err(|_| PaperLedgerError::Capacity)?;
        for (id, state) in &self.accounts {
            accounts.push(PaperAccountBootstrap {
                account_id: *id,
                revision: state.revision,
                eligible: state.eligible,
                cash: self
                    .cash
                    .iter()
                    .filter(|((account, _), _)| account == id)
                    .map(|((_, currency), amount)| Money::new(*amount, *currency))
                    .collect(),
                capital: state.settled_capital,
                peak_capital: state.peak_marked_equity,
                gross_exposure: state.marked_gross_exposure,
                realized_loss: state.realized_loss,
                realized_pnl: state.realized_pnl,
                positions: self
                    .positions
                    .iter()
                    .filter(|((account, _), _)| account == id)
                    .map(|((_, instrument), lots)| (*instrument, *lots))
                    .collect(),
                position_cost_basis: self
                    .position_cost_basis
                    .iter()
                    .filter(|((account, _), _)| account == id)
                    .map(|((_, instrument), amount)| {
                        (*instrument, Money::new(*amount, state.currency))
                    })
                    .collect(),
            });
        }
        self.action_origin = Some(Box::new(PaperActionOrigin {
            opened_at,
            accounts,
        }));
        Ok(())
    }

    /// Source coverage is mandatory. Recovery-decoded or general action vectors have no such
    /// admission. Original worker orders/fills are supplied internally, never by a control caller.
    pub(crate) fn replay_source_actions(
        &self,
        plan: &CorporateActionPlan,
        source_reference: &[u8],
        snapshot: &PaperExecutionSnapshot,
        valued_at: Timestamp,
        maximum_mark_age_nanos: u64,
    ) -> Result<Self, PaperLedgerError> {
        self.replay_source_actions_with_virtual_marks(plan, source_reference, snapshot, valued_at, maximum_mark_age_nanos, &[])
    }

    pub(crate) fn replay_source_actions_with_virtual_marks(
        &self,
        plan: &CorporateActionPlan,
        source_reference: &[u8],
        snapshot: &PaperExecutionSnapshot,
        valued_at: Timestamp,
        maximum_mark_age_nanos: u64,
        virtual_marks: &[super::PaperMarkEvidence],
    ) -> Result<Self, PaperLedgerError> {
        let origin = self
            .action_origin
            .as_ref()
            .ok_or(PaperLedgerError::InvalidRecovery)?;
        let coverage = plan
            .source_admission()
            .ok_or(PaperLedgerError::InvalidActionEvidence)?;
        if source_reference.is_empty()
            || source_reference.len() > 64 * 1024
            || !snapshot.complete()
            || valued_at < origin.opened_at
            || plan.valuation_cutoff() != valued_at
            || valued_at > plan.knowledge_cutoff()
            || plan.steps().len() > MAXIMUM_ACTIONS
            || self.action_receipt.as_ref().is_some_and(|receipt| {
                receipt.knowledge_cutoff > plan.knowledge_cutoff() || receipt.valued_at > valued_at
            })
        {
            return Err(PaperLedgerError::InvalidActionEvidence);
        }
        let origin_date = source_action_date(origin.opened_at, coverage.uses_us_equity_dates())?;
        let valuation_date = source_action_date(valued_at, coverage.uses_us_equity_dates())?;
        if coverage.interval().0 > origin_date || coverage.interval().1 < valuation_date {
            return Err(PaperLedgerError::InvalidActionEvidence);
        }
        let mut original_marks = self.marks.clone();
        let mut seen_marks = std::collections::BTreeSet::new();
        if virtual_marks.len() > 32 { return Err(PaperLedgerError::Capacity); }
        for &mark in virtual_marks {
            if !seen_marks.insert(mark.execution_terms().instrument_id())
                || !coverage.uses_us_equity_dates() || !mark.is_virtual_paper() || mark.observed_at() > valued_at
                || !coverage.instruments().contains(&mark.execution_terms().instrument_id())
                || original_marks.get(&mark.execution_terms().instrument_id()).is_some_and(|old| mark.validate_successor(*old).is_err())
            { return Err(PaperLedgerError::InvalidMark); }
            if !original_marks.contains_key(&mark.execution_terms().instrument_id()) && original_marks.len() >= self.config.maximum_positions {
                return Err(PaperLedgerError::Capacity);
            }
            original_marks.insert(mark.execution_terms().instrument_id(), mark);
        }
        let applied_economics = effective_economics_digest(plan, origin.opened_at, valued_at)?;
        let receipt = PaperActionReceipt {
            content: plan.content_hash().bytes(), source: coverage.evidence_digest().bytes(), applied_economics,
            source_reference: source_reference.to_vec(), covered_instruments: coverage.instruments().to_vec(),
            covered_interval: coverage.interval(), us_equity_dates: coverage.uses_us_equity_dates(),
            knowledge_cutoff: plan.knowledge_cutoff(), valued_at,
        };
        if !self.reservations.is_empty() {
            // Orders/fills/reservations remain exactly native when no effective entitlement,
            // split, payment-state or other financial effect changed. A new genuine source
            // receipt and original quote marks can then refresh valuation without replaying fills.
            if !coverage.uses_us_equity_dates() || self.action_receipt.as_ref().is_none_or(|old|
                !old.us_equity_dates || old.applied_economics != applied_economics)
                || self.positions.keys().any(|(_, id)| !coverage.instruments().contains(id))
                || self.cash_entitlements.iter().any(|claim| !plan.admitted().iter().any(|record| record.evidence_digest().bytes() == claim.evidence))
                || self.reservations.values().any(|reservation| !coverage.instruments().contains(&reservation.terms.instrument_id())) {
                return Err(PaperLedgerError::InvalidActionEvidence);
            }
            let mut refreshed = self.clone();
            refreshed.marks = original_marks;
            for account in refreshed.accounts.values_mut() {
                account.revision = account.revision.get().checked_add(1).and_then(NonZeroU64::new).ok_or(PaperLedgerError::Overflow)?;
            }
            refreshed.refresh_action_marks(valued_at, maximum_mark_age_nanos)?;
            refreshed.action_receipt = Some(receipt);
            return Ok(refreshed);
        }
        let mut replay = Self::try_new(self.config, origin.accounts.clone())?;
        replay.action_origin = self.action_origin.clone();
        let count = plan
            .steps()
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(snapshot.fills().len()))
            .ok_or(PaperLedgerError::Capacity)?;
        let mut operations = Vec::new();
        operations
            .try_reserve_exact(count)
            .map_err(|_| PaperLedgerError::Capacity)?;
        for fill in snapshot.fills() {
            if fill.event_at() < origin.opened_at || fill.event_at() > valued_at {
                return Err(PaperLedgerError::InvalidActionEvidence);
            }
            let order = snapshot
                .orders()
                .iter()
                .find(|order| order.order_id() == fill.order_id())
                .ok_or(PaperLedgerError::InvalidRecovery)?;
            if !coverage
                .instruments()
                .contains(&order.execution_terms().instrument_id())
            {
                return Err(PaperLedgerError::InvalidActionEvidence);
            }
            operations.push(Operation::Fill(fill, order));
        }
        for step in plan.steps() {
            let index = step_index(step);
            let record = plan
                .admitted()
                .get(index)
                .ok_or(PaperLedgerError::InvalidActionEvidence)?;
            let at = record
                .application_at()
                .ok_or(PaperLedgerError::InvalidActionEvidence)?;
            if at > valued_at {
                return Err(PaperLedgerError::InvalidActionEvidence);
            }
            // Initial holdings belong to the retained bootstrap clock. Earlier actions do not
            // retroactively create entitlements on inventory that this paper account did not own.
            if at < origin.opened_at {
                continue;
            }
            operations.push(Operation::Action(index, step, at));
            if let Some(at) = record
                .application()
                .and_then(|value| value.simulated_cash_settlement_at())
            {
                if at <= valued_at {
                    operations.push(Operation::Settle(index, at));
                }
            }
        }
        operations.sort_by_key(Operation::key);
        for operation in operations {
            match operation {
                Operation::Fill(fill, order) => replay.replay_fill(*fill, order)?,
                Operation::Action(index, step, at) => {
                    let record = &plan.admitted()[index];
                    let instrument = record
                        .observation()
                        .context()
                        .provenance()
                        .instrument_id()
                        .ok_or(PaperLedgerError::InvalidActionEvidence)?;
                    match step {
                        AdjustmentStep::Split {
                            quantity_factor, ..
                        } => {
                            for ((_, subject), lots) in &mut replay.positions {
                                if *subject != instrument {
                                    continue;
                                }
                                // Integer division occurs only after multiplication. A fractional
                                // residual needs actual cash-in-lieu authority and is never rounded.
                                let numerator = i128::from(*lots)
                                    .checked_mul(i128::from(quantity_factor.numerator().get()))
                                    .ok_or(PaperLedgerError::Overflow)?;
                                let denominator = i128::from(quantity_factor.denominator().get());
                                if numerator % denominator != 0 {
                                    return Err(PaperLedgerError::FractionalActionInventory);
                                }
                                *lots = i64::try_from(numerator / denominator)
                                    .map_err(|_| PaperLedgerError::Overflow)?;
                            }
                        }
                        AdjustmentStep::CashDividend { amount, .. } => {
                            let terms = snapshot
                                .orders()
                                .iter()
                                .filter(|order| {
                                    order.execution_terms().instrument_id() == instrument
                                })
                                .filter(|order| order.accepted_at() <= at)
                                .max_by_key(|order| order.accepted_at())
                                .map(PaperOrderSnapshot::execution_terms);
                            for ((account, subject), lots) in &replay.positions {
                                if *subject != instrument || *lots == 0 {
                                    continue;
                                }
                                let terms = terms.ok_or(PaperLedgerError::InvalidActionEvidence)?;
                                if terms.contract_multiplier() != Decimal::ONE
                                    || amount.currency() != terms.quote_currency()
                                {
                                    return Err(PaperLedgerError::UnsupportedSettlement);
                                }
                                let units = Decimal::from(*lots)
                                    .checked_mul(terms.lot_size().as_decimal())
                                    .ok_or(PaperLedgerError::Overflow)?;
                                let amount = amount
                                    .checked_mul_decimal(units)
                                    .map_err(|_| PaperLedgerError::Overflow)?;
                                if replay.cash_entitlements.len() >= MAXIMUM_ACTIONS {
                                    return Err(PaperLedgerError::Capacity);
                                }
                                replay
                                    .cash_entitlements
                                    .try_reserve(1)
                                    .map_err(|_| PaperLedgerError::Capacity)?;
                                replay.cash_entitlements.push(PaperCashEntitlement {
                                    account_id: *account,
                                    instrument_id: instrument,
                                    evidence: record.evidence_digest().bytes(),
                                    entitled_at: at,
                                    amount,
                                    payable_date: record
                                        .application()
                                        .and_then(|value| value.payable_date()),
                                    settlement_at: record
                                        .application()
                                        .and_then(|value| value.simulated_cash_settlement_at()),
                                    settled: false,
                                });
                            }
                        }
                        AdjustmentStep::SymbolChange { .. } | AdjustmentStep::Delisting { .. } => {}
                        _ => return Err(PaperLedgerError::UnsupportedAction),
                    }
                }
                Operation::Settle(index, at) => {
                    let evidence = plan.admitted()[index].evidence_digest().bytes();
                    for claim in &mut replay.cash_entitlements {
                        if claim.evidence != evidence {
                            continue;
                        }
                        if claim.settled
                            || claim.settlement_at != Some(at)
                            || at < claim.entitled_at
                        {
                            return Err(PaperLedgerError::InvalidActionEvidence);
                        }
                        let cash = replay
                            .cash
                            .get_mut(&(claim.account_id, claim.amount.currency()))
                            .ok_or(PaperLedgerError::UnknownAccountOrCurrency)?;
                        *cash = cash
                            .checked_add(claim.amount.amount())
                            .ok_or(PaperLedgerError::Overflow)?;
                        let account = replay
                            .accounts
                            .get_mut(&claim.account_id)
                            .ok_or(PaperLedgerError::UnknownAccountOrCurrency)?;
                        account.settled_capital = account
                            .settled_capital
                            .checked_add(claim.amount)
                            .map_err(|_| PaperLedgerError::Overflow)?;
                        // The native account is cash-funded: recovery rejects negative capital.
                        // Retain the prior ledger atomically when a short payable cannot settle;
                        // do not mint a loan, erase the obligation, or publish unrecoverable cash.
                        if cash.is_sign_negative()
                            || account.settled_capital.amount().is_sign_negative()
                        {
                            return Err(PaperLedgerError::InsufficientCash);
                        }
                        claim.settled = true;
                    }
                }
            }
        }
        for step in plan.steps() {
            if !matches!(step, AdjustmentStep::Split { .. }) {
                continue;
            }
            let record = &plan.admitted()[step_index(step)];
            let instrument = record
                .observation()
                .context()
                .provenance()
                .instrument_id()
                .ok_or(PaperLedgerError::InvalidActionEvidence)?;
            if replay
                .positions
                .keys()
                .any(|(_, subject)| *subject == instrument)
                && original_marks.get(&instrument).is_none_or(|mark| {
                    record
                        .application_at()
                        .is_none_or(|at| mark.observed_at() < at)
                })
            {
                return Err(PaperLedgerError::StaleMark);
            }
        }
        replay.marks = original_marks;
        for (id, state) in &mut replay.accounts {
            let prior = self
                .accounts
                .get(id)
                .ok_or(PaperLedgerError::InvalidRecovery)?;
            state.revision = prior
                .revision
                .get()
                .checked_add(1)
                .and_then(NonZeroU64::new)
                .ok_or(PaperLedgerError::Overflow)?;
            if prior.peak_marked_equity.amount() > state.peak_marked_equity.amount() {
                state.peak_marked_equity = prior.peak_marked_equity;
            }
        }
        replay.refresh_action_marks(valued_at, maximum_mark_age_nanos)?;
        replay.action_receipt = Some(receipt);
        Ok(replay)
    }

    fn replay_fill(
        &mut self,
        fill: PaperFillSnapshot,
        order: &PaperOrderSnapshot,
    ) -> Result<(), PaperLedgerError> {
        let id = order.account_id();
        let terms = order.execution_terms();
        let key = (id, terms.instrument_id());
        let account = self
            .accounts
            .get_mut(&id)
            .ok_or(PaperLedgerError::UnknownAccountOrCurrency)?;
        if fill.notional().currency() != account.currency
            || fill.fee().currency() != account.currency
        {
            return Err(PaperLedgerError::FeeCurrencyMismatch);
        }
        let transition = transition_position(
            self.positions.get(&key).copied().unwrap_or(0),
            Money::new(
                self.position_cost_basis
                    .get(&key)
                    .copied()
                    .unwrap_or_default(),
                account.currency,
            ),
            order.side(),
            fill.quantity(),
            fill.notional(),
            self.config.fee_schedule.money_scale(),
        )?;
        if !self.config.allow_short && transition.next_lots < 0 {
            return Err(PaperLedgerError::InsufficientPosition);
        }
        let cash = self
            .cash
            .get_mut(&(id, account.currency))
            .ok_or(PaperLedgerError::UnknownAccountOrCurrency)?;
        let signed = if order.side() == OrderSide::Buy {
            -fill.notional().amount()
        } else {
            fill.notional().amount()
        };
        *cash = cash
            .checked_add(signed)
            .and_then(|value| value.checked_sub(fill.fee().amount()))
            .ok_or(PaperLedgerError::Overflow)?;
        if cash.is_sign_negative() {
            return Err(PaperLedgerError::InsufficientCash);
        }
        account.settled_capital = account
            .settled_capital
            .checked_add(transition.realized_pnl)
            .and_then(|value| value.checked_sub(fill.fee()))
            .map_err(|_| PaperLedgerError::Overflow)?;
        account.realized_pnl = account
            .realized_pnl
            .checked_add(transition.realized_pnl)
            .map_err(|_| PaperLedgerError::Overflow)?;
        let loss = (-transition.realized_pnl.amount()).max(Decimal::ZERO);
        account.realized_loss = account
            .realized_loss
            .checked_add(Money::new(loss, account.currency))
            .and_then(|value| value.checked_add(fill.fee()))
            .map_err(|_| PaperLedgerError::Overflow)?;
        if transition.next_lots == 0 {
            self.positions.remove(&key);
            self.position_cost_basis.remove(&key);
        } else {
            self.positions.insert(key, transition.next_lots);
            self.position_cost_basis
                .insert(key, transition.next_cost_basis.amount());
        }
        Ok(())
    }
    pub(super) fn unpaid_action_cash(
        &self,
        id: AccountId,
        currency: Currency,
    ) -> Result<Money, PaperLedgerError> {
        self.cash_entitlements
            .iter()
            .filter(|value| value.account_id == id && !value.settled)
            .try_fold(Money::new(Decimal::ZERO, currency), |sum, value| {
                sum.checked_add(value.amount)
                    .map_err(|_| PaperLedgerError::Overflow)
            })
    }
}

enum Operation<'a> {
    Fill(&'a PaperFillSnapshot, &'a PaperOrderSnapshot),
    Action(usize, &'a AdjustmentStep, Timestamp),
    Settle(usize, Timestamp),
}
impl Operation<'_> {
    fn key(&self) -> (Timestamp, u8, u64) {
        match self {
            Self::Action(index, _, at) => (*at, 0, *index as u64),
            Self::Fill(fill, _) => (fill.event_at(), 1, fill.sequence()),
            Self::Settle(index, at) => (*at, 2, *index as u64),
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
        | AdjustmentStep::Delisting { admitted_index, .. }
        | AdjustmentStep::SymbolChange { admitted_index, .. } => *admitted_index,
    }
}

/// Source US-equity ex-dates use the actual New York civil date, including DST.
pub(super) fn us_equity_action_date(at: Timestamp) -> Result<CalendarDate, PaperLedgerError> {
    use chrono::Datelike as _;
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(at.unix_nanos())
        .with_timezone(&chrono_tz::America::New_York).date_naive();
    CalendarDate::new(u16::try_from(date.year()).map_err(|_| PaperLedgerError::InvalidActionEvidence)?,
        u8::try_from(date.month()).map_err(|_| PaperLedgerError::InvalidActionEvidence)?,
        u8::try_from(date.day()).map_err(|_| PaperLedgerError::InvalidActionEvidence)?)
        .map_err(|_| PaperLedgerError::InvalidActionEvidence)
}

pub(super) fn source_action_date(at: Timestamp, us_equity: bool) -> Result<CalendarDate, PaperLedgerError> {
    if us_equity { us_equity_action_date(at) } else { at.utc_calendar_date().map_err(|_| PaperLedgerError::InvalidActionEvidence) }
}

/// Financial effects actually applicable to originally held inventory, independent of a later
/// source capture revision. Payment crossing changes the digest even when terms are unchanged.
fn effective_economics_digest(plan: &CorporateActionPlan, origin: Timestamp, valued_at: Timestamp) -> Result<[u8;32], PaperLedgerError> {
    use sha2::{Digest as _, Sha256};
    struct HashWriter(Sha256);
    impl std::io::Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> { self.0.update(bytes); Ok(bytes.len()) }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    if plan.steps().len() > MAXIMUM_ACTIONS { return Err(PaperLedgerError::Capacity); }
    let mut effects = Vec::new();
    effects.try_reserve_exact(plan.steps().len()).map_err(|_| PaperLedgerError::Capacity)?;
    for step in plan.steps() {
        let record = plan.admitted().get(step_index(step)).ok_or(PaperLedgerError::InvalidActionEvidence)?;
        let application = record.application().ok_or(PaperLedgerError::InvalidActionEvidence)?;
        let at = application.application_at();
        if at < origin { continue; }
        if at > valued_at { return Err(PaperLedgerError::InvalidActionEvidence); }
        let instrument = record.observation().context().provenance().instrument_id().ok_or(PaperLedgerError::InvalidActionEvidence)?;
        let settlement = application.simulated_cash_settlement_at();
        let mut hash = HashWriter(Sha256::new());
        serde_json::to_writer(&mut hash, &(instrument, application.source_date(), at,
            record.observation().action(), application.payable_date(), application.payment_policy(),
            settlement, settlement.is_some_and(|at| at <= valued_at)))
            .map_err(|_| PaperLedgerError::InvalidActionEvidence)?;
        let effect: [u8;32] = hash.0.finalize().into(); effects.push(effect);
    }
    effects.sort_unstable();
    let mut hash = Sha256::new(); hash.update(b"market-squawk/paper-effective-source-economics/v1\0");
    hash.update(origin.unix_nanos().to_be_bytes()); hash.update(plan.policy().version().get().to_be_bytes());
    hash.update([match plan.policy().adjustment() { market_squawk_data::CorporateActionAdjustment::Raw => 0,
        market_squawk_data::CorporateActionAdjustment::SplitAdjusted => 1, market_squawk_data::CorporateActionAdjustment::TotalReturn => 2 }]);
    hash.update((effects.len() as u64).to_be_bytes());
    for effect in effects { hash.update(effect); }
    Ok(hash.finalize().into())
}
