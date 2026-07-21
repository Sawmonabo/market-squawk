//! Dispatcher-authorized complete account-state replacement.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, TryLockError};

use market_squawk_domain::{AccountId, OrderId};
use thiserror::Error;

use super::{AccountRiskCoordinator, AccountState, partition_index};
use crate::{
    ExecutionStateSourceBinding, MAX_RECONCILED_ACCOUNTS, OrderIntentDigest, ReconciledAccountState,
};

/// Exact current reservation identity bound into one dispatcher reconciliation invocation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct AccountReplacementReservationBinding {
    order_id: OrderId,
    intent_digest: OrderIntentDigest,
    account_revision: u64,
}

impl AccountReplacementReservationBinding {
    pub(crate) const fn new(
        order_id: OrderId,
        intent_digest: OrderIntentDigest,
        account_revision: u64,
    ) -> Self {
        Self {
            order_id,
            intent_digest,
            account_revision,
        }
    }
}

/// One complete account candidate plus the exact leases it supersedes.
#[derive(Debug)]
pub(crate) struct AccountReplacementCandidate {
    state: ReconciledAccountState,
    expected_revision: u64,
    reservations: Box<[AccountReplacementReservationBinding]>,
}

impl AccountReplacementCandidate {
    pub(crate) fn try_new(
        state: ReconciledAccountState,
        expected_revision: u64,
        mut reservations: Vec<AccountReplacementReservationBinding>,
    ) -> Result<Self, AccountReplacementError> {
        reservations.sort_unstable();
        if expected_revision == 0
            || reservations.is_empty()
            || reservations.windows(2).any(|pair| pair[0] == pair[1])
            || reservations
                .iter()
                .any(|binding| binding.account_revision != expected_revision)
        {
            return Err(AccountReplacementError::InvalidReservationClosure);
        }
        Ok(Self {
            state,
            expected_revision,
            reservations: reservations.into_boxed_slice(),
        })
    }

    fn try_new_unreserved(
        state: ReconciledAccountState,
        expected_revision: u64,
    ) -> Result<Self, AccountReplacementError> {
        if expected_revision == 0 {
            return Err(AccountReplacementError::InvalidReservationClosure);
        }
        Ok(Self {
            state,
            expected_revision,
            reservations: Box::new([]),
        })
    }

    pub(crate) const fn account_id(&self) -> AccountId {
        self.state.account_id()
    }
}

/// One bounded all-before-mutation dispatcher replacement transaction.
#[derive(Debug)]
pub(crate) struct AccountStateReplacementBatch {
    source: ExecutionStateSourceBinding,
    invocation_digest: [u8; 32],
    candidates: Box<[AccountReplacementCandidate]>,
}

impl AccountStateReplacementBatch {
    pub(crate) fn try_new(
        source: ExecutionStateSourceBinding,
        invocation_digest: [u8; 32],
        mut candidates: Vec<AccountReplacementCandidate>,
    ) -> Result<Self, AccountReplacementError> {
        if candidates.is_empty()
            || candidates.len() > MAX_RECONCILED_ACCOUNTS
            || invocation_digest == [0; 32]
        {
            return Err(AccountReplacementError::InvalidAccountClosure);
        }
        candidates.sort_unstable_by_key(AccountReplacementCandidate::account_id);
        if candidates
            .windows(2)
            .any(|pair| pair[0].account_id() == pair[1].account_id())
        {
            return Err(AccountReplacementError::InvalidAccountClosure);
        }
        Ok(Self {
            source,
            invocation_digest,
            candidates: candidates.into_boxed_slice(),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AccountReplacementSource {
    configuration_digest: [u8; 32],
    snapshot_sequence: u64,
    snapshot_digest: [u8; 32],
    invocation_digest: [u8; 32],
}

#[derive(Debug)]
struct PreparedAccountState {
    account_id: AccountId,
    revision: u64,
    eligible: bool,
    cash: market_squawk_domain::Money,
    settled_capital: market_squawk_domain::Money,
    capital: market_squawk_domain::Money,
    peak_capital: market_squawk_domain::Money,
    gross_exposure: market_squawk_domain::Money,
    unrealized_pnl: market_squawk_domain::Money,
    drawdown: market_squawk_domain::Money,
    mark_digest: [u8; 32],
    realized_pnl: market_squawk_domain::Money,
    realized_loss: market_squawk_domain::Money,
    positions: HashMap<market_squawk_domain::InstrumentId, i64>,
    position_cost_basis: HashMap<market_squawk_domain::InstrumentId, market_squawk_domain::Money>,
}

impl AccountRiskCoordinator {
    pub(crate) fn replace_unreserved_reconciled_accounts(
        &self,
        source: ExecutionStateSourceBinding,
        invocation_digest: [u8; 32],
        states: &[ReconciledAccountState],
    ) -> Result<(), AccountReplacementError> {
        if states.is_empty() || states.len() > MAX_RECONCILED_ACCOUNTS {
            return Err(AccountReplacementError::InvalidAccountClosure);
        }
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(states.len())
            .map_err(|_| AccountReplacementError::Allocation)?;
        for state in states {
            let index = partition_index(state.account_id(), self.config.partition_count.get());
            let partition = match self.partitions[index].try_lock() {
                Ok(partition) => partition,
                Err(TryLockError::WouldBlock) => return Err(AccountReplacementError::Busy),
                Err(TryLockError::Poisoned(_)) => return Err(AccountReplacementError::Poisoned),
            };
            let current = partition
                .accounts
                .get(&state.account_id())
                .ok_or(AccountReplacementError::AccountNotFound)?;
            if current
                .reservations
                .iter()
                .any(|reservation| reservation.retained())
            {
                return Err(AccountReplacementError::InvalidReservationClosure);
            }
            let current_revision = current.account_revision.load(Ordering::Acquire);
            if state.revision().get() <= current_revision {
                if !current.matches_reconciled(state) {
                    return Err(AccountReplacementError::RevisionRollback);
                }
                continue;
            }
            candidates.push(AccountReplacementCandidate::try_new_unreserved(
                state.clone(),
                current_revision,
            )?);
        }
        if candidates.is_empty() {
            return Ok(());
        }
        self.replace_reconciled_accounts(AccountStateReplacementBatch::try_new(
            source,
            invocation_digest,
            candidates,
        )?)
    }

    pub(crate) fn replace_reconciled_accounts(
        &self,
        batch: AccountStateReplacementBatch,
    ) -> Result<(), AccountReplacementError> {
        let mut partition_indices = Vec::new();
        partition_indices
            .try_reserve_exact(batch.candidates.len())
            .map_err(|_| AccountReplacementError::Allocation)?;
        partition_indices.extend(batch.candidates.iter().map(|candidate| {
            partition_index(candidate.account_id(), self.config.partition_count.get())
        }));
        partition_indices.sort_unstable();
        partition_indices.dedup();

        let mut guards = Vec::new();
        guards
            .try_reserve_exact(partition_indices.len())
            .map_err(|_| AccountReplacementError::Allocation)?;
        for index in partition_indices {
            let guard = match self.partitions[index].try_lock() {
                Ok(guard) => guard,
                Err(TryLockError::WouldBlock) => return Err(AccountReplacementError::Busy),
                Err(TryLockError::Poisoned(_)) => return Err(AccountReplacementError::Poisoned),
            };
            guards.push((index, guard));
        }

        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(batch.candidates.len())
            .map_err(|_| AccountReplacementError::Allocation)?;
        for candidate in &batch.candidates {
            let partition =
                partition_index(candidate.account_id(), self.config.partition_count.get());
            let (_, guard) = guards
                .iter()
                .find(|(index, _)| *index == partition)
                .ok_or(AccountReplacementError::AccountNotFound)?;
            let current = guard
                .accounts
                .get(&candidate.account_id())
                .ok_or(AccountReplacementError::AccountNotFound)?;
            prepared.push(prepare_candidate(
                current,
                candidate,
                batch.source,
                batch.invocation_digest,
                self.config.max_positions_per_account.get(),
            )?);
        }

        let source = AccountReplacementSource {
            configuration_digest: batch.source.configuration_digest(),
            snapshot_sequence: batch.source.snapshot_sequence().get(),
            snapshot_digest: batch.source.snapshot_digest(),
            invocation_digest: batch.invocation_digest,
        };
        for replacement in prepared {
            let partition =
                partition_index(replacement.account_id, self.config.partition_count.get());
            let (_, guard) = guards
                .iter_mut()
                .find(|(index, _)| *index == partition)
                .ok_or(AccountReplacementError::AccountNotFound)?;
            let current = guard
                .accounts
                .get_mut(&replacement.account_id)
                .ok_or(AccountReplacementError::AccountNotFound)?;

            // This is the transaction's first mutation. Every allocation and validation for every
            // affected account is complete. Old leases first observe a revision mismatch and an
            // isolated reconciliation latch; only the newly published state receives a clear latch.
            current
                .account_revision
                .store(replacement.revision, Ordering::Release);
            current
                .reconciliation_required
                .store(true, Ordering::Release);
            current.reservations.clear();
            current.eligible = replacement.eligible;
            current.cash = replacement.cash;
            current.settled_capital = replacement.settled_capital;
            current.capital = replacement.capital;
            current.peak_capital = replacement.peak_capital;
            current.gross_exposure = replacement.gross_exposure;
            current.unrealized_pnl = replacement.unrealized_pnl;
            current.drawdown = replacement.drawdown;
            current.mark_digest = replacement.mark_digest;
            current.realized_pnl = replacement.realized_pnl;
            current.realized_loss = replacement.realized_loss;
            current.positions = replacement.positions;
            current.position_cost_basis = replacement.position_cost_basis;
            current.account_revision = Arc::new(AtomicU64::new(replacement.revision));
            current.reconciliation_required = Arc::new(AtomicBool::new(false));
            current.last_reconciliation = Some(source);
        }
        Ok(())
    }
}

impl AccountState {
    fn matches_reconciled(&self, state: &ReconciledAccountState) -> bool {
        self.eligible == state.eligible()
            && self.currency == state.currency()
            && self.cash == state.cash()
            && self.settled_capital == state.settled_capital()
            && self.capital == state.marked_equity()
            && self.peak_capital == state.peak_marked_equity()
            && self.gross_exposure == state.marked_gross_exposure()
            && self.unrealized_pnl == state.unrealized_pnl()
            && self.drawdown == state.drawdown()
            && self.mark_digest == state.mark_digest()
            && self.realized_pnl == state.realized_pnl()
            && self.realized_loss == state.realized_loss()
            && self.positions.len() == state.positions().len()
            && state
                .positions()
                .iter()
                .all(|(instrument, lots)| self.positions.get(instrument) == Some(lots))
            && self.position_cost_basis.len() == state.position_cost_basis().len()
            && state
                .position_cost_basis()
                .iter()
                .all(|(instrument, basis)| self.position_cost_basis.get(instrument) == Some(basis))
    }
}

fn prepare_candidate(
    current: &AccountState,
    candidate: &AccountReplacementCandidate,
    source: ExecutionStateSourceBinding,
    invocation_digest: [u8; 32],
    maximum_positions: usize,
) -> Result<PreparedAccountState, AccountReplacementError> {
    let current_revision = current.account_revision.load(Ordering::Acquire);
    if current_revision != candidate.expected_revision
        || candidate.state.revision().get() <= current_revision
    {
        return Err(AccountReplacementError::RevisionRollback);
    }
    if candidate.state.currency() != current.currency
        || candidate.state.cash().currency() != current.currency
    {
        return Err(AccountReplacementError::CurrencyMismatch);
    }
    if candidate.state.peak_capital().amount() < current.peak_capital.amount()
        || candidate.state.realized_loss().amount() < current.realized_loss.amount()
    {
        return Err(AccountReplacementError::FinancialRollback);
    }
    if let Some(previous) = current.last_reconciliation
        && (previous.configuration_digest != source.configuration_digest()
            || source.snapshot_sequence().get() <= previous.snapshot_sequence)
    {
        return Err(AccountReplacementError::SourceRollbackOrMismatch);
    }
    if let Some(previous) = current.last_reconciliation
        && (previous.snapshot_digest == source.snapshot_digest()
            || previous.invocation_digest == invocation_digest)
    {
        return Err(AccountReplacementError::SourceRollbackOrMismatch);
    }

    let retained_count = current
        .reservations
        .iter()
        .filter(|reservation| reservation.retained())
        .count();
    let mut observed_reservations = Vec::new();
    observed_reservations
        .try_reserve_exact(retained_count)
        .map_err(|_| AccountReplacementError::Allocation)?;
    observed_reservations.extend(
        current
            .reservations
            .iter()
            .filter(|reservation| reservation.retained())
            .map(|reservation| {
                AccountReplacementReservationBinding::new(
                    reservation.order_id,
                    reservation.intent_digest,
                    reservation.lease.expected_account_revision(),
                )
            }),
    );
    observed_reservations.sort_unstable();
    if observed_reservations.as_slice() != candidate.reservations.as_ref() {
        return Err(AccountReplacementError::InvalidReservationClosure);
    }
    if candidate.state.positions().len() > maximum_positions {
        return Err(AccountReplacementError::Capacity);
    }
    let mut positions = HashMap::new();
    positions
        .try_reserve(candidate.state.positions().len())
        .map_err(|_| AccountReplacementError::Allocation)?;
    for (instrument_id, lots) in candidate.state.positions() {
        if positions.insert(*instrument_id, *lots).is_some() {
            return Err(AccountReplacementError::InvalidAccountClosure);
        }
    }
    let mut position_cost_basis = HashMap::new();
    position_cost_basis
        .try_reserve(candidate.state.position_cost_basis().len())
        .map_err(|_| AccountReplacementError::Allocation)?;
    for (instrument_id, basis) in candidate.state.position_cost_basis() {
        if position_cost_basis.insert(*instrument_id, *basis).is_some() {
            return Err(AccountReplacementError::InvalidAccountClosure);
        }
    }
    Ok(PreparedAccountState {
        account_id: candidate.account_id(),
        revision: candidate.state.revision().get(),
        eligible: candidate.state.eligible(),
        cash: candidate.state.cash(),
        settled_capital: candidate.state.settled_capital(),
        capital: candidate.state.capital(),
        peak_capital: candidate.state.peak_capital(),
        gross_exposure: candidate.state.gross_exposure(),
        unrealized_pnl: candidate.state.unrealized_pnl(),
        drawdown: candidate.state.drawdown(),
        mark_digest: candidate.state.mark_digest(),
        realized_pnl: candidate.state.realized_pnl(),
        realized_loss: candidate.state.realized_loss(),
        positions,
        position_cost_basis,
    })
}

/// Fail-closed authoritative account replacement failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum AccountReplacementError {
    #[error("account replacement affected-account closure is invalid")]
    InvalidAccountClosure,
    #[error("account replacement reservation closure is invalid")]
    InvalidReservationClosure,
    #[error("account replacement target does not exist")]
    AccountNotFound,
    #[error("account replacement partition is busy")]
    Busy,
    #[error("account replacement partition is poisoned")]
    Poisoned,
    #[error("account replacement revision is stale or non-monotonic")]
    RevisionRollback,
    #[error("account replacement source configuration or sequence regressed")]
    SourceRollbackOrMismatch,
    #[error("account replacement currency does not match authoritative state")]
    CurrencyMismatch,
    #[error("account replacement cumulative financial state regressed")]
    FinancialRollback,
    #[error("account replacement exceeds coordinator capacity")]
    Capacity,
    #[error("account replacement bounded allocation failed")]
    Allocation,
}

#[cfg(test)]
mod tests;
