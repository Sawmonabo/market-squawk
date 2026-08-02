//! Versioned complete paper account-state projections.

use std::num::NonZeroU64;

use market_squawk_domain::{AccountId, Currency, InstrumentId, Money};
use market_squawk_execution::ReconciledAccountState;

use super::{PaperLedger, PaperLedgerError};

/// One initial account image supplied by trusted local configuration or recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperAccountBootstrap {
    pub account_id: AccountId,
    pub revision: NonZeroU64,
    pub eligible: bool,
    pub cash: Vec<Money>,
    pub capital: Money,
    pub peak_capital: Money,
    pub gross_exposure: Money,
    pub realized_loss: Money,
    /// Signed cumulative realized trading profit and loss, excluding fees.
    pub realized_pnl: Money,
    /// Signed lots by stable instrument identity.
    pub positions: Vec<(InstrumentId, i64)>,
    /// Nonnegative open-cost basis keyed exactly to every retained position.
    pub position_cost_basis: Vec<(InstrumentId, Money)>,
}

/// Versioned financial-risk dimensions retained with a complete paper snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaperAccountRiskSnapshot {
    account_id: AccountId,
    revision: NonZeroU64,
    eligible: bool,
    currency: Currency,
    settled_capital: Money,
    marked_equity: Money,
    peak_marked_equity: Money,
    marked_gross_exposure: Money,
    unrealized_pnl: Money,
    drawdown: Money,
    mark_digest: [u8; 32],
    realized_loss: Money,
    realized_pnl: Money,
}

impl PaperAccountRiskSnapshot {
    pub const fn account_id(self) -> AccountId {
        self.account_id
    }
    pub const fn revision(self) -> NonZeroU64 {
        self.revision
    }
    pub const fn eligible(self) -> bool {
        self.eligible
    }
    pub const fn currency(self) -> Currency {
        self.currency
    }
    pub const fn settled_capital(self) -> Money {
        self.settled_capital
    }
    pub const fn marked_equity(self) -> Money {
        self.marked_equity
    }
    pub const fn peak_marked_equity(self) -> Money {
        self.peak_marked_equity
    }
    pub const fn marked_gross_exposure(self) -> Money {
        self.marked_gross_exposure
    }
    pub const fn unrealized_pnl(self) -> Money {
        self.unrealized_pnl
    }
    pub const fn drawdown(self) -> Money {
        self.drawdown
    }
    pub const fn mark_digest(self) -> [u8; 32] {
        self.mark_digest
    }
    pub const fn capital(self) -> Money {
        self.marked_equity
    }
    pub const fn peak_capital(self) -> Money {
        self.peak_marked_equity
    }
    pub const fn gross_exposure(self) -> Money {
        self.marked_gross_exposure
    }
    pub const fn realized_loss(self) -> Money {
        self.realized_loss
    }

    pub const fn realized_pnl(self) -> Money {
        self.realized_pnl
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PaperAccountRiskState {
    pub(super) revision: NonZeroU64,
    pub(super) eligible: bool,
    pub(super) currency: Currency,
    pub(super) settled_capital: Money,
    pub(super) marked_equity: Money,
    pub(super) peak_marked_equity: Money,
    pub(super) marked_gross_exposure: Money,
    pub(super) unrealized_pnl: Money,
    pub(super) drawdown: Money,
    pub(super) mark_digest: [u8; 32],
    pub(super) realized_loss: Money,
    pub(super) realized_pnl: Money,
}

impl PaperAccountRiskSnapshot {
    pub(super) const fn new(account_id: AccountId, account: PaperAccountRiskState) -> Self {
        Self {
            account_id,
            revision: account.revision,
            eligible: account.eligible,
            currency: account.currency,
            settled_capital: account.settled_capital,
            marked_equity: account.marked_equity,
            peak_marked_equity: account.peak_marked_equity,
            marked_gross_exposure: account.marked_gross_exposure,
            unrealized_pnl: account.unrealized_pnl,
            drawdown: account.drawdown,
            mark_digest: account.mark_digest,
            realized_loss: account.realized_loss,
            realized_pnl: account.realized_pnl,
        }
    }
}

impl PaperLedger {
    pub(crate) fn account_risk_snapshot(&self) -> Vec<PaperAccountRiskSnapshot> {
        self.accounts
            .iter()
            .map(|(account_id, account)| PaperAccountRiskSnapshot::new(*account_id, *account))
            .collect()
    }

    pub(crate) fn reconciled_account_state(
        &self,
        account_id: AccountId,
        valued_at: market_squawk_domain::Timestamp,
        maximum_mark_age_nanos: u64,
    ) -> Result<ReconciledAccountState, PaperLedgerError> {
        let account = self
            .accounts
            .get(&account_id)
            .copied()
            .ok_or(PaperLedgerError::UnknownAccountOrCurrency)?;
        self.validate_account_marks(account_id, account, valued_at, maximum_mark_age_nanos)?;
        self.reconciled_account_state_projection(account_id, account)
    }

    fn reconciled_account_state_projection(
        &self,
        account_id: AccountId,
        account: PaperAccountRiskState,
    ) -> Result<ReconciledAccountState, PaperLedgerError> {
        let cash = self.cash(account_id, account.currency)?;
        let positions = self
            .positions
            .iter()
            .filter(|((candidate, _), _)| *candidate == account_id)
            .map(|((_, instrument_id), lots)| (*instrument_id, *lots))
            .collect();
        let position_cost_basis = self
            .position_cost_basis
            .iter()
            .filter(|((candidate, _), _)| *candidate == account_id)
            .map(|((_, instrument_id), amount)| {
                (*instrument_id, Money::new(*amount, account.currency))
            })
            .collect();
        ReconciledAccountState::try_new(
            account_id,
            account.revision,
            account.eligible,
            account.currency,
            cash,
            account.settled_capital,
            account.marked_equity,
            account.peak_marked_equity,
            account.marked_gross_exposure,
            account.unrealized_pnl,
            account.drawdown,
            account.mark_digest,
            account.realized_pnl,
            account.realized_loss,
            positions,
            position_cost_basis,
        )
        .map_err(|_| PaperLedgerError::InvalidRecovery)
    }

    pub(crate) fn recovery_reconciled_account_states(
        &self,
    ) -> Result<Vec<ReconciledAccountState>, PaperLedgerError> {
        let mut states = Vec::new();
        states
            .try_reserve_exact(self.accounts.len())
            .map_err(|_| PaperLedgerError::Capacity)?;
        for (account_id, account) in &self.accounts {
            states.push(self.reconciled_account_state_projection(*account_id, *account)?);
        }
        Ok(states)
    }

    pub(crate) fn reconciled_account_states(
        &self,
        valued_at: market_squawk_domain::Timestamp,
        maximum_mark_age_nanos: u64,
    ) -> Result<Vec<ReconciledAccountState>, PaperLedgerError> {
        let mut states = Vec::new();
        states
            .try_reserve_exact(self.accounts.len())
            .map_err(|_| PaperLedgerError::Capacity)?;
        for account_id in self.accounts.keys() {
            states.push(self.reconciled_account_state(
                *account_id,
                valued_at,
                maximum_mark_age_nanos,
            )?);
        }
        Ok(states)
    }
}
