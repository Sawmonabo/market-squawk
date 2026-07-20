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
    /// Signed lots by stable instrument identity.
    pub positions: Vec<(InstrumentId, i64)>,
}

/// Versioned financial-risk dimensions retained with a complete paper snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaperAccountRiskSnapshot {
    account_id: AccountId,
    revision: NonZeroU64,
    eligible: bool,
    currency: Currency,
    capital: Money,
    peak_capital: Money,
    gross_exposure: Money,
    realized_loss: Money,
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
    pub const fn capital(self) -> Money {
        self.capital
    }
    pub const fn peak_capital(self) -> Money {
        self.peak_capital
    }
    pub const fn gross_exposure(self) -> Money {
        self.gross_exposure
    }
    pub const fn realized_loss(self) -> Money {
        self.realized_loss
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PaperAccountRiskState {
    pub(super) revision: NonZeroU64,
    pub(super) eligible: bool,
    pub(super) currency: Currency,
    pub(super) capital: Money,
    pub(super) peak_capital: Money,
    pub(super) gross_exposure: Money,
    pub(super) realized_loss: Money,
}

impl PaperLedger {
    pub(crate) fn account_risk_snapshot(&self) -> Vec<PaperAccountRiskSnapshot> {
        self.accounts
            .iter()
            .map(|(account_id, account)| PaperAccountRiskSnapshot {
                account_id: *account_id,
                revision: account.revision,
                eligible: account.eligible,
                currency: account.currency,
                capital: account.capital,
                peak_capital: account.peak_capital,
                gross_exposure: account.gross_exposure,
                realized_loss: account.realized_loss,
            })
            .collect()
    }

    pub(crate) fn reconciled_account_state(
        &self,
        account_id: AccountId,
    ) -> Result<ReconciledAccountState, PaperLedgerError> {
        let account = self
            .accounts
            .get(&account_id)
            .copied()
            .ok_or(PaperLedgerError::UnknownAccountOrCurrency)?;
        let cash = self.cash(account_id, account.currency)?;
        let positions = self
            .positions
            .iter()
            .filter(|((candidate, _), _)| *candidate == account_id)
            .map(|((_, instrument_id), lots)| (*instrument_id, *lots))
            .collect();
        ReconciledAccountState::try_new(
            account_id,
            account.revision,
            account.eligible,
            account.currency,
            cash,
            account.capital,
            account.peak_capital,
            account.gross_exposure,
            account.realized_loss,
            positions,
        )
        .map_err(|_| PaperLedgerError::InvalidRecovery)
    }
}
