//! Complete bounded account images returned only through configured adapter reconciliation.

use std::num::NonZeroU64;

use market_squawk_domain::{AccountId, Currency, InstrumentId, Money};
use thiserror::Error;

/// Current complete account-replacement schema.
pub const ACCOUNT_REPLACEMENT_SCHEMA_VERSION: u32 = 2;

/// Maximum account images returned by one bounded reconciliation call.
pub const MAX_RECONCILED_ACCOUNTS: usize = 256;

/// Maximum complete positions retained for one reconciled account.
pub const MAX_RECONCILED_POSITIONS_PER_ACCOUNT: usize = 4_096;

/// Exact backend state identity that the dispatcher binds to its own reconciliation invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionStateSourceBinding {
    schema_version: u32,
    configuration_digest: [u8; 32],
    snapshot_sequence: NonZeroU64,
    snapshot_digest: [u8; 32],
}

impl ExecutionStateSourceBinding {
    /// Validates the closed schema and nonzero source sequence.
    pub fn try_new(
        schema_version: u32,
        configuration_digest: [u8; 32],
        snapshot_sequence: NonZeroU64,
        snapshot_digest: [u8; 32],
    ) -> Result<Self, ReconciledAccountStateError> {
        if schema_version != ACCOUNT_REPLACEMENT_SCHEMA_VERSION {
            return Err(ReconciledAccountStateError::UnsupportedSchema);
        }
        if configuration_digest == [0; 32] || snapshot_digest == [0; 32] {
            return Err(ReconciledAccountStateError::InvalidDigest);
        }
        Ok(Self {
            schema_version,
            configuration_digest,
            snapshot_sequence,
            snapshot_digest,
        })
    }

    pub const fn schema_version(self) -> u32 {
        self.schema_version
    }
    pub const fn configuration_digest(self) -> [u8; 32] {
        self.configuration_digest
    }
    pub const fn snapshot_sequence(self) -> NonZeroU64 {
        self.snapshot_sequence
    }
    pub const fn snapshot_digest(self) -> [u8; 32] {
        self.snapshot_digest
    }
}

/// Complete authoritative state for one account affected by the requested reconciliation set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciledAccountState {
    account_id: AccountId,
    revision: NonZeroU64,
    eligible: bool,
    currency: Currency,
    cash: Money,
    capital: Money,
    peak_capital: Money,
    gross_exposure: Money,
    realized_pnl: Money,
    realized_loss: Money,
    positions: Box<[(InstrumentId, i64)]>,
    position_cost_basis: Box<[(InstrumentId, Money)]>,
}

impl ReconciledAccountState {
    /// Validates a complete bounded single-currency account image.
    #[allow(
        clippy::too_many_arguments,
        reason = "complete account replacement keeps independent financial dimensions explicit"
    )]
    pub fn try_new(
        account_id: AccountId,
        revision: NonZeroU64,
        eligible: bool,
        currency: Currency,
        cash: Money,
        capital: Money,
        peak_capital: Money,
        gross_exposure: Money,
        realized_pnl: Money,
        realized_loss: Money,
        mut positions: Vec<(InstrumentId, i64)>,
        mut position_cost_basis: Vec<(InstrumentId, Money)>,
    ) -> Result<Self, ReconciledAccountStateError> {
        if positions.len() > MAX_RECONCILED_POSITIONS_PER_ACCOUNT
            || position_cost_basis.len() > MAX_RECONCILED_POSITIONS_PER_ACCOUNT
        {
            return Err(ReconciledAccountStateError::TooManyPositions);
        }
        let money = [cash, capital, peak_capital, gross_exposure, realized_loss];
        if money
            .iter()
            .any(|value| value.currency() != currency || value.amount().is_sign_negative())
            || realized_pnl.currency() != currency
            || capital.amount().is_zero()
            || peak_capital.amount() < capital.amount()
        {
            return Err(ReconciledAccountStateError::InvalidFinancialState);
        }
        positions.sort_unstable_by_key(|(instrument_id, _)| *instrument_id);
        if positions.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(ReconciledAccountStateError::DuplicatePosition);
        }
        position_cost_basis.sort_unstable_by_key(|(instrument_id, _)| *instrument_id);
        if position_cost_basis
            .windows(2)
            .any(|pair| pair[0].0 == pair[1].0)
            || positions.len() != position_cost_basis.len()
            || positions.iter().zip(&position_cost_basis).any(
                |((position_id, lots), (basis_id, basis))| {
                    position_id != basis_id
                        || basis.currency() != currency
                        || basis.amount().is_sign_negative()
                        || (*lots == 0 && !basis.amount().is_zero())
                        || (*lots != 0 && basis.amount().is_zero())
                },
            )
        {
            return Err(ReconciledAccountStateError::InvalidPositionCostBasis);
        }
        Ok(Self {
            account_id,
            revision,
            eligible,
            currency,
            cash,
            capital,
            peak_capital,
            gross_exposure,
            realized_pnl,
            realized_loss,
            positions: positions.into_boxed_slice(),
            position_cost_basis: position_cost_basis.into_boxed_slice(),
        })
    }

    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }
    pub const fn revision(&self) -> NonZeroU64 {
        self.revision
    }
    pub const fn eligible(&self) -> bool {
        self.eligible
    }
    pub const fn currency(&self) -> Currency {
        self.currency
    }
    pub const fn cash(&self) -> Money {
        self.cash
    }
    pub const fn capital(&self) -> Money {
        self.capital
    }
    pub const fn peak_capital(&self) -> Money {
        self.peak_capital
    }
    pub const fn gross_exposure(&self) -> Money {
        self.gross_exposure
    }
    pub const fn realized_pnl(&self) -> Money {
        self.realized_pnl
    }
    pub const fn realized_loss(&self) -> Money {
        self.realized_loss
    }
    pub const fn positions(&self) -> &[(InstrumentId, i64)] {
        &self.positions
    }
    pub const fn position_cost_basis(&self) -> &[(InstrumentId, Money)] {
        &self.position_cost_basis
    }
}

/// Invalid complete account reconciliation image.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ReconciledAccountStateError {
    #[error("account replacement schema is unsupported")]
    UnsupportedSchema,
    #[error("account replacement exceeded the hard position bound")]
    TooManyPositions,
    #[error("account replacement financial state is invalid")]
    InvalidFinancialState,
    #[error("account replacement contains a duplicate instrument position")]
    DuplicatePosition,
    #[error("account replacement position cost basis is incomplete or invalid")]
    InvalidPositionCostBasis,
    #[error("account replacement source digest must be nonzero")]
    InvalidDigest,
}
