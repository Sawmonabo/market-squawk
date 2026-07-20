//! Paper-order state contract.

use market_squawk_domain::QuantityLots;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Legal paper order states.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperOrderState {
    New,
    Accepted,
    PartiallyFilled,
    Filled,
    CancelPending,
    Canceled,
    Rejected,
    Expired,
}

/// Controlled paper lifecycle with no public state setter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperOrderLifecycle {
    state: PaperOrderState,
    requested: QuantityLots,
    cumulative_filled: QuantityLots,
    revision: u64,
    last_sequence: u64,
}

impl PaperOrderLifecycle {
    /// Creates a new lifecycle for a positive requested quantity.
    pub fn try_new(requested: QuantityLots) -> Result<Self, PaperStateError> {
        if requested.get() == 0 {
            return Err(PaperStateError::InvalidQuantity);
        }
        Ok(Self {
            state: PaperOrderState::New,
            requested,
            cumulative_filled: QuantityLots::new(0)
                .map_err(|_| PaperStateError::InvalidQuantity)?,
            revision: 0,
            last_sequence: 0,
        })
    }

    pub const fn state(&self) -> PaperOrderState {
        self.state
    }

    pub const fn requested(&self) -> QuantityLots {
        self.requested
    }

    pub const fn cumulative_filled(&self) -> QuantityLots {
        self.cumulative_filled
    }

    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub const fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    pub(crate) fn try_restore(
        state: PaperOrderState,
        requested: QuantityLots,
        cumulative_filled: QuantityLots,
        revision: u64,
        last_sequence: u64,
    ) -> Result<Self, PaperStateError> {
        let requested_lots = requested.get();
        let filled_lots = cumulative_filled.get();
        let shape_valid = requested_lots > 0
            && filled_lots <= requested_lots
            && revision <= last_sequence
            && match state {
                PaperOrderState::New => filled_lots == 0 && revision == 0 && last_sequence == 0,
                PaperOrderState::Accepted => filled_lots == 0 && revision > 0 && last_sequence > 0,
                PaperOrderState::PartiallyFilled => {
                    filled_lots > 0
                        && filled_lots < requested_lots
                        && revision > 0
                        && last_sequence > 0
                }
                PaperOrderState::Filled => {
                    filled_lots == requested_lots && revision > 0 && last_sequence > 0
                }
                PaperOrderState::CancelPending => {
                    filled_lots < requested_lots && revision > 0 && last_sequence > 0
                }
                PaperOrderState::Canceled | PaperOrderState::Expired => {
                    filled_lots < requested_lots && revision > 0 && last_sequence > 0
                }
                PaperOrderState::Rejected => filled_lots == 0 && revision > 0 && last_sequence > 0,
            };
        if !shape_valid {
            return Err(PaperStateError::InvalidTransition);
        }
        Ok(Self {
            state,
            requested,
            cumulative_filled,
            revision,
            last_sequence,
        })
    }

    pub fn accept(&mut self, sequence: u64) -> Result<(), PaperStateError> {
        self.transition_from_new(PaperOrderState::Accepted, sequence)
    }

    pub fn reject(&mut self, sequence: u64) -> Result<(), PaperStateError> {
        self.transition_from_new(PaperOrderState::Rejected, sequence)
    }

    pub fn request_cancel(&mut self, sequence: u64) -> Result<(), PaperStateError> {
        self.ensure_mutable(sequence)?;
        if !matches!(
            self.state,
            PaperOrderState::New | PaperOrderState::Accepted | PaperOrderState::PartiallyFilled
        ) {
            return Err(PaperStateError::InvalidTransition);
        }
        self.commit(PaperOrderState::CancelPending, sequence)
    }

    pub fn confirm_cancel(&mut self, sequence: u64) -> Result<(), PaperStateError> {
        self.ensure_mutable(sequence)?;
        if self.state != PaperOrderState::CancelPending {
            return Err(PaperStateError::InvalidTransition);
        }
        self.commit(PaperOrderState::Canceled, sequence)
    }

    pub fn expire(&mut self, sequence: u64) -> Result<(), PaperStateError> {
        self.ensure_mutable(sequence)?;
        self.commit(PaperOrderState::Expired, sequence)
    }

    pub fn apply_fill(
        &mut self,
        quantity: QuantityLots,
        sequence: u64,
    ) -> Result<(), PaperStateError> {
        self.ensure_mutable(sequence)?;
        if quantity.get() == 0
            || !matches!(
                self.state,
                PaperOrderState::Accepted
                    | PaperOrderState::PartiallyFilled
                    | PaperOrderState::CancelPending
            )
        {
            return Err(PaperStateError::InvalidTransition);
        }
        let cumulative = self
            .cumulative_filled
            .checked_add(quantity)
            .map_err(|_| PaperStateError::QuantityOverflow)?;
        if cumulative > self.requested {
            return Err(PaperStateError::Overfill);
        }
        let next = if cumulative == self.requested {
            PaperOrderState::Filled
        } else if self.state == PaperOrderState::CancelPending {
            PaperOrderState::CancelPending
        } else {
            PaperOrderState::PartiallyFilled
        };
        let revision = self.next_revision()?;
        self.cumulative_filled = cumulative;
        self.state = next;
        self.revision = revision;
        self.last_sequence = sequence;
        Ok(())
    }

    fn transition_from_new(
        &mut self,
        next: PaperOrderState,
        sequence: u64,
    ) -> Result<(), PaperStateError> {
        self.ensure_mutable(sequence)?;
        if self.state != PaperOrderState::New {
            return Err(PaperStateError::InvalidTransition);
        }
        self.commit(next, sequence)
    }

    fn ensure_mutable(&self, sequence: u64) -> Result<(), PaperStateError> {
        if self.is_terminal() {
            return Err(PaperStateError::Terminal);
        }
        if sequence <= self.last_sequence {
            return Err(PaperStateError::SequenceRegression);
        }
        Ok(())
    }

    fn commit(&mut self, next: PaperOrderState, sequence: u64) -> Result<(), PaperStateError> {
        let revision = self.next_revision()?;
        self.state = next;
        self.revision = revision;
        self.last_sequence = sequence;
        Ok(())
    }

    fn next_revision(&self) -> Result<u64, PaperStateError> {
        self.revision
            .checked_add(1)
            .ok_or(PaperStateError::RevisionExhausted)
    }

    const fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            PaperOrderState::Filled
                | PaperOrderState::Canceled
                | PaperOrderState::Rejected
                | PaperOrderState::Expired
        )
    }
}

/// Paper lifecycle validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PaperStateError {
    #[error("paper order quantity must be positive")]
    InvalidQuantity,
    #[error("paper order transition is not legal from the current state")]
    InvalidTransition,
    #[error("paper order is terminal")]
    Terminal,
    #[error("paper event sequence did not advance")]
    SequenceRegression,
    #[error("paper fill exceeds requested quantity")]
    Overfill,
    #[error("paper quantity arithmetic overflowed")]
    QuantityOverflow,
    #[error("paper order revision exhausted")]
    RevisionExhausted,
}
