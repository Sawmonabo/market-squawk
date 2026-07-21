//! Monotonic backend-to-risk financial reconciliation fence.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;

/// Cloneable fail-closed notification authority held by the configured execution backend.
///
/// Requiring a sequence can only remove risk authority. Only the execution dispatcher can advance
/// the applied sequence after authoritative account replacement and backend acknowledgement.
#[derive(Clone, Debug)]
pub struct AccountRiskReconciliationFence {
    state: Arc<AccountRiskReconciliationState>,
}

#[derive(Debug)]
struct AccountRiskReconciliationState {
    required_sequence: AtomicU64,
    applied_sequence: AtomicU64,
}

impl AccountRiskReconciliationFence {
    pub(super) fn new(applied_sequence: u64) -> Self {
        Self {
            state: Arc::new(AccountRiskReconciliationState {
                required_sequence: AtomicU64::new(applied_sequence),
                applied_sequence: AtomicU64::new(applied_sequence),
            }),
        }
    }

    /// Fences subsequent account reservations before the backend commits this sequence.
    ///
    /// Duplicate/coalesced notifications are accepted. A sequence already superseded by an
    /// applied reconciliation is rejected as backend rollback.
    pub fn require(&self, sequence: NonZeroU64) -> Result<(), AccountReconciliationFenceError> {
        let sequence = sequence.get();
        if sequence <= self.state.applied_sequence.load(Ordering::Acquire) {
            return Err(AccountReconciliationFenceError::SequenceRollback);
        }
        self.state
            .required_sequence
            .fetch_max(sequence, Ordering::AcqRel);
        Ok(())
    }

    /// Returns the newest backend sequence that must be reconciled.
    pub fn required_sequence(&self) -> u64 {
        self.state.required_sequence.load(Ordering::Acquire)
    }

    /// Returns the exact backend sequence already applied to authoritative risk state.
    pub fn applied_sequence(&self) -> u64 {
        self.state.applied_sequence.load(Ordering::Acquire)
    }

    /// Reports whether authoritative risk state has caught up to every committed backend mutation.
    pub fn is_current(&self) -> bool {
        self.applied_sequence() >= self.required_sequence()
    }

    pub(crate) fn acknowledge(
        &self,
        sequence: NonZeroU64,
    ) -> Result<(), AccountReconciliationFenceError> {
        let sequence = sequence.get();
        let applied = self.applied_sequence();
        if sequence < applied {
            return Err(AccountReconciliationFenceError::SequenceMismatch);
        }
        self.state
            .required_sequence
            .fetch_max(sequence, Ordering::AcqRel);
        self.state
            .applied_sequence
            .fetch_max(sequence, Ordering::AcqRel);
        Ok(())
    }
}

/// Invalid backend financial sequence transition.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AccountReconciliationFenceError {
    #[error("backend financial sequence regressed behind applied risk state")]
    SequenceRollback,
    #[error("reconciled backend sequence does not match the required risk fence")]
    SequenceMismatch,
}
