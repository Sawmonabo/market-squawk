//! Monotonic backend-to-risk financial reconciliation fence.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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
    publication_owned: AtomicBool,
}

/// Try-only linearization guard shared by reservation publication and backend financial fencing.
#[derive(Debug)]
pub(crate) struct AccountReservationPublication {
    state: Arc<AccountRiskReconciliationState>,
}

impl AccountRiskReconciliationFence {
    pub(super) fn new(applied_sequence: u64) -> Self {
        Self {
            state: Arc::new(AccountRiskReconciliationState {
                required_sequence: AtomicU64::new(applied_sequence),
                applied_sequence: AtomicU64::new(applied_sequence),
                publication_owned: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn try_begin_reservation_publication(
        &self,
    ) -> Result<AccountReservationPublication, AccountReconciliationFenceError> {
        self.state
            .publication_owned
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| AccountReconciliationFenceError::PublicationBusy)?;
        Ok(AccountReservationPublication {
            state: Arc::clone(&self.state),
        })
    }

    /// Fences subsequent account reservations before the backend commits this sequence.
    ///
    /// Duplicate/coalesced notifications are accepted. A sequence already superseded by an
    /// applied reconciliation is rejected as backend rollback.
    pub fn require(&self, sequence: NonZeroU64) -> Result<(), AccountReconciliationFenceError> {
        let _publication = self.try_begin_reservation_publication()?;
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

impl Drop for AccountReservationPublication {
    fn drop(&mut self) {
        self.state.publication_owned.store(false, Ordering::Release);
    }
}

/// Invalid backend financial sequence transition.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AccountReconciliationFenceError {
    #[error("backend financial sequence regressed behind applied risk state")]
    SequenceRollback,
    #[error("reconciled backend sequence does not match the required risk fence")]
    SequenceMismatch,
    #[error("account reservation publication is currently being committed")]
    PublicationBusy,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::{Arc, Barrier, mpsc};

    use super::{AccountReconciliationFenceError, AccountRiskReconciliationFence};

    #[test]
    fn financial_fence_cannot_cross_an_inflight_reservation_publication()
    -> Result<(), Box<dyn std::error::Error>> {
        let fence = AccountRiskReconciliationFence::new(0);
        let publication = fence.try_begin_reservation_publication()?;
        let start = Arc::new(Barrier::new(2));
        let (result_sender, result_receiver) = mpsc::channel();
        let worker_fence = fence.clone();
        let worker_start = Arc::clone(&start);
        let worker = std::thread::spawn(move || {
            worker_start.wait();
            let result = worker_fence.require(NonZeroU64::MIN);
            let _ignored = result_sender.send(result);
        });

        start.wait();
        let result = result_receiver.recv()?;
        assert_eq!(
            result,
            Err(AccountReconciliationFenceError::PublicationBusy)
        );
        assert!(fence.is_current());

        drop(publication);
        worker
            .join()
            .map_err(|_| std::io::Error::other("fence worker panicked"))?;
        fence.require(NonZeroU64::MIN)?;
        assert!(!fence.is_current());
        Ok(())
    }
}
