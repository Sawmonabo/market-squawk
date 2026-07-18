//! Linearized admission and closure for queue operations.

#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};

const CLOSED: usize = 1_usize << (usize::BITS - 1);
const ACTIVE_MASK: usize = CLOSED - 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::capture) enum OperationRegistrationError {
    Closed,
    CountOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::capture) enum OperationFinishError {
    CounterUnderflow,
}

#[derive(Debug)]
pub(in crate::capture) struct OperationLifecycle {
    state: AtomicUsize,
}

impl OperationLifecycle {
    pub(in crate::capture) fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
        }
    }

    pub(in crate::capture) fn begin(&self) -> Result<(), OperationRegistrationError> {
        let mut observed = self.state.load(Ordering::Acquire);
        loop {
            if observed & CLOSED != 0 {
                return Err(OperationRegistrationError::Closed);
            }
            if observed & ACTIVE_MASK == ACTIVE_MASK {
                self.close_registration();
                return Err(OperationRegistrationError::CountOverflow);
            }
            let next = observed + 1;
            match self.state.compare_exchange_weak(
                observed,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_previous) => return Ok(()),
                Err(current) => observed = current,
            }
        }
    }

    pub(in crate::capture) fn close_registration(&self) {
        self.state.fetch_or(CLOSED, Ordering::AcqRel);
    }

    pub(in crate::capture) fn active_operations(&self) -> usize {
        self.state.load(Ordering::Acquire) & ACTIVE_MASK
    }

    pub(in crate::capture) fn is_terminally_closed(&self) -> bool {
        let state = self.state.load(Ordering::Acquire);
        state & CLOSED != 0 && state & ACTIVE_MASK == 0
    }

    /// Finishes one admitted operation and reports whether it made closure terminal.
    pub(in crate::capture) fn finish(&self) -> Result<bool, OperationFinishError> {
        self.state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                let active = state & ACTIVE_MASK;
                active.checked_sub(1).map(|next| (state & CLOSED) | next)
            })
            .map(|previous| previous & CLOSED != 0 && previous & ACTIVE_MASK == 1)
            .map_err(|_current| OperationFinishError::CounterUnderflow)
    }

    #[cfg(test)]
    pub(super) fn set_active_operations_for_test(&self, value: usize) {
        self.state.store(value.min(ACTIVE_MASK), Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn maximum_active_operations_for_test(&self) -> usize {
        ACTIVE_MASK
    }

    #[cfg(all(test, feature = "capture-benchmark"))]
    pub(in crate::capture) fn registration_is_closed_for_test(&self) -> bool {
        self.state.load(Ordering::Acquire) & CLOSED != 0
    }
}
