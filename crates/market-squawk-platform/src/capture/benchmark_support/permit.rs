//! Bounded setup-only capacity permits.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar};
use std::time::{Duration, Instant};

use super::types::BenchmarkSupportError;

const PERMIT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(super) struct PermitCoordinator {
    maximum: usize,
    state: std::sync::Mutex<PermitState>,
    changed: Condvar,
    failed: AtomicBool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PermitState {
    available: usize,
    closed: bool,
}

#[derive(Debug)]
pub(super) struct AcquiredPermit {
    coordinator: Arc<PermitCoordinator>,
    committed: bool,
}

impl PermitCoordinator {
    pub(super) fn new(
        maximum: NonZeroUsize,
        available: usize,
    ) -> Result<Self, BenchmarkSupportError> {
        if available > maximum.get() {
            return Err(BenchmarkSupportError::InvalidFixture);
        }
        Ok(Self {
            maximum: maximum.get(),
            state: std::sync::Mutex::new(PermitState {
                available,
                closed: false,
            }),
            changed: Condvar::new(),
            failed: AtomicBool::new(false),
        })
    }

    pub(super) fn acquire(self: &Arc<Self>) -> Result<AcquiredPermit, BenchmarkSupportError> {
        let deadline = Instant::now()
            .checked_add(PERMIT_TIMEOUT)
            .ok_or(BenchmarkSupportError::InvalidFixture)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        loop {
            if state.closed {
                return Err(BenchmarkSupportError::Reconciliation);
            }
            if state.available > 0 {
                state.available -= 1;
                return Ok(AcquiredPermit {
                    coordinator: Arc::clone(self),
                    committed: false,
                });
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(BenchmarkSupportError::PermitTimeout);
            }
            let (next, wait) = self
                .changed
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
            state = next;
            if wait.timed_out() && state.available == 0 {
                return Err(BenchmarkSupportError::PermitTimeout);
            }
        }
    }

    pub(super) fn release(&self) -> Result<(), BenchmarkSupportError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        if state.closed || state.available >= self.maximum {
            return Err(BenchmarkSupportError::ObservationInvariant);
        }
        state.available += 1;
        self.changed.notify_one();
        Ok(())
    }

    pub(super) fn available(&self) -> Result<usize, BenchmarkSupportError> {
        if self.failed.load(Ordering::Acquire) {
            return Err(BenchmarkSupportError::ObservationInvariant);
        }
        self.state
            .lock()
            .map(|state| state.available)
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)
    }

    pub(super) const fn maximum(&self) -> usize {
        self.maximum
    }

    pub(super) fn close(&self) -> Result<(), BenchmarkSupportError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        state.closed = true;
        self.changed.notify_all();
        Ok(())
    }
}

impl AcquiredPermit {
    pub(super) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for AcquiredPermit {
    fn drop(&mut self) {
        if !self.committed && self.coordinator.release().is_err() {
            self.coordinator.failed.store(true, Ordering::Release);
        }
    }
}
