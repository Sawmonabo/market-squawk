//! Unified fixed, resident-generation, and queued-record capture accounting.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};

use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AccountingComponent {
    Fixed,
    ResidentGeneration,
    Record,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CaptureAccountingStatus {
    Healthy = 0,
    TransitionOverflow = 1,
    EpochOverflow = 2,
    InvariantViolated = 3,
}

impl CaptureAccountingStatus {
    fn from_raw(raw: u8) -> Self {
        match raw {
            0 => Self::Healthy,
            1 => Self::TransitionOverflow,
            2 => Self::EpochOverflow,
            _ => Self::InvariantViolated,
        }
    }
}

const fn checked_transition_enter(current: usize) -> Option<usize> {
    current.checked_add(1)
}

const fn checked_transition_leave(current: usize) -> Option<usize> {
    current.checked_sub(1)
}

#[derive(Clone, Copy, Debug)]
struct CaptureSnapshotRead {
    status_before: u8,
    epoch_before: u64,
    active_before: usize,
    fixed: usize,
    resident: usize,
    record: usize,
    total: usize,
    active_after_components: usize,
    epoch_after: u64,
    active_final: usize,
    status_after: u8,
}

impl CaptureSnapshotRead {
    fn reconciles(self, ceiling: usize) -> bool {
        self.fixed
            .checked_add(self.resident)
            .and_then(|subtotal| subtotal.checked_add(self.record))
            .is_some_and(|sum| sum == self.total && sum <= ceiling)
    }

    const fn is_quiescent(self) -> bool {
        self.active_before == 0 && self.active_after_components == 0 && self.active_final == 0
    }

    fn is_coherent(self, ceiling: usize) -> bool {
        self.status_before == self.status_after
            && self.epoch_before == self.epoch_after
            && self.is_quiescent()
            && self.reconciles(ceiling)
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum CaptureAccountingError {
    #[error("capture memory ceiling exceeded: required {required} bytes, ceiling {ceiling} bytes")]
    BudgetExceeded { required: usize, ceiling: usize },
    #[error("capture accounting arithmetic overflowed")]
    ArithmeticOverflow,
    #[error("capture accounting transition count overflowed")]
    TransitionOverflow,
    #[error("capture accounting epoch overflowed")]
    EpochOverflow,
    #[error("capture accounting invariant was violated")]
    InvariantViolated,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
/// Failure to obtain one coherent bounded capture-accounting snapshot.
pub enum CaptureAccountingSnapshotError {
    #[error("capture accounting snapshot remained contended after {attempts} attempts")]
    Contended { attempts: NonZeroUsize },
    #[error("capture accounting transition count overflowed")]
    TransitionOverflow,
    #[error("capture accounting epoch overflowed")]
    EpochOverflow,
    #[error("capture accounting invariant was violated")]
    InvariantViolated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Coherent view of every component in the unified per-channel memory ledger.
pub struct CaptureAccountingSnapshot {
    completed_epoch: u64,
    fixed_capture_bytes: usize,
    resident_generation_bytes: usize,
    record_reservation_bytes: usize,
    total_accounted_bytes: usize,
    accounting_invariant_failures: u64,
}

impl CaptureAccountingSnapshot {
    /// Returns the epoch after the latest completed reserve or release transition.
    pub const fn completed_epoch(self) -> u64 {
        self.completed_epoch
    }

    /// Returns accounting-core plus currently resident fixed infrastructure bytes.
    pub const fn fixed_capture_bytes(self) -> usize {
        self.fixed_capture_bytes
    }

    /// Returns bytes retained by every still-reachable accounted generation.
    pub const fn resident_generation_bytes(self) -> usize {
        self.resident_generation_bytes
    }

    /// Returns dynamic record bytes reserved across queued and writer-owned records.
    pub const fn record_reservation_bytes(self) -> usize {
        self.record_reservation_bytes
    }

    /// Returns the authoritative reconciled sum of all three components.
    pub const fn total_accounted_bytes(self) -> usize {
        self.total_accounted_bytes
    }

    /// Returns the saturating count of detected accounting invariant failures.
    pub const fn accounting_invariant_failures(self) -> u64 {
        self.accounting_invariant_failures
    }
}

#[derive(Debug)]
pub(super) struct CaptureMemoryAccounting {
    configured_ceiling: NonZeroUsize,
    fixed_capture_bytes: AtomicUsize,
    resident_generation_bytes: AtomicUsize,
    record_reservation_bytes: AtomicUsize,
    total_accounted_bytes: AtomicUsize,
    accounting_invariant_failures: AtomicU64,
    active_transitions: AtomicUsize,
    completed_epoch: AtomicU64,
    status: AtomicU8,
}

impl CaptureMemoryAccounting {
    pub(super) const fn configured_ceiling(&self) -> NonZeroUsize {
        self.configured_ceiling
    }

    pub(super) fn try_new(
        initial_fixed_bytes: usize,
        configured_ceiling: NonZeroUsize,
    ) -> Result<Self, CaptureAccountingError> {
        if initial_fixed_bytes > configured_ceiling.get() {
            return Err(CaptureAccountingError::BudgetExceeded {
                required: initial_fixed_bytes,
                ceiling: configured_ceiling.get(),
            });
        }
        Ok(Self {
            configured_ceiling,
            fixed_capture_bytes: AtomicUsize::new(initial_fixed_bytes),
            resident_generation_bytes: AtomicUsize::new(0),
            record_reservation_bytes: AtomicUsize::new(0),
            total_accounted_bytes: AtomicUsize::new(initial_fixed_bytes),
            accounting_invariant_failures: AtomicU64::new(0),
            active_transitions: AtomicUsize::new(0),
            completed_epoch: AtomicU64::new(0),
            status: AtomicU8::new(CaptureAccountingStatus::Healthy as u8),
        })
    }

    pub(super) fn try_reserve(
        self: &Arc<Self>,
        component: AccountingComponent,
        bytes: usize,
    ) -> Result<CaptureMemoryReservation, CaptureAccountingError> {
        let mut transition = self.try_enter_transition()?;
        if let Err(error) = self.ensure_healthy() {
            transition.cancel()?;
            return Err(error);
        }
        let ceiling = self.configured_ceiling.get();
        let total_result = self.total_accounted_bytes.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |current| current.checked_add(bytes).filter(|next| *next <= ceiling),
        );
        if let Err(current) = total_result {
            transition.cancel()?;
            return match current.checked_add(bytes) {
                Some(required) => Err(CaptureAccountingError::BudgetExceeded { required, ceiling }),
                None => Err(CaptureAccountingError::ArithmeticOverflow),
            };
        }
        if self
            .component_counter(component)
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(bytes)
            })
            .is_err()
        {
            self.publish_terminal(CaptureAccountingStatus::InvariantViolated);
            transition.cancel()?;
            return Err(CaptureAccountingError::InvariantViolated);
        }
        transition.finish()?;
        Ok(CaptureMemoryReservation {
            accounting: Arc::clone(self),
            component,
            bytes,
            active: true,
        })
    }

    pub(super) fn try_snapshot(
        &self,
        max_attempts: NonZeroUsize,
    ) -> Result<CaptureAccountingSnapshot, CaptureAccountingSnapshotError> {
        for _attempt in 0..max_attempts.get() {
            let status_before = self.status.load(Ordering::SeqCst);
            self.snapshot_status(status_before)?;
            let epoch_before = self.completed_epoch.load(Ordering::SeqCst);
            let active_before = self.active_transitions.load(Ordering::SeqCst);
            let fixed = self.fixed_capture_bytes.load(Ordering::SeqCst);
            let resident = self.resident_generation_bytes.load(Ordering::SeqCst);
            let record = self.record_reservation_bytes.load(Ordering::SeqCst);
            let total = self.total_accounted_bytes.load(Ordering::SeqCst);
            let failures = self.accounting_invariant_failures.load(Ordering::SeqCst);
            let ceiling = self.configured_ceiling.get();
            let active_after_components = self.active_transitions.load(Ordering::SeqCst);
            let epoch_after = self.completed_epoch.load(Ordering::SeqCst);
            let active_final = self.active_transitions.load(Ordering::SeqCst);
            let status_after = self.status.load(Ordering::SeqCst);
            self.snapshot_status(status_after)?;
            let read = CaptureSnapshotRead {
                status_before,
                epoch_before,
                active_before,
                fixed,
                resident,
                record,
                total,
                active_after_components,
                epoch_after,
                active_final,
                status_after,
            };
            if read.is_coherent(ceiling) {
                return Ok(CaptureAccountingSnapshot {
                    completed_epoch: epoch_after,
                    fixed_capture_bytes: fixed,
                    resident_generation_bytes: resident,
                    record_reservation_bytes: record,
                    total_accounted_bytes: total,
                    accounting_invariant_failures: failures,
                });
            }
            if !read.reconciles(ceiling) && read.is_quiescent() {
                self.publish_terminal(CaptureAccountingStatus::InvariantViolated);
                return Err(CaptureAccountingSnapshotError::InvariantViolated);
            }
            std::hint::spin_loop();
        }
        Err(CaptureAccountingSnapshotError::Contended {
            attempts: max_attempts,
        })
    }

    fn release(
        &self,
        component: AccountingComponent,
        bytes: usize,
    ) -> Result<(), CaptureAccountingError> {
        let mut transition = self.try_enter_transition()?;
        if let Err(error) = self.ensure_healthy() {
            transition.cancel()?;
            return Err(error);
        }
        if self
            .component_counter(component)
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_sub(bytes)
            })
            .is_err()
        {
            self.publish_terminal(CaptureAccountingStatus::InvariantViolated);
            transition.cancel()?;
            return Err(CaptureAccountingError::InvariantViolated);
        }
        if self
            .total_accounted_bytes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_sub(bytes)
            })
            .is_err()
        {
            self.publish_terminal(CaptureAccountingStatus::InvariantViolated);
            transition.cancel()?;
            return Err(CaptureAccountingError::InvariantViolated);
        }
        transition.finish()
    }

    fn component_counter(&self, component: AccountingComponent) -> &AtomicUsize {
        match component {
            AccountingComponent::Fixed => &self.fixed_capture_bytes,
            AccountingComponent::ResidentGeneration => &self.resident_generation_bytes,
            AccountingComponent::Record => &self.record_reservation_bytes,
        }
    }

    fn try_enter_transition(&self) -> Result<TransitionGuard<'_>, CaptureAccountingError> {
        self.ensure_healthy()?;
        let entered = self.active_transitions.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            checked_transition_enter,
        );
        if entered.is_err() {
            self.publish_terminal(CaptureAccountingStatus::TransitionOverflow);
            return Err(CaptureAccountingError::TransitionOverflow);
        }
        if let Err(error) = self.ensure_healthy() {
            self.leave_transition()?;
            return Err(error);
        }
        Ok(TransitionGuard {
            accounting: self,
            completed: false,
        })
    }

    fn ensure_healthy(&self) -> Result<(), CaptureAccountingError> {
        match CaptureAccountingStatus::from_raw(self.status.load(Ordering::SeqCst)) {
            CaptureAccountingStatus::Healthy => Ok(()),
            CaptureAccountingStatus::TransitionOverflow => {
                Err(CaptureAccountingError::TransitionOverflow)
            }
            CaptureAccountingStatus::EpochOverflow => Err(CaptureAccountingError::EpochOverflow),
            CaptureAccountingStatus::InvariantViolated => {
                Err(CaptureAccountingError::InvariantViolated)
            }
        }
    }

    fn snapshot_status(&self, raw: u8) -> Result<(), CaptureAccountingSnapshotError> {
        match CaptureAccountingStatus::from_raw(raw) {
            CaptureAccountingStatus::Healthy => Ok(()),
            CaptureAccountingStatus::TransitionOverflow => {
                Err(CaptureAccountingSnapshotError::TransitionOverflow)
            }
            CaptureAccountingStatus::EpochOverflow => {
                Err(CaptureAccountingSnapshotError::EpochOverflow)
            }
            CaptureAccountingStatus::InvariantViolated => {
                Err(CaptureAccountingSnapshotError::InvariantViolated)
            }
        }
    }

    fn publish_terminal(&self, terminal: CaptureAccountingStatus) {
        let _first = self.status.compare_exchange(
            CaptureAccountingStatus::Healthy as u8,
            terminal as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        let _previous = self.accounting_invariant_failures.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |current| Some(current.saturating_add(1)),
        );
    }

    fn complete_epoch(&self) -> Result<(), CaptureAccountingError> {
        if self
            .completed_epoch
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(1)
            })
            .is_err()
        {
            self.publish_terminal(CaptureAccountingStatus::EpochOverflow);
            return Err(CaptureAccountingError::EpochOverflow);
        }
        Ok(())
    }

    fn leave_transition(&self) -> Result<(), CaptureAccountingError> {
        if self
            .active_transitions
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, checked_transition_leave)
            .is_err()
        {
            self.publish_terminal(CaptureAccountingStatus::InvariantViolated);
            return Err(CaptureAccountingError::InvariantViolated);
        }
        Ok(())
    }

    #[cfg(test)]
    fn with_held_transition_for_test<T>(
        &self,
        test: impl FnOnce() -> Result<T, Box<dyn std::error::Error>>,
    ) -> Result<T, Box<dyn std::error::Error>> {
        let mut transition = self.try_enter_transition()?;
        let result = test();
        transition.cancel()?;
        result
    }

    #[cfg(test)]
    fn abandon_transition_for_test(&self) -> Result<(), CaptureAccountingError> {
        let transition = self.try_enter_transition()?;
        drop(transition);
        Ok(())
    }

    #[cfg(test)]
    fn for_test_with_epoch(
        initial_fixed_bytes: usize,
        configured_ceiling: NonZeroUsize,
        completed_epoch: std::num::NonZeroU64,
    ) -> Result<Arc<Self>, CaptureAccountingError> {
        let accounting = Arc::new(Self::try_new(initial_fixed_bytes, configured_ceiling)?);
        accounting
            .completed_epoch
            .store(completed_epoch.get(), Ordering::SeqCst);
        Ok(accounting)
    }
}

#[derive(Debug)]
pub(super) struct CaptureMemoryReservation {
    accounting: Arc<CaptureMemoryAccounting>,
    component: AccountingComponent,
    bytes: usize,
    active: bool,
}

impl Drop for CaptureMemoryReservation {
    fn drop(&mut self) {
        if self.active {
            let _released = self.accounting.release(self.component, self.bytes);
            self.active = false;
        }
    }
}

struct TransitionGuard<'a> {
    accounting: &'a CaptureMemoryAccounting,
    completed: bool,
}

impl TransitionGuard<'_> {
    fn finish(&mut self) -> Result<(), CaptureAccountingError> {
        let epoch = self.accounting.complete_epoch();
        let leave = self.accounting.leave_transition();
        self.completed = true;
        epoch.and(leave)
    }

    fn cancel(&mut self) -> Result<(), CaptureAccountingError> {
        let leave = self.accounting.leave_transition();
        self.completed = true;
        leave
    }
}

impl Drop for TransitionGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.accounting
                .publish_terminal(CaptureAccountingStatus::InvariantViolated);
            let _leave = self.accounting.leave_transition();
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, market_squawk_loom))]
mod loom_model;
