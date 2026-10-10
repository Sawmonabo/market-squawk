//! Process-local ordering and wakeups; durable budget admission remains authoritative.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{Mutex, MutexGuard, Notify, futures::Notified};

use crate::BudgetUnavailableReason;

#[derive(Debug, Default)]
pub(crate) struct RequestAdmission {
    turn: Mutex<()>,
    changed: Notify,
    availability_changed: Notify,
    // Shared revocation coordinates, not another quota counter. Request capacity does not
    // revoke an established transport; provider refusal and terminal failures do.
    availability_generation: AtomicU64,
    transport_generation: AtomicU64,
    terminal: AtomicBool,
}

impl RequestAdmission {
    pub(in crate::policy) fn availability_generation(&self) -> u64 {
        self.availability_generation.load(Ordering::Acquire)
    }

    pub(in crate::policy) fn transport_generation(&self) -> u64 {
        self.transport_generation.load(Ordering::Acquire)
    }

    pub(in crate::policy) fn availability_generation_is_current(&self, generation: u64) -> bool {
        !self.terminal.load(Ordering::Acquire) && self.availability_generation() == generation
    }

    pub(in crate::policy) fn transport_generation_is_current(&self, generation: u64) -> bool {
        !self.terminal.load(Ordering::Acquire) && self.transport_generation() == generation
    }

    pub(in crate::policy) fn invalidate(
        &self,
        transport: bool,
    ) -> Result<(), BudgetUnavailableReason> {
        if self.terminal.load(Ordering::Acquire) {
            return Err(BudgetUnavailableReason::PersistenceUnavailable);
        }
        let increment = |counter: &AtomicU64| {
            counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
        };
        if increment(&self.availability_generation).is_err()
            || (transport && increment(&self.transport_generation).is_err())
        {
            self.terminalize();
            return Err(BudgetUnavailableReason::AvailabilityGenerationExhausted);
        }
        Ok(())
    }

    pub(in crate::policy) fn terminalize(&self) {
        self.terminal.store(true, Ordering::Release);
        self.changed.notify_waiters();
        self.availability_changed.notify_waiters();
    }

    pub(in crate::policy) fn shared_allocation_charge(&self) -> Option<usize> {
        std::mem::size_of::<Self>()
            .checked_add(crate::conservative_arc_control_block_charge::<Self>())
    }

    pub(crate) async fn turn(&self) -> MutexGuard<'_, ()> {
        self.turn.lock().await
    }

    pub(crate) fn changed(&self) -> Notified<'_> {
        self.changed.notified()
    }

    pub(crate) fn availability_changed(&self) -> Notified<'_> {
        self.availability_changed.notified()
    }

    pub(in crate::policy) fn notify_on_drop(&self) -> AdmissionChange<'_> {
        AdmissionChange(self, true)
    }

    pub(in crate::policy) fn notify_capacity_on_drop(&self) -> AdmissionChange<'_> {
        AdmissionChange(self, false)
    }
}

/// Declare before state/store guards so notification follows their release on every exit.
pub(in crate::policy) struct AdmissionChange<'a>(&'a RequestAdmission, bool);

impl Drop for AdmissionChange<'_> {
    fn drop(&mut self) {
        self.0.changed.notify_waiters();
        if self.1 {
            self.0.availability_changed.notify_waiters();
        }
    }
}
