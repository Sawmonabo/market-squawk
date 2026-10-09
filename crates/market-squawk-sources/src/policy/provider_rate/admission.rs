//! Process-local ordering and wakeups; durable budget admission remains authoritative.

use tokio::sync::{Mutex, MutexGuard, Notify, futures::Notified};

#[derive(Debug, Default)]
pub(crate) struct RequestAdmission {
    turn: Mutex<()>,
    changed: Notify,
    availability_changed: Notify,
}

impl RequestAdmission {
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
