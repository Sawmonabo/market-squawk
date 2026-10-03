//! Process-generation lifecycle control for the installed per-user service.

use std::{fmt, sync::Mutex};

use market_squawk_runtime::RuntimeIdentity;
use thiserror::Error;
use tokio::sync::Notify;

/// Terminal result returned only after every installed-service shutdown barrier completes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstalledServiceRunOutcome {
    /// An operator or the operating system requested an ordinary stop.
    Stopped,
    /// Durable state requires the installed supervisor to start the exact next generation.
    RestartRequested {
        /// Runtime identity the replacement process must prove before publication.
        expected_next: RuntimeIdentity,
    },
}

/// Capacity-one, process-local signal backed by a durable restart decision.
///
/// This authority does not persist recovery state and cannot select a workspace. Its caller must
/// first commit the exact restart handoff through the durable recovery authority. The signal only
/// transfers that already-authorized decision to the outer service lifecycle.
pub(crate) struct InstalledServiceLifecycle {
    current: RuntimeIdentity,
    requested: Mutex<Option<RuntimeIdentity>>,
    notify: Notify,
}

impl InstalledServiceLifecycle {
    /// Binds restart admission to the exact currently serving runtime generation.
    #[must_use]
    pub(crate) fn new(current: RuntimeIdentity) -> Self {
        Self {
            current,
            requested: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    /// Returns the exact runtime generation owned by this lifecycle signal.
    #[must_use]
    pub(crate) const fn current(&self) -> RuntimeIdentity {
        self.current
    }

    /// Signals one already-persisted restart handoff.
    ///
    /// The next identity must retain the installation and advance the service generation exactly
    /// once. Workspace changes are admitted only because their durable selector is validated by
    /// the recovery authority before this method is called.
    pub(crate) fn request_restart(
        &self,
        expected_next: RuntimeIdentity,
    ) -> Result<(), ServiceLifecycleError> {
        let expected_generation = self
            .current
            .service_generation()
            .get()
            .checked_add(1)
            .ok_or(ServiceLifecycleError::InvalidTransition)?;
        if expected_next.installation_id() != self.current.installation_id()
            || expected_next.service_generation().get() != expected_generation
        {
            return Err(ServiceLifecycleError::InvalidTransition);
        }
        let mut requested = self
            .requested
            .lock()
            .map_err(|_| ServiceLifecycleError::Unavailable)?;
        match *requested {
            Some(existing) if existing == expected_next => return Ok(()),
            Some(_) => return Err(ServiceLifecycleError::Conflict),
            None => *requested = Some(expected_next),
        }
        drop(requested);
        self.notify.notify_waiters();
        Ok(())
    }

    pub(crate) async fn wait_for_restart(&self) -> RuntimeIdentity {
        loop {
            let notified = self.notify.notified();
            if let Ok(requested) = self.requested.lock()
                && let Some(expected_next) = *requested
            {
                return expected_next;
            }
            notified.await;
        }
    }
}

impl fmt::Debug for InstalledServiceLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledServiceLifecycle")
            .field("current", &self.current)
            .field("requested", &"[DURABLE DECISION SIGNAL]")
            .finish()
    }
}

/// Closed service-lifecycle signal failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum ServiceLifecycleError {
    /// The requested runtime is not the exact next generation of this installation.
    #[error("installed-service restart transition is invalid")]
    InvalidTransition,
    /// A different restart is already pending.
    #[error("installed-service restart transition conflicts with a pending request")]
    Conflict,
    /// The bounded restart signal cannot be inspected.
    #[error("installed-service lifecycle authority is unavailable")]
    Unavailable,
}
