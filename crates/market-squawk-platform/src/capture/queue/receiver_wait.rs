//! Test-only receiver pause coordination outside queue ownership state.

use std::time::{Duration, Instant};

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, Ordering};
#[cfg(loom)]
use loom::sync::{Condvar, Mutex};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(loom))]
use std::sync::{Condvar, Mutex};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(in crate::capture) enum ReceiverPauseError {
    #[error("capture receiver test coordination state is poisoned")]
    Poisoned,
    #[error("capture receiver did not reach the test coordination barrier before its deadline")]
    DeadlineElapsed,
}

#[derive(Debug, Default)]
struct ReceiverPauseState {
    requested: bool,
    parked: bool,
}

#[derive(Debug)]
pub(super) struct ReceiverTestCoordination {
    pub(super) requested_hint: AtomicBool,
    state: Mutex<ReceiverPauseState>,
    changed: Condvar,
}

impl ReceiverTestCoordination {
    pub(super) fn new() -> Self {
        Self {
            requested_hint: AtomicBool::new(false),
            state: Mutex::new(ReceiverPauseState::default()),
            changed: Condvar::new(),
        }
    }

    pub(super) fn request(&self) -> Result<ReceiverPauseGuard<'_>, ReceiverPauseError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_error| ReceiverPauseError::Poisoned)?;
        state.requested = true;
        state.parked = false;
        self.requested_hint.store(true, Ordering::Release);
        drop(state);
        Ok(ReceiverPauseGuard { coordination: self })
    }

    pub(super) fn park_if_requested(&self) -> Result<(), ReceiverPauseError> {
        if !self.requested_hint.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_error| ReceiverPauseError::Poisoned)?;
        if !state.requested {
            return Ok(());
        }
        state.parked = true;
        self.changed.notify_all();
        while state.requested {
            state = self
                .changed
                .wait(state)
                .map_err(|_error| ReceiverPauseError::Poisoned)?;
        }
        state.parked = false;
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct ReceiverPauseGuard<'a> {
    coordination: &'a ReceiverTestCoordination,
}

impl ReceiverPauseGuard<'_> {
    pub(super) fn wait_until_parked(&self, timeout: Duration) -> Result<(), ReceiverPauseError> {
        let start = Instant::now();
        let deadline = start.checked_add(timeout).unwrap_or(start);
        let mut state = self
            .coordination
            .state
            .lock()
            .map_err(|_error| ReceiverPauseError::Poisoned)?;
        while !state.parked {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ReceiverPauseError::DeadlineElapsed);
            }
            let (next, timeout_result) = self
                .coordination
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_error| ReceiverPauseError::Poisoned)?;
            state = next;
            if timeout_result.timed_out() && !state.parked {
                return Err(ReceiverPauseError::DeadlineElapsed);
            }
        }
        Ok(())
    }
}

impl Drop for ReceiverPauseGuard<'_> {
    fn drop(&mut self) {
        match self.coordination.state.lock() {
            Ok(mut state) => state.requested = false,
            Err(poisoned) => poisoned.into_inner().requested = false,
        }
        self.coordination
            .requested_hint
            .store(false, Ordering::Release);
        self.coordination.changed.notify_all();
    }
}
