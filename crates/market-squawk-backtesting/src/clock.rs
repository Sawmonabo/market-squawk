//! Deterministic event-time progression independent of wall-clock time.

use market_squawk_domain::Timestamp;
use thiserror::Error;

/// Monotonic research clock advanced only by admitted point-in-time observations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EventTimeClock {
    current: Option<Timestamp>,
}

impl EventTimeClock {
    /// Advances to a nondecreasing admitted decision time.
    ///
    /// # Errors
    ///
    /// Rejects a timestamp earlier than the current event time.
    pub fn advance(&mut self, next: Timestamp) -> Result<(), EventTimeClockError> {
        if self.current.is_some_and(|current| next < current) {
            return Err(EventTimeClockError::Regression);
        }
        self.current = Some(next);
        Ok(())
    }

    /// Returns the current admitted event time.
    #[must_use]
    pub const fn current(self) -> Option<Timestamp> {
        self.current
    }

    /// Returns whether a snapshot is strictly after a signal and its configured latency.
    pub fn is_execution_eligible(
        self,
        signal_at: Timestamp,
        latency_nanos: i64,
    ) -> Result<bool, EventTimeClockError> {
        let eligible_at = signal_at
            .checked_add_nanos(latency_nanos)
            .map_err(|_| EventTimeClockError::Overflow)?;
        Ok(self
            .current
            .is_some_and(|current| current > signal_at && current >= eligible_at))
    }
}

/// Deterministic clock contract failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum EventTimeClockError {
    /// Admitted decision time moved backward.
    #[error("backtest event time regressed")]
    Regression,
    /// Eligibility arithmetic exceeded the timestamp representation.
    #[error("backtest event-time arithmetic overflowed")]
    Overflow,
}
