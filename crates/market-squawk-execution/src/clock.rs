//! Sealed production wall-plus-monotonic clock.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use market_squawk_domain::Timestamp;
use thiserror::Error;

use crate::account::AccountReservationStateError;

const RESERVATION_ACTIVE: u8 = 0;
const RESERVATION_SUBMITTED: u8 = 1;
const RESERVATION_ACCEPTED: u8 = 2;
const RESERVATION_RELEASED: u8 = 3;
const RESERVATION_RECONCILIATION: u8 = 4;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ClockReading {
    pub(crate) wall: Timestamp,
    pub(crate) monotonic: Instant,
}

/// Trusted clock failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ClockError {
    /// The platform wall time cannot fit the signed nanosecond domain.
    #[error("platform wall clock is outside the supported timestamp range")]
    WallClockRange,
    /// A configured monotonic deadline cannot be represented.
    #[error("monotonic deadline is outside the platform instant range")]
    MonotonicDeadlineRange,
}

pub(crate) fn system_now() -> Result<ClockReading, ClockError> {
    let system = SystemTime::now();
    let nanos = match system.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_to_i128(duration),
        Err(error) => -duration_to_i128(error.duration()),
    };
    let nanos = i64::try_from(nanos).map_err(|_| ClockError::WallClockRange)?;
    Ok(ClockReading {
        wall: Timestamp::from_unix_nanos(nanos),
        monotonic: Instant::now(),
    })
}

pub(crate) fn monotonic_deadline(
    reading: ClockReading,
    nanoseconds: i64,
) -> Result<Instant, ClockError> {
    let nanoseconds = u64::try_from(nanoseconds).map_err(|_| ClockError::MonotonicDeadlineRange)?;
    reading
        .monotonic
        .checked_add(Duration::from_nanos(nanoseconds))
        .ok_or(ClockError::MonotonicDeadlineRange)
}

pub(crate) fn deadline_expired(
    reading: ClockReading,
    wall_deadline: Timestamp,
    monotonic_deadline: Instant,
) -> bool {
    reading.wall > wall_deadline || reading.monotonic > monotonic_deadline
}

fn duration_to_i128(duration: Duration) -> i128 {
    i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
}

#[cfg(test)]
mod tests {
    use super::{ClockReading, deadline_expired};
    use market_squawk_domain::Timestamp;
    use std::time::{Duration, Instant};

    #[test]
    fn wall_and_monotonic_deadlines_are_inclusive_through_equality() {
        let monotonic = Instant::now();
        let wall = Timestamp::from_unix_nanos(100);
        let exact = ClockReading { wall, monotonic };
        assert!(!deadline_expired(exact, wall, monotonic));

        let after_wall = ClockReading {
            wall: Timestamp::from_unix_nanos(101),
            monotonic,
        };
        assert!(deadline_expired(after_wall, wall, monotonic));
        let after_monotonic = ClockReading {
            wall,
            monotonic: monotonic + Duration::from_nanos(1),
        };
        assert!(deadline_expired(after_monotonic, wall, monotonic));
    }
}

#[derive(Debug)]
pub(crate) struct AccountReservationLease {
    status: AtomicU8,
    account_revision: Arc<AtomicU64>,
    reconciliation_required: Arc<AtomicBool>,
    expected_account_revision: u64,
    wall_expiry: Timestamp,
    monotonic_expiry: Instant,
}

impl AccountReservationLease {
    pub(crate) fn new(
        account_revision: Arc<AtomicU64>,
        reconciliation_required: Arc<AtomicBool>,
        expected_account_revision: u64,
        wall_expiry: Timestamp,
        monotonic_expiry: Instant,
    ) -> Self {
        Self {
            status: AtomicU8::new(RESERVATION_ACTIVE),
            account_revision,
            reconciliation_required,
            expected_account_revision,
            wall_expiry,
            monotonic_expiry,
        }
    }

    pub(crate) fn validate(&self, now: ClockReading) -> Result<(), AccountReservationStateError> {
        if self.status.load(Ordering::Acquire) != RESERVATION_ACTIVE {
            return Err(AccountReservationStateError::NotActive);
        }
        if self.account_revision.load(Ordering::Acquire) != self.expected_account_revision {
            return Err(AccountReservationStateError::AccountStateChanged);
        }
        if deadline_expired(now, self.wall_expiry, self.monotonic_expiry) {
            return Err(AccountReservationStateError::Expired);
        }
        Ok(())
    }

    pub(crate) fn counts_against_limits(&self) -> bool {
        matches!(
            self.status.load(Ordering::Acquire),
            RESERVATION_ACTIVE
                | RESERVATION_SUBMITTED
                | RESERVATION_ACCEPTED
                | RESERVATION_RECONCILIATION
        )
    }

    pub(crate) const fn wall_expiry(&self) -> Timestamp {
        self.wall_expiry
    }

    pub(crate) const fn expected_account_revision(&self) -> u64 {
        self.expected_account_revision
    }

    pub(crate) fn begin_submission(&self) -> Result<(), AccountReservationStateError> {
        self.status
            .compare_exchange(
                RESERVATION_ACTIVE,
                RESERVATION_SUBMITTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|_| AccountReservationStateError::NotActive)
    }

    pub(crate) fn mark_accepted(&self) -> Result<(), AccountReservationStateError> {
        self.status
            .compare_exchange(
                RESERVATION_SUBMITTED,
                RESERVATION_ACCEPTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|_| AccountReservationStateError::NotSubmitted)
    }

    pub(crate) fn mark_known_not_accepted(&self) {
        let _ = self.status.compare_exchange(
            RESERVATION_SUBMITTED,
            RESERVATION_RELEASED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(crate) fn mark_terminal_unfilled(&self) {
        let _ = self.status.compare_exchange(
            RESERVATION_ACCEPTED,
            RESERVATION_RELEASED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(crate) fn release_if_active(&self) {
        let _ = self.status.compare_exchange(
            RESERVATION_ACTIVE,
            RESERVATION_RELEASED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(crate) fn mark_reconciliation_required(&self) {
        self.reconciliation_required.store(true, Ordering::Release);
        let mut observed = self.status.load(Ordering::Acquire);
        while matches!(observed, RESERVATION_SUBMITTED | RESERVATION_ACCEPTED) {
            match self.status.compare_exchange_weak(
                observed,
                RESERVATION_RECONCILIATION,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => observed = current,
            }
        }
    }

    pub(crate) fn fail_safe_drop(&self) {
        match self.status.load(Ordering::Acquire) {
            RESERVATION_ACTIVE => self.release_if_active(),
            RESERVATION_SUBMITTED | RESERVATION_ACCEPTED => {
                self.mark_reconciliation_required();
            }
            _ => {}
        }
    }
}
