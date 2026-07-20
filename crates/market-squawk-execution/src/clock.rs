//! Sealed production wall-plus-monotonic clock.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use market_squawk_domain::Timestamp;
use thiserror::Error;

use crate::account::AccountReservationStateError;

const RESERVATION_ACTIVE: u8 = 0;
const RESERVATION_RELEASED: u8 = 1;
const RESERVATION_RECONCILIATION: u8 = 2;

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

fn duration_to_i128(duration: Duration) -> i128 {
    i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
}

#[derive(Debug)]
pub(crate) struct AccountReservationLease {
    status: AtomicU8,
    account_revision: Arc<AtomicU64>,
    expected_account_revision: u64,
    wall_expiry: Timestamp,
    monotonic_expiry: Instant,
}

impl AccountReservationLease {
    pub(crate) fn new(
        account_revision: Arc<AtomicU64>,
        expected_account_revision: u64,
        wall_expiry: Timestamp,
        monotonic_expiry: Instant,
    ) -> Self {
        Self {
            status: AtomicU8::new(RESERVATION_ACTIVE),
            account_revision,
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
        if now.wall >= self.wall_expiry || now.monotonic >= self.monotonic_expiry {
            return Err(AccountReservationStateError::Expired);
        }
        Ok(())
    }

    pub(crate) fn counts_against_limits(&self) -> bool {
        matches!(
            self.status.load(Ordering::Acquire),
            RESERVATION_ACTIVE | RESERVATION_RECONCILIATION
        )
    }

    pub(crate) fn release(&self) {
        let _ = self.status.compare_exchange(
            RESERVATION_ACTIVE,
            RESERVATION_RELEASED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(crate) fn mark_reconciliation_required(&self) {
        let _ = self.status.compare_exchange(
            RESERVATION_ACTIVE,
            RESERVATION_RECONCILIATION,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}
