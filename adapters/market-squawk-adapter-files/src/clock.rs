//! Paired wall/monotonic request-deadline sealing.

use std::fmt;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use market_squawk_domain::Timestamp;
use thiserror::Error;

use crate::FileAdapterError;

/// One inseparable wall-clock and process-monotonic observation.
#[derive(Clone, Copy, Debug)]
pub struct ExtractionClockReading {
    wall: Timestamp,
    monotonic: Instant,
}

impl ExtractionClockReading {
    /// Constructs one paired observation supplied by an extraction host or deterministic test.
    pub const fn new(wall: Timestamp, monotonic: Instant) -> Self {
        Self { wall, monotonic }
    }

    const fn wall(self) -> Timestamp {
        self.wall
    }

    const fn monotonic(self) -> Instant {
        self.monotonic
    }
}

/// Failure to obtain a trustworthy, representable extraction clock observation.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ExtractionClockError {
    /// The configured clock could not provide an observation.
    #[error("extraction clock is unavailable")]
    Unavailable,
    /// The current wall time cannot be represented by the canonical timestamp type.
    #[error("extraction clock is outside the supported timestamp range")]
    Range,
}

/// Injectable wall-plus-monotonic clock used for deadlines and operation provenance time.
pub trait ExtractionClock: Send + Sync + fmt::Debug {
    /// Returns one paired observation.
    ///
    /// # Errors
    ///
    /// Returns a typed clock error when either component is unavailable or unrepresentable.
    fn observe(&self) -> Result<ExtractionClockReading, ExtractionClockError>;
}

/// Production system wall-plus-monotonic clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemExtractionClock;

impl ExtractionClock for SystemExtractionClock {
    fn observe(&self) -> Result<ExtractionClockReading, ExtractionClockError> {
        // Sampling monotonic time first is conservative: time spent reading the wall clock is
        // charged to the caller's remaining deadline instead of extending it.
        let monotonic = Instant::now();
        let system = SystemTime::now();
        let unix_nanos = match system.duration_since(UNIX_EPOCH) {
            Ok(duration) => duration_to_i128(duration),
            Err(error) => -duration_to_i128(error.duration()),
        };
        let unix_nanos = i64::try_from(unix_nanos).map_err(|_| ExtractionClockError::Range)?;
        Ok(ExtractionClockReading::new(
            Timestamp::from_unix_nanos(unix_nanos),
            monotonic,
        ))
    }
}

fn duration_to_i128(duration: Duration) -> i128 {
    i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RequestDeadline {
    started_wall: Timestamp,
    started_at: Instant,
    expires_at: Instant,
}

impl RequestDeadline {
    pub(crate) fn seal(
        clock: &dyn ExtractionClock,
        wall_deadline: Timestamp,
        admission_expiry: Instant,
    ) -> Result<Self, FileAdapterError> {
        let reading = clock
            .observe()
            .map_err(|_| FileAdapterError::ClockFailure)?;
        if reading.monotonic() >= admission_expiry || wall_deadline <= reading.wall() {
            return Err(FileAdapterError::DeadlineExceeded);
        }
        let delta = wall_deadline
            .unix_nanos()
            .checked_sub(reading.wall().unix_nanos())
            .and_then(|nanos| u64::try_from(nanos).ok())
            .ok_or(FileAdapterError::ClockFailure)?;
        let request_deadline = reading
            .monotonic()
            .checked_add(Duration::from_nanos(delta))
            .ok_or(FileAdapterError::ClockFailure)?;
        Ok(Self {
            started_wall: reading.wall(),
            started_at: reading.monotonic(),
            expires_at: request_deadline.min(admission_expiry),
        })
    }

    pub(crate) fn checkpoint(self, clock: &dyn ExtractionClock) -> Result<(), FileAdapterError> {
        self.trusted_timestamp(clock).map(|_| ())
    }

    pub(crate) fn trusted_timestamp(
        self,
        clock: &dyn ExtractionClock,
    ) -> Result<Timestamp, FileAdapterError> {
        let observed = clock
            .observe()
            .map_err(|_| FileAdapterError::ClockFailure)?;
        if observed.monotonic() < self.started_at {
            return Err(FileAdapterError::ClockFailure);
        }
        if observed.monotonic() >= self.expires_at {
            return Err(FileAdapterError::DeadlineExceeded);
        }
        let elapsed = observed.monotonic().duration_since(self.started_at);
        let elapsed_nanos =
            i64::try_from(elapsed.as_nanos()).map_err(|_| FileAdapterError::ClockFailure)?;
        let unix_nanos = self
            .started_wall
            .unix_nanos()
            .checked_add(elapsed_nanos)
            .ok_or(FileAdapterError::ClockFailure)?;
        Ok(Timestamp::from_unix_nanos(unix_nanos))
    }

    pub(crate) const fn monotonic_expiry(self) -> Instant {
        self.expires_at
    }
}
