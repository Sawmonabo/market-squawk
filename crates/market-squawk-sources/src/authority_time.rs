//! Registry-owned paired wall/monotonic time and permanent continuity authority.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use market_squawk_domain::{
    CaptureRetainedComponent, CaptureRetainedSizeError, Timestamp,
    checked_arc_value_allocation_bytes,
};

use crate::policy::AuthorityDurabilitySession;
use crate::registry::RegistryError;

const MAX_HIGH_WATER_SNAPSHOT_ATTEMPTS: usize = 16;

/// Process-local monotonic observation represented without exposing caller-authored `Instant`s.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct RegistryMonotonicInstant(u64);

impl RegistryMonotonicInstant {
    pub(crate) const fn from_nanos(value: u64) -> Self {
        Self(value)
    }

    pub(crate) const fn as_nanos(self) -> u64 {
        self.0
    }

    pub(crate) fn checked_add(self, duration: Duration) -> Option<Self> {
        let nanos = u64::try_from(duration.as_nanos()).ok()?;
        self.0.checked_add(nanos).map(Self)
    }
}

/// One inseparable raw wall/monotonic sample from a registry clock source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RawRegistryClockObservation {
    wall: Timestamp,
    monotonic: RegistryMonotonicInstant,
}

impl RawRegistryClockObservation {
    pub(crate) const fn new(wall: Timestamp, monotonic: RegistryMonotonicInstant) -> Self {
        Self { wall, monotonic }
    }
}

/// Private clock-source boundary. Only [`SealedRegistryClock`] may turn a sample into authority.
pub(crate) trait RawRegistryClockSource: Send + Sync + std::fmt::Debug {
    fn observe_raw(&self) -> Result<RawRegistryClockObservation, RegistryError>;

    fn shared_allocation_charge(&self) -> usize;
}

/// A sample that passed the registry's paired high-water transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TrustedRegistryTime {
    wall: Timestamp,
    monotonic: RegistryMonotonicInstant,
}

impl TrustedRegistryTime {
    pub(crate) const fn new(wall: Timestamp, monotonic: RegistryMonotonicInstant) -> Self {
        Self { wall, monotonic }
    }

    pub(crate) const fn wall(self) -> Timestamp {
        self.wall
    }

    pub(crate) const fn monotonic(self) -> RegistryMonotonicInstant {
        self.monotonic
    }

    pub(crate) fn checked_deadline(
        self,
        until: Timestamp,
    ) -> Result<Option<RegistryMonotonicInstant>, RegistryError> {
        if until < self.wall {
            return Ok(None);
        }
        let delta = until
            .unix_nanos()
            .checked_sub(self.wall.unix_nanos())
            .ok_or(RegistryError::HealthDeadlineOverflow)?;
        let nanos = u64::try_from(delta).map_err(|_| RegistryError::HealthDeadlineOverflow)?;
        self.monotonic
            .checked_add(Duration::from_nanos(nanos))
            .map(Some)
            .ok_or(RegistryError::HealthDeadlineOverflow)
    }
}

#[derive(Debug)]
struct AuthorityTimeContinuityState {
    terminal: AtomicBool,
    sequence: AtomicU64,
    wall_high_water: AtomicI64,
    monotonic_high_water: AtomicU64,
}

/// O(1)-clone permanent continuity latch shared by every live authority in one registry.
#[derive(Clone, Debug)]
pub(crate) struct AuthorityTimeContinuity(Arc<AuthorityTimeContinuityState>);

impl AuthorityTimeContinuity {
    fn new() -> Self {
        Self(Arc::new(AuthorityTimeContinuityState {
            terminal: AtomicBool::new(false),
            sequence: AtomicU64::new(0),
            wall_high_water: AtomicI64::new(i64::MIN),
            monotonic_high_water: AtomicU64::new(0),
        }))
    }

    pub(crate) fn is_continuous(&self) -> bool {
        !self.0.terminal.load(Ordering::Acquire)
    }

    fn latch(&self) {
        self.0.terminal.store(true, Ordering::Release);
    }

    pub(crate) fn shares_allocation_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(crate) fn checked_shared_allocation_bytes(
        &self,
    ) -> Result<usize, CaptureRetainedSizeError> {
        checked_arc_value_allocation_bytes::<AuthorityTimeContinuityState>(0).map_err(|_| {
            CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Continuity,
            }
        })
    }

    fn publish(&self, observed: TrustedRegistryTime) -> Result<(), RegistryError> {
        if !self.is_continuous() {
            return Err(RegistryError::AuthorityTimeDiscontinuous);
        }
        let current = self.0.sequence.load(Ordering::Acquire);
        let odd = current
            .checked_add(1)
            .ok_or(RegistryError::AuthorityTimeDiscontinuous)?;
        let even = odd
            .checked_add(1)
            .ok_or(RegistryError::AuthorityTimeDiscontinuous)?;
        self.0.sequence.store(odd, Ordering::Release);
        self.0
            .wall_high_water
            .store(observed.wall().unix_nanos(), Ordering::Release);
        self.0
            .monotonic_high_water
            .store(observed.monotonic().as_nanos(), Ordering::Release);
        self.0.sequence.store(even, Ordering::Release);
        Ok(())
    }

    fn high_water(&self) -> Result<TrustedRegistryTime, RegistryError> {
        for _attempt in 0..MAX_HIGH_WATER_SNAPSHOT_ATTEMPTS {
            if !self.is_continuous() {
                return Err(RegistryError::AuthorityTimeDiscontinuous);
            }
            let before = self.0.sequence.load(Ordering::Acquire);
            if before == 0 || before & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let wall = self.0.wall_high_water.load(Ordering::Acquire);
            let monotonic = self.0.monotonic_high_water.load(Ordering::Acquire);
            let after = self.0.sequence.load(Ordering::Acquire);
            if before == after && after & 1 == 0 {
                return Ok(TrustedRegistryTime::new(
                    Timestamp::from_unix_nanos(wall),
                    RegistryMonotonicInstant::from_nanos(monotonic),
                ));
            }
            std::hint::spin_loop();
        }
        Err(RegistryError::TrustedReceiptHighWaterUnavailable)
    }

    pub(crate) fn validate_receipt(
        &self,
        receipt: &TrustedReceiptObservation,
        session_started_at: TrustedRegistryTime,
    ) -> Result<(), RegistryError> {
        if !self.shares_allocation_with(&receipt.continuity) {
            return Err(RegistryError::TrustedReceiptContinuityMismatch);
        }
        if receipt.time.wall() < session_started_at.wall()
            || receipt.time.monotonic() < session_started_at.monotonic()
        {
            return Err(RegistryError::TrustedReceiptOutOfRange);
        }
        let high_water = self.high_water()?;
        if receipt.time.wall() > high_water.wall()
            || receipt.time.monotonic() > high_water.monotonic()
        {
            return Err(RegistryError::TrustedReceiptOutOfRange);
        }
        Ok(())
    }
}

impl PartialEq for AuthorityTimeContinuity {
    fn eq(&self, other: &Self) -> bool {
        self.shares_allocation_with(other)
    }
}

impl Eq for AuthorityTimeContinuity {}

/// Opaque source-owned receipt-time proof embedded in live frames and downstream evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TrustedReceiptObservation {
    time: TrustedRegistryTime,
    continuity: AuthorityTimeContinuity,
}

impl TrustedReceiptObservation {
    pub(crate) const fn received_at(&self) -> Timestamp {
        self.time.wall()
    }

    #[cfg(test)]
    pub(crate) const fn time(&self) -> TrustedRegistryTime {
        self.time
    }

    pub(crate) const fn continuity(&self) -> &AuthorityTimeContinuity {
        &self.continuity
    }
}

#[cfg(test)]
pub(crate) fn trusted_test_receipt(
    wall: Timestamp,
    monotonic_nanos: u64,
) -> Result<TrustedReceiptObservation, RegistryError> {
    let continuity = AuthorityTimeContinuity::new();
    let time =
        TrustedRegistryTime::new(wall, RegistryMonotonicInstant::from_nanos(monotonic_nanos));
    continuity.publish(time)?;
    Ok(TrustedReceiptObservation { time, continuity })
}

/// Single linearization point for paired sampling, cursor validation, and continuity publication.
#[derive(Debug)]
pub(crate) struct SealedRegistryClock {
    source: Arc<dyn RawRegistryClockSource>,
    cursor: Mutex<Option<TrustedRegistryTime>>,
    continuity: AuthorityTimeContinuity,
    durability: OnceLock<Weak<AuthorityDurabilitySession>>,
}

impl SealedRegistryClock {
    pub(crate) fn new(source: Arc<dyn RawRegistryClockSource>) -> Self {
        Self {
            source,
            cursor: Mutex::new(None),
            continuity: AuthorityTimeContinuity::new(),
            durability: OnceLock::new(),
        }
    }

    pub(crate) fn bind_durability(
        &self,
        durability: &Arc<AuthorityDurabilitySession>,
    ) -> Result<(), RegistryError> {
        self.durability
            .set(Arc::downgrade(durability))
            .map_err(|_| RegistryError::AuthorityTimeDurabilityAlreadyBound)
    }

    pub(crate) const fn continuity(&self) -> &AuthorityTimeContinuity {
        &self.continuity
    }

    pub(crate) fn observe(&self) -> Result<TrustedRegistryTime, RegistryError> {
        if !self.continuity.is_continuous() {
            return Err(RegistryError::AuthorityTimeDiscontinuous);
        }
        let mut cursor = match self.cursor.lock() {
            Ok(cursor) => cursor,
            Err(_poisoned) => return Err(self.fail(RegistryError::TrustedClockUnavailable)),
        };
        let raw = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.source.observe_raw()
        })) {
            Ok(Ok(raw)) => raw,
            Ok(Err(_)) | Err(_) => {
                return Err(self.fail(RegistryError::TrustedClockUnavailable));
            }
        };
        let observed = TrustedRegistryTime::new(raw.wall, raw.monotonic);
        if cursor.is_some_and(|previous| {
            observed.wall() < previous.wall() || observed.monotonic() < previous.monotonic()
        }) {
            return Err(self.fail(RegistryError::TrustedClockRegression));
        }
        if let Err(error) = self.continuity.publish(observed) {
            return Err(self.fail(error));
        }
        *cursor = Some(observed);
        Ok(observed)
    }

    pub(crate) fn observe_receipt(&self) -> Result<TrustedReceiptObservation, RegistryError> {
        self.observe().map(|time| TrustedReceiptObservation {
            time,
            continuity: self.continuity.clone(),
        })
    }

    pub(crate) fn shared_allocation_charge(&self) -> Option<usize> {
        checked_arc_value_allocation_bytes::<Self>(self.source.shared_allocation_charge()).ok()
    }

    fn fail(&self, error: RegistryError) -> RegistryError {
        self.continuity.latch();
        if let Some(durability) = self.durability.get().and_then(Weak::upgrade) {
            durability.latch_terminal_for_time_discontinuity();
        }
        error
    }

    #[cfg(test)]
    fn poison_cursor_for_test(&self) {
        match self.cursor.lock() {
            Ok(_cursor) => std::panic::resume_unwind(Box::new("deliberate cursor poison")),
            Err(_poisoned) => {}
        }
    }
}

/// System implementation whose monotonic component is elapsed nanoseconds from construction.
#[derive(Debug)]
pub(crate) struct SystemRawRegistryClock {
    monotonic_origin: Instant,
}

impl SystemRawRegistryClock {
    pub(crate) fn try_new() -> Result<Self, RegistryError> {
        let clock = Self {
            monotonic_origin: Instant::now(),
        };
        let _initial = clock.observe_raw()?;
        Ok(clock)
    }
}

impl RawRegistryClockSource for SystemRawRegistryClock {
    fn observe_raw(&self) -> Result<RawRegistryClockObservation, RegistryError> {
        let wall_duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| RegistryError::TrustedClockUnavailable)?;
        let wall_nanos = i64::try_from(wall_duration.as_nanos())
            .map_err(|_| RegistryError::TrustedClockUnavailable)?;
        let elapsed = Instant::now()
            .checked_duration_since(self.monotonic_origin)
            .ok_or(RegistryError::TrustedClockUnavailable)?;
        let monotonic_nanos = u64::try_from(elapsed.as_nanos())
            .map_err(|_| RegistryError::TrustedClockUnavailable)?;
        Ok(RawRegistryClockObservation::new(
            Timestamp::from_unix_nanos(wall_nanos),
            RegistryMonotonicInstant::from_nanos(monotonic_nanos),
        ))
    }

    fn shared_allocation_charge(&self) -> usize {
        std::mem::size_of::<Self>() + crate::conservative_arc_control_block_charge::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    #[derive(Debug)]
    struct UnwindingRawRegistryClock;

    impl RawRegistryClockSource for UnwindingRawRegistryClock {
        fn observe_raw(&self) -> Result<RawRegistryClockObservation, RegistryError> {
            std::panic::resume_unwind(Box::new("deliberate clock-source unwind"))
        }

        fn shared_allocation_charge(&self) -> usize {
            std::mem::size_of::<Self>() + crate::conservative_arc_control_block_charge::<Self>()
        }
    }

    #[derive(Debug)]
    struct FixedRawRegistryClock;

    impl RawRegistryClockSource for FixedRawRegistryClock {
        fn observe_raw(&self) -> Result<RawRegistryClockObservation, RegistryError> {
            Ok(RawRegistryClockObservation::new(
                Timestamp::from_unix_nanos(1),
                RegistryMonotonicInstant::from_nanos(1),
            ))
        }

        fn shared_allocation_charge(&self) -> usize {
            std::mem::size_of::<Self>() + crate::conservative_arc_control_block_charge::<Self>()
        }
    }

    fn time(value: u64) -> Result<TrustedRegistryTime, RegistryError> {
        let wall = i64::try_from(value).map_err(|_| RegistryError::TrustedClockUnavailable)?;
        Ok(TrustedRegistryTime::new(
            Timestamp::from_unix_nanos(wall),
            RegistryMonotonicInstant::from_nanos(value),
        ))
    }

    #[test]
    fn poisoned_cursor_latches_continuity_permanently() -> TestResult {
        let unwinding_source_clock = SealedRegistryClock::new(Arc::new(UnwindingRawRegistryClock));
        assert_eq!(
            unwinding_source_clock.observe(),
            Err(RegistryError::TrustedClockUnavailable)
        );
        assert!(!unwinding_source_clock.continuity().is_continuous());

        let clock = Arc::new(SealedRegistryClock::new(Arc::new(FixedRawRegistryClock)));
        let unwinding_clock = Arc::clone(&clock);
        let unwind = std::thread::spawn(move || {
            unwinding_clock.poison_cursor_for_test();
        });
        assert!(unwind.join().is_err());

        assert_eq!(clock.observe(), Err(RegistryError::TrustedClockUnavailable));
        assert_eq!(
            clock.observe(),
            Err(RegistryError::AuthorityTimeDiscontinuous)
        );
        assert!(!clock.continuity().is_continuous());
        Ok(())
    }

    #[test]
    fn paired_high_water_never_exposes_a_torn_wall_monotonic_sample() -> TestResult {
        let continuity = AuthorityTimeContinuity::new();
        continuity.publish(time(1)?)?;
        let writer_continuity = continuity.clone();
        let writer = std::thread::spawn(move || -> Result<(), RegistryError> {
            for value in 2..10_000 {
                writer_continuity.publish(time(value)?)?;
            }
            Ok(())
        });

        for _attempt in 0..10_000 {
            match continuity.high_water() {
                Ok(observed) => {
                    let wall = u64::try_from(observed.wall().unix_nanos())?;
                    assert_eq!(wall, observed.monotonic().as_nanos());
                }
                Err(RegistryError::TrustedReceiptHighWaterUnavailable) => {}
                Err(error) => return Err(error.into()),
            }
        }
        let writer_result = writer
            .join()
            .map_err(|_| std::io::Error::other("high-water writer unwound"))?;
        writer_result?;
        let final_high_water = continuity.high_water()?;
        assert_eq!(final_high_water.wall().unix_nanos(), 9_999);
        assert_eq!(final_high_water.monotonic().as_nanos(), 9_999);
        Ok(())
    }

    #[test]
    fn receipt_range_and_continuity_validation_rejects_every_forged_dimension() -> TestResult {
        let continuity = AuthorityTimeContinuity::new();
        let session_started_at = time(10)?;
        continuity.publish(time(20)?)?;
        let valid = TrustedReceiptObservation {
            time: time(15)?,
            continuity: continuity.clone(),
        };
        continuity.validate_receipt(&valid, session_started_at)?;

        let before_session = TrustedReceiptObservation {
            time: time(9)?,
            continuity: continuity.clone(),
        };
        assert_eq!(
            continuity.validate_receipt(&before_session, session_started_at),
            Err(RegistryError::TrustedReceiptOutOfRange)
        );
        let beyond_high_water = TrustedReceiptObservation {
            time: time(21)?,
            continuity: continuity.clone(),
        };
        assert_eq!(
            continuity.validate_receipt(&beyond_high_water, session_started_at),
            Err(RegistryError::TrustedReceiptOutOfRange)
        );
        let wrong_continuity = TrustedReceiptObservation {
            time: time(15)?,
            continuity: AuthorityTimeContinuity::new(),
        };
        assert_eq!(
            continuity.validate_receipt(&wrong_continuity, session_started_at),
            Err(RegistryError::TrustedReceiptContinuityMismatch)
        );
        Ok(())
    }
}
