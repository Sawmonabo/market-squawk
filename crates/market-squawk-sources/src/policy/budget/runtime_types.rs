//! Provider budget runtime value types and persistent allocation state.

use super::*;

/// A checked provider `Retry-After` instruction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryAfter {
    /// Relative positive delay in nanoseconds.
    Delay(NonZeroU64),
    /// Absolute retry instant.
    AtWallClock(Timestamp),
}

/// Monotonic process-local instant used for enforcement deadlines.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MonotonicInstant(u64);

impl MonotonicInstant {
    pub(in crate::policy) const fn from_nanos(value: u64) -> Self {
        Self(value)
    }

    /// Returns the monotonic nanosecond reading.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    pub(in crate::policy) fn checked_add(self, nanos: u64) -> Option<Self> {
        self.0.checked_add(nanos).map(Self)
    }
}

/// Paired wall/monotonic observation for converting an absolute HTTP Retry-After date once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::policy) struct ClockObservation {
    pub(in crate::policy) wall_clock: Timestamp,
    pub(in crate::policy) monotonic: MonotonicInstant,
}

impl ClockObservation {
    pub(in crate::policy) const fn new(
        wall_clock: Timestamp,
        monotonic: MonotonicInstant,
    ) -> Self {
        Self {
            wall_clock,
            monotonic,
        }
    }
}

/// Why a request cannot be dispatched without waiting for external state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BudgetUnavailableReason {
    /// All local concurrent permits are in use and no future release time is knowable.
    ConcurrencyExhausted,
    /// Provider backoff or Retry-After is active until its recorded deadline.
    CoolingDown,
    /// The current request window has no remaining request capacity.
    RequestWindowExhausted,
    /// Provider access was administratively disabled.
    Disabled,
    /// The local clock regressed relative to budget state.
    ClockRegression,
    /// Checked deadline arithmetic overflowed.
    DeadlineOverflow,
    /// A provider retry instruction exceeded the configured maximum backoff.
    RetryAfterExceedsPolicy,
    /// The shared synchronization primitive was poisoned.
    StatePoisoned,
    /// Internal checked counters could not advance consistently.
    StateCorrupt,
    /// Availability changed legitimately after a candidate lease was minted.
    AvailabilityChanged,
    /// Process clock could not produce a representable paired observation.
    ClockUnavailable,
    /// Availability generation exhausted and the allocation became irreversibly terminal.
    AvailabilityGenerationExhausted,
    /// Required restart-durable state could not be validated or replaced.
    PersistenceUnavailable,
}

/// Atomic dispatch outcome for one shared provider/account budget.
#[derive(Debug)]
pub enum BudgetDecision {
    /// A request slot was atomically reserved until this permit is dropped.
    Ready(BudgetPermit),
    /// Dispatch must wait until the inclusive instant is reached.
    WaitUntil(MonotonicInstant),
    /// Dispatch is unavailable without an external state change.
    Unavailable(BudgetUnavailableReason),
}

#[derive(Debug)]
pub(in crate::policy) struct BudgetState {
    pub(in crate::policy) window_started_at: MonotonicInstant,
    pub(in crate::policy) restored_window_ends_at: Option<MonotonicInstant>,
    pub(in crate::policy) requests_used: u32,
    pub(in crate::policy) in_flight: u16,
    pub(in crate::policy) unavailable_until: Option<MonotonicInstant>,
    pub(in crate::policy) disabled: bool,
    pub(in crate::policy) consecutive_refusals: u32,
}

pub(in crate::policy) struct BudgetAllocation {
    pub(in crate::policy) policy: ProviderBudgetPolicy,
    pub(in crate::policy) state: Mutex<BudgetState>,
    pub(in crate::policy) clock: Arc<dyn BudgetClock>,
    pub(in crate::policy) availability_generation: AtomicU64,
    pub(in crate::policy) terminal: AtomicBool,
    pub(in crate::policy) durability: Option<BudgetDurabilityBinding>,
}

#[derive(Clone)]
pub(in crate::policy) struct BudgetDurabilityBinding {
    pub(in crate::policy) session: Arc<AuthorityDurabilitySession>,
    pub(in crate::policy) slot: usize,
}

impl std::fmt::Debug for BudgetDurabilityBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BudgetDurabilityBinding")
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}
