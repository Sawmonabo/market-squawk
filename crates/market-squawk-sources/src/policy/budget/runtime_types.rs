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
    pub(in crate::policy) const fn new(wall_clock: Timestamp, monotonic: MonotonicInstant) -> Self {
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
    /// A response-control operation unexpectedly produced a request permit.
    Ready(BudgetPermit),
    /// Dispatch must wait until the inclusive instant is reached.
    WaitUntil(MonotonicInstant),
    /// Dispatch is unavailable without an external state change.
    Unavailable(BudgetUnavailableReason),
}

/// Concurrency-reservation outcome before any provider request window is charged.
#[derive(Debug)]
pub enum BudgetReservationDecision {
    /// Concurrency is reserved; the request must still commit at the transport dispatch boundary.
    Ready(BudgetReservation),
    /// Reservation must wait until the inclusive instant is reached.
    WaitUntil(MonotonicInstant),
    /// Reservation is unavailable without an external state change.
    Unavailable(BudgetUnavailableReason),
}

/// Dispatch outcome after consuming one exact concurrency reservation.
#[derive(Debug)]
pub enum BudgetDispatchDecision {
    /// Request windows are durably charged and the permit must span response classification.
    Ready(BudgetPermit),
    /// No request was charged; retry at or after the inclusive instant.
    WaitUntil(MonotonicInstant),
    /// No request was charged and progress requires an external state change.
    Unavailable(BudgetUnavailableReason),
}

#[derive(Debug)]
pub(in crate::policy) struct BudgetState {
    pub(in crate::policy) window_started_at: MonotonicInstant,
    pub(in crate::policy) restored_window_ends_at: Option<MonotonicInstant>,
    pub(in crate::policy) requests_used: u32,
    pub(in crate::policy) primary_sliding_releases: VecDeque<MonotonicInstant>,
    pub(in crate::policy) additional_windows: Vec<BudgetWindowRuntimeState>,
    pub(in crate::policy) in_flight: u16,
    pub(in crate::policy) unavailable_until: Option<MonotonicInstant>,
    pub(in crate::policy) disabled: bool,
    pub(in crate::policy) consecutive_refusals: u32,
}

#[derive(Debug)]
pub(in crate::policy) struct BudgetWindowRuntimeState {
    pub(in crate::policy) window_started_at: MonotonicInstant,
    pub(in crate::policy) restored_window_ends_at: Option<MonotonicInstant>,
    pub(in crate::policy) requests_used: u32,
    pub(in crate::policy) sliding_releases: VecDeque<MonotonicInstant>,
}

impl BudgetWindowRuntimeState {
    fn new(window: ProviderBudgetWindow, starts_at: MonotonicInstant) -> Self {
        Self {
            window_started_at: starts_at,
            restored_window_ends_at: None,
            requests_used: 0,
            sliding_releases: preallocated_sliding_releases(window),
        }
    }

    fn dynamic_retained_bytes(&self) -> Option<usize> {
        self.sliding_releases
            .capacity()
            .checked_mul(std::mem::size_of::<MonotonicInstant>())
    }
}

impl BudgetState {
    pub(in crate::policy) fn new(
        policy: &ProviderBudgetPolicy,
        starts_at: MonotonicInstant,
    ) -> Self {
        let additional_windows = policy
            .windows()
            .skip(1)
            .map(|window| BudgetWindowRuntimeState::new(window, starts_at))
            .collect();
        Self {
            window_started_at: starts_at,
            restored_window_ends_at: None,
            requests_used: 0,
            primary_sliding_releases: policy
                .window(0)
                .map_or_else(VecDeque::new, preallocated_sliding_releases),
            additional_windows,
            in_flight: 0,
            unavailable_until: None,
            disabled: false,
            consecutive_refusals: 0,
        }
    }

    pub(in crate::policy) fn dynamic_retained_bytes(&self) -> Option<usize> {
        let primary = self
            .primary_sliding_releases
            .capacity()
            .checked_mul(std::mem::size_of::<MonotonicInstant>())?;
        let windows = self
            .additional_windows
            .capacity()
            .checked_mul(std::mem::size_of::<BudgetWindowRuntimeState>())?;
        self.additional_windows
            .iter()
            .try_fold(primary.checked_add(windows)?, |bytes, window| {
                bytes.checked_add(window.dynamic_retained_bytes()?)
            })
    }
}

fn preallocated_sliding_releases(window: ProviderBudgetWindow) -> VecDeque<MonotonicInstant> {
    if window.semantics() == BudgetWindowSemantics::Sliding {
        VecDeque::with_capacity(
            usize::try_from(window.requests_per_window()).map_or(0, std::convert::identity),
        )
    } else {
        VecDeque::new()
    }
}

pub(in crate::policy) struct BudgetAllocation {
    pub(in crate::policy) policy: ProviderBudgetPolicy,
    pub(in crate::policy) state: Mutex<BudgetState>,
    pub(in crate::policy) clock: Arc<dyn BudgetClock>,
    pub(in crate::policy) availability_generation: AtomicU64,
    pub(in crate::policy) terminal: AtomicBool,
    pub(in crate::policy) durability: Option<BudgetDurabilityBinding>,
    pub(in crate::policy) provider_rate: Option<ProviderRateBinding>,
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
