/// One shared provider/account budget identity without credentials or alternate identities.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetScope {
    provider: SourceIdentifier,
    authorization_account: Option<SourceIdentifier>,
}

impl BudgetScope {
    /// Constructs a bounded configured provider/account budget key.
    pub const fn new(value: SourceIdentifier) -> Self {
        Self {
            provider: value,
            authorization_account: None,
        }
    }

    /// Constructs a provider scope qualified by one non-secret authorization/account reference.
    pub const fn with_authorization_account(
        provider: SourceIdentifier,
        authorization_account: SourceIdentifier,
    ) -> Self {
        Self {
            provider,
            authorization_account: Some(authorization_account),
        }
    }

    /// Derives the only valid provider/account scope for an evidenced authorization grant.
    ///
    /// # Errors
    ///
    /// Rejects local user-owned authorization because it must not have a remote provider budget.
    pub fn for_authorization(
        provider: SourceIdentifier,
        authorization: &crate::AuthorizationGrant,
    ) -> Result<Self, NetworkPolicyError> {
        match authorization.mode() {
            crate::AuthorizationMode::PublicInterface => Ok(Self::new(provider)),
            crate::AuthorizationMode::UserAuthorized | crate::AuthorizationMode::Licensed => {
                Ok(Self::with_authorization_account(
                    provider,
                    authorization.basis().as_source_identifier().clone(),
                ))
            }
            crate::AuthorizationMode::UserOwnedLocal => {
                Err(NetworkPolicyError::InvalidBudgetScope)
            }
        }
    }

    /// Returns the configured scope key.
    pub const fn as_source_identifier(&self) -> &SourceIdentifier {
        &self.provider
    }

    /// Returns the non-secret authorization/account reference when configured.
    pub const fn authorization_account(&self) -> Option<&SourceIdentifier> {
        self.authorization_account.as_ref()
    }

    fn dynamic_retained_bytes(&self) -> Option<usize> {
        self.provider.retained_bytes().checked_add(
            self.authorization_account
                .as_ref()
                .map_or(0, SourceIdentifier::retained_bytes),
        )
    }
}

/// Bounded exponential-backoff settings applied to provider refusal responses.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackoffPolicy {
    initial_nanos: NonZeroU64,
    maximum_nanos: NonZeroU64,
    jitter_basis_points: u16,
}

impl BackoffPolicy {
    /// Constructs bounded backoff settings.
    ///
    /// # Errors
    ///
    /// Rejects an initial delay above its maximum or jitter above 100 percent.
    pub const fn try_new(
        initial_nanos: NonZeroU64,
        maximum_nanos: NonZeroU64,
        jitter_basis_points: u16,
    ) -> Result<Self, NetworkPolicyError> {
        if initial_nanos.get() > maximum_nanos.get() || jitter_basis_points > 10_000 {
            Err(NetworkPolicyError::InvalidBudgetPolicy)
        } else {
            Ok(Self {
                initial_nanos,
                maximum_nanos,
                jitter_basis_points,
            })
        }
    }

    /// Returns the maximum provider backoff in nanoseconds.
    pub const fn maximum_nanos(self) -> u64 {
        self.maximum_nanos.get()
    }

    fn delay_nanos(self, attempt: u32, jitter_sample_basis_points: u16) -> u64 {
        let shift = attempt.min(63);
        let base = self
            .initial_nanos
            .get()
            .checked_shl(shift)
            .unwrap_or(self.maximum_nanos.get())
            .min(self.maximum_nanos.get());
        let sample = jitter_sample_basis_points.min(self.jitter_basis_points);
        let jitter =
            (u128::from(base) * u128::from(sample) / 10_000).min(u128::from(u64::MAX)) as u64;
        base.checked_add(jitter)
            .unwrap_or(self.maximum_nanos.get())
            .min(self.maximum_nanos.get())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BackoffPolicyWire {
    initial_nanos: NonZeroU64,
    maximum_nanos: NonZeroU64,
    jitter_basis_points: u16,
}

impl<'de> Deserialize<'de> for BackoffPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BackoffPolicyWire::deserialize(deserializer)?;
        Self::try_new(
            wire.initial_nanos,
            wire.maximum_nanos,
            wire.jitter_basis_points,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Published request-window and local concurrency limits for one shared scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderBudgetPolicy {
    scope: BudgetScope,
    requests_per_window: NonZeroU32,
    window_nanos: NonZeroU64,
    max_concurrent: NonZeroU16,
    backoff: BackoffPolicy,
}

impl ProviderBudgetPolicy {
    /// Constructs a provider budget with no alternate identity, endpoint, or shard policy.
    pub fn try_new(
        scope: BudgetScope,
        requests_per_window: NonZeroU32,
        window_nanos: NonZeroU64,
        max_concurrent: NonZeroU16,
        backoff: BackoffPolicy,
    ) -> Result<Self, NetworkPolicyError> {
        if window_nanos.get() > i64::MAX as u64
            || u32::from(max_concurrent.get()) > requests_per_window.get()
        {
            return Err(NetworkPolicyError::InvalidBudgetPolicy);
        }
        Ok(Self {
            scope,
            requests_per_window,
            window_nanos,
            max_concurrent,
            backoff,
        })
    }

    /// Returns the single shared provider/account scope.
    pub const fn scope(&self) -> &BudgetScope {
        &self.scope
    }

    fn dynamic_retained_bytes(&self) -> Option<usize> {
        self.scope.dynamic_retained_bytes()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderBudgetPolicyWire {
    scope: BudgetScope,
    requests_per_window: NonZeroU32,
    window_nanos: NonZeroU64,
    max_concurrent: NonZeroU16,
    backoff: BackoffPolicy,
}

impl<'de> Deserialize<'de> for ProviderBudgetPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderBudgetPolicyWire::deserialize(deserializer)?;
        Self::try_new(
            wire.scope,
            wire.requests_per_window,
            wire.window_nanos,
            wire.max_concurrent,
            wire.backoff,
        )
        .map_err(serde::de::Error::custom)
    }
}

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
    const fn from_nanos(value: u64) -> Self {
        Self(value)
    }

    /// Returns the monotonic nanosecond reading.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    fn checked_add(self, nanos: u64) -> Option<Self> {
        self.0.checked_add(nanos).map(Self)
    }
}

/// Paired wall/monotonic observation for converting an absolute HTTP Retry-After date once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClockObservation {
    wall_clock: Timestamp,
    monotonic: MonotonicInstant,
}

impl ClockObservation {
    const fn new(wall_clock: Timestamp, monotonic: MonotonicInstant) -> Self {
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
    /// Process clock could not produce a representable paired observation.
    ClockUnavailable,
    /// Availability generation exhausted and the allocation became irreversibly terminal.
    AvailabilityGenerationExhausted,
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
struct BudgetState {
    window_started_at: MonotonicInstant,
    requests_used: u32,
    in_flight: u16,
    unavailable_until: Option<MonotonicInstant>,
    disabled: bool,
    consecutive_refusals: u32,
}

struct BudgetAllocation {
    policy: ProviderBudgetPolicy,
    state: Mutex<BudgetState>,
    clock: Arc<dyn BudgetClock>,
    availability_generation: AtomicU64,
    terminal: AtomicBool,
}

/// Thread-safe budget shared by every worker in one configured provider/account scope.
#[derive(Clone)]
pub struct SharedProviderBudget {
    allocation: Arc<BudgetAllocation>,
}

impl std::fmt::Debug for SharedProviderBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedProviderBudget")
            .field("scope", self.allocation.policy.scope())
            .finish_non_exhaustive()
    }
}

impl SharedProviderBudget {
    fn new(
        policy: ProviderBudgetPolicy,
        starts_at: MonotonicInstant,
        clock: Arc<dyn BudgetClock>,
    ) -> Self {
        Self {
            allocation: Arc::new(BudgetAllocation {
                policy,
                state: Mutex::new(BudgetState {
                    window_started_at: starts_at,
                    requests_used: 0,
                    in_flight: 0,
                    unavailable_until: None,
                    disabled: false,
                    consecutive_refusals: 0,
                }),
                clock,
                availability_generation: AtomicU64::new(1),
                terminal: AtomicBool::new(false),
            }),
        }
    }

    /// Returns whether both handles share the process-authoritative allocation.
    pub fn shares_allocation_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.allocation, &other.allocation)
    }

    fn policy(&self) -> &ProviderBudgetPolicy {
        &self.allocation.policy
    }

    fn revoke_availability(&self) -> Result<(), BudgetUnavailableReason> {
        if self.allocation.terminal.load(Ordering::Acquire) {
            return Err(BudgetUnavailableReason::AvailabilityGenerationExhausted);
        }
        if self
            .allocation
            .availability_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                generation.checked_add(1)
            })
            .is_err()
        {
            self.allocation.terminal.store(true, Ordering::Release);
            return Err(BudgetUnavailableReason::AvailabilityGenerationExhausted);
        }
        Ok(())
    }

    fn revoke_and_fail<T>(
        &self,
        reason: BudgetUnavailableReason,
    ) -> Result<T, BudgetUnavailableReason> {
        match self.revoke_availability() {
            Ok(()) => Err(reason),
            Err(terminal) => Err(terminal),
        }
    }

    fn unavailable(&self, reason: BudgetUnavailableReason) -> BudgetDecision {
        match self.revoke_availability() {
            Ok(()) => BudgetDecision::Unavailable(reason),
            Err(overflow) => BudgetDecision::Unavailable(overflow),
        }
    }

    fn wait_until(&self, deadline: MonotonicInstant) -> BudgetDecision {
        match self.revoke_availability() {
            Ok(()) => BudgetDecision::WaitUntil(deadline),
            Err(overflow) => BudgetDecision::Unavailable(overflow),
        }
    }

    /// Atomically reserves one request from the shared window and concurrency limit.
    pub fn try_acquire(&self) -> BudgetDecision {
        if self.allocation.terminal.load(Ordering::Acquire) {
            return BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted,
            );
        }
        let Ok(observation) = self.allocation.clock.observation() else {
            return self.unavailable(BudgetUnavailableReason::ClockUnavailable);
        };
        let now = observation.monotonic;
        let Ok(mut state) = self.allocation.state.lock() else {
            return self.unavailable(BudgetUnavailableReason::StatePoisoned);
        };
        if state.disabled {
            return self.unavailable(BudgetUnavailableReason::Disabled);
        }
        if now < state.window_started_at {
            return self.unavailable(BudgetUnavailableReason::ClockRegression);
        }
        if let Some(until) = state.unavailable_until {
            if now < until {
                return self.wait_until(until);
            }
            state.unavailable_until = None;
        }
        let Some(window_ends_at) = state
            .window_started_at
            .checked_add(self.policy().window_nanos.get())
        else {
            return self.unavailable(BudgetUnavailableReason::DeadlineOverflow);
        };
        if now >= window_ends_at {
            state.window_started_at = now;
            state.requests_used = 0;
        } else if state.requests_used >= self.policy().requests_per_window.get() {
            return self.wait_until(window_ends_at);
        }
        if state.in_flight >= self.policy().max_concurrent.get() {
            return self.unavailable(BudgetUnavailableReason::ConcurrencyExhausted);
        }
        let Some(requests_used) = state.requests_used.checked_add(1) else {
            return self.unavailable(BudgetUnavailableReason::StateCorrupt);
        };
        let Some(in_flight) = state.in_flight.checked_add(1) else {
            return self.unavailable(BudgetUnavailableReason::StateCorrupt);
        };
        state.requests_used = requests_used;
        state.in_flight = in_flight;
        let became_unavailable = requests_used >= self.policy().requests_per_window.get()
            || in_flight >= self.policy().max_concurrent.get();
        if became_unavailable
            && let Err(reason) = self.revoke_availability()
        {
            return BudgetDecision::Unavailable(reason);
        }
        BudgetDecision::Ready(BudgetPermit {
            allocation: Arc::clone(&self.allocation),
            released: false,
        })
    }

    /// Applies a bounded provider retry instruction to every worker sharing this budget.
    pub fn apply_retry_after(&self, retry_after: RetryAfter) -> BudgetDecision {
        if self.allocation.terminal.load(Ordering::Acquire) {
            return BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted,
            );
        }
        let Ok(observation) = self.allocation.clock.observation() else {
            return self.unavailable(BudgetUnavailableReason::ClockUnavailable);
        };
        let deadline = match retry_after {
            RetryAfter::Delay(delay) => {
                if delay.get() > self.policy().backoff.maximum_nanos() {
                    return self.fail_closed_retry_after();
                }
                let Some(deadline) = observation.monotonic.checked_add(delay.get()) else {
                    return self.unavailable(BudgetUnavailableReason::DeadlineOverflow);
                };
                deadline
            }
            RetryAfter::AtWallClock(deadline) => {
                let delay = deadline
                    .unix_nanos()
                    .checked_sub(observation.wall_clock.unix_nanos());
                let Some(delay) = delay else {
                    return self.unavailable(BudgetUnavailableReason::DeadlineOverflow);
                };
                if delay <= 0 {
                    return self.wait_until(observation.monotonic);
                }
                let Ok(delay) = u64::try_from(delay) else {
                    return self.unavailable(BudgetUnavailableReason::DeadlineOverflow);
                };
                if delay > self.policy().backoff.maximum_nanos() {
                    return self.fail_closed_retry_after();
                }
                let Some(deadline) = observation.monotonic.checked_add(delay) else {
                    return self.unavailable(BudgetUnavailableReason::DeadlineOverflow);
                };
                deadline
            }
        };
        let Ok(mut state) = self.allocation.state.lock() else {
            return self.unavailable(BudgetUnavailableReason::StatePoisoned);
        };
        let effective = state
            .unavailable_until
            .map_or(deadline, |current| current.max(deadline));
        state.unavailable_until = Some(effective);
        let revoked = self.revoke_availability();
        drop(state);
        match revoked {
            Ok(()) => BudgetDecision::WaitUntil(effective),
            Err(reason) => BudgetDecision::Unavailable(reason),
        }
    }

    /// Applies capped exponential backoff with a bounded caller-supplied jitter sample.
    ///
    /// The sample is capped by the configured jitter ceiling and cannot select an alternate
    /// identity, endpoint, proxy, or request shard.
    pub fn apply_refusal(&self, jitter_sample_basis_points: u16) -> BudgetDecision {
        if self.allocation.terminal.load(Ordering::Acquire) {
            return BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted,
            );
        }
        let Ok(observation) = self.allocation.clock.observation() else {
            return self.unavailable(BudgetUnavailableReason::ClockUnavailable);
        };
        let now = observation.monotonic;
        let Ok(mut state) = self.allocation.state.lock() else {
            return self.unavailable(BudgetUnavailableReason::StatePoisoned);
        };
        let attempt = state.consecutive_refusals;
        let Some(next_attempt) = attempt.checked_add(1) else {
            state.disabled = true;
            return self.unavailable(BudgetUnavailableReason::StateCorrupt);
        };
        let delay = self
            .policy()
            .backoff
            .delay_nanos(attempt, jitter_sample_basis_points);
        let Some(deadline) = now.checked_add(delay) else {
            return self.unavailable(BudgetUnavailableReason::DeadlineOverflow);
        };
        state.consecutive_refusals = next_attempt;
        let effective = state
            .unavailable_until
            .map_or(deadline, |current| current.max(deadline));
        state.unavailable_until = Some(effective);
        let revoked = self.revoke_availability();
        drop(state);
        match revoked {
            Ok(()) => BudgetDecision::WaitUntil(effective),
            Err(reason) => BudgetDecision::Unavailable(reason),
        }
    }

    /// Resets state-owned consecutive refusal escalation after a confirmed successful response.
    pub fn record_success(&self) -> Result<(), BudgetUnavailableReason> {
        if self.allocation.terminal.load(Ordering::Acquire) {
            return Err(BudgetUnavailableReason::AvailabilityGenerationExhausted);
        }
        let mut state = self
            .allocation
            .state
            .lock()
            .map_err(|_| {
                let _ = self.revoke_availability();
                BudgetUnavailableReason::StatePoisoned
            })?;
        state.consecutive_refusals = 0;
        Ok(())
    }

    /// Permanently disables dispatch until a new budget instance is explicitly configured.
    pub fn disable(&self) -> BudgetDecision {
        if self.allocation.terminal.load(Ordering::Acquire) {
            return BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted,
            );
        }
        let Ok(mut state) = self.allocation.state.lock() else {
            return self.unavailable(BudgetUnavailableReason::StatePoisoned);
        };
        state.disabled = true;
        self.unavailable(BudgetUnavailableReason::Disabled)
    }

    fn fail_closed_retry_after(&self) -> BudgetDecision {
        let Ok(mut state) = self.allocation.state.lock() else {
            return self.unavailable(BudgetUnavailableReason::StatePoisoned);
        };
        state.disabled = true;
        self.unavailable(BudgetUnavailableReason::RetryAfterExceedsPolicy)
    }
}

#[path = "budget/coordinator.rs"]
mod budget_coordinator;

use budget_coordinator::BudgetClock;
pub use budget_coordinator::{BudgetPermit, BudgetPoolError};
pub(crate) use budget_coordinator::{BudgetAvailabilityLease, ProviderBudgetPool};
