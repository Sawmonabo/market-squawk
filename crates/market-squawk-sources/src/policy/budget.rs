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

    /// Returns the configured scope key.
    pub const fn as_source_identifier(&self) -> &SourceIdentifier {
        &self.provider
    }

    /// Returns the non-secret authorization/account reference when configured.
    pub const fn authorization_account(&self) -> Option<&SourceIdentifier> {
        self.authorization_account.as_ref()
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

/// Thread-safe budget shared by every worker in one configured provider/account scope.
#[derive(Clone)]
pub struct SharedProviderBudget {
    policy: ProviderBudgetPolicy,
    state: Arc<Mutex<BudgetState>>,
    clock: Arc<dyn BudgetClock>,
}

impl std::fmt::Debug for SharedProviderBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedProviderBudget")
            .field("scope", self.policy.scope())
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
            policy,
            state: Arc::new(Mutex::new(BudgetState {
                window_started_at: starts_at,
                requests_used: 0,
                in_flight: 0,
                unavailable_until: None,
                disabled: false,
                consecutive_refusals: 0,
            })),
            clock,
        }
    }

    /// Atomically reserves one request from the shared window and concurrency limit.
    pub fn try_acquire(&self) -> BudgetDecision {
        let Ok(observation) = self.clock.observation() else {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::ClockUnavailable);
        };
        let now = observation.monotonic;
        let Ok(mut state) = self.state.lock() else {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::StatePoisoned);
        };
        if state.disabled {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled);
        }
        if now < state.window_started_at {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::ClockRegression);
        }
        if let Some(until) = state.unavailable_until {
            if now < until {
                return BudgetDecision::WaitUntil(until);
            }
            state.unavailable_until = None;
        }
        let Some(window_ends_at) = state
            .window_started_at
            .checked_add(self.policy.window_nanos.get())
        else {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::DeadlineOverflow);
        };
        if now >= window_ends_at {
            state.window_started_at = now;
            state.requests_used = 0;
        } else if state.requests_used >= self.policy.requests_per_window.get() {
            return BudgetDecision::WaitUntil(window_ends_at);
        }
        if state.in_flight >= self.policy.max_concurrent.get() {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::ConcurrencyExhausted);
        }
        let Some(requests_used) = state.requests_used.checked_add(1) else {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::StateCorrupt);
        };
        let Some(in_flight) = state.in_flight.checked_add(1) else {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::StateCorrupt);
        };
        state.requests_used = requests_used;
        state.in_flight = in_flight;
        BudgetDecision::Ready(BudgetPermit {
            state: Arc::clone(&self.state),
            released: false,
        })
    }

    /// Applies a bounded provider retry instruction to every worker sharing this budget.
    pub fn apply_retry_after(&self, retry_after: RetryAfter) -> BudgetDecision {
        let Ok(observation) = self.clock.observation() else {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::ClockUnavailable);
        };
        let deadline = match retry_after {
            RetryAfter::Delay(delay) => {
                if delay.get() > self.policy.backoff.maximum_nanos() {
                    return self.fail_closed_retry_after();
                }
                let Some(deadline) = observation.monotonic.checked_add(delay.get()) else {
                    return BudgetDecision::Unavailable(BudgetUnavailableReason::DeadlineOverflow);
                };
                deadline
            }
            RetryAfter::AtWallClock(deadline) => {
                let delay = deadline
                    .unix_nanos()
                    .checked_sub(observation.wall_clock.unix_nanos());
                let Some(delay) = delay else {
                    return BudgetDecision::Unavailable(BudgetUnavailableReason::DeadlineOverflow);
                };
                if delay <= 0 {
                    return BudgetDecision::WaitUntil(observation.monotonic);
                }
                let Ok(delay) = u64::try_from(delay) else {
                    return BudgetDecision::Unavailable(BudgetUnavailableReason::DeadlineOverflow);
                };
                if delay > self.policy.backoff.maximum_nanos() {
                    return self.fail_closed_retry_after();
                }
                let Some(deadline) = observation.monotonic.checked_add(delay) else {
                    return BudgetDecision::Unavailable(BudgetUnavailableReason::DeadlineOverflow);
                };
                deadline
            }
        };
        let Ok(mut state) = self.state.lock() else {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::StatePoisoned);
        };
        let effective = state
            .unavailable_until
            .map_or(deadline, |current| current.max(deadline));
        state.unavailable_until = Some(effective);
        BudgetDecision::WaitUntil(effective)
    }

    /// Applies capped exponential backoff with a bounded caller-supplied jitter sample.
    ///
    /// The sample is capped by the configured jitter ceiling and cannot select an alternate
    /// identity, endpoint, proxy, or request shard.
    pub fn apply_refusal(&self, jitter_sample_basis_points: u16) -> BudgetDecision {
        let Ok(observation) = self.clock.observation() else {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::ClockUnavailable);
        };
        let now = observation.monotonic;
        let Ok(mut state) = self.state.lock() else {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::StatePoisoned);
        };
        let attempt = state.consecutive_refusals;
        let Some(next_attempt) = attempt.checked_add(1) else {
            state.disabled = true;
            return BudgetDecision::Unavailable(BudgetUnavailableReason::StateCorrupt);
        };
        let delay = self
            .policy
            .backoff
            .delay_nanos(attempt, jitter_sample_basis_points);
        let Some(deadline) = now.checked_add(delay) else {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::DeadlineOverflow);
        };
        state.consecutive_refusals = next_attempt;
        let effective = state
            .unavailable_until
            .map_or(deadline, |current| current.max(deadline));
        state.unavailable_until = Some(effective);
        BudgetDecision::WaitUntil(effective)
    }

    /// Resets state-owned consecutive refusal escalation after a confirmed successful response.
    pub fn record_success(&self) -> Result<(), BudgetUnavailableReason> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| BudgetUnavailableReason::StatePoisoned)?;
        state.consecutive_refusals = 0;
        Ok(())
    }

    /// Permanently disables dispatch until a new budget instance is explicitly configured.
    pub fn disable(&self) -> BudgetDecision {
        let Ok(mut state) = self.state.lock() else {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::StatePoisoned);
        };
        state.disabled = true;
        BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled)
    }

    fn fail_closed_retry_after(&self) -> BudgetDecision {
        let Ok(mut state) = self.state.lock() else {
            return BudgetDecision::Unavailable(BudgetUnavailableReason::StatePoisoned);
        };
        state.disabled = true;
        BudgetDecision::Unavailable(BudgetUnavailableReason::RetryAfterExceedsPolicy)
    }
}

/// Sole composition-owned mint for one shared budget per structured provider/account scope.
pub(crate) struct ProviderBudgetPool {
    budgets: HashMap<BudgetScope, SharedProviderBudget>,
    clock: Arc<dyn BudgetClock>,
}

impl std::fmt::Debug for ProviderBudgetPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderBudgetPool")
            .field("registered_scopes", &self.budgets.len())
            .finish_non_exhaustive()
    }
}

impl ProviderBudgetPool {
    pub(crate) fn new() -> Result<Self, BudgetPoolError> {
        let clock: Arc<dyn BudgetClock> = Arc::new(SystemBudgetClock::new());
        clock
            .observation()
            .map_err(|_| BudgetPoolError::ClockUnavailable)?;
        Ok(Self {
            budgets: HashMap::new(),
            clock,
        })
    }

    /// Registers a policy or returns the existing handle when the exact policy already exists.
    ///
    /// # Errors
    ///
    /// Rejects a conflicting policy for an already registered scope.
    pub(crate) fn register(
        &mut self,
        policy: ProviderBudgetPolicy,
    ) -> Result<SharedProviderBudget, BudgetPoolError> {
        if let Some(existing) = self.budgets.get(policy.scope()) {
            if existing.policy == policy {
                return Ok(existing.clone());
            }
            return Err(BudgetPoolError::ConflictingPolicy);
        }
        let scope = policy.scope().clone();
        let starts_at = self
            .clock
            .observation()
            .map_err(|_| BudgetPoolError::ClockUnavailable)?
            .monotonic;
        let budget = SharedProviderBudget::new(policy, starts_at, Arc::clone(&self.clock));
        self.budgets.insert(scope, budget.clone());
        Ok(budget)
    }

    pub(crate) fn policies(&self) -> Vec<ProviderBudgetPolicy> {
        self.budgets
            .values()
            .map(|budget| budget.policy.clone())
            .collect()
    }
}

/// Shared budget registration failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum BudgetPoolError {
    /// The scope already exists with different published/local limits.
    #[error("provider budget scope already has a conflicting policy")]
    ConflictingPolicy,
    /// Process monotonic/wall clock observation was unavailable or unrepresentable.
    #[error("provider budget clock is unavailable")]
    ClockUnavailable,
}

trait BudgetClock: Send + Sync {
    fn observation(&self) -> Result<ClockObservation, BudgetUnavailableReason>;
}

#[derive(Debug)]
struct SystemBudgetClock {
    origin: Instant,
}

impl SystemBudgetClock {
    fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl BudgetClock for SystemBudgetClock {
    fn observation(&self) -> Result<ClockObservation, BudgetUnavailableReason> {
        let elapsed_duration = Instant::now()
            .checked_duration_since(self.origin)
            .ok_or(BudgetUnavailableReason::ClockUnavailable)?;
        let elapsed = u64::try_from(elapsed_duration.as_nanos())
            .map_err(|_| BudgetUnavailableReason::ClockUnavailable)?;
        let wall_duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BudgetUnavailableReason::ClockUnavailable)?;
        let wall_nanos = i64::try_from(wall_duration.as_nanos())
            .map_err(|_| BudgetUnavailableReason::ClockUnavailable)?;
        Ok(ClockObservation::new(
            Timestamp::from_unix_nanos(wall_nanos),
            MonotonicInstant::from_nanos(elapsed),
        ))
    }
}

/// RAII reservation for one in-flight provider request.
#[derive(Debug)]
pub struct BudgetPermit {
    state: Arc<Mutex<BudgetState>>,
    released: bool,
}

impl BudgetPermit {
    /// Explicitly releases the concurrency slot; request-window consumption remains recorded.
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        if let Ok(mut state) = self.state.lock()
            && let Some(in_flight) = state.in_flight.checked_sub(1)
        {
            state.in_flight = in_flight;
        }
        self.released = true;
    }
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        self.release_inner();
    }
}
