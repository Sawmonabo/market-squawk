use super::*;

/// Lock-free lease proving a budget remained available at one allocation generation.
#[derive(Clone)]
pub(crate) struct BudgetAvailabilityLease {
    allocation: Arc<BudgetAllocation>,
    generation: u64,
}

impl std::fmt::Debug for BudgetAvailabilityLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BudgetAvailabilityLease")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl BudgetAvailabilityLease {
    pub(crate) fn is_available(&self) -> bool {
        !self.allocation.terminal.load(Ordering::Acquire)
            && self.allocation.availability_generation.load(Ordering::Acquire) == self.generation
            && !self.allocation.terminal.load(Ordering::Acquire)
    }

    pub(crate) fn shared_allocation_charge(&self) -> Option<usize> {
        std::mem::size_of::<BudgetAllocation>()
            .checked_add(crate::conservative_arc_control_block_charge::<
                BudgetAllocation,
            >())
            .and_then(|bytes| {
                self.allocation
                    .policy
                    .dynamic_retained_bytes()
                    .and_then(|dynamic| bytes.checked_add(dynamic))
            })
            .and_then(|bytes| {
                bytes.checked_add(self.allocation.clock.shared_allocation_charge())
            })
    }
}

impl SharedProviderBudget {
    pub(crate) fn availability_lease(
        &self,
    ) -> Result<BudgetAvailabilityLease, BudgetUnavailableReason> {
        if self.allocation.terminal.load(Ordering::Acquire) {
            return Err(BudgetUnavailableReason::AvailabilityGenerationExhausted);
        }
        let observation = match self.allocation.clock.observation() {
            Ok(observation) => observation,
            Err(reason) => return self.revoke_and_fail(reason),
        };
        let mut state = match self.allocation.state.lock() {
            Ok(state) => state,
            Err(_) => return self.revoke_and_fail(BudgetUnavailableReason::StatePoisoned),
        };
        if state.disabled {
            return self.revoke_and_fail(BudgetUnavailableReason::Disabled);
        }
        if observation.monotonic < state.window_started_at {
            return self.revoke_and_fail(BudgetUnavailableReason::ClockRegression);
        }
        if state
            .unavailable_until
            .is_some_and(|until| observation.monotonic < until)
        {
            return self.revoke_and_fail(BudgetUnavailableReason::CoolingDown);
        }
        state.unavailable_until = None;
        let Some(window_end) = state
            .window_started_at
            .checked_add(self.policy().window_nanos.get())
        else {
            return self.revoke_and_fail(BudgetUnavailableReason::DeadlineOverflow);
        };
        if observation.monotonic >= window_end {
            state.window_started_at = observation.monotonic;
            state.requests_used = 0;
        } else if state.requests_used >= self.policy().requests_per_window.get() {
            return self.revoke_and_fail(BudgetUnavailableReason::RequestWindowExhausted);
        }
        if state.in_flight >= self.policy().max_concurrent.get() {
            return self.revoke_and_fail(BudgetUnavailableReason::ConcurrencyExhausted);
        }
        let generation = self
            .allocation
            .availability_generation
            .load(Ordering::Acquire);
        drop(state);
        let lease = BudgetAvailabilityLease {
            allocation: Arc::clone(&self.allocation),
            generation,
        };
        if lease.is_available() {
            Ok(lease)
        } else {
            self.revoke_and_fail(BudgetUnavailableReason::StateCorrupt)
        }
    }
}

/// Sole composition-owned mint for one shared budget per structured provider/account scope.
pub(crate) struct ProviderBudgetPool {
    budgets: HashMap<BudgetScope, SharedProviderBudget>,
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
        Ok(Self {
            budgets: HashMap::new(),
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
            if existing.policy() == &policy {
                return Ok(existing.clone());
            }
            return Err(BudgetPoolError::ConflictingPolicy);
        }
        let scope = policy.scope().clone();
        let mut coordinated = coordinate_budget_policies(std::slice::from_ref(&policy))?;
        let budget = coordinated
            .remove(&scope)
            .ok_or(BudgetPoolError::CoordinatorCorrupt)?;
        self.budgets.insert(scope, budget.clone());
        Ok(budget)
    }

    pub(crate) fn register_all(
        &mut self,
        policies: &[ProviderBudgetPolicy],
    ) -> Result<(), BudgetPoolError> {
        let coordinated = coordinate_budget_policies(policies)?;
        self.budgets.extend(coordinated);
        Ok(())
    }

    pub(crate) fn policies(&self) -> Vec<ProviderBudgetPolicy> {
        self.budgets
            .values()
            .map(|budget| budget.policy().clone())
            .collect()
    }
}

const MAX_PROCESS_BUDGET_SCOPES: usize = 4_096;

struct ProcessBudgetCoordinator {
    allocations: HashMap<BudgetScope, Arc<BudgetAllocation>>,
    capacity: usize,
}

impl ProcessBudgetCoordinator {
    fn new(capacity: usize) -> Self {
        Self {
            allocations: HashMap::new(),
            capacity,
        }
    }

    fn coordinate(
        &mut self,
        policies: &[ProviderBudgetPolicy],
    ) -> Result<HashMap<BudgetScope, SharedProviderBudget>, BudgetPoolError> {
        let mut result: HashMap<BudgetScope, SharedProviderBudget> =
            HashMap::with_capacity(policies.len());
        let mut staged = Vec::new();
        for policy in policies {
            if let Some(existing) = result.get(policy.scope()) {
                if existing.policy() != policy {
                    return Err(BudgetPoolError::ConflictingPolicy);
                }
                continue;
            }
            if let Some(existing) = self.allocations.get(policy.scope()) {
                if existing.policy != *policy {
                    return Err(BudgetPoolError::ConflictingPolicy);
                }
                result.insert(
                    policy.scope().clone(),
                    SharedProviderBudget {
                        allocation: Arc::clone(existing),
                    },
                );
                continue;
            }
            let clock: Arc<dyn BudgetClock> = Arc::new(SystemBudgetClock::new());
            let starts_at = clock
                .observation()
                .map_err(|_| BudgetPoolError::ClockUnavailable)?
                .monotonic;
            let budget = SharedProviderBudget::new(policy.clone(), starts_at, clock);
            result.insert(policy.scope().clone(), budget.clone());
            staged.push((policy.scope().clone(), budget));
        }
        if self
            .allocations
            .len()
            .checked_add(staged.len())
            .is_none_or(|count| count > self.capacity)
        {
            return Err(BudgetPoolError::CoordinatorCapacity);
        }
        for (scope, budget) in staged {
            self.allocations.insert(scope, budget.allocation);
        }
        Ok(result)
    }
}

static BUDGET_COORDINATOR: OnceLock<Mutex<ProcessBudgetCoordinator>> = OnceLock::new();

fn coordinate_budget_policies(
    policies: &[ProviderBudgetPolicy],
) -> Result<HashMap<BudgetScope, SharedProviderBudget>, BudgetPoolError> {
    let coordinator = BUDGET_COORDINATOR
        .get_or_init(|| Mutex::new(ProcessBudgetCoordinator::new(MAX_PROCESS_BUDGET_SCOPES)));
    let mut coordinator = coordinator
        .lock()
        .map_err(|_| BudgetPoolError::CoordinatorPoisoned)?;
    coordinator.coordinate(policies)
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
    /// The process-wide coordinator lock was poisoned.
    #[error("provider budget coordinator is poisoned")]
    CoordinatorPoisoned,
    /// The bounded process-lifetime authoritative-scope capacity was exhausted.
    #[error("provider budget coordinator capacity exhausted")]
    CoordinatorCapacity,
    /// Coordinator staging lost an allocation before publication.
    #[error("provider budget coordinator state is corrupt")]
    CoordinatorCorrupt,
}

pub(super) trait BudgetClock: Send + Sync {
    fn observation(&self) -> Result<ClockObservation, BudgetUnavailableReason>;

    fn shared_allocation_charge(&self) -> usize;
}

#[cfg(test)]
mod coordinator_tests {
    use std::error::Error;
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
    use std::sync::atomic::Ordering;

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn Error>>;

    fn test_policy(scope: &str, requests_per_window: u32) -> TestResult<ProviderBudgetPolicy> {
        Ok(ProviderBudgetPolicy::try_new(
            BudgetScope::new(SourceIdentifier::try_from(scope)?),
            NonZeroU32::new(requests_per_window).ok_or("request limit must be nonzero")?,
            NonZeroU64::new(60_000_000_000).ok_or("window must be nonzero")?,
            NonZeroU16::new(1).ok_or("concurrency must be nonzero")?,
            BackoffPolicy::try_new(
                NonZeroU64::new(1_000_000).ok_or("backoff must be nonzero")?,
                NonZeroU64::new(60_000_000_000).ok_or("backoff cap must be nonzero")?,
                0,
            )?,
        )?)
    }

    fn register_fresh(policy: ProviderBudgetPolicy) -> TestResult<SharedProviderBudget> {
        let mut pool = ProviderBudgetPool::new()?;
        Ok(pool.register(policy)?)
    }

    #[test]
    fn dropping_every_external_handle_cannot_reset_request_state() -> TestResult {
        let policy = test_policy("drop-reset-request-state", 1)?;
        let budget = register_fresh(policy.clone())?;
        let permit = match budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("unexpected first acquire: {other:?}").into()),
        };
        permit.release();
        drop(budget);

        let restored = register_fresh(policy)?;
        assert!(matches!(restored.try_acquire(), BudgetDecision::WaitUntil(_)));
        Ok(())
    }

    #[test]
    fn dropping_every_external_handle_preserves_refusal_disabled_and_terminal_state()
    -> TestResult {
        let refusal_policy = test_policy("drop-reset-refusal-state", 2)?;
        let refusal = register_fresh(refusal_policy.clone())?;
        let deadline = match refusal.apply_refusal(0) {
            BudgetDecision::WaitUntil(deadline) => deadline,
            other => return Err(format!("unexpected refusal decision: {other:?}").into()),
        };
        drop(refusal);
        let refusal_restored = register_fresh(refusal_policy)?;
        assert!(matches!(
            refusal_restored.try_acquire(),
            BudgetDecision::WaitUntil(observed) if observed == deadline
        ));

        let disabled_policy = test_policy("drop-reset-disabled-state", 2)?;
        let disabled = register_fresh(disabled_policy.clone())?;
        assert!(matches!(
            disabled.disable(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled)
        ));
        drop(disabled);
        let disabled_restored = register_fresh(disabled_policy)?;
        assert!(matches!(
            disabled_restored.try_acquire(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::Disabled)
        ));

        let terminal_policy = test_policy("drop-reset-terminal-state", 2)?;
        let terminal = register_fresh(terminal_policy.clone())?;
        terminal
            .allocation
            .availability_generation
            .store(u64::MAX, Ordering::Release);
        assert!(matches!(
            terminal.disable(),
            BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted
            )
        ));
        drop(terminal);
        let terminal_restored = register_fresh(terminal_policy)?;
        assert!(matches!(
            terminal_restored.try_acquire(),
            BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted
            )
        ));
        Ok(())
    }

    #[test]
    fn coordinator_capacity_and_conflict_fail_without_mutating_authoritative_state()
    -> TestResult {
        let first_policy = test_policy("bounded-coordinator-first", 1)?;
        let second_policy = test_policy("bounded-coordinator-second", 1)?;
        let mut coordinator = ProcessBudgetCoordinator::new(1);
        let first = coordinator.coordinate(std::slice::from_ref(&first_policy))?;
        let first_budget = first
            .get(first_policy.scope())
            .ok_or("first coordinated budget missing")?;
        let permit = match first_budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("unexpected bounded acquire: {other:?}").into()),
        };
        permit.release();
        drop(first);
        let retained = Arc::clone(
            coordinator
                .allocations
                .get(first_policy.scope())
                .ok_or("retained allocation missing")?,
        );

        assert!(matches!(
            coordinator.coordinate(std::slice::from_ref(&second_policy)),
            Err(BudgetPoolError::CoordinatorCapacity)
        ));
        assert_eq!(coordinator.allocations.len(), 1);
        assert!(!coordinator
            .allocations
            .contains_key(second_policy.scope()));
        assert!(Arc::ptr_eq(
            coordinator
                .allocations
                .get(first_policy.scope())
                .ok_or("first allocation removed after capacity failure")?,
            &retained,
        ));

        let conflicting = test_policy("bounded-coordinator-first", 2)?;
        assert!(matches!(
            coordinator.coordinate(std::slice::from_ref(&conflicting)),
            Err(BudgetPoolError::ConflictingPolicy)
        ));
        assert_eq!(coordinator.allocations.len(), 1);
        let restored = coordinator.coordinate(std::slice::from_ref(&first_policy))?;
        assert!(matches!(
            restored
                .get(first_policy.scope())
                .ok_or("restored retained allocation missing")?
                .try_acquire(),
            BudgetDecision::WaitUntil(_)
        ));
        Ok(())
    }
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

    fn shared_allocation_charge(&self) -> usize {
        std::mem::size_of::<Self>() + crate::conservative_arc_control_block_charge::<Self>()
    }
}

/// RAII reservation for one in-flight provider request.
pub struct BudgetPermit {
    pub(super) allocation: Arc<BudgetAllocation>,
    pub(super) released: bool,
}

impl std::fmt::Debug for BudgetPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BudgetPermit")
            .field("released", &self.released)
            .finish_non_exhaustive()
    }
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
        if let Ok(mut state) = self.allocation.state.lock()
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
