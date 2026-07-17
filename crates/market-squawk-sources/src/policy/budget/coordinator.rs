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
static BUDGET_COORDINATOR: OnceLock<Mutex<HashMap<BudgetScope, Weak<BudgetAllocation>>>> =
    OnceLock::new();

fn coordinate_budget_policies(
    policies: &[ProviderBudgetPolicy],
) -> Result<HashMap<BudgetScope, SharedProviderBudget>, BudgetPoolError> {
    let coordinator = BUDGET_COORDINATOR.get_or_init(|| Mutex::new(HashMap::new()));
    let mut coordinator = coordinator
        .lock()
        .map_err(|_| BudgetPoolError::CoordinatorPoisoned)?;
    coordinator.retain(|_, allocation| allocation.strong_count() > 0);
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
        if let Some(existing) = coordinator.get(policy.scope()).and_then(Weak::upgrade) {
            if existing.policy != *policy {
                return Err(BudgetPoolError::ConflictingPolicy);
            }
            result.insert(
                policy.scope().clone(),
                SharedProviderBudget {
                    allocation: existing,
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
    if coordinator
        .len()
        .checked_add(staged.len())
        .is_none_or(|count| count > MAX_PROCESS_BUDGET_SCOPES)
    {
        return Err(BudgetPoolError::CoordinatorCapacity);
    }
    for (scope, budget) in staged {
        coordinator.insert(scope, Arc::downgrade(&budget.allocation));
    }
    Ok(result)
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
    /// The bounded process-wide active-scope capacity was exhausted.
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
