use super::*;

/// Test probe for dispatch-generation and durable-budget failure cases.
/// Received-data authority does not retain request availability.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct BudgetAvailabilityLease {
    allocation: Arc<BudgetAllocation>,
    generation: u64,
    provider_generation: Option<u64>,
}

#[cfg(test)]
impl std::fmt::Debug for BudgetAvailabilityLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BudgetAvailabilityLease")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
impl BudgetAvailabilityLease {
    pub(crate) fn is_available(&self) -> bool {
        self.provider_generation.is_none_or(|generation| {
            self.allocation
                .provider_rate
                .as_ref()
                .is_some_and(|binding| binding.availability_generation_is_current(generation))
        }) && !self.allocation.terminal.load(Ordering::Acquire)
            && !self.allocation.state.is_poisoned()
            && self
                .allocation
                .durability
                .as_ref()
                .is_none_or(|binding| binding.session.is_available())
            && self
                .allocation
                .availability_generation
                .load(Ordering::Acquire)
                == self.generation
            && !self.allocation.state.is_poisoned()
            && !self.allocation.terminal.load(Ordering::Acquire)
    }
}

#[cfg(test)]
impl SharedProviderBudget {
    pub(crate) fn availability_lease(
        &self,
    ) -> Result<BudgetAvailabilityLease, BudgetUnavailableReason> {
        let operation = self.admit_runtime_operation()?;
        if self.allocation.terminal.load(Ordering::Acquire) {
            return Err(BudgetUnavailableReason::AvailabilityGenerationExhausted);
        }
        let mut state = match self.allocation.state.lock() {
            Ok(state) => state,
            Err(_) => {
                return self.terminal_fail(BudgetUnavailableReason::StatePoisoned, &operation);
            }
        };
        if let Some(binding) = &self.allocation.provider_rate {
            let (availability, provider_generation) = binding
                .availability_lease_generation()
                .map_err(|reason| self.terminal_fault(reason, &operation))?;
            let reason = match availability {
                ProviderRateAvailability::Available => None,
                ProviderRateAvailability::WaitUntil(_) => {
                    Some(BudgetUnavailableReason::AvailabilityChanged)
                }
                ProviderRateAvailability::Unavailable(reason) => Some(reason),
            };
            if let Some(reason) = reason {
                self.revoke_availability(&operation)?;
                return Err(reason);
            }
            let lease = BudgetAvailabilityLease {
                allocation: Arc::clone(&self.allocation),
                generation: self
                    .allocation
                    .availability_generation
                    .load(Ordering::Acquire),
                provider_generation: Some(provider_generation),
            };
            drop(state);
            return if lease.is_available() {
                Ok(lease)
            } else {
                Err(BudgetUnavailableReason::AvailabilityChanged)
            };
        }
        let observation = match self.allocation.clock.observation() {
            Ok(observation) => observation,
            Err(_reason) => {
                return self.terminal_fail(BudgetUnavailableReason::ClockUnavailable, &operation);
            }
        };
        if state.disabled {
            return self.revoke_persist_and_fail(
                &state,
                observation,
                BudgetUnavailableReason::Disabled,
                &operation,
            );
        }
        if state
            .unavailable_until
            .is_some_and(|until| observation.monotonic < until)
        {
            return self.revoke_persist_and_fail(
                &state,
                observation,
                BudgetUnavailableReason::CoolingDown,
                &operation,
            );
        }
        state.unavailable_until = None;
        let availability =
            evaluate_budget_windows(self.policy(), &mut state, observation.monotonic)
                .map_err(|reason| self.terminal_fault(reason, &operation))?;
        if availability.blocker.is_some() {
            return self.revoke_persist_and_fail(
                &state,
                observation,
                BudgetUnavailableReason::RequestWindowExhausted,
                &operation,
            );
        }
        if state.in_flight > self.policy().max_concurrent() {
            return self.terminal_fail(BudgetUnavailableReason::StateCorrupt, &operation);
        }
        if state.in_flight == self.policy().max_concurrent() {
            return self.revoke_persist_and_fail(
                &state,
                observation,
                BudgetUnavailableReason::ConcurrencyExhausted,
                &operation,
            );
        }
        let generation = self
            .allocation
            .availability_generation
            .load(Ordering::Acquire);
        self.persist_locked(&state, observation, &operation)?;
        drop(state);
        let lease = BudgetAvailabilityLease {
            allocation: Arc::clone(&self.allocation),
            generation,
            provider_generation: None,
        };
        if lease.is_available() {
            Ok(lease)
        } else if !self.durability_is_available() {
            self.terminal_fail(BudgetUnavailableReason::PersistenceUnavailable, &operation)
        } else if self.allocation.terminal.load(Ordering::Acquire) {
            Err(BudgetUnavailableReason::AvailabilityGenerationExhausted)
        } else {
            Err(BudgetUnavailableReason::AvailabilityChanged)
        }
    }
}

#[path = "coordinator/durability.rs"]
mod durability;
pub(in crate::policy) use durability::CleanShutdownProof;
pub(crate) use durability::ProviderBudgetPool;
pub use durability::{BudgetPermit, BudgetPermitLease, BudgetPoolError, BudgetReservation};

pub(super) trait BudgetClock: Send + Sync {
    fn observation(&self) -> Result<ClockObservation, BudgetUnavailableReason>;

    fn shared_allocation_charge(&self) -> usize;
}

#[derive(Debug)]
pub(in crate::policy) struct SystemBudgetClock {
    origin: Instant,
}

impl SystemBudgetClock {
    pub(in crate::policy) fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl BudgetClock for SystemBudgetClock {
    fn observation(&self) -> Result<ClockObservation, BudgetUnavailableReason> {
        observe_system_clocks(self.origin, SystemTime::now, Instant::now)
    }

    fn shared_allocation_charge(&self) -> usize {
        std::mem::size_of::<Self>() + crate::conservative_arc_control_block_charge::<Self>()
    }
}

/// Samples wall time before monotonic time so a scheduling delay can only extend a converted
/// provider deadline. Sampling in the inverse order could make Retry-After and rate-window
/// deadlines expire early by the suspension interval between the two reads.
fn observe_system_clocks(
    origin: Instant,
    wall_now: impl FnOnce() -> SystemTime,
    monotonic_now: impl FnOnce() -> Instant,
) -> Result<ClockObservation, BudgetUnavailableReason> {
    let wall_duration = wall_now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BudgetUnavailableReason::ClockUnavailable)?;
    let elapsed_duration = monotonic_now()
        .checked_duration_since(origin)
        .ok_or(BudgetUnavailableReason::ClockUnavailable)?;
    let elapsed = u64::try_from(elapsed_duration.as_nanos())
        .map_err(|_| BudgetUnavailableReason::ClockUnavailable)?;
    let wall_nanos = i64::try_from(wall_duration.as_nanos())
        .map_err(|_| BudgetUnavailableReason::ClockUnavailable)?;
    Ok(ClockObservation::new(
        Timestamp::from_unix_nanos(wall_nanos),
        MonotonicInstant::from_nanos(elapsed),
    ))
}
