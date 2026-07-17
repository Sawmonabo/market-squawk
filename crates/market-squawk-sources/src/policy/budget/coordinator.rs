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
            && !self.allocation.state.is_poisoned()
            && self
                .allocation
                .durability
                .as_ref()
                .is_none_or(|binding| binding.session.is_available())
            && self.allocation.availability_generation.load(Ordering::Acquire) == self.generation
            && !self.allocation.state.is_poisoned()
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
        let operation = self.admit_runtime_operation()?;
        if self.allocation.terminal.load(Ordering::Acquire) {
            return Err(BudgetUnavailableReason::AvailabilityGenerationExhausted);
        }
        let observation = match self.allocation.clock.observation() {
            Ok(observation) => observation,
            Err(_reason) => {
                return self.terminal_fail(
                    BudgetUnavailableReason::ClockUnavailable,
                    &operation,
                );
            }
        };
        let mut state = match self.allocation.state.lock() {
            Ok(state) => state,
            Err(_) => {
                return self.terminal_fail(
                    BudgetUnavailableReason::StatePoisoned,
                    &operation,
                );
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
        if observation.monotonic < state.window_started_at {
            return self.terminal_fail(
                BudgetUnavailableReason::ClockRegression,
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
        let Some(window_end) = state.restored_window_ends_at.or_else(|| {
            state
                .window_started_at
                .checked_add(self.policy().window_nanos())
        })
        else {
            return self.terminal_fail(
                BudgetUnavailableReason::DeadlineOverflow,
                &operation,
            );
        };
        if observation.monotonic >= window_end {
            state.window_started_at = observation.monotonic;
            state.restored_window_ends_at = None;
            state.requests_used = 0;
        } else if state.requests_used > self.policy().requests_per_window() {
            return self.terminal_fail(
                BudgetUnavailableReason::StateCorrupt,
                &operation,
            );
        } else if state.requests_used == self.policy().requests_per_window() {
            return self.revoke_persist_and_fail(
                &state,
                observation,
                BudgetUnavailableReason::RequestWindowExhausted,
                &operation,
            );
        }
        if state.in_flight > self.policy().max_concurrent() {
            return self.terminal_fail(
                BudgetUnavailableReason::StateCorrupt,
                &operation,
            );
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
        };
        if lease.is_available() {
            Ok(lease)
        } else if !self.durability_is_available() {
            self.terminal_fail(
                BudgetUnavailableReason::PersistenceUnavailable,
                &operation,
            )
        } else if self.allocation.terminal.load(Ordering::Acquire) {
            Err(BudgetUnavailableReason::AvailabilityGenerationExhausted)
        } else {
            Err(BudgetUnavailableReason::AvailabilityChanged)
        }
    }
}

#[path = "coordinator/durability.rs"]
mod durability;
pub use durability::{BudgetPermit, BudgetPoolError};
pub(in crate::policy) use durability::CleanShutdownProof;
pub(crate) use durability::ProviderBudgetPool;

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
