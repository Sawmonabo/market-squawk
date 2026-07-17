//! Thread-safe provider budget enforcement and fail-closed runtime transitions.

use super::*;

#[path = "runtime/failure.rs"]
mod failure;

/// Thread-safe budget shared by every worker in one canonical collision group.
#[derive(Clone)]
pub struct SharedProviderBudget {
    pub(in crate::policy) allocation: Arc<BudgetAllocation>,
}

/// Unforgeable runtime-operation composition proof retained across every fatal boundary.
pub(in crate::policy) struct RuntimeOperationAdmission {
    kind: RuntimeOperationAdmissionKind,
}

enum RuntimeOperationAdmissionKind {
    Ephemeral,
    Durable(AuthorityOperationAdmission),
}

impl std::fmt::Debug for RuntimeOperationAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeOperationAdmission")
            .field(
                "durable",
                &matches!(&self.kind, RuntimeOperationAdmissionKind::Durable(_)),
            )
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
impl RuntimeOperationAdmission {
    pub(in crate::policy) fn ephemeral_for_test() -> Self {
        Self {
            kind: RuntimeOperationAdmissionKind::Ephemeral,
        }
    }

    pub(in crate::policy) fn durable_for_test(token: AuthorityOperationAdmission) -> Self {
        Self {
            kind: RuntimeOperationAdmissionKind::Durable(token),
        }
    }
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
    #[cfg(test)]
    #[allow(clippy::panic)]
    pub(crate) fn poison_state_during_admitted_unwind_for_test(&self) -> bool {
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let Ok(_operation) = self.admit_runtime_operation() else {
                return;
            };
            let Ok(mut state) = self.allocation.state.lock() else {
                return;
            };
            state.requests_used = state.requests_used.saturating_add(1);
            panic!("test-only admitted budget-state unwind");
        }));
        unwind.is_err() && self.allocation.state.is_poisoned()
    }

    pub(in crate::policy) fn new(
        policy: ProviderBudgetPolicy,
        starts_at: MonotonicInstant,
        clock: Arc<dyn BudgetClock>,
    ) -> Self {
        Self {
            allocation: Arc::new(BudgetAllocation {
                policy,
                state: Mutex::new(BudgetState {
                    window_started_at: starts_at,
                    restored_window_ends_at: None,
                    requests_used: 0,
                    in_flight: 0,
                    unavailable_until: None,
                    disabled: false,
                    consecutive_refusals: 0,
                }),
                clock,
                availability_generation: AtomicU64::new(1),
                terminal: AtomicBool::new(false),
                durability: None,
            }),
        }
    }

    pub(in crate::policy) fn new_durable(
        policy: ProviderBudgetPolicy,
        starts_at: MonotonicInstant,
        clock: Arc<dyn BudgetClock>,
        binding: BudgetDurabilityBinding,
    ) -> Self {
        Self {
            allocation: Arc::new(BudgetAllocation {
                policy,
                state: Mutex::new(BudgetState {
                    window_started_at: starts_at,
                    restored_window_ends_at: None,
                    requests_used: 0,
                    in_flight: 0,
                    unavailable_until: None,
                    disabled: false,
                    consecutive_refusals: 0,
                }),
                clock,
                availability_generation: AtomicU64::new(1),
                terminal: AtomicBool::new(false),
                durability: Some(binding),
            }),
        }
    }

    pub(in crate::policy) fn from_checkpoint(
        policy: ProviderBudgetPolicy,
        checkpoint: &BudgetCheckpointState,
        clock: Arc<dyn BudgetClock>,
        binding: BudgetDurabilityBinding,
    ) -> Result<Self, AuthorityPersistenceError> {
        let observation = clock
            .observation()
            .map_err(|_| AuthorityPersistenceError::InvalidState)?;
        validate_checkpoint(&policy, checkpoint, observation)?;
        let window_remaining = checkpoint
            .window_ends_wall
            .unix_nanos()
            .saturating_sub(observation.wall_clock.unix_nanos());
        let restored_window_ends_at = if window_remaining > 0 {
            let remaining = u64::try_from(window_remaining)
                .map_err(|_| AuthorityPersistenceError::InvalidState)?;
            Some(
                observation
                    .monotonic
                    .checked_add(remaining)
                    .ok_or(AuthorityPersistenceError::InvalidState)?,
            )
        } else {
            None
        };
        let unavailable_until = match checkpoint.unavailable_until_wall {
            Some(until) if until > observation.wall_clock => {
                let delta = until
                    .unix_nanos()
                    .checked_sub(observation.wall_clock.unix_nanos())
                    .and_then(|value| u64::try_from(value).ok())
                    .ok_or(AuthorityPersistenceError::InvalidState)?;
                Some(
                    observation
                        .monotonic
                        .checked_add(delta)
                        .ok_or(AuthorityPersistenceError::InvalidState)?,
                )
            }
            Some(_) | None => None,
        };
        let window_expired = restored_window_ends_at.is_none();
        Ok(Self {
            allocation: Arc::new(BudgetAllocation {
                policy,
                state: Mutex::new(BudgetState {
                    window_started_at: observation.monotonic,
                    restored_window_ends_at,
                    requests_used: if window_expired {
                        0
                    } else {
                        checkpoint.requests_used
                    },
                    in_flight: if window_expired {
                        0
                    } else {
                        checkpoint.in_flight
                    },
                    unavailable_until,
                    disabled: checkpoint.disabled || checkpoint.poisoned,
                    consecutive_refusals: checkpoint.consecutive_refusals,
                }),
                clock,
                availability_generation: AtomicU64::new(
                    checkpoint.availability_generation,
                ),
                terminal: AtomicBool::new(checkpoint.terminal || checkpoint.poisoned),
                durability: Some(binding),
            }),
        })
    }

    /// Returns whether both handles share the process-authoritative allocation.
    pub fn shares_allocation_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.allocation, &other.allocation)
    }

    pub(in crate::policy) fn policy(&self) -> &ProviderBudgetPolicy {
        &self.allocation.policy
    }

    pub(in crate::policy) fn durability_is_available(&self) -> bool {
        self.allocation
            .durability
            .as_ref()
            .is_none_or(|binding| binding.session.is_available())
    }

    pub(in crate::policy) fn admit_runtime_operation(
        &self,
    ) -> Result<RuntimeOperationAdmission, BudgetUnavailableReason> {
        let kind = match &self.allocation.durability {
            Some(binding) => RuntimeOperationAdmissionKind::Durable(
                binding
                    .session
                    .admit_operation()
                    .map_err(|_| BudgetUnavailableReason::PersistenceUnavailable)?,
            ),
            None => RuntimeOperationAdmissionKind::Ephemeral,
        };
        Ok(RuntimeOperationAdmission { kind })
    }

    pub(in crate::policy) fn validated_durable_admission<'a>(
        &self,
        admission: &'a RuntimeOperationAdmission,
    ) -> Result<Option<&'a AuthorityOperationAdmission>, BudgetUnavailableReason> {
        match (&self.allocation.durability, &admission.kind) {
            (None, RuntimeOperationAdmissionKind::Ephemeral) => Ok(None),
            (Some(binding), RuntimeOperationAdmissionKind::Durable(token))
                if token.belongs_to(&binding.session) =>
            {
                Ok(Some(token))
            }
            (binding, token) => {
                if let Some(binding) = binding {
                    binding.session.invalidate();
                }
                if let RuntimeOperationAdmissionKind::Durable(token) = token {
                    token.invalidate_session();
                }
                self.allocation.terminal.store(true, Ordering::Release);
                let _previous = self.allocation.availability_generation.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |generation| generation.checked_add(1),
                );
                Err(BudgetUnavailableReason::PersistenceUnavailable)
            }
        }
    }

    pub(in crate::policy) fn checkpoint_locked(
        &self,
        state: &BudgetState,
        observation: ClockObservation,
    ) -> Result<BudgetCheckpointState, AuthorityPersistenceError> {
        checkpoint_from_runtime(
            self.policy(),
            state,
            observation,
            self.allocation
                .availability_generation
                .load(Ordering::Acquire),
            self.allocation.terminal.load(Ordering::Acquire),
        )
    }

    pub(in crate::policy) fn persist_locked(
        &self,
        state: &BudgetState,
        observation: ClockObservation,
        admission: &RuntimeOperationAdmission,
    ) -> Result<(), BudgetUnavailableReason> {
        let Some(binding) = &self.allocation.durability else {
            self.validated_durable_admission(admission)?;
            return Ok(());
        };
        let durable_admission = self
            .validated_durable_admission(admission)?
            .ok_or_else(|| self.latch_persistence_failure(admission))?;
        let checkpoint = self
            .checkpoint_locked(state, observation)
            .map_err(|_| self.latch_persistence_failure(admission))?;
        binding
            .session
            .update_budget_admitted(
                durable_admission,
                binding.slot,
                checkpoint,
                observation.wall_clock,
            )
            .map_err(|_| self.latch_persistence_failure(admission))
    }

    pub(in crate::policy) fn latch_persistence_failure(
        &self,
        admission: &RuntimeOperationAdmission,
    ) -> BudgetUnavailableReason {
        self.terminal_fault(BudgetUnavailableReason::PersistenceUnavailable, admission)
    }

    pub(in crate::policy) fn revoke_availability(
        &self,
        admission: &RuntimeOperationAdmission,
    ) -> Result<(), BudgetUnavailableReason> {
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
            return Err(self.terminal_fault(
                BudgetUnavailableReason::AvailabilityGenerationExhausted,
                admission,
            ));
        }
        Ok(())
    }

    pub(in crate::policy) fn revoke_persist_and_fail<T>(
        &self,
        state: &BudgetState,
        observation: ClockObservation,
        reason: BudgetUnavailableReason,
        admission: &RuntimeOperationAdmission,
    ) -> Result<T, BudgetUnavailableReason> {
        match self.revoke_availability(admission) {
            Ok(()) => {
                self.persist_locked(state, observation, admission)?;
                Err(reason)
            }
            Err(terminal) => Err(terminal),
        }
    }

    fn unavailable_locked(
        &self,
        state: &BudgetState,
        observation: ClockObservation,
        reason: BudgetUnavailableReason,
        admission: &RuntimeOperationAdmission,
    ) -> BudgetDecision {
        match self.revoke_availability(admission) {
            Ok(()) => match self.persist_locked(state, observation, admission) {
                Ok(()) => BudgetDecision::Unavailable(reason),
                Err(persistence) => BudgetDecision::Unavailable(persistence),
            },
            Err(terminal) => BudgetDecision::Unavailable(terminal),
        }
    }

    fn wait_until_locked(
        &self,
        state: &BudgetState,
        observation: ClockObservation,
        deadline: MonotonicInstant,
        admission: &RuntimeOperationAdmission,
    ) -> BudgetDecision {
        match self.revoke_availability(admission) {
            Ok(()) => match self.persist_locked(state, observation, admission) {
                Ok(()) => BudgetDecision::WaitUntil(deadline),
                Err(persistence) => BudgetDecision::Unavailable(persistence),
            },
            Err(terminal) => BudgetDecision::Unavailable(terminal),
        }
    }

    /// Atomically reserves one request from the shared window and concurrency limit.
    pub fn try_acquire(&self) -> BudgetDecision {
        let operation = match self.admit_runtime_operation() {
            Ok(operation) => operation,
            Err(reason) => return BudgetDecision::Unavailable(reason),
        };
        if self.allocation.terminal.load(Ordering::Acquire) {
            return BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted,
            );
        }
        let Ok(observation) = self.allocation.clock.observation() else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::ClockUnavailable,
                &operation,
            );
        };
        let now = observation.monotonic;
        let Ok(mut state) = self.allocation.state.lock() else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::StatePoisoned,
                &operation,
            );
        };
        if state.disabled {
            return self.unavailable_locked(
                &state,
                observation,
                BudgetUnavailableReason::Disabled,
                &operation,
            );
        }
        if now < state.window_started_at {
            return self.terminal_unavailable(
                BudgetUnavailableReason::ClockRegression,
                &operation,
            );
        }
        if let Some(until) = state.unavailable_until {
            if now < until {
                return self.wait_until_locked(
                    &state,
                    observation,
                    until,
                    &operation,
                );
            }
            state.unavailable_until = None;
        }
        let Some(window_ends_at) = state.restored_window_ends_at.or_else(|| {
            state
                .window_started_at
                .checked_add(self.policy().window_nanos())
        })
        else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::DeadlineOverflow,
                &operation,
            );
        };
        if now >= window_ends_at {
            state.window_started_at = now;
            state.restored_window_ends_at = None;
            state.requests_used = 0;
        } else if state.requests_used > self.policy().requests_per_window() {
            return self.terminal_unavailable(
                BudgetUnavailableReason::StateCorrupt,
                &operation,
            );
        } else if state.requests_used == self.policy().requests_per_window() {
            return self.wait_until_locked(
                &state,
                observation,
                window_ends_at,
                &operation,
            );
        }
        if state.in_flight > self.policy().max_concurrent() {
            return self.terminal_unavailable(
                BudgetUnavailableReason::StateCorrupt,
                &operation,
            );
        }
        if state.in_flight == self.policy().max_concurrent() {
            return self.unavailable_locked(
                &state,
                observation,
                BudgetUnavailableReason::ConcurrencyExhausted,
                &operation,
            );
        }
        let Some(requests_used) = state.requests_used.checked_add(1) else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::StateCorrupt,
                &operation,
            );
        };
        let Some(in_flight) = state.in_flight.checked_add(1) else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::StateCorrupt,
                &operation,
            );
        };
        state.requests_used = requests_used;
        state.in_flight = in_flight;
        let became_unavailable = requests_used >= self.policy().requests_per_window()
            || in_flight >= self.policy().max_concurrent();
        let revoked = if became_unavailable {
            self.revoke_availability(&operation)
        } else {
            Ok(())
        };
        if let Err(reason) = revoked {
            return BudgetDecision::Unavailable(reason);
        }
        if let Err(reason) = self.persist_locked(&state, observation, &operation) {
            return BudgetDecision::Unavailable(reason);
        }
        BudgetDecision::Ready(BudgetPermit {
            allocation: Arc::clone(&self.allocation),
            runtime_admission: operation,
            released: false,
        })
    }

    /// Applies a bounded provider retry instruction to every worker sharing this budget.
    pub fn apply_retry_after(&self, retry_after: RetryAfter) -> BudgetDecision {
        let operation = match self.admit_runtime_operation() {
            Ok(operation) => operation,
            Err(reason) => return BudgetDecision::Unavailable(reason),
        };
        if self.allocation.terminal.load(Ordering::Acquire) {
            return BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted,
            );
        }
        let Ok(observation) = self.allocation.clock.observation() else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::ClockUnavailable,
                &operation,
            );
        };
        let deadline = match retry_after {
            RetryAfter::Delay(delay) => {
                if delay.get() > self.policy().backoff().maximum_nanos() {
                    return self.fail_closed_retry_after(observation, &operation);
                }
                let Some(deadline) = observation.monotonic.checked_add(delay.get()) else {
                    return self.terminal_unavailable(
                        BudgetUnavailableReason::DeadlineOverflow,
                        &operation,
                    );
                };
                deadline
            }
            RetryAfter::AtWallClock(deadline) => {
                let delay = deadline
                    .unix_nanos()
                    .checked_sub(observation.wall_clock.unix_nanos());
                let Some(delay) = delay else {
                    return self.terminal_unavailable(
                        BudgetUnavailableReason::DeadlineOverflow,
                        &operation,
                    );
                };
                if delay <= 0 {
                    let Ok(state) = self.allocation.state.lock() else {
                        return self.terminal_unavailable(
                            BudgetUnavailableReason::StatePoisoned,
                            &operation,
                        );
                    };
                    return self.wait_until_locked(
                        &state,
                        observation,
                        observation.monotonic,
                        &operation,
                    );
                }
                let delay = delay.unsigned_abs();
                if delay > self.policy().backoff().maximum_nanos() {
                    return self.fail_closed_retry_after(observation, &operation);
                }
                let Some(deadline) = observation.monotonic.checked_add(delay) else {
                    return self.terminal_unavailable(
                        BudgetUnavailableReason::DeadlineOverflow,
                        &operation,
                    );
                };
                deadline
            }
        };
        let Ok(mut state) = self.allocation.state.lock() else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::StatePoisoned,
                &operation,
            );
        };
        let effective = state
            .unavailable_until
            .map_or(deadline, |current| current.max(deadline));
        state.unavailable_until = Some(effective);
        if let Err(reason) = self.revoke_availability(&operation) {
            return BudgetDecision::Unavailable(reason);
        }
        let persisted = self.persist_locked(&state, observation, &operation);
        drop(state);
        if let Err(reason) = persisted {
            return BudgetDecision::Unavailable(reason);
        }
        BudgetDecision::WaitUntil(effective)
    }

    /// Applies capped exponential backoff with a bounded caller-supplied jitter sample.
    ///
    /// The sample is capped by the configured jitter ceiling and cannot select an alternate
    /// identity, endpoint, proxy, or request shard.
    pub fn apply_refusal(&self, jitter_sample_basis_points: u16) -> BudgetDecision {
        let operation = match self.admit_runtime_operation() {
            Ok(operation) => operation,
            Err(reason) => return BudgetDecision::Unavailable(reason),
        };
        if self.allocation.terminal.load(Ordering::Acquire) {
            return BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted,
            );
        }
        let Ok(observation) = self.allocation.clock.observation() else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::ClockUnavailable,
                &operation,
            );
        };
        let now = observation.monotonic;
        let Ok(mut state) = self.allocation.state.lock() else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::StatePoisoned,
                &operation,
            );
        };
        let attempt = state.consecutive_refusals;
        let Some(next_attempt) = attempt.checked_add(1) else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::StateCorrupt,
                &operation,
            );
        };
        let delay = self
            .policy()
            .backoff()
            .delay_nanos(attempt, jitter_sample_basis_points);
        let Some(deadline) = now.checked_add(delay) else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::DeadlineOverflow,
                &operation,
            );
        };
        state.consecutive_refusals = next_attempt;
        let effective = state
            .unavailable_until
            .map_or(deadline, |current| current.max(deadline));
        state.unavailable_until = Some(effective);
        if let Err(reason) = self.revoke_availability(&operation) {
            return BudgetDecision::Unavailable(reason);
        }
        let persisted = self.persist_locked(&state, observation, &operation);
        drop(state);
        if let Err(reason) = persisted {
            return BudgetDecision::Unavailable(reason);
        }
        BudgetDecision::WaitUntil(effective)
    }

    /// Resets state-owned consecutive refusal escalation after a confirmed successful response.
    pub fn record_success(&self) -> Result<(), BudgetUnavailableReason> {
        let operation = self.admit_runtime_operation()?;
        if self.allocation.terminal.load(Ordering::Acquire) {
            return Err(BudgetUnavailableReason::AvailabilityGenerationExhausted);
        }
        let observation = self
            .allocation
            .clock
            .observation()
            .map_err(|_| {
                self.terminal_fault(
                    BudgetUnavailableReason::ClockUnavailable,
                    &operation,
                )
            })?;
        let mut state = self
            .allocation
            .state
            .lock()
            .map_err(|_| {
                self.terminal_fault(
                    BudgetUnavailableReason::StatePoisoned,
                    &operation,
                )
            })?;
        state.consecutive_refusals = 0;
        self.persist_locked(&state, observation, &operation)?;
        Ok(())
    }

    /// Permanently disables dispatch until a new budget instance is explicitly configured.
    pub fn disable(&self) -> BudgetDecision {
        let operation = match self.admit_runtime_operation() {
            Ok(operation) => operation,
            Err(reason) => return BudgetDecision::Unavailable(reason),
        };
        if self.allocation.terminal.load(Ordering::Acquire) {
            return BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted,
            );
        }
        let Ok(observation) = self.allocation.clock.observation() else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::ClockUnavailable,
                &operation,
            );
        };
        let Ok(mut state) = self.allocation.state.lock() else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::StatePoisoned,
                &operation,
            );
        };
        state.disabled = true;
        self.unavailable_locked(
            &state,
            observation,
            BudgetUnavailableReason::Disabled,
            &operation,
        )
    }

    fn fail_closed_retry_after(
        &self,
        observation: ClockObservation,
        admission: &RuntimeOperationAdmission,
    ) -> BudgetDecision {
        let Ok(mut state) = self.allocation.state.lock() else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::StatePoisoned,
                admission,
            );
        };
        state.disabled = true;
        self.unavailable_locked(
            &state,
            observation,
            BudgetUnavailableReason::RetryAfterExceedsPolicy,
            admission,
        )
    }
}
