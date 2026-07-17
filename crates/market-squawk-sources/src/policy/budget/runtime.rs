//! Thread-safe provider budget enforcement and fail-closed runtime transitions.

use super::*;

/// Thread-safe budget shared by every worker in one canonical collision group.
#[derive(Clone)]
pub struct SharedProviderBudget {
    pub(in crate::policy) allocation: Arc<BudgetAllocation>,
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
    ) -> Result<(), BudgetUnavailableReason> {
        let Some(binding) = &self.allocation.durability else {
            return Ok(());
        };
        let checkpoint = self
            .checkpoint_locked(state, observation)
            .map_err(|_| self.latch_persistence_failure())?;
        binding
            .session
            .update_budget(binding.slot, checkpoint, observation.wall_clock)
            .map_err(|_| self.latch_persistence_failure())
    }

    pub(in crate::policy) fn latch_persistence_failure(&self) -> BudgetUnavailableReason {
        self.allocation.terminal.store(true, Ordering::Release);
        let _ = self.allocation.availability_generation.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |generation| generation.checked_add(1),
        );
        BudgetUnavailableReason::PersistenceUnavailable
    }

    pub(in crate::policy) fn revoke_availability(&self) -> Result<(), BudgetUnavailableReason> {
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

    pub(in crate::policy) fn revoke_and_fail<T>(
        &self,
        reason: BudgetUnavailableReason,
    ) -> Result<T, BudgetUnavailableReason> {
        match self.revoke_availability() {
            Ok(()) => Err(reason),
            Err(terminal) => Err(terminal),
        }
    }

    pub(in crate::policy) fn revoke_persist_and_fail<T>(
        &self,
        state: &BudgetState,
        observation: ClockObservation,
        reason: BudgetUnavailableReason,
    ) -> Result<T, BudgetUnavailableReason> {
        let failure = self.revoke_and_fail::<T>(reason);
        self.persist_locked(state, observation)?;
        failure
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

    fn unavailable_locked(
        &self,
        state: &BudgetState,
        observation: ClockObservation,
        reason: BudgetUnavailableReason,
    ) -> BudgetDecision {
        let decision = self.unavailable(reason);
        if let Err(persistence) = self.persist_locked(state, observation) {
            BudgetDecision::Unavailable(persistence)
        } else {
            decision
        }
    }

    fn wait_until_locked(
        &self,
        state: &BudgetState,
        observation: ClockObservation,
        deadline: MonotonicInstant,
    ) -> BudgetDecision {
        let decision = self.wait_until(deadline);
        if let Err(persistence) = self.persist_locked(state, observation) {
            BudgetDecision::Unavailable(persistence)
        } else {
            decision
        }
    }

    fn unavailable_observed(
        &self,
        observation: ClockObservation,
        reason: BudgetUnavailableReason,
    ) -> BudgetDecision {
        let Ok(state) = self.allocation.state.lock() else {
            return BudgetDecision::Unavailable(self.latch_persistence_failure());
        };
        self.unavailable_locked(&state, observation, reason)
    }

    /// Atomically reserves one request from the shared window and concurrency limit.
    pub fn try_acquire(&self) -> BudgetDecision {
        if !self.durability_is_available() {
            return BudgetDecision::Unavailable(self.latch_persistence_failure());
        }
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
            return self.unavailable_locked(
                &state,
                observation,
                BudgetUnavailableReason::Disabled,
            );
        }
        if now < state.window_started_at {
            return self.unavailable_locked(
                &state,
                observation,
                BudgetUnavailableReason::ClockRegression,
            );
        }
        if let Some(until) = state.unavailable_until {
            if now < until {
                return self.wait_until_locked(&state, observation, until);
            }
            state.unavailable_until = None;
        }
        let Some(window_ends_at) = state.restored_window_ends_at.or_else(|| {
            state
                .window_started_at
                .checked_add(self.policy().window_nanos())
        })
        else {
            return self.unavailable_locked(
                &state,
                observation,
                BudgetUnavailableReason::DeadlineOverflow,
            );
        };
        if now >= window_ends_at {
            state.window_started_at = now;
            state.restored_window_ends_at = None;
            state.requests_used = 0;
        } else if state.requests_used >= self.policy().requests_per_window() {
            return self.wait_until_locked(&state, observation, window_ends_at);
        }
        if state.in_flight >= self.policy().max_concurrent() {
            return self.unavailable_locked(
                &state,
                observation,
                BudgetUnavailableReason::ConcurrencyExhausted,
            );
        }
        let Some(requests_used) = state.requests_used.checked_add(1) else {
            return self.unavailable_locked(
                &state,
                observation,
                BudgetUnavailableReason::StateCorrupt,
            );
        };
        let Some(in_flight) = state.in_flight.checked_add(1) else {
            return self.unavailable_locked(
                &state,
                observation,
                BudgetUnavailableReason::StateCorrupt,
            );
        };
        state.requests_used = requests_used;
        state.in_flight = in_flight;
        let became_unavailable = requests_used >= self.policy().requests_per_window()
            || in_flight >= self.policy().max_concurrent();
        let revoked = if became_unavailable {
            self.revoke_availability()
        } else {
            Ok(())
        };
        if let Err(reason) = self.persist_locked(&state, observation) {
            return BudgetDecision::Unavailable(reason);
        }
        if let Err(reason) = revoked {
            return BudgetDecision::Unavailable(reason);
        }
        BudgetDecision::Ready(BudgetPermit {
            allocation: Arc::clone(&self.allocation),
            released: false,
        })
    }

    /// Applies a bounded provider retry instruction to every worker sharing this budget.
    pub fn apply_retry_after(&self, retry_after: RetryAfter) -> BudgetDecision {
        if !self.durability_is_available() {
            return BudgetDecision::Unavailable(self.latch_persistence_failure());
        }
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
                if delay.get() > self.policy().backoff().maximum_nanos() {
                    return self.fail_closed_retry_after(observation);
                }
                let Some(deadline) = observation.monotonic.checked_add(delay.get()) else {
                    return self.unavailable_observed(
                        observation,
                        BudgetUnavailableReason::DeadlineOverflow,
                    );
                };
                deadline
            }
            RetryAfter::AtWallClock(deadline) => {
                let delay = deadline
                    .unix_nanos()
                    .checked_sub(observation.wall_clock.unix_nanos());
                let Some(delay) = delay else {
                    return self.unavailable_observed(
                        observation,
                        BudgetUnavailableReason::DeadlineOverflow,
                    );
                };
                if delay <= 0 {
                    let Ok(state) = self.allocation.state.lock() else {
                        return BudgetDecision::Unavailable(self.latch_persistence_failure());
                    };
                    return self.wait_until_locked(
                        &state,
                        observation,
                        observation.monotonic,
                    );
                }
                let Ok(delay) = u64::try_from(delay) else {
                    return self.unavailable_observed(
                        observation,
                        BudgetUnavailableReason::DeadlineOverflow,
                    );
                };
                if delay > self.policy().backoff().maximum_nanos() {
                    return self.fail_closed_retry_after(observation);
                }
                let Some(deadline) = observation.monotonic.checked_add(delay) else {
                    return self.unavailable_observed(
                        observation,
                        BudgetUnavailableReason::DeadlineOverflow,
                    );
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
        let persisted = self.persist_locked(&state, observation);
        drop(state);
        if let Err(reason) = persisted {
            return BudgetDecision::Unavailable(reason);
        }
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
        if !self.durability_is_available() {
            return BudgetDecision::Unavailable(self.latch_persistence_failure());
        }
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
            return self.unavailable_locked(
                &state,
                observation,
                BudgetUnavailableReason::StateCorrupt,
            );
        };
        let delay = self
            .policy()
            .backoff()
            .delay_nanos(attempt, jitter_sample_basis_points);
        let Some(deadline) = now.checked_add(delay) else {
            return self.unavailable_locked(
                &state,
                observation,
                BudgetUnavailableReason::DeadlineOverflow,
            );
        };
        state.consecutive_refusals = next_attempt;
        let effective = state
            .unavailable_until
            .map_or(deadline, |current| current.max(deadline));
        state.unavailable_until = Some(effective);
        let revoked = self.revoke_availability();
        let persisted = self.persist_locked(&state, observation);
        drop(state);
        if let Err(reason) = persisted {
            return BudgetDecision::Unavailable(reason);
        }
        match revoked {
            Ok(()) => BudgetDecision::WaitUntil(effective),
            Err(reason) => BudgetDecision::Unavailable(reason),
        }
    }

    /// Resets state-owned consecutive refusal escalation after a confirmed successful response.
    pub fn record_success(&self) -> Result<(), BudgetUnavailableReason> {
        if !self.durability_is_available() {
            return Err(self.latch_persistence_failure());
        }
        if self.allocation.terminal.load(Ordering::Acquire) {
            return Err(BudgetUnavailableReason::AvailabilityGenerationExhausted);
        }
        let observation = self
            .allocation
            .clock
            .observation()
            .map_err(|_| {
                self.revoke_availability()
                    .err()
                    .unwrap_or(BudgetUnavailableReason::ClockUnavailable)
            })?;
        let mut state = self
            .allocation
            .state
            .lock()
            .map_err(|_| {
                self.revoke_availability()
                    .err()
                    .unwrap_or(BudgetUnavailableReason::StatePoisoned)
            })?;
        state.consecutive_refusals = 0;
        self.persist_locked(&state, observation)?;
        Ok(())
    }

    /// Permanently disables dispatch until a new budget instance is explicitly configured.
    pub fn disable(&self) -> BudgetDecision {
        if !self.durability_is_available() {
            return BudgetDecision::Unavailable(self.latch_persistence_failure());
        }
        if self.allocation.terminal.load(Ordering::Acquire) {
            return BudgetDecision::Unavailable(
                BudgetUnavailableReason::AvailabilityGenerationExhausted,
            );
        }
        let Ok(observation) = self.allocation.clock.observation() else {
            return BudgetDecision::Unavailable(self.latch_persistence_failure());
        };
        let Ok(mut state) = self.allocation.state.lock() else {
            return self.unavailable(BudgetUnavailableReason::StatePoisoned);
        };
        state.disabled = true;
        self.unavailable_locked(&state, observation, BudgetUnavailableReason::Disabled)
    }

    fn fail_closed_retry_after(&self, observation: ClockObservation) -> BudgetDecision {
        let Ok(mut state) = self.allocation.state.lock() else {
            return self.unavailable(BudgetUnavailableReason::StatePoisoned);
        };
        state.disabled = true;
        self.unavailable_locked(
            &state,
            observation,
            BudgetUnavailableReason::RetryAfterExceedsPolicy,
        )
    }
}
