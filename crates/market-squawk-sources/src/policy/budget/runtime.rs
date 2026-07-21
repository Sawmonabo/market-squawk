//! Thread-safe provider budget enforcement and fail-closed runtime transitions.

use super::*;

#[path = "runtime/failure.rs"]
mod failure;

#[derive(Clone, Copy)]
pub(in crate::policy) struct BudgetWindowsAvailability {
    pub(in crate::policy) blocker: Option<MonotonicInstant>,
    sliding_deadlines: [Option<MonotonicInstant>; MAX_PROVIDER_BUDGET_WINDOWS],
}

pub(in crate::policy) fn evaluate_budget_windows(
    policy: &ProviderBudgetPolicy,
    state: &mut BudgetState,
    now: MonotonicInstant,
) -> Result<BudgetWindowsAvailability, BudgetUnavailableReason> {
    if state.additional_windows.len() + 1 != policy.window_count() {
        return Err(BudgetUnavailableReason::StateCorrupt);
    }
    let mut availability = BudgetWindowsAvailability {
        blocker: None,
        sliding_deadlines: [None; MAX_PROVIDER_BUDGET_WINDOWS],
    };
    let primary = policy
        .window(0)
        .ok_or(BudgetUnavailableReason::StateCorrupt)?;
    evaluate_budget_window(
        primary,
        &mut state.window_started_at,
        &mut state.restored_window_ends_at,
        &mut state.requests_used,
        &mut state.primary_sliding_releases,
        now,
        0,
        &mut availability,
    )?;
    for (index, (window, window_state)) in policy
        .windows()
        .skip(1)
        .zip(&mut state.additional_windows)
        .enumerate()
    {
        evaluate_budget_window(
            window,
            &mut window_state.window_started_at,
            &mut window_state.restored_window_ends_at,
            &mut window_state.requests_used,
            &mut window_state.sliding_releases,
            now,
            index + 1,
            &mut availability,
        )?;
    }
    Ok(availability)
}

#[allow(clippy::too_many_arguments)]
fn evaluate_budget_window(
    window: ProviderBudgetWindow,
    window_started_at: &mut MonotonicInstant,
    restored_window_ends_at: &mut Option<MonotonicInstant>,
    requests_used: &mut u32,
    sliding_releases: &mut VecDeque<MonotonicInstant>,
    now: MonotonicInstant,
    index: usize,
    availability: &mut BudgetWindowsAvailability,
) -> Result<(), BudgetUnavailableReason> {
    if now < *window_started_at {
        return Err(BudgetUnavailableReason::ClockRegression);
    }
    match window.semantics() {
        BudgetWindowSemantics::Tumbling => {
            if !sliding_releases.is_empty() || sliding_releases.capacity() != 0 {
                return Err(BudgetUnavailableReason::StateCorrupt);
            }
            let window_ends_at = restored_window_ends_at
                .or_else(|| window_started_at.checked_add(window.window_nanos()))
                .ok_or(BudgetUnavailableReason::DeadlineOverflow)?;
            if now >= window_ends_at {
                *window_started_at = now;
                *restored_window_ends_at = None;
                *requests_used = 0;
            } else if *requests_used > window.requests_per_window() {
                return Err(BudgetUnavailableReason::StateCorrupt);
            } else if *requests_used == window.requests_per_window() {
                availability.blocker = Some(
                    availability
                        .blocker
                        .map_or(window_ends_at, |blocker| blocker.max(window_ends_at)),
                );
            }
        }
        BudgetWindowSemantics::Sliding => {
            if restored_window_ends_at.is_some() {
                return Err(BudgetUnavailableReason::StateCorrupt);
            }
            while sliding_releases.front().is_some_and(|deadline| *deadline <= now) {
                let _expired = sliding_releases.pop_front();
            }
            let retained = u32::try_from(sliding_releases.len())
                .map_err(|_| BudgetUnavailableReason::StateCorrupt)?;
            let required_capacity = usize::try_from(window.requests_per_window())
                .map_err(|_| BudgetUnavailableReason::StateCorrupt)?;
            if retained > window.requests_per_window()
                || sliding_releases.capacity() < required_capacity
            {
                return Err(BudgetUnavailableReason::StateCorrupt);
            }
            *requests_used = retained;
            *window_started_at = now;
            if retained == window.requests_per_window() {
                let blocker = sliding_releases
                    .front()
                    .copied()
                    .ok_or(BudgetUnavailableReason::StateCorrupt)?;
                availability.blocker = Some(
                    availability
                        .blocker
                        .map_or(blocker, |current| current.max(blocker)),
                );
            } else {
                let deadline = now
                    .checked_add(window.window_nanos())
                    .ok_or(BudgetUnavailableReason::DeadlineOverflow)?;
                let slot = availability
                    .sliding_deadlines
                    .get_mut(index)
                    .ok_or(BudgetUnavailableReason::StateCorrupt)?;
                *slot = Some(deadline);
            }
        }
    }
    Ok(())
}

fn validate_budget_windows(
    policy: &ProviderBudgetPolicy,
    state: &BudgetState,
    now: MonotonicInstant,
) -> Result<(), BudgetUnavailableReason> {
    if state.additional_windows.len() + 1 != policy.window_count() {
        return Err(BudgetUnavailableReason::StateCorrupt);
    }
    let primary = policy
        .window(0)
        .ok_or(BudgetUnavailableReason::StateCorrupt)?;
    validate_budget_window(
        primary,
        state.window_started_at,
        state.restored_window_ends_at,
        state.requests_used,
        &state.primary_sliding_releases,
        now,
    )?;
    for (window, window_state) in policy
        .windows()
        .skip(1)
        .zip(&state.additional_windows)
    {
        validate_budget_window(
            window,
            window_state.window_started_at,
            window_state.restored_window_ends_at,
            window_state.requests_used,
            &window_state.sliding_releases,
            now,
        )?;
    }
    Ok(())
}

fn validate_budget_window(
    window: ProviderBudgetWindow,
    window_started_at: MonotonicInstant,
    restored_window_ends_at: Option<MonotonicInstant>,
    requests_used: u32,
    sliding_releases: &VecDeque<MonotonicInstant>,
    now: MonotonicInstant,
) -> Result<(), BudgetUnavailableReason> {
    if now < window_started_at {
        return Err(BudgetUnavailableReason::ClockRegression);
    }
    match window.semantics() {
        BudgetWindowSemantics::Tumbling => {
            if !sliding_releases.is_empty()
                || sliding_releases.capacity() != 0
                || requests_used > window.requests_per_window()
                || restored_window_ends_at
                    .or_else(|| window_started_at.checked_add(window.window_nanos()))
                    .is_none()
            {
                return Err(BudgetUnavailableReason::StateCorrupt);
            }
        }
        BudgetWindowSemantics::Sliding => {
            let retained = u32::try_from(sliding_releases.len())
                .map_err(|_| BudgetUnavailableReason::StateCorrupt)?;
            let required_capacity = usize::try_from(window.requests_per_window())
                .map_err(|_| BudgetUnavailableReason::StateCorrupt)?;
            if restored_window_ends_at.is_some()
                || retained != requests_used
                || retained > window.requests_per_window()
                || sliding_releases.capacity() < required_capacity
                || sliding_releases
                    .iter()
                    .zip(sliding_releases.iter().skip(1))
                    .any(|(left, right)| left > right)
            {
                return Err(BudgetUnavailableReason::StateCorrupt);
            }
        }
    }
    Ok(())
}

fn admit_budget_windows(
    policy: &ProviderBudgetPolicy,
    state: &mut BudgetState,
    availability: BudgetWindowsAvailability,
) -> Result<bool, BudgetUnavailableReason> {
    for (index, window) in policy.windows().enumerate() {
        if window.semantics() == BudgetWindowSemantics::Sliding
            && availability
                .sliding_deadlines
                .get(index)
                .copied()
                .flatten()
                .is_none()
        {
            return Err(BudgetUnavailableReason::StateCorrupt);
        }
    }
    let primary_deadline = availability
        .sliding_deadlines
        .first()
        .copied()
        .flatten();
    admit_budget_window(
        policy
            .window(0)
            .ok_or(BudgetUnavailableReason::StateCorrupt)?,
        &mut state.requests_used,
        &mut state.primary_sliding_releases,
        primary_deadline,
    );
    for (index, (window, window_state)) in policy
        .windows()
        .skip(1)
        .zip(&mut state.additional_windows)
        .enumerate()
    {
        let deadline = availability
            .sliding_deadlines
            .get(index + 1)
            .copied()
            .flatten();
        admit_budget_window(
            window,
            &mut window_state.requests_used,
            &mut window_state.sliding_releases,
            deadline,
        );
    }
    let primary_exhausted = policy
        .window(0)
        .is_some_and(|window| state.requests_used >= window.requests_per_window());
    let additional_exhausted = policy
        .windows()
        .skip(1)
        .zip(&state.additional_windows)
        .any(|(window, runtime)| runtime.requests_used >= window.requests_per_window());
    Ok(primary_exhausted || additional_exhausted)
}

fn admit_budget_window(
    window: ProviderBudgetWindow,
    requests_used: &mut u32,
    sliding_releases: &mut VecDeque<MonotonicInstant>,
    sliding_deadline: Option<MonotonicInstant>,
) {
    *requests_used += 1;
    if window.semantics() == BudgetWindowSemantics::Sliding
        && let Some(deadline) = sliding_deadline
    {
        sliding_releases.push_back(deadline);
    }
}

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
        let state = BudgetState::new(&policy, starts_at);
        Self {
            allocation: Arc::new(BudgetAllocation {
                policy,
                state: Mutex::new(state),
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
        let state = BudgetState::new(&policy, starts_at);
        Self {
            allocation: Arc::new(BudgetAllocation {
                policy,
                state: Mutex::new(state),
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
        let state = runtime_state_from_checkpoint(&policy, checkpoint, observation)?;
        Ok(Self {
            allocation: Arc::new(BudgetAllocation {
                policy,
                state: Mutex::new(state),
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

    /// Observes the remaining duration for a deadline returned by this budget.
    ///
    /// The duration is zero at and after the inclusive deadline. The supervisor owns
    /// cancellation-aware sleeping; this method only converts the budget's private monotonic epoch
    /// without exposing its clock or duplicating provider backoff policy.
    ///
    /// # Errors
    ///
    /// Fails closed if the budget is terminal or disabled, its clock or state is unavailable, its
    /// monotonic clock regressed below the current window origin, or its counters are corrupt.
    pub fn remaining_wait(
        &self,
        deadline: MonotonicInstant,
    ) -> Result<std::time::Duration, BudgetUnavailableReason> {
        let operation = self.admit_runtime_operation()?;
        if self.allocation.terminal.load(Ordering::Acquire) {
            return Err(BudgetUnavailableReason::AvailabilityGenerationExhausted);
        }
        let observation = match self.allocation.clock.observation() {
            Ok(observation) => observation,
            Err(_) => {
                return self.terminal_fail(
                    BudgetUnavailableReason::ClockUnavailable,
                    &operation,
                );
            }
        };
        let state = match self.allocation.state.lock() {
            Ok(state) => state,
            Err(_) => {
                return self.terminal_fail(BudgetUnavailableReason::StatePoisoned, &operation);
            }
        };
        if let Err(reason) = validate_budget_windows(self.policy(), &state, observation.monotonic) {
            drop(state);
            return self.terminal_fail(reason, &operation);
        }
        if state.in_flight > self.policy().max_concurrent() {
            drop(state);
            return self.terminal_fail(BudgetUnavailableReason::StateCorrupt, &operation);
        }
        if state.disabled {
            return Err(BudgetUnavailableReason::Disabled);
        }
        let remaining_nanos = deadline
            .as_nanos()
            .saturating_sub(observation.monotonic.as_nanos());
        Ok(std::time::Duration::from_nanos(remaining_nanos))
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
        let availability = match evaluate_budget_windows(self.policy(), &mut state, now) {
            Ok(availability) => availability,
            Err(reason) => return self.terminal_unavailable(reason, &operation),
        };
        if let Some(blocker) = availability.blocker {
            return self.wait_until_locked(
                &state,
                observation,
                blocker,
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
        let Some(in_flight) = state.in_flight.checked_add(1) else {
            return self.terminal_unavailable(
                BudgetUnavailableReason::StateCorrupt,
                &operation,
            );
        };
        let windows_exhausted = match admit_budget_windows(self.policy(), &mut state, availability)
        {
            Ok(exhausted) => exhausted,
            Err(reason) => return self.terminal_unavailable(reason, &operation),
        };
        state.in_flight = in_flight;
        let became_unavailable = windows_exhausted || in_flight >= self.policy().max_concurrent();
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
