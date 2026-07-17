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

#[derive(Clone)]
struct RegisteredBudget {
    persisted: PersistedProviderBudgetPolicy,
    budget: SharedProviderBudget,
}

/// Sole composition-owned mint for conservatively colliding network/authorization authority.
pub(crate) struct ProviderBudgetPool {
    budgets: Vec<RegisteredBudget>,
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
            budgets: Vec::new(),
        })
    }

    /// Registers a policy or returns the existing handle when the exact policy already exists.
    ///
    /// # Errors
    ///
    /// Rejects a conflicting policy for an already registered scope.
    pub(crate) fn register(
        &mut self,
        resolved: ResolvedProviderBudgetPolicy,
    ) -> Result<SharedProviderBudget, BudgetPoolError> {
        if let Some(existing) = self
            .budgets
            .iter()
            .find(|registered| registered.persisted == *resolved.persisted())
        {
            return Ok(existing.budget.clone());
        }
        if self.budgets.len() == MAX_PROCESS_BUDGET_SCOPES {
            return Err(BudgetPoolError::CoordinatorCapacity);
        }
        self.budgets
            .try_reserve(1)
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        let mut coordinated = coordinate_budget_policies(std::slice::from_ref(&resolved))?;
        let budget = coordinated
            .pop()
            .ok_or(BudgetPoolError::CoordinatorCorrupt)?;
        self.budgets.push(RegisteredBudget {
            persisted: resolved.persisted().clone(),
            budget: budget.clone(),
        });
        Ok(budget)
    }

    pub(crate) fn register_all(
        &mut self,
        policies: &[ResolvedProviderBudgetPolicy],
    ) -> Result<(), BudgetPoolError> {
        let additional = policies
            .iter()
            .enumerate()
            .filter(|(index, candidate)| {
                !self
                    .budgets
                    .iter()
                    .any(|registered| registered.persisted == *candidate.persisted())
                    && !policies[..*index]
                        .iter()
                        .any(|earlier| earlier.persisted() == candidate.persisted())
            })
            .count();
        if self
            .budgets
            .len()
            .checked_add(additional)
            .is_none_or(|count| count > MAX_PROCESS_BUDGET_SCOPES)
        {
            return Err(BudgetPoolError::CoordinatorCapacity);
        }
        self.budgets
            .try_reserve(additional)
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        let coordinated = coordinate_budget_policies(policies)?;
        if coordinated.len() != policies.len() {
            return Err(BudgetPoolError::CoordinatorCorrupt);
        }
        for (resolved, budget) in policies.iter().zip(coordinated) {
            if self
                .budgets
                .iter()
                .any(|registered| registered.persisted == *resolved.persisted())
            {
                continue;
            }
            self.budgets.push(RegisteredBudget {
                persisted: resolved.persisted().clone(),
                budget,
            });
        }
        Ok(())
    }

    pub(crate) fn policies(&self) -> Vec<PersistedProviderBudgetPolicy> {
        self.budgets
            .iter()
            .map(|registered| registered.persisted.clone())
            .collect()
    }
}

#[derive(Clone)]
struct CoordinatedBudgetAllocation {
    collision_key: BudgetCollisionKey,
    allocation: Arc<BudgetAllocation>,
}

struct ProcessBudgetCoordinator {
    allocations: Vec<CoordinatedBudgetAllocation>,
    capacity: usize,
}

impl ProcessBudgetCoordinator {
    fn new(capacity: usize) -> Self {
        Self {
            allocations: Vec::new(),
            capacity,
        }
    }

    fn coordinate(
        &mut self,
        policies: &[ResolvedProviderBudgetPolicy],
    ) -> Result<Vec<SharedProviderBudget>, BudgetPoolError> {
        let remaining_capacity = self
            .capacity
            .checked_sub(self.allocations.len())
            .ok_or(BudgetPoolError::CoordinatorCorrupt)?;
        let mut working = Vec::new();
        working
            .try_reserve(self.allocations.len().saturating_add(remaining_capacity))
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        working.extend(self.allocations.iter().cloned());
        let mut result = Vec::new();
        result
            .try_reserve(policies.len())
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        for resolved in policies {
            let mut matching_index = None;
            for (index, allocation) in working.iter().enumerate() {
                if !allocation
                    .collision_key
                    .collides_with(resolved.collision_key())
                {
                    continue;
                }
                if matching_index.replace(index).is_some() {
                    return Err(BudgetPoolError::BridgingIdentity);
                }
            }
            if let Some(index) = matching_index {
                let existing = working
                    .get_mut(index)
                    .ok_or(BudgetPoolError::CoordinatorCorrupt)?;
                if !existing.allocation.policy.has_same_limits_as(resolved.policy()) {
                    return Err(BudgetPoolError::ConflictingPolicy);
                }
                existing
                    .collision_key
                    .merge_public_authorities(resolved.collision_key())
                    .map_err(|error| match error {
                        BudgetCollisionMergeError::Capacity => {
                            BudgetPoolError::CanonicalAuthorityCapacity
                        }
                        BudgetCollisionMergeError::Allocation => {
                            BudgetPoolError::CanonicalAuthorityAllocation
                        }
                    })?;
                result.push(SharedProviderBudget {
                    allocation: Arc::clone(&existing.allocation),
                });
                continue;
            }
            let clock: Arc<dyn BudgetClock> = Arc::new(SystemBudgetClock::new());
            let starts_at = clock
                .observation()
                .map_err(|_| BudgetPoolError::ClockUnavailable)?
                .monotonic;
            let budget = SharedProviderBudget::new(resolved.policy().clone(), starts_at, clock);
            working.push(CoordinatedBudgetAllocation {
                collision_key: resolved.collision_key().clone(),
                allocation: Arc::clone(&budget.allocation),
            });
            result.push(budget);
        }
        if working.len() > self.capacity {
            return Err(BudgetPoolError::CoordinatorCapacity);
        }
        self.allocations = working;
        Ok(result)
    }
}

static BUDGET_COORDINATOR: OnceLock<Mutex<ProcessBudgetCoordinator>> = OnceLock::new();

fn coordinate_budget_policies(
    policies: &[ResolvedProviderBudgetPolicy],
) -> Result<Vec<SharedProviderBudget>, BudgetPoolError> {
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
    /// One declaration overlapped multiple extant allocations and cannot be merged safely.
    #[error("provider budget identity bridges independent authoritative allocations")]
    BridgingIdentity,
    /// Process monotonic/wall clock observation was unavailable or unrepresentable.
    #[error("provider budget clock is unavailable")]
    ClockUnavailable,
    /// The process-wide coordinator lock was poisoned.
    #[error("provider budget coordinator is poisoned")]
    CoordinatorPoisoned,
    /// The bounded process-lifetime authoritative-scope capacity was exhausted.
    #[error("provider budget coordinator capacity exhausted")]
    CoordinatorCapacity,
    /// Memory for bounded coordinator staging or registry publication could not be reserved.
    #[error("provider budget coordinator allocation failed")]
    CoordinatorAllocation,
    /// The bounded canonical-authority union for one allocation was exhausted.
    #[error("provider budget canonical-authority capacity exhausted")]
    CanonicalAuthorityCapacity,
    /// Memory for a checked canonical-authority union could not be reserved.
    #[error("provider budget canonical-authority allocation failed")]
    CanonicalAuthorityAllocation,
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

    use market_squawk_domain::{
        AuthorizationBasis, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
        ExactPayloadEvidence,
    };

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

    #[derive(Debug)]
    struct NoAccountSubjects;

    impl crate::AuthorizationSubjectResolver for NoAccountSubjects {
        fn resolve_subject_record(
            &self,
            _mode: crate::AuthorizationMode,
            _evidence: EvidenceDigest,
        ) -> Result<SourceIdentifier, crate::AuthorizationSubjectResolutionError> {
            Err(crate::AuthorizationSubjectResolutionError::UnsupportedMode)
        }
    }

    fn resolved_policy(
        scope: &str,
        requests_per_window: u32,
    ) -> TestResult<ResolvedProviderBudgetPolicy> {
        let policy = test_policy(scope, requests_per_window)?;
        let authorization = crate::AuthorizationGrant::new(
            crate::AuthorizationMode::PublicInterface,
            AuthorizationBasis::new(SourceIdentifier::try_from("public-interface-terms")?),
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                [1; 32],
            )),
            EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        );
        Ok(ResolvedProviderBudgetPolicy::try_new(
            policy,
            EndpointPolicy::try_new([format!("https://{scope}.example.test/path")])?,
            authorization,
            &NoAccountSubjects,
        )?)
    }

    fn register_fresh(policy: ResolvedProviderBudgetPolicy) -> TestResult<SharedProviderBudget> {
        let mut pool = ProviderBudgetPool::new()?;
        Ok(pool.register(policy)?)
    }

    #[test]
    fn account_qualified_policy_has_an_exact_shared_allocation_charge() -> TestResult {
        fn capacity_identifier(character: char) -> TestResult<SourceIdentifier> {
            let mut value = String::with_capacity(SourceIdentifier::MAX_LENGTH);
            value.push(character);
            Ok(SourceIdentifier::try_from(value)?)
        }

        let provider = capacity_identifier('p')?;
        let account = capacity_identifier('a')?;
        let expected_dynamic = provider
            .retained_bytes()
            .checked_add(account.retained_bytes())
            .ok_or("budget policy dynamic charge overflow")?;
        let policy = ProviderBudgetPolicy::try_new(
            BudgetScope::with_authorization_account(provider, account),
            NonZeroU32::new(1).ok_or("request limit must be nonzero")?,
            NonZeroU64::new(60_000_000_000).ok_or("window must be nonzero")?,
            NonZeroU16::new(1).ok_or("concurrency must be nonzero")?,
            BackoffPolicy::try_new(
                NonZeroU64::new(1_000_000).ok_or("backoff must be nonzero")?,
                NonZeroU64::new(60_000_000_000).ok_or("backoff cap must be nonzero")?,
                0,
            )?,
        )?;
        let clock = Arc::new(SystemBudgetClock::new());
        let starts_at = clock
            .observation()
            .map_err(|reason| std::io::Error::other(format!("clock unavailable: {reason:?}")))?
            .monotonic;
        let budget = SharedProviderBudget::new(policy, starts_at, clock.clone());
        let lease = budget.availability_lease().map_err(|reason| {
            std::io::Error::other(format!("budget lease unavailable: {reason:?}"))
        })?;
        let expected = std::mem::size_of::<BudgetAllocation>()
            .checked_add(crate::conservative_arc_control_block_charge::<
                BudgetAllocation,
            >())
            .and_then(|bytes| bytes.checked_add(expected_dynamic))
            .and_then(|bytes| bytes.checked_add(clock.shared_allocation_charge()))
            .ok_or("shared budget allocation charge overflow")?;

        assert_eq!(lease.shared_allocation_charge(), Some(expected));
        Ok(())
    }

    #[test]
    fn dropping_every_external_handle_cannot_reset_request_state() -> TestResult {
        let policy = resolved_policy("drop-reset-request-state", 1)?;
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
        let refusal_policy = resolved_policy("drop-reset-refusal-state", 2)?;
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

        let disabled_policy = resolved_policy("drop-reset-disabled-state", 2)?;
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

        let terminal_policy = resolved_policy("drop-reset-terminal-state", 2)?;
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
        let first_policy = resolved_policy("bounded-coordinator-first", 1)?;
        let second_policy = resolved_policy("bounded-coordinator-second", 1)?;
        let mut coordinator = ProcessBudgetCoordinator::new(1);
        let first = coordinator.coordinate(std::slice::from_ref(&first_policy))?;
        let first_budget = first
            .first()
            .ok_or("first coordinated budget missing")?;
        let permit = match first_budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("unexpected bounded acquire: {other:?}").into()),
        };
        permit.release();
        drop(first);
        let retained = Arc::clone(
            &coordinator
                .allocations
                .first()
                .ok_or("retained allocation missing")?
                .allocation,
        );

        assert!(matches!(
            coordinator.coordinate(std::slice::from_ref(&second_policy)),
            Err(BudgetPoolError::CoordinatorCapacity)
        ));
        assert_eq!(coordinator.allocations.len(), 1);
        assert!(Arc::ptr_eq(
            &coordinator
                .allocations
                .first()
                .ok_or("first allocation removed after capacity failure")?
                .allocation,
            &retained,
        ));

        let conflicting = resolved_policy("bounded-coordinator-first", 2)?;
        assert!(matches!(
            coordinator.coordinate(std::slice::from_ref(&conflicting)),
            Err(BudgetPoolError::ConflictingPolicy)
        ));
        assert_eq!(coordinator.allocations.len(), 1);
        let restored = coordinator.coordinate(std::slice::from_ref(&first_policy))?;
        assert!(matches!(
            restored
                .first()
                .ok_or("restored retained allocation missing")?
                .try_acquire(),
            BudgetDecision::WaitUntil(_)
        ));
        Ok(())
    }

    #[test]
    fn canonical_authority_union_accepts_exact_bound_and_rejects_one_over_atomically()
    -> TestResult {
        fn authority(host: &str) -> TestResult<CanonicalNetworkAuthority> {
            Ok(CanonicalNetworkAuthority {
                host: SourceIdentifier::try_from(host)?,
                port: 443,
            })
        }

        let mut exact = BudgetCollisionKey::Public(vec![authority("bound-a.example.test")?]);
        let additional = BudgetCollisionKey::Public(vec![authority("bound-b.example.test")?]);
        exact.merge_public_authorities_with_limit(&additional, 2)?;
        assert_eq!(
            exact,
            BudgetCollisionKey::Public(vec![
                authority("bound-a.example.test")?,
                authority("bound-b.example.test")?,
            ])
        );

        let before = exact.clone();
        let one_over = BudgetCollisionKey::Public(vec![authority("bound-c.example.test")?]);
        assert_eq!(
            exact.merge_public_authorities_with_limit(&one_over, 2),
            Err(BudgetCollisionMergeError::Capacity)
        );
        assert_eq!(exact, before);
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
