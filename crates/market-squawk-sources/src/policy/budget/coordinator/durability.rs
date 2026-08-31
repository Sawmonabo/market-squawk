use super::*;

#[path = "durability/restore.rs"]
mod restore;

use restore::combine_durable_group;

#[derive(Clone)]
struct RegisteredBudget {
    persisted: PersistedProviderBudgetPolicy,
    budget: SharedProviderBudget,
}

/// Non-cloneable proof that every unique runtime allocation reconciled with one session.
pub(crate) struct CleanShutdownProof {
    session: Arc<AuthorityDurabilitySession>,
}

impl std::fmt::Debug for CleanShutdownProof {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CleanShutdownProof")
            .finish_non_exhaustive()
    }
}

impl CleanShutdownProof {
    pub(crate) fn belongs_to(&self, session: &AuthorityDurabilitySession) -> bool {
        std::ptr::eq(Arc::as_ptr(&self.session), session)
    }

    pub(crate) fn invalidate_bound_session(&self) {
        self.session.invalidate();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanShutdownValidationError {
    StateUnavailable,
    TerminalAllocation,
    DurabilityMismatch,
    CheckpointMismatch,
    ActiveRequest,
    SlotCollision,
    DeclarationMismatch,
    OrphanedGroup,
}

/// Sole composition-owned mint for conservatively colliding network/authorization authority.
pub(crate) struct ProviderBudgetPool {
    budgets: Vec<RegisteredBudget>,
    durability: Option<Arc<AuthorityDurabilitySession>>,
    provider_rate: Option<ProviderRateAuthority>,
    local_coordinator: Option<ProcessBudgetCoordinator>,
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
    pub(crate) fn has_active_requests(&self) -> Result<bool, CleanShutdownValidationError> {
        for (index, registered) in self.budgets.iter().enumerate() {
            if self.budgets[..index].iter().any(|earlier| {
                Arc::ptr_eq(&earlier.budget.allocation, &registered.budget.allocation)
            }) {
                continue;
            }
            let state = registered
                .budget
                .allocation
                .state
                .lock()
                .map_err(|_| CleanShutdownValidationError::StateUnavailable)?;
            if state.in_flight != 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn new() -> Result<Self, BudgetPoolError> {
        Ok(Self {
            budgets: Vec::new(),
            durability: None,
            provider_rate: None,
            local_coordinator: None,
        })
    }

    pub(crate) fn new_in_memory_with_provider_rate(provider_rate: ProviderRateAuthority) -> Self {
        Self {
            budgets: Vec::new(),
            durability: None,
            provider_rate: Some(provider_rate),
            local_coordinator: Some(ProcessBudgetCoordinator::new(MAX_PROCESS_BUDGET_SCOPES)),
        }
    }

    pub(crate) fn new_durable(session: Arc<AuthorityDurabilitySession>) -> Self {
        Self {
            budgets: Vec::new(),
            durability: Some(session),
            provider_rate: None,
            local_coordinator: None,
        }
    }

    pub(crate) fn new_durable_with_provider_rate(
        session: Arc<AuthorityDurabilitySession>,
        provider_rate: ProviderRateAuthority,
    ) -> Self {
        Self {
            budgets: Vec::new(),
            durability: Some(session),
            provider_rate: Some(provider_rate),
            local_coordinator: Some(ProcessBudgetCoordinator::new(MAX_PROCESS_BUDGET_SCOPES)),
        }
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
        let mut coordinated = self.coordinate(std::slice::from_ref(&resolved), None)?;
        let budget = coordinated
            .pop()
            .ok_or(BudgetPoolError::CoordinatorCorrupt)?;
        self.budgets.push(RegisteredBudget {
            persisted: resolved.persisted().clone(),
            budget: budget.clone(),
        });
        Ok(budget)
    }

    pub(crate) fn register_durable(
        &mut self,
        resolved: ResolvedProviderBudgetPolicy,
        registry: &crate::RegistryAuthorityState,
    ) -> Result<SharedProviderBudget, BudgetPoolError> {
        let session = self
            .durability
            .as_ref()
            .ok_or(BudgetPoolError::ConflictingDurability)?;
        if let Some(existing) = self
            .budgets
            .iter()
            .find(|registered| registered.persisted == *resolved.persisted())
        {
            let observation = existing
                .budget
                .allocation
                .clock
                .observation()
                .map_err(|_| BudgetPoolError::ClockUnavailable)?;
            session
                .persist_registry(registry.clone(), observation.wall_clock)
                .map_err(|_| BudgetPoolError::Persistence)?;
            return Ok(existing.budget.clone());
        }
        if self.budgets.len() == MAX_PROCESS_BUDGET_SCOPES {
            return Err(BudgetPoolError::CoordinatorCapacity);
        }
        self.budgets
            .try_reserve(1)
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        let budget = if let Some(coordinator) = &mut self.local_coordinator {
            let mut coordinated = coordinator.coordinate_with_provider_rate(
                std::slice::from_ref(&resolved),
                Some(DurableRegistration { session, registry }),
                self.provider_rate.as_ref(),
            )?;
            coordinated
                .pop()
                .ok_or(BudgetPoolError::CoordinatorCorrupt)?
        } else {
            coordinate_durable_budget_policy(&resolved, session, registry)?
        };
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
        let coordinated = self.coordinate(policies, None)?;
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

    pub(crate) fn restore_durable(
        &mut self,
        groups: Vec<(Vec<ResolvedProviderBudgetPolicy>, BudgetCheckpointState)>,
    ) -> Result<(), BudgetPoolError> {
        let session = Arc::clone(
            self.durability
                .as_ref()
                .ok_or(BudgetPoolError::ConflictingDurability)?,
        );
        let declaration_count = groups
            .iter()
            .try_fold(0_usize, |count, (declarations, _checkpoint)| {
                count.checked_add(declarations.len())
            })
            .ok_or(BudgetPoolError::CoordinatorCapacity)?;
        if declaration_count > MAX_PROCESS_BUDGET_SCOPES {
            return Err(BudgetPoolError::CoordinatorCapacity);
        }
        self.budgets
            .try_reserve(declaration_count)
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        let mut staged = Vec::new();
        staged
            .try_reserve(groups.len())
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        for (declarations, checkpoint) in groups {
            let combined = combine_durable_group(&declarations)?;
            staged.push((declarations, combined, checkpoint));
        }
        let mut groups_to_coordinate = Vec::new();
        groups_to_coordinate
            .try_reserve(staged.len())
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        groups_to_coordinate.extend(
            staged.iter().map(|(_declarations, combined, checkpoint)| {
                (combined.clone(), checkpoint.clone())
            }),
        );
        let coordinated = if let Some(coordinator) = &mut self.local_coordinator {
            coordinator.coordinate_restored_with_provider_rate(
                &groups_to_coordinate,
                &session,
                self.provider_rate.as_ref(),
            )?
        } else {
            coordinate_restored_budget_groups(&groups_to_coordinate, &session)?
        };
        if coordinated.len() != staged.len() {
            return Err(BudgetPoolError::CoordinatorCorrupt);
        }
        for ((declarations, _combined, _checkpoint), budget) in staged.into_iter().zip(coordinated)
        {
            for declaration in declarations {
                self.budgets.push(RegisteredBudget {
                    persisted: declaration.persisted().clone(),
                    budget: budget.clone(),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn policies(&self) -> Vec<PersistedProviderBudgetPolicy> {
        self.budgets
            .iter()
            .map(|registered| registered.persisted.clone())
            .collect()
    }

    fn coordinate(
        &mut self,
        policies: &[ResolvedProviderBudgetPolicy],
        durable: Option<DurableRegistration<'_>>,
    ) -> Result<Vec<SharedProviderBudget>, BudgetPoolError> {
        match &mut self.local_coordinator {
            Some(coordinator) => coordinator.coordinate_with_provider_rate(
                policies,
                durable,
                self.provider_rate.as_ref(),
            ),
            None => coordinate_budget_policies(policies),
        }
    }

    pub(crate) fn policies_with(
        &self,
        declaration: &PersistedProviderBudgetPolicy,
    ) -> Vec<PersistedProviderBudgetPolicy> {
        let mut policies = self.policies();
        if !policies.contains(declaration) {
            policies.push(declaration.clone());
        }
        policies
    }

    pub(crate) fn validate_clean_shutdown(
        &self,
        session: &Arc<AuthorityDurabilitySession>,
    ) -> Result<CleanShutdownProof, CleanShutdownValidationError> {
        let Ok(groups) = session.budget_groups() else {
            return Err(CleanShutdownValidationError::StateUnavailable);
        };
        let mut bound_slots = [false; MAX_PROCESS_BUDGET_SCOPES];
        let mut unique_allocations = 0_usize;
        for (index, registered) in self.budgets.iter().enumerate() {
            if self.budgets[..index].iter().any(|earlier| {
                Arc::ptr_eq(&earlier.budget.allocation, &registered.budget.allocation)
            }) {
                continue;
            }
            unique_allocations = unique_allocations
                .checked_add(1)
                .ok_or(CleanShutdownValidationError::StateUnavailable)?;
            let allocation = &registered.budget.allocation;
            if allocation.terminal.load(Ordering::Acquire) {
                session.invalidate();
                return Err(CleanShutdownValidationError::TerminalAllocation);
            }
            if allocation.state.is_poisoned() {
                session.invalidate();
                return Err(CleanShutdownValidationError::StateUnavailable);
            }
            let Some(binding) = &allocation.durability else {
                session.invalidate();
                return Err(CleanShutdownValidationError::DurabilityMismatch);
            };
            if !Arc::ptr_eq(&binding.session, session) {
                session.invalidate();
                return Err(CleanShutdownValidationError::DurabilityMismatch);
            }
            let Some(group) = groups.get(binding.slot) else {
                session.invalidate();
                return Err(CleanShutdownValidationError::CheckpointMismatch);
            };
            let Some(slot_seen) = bound_slots.get_mut(binding.slot) else {
                session.invalidate();
                return Err(CleanShutdownValidationError::CheckpointMismatch);
            };
            if *slot_seen {
                session.invalidate();
                return Err(CleanShutdownValidationError::SlotCollision);
            }
            *slot_seen = true;
            let policy_count = self
                .budgets
                .iter()
                .filter(|candidate| {
                    Arc::ptr_eq(&candidate.budget.allocation, &registered.budget.allocation)
                })
                .count();
            let declarations_match = self
                .budgets
                .iter()
                .filter(|candidate| {
                    Arc::ptr_eq(&candidate.budget.allocation, &registered.budget.allocation)
                })
                .all(|candidate| group.declarations().contains(&candidate.persisted));
            if policy_count != group.declarations().len() || !declarations_match {
                session.invalidate();
                return Err(CleanShutdownValidationError::DeclarationMismatch);
            }
            let Ok(state) = allocation.state.lock() else {
                session.invalidate();
                return Err(CleanShutdownValidationError::StateUnavailable);
            };
            if state.in_flight != 0 {
                session.invalidate();
                return Err(CleanShutdownValidationError::ActiveRequest);
            }
            if group.checkpoint().in_flight() != state.in_flight {
                session.invalidate();
                return Err(CleanShutdownValidationError::CheckpointMismatch);
            }
        }
        if unique_allocations != groups.len() {
            session.invalidate();
            return Err(CleanShutdownValidationError::OrphanedGroup);
        }
        Ok(CleanShutdownProof {
            session: Arc::clone(session),
        })
    }
}

struct DurableRegistration<'a> {
    session: &'a Arc<AuthorityDurabilitySession>,
    registry: &'a crate::RegistryAuthorityState,
}

#[derive(Clone)]
struct CoordinatedBudgetAllocation {
    collision_key: BudgetCollisionKey,
    allocation: Arc<BudgetAllocation>,
}

enum StagedProviderAllocationBacking {
    Existing(Arc<BudgetAllocation>),
    New { policy: ProviderBudgetPolicy },
}

struct StagedProviderAllocation {
    collision_key: BudgetCollisionKey,
    backing: StagedProviderAllocationBacking,
    declarations: Vec<PersistedProviderBudgetPolicy>,
    input_indexes: Vec<usize>,
}

impl StagedProviderAllocation {
    fn policy(&self) -> &ProviderBudgetPolicy {
        match &self.backing {
            StagedProviderAllocationBacking::Existing(allocation) => &allocation.policy,
            StagedProviderAllocationBacking::New { policy } => policy,
        }
    }
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

    fn discard_cleanly_closed_durable_allocations(&mut self) {
        self.allocations.retain(|allocation| {
            allocation
                .allocation
                .durability
                .as_ref()
                .is_none_or(|binding| !binding.session.closed_clean())
        });
    }

    fn coordinate(
        &mut self,
        policies: &[ResolvedProviderBudgetPolicy],
        durable: Option<DurableRegistration<'_>>,
    ) -> Result<Vec<SharedProviderBudget>, BudgetPoolError> {
        self.coordinate_with_provider_rate(policies, durable, None)
    }

    fn coordinate_with_provider_rate(
        &mut self,
        policies: &[ResolvedProviderBudgetPolicy],
        durable: Option<DurableRegistration<'_>>,
        provider_rate: Option<&ProviderRateAuthority>,
    ) -> Result<Vec<SharedProviderBudget>, BudgetPoolError> {
        if let Some(provider_rate) = provider_rate {
            return self.coordinate_atomic_provider_rate(policies, durable, provider_rate);
        }
        self.discard_cleanly_closed_durable_allocations();
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
            let provider_rate_binding = provider_rate
                .map(|authority| {
                    ProviderRateDeclaration::from_resolved(resolved)
                        .and_then(|declaration| authority.register_binding(&declaration))
                })
                .transpose()?;
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
                if !existing
                    .allocation
                    .policy
                    .has_same_limits_as(resolved.policy())
                {
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
                match (&existing.allocation.durability, &durable) {
                    (None, None) => {}
                    (Some(binding), Some(registration))
                        if Arc::ptr_eq(&binding.session, registration.session) =>
                    {
                        let observation = existing
                            .allocation
                            .clock
                            .observation()
                            .map_err(|_| BudgetPoolError::ClockUnavailable)?;
                        registration
                            .session
                            .add_budget_declaration(
                                binding.slot,
                                registration.registry.clone(),
                                resolved.persisted().clone(),
                                observation.wall_clock,
                            )
                            .map_err(|_| BudgetPoolError::Persistence)?;
                    }
                    (None, Some(_)) | (Some(_), None) | (Some(_), Some(_)) => {
                        return Err(BudgetPoolError::ConflictingDurability);
                    }
                }
                match (&existing.allocation.provider_rate, &provider_rate_binding) {
                    (None, None) => {}
                    (Some(existing), Some(candidate)) if existing.same_group(candidate) => {}
                    (None, Some(_)) | (Some(_), None) | (Some(_), Some(_)) => {
                        return Err(BudgetPoolError::ConflictingDurability);
                    }
                }
                result.push(SharedProviderBudget {
                    allocation: Arc::clone(&existing.allocation),
                });
                continue;
            }
            if working.len() == self.capacity {
                return Err(BudgetPoolError::CoordinatorCapacity);
            }
            let clock = provider_rate_binding
                .as_ref()
                .map(ProviderRateBinding::clock)
                .unwrap_or_else(|| Arc::new(SystemBudgetClock::new()));
            let observation = clock
                .observation()
                .map_err(|_| BudgetPoolError::ClockUnavailable)?;
            let budget = if let Some(registration) = &durable {
                let state = BudgetState::new(resolved.policy(), observation.monotonic);
                let checkpoint =
                    checkpoint_from_runtime(resolved.policy(), &state, observation, 1, false)
                        .map_err(|_| BudgetPoolError::Persistence)?;
                let slot = registration
                    .session
                    .register_budget_group(
                        registration.registry.clone(),
                        resolved.persisted().clone(),
                        checkpoint,
                        observation.wall_clock,
                    )
                    .map_err(|_| BudgetPoolError::Persistence)?;
                let durability = BudgetDurabilityBinding {
                    session: Arc::clone(registration.session),
                    slot,
                };
                match provider_rate_binding {
                    Some(provider_rate) => SharedProviderBudget::new_durable_with_provider_rate(
                        resolved.policy().clone(),
                        observation.monotonic,
                        durability,
                        provider_rate,
                    ),
                    None => SharedProviderBudget::new_durable(
                        resolved.policy().clone(),
                        observation.monotonic,
                        clock,
                        durability,
                    ),
                }
            } else {
                match provider_rate_binding {
                    Some(provider_rate) => SharedProviderBudget::new_with_provider_rate(
                        resolved.policy().clone(),
                        provider_rate,
                    )?,
                    None => SharedProviderBudget::new(
                        resolved.policy().clone(),
                        observation.monotonic,
                        clock,
                    ),
                }
            };
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

    fn coordinate_atomic_provider_rate(
        &mut self,
        policies: &[ResolvedProviderBudgetPolicy],
        durable: Option<DurableRegistration<'_>>,
        provider_rate: &ProviderRateAuthority,
    ) -> Result<Vec<SharedProviderBudget>, BudgetPoolError> {
        if policies.is_empty() {
            return Ok(Vec::new());
        }
        self.discard_cleanly_closed_durable_allocations();
        let mut staged = Vec::new();
        staged
            .try_reserve(self.capacity)
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        staged.extend(
            self.allocations
                .iter()
                .map(|allocation| StagedProviderAllocation {
                    collision_key: allocation.collision_key.clone(),
                    backing: StagedProviderAllocationBacking::Existing(Arc::clone(
                        &allocation.allocation,
                    )),
                    declarations: Vec::new(),
                    input_indexes: Vec::new(),
                }),
        );
        let mut input_allocation_indexes = Vec::new();
        input_allocation_indexes
            .try_reserve_exact(policies.len())
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        let mut provider_declarations = Vec::new();
        provider_declarations
            .try_reserve_exact(policies.len())
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;

        for (input_index, resolved) in policies.iter().enumerate() {
            provider_declarations.push(ProviderRateDeclaration::from_resolved(resolved)?);
            let mut matching_index = None;
            for (index, allocation) in staged.iter().enumerate() {
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
            let allocation_index = if let Some(index) = matching_index {
                let existing = staged
                    .get_mut(index)
                    .ok_or(BudgetPoolError::CoordinatorCorrupt)?;
                if !existing.policy().has_same_limits_as(resolved.policy()) {
                    return Err(BudgetPoolError::ConflictingPolicy);
                }
                if let StagedProviderAllocationBacking::Existing(allocation) = &existing.backing {
                    match (&allocation.durability, &durable) {
                        (None, None) => {}
                        (Some(binding), Some(registration))
                            if Arc::ptr_eq(&binding.session, registration.session) => {}
                        (None, Some(_)) | (Some(_), None) | (Some(_), Some(_)) => {
                            return Err(BudgetPoolError::ConflictingDurability);
                        }
                    }
                    if allocation.provider_rate.is_none() {
                        return Err(BudgetPoolError::ConflictingDurability);
                    }
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
                if !existing.declarations.contains(resolved.persisted()) {
                    existing.declarations.push(resolved.persisted().clone());
                }
                existing.input_indexes.push(input_index);
                index
            } else {
                if staged.len() == self.capacity {
                    return Err(BudgetPoolError::CoordinatorCapacity);
                }
                let index = staged.len();
                staged.push(StagedProviderAllocation {
                    collision_key: resolved.collision_key().clone(),
                    backing: StagedProviderAllocationBacking::New {
                        policy: resolved.policy().clone(),
                    },
                    declarations: vec![resolved.persisted().clone()],
                    input_indexes: vec![input_index],
                });
                index
            };
            input_allocation_indexes.push(allocation_index);
        }

        let local_persisted = std::cell::Cell::new(false);
        let coordinated = provider_rate.with_prepared_registration_bindings(
            &provider_declarations,
            |bindings, observation| {
                let mut staged_bindings = Vec::new();
                staged_bindings
                    .try_reserve_exact(staged.len())
                    .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
                staged_bindings.resize_with(staged.len(), || None);
                for (stage_index, allocation) in staged.iter().enumerate() {
                    if allocation.input_indexes.is_empty() {
                        continue;
                    }
                    let first_input = *allocation
                        .input_indexes
                        .first()
                        .ok_or(BudgetPoolError::CoordinatorCorrupt)?;
                    let candidate = bindings
                        .get(first_input)
                        .ok_or(BudgetPoolError::CoordinatorCorrupt)?
                        .clone();
                    if allocation.input_indexes.iter().any(|input| {
                        bindings
                            .get(*input)
                            .is_none_or(|binding| !candidate.same_group(binding))
                    }) {
                        return Err(BudgetPoolError::ConflictingDurability);
                    }
                    if let StagedProviderAllocationBacking::Existing(existing) = &allocation.backing
                    {
                        let retained = existing
                            .provider_rate
                            .as_ref()
                            .ok_or(BudgetPoolError::ConflictingDurability)?;
                        if !candidate.same_group(retained) {
                            return Err(BudgetPoolError::ConflictingDurability);
                        }
                    }
                    staged_bindings[stage_index] = Some(candidate);
                }
                if staged.iter().enumerate().any(|(stage_index, allocation)| {
                    matches!(
                        &allocation.backing,
                        StagedProviderAllocationBacking::New { .. }
                    ) && staged_bindings[stage_index].is_none()
                }) {
                    return Err(BudgetPoolError::CoordinatorCorrupt);
                }

                let mut working = Vec::new();
                working
                    .try_reserve_exact(staged.len())
                    .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
                let mut result = Vec::new();
                result
                    .try_reserve_exact(input_allocation_indexes.len())
                    .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;

                let mut durable_groups = Vec::new();
                let mut durable_stage_indexes = Vec::new();
                let mut new_slots = vec![None; staged.len()];
                if let Some(registration) = &durable {
                    durable_groups
                        .try_reserve_exact(
                            staged
                                .iter()
                                .filter(|allocation| !allocation.input_indexes.is_empty())
                                .count(),
                        )
                        .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
                    durable_stage_indexes
                        .try_reserve_exact(durable_groups.capacity())
                        .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
                    for (stage_index, allocation) in staged.iter().enumerate() {
                        if allocation.input_indexes.is_empty() {
                            continue;
                        }
                        let target = match &allocation.backing {
                            StagedProviderAllocationBacking::Existing(existing) => {
                                let binding = existing
                                    .durability
                                    .as_ref()
                                    .ok_or(BudgetPoolError::ConflictingDurability)?;
                                DurableBudgetRegistrationTarget::Existing { slot: binding.slot }
                            }
                            StagedProviderAllocationBacking::New { policy } => {
                                let state = BudgetState::new(policy, observation.monotonic);
                                let checkpoint =
                                    checkpoint_from_runtime(policy, &state, observation, 1, false)
                                        .map_err(|_| BudgetPoolError::Persistence)?;
                                DurableBudgetRegistrationTarget::New { checkpoint }
                            }
                        };
                        durable_stage_indexes.push(stage_index);
                        durable_groups.push(DurableBudgetRegistrationGroup {
                            target,
                            declarations: allocation.declarations.clone(),
                        });
                    }
                    if durable_groups.is_empty() {
                        return Err(BudgetPoolError::CoordinatorCorrupt);
                    }
                    let slots = registration
                        .session
                        .register_budget_batch(
                            registration.registry.clone(),
                            &durable_groups,
                            observation.wall_clock,
                        )
                        .map_err(|_| BudgetPoolError::Persistence)?;
                    local_persisted.set(true);
                    if slots.len() != durable_stage_indexes.len() {
                        registration.session.invalidate();
                        return Err(BudgetPoolError::CoordinatorCorrupt);
                    }
                    for (stage_index, slot) in durable_stage_indexes.iter().zip(slots.iter()) {
                        new_slots[*stage_index] = Some(*slot);
                    }
                }
                for (stage_index, allocation) in staged.iter().enumerate() {
                    let runtime = match &allocation.backing {
                        StagedProviderAllocationBacking::Existing(existing) => Arc::clone(existing),
                        StagedProviderAllocationBacking::New { policy } => {
                            let binding = staged_bindings[stage_index]
                                .as_ref()
                                .ok_or(BudgetPoolError::CoordinatorCorrupt)?
                                .clone();
                            if let Some(registration) = &durable {
                                let slot = new_slots[stage_index]
                                    .ok_or(BudgetPoolError::CoordinatorCorrupt)?;
                                SharedProviderBudget::new_durable_with_provider_rate(
                                    policy.clone(),
                                    observation.monotonic,
                                    BudgetDurabilityBinding {
                                        session: Arc::clone(registration.session),
                                        slot,
                                    },
                                    binding,
                                )
                                .allocation
                            } else {
                                SharedProviderBudget::new_with_provider_rate_at(
                                    policy.clone(),
                                    observation.monotonic,
                                    binding,
                                )
                                .allocation
                            }
                        }
                    };
                    working.push(CoordinatedBudgetAllocation {
                        collision_key: allocation.collision_key.clone(),
                        allocation: runtime,
                    });
                }
                for allocation_index in &input_allocation_indexes {
                    result.push(SharedProviderBudget {
                        allocation: Arc::clone(
                            &working
                                .get(*allocation_index)
                                .ok_or(BudgetPoolError::CoordinatorCorrupt)?
                                .allocation,
                        ),
                    });
                }
                Ok((working, result))
            },
        );
        match coordinated {
            Ok((working, result)) => {
                self.allocations = working;
                Ok(result)
            }
            Err(error) => {
                if local_persisted.get() {
                    if let Some(registration) = durable {
                        registration.session.invalidate();
                    }
                }
                Err(error)
            }
        }
    }

    fn coordinate_restored(
        &mut self,
        groups: &[(ResolvedProviderBudgetPolicy, BudgetCheckpointState)],
        session: &Arc<AuthorityDurabilitySession>,
    ) -> Result<Vec<SharedProviderBudget>, BudgetPoolError> {
        self.coordinate_restored_with_provider_rate(groups, session, None)
    }

    fn coordinate_restored_with_provider_rate(
        &mut self,
        groups: &[(ResolvedProviderBudgetPolicy, BudgetCheckpointState)],
        session: &Arc<AuthorityDurabilitySession>,
        provider_rate: Option<&ProviderRateAuthority>,
    ) -> Result<Vec<SharedProviderBudget>, BudgetPoolError> {
        if let Some(provider_rate) = provider_rate {
            return self.coordinate_restored_atomic_provider_rate(groups, session, provider_rate);
        }
        self.discard_cleanly_closed_durable_allocations();
        let total = self
            .allocations
            .len()
            .checked_add(groups.len())
            .ok_or(BudgetPoolError::CoordinatorCapacity)?;
        if total > self.capacity {
            return Err(BudgetPoolError::CoordinatorCapacity);
        }
        let mut working = Vec::new();
        working
            .try_reserve(total)
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        working.extend(self.allocations.iter().cloned());
        let mut restored = Vec::new();
        restored
            .try_reserve(groups.len())
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        for (slot, (resolved, checkpoint)) in groups.iter().enumerate() {
            if working.iter().any(|allocation| {
                allocation
                    .collision_key
                    .collides_with(resolved.collision_key())
            }) {
                return Err(BudgetPoolError::ConflictingDurability);
            }
            let durability = BudgetDurabilityBinding {
                session: Arc::clone(session),
                slot,
            };
            let provider_rate_binding = provider_rate
                .map(|authority| {
                    ProviderRateDeclaration::from_resolved(resolved)
                        .and_then(|declaration| authority.register_binding(&declaration))
                })
                .transpose()?;
            let budget = match provider_rate_binding {
                Some(provider_rate) => SharedProviderBudget::from_checkpoint_with_provider_rate(
                    resolved.policy().clone(),
                    checkpoint,
                    durability,
                    provider_rate,
                ),
                None => {
                    let clock: Arc<dyn BudgetClock> = Arc::new(SystemBudgetClock::new());
                    SharedProviderBudget::from_checkpoint(
                        resolved.policy().clone(),
                        checkpoint,
                        clock,
                        durability,
                    )
                }
            }
            .map_err(|_| BudgetPoolError::Persistence)?;
            working.push(CoordinatedBudgetAllocation {
                collision_key: resolved.collision_key().clone(),
                allocation: Arc::clone(&budget.allocation),
            });
            restored.push(budget);
        }
        self.allocations = working;
        Ok(restored)
    }

    fn coordinate_restored_atomic_provider_rate(
        &mut self,
        groups: &[(ResolvedProviderBudgetPolicy, BudgetCheckpointState)],
        session: &Arc<AuthorityDurabilitySession>,
        provider_rate: &ProviderRateAuthority,
    ) -> Result<Vec<SharedProviderBudget>, BudgetPoolError> {
        self.discard_cleanly_closed_durable_allocations();
        let total = self
            .allocations
            .len()
            .checked_add(groups.len())
            .ok_or(BudgetPoolError::CoordinatorCapacity)?;
        if total > self.capacity {
            return Err(BudgetPoolError::CoordinatorCapacity);
        }
        for (resolved, _checkpoint) in groups {
            if self.allocations.iter().any(|allocation| {
                allocation
                    .collision_key
                    .collides_with(resolved.collision_key())
            }) {
                return Err(BudgetPoolError::ConflictingDurability);
            }
        }
        for (index, (resolved, _checkpoint)) in groups.iter().enumerate() {
            if groups[..index].iter().any(|(earlier, _checkpoint)| {
                earlier
                    .collision_key()
                    .collides_with(resolved.collision_key())
            }) {
                return Err(BudgetPoolError::ConflictingDurability);
            }
        }
        let mut declarations = Vec::new();
        declarations
            .try_reserve_exact(groups.len())
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        for (resolved, _checkpoint) in groups {
            declarations.push(ProviderRateDeclaration::from_resolved(resolved)?);
        }
        let (working, restored) = provider_rate.with_prepared_registration_bindings(
            &declarations,
            |bindings, _observation| {
                let mut working = Vec::new();
                working
                    .try_reserve_exact(total)
                    .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
                working.extend(self.allocations.iter().cloned());
                let mut restored = Vec::new();
                restored
                    .try_reserve_exact(groups.len())
                    .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
                for (slot, ((resolved, checkpoint), provider_rate)) in
                    groups.iter().zip(bindings).enumerate()
                {
                    let durability = BudgetDurabilityBinding {
                        session: Arc::clone(session),
                        slot,
                    };
                    let budget = SharedProviderBudget::from_checkpoint_with_provider_rate(
                        resolved.policy().clone(),
                        checkpoint,
                        durability,
                        provider_rate.clone(),
                    )
                    .map_err(|_| BudgetPoolError::Persistence)?;
                    working.push(CoordinatedBudgetAllocation {
                        collision_key: resolved.collision_key().clone(),
                        allocation: Arc::clone(&budget.allocation),
                    });
                    restored.push(budget);
                }
                Ok((working, restored))
            },
        )?;
        self.allocations = working;
        Ok(restored)
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
    coordinator.coordinate(policies, None)
}

fn coordinate_durable_budget_policy(
    policy: &ResolvedProviderBudgetPolicy,
    session: &Arc<AuthorityDurabilitySession>,
    registry: &crate::RegistryAuthorityState,
) -> Result<SharedProviderBudget, BudgetPoolError> {
    let coordinator = BUDGET_COORDINATOR
        .get_or_init(|| Mutex::new(ProcessBudgetCoordinator::new(MAX_PROCESS_BUDGET_SCOPES)));
    let mut coordinator = coordinator
        .lock()
        .map_err(|_| BudgetPoolError::CoordinatorPoisoned)?;
    let mut coordinated = coordinator.coordinate(
        std::slice::from_ref(policy),
        Some(DurableRegistration { session, registry }),
    )?;
    coordinated.pop().ok_or(BudgetPoolError::CoordinatorCorrupt)
}

fn coordinate_restored_budget_groups(
    groups: &[(ResolvedProviderBudgetPolicy, BudgetCheckpointState)],
    session: &Arc<AuthorityDurabilitySession>,
) -> Result<Vec<SharedProviderBudget>, BudgetPoolError> {
    let coordinator = BUDGET_COORDINATOR
        .get_or_init(|| Mutex::new(ProcessBudgetCoordinator::new(MAX_PROCESS_BUDGET_SCOPES)));
    let mut coordinator = coordinator
        .lock()
        .map_err(|_| BudgetPoolError::CoordinatorPoisoned)?;
    coordinator.coordinate_restored(groups, session)
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
    /// The same canonical allocation was requested with incompatible persistence composition.
    #[error("provider budget allocation has conflicting durability composition")]
    ConflictingDurability,
    /// Required durable provider-budget state could not be published.
    #[error("provider budget durable state publication failed")]
    Persistence,
}

/// Non-serializable proof that one exact provider request permit remains active.
///
/// The lease is tied to the permit allocation, its post-admission availability generation, and
/// the permit's lifetime. It cannot be reconstructed from provider health DTOs and becomes invalid
/// immediately when the permit is released or the shared budget is revoked or terminal.
#[derive(Clone)]
pub struct BudgetPermitLease {
    allocation: Arc<BudgetAllocation>,
    availability_generation: u64,
    active: Arc<AtomicBool>,
}

impl std::fmt::Debug for BudgetPermitLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BudgetPermitLease")
            .field("availability_generation", &self.availability_generation)
            .finish_non_exhaustive()
    }
}

impl BudgetPermitLease {
    pub(crate) fn is_current(&self) -> bool {
        self.active.load(Ordering::Acquire)
            && !self.allocation.terminal.load(Ordering::Acquire)
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
                == self.availability_generation
            && self.active.load(Ordering::Acquire)
            && !self.allocation.state.is_poisoned()
            && !self.allocation.terminal.load(Ordering::Acquire)
    }

    pub(crate) fn shares_allocation_with(&self, budget: &SharedProviderBudget) -> bool {
        Arc::ptr_eq(&self.allocation, &budget.allocation)
    }

    pub(crate) fn shared_allocation_charge(&self) -> Option<usize> {
        let state_dynamic = self
            .allocation
            .state
            .lock()
            .ok()?
            .dynamic_retained_bytes()?;
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
            .and_then(|bytes| bytes.checked_add(state_dynamic))
            .and_then(|bytes| bytes.checked_add(self.allocation.clock.shared_allocation_charge()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<AtomicBool>()))
            .and_then(|bytes| {
                bytes.checked_add(crate::conservative_arc_control_block_charge::<AtomicBool>())
            })
    }
}

/// RAII concurrency reservation for one request that has not reached transport dispatch.
///
/// Dropping or releasing it consumes no provider request-window capacity. Only
/// [`BudgetReservation::commit_dispatch`] or the weighted-response
/// [`BudgetReservation::commit_dispatch_with_response_bound`] transport seam can produce a
/// [`BudgetPermit`] and an active request lease.
pub struct BudgetReservation {
    pub(in crate::policy) allocation: Arc<BudgetAllocation>,
    pub(in crate::policy) runtime_admission: Option<RuntimeOperationAdmission>,
    pub(in crate::policy) provider_rate: Option<ProviderRateReservation>,
    pub(in crate::policy) released: bool,
}

impl std::fmt::Debug for BudgetReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BudgetReservation")
            .field("released", &self.released)
            .finish_non_exhaustive()
    }
}

/// RAII reservation for one in-flight provider request.
pub struct BudgetPermit {
    pub(in crate::policy) allocation: Arc<BudgetAllocation>,
    pub(in crate::policy) runtime_admission: RuntimeOperationAdmission,
    pub(in crate::policy) provider_rate: Option<ProviderRatePermit>,
    pub(in crate::policy) active: Arc<AtomicBool>,
    pub(in crate::policy) released: bool,
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
    /// Borrows the exact active request as an opaque process-local authority lease.
    pub fn active_lease(&self) -> BudgetPermitLease {
        BudgetPermitLease {
            allocation: Arc::clone(&self.allocation),
            availability_generation: self
                .allocation
                .availability_generation
                .load(Ordering::Acquire),
            active: Arc::clone(&self.active),
        }
    }

    /// Atomically terminalizes this exact dispatched provider response and releases concurrency.
    ///
    /// The durable aggregate store consumes the exact permit first. The local allocation then
    /// mirrors the returned refusal/cooldown state and persists its released in-flight slot. A
    /// weighted permit cannot be terminalized through the legacy success/refusal controls.
    ///
    /// # Errors
    ///
    /// Fails closed when this permit is not bound to the product-wide provider-rate authority,
    /// the exact response settlement is rejected, or local state cannot mirror the durable
    /// receipt.
    pub fn settle_response(
        mut self,
        settlement: crate::ProviderRateResponseSettlement,
    ) -> Result<crate::ProviderRateResponseSettlementReceipt, BudgetUnavailableReason> {
        if self.released
            || !self.active.load(Ordering::Acquire)
            || self.allocation.terminal.load(Ordering::Acquire)
        {
            return Err(BudgetUnavailableReason::AvailabilityGenerationExhausted);
        }
        let budget = SharedProviderBudget {
            allocation: Arc::clone(&self.allocation),
        };
        if !budget.policy().has_weighted_windows() {
            return Err(BudgetUnavailableReason::PersistenceUnavailable);
        }
        let provider_rate = self
            .provider_rate
            .as_mut()
            .ok_or(BudgetUnavailableReason::PersistenceUnavailable)?;
        let receipt = provider_rate
            .settle_response(settlement)
            .map_err(|reason| budget.terminal_fault(reason, &self.runtime_admission))?;

        // The aggregate permit has now been consumed. Prevent every later error path and Drop
        // from attempting a second release against the exact durable permit.
        self.active.store(false, Ordering::Release);
        self.released = true;

        let mut state = self.allocation.state.lock().map_err(|_| {
            budget.terminal_fault(
                BudgetUnavailableReason::StatePoisoned,
                &self.runtime_admission,
            )
        })?;
        let observation = self.allocation.clock.observation().map_err(|_| {
            budget.terminal_fault(
                BudgetUnavailableReason::ClockUnavailable,
                &self.runtime_admission,
            )
        })?;
        let in_flight = state.in_flight.checked_sub(1).ok_or_else(|| {
            budget.terminal_fault(
                BudgetUnavailableReason::StateCorrupt,
                &self.runtime_admission,
            )
        })?;
        state.in_flight = in_flight;
        let mirrored_version = self
            .allocation
            .provider_rate_state_version
            .load(Ordering::Acquire);
        if receipt.state_version() > mirrored_version {
            state.consecutive_refusals = receipt.consecutive_refusals();
            match receipt.availability() {
                crate::ProviderRateSettlementAvailability::Available => {
                    if state.disabled {
                        drop(state);
                        return Err(budget.terminal_fault(
                            BudgetUnavailableReason::StateCorrupt,
                            &self.runtime_admission,
                        ));
                    }
                    state.unavailable_until = None;
                }
                crate::ProviderRateSettlementAvailability::WaitUntil(deadline) => {
                    let deadline = wall_deadline_to_monotonic(
                        observation.wall_clock,
                        observation.monotonic,
                        deadline,
                    )
                    .map_err(|reason| budget.terminal_fault(reason, &self.runtime_admission))?;
                    state.unavailable_until = Some(deadline);
                }
                crate::ProviderRateSettlementAvailability::Unavailable(reason)
                    if matches!(
                        reason,
                        BudgetUnavailableReason::Disabled
                            | BudgetUnavailableReason::RetryAfterExceedsPolicy
                    ) =>
                {
                    state.disabled = true;
                    state.unavailable_until = None;
                }
                crate::ProviderRateSettlementAvailability::Unavailable(_) => {
                    drop(state);
                    return Err(budget.terminal_fault(
                        BudgetUnavailableReason::StateCorrupt,
                        &self.runtime_admission,
                    ));
                }
            }
            self.allocation
                .provider_rate_state_version
                .store(receipt.state_version(), Ordering::Release);
        }
        budget.persist_locked(&state, observation, &self.runtime_admission)?;
        Ok(receipt)
    }

    /// Explicitly releases the in-flight slot; request-window consumption remains recorded.
    ///
    /// For a weighted provider-rate permit, the durable store conservatively terminalizes its
    /// pending maximum response claim as unknown completion before releasing concurrency.
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        self.active.store(false, Ordering::Release);
        let budget = SharedProviderBudget {
            allocation: Arc::clone(&self.allocation),
        };
        let admission = &self.runtime_admission;
        if !budget.durability_is_available() {
            let _reason =
                budget.terminal_fault(BudgetUnavailableReason::PersistenceUnavailable, admission);
            self.released = true;
            if let Some(permit) = &mut self.provider_rate {
                let _released = permit.release();
            }
            return;
        }
        let Ok(mut state) = self.allocation.state.lock() else {
            let _reason = budget.terminal_fault(BudgetUnavailableReason::StatePoisoned, admission);
            self.released = true;
            if let Some(permit) = &mut self.provider_rate {
                let _released = permit.release();
            }
            return;
        };
        let Ok(observation) = self.allocation.clock.observation() else {
            let _reason =
                budget.terminal_fault(BudgetUnavailableReason::ClockUnavailable, admission);
            self.released = true;
            if let Some(permit) = &mut self.provider_rate {
                let _released = permit.release();
            }
            return;
        };
        let Some(in_flight) = state.in_flight.checked_sub(1) else {
            let _reason = budget.terminal_fault(BudgetUnavailableReason::StateCorrupt, admission);
            drop(state);
            self.released = true;
            if let Some(permit) = &mut self.provider_rate {
                let _released = permit.release();
            }
            return;
        };
        state.in_flight = in_flight;
        let _persisted = budget.persist_locked(&state, observation, admission);
        drop(state);
        let provider_release = self
            .provider_rate
            .as_mut()
            .map_or(Ok(()), ProviderRatePermit::release);
        self.released = true;
        if let Err(reason) = provider_release {
            let _reason = budget.terminal_fault(reason, admission);
        }
    }
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        self.release_inner();
    }
}

include!("tests.rs");
