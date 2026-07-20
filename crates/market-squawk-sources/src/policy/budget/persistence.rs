//! Restart-durable provider-budget checkpoints and the narrow opaque store contract.

use super::*;

#[path = "persistence/terminal.rs"]
mod terminal;
#[path = "persistence/lifecycle.rs"]
pub(in crate::policy) mod lifecycle;

pub(crate) const MAX_DURABLE_AUTHORITY_STATE_BYTES: usize = 8 * 1024 * 1024;
const DURABLE_AUTHORITY_FORMAT_VERSION: u16 = 1;

/// Opaque, synchronous durability boundary used only by the source control plane.
///
/// Implementations must make a successful [`Self::store`] durable before returning. Production
/// composition accepts only the path-confined platform store; alternate implementations are
/// crate-private deterministic fault injectors used by this module's tests.
pub(crate) trait AuthorityStateStore: std::fmt::Debug + Send + Sync {
    /// Loads the sole canonical payload without following alternate or recovery files.
    ///
    /// # Errors
    ///
    /// Returns a fail-closed error for unavailable, corrupt, ambiguous, or oversized state.
    fn load(&self) -> Result<Option<Vec<u8>>, AuthorityStateStoreError>;

    /// Atomically and durably replaces the sole canonical payload.
    ///
    /// # Errors
    ///
    /// Returns a fail-closed error unless the replacement and required directory metadata are
    /// durable.
    fn store(&self, payload: &[u8]) -> Result<(), AuthorityStateStoreError>;
}

/// Redacted failure returned by an opaque authority-state store adapter.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum AuthorityStateStoreError {
    /// State could not be read or durably replaced.
    #[error("authority state store is unavailable")]
    Unavailable,
    /// Canonical bytes were corrupt, ambiguous, or outside the store's bounds.
    #[error("authority state store contains invalid canonical state")]
    InvalidState,
}

impl AuthorityStateStore for market_squawk_platform::LocalAuthorityStateStore {
    fn load(&self) -> Result<Option<Vec<u8>>, AuthorityStateStoreError> {
        market_squawk_platform::LocalAuthorityStateStore::load(self)
            .map_err(map_local_store_error)
    }

    fn store(&self, payload: &[u8]) -> Result<(), AuthorityStateStoreError> {
        market_squawk_platform::LocalAuthorityStateStore::store(self, payload)
            .map_err(map_local_store_error)
    }
}

fn map_local_store_error(
    error: market_squawk_platform::LocalAuthorityStateStoreError,
) -> AuthorityStateStoreError {
    use market_squawk_platform::LocalAuthorityStateStoreError;

    match error {
        LocalAuthorityStateStoreError::UnsafeRoot
        | LocalAuthorityStateStoreError::UnsafeFileType
        | LocalAuthorityStateStoreError::PayloadTooLarge { .. }
        | LocalAuthorityStateStoreError::EnvelopeTooLarge { .. }
        | LocalAuthorityStateStoreError::CorruptEnvelope
        | LocalAuthorityStateStoreError::GenerationConflict
        | LocalAuthorityStateStoreError::GenerationExhausted
        | LocalAuthorityStateStoreError::StaleCommitContext => {
            AuthorityStateStoreError::InvalidState
        }
        LocalAuthorityStateStoreError::AlreadyLocked
        | LocalAuthorityStateStoreError::Allocation
        | LocalAuthorityStateStoreError::WriterUnavailable
        | LocalAuthorityStateStoreError::AtomicReplaceUnsupported
        | LocalAuthorityStateStoreError::RecoveryRequired
        | LocalAuthorityStateStoreError::FinalizationPending
        | LocalAuthorityStateStoreError::SecureRootUnsupported
        | LocalAuthorityStateStoreError::VerificationFailed
        | LocalAuthorityStateStoreError::Io { .. } => AuthorityStateStoreError::Unavailable,
    }
}

/// Durable authority-state validation or publication failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum AuthorityPersistenceError {
    /// The opaque store rejected a load or durable replacement.
    #[error("authority state persistence failed")]
    Store,
    /// The canonical source-owned envelope was malformed, noncanonical, or inconsistent.
    #[error("durable authority state is invalid")]
    InvalidState,
    /// The saved wall high-water is ahead of the trusted current wall observation.
    #[error("trusted wall clock rolled back below durable authority state")]
    WallRollback,
    /// The saved checkpoint claims a write after the trusted current wall observation.
    #[error("durable authority state is from the future")]
    FutureState,
    /// The run-generation counter cannot advance safely.
    #[error("durable authority run generation exhausted")]
    GenerationExhausted,
    /// Canonical serialization exceeded the bounded platform payload contract.
    #[error("durable authority state exceeds its byte bound")]
    StateTooLarge,
    /// A closed or failed durability session cannot publish further authority.
    #[error("durable authority session is unavailable")]
    SessionUnavailable,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DurableRunState {
    Clean,
    InUse,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BudgetCheckpointState {
    pub(super) window_started_wall: Timestamp,
    pub(super) window_ends_wall: Timestamp,
    pub(super) requests_used: u32,
    pub(super) in_flight: u16,
    pub(super) unavailable_until_wall: Option<Timestamp>,
    pub(super) disabled: bool,
    pub(super) consecutive_refusals: u32,
    pub(super) availability_generation: u64,
    pub(super) terminal: bool,
    pub(super) poisoned: bool,
}

impl BudgetCheckpointState {
    pub(crate) fn terminalize_unclean(&mut self) {
        self.terminal = true;
        self.poisoned = true;
        self.disabled = true;
        self.availability_generation = self.availability_generation.saturating_add(1);
    }

    pub(crate) const fn in_flight(&self) -> u16 {
        self.in_flight
    }

    fn shift_wall_anchor(&mut self, delta: i64) -> Result<(), AuthorityPersistenceError> {
        self.window_started_wall = self
            .window_started_wall
            .checked_add_nanos(delta)
            .map_err(|_| AuthorityPersistenceError::InvalidState)?;
        self.window_ends_wall = self
            .window_ends_wall
            .checked_add_nanos(delta)
            .map_err(|_| AuthorityPersistenceError::InvalidState)?;
        self.unavailable_until_wall = self
            .unavailable_until_wall
            .map(|deadline| deadline.checked_add_nanos(delta))
            .transpose()
            .map_err(|_| AuthorityPersistenceError::InvalidState)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableBudgetGroup {
    declarations: BoundedVec<PersistedProviderBudgetPolicy, MAX_PROCESS_BUDGET_SCOPES>,
    checkpoint: BudgetCheckpointState,
}

impl DurableBudgetGroup {
    pub(crate) fn try_new(
        declaration: PersistedProviderBudgetPolicy,
        checkpoint: BudgetCheckpointState,
    ) -> Result<Self, AuthorityPersistenceError> {
        Ok(Self {
            declarations: BoundedVec::singleton(declaration),
            checkpoint,
        })
    }

    pub(crate) fn declarations(&self) -> &[PersistedProviderBudgetPolicy] {
        self.declarations.as_slice()
    }

    pub(crate) const fn checkpoint(&self) -> &BudgetCheckpointState {
        &self.checkpoint
    }

    fn add_declaration(
        &mut self,
        declaration: PersistedProviderBudgetPolicy,
    ) -> Result<(), AuthorityPersistenceError> {
        if self.declarations.as_slice().contains(&declaration) {
            return Ok(());
        }
        let mut declarations = Vec::new();
        declarations
            .try_reserve(
                self.declarations
                    .len()
                    .checked_add(1)
                    .ok_or(AuthorityPersistenceError::StateTooLarge)?,
            )
            .map_err(|_| AuthorityPersistenceError::StateTooLarge)?;
        declarations.extend(self.declarations.as_slice().iter().cloned());
        declarations.push(declaration);
        self.declarations = BoundedVec::try_new(declarations)
            .map_err(|_| AuthorityPersistenceError::StateTooLarge)?;
        Ok(())
    }

    fn canonicalize(&mut self) -> Result<(), AuthorityPersistenceError> {
        let mut keyed = self
            .declarations
            .as_slice()
            .iter()
            .map(|declaration| {
                canonical_json_bytes(&declaration).map(|key| (key, declaration))
            })
            .collect::<Result<Vec<_>, _>>()?;
        keyed.sort_by(|left, right| left.0.cmp(&right.0));
        if keyed.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(AuthorityPersistenceError::InvalidState);
        }
        self.declarations = BoundedVec::try_new(
            keyed
                .into_iter()
                .map(|(_key, declaration)| declaration.clone())
                .collect(),
        )
        .map_err(|_| AuthorityPersistenceError::StateTooLarge)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableAuthorityEnvelope {
    format_version: u16,
    run_generation: u64,
    run_state: DurableRunState,
    saved_at_wall: Timestamp,
    wall_high_water: Timestamp,
    registry: crate::RegistryAuthorityState,
    budgets: BoundedVec<DurableBudgetGroup, MAX_PROCESS_BUDGET_SCOPES>,
}

impl DurableAuthorityEnvelope {
    fn empty(now: Timestamp) -> Self {
        Self {
            format_version: DURABLE_AUTHORITY_FORMAT_VERSION,
            run_generation: 0,
            run_state: DurableRunState::Clean,
            saved_at_wall: now,
            wall_high_water: now,
            registry: crate::RegistryAuthorityState::empty(),
            budgets: BoundedVec::empty(),
        }
    }

    fn canonicalize(&mut self) -> Result<(), AuthorityPersistenceError> {
        self.registry.canonicalize()?;
        let mut groups = self.budgets.as_slice().to_vec();
        for group in &mut groups {
            group.canonicalize()?;
        }
        let mut keyed = groups
            .into_iter()
            .map(|group| canonical_json_bytes(&group).map(|key| (key, group)))
            .collect::<Result<Vec<_>, _>>()?;
        keyed.sort_by(|left, right| left.0.cmp(&right.0));
        self.budgets = BoundedVec::try_new(
            keyed
                .into_iter()
                .map(|(_key, group)| group)
                .collect(),
        )
        .map_err(|_| AuthorityPersistenceError::StateTooLarge)?;
        Ok(())
    }

    fn validate(&self, now: Timestamp) -> Result<(), AuthorityPersistenceError> {
        if self.format_version != DURABLE_AUTHORITY_FORMAT_VERSION
            || self.run_generation == 0
            || self.saved_at_wall != self.wall_high_water
        {
            return Err(AuthorityPersistenceError::InvalidState);
        }
        if self.wall_high_water > now {
            return Err(AuthorityPersistenceError::WallRollback);
        }
        if self.saved_at_wall > now {
            return Err(AuthorityPersistenceError::FutureState);
        }
        let checkpoint_observation = ClockObservation::new(now, MonotonicInstant::from_nanos(0));
        for group in self.budgets.as_slice() {
            let first = group
                .declarations
                .as_slice()
                .first()
                .ok_or(AuthorityPersistenceError::InvalidState)?;
            if group
                .declarations
                .as_slice()
                .iter()
                .skip(1)
                .any(|declaration| {
                    !first.policy().has_same_limits_as(declaration.policy())
                })
            {
                return Err(AuthorityPersistenceError::InvalidState);
            }
            validate_checkpoint(first.policy(), &group.checkpoint, checkpoint_observation)?;
        }
        if self.run_state == DurableRunState::Clean
            && self
                .budgets
                .as_slice()
                .iter()
                .any(|group| group.checkpoint.in_flight() != 0)
        {
            return Err(AuthorityPersistenceError::InvalidState);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct AuthorityDurabilitySession {
    store: Mutex<Option<Arc<dyn AuthorityStateStore>>>,
    envelope: Mutex<DurableAuthorityEnvelope>,
    recovered_unclean: bool,
    lifecycle: lifecycle::AuthorityLifecycleWord,
}

/// Linear capability for a newly opened run that has not entered registry ownership yet.
#[derive(Debug)]
pub(crate) struct UnpublishedAuthoritySession {
    session: Arc<AuthorityDurabilitySession>,
    finalized: bool,
}

impl UnpublishedAuthoritySession {
    pub(crate) fn session(&self) -> &Arc<AuthorityDurabilitySession> {
        &self.session
    }

    pub(crate) fn publish(mut self) -> Result<(), AuthorityPersistenceError> {
        if !self.session.is_available() {
            return Err(AuthorityPersistenceError::SessionUnavailable);
        }
        self.finalized = true;
        Ok(())
    }

    pub(crate) fn rollback(mut self) -> Result<(), AuthorityPersistenceError> {
        let result = self.session.rollback_unpublished_open();
        if result.is_ok() {
            self.finalized = true;
        }
        result
    }
}

impl Drop for UnpublishedAuthoritySession {
    fn drop(&mut self) {
        if !self.finalized {
            self.session.invalidate();
        }
    }
}

impl AuthorityDurabilitySession {
    pub(crate) fn open_unpublished(
        store: Arc<dyn AuthorityStateStore>,
        now: Timestamp,
    ) -> Result<UnpublishedAuthoritySession, AuthorityPersistenceError> {
        Self::open_session(store, now).map(|session| UnpublishedAuthoritySession {
            session,
            finalized: false,
        })
    }

    #[cfg(test)]
    pub(crate) fn open(
        store: Arc<dyn AuthorityStateStore>,
        now: Timestamp,
    ) -> Result<Arc<Self>, AuthorityPersistenceError> {
        Self::open_session(store, now)
    }

    fn open_session(
        store: Arc<dyn AuthorityStateStore>,
        now: Timestamp,
    ) -> Result<Arc<Self>, AuthorityPersistenceError> {
        let mut envelope = match store.load().map_err(|_| AuthorityPersistenceError::Store)? {
            Some(bytes) => {
                let envelope = deserialize_canonical_envelope(&bytes)?;
                envelope.validate(now)?;
                envelope
            }
            None => DurableAuthorityEnvelope::empty(now),
        };
        let recovered_unclean = envelope.run_state == DurableRunState::InUse;
        if recovered_unclean {
            let mut groups = envelope.budgets.as_slice().to_vec();
            for group in &mut groups {
                group.checkpoint.terminalize_unclean();
            }
            envelope.budgets = BoundedVec::try_new(groups)
                .map_err(|_| AuthorityPersistenceError::StateTooLarge)?;
        }
        envelope.run_generation = envelope
            .run_generation
            .checked_add(1)
            .ok_or(AuthorityPersistenceError::GenerationExhausted)?;
        envelope.run_state = DurableRunState::InUse;
        envelope.saved_at_wall = now;
        envelope.wall_high_water = now;
        let payload = serialize_canonical_envelope(&envelope)?;
        store
            .store(&payload)
            .map_err(|_| AuthorityPersistenceError::Store)?;
        Ok(Arc::new(Self {
            store: Mutex::new(Some(store)),
            envelope: Mutex::new(envelope),
            recovered_unclean,
            lifecycle: lifecycle::AuthorityLifecycleWord::new(Self::initial_lifecycle_word()),
        }))
    }

    pub(crate) fn is_available(&self) -> bool {
        !self.recovered_unclean
            && !self.envelope.is_poisoned()
            && !self.store.is_poisoned()
            && self.lifecycle_is_active()
    }

    pub(crate) fn closed_clean(&self) -> bool {
        !self.recovered_unclean
            && !self.envelope.is_poisoned()
            && !self.store.is_poisoned()
            && self.lifecycle_is_closed()
    }

    pub(crate) fn invalidate(&self) {
        if !self.fail_active_session_without_terminal_write() {
            return;
        }
        let _envelope = match self.envelope.lock() {
            Ok(envelope) => envelope,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut store = match self.store.lock() {
            Ok(store) => store,
            Err(poisoned) => poisoned.into_inner(),
        };
        *store = None;
    }

    pub(crate) const fn recovered_unclean(&self) -> bool {
        self.recovered_unclean
    }

    pub(crate) fn registry_state(
        &self,
    ) -> Result<crate::RegistryAuthorityState, AuthorityPersistenceError> {
        self.envelope
            .lock()
            .map(|envelope| envelope.registry.clone())
            .map_err(|_| self.fail(AuthorityPersistenceError::SessionUnavailable))
    }

    pub(crate) fn budget_groups(
        &self,
    ) -> Result<Vec<DurableBudgetGroup>, AuthorityPersistenceError> {
        self.envelope
            .lock()
            .map(|envelope| envelope.budgets.as_slice().to_vec())
            .map_err(|_| self.fail(AuthorityPersistenceError::SessionUnavailable))
    }

    pub(crate) fn register_budget_group(
        self: &Arc<Self>,
        registry: crate::RegistryAuthorityState,
        declaration: PersistedProviderBudgetPolicy,
        checkpoint: BudgetCheckpointState,
        wall: Timestamp,
    ) -> Result<usize, AuthorityPersistenceError> {
        let mut assigned = None;
        self.transact(wall, |envelope, wall_adjustment| {
            let mut budgets = envelope.budgets.as_slice().to_vec();
            if budgets.len() == MAX_PROCESS_BUDGET_SCOPES {
                return Err(AuthorityPersistenceError::StateTooLarge);
            }
            assigned = Some(budgets.len());
            let mut anchored = checkpoint;
            anchored.shift_wall_anchor(wall_adjustment)?;
            budgets.push(DurableBudgetGroup::try_new(declaration, anchored)?);
            envelope.budgets = BoundedVec::try_new(budgets)
                .map_err(|_| AuthorityPersistenceError::StateTooLarge)?;
            envelope.registry = registry;
            Ok(())
        })?;
        assigned.ok_or(AuthorityPersistenceError::InvalidState)
    }

    pub(crate) fn add_budget_declaration(
        self: &Arc<Self>,
        slot: usize,
        registry: crate::RegistryAuthorityState,
        declaration: PersistedProviderBudgetPolicy,
        wall: Timestamp,
    ) -> Result<(), AuthorityPersistenceError> {
        self.transact(wall, |envelope, _wall_adjustment| {
            let mut budgets = envelope.budgets.as_slice().to_vec();
            budgets
                .get_mut(slot)
                .ok_or(AuthorityPersistenceError::InvalidState)?
                .add_declaration(declaration)?;
            envelope.budgets = BoundedVec::try_new(budgets)
                .map_err(|_| AuthorityPersistenceError::StateTooLarge)?;
            envelope.registry = registry;
            Ok(())
        })
    }

    #[cfg(test)]
    pub(crate) fn update_budget(
        self: &Arc<Self>,
        slot: usize,
        checkpoint: BudgetCheckpointState,
        wall: Timestamp,
    ) -> Result<(), AuthorityPersistenceError> {
        self.transact(wall, |envelope, wall_adjustment| {
            let mut budgets = envelope.budgets.as_slice().to_vec();
            let mut anchored = checkpoint;
            anchored.shift_wall_anchor(wall_adjustment)?;
            budgets
                .get_mut(slot)
                .ok_or(AuthorityPersistenceError::InvalidState)?
                .checkpoint = anchored;
            envelope.budgets = BoundedVec::try_new(budgets)
                .map_err(|_| AuthorityPersistenceError::StateTooLarge)?;
            Ok(())
        })
    }

    pub(in crate::policy) fn update_budget_admitted(
        &self,
        admission: &lifecycle::AuthorityOperationAdmission,
        slot: usize,
        checkpoint: BudgetCheckpointState,
        wall: Timestamp,
    ) -> Result<(), AuthorityPersistenceError> {
        self.transact_with_admission(admission, wall, |envelope, wall_adjustment| {
            let mut budgets = envelope.budgets.as_slice().to_vec();
            let mut anchored = checkpoint;
            anchored.shift_wall_anchor(wall_adjustment)?;
            budgets
                .get_mut(slot)
                .ok_or(AuthorityPersistenceError::InvalidState)?
                .checkpoint = anchored;
            envelope.budgets = BoundedVec::try_new(budgets)
                .map_err(|_| AuthorityPersistenceError::StateTooLarge)?;
            Ok(())
        })
    }

    pub(crate) fn persist_registry(
        self: &Arc<Self>,
        registry: crate::RegistryAuthorityState,
        wall: Timestamp,
    ) -> Result<(), AuthorityPersistenceError> {
        self.transact(wall, |envelope, _wall_adjustment| {
            envelope.registry = registry;
            Ok(())
        })
    }

    pub(crate) fn close_clean(
        self: &Arc<Self>,
        proof: CleanShutdownProof,
        registry: crate::RegistryAuthorityState,
        wall: Timestamp,
    ) -> Result<(), AuthorityPersistenceError> {
        if !proof.belongs_to(self) {
            proof.invalidate_bound_session();
            self.invalidate();
            return Err(AuthorityPersistenceError::SessionUnavailable);
        }
        self.close_clean_after_validation(registry, wall)
    }

    #[cfg(test)]
    pub(crate) fn close_clean_for_test(
        self: &Arc<Self>,
        registry: crate::RegistryAuthorityState,
        wall: Timestamp,
    ) -> Result<(), AuthorityPersistenceError> {
        self.close_clean_after_validation(registry, wall)
    }

    fn close_clean_after_validation(
        self: &Arc<Self>,
        registry: crate::RegistryAuthorityState,
        wall: Timestamp,
    ) -> Result<(), AuthorityPersistenceError> {
        if self.recovered_unclean {
            return Err(AuthorityPersistenceError::SessionUnavailable);
        }
        self.begin_clean_close()?;
        let result = self.store_clean_envelope(registry, wall);
        self.finish_clean_close(result.is_ok());
        self.detach_store();
        result
    }

    fn rollback_unpublished_open(
        self: &Arc<Self>,
    ) -> Result<(), AuthorityPersistenceError> {
        if self.recovered_unclean {
            return Err(AuthorityPersistenceError::SessionUnavailable);
        }
        let (registry, wall) = self
            .envelope
            .lock()
            .map(|envelope| (envelope.registry.clone(), envelope.wall_high_water))
            .map_err(|_| self.fail(AuthorityPersistenceError::SessionUnavailable))?;
        self.close_clean_after_validation(registry, wall)
    }

    fn transact(
        self: &Arc<Self>,
        wall: Timestamp,
        mutation: impl FnOnce(
            &mut DurableAuthorityEnvelope,
            i64,
        ) -> Result<(), AuthorityPersistenceError>,
    ) -> Result<(), AuthorityPersistenceError> {
        let admission = self.admit_operation()?;
        self.transact_with_admission(&admission, wall, mutation)
    }

    fn transact_with_admission(
        &self,
        admission: &lifecycle::AuthorityOperationAdmission,
        wall: Timestamp,
        mutation: impl FnOnce(
            &mut DurableAuthorityEnvelope,
            i64,
        ) -> Result<(), AuthorityPersistenceError>,
    ) -> Result<(), AuthorityPersistenceError> {
        if !admission.belongs_to(self) {
            self.invalidate();
            return Err(AuthorityPersistenceError::SessionUnavailable);
        }
        let result = self.transact_admitted(admission, wall, mutation);
        if result.is_err() {
            admission.latch_terminal();
            let _terminal = self.persist_terminal_and_detach();
        }
        result
    }

    fn transact_admitted(
        &self,
        admission: &lifecycle::AuthorityOperationAdmission,
        wall: Timestamp,
        mutation: impl FnOnce(
            &mut DurableAuthorityEnvelope,
            i64,
        ) -> Result<(), AuthorityPersistenceError>,
    ) -> Result<(), AuthorityPersistenceError> {
        if !admission.is_active_for(self) {
            return Err(AuthorityPersistenceError::SessionUnavailable);
        }
        let mut current = self
            .envelope
            .lock()
            .map_err(|_| AuthorityPersistenceError::SessionUnavailable)?;
        if !admission.is_active_for(self) {
            return Err(AuthorityPersistenceError::SessionUnavailable);
        }
        let effective_wall = wall.max(current.wall_high_water);
        let wall_adjustment = effective_wall
            .unix_nanos()
            .checked_sub(wall.unix_nanos())
            .ok_or(AuthorityPersistenceError::InvalidState)?;
        let mut candidate = current.clone();
        mutation(&mut candidate, wall_adjustment)?;
        candidate.saved_at_wall = effective_wall;
        candidate.wall_high_water = effective_wall;
        let payload = serialize_canonical_envelope(&candidate)?;
        let store = self
            .store
            .lock()
            .map_err(|_| AuthorityPersistenceError::SessionUnavailable)?;
        if !admission.is_active_for(self) {
            return Err(AuthorityPersistenceError::SessionUnavailable);
        }
        let Some(active_store) = store.as_ref() else {
            return Err(AuthorityPersistenceError::SessionUnavailable);
        };
        if active_store.store(&payload).is_err() {
            return Err(AuthorityPersistenceError::Store);
        }
        *current = candidate;
        if !admission.is_active_for(self) {
            return Err(AuthorityPersistenceError::SessionUnavailable);
        }
        Ok(())
    }

    fn store_clean_envelope(
        &self,
        registry: crate::RegistryAuthorityState,
        wall: Timestamp,
    ) -> Result<(), AuthorityPersistenceError> {
        let mut current = self
            .envelope
            .lock()
            .map_err(|_| AuthorityPersistenceError::SessionUnavailable)?;
        if current
            .budgets
            .as_slice()
            .iter()
            .any(|group| group.checkpoint.in_flight != 0)
        {
            return Err(AuthorityPersistenceError::InvalidState);
        }
        let effective_wall = wall.max(current.wall_high_water);
        let mut candidate = current.clone();
        candidate.registry = registry;
        candidate.run_state = DurableRunState::Clean;
        candidate.saved_at_wall = effective_wall;
        candidate.wall_high_water = effective_wall;
        let payload = serialize_canonical_envelope(&candidate)?;
        let store = self
            .store
            .lock()
            .map_err(|_| AuthorityPersistenceError::SessionUnavailable)?;
        let active_store = store
            .as_ref()
            .ok_or(AuthorityPersistenceError::SessionUnavailable)?;
        active_store
            .store(&payload)
            .map_err(|_| AuthorityPersistenceError::Store)?;
        *current = candidate;
        Ok(())
    }

    fn fail(&self, error: AuthorityPersistenceError) -> AuthorityPersistenceError {
        self.invalidate();
        error
    }
}

fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, AuthorityPersistenceError> {
    serde_json::to_vec(value).map_err(|_| AuthorityPersistenceError::InvalidState)
}

fn serialize_canonical_envelope(
    envelope: &DurableAuthorityEnvelope,
) -> Result<Vec<u8>, AuthorityPersistenceError> {
    let mut canonical = envelope.clone();
    canonical.canonicalize()?;
    let payload = canonical_json_bytes(&canonical)?;
    if payload.len() > MAX_DURABLE_AUTHORITY_STATE_BYTES {
        return Err(AuthorityPersistenceError::StateTooLarge);
    }
    Ok(payload)
}

fn deserialize_canonical_envelope(
    payload: &[u8],
) -> Result<DurableAuthorityEnvelope, AuthorityPersistenceError> {
    if payload.len() > MAX_DURABLE_AUTHORITY_STATE_BYTES {
        return Err(AuthorityPersistenceError::StateTooLarge);
    }
    let envelope: DurableAuthorityEnvelope =
        serde_json::from_slice(payload).map_err(|_| AuthorityPersistenceError::InvalidState)?;
    let canonical = serialize_canonical_envelope(&envelope)?;
    if canonical != payload {
        return Err(AuthorityPersistenceError::InvalidState);
    }
    Ok(envelope)
}

include!("persistence/tests.rs");
