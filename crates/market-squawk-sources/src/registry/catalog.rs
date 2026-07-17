/// Stateful registry that alone can mint opaque registration and current-session handles.
#[derive(Debug)]
pub struct AuthoritativeSourceRegistry {
    instance_id: u64,
    entries: BTreeMap<SourceId, RegistryEntry>,
    budgets: ProviderBudgetPool,
    history: BTreeMap<SourceId, SourceAuthorityHistory>,
    clock: Arc<dyn RegistryClock>,
}

impl Drop for AuthoritativeSourceRegistry {
    fn drop(&mut self) {
        // Registry lifetime is an authority dimension. Retained session, capture, frame, and
        // pre-feed handles must fail closed once their sole authoritative owner exits.
        for entry in self.entries.values_mut() {
            if let Some(active) = entry.active.take() {
                active.lease.invalidate();
                active.capture.mark_incomplete();
            }
            entry.health_authority = None;
        }
    }
}

impl AuthoritativeSourceRegistry {
    /// Creates an empty registry with a process-unique instance identity.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::RegistryIdentityExhausted`] if the process-wide identifier space is
    /// exhausted.
    pub fn try_new() -> Result<Self, RegistryError> {
        Self::try_new_with_authority_state(RegistryAuthorityState::empty())
    }

    /// Restores bounded authority tombstones before any source can be registered.
    ///
    /// # Errors
    ///
    /// Rejects unsupported/tampered state, duplicate sources/budget scopes, or coordinator failure.
    pub fn try_new_with_authority_state(
        state: RegistryAuthorityState,
    ) -> Result<Self, RegistryError> {
        Self::try_new_with_authority_state_and_clock(
            state,
            Arc::new(SystemRegistryClock::try_new()?),
        )
    }

    fn try_new_with_authority_state_and_clock(
        state: RegistryAuthorityState,
        clock: Arc<dyn RegistryClock>,
    ) -> Result<Self, RegistryError> {
        let instance_id = NEXT_REGISTRY_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| RegistryError::RegistryIdentityExhausted)?;
        let mut budgets =
            ProviderBudgetPool::new().map_err(|_| RegistryError::BudgetCoordinator)?;
        budgets
            .register_all(state.budget_policies.as_slice())
            .map_err(|_| RegistryError::BudgetCoordinator)?;
        let history = state
            .sources
            .as_slice()
            .iter()
            .map(|source| {
                (
                    source.source_id.clone(),
                    SourceAuthorityHistory {
                        used_revisions: source.used_revisions.as_slice().to_vec(),
                        last_epoch: source.last_epoch,
                        generation_high_water: source.generation_high_water,
                    },
                )
            })
            .collect();
        Ok(Self {
            instance_id,
            entries: BTreeMap::new(),
            budgets,
            history,
            clock,
        })
    }

    /// Exports bounded serializable tombstones; live handles and leases are deliberately absent.
    ///
    /// # Errors
    ///
    /// Fails if configured source or budget counts exceed persisted bounds.
    pub fn export_authority_state(&self) -> Result<RegistryAuthorityState, RegistryError> {
        let mut sources = Vec::with_capacity(self.history.len());
        for (source_id, history) in &self.history {
            sources.push(PersistedSourceAuthority {
                source_id: source_id.clone(),
                used_revisions: BoundedVec::try_new(history.used_revisions.clone())
                    .map_err(|_| RegistryError::RevisionHistoryExhausted)?,
                last_epoch: history.last_epoch,
                generation_high_water: history.generation_high_water,
            });
        }
        RegistryAuthorityState::try_new(sources, self.budgets.policies())
    }

    /// Registers effective, checked source metadata and returns an opaque handle.
    ///
    /// # Errors
    ///
    /// Rejects duplicate source identity and metadata whose authorization or coverage is not
    /// effective at `at`.
    pub fn register(
        &mut self,
        metadata: SourceMetadata,
        at: Timestamp,
    ) -> Result<RegisteredSource, RegistryError> {
        if !metadata.is_effective_at(at) {
            return Err(RegistryError::MetadataNotEffective);
        }
        if self.entries.contains_key(metadata.source_id()) {
            return Err(RegistryError::SourceAlreadyRegistered);
        }
        let previous = self.history.get(metadata.source_id());
        if previous.is_none() && self.history.len() >= MAX_AUTHORITY_SOURCES {
            return Err(RegistryError::AuthorityStateCapacity);
        }
        if previous.is_some_and(|history| history.used_revisions.contains(metadata.revision())) {
            return Err(RegistryError::RevisionAlreadyUsed);
        }
        if previous.is_some_and(|history| history.used_revisions.len() == MAX_REVISIONS_PER_SOURCE)
        {
            return Err(RegistryError::RevisionHistoryExhausted);
        }
        let source_id = metadata.source_id().clone();
        let revision = metadata.revision().clone();
        let epoch = previous.map_or(Ok(1), |history| {
            history
                .last_epoch
                .checked_add(1)
                .ok_or(RegistryError::EpochExhausted)
        })?;
        let generation_high_water = previous.and_then(|history| history.generation_high_water);
        let mut used_revisions =
            previous.map_or_else(Vec::new, |history| history.used_revisions.clone());
        used_revisions.push(revision.clone());
        let budget = metadata
            .budget_policy()
            .cloned()
            .map(|policy| self.budgets.register(policy))
            .transpose()
            .map_err(|_| RegistryError::BudgetCoordinator)?;
        self.history.insert(
            source_id.clone(),
            SourceAuthorityHistory {
                used_revisions: used_revisions.clone(),
                last_epoch: epoch,
                generation_high_water,
            },
        );
        self.entries.insert(
            source_id.clone(),
            RegistryEntry {
                metadata,
                epoch,
                revoked: false,
                active: None,
                health_authority: None,
                universe_attestation: None,
                generation_high_water,
                used_revisions,
            },
        );
        Ok(RegisteredSource {
            registry_id: self.instance_id,
            source_id,
            revision,
            epoch,
            budget,
        })
    }

    /// Atomically replaces a registered source revision and invalidates every prior session.
    ///
    /// # Errors
    ///
    /// Rejects stale/transplanted handles, source changes, identical revisions, ineffective
    /// metadata, and epoch overflow.
    pub fn replace_metadata(
        &mut self,
        registered: &RegisteredSource,
        metadata: SourceMetadata,
        at: Timestamp,
    ) -> Result<RegisteredSource, RegistryError> {
        self.validate_registered_structure(registered)?;
        if metadata.source_id() != &registered.source_id {
            return Err(RegistryError::HandleTransplanted);
        }
        if !metadata.is_effective_at(at) {
            return Err(RegistryError::MetadataNotEffective);
        }
        let entry = self
            .entries
            .get_mut(&registered.source_id)
            .ok_or(RegistryError::UnknownSource)?;
        if entry.used_revisions.contains(metadata.revision()) {
            return Err(RegistryError::RevisionAlreadyUsed);
        }
        if entry.used_revisions.len() == MAX_REVISIONS_PER_SOURCE {
            return Err(RegistryError::RevisionHistoryExhausted);
        }
        let epoch = entry
            .epoch
            .checked_add(1)
            .ok_or(RegistryError::EpochExhausted)?;
        let revision = metadata.revision().clone();
        let budget = metadata
            .budget_policy()
            .cloned()
            .map(|policy| self.budgets.register(policy))
            .transpose()
            .map_err(|_| RegistryError::BudgetCoordinator)?;
        entry.metadata = metadata;
        entry.health_authority = None;
        entry.universe_attestation = None;
        entry.used_revisions.push(revision.clone());
        entry.epoch = epoch;
        if let Some(active) = entry.active.take() {
            active.lease.invalidate();
            active.capture.mark_incomplete();
        }
        if let Some(history) = self.history.get_mut(&registered.source_id) {
            history.used_revisions.push(revision.clone());
            history.last_epoch = epoch;
        }
        Ok(RegisteredSource {
            registry_id: self.instance_id,
            source_id: registered.source_id.clone(),
            revision,
            epoch,
            budget,
        })
    }

    /// Records exact membership evidence for an `all_declared` provider product universe.
    ///
    /// Recording or replacing an attestation invalidates current health/scope authority so a
    /// later runtime subscription observation must explicitly requalify the generation.
    ///
    /// # Errors
    ///
    /// Rejects stale handles, wrong products, ineffective evidence, or non-live metadata.
    pub fn attest_instrument_universe(
        &mut self,
        registered: &RegisteredSource,
        attestation: InstrumentUniverseAttestation,
        at: Timestamp,
    ) -> Result<(), RegistryError> {
        self.validate_registered(registered, at)?;
        let entry = self
            .entries
            .get_mut(&registered.source_id)
            .ok_or(RegistryError::UnknownSource)?;
        let live = entry
            .metadata
            .coverage()
            .live()
            .ok_or(RegistryError::LiveScopeNotCovered)?;
        if &attestation.provider_product != live.provider_product()
            || !attestation.is_effective_at(at)
        {
            return Err(RegistryError::UniverseAttestationMismatch);
        }
        let next_health_epoch = entry
            .active
            .as_ref()
            .map(|active| {
                active
                    .lease
                    .next_health_epoch()
                    .ok_or(RegistryError::HealthEpochExhausted)
            })
            .transpose()?;
        entry.universe_attestation = Some(attestation);
        entry.health_authority = None;
        if let (Some(active), Some(epoch)) = (&entry.active, next_health_epoch) {
            active
                .lease
                .commit_live_qualification(epoch, false, None, None);
        }
        Ok(())
    }

    /// Starts the sole current session for a registered source.
    ///
    /// A later session start invalidates the previous session even if callers retain its handle.
    ///
    /// # Errors
    ///
    /// Rejects stale/transplanted handles, ineffective metadata, or a non-increasing generation.
    pub fn begin_session(
        &mut self,
        registered: &RegisteredSource,
        session_id: SessionId,
        generation: ConnectionGeneration,
        at: Timestamp,
    ) -> Result<CurrentSourceSession, RegistryError> {
        self.validate_registered(registered, at)?;
        let started_at = self.clock.observe()?;
        let entry = self
            .entries
            .get_mut(&registered.source_id)
            .ok_or(RegistryError::UnknownSource)?;
        if entry
            .generation_high_water
            .is_some_and(|previous| generation <= previous)
        {
            return Err(RegistryError::GenerationNotAdvanced);
        }
        if let Some(active) = entry.active.take() {
            active.lease.invalidate();
            active.capture.mark_incomplete();
        }
        let lease = Arc::new(SessionLeaseState {
            current: AtomicBool::new(true),
            live_qualified: AtomicBool::new(false),
            health_epoch: AtomicU64::new(0),
            valid_from_nanos: AtomicI64::new(i64::MAX),
            valid_until_nanos: AtomicI64::new(i64::MIN),
            last_health_observed_nanos: AtomicI64::new(i64::MIN),
            frame_ordinal: AtomicU64::new(0),
        });
        let capture = crate::CaptureGenerationLease::new_generation();
        entry.active = Some(ActiveSessionKey {
            session_id: session_id.clone(),
            generation,
            lease: Arc::clone(&lease),
            capture_issuer_taken: false,
            health_reporter_taken: false,
            raw_frame_factory_taken: false,
            capture: capture.clone(),
            started_at,
        });
        entry.health_authority = None;
        entry.generation_high_water = Some(generation);
        if let Some(history) = self.history.get_mut(&registered.source_id) {
            history.generation_high_water = Some(generation);
        }
        let binding = FrameSessionBinding::new(
            registered.source_id.clone(),
            registered.revision.clone(),
            session_id,
            generation,
        );
        Ok(CurrentSourceSession {
            registry_id: self.instance_id,
            epoch: registered.epoch,
            binding,
            budget: registered.budget.clone(),
            lease,
            capture,
            started_at: started_at.wall(),
        })
    }

    /// Moves the sole admission issuer and a cloneable degrade-only capability to capture wiring.
    ///
    /// # Errors
    ///
    /// Rejects stale/transplanted sessions and a second attempt to take the non-clone issuer.
    pub fn take_capture_generation_capabilities(
        &mut self,
        session: &CurrentSourceSession,
    ) -> Result<crate::CaptureGenerationCapabilities, RegistryError> {
        self.validate_session_structure(session)?;
        let entry = self
            .entries
            .get_mut(session.source_id())
            .ok_or(RegistryError::UnknownSource)?;
        let active = entry
            .active
            .as_mut()
            .ok_or(RegistryError::SessionNotCurrent)?;
        if active.capture_issuer_taken {
            return Err(RegistryError::CaptureIssuerAlreadyTaken);
        }
        active.capture_issuer_taken = true;
        Ok(crate::CaptureGenerationCapabilities::new(
            session.binding.clone(),
            session.capture.clone(),
        ))
    }

    /// Moves the sole current-health reporter into source supervision.
    ///
    /// # Errors
    ///
    /// Rejects stale/transplanted sessions or a second reporter request.
    pub fn take_current_health_reporter(
        &mut self,
        session: &CurrentSourceSession,
    ) -> Result<CurrentHealthReporter, RegistryError> {
        self.validate_session_structure(session)?;
        let entry = self
            .entries
            .get_mut(session.source_id())
            .ok_or(RegistryError::UnknownSource)?;
        let active = entry
            .active
            .as_mut()
            .ok_or(RegistryError::SessionNotCurrent)?;
        if active.health_reporter_taken {
            return Err(RegistryError::HealthReporterAlreadyTaken);
        }
        active.health_reporter_taken = true;
        let session_started_at = active.started_at;
        Ok(CurrentHealthReporter {
            binding: session.binding.clone(),
            lease: Arc::clone(&session.lease),
            freshness: entry.metadata.freshness_policy(),
            budget: session.budget.clone(),
            clock: Arc::clone(&self.clock),
            session_started_at,
            not_sync: PhantomData,
        })
    }

    /// Moves the sole raw-frame construction capability into the live adapter boundary.
    ///
    /// The factory carries exact session identity and a checked frame counter/currentness lease,
    /// but exposes no registry, health, capture, or budget authority.
    ///
    /// # Errors
    ///
    /// Rejects stale/transplanted sessions and a second factory request for the generation.
    pub fn take_raw_frame_factory(
        &mut self,
        session: &CurrentSourceSession,
    ) -> Result<RawFrameFactory, RegistryError> {
        self.validate_session_structure(session)?;
        let entry = self
            .entries
            .get_mut(session.source_id())
            .ok_or(RegistryError::UnknownSource)?;
        let active = entry
            .active
            .as_mut()
            .ok_or(RegistryError::SessionNotCurrent)?;
        if active.raw_frame_factory_taken {
            return Err(RegistryError::RawFrameFactoryAlreadyTaken);
        }
        active.raw_frame_factory_taken = true;
        Ok(RawFrameFactory {
            binding: session.binding.clone(),
            lease: Arc::clone(&session.lease),
            not_sync: PhantomData,
        })
    }

    /// Ends the exact current session, invalidating its retained handle.
    ///
    /// # Errors
    ///
    /// Rejects a stale, ended, or transplanted session.
    pub fn end_session(
        &mut self,
        session: &CurrentSourceSession,
        _at: Timestamp,
    ) -> Result<(), RegistryError> {
        self.validate_session_structure(session)?;
        let entry = self
            .entries
            .get_mut(session.source_id())
            .ok_or(RegistryError::UnknownSource)?;
        if let Some(active) = entry.active.take() {
            active.lease.invalidate();
            active.capture.mark_incomplete();
        }
        entry.health_authority = None;
        Ok(())
    }

    /// Revokes the exact registered source and all sessions minted from it.
    ///
    /// # Errors
    ///
    /// Rejects a stale or transplanted registration handle. Revocation remains available when the
    /// epoch is exhausted: the terminal revoked bit and synchronous lease invalidation do not
    /// require a successor authority epoch.
    pub fn revoke(
        &mut self,
        registered: &RegisteredSource,
        at: Timestamp,
    ) -> Result<(), RegistryError> {
        let _ = at;
        self.validate_registered_structure(registered)?;
        let entry = self
            .entries
            .get_mut(&registered.source_id)
            .ok_or(RegistryError::UnknownSource)?;
        let history = self
            .history
            .get_mut(&registered.source_id)
            .ok_or(RegistryError::InvalidAuthorityState)?;
        let revoked_epoch = entry.epoch.checked_add(1).unwrap_or(entry.epoch);
        history.last_epoch = revoked_epoch;
        entry.epoch = revoked_epoch;
        entry.revoked = true;
        if let Some(active) = entry.active.take() {
            active.lease.invalidate();
            active.capture.mark_incomplete();
        }
        entry.health_authority = None;
        Ok(())
    }

    /// Revalidates a registration handle against registry-owned current state.
    ///
    /// # Errors
    ///
    /// Fails closed after transplant, revision replacement, revocation, or effective-time expiry.
    pub fn validate_registered(
        &self,
        registered: &RegisteredSource,
        at: Timestamp,
    ) -> Result<&SourceMetadata, RegistryError> {
        let entry = self.validate_registered_structure(registered)?;
        if !entry.metadata.is_effective_at(at) {
            return Err(RegistryError::MetadataNotEffective);
        }
        Ok(&entry.metadata)
    }

    /// Revalidates a current session and returns a registry-borrowing authority view.
    ///
    /// The returned value cannot outlive this registry borrow and is the only session form intended
    /// for downstream live-qualification authority checks.
    ///
    /// # Errors
    ///
    /// Fails closed after session rollover/end, metadata revision, revocation, transplant, or
    /// effective-time expiry.
    pub fn validate_session<'a>(
        &'a self,
        session: &'a CurrentSourceSession,
        at: Timestamp,
    ) -> Result<ValidatedSourceSession<'a>, RegistryError> {
        let entry = self.validate_session_structure(session)?;
        if !entry.metadata.is_effective_at(at) {
            return Err(RegistryError::MetadataNotEffective);
        }
        Ok(ValidatedSourceSession {
            metadata: &entry.metadata,
            session,
        })
    }

}
