/// Stateful registry that alone can mint opaque registration and current-session handles.
#[derive(Debug)]
pub struct AuthoritativeSourceRegistry {
    instance_id: u64,
    entries: BTreeMap<SourceId, RegistryEntry>,
    budgets: ProviderBudgetPool,
    history: BTreeMap<SourceId, SourceAuthorityHistory>,
    clock: Arc<SealedRegistryClock>,
    authorization_subject_resolver: Arc<dyn crate::AuthorizationSubjectResolver>,
    composition: AuthorityComposition,
}

#[derive(Debug)]
enum AuthorityComposition {
    Durable(Arc<AuthorityDurabilitySession>),
    InMemoryDiagnostic,
    InMemoryExtractionInspection,
}

impl Drop for AuthoritativeSourceRegistry {
    fn drop(&mut self) {
        // Registry lifetime is an authority dimension. Retained session, capture, frame, and
        // pre-feed handles must fail closed once their sole authoritative owner exits.
        for entry in self.entries.values_mut() {
            entry.registration_lease.invalidate();
            if let Some(active) = entry.active.take() {
                active.lease.invalidate();
                active.capture.mark_incomplete();
            }
            entry.health_authority = None;
        }
        if let AuthorityComposition::Durable(durability) = &self.composition {
            durability.invalidate();
        }
    }
}

impl AuthoritativeSourceRegistry {
    /// Registers new metadata or resumes the exact latest revision after a clean restart.
    ///
    /// Resume is deliberately narrower than ordinary registration: the source must not already be
    /// registered in this process, the proposed revision must be the latest durable revision, and
    /// its complete revision-bound payload evidence must equal the persisted evidence. A legacy
    /// state without that evidence cannot resume. A genuinely unused revision follows the strict
    /// registration transaction and consumes one revision-history slot.
    ///
    /// # Errors
    ///
    /// Rejects active duplicates, stale or changed revision evidence, explicit revocation, an
    /// ineffective declaration, unclean durability, exhausted epochs/history, and any persistence
    /// or shared-budget coordination failure without publishing a partial registry entry.
    pub fn register_or_resume_exact(
        &mut self,
        metadata: SourceMetadata,
        at: Timestamp,
    ) -> Result<RegisteredSource, RegistryError> {
        if self.entries.contains_key(metadata.source_id()) {
            return Err(RegistryError::SourceAlreadyRegistered);
        }
        let Some(history) = self.history.get(metadata.source_id()) else {
            return self.register(metadata, at);
        };
        if history.revoked {
            return Err(RegistryError::SourceRevoked);
        }
        if !history.used_revisions.contains(metadata.revision()) {
            return self.register(metadata, at);
        }
        self.resume_exact(metadata, at)
    }

    fn resume_exact(
        &mut self,
        metadata: SourceMetadata,
        at: Timestamp,
    ) -> Result<RegisteredSource, RegistryError> {
        if matches!(
            &self.composition,
            AuthorityComposition::Durable(durability) if durability.recovered_unclean()
        ) {
            return Err(RegistryError::UncleanAuthorityPredecessor);
        }
        let _trusted_operation_time = self.clock.observe()?;
        if !metadata.is_effective_at(at) {
            return Err(RegistryError::MetadataNotEffective);
        }
        let history = self
            .history
            .get(metadata.source_id())
            .ok_or(RegistryError::UnknownSource)?;
        let latest = history
            .latest_revision_evidence
            .as_ref()
            .ok_or(RegistryError::RevisionEvidenceUnavailable)?;
        if latest.metadata_revision() != metadata.revision() {
            return Err(RegistryError::RevisionNotLatest);
        }
        if latest != metadata.revision_evidence() {
            return Err(RegistryError::RevisionEvidenceMismatch);
        }
        let epoch = history
            .last_epoch
            .checked_add(1)
            .ok_or(RegistryError::EpochExhausted)?;
        let generation_high_water = history.generation_high_water;
        let used_revisions = history.used_revisions.clone();
        let resolved_budget = resolve_provider_budget_policy(
            &metadata,
            self.authorization_subject_resolver.as_ref(),
        )?;
        let mut candidate_history = self.history.clone();
        candidate_history
            .get_mut(metadata.source_id())
            .ok_or(RegistryError::InvalidAuthorityState)?
            .last_epoch = epoch;
        let policies = resolved_budget.as_ref().map_or_else(
            || self.budgets.policies(),
            |policy| self.budgets.policies_with(policy.persisted()),
        );
        let candidate_state = authority_state_from_history(&candidate_history, policies)?;
        let budget = match resolved_budget {
            Some(policy) => Some(match &self.composition {
                AuthorityComposition::Durable(_) => self
                    .budgets
                    .register_durable(policy, &candidate_state)
                    .map_err(map_budget_pool_error)?,
                AuthorityComposition::InMemoryDiagnostic
                | AuthorityComposition::InMemoryExtractionInspection => self
                    .budgets
                    .register(policy)
                    .map_err(map_budget_pool_error)?,
            }),
            None => {
                self.persist_registry_candidate(candidate_state)?;
                None
            }
        };
        let source_id = metadata.source_id().clone();
        let revision = metadata.revision().clone();
        self.history = candidate_history;
        let registration_lease = Arc::new(RegistrationLeaseState::new());
        self.entries.insert(
            source_id.clone(),
            RegistryEntry {
                metadata,
                epoch,
                revoked: false,
                registration_lease: Arc::clone(&registration_lease),
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
            lease: registration_lease,
        })
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
        if self
            .history
            .get(metadata.source_id())
            .is_some_and(|history| history.revoked)
        {
            return Err(RegistryError::SourceRevoked);
        }
        if matches!(
            &self.composition,
            AuthorityComposition::Durable(durability) if durability.recovered_unclean()
        ) {
            return Err(RegistryError::UncleanAuthorityPredecessor);
        }
        let _trusted_operation_time = self.clock.observe()?;
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
        let resolved_budget = resolve_provider_budget_policy(
            &metadata,
            self.authorization_subject_resolver.as_ref(),
        )?;
        let mut candidate_history = self.history.clone();
        candidate_history.insert(
            source_id.clone(),
            SourceAuthorityHistory {
                used_revisions: used_revisions.clone(),
                latest_revision_evidence: Some(metadata.revision_evidence().clone()),
                revoked: false,
                last_epoch: epoch,
                generation_high_water,
            },
        );
        let policies = resolved_budget.as_ref().map_or_else(
            || self.budgets.policies(),
            |policy| self.budgets.policies_with(policy.persisted()),
        );
        let candidate_state = authority_state_from_history(&candidate_history, policies)?;
        let budget = match resolved_budget {
            Some(policy) => Some(match &self.composition {
                AuthorityComposition::Durable(_) => self
                    .budgets
                    .register_durable(policy, &candidate_state)
                    .map_err(map_budget_pool_error)?,
                AuthorityComposition::InMemoryDiagnostic
                | AuthorityComposition::InMemoryExtractionInspection => self
                    .budgets
                    .register(policy)
                    .map_err(map_budget_pool_error)?,
            }),
            None => {
                self.persist_registry_candidate(candidate_state)?;
                None
            }
        };
        self.history = candidate_history;
        let registration_lease = Arc::new(RegistrationLeaseState::new());
        self.entries.insert(
            source_id.clone(),
            RegistryEntry {
                metadata,
                epoch,
                revoked: false,
                registration_lease: Arc::clone(&registration_lease),
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
            lease: registration_lease,
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
        let _trusted_operation_time = self.clock.observe()?;
        if metadata.source_id() != &registered.source_id {
            return Err(RegistryError::HandleTransplanted);
        }
        if !metadata.is_effective_at(at) {
            return Err(RegistryError::MetadataNotEffective);
        }
        let entry = self
            .entries
            .get(&registered.source_id)
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
        let resolved_budget = resolve_provider_budget_policy(
            &metadata,
            self.authorization_subject_resolver.as_ref(),
        )?;
        let mut candidate_history = self.history.clone();
        let history = candidate_history
            .get_mut(&registered.source_id)
            .ok_or(RegistryError::InvalidAuthorityState)?;
        history.used_revisions.push(revision.clone());
        history.latest_revision_evidence = Some(metadata.revision_evidence().clone());
        history.last_epoch = epoch;
        let policies = resolved_budget.as_ref().map_or_else(
            || self.budgets.policies(),
            |policy| self.budgets.policies_with(policy.persisted()),
        );
        let candidate_state = authority_state_from_history(&candidate_history, policies)?;
        let budget = match resolved_budget {
            Some(policy) => Some(match &self.composition {
                AuthorityComposition::Durable(_) => self
                    .budgets
                    .register_durable(policy, &candidate_state)
                    .map_err(map_budget_pool_error)?,
                AuthorityComposition::InMemoryDiagnostic
                | AuthorityComposition::InMemoryExtractionInspection => self
                    .budgets
                    .register(policy)
                    .map_err(map_budget_pool_error)?,
            }),
            None => {
                self.persist_registry_candidate(candidate_state)?;
                None
            }
        };
        let entry = self
            .entries
            .get_mut(&registered.source_id)
            .ok_or(RegistryError::UnknownSource)?;
        entry.registration_lease.invalidate();
        let registration_lease = Arc::new(RegistrationLeaseState::new());
        entry.metadata = metadata;
        entry.health_authority = None;
        entry.universe_attestation = None;
        entry.used_revisions.push(revision.clone());
        entry.epoch = epoch;
        entry.registration_lease = Arc::clone(&registration_lease);
        if let Some(active) = entry.active.take() {
            active.lease.invalidate();
            active.capture.mark_incomplete();
        }
        self.history = candidate_history;
        Ok(RegisteredSource {
            registry_id: self.instance_id,
            source_id: registered.source_id.clone(),
            revision,
            epoch,
            budget,
            lease: registration_lease,
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
        let next_health_epoch = match entry.active.as_ref() {
            Some(active) => match active.lease.next_health_epoch() {
                Some(epoch) => Some(epoch),
                None => {
                    entry.terminally_invalidate_health_authority();
                    return Err(RegistryError::HealthEpochExhausted);
                }
            },
            None => None,
        };
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
        if matches!(
            &self.composition,
            AuthorityComposition::InMemoryExtractionInspection
        ) {
            return Err(RegistryError::LiveAuthorityUnavailableForExtractionRegistry);
        }
        self.validate_registered(registered, at)?;
        if matches!(
            &self.composition,
            AuthorityComposition::Durable(durability) if durability.recovered_unclean()
        ) {
            return Err(RegistryError::UncleanAuthorityPredecessor);
        }
        let started_at = self.clock.observe()?;
        let entry = self
            .entries
            .get(&registered.source_id)
            .ok_or(RegistryError::UnknownSource)?;
        if entry
            .active
            .as_ref()
            .is_some_and(|active| active.lease.is_terminal())
        {
            return Err(RegistryError::HealthEpochExhausted);
        }
        if entry
            .generation_high_water
            .is_some_and(|previous| generation <= previous)
        {
            return Err(RegistryError::GenerationNotAdvanced);
        }
        let mut candidate_history = self.history.clone();
        candidate_history
            .get_mut(&registered.source_id)
            .ok_or(RegistryError::InvalidAuthorityState)?
            .generation_high_water = Some(generation);
        let candidate_state =
            authority_state_from_history(&candidate_history, self.budgets.policies())?;
        self.persist_registry_candidate_at(candidate_state, started_at.wall())?;
        let entry = self
            .entries
            .get_mut(&registered.source_id)
            .ok_or(RegistryError::UnknownSource)?;
        if let Some(active) = entry.active.take() {
            active.lease.invalidate();
            active.capture.mark_incomplete();
        }
        let lease = Arc::new(SessionLeaseState {
            current: AtomicBool::new(true),
            terminal: AtomicBool::new(false),
            live_qualified: AtomicBool::new(false),
            health_epoch: AtomicU64::new(0),
            minimum_valid_health_epoch: AtomicU64::new(0),
            valid_from_nanos: AtomicI64::new(i64::MAX),
            valid_until_nanos: AtomicI64::new(i64::MIN),
            last_health_observed_nanos: AtomicI64::new(i64::MIN),
            frame_ordinal: AtomicU64::new(0),
            continuity: self.clock.continuity().clone(),
            started_at,
        });
        let capture = crate::CaptureGenerationLease::new_generation(
            self.clock.continuity().clone(),
            started_at,
        );
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
        self.history = candidate_history;
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
            started_at,
        })
    }

    /// Starts the next durable connection generation without exposing a caller-owned counter.
    ///
    /// The registry derives generation one for a never-started source or atomically advances the
    /// persisted high-water, then delegates to the ordinary begin-session transaction. A failed
    /// persistence step publishes neither the new high-water nor current session authority.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::ConnectionGenerationExhausted`] at `u64::MAX`; otherwise returns
    /// the same validation and persistence failures as [`Self::begin_session`].
    pub fn begin_next_session(
        &mut self,
        registered: &RegisteredSource,
        session_id: SessionId,
        at: Timestamp,
    ) -> Result<CurrentSourceSession, RegistryError> {
        let entry = self.validate_registered_structure(registered)?;
        let generation = match entry.generation_high_water {
            Some(previous) => previous
                .checked_next()
                .map_err(|_error| RegistryError::ConnectionGenerationExhausted)?,
            None => ConnectionGeneration::new(1)
                .map_err(|_error| RegistryError::ConnectionGenerationExhausted)?,
        };
        self.begin_session(registered, session_id, generation, at)
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
            clock: Arc::clone(&self.clock),
            not_sync: PhantomData,
        })
    }

    /// Mints the sole one-use live-adapter authority for this exact current generation.
    ///
    /// Capture initialization must already be healthy. The returned capability internally owns
    /// the session lease, capture generation, sole frame factory, registry clock, and exact shared
    /// provider-budget allocation; callers cannot assemble or substitute those components.
    ///
    /// # Errors
    ///
    /// Rejects stale/transplanted sessions, unhealthy capture, and any prior frame-factory or
    /// live-generation issuance for this generation.
    pub fn take_live_source_generation(
        &mut self,
        session: &CurrentSourceSession,
    ) -> Result<LiveSourceGeneration, RegistryError> {
        self.validate_session_structure(session)?;
        let entry = self
            .entries
            .get_mut(session.source_id())
            .ok_or(RegistryError::UnknownSource)?;
        let active = entry
            .active
            .as_mut()
            .ok_or(RegistryError::SessionNotCurrent)?;
        if !active.capture_issuer_taken || !active.capture.is_healthy() {
            return Err(RegistryError::CaptureNotHealthy);
        }
        if active.raw_frame_factory_taken {
            return Err(RegistryError::RawFrameFactoryAlreadyTaken);
        }
        active.raw_frame_factory_taken = true;
        Ok(LiveSourceGeneration {
            binding: session.binding.clone(),
            lease: Arc::clone(&session.lease),
            capture: session.capture.clone(),
            frames: RawFrameFactory {
                binding: session.binding.clone(),
                lease: Arc::clone(&session.lease),
                clock: Arc::clone(&self.clock),
                not_sync: PhantomData,
            },
            budget: session.budget.clone(),
            budget_witness: session.budget.clone(),
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
        _at: Timestamp,
    ) -> Result<(), RegistryError> {
        self.validate_registered_structure(registered)?;
        let entry = self
            .entries
            .get(&registered.source_id)
            .ok_or(RegistryError::UnknownSource)?;
        self.history
            .get(&registered.source_id)
            .ok_or(RegistryError::InvalidAuthorityState)?;
        let revoked_epoch = entry.epoch.checked_add(1).unwrap_or(entry.epoch);
        let mut candidate_history = self.history.clone();
        let history = candidate_history
            .get_mut(&registered.source_id)
            .ok_or(RegistryError::InvalidAuthorityState)?;
        history.last_epoch = revoked_epoch;
        history.latest_revision_evidence = None;
        history.revoked = true;
        let candidate_state =
            authority_state_from_history(&candidate_history, self.budgets.policies())?;
        let persistence = self.persist_registry_candidate(candidate_state);
        let entry = self
            .entries
            .get_mut(&registered.source_id)
            .ok_or(RegistryError::UnknownSource)?;
        self.history = candidate_history;
        entry.epoch = revoked_epoch;
        entry.revoked = true;
        entry.registration_lease.invalidate();
        if let Some(active) = entry.active.take() {
            active.lease.invalidate();
            active.capture.mark_incomplete();
        }
        entry.health_authority = None;
        persistence
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

fn authority_state_from_history(
    history: &BTreeMap<SourceId, SourceAuthorityHistory>,
    policies: Vec<PersistedProviderBudgetPolicy>,
) -> Result<RegistryAuthorityState, RegistryError> {
    let mut sources = Vec::new();
    sources
        .try_reserve(history.len())
        .map_err(|_| RegistryError::AuthorityStateCapacity)?;
    for (source_id, source_history) in history {
        sources.push(PersistedSourceAuthority {
            source_id: source_id.clone(),
            used_revisions: BoundedVec::try_new(source_history.used_revisions.clone())
                .map_err(|_| RegistryError::RevisionHistoryExhausted)?,
            latest_revision_evidence: source_history.latest_revision_evidence.clone(),
            revoked: source_history.revoked,
            last_epoch: source_history.last_epoch,
            generation_high_water: source_history.generation_high_water,
        });
    }
    RegistryAuthorityState::try_new(sources, policies)
}

fn resolve_provider_budget_policy(
    metadata: &SourceMetadata,
    resolver: &dyn crate::AuthorizationSubjectResolver,
) -> Result<Option<ResolvedProviderBudgetPolicy>, RegistryError> {
    let Some(policy) = metadata.budget_policy() else {
        return Ok(None);
    };
    let crate::NetworkAccessPolicy::Allowlisted(endpoint_policy) = metadata.network_policy() else {
        return Err(RegistryError::InvalidAuthorityState);
    };
    ResolvedProviderBudgetPolicy::try_new(
        policy.clone(),
        endpoint_policy.clone(),
        metadata.authorization().clone(),
        resolver,
    )
    .map(Some)
    .map_err(map_budget_resolution_error)
}

fn map_budget_resolution_error(error: BudgetPolicyResolutionError) -> RegistryError {
    match error {
        BudgetPolicyResolutionError::InvalidPolicy => RegistryError::InvalidAuthorityState,
        BudgetPolicyResolutionError::SubjectResolution(_) => {
            RegistryError::AuthorizationSubjectResolution
        }
        BudgetPolicyResolutionError::SubjectMismatch => RegistryError::AuthorizationSubjectMismatch,
    }
}

fn map_budget_pool_error(error: crate::BudgetPoolError) -> RegistryError {
    match error {
        crate::BudgetPoolError::Persistence => RegistryError::AuthorityPersistence,
        crate::BudgetPoolError::ConflictingPolicy
        | crate::BudgetPoolError::BridgingIdentity
        | crate::BudgetPoolError::ClockUnavailable
        | crate::BudgetPoolError::CoordinatorPoisoned
        | crate::BudgetPoolError::CoordinatorCapacity
        | crate::BudgetPoolError::CoordinatorAllocation
        | crate::BudgetPoolError::CanonicalAuthorityCapacity
        | crate::BudgetPoolError::CanonicalAuthorityAllocation
        | crate::BudgetPoolError::CoordinatorCorrupt
        | crate::BudgetPoolError::ConflictingDurability => RegistryError::BudgetCoordinator,
    }
}

fn history_from_state(
    state: &RegistryAuthorityState,
) -> BTreeMap<SourceId, SourceAuthorityHistory> {
    state
        .sources
        .as_slice()
        .iter()
        .map(|source| {
            (
                source.source_id.clone(),
                SourceAuthorityHistory {
                    used_revisions: source.used_revisions.as_slice().to_vec(),
                    latest_revision_evidence: source.latest_revision_evidence.clone(),
                    revoked: source.revoked,
                    last_epoch: source.last_epoch,
                    generation_high_water: source.generation_high_water,
                },
            )
        })
        .collect()
}

fn same_persisted_policy_set(
    expected: &[PersistedProviderBudgetPolicy],
    observed: &[PersistedProviderBudgetPolicy],
) -> bool {
    expected.len() == observed.len()
        && observed.iter().enumerate().all(|(index, policy)| {
            !observed[index.saturating_add(1)..].contains(policy) && expected.contains(policy)
        })
}

fn map_authority_persistence_error(error: AuthorityPersistenceError) -> RegistryError {
    match error {
        AuthorityPersistenceError::WallRollback => RegistryError::DurableWallRollback,
        AuthorityPersistenceError::FutureState => RegistryError::DurableFutureState,
        AuthorityPersistenceError::GenerationExhausted => {
            RegistryError::DurableRunGenerationExhausted
        }
        AuthorityPersistenceError::Store
        | AuthorityPersistenceError::InvalidState
        | AuthorityPersistenceError::StateTooLarge
        | AuthorityPersistenceError::SessionUnavailable => RegistryError::AuthorityPersistence,
    }
}
