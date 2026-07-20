/// Sole non-clone supervisor capability that can bind audit health to current authority.
#[derive(Debug)]
pub struct CurrentHealthReporter {
    binding: FrameSessionBinding,
    lease: Arc<SessionLeaseState>,
    freshness: crate::FreshnessPolicy,
    budget: Option<SharedProviderBudget>,
    clock: Arc<SealedRegistryClock>,
    session_started_at: TrustedRegistryTime,
    not_sync: PhantomData<Cell<()>>,
}

impl CurrentHealthReporter {
    /// Binds a locally constructed snapshot to this exact session allocation and metadata policy.
    ///
    /// Deserialized audit DTOs have no process-local binding and cannot be reported.
    ///
    /// # Errors
    ///
    /// Rejects stale sessions, reconstructed/transplanted snapshots, and policy mismatch.
    pub fn report(
        &mut self,
        snapshot: crate::SourceHealthSnapshot,
    ) -> Result<CurrentHealthUpdate, RegistryError> {
        if !self.lease.is_current()
            || !snapshot.uses_freshness_policy(self.freshness)
            || !snapshot
                .authority_binding()
                .is_some_and(|binding| binding.shares_allocation_with(&self.binding))
        {
            return Err(RegistryError::HealthBindingMismatch);
        }
        let trusted_reported_at = self.clock.observe()?;
        if trusted_reported_at.monotonic() < self.session_started_at.monotonic() {
            return Err(RegistryError::TrustedClockRegression);
        }
        if snapshot.observed_at() < self.session_started_at.wall()
            || snapshot.observed_at() > trusted_reported_at.wall()
        {
            return Err(RegistryError::InvalidHealthTemporalOrder);
        }
        Ok(CurrentHealthUpdate {
            snapshot,
            binding: self.binding.clone(),
            lease: Arc::clone(&self.lease),
            budget: CurrentBudgetAuthority::observe(self.budget.as_ref()),
            trusted_reported_at,
        })
    }
}

/// Owned, non-serializable current-health update consumable only by the registry.
#[derive(Debug)]
pub struct CurrentHealthUpdate {
    snapshot: crate::SourceHealthSnapshot,
    binding: FrameSessionBinding,
    lease: Arc<SessionLeaseState>,
    budget: CurrentBudgetAuthority,
    trusted_reported_at: TrustedRegistryTime,
}

/// Opaque, non-serializable proof that one metadata revision was registered by one registry.
#[derive(Debug)]
pub struct RegisteredSource {
    registry_id: u64,
    source_id: SourceId,
    revision: MetadataRevision,
    epoch: u64,
    budget: Option<SharedProviderBudget>,
}

impl RegisteredSource {
    /// Returns the registered source identity for diagnostics and registry lookups.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact registered metadata revision.
    pub const fn revision(&self) -> &MetadataRevision {
        &self.revision
    }

    /// Returns the registry-coordinated shared provider budget when networking is enabled.
    pub const fn budget(&self) -> Option<&SharedProviderBudget> {
        self.budget.as_ref()
    }
}

/// Opaque, non-serializable handle for one exact current connection session.
#[derive(Debug)]
pub struct CurrentSourceSession {
    registry_id: u64,
    epoch: u64,
    binding: FrameSessionBinding,
    budget: Option<SharedProviderBudget>,
    lease: Arc<SessionLeaseState>,
    capture: crate::CaptureGenerationLease,
    started_at: TrustedRegistryTime,
}

impl CurrentSourceSession {
    /// Returns the source identity bound to the session.
    pub fn source_id(&self) -> &SourceId {
        self.binding.source_id()
    }

    /// Returns the exact metadata revision bound to the session.
    pub fn revision(&self) -> &MetadataRevision {
        self.binding.metadata_revision()
    }

    /// Returns the source-defined session identity.
    pub fn session_id(&self) -> &SessionId {
        self.binding.session_id()
    }

    /// Returns the nonzero connection generation.
    pub fn generation(&self) -> ConnectionGeneration {
        self.binding.connection_generation()
    }

    /// Returns the registry-sealed session start timestamp.
    pub const fn started_at(&self) -> Timestamp {
        self.started_at.wall()
    }

    pub(crate) const fn frame_binding(&self) -> &FrameSessionBinding {
        &self.binding
    }

    /// Returns the registry-coordinated budget for this canonical collision group.
    pub const fn budget(&self) -> Option<&SharedProviderBudget> {
        self.budget.as_ref()
    }

    /// Performs a lock-free fail-closed currentness check suitable for the live path.
    ///
    /// # Errors
    ///
    /// Fails after rollover, end, metadata replacement, or revocation.
    pub fn validate_current_lease(&self) -> Result<(), RegistryError> {
        if self.lease.is_current() {
            Ok(())
        } else {
            Err(RegistryError::SessionNotCurrent)
        }
    }

    /// Proves that a raw frame retains the exact registry-issued binding allocation.
    ///
    /// Deserialized/replayed frames are value-equivalent data but do not share this process-local
    /// allocation and therefore fail this authority check.
    ///
    /// # Errors
    ///
    /// Rejects stale leases and frames from another/reconstructed session.
    pub fn validate_live_frame<'a>(
        &self,
        frame: &'a crate::RawMarketFrame,
    ) -> Result<crate::ValidatedRawMarketFrame<'a>, RegistryError> {
        self.validate_current_lease()?;
        if !self.binding.shares_allocation_with(frame.binding()) {
            return Err(RegistryError::HandleTransplanted);
        }
        let receipt = frame
            .trusted_receipt()
            .ok_or(RegistryError::TrustedReceiptContinuityMismatch)?;
        self.lease.validate_receipt(receipt)?;
        Ok(crate::ValidatedRawMarketFrame::new(frame, receipt))
    }
}

/// Once-issued, non-serializable raw-frame construction capability for one exact generation.
#[derive(Debug)]
pub struct RawFrameFactory {
    binding: FrameSessionBinding,
    lease: Arc<SessionLeaseState>,
    clock: Arc<SealedRegistryClock>,
    not_sync: PhantomData<Cell<()>>,
}

impl RawFrameFactory {
    /// Constructs one bounded exact transport frame under this generation's identity.
    ///
    /// # Errors
    ///
    /// Fails closed after session invalidation or frame-ordinal exhaustion and rejects payloads
    /// larger than [`crate::MAX_RAW_FRAME_BYTES`].
    pub fn try_frame(
        &mut self,
        transport: crate::TransportFrameKind,
        payload: bytes::Bytes,
    ) -> Result<crate::RawMarketFrame, crate::SourceError> {
        let receipt = self.clock.observe_receipt().map_err(|error| match error {
            RegistryError::TrustedClockUnavailable => crate::SourceError::TrustedTimeUnavailable,
            _ => crate::SourceError::TrustedTimeDiscontinuity,
        })?;
        self.lease
            .validate_receipt(&receipt)
            .map_err(|_error| crate::SourceError::TrustedTimeDiscontinuity)?;
        crate::RawMarketFrame::try_from_parts(
            self.binding.clone(),
            self.lease.next_frame_id()?,
            receipt,
            transport,
            payload,
        )
    }
}

/// Registry-borrowing current-session authority view.
#[derive(Debug)]
pub struct ValidatedSourceSession<'a> {
    metadata: &'a SourceMetadata,
    session: &'a CurrentSourceSession,
}

impl<'a> ValidatedSourceSession<'a> {
    /// Returns registry-owned current metadata.
    pub const fn metadata(&self) -> &'a SourceMetadata {
        self.metadata
    }

    /// Returns the exact current-session handle that was revalidated.
    pub const fn session(&self) -> &'a CurrentSourceSession {
        self.session
    }
}

/// Opaque registry-owned current health, authorization, and subscription authority.
#[derive(Debug)]
pub struct ValidatedCurrentSourceAuthority<'a> {
    validated: ValidatedSourceSession<'a>,
    health: &'a CurrentHealthAuthority,
    attestation: Option<&'a InstrumentUniverseAttestation>,
    validated_at: TrustedRegistryTime,
    clock: &'a Arc<SealedRegistryClock>,
}

impl<'a> ValidatedCurrentSourceAuthority<'a> {
    /// Returns current registry-owned metadata.
    pub const fn metadata(&self) -> &'a SourceMetadata {
        self.validated.metadata
    }

    /// Upgrades exact session/capture-validated data through current health and coverage authority.
    ///
    /// # Errors
    ///
    /// Rejects stale session, capture, health, authorization, coverage, protocol, or observation
    /// evidence. Non-data decoder dispositions cannot be passed to this method.
    pub fn validate_data_outcome_owned(
        &self,
        captured: CapturedDecodedProviderBatch,
    ) -> Result<CurrentDecodedProviderBatches, RegistryError> {
        let (batch, receipt) = captured.into_parts();
        self.validate_decoded_batch_owned(batch, receipt)
    }

    /// Issues an owned opaque source lease for pre-feed generation registration.
    ///
    /// The returned value retains the exact process-local session allocation, current health
    /// epoch, capture generation, and inclusive deadline. It is intentionally non-serializable;
    /// replayed or reconstructed identity data cannot mint it.
    ///
    /// # Errors
    ///
    /// Rejects a stale session, changed health epoch, unhealthy capture generation, or expired
    /// current-health deadline.
    pub fn try_current_lease(&self) -> Result<CurrentSourceAuthorityLease, RegistryError> {
        let mint_at = self.clock.observe()?;
        self.validated.session.validate_current_lease()?;
        if mint_at.monotonic() < self.validated_at.monotonic() {
            return Err(RegistryError::TrustedClockRegression);
        }
        if mint_at.wall() < self.health.accepted_at.wall()
            || mint_at.wall() < self.health.valid_from
            || mint_at.wall() > self.health.valid_until
            || mint_at.monotonic() > self.health.valid_until_monotonic
            || !self
                .validated
                .session
                .lease
                .validate_health_epoch(self.health.epoch, mint_at.wall())
            || !self.health.budget.is_available()
            || !self.validated.session.capture.is_healthy()
        {
            return Err(RegistryError::HealthNotQualified);
        }
        let lease = CurrentSourceAuthorityLease {
            registry_id: self.validated.session.registry_id,
            binding: self.validated.session.binding.clone(),
            health_epoch: self.health.epoch,
            valid_from: self.health.valid_from,
            valid_until: self.health.valid_until,
            trusted_valid_from: self.health.accepted_at.wall(),
            trusted_valid_from_monotonic: self.health.accepted_at.monotonic(),
            valid_until_monotonic: self.health.valid_until_monotonic,
            lease: Arc::clone(&self.validated.session.lease),
            capture: self.validated.session.capture.clone(),
            budget: self.health.budget.clone(),
            clock: Arc::clone(self.clock),
        };
        lease.validate_at(mint_at.wall())?;
        Ok(lease)
    }

    /// Narrows current health authority to an exact venue/instrument/event/depth tuple.
    ///
    /// # Errors
    ///
    /// Rejects a stale lease, venue/instrument outside proven coverage, or an undeclared event and
    /// depth combination. Explicitly partial membership never becomes positive authority.
    pub fn validate_live_scope(
        &self,
        venue: &VenueId,
        instrument: InstrumentId,
        event_class: LiveEventClass,
        depth: Option<MarketDepth>,
    ) -> Result<ValidatedLiveScope, RegistryError> {
        let scope_validated_at = self.clock.observe()?;
        self.validated.session.validate_current_lease()?;
        if scope_validated_at.monotonic() < self.validated_at.monotonic() {
            return Err(RegistryError::TrustedClockRegression);
        }
        if !self
            .validated
            .session
            .lease
            .validate_health_epoch(self.health.epoch, scope_validated_at.wall())
            || !self.health.budget.is_available()
        {
            return Err(RegistryError::HealthNotQualified);
        }
        let coverage = self.validated.metadata.coverage();
        let instrument_proven = match coverage.instruments().membership(instrument) {
            crate::InstrumentCoverageMembership::Enumerated => true,
            crate::InstrumentCoverageMembership::EvidenceBackedUniverse => self
                .attestation
                .is_some_and(|attestation| attestation.contains(instrument)),
            crate::InstrumentCoverageMembership::PartialUnproven
            | crate::InstrumentCoverageMembership::Outside => false,
        };
        if !coverage.topology().contains_venue(venue) || !instrument_proven {
            return Err(RegistryError::LiveScopeNotCovered);
        }
        let live = coverage.live().ok_or(RegistryError::LiveScopeNotCovered)?;
        let rule = live
            .rule_for(event_class, depth)
            .ok_or(RegistryError::LiveScopeNotCovered)?;
        let valid_until = self
            .attestation
            .and_then(InstrumentUniverseAttestation::inclusive_deadline)
            .map_or(self.health.valid_until, |until| {
                until.min(self.health.valid_until)
            });
        let scope_deadline = scope_validated_at
            .checked_deadline(valid_until)?
            .map(|deadline| deadline.min(self.health.valid_until_monotonic));
        if scope_validated_at.wall() < self.health.accepted_at.wall()
            || scope_validated_at.wall() < self.health.valid_from
            || scope_validated_at.wall() > valid_until
            || scope_deadline.is_none()
        {
            return Err(RegistryError::HealthNotQualified);
        }
        let topology = self.validated.metadata.coverage().topology();
        let consolidation = if topology.is_single_venue() {
            CoverageConsolidation::SingleVenue
        } else if topology.is_consolidated() {
            CoverageConsolidation::Consolidated
        } else {
            CoverageConsolidation::Partial
        };
        let static_coverage = self.validated.metadata.coverage();
        let coverage = CurrentCoveragePolicy {
            source_id: self.validated.session.binding.source_id().clone(),
            venue: venue.clone(),
            provider_product: live.provider_product().clone(),
            provider_channel: live.provider_channel().clone(),
            event_class,
            depth,
            delay: static_coverage.delay(),
            consolidation,
            delivery: static_coverage.delivery(),
            evidence: static_coverage.evidence().clone(),
            effective_from: static_coverage.effective_interval().starts_at(),
            effective_until: static_coverage.inclusive_coverage_deadline(),
            metadata_revision: self.validated.session.binding.metadata_revision().clone(),
        };
        Ok(ValidatedLiveScope {
            registry_id: self.validated.session.registry_id,
            binding: self.validated.session.binding.clone(),
            venue: venue.clone(),
            instrument,
            rule: rule.clone(),
            provider_product: live.provider_product().clone(),
            provider_channel: live.provider_channel().clone(),
            authorization: self.health.authorization.clone(),
            runtime_coverage: self.health.coverage.clone(),
            protocol: self.validated.metadata.protocol_profile().clone(),
            freshness: self.validated.metadata.freshness_policy(),
            quality_ceiling: self.validated.metadata.quality_ceiling(),
            static_authorization: self.validated.metadata.authorization().clone(),
            coverage,
            valid_until,
            valid_from: self.health.valid_from,
            trusted_valid_from: self.health.accepted_at.wall(),
            trusted_valid_from_monotonic: self.health.accepted_at.monotonic(),
            valid_until_monotonic: scope_deadline.ok_or(RegistryError::HealthNotQualified)?,
            health_epoch: self.health.epoch,
            lease: Arc::clone(&self.validated.session.lease),
            capture: self.validated.session.capture.clone(),
            budget: self.health.budget.clone(),
            clock: Arc::clone(self.clock),
            universe_evidence: self.attestation.map(|value| value.evidence.clone()),
        })
    }

    /// Consumes a decoded provider batch into an owned, non-serializable shard-ingress envelope.
    ///
    /// # Errors
    ///
    /// Rejects binding/rule/profile/scope transplants and stale current health authority.
    pub fn validate_decoded_batch_owned(
        &self,
        batch: crate::DecodedProviderBatch,
        receipt: crate::CaptureAdmissionReceipt,
    ) -> Result<CurrentDecodedProviderBatches, RegistryError> {
        self.validated.session.validate_current_lease()?;
        if !self
            .validated
            .session
            .binding
            .shares_allocation_with(batch.evidence().binding())
        {
            return Err(RegistryError::HandleTransplanted);
        }
        if !receipt
            .binding()
            .shares_allocation_with(batch.evidence().binding())
            || receipt.received_at() != batch.evidence().received_at()
            || receipt.trusted_receipt() != batch.evidence().trusted_receipt()
            || receipt.frame_id() != batch.evidence().frame_id()
            || receipt.payload_digest() != batch.evidence().payload_digest()
            || !receipt.lease().is_healthy()
            || !receipt
                .lease()
                .shares_allocation_with(&self.validated.session.capture)
        {
            return Err(RegistryError::CaptureReceiptMismatch);
        }
        self.validated
            .session
            .lease
            .validate_receipt(receipt.trusted_receipt())?;
        let crate::SourceProtocolProfile::Live(protocol) =
            self.validated.metadata.protocol_profile()
        else {
            return Err(RegistryError::DecoderProfileMismatch);
        };
        if batch.evidence().decoder_rule() != protocol.decoder_rule() {
            return Err(RegistryError::DecoderProfileMismatch);
        }
        let mut observation_authorities = Vec::with_capacity(batch.observations().len());
        for observation in batch.observations() {
            validate_observation_profile(protocol, observation)?;
            let scope = self.validate_live_scope(
                observation.venue(),
                observation.instrument(),
                observation.event_class(),
                observation.depth(),
            )?;
            if !scope.matches_snapshot_evidence(observation.snapshot()) {
                return Err(RegistryError::DecoderProfileMismatch);
            }
            observation_authorities.push(scope);
        }
        let (decoder_evidence, provider_observations) = batch.into_parts();
        let frame_evidence = CurrentFrameEvidence::new(decoder_evidence);
        let observations = provider_observations
            .into_iter()
            .zip(observation_authorities)
            .map(|(observation, scope)| {
                scope.into_current_observation(observation, frame_evidence.clone())
            })
            .collect::<Result<Vec<_>, RegistryError>>()?;
        let mut positions: HashMap<CurrentBatchKey, usize> =
            HashMap::with_capacity(observations.len());
        let mut groups: Vec<(CurrentBatchKey, Vec<CurrentProviderObservation>)> = Vec::new();
        for observation in observations {
            let key = observation.key().clone();
            if let Some(index) = positions.get(&key).copied() {
                groups[index].1.push(observation);
            } else {
                let index = groups.len();
                positions.insert(key.clone(), index);
                groups.push((key, vec![observation]));
            }
        }
        let batches = groups
            .into_iter()
            .map(|(key, observations)| {
                let observation_unique_allocations =
                    observations.iter().try_fold(0_usize, |total, observation| {
                        let provider = observation
                            .observation
                            .dynamic_retained_bytes()
                            .map_err(|_| RegistryError::RetainedSizeOverflow)?;
                        total
                            .checked_add(observation.policy.deep_allocation_charge()?)
                            .and_then(|bytes| {
                                bytes.checked_add(observation.key.dynamic_retained_bytes())
                            })
                            .and_then(|bytes| bytes.checked_add(provider))
                            .ok_or(RegistryError::RetainedSizeOverflow)
                    })?;
                let frame_shared_allocation = observations
                    .first()
                    .ok_or(RegistryError::DecoderProfileMismatch)?
                    .frame_evidence
                    .shared_allocation_charge()?;
                let authority_shared_allocation = observations
                    .first()
                    .ok_or(RegistryError::DecoderProfileMismatch)?
                    .authority
                    .shared_allocation_charge()?;
                let retained_bytes = current_routed_batch_retained_bytes(
                    key.dynamic_retained_bytes(),
                    observations.len(),
                    observation_unique_allocations,
                    authority_shared_allocation,
                    frame_shared_allocation,
                )?;
                let observations = observations.into_boxed_slice();
                let authority = observations
                    .first()
                    .ok_or(RegistryError::DecoderProfileMismatch)?
                    .authority
                    .clone();
                Ok(CurrentDecodedProviderBatch {
                    key,
                    retained_bytes,
                    authority,
                    observations,
                })
            })
            .collect::<Result<Vec<_>, RegistryError>>()?
            .into_boxed_slice();
        Ok(CurrentDecodedProviderBatches { batches })
    }
}

include!("authority/live_scope.rs");
