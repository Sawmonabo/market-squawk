/// Sole non-clone supervisor capability that can bind audit health to current authority.
#[derive(Debug)]
pub struct CurrentHealthReporter {
    binding: FrameSessionBinding,
    lease: Arc<SessionLeaseState>,
    freshness: crate::FreshnessPolicy,
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
        Ok(CurrentHealthUpdate {
            snapshot,
            binding: self.binding.clone(),
            lease: Arc::clone(&self.lease),
        })
    }
}

/// Owned, non-serializable current-health update consumable only by the registry.
#[derive(Debug)]
pub struct CurrentHealthUpdate {
    snapshot: crate::SourceHealthSnapshot,
    binding: FrameSessionBinding,
    lease: Arc<SessionLeaseState>,
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

    pub(crate) const fn frame_binding(&self) -> &FrameSessionBinding {
        &self.binding
    }

    /// Returns the same registry-coordinated budget shared by this provider/account scope.
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
        if self.binding.shares_allocation_with(frame.binding()) {
            Ok(crate::ValidatedRawMarketFrame::new(frame))
        } else {
            Err(RegistryError::HandleTransplanted)
        }
    }
}

/// Once-issued, non-serializable raw-frame construction capability for one exact generation.
#[derive(Debug)]
pub struct RawFrameFactory {
    binding: FrameSessionBinding,
    lease: Arc<SessionLeaseState>,
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
        received_at: Timestamp,
        transport: crate::TransportFrameKind,
        payload: bytes::Bytes,
    ) -> Result<crate::RawMarketFrame, crate::SourceError> {
        crate::RawMarketFrame::try_from_parts(
            self.binding.clone(),
            self.lease.next_frame_id()?,
            received_at,
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
}

impl<'a> ValidatedCurrentSourceAuthority<'a> {
    /// Returns current registry-owned metadata.
    pub const fn metadata(&self) -> &'a SourceMetadata {
        self.validated.metadata
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
        self.validated.session.validate_current_lease()?;
        if !self
            .validated
            .session
            .lease
            .validate_health_epoch(self.health.epoch, self.health.observed_at)
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
            health_epoch: self.health.epoch,
            lease: Arc::clone(&self.validated.session.lease),
            capture: self.validated.session.capture.clone(),
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
    ) -> Result<CurrentDecodedProviderBatch, RegistryError> {
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
            || receipt.frame_id() != batch.evidence().frame_id()
            || receipt.payload_digest() != batch.evidence().payload_digest()
            || !receipt.lease().is_healthy()
            || !receipt
                .lease()
                .shares_allocation_with(&self.validated.session.capture)
        {
            return Err(RegistryError::CaptureReceiptMismatch);
        }
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
        let retained_bytes = batch
            .retained_bytes()
            .map_err(|_| RegistryError::RetainedSizeOverflow)?;
        let first = batch
            .observations()
            .first()
            .ok_or(RegistryError::DecoderProfileMismatch)?;
        let key = CurrentBatchKey {
            venue: first.venue().clone(),
            instrument: first.instrument(),
        };
        let observations = batch
            .into_observations()
            .into_iter()
            .zip(observation_authorities)
            .map(|(observation, scope)| scope.into_current_observation(observation))
            .collect::<Result<Vec<_>, RegistryError>>()?
            .into_boxed_slice();
        let policy_allocations = observations.iter().try_fold(0_usize, |total, observation| {
            total
                .checked_add(observation.policy.deep_allocation_charge()?)
                .ok_or(RegistryError::RetainedSizeOverflow)
        })?;
        let authority_allocation = current_authority_shared_allocation_charge()?;
        let structural = observations
            .len()
            .checked_mul(std::mem::size_of::<CurrentProviderObservation>())
            .and_then(|bytes| retained_bytes.checked_add(bytes))
            .and_then(|bytes| bytes.checked_add(policy_allocations))
            .and_then(|bytes| bytes.checked_add(authority_allocation))
            .ok_or(RegistryError::RetainedSizeOverflow)?;
        let authority = observations
            .first()
            .ok_or(RegistryError::DecoderProfileMismatch)?
            .authority
            .clone();
        Ok(CurrentDecodedProviderBatch {
            key,
            retained_bytes: structural,
            authority,
            observations,
        })
    }
}

/// Opaque current authority for one exact live coverage tuple.
#[derive(Debug)]
pub struct ValidatedLiveScope {
    binding: FrameSessionBinding,
    venue: VenueId,
    instrument: InstrumentId,
    rule: crate::LiveCoverageRule,
    provider_product: market_squawk_domain::ProviderProduct,
    provider_channel: market_squawk_domain::ProviderChannel,
    authorization: crate::AuthorizationHealth,
    runtime_coverage: crate::CoverageHealth,
    protocol: crate::SourceProtocolProfile,
    freshness: crate::FreshnessPolicy,
    quality_ceiling: market_squawk_domain::DataQuality,
    static_authorization: crate::AuthorizationGrant,
    coverage: CurrentCoveragePolicy,
    valid_until: Timestamp,
    health_epoch: u64,
    lease: Arc<SessionLeaseState>,
    capture: crate::CaptureGenerationLease,
    universe_evidence: Option<ExactPayloadEvidence>,
}

impl ValidatedLiveScope {
    /// Revalidates the allocation/health epoch and inclusive deadline in O(1).
    ///
    /// # Errors
    ///
    /// Fails after health/subscription change, session/revision rollover, or deadline expiry.
    pub fn validate_at(&self, at: Timestamp) -> Result<(), RegistryError> {
        if at <= self.valid_until
            && self.lease.validate_health_epoch(self.health_epoch, at)
            && self.capture.is_healthy()
        {
            Ok(())
        } else {
            Err(RegistryError::HealthNotQualified)
        }
    }

    /// Returns the exact session binding allocation retained by this scope.
    pub const fn binding(&self) -> &FrameSessionBinding {
        &self.binding
    }

    /// Returns the exact authorized venue.
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    /// Returns the exact authorized internal instrument.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    /// Returns the exact metadata-bound event/depth/snapshot rule.
    pub const fn rule(&self) -> &crate::LiveCoverageRule {
        &self.rule
    }

    /// Returns the exact runtime-acknowledged provider product.
    pub const fn provider_product(&self) -> &market_squawk_domain::ProviderProduct {
        &self.provider_product
    }

    /// Returns the exact runtime-acknowledged provider channel.
    pub const fn provider_channel(&self) -> &market_squawk_domain::ProviderChannel {
        &self.provider_channel
    }

    /// Returns exact runtime authorization evidence and deadline.
    pub const fn authorization(&self) -> &crate::AuthorizationHealth {
        &self.authorization
    }

    /// Returns exact runtime subscription evidence and deadline.
    pub const fn runtime_coverage(&self) -> &crate::CoverageHealth {
        &self.runtime_coverage
    }

    /// Returns metadata-bound decoder/sequence/checksum/numeric/timestamp profiles.
    pub const fn protocol_profile(&self) -> &crate::SourceProtocolProfile {
        &self.protocol
    }

    /// Returns all five metadata-bound freshness limits.
    pub const fn freshness_policy(&self) -> crate::FreshnessPolicy {
        self.freshness
    }

    /// Returns the inclusive earliest effective deadline.
    pub const fn valid_until(&self) -> Timestamp {
        self.valid_until
    }

    /// Returns registry-owned universe evidence when `all_declared` membership was used.
    pub const fn universe_evidence(&self) -> Option<&ExactPayloadEvidence> {
        self.universe_evidence.as_ref()
    }

    fn matches_snapshot_evidence(&self, observed: &crate::ProviderSnapshotEvidence) -> bool {
        match (self.rule.snapshot_applicability(), observed) {
            (
                market_squawk_domain::SnapshotApplicability::Required,
                crate::ProviderSnapshotEvidence::InitializingSnapshot { .. }
                | crate::ProviderSnapshotEvidence::Delta { .. },
            ) => true,
            (
                market_squawk_domain::SnapshotApplicability::NotApplicable { metadata_rule },
                crate::ProviderSnapshotEvidence::NotApplicable(observed_rule),
            ) => metadata_rule == observed_rule,
            _ => false,
        }
    }

    fn into_current_observation(
        self,
        observation: crate::ProviderNormalizedObservation,
    ) -> Result<CurrentProviderObservation, RegistryError> {
        let crate::SourceProtocolProfile::Live(protocol) = self.protocol else {
            return Err(RegistryError::DecoderProfileMismatch);
        };
        let stream_key = CurrentStreamKey {
            source_id: self.binding.source_id().clone(),
            venue: self.venue.clone(),
            instrument: self.instrument,
            provider_product: self.provider_product.clone(),
            provider_channel: self.provider_channel.clone(),
        };
        let authority = CurrentSourceAuthorityLease {
            binding: self.binding,
            health_epoch: self.health_epoch,
            valid_until: self.valid_until,
            lease: self.lease,
            capture: self.capture,
        };
        Ok(CurrentProviderObservation {
            observation,
            policy: CurrentLivePolicy {
                stream_key,
                quality_ceiling: self.quality_ceiling,
                static_authorization: self.static_authorization,
                runtime_authorization: self.authorization,
                coverage: self.coverage,
                runtime_coverage: self.runtime_coverage,
                rule: self.rule,
                protocol: *protocol,
                freshness: self.freshness,
                valid_until: self.valid_until,
                universe_evidence: self.universe_evidence,
            },
            authority,
        })
    }
}
