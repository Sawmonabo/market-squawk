/// Opaque current authority for one exact live coverage tuple.
#[derive(Debug)]
pub struct ValidatedLiveScope {
    registry_id: u64,
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
    valid_from: Timestamp,
    trusted_valid_from: Timestamp,
    trusted_valid_from_monotonic: RegistryMonotonicInstant,
    valid_until_monotonic: RegistryMonotonicInstant,
    health_epoch: u64,
    lease: Arc<SessionLeaseState>,
    capture: crate::CaptureGenerationLease,
    budget: CurrentBudgetAuthority,
    clock: Arc<SealedRegistryClock>,
    universe_evidence: Option<ExactPayloadEvidence>,
}

impl ValidatedLiveScope {
    #[cfg(test)]
    pub(in crate::registry) fn queued_authority_for_test(&self) -> CurrentSourceAuthorityLease {
        CurrentSourceAuthorityLease {
            registry_id: self.registry_id,
            binding: self.binding.clone(),
            health_epoch: self.health_epoch,
            valid_from: self.valid_from,
            valid_until: self.valid_until,
            trusted_valid_from: self.trusted_valid_from,
            trusted_valid_from_monotonic: self.trusted_valid_from_monotonic,
            valid_until_monotonic: self.valid_until_monotonic,
            lease: Arc::clone(&self.lease),
            capture: self.capture.clone(),
            budget: self.budget.clone(),
            clock: Arc::clone(&self.clock),
        }
    }

    /// Revalidates the allocation/health epoch and inclusive deadline in O(1).
    ///
    /// `at` is the processor-owned wall-clock projection for the scoped event. A fresh sealed
    /// wall/monotonic observation is checked independently, preventing an old projection from
    /// extending authority across expiry or a wall-clock discontinuity.
    ///
    /// # Errors
    ///
    /// Fails after health/subscription change, session/revision rollover, or deadline expiry.
    pub fn validate_at(&self, at: Timestamp) -> Result<(), RegistryError> {
        let trusted = self.clock.observe()?;
        if trusted.monotonic() < self.trusted_valid_from_monotonic {
            return Err(RegistryError::TrustedClockRegression);
        }
        if trusted.wall() >= self.trusted_valid_from
            && trusted.wall() <= self.valid_until
            && trusted.monotonic() <= self.valid_until_monotonic
            && at >= self.valid_from
            && at <= self.valid_until
            && self.lease.validate_health_epoch(self.health_epoch, at)
            && self.capture.is_healthy()
            && self.budget.is_available()
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
        frame_evidence: CurrentFrameEvidence,
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
            registry_id: self.registry_id,
            binding: self.binding,
            health_epoch: self.health_epoch,
            valid_from: self.valid_from,
            trusted_valid_from: self.trusted_valid_from,
            trusted_valid_from_monotonic: self.trusted_valid_from_monotonic,
            valid_until: self.valid_until,
            valid_until_monotonic: self.valid_until_monotonic,
            lease: self.lease,
            capture: self.capture,
            budget: self.budget,
            clock: self.clock,
        };
        let key = CurrentBatchKey {
            venue: self.venue.clone(),
            instrument: self.instrument,
        };
        Ok(CurrentProviderObservation {
            key,
            frame_evidence,
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
