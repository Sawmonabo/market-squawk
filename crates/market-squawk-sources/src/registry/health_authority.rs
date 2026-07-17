impl AuthoritativeSourceRegistry {
    /// Records registry-owned current health for the exact session generation.
    ///
    /// Any non-executable dimension clears qualification immediately. A later fully healthy
    /// observation may requalify only while the same lease remains current.
    ///
    /// # Errors
    ///
    /// Rejects stale/transplanted sessions or health evidence bound to another session tuple.
    pub fn record_health(
        &mut self,
        session: &CurrentSourceSession,
        update: CurrentHealthUpdate,
    ) -> Result<(), RegistryError> {
        let health = &update.snapshot;
        let session_started_at = self
            .validate_session_structure(session)?
            .active
            .as_ref()
            .ok_or(RegistryError::SessionNotCurrent)?
            .started_at;
        if !Arc::ptr_eq(&update.lease, &session.lease)
            || !update.binding.shares_allocation_with(&session.binding)
        {
            return Err(RegistryError::HealthBindingMismatch);
        }
        if health.source_id() != session.source_id()
            || health.metadata_revision() != session.revision()
            || health.session_id() != session.session_id()
            || health.connection_generation() != session.generation()
        {
            return Err(RegistryError::HealthBindingMismatch);
        }
        let validation_at = self.clock.observe()?;
        if update.trusted_reported_at.monotonic() < session_started_at.monotonic()
            || validation_at.monotonic() < update.trusted_reported_at.monotonic()
        {
            return Err(RegistryError::TrustedClockRegression);
        }
        if health.observed_at() < session_started_at.wall()
            || health.observed_at() > update.trusted_reported_at.wall()
            || update.trusted_reported_at.wall() > validation_at.wall()
        {
            return Err(RegistryError::InvalidHealthTemporalOrder);
        }
        let entry = self
            .entries
            .get_mut(session.source_id())
            .ok_or(RegistryError::UnknownSource)?;
        if !health.uses_freshness_policy(entry.metadata.freshness_policy()) {
            return Err(RegistryError::HealthPolicyMismatch);
        }
        let previous_observed = session
            .lease
            .last_health_observed_nanos
            .load(Ordering::Acquire);
        if health.observed_at().unix_nanos() <= previous_observed {
            return Err(RegistryError::StaleHealthObservation);
        }
        let live_declaration = entry.metadata.coverage().live();
        let exact_runtime_coverage = matches!(
            (health.coverage(), live_declaration),
            (
                crate::CoverageHealth::Sufficient {
                    provider_product,
                    provider_channel,
                    ..
                },
                Some(live),
            ) if provider_product == live.provider_product()
                && provider_channel == live.provider_channel()
        );
        let qualified = session.capture.is_healthy()
            && matches!(health.connection(), crate::ConnectionLiveness::Live { .. })
            && matches!(
                health.transport_freshness(),
                crate::TransportFreshness::Fresh { .. }
            )
            && matches!(
                health.market_freshness(),
                crate::MarketFreshness::Fresh { .. }
            )
            && matches!(
                health.source_freshness(),
                crate::SourceTimestampFreshness::Fresh { .. }
            )
            && health.stream_integrity() == market_squawk_domain::StreamIntegrityState::Healthy
            && health.capture_integrity()
                != market_squawk_domain::CaptureIntegrityState::Incomplete
            && matches!(
                health.authorization(),
                crate::AuthorizationHealth::Valid { .. }
            )
            && exact_runtime_coverage
            && health.budget() == crate::BudgetHealth::Available
            && update.budget.health() == crate::BudgetHealth::Available
            && health.last_error().is_none();
        let valid_until = health.live_valid_until().map(|health_until| {
            let authorization_until = entry
                .metadata
                .authorization()
                .inclusive_authorization_deadline()
                .unwrap_or(Timestamp::from_unix_nanos(i64::MAX));
            let coverage_until = entry
                .metadata
                .coverage()
                .inclusive_coverage_deadline()
                .unwrap_or(Timestamp::from_unix_nanos(i64::MAX));
            health_until.min(authorization_until).min(coverage_until)
        });
        let valid_until_monotonic = valid_until
            .map(|until| validation_at.checked_deadline(until))
            .transpose()?
            .flatten();
        let qualified = qualified
            && validation_at.wall() >= health.observed_at()
            && valid_until_monotonic.is_some();
        let epoch = match session.lease.next_health_epoch() {
            Some(epoch) => epoch,
            None => {
                entry.terminally_invalidate_health_authority();
                return Err(RegistryError::HealthEpochExhausted);
            }
        };
        let next_authority = if qualified {
            Some(CurrentHealthAuthority {
                epoch,
                observed_at: health.observed_at(),
                trusted_reported_at: update.trusted_reported_at,
                accepted_at: validation_at,
                valid_from: health.observed_at(),
                valid_until: valid_until.ok_or(RegistryError::HealthNotQualified)?,
                valid_until_monotonic: valid_until_monotonic
                    .ok_or(RegistryError::HealthNotQualified)?,
                authorization: health.authorization().clone(),
                coverage: health.coverage().clone(),
                budget: update.budget,
            })
        } else {
            None
        };
        session.lease.commit_live_qualification(
            epoch,
            qualified,
            qualified.then_some(health.observed_at()),
            valid_until,
        );
        session
            .lease
            .last_health_observed_nanos
            .store(health.observed_at().unix_nanos(), Ordering::Release);
        entry.health_authority = next_authority;
        Ok(())
    }

    /// Returns opaque current health/subscription authority for live scope validation.
    ///
    /// # Errors
    ///
    /// Fails closed when the session is stale, metadata is ineffective, or current registry-owned
    /// health has not established all execution prerequisites.
    pub fn validate_current_authority<'a>(
        &'a self,
        session: &'a CurrentSourceSession,
    ) -> Result<ValidatedCurrentSourceAuthority<'a>, RegistryError> {
        let validation_at = self.clock.observe()?;
        let validated = self.validate_session(session, validation_at.wall())?;
        let entry = self.validate_session_structure(session)?;
        let health = entry
            .health_authority
            .as_ref()
            .ok_or(RegistryError::HealthNotQualified)?;
        if validation_at.monotonic() < health.accepted_at.monotonic() {
            return Err(RegistryError::TrustedClockRegression);
        }
        if health.observed_at > health.trusted_reported_at.wall()
            || health.trusted_reported_at.wall() > validation_at.wall()
            || health.accepted_at.wall() > validation_at.wall()
            || validation_at.wall() < health.observed_at
            || validation_at.wall() > health.valid_until
            || validation_at.monotonic() > health.valid_until_monotonic
            || !session
                .lease
                .validate_health_epoch(health.epoch, validation_at.wall())
        {
            return Err(RegistryError::HealthNotQualified);
        }
        if !health.budget.is_available() {
            return Err(RegistryError::HealthNotQualified);
        }
        if !session.capture.is_healthy() {
            return Err(RegistryError::CaptureNotHealthy);
        }
        let attestation = entry
            .universe_attestation
            .as_ref()
            .filter(|attestation| attestation.is_effective_at(validation_at.wall()));
        Ok(ValidatedCurrentSourceAuthority {
            validated,
            health,
            attestation,
            validated_at: validation_at,
            clock: &self.clock,
        })
    }

    fn validate_registered_structure(
        &self,
        registered: &RegisteredSource,
    ) -> Result<&RegistryEntry, RegistryError> {
        if registered.registry_id != self.instance_id {
            return Err(RegistryError::HandleTransplanted);
        }
        let entry = self
            .entries
            .get(&registered.source_id)
            .ok_or(RegistryError::UnknownSource)?;
        if entry.revoked {
            return Err(RegistryError::SourceRevoked);
        }
        if entry.epoch != registered.epoch
            || entry.metadata.revision() != &registered.revision
            || entry.metadata.source_id() != &registered.source_id
        {
            return Err(RegistryError::StaleHandle);
        }
        Ok(entry)
    }

    fn validate_session_structure(
        &self,
        session: &CurrentSourceSession,
    ) -> Result<&RegistryEntry, RegistryError> {
        if session.registry_id != self.instance_id {
            return Err(RegistryError::HandleTransplanted);
        }
        let entry = self
            .entries
            .get(session.source_id())
            .ok_or(RegistryError::UnknownSource)?;
        if entry.revoked {
            return Err(RegistryError::SourceRevoked);
        }
        if entry.epoch != session.epoch
            || entry.metadata.revision() != session.revision()
            || entry.metadata.source_id() != session.source_id()
        {
            return Err(RegistryError::StaleHandle);
        }
        let active = entry
            .active
            .as_ref()
            .ok_or(RegistryError::SessionNotCurrent)?;
        if active.session_id != *session.session_id()
            || active.generation != session.generation()
            || active.started_at.wall() != session.started_at
            || !Arc::ptr_eq(&active.lease, &session.lease)
            || !active.lease.is_current()
        {
            return Err(RegistryError::SessionNotCurrent);
        }
        Ok(entry)
    }
}
