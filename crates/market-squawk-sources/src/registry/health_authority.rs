impl AuthoritativeSourceRegistry {
    /// Records registry-owned current health for the exact session generation.
    ///
    /// Any required current-data dimension clears qualification immediately. A later fully healthy
    /// observation may requalify only while the same lease remains current. Direct-verified data
    /// retains the stricter execution-quality timestamp contract.
    ///
    /// # Errors
    ///
    /// Rejects stale/transplanted sessions or health evidence bound to another session tuple.
    pub fn record_health(
        &mut self,
        session: &CurrentSourceSession,
        update: CurrentHealthUpdate,
    ) -> Result<(), RegistryError> {
        self.record_health_with_qualification(session, update)
            .map(|_recording| ())
    }

    /// Records health and reports whether the registry issued current-data authority.
    ///
    /// The returned classification is computed by the same closed predicate that owns health
    /// authority. It exists so a capture-first caller can distinguish an aged bootstrap from a
    /// revoked or otherwise invalid source without recreating the predicate outside the registry.
    ///
    /// # Errors
    ///
    /// Rejects stale/transplanted sessions or health evidence bound to another session tuple.
    pub fn record_health_with_qualification(
        &mut self,
        session: &CurrentSourceSession,
        update: CurrentHealthUpdate,
    ) -> Result<CurrentHealthRecording, RegistryError> {
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
        let live_declaration = match health.coverage() {
            crate::CoverageHealth::Sufficient {
                provider_product,
                provider_channel,
                ..
            } => entry
                .metadata
                .coverage()
                .live_for(provider_product, provider_channel),
            _ => None,
        };
        let quality_ceiling = entry.metadata.quality_ceiling();
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
        let mut causes = 0_u16;
        if !session.capture.is_healthy() {
            causes |= CurrentHealthUnqualification::CAPTURE;
        }
        if !matches!(health.connection(), crate::ConnectionLiveness::Live { .. }) {
            causes |= CurrentHealthUnqualification::CONNECTION_FRESHNESS;
        }
        if !matches!(
            health.transport_freshness(),
            crate::TransportFreshness::Fresh { .. }
        ) {
            causes |= CurrentHealthUnqualification::TRANSPORT_FRESHNESS;
        }
        if !matches!(
            health.market_freshness(),
            crate::MarketFreshness::Fresh { .. }
        ) {
            causes |= CurrentHealthUnqualification::MARKET_FRESHNESS;
        }
        if matches!(
            health.source_freshness(),
            crate::SourceTimestampFreshness::Stale { .. }
        ) || quality_ceiling == market_squawk_domain::DataQuality::DirectVerified
            && matches!(
                health.source_freshness(),
                crate::SourceTimestampFreshness::Uninitialized
            )
        {
            causes |= CurrentHealthUnqualification::SOURCE_FRESHNESS;
        }
        if health.stream_integrity() != market_squawk_domain::StreamIntegrityState::Healthy {
            causes |= CurrentHealthUnqualification::STREAM_INTEGRITY;
        }
        if health.capture_integrity() == market_squawk_domain::CaptureIntegrityState::Incomplete {
            causes |= CurrentHealthUnqualification::CAPTURE_INTEGRITY;
        }
        if !matches!(
            health.authorization(),
            crate::AuthorizationHealth::Valid { .. }
        ) {
            causes |= CurrentHealthUnqualification::AUTHORIZATION;
        }
        if !exact_runtime_coverage {
            causes |= CurrentHealthUnqualification::COVERAGE;
        }
        if health.budget() != crate::BudgetHealth::Available {
            causes |= CurrentHealthUnqualification::SNAPSHOT_BUDGET;
        }
        if update.budget.health() != crate::BudgetHealth::Available {
            causes |= CurrentHealthUnqualification::REPORTER_BUDGET;
        }
        if health.last_error().is_some() {
            causes |= CurrentHealthUnqualification::LAST_ERROR;
        }
        if validation_at.wall() < health.observed_at() {
            causes |= CurrentHealthUnqualification::OBSERVATION_TIME;
        }
        if !matches!(
            health.authorization(),
            crate::AuthorizationHealth::Valid { valid_until, .. }
                if validation_at.wall() <= *valid_until
        ) {
            causes |= CurrentHealthUnqualification::AUTHORIZATION;
        }
        if !matches!(
            health.coverage(),
            crate::CoverageHealth::Sufficient { valid_until, .. }
                if validation_at.wall() <= *valid_until
        ) {
            causes |= CurrentHealthUnqualification::COVERAGE;
        }
        let valid_until = health
            .current_data_valid_until(quality_ceiling)
            .map(|health_until| {
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
        let metadata_authorization_current = entry
            .metadata
            .authorization()
            .inclusive_authorization_deadline()
            .is_none_or(|deadline| validation_at.wall() <= deadline);
        let metadata_coverage_current = entry
            .metadata
            .coverage()
            .inclusive_coverage_deadline()
            .is_none_or(|deadline| validation_at.wall() <= deadline);
        if !metadata_authorization_current || !metadata_coverage_current {
            causes |= CurrentHealthUnqualification::STATIC_DEADLINE;
        } else if valid_until_monotonic.is_none() {
            causes |= CurrentHealthUnqualification::CURRENT_DATA_DEADLINE;
        }
        let qualified = causes == 0;
        let epoch = match session.lease.next_health_epoch() {
            Some(epoch) => epoch,
            None => {
                entry.terminally_invalidate_health_authority();
                return Err(RegistryError::HealthEpochExhausted);
            }
        };
        // Permission survives ordinary price freshness, but never its own static/runtime expiry.
        let permission_valid_until = match (health.authorization(), health.coverage()) {
            (
                crate::AuthorizationHealth::Valid {
                    valid_until: authorization,
                    ..
                },
                crate::CoverageHealth::Sufficient {
                    valid_until: coverage,
                    ..
                },
            ) => Some(
                (*authorization)
                    .min(*coverage)
                    .min(
                        entry
                            .metadata
                            .authorization()
                            .inclusive_authorization_deadline()
                            .unwrap_or(Timestamp::from_unix_nanos(i64::MAX)),
                    )
                    .min(
                        entry
                            .metadata
                            .coverage()
                            .inclusive_coverage_deadline()
                            .unwrap_or(Timestamp::from_unix_nanos(i64::MAX)),
                    ),
            ),
            _ => None,
        };
        let next_authority = if qualified {
            Some(CurrentHealthAuthority {
                snapshot: Arc::new(health.clone()),
                epoch,
                observed_at: health.observed_at(),
                trusted_reported_at: update.trusted_reported_at,
                accepted_at: validation_at,
                valid_from: health.observed_at(),
                valid_until: valid_until.ok_or(RegistryError::HealthNotQualified)?,
                valid_until_monotonic: valid_until_monotonic
                    .ok_or(RegistryError::HealthNotQualified)?,
                permission_valid_until: permission_valid_until
                    .ok_or(RegistryError::HealthNotQualified)?,
                permission_valid_until_monotonic: validation_at
                    .checked_deadline(
                        permission_valid_until.ok_or(RegistryError::HealthNotQualified)?,
                    )?
                    .ok_or(RegistryError::HealthNotQualified)?,
                authorization: health.authorization().clone(),
                coverage: health.coverage().clone(),
                budget: update.budget,
            })
        } else {
            None
        };
        let benign_renewal = entry
            .health_authority
            .as_ref()
            .zip(next_authority.as_ref())
            .is_some_and(|(previous, next)| previous.is_benign_renewal(next));
        session.lease.commit_live_qualification(
            epoch,
            qualified,
            benign_renewal,
            qualified.then_some(health.observed_at()),
            valid_until,
        );
        session
            .lease
            .last_health_observed_nanos
            .store(health.observed_at().unix_nanos(), Ordering::Release);
        entry.health_authority = next_authority;
        Ok(if qualified {
            CurrentHealthRecording::Qualified
        } else {
            CurrentHealthRecording::Unqualified(CurrentHealthUnqualification::new(causes))
        })
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
            || !session.lease.validate_health_epoch(health.epoch)
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
            provider_identities: &entry.provider_identities,
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
            || !Arc::ptr_eq(&entry.registration_lease, &registered.lease)
            || !entry.registration_lease.is_current()
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
            || active.started_at.wall() != session.started_at.wall()
            || !Arc::ptr_eq(&active.lease, &session.lease)
            || !active.lease.is_current()
        {
            return Err(RegistryError::SessionNotCurrent);
        }
        Ok(entry)
    }
}

impl CurrentHealthAuthority {
    /// Renewal may extend time, but cannot replace or narrow the evidence authorizing old work.
    fn is_benign_renewal(&self, next: &Self) -> bool {
        let authorization_continues = match (&self.authorization, &next.authorization) {
            (
                crate::AuthorizationHealth::Valid {
                    evidence: before,
                    valid_until: before_until,
                },
                crate::AuthorizationHealth::Valid {
                    evidence: after,
                    valid_until: after_until,
                },
            ) => before == after && after_until >= before_until,
            _ => false,
        };
        let coverage_continues = match (&self.coverage, &next.coverage) {
            (
                crate::CoverageHealth::Sufficient {
                    evidence: before,
                    provider_product: before_product,
                    provider_channel: before_channel,
                    valid_until: before_until,
                },
                crate::CoverageHealth::Sufficient {
                    evidence: after,
                    provider_product: after_product,
                    provider_channel: after_channel,
                    valid_until: after_until,
                },
            ) => {
                before == after
                    && before_product == after_product
                    && before_channel == after_channel
                    && after_until >= before_until
            }
            _ => false,
        };
        authorization_continues
            && coverage_continues
            && next.valid_until >= self.valid_until
            && next.valid_until_monotonic >= self.valid_until_monotonic
            && self.budget.is_available()
    }
}
