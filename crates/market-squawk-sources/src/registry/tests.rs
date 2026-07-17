#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::marker::PhantomData;
    use std::str::FromStr;
    use std::sync::{Arc, Barrier, Mutex};
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use market_squawk_domain::{
        AggressorSide, ConnectionGeneration, InstrumentId, IntegrityRule, MetadataRevision,
        RuleVersion, SequenceNumber, SequenceValidationRule, SourceId, SourceIdentifier, Timestamp,
        VenueId,
    };

    use super::{
        AuthoritativeSourceRegistry, RawFrameFactory, RegistryClock, RegistryError,
        SessionLeaseState, TrustedRegistryTime, validate_observation_profile,
    };
    use crate::{
        ChecksumValidationProfile, FrameSessionBinding, LiveProtocolProfile,
        ProviderAggressorEvidence, ProviderChecksumEvidence, ProviderDecimalLexeme,
        ProviderNormalizedObservation, ProviderNumericPolicy, ProviderObservationPayload,
        ProviderPrice, ProviderQuantity, ProviderSequenceEvidence, ProviderSnapshotEvidence,
        ProviderTimestampEvidence, SemanticInterpretationProfile, SequenceValidationProfile,
        CurrentHealthReporter, CurrentSourceSession, SessionId, SourceError, TransportFrameKind,
    };
    use crate::registry::test_support::{
        TestResult, direct_metadata, direct_metadata_with_provider_and_limit, healthy_snapshot,
    };

    #[derive(Clone, Copy, Debug)]
    struct ManualClockState {
        reading: TrustedRegistryTime,
        available: bool,
    }

    #[derive(Debug)]
    struct ManualRegistryClock {
        state: Mutex<ManualClockState>,
    }

    impl ManualRegistryClock {
        fn new(reading: TrustedRegistryTime) -> Self {
            Self {
                state: Mutex::new(ManualClockState {
                    reading,
                    available: true,
                }),
            }
        }

        fn set(&self, reading: TrustedRegistryTime) -> TestResult {
            let mut state = self
                .state
                .lock()
                .map_err(|_| std::io::Error::other("manual registry clock mutex poisoned"))?;
            state.reading = reading;
            state.available = true;
            Ok(())
        }

        fn fail(&self) -> TestResult {
            let mut state = self
                .state
                .lock()
                .map_err(|_| std::io::Error::other("manual registry clock mutex poisoned"))?;
            state.available = false;
            Ok(())
        }
    }

    impl RegistryClock for ManualRegistryClock {
        fn observe(&self) -> Result<TrustedRegistryTime, RegistryError> {
            let state = self
                .state
                .lock()
                .map_err(|_| RegistryError::TrustedClockUnavailable)?;
            if state.available {
                Ok(state.reading)
            } else {
                Err(RegistryError::TrustedClockUnavailable)
            }
        }

        fn shared_allocation_charge(&self) -> usize {
            std::mem::size_of::<Self>()
                + crate::conservative_arc_control_block_charge::<Self>()
        }
    }

    #[derive(Debug)]
    struct HealthHarness {
        registry: AuthoritativeSourceRegistry,
        registered: super::RegisteredSource,
        session: CurrentSourceSession,
        reporter: CurrentHealthReporter,
        clock: Arc<ManualRegistryClock>,
        wall_origin: Timestamp,
        monotonic_origin: Instant,
    }

    impl HealthHarness {
        fn new(source: &str) -> TestResult<Self> {
            let wall_origin = Timestamp::from_unix_nanos(1_000_000_000);
            let monotonic_origin = Instant::now();
            let clock = Arc::new(ManualRegistryClock::new(TrustedRegistryTime::new(
                wall_origin,
                monotonic_origin,
            )));
            let mut registry =
                AuthoritativeSourceRegistry::try_new_with_authority_state_and_clock(
                    super::RegistryAuthorityState::empty(),
                    clock.clone(),
                )?;
            let registered = registry.register(direct_metadata(source, "revision-1")?, wall_origin)?;
            let session = registry.begin_session(
                &registered,
                SessionId::new(SourceIdentifier::try_from("session-1")?),
                ConnectionGeneration::new(1)?,
                wall_origin,
            )?;
            let capabilities = registry.take_capture_generation_capabilities(&session)?;
            let (mut capture_control, _capture_admission, _capture_degradation) =
                capabilities.into_parts();
            capture_control.mark_healthy()?;
            let reporter = registry.take_current_health_reporter(&session)?;
            Ok(Self {
                registry,
                registered,
                session,
                reporter,
                clock,
                wall_origin,
                monotonic_origin,
            })
        }

        fn timestamp(&self, offset_nanos: i64) -> TestResult<Timestamp> {
            Ok(self.wall_origin.checked_add_nanos(offset_nanos)?)
        }

        fn reading(&self, wall_offset: i64, monotonic_offset: u64) -> TestResult<TrustedRegistryTime> {
            let monotonic = self
                .monotonic_origin
                .checked_add(Duration::from_nanos(monotonic_offset))
                .ok_or_else(|| std::io::Error::other("manual monotonic time overflowed"))?;
            Ok(TrustedRegistryTime::new(
                self.timestamp(wall_offset)?,
                monotonic,
            ))
        }

        fn set_time(&self, wall_offset: i64, monotonic_offset: u64) -> TestResult {
            self.clock
                .set(self.reading(wall_offset, monotonic_offset)?)
        }

        fn snapshot(&self, observed_offset: i64, deadline_offset: i64) -> TestResult<crate::SourceHealthSnapshot> {
            healthy_snapshot(
                &self.session,
                self.timestamp(observed_offset)?,
                self.timestamp(deadline_offset)?,
            )
        }

        fn accept_health(
            &mut self,
            observed_offset: i64,
            reported_offset: i64,
            accepted_offset: i64,
            deadline_offset: i64,
        ) -> TestResult {
            self.set_time(reported_offset, u64::try_from(reported_offset)?)?;
            let snapshot = self.snapshot(observed_offset, deadline_offset)?;
            let update = self.reporter.report(snapshot)?;
            self.set_time(accepted_offset, u64::try_from(accepted_offset)?)?;
            self.registry.record_health(&self.session, update)?;
            Ok(())
        }

        fn epoch_and_cursor(&self) -> (u64, i64) {
            (
                self.session.lease.health_epoch.load(std::sync::atomic::Ordering::Acquire),
                self.session
                    .lease
                    .last_health_observed_nanos
                    .load(std::sync::atomic::Ordering::Acquire),
            )
        }
    }


    #[test]
    fn frame_ordinal_exhaustion_terminally_invalidates_factory()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = FrameSessionBinding::new(
            SourceId::try_from("source-a")?,
            MetadataRevision::new(SourceIdentifier::try_from("revision-a")?),
            SessionId::new(SourceIdentifier::try_from("session-a")?),
            ConnectionGeneration::new(1)?,
        );
        let lease = Arc::new(SessionLeaseState {
            current: AtomicBool::new(true),
            live_qualified: AtomicBool::new(false),
            health_epoch: AtomicU64::new(0),
            valid_from_nanos: AtomicI64::new(i64::MAX),
            valid_until_nanos: AtomicI64::new(i64::MIN),
            last_health_observed_nanos: AtomicI64::new(i64::MIN),
            frame_ordinal: AtomicU64::new(u64::MAX),
        });
        let mut factory = RawFrameFactory {
            binding,
            lease: Arc::clone(&lease),
            not_sync: PhantomData::<Cell<()>>,
        };
        assert!(matches!(
            factory.try_frame(
                Timestamp::from_unix_nanos(1),
                TransportFrameKind::Binary,
                Bytes::from_static(b"frame"),
            ),
            Err(SourceError::FrameIdentityExhausted)
        ));
        assert!(!lease.is_current());
        Ok(())
    }

    #[test]
    fn semantic_rules_cannot_be_transplanted_across_event_families()
    -> Result<(), Box<dyn std::error::Error>> {
        let aggressor = rule("aggressor")?;
        let corporate_action = rule("corporate-action")?;
        let timestamp = rule("timestamp")?;
        let sequence = rule("sequence")?;
        let no_checksum = rule("no-checksum")?;
        let no_snapshot = rule("no-snapshot")?;
        let protocol = LiveProtocolProfile::new(
            rule("decoder")?,
            SemanticInterpretationProfile::new(
                aggressor.clone(),
                rule("auction")?,
                rule("trading-status")?,
                corporate_action.clone(),
            ),
            timestamp.clone(),
            SequenceValidationProfile::Provided {
                rule: sequence.clone(),
                progression: SequenceValidationRule::Consecutive,
            },
            ChecksumValidationProfile::Unsupported {
                rule: no_checksum.clone(),
            },
            true,
            ProviderNumericPolicy::ExactDecimalLexeme,
        );
        let observation = |payload| {
            ProviderNormalizedObservation::try_new(
                SourceIdentifier::try_from("message-1")?,
                VenueId::try_from("coinbase")?,
                InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?,
                ProviderTimestampEvidence::Provided {
                    value: Timestamp::from_unix_nanos(1),
                    rule: timestamp.clone(),
                },
                ProviderSequenceEvidence::Provided {
                    value: SequenceNumber::new(1),
                    rule: sequence.clone(),
                },
                ProviderSnapshotEvidence::NotApplicable(no_snapshot.clone()),
                ProviderChecksumEvidence::Unsupported {
                    rule: no_checksum.clone(),
                },
                payload,
            )
            .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
        };
        let price = || {
            ProviderDecimalLexeme::try_new("1")
                .map(ProviderPrice::new)
                .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
        };
        let quantity = || {
            ProviderDecimalLexeme::try_new("1")
                .map(ProviderQuantity::new)
                .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
        };
        let valid = observation(ProviderObservationPayload::Trade {
            trade_id: SourceIdentifier::try_from("trade-1")?,
            price: price()?,
            quantity: quantity()?,
            aggressor: ProviderAggressorEvidence::new(AggressorSide::Buy, None, aggressor),
        })?;
        assert!(validate_observation_profile(&protocol, &valid).is_ok());

        let transplanted = observation(ProviderObservationPayload::Trade {
            trade_id: SourceIdentifier::try_from("trade-2")?,
            price: price()?,
            quantity: quantity()?,
            aggressor: ProviderAggressorEvidence::new(AggressorSide::Buy, None, corporate_action),
        })?;
        assert!(validate_observation_profile(&protocol, &transplanted).is_err());
        Ok(())
    }

    #[test]
    fn reporter_rejects_future_health_without_advancing_authority() -> TestResult {
        let mut harness = HealthHarness::new("trusted-time-future-report")?;
        harness.set_time(20, 20)?;
        let before = harness.epoch_and_cursor();
        let future = harness.snapshot(21, 1_000)?;
        assert!(matches!(
            harness.reporter.report(future),
            Err(RegistryError::InvalidHealthTemporalOrder)
        ));
        assert_eq!(harness.epoch_and_cursor(), before);

        harness.accept_health(10, 20, 30, 1_000)?;
        assert_eq!(harness.epoch_and_cursor(), (1, harness.timestamp(10)?.unix_nanos()));
        Ok(())
    }

    #[test]
    fn reporter_rejects_observation_before_sealed_session_start_without_mutation() -> TestResult {
        let mut harness = HealthHarness::new("trusted-time-before-session")?;
        harness.set_time(20, 20)?;
        let before = harness.epoch_and_cursor();
        let before_session = harness.snapshot(-1, 1_000)?;
        assert!(matches!(
            harness.reporter.report(before_session),
            Err(RegistryError::InvalidHealthTemporalOrder)
        ));
        assert_eq!(harness.epoch_and_cursor(), before);
        harness.accept_health(10, 20, 30, 1_000)?;
        Ok(())
    }

    #[test]
    fn record_rejects_wall_and_monotonic_inversions_without_mutation() -> TestResult {
        let mut wall = HealthHarness::new("trusted-time-wall-inversion")?;
        wall.set_time(20, 20)?;
        let wall_update = wall.reporter.report(wall.snapshot(10, 1_000)?)?;
        let before_wall = wall.epoch_and_cursor();
        wall.set_time(19, 30)?;
        assert_eq!(
            wall.registry.record_health(&wall.session, wall_update),
            Err(RegistryError::InvalidHealthTemporalOrder)
        );
        assert_eq!(wall.epoch_and_cursor(), before_wall);
        wall.accept_health(11, 31, 40, 1_000)?;

        let mut monotonic = HealthHarness::new("trusted-time-monotonic-inversion")?;
        monotonic.set_time(20, 20)?;
        let monotonic_update = monotonic
            .reporter
            .report(monotonic.snapshot(10, 1_000)?)?;
        let before_monotonic = monotonic.epoch_and_cursor();
        monotonic.set_time(30, 19)?;
        assert_eq!(
            monotonic
                .registry
                .record_health(&monotonic.session, monotonic_update),
            Err(RegistryError::TrustedClockRegression)
        );
        assert_eq!(monotonic.epoch_and_cursor(), before_monotonic);
        monotonic.accept_health(11, 31, 40, 1_000)?;
        Ok(())
    }

    #[test]
    fn health_epoch_overflow_is_failure_atomic_and_preserves_prior_authority() -> TestResult {
        let mut harness = HealthHarness::new("trusted-time-health-epoch")?;
        harness.accept_health(10, 20, 30, 1_000)?;
        harness.set_time(40, 40)?;
        harness
            .session
            .lease
            .health_epoch
            .store(u64::MAX, std::sync::atomic::Ordering::Release);
        let source_id = harness.session.source_id().clone();
        let health = harness
            .registry
            .entries
            .get_mut(&source_id)
            .and_then(|entry| entry.health_authority.as_mut())
            .ok_or_else(|| std::io::Error::other("qualified health authority missing"))?;
        health.epoch = u64::MAX;
        let prior_lease = harness
            .registry
            .validate_current_authority(&harness.session)?
            .try_current_lease()?;

        harness.set_time(50, 50)?;
        let successor = harness.reporter.report(harness.snapshot(41, 1_000)?)?;
        harness.set_time(60, 60)?;
        let before = harness.epoch_and_cursor();
        assert_eq!(
            harness.registry.record_health(&harness.session, successor),
            Err(RegistryError::HealthEpochExhausted)
        );
        assert_eq!(harness.epoch_and_cursor(), before);
        let authority = harness
            .registry
            .entries
            .get(&source_id)
            .and_then(|entry| entry.health_authority.as_ref())
            .ok_or_else(|| std::io::Error::other("prior health authority was removed"))?;
        assert_eq!(authority.epoch, u64::MAX);
        assert_eq!(authority.observed_at, harness.timestamp(10)?);
        prior_lease.validate_at(harness.timestamp(60)?)?;
        Ok(())
    }

    #[test]
    fn live_lease_rejects_event_projection_before_observed_lower_bound() -> TestResult {
        let mut harness = HealthHarness::new("trusted-time-lower-bound")?;
        harness.accept_health(10, 20, 30, 1_000)?;
        harness.set_time(40, 40)?;
        let lease = harness
            .registry
            .validate_current_authority(&harness.session)?
            .try_current_lease()?;
        assert_eq!(
            lease.validate_at(harness.timestamp(9)?),
            Err(RegistryError::HealthNotQualified)
        );
        Ok(())
    }

    #[test]
    fn borrowed_current_authority_cannot_mint_after_sealed_expiry() -> TestResult {
        let mut harness = HealthHarness::new("trusted-time-cached-current")?;
        harness.accept_health(10, 20, 30, 1_000)?;
        harness.set_time(40, 40)?;
        let current = harness
            .registry
            .validate_current_authority(&harness.session)?;
        harness.set_time(1_001, 1_001)?;
        assert!(matches!(
            current.try_current_lease(),
            Err(RegistryError::HealthNotQualified)
        ));
        Ok(())
    }

    #[test]
    fn sealed_wall_and_monotonic_discontinuities_fail_closed() -> TestResult {
        let mut harness = HealthHarness::new("trusted-time-discontinuities")?;
        harness.accept_health(10, 20, 30, 1_000)?;
        harness.set_time(40, 40)?;
        let lease = harness
            .registry
            .validate_current_authority(&harness.session)?
            .try_current_lease()?;

        harness.set_time(50, 29)?;
        assert_eq!(
            lease.validate_at(harness.timestamp(50)?),
            Err(RegistryError::TrustedClockRegression)
        );
        harness.set_time(5, 50)?;
        assert_eq!(
            lease.validate_at(harness.timestamp(50)?),
            Err(RegistryError::HealthNotQualified)
        );
        harness.set_time(1_001, 60)?;
        assert_eq!(
            lease.validate_at(harness.timestamp(60)?),
            Err(RegistryError::HealthNotQualified)
        );
        harness.set_time(50, 1_001)?;
        assert_eq!(
            lease.validate_at(harness.timestamp(50)?),
            Err(RegistryError::HealthNotQualified)
        );
        Ok(())
    }

    #[test]
    fn unavailable_trusted_clock_is_failure_atomic_at_report_record_and_mint() -> TestResult {
        let mut harness = HealthHarness::new("trusted-time-unavailable")?;
        let before_report = harness.epoch_and_cursor();
        let report_snapshot = harness.snapshot(10, 1_000)?;
        harness.clock.fail()?;
        assert!(matches!(
            harness.reporter.report(report_snapshot),
            Err(RegistryError::TrustedClockUnavailable)
        ));
        assert_eq!(harness.epoch_and_cursor(), before_report);

        harness.set_time(20, 20)?;
        let record_update = harness.reporter.report(harness.snapshot(10, 1_000)?)?;
        let before_record = harness.epoch_and_cursor();
        harness.clock.fail()?;
        assert_eq!(
            harness
                .registry
                .record_health(&harness.session, record_update),
            Err(RegistryError::TrustedClockUnavailable)
        );
        assert_eq!(harness.epoch_and_cursor(), before_record);

        harness.accept_health(11, 31, 40, 1_000)?;
        harness.set_time(50, 50)?;
        let current = harness
            .registry
            .validate_current_authority(&harness.session)?;
        let before_mint = harness.epoch_and_cursor();
        harness.clock.fail()?;
        assert!(matches!(
            current.try_current_lease(),
            Err(RegistryError::TrustedClockUnavailable)
        ));
        assert_eq!(harness.epoch_and_cursor(), before_mint);
        harness.set_time(51, 51)?;
        current.try_current_lease()?.validate_at(harness.timestamp(51)?)?;
        Ok(())
    }

    #[test]
    fn trusted_deadline_conversion_rejects_wall_delta_overflow() {
        let reading = TrustedRegistryTime::new(
            Timestamp::from_unix_nanos(i64::MIN),
            Instant::now(),
        );
        assert_eq!(
            reading.checked_deadline(Timestamp::from_unix_nanos(i64::MAX)),
            Err(RegistryError::HealthDeadlineOverflow)
        );
    }

    #[test]
    fn restored_registration_epoch_overflow_preserves_authority_state() -> TestResult {
        let at = Timestamp::from_unix_nanos(1);
        let mut original = AuthoritativeSourceRegistry::try_new()?;
        original.register(direct_metadata("restored-epoch-source", "revision-1")?, at)?;
        let mut wire = serde_json::to_value(original.export_authority_state()?)?;
        let sources = wire
            .get_mut("sources")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or_else(|| std::io::Error::other("authority sources were not an array"))?;
        let persisted = sources
            .first_mut()
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_else(|| std::io::Error::other("persisted source authority missing"))?;
        persisted.insert("last_epoch".to_owned(), serde_json::json!(u64::MAX));
        let state = serde_json::from_value(wire)?;
        let mut restored = AuthoritativeSourceRegistry::try_new_with_authority_state(state)?;
        let before = restored.export_authority_state()?;

        assert!(matches!(
            restored.register(
                direct_metadata("restored-epoch-source", "revision-2")?,
                at
            ),
            Err(RegistryError::EpochExhausted)
        ));
        assert_eq!(restored.export_authority_state()?, before);
        assert!(restored.entries.is_empty());
        Ok(())
    }

    #[test]
    fn normal_revocation_persists_epoch_before_restart_reregistration() -> TestResult {
        let at = Timestamp::from_unix_nanos(1);
        let mut registry = AuthoritativeSourceRegistry::try_new()?;
        let registered =
            registry.register(direct_metadata("revoked-epoch-source", "revision-1")?, at)?;
        assert_eq!(registered.epoch, 1);
        registry.revoke(&registered, at)?;
        assert!(matches!(
            registry.validate_registered(&registered, at),
            Err(RegistryError::SourceRevoked)
        ));
        let revoked_state = registry.export_authority_state()?;

        let mut restarted =
            AuthoritativeSourceRegistry::try_new_with_authority_state(revoked_state)?;
        let successor = restarted.register(
            direct_metadata("revoked-epoch-source", "revision-2")?,
            at,
        )?;
        assert_eq!(successor.epoch, 3);
        Ok(())
    }

    #[test]
    fn max_epoch_blocks_replacement_but_never_blocks_terminal_revocation() -> TestResult {
        let mut harness = HealthHarness::new("active-epoch-source")?;
        harness.accept_health(10, 20, 30, 1_000)?;
        harness.set_time(40, 40)?;
        let live_lease = harness
            .registry
            .validate_current_authority(&harness.session)?
            .try_current_lease()?;
        harness.registered.epoch = u64::MAX;
        harness.session.epoch = u64::MAX;
        let entry = harness
            .registry
            .entries
            .get_mut(harness.registered.source_id())
            .ok_or_else(|| std::io::Error::other("registered source entry missing"))?;
        entry.epoch = u64::MAX;
        let history = harness
            .registry
            .history
            .get_mut(harness.registered.source_id())
            .ok_or_else(|| std::io::Error::other("registered source history missing"))?;
        history.last_epoch = u64::MAX;
        let before = harness.registry.export_authority_state()?;

        assert!(matches!(
            harness.registry.replace_metadata(
                &harness.registered,
                direct_metadata("active-epoch-source", "revision-2")?,
                harness.timestamp(40)?,
            ),
            Err(RegistryError::EpochExhausted)
        ));
        assert_eq!(harness.registry.export_authority_state()?, before);
        assert!(
            harness
                .registry
                .validate_session(&harness.session, harness.timestamp(40)?)
                .is_ok()
        );
        live_lease.validate_at(harness.timestamp(40)?)?;

        harness
            .registry
            .revoke(&harness.registered, harness.timestamp(41)?)?;
        assert_eq!(harness.registry.export_authority_state()?, before);
        assert!(matches!(
            harness
                .registry
                .validate_registered(&harness.registered, harness.timestamp(41)?),
            Err(RegistryError::SourceRevoked)
        ));
        assert!(matches!(
            harness
                .registry
                .validate_session(&harness.session, harness.timestamp(41)?),
            Err(RegistryError::SourceRevoked)
        ));
        assert_eq!(
            live_lease.validate_at(harness.timestamp(41)?),
            Err(RegistryError::HealthNotQualified)
        );

        let mut restarted =
            AuthoritativeSourceRegistry::try_new_with_authority_state(before.clone())?;
        assert!(matches!(
            restarted.register(
                direct_metadata("active-epoch-source", "revision-2")?,
                harness.timestamp(41)?
            ),
            Err(RegistryError::EpochExhausted)
        ));
        assert_eq!(restarted.export_authority_state()?, before);
        Ok(())
    }

    #[test]
    fn concurrent_conflicting_budget_registration_has_one_authoritative_winner() -> TestResult {
        let at = Timestamp::from_unix_nanos(1);
        let first_registry = AuthoritativeSourceRegistry::try_new()?;
        let second_registry = AuthoritativeSourceRegistry::try_new()?;
        let first_metadata = direct_metadata_with_provider_and_limit(
            "concurrent-policy-source-a",
            "revision-1",
            "concurrent-budget-conflict-scope",
            11,
        )?;
        let second_metadata = direct_metadata_with_provider_and_limit(
            "concurrent-policy-source-b",
            "revision-1",
            "concurrent-budget-conflict-scope",
            12,
        )?;
        let start = Arc::new(Barrier::new(2));
        let finish = Arc::new(Barrier::new(3));
        let first_start = Arc::clone(&start);
        let first_finish = Arc::clone(&finish);
        let first = std::thread::spawn(move || {
            let mut registry = first_registry;
            first_start.wait();
            let result = registry.register(first_metadata, at);
            first_finish.wait();
            (registry, result)
        });
        let second_start = Arc::clone(&start);
        let second_finish = Arc::clone(&finish);
        let second = std::thread::spawn(move || {
            let mut registry = second_registry;
            second_start.wait();
            let result = registry.register(second_metadata, at);
            second_finish.wait();
            (registry, result)
        });
        finish.wait();
        let (first_registry, first_result) = first
            .join()
            .map_err(|_| std::io::Error::other("first registration thread panicked"))?;
        let (second_registry, second_result) = second
            .join()
            .map_err(|_| std::io::Error::other("second registration thread panicked"))?;

        assert_ne!(first_result.is_ok(), second_result.is_ok());
        let empty = super::RegistryAuthorityState::empty();
        if first_result.is_ok() {
            assert!(matches!(
                second_result,
                Err(RegistryError::BudgetCoordinator)
            ));
            assert!(second_registry.entries.is_empty());
            assert_eq!(second_registry.export_authority_state()?, empty);
        } else {
            assert!(matches!(
                first_result,
                Err(RegistryError::BudgetCoordinator)
            ));
            assert!(first_registry.entries.is_empty());
            assert_eq!(first_registry.export_authority_state()?, empty);
        }
        Ok(())
    }

    fn rule(value: &str) -> Result<IntegrityRule, Box<dyn std::error::Error>> {
        Ok(IntegrityRule::new(
            SourceIdentifier::try_from(value)?,
            RuleVersion::new(1)?,
        ))
    }
}
