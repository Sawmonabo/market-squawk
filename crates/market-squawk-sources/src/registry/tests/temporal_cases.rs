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
    fn health_epoch_exhaustion_terminally_revokes_every_generation_authority() -> TestResult {
        use std::str::FromStr;

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
        let prior_scope = harness
            .registry
            .validate_current_authority(&harness.session)?
            .validate_live_scope(
                &VenueId::try_from("coinbase")?,
                InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?,
                LiveEventClass::Trade,
                None,
            )?;
        let prior_queued = prior_scope.queued_authority_for_test();
        let mut raw_frames = harness.registry.take_raw_frame_factory(&harness.session)?;
        raw_frames.try_frame(
            harness.timestamp(40)?,
            crate::TransportFrameKind::Binary,
            bytes::Bytes::from_static(b"before-exhaustion"),
        )?;

        harness.set_time(50, 50)?;
        let successor = harness.reporter.report(harness.snapshot(41, 1_000)?)?;
        harness.set_time(60, 60)?;
        assert_eq!(
            harness.registry.record_health(&harness.session, successor),
            Err(RegistryError::HealthEpochExhausted)
        );
        assert!(harness
            .registry
            .entries
            .get(&source_id)
            .and_then(|entry| entry.health_authority.as_ref())
            .is_none());
        assert!(harness.session.lease.is_terminal());
        assert!(!harness.session.capture.is_healthy());
        assert_eq!(
            harness.session.validate_current_lease(),
            Err(RegistryError::SessionNotCurrent)
        );
        assert!(matches!(
            harness.registry.validate_current_authority(&harness.session),
            Err(RegistryError::SessionNotCurrent)
        ));
        assert_eq!(
            prior_lease.validate_at(harness.timestamp(60)?),
            Err(RegistryError::HealthNotQualified)
        );
        assert_eq!(
            prior_scope.validate_at(harness.timestamp(60)?),
            Err(RegistryError::HealthNotQualified)
        );
        assert_eq!(
            prior_queued.validate_at(harness.timestamp(60)?),
            Err(RegistryError::HealthNotQualified)
        );
        assert!(matches!(
            raw_frames.try_frame(
                harness.timestamp(60)?,
                crate::TransportFrameKind::Binary,
                bytes::Bytes::from_static(b"after-exhaustion"),
            ),
            Err(crate::SourceError::SessionNotCurrent)
        ));
        Ok(())
    }

    #[test]
    fn terminal_health_epoch_requires_new_source_epoch_and_generation_to_recover() -> TestResult {
        let mut harness = HealthHarness::new("terminal-health-recovery")?;
        harness.accept_health(10, 20, 30, 1_000)?;
        harness
            .session
            .lease
            .health_epoch
            .store(u64::MAX, std::sync::atomic::Ordering::Release);
        let source_id = harness.session.source_id().clone();
        harness
            .registry
            .entries
            .get_mut(&source_id)
            .and_then(|entry| entry.health_authority.as_mut())
            .ok_or_else(|| std::io::Error::other("qualified health authority missing"))?
            .epoch = u64::MAX;
        harness.set_time(40, 40)?;
        let update = harness.reporter.report(harness.snapshot(31, 1_000)?)?;
        harness.set_time(50, 50)?;
        assert_eq!(
            harness.registry.record_health(&harness.session, update),
            Err(RegistryError::HealthEpochExhausted)
        );
        assert!(matches!(
            harness.reporter.report(harness.snapshot(32, 1_000)?),
            Err(RegistryError::HealthBindingMismatch)
        ));
        assert!(matches!(
            harness.registry.begin_session(
                &harness.registered,
                SessionId::new(SourceIdentifier::try_from("session-retry")?),
                ConnectionGeneration::new(2)?,
                harness.timestamp(50)?,
            ),
            Err(RegistryError::HealthEpochExhausted)
        ));

        let replacement = harness.registry.replace_metadata(
            &harness.registered,
            direct_metadata("terminal-health-recovery", "revision-2")?,
            harness.timestamp(50)?,
        )?;
        assert!(replacement.epoch > harness.registered.epoch);
        let successor = harness.registry.begin_session(
            &replacement,
            SessionId::new(SourceIdentifier::try_from("session-2")?),
            ConnectionGeneration::new(2)?,
            harness.timestamp(50)?,
        )?;
        let capabilities = harness
            .registry
            .take_capture_generation_capabilities(&successor)?;
        let (mut capture_control, _capture_admission, _capture_degradation) =
            capabilities.into_parts();
        capture_control.mark_healthy()?;
        let mut reporter = harness.registry.take_current_health_reporter(&successor)?;
        harness.set_time(60, 60)?;
        let update = reporter.report(healthy_snapshot(
            &successor,
            harness.timestamp(51)?,
            harness.timestamp(1_000)?,
        )?)?;
        harness.set_time(70, 70)?;
        harness.registry.record_health(&successor, update)?;
        harness
            .registry
            .validate_current_authority(&successor)?
            .try_current_lease()?
            .validate_at(harness.timestamp(70)?)?;
        Ok(())
    }

    #[test]
    fn source_epoch_exhaustion_cannot_revive_a_terminal_health_generation() -> TestResult {
        let mut harness = HealthHarness::new("terminal-source-epoch")?;
        harness.accept_health(10, 20, 30, 1_000)?;
        harness.registered.epoch = u64::MAX;
        harness.session.epoch = u64::MAX;
        harness
            .session
            .lease
            .health_epoch
            .store(u64::MAX, std::sync::atomic::Ordering::Release);
        let source_id = harness.session.source_id().clone();
        let entry = harness
            .registry
            .entries
            .get_mut(&source_id)
            .ok_or_else(|| std::io::Error::other("registered source entry missing"))?;
        entry.epoch = u64::MAX;
        entry
            .health_authority
            .as_mut()
            .ok_or_else(|| std::io::Error::other("qualified health authority missing"))?
            .epoch = u64::MAX;
        harness
            .registry
            .history
            .get_mut(&source_id)
            .ok_or_else(|| std::io::Error::other("registered source history missing"))?
            .last_epoch = u64::MAX;

        harness.set_time(40, 40)?;
        let update = harness.reporter.report(harness.snapshot(31, 1_000)?)?;
        harness.set_time(50, 50)?;
        assert_eq!(
            harness.registry.record_health(&harness.session, update),
            Err(RegistryError::HealthEpochExhausted)
        );
        assert!(matches!(
            harness.registry.replace_metadata(
                &harness.registered,
                direct_metadata("terminal-source-epoch", "revision-2")?,
                harness.timestamp(50)?,
            ),
            Err(RegistryError::EpochExhausted)
        ));
        assert!(matches!(
            harness.registry.begin_session(
                &harness.registered,
                SessionId::new(SourceIdentifier::try_from("session-2")?),
                ConnectionGeneration::new(2)?,
                harness.timestamp(50)?,
            ),
            Err(RegistryError::HealthEpochExhausted)
        ));
        assert!(harness.session.lease.is_terminal());
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
    fn retained_authorities_enforce_acceptance_wall_bound_after_partial_rollback() -> TestResult {
        use std::str::FromStr;

        let mut harness = HealthHarness::new("trusted-time-acceptance-wall")?;
        harness.accept_health(10, 20, 30, 1_000)?;
        harness.set_time(30, 30)?;
        let current = harness
            .registry
            .validate_current_authority(&harness.session)?;
        let lease = current.try_current_lease()?;
        let scope = current.validate_live_scope(
            &VenueId::try_from("coinbase")?,
            InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?,
            LiveEventClass::Trade,
            None,
        )?;
        let queued = scope.queued_authority_for_test();
        let event_at = harness.timestamp(10)?;

        lease.validate_at(event_at)?;
        scope.validate_at(event_at)?;
        queued.validate_at(event_at)?;

        harness.set_time(25, 40)?;
        assert_eq!(
            lease.validate_at(event_at),
            Err(RegistryError::HealthNotQualified)
        );
        assert_eq!(
            scope.validate_at(event_at),
            Err(RegistryError::HealthNotQualified)
        );
        assert_eq!(
            queued.validate_at(event_at),
            Err(RegistryError::HealthNotQualified)
        );

        harness.set_time(40, 50)?;
        lease.validate_at(event_at)?;
        scope.validate_at(event_at)?;
        queued.validate_at(event_at)?;
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
    fn distinct_source_capacity_is_enforced_before_any_registration_mutation() -> TestResult {
        let at = Timestamp::from_unix_nanos(1);
        let mut registry = AuthoritativeSourceRegistry::try_new()?;
        let registered = registry.register(direct_metadata("capacity-source", "revision-1")?, at)?;
        for index in 1..MAX_AUTHORITY_SOURCES {
            let source_id = SourceId::try_from(format!("capacity-history-{index}"))?;
            registry.history.insert(
                source_id,
                SourceAuthorityHistory {
                    used_revisions: vec![MetadataRevision::new(SourceIdentifier::try_from(
                        format!("revision-{index}"),
                    )?)],
                    last_epoch: 1,
                    generation_high_water: None,
                },
            );
        }
        assert_eq!(registry.history.len(), MAX_AUTHORITY_SOURCES);

        let replacement = registry.replace_metadata(
            &registered,
            direct_metadata("capacity-source", "revision-2")?,
            at,
        )?;
        assert_eq!(replacement.revision.as_source_identifier().as_str(), "revision-2");
        registry.revoke(&replacement, at)?;
        let restored_state = registry.export_authority_state()?;
        let mut registry =
            AuthoritativeSourceRegistry::try_new_with_authority_state(restored_state)?;
        registry.register(direct_metadata("capacity-source", "revision-3")?, at)?;
        let before = registry.export_authority_state()?;
        let budget_policies_before = registry.budgets.policies();
        let entries_before = registry.entries.len();

        assert!(matches!(
            registry.register(
                direct_metadata("capacity-overflow-source", "revision-1")?,
                at,
            ),
            Err(RegistryError::AuthorityStateCapacity)
        ));
        assert_eq!(registry.export_authority_state()?, before);
        assert_eq!(registry.budgets.policies(), budget_policies_before);
        assert_eq!(registry.entries.len(), entries_before);
        assert!(!registry
            .entries
            .contains_key(&SourceId::try_from("capacity-overflow-source")?));
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
