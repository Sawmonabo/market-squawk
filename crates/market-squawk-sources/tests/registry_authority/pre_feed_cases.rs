#[test]
fn pre_feed_current_leases_are_deadline_capture_health_and_registry_bound() -> TestResult {
    fn record_qualified_health(
        registry: &mut AuthoritativeSourceRegistry,
        session: &market_squawk_sources::CurrentSourceSession,
        reporter: &mut market_squawk_sources::CurrentHealthReporter,
        observed_at: i64,
    ) -> TestResult {
        let observed = Timestamp::from_unix_nanos(observed_at);
        let valid_until = observed.checked_add_nanos(10_000_000_000)?;
        let health = SourceHealthSnapshot::try_new(
            session,
            observed,
            ConnectionLiveness::Live {
                last_activity_at: observed,
            },
            Some(observed),
            Some(observed),
            Some(observed),
            FreshnessPolicy::try_new(
                5_000_000_000,
                1_000_000_000,
                2_000_000_000,
                1_000_000_000,
                100_000_000,
            )?,
            StreamIntegrityState::Healthy,
            CaptureIntegrityState::Healthy,
            AuthorizationHealth::Valid {
                evidence: exact_evidence(21),
                valid_until,
            },
            CoverageHealth::Sufficient {
                evidence: exact_evidence(22),
                provider_product: ProviderProduct::new(source_identifier("direct-product")?),
                provider_channel: ProviderChannel::new(source_identifier("trades")?),
                valid_until,
            },
            BudgetHealth::Available,
            None,
            Vec::new(),
        )?;
        let update = reporter.report(health)?;
        registry.record_health(session, update)?;
        Ok(())
    }

    fn setup() -> TestResult<(
        AuthoritativeSourceRegistry,
        market_squawk_sources::CurrentSourceSession,
        market_squawk_sources::CurrentHealthReporter,
        market_squawk_sources::CaptureDegradationCapability,
        market_squawk_sources::CaptureAdmissionIssuer,
        market_squawk_sources::RawFrameFactory,
    )> {
        let mut registry = AuthoritativeSourceRegistry::try_new()?;
        let registered = registry.register(
            direct_metadata("source-a", "revision-a", 0, None)?,
            Timestamp::from_unix_nanos(1),
        )?;
        let session = registry.begin_session(
            &registered,
            SessionId::new(source_identifier("session-a")?),
            ConnectionGeneration::new(1)?,
            Timestamp::from_unix_nanos(1),
        )?;
        let capabilities = registry.take_capture_generation_capabilities(&session)?;
        let (mut capture_control, admission, degrade) = capabilities.into_parts();
        capture_control.mark_healthy()?;
        let reporter = registry.take_current_health_reporter(&session)?;
        let frames = registry.take_raw_frame_factory(&session)?;
        Ok((registry, session, reporter, degrade, admission, frames))
    }

    let (
        mut first,
        first_session,
        mut first_reporter,
        first_degrade,
        _first_admission,
        _first_frames,
    ) = setup()?;
    let first_at = now_timestamp()?;
    record_qualified_health(
        &mut first,
        &first_session,
        &mut first_reporter,
        first_at.unix_nanos(),
    )?;
    let lease = {
        let current = first.validate_current_authority(&first_session)?;
        let lease = current.try_current_lease()?;
        assert_eq!(lease.valid_from(), first_at);
        assert!(lease.validate_at(first_at.checked_sub_nanos(1)?).is_err());
        lease
    };
    assert!(lease.validate_at(first_at).is_ok());
    assert!(lease.validate_at(first_at.checked_sub_nanos(1)?).is_err());
    let refreshed_at = next_timestamp_after(first_at)?;
    record_qualified_health(
        &mut first,
        &first_session,
        &mut first_reporter,
        refreshed_at.unix_nanos(),
    )?;
    assert!(lease.validate_at(refreshed_at).is_err());
    let refreshed = first
        .validate_current_authority(&first_session)?
        .try_current_lease()?;
    assert!(refreshed.health_epoch() > lease.health_epoch());

    let (
        mut second,
        second_session,
        mut second_reporter,
        _second_degrade,
        _second_admission,
        _second_frames,
    ) = setup()?;
    let second_at = now_timestamp()?;
    record_qualified_health(
        &mut second,
        &second_session,
        &mut second_reporter,
        second_at.unix_nanos(),
    )?;
    let second_lease = second
        .validate_current_authority(&second_session)?
        .try_current_lease()?;
    assert!(!refreshed.shares_registry_lineage_with(&second_lease));
    assert!(
        !refreshed
            .binding()
            .shares_allocation_with(second_lease.binding())
    );

    let (
        mut exiting,
        exiting_session,
        mut exiting_reporter,
        _exiting_degrade,
        exiting_admission,
        mut exiting_frames,
    ) = setup()?;
    let exiting_at = now_timestamp()?;
    record_qualified_health(
        &mut exiting,
        &exiting_session,
        &mut exiting_reporter,
        exiting_at.unix_nanos(),
    )?;
    let exiting_lease = exiting
        .validate_current_authority(&exiting_session)?
        .try_current_lease()?;
    assert!(exiting_lease.validate_at(exiting_at).is_ok());
    let exiting_frame = exiting_frames.try_frame(
        exiting_at,
        TransportFrameKind::Binary,
        Bytes::from_static(b"before-registry-exit"),
    )?;
    exiting_session.validate_live_frame(&exiting_frame)?;
    exiting_admission.validate_active(&exiting_frame)?;
    drop(exiting);
    assert_eq!(
        exiting_lease.validate_at(exiting_at),
        Err(RegistryError::HealthNotQualified)
    );
    assert!(matches!(
        exiting_session.validate_live_frame(&exiting_frame),
        Err(RegistryError::SessionNotCurrent)
    ));
    assert_eq!(
        exiting_admission.validate_active(&exiting_frame),
        Err(market_squawk_sources::CaptureAdmissionError::NotHealthy)
    );
    assert!(matches!(
        exiting_frames.try_frame(
            Timestamp::from_unix_nanos(2),
            TransportFrameKind::Binary,
            Bytes::from_static(b"after-registry-exit"),
        ),
        Err(market_squawk_sources::SourceError::SessionNotCurrent)
    ));

    first_degrade.mark_incomplete();
    assert!(refreshed.validate_at(refreshed_at).is_err());
    Ok(())
}

