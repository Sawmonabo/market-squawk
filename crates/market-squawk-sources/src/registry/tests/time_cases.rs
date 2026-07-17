#[test]
fn frame_factory_owns_receipt_time_and_accepts_equal_wall_progress() -> TestResult {
    let mut harness = HealthHarness::new("receipt-source-owned")?;
    let mut factory = harness
        .registry
        .take_raw_frame_factory(&harness.session)?;
    harness.set_time(0, 1)?;
    let first = factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"one"))?;
    harness.set_time(0, 2)?;
    let second = factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"two"))?;

    assert_eq!(first.received_at(), harness.wall_origin);
    assert_eq!(second.received_at(), harness.wall_origin);
    assert_eq!(first.frame_id().get(), 1);
    assert_eq!(second.frame_id().get(), 2);
    Ok(())
}

#[test]
fn either_clock_component_rollback_latches_permanently() -> TestResult {
    for (source, rollback_wall, rollback_monotonic) in
        [("wall-rollback", 9_i64, 11_u64), ("monotonic-rollback", 11, 9)]
    {
        let mut harness = HealthHarness::new(source)?;
        let mut factory = harness
            .registry
            .take_raw_frame_factory(&harness.session)?;
        harness.set_time(10, 10)?;
        factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"before"))?;
        harness.set_time(rollback_wall, rollback_monotonic)?;
        assert_eq!(
            factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"rollback")),
            Err(SourceError::TrustedTimeDiscontinuity)
        );
        harness.set_time(20, 20)?;
        assert_eq!(
            factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"after")),
            Err(SourceError::TrustedTimeDiscontinuity)
        );
        assert_eq!(
            harness.session.validate_current_lease(),
            Err(RegistryError::SessionNotCurrent)
        );
    }
    Ok(())
}

#[test]
fn clock_source_failure_latches_permanently() -> TestResult {
    let mut harness = HealthHarness::new("clock-source-failure")?;
    let mut factory = harness
        .registry
        .take_raw_frame_factory(&harness.session)?;
    harness.clock.fail()?;
    assert_eq!(
        factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"failure")),
        Err(SourceError::TrustedTimeUnavailable)
    );
    harness.set_time(1, 1)?;
    assert_eq!(
        factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"recovered")),
        Err(SourceError::TrustedTimeDiscontinuity)
    );
    Ok(())
}

#[test]
fn deserialized_frame_has_no_live_continuity_authority() -> TestResult {
    let mut harness = HealthHarness::new("missing-frame-continuity")?;
    let mut factory = harness
        .registry
        .take_raw_frame_factory(&harness.session)?;
    harness.set_time(1, 1)?;
    let frame = factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"frame"))?;
    let mut reconstructed = frame.clone();
    reconstructed.strip_trusted_receipt_for_test();

    assert!(matches!(
        harness.session.validate_live_frame(&reconstructed),
        Err(RegistryError::TrustedReceiptContinuityMismatch)
    ));
    Ok(())
}

#[test]
fn retained_capture_authority_and_same_registry_replacement_reject_after_latch() -> TestResult {
    let wall = Timestamp::from_unix_nanos(1_000_000_000);
    let monotonic = RegistryMonotonicInstant::from_nanos(0);
    let clock = Arc::new(ManualRegistryClock::new(TrustedRegistryTime::new(
        wall, monotonic,
    )));
    let mut registry =
        AuthoritativeSourceRegistry::try_new_ephemeral_with_authority_state_and_clock_for_diagnostics(
            super::super::RegistryAuthorityState::empty(),
            clock.clone(),
        )?;
    let registered = registry.register(direct_metadata("retained-time", "revision-1")?, wall)?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(SourceIdentifier::try_from("session-1")?),
        ConnectionGeneration::new(1)?,
        wall,
    )?;
    let capabilities = registry.take_capture_generation_capabilities(&session)?;
    let (mut initialization, admission, _degradation) = capabilities.into_parts();
    initialization.mark_healthy()?;
    let mut factory = registry.take_raw_frame_factory(&session)?;
    clock.set(TrustedRegistryTime::new(
        wall.checked_add_nanos(10)?,
        monotonic
            .checked_add(Duration::from_nanos(10))
            .ok_or_else(|| std::io::Error::other("manual monotonic overflow"))?,
    ))?;
    let before = factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"before"))?;
    clock.set(TrustedRegistryTime::new(
        wall.checked_add_nanos(9)?,
        monotonic
            .checked_add(Duration::from_nanos(11))
            .ok_or_else(|| std::io::Error::other("manual monotonic overflow"))?,
    ))?;
    assert_eq!(
        factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"rollback")),
        Err(SourceError::TrustedTimeDiscontinuity)
    );
    assert_eq!(
        admission.preflight(&before),
        Err(crate::CaptureAdmissionError::NotHealthy)
    );
    assert!(matches!(
        registry.replace_metadata(
            &registered,
            direct_metadata("retained-time", "revision-2")?,
            wall.checked_add_nanos(20)?,
        ),
        Err(RegistryError::AuthorityTimeDiscontinuous)
    ));

    let mut fresh = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    assert!(fresh
        .register(direct_metadata("fresh-time", "revision-1")?, wall)
        .is_ok());
    Ok(())
}

#[test]
fn durable_time_fault_preserves_in_use_restart_rejection() -> TestResult {
    let store = Arc::new(FailingAuthorityStore::default());
    let clock = Arc::new(ManualRegistryClock::new(TrustedRegistryTime::new(
        Timestamp::from_unix_nanos(1_000_000_000),
        RegistryMonotonicInstant::from_nanos(0),
    )));
    let store_for_registry: Arc<dyn AuthorityStateStore> = store.clone();
    let raw_clock: Arc<dyn RawRegistryClockSource> = clock.clone();
    let mut registry = durable_registry_with_test_store_and_clock(store_for_registry, raw_clock)?;
    let registered = registry.register(
        direct_metadata("durable-time-fault", "revision-1")?,
        Timestamp::from_unix_nanos(1_000_000_000),
    )?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(SourceIdentifier::try_from("session-1")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1_000_000_000),
    )?;
    let mut factory = registry.take_raw_frame_factory(&session)?;
    clock.fail()?;
    assert_eq!(
        factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"fault")),
        Err(SourceError::TrustedTimeUnavailable)
    );
    drop(factory);
    drop(registry);

    let store_for_restart: Arc<dyn AuthorityStateStore> = store;
    assert!(matches!(
        durable_registry_with_test_store(store_for_restart),
        Err(RegistryError::UncleanAuthorityPredecessor)
    ));
    Ok(())
}
