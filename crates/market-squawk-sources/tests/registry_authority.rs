mod common;

use std::num::NonZeroU64;

use bytes::Bytes;
use market_squawk_domain::{
    CaptureAuthorityBundle, CaptureDegradation, CaptureIntegrityState, ConnectionGeneration,
    CoverageConsolidation, CoverageDelay, DeliveryEvidence, ProviderChannel, ProviderProduct,
    RawCaptureFrameView, StreamIntegrityState, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationHealth, BudgetDecision, BudgetHealth,
    ConnectionLiveness, CoverageHealth, FreshnessPolicy, RawMarketFrame, RegistryAuthorityState,
    RegistryError, RetryAfter, SessionId, SourceHealthSnapshot, TransportFrameKind,
};
use static_assertions::{assert_impl_all, assert_not_impl_any};

use common::{
    TestResult, direct_metadata, direct_metadata_with_instruments, exact_evidence,
    source_identifier,
};

assert_not_impl_any!(market_squawk_sources::RegisteredSource: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_sources::CurrentSourceSession: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_impl_all!(market_squawk_sources::CurrentDecodedProviderBatch: Send);
assert_not_impl_any!(market_squawk_sources::CurrentDecodedProviderBatch: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_impl_all!(market_squawk_sources::CurrentDecodedProviderBatches: Send);
assert_not_impl_any!(market_squawk_sources::CurrentDecodedProviderBatches: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_sources::CurrentFrameEvidence: serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_sources::CaptureAdmissionIssuer: Clone, Sync, serde::Serialize);
assert_not_impl_any!(market_squawk_sources::CaptureInitializationControl: Clone, Sync, serde::Serialize);
assert_not_impl_any!(market_squawk_sources::CaptureGenerationCapabilities: Clone, Sync, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_sources::CurrentHealthReporter: Clone, Sync, serde::Serialize);
assert_not_impl_any!(market_squawk_sources::CurrentHealthUpdate: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_sources::RawFrameFactory: Clone, Sync, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_sources::CurrentCoveragePolicy: serde::Serialize, serde::de::DeserializeOwned);
assert_impl_all!(market_squawk_sources::RawMarketFrame: RawCaptureFrameView);
assert_impl_all!(market_squawk_sources::CaptureGenerationCapabilities: CaptureAuthorityBundle);

fn direct_metadata_for_provider(
    source: &str,
    revision: &str,
    provider: &str,
) -> TestResult<market_squawk_sources::SourceMetadata> {
    let mut wire = serde_json::to_value(direct_metadata(source, revision, 0, None)?)?;
    let metadata = wire
        .as_object_mut()
        .ok_or("source metadata did not serialize as an object")?;
    metadata.insert("provider".to_owned(), serde_json::json!(provider));
    let budget = metadata
        .get_mut("budget")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("source metadata budget was absent")?;
    let scope = budget
        .get_mut("scope")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("source metadata budget scope was absent")?;
    scope.insert("provider".to_owned(), serde_json::json!(provider));
    Ok(serde_json::from_value(wire)?)
}

fn healthy_snapshot(
    session: &market_squawk_sources::CurrentSourceSession,
    observed_at: i64,
) -> TestResult<SourceHealthSnapshot> {
    Ok(SourceHealthSnapshot::try_new(
        session,
        Timestamp::from_unix_nanos(observed_at),
        ConnectionLiveness::Live {
            last_activity_at: Timestamp::from_unix_nanos(observed_at),
        },
        Some(Timestamp::from_unix_nanos(observed_at)),
        Some(Timestamp::from_unix_nanos(observed_at)),
        Some(Timestamp::from_unix_nanos(observed_at)),
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
            evidence: exact_evidence(31),
            valid_until: Timestamp::from_unix_nanos(100),
        },
        CoverageHealth::Sufficient {
            evidence: exact_evidence(32),
            provider_product: ProviderProduct::new(source_identifier("direct-product")?),
            provider_channel: ProviderChannel::new(source_identifier("trades")?),
            valid_until: Timestamp::from_unix_nanos(100),
        },
        BudgetHealth::Available,
        None,
        Vec::new(),
    )?)
}

#[test]
fn domain_capture_bundle_retains_exact_registry_identity_and_one_way_health() -> TestResult {
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
    let bundle = registry.take_capture_generation_capabilities(&session)?;
    let identity = bundle.identity();
    assert_eq!(identity.source_id().as_str(), "source-a");
    assert_eq!(
        identity.metadata_revision().as_source_identifier().as_str(),
        "revision-a"
    );
    assert_eq!(identity.session_identifier().as_str(), "session-a");
    assert_eq!(identity.connection_generation().get(), 1);

    let (mut initializer, _admission, degradation) = bundle.into_parts();
    assert_eq!(degradation.integrity(), CaptureIntegrityState::Incomplete);
    market_squawk_domain::CaptureInitializer::mark_healthy(&mut initializer)?;
    assert_eq!(degradation.integrity(), CaptureIntegrityState::Healthy);
    degradation.mark_incomplete();
    assert_eq!(degradation.integrity(), CaptureIntegrityState::Incomplete);
    Ok(())
}

#[test]
fn raw_frame_view_reports_exact_generation_local_identity_and_deep_bound() -> TestResult {
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
    let mut factory = registry.take_raw_frame_factory(&session)?;
    let frame = factory.try_frame(
        Timestamp::from_unix_nanos(2),
        TransportFrameKind::Binary,
        Bytes::from_static(b"frame"),
    )?;

    assert_eq!(RawCaptureFrameView::source_id(&frame).as_str(), "source-a");
    assert_eq!(
        RawCaptureFrameView::session_identifier(&frame).as_str(),
        "session-a"
    );
    assert_eq!(RawCaptureFrameView::frame_ordinal(&frame).get(), 1);
    assert_eq!(RawCaptureFrameView::payload(&frame), b"frame");
    assert!(RawCaptureFrameView::retained_bytes(&frame) >= frame.retained_payload_bytes());
    Ok(())
}

#[test]
fn handles_reject_registry_transplant_and_session_resurrection() -> TestResult {
    let mut first = AuthoritativeSourceRegistry::try_new()?;
    let second = AuthoritativeSourceRegistry::try_new()?;
    let registered = first.register(
        direct_metadata("source-a", "rev-a", 0, Some(100))?,
        Timestamp::from_unix_nanos(1),
    )?;
    assert!(matches!(
        second.validate_registered(&registered, Timestamp::from_unix_nanos(1)),
        Err(RegistryError::HandleTransplanted)
    ));
    let session = first.begin_session(
        &registered,
        SessionId::new(source_identifier("session-a")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    first.end_session(&session, Timestamp::from_unix_nanos(100))?;
    assert!(matches!(
        first.begin_session(
            &registered,
            SessionId::new(source_identifier("session-b")?),
            ConnectionGeneration::new(1)?,
            Timestamp::from_unix_nanos(2),
        ),
        Err(RegistryError::GenerationNotAdvanced)
    ));
    let next = first.begin_session(
        &registered,
        SessionId::new(source_identifier("session-b")?),
        ConnectionGeneration::new(2)?,
        Timestamp::from_unix_nanos(2),
    )?;
    assert!(matches!(
        first.validate_session(&session, Timestamp::from_unix_nanos(2)),
        Err(RegistryError::SessionNotCurrent)
    ));
    first.validate_session(&next, Timestamp::from_unix_nanos(2))?;
    Ok(())
}

#[test]
fn raw_frame_factory_is_once_issued_and_fails_after_session_end() -> TestResult {
    let mut registry = AuthoritativeSourceRegistry::try_new()?;
    let registered = registry.register(
        direct_metadata("source-a", "rev-a", 0, Some(100))?,
        Timestamp::from_unix_nanos(1),
    )?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(source_identifier("session-a")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    assert!(matches!(
        registry.take_raw_frame_factory(&session),
        Err(RegistryError::RawFrameFactoryAlreadyTaken)
    ));
    let frame = frames.try_frame(
        Timestamp::from_unix_nanos(2),
        TransportFrameKind::Binary,
        Bytes::from_static(b"first"),
    )?;
    session.validate_live_frame(&frame)?;
    registry.end_session(&session, Timestamp::from_unix_nanos(3))?;
    assert!(matches!(
        frames.try_frame(
            Timestamp::from_unix_nanos(4),
            TransportFrameKind::Binary,
            Bytes::from_static(b"late"),
        ),
        Err(market_squawk_sources::SourceError::SessionNotCurrent)
    ));
    Ok(())
}

#[test]
fn adjacent_revision_cutover_and_expired_cleanup_are_administratively_valid() -> TestResult {
    let mut registry = AuthoritativeSourceRegistry::try_new()?;
    let first = registry.register(
        direct_metadata("source-a", "rev-a", 0, Some(100))?,
        Timestamp::from_unix_nanos(50),
    )?;
    let second = registry.replace_metadata(
        &first,
        direct_metadata("source-a", "rev-b", 100, Some(200))?,
        Timestamp::from_unix_nanos(100),
    )?;
    assert!(matches!(
        registry.validate_registered(&first, Timestamp::from_unix_nanos(100)),
        Err(RegistryError::StaleHandle)
    ));
    registry.validate_registered(&second, Timestamp::from_unix_nanos(100))?;
    assert!(matches!(
        registry.replace_metadata(
            &second,
            direct_metadata("source-a", "rev-a", 200, Some(300))?,
            Timestamp::from_unix_nanos(200),
        ),
        Err(RegistryError::RevisionAlreadyUsed)
    ));
    registry.revoke(&second, Timestamp::from_unix_nanos(250))?;
    Ok(())
}

#[test]
fn two_sources_with_one_scope_share_concurrency_and_cooldown() -> TestResult {
    let mut registry = AuthoritativeSourceRegistry::try_new()?;
    let first = registry.register(
        direct_metadata("source-a", "rev-a", 0, None)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let second = registry.register(
        direct_metadata("source-b", "rev-b", 0, None)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let first_budget = first.budget().ok_or("missing first budget")?;
    let second_budget = second.budget().ok_or("missing second budget")?;
    let permit = match first_budget.try_acquire() {
        BudgetDecision::Ready(permit) => permit,
        other => return Err(format!("unexpected first budget decision: {other:?}").into()),
    };
    assert!(matches!(
        second_budget.try_acquire(),
        BudgetDecision::Unavailable(_)
    ));
    permit.release();
    assert!(matches!(
        second_budget.try_acquire(),
        BudgetDecision::Ready(_)
    ));
    Ok(())
}

#[test]
fn process_coordinator_interns_registry_and_restored_budget_allocations() -> TestResult {
    let provider = "process-budget-interner-provider";
    let mut first_registry = AuthoritativeSourceRegistry::try_new()?;
    let first = first_registry.register(
        direct_metadata_for_provider("interner-a", "interner-rev-a", provider)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let first_budget = first.budget().ok_or("first coordinated budget missing")?;

    let mut second_registry = AuthoritativeSourceRegistry::try_new()?;
    let second = second_registry.register(
        direct_metadata_for_provider("interner-b", "interner-rev-b", provider)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let second_budget = second.budget().ok_or("second coordinated budget missing")?;
    assert!(first_budget.shares_allocation_with(second_budget));

    let state = first_registry.export_authority_state()?;
    let mut restored = AuthoritativeSourceRegistry::try_new_with_authority_state(state)?;
    let restored_source = restored.register(
        direct_metadata_for_provider("interner-c", "interner-rev-c", provider)?,
        Timestamp::from_unix_nanos(1),
    )?;
    assert!(
        first_budget.shares_allocation_with(
            restored_source
                .budget()
                .ok_or("restored coordinated budget missing")?
        )
    );
    Ok(())
}

#[test]
fn process_coordinator_rejects_conflicting_restored_policy() -> TestResult {
    let provider = "process-budget-conflict-provider";
    let mut owner = AuthoritativeSourceRegistry::try_new()?;
    let _registered = owner.register(
        direct_metadata_for_provider("conflict-a", "conflict-rev-a", provider)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let state = owner.export_authority_state()?;
    let mut wire = serde_json::to_value(state)?;
    let policies = wire
        .as_object_mut()
        .and_then(|object| object.get_mut("budget_policies"))
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("authority state budget policies missing")?;
    let policy = policies
        .first_mut()
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("authority state policy missing")?;
    policy.insert("requests_per_window".to_owned(), serde_json::json!(11));
    let conflicting: RegistryAuthorityState = serde_json::from_value(wire)?;

    assert!(matches!(
        AuthoritativeSourceRegistry::try_new_with_authority_state(conflicting),
        Err(RegistryError::BudgetCoordinator)
    ));
    Ok(())
}

#[test]
fn coordinated_budget_proof_controls_health_and_queued_authority() -> TestResult {
    let mut registry = AuthoritativeSourceRegistry::try_new()?;
    let registered = registry.register(
        direct_metadata_for_provider(
            "budget-source",
            "budget-revision",
            "budget-authority-test-provider",
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(source_identifier("budget-session")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let capabilities = registry.take_capture_generation_capabilities(&session)?;
    let (mut capture_control, _admission, _degradation) = capabilities.into_parts();
    capture_control.mark_healthy()?;
    let mut reporter = registry.take_current_health_reporter(&session)?;
    let budget = session.budget().ok_or("remote session budget missing")?;

    let permit = match budget.try_acquire() {
        BudgetDecision::Ready(permit) => permit,
        other => return Err(format!("unexpected budget decision: {other:?}").into()),
    };
    registry.record_health(&session, reporter.report(healthy_snapshot(&session, 2)?)?)?;
    assert!(matches!(
        registry.validate_current_authority(&session, Timestamp::from_unix_nanos(2)),
        Err(RegistryError::HealthNotQualified)
    ));
    permit.release();

    registry.record_health(&session, reporter.report(healthy_snapshot(&session, 3)?)?)?;
    let queued = registry
        .validate_current_authority(&session, Timestamp::from_unix_nanos(3))?
        .try_current_lease(Timestamp::from_unix_nanos(3))?;
    assert!(queued.validate_at(Timestamp::from_unix_nanos(3)).is_ok());

    assert!(matches!(
        budget.apply_retry_after(RetryAfter::Delay(
            NonZeroU64::new(60_000_000_000).ok_or("nonzero retry delay")?
        )),
        BudgetDecision::WaitUntil(_)
    ));
    assert_eq!(
        queued.validate_at(Timestamp::from_unix_nanos(3)),
        Err(RegistryError::HealthNotQualified)
    );
    registry.record_health(&session, reporter.report(healthy_snapshot(&session, 4)?)?)?;
    assert!(matches!(
        registry.validate_current_authority(&session, Timestamp::from_unix_nanos(4)),
        Err(RegistryError::HealthNotQualified)
    ));

    assert!(matches!(budget.disable(), BudgetDecision::Unavailable(_)));
    registry.record_health(&session, reporter.report(healthy_snapshot(&session, 5)?)?)?;
    assert!(matches!(
        registry.validate_current_authority(&session, Timestamp::from_unix_nanos(5)),
        Err(RegistryError::HealthNotQualified)
    ));
    Ok(())
}

#[test]
fn replayed_frames_lose_transient_authority_and_ended_lease_stays_invalid() -> TestResult {
    let mut registry = AuthoritativeSourceRegistry::try_new()?;
    let registered = registry.register(
        direct_metadata("source-a", "rev-a", 0, None)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(source_identifier("session-a")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let frame = frames.try_frame(
        Timestamp::from_unix_nanos(2),
        TransportFrameKind::Binary,
        Bytes::from_static(b"payload"),
    )?;
    session.validate_live_frame(&frame)?;
    let replayed: RawMarketFrame = serde_json::from_str(&serde_json::to_string(&frame)?)?;
    assert!(matches!(
        session.validate_live_frame(&replayed),
        Err(RegistryError::HandleTransplanted)
    ));
    registry.end_session(&session, Timestamp::from_unix_nanos(3))?;
    assert!(matches!(
        session.validate_current_lease(),
        Err(RegistryError::SessionNotCurrent)
    ));
    Ok(())
}

#[test]
fn authority_state_round_trip_blocks_revision_and_generation_reuse_after_restart() -> TestResult {
    let mut first = AuthoritativeSourceRegistry::try_new()?;
    let registered = first.register(
        direct_metadata("source-a", "rev-a", 0, None)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let _session = first.begin_session(
        &registered,
        SessionId::new(source_identifier("session-a")?),
        ConnectionGeneration::new(5)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let state = first.export_authority_state()?;
    let wire = serde_json::to_string(&state)?;
    let restored: RegistryAuthorityState = serde_json::from_str(&wire)?;
    let mut restarted = AuthoritativeSourceRegistry::try_new_with_authority_state(restored)?;
    assert!(matches!(
        restarted.register(
            direct_metadata("source-a", "rev-a", 0, None)?,
            Timestamp::from_unix_nanos(2),
        ),
        Err(RegistryError::RevisionAlreadyUsed)
    ));
    let next = restarted.register(
        direct_metadata("source-a", "rev-b", 0, None)?,
        Timestamp::from_unix_nanos(2),
    )?;
    assert!(matches!(
        restarted.begin_session(
            &next,
            SessionId::new(source_identifier("session-a")?),
            ConnectionGeneration::new(5)?,
            Timestamp::from_unix_nanos(2),
        ),
        Err(RegistryError::GenerationNotAdvanced)
    ));
    let future = wire.replace("\"schema_version\":1", "\"schema_version\":2");
    assert!(serde_json::from_str::<RegistryAuthorityState>(&future).is_err());
    Ok(())
}

#[test]
fn pre_feed_current_leases_are_deadline_capture_health_and_registry_bound() -> TestResult {
    fn record_qualified_health(
        registry: &mut AuthoritativeSourceRegistry,
        session: &market_squawk_sources::CurrentSourceSession,
        reporter: &mut market_squawk_sources::CurrentHealthReporter,
        observed_at: i64,
    ) -> TestResult {
        let health = SourceHealthSnapshot::try_new(
            session,
            Timestamp::from_unix_nanos(observed_at),
            ConnectionLiveness::Live {
                last_activity_at: Timestamp::from_unix_nanos(observed_at),
            },
            Some(Timestamp::from_unix_nanos(observed_at)),
            Some(Timestamp::from_unix_nanos(observed_at)),
            Some(Timestamp::from_unix_nanos(observed_at)),
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
                valid_until: Timestamp::from_unix_nanos(12),
            },
            CoverageHealth::Sufficient {
                evidence: exact_evidence(22),
                provider_product: ProviderProduct::new(source_identifier("direct-product")?),
                provider_channel: ProviderChannel::new(source_identifier("trades")?),
                valid_until: Timestamp::from_unix_nanos(12),
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
    record_qualified_health(&mut first, &first_session, &mut first_reporter, 2)?;
    let lease = {
        let current =
            first.validate_current_authority(&first_session, Timestamp::from_unix_nanos(2))?;
        let lease = current.try_current_lease(Timestamp::from_unix_nanos(12))?;
        assert_eq!(lease.valid_until(), Timestamp::from_unix_nanos(12));
        assert!(
            current
                .try_current_lease(Timestamp::from_unix_nanos(13))
                .is_err()
        );
        lease
    };
    assert!(lease.validate_at(Timestamp::from_unix_nanos(12)).is_ok());
    assert!(lease.validate_at(Timestamp::from_unix_nanos(13)).is_err());
    record_qualified_health(&mut first, &first_session, &mut first_reporter, 3)?;
    assert!(lease.validate_at(Timestamp::from_unix_nanos(3)).is_err());
    let refreshed = first
        .validate_current_authority(&first_session, Timestamp::from_unix_nanos(3))?
        .try_current_lease(Timestamp::from_unix_nanos(3))?;
    assert!(refreshed.health_epoch() > lease.health_epoch());

    let (
        mut second,
        second_session,
        mut second_reporter,
        _second_degrade,
        _second_admission,
        _second_frames,
    ) = setup()?;
    record_qualified_health(&mut second, &second_session, &mut second_reporter, 2)?;
    let second_lease = second
        .validate_current_authority(&second_session, Timestamp::from_unix_nanos(2))?
        .try_current_lease(Timestamp::from_unix_nanos(2))?;
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
    record_qualified_health(&mut exiting, &exiting_session, &mut exiting_reporter, 2)?;
    let exiting_lease = exiting
        .validate_current_authority(&exiting_session, Timestamp::from_unix_nanos(2))?
        .try_current_lease(Timestamp::from_unix_nanos(2))?;
    assert!(
        exiting_lease
            .validate_at(Timestamp::from_unix_nanos(2))
            .is_ok()
    );
    let exiting_frame = exiting_frames.try_frame(
        Timestamp::from_unix_nanos(2),
        TransportFrameKind::Binary,
        Bytes::from_static(b"before-registry-exit"),
    )?;
    exiting_session.validate_live_frame(&exiting_frame)?;
    exiting_admission.validate_active(&exiting_frame)?;
    drop(exiting);
    assert_eq!(
        exiting_lease.validate_at(Timestamp::from_unix_nanos(2)),
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
    assert!(
        refreshed
            .validate_at(Timestamp::from_unix_nanos(3))
            .is_err()
    );
    Ok(())
}

#[test]
fn current_authority_is_scoped_by_venue_instrument_event_and_depth() -> TestResult {
    use std::str::FromStr;

    use market_squawk_domain::{
        AggressorSide, InstrumentId, IntegrityRule, LiveEventClass, RuleVersion, SequenceNumber,
        VenueId,
    };
    use market_squawk_sources::{
        DecodedProviderBatch, DecoderEvidence, ProviderAggressorEvidence, ProviderChecksumEvidence,
        ProviderDecimalLexeme, ProviderNormalizedObservation, ProviderObservationPayload,
        ProviderPrice, ProviderQuantity, ProviderSequenceEvidence, ProviderSnapshotEvidence,
        ProviderTimestampEvidence,
    };

    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let mut covered_instruments = (1_u128..4_096)
        .map(|value| InstrumentId::from_str(&format!("{value:032x}")))
        .collect::<Result<Vec<_>, _>>()?;
    let other_instrument = *covered_instruments
        .first()
        .ok_or("maximum-universe fixture must not be empty")?;
    covered_instruments.push(instrument);
    let mut registry = AuthoritativeSourceRegistry::try_new()?;
    let registered = registry.register(
        direct_metadata_with_instruments("source-a", "revision-a", 0, None, covered_instruments)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(source_identifier("session-a")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let capabilities = registry.take_capture_generation_capabilities(&session)?;
    let (mut capture_control, mut capture_admission, _degrade) = capabilities.into_parts();
    capture_control.mark_healthy()?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let mut health_reporter = registry.take_current_health_reporter(&session)?;
    assert!(matches!(
        registry.validate_current_authority(&session, Timestamp::from_unix_nanos(2)),
        Err(RegistryError::HealthNotQualified)
    ));
    let health = SourceHealthSnapshot::try_new(
        &session,
        Timestamp::from_unix_nanos(2),
        ConnectionLiveness::Live {
            last_activity_at: Timestamp::from_unix_nanos(2),
        },
        Some(Timestamp::from_unix_nanos(2)),
        Some(Timestamp::from_unix_nanos(2)),
        Some(Timestamp::from_unix_nanos(2)),
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
            evidence: exact_evidence(11),
            valid_until: Timestamp::from_unix_nanos(12),
        },
        CoverageHealth::Sufficient {
            evidence: exact_evidence(12),
            provider_product: ProviderProduct::new(source_identifier("direct-product")?),
            provider_channel: ProviderChannel::new(source_identifier("trades")?),
            valid_until: Timestamp::from_unix_nanos(12),
        },
        BudgetHealth::Available,
        None,
        Vec::new(),
    )?;
    let update = health_reporter.report(health)?;
    registry.record_health(&session, update)?;
    let current = registry.validate_current_authority(&session, Timestamp::from_unix_nanos(2))?;
    current.validate_live_scope(
        &VenueId::try_from("coinbase")?,
        instrument,
        LiveEventClass::Trade,
        None,
    )?;
    let first_frame = frames.try_frame(
        Timestamp::from_unix_nanos(3),
        TransportFrameKind::Binary,
        Bytes::from_static(b"same-payload"),
    )?;
    let second_frame = frames.try_frame(
        Timestamp::from_unix_nanos(3),
        TransportFrameKind::Binary,
        Bytes::from_static(b"same-payload"),
    )?;
    capture_admission.preflight(&first_frame)?;
    let receipt = capture_admission.issue_after_enqueue(&first_frame)?;
    capture_admission.validate_active(&first_frame)?;
    let validated = session.validate_live_frame(&second_frame)?;
    let decoder_rule =
        IntegrityRule::new(source_identifier("coinbase-decoder")?, RuleVersion::new(1)?);
    let evidence = DecoderEvidence::from_validated_frame(&validated, decoder_rule);
    let rule = |name: &str| -> TestResult<IntegrityRule> {
        Ok(IntegrityRule::new(
            source_identifier(name)?,
            RuleVersion::new(1)?,
        ))
    };
    let observation = ProviderNormalizedObservation::try_new(
        source_identifier("trade-1")?,
        VenueId::try_from("coinbase")?,
        instrument,
        ProviderTimestampEvidence::Provided {
            value: Timestamp::from_unix_nanos(3),
            rule: rule("coinbase-timestamp")?,
        },
        ProviderSequenceEvidence::Provided {
            value: SequenceNumber::new(1),
            rule: rule("coinbase-sequence")?,
        },
        ProviderSnapshotEvidence::NotApplicable(rule("trade-no-snapshot-v1")?),
        ProviderChecksumEvidence::Unsupported {
            rule: rule("coinbase-no-checksum")?,
        },
        ProviderObservationPayload::Trade {
            trade_id: source_identifier("trade-1")?,
            price: ProviderPrice::new(ProviderDecimalLexeme::try_new("100.00")?),
            quantity: ProviderQuantity::new(ProviderDecimalLexeme::try_new("1.00")?),
            aggressor: ProviderAggressorEvidence::new(
                AggressorSide::Buy,
                Some(source_identifier("BUY")?),
                rule("coinbase-aggressor")?,
            ),
        },
    )?;
    let batch = DecodedProviderBatch::try_new(evidence, vec![observation])?;
    assert!(matches!(
        current.validate_decoded_batch_owned(batch, receipt),
        Err(RegistryError::CaptureReceiptMismatch)
    ));
    assert!(
        current
            .validate_live_scope(
                &VenueId::try_from("kraken")?,
                instrument,
                LiveEventClass::Trade,
                None,
            )
            .is_err()
    );

    let current_frame = frames.try_frame(
        Timestamp::from_unix_nanos(4),
        TransportFrameKind::Binary,
        Bytes::from_static(b"current-payload"),
    )?;
    capture_admission.preflight(&current_frame)?;
    let current_receipt = capture_admission.issue_after_enqueue(&current_frame)?;
    capture_admission.validate_active(&current_frame)?;
    let current_validated = session.validate_live_frame(&current_frame)?;
    let current_evidence = DecoderEvidence::from_validated_frame(
        &current_validated,
        IntegrityRule::new(source_identifier("coinbase-decoder")?, RuleVersion::new(1)?),
    );
    let current_payload_digest = current_evidence.payload_digest();
    let make_current_observation =
        |instrument: InstrumentId, trade_id: &str, sequence: u64| -> TestResult<_> {
            Ok(ProviderNormalizedObservation::try_new(
                source_identifier(trade_id)?,
                VenueId::try_from("coinbase")?,
                instrument,
                ProviderTimestampEvidence::Provided {
                    value: Timestamp::from_unix_nanos(4),
                    rule: rule("coinbase-timestamp")?,
                },
                ProviderSequenceEvidence::Provided {
                    value: SequenceNumber::new(sequence),
                    rule: rule("coinbase-sequence")?,
                },
                ProviderSnapshotEvidence::NotApplicable(rule("trade-no-snapshot-v1")?),
                ProviderChecksumEvidence::Unsupported {
                    rule: rule("coinbase-no-checksum")?,
                },
                ProviderObservationPayload::Trade {
                    trade_id: source_identifier(trade_id)?,
                    price: ProviderPrice::new(ProviderDecimalLexeme::try_new("100.00")?),
                    quantity: ProviderQuantity::new(ProviderDecimalLexeme::try_new("1.00")?),
                    aggressor: ProviderAggressorEvidence::new(
                        AggressorSide::Buy,
                        Some(source_identifier("BUY")?),
                        rule("coinbase-aggressor")?,
                    ),
                },
            )?)
        };
    let first_current_observation = make_current_observation(instrument, "trade-2", 2)?;
    let other_observation = make_current_observation(other_instrument, "trade-3", 3)?;
    let second_current_observation = make_current_observation(instrument, "trade-4", 4)?;
    let current_batches = current.validate_decoded_batch_owned(
        DecodedProviderBatch::try_new(
            current_evidence,
            vec![
                first_current_observation,
                other_observation,
                second_current_observation,
            ],
        )?,
        current_receipt,
    )?;
    let mut routed_batches = current_batches.into_iter();
    assert_eq!(routed_batches.len(), 2);
    let current_batch = routed_batches
        .next()
        .ok_or("current routing collection lost its first batch")?;
    assert_eq!(current_batch.key().instrument(), instrument);
    assert!(current_batch.retained_bytes() < 128 * 1024);
    let mut current_observations = current_batch.into_observations();
    assert_eq!(current_observations.len(), 2);
    let current_observation = current_observations
        .next()
        .ok_or("current batch lost its observation")?;
    assert_eq!(
        current_observation
            .observation()
            .source_identifier()
            .as_str(),
        "trade-2"
    );
    let coverage = current_observation.policy().coverage();
    assert_eq!(coverage.source_id().as_str(), "source-a");
    assert_eq!(coverage.venue().as_str(), "coinbase");
    assert_eq!(
        coverage.provider_product().as_source_identifier().as_str(),
        "direct-product"
    );
    assert_eq!(
        coverage.provider_channel().as_source_identifier().as_str(),
        "trades"
    );
    assert_eq!(coverage.event_class(), LiveEventClass::Trade);
    assert_eq!(coverage.depth(), None);
    assert_eq!(coverage.delay(), CoverageDelay::RealTime);
    assert_eq!(coverage.consolidation(), CoverageConsolidation::SingleVenue);
    assert_eq!(coverage.delivery(), DeliveryEvidence::DirectVenue);
    assert_eq!(coverage.evidence(), &exact_evidence(3));
    assert_eq!(coverage.effective_from(), Timestamp::from_unix_nanos(0));
    assert_eq!(coverage.effective_until(), None);
    assert_eq!(
        coverage.metadata_revision().as_source_identifier().as_str(),
        "revision-a"
    );
    assert_eq!(
        current_observation.frame_evidence().frame_id(),
        current_frame.frame_id()
    );
    assert_eq!(
        current_observation.frame_evidence().received_at(),
        current_frame.received_at()
    );
    assert_eq!(
        current_observation.frame_evidence().payload_digest(),
        current_payload_digest
    );
    assert!(
        current_observation
            .frame_evidence()
            .binding()
            .shares_allocation_with(current_frame.binding())
    );
    assert_eq!(
        current_observation
            .frame_evidence()
            .decoder_rule()
            .provider_rule()
            .as_str(),
        "coinbase-decoder"
    );
    let second_current_observation = current_observations
        .next()
        .ok_or("current batch lost its second observation")?;
    assert_eq!(
        second_current_observation
            .observation()
            .source_identifier()
            .as_str(),
        "trade-4"
    );
    let other_batch = routed_batches
        .next()
        .ok_or("current routing collection lost its second batch")?;
    assert_eq!(other_batch.key().instrument(), other_instrument);
    assert_eq!(other_batch.into_observations().len(), 1);
    Ok(())
}
