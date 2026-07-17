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
    next_timestamp_after, now_timestamp, source_identifier,
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
    let observed_at = Timestamp::from_unix_nanos(observed_at);
    let valid_until = observed_at.checked_add_nanos(10_000_000_000)?;
    Ok(SourceHealthSnapshot::try_new(
        session,
        observed_at,
        ConnectionLiveness::Live {
            last_activity_at: observed_at,
        },
        Some(observed_at),
        Some(observed_at),
        Some(observed_at),
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
            valid_until,
        },
        CoverageHealth::Sufficient {
            evidence: exact_evidence(32),
            provider_product: ProviderProduct::new(source_identifier("direct-product")?),
            provider_channel: ProviderChannel::new(source_identifier("trades")?),
            valid_until,
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

    let permit = match first_budget.try_acquire() {
        BudgetDecision::Ready(permit) => permit,
        other => return Err(format!("unexpected coordinated acquire: {other:?}").into()),
    };
    assert!(matches!(
        second_budget.try_acquire(),
        BudgetDecision::Unavailable(
            market_squawk_sources::BudgetUnavailableReason::ConcurrencyExhausted
        )
    ));
    permit.release();
    assert!(matches!(
        second_budget.try_acquire(),
        BudgetDecision::Ready(_)
    ));

    let cooldown = match second_budget.apply_retry_after(RetryAfter::Delay(
        NonZeroU64::new(60_000_000_000).ok_or("nonzero retry delay")?,
    )) {
        BudgetDecision::WaitUntil(deadline) => deadline,
        other => return Err(format!("unexpected coordinated cooldown: {other:?}").into()),
    };
    assert!(matches!(
        first_budget.try_acquire(),
        BudgetDecision::WaitUntil(deadline) if deadline == cooldown
    ));
    assert!(matches!(
        restored_source
            .budget()
            .ok_or("restored coordinated budget missing")?
            .try_acquire(),
        BudgetDecision::WaitUntil(deadline) if deadline == cooldown
    ));
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

    drop(_registered);
    drop(owner);

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
    let first_health_at = now_timestamp()?;
    let qualified_health_at = next_timestamp_after(first_health_at)?;
    let cooling_health_at = next_timestamp_after(qualified_health_at)?;
    let disabled_health_at = next_timestamp_after(cooling_health_at)?;

    let permit = match budget.try_acquire() {
        BudgetDecision::Ready(permit) => permit,
        other => return Err(format!("unexpected budget decision: {other:?}").into()),
    };
    registry.record_health(
        &session,
        reporter.report(healthy_snapshot(&session, first_health_at.unix_nanos())?)?,
    )?;
    assert!(matches!(
        registry.validate_current_authority(&session),
        Err(RegistryError::HealthNotQualified)
    ));
    permit.release();

    registry.record_health(
        &session,
        reporter.report(healthy_snapshot(
            &session,
            qualified_health_at.unix_nanos(),
        )?)?,
    )?;
    let queued = registry
        .validate_current_authority(&session)?
        .try_current_lease()?;
    assert!(queued.validate_at(qualified_health_at).is_ok());

    assert!(matches!(
        budget.apply_retry_after(RetryAfter::Delay(
            NonZeroU64::new(60_000_000_000).ok_or("nonzero retry delay")?
        )),
        BudgetDecision::WaitUntil(_)
    ));
    assert_eq!(
        queued.validate_at(qualified_health_at),
        Err(RegistryError::HealthNotQualified)
    );
    registry.record_health(
        &session,
        reporter.report(healthy_snapshot(&session, cooling_health_at.unix_nanos())?)?,
    )?;
    assert!(matches!(
        registry.validate_current_authority(&session),
        Err(RegistryError::HealthNotQualified)
    ));

    assert!(matches!(budget.disable(), BudgetDecision::Unavailable(_)));
    registry.record_health(
        &session,
        reporter.report(healthy_snapshot(&session, disabled_health_at.unix_nanos())?)?,
    )?;
    assert!(matches!(
        registry.validate_current_authority(&session),
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

include!("registry_authority/pre_feed_cases.rs");
include!("registry_authority/current_scope_cases.rs");
