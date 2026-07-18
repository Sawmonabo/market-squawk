mod common;

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use market_squawk_domain::{
    CaptureAuthorityBundle, CaptureDegradation, CaptureIntegrityState,
    CaptureResidentGenerationLease, CaptureResidentToken, ConnectionGeneration,
    CoverageConsolidation, CoverageDelay, DeliveryEvidence, DigestAlgorithm, EvidenceDigest,
    ProviderChannel, ProviderProduct, RawCaptureFrameView, SourceIdentifier, StreamIntegrityState,
    Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationHealth, AuthorizationMode,
    AuthorizationSubjectResolutionError, AuthorizationSubjectResolver, BudgetDecision,
    BudgetHealth, ConnectionLiveness, CoverageHealth, FreshnessPolicy, RawMarketFrame,
    RegistryAuthorityState, RegistryError, RetryAfter, SessionId, SourceHealthSnapshot,
    TransportFrameKind,
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
assert_not_impl_any!(market_squawk_sources::CaptureAdmissionReceipt: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_sources::CaptureInitializationControl: Clone, Sync, serde::Serialize);
assert_not_impl_any!(market_squawk_sources::CaptureGenerationCapabilities: Clone, Sync, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_sources::CurrentHealthReporter: Clone, Sync, serde::Serialize);
assert_not_impl_any!(market_squawk_sources::CurrentHealthUpdate: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_sources::RawFrameFactory: Clone, Sync, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_sources::CurrentCoveragePolicy: serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_platform::LocalAuthorityStateStore: Clone);
assert_impl_all!(market_squawk_sources::RawMarketFrame: RawCaptureFrameView);
assert_impl_all!(market_squawk_sources::CaptureGenerationCapabilities: CaptureAuthorityBundle);

#[derive(Debug)]
struct TestResidentToken(Option<Arc<AtomicUsize>>);

impl CaptureResidentToken for TestResidentToken {}

impl Drop for TestResidentToken {
    fn drop(&mut self) {
        if let Some(drops) = &self.0 {
            drops.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn observed_resident_generation_lease() -> (CaptureResidentGenerationLease, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    let token = Arc::new(TestResidentToken(Some(Arc::clone(&drops))));
    (CaptureResidentGenerationLease::new(token), drops)
}

fn direct_metadata_for_provider(
    source: &str,
    revision: &str,
    provider: &str,
    endpoint: &str,
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
    let network = metadata
        .get_mut("network")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|network| network.get_mut("allowlisted"))
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("source metadata allowlist was absent")?;
    network.insert("endpoints".to_owned(), serde_json::json!([endpoint]));
    Ok(serde_json::from_value(wire)?)
}

type RemoteAuthorizationAlias<'a> = (&'a str, u8, Option<(&'a str, &'a str)>);

fn remote_metadata_alias(
    source: &str,
    revision: &str,
    provider: &str,
    endpoints: &[&str],
    authorization: Option<RemoteAuthorizationAlias<'_>>,
    requests_per_window: u32,
) -> TestResult<market_squawk_sources::SourceMetadata> {
    let mut wire = serde_json::to_value(direct_metadata(source, revision, 0, None)?)?;
    let metadata = wire
        .as_object_mut()
        .ok_or("source metadata did not serialize as an object")?;
    metadata.insert("provider".to_owned(), serde_json::json!(provider));
    let network = metadata
        .get_mut("network")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|network| network.get_mut("allowlisted"))
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("source metadata allowlist was absent")?;
    network.insert("endpoints".to_owned(), serde_json::json!(endpoints));
    let budget = metadata
        .get_mut("budget")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("source metadata budget was absent")?;
    budget.insert(
        "requests_per_window".to_owned(),
        serde_json::json!(requests_per_window),
    );
    let scope = budget
        .get_mut("scope")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("source metadata budget scope was absent")?;
    scope.insert("provider".to_owned(), serde_json::json!(provider));
    if let Some((account_alias, _, _)) = authorization {
        scope.insert(
            "authorization_account".to_owned(),
            serde_json::json!(account_alias),
        );
    }

    if let Some((account_alias, evidence_byte, locator)) = authorization {
        metadata.insert("source_class".to_owned(), serde_json::json!("broker"));
        let authorization = metadata
            .get_mut("authorization")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or("source authorization was absent")?;
        authorization.insert("mode".to_owned(), serde_json::json!("user_authorized"));
        authorization.insert("basis".to_owned(), serde_json::json!(account_alias));
        let evidence = authorization
            .get_mut("evidence")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or("authorization evidence was absent")?;
        let content_digest = evidence
            .get_mut("content_digest")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or("authorization evidence digest was absent")?;
        content_digest.insert(
            "bytes".to_owned(),
            serde_json::json!(vec![evidence_byte; 32]),
        );
        if let Some((reference, version)) = locator {
            evidence.insert(
                "version_pinned_locator".to_owned(),
                serde_json::json!({"reference": reference, "version": version}),
            );
        }
        let coverage = metadata
            .get_mut("coverage")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or("source coverage was absent")?;
        coverage.insert(
            "delivery".to_owned(),
            serde_json::json!("authorized_broker"),
        );
    }
    Ok(serde_json::from_value(wire)?)
}

#[derive(Debug)]
struct EvidenceBoundSubjectResolver {
    records: HashMap<EvidenceDigest, SourceIdentifier>,
}

impl AuthorizationSubjectResolver for EvidenceBoundSubjectResolver {
    fn resolve_subject_record(
        &self,
        mode: AuthorizationMode,
        evidence: EvidenceDigest,
    ) -> Result<SourceIdentifier, AuthorizationSubjectResolutionError> {
        if !matches!(
            mode,
            AuthorizationMode::UserAuthorized | AuthorizationMode::Licensed
        ) {
            return Err(AuthorizationSubjectResolutionError::UnsupportedMode);
        }
        self.records
            .get(&evidence)
            .cloned()
            .ok_or(AuthorizationSubjectResolutionError::EvidenceUnresolved)
    }
}

fn subject_resolver(records: &[(u8, &str)]) -> TestResult<Arc<dyn AuthorizationSubjectResolver>> {
    let records = records
        .iter()
        .map(|(byte, record)| {
            Ok((
                EvidenceDigest::new(DigestAlgorithm::Sha256, [*byte; 32]),
                source_identifier(record)?,
            ))
        })
        .collect::<TestResult<HashMap<_, _>>>()?;
    Ok(Arc::new(EvidenceBoundSubjectResolver { records }))
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
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
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
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
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
    let frame = factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"frame"))?;

    assert_eq!(RawCaptureFrameView::source_id(&frame).as_str(), "source-a");
    assert_eq!(
        RawCaptureFrameView::session_identifier(&frame).as_str(),
        "session-a"
    );
    assert_eq!(RawCaptureFrameView::frame_ordinal(&frame).get(), 1);
    assert_eq!(RawCaptureFrameView::payload(&frame), b"frame");
    assert!(
        RawCaptureFrameView::checked_retained_footprint(&frame)?.checked_complete_bytes()?
            >= frame.retained_payload_bytes()
    );
    Ok(())
}

#[test]
fn handles_reject_registry_transplant_and_session_resurrection() -> TestResult {
    let mut first = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let second = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
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
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
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
    let frame = frames.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"first"))?;
    session.validate_live_frame(&frame)?;
    registry.end_session(&session, Timestamp::from_unix_nanos(3))?;
    assert!(matches!(
        frames.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"late"),),
        Err(market_squawk_sources::SourceError::SessionNotCurrent)
    ));
    Ok(())
}

#[test]
fn adjacent_revision_cutover_and_expired_cleanup_are_administratively_valid() -> TestResult {
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
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
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
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
    let mut first_registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let first = first_registry.register(
        direct_metadata_for_provider(
            "interner-a",
            "interner-rev-a",
            "display-provider-alias-a",
            "wss://process-interner.example.test/feed-a",
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    let first_budget = first.budget().ok_or("first coordinated budget missing")?;

    let mut second_registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let second = second_registry.register(
        direct_metadata_for_provider(
            "interner-b",
            "interner-rev-b",
            "display-provider-alias-b",
            "https://process-interner.example.test/feed-b",
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    let second_budget = second.budget().ok_or("second coordinated budget missing")?;
    assert!(first_budget.shares_allocation_with(second_budget));

    let state = first_registry.export_authority_state()?;
    let mut restored =
        AuthoritativeSourceRegistry::try_new_ephemeral_with_authority_state_for_diagnostics(state)?;
    let restored_source = restored.register(
        direct_metadata_for_provider(
            "interner-c",
            "interner-rev-c",
            "display-provider-alias-c",
            "wss://process-interner.example.test/feed-c",
        )?,
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
fn account_aliases_and_locator_metadata_cannot_multiply_one_credential_budget() -> TestResult {
    let resolver = subject_resolver(&[(41, "credential-record-a"), (44, "credential-record-a")])?;
    let mut first = AuthoritativeSourceRegistry::try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(
        Arc::clone(&resolver),
    )?;
    let mut second = AuthoritativeSourceRegistry::try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(
        Arc::clone(&resolver),
    )?;
    let first_source = first.register(
        remote_metadata_alias(
            "account-alias-a",
            "account-alias-rev-a",
            "provider-display-a",
            &["wss://identity-budget.example.test/feed-a"],
            Some(("account-display-a", 41, Some(("record-a", "version-a")))),
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    let second_source = second.register(
        remote_metadata_alias(
            "account-alias-b",
            "account-alias-rev-b",
            "provider-display-b",
            &["wss://identity-budget.example.test/feed-b"],
            Some(("account-display-b", 44, Some(("record-b", "version-b")))),
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    let first_budget = first_source
        .budget()
        .ok_or("first account budget missing")?;
    let second_budget = second_source
        .budget()
        .ok_or("second account budget missing")?;
    assert!(first_budget.shares_allocation_with(second_budget));

    let permit = match first_budget.try_acquire() {
        BudgetDecision::Ready(permit) => permit,
        other => return Err(format!("unexpected account acquire: {other:?}").into()),
    };
    assert!(matches!(
        second_budget.try_acquire(),
        BudgetDecision::Unavailable(
            market_squawk_sources::BudgetUnavailableReason::ConcurrencyExhausted
        )
    ));
    permit.release();
    Ok(())
}

#[test]
fn public_endpoint_subset_and_superset_share_on_any_authority_overlap() -> TestResult {
    let mut first = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let mut second = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let first_source = first.register(
        remote_metadata_alias(
            "overlap-subset-a",
            "overlap-subset-rev-a",
            "overlap-display-a",
            &[
                "wss://overlap-subset.example.test/feed-a",
                "https://independent-a.example.test/data",
            ],
            None,
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    let second_source = second.register(
        remote_metadata_alias(
            "overlap-subset-b",
            "overlap-subset-rev-b",
            "overlap-display-b",
            &["https://overlap-subset.example.test/feed-b"],
            None,
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    assert!(
        first_source
            .budget()
            .ok_or("first overlap budget missing")?
            .shares_allocation_with(
                second_source
                    .budget()
                    .ok_or("second overlap budget missing")?
            )
    );
    Ok(())
}

#[test]
fn public_bridge_declaration_fails_without_merging_existing_allocations() -> TestResult {
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let first = registry.register(
        remote_metadata_alias(
            "bridge-a",
            "bridge-rev-a",
            "bridge-display-a",
            &["wss://bridge-a.example.test/feed"],
            None,
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    let second = registry.register(
        remote_metadata_alias(
            "bridge-b",
            "bridge-rev-b",
            "bridge-display-b",
            &["wss://bridge-b.example.test/feed"],
            None,
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    let first_budget = first.budget().ok_or("first bridge budget missing")?;
    let second_budget = second.budget().ok_or("second bridge budget missing")?;
    assert!(!first_budget.shares_allocation_with(second_budget));

    assert!(matches!(
        registry.register(
            remote_metadata_alias(
                "bridge-c",
                "bridge-rev-c",
                "bridge-display-c",
                &[
                    "https://bridge-a.example.test/other",
                    "https://bridge-b.example.test/other",
                ],
                None,
                2,
            )?,
            Timestamp::from_unix_nanos(1),
        ),
        Err(RegistryError::BudgetCoordinator)
    ));
    assert!(!first_budget.shares_allocation_with(second_budget));
    let first_permit = match first_budget.try_acquire() {
        BudgetDecision::Ready(permit) => permit,
        other => return Err(format!("first bridge allocation changed: {other:?}").into()),
    };
    let second_permit = match second_budget.try_acquire() {
        BudgetDecision::Ready(permit) => permit,
        other => return Err(format!("second bridge allocation changed: {other:?}").into()),
    };
    first_permit.release();
    second_permit.release();
    Ok(())
}

#[test]
fn account_budget_requires_trusted_subject_resolution() -> TestResult {
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    assert!(matches!(
        registry.register(
            remote_metadata_alias(
                "unresolved-account",
                "unresolved-account-rev",
                "unresolved-provider",
                &["wss://unresolved-account.example.test/feed"],
                Some(("caller-account-alias", 49, None)),
                2,
            )?,
            Timestamp::from_unix_nanos(1),
        ),
        Err(RegistryError::AuthorizationSubjectResolution)
    ));
    Ok(())
}

#[test]
fn restored_account_subject_is_freshly_resolved_and_tampering_fails_closed() -> TestResult {
    let resolver = subject_resolver(&[(51, "credential-record-restore")])?;
    let mut owner = AuthoritativeSourceRegistry::try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(
        Arc::clone(&resolver),
    )?;
    let registered = owner.register(
        remote_metadata_alias(
            "restore-account",
            "restore-account-rev",
            "restore-provider",
            &["wss://restore-account.example.test/feed"],
            Some(("restore-display-account", 51, None)),
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    let state = owner.export_authority_state()?;
    let serialized = serde_json::to_string(&state)?;
    assert!(!serialized.contains("canonical_identity"));
    assert!(matches!(
        AuthoritativeSourceRegistry::try_new_ephemeral_with_authority_state_for_diagnostics(
            state.clone()
        ),
        Err(RegistryError::AuthorizationSubjectResolution)
    ));
    let restored =
        AuthoritativeSourceRegistry::try_new_ephemeral_with_authority_state_and_authorization_subject_resolver_for_diagnostics(
            state.clone(),
            Arc::clone(&resolver),
        )?;
    let owner_budget = registered.budget().ok_or("owner account budget missing")?;
    let restored_state = restored.export_authority_state()?;
    let mut second_restore =
        AuthoritativeSourceRegistry::try_new_ephemeral_with_authority_state_and_authorization_subject_resolver_for_diagnostics(
            restored_state,
            Arc::clone(&resolver),
        )?;
    let restored_alias = second_restore.register(
        remote_metadata_alias(
            "restore-account-alias",
            "restore-account-alias-rev",
            "restore-provider-alias",
            &["https://different-account-endpoint.example.test/data"],
            Some(("different-display-account", 51, None)),
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    assert!(
        owner_budget.shares_allocation_with(
            restored_alias
                .budget()
                .ok_or("restored account budget missing")?
        )
    );

    let mut tampered = serde_json::to_value(state)?;
    let subject = tampered
        .as_object_mut()
        .and_then(|object| object.get_mut("budget_policies"))
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|policies| policies.first_mut())
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|policy| policy.get_mut("resolved_subject_record"))
        .ok_or("persisted resolved subject record missing")?;
    *subject = serde_json::json!("tampered-credential-record");
    let tampered: RegistryAuthorityState = serde_json::from_value(tampered)?;
    assert!(matches!(
        AuthoritativeSourceRegistry::try_new_ephemeral_with_authority_state_and_authorization_subject_resolver_for_diagnostics(
            tampered,
            resolver,
        ),
        Err(RegistryError::AuthorizationSubjectMismatch)
    ));
    Ok(())
}

#[test]
fn distinct_resolved_credentials_receive_distinct_account_allocations() -> TestResult {
    let resolver = subject_resolver(&[(42, "credential-record-a"), (43, "credential-record-b")])?;
    let mut registry =
        AuthoritativeSourceRegistry::try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(resolver)?;
    let first = registry.register(
        remote_metadata_alias(
            "distinct-account-a",
            "distinct-account-rev-a",
            "provider-a",
            &["wss://distinct-budget.example.test/feed"],
            Some(("display-account", 42, None)),
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    let second = registry.register(
        remote_metadata_alias(
            "distinct-account-b",
            "distinct-account-rev-b",
            "provider-b",
            &["wss://distinct-budget.example.test/feed"],
            Some(("display-account", 43, None)),
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    assert!(
        !first
            .budget()
            .ok_or("first distinct budget missing")?
            .shares_allocation_with(second.budget().ok_or("second distinct budget missing")?)
    );
    Ok(())
}

#[test]
fn canonical_endpoint_origins_normalize_idna_ports_paths_and_allowlist_order() -> TestResult {
    let mut first = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let mut second = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let first_source = first.register(
        remote_metadata_alias(
            "canonical-origin-a",
            "canonical-origin-rev-a",
            "origin-display-a",
            &[
                "WSS://BÜCHER.example.test:443/feed-a",
                "https://[2001:db8::1]:443/path-a",
            ],
            None,
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    let second_source = second.register(
        remote_metadata_alias(
            "canonical-origin-b",
            "canonical-origin-rev-b",
            "origin-display-b",
            &[
                "https://[2001:0db8:0:0:0:0:0:1]/path-b",
                "wss://xn--bcher-kva.example.test/feed-b",
            ],
            None,
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    assert!(
        first_source
            .budget()
            .ok_or("first canonical budget missing")?
            .shares_allocation_with(
                second_source
                    .budget()
                    .ok_or("second canonical budget missing")?
            )
    );

    let mut scheme_alias = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let third_source = scheme_alias.register(
        remote_metadata_alias(
            "canonical-origin-c",
            "canonical-origin-rev-c",
            "origin-display-c",
            &[
                "https://xn--bcher-kva.example.test/feed-b",
                "https://[2001:db8::1]/path-b",
            ],
            None,
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    assert!(
        first_source
            .budget()
            .ok_or("first canonical budget missing")?
            .shares_allocation_with(
                third_source
                    .budget()
                    .ok_or("third canonical budget missing")?
            )
    );
    Ok(())
}

#[test]
fn one_canonical_identity_rejects_conflicting_alias_policy_without_publication() -> TestResult {
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let first = registry.register(
        remote_metadata_alias(
            "conflicting-alias-a",
            "conflicting-alias-rev-a",
            "conflicting-display-a",
            &["wss://conflicting-alias.example.test/feed-a"],
            None,
            2,
        )?,
        Timestamp::from_unix_nanos(1),
    )?;
    assert!(matches!(
        registry.register(
            remote_metadata_alias(
                "conflicting-alias-b",
                "conflicting-alias-rev-b",
                "conflicting-display-b",
                &["wss://conflicting-alias.example.test/feed-b"],
                None,
                3,
            )?,
            Timestamp::from_unix_nanos(1),
        ),
        Err(RegistryError::BudgetCoordinator)
    ));
    assert!(first.budget().is_some());
    assert!(
        registry
            .validate_registered(&first, Timestamp::from_unix_nanos(1))
            .is_ok()
    );
    Ok(())
}

#[test]
fn process_coordinator_rejects_conflicting_restored_policy() -> TestResult {
    let provider = "process-budget-conflict-provider";
    let mut owner = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let _registered = owner.register(
        direct_metadata_for_provider(
            "conflict-a",
            "conflict-rev-a",
            provider,
            "wss://process-conflict.example.test/feed",
        )?,
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
    let declared_policy = policy
        .get_mut("policy")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("authority state declared policy missing")?;
    declared_policy.insert("requests_per_window".to_owned(), serde_json::json!(11));
    let conflicting: RegistryAuthorityState = serde_json::from_value(wire)?;

    drop(_registered);
    drop(owner);

    assert!(matches!(
        AuthoritativeSourceRegistry::try_new_ephemeral_with_authority_state_for_diagnostics(
            conflicting
        ),
        Err(RegistryError::BudgetCoordinator)
    ));
    Ok(())
}

#[test]
fn coordinated_budget_proof_controls_health_and_queued_authority() -> TestResult {
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(
        direct_metadata_for_provider(
            "budget-source",
            "budget-revision",
            "budget-authority-test-provider",
            "wss://budget-authority.example.test/feed",
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
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
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
    let frame = frames.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"payload"))?;
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
    let mut first = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
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
    let mut restarted =
        AuthoritativeSourceRegistry::try_new_ephemeral_with_authority_state_for_diagnostics(
            restored,
        )?;
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
