mod common;

use bytes::Bytes;
use market_squawk_domain::{
    CaptureIntegrityState, ConnectionGeneration, CoverageConsolidation, CoverageDelay,
    DeliveryEvidence, ProviderChannel, ProviderProduct, StreamIntegrityState, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationHealth, BudgetDecision, BudgetHealth,
    ConnectionLiveness, CoverageHealth, FreshnessPolicy, RawMarketFrame, RegistryAuthorityState,
    RegistryError, SessionId, SourceHealthSnapshot, TransportFrameKind,
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
assert_not_impl_any!(market_squawk_sources::CaptureAdmissionIssuer: Clone, Sync, serde::Serialize);
assert_not_impl_any!(market_squawk_sources::CaptureInitializationControl: Clone, Sync, serde::Serialize);
assert_not_impl_any!(market_squawk_sources::CaptureGenerationCapabilities: Clone, Sync, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_sources::CurrentHealthReporter: Clone, Sync, serde::Serialize);
assert_not_impl_any!(market_squawk_sources::CurrentHealthUpdate: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_sources::RawFrameFactory: Clone, Sync, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(market_squawk_sources::CurrentCoveragePolicy: serde::Serialize, serde::de::DeserializeOwned);

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
    let current_observation = ProviderNormalizedObservation::try_new(
        source_identifier("trade-2")?,
        VenueId::try_from("coinbase")?,
        instrument,
        ProviderTimestampEvidence::Provided {
            value: Timestamp::from_unix_nanos(4),
            rule: rule("coinbase-timestamp")?,
        },
        ProviderSequenceEvidence::Provided {
            value: SequenceNumber::new(2),
            rule: rule("coinbase-sequence")?,
        },
        ProviderSnapshotEvidence::NotApplicable(rule("trade-no-snapshot-v1")?),
        ProviderChecksumEvidence::Unsupported {
            rule: rule("coinbase-no-checksum")?,
        },
        ProviderObservationPayload::Trade {
            trade_id: source_identifier("trade-2")?,
            price: ProviderPrice::new(ProviderDecimalLexeme::try_new("100.00")?),
            quantity: ProviderQuantity::new(ProviderDecimalLexeme::try_new("1.00")?),
            aggressor: ProviderAggressorEvidence::new(
                AggressorSide::Buy,
                Some(source_identifier("BUY")?),
                rule("coinbase-aggressor")?,
            ),
        },
    )?;
    let current_batch = current.validate_decoded_batch_owned(
        DecodedProviderBatch::try_new(
            current_evidence,
            vec![current_observation.clone(), current_observation],
        )?,
        current_receipt,
    )?;
    assert!(current_batch.retained_bytes() < 128 * 1024);
    let mut current_observations = current_batch.into_observations();
    assert_eq!(current_observations.len(), 2);
    let current_observation = current_observations
        .next()
        .ok_or("current batch lost its observation")?;
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
    Ok(())
}
