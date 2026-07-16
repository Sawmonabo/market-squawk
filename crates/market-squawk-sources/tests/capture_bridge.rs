mod common;

use std::str::FromStr;
use std::time::Duration;

use bytes::Bytes;
use market_squawk_domain::{
    AggressorSide, CaptureIntegrityState, ConnectionGeneration, InstrumentId, IntegrityRule,
    LiveEventClass, ProviderChannel, ProviderProduct, RuleVersion, SequenceNumber,
    StreamIntegrityState, Timestamp, VenueId,
};
use market_squawk_platform::{
    CaptureWriterPolicy, MemoryCaptureSink, raw_capture_channel, spawn_capture_writer,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationHealth, BudgetHealth, ConnectionLiveness,
    CoverageHealth, DecodedProviderBatch, DecoderEvidence, FreshnessPolicy,
    ProviderAggressorEvidence, ProviderChecksumEvidence, ProviderDecimalLexeme,
    ProviderNormalizedObservation, ProviderObservationPayload, ProviderPrice, ProviderQuantity,
    ProviderSequenceEvidence, ProviderSnapshotEvidence, ProviderTimestampEvidence, RegistryError,
    SessionId, SourceHealthSnapshot, TransportFrameKind,
};

use common::{TestResult, direct_metadata, exact_evidence, source_identifier};

fn rule(name: &str) -> TestResult<IntegrityRule> {
    Ok(IntegrityRule::new(
        source_identifier(name)?,
        RuleVersion::new(1)?,
    ))
}

#[tokio::test]
async fn platform_returns_exact_registry_receipt_and_later_degradation_revokes_current_batch()
-> TestResult {
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
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let mut health_reporter = registry.take_current_health_reporter(&session)?;
    let (publisher, mut control, writer) = raw_capture_channel(std::num::NonZeroUsize::MIN, bundle);
    let writer_handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;

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

    let frame = frames.try_frame(
        Timestamp::from_unix_nanos(3),
        TransportFrameKind::Binary,
        Bytes::from_static(b"exact-frame"),
    )?;
    let validated = session.validate_live_frame(&frame)?;
    let evidence = DecoderEvidence::from_validated_frame(&validated, rule("coinbase-decoder")?);
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
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
    let receipt = publisher.try_publish(&frame)?;
    let current_batch = current.validate_decoded_batch_owned(batch, receipt)?;
    current_batch.validate_at(Timestamp::from_unix_nanos(3))?;

    drop(control);
    assert!(matches!(
        current_batch.validate_at(Timestamp::from_unix_nanos(3)),
        Err(RegistryError::HealthNotQualified)
    ));
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);

    let _outcome = writer_handle.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[test]
fn exact_platform_publisher_type_preserves_task_five_frame_and_receipt_association() {
    fn assert_association<B>()
    where
        B: market_squawk_domain::CaptureAuthorityBundle<
                Frame = market_squawk_sources::RawMarketFrame,
                Receipt = market_squawk_sources::CaptureAdmissionReceipt,
            >,
    {
    }

    assert_association::<market_squawk_sources::CaptureGenerationCapabilities>();
    let _event_class = LiveEventClass::Trade;
}
