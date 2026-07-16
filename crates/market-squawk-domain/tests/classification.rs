use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    BookIntegrity, CaptureIntegrityState, ChecksumIntegrity, ConnectionGeneration, DataQuality,
    DeliveryEvidence, EligibilityFailure, EventTimingEvidence, ExecutionEligibility,
    FairValueHierarchy, FreshnessEvidence, FreshnessState, InstrumentId, MarketDepth,
    PrecisionIntegrity, QualificationEvidence, QualificationEvidenceInput, SequenceIntegrity,
    SnapshotConsistency, SourceAuthorization, SourceCoverageEvidence, SourceId,
    StreamIntegrityState, Timestamp, TradingStatus, VenueId,
};

fn qualification_input(
    quality: DataQuality,
    hierarchy: Option<FairValueHierarchy>,
) -> Result<QualificationEvidenceInput, Box<dyn Error>> {
    let received_at = Timestamp::from_unix_nanos(1_000);
    Ok(QualificationEvidenceInput::new(
        quality,
        hierarchy,
        SourceAuthorization::Authorized,
        DeliveryEvidence::DirectVenue,
        SourceId::try_from("direct-feed")?,
        VenueId::try_from("XNYS")?,
        InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb")?,
        ConnectionGeneration::new(7)?,
        SequenceIntegrity::Valid,
        SnapshotConsistency::Consistent,
        ChecksumIntegrity::Valid,
        EventTimingEvidence::assess(Some(received_at), received_at, 0)?,
        FreshnessEvidence::assess(Some(received_at), None, received_at, 50)?,
        TradingStatus::Active,
        PrecisionIntegrity::Valid,
        SourceCoverageEvidence::Explicit,
        BookIntegrity::Consistent,
        StreamIntegrityState::Healthy,
        CaptureIntegrityState::Healthy,
    ))
}

#[test]
fn classification_domains_remain_independent() {
    let hierarchy = FairValueHierarchy::Level2;
    let depth = MarketDepth::OrderLevel;
    let quality = DataQuality::DirectUnverified;

    assert_eq!(hierarchy, FairValueHierarchy::Level2);
    assert_eq!(depth, MarketDepth::OrderLevel);
    assert_eq!(quality, DataQuality::DirectUnverified);
}

#[test]
// The controlling Task 4 contract explicitly exercises the `TryFrom` call shape. Production owns
// the infallible `From` implementation, so this resolves through the standard blanket conversion.
#[allow(clippy::unnecessary_fallible_conversions)]
fn modeled_level_two_evidence_is_not_execution_eligible() -> Result<(), Box<dyn Error>> {
    let evidence = QualificationEvidence::try_from(qualification_input(
        DataQuality::Modeled,
        Some(FairValueHierarchy::Level2),
    )?)?;

    assert_eq!(
        evidence.execution_eligibility(),
        ExecutionEligibility::Ineligible
    );
    assert!(evidence.has_failure(EligibilityFailure::QualityNotDirectVerified));
    assert!(evidence.has_failure(EligibilityFailure::FairValueEvidenceNotLevel1));
    Ok(())
}

#[test]
fn level_two_evidence_cannot_authorize_execution_even_with_direct_quality()
-> Result<(), Box<dyn Error>> {
    let evidence: QualificationEvidence = qualification_input(
        DataQuality::DirectVerified,
        Some(FairValueHierarchy::Level2),
    )?
    .into();

    assert_eq!(
        evidence.execution_eligibility(),
        ExecutionEligibility::Ineligible
    );
    assert!(evidence.has_failure(EligibilityFailure::FairValueEvidenceNotLevel1));
    Ok(())
}

#[test]
fn complete_direct_evidence_is_execution_eligible() -> Result<(), Box<dyn Error>> {
    let evidence: QualificationEvidence = qualification_input(
        DataQuality::DirectVerified,
        Some(FairValueHierarchy::Level1),
    )?
    .into();

    assert_eq!(
        evidence.execution_eligibility(),
        ExecutionEligibility::Eligible
    );
    assert_eq!(
        evidence.source_authorization(),
        SourceAuthorization::Authorized
    );
    assert_eq!(evidence.delivery_evidence(), DeliveryEvidence::DirectVenue);
    assert_eq!(evidence.sequence_integrity(), SequenceIntegrity::Valid);
    assert_eq!(
        evidence.snapshot_consistency(),
        SnapshotConsistency::Consistent
    );
    assert_eq!(evidence.checksum_integrity(), ChecksumIntegrity::Valid);
    assert_eq!(
        evidence.event_timing().integrity(),
        market_squawk_domain::TimestampIntegrity::Valid
    );
    assert_eq!(evidence.event_timing().maximum_future_skew_nanos(), 0);
    assert_eq!(evidence.trading_status(), TradingStatus::Active);
    assert_eq!(evidence.precision_integrity(), PrecisionIntegrity::Valid);
    assert_eq!(evidence.source_coverage(), SourceCoverageEvidence::Explicit);
    assert_eq!(evidence.book_integrity(), BookIntegrity::Consistent);
    assert_eq!(evidence.stream_integrity(), StreamIntegrityState::Healthy);
    assert_eq!(evidence.capture_integrity(), CaptureIntegrityState::Healthy);
    assert!(evidence.failures().is_empty());
    Ok(())
}

#[test]
fn ordinary_direct_market_data_needs_no_fair_value_assertion() -> Result<(), Box<dyn Error>> {
    let evidence: QualificationEvidence =
        qualification_input(DataQuality::DirectVerified, None)?.into();

    assert_eq!(evidence.fair_value_hierarchy(), None);
    assert_eq!(
        evidence.execution_eligibility(),
        ExecutionEligibility::Eligible
    );
    Ok(())
}

#[test]
fn heartbeat_does_not_update_market_freshness() -> Result<(), Box<dyn Error>> {
    let market_at = Timestamp::from_unix_nanos(1_000);
    let heartbeat_at = Timestamp::from_unix_nanos(2_000);
    let evaluated_at = Timestamp::from_unix_nanos(2_000);

    let freshness =
        FreshnessEvidence::assess(Some(market_at), Some(heartbeat_at), evaluated_at, 100)?;

    assert_eq!(freshness.last_market_event_at(), Some(market_at));
    assert_eq!(freshness.last_heartbeat_at(), Some(heartbeat_at));
    assert_eq!(freshness.evaluated_at(), evaluated_at);
    assert_eq!(freshness.maximum_age_nanos(), 100);
    assert_eq!(freshness.state(), FreshnessState::Stale);
    Ok(())
}

#[test]
fn capture_failure_fails_closed() -> Result<(), Box<dyn Error>> {
    let input = qualification_input(
        DataQuality::DirectVerified,
        Some(FairValueHierarchy::Level1),
    )?
    .with_capture_integrity(CaptureIntegrityState::Incomplete);
    let evidence: QualificationEvidence = input.into();

    assert_eq!(
        evidence.execution_eligibility(),
        ExecutionEligibility::Ineligible
    );
    assert!(evidence.has_failure(EligibilityFailure::CaptureIntegrity));
    Ok(())
}

#[test]
fn unsupported_sequence_is_not_immediate_action_evidence() -> Result<(), Box<dyn Error>> {
    let evidence: QualificationEvidence = qualification_input(DataQuality::DirectVerified, None)?
        .with_sequence_integrity(SequenceIntegrity::NotSupported)
        .into();

    assert_eq!(
        evidence.execution_eligibility(),
        ExecutionEligibility::Ineligible
    );
    assert!(evidence.has_failure(EligibilityFailure::SequenceIntegrity));
    Ok(())
}

#[test]
fn authorized_indirect_delivery_cannot_synthesize_eligibility() -> Result<(), Box<dyn Error>> {
    let evidence: QualificationEvidence = qualification_input(DataQuality::DirectVerified, None)?
        .with_delivery_evidence(DeliveryEvidence::Indirect)
        .into();

    assert_eq!(
        evidence.execution_eligibility(),
        ExecutionEligibility::Ineligible
    );
    assert!(evidence.has_failure(EligibilityFailure::DeliveryNotDirect));
    Ok(())
}

#[test]
fn deserialization_cannot_forge_derived_time_integrity() -> Result<(), Box<dyn Error>> {
    let received_at = Timestamp::from_unix_nanos(100);
    let mut value = serde_json::to_value(EventTimingEvidence::assess(None, received_at, 0)?)?;
    value["integrity"] = serde_json::json!("valid");

    assert!(serde_json::from_value::<EventTimingEvidence>(value).is_err());
    Ok(())
}

#[test]
fn deserialization_cannot_forge_market_freshness() -> Result<(), Box<dyn Error>> {
    let market_at = Timestamp::from_unix_nanos(100);
    let evaluated_at = Timestamp::from_unix_nanos(1_000);
    let mut value = serde_json::to_value(FreshnessEvidence::assess(
        Some(market_at),
        None,
        evaluated_at,
        10,
    )?)?;
    value["state"] = serde_json::json!("fresh");

    assert!(serde_json::from_value::<FreshnessEvidence>(value).is_err());
    Ok(())
}
