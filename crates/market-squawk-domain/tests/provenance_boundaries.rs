use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    AggressorSide, AvailabilityEvidence, BookIntegrity, CaptureIntegrityState, ChecksumCapability,
    ChecksumEvidence, ChecksumScope, ChecksumValue, DataQuality, DeliveryEvidence,
    ExecutionEligibility, InstrumentId, IntegrityCapabilities, IntegrityRule, LiveProvenance,
    LiveTimingAssessment, LiveTimingPolicy, LiveVerificationState, MarketDepth, MarketEventTiming,
    PayloadReference, PrecisionIntegrity, PriceTicks, ProvenanceError, QualificationEvidence,
    QualificationEvidenceId, QualificationEvidenceInput, QuantityLots, ResearchContext,
    ResearchProvenance, ResearchTime, RevisionNumber, RuleVersion, SequenceCapability,
    SequenceEvidence, SequenceNumber, SequenceValidationRule, SnapshotEvidence,
    SourceAuthorization, SourceCoverageEvidence, SourceId, SourceIdentifier, StreamIntegrityState,
    Timestamp, TradeEvent, TradingStatus, VenueId,
};

fn instrument() -> Result<InstrumentId, Box<dyn Error>> {
    InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb").map_err(Into::into)
}

fn payload_reference() -> Result<PayloadReference, Box<dyn Error>> {
    Ok(PayloadReference::SourceReference(
        SourceIdentifier::try_from("capture:7:42")?,
    ))
}

fn eligible_qualification() -> Result<QualificationEvidence, Box<dyn Error>> {
    let generation = market_squawk_domain::ConnectionGeneration::new(7)?;
    let sequence_rule = IntegrityRule::new(
        SourceIdentifier::try_from("provider.sequence.consecutive")?,
        RuleVersion::new(1)?,
    );
    let checksum_rule = IntegrityRule::new(
        SourceIdentifier::try_from("provider.checksum.crc32")?,
        RuleVersion::new(1)?,
    );
    let input = QualificationEvidenceInput::new(
        QualificationEvidenceId::new(SourceIdentifier::try_from("qualification:7:42")?),
        DataQuality::DirectVerified,
        IntegrityCapabilities::new(SequenceCapability::Provided, ChecksumCapability::Provided),
        SourceAuthorization::Authorized,
        DeliveryEvidence::DirectVenue,
        SourceId::try_from("direct-feed")?,
        VenueId::try_from("XNYS")?,
        instrument()?,
        generation,
        SequenceEvidence::validate(
            SequenceCapability::Provided,
            Some(sequence_rule),
            SequenceValidationRule::Consecutive,
            generation,
            Some(SequenceNumber::new(40)),
            Some(SequenceNumber::new(41)),
            Some(SequenceNumber::new(42)),
        )?,
        SnapshotEvidence::assess(
            generation,
            generation,
            Some(SequenceNumber::new(40)),
            Some(SequenceNumber::new(42)),
        )?,
        ChecksumEvidence::validate(
            ChecksumCapability::Provided,
            Some(checksum_rule),
            generation,
            Some(ChecksumScope::new(
                MarketDepth::PriceLevel,
                10,
                SourceIdentifier::try_from("top-ten-bid-ask")?,
            )?),
            Some(ChecksumValue::new(10)),
            Some(ChecksumValue::new(10)),
        )?,
        LiveTimingAssessment::assess(
            generation,
            Some(MarketEventTiming::new(
                Some(Timestamp::from_unix_nanos(995)),
                Timestamp::from_unix_nanos(1_000),
            )),
            None,
            Timestamp::from_unix_nanos(1_010),
            LiveTimingPolicy::new(5, 50, 100, 50)?,
        )?,
        TradingStatus::Active,
        PrecisionIntegrity::Valid,
        SourceCoverageEvidence::Explicit,
        BookIntegrity::Consistent,
        StreamIntegrityState::Healthy,
        CaptureIntegrityState::Healthy,
    );
    QualificationEvidence::try_from(input).map_err(Into::into)
}

#[test]
fn decoded_live_provenance_cannot_claim_direct_verified() -> Result<(), Box<dyn Error>> {
    let result = LiveProvenance::decoded(
        SourceId::try_from("direct-feed")?,
        Some(instrument()?),
        Some(VenueId::try_from("XNYS")?),
        SourceIdentifier::try_from("trade-42")?,
        Some(Timestamp::from_unix_nanos(995)),
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_001),
        DataQuality::DirectVerified,
        market_squawk_domain::ConnectionGeneration::new(7)?,
        SourceCoverageEvidence::Explicit,
        payload_reference()?,
    );

    assert!(matches!(
        result,
        Err(ProvenanceError::UnqualifiedDirectVerified)
    ));
    Ok(())
}

#[test]
fn decoded_event_is_explicitly_unverified_before_book_qualification() -> Result<(), Box<dyn Error>>
{
    let provenance = LiveProvenance::decoded(
        SourceId::try_from("direct-feed")?,
        Some(instrument()?),
        Some(VenueId::try_from("XNYS")?),
        SourceIdentifier::try_from("trade-42")?,
        Some(Timestamp::from_unix_nanos(995)),
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_001),
        DataQuality::DirectUnverified,
        market_squawk_domain::ConnectionGeneration::new(7)?,
        SourceCoverageEvidence::Explicit,
        payload_reference()?,
    )?;
    let event = TradeEvent::new(
        provenance,
        PriceTicks::new(100),
        QuantityLots::new(2)?,
        AggressorSide::Buy,
    )?;

    assert_eq!(event.provenance().quality(), DataQuality::DirectUnverified);
    assert_eq!(event.provenance().qualification_evidence_id(), None);
    assert_eq!(event.provenance().connection_generation().get(), 7);
    assert_eq!(
        event.provenance().coverage(),
        SourceCoverageEvidence::Explicit
    );
    Ok(())
}

#[test]
fn research_unknown_availability_remains_unknown_and_conservative() -> Result<(), Box<dyn Error>> {
    let provenance = ResearchProvenance::new(
        SourceId::try_from("historical-file")?,
        Some(instrument()?),
        None,
        SourceIdentifier::try_from("row-42")?,
        None,
        Timestamp::from_unix_nanos(500),
        Timestamp::from_unix_nanos(600),
        DataQuality::Aggregated,
        payload_reference()?,
        AvailabilityEvidence::unknown(),
    )?;
    let context = ResearchContext::new(
        provenance,
        ResearchTime::new(
            Timestamp::from_unix_nanos(100),
            None,
            RevisionNumber::new(1)?,
            None,
        )?,
    )?;

    assert_eq!(
        context
            .provenance()
            .availability()
            .conservative_available_at(),
        None
    );
    assert!(
        !context
            .provenance()
            .availability()
            .is_point_in_time_evidenced()
    );
    Ok(())
}

#[test]
fn inferred_availability_is_not_silently_point_in_time_evidence() -> Result<(), Box<dyn Error>> {
    let inferred = AvailabilityEvidence::inferred(
        Timestamp::from_unix_nanos(300),
        SourceIdentifier::try_from("provider-date-field")?,
    );

    assert_eq!(
        inferred.reported_at(),
        Some(Timestamp::from_unix_nanos(300))
    );
    assert_eq!(inferred.conservative_available_at(), None);
    assert!(!inferred.is_point_in_time_evidenced());
    Ok(())
}

#[test]
fn current_schema_is_authored_internally_and_unknown_v1_fields_are_rejected()
-> Result<(), Box<dyn Error>> {
    let provenance = LiveProvenance::decoded(
        SourceId::try_from("direct-feed")?,
        Some(instrument()?),
        Some(VenueId::try_from("XNYS")?),
        SourceIdentifier::try_from("trade-42")?,
        Some(Timestamp::from_unix_nanos(995)),
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_001),
        DataQuality::DirectUnverified,
        market_squawk_domain::ConnectionGeneration::new(7)?,
        SourceCoverageEvidence::Explicit,
        payload_reference()?,
    )?;
    let mut value = serde_json::to_value(provenance)?;
    assert_eq!(value["schema_version"], serde_json::json!(1));
    value["unexpected"] = serde_json::json!(true);
    assert!(serde_json::from_value::<LiveProvenance>(value).is_err());
    Ok(())
}

#[test]
fn verified_records_round_trip_as_archival_assertions_that_require_requalification()
-> Result<(), Box<dyn Error>> {
    let decoded = LiveProvenance::decoded(
        SourceId::try_from("direct-feed")?,
        Some(instrument()?),
        Some(VenueId::try_from("XNYS")?),
        SourceIdentifier::try_from("trade-42")?,
        Some(Timestamp::from_unix_nanos(995)),
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_001),
        DataQuality::DirectUnverified,
        market_squawk_domain::ConnectionGeneration::new(7)?,
        SourceCoverageEvidence::Explicit,
        payload_reference()?,
    )?;
    let qualification = eligible_qualification()?;
    assert_eq!(
        qualification.execution_eligibility(),
        ExecutionEligibility::Eligible
    );
    let qualified = decoded.promote(&qualification)?;
    assert!(qualified.is_currently_qualified());

    let wire = serde_json::to_string(&qualified)?;
    let recorded: LiveProvenance = serde_json::from_str(&wire)?;

    assert_eq!(recorded.quality(), DataQuality::DirectVerified);
    assert_eq!(
        recorded.verification_state(),
        LiveVerificationState::RecordedRequiresRequalification
    );
    assert!(!recorded.is_currently_qualified());
    assert!(recorded.requires_requalification());
    assert_eq!(
        recorded
            .qualification_evidence_id()
            .map(QualificationEvidenceId::as_source_identifier)
            .map(SourceIdentifier::as_str),
        Some("qualification:7:42")
    );
    Ok(())
}

#[test]
fn qualification_promotion_rejects_identity_generation_timing_and_coverage_mismatch()
-> Result<(), Box<dyn Error>> {
    let qualification = eligible_qualification()?;
    let cases = [
        (
            LiveProvenance::decoded(
                SourceId::try_from("other-feed")?,
                Some(instrument()?),
                Some(VenueId::try_from("XNYS")?),
                SourceIdentifier::try_from("trade-42")?,
                Some(Timestamp::from_unix_nanos(995)),
                Timestamp::from_unix_nanos(1_000),
                Timestamp::from_unix_nanos(1_001),
                DataQuality::DirectUnverified,
                market_squawk_domain::ConnectionGeneration::new(7)?,
                SourceCoverageEvidence::Explicit,
                payload_reference()?,
            )?,
            ProvenanceError::QualificationIdentityMismatch,
        ),
        (
            LiveProvenance::decoded(
                SourceId::try_from("direct-feed")?,
                Some(instrument()?),
                Some(VenueId::try_from("XNAS")?),
                SourceIdentifier::try_from("trade-42")?,
                Some(Timestamp::from_unix_nanos(995)),
                Timestamp::from_unix_nanos(1_000),
                Timestamp::from_unix_nanos(1_001),
                DataQuality::DirectUnverified,
                market_squawk_domain::ConnectionGeneration::new(7)?,
                SourceCoverageEvidence::Explicit,
                payload_reference()?,
            )?,
            ProvenanceError::QualificationIdentityMismatch,
        ),
        (
            LiveProvenance::decoded(
                SourceId::try_from("direct-feed")?,
                Some(instrument()?),
                Some(VenueId::try_from("XNYS")?),
                SourceIdentifier::try_from("trade-42")?,
                Some(Timestamp::from_unix_nanos(995)),
                Timestamp::from_unix_nanos(1_000),
                Timestamp::from_unix_nanos(1_001),
                DataQuality::DirectUnverified,
                market_squawk_domain::ConnectionGeneration::new(8)?,
                SourceCoverageEvidence::Explicit,
                payload_reference()?,
            )?,
            ProvenanceError::QualificationIdentityMismatch,
        ),
        (
            LiveProvenance::decoded(
                SourceId::try_from("direct-feed")?,
                Some(instrument()?),
                Some(VenueId::try_from("XNYS")?),
                SourceIdentifier::try_from("trade-42")?,
                Some(Timestamp::from_unix_nanos(994)),
                Timestamp::from_unix_nanos(1_000),
                Timestamp::from_unix_nanos(1_001),
                DataQuality::DirectUnverified,
                market_squawk_domain::ConnectionGeneration::new(7)?,
                SourceCoverageEvidence::Explicit,
                payload_reference()?,
            )?,
            ProvenanceError::QualificationTimingMismatch,
        ),
        (
            LiveProvenance::decoded(
                SourceId::try_from("direct-feed")?,
                Some(instrument()?),
                Some(VenueId::try_from("XNYS")?),
                SourceIdentifier::try_from("trade-42")?,
                Some(Timestamp::from_unix_nanos(995)),
                Timestamp::from_unix_nanos(1_000),
                Timestamp::from_unix_nanos(1_001),
                DataQuality::DirectUnverified,
                market_squawk_domain::ConnectionGeneration::new(7)?,
                SourceCoverageEvidence::Insufficient,
                payload_reference()?,
            )?,
            ProvenanceError::QualificationCoverageMismatch,
        ),
    ];

    for (provenance, expected) in cases {
        assert_eq!(provenance.promote(&qualification), Err(expected));
    }
    Ok(())
}
