use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    BookIntegrity, CaptureIntegrityState, ChecksumCapability, ChecksumEvidence, ChecksumIntegrity,
    ChecksumScope, ChecksumValue, ConnectionGeneration, DataQuality, DeliveryEvidence,
    EligibilityFailure, ExecutionEligibility, FreshnessState, InstrumentId, IntegrityCapabilities,
    IntegrityEvidenceError, IntegrityRule, LiveTimingAssessment, LiveTimingPolicy, MarketDepth,
    MarketEventTiming, PrecisionIntegrity, QualificationComponent, QualificationError,
    QualificationEvidence, QualificationEvidenceId, QualificationEvidenceInput, RuleVersion,
    SequenceCapability, SequenceEvidence, SequenceIntegrity, SequenceNumber,
    SequenceValidationRule, SnapshotEvidence, SourceAuthorization, SourceCoverageEvidence,
    SourceId, SourceIdentifier, StreamIntegrityState, Timestamp, TimestampIntegrity, TradingStatus,
    VenueId,
};

fn generation(value: u64) -> Result<ConnectionGeneration, Box<dyn Error>> {
    ConnectionGeneration::new(value).map_err(Into::into)
}

fn rule(name: &str) -> Result<IntegrityRule, Box<dyn Error>> {
    Ok(IntegrityRule::new(
        SourceIdentifier::try_from(name)?,
        RuleVersion::new(1)?,
    ))
}

fn timing(
    source_at: Option<i64>,
    received_at: i64,
    evaluated_at: i64,
    policy: LiveTimingPolicy,
) -> Result<LiveTimingAssessment, Box<dyn Error>> {
    let market_event = MarketEventTiming::new(
        source_at.map(Timestamp::from_unix_nanos),
        Timestamp::from_unix_nanos(received_at),
    );
    Ok(LiveTimingAssessment::assess(
        generation(7)?,
        Some(market_event),
        Some(Timestamp::from_unix_nanos(evaluated_at)),
        Timestamp::from_unix_nanos(evaluated_at),
        policy,
    )?)
}

fn policy() -> Result<LiveTimingPolicy, Box<dyn Error>> {
    LiveTimingPolicy::new(5, 25, 50, 20).map_err(Into::into)
}

fn valid_sequence() -> Result<SequenceEvidence, Box<dyn Error>> {
    Ok(SequenceEvidence::validate(
        SequenceCapability::Provided,
        Some(rule("provider.sequence.consecutive")?),
        SequenceValidationRule::Consecutive,
        generation(7)?,
        Some(SequenceNumber::new(40)),
        Some(SequenceNumber::new(41)),
        Some(SequenceNumber::new(42)),
    )?)
}

fn valid_snapshot() -> Result<SnapshotEvidence, Box<dyn Error>> {
    Ok(SnapshotEvidence::assess(
        generation(7)?,
        generation(7)?,
        Some(SequenceNumber::new(40)),
        Some(SequenceNumber::new(42)),
    )?)
}

fn checksum(capability: ChecksumCapability) -> Result<ChecksumEvidence, Box<dyn Error>> {
    match capability {
        ChecksumCapability::Provided => Ok(ChecksumEvidence::validate(
            capability,
            Some(rule("provider.checksum.crc32")?),
            generation(7)?,
            Some(ChecksumScope::new(
                MarketDepth::PriceLevel,
                10,
                SourceIdentifier::try_from("top-ten-bid-ask")?,
            )?),
            Some(ChecksumValue::new(0xAABBCCDD)),
            Some(ChecksumValue::new(0xAABBCCDD)),
        )?),
        ChecksumCapability::Unsupported => Ok(ChecksumEvidence::unsupported(generation(7)?)),
    }
}

fn qualification_input() -> Result<QualificationEvidenceInput, Box<dyn Error>> {
    Ok(QualificationEvidenceInput::new(
        QualificationEvidenceId::new(SourceIdentifier::try_from(
            "qualification:direct-feed:7:42",
        )?),
        DataQuality::DirectVerified,
        IntegrityCapabilities::new(SequenceCapability::Provided, ChecksumCapability::Provided),
        SourceAuthorization::Authorized,
        DeliveryEvidence::DirectVenue,
        SourceId::try_from("direct-feed")?,
        VenueId::try_from("XNYS")?,
        InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb")?,
        generation(7)?,
        valid_sequence()?,
        valid_snapshot()?,
        checksum(ChecksumCapability::Provided)?,
        timing(Some(995), 1_000, 1_010, policy()?)?,
        TradingStatus::Active,
        PrecisionIntegrity::Valid,
        SourceCoverageEvidence::Explicit,
        BookIntegrity::Consistent,
        StreamIntegrityState::Healthy,
        CaptureIntegrityState::Healthy,
    ))
}

#[test]
fn ancient_source_event_received_now_is_not_fresh_direct_evidence() -> Result<(), Box<dyn Error>> {
    let assessment = timing(Some(900), 1_000, 1_010, policy()?)?;

    assert_eq!(
        assessment.timestamp_integrity(),
        TimestampIntegrity::Invalid
    );
    assert_eq!(assessment.freshness(), FreshnessState::Fresh);
    Ok(())
}

#[test]
fn missing_source_timestamp_is_invalid_for_direct_verification() -> Result<(), Box<dyn Error>> {
    let assessment = timing(None, 1_000, 1_010, policy()?)?;

    assert_eq!(
        assessment.timestamp_integrity(),
        TimestampIntegrity::Invalid
    );
    Ok(())
}

#[test]
fn heartbeat_cannot_refresh_market_freshness() -> Result<(), Box<dyn Error>> {
    let market_event = MarketEventTiming::new(
        Some(Timestamp::from_unix_nanos(1_000)),
        Timestamp::from_unix_nanos(1_000),
    );
    let assessment = LiveTimingAssessment::assess(
        generation(7)?,
        Some(market_event),
        Some(Timestamp::from_unix_nanos(2_000)),
        Timestamp::from_unix_nanos(2_000),
        LiveTimingPolicy::new(0, 2_000, 2_000, 10)?,
    )?;

    assert_eq!(assessment.freshness(), FreshnessState::Stale);
    Ok(())
}

#[test]
fn time_boundaries_are_inclusive_and_one_nanosecond_beyond_fails() -> Result<(), Box<dyn Error>> {
    let boundary_policy = LiveTimingPolicy::new(5, 25, 50, 20)?;
    let at_boundary = timing(Some(975), 1_000, 1_020, boundary_policy)?;
    let beyond_transport = timing(Some(974), 1_000, 1_020, boundary_policy)?;
    let beyond_freshness = timing(Some(995), 1_000, 1_021, boundary_policy)?;
    let future_boundary = timing(Some(1_005), 1_000, 1_000, boundary_policy)?;
    let future_beyond = timing(Some(1_006), 1_000, 1_000, boundary_policy)?;

    assert_eq!(at_boundary.timestamp_integrity(), TimestampIntegrity::Valid);
    assert_eq!(at_boundary.freshness(), FreshnessState::Fresh);
    assert_eq!(
        beyond_transport.timestamp_integrity(),
        TimestampIntegrity::Invalid
    );
    assert_eq!(beyond_freshness.freshness(), FreshnessState::Stale);
    assert_eq!(
        future_boundary.timestamp_integrity(),
        TimestampIntegrity::Valid
    );
    assert_eq!(
        future_beyond.timestamp_integrity(),
        TimestampIntegrity::Invalid
    );
    Ok(())
}

#[test]
fn timing_rejects_evaluation_before_receive_and_handles_i64_edges() -> Result<(), Box<dyn Error>> {
    let event = MarketEventTiming::new(
        Some(Timestamp::from_unix_nanos(i64::MIN)),
        Timestamp::from_unix_nanos(i64::MIN),
    );
    let assessment = LiveTimingAssessment::assess(
        generation(7)?,
        Some(event),
        None,
        Timestamp::from_unix_nanos(i64::MAX),
        LiveTimingPolicy::new(0, i64::MAX as u64, i64::MAX as u64, i64::MAX as u64)?,
    )?;
    assert_eq!(
        assessment.timestamp_integrity(),
        TimestampIntegrity::Invalid
    );
    assert_eq!(assessment.freshness(), FreshnessState::Stale);

    let future_event = MarketEventTiming::new(
        Some(Timestamp::from_unix_nanos(2)),
        Timestamp::from_unix_nanos(2),
    );
    assert!(
        LiveTimingAssessment::assess(
            generation(7)?,
            Some(future_event),
            None,
            Timestamp::from_unix_nanos(1),
            policy()?,
        )
        .is_err()
    );
    assert!(LiveTimingPolicy::new(u64::MAX, 1, 1, 1).is_err());
    Ok(())
}

#[test]
fn sequence_and_checksum_results_are_derived_and_auditable() -> Result<(), Box<dyn Error>> {
    let sequence = valid_sequence()?;
    assert_eq!(sequence.integrity(), SequenceIntegrity::Valid);
    assert_eq!(sequence.previous_sequence(), Some(SequenceNumber::new(41)));
    assert_eq!(sequence.observed_sequence(), Some(SequenceNumber::new(42)));
    assert_eq!(sequence.snapshot_sequence(), Some(SequenceNumber::new(40)));
    assert_eq!(
        sequence.rule().map(IntegrityRule::version),
        Some(RuleVersion::new(1)?)
    );

    let checksum = checksum(ChecksumCapability::Provided)?;
    assert_eq!(checksum.integrity(), ChecksumIntegrity::Valid);
    assert_eq!(checksum.expected(), Some(ChecksumValue::new(0xAABBCCDD)));
    assert_eq!(checksum.computed(), Some(ChecksumValue::new(0xAABBCCDD)));
    assert_eq!(
        checksum.scope().map(ChecksumScope::depth),
        Some(MarketDepth::PriceLevel)
    );
    Ok(())
}

#[test]
fn capability_contradictions_are_typed_errors() -> Result<(), Box<dyn Error>> {
    assert!(matches!(
        SequenceEvidence::validate(
            SequenceCapability::Unsupported,
            Some(rule("provider.sequence")?),
            SequenceValidationRule::Consecutive,
            generation(7)?,
            None,
            Some(SequenceNumber::new(1)),
            Some(SequenceNumber::new(2)),
        ),
        Err(IntegrityEvidenceError::CapabilityContradiction { .. })
    ));
    assert!(matches!(
        ChecksumEvidence::validate(
            ChecksumCapability::Unsupported,
            Some(rule("provider.checksum")?),
            generation(7)?,
            None,
            None,
            None,
        ),
        Err(IntegrityEvidenceError::CapabilityContradiction { .. })
    ));
    Ok(())
}

#[test]
fn qualification_rejects_generation_and_capability_disagreement() -> Result<(), Box<dyn Error>> {
    let mismatched_sequence = SequenceEvidence::validate(
        SequenceCapability::Provided,
        Some(rule("provider.sequence")?),
        SequenceValidationRule::Consecutive,
        generation(8)?,
        Some(SequenceNumber::new(40)),
        Some(SequenceNumber::new(41)),
        Some(SequenceNumber::new(42)),
    )?;
    let result = QualificationEvidence::try_from(
        qualification_input()?.with_sequence_evidence(mismatched_sequence),
    );
    assert!(matches!(
        result,
        Err(QualificationError::GenerationMismatch {
            component: QualificationComponent::Sequence,
            ..
        })
    ));

    let result = QualificationEvidence::try_from(
        qualification_input()?.with_integrity_capabilities(IntegrityCapabilities::new(
            SequenceCapability::Provided,
            ChecksumCapability::Unsupported,
        )),
    );
    assert!(matches!(
        result,
        Err(QualificationError::CapabilityMismatch {
            component: QualificationComponent::Checksum,
            ..
        })
    ));
    Ok(())
}

#[test]
fn quality_is_evaluator_output_subject_to_source_ceiling() -> Result<(), Box<dyn Error>> {
    let direct = QualificationEvidence::try_from(qualification_input()?)?;
    assert_eq!(direct.quality(), DataQuality::DirectVerified);
    assert_eq!(
        direct.execution_eligibility(),
        ExecutionEligibility::Eligible
    );

    let modeled = QualificationEvidence::try_from(
        qualification_input()?.with_quality_ceiling(DataQuality::Modeled),
    )?;
    assert_eq!(modeled.quality(), DataQuality::Modeled);
    assert_eq!(
        modeled.execution_eligibility(),
        ExecutionEligibility::Ineligible
    );
    assert!(modeled.has_failure(EligibilityFailure::QualityCeiling));
    Ok(())
}

#[test]
fn unsupported_checksum_is_eligible_only_when_metadata_declares_it() -> Result<(), Box<dyn Error>> {
    let input = qualification_input()?
        .with_integrity_capabilities(IntegrityCapabilities::new(
            SequenceCapability::Provided,
            ChecksumCapability::Unsupported,
        ))
        .with_checksum_evidence(checksum(ChecksumCapability::Unsupported)?);
    let evidence = QualificationEvidence::try_from(input)?;

    assert_eq!(
        evidence.checksum_evidence().integrity(),
        ChecksumIntegrity::NotSupported
    );
    assert_eq!(
        evidence.execution_eligibility(),
        ExecutionEligibility::Eligible
    );
    Ok(())
}

#[test]
fn disabled_capture_is_explicitly_eligible_but_incomplete_capture_is_not()
-> Result<(), Box<dyn Error>> {
    let disabled = QualificationEvidence::try_from(
        qualification_input()?.with_capture_integrity(CaptureIntegrityState::Disabled),
    )?;
    assert_eq!(
        disabled.execution_eligibility(),
        ExecutionEligibility::Eligible
    );

    let incomplete = QualificationEvidence::try_from(
        qualification_input()?.with_capture_integrity(CaptureIntegrityState::Incomplete),
    )?;
    assert_eq!(
        incomplete.execution_eligibility(),
        ExecutionEligibility::Ineligible
    );
    assert!(incomplete.has_failure(EligibilityFailure::CaptureIntegrity));
    Ok(())
}
