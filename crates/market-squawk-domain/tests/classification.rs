use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    BookIntegrity, CaptureIntegrityState, ChecksumCapability, ChecksumEvidence, ChecksumScope,
    ChecksumValue, ConnectionGeneration, DataQuality, DeliveryEvidence, EligibilityFailure,
    ExecutionEligibility, FairValueHierarchy, InstrumentId, IntegrityCapabilities, IntegrityRule,
    LiveTimingAssessment, LiveTimingPolicy, MarketDepth, MarketEventTiming, PrecisionIntegrity,
    QualificationEvidence, QualificationEvidenceId, QualificationEvidenceInput, RuleVersion,
    SequenceCapability, SequenceEvidence, SequenceNumber, SequenceValidationRule, SnapshotEvidence,
    SourceAuthorization, SourceCoverageEvidence, SourceId, SourceIdentifier, StreamIntegrityState,
    Timestamp, TradingStatus, VenueId,
};

fn generation() -> Result<ConnectionGeneration, Box<dyn Error>> {
    ConnectionGeneration::new(7).map_err(Into::into)
}

fn rule(name: &str) -> Result<IntegrityRule, Box<dyn Error>> {
    Ok(IntegrityRule::new(
        SourceIdentifier::try_from(name)?,
        RuleVersion::new(1)?,
    ))
}

fn sequence(
    previous: Option<u64>,
    observed: Option<u64>,
) -> Result<SequenceEvidence, Box<dyn Error>> {
    match observed {
        Some(observed) => Ok(SequenceEvidence::validate(
            SequenceCapability::Provided,
            Some(rule("provider.sequence.consecutive")?),
            SequenceValidationRule::Consecutive,
            generation()?,
            Some(SequenceNumber::new(40)),
            previous.map(SequenceNumber::new),
            Some(SequenceNumber::new(observed)),
        )?),
        None => Ok(SequenceEvidence::uninitialized(
            rule("provider.sequence.consecutive")?,
            SequenceValidationRule::Consecutive,
            generation()?,
            Some(SequenceNumber::new(40)),
        )),
    }
}

fn checksum(expected: u64, computed: u64) -> Result<ChecksumEvidence, Box<dyn Error>> {
    Ok(ChecksumEvidence::validate(
        ChecksumCapability::Provided,
        Some(rule("provider.checksum.crc32")?),
        generation()?,
        Some(ChecksumScope::new(
            MarketDepth::PriceLevel,
            10,
            SourceIdentifier::try_from("top-ten-bid-ask")?,
        )?),
        Some(ChecksumValue::new(expected)),
        Some(ChecksumValue::new(computed)),
    )?)
}

fn timing(
    source_at: Option<i64>,
    received_at: i64,
    evaluated_at: i64,
) -> Result<LiveTimingAssessment, Box<dyn Error>> {
    Ok(LiveTimingAssessment::assess(
        generation()?,
        Some(MarketEventTiming::new(
            source_at.map(Timestamp::from_unix_nanos),
            Timestamp::from_unix_nanos(received_at),
        )),
        None,
        Timestamp::from_unix_nanos(evaluated_at),
        LiveTimingPolicy::new(5, 50, 100, 50)?,
    )?)
}

fn base_input() -> Result<QualificationEvidenceInput, Box<dyn Error>> {
    Ok(QualificationEvidenceInput::new(
        QualificationEvidenceId::new(SourceIdentifier::try_from("qualification:7:42")?),
        DataQuality::DirectVerified,
        IntegrityCapabilities::new(SequenceCapability::Provided, ChecksumCapability::Provided),
        SourceAuthorization::Authorized,
        DeliveryEvidence::DirectVenue,
        SourceId::try_from("direct-feed")?,
        VenueId::try_from("XNYS")?,
        InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb")?,
        generation()?,
        sequence(Some(41), Some(42))?,
        SnapshotEvidence::assess(
            generation()?,
            generation()?,
            Some(SequenceNumber::new(40)),
            Some(SequenceNumber::new(42)),
        )?,
        checksum(10, 10)?,
        timing(Some(995), 1_000, 1_010)?,
        TradingStatus::Active,
        PrecisionIntegrity::Valid,
        SourceCoverageEvidence::Explicit,
        BookIntegrity::Consistent,
        StreamIntegrityState::Healthy,
        CaptureIntegrityState::Healthy,
    ))
}

fn assert_ineligible(
    input: QualificationEvidenceInput,
    failure: EligibilityFailure,
) -> Result<(), Box<dyn Error>> {
    let evidence = QualificationEvidence::try_from(input)?;
    assert_eq!(
        evidence.execution_eligibility(),
        ExecutionEligibility::Ineligible
    );
    assert!(evidence.has_failure(failure));
    Ok(())
}

#[test]
fn classification_domains_remain_independent_and_level_three_is_not_live_input()
-> Result<(), Box<dyn Error>> {
    let hierarchy = FairValueHierarchy::Level3;
    let depth = MarketDepth::OrderLevel;
    let evidence = QualificationEvidence::try_from(base_input()?)?;

    assert_eq!(hierarchy, FairValueHierarchy::Level3);
    assert_eq!(depth, MarketDepth::OrderLevel);
    assert_eq!(evidence.quality(), DataQuality::DirectVerified);
    assert_eq!(
        evidence.execution_eligibility(),
        ExecutionEligibility::Eligible
    );
    Ok(())
}

#[test]
fn every_non_direct_quality_ceiling_fails_closed() -> Result<(), Box<dyn Error>> {
    for ceiling in [
        DataQuality::DirectUnverified,
        DataQuality::OfficialDelayed,
        DataQuality::Aggregated,
        DataQuality::Indicative,
        DataQuality::Modeled,
        DataQuality::Estimated,
        DataQuality::Stale,
        DataQuality::Quarantined,
    ] {
        let evidence =
            QualificationEvidence::try_from(base_input()?.with_quality_ceiling(ceiling))?;
        assert_eq!(
            evidence.execution_eligibility(),
            ExecutionEligibility::Ineligible
        );
        assert!(evidence.has_failure(EligibilityFailure::QualityCeiling));
        assert_ne!(evidence.quality(), DataQuality::DirectVerified);
    }
    Ok(())
}

#[test]
fn every_nonaffirmative_authorization_and_delivery_variant_fails_closed()
-> Result<(), Box<dyn Error>> {
    assert_ineligible(
        base_input()?.with_source_authorization(SourceAuthorization::Unauthorized),
        EligibilityFailure::SourceUnauthorized,
    )?;
    for delivery in [DeliveryEvidence::Indirect, DeliveryEvidence::Unknown] {
        assert_ineligible(
            base_input()?.with_delivery_evidence(delivery),
            EligibilityFailure::DeliveryNotDirect,
        )?;
    }
    let broker = QualificationEvidence::try_from(
        base_input()?.with_delivery_evidence(DeliveryEvidence::AuthorizedBroker),
    )?;
    assert_eq!(
        broker.execution_eligibility(),
        ExecutionEligibility::Eligible
    );
    Ok(())
}

#[test]
fn every_nonaffirmative_sequence_and_snapshot_state_fails_closed() -> Result<(), Box<dyn Error>> {
    assert_ineligible(
        base_input()?
            .with_sequence_evidence(sequence(Some(41), Some(43))?)
            .with_snapshot_evidence(SnapshotEvidence::assess(
                generation()?,
                generation()?,
                Some(SequenceNumber::new(40)),
                Some(SequenceNumber::new(43)),
            )?),
        EligibilityFailure::SequenceIntegrity,
    )?;
    assert_ineligible(
        base_input()?.with_sequence_evidence(sequence(None, None)?),
        EligibilityFailure::SequenceIntegrity,
    )?;
    assert_ineligible(
        base_input()?
            .with_integrity_capabilities(IntegrityCapabilities::new(
                SequenceCapability::Unsupported,
                ChecksumCapability::Provided,
            ))
            .with_sequence_evidence(SequenceEvidence::unsupported(generation()?)),
        EligibilityFailure::SequenceIntegrity,
    )?;

    let inconsistent_sequence = sequence(Some(41), Some(39))?;
    let inconsistent_snapshot = SnapshotEvidence::assess(
        generation()?,
        generation()?,
        Some(SequenceNumber::new(40)),
        Some(SequenceNumber::new(39)),
    )?;
    let inconsistent = QualificationEvidence::try_from(
        base_input()?
            .with_sequence_evidence(inconsistent_sequence)
            .with_snapshot_evidence(inconsistent_snapshot),
    )?;
    assert!(inconsistent.has_failure(EligibilityFailure::SnapshotConsistency));
    assert_eq!(inconsistent.quality(), DataQuality::Quarantined);

    assert_ineligible(
        base_input()?.with_snapshot_evidence(SnapshotEvidence::uninitialized(generation()?)),
        EligibilityFailure::SnapshotConsistency,
    )?;
    Ok(())
}

#[test]
fn every_nonaffirmative_checksum_state_fails_closed_under_supported_metadata()
-> Result<(), Box<dyn Error>> {
    assert_ineligible(
        base_input()?.with_checksum_evidence(checksum(10, 11)?),
        EligibilityFailure::ChecksumIntegrity,
    )?;
    assert_ineligible(
        base_input()?.with_checksum_evidence(ChecksumEvidence::unchecked(
            rule("provider.checksum.crc32")?,
            generation()?,
            ChecksumScope::new(
                MarketDepth::PriceLevel,
                10,
                SourceIdentifier::try_from("top-ten-bid-ask")?,
            )?,
        )),
        EligibilityFailure::ChecksumIntegrity,
    )?;
    Ok(())
}

#[test]
fn every_nonaffirmative_timing_and_freshness_state_fails_closed() -> Result<(), Box<dyn Error>> {
    assert_ineligible(
        base_input()?.with_timing(timing(None, 1_000, 1_010)?),
        EligibilityFailure::EventTiming,
    )?;
    assert_ineligible(
        base_input()?.with_timing(timing(Some(995), 1_000, 1_100)?),
        EligibilityFailure::MarketFreshness,
    )?;
    let unknown = LiveTimingAssessment::assess(
        generation()?,
        None,
        Some(Timestamp::from_unix_nanos(1_010)),
        Timestamp::from_unix_nanos(1_010),
        LiveTimingPolicy::new(5, 50, 100, 50)?,
    )?;
    let unknown_evidence = QualificationEvidence::try_from(base_input()?.with_timing(unknown))?;
    assert!(unknown_evidence.has_failure(EligibilityFailure::EventTiming));
    assert!(unknown_evidence.has_failure(EligibilityFailure::MarketFreshness));
    Ok(())
}

#[test]
fn every_nonaffirmative_status_precision_coverage_and_book_variant_fails_closed()
-> Result<(), Box<dyn Error>> {
    for status in [
        TradingStatus::Halted,
        TradingStatus::Inactive,
        TradingStatus::Delisted,
    ] {
        assert_ineligible(
            base_input()?.with_trading_status(status),
            EligibilityFailure::TradingStatus,
        )?;
    }
    assert_ineligible(
        base_input()?.with_precision_integrity(PrecisionIntegrity::Invalid),
        EligibilityFailure::Precision,
    )?;
    for coverage in [
        SourceCoverageEvidence::Insufficient,
        SourceCoverageEvidence::Unknown,
    ] {
        assert_ineligible(
            base_input()?.with_source_coverage(coverage),
            EligibilityFailure::Coverage,
        )?;
    }
    for book in [BookIntegrity::Crossed, BookIntegrity::Unknown] {
        assert_ineligible(
            base_input()?.with_book_integrity(book),
            EligibilityFailure::BookIntegrity,
        )?;
    }
    Ok(())
}

#[test]
fn every_nonhealthy_stream_state_and_incomplete_capture_fail_closed() -> Result<(), Box<dyn Error>>
{
    for state in [
        StreamIntegrityState::Initializing,
        StreamIntegrityState::Synchronizing,
        StreamIntegrityState::Validating,
        StreamIntegrityState::Stale,
        StreamIntegrityState::GapDetected,
        StreamIntegrityState::ChecksumFailed,
        StreamIntegrityState::Divergent,
        StreamIntegrityState::Quarantined,
    ] {
        assert_ineligible(
            base_input()?.with_stream_integrity(state),
            EligibilityFailure::StreamIntegrity,
        )?;
    }
    assert_ineligible(
        base_input()?.with_capture_integrity(CaptureIntegrityState::Incomplete),
        EligibilityFailure::CaptureIntegrity,
    )?;
    let disabled = QualificationEvidence::try_from(
        base_input()?.with_capture_integrity(CaptureIntegrityState::Disabled),
    )?;
    assert_eq!(
        disabled.execution_eligibility(),
        ExecutionEligibility::Eligible
    );
    Ok(())
}
