use crate::support;

use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    BindingError, BookStateBinding, BoundAssessment, CanonicalStateDigest, CanonicalizationRule,
    ChecksumCapability, ChecksumEvidence, ChecksumIntegrity, ChecksumScope, ChecksumValue,
    CoverageConsolidation, CoverageDelay, CoverageDimension, CoverageError, CoverageScope,
    CoverageStatus, DataQuality, EvidenceDigest, IntegrityEvidenceError, LiveEventClass,
    LiveEvidenceBinding, MarketDepth, PayloadHashAlgorithm, QualificationAssessment, RuleVersion,
    SequenceCapability, SequenceEvidence, SequenceIntegrity, SequenceNumber,
    SequenceValidationRule, SourceCoverageRecord, SourceIdentifier, Timestamp,
};
use support::live::{BindingSpec, binding, rule, valid_assessment_input};

#[test]
fn live_qualification_is_derived_without_fair_value_classification() -> Result<(), Box<dyn Error>> {
    let assessment = QualificationAssessment::try_from(valid_assessment_input()?)?;

    assert_eq!(assessment.recorded_quality(), DataQuality::DirectVerified);
    assert_eq!(
        assessment.assessment_status_at(Timestamp::from_unix_nanos(1_020)),
        market_squawk_domain::AssessmentStatus::Satisfied
    );
    Ok(())
}

#[test]
fn sequence_and_checksum_results_are_derived_from_retained_operands() -> Result<(), Box<dyn Error>>
{
    let generation = market_squawk_domain::ConnectionGeneration::new(7)?;
    let sequence = SequenceEvidence::validate(
        SequenceCapability::Provided,
        Some(rule("provider.sequence.consecutive")?),
        SequenceValidationRule::Consecutive,
        generation,
        Some(SequenceNumber::new(40)),
        Some(SequenceNumber::new(41)),
        Some(SequenceNumber::new(42)),
    )?;
    assert_eq!(sequence.integrity(), SequenceIntegrity::Valid);
    assert_eq!(sequence.previous_sequence(), Some(SequenceNumber::new(41)));

    let checksum = ChecksumEvidence::validate_book(
        ChecksumCapability::Provided,
        Some(rule("provider.checksum.crc32")?),
        generation,
        Some(ChecksumScope::new(
            MarketDepth::PriceLevel,
            10,
            SourceIdentifier::try_from("top-ten-bid-ask")?,
        )?),
        Some(ChecksumValue::new(10)),
        Some(ChecksumValue::new(11)),
    )?;
    assert_eq!(checksum.integrity(), ChecksumIntegrity::Failed);
    assert_eq!(checksum.expected(), Some(ChecksumValue::new(10)));
    assert_eq!(checksum.computed(), Some(ChecksumValue::new(11)));
    Ok(())
}

#[test]
fn unsupported_capability_cannot_be_paired_with_supplied_evidence() -> Result<(), Box<dyn Error>> {
    let generation = market_squawk_domain::ConnectionGeneration::new(7)?;
    assert!(matches!(
        SequenceEvidence::validate(
            SequenceCapability::Unsupported,
            Some(rule("provider.sequence")?),
            SequenceValidationRule::Consecutive,
            generation,
            None,
            Some(SequenceNumber::new(1)),
            Some(SequenceNumber::new(2)),
        ),
        Err(IntegrityEvidenceError::CapabilityContradiction { .. })
    ));
    assert!(matches!(
        ChecksumEvidence::validate_book(
            ChecksumCapability::Unsupported,
            Some(rule("provider.checksum")?),
            generation,
            None,
            None,
            None,
        ),
        Err(IntegrityEvidenceError::CapabilityContradiction { .. })
    ));
    Ok(())
}

#[test]
fn assessment_window_rejects_reversed_interval_and_checks_exact_boundary()
-> Result<(), Box<dyn Error>> {
    let binding = binding(&BindingSpec::default())?;
    assert!(matches!(
        BoundAssessment::new(
            binding.clone(),
            Timestamp::from_unix_nanos(11),
            Timestamp::from_unix_nanos(10),
            DataQuality::DirectUnverified,
        ),
        Err(BindingError::ValidityBeforeEvaluation)
    ));
    let window = BoundAssessment::new(
        binding,
        Timestamp::from_unix_nanos(10),
        Timestamp::from_unix_nanos(20),
        DataQuality::DirectUnverified,
    )?;
    assert!(window.is_valid_at(Timestamp::from_unix_nanos(20)));
    assert!(!window.is_valid_at(Timestamp::from_unix_nanos(21)));
    Ok(())
}

#[test]
fn book_binding_requires_exact_state_identity() -> Result<(), Box<dyn Error>> {
    let spec = BindingSpec::default();
    assert!(matches!(
        LiveEvidenceBinding::new(
            market_squawk_domain::SourceId::try_from(spec.source)?,
            SourceIdentifier::try_from(spec.session)?,
            market_squawk_domain::MetadataRevision::new(SourceIdentifier::try_from(
                spec.metadata_revision
            )?),
            market_squawk_domain::AuthorizationBasis::new(SourceIdentifier::try_from(
                spec.authorization_basis
            )?),
            market_squawk_domain::VenueId::try_from(spec.venue)?,
            market_squawk_domain::InstrumentId::from_str(spec.instrument)?,
            market_squawk_domain::ConnectionGeneration::new(spec.generation)?,
            market_squawk_domain::ProviderProduct::new(SourceIdentifier::try_from(spec.product)?),
            market_squawk_domain::ProviderChannel::new(SourceIdentifier::try_from(spec.channel)?),
            LiveEventClass::BookDelta,
            SourceIdentifier::try_from(spec.source_identifier)?,
            EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [1; 32]),
            CanonicalStateDigest::new(
                EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [2; 32]),
                CanonicalizationRule::new(
                    SourceIdentifier::try_from("market-squawk.book.price-level-v1")?,
                    RuleVersion::new(1)?,
                ),
            ),
            None,
        ),
        Err(BindingError::MissingBookState)
    ));
    Ok(())
}

#[test]
fn coverage_scope_is_independently_checked_against_binding() -> Result<(), Box<dyn Error>> {
    let binding = binding(&BindingSpec::default())?;
    let wrong_venue = CoverageScope::new(
        binding.source_id().clone(),
        market_squawk_domain::VenueId::try_from("KRAKEN")?,
        binding.provider_product().clone(),
        binding.provider_channel().clone(),
        binding.event_class(),
        binding.book_state().map(BookStateBinding::depth),
        CoverageDelay::RealTime,
        CoverageConsolidation::SingleVenue,
        Timestamp::from_unix_nanos(900),
        None,
        binding.metadata_revision().clone(),
    )?;
    assert_eq!(
        SourceCoverageRecord::new(binding.clone(), wrong_venue, CoverageStatus::Sufficient),
        Err(CoverageError::BindingMismatch(CoverageDimension::Venue))
    );

    let delayed = CoverageScope::new(
        binding.source_id().clone(),
        binding.venue_id().clone(),
        binding.provider_product().clone(),
        binding.provider_channel().clone(),
        binding.event_class(),
        binding.book_state().map(BookStateBinding::depth),
        CoverageDelay::Delayed(1),
        CoverageConsolidation::SingleVenue,
        Timestamp::from_unix_nanos(900),
        None,
        binding.metadata_revision().clone(),
    )?;
    assert_eq!(
        SourceCoverageRecord::new(binding, delayed, CoverageStatus::Sufficient),
        Err(CoverageError::ContradictorySufficientStatus)
    );
    Ok(())
}

#[test]
fn deserialization_replays_binding_window_and_coverage_constructors() -> Result<(), Box<dyn Error>>
{
    let binding = binding(&BindingSpec::default())?;
    let mut binding_wire = serde_json::to_value(&binding)?;
    binding_wire["book_state"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<LiveEvidenceBinding>(binding_wire).is_err());

    let window = BoundAssessment::new(
        binding.clone(),
        Timestamp::from_unix_nanos(10),
        Timestamp::from_unix_nanos(20),
        DataQuality::DirectUnverified,
    )?;
    let mut window_wire = serde_json::to_value(window)?;
    window_wire["valid_until"] = serde_json::json!(9);
    assert!(serde_json::from_value::<BoundAssessment<DataQuality>>(window_wire).is_err());

    let coverage = support::live::coverage_record(binding)?;
    let mut coverage_wire = serde_json::to_value(coverage)?;
    coverage_wire["scope"]["effective_until"] = serde_json::json!(899);
    assert!(serde_json::from_value::<SourceCoverageRecord>(coverage_wire).is_err());
    Ok(())
}
