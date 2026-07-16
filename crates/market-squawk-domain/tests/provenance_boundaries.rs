mod support;

use std::error::Error;

use market_squawk_domain::{
    AvailabilityEvidence, CoverageStatus, DataQuality, DecodedLiveProvenanceInput,
    ExecutionEligibility, LiveProvenance, PayloadHash, PayloadHashAlgorithm, PayloadReference,
    ProvenanceError, ResearchContext, ResearchProvenance, ResearchTime, RevisionNumber, SourceId,
    SourceIdentifier, Timestamp,
};
use support::live::{BindingSpec, binding};

#[test]
fn decoded_provenance_rejects_direct_verified_and_remains_archive_ineligible()
-> Result<(), Box<dyn Error>> {
    let binding = binding(&BindingSpec::default())?;
    let direct = LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        binding.clone(),
        Some(Timestamp::from_unix_nanos(995)),
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_010),
        DataQuality::DirectVerified,
        CoverageStatus::Sufficient,
        PayloadReference::SourceReference(SourceIdentifier::try_from("capture:7:42")?),
    ));
    assert_eq!(direct, Err(ProvenanceError::UnqualifiedDirectVerified));

    let decoded = LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        binding,
        Some(Timestamp::from_unix_nanos(995)),
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_010),
        DataQuality::DirectUnverified,
        CoverageStatus::Sufficient,
        PayloadReference::SourceReference(SourceIdentifier::try_from("capture:7:42")?),
    ))?;
    assert_eq!(
        decoded.execution_eligibility(),
        ExecutionEligibility::Ineligible
    );
    assert!(decoded.requires_requalification());
    Ok(())
}

#[test]
fn content_hash_must_match_complete_binding() -> Result<(), Box<dyn Error>> {
    let result = LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        binding(&BindingSpec::default())?,
        None,
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_010),
        DataQuality::DirectUnverified,
        CoverageStatus::Sufficient,
        PayloadReference::ContentHash(PayloadHash::new(PayloadHashAlgorithm::Sha256, [9; 32])),
    ));
    assert_eq!(result, Err(ProvenanceError::PayloadDigestMismatch));
    Ok(())
}

#[test]
fn research_unknown_availability_remains_unknown_and_conservative() -> Result<(), Box<dyn Error>> {
    let provenance = ResearchProvenance::new(
        SourceId::try_from("historical-file")?,
        None,
        None,
        SourceIdentifier::try_from("row-42")?,
        None,
        Timestamp::from_unix_nanos(500),
        Timestamp::from_unix_nanos(600),
        DataQuality::Aggregated,
        PayloadReference::SourceReference(SourceIdentifier::try_from("fixture:42")?),
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
fn inferred_availability_is_not_point_in_time_evidence() -> Result<(), Box<dyn Error>> {
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
fn unknown_schema_fields_are_rejected() -> Result<(), Box<dyn Error>> {
    let decoded = LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        binding(&BindingSpec::default())?,
        None,
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_010),
        DataQuality::DirectUnverified,
        CoverageStatus::Unknown,
        PayloadReference::SourceReference(SourceIdentifier::try_from("capture:7:42")?),
    ))?;
    let mut value = serde_json::to_value(decoded)?;
    value["unexpected"] = serde_json::json!(true);
    assert!(serde_json::from_value::<LiveProvenance>(value).is_err());
    Ok(())
}
