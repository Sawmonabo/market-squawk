mod support;

use std::error::Error;

use market_squawk_domain::{
    AssessmentStatus, BindingError, BoundAssessment, CoverageConsolidation, CoverageDelay,
    CoverageScope, CoverageStatus, QualificationAssessment, SourceCoverageRecord, Timestamp,
};
use support::live::{BindingSpec, assessment_input, binding};

#[test]
fn coverage_effective_until_is_a_hard_bound_assessment_deadline() -> Result<(), Box<dyn Error>> {
    let binding = binding(&BindingSpec::default())?;
    let effective_until = Timestamp::from_unix_nanos(1_020);
    let scope = CoverageScope::new(
        binding.source_id().clone(),
        binding.venue_id().clone(),
        binding.provider_product().clone(),
        binding.provider_channel().clone(),
        binding.event_class(),
        binding.book_state().map(|state| state.depth()),
        CoverageDelay::RealTime,
        CoverageConsolidation::SingleVenue,
        Timestamp::from_unix_nanos(900),
        Some(effective_until),
        binding.metadata_revision().clone(),
    )?;
    let coverage = SourceCoverageRecord::new(binding.clone(), scope, CoverageStatus::Sufficient)?;

    assert!(
        BoundAssessment::new(
            binding.clone(),
            effective_until,
            effective_until,
            coverage.clone()
        )
        .is_ok()
    );
    assert_eq!(
        BoundAssessment::new(
            binding,
            effective_until,
            Timestamp::from_unix_nanos(1_021),
            coverage,
        ),
        Err(BindingError::ValidityExceedsEvidence)
    );
    Ok(())
}

#[test]
fn qualification_validity_is_inclusive_and_rejects_one_nanosecond_later()
-> Result<(), Box<dyn Error>> {
    let binding = binding(&BindingSpec::default())?;
    let effective_until = Timestamp::from_unix_nanos(1_020);
    let assessment = QualificationAssessment::try_from(assessment_input(
        binding.clone(),
        None,
        binding,
        effective_until,
    )?)?;
    assert_eq!(assessment.valid_until(), effective_until);
    assert_eq!(
        assessment.assessment_status_at(effective_until),
        AssessmentStatus::Satisfied
    );
    assert_eq!(
        assessment.assessment_status_at(Timestamp::from_unix_nanos(1_021)),
        AssessmentStatus::Rejected
    );
    Ok(())
}
