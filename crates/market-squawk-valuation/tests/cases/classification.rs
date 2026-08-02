use market_squawk_domain::FairValueHierarchy;
use market_squawk_valuation::{ClassificationRuleset, DecisionReasonCode};

use super::{CatalogFixture, TestResult, measurement};

#[test]
fn producer_time_semantics_fail_closed_on_stale_or_post_measurement_inputs() -> TestResult {
    let fixture = CatalogFixture::open()?;
    let mut service = fixture.service(8)?;
    let rules = ClassificationRuleset::current(100)?;

    let available_before_measurement = service.classify(measurement(900, 1)?, rules.clone())?;
    assert_eq!(
        available_before_measurement.hierarchy(),
        FairValueHierarchy::Level2
    );
    assert!(
        available_before_measurement
            .reasons()
            .iter()
            .any(|reason| reason.code() == DecisionReasonCode::QualityNotLevel1)
    );

    let stale_but_available = service.classify(measurement(800, 2)?, rules.clone())?;
    assert_eq!(
        stale_but_available.hierarchy(),
        FairValueHierarchy::Unclassified
    );
    assert!(
        stale_but_available
            .reasons()
            .iter()
            .any(|reason| reason.code() == DecisionReasonCode::EvidenceTooOld)
    );

    let available_after_measurement = service.classify(measurement(1_001, 3)?, rules)?;
    assert_eq!(
        available_after_measurement.hierarchy(),
        FairValueHierarchy::Unclassified
    );
    assert!(
        available_after_measurement
            .reasons()
            .iter()
            .any(|reason| reason.code() == DecisionReasonCode::PostMeasurementEvidence)
    );
    Ok(())
}
