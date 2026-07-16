mod support;

use std::error::Error;

use market_squawk_domain::QualificationAssessment;
use serde::Deserialize;
use support::live::valid_assessment_input;

fn assert_deserializable<T>()
where
    T: for<'de> Deserialize<'de>,
{
}

#[test]
fn qualification_assessment_has_a_durable_checked_wire_contract() -> Result<(), Box<dyn Error>> {
    assert_deserializable::<QualificationAssessment>();
    let assessment = QualificationAssessment::try_from(valid_assessment_input()?)?;
    let restored: QualificationAssessment =
        serde_json::from_str(&serde_json::to_string(&assessment)?)?;
    assert_eq!(restored, assessment);
    Ok(())
}

#[test]
fn qualification_wire_denies_unknown_fields() -> Result<(), Box<dyn Error>> {
    let assessment = QualificationAssessment::try_from(valid_assessment_input()?)?;
    let mut value = serde_json::to_value(assessment)?;
    value["unexpected"] = serde_json::json!(true);
    assert!(serde_json::from_value::<QualificationAssessment>(value).is_err());
    Ok(())
}

#[test]
fn qualification_wire_rejects_every_tampered_derived_field() -> Result<(), Box<dyn Error>> {
    let assessment = QualificationAssessment::try_from(valid_assessment_input()?)?;
    let value = serde_json::to_value(assessment)?;
    for (field, replacement) in [
        ("recorded_quality", serde_json::json!("stale")),
        ("failures", serde_json::json!(1)),
        ("evaluated_at", serde_json::json!(1_011)),
        ("valid_until", serde_json::json!(1_019)),
    ] {
        let mut tampered = value.clone();
        tampered[field] = replacement;
        assert!(
            serde_json::from_value::<QualificationAssessment>(tampered).is_err(),
            "tampered {field} must be rejected"
        );
    }
    Ok(())
}

#[test]
fn qualification_wire_revalidates_nested_bindings_and_derived_evidence()
-> Result<(), Box<dyn Error>> {
    let assessment = QualificationAssessment::try_from(valid_assessment_input()?)?;
    let value = serde_json::to_value(assessment)?;

    let mut transplanted = value.clone();
    transplanted["source_policy"]["binding"]["source_identifier"] =
        serde_json::json!("different-update");
    assert!(serde_json::from_value::<QualificationAssessment>(transplanted).is_err());

    let mut forged_sequence = value.clone();
    forged_sequence["integrity"]["sequence"]["result"]["integrity"] = serde_json::json!("invalid");
    assert!(serde_json::from_value::<QualificationAssessment>(forged_sequence).is_err());

    let mut forged_timing = value;
    forged_timing["integrity"]["timing"]["result"]["freshness"] = serde_json::json!("stale");
    assert!(serde_json::from_value::<QualificationAssessment>(forged_timing).is_err());
    Ok(())
}
