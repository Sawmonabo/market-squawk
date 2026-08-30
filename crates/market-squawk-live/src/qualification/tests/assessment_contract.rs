use market_squawk_domain::{
    AssessmentStatus, CoverageStatus, DataQuality, EligibilityFailure, PayloadHash,
    PayloadReference, TradingStatus,
};

use super::{
    FixturePolicy, TestResult, canonical_digest, current_fixture, provenance, qualify,
    unsupported_evidence,
};

#[test]
fn canonical_state_digest_declares_v2_encoding_rule() -> TestResult {
    let digest = canonical_digest(b"current-v2-canonical-state")?;
    let rule = digest.canonicalization_rule();

    assert_eq!(rule.rule().as_str(), "market-squawk-live-state-v2");
    assert_eq!(rule.version().get(), 2);
    Ok(())
}

#[test]
fn trading_status_and_coinbase_quality_ceiling_cannot_be_promoted() -> TestResult {
    let direct = current_fixture(FixturePolicy::default(), 1)?;
    let halted = qualify(
        &direct.observations[0],
        unsupported_evidence(1, TradingStatus::Halted)?,
    )?;
    assert_eq!(
        halted.assessment.recorded_quality(),
        DataQuality::DirectUnverified
    );
    assert!(
        halted
            .assessment
            .has_failure(EligibilityFailure::TradingStatus)
    );

    let coinbase = current_fixture(
        FixturePolicy {
            quality: DataQuality::DirectUnverified,
            ..FixturePolicy::default()
        },
        1,
    )?;
    let qualified = qualify(
        &coinbase.observations[0],
        unsupported_evidence(1, TradingStatus::Active)?,
    )?;
    assert_eq!(
        qualified.assessment.recorded_quality(),
        DataQuality::DirectUnverified
    );
    assert!(
        qualified
            .assessment
            .has_failure(EligibilityFailure::QualityCeiling)
    );
    assert_eq!(
        qualified
            .assessment
            .assessment_status_at(qualified.valid_until),
        AssessmentStatus::Rejected
    );
    Ok(())
}

#[test]
fn assessment_provenance_retains_binding_payload_and_assessment_reference() -> TestResult {
    let fixture = current_fixture(FixturePolicy::default(), 1)?;
    let qualified = qualify(
        &fixture.observations[0],
        unsupported_evidence(1, TradingStatus::Active)?,
    )?;
    let provenance = provenance(&qualified.event)?;
    let frame = fixture.observations[0]
        .evidence()
        .transport_frame()
        .expect("fixture transport frame");

    assert_eq!(provenance.binding(), qualified.assessment.binding());
    assert_eq!(
        provenance.assessment_reference(),
        Some(qualified.assessment.assessment_id().as_source_identifier())
    );
    assert_eq!(
        provenance.payload_reference(),
        &PayloadReference::ContentHash(PayloadHash::new(
            frame.payload_digest().algorithm(),
            frame.payload_digest().bytes(),
        ))
    );
    assert_eq!(provenance.received_at(), frame.received_at());
    assert_eq!(
        provenance.available_at(),
        qualified.assessment.evaluated_at()
    );
    assert_eq!(
        provenance.ingested_at(),
        qualified.assessment.evaluated_at()
    );
    assert_eq!(
        provenance.recorded_quality(),
        qualified.assessment.recorded_quality()
    );
    assert_eq!(provenance.recorded_coverage(), CoverageStatus::Sufficient);
    Ok(())
}

#[test]
fn serialized_assessment_rejects_binding_dimension_mutation_and_transplant() -> TestResult {
    let fixture = current_fixture(FixturePolicy::default(), 1)?;
    let qualified = qualify(
        &fixture.observations[0],
        unsupported_evidence(1, TradingStatus::Active)?,
    )?;
    let original = serde_json::to_value(&qualified.assessment)?;
    let mutations = [
        ("/binding/source_id", serde_json::json!("other-source")),
        ("/binding/session_id", serde_json::json!("other-session")),
        (
            "/binding/metadata_revision",
            serde_json::json!("revision-2"),
        ),
        (
            "/binding/authorization_basis",
            serde_json::json!("other-terms"),
        ),
        ("/binding/venue_id", serde_json::json!("kraken")),
        (
            "/binding/instrument_id",
            serde_json::json!("5c74ab95-53b9-42ad-9b66-0ed403b88fed"),
        ),
        (
            "/binding/provider_product",
            serde_json::json!("other-product"),
        ),
        (
            "/binding/provider_channel",
            serde_json::json!("other-channel"),
        ),
        ("/binding/event_class", serde_json::json!("quote")),
        (
            "/binding/source_identifier",
            serde_json::json!("other-provider-object"),
        ),
        ("/binding/connection_generation", serde_json::json!(2)),
        (
            "/binding/payload_digest/algorithm",
            serde_json::json!("blake3"),
        ),
        (
            "/binding/canonical_state_digest/canonicalization_rule/version",
            serde_json::json!(3),
        ),
    ];
    for (path, replacement) in mutations {
        let mut transplanted = original.clone();
        let target = transplanted
            .pointer_mut(path)
            .ok_or("assessment binding path is missing")?;
        *target = replacement;
        assert!(
            serde_json::from_value::<market_squawk_domain::QualificationAssessment>(transplanted)
                .is_err(),
            "mutation at {path} must fail closed"
        );
    }
    Ok(())
}
