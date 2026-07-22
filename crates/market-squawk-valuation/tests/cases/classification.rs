use market_squawk_domain::{DataQuality, FairValueHierarchy};
use market_squawk_valuation::{
    ClassificationRuleset, DecisionReasonCode, EvidenceVerification, InputInstrumentRelation,
    InputObservability, InputSignificance, MarketAccess, MarketActivity, Predicate,
    PriceAdjustment,
};

use super::{Scenario, input, measurement, service};

#[test]
fn level_one_requires_the_complete_truth_table() -> Result<(), Box<dyn std::error::Error>> {
    let ruleset = ClassificationRuleset::current(100)?;
    let mut service = service(16);
    let decision = service.classify(
        measurement(vec![input(
            Scenario::default(),
            1,
            InputSignificance::Significant,
        )]),
        ruleset.clone(),
    )?;

    assert_eq!(decision.hierarchy(), FairValueHierarchy::Level1);
    assert_eq!(decision.ruleset_version(), ruleset.version());
    assert_eq!(decision.ruleset_hash(), ruleset.hash());
    assert_eq!(decision.truth_table().len(), Predicate::ALL.len());
    assert!(decision.truth_table().iter().all(|result| result.passed()));

    let cases = [
        (
            "similar instrument",
            Scenario {
                relation: InputInstrumentRelation::Similar,
                ..Scenario::default()
            },
            FairValueHierarchy::Level2,
            DecisionReasonCode::NotIdenticalInstrument,
        ),
        (
            "inactive market",
            Scenario {
                activity: MarketActivity::Inactive,
                ..Scenario::default()
            },
            FairValueHierarchy::Level2,
            DecisionReasonCode::MarketNotActive,
        ),
        (
            "inaccessible market",
            Scenario {
                access: MarketAccess::Inaccessible,
                ..Scenario::default()
            },
            FairValueHierarchy::Level2,
            DecisionReasonCode::MarketNotAccessible,
        ),
        (
            "observable adjustment",
            Scenario {
                adjustment: PriceAdjustment::Observable,
                ..Scenario::default()
            },
            FairValueHierarchy::Level2,
            DecisionReasonCode::AdjustedPrice,
        ),
        (
            "official delay",
            Scenario {
                quality: DataQuality::OfficialDelayed,
                ..Scenario::default()
            },
            FairValueHierarchy::Level2,
            DecisionReasonCode::QualityNotLevel1,
        ),
        (
            "modeled observable input",
            Scenario {
                quality: DataQuality::Modeled,
                observability: InputObservability::Observable,
                ..Scenario::default()
            },
            FairValueHierarchy::Level2,
            DecisionReasonCode::NotQuotedPrice,
        ),
        (
            "unobservable estimate",
            Scenario {
                quality: DataQuality::Estimated,
                observability: InputObservability::Unobservable,
                ..Scenario::default()
            },
            FairValueHierarchy::Level3,
            DecisionReasonCode::UnobservableSignificantInput,
        ),
        (
            "post measurement quote",
            Scenario {
                source_timestamp: 1_001,
                ..Scenario::default()
            },
            FairValueHierarchy::Unclassified,
            DecisionReasonCode::PostMeasurementEvidence,
        ),
        (
            "stale quote",
            Scenario {
                source_timestamp: 800,
                ..Scenario::default()
            },
            FairValueHierarchy::Unclassified,
            DecisionReasonCode::EvidenceTooOld,
        ),
        (
            "quarantined quote",
            Scenario {
                quality: DataQuality::Quarantined,
                ..Scenario::default()
            },
            FairValueHierarchy::Unclassified,
            DecisionReasonCode::EvidenceQuarantined,
        ),
        (
            "unverified evidence",
            Scenario {
                verification: EvidenceVerification::Unverified,
                ..Scenario::default()
            },
            FairValueHierarchy::Unclassified,
            DecisionReasonCode::SourceEvidenceUnverified,
        ),
        (
            "currency mismatch",
            Scenario {
                input_currency: "EUR",
                ..Scenario::default()
            },
            FairValueHierarchy::Unclassified,
            DecisionReasonCode::CurrencyMismatch,
        ),
        (
            "scale mismatch",
            Scenario {
                input_scale: 4,
                ..Scenario::default()
            },
            FairValueHierarchy::Unclassified,
            DecisionReasonCode::ScaleMismatch,
        ),
    ];

    for (name, scenario, expected, reason) in cases {
        let decision = service.classify(
            measurement(vec![input(
                scenario,
                name.as_bytes()[0],
                InputSignificance::Significant,
            )]),
            ruleset.clone(),
        )?;
        assert_eq!(decision.hierarchy(), expected, "{name}");
        assert!(
            decision
                .reasons()
                .iter()
                .any(|value| value.code() == reason),
            "{name} must retain {reason:?}"
        );
    }
    Ok(())
}

#[test]
fn lowest_significant_input_controls_without_quality_or_depth_promotion()
-> Result<(), Box<dyn std::error::Error>> {
    let ruleset = ClassificationRuleset::current(100)?;
    let mut service = service(8);
    let unobservable = Scenario {
        quality: DataQuality::DirectVerified,
        observability: InputObservability::Unobservable,
        adjustment: PriceAdjustment::Unobservable,
        ..Scenario::default()
    };
    let significant = service.classify(
        measurement(vec![
            input(Scenario::default(), 31, InputSignificance::Significant),
            input(unobservable, 32, InputSignificance::Significant),
        ]),
        ruleset.clone(),
    )?;
    assert_eq!(significant.hierarchy(), FairValueHierarchy::Level3);

    let immaterial = service.classify(
        measurement(vec![
            input(Scenario::default(), 41, InputSignificance::Significant),
            input(unobservable, 42, InputSignificance::NotSignificant),
        ]),
        ruleset,
    )?;
    assert_eq!(immaterial.hierarchy(), FairValueHierarchy::Level1);
    Ok(())
}
