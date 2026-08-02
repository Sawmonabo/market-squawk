use super::*;
use crate::DecisionReasonCode;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn legacy_v1_unassessed_analytics_recovers_frozen_ids_but_v2_is_unclassified() -> TestResult {
    let instrument_id = "9f3914d3-9ef4-42f7-a707-3f2dcde861d1";
    let manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("legacy-fair-value-analytics")?,
        1,
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([7; 32]),
    )?;
    let evidence = FairValueEvidence::try_from_parts(FairValueEvidenceParts {
        source_id: SourceId::try_from("market-squawk.analytics")?,
        source_identifier: SourceIdentifier::try_from("legacy-feature-row")?,
        payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [9; 32]),
        origin: EvidenceOrigin::Analytics {
            feature_key: FeatureKey::try_new("legacy_fair_value", NonZeroU32::MIN)?,
            semantic_digest: [4; 32],
            manifest,
            object_graph_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]),
            query_identity: EvidenceDigest::new(DigestAlgorithm::Sha256, [2; 32]),
            result_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [3; 32]),
            row: 0,
            revision: 1,
        },
        source_timestamp: Some(Timestamp::from_unix_nanos(950)),
        effective_at: Some(Timestamp::from_unix_nanos(950)),
        published_at: None,
        available_at: Some(Timestamp::from_unix_nanos(950)),
        received_at: None,
        qualification_evaluated_at: None,
        qualification_valid_until: None,
        ingested_at: Timestamp::from_unix_nanos(950),
        verification: EvidenceVerification::Verified,
    })?;
    let evidence_id = evidence.hash();
    let payload: InputPayload = canonical(&serde_json::to_vec(&InputPayload {
        version: 1,
        subject_instrument_id: instrument_id.to_owned(),
        reference_instrument_id: instrument_id.to_owned(),
        relationship: 1,
        amount: AmountPayload {
            mantissa: "10000".to_owned(),
            decimal_scale: 2,
            currency: "USD".to_owned(),
            accounting_scale: 2,
        },
        significance: 1,
        observability: 2,
        adjustment: 1,
        market_activity: 3,
        market_access: 3,
        data_quality: 6,
        evidence_id: evidence_id.bytes(),
        use_assessment: None,
        market_access_id: None,
    })?)?;
    let input = input_from_payload(
        payload,
        &BTreeMap::from([(evidence_id, evidence)]),
        &BTreeMap::new(),
    )?;
    assert!(input.use_assessment().is_none());

    let input_id = input.id();
    let measurement = measurement_from_payload(
        MeasurementPayload {
            version: 1,
            account_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_owned(),
            instrument_id: instrument_id.to_owned(),
            amount: AmountPayload {
                mantissa: "10000".to_owned(),
                decimal_scale: 2,
                currency: "USD".to_owned(),
                accounting_scale: 2,
            },
            measurement_at_ns: 1_000,
            prepared_at_ns: 1_100,
            prepared_by: "legacy-preparer".to_owned(),
            method: 2,
            input_ids: vec![input_id.bytes()],
        },
        &BTreeMap::from([(input_id, input)]),
    )?;
    let legacy_decision = ClassificationRuleset::versioned(1, 100)?.classify(&measurement)?;
    assert_eq!(legacy_decision.hierarchy(), FairValueHierarchy::Level2);
    assert_eq!(
        (
            input_id.bytes(),
            measurement.id().bytes(),
            legacy_decision.id().bytes(),
        ),
        (
            [
                167, 129, 110, 5, 131, 15, 12, 6, 57, 180, 32, 106, 11, 112, 213, 225, 15, 75, 104,
                128, 244, 42, 92, 180, 126, 168, 105, 101, 199, 234, 38, 187,
            ],
            [
                127, 101, 51, 235, 4, 64, 50, 13, 67, 220, 0, 229, 35, 96, 252, 60, 124, 3, 167, 0,
                214, 65, 11, 255, 37, 32, 242, 37, 110, 212, 68, 240,
            ],
            [
                158, 221, 131, 103, 83, 166, 71, 229, 133, 149, 176, 11, 205, 36, 231, 177, 223,
                214, 91, 123, 113, 11, 170, 43, 53, 112, 85, 85, 132, 174, 140, 7,
            ],
        )
    );
    let current_decision = ClassificationRuleset::current(100)?.classify(&measurement)?;
    assert_eq!(
        current_decision.hierarchy(),
        FairValueHierarchy::Unclassified
    );
    assert!(
        current_decision
            .reasons()
            .iter()
            .any(|reason| { reason.code() == DecisionReasonCode::InputUseAssessmentMissing })
    );
    Ok(())
}
