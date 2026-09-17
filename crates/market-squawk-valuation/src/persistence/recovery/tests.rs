use super::*;
use market_squawk_domain::AccountId;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn current_amount_basis_is_required_and_round_trips_with_exact_identities() -> TestResult {
    let instrument_id: InstrumentId = "9f3914d3-9ef4-42f7-a707-3f2dcde861d1".parse()?;
    let account_id: AccountId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse()?;
    let currency = Currency::try_from("USD")?;
    let manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("fair-value-basis-recovery")?,
        1,
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([7; 32]),
    )?;
    let evidence = FairValueEvidence::try_from_parts(FairValueEvidenceParts {
        source_id: SourceId::try_from("fair-value-basis-recovery")?,
        source_identifier: SourceIdentifier::try_from("entity-total-row")?,
        payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [9; 32]),
        origin: EvidenceOrigin::Research {
            manifest: manifest.clone(),
            object_graph_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]),
            query_identity: EvidenceDigest::new(DigestAlgorithm::Sha256, [2; 32]),
            result_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [3; 32]),
            row: 0,
            revision: 1,
        },
        source_timestamp: Some(Timestamp::from_unix_nanos(900)),
        effective_at: Some(Timestamp::from_unix_nanos(900)),
        published_at: Some(Timestamp::from_unix_nanos(925)),
        available_at: Some(Timestamp::from_unix_nanos(950)),
        received_at: Some(Timestamp::from_unix_nanos(950)),
        qualification_evaluated_at: None,
        qualification_valid_until: None,
        ingested_at: Timestamp::from_unix_nanos(975),
        verification: EvidenceVerification::Verified,
    })?;
    let input_amount = ValuationAmount::try_new(
        Money::new(Decimal::new(10_000, 2), currency),
        2,
        ValuationAmountBasis::ReportingEntityTotal,
    )?;
    let input = ValuationInput::try_from_spec(ValuationInputSpec {
        subject_instrument_id: instrument_id,
        reference_instrument_id: instrument_id,
        relationship: InputInstrumentRelation::Identical,
        amount: input_amount,
        significance: InputSignificance::Significant,
        observability: InputObservability::Observable,
        adjustment: PriceAdjustment::None,
        market_activity: MarketActivity::NotAssessed,
        market_access: MarketAccess::NotAssessed,
        market_access_assessment: None,
        data_quality: DataQuality::OfficialDelayed,
        evidence: evidence.clone(),
        use_assessment: None,
    })?;
    let recovered_input = input_from_payload(
        canonical(&serde_json::to_vec(&InputPayload {
            version: PAYLOAD_VERSION,
            subject_instrument_id: instrument_id.to_string(),
            reference_instrument_id: instrument_id.to_string(),
            relationship: 1,
            amount: amount_payload(input_amount),
            significance: 1,
            observability: 2,
            adjustment: 1,
            market_activity: 3,
            market_access: 3,
            data_quality: 3,
            evidence_id: evidence.hash().bytes(),
            use_assessment: None,
            market_access_id: None,
        })?)?,
        &BTreeMap::from([(evidence.hash(), evidence)]),
        &BTreeMap::new(),
    )?;
    assert_eq!(recovered_input.id(), input.id());
    assert_eq!(
        recovered_input.amount().basis(),
        ValuationAmountBasis::ReportingEntityTotal
    );

    let analytics_evidence = FairValueEvidence::try_from_parts(FairValueEvidenceParts {
        source_id: SourceId::try_from("market-squawk.analytics")?,
        source_identifier: SourceIdentifier::try_from("unassessed-analytics-row")?,
        payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [19; 32]),
        origin: EvidenceOrigin::Analytics {
            feature_key: FeatureKey::try_new("unassessed_entity_total", NonZeroU32::MIN)?,
            semantic_digest: [14; 32],
            manifest,
            object_graph_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [11; 32]),
            query_identity: EvidenceDigest::new(DigestAlgorithm::Sha256, [12; 32]),
            result_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [13; 32]),
            row: 0,
            revision: 1,
        },
        source_timestamp: Some(Timestamp::from_unix_nanos(900)),
        effective_at: Some(Timestamp::from_unix_nanos(900)),
        published_at: Some(Timestamp::from_unix_nanos(925)),
        available_at: Some(Timestamp::from_unix_nanos(950)),
        received_at: Some(Timestamp::from_unix_nanos(950)),
        qualification_evaluated_at: None,
        qualification_valid_until: None,
        ingested_at: Timestamp::from_unix_nanos(975),
        verification: EvidenceVerification::Verified,
    })?;
    let analytics_evidence_id = analytics_evidence.hash();
    assert_eq!(
        input_from_payload(
            InputPayload {
                version: PAYLOAD_VERSION,
                subject_instrument_id: instrument_id.to_string(),
                reference_instrument_id: instrument_id.to_string(),
                relationship: 1,
                amount: amount_payload(input_amount),
                significance: 1,
                observability: 2,
                adjustment: 1,
                market_activity: 3,
                market_access: 3,
                data_quality: 6,
                evidence_id: analytics_evidence_id.bytes(),
                use_assessment: None,
                market_access_id: None,
            },
            &BTreeMap::from([(analytics_evidence_id, analytics_evidence)]),
            &BTreeMap::new(),
        ),
        Err(FairValueError::InvalidInputAssessment)
    );

    let measurement_amount = ValuationAmount::try_new(
        Money::new(Decimal::new(125, 2), currency),
        2,
        ValuationAmountBasis::PerInstrumentUnit,
    )?;
    let measurement = ValuationMeasurement::try_new(ValuationMeasurementSpec {
        account_id,
        instrument_id,
        amount: measurement_amount,
        measurement_at: Timestamp::from_unix_nanos(1_000),
        prepared_at: Timestamp::from_unix_nanos(1_100),
        prepared_by: ActorId::try_from("basis-recovery-preparer")?,
        method: ValuationMethod::IncomeApproach,
        inputs: vec![input],
    })?;
    let recovered_measurement = measurement_from_payload(
        canonical(&serde_json::to_vec(&MeasurementPayload {
            version: PAYLOAD_VERSION,
            account_id: account_id.to_string(),
            instrument_id: instrument_id.to_string(),
            amount: amount_payload(measurement_amount),
            measurement_at_ns: 1_000,
            prepared_at_ns: 1_100,
            prepared_by: "basis-recovery-preparer".to_owned(),
            method: 3,
            input_ids: vec![recovered_input.id().bytes()],
        })?)?,
        &BTreeMap::from([(recovered_input.id(), recovered_input)]),
    )?;
    assert_eq!(recovered_measurement.id(), measurement.id());
    assert_eq!(
        recovered_measurement.evidence_hash(),
        measurement.evidence_hash()
    );
    assert_eq!(
        recovered_measurement.amount_basis(),
        ValuationAmountBasis::PerInstrumentUnit
    );

    let mut missing_basis = serde_json::to_value(amount_payload(measurement_amount))?;
    let object = missing_basis
        .as_object_mut()
        .ok_or("amount payload did not serialize as an object")?;
    if object.remove("basis").is_none() {
        return Err("amount payload did not contain basis".into());
    }
    assert_eq!(
        canonical::<AmountPayload>(&serde_json::to_vec(&missing_basis)?),
        Err(FairValueError::CorruptPersistence)
    );

    let current_ruleset = ClassificationRuleset::current(100)?;
    let wrong_ruleset_version = current_ruleset
        .version()
        .checked_sub(1)
        .ok_or("current ruleset version has no invalid predecessor")?;
    assert_eq!(
        current_ruleset_from_payload(wrong_ruleset_version, 100),
        Err(FairValueError::CorruptPersistence)
    );
    let mut missing_ruleset_version = serde_json::to_value(DecisionPayload::Rules {
        version: PAYLOAD_VERSION,
        measurement_id: measurement.id().bytes(),
        max_quote_age_nanos: 100,
        ruleset_version: current_ruleset.version(),
    })?;
    let object = missing_ruleset_version
        .as_object_mut()
        .ok_or("rules payload did not serialize as an object")?;
    if object.remove("ruleset_version").is_none() {
        return Err("rules payload did not contain ruleset_version".into());
    }
    assert_eq!(
        canonical::<DecisionPayload>(&serde_json::to_vec(&missing_ruleset_version)?),
        Err(FairValueError::CorruptPersistence)
    );
    Ok(())
}
