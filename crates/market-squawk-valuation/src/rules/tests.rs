use std::{num::NonZeroU32, sync::Arc, time::Duration};

use market_squawk_analytics::FeatureKey;
use market_squawk_data::{
    AnalyticalDataService, AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig,
    CatalogLimit, CatalogResultLimits, DatasetId, DatasetManifestRef, DatasetSchemaRegistry,
    ObjectStoreConfig, Sha256Digest,
};
use market_squawk_domain::{
    AccountId, Currency, DataQuality, DigestAlgorithm, EvidenceDigest, FairValueHierarchy,
    InstrumentId, Money, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_platform::LocalPaths;
use rust_decimal::Decimal;

use super::*;
use crate::evidence::FairValueEvidenceParts;
use crate::measurement::ValuationInputSpec;
use crate::{
    ActorId, ApprovedMarketAccess, EvidenceOrigin, EvidenceVerification, FairValueError,
    FairValueEvidence, FairValueLimitInput, FairValueLimits, FairValueService, ValuationAmount,
    ValuationAmountBasis, ValuationMeasurementSpec, ValuationMethod,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const MEASUREMENT_AT: i64 = 1_000;

#[test]
fn incomplete_or_unverified_evidence_is_unclassified_while_complete_observable_evidence_is_level_two()
-> TestResult {
    let rules = ClassificationRuleset::current(100)?;
    let incomplete = research_input(None, EvidenceVerification::Unverified)?;
    let incomplete_decision = rules.classify(&measurement(incomplete)?)?;
    assert_eq!(
        incomplete_decision.hierarchy(),
        FairValueHierarchy::Unclassified
    );

    let complete = research_input(
        Some(Timestamp::from_unix_nanos(950)),
        EvidenceVerification::Verified,
    )?;
    let complete_decision = rules.classify(&measurement(complete)?)?;
    assert_eq!(complete_decision.hierarchy(), FairValueHierarchy::Level2);
    Ok(())
}

#[test]
fn explicitly_stale_quality_fails_closed_under_current_rules() -> TestResult {
    let input = ValuationInput::try_from_spec(input_spec(
        evidence(
            research_origin()?,
            Some(Timestamp::from_unix_nanos(950)),
            EvidenceVerification::Verified,
        )?,
        InputObservability::Observable,
        MarketActivity::NotAssessed,
        None,
        DataQuality::Stale,
    )?)?;
    let measurement = measurement(input)?;

    let current = ClassificationRuleset::current(100)?.classify(&measurement)?;
    assert_eq!(current.hierarchy(), FairValueHierarchy::Unclassified);
    Ok(())
}

#[test]
fn expired_selected_market_observation_cannot_reach_level_one() -> TestResult {
    let rules = ClassificationRuleset::current(100)?;
    let input = market_input(&rules)?;
    let decision = rules.classify(&measurement(input)?)?;

    assert_eq!(decision.hierarchy(), FairValueHierarchy::Unclassified);
    Ok(())
}

#[test]
fn modeled_analytics_requires_an_explicit_input_use_assessment() -> TestResult {
    let result = ValuationInput::try_from_spec(input_spec(
        evidence(
            analytics_origin()?,
            Some(Timestamp::from_unix_nanos(950)),
            EvidenceVerification::Verified,
        )?,
        InputObservability::Observable,
        MarketActivity::NotAssessed,
        None,
        DataQuality::Modeled,
    )?);

    assert_eq!(result, Err(FairValueError::InvalidInputAssessment));
    Ok(())
}

#[test]
fn later_inaccessible_access_supersedes_stale_access_across_recovery() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (analytical, limits) = catalog_fixture(directory.path())?;
    let rules = ClassificationRuleset::current(100)?;
    let stale_measurement = {
        let mut service = FairValueService::open(analytical.fair_value_catalog(), limits)?;
        let accessible = approve_access(
            &mut service,
            MarketAccess::Accessible,
            (900, 910, 920),
            "first",
        )?;
        approve_access(
            &mut service,
            MarketAccess::Inaccessible,
            (950, 930, 940),
            "second",
        )?;
        let value = measurement(market_input_with_access(&rules, accessible.as_ref())?)?;
        assert!(matches!(
            service.classify(value.clone(), rules.clone()),
            Err(FairValueError::InvalidMarketAccessAssessment)
        ));
        value
    };

    let mut reopened = FairValueService::open(analytical.fair_value_catalog(), limits)?;
    assert!(matches!(
        reopened.classify(stale_measurement, rules),
        Err(FairValueError::InvalidMarketAccessAssessment)
    ));
    Ok(())
}

fn catalog_fixture(root: &std::path::Path) -> TestResult<(AnalyticalDataService, FairValueLimits)> {
    let paths = LocalPaths::prepare(root.join("local"))?;
    let catalog = CatalogAuthority::open(CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 16 * 1024 * 1024)?,
    )?)?;
    let analytical = AnalyticalDataService::initialize(
        catalog,
        AnalyticalManifestCatalog::open(paths.catalog()?, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
    )?;
    let limits = FairValueLimits::try_new(FairValueLimitInput {
        max_measurements: 4,
        max_inputs_per_measurement: 2,
        max_records_per_family: 8,
        max_query_results: 8,
        max_retained_bytes: 512 * 1024,
    })?;
    Ok((analytical, limits))
}

fn approve_access(
    service: &mut FairValueService,
    conclusion: MarketAccess,
    times: (i64, i64, i64),
    actor_prefix: &str,
) -> TestResult<Arc<ApprovedMarketAccess>> {
    let preparer = ActorId::try_from(format!("{actor_prefix}-access-preparer").as_str())?;
    let approver = ActorId::try_from(format!("{actor_prefix}-access-approver").as_str())?;
    Ok(service.approve_market_access(
        account()?,
        VenueId::try_from("XNYS")?,
        instrument()?,
        conclusion,
        Timestamp::from_unix_nanos(times.0),
        Timestamp::from_unix_nanos(2_000),
        "governed reporting-entity market-access assessment",
        preparer,
        Timestamp::from_unix_nanos(times.1),
        approver,
        Timestamp::from_unix_nanos(times.2),
    )?)
}

fn account() -> Result<AccountId, Box<dyn std::error::Error>> {
    Ok("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse()?)
}

fn instrument() -> Result<InstrumentId, Box<dyn std::error::Error>> {
    Ok("9f3914d3-9ef4-42f7-a707-3f2dcde861d1".parse()?)
}

fn amount() -> Result<ValuationAmount, FairValueError> {
    let currency = Currency::try_from("USD").map_err(|_| FairValueError::InvalidAmount)?;
    ValuationAmount::try_new(
        Money::new(Decimal::new(10_000, 2), currency),
        2,
        ValuationAmountBasis::PerInstrumentUnit,
    )
}

fn manifest() -> Result<DatasetManifestRef, Box<dyn std::error::Error>> {
    Ok(DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("fair-value-rules-test")?,
        1,
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([7; 32]),
    )?)
}

fn research_origin() -> Result<EvidenceOrigin, Box<dyn std::error::Error>> {
    Ok(EvidenceOrigin::Research {
        manifest: manifest()?,
        object_graph_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]),
        query_identity: EvidenceDigest::new(DigestAlgorithm::Sha256, [2; 32]),
        result_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [3; 32]),
        row: 0,
        revision: 1,
    })
}

fn analytics_origin() -> Result<EvidenceOrigin, Box<dyn std::error::Error>> {
    Ok(EvidenceOrigin::Analytics {
        feature_key: FeatureKey::try_new("scenario_stress_total", NonZeroU32::MIN)?,
        semantic_digest: [4; 32],
        manifest: manifest()?,
        object_graph_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]),
        query_identity: EvidenceDigest::new(DigestAlgorithm::Sha256, [2; 32]),
        result_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [3; 32]),
        row: 0,
        revision: 1,
    })
}

fn market_origin(
    rules: &ClassificationRuleset,
) -> Result<EvidenceOrigin, Box<dyn std::error::Error>> {
    Ok(EvidenceOrigin::Market {
        venue_id: VenueId::try_from("XNYS")?,
        assessment_id: SourceIdentifier::try_from("qualification-1")?,
        binding_digest: [5; 32],
        canonical_state_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [6; 32]),
        committed_state_revision: 1,
        definition_revision: 1,
        activity_policy_hash: rules.market_activity_policy().hash().bytes(),
        activity_set_hash: [8; 32],
    })
}

fn evidence(
    origin: EvidenceOrigin,
    relevance_at: Option<Timestamp>,
    verification: EvidenceVerification,
) -> Result<FairValueEvidence, Box<dyn std::error::Error>> {
    let is_market = matches!(origin, EvidenceOrigin::Market { .. });
    let available_at = Timestamp::from_unix_nanos(960);
    Ok(FairValueEvidence::try_from_parts(FairValueEvidenceParts {
        source_id: SourceId::try_from(if is_market {
            "test-market"
        } else {
            "test-research"
        })?,
        source_identifier: SourceIdentifier::try_from("record-1")?,
        payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [9; 32]),
        origin,
        source_timestamp: is_market.then_some(Timestamp::from_unix_nanos(950)),
        effective_at: (!is_market).then_some(relevance_at).flatten(),
        published_at: None,
        available_at: Some(available_at),
        received_at: Some(if is_market {
            Timestamp::from_unix_nanos(955)
        } else {
            available_at
        }),
        qualification_evaluated_at: is_market.then_some(Timestamp::from_unix_nanos(900)),
        qualification_valid_until: is_market.then_some(Timestamp::from_unix_nanos(990)),
        ingested_at: Timestamp::from_unix_nanos(970),
        verification,
    })?)
}

fn research_input(
    relevance_at: Option<Timestamp>,
    verification: EvidenceVerification,
) -> Result<ValuationInput, Box<dyn std::error::Error>> {
    Ok(ValuationInput::try_from_spec(input_spec(
        evidence(research_origin()?, relevance_at, verification)?,
        InputObservability::Observable,
        MarketActivity::NotAssessed,
        None,
        DataQuality::OfficialDelayed,
    )?)?)
}

fn market_input(
    rules: &ClassificationRuleset,
) -> Result<ValuationInput, Box<dyn std::error::Error>> {
    let instrument_id = instrument()?;
    let venue_id = VenueId::try_from("XNYS")?;
    let access = ApprovedMarketAccess::try_new(
        account()?,
        venue_id,
        instrument_id,
        MarketAccess::Accessible,
        Timestamp::from_unix_nanos(900),
        Timestamp::from_unix_nanos(2_000),
        "reporting entity can transact in the assessed market",
        ActorId::try_from("access-preparer")?,
        Timestamp::from_unix_nanos(910),
        ActorId::try_from("access-approver")?,
        Timestamp::from_unix_nanos(920),
        None,
    )?;
    market_input_with_access(rules, &access)
}

fn market_input_with_access(
    rules: &ClassificationRuleset,
    access: &ApprovedMarketAccess,
) -> Result<ValuationInput, Box<dyn std::error::Error>> {
    Ok(ValuationInput::try_from_spec(input_spec(
        evidence(
            market_origin(rules)?,
            Some(Timestamp::from_unix_nanos(950)),
            EvidenceVerification::Verified,
        )?,
        InputObservability::QuotedPrice,
        MarketActivity::Active,
        Some(access.clone()),
        DataQuality::DirectVerified,
    )?)?)
}

fn input_spec(
    evidence: FairValueEvidence,
    observability: InputObservability,
    market_activity: MarketActivity,
    market_access_assessment: Option<ApprovedMarketAccess>,
    data_quality: DataQuality,
) -> TestResult<ValuationInputSpec> {
    let instrument_id = instrument()?;
    let market_access = market_access_assessment
        .as_ref()
        .map_or(MarketAccess::NotAssessed, |value| value.conclusion());
    Ok(ValuationInputSpec {
        subject_instrument_id: instrument_id,
        reference_instrument_id: instrument_id,
        relationship: InputInstrumentRelation::Identical,
        amount: amount()?,
        significance: InputSignificance::Significant,
        observability,
        adjustment: PriceAdjustment::None,
        market_activity,
        market_access,
        market_access_assessment,
        data_quality,
        evidence,
        use_assessment: None,
    })
}

fn measurement(input: ValuationInput) -> Result<ValuationMeasurement, Box<dyn std::error::Error>> {
    Ok(ValuationMeasurement::try_new(ValuationMeasurementSpec {
        account_id: account()?,
        instrument_id: instrument()?,
        amount: amount()?,
        measurement_at: Timestamp::from_unix_nanos(MEASUREMENT_AT),
        prepared_at: Timestamp::from_unix_nanos(1_100),
        prepared_by: ActorId::try_from("measurement-preparer")?,
        method: ValuationMethod::MarketApproach,
        inputs: vec![input],
    })?)
}
