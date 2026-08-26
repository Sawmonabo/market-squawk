use bytes::Bytes;
use market_squawk_adapter_bls::{
    BlsAccessTier, BlsAuthorization, BlsParseError, BlsRegistrationKey, BlsRequestPlan,
    BlsResponse, BlsSeriesMetadata, BlsSource, BlsSourceConfig, BlsSourceError, BlsUsageOperation,
    BlsUsagePolicy, BlsVintageCapability,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, SourceIdentifier,
};
use sha2::{Digest, Sha256};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn metadata(series_id: &str, unit: &str) -> Result<BlsSeriesMetadata, BlsSourceError> {
    let payload = Bytes::from(format!(
        r#"{{"schema_version":1,"series_id":"{series_id}","title":"Test series","unit":"{unit}","frequency":"monthly","seasonal_adjustment":"not-specified","measure":"test-measure"}}"#
    ));
    let evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(&payload).into(),
    ));
    BlsSeriesMetadata::parse_exact(
        payload,
        evidence,
        SourceIdentifier::try_from("user-approved:test-metadata")
            .map_err(|_| BlsSourceError::InvalidSeriesMetadata)?,
    )
}

fn usage_policy() -> Result<BlsUsagePolicy, BlsSourceError> {
    BlsUsagePolicy::try_owner_authorized(EvidenceDigest::new(DigestAlgorithm::Sha256, [42; 32]))
}

#[test]
fn public_and_registered_plans_obey_documented_tier_bounds() -> TestResult {
    let series = (0..26)
        .map(|index| format!("SERIES{index:04}"))
        .collect::<Vec<_>>();
    let public = BlsRequestPlan::try_new(BlsAccessTier::PublicV1, series.clone(), 2000, 2011)?;
    assert_eq!(public.chunks().len(), 4);
    assert!(
        public
            .chunks()
            .iter()
            .all(|chunk| chunk.series().len() <= 25)
    );
    assert!(public.chunks().iter().all(|chunk| chunk.year_count() <= 10));

    let registered = BlsRequestPlan::try_new(BlsAccessTier::RegisteredV2, series, 2000, 2021)?;
    assert_eq!(registered.chunks().len(), 3);
    assert!(
        registered
            .chunks()
            .iter()
            .all(|chunk| chunk.series().len() <= 50)
    );
    assert!(
        registered
            .chunks()
            .iter()
            .all(|chunk| chunk.year_count() <= 10)
    );
    assert_eq!(registered.limits().documented_years_per_query(), 20);
    assert_eq!(registered.limits().enforced_years_per_query(), 10);
    assert_eq!(registered.limits().documented_daily_queries(), 500);
    assert_eq!(registered.limits().daily_queries(), 400);
    assert_eq!(registered.limits().enforced_requests_per_second(), 1);
    Ok(())
}

#[test]
fn parser_retains_partial_messages_preliminary_flags_and_missing_values() -> TestResult {
    let response = BlsResponse::parse(
        include_bytes!("../fixtures/series.json"),
        BlsAccessTier::PublicV1,
    )?;
    assert!(response.is_partial());
    assert_eq!(response.messages().len(), 1);
    assert!(response.series()[0].observations()[0].is_preliminary());
    assert_eq!(
        response.series()[0].observations()[0]
            .value()
            .map(|value| value.to_string()),
        Some("4.2".to_owned())
    );
    assert!(response.series()[0].observations()[1].value().is_none());
    assert_eq!(
        response.vintage_capability(),
        BlsVintageCapability::LocallyObservedVersionsOnly
    );
    Ok(())
}

#[test]
fn requested_series_binding_rejects_missing_extra_and_duplicate_results() -> TestResult {
    let requested = ["LNS14000000", "CIU1010000000000A"];
    let missing = br#"{
        "status":"REQUEST_SUCCEEDED","responseTime":1,"message":[],
        "Results":{"series":[{"seriesID":"LNS14000000","data":[]}]}
    }"#;
    assert!(matches!(
        BlsResponse::parse_for_request(missing, BlsAccessTier::PublicV1, &requested, 2020, 2026),
        Err(BlsParseError::RequestSeriesMismatch)
    ));

    let duplicate = br#"{
        "status":"REQUEST_SUCCEEDED","responseTime":1,"message":[],
        "Results":{"series":[
            {"seriesID":"LNS14000000","data":[]},
            {"seriesID":"LNS14000000","data":[]}
        ]}
    }"#;
    assert!(matches!(
        BlsResponse::parse_for_request(
            duplicate,
            BlsAccessTier::PublicV1,
            &["LNS14000000"],
            2020,
            2026,
        ),
        Err(BlsParseError::RequestSeriesMismatch)
    ));

    let extra = br#"{
        "status":"REQUEST_SUCCEEDED","responseTime":1,"message":[],
        "Results":{"series":[
            {"seriesID":"LNS14000000","data":[]},
            {"seriesID":"UNREQUESTED","data":[]}
        ]}
    }"#;
    assert!(matches!(
        BlsResponse::parse_for_request(
            extra,
            BlsAccessTier::PublicV1,
            &["LNS14000000"],
            2020,
            2026,
        ),
        Err(BlsParseError::RequestSeriesMismatch)
    ));

    let wrong_year = br#"{
        "status":"REQUEST_SUCCEEDED","responseTime":1,"message":[],
        "Results":{"series":[{"seriesID":"LNS14000000","data":[{
            "year":"2019","period":"M01","periodName":"January","value":"1.0",
            "footnotes":[]
        }]}]}
    }"#;
    assert!(matches!(
        BlsResponse::parse_for_request(
            wrong_year,
            BlsAccessTier::PublicV1,
            &["LNS14000000"],
            2020,
            2026,
        ),
        Err(BlsParseError::RequestYearMismatch)
    ));

    let invalid_period = br#"{
        "status":"REQUEST_SUCCEEDED","responseTime":1,"message":[],
        "Results":{"series":[{"seriesID":"LNS14000000","data":[{
            "year":"2026","period":"M14","periodName":"invalid","value":"1.0",
            "footnotes":[]
        }]}]}
    }"#;
    assert!(matches!(
        BlsResponse::parse_for_request(
            invalid_period,
            BlsAccessTier::PublicV1,
            &["LNS14000000"],
            2026,
            2026,
        ),
        Err(BlsParseError::InvalidField("period"))
    ));
    Ok(())
}

#[test]
fn documented_identifier_characters_are_accepted_consistently() -> TestResult {
    let identifier = "SERIES_1-2#A".to_owned();
    let plan = BlsRequestPlan::try_new(
        BlsAccessTier::PublicV1,
        vec![identifier.clone()],
        2025,
        2025,
    )?;
    assert_eq!(plan.chunks()[0].series(), &[identifier]);
    Ok(())
}

#[test]
fn registered_key_is_validated_and_debug_redacted() -> TestResult {
    let secret = "fake-fake-fake-fake-fake-fake-fake-fake";
    let key = BlsRegistrationKey::try_new(secret.to_owned())?;
    assert!(!format!("{key:?}").contains(secret));
    assert!(BlsRegistrationKey::try_new(String::new()).is_err());
    assert!(BlsRegistrationKey::try_new("contains whitespace".to_owned()).is_err());
    assert!(BlsRegistrationKey::try_new("x".repeat(257)).is_err());
    Ok(())
}

#[test]
fn source_dataset_identity_binds_tier_series_and_year_window() -> TestResult {
    let usage_policy = usage_policy()?;
    assert!(usage_policy.admits(BlsUsageOperation::ModelTraining));
    assert!(usage_policy.admits(BlsUsageOperation::Backtest));
    assert!(!usage_policy.admits(BlsUsageOperation::Export));
    assert!(!usage_policy.admits(BlsUsageOperation::Sale));
    assert!(!usage_policy.admits(BlsUsageOperation::Redistribute));
    assert!(
        BlsUsagePolicy::try_owner_authorized(
            EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32],)
        )
        .is_err()
    );
    let public = BlsSourceConfig::try_new(
        BlsAuthorization::PublicV1,
        usage_policy,
        vec![metadata("LNS14000000", "percent")?],
        2020,
        2026,
    )?;
    let other_series = BlsSourceConfig::try_new(
        BlsAuthorization::PublicV1,
        usage_policy,
        vec![metadata("CUUR0000SA0", "index")?],
        2020,
        2026,
    )?;
    let registered = BlsSourceConfig::try_new(
        BlsAuthorization::RegisteredV2(BlsRegistrationKey::try_new(
            "fake-fake-fake-fake-fake-fake-fake-fake".to_owned(),
        )?),
        usage_policy,
        vec![metadata("LNS14000000", "percent")?],
        2020,
        2026,
    )?;
    let plan_digest = public
        .dataset()
        .as_str()
        .strip_prefix("bls:timeseries:public-v1:")
        .ok_or("BLS provider dataset prefix")?;
    assert_eq!(
        BlsSource::analytical_dataset_identifier(public.dataset())?.as_str(),
        format!("bls.timeseries.public-v1.{plan_digest}")
    );
    assert_ne!(public.dataset(), other_series.dataset());
    assert_ne!(public.dataset(), registered.dataset());
    let over_daily_plan = (0..626)
        .map(|index| metadata(&format!("SERIES{index:04}"), "count"))
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        BlsSourceConfig::try_new(
            BlsAuthorization::PublicV1,
            usage_policy,
            over_daily_plan,
            2026,
            2026,
        )
        .is_err()
    );
    Ok(())
}
