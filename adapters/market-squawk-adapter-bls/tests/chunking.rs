use market_squawk_adapter_bls::{BlsAccessTier, BlsRequestPlan, BlsResponse, BlsVintageCapability};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn public_and_registered_plans_obey_tier_and_conflict_safe_bounds() -> TestResult {
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
