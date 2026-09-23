use market_squawk_adapter_fred::{
    FredObservationPage, FredParseLimits, FredReleaseObservationPage, FredVintagePage,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn preserves_closed_realtime_dates_missing_values_and_page_cursor() -> TestResult {
    let limits = FredParseLimits::production_defaults();
    let observations =
        FredObservationPage::parse(include_bytes!("../fixtures/observations.json"), limits)?;
    assert_eq!(observations.offset(), 0);
    assert_eq!(observations.next_offset(), Some(2));
    assert_eq!(observations.observations().len(), 2);
    assert_eq!(observations.observations()[0].raw_value(), "101.25");
    assert_eq!(
        observations.observations()[0].realtime_start().to_string(),
        "2024-01-01"
    );
    assert!(observations.observations()[1].value().is_none());
    assert_eq!(observations.observations()[1].raw_value(), ".");

    let vintages = FredVintagePage::parse(include_bytes!("../fixtures/vintages.json"), limits)?;
    assert_eq!(vintages.next_offset(), Some(2));
    assert_eq!(vintages.vintage_dates()[0].to_string(), "2024-01-10");

    let release = FredReleaseObservationPage::parse_for_request(
        br#"{
          "has_more": true,
          "next_cursor": "PAYEMS,2024-03-01",
          "release": {
            "release_id": 10,
            "name": "Employment Situation",
            "url": "https://www.bls.gov/news.release/empsit.htm",
            "sources": [{"name":"U.S. Bureau of Labor Statistics","url":"https://www.bls.gov/"}]
          },
          "series": [{
            "series_id": "PAYEMS",
            "title": "All Employees, Total Nonfarm",
            "frequency": "Monthly",
            "units": "Thousands of Persons",
            "seasonal_adjustment": "Seasonally Adjusted",
            "last_updated": "2024-04-05T12:30:00Z",
            "copyright_id": "public domain: citation requested",
            "notes": "Source ID: CES0000000001",
            "observations": [{"date":"2024-01-01","value":"."}]
          }]
        }"#,
        limits,
        10,
        None,
    )?;
    assert_eq!(release.release().release_id(), 10);
    assert_eq!(release.observation_count(), 1);
    assert!(release.series()[0].observations()[0].value().is_none());

    let mut unstable: serde_json::Value =
        serde_json::from_slice(include_bytes!("../fixtures/observations.json"))?;
    unstable["observations"][1]["date"] = "2023-01-01".into();
    assert!(FredObservationPage::parse(&serde_json::to_vec(&unstable)?, limits).is_err());
    let limits = FredParseLimits::try_new(1, 16 * 1024, 1_024)?;
    assert!(
        FredObservationPage::parse(include_bytes!("../fixtures/observations.json"), limits)
            .is_err()
    );
    Ok(())
}
