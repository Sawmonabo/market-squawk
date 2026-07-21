use market_squawk_adapter_treasury::{
    AverageInterestRate, DailyParYieldCurvePage, FiscalDataPage, FiscalDataParseLimits,
    TreasuryFiscalQuery, TreasuryRateProfile, TreasuryYieldCurveProfile,
};
use market_squawk_domain::{CalendarDate, DataQuality};
use sha2::{Digest, Sha256};
use std::num::NonZeroU16;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn official_average_rate_profile_preserves_exact_decimal_and_methodology_evidence() -> TestResult {
    let query = TreasuryFiscalQuery::average_interest_rates_v2(
        CalendarDate::new(2026, 1, 1)?,
        CalendarDate::new(2026, 12, 31)?,
        NonZeroU16::new(1).ok_or("page size must be non-zero")?,
    )?;
    let page = FiscalDataPage::parse(
        include_bytes!("../fixtures/average_interest_rates.json"),
        &query.page(1)?,
        FiscalDataParseLimits::production_defaults(),
    )?;
    let profile = TreasuryRateProfile::average_interest_rates_v2();
    let rate = AverageInterestRate::try_from_record(&page.records()[0], &profile)?;
    assert_eq!(rate.record_date().to_string(), "2026-06-30");
    assert_eq!(rate.rate_percent().to_string(), "3.706");
    assert_eq!(rate.security_description(), "Treasury Bills");
    assert_eq!(rate.source_line_number(), "1");
    assert_eq!(rate.source_payload_digest(), page.response_payload_digest());
    assert_eq!(profile.endpoint(), "/v2/accounting/od/avg_interest_rates");
    assert!(
        profile
            .source_url()
            .starts_with("https://fiscaldata.treasury.gov/")
    );
    assert_eq!(rate.schema_digest(), page.schema_digest());
    Ok(())
}

#[test]
fn daily_par_yield_curve_is_civil_dated_and_indicative() -> TestResult {
    let profile = TreasuryYieldCurveProfile::daily_par_yield_curve();
    let request = profile.page(2026, 0)?;
    assert!(!request.url().contains("page="));
    assert!(profile.page(2026, 1).is_err());
    let exact_payload = include_bytes!("../fixtures/daily_par_yield_curve.xml");
    let page = DailyParYieldCurvePage::parse(
        exact_payload,
        &request,
        FiscalDataParseLimits::production_defaults(),
    )?;

    let observation = &page.observations()[0];
    assert_eq!(profile.quality(), DataQuality::Indicative);
    assert_eq!(observation.record_date().to_string(), "2026-01-02");
    assert_eq!(observation.source_record_id(), "140");
    assert_eq!(
        observation
            .one_month_percent()
            .map(|value| value.to_string())
            .as_deref(),
        Some("3.72")
    );
    assert_eq!(
        observation
            .thirty_year_percent()
            .map(|value| value.to_string())
            .as_deref(),
        Some("4.86")
    );
    assert_eq!(
        observation.source_payload_digest(),
        page.response_payload_digest()
    );
    assert_eq!(
        page.response_payload_digest(),
        <[u8; 32]>::from(Sha256::digest(exact_payload))
    );
    assert!(
        profile
            .methodology_url()
            .starts_with("https://home.treasury.gov/")
    );
    Ok(())
}

#[test]
fn daily_par_yield_curve_rejects_wrong_namespace_and_rows_without_rates() -> TestResult {
    let profile = TreasuryYieldCurveProfile::daily_par_yield_curve();
    let request = profile.page(2026, 0)?;
    let exact_payload =
        std::str::from_utf8(include_bytes!("../fixtures/daily_par_yield_curve.xml"))?;
    let wrong_namespace = exact_payload.replace(
        "http://schemas.microsoft.com/ado/2007/08/dataservices\"",
        "https://attacker.invalid/dataservices\"",
    );
    assert!(
        DailyParYieldCurvePage::parse(
            wrong_namespace.as_bytes(),
            &request,
            FiscalDataParseLimits::production_defaults(),
        )
        .is_err()
    );

    let no_rates = br#"<?xml version="1.0"?>
      <feed xmlns="http://www.w3.org/2005/Atom"
            xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
            xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
        <title>DailyTreasuryYieldCurveRateData</title>
        <id>https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml-item?data=daily_treasury_yield_curve</id>
        <updated>2026-07-21T06:54:08Z</updated>
        <entry>
          <id>https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml-item?data=daily_treasury_yield_curve&amp;id=140</id>
          <updated>2026-07-21T06:54:08Z</updated>
          <content><m:properties>
            <d:Id m:type="Edm.Int32">140</d:Id>
            <d:NEW_DATE m:type="Edm.DateTime">2026-01-02T00:00:00</d:NEW_DATE>
          </m:properties></content>
        </entry>
      </feed>"#;
    assert!(
        DailyParYieldCurvePage::parse(
            no_rates,
            &request,
            FiscalDataParseLimits::production_defaults(),
        )
        .is_err()
    );
    Ok(())
}
