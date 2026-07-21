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
    let exact_payload = include_bytes!("../fixtures/daily_par_yield_curve.xml");
    let page = DailyParYieldCurvePage::parse(
        exact_payload,
        &profile.page(2026, 0)?,
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
