use market_squawk_adapter_treasury::{
    AverageInterestRate, FiscalDataPage, FiscalDataParseLimits, TreasuryRateProfile,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn official_average_rate_profile_preserves_exact_decimal_and_methodology_evidence() -> TestResult {
    let page = FiscalDataPage::parse(
        include_bytes!("../fixtures/average_interest_rates.json"),
        1,
        FiscalDataParseLimits::production_defaults(),
    )?;
    let profile = TreasuryRateProfile::average_interest_rates_v2();
    let rate = AverageInterestRate::try_from_record(&page.records()[0], &profile)?;
    assert_eq!(rate.record_date().to_string(), "2026-06-30");
    assert_eq!(rate.rate_percent().to_string(), "3.706");
    assert_eq!(rate.security_description(), "Treasury Bills");
    assert_eq!(profile.endpoint(), "/v2/accounting/od/avg_interest_rates");
    assert!(
        profile
            .source_url()
            .starts_with("https://fiscaldata.treasury.gov/")
    );
    assert_eq!(rate.schema_digest(), page.schema_digest());
    Ok(())
}
