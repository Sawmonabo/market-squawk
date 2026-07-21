use market_squawk_adapter_treasury::{
    FiscalDataPage, FiscalDataParseLimits, TreasuryPaginationTracker, TreasuryProtocolError,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn accepts_one_based_schema_bound_pages_and_rejects_repetition() -> TestResult {
    let page = FiscalDataPage::parse(
        include_bytes!("../fixtures/average_interest_rates.json"),
        1,
        FiscalDataParseLimits::production_defaults(),
    )?;
    assert_eq!(page.page_number(), 1);
    assert_eq!(page.records().len(), 1);
    assert_eq!(page.total_pages(), 4_977);
    assert_eq!(page.records()[0].get("record_date"), Some("2026-06-30"));

    let mut tracker = TreasuryPaginationTracker::try_new(5_000, 10_000)?;
    assert!(!tracker.accept(&page)?);
    assert_eq!(
        tracker.accept(&page),
        Err(TreasuryProtocolError::UnexpectedPage {
            expected: 2,
            actual: 1,
        })
    );
    Ok(())
}

#[test]
fn rejects_missing_page_numbers_before_parsing_rows() {
    assert!(
        FiscalDataPage::parse(
            include_bytes!("../fixtures/average_interest_rates.json"),
            2,
            FiscalDataParseLimits::production_defaults(),
        )
        .is_err()
    );
}
