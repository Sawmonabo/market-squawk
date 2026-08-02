use market_squawk_adapter_treasury::{
    FiscalDataPage, FiscalDataParseLimits, TreasuryFiscalQuery, TreasuryPaginationTracker,
    TreasuryProtocolError,
};
use market_squawk_domain::CalendarDate;
use sha2::{Digest, Sha256};
use std::num::NonZeroU16;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn accepts_one_based_schema_bound_pages_and_rejects_repetition() -> TestResult {
    let query = average_rates_query(2026)?;
    let request = query.page(1)?;
    let exact_payload = include_bytes!("../fixtures/average_interest_rates.json");
    let page = FiscalDataPage::parse(
        exact_payload,
        &request,
        FiscalDataParseLimits::production_defaults(),
    )?;
    assert_eq!(page.page_number(), 1);
    assert_eq!(page.records().len(), 1);
    assert_eq!(page.total_pages(), 4_977);
    assert_eq!(page.records()[0].get("record_date"), Some("2026-06-30"));

    assert_eq!(
        page.records()[0].source_payload_digest(),
        page.response_payload_digest()
    );
    assert_eq!(
        page.response_payload_digest(),
        <[u8; 32]>::from(Sha256::digest(exact_payload))
    );

    let mut tracker = TreasuryPaginationTracker::try_new(&query, 5_000, 10_000)?;
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
fn rejects_missing_page_numbers_before_parsing_rows() -> TestResult {
    let query = average_rates_query(2026)?;
    let request = query.page(2)?;
    assert!(
        FiscalDataPage::parse(
            include_bytes!("../fixtures/average_interest_rates.json"),
            &request,
            FiscalDataParseLimits::production_defaults(),
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn tracker_rejects_a_page_bound_to_another_canonical_query() -> TestResult {
    let expected = average_rates_query(2026)?;
    let date = CalendarDate::new(2026, 6, 30)?;
    let transplanted = TreasuryFiscalQuery::average_interest_rates_v2(
        date,
        date,
        NonZeroU16::new(1).ok_or("page size must be non-zero")?,
    )?;
    let page = FiscalDataPage::parse(
        include_bytes!("../fixtures/average_interest_rates.json"),
        &transplanted.page(1)?,
        FiscalDataParseLimits::production_defaults(),
    )?;

    let mut tracker = TreasuryPaginationTracker::try_new(&expected, 5_000, 10_000)?;
    assert_eq!(
        tracker.accept(&page),
        Err(TreasuryProtocolError::QueryBindingMismatch)
    );
    assert_ne!(
        expected.page(1)?.request_digest(),
        expected.page(2)?.request_digest()
    );
    Ok(())
}

#[test]
fn parser_rejects_rows_outside_the_bound_date_filter() -> TestResult {
    let wrong_year = average_rates_query(2025)?;
    assert_eq!(
        FiscalDataPage::parse(
            include_bytes!("../fixtures/average_interest_rates.json"),
            &wrong_year.page(1)?,
            FiscalDataParseLimits::production_defaults(),
        ),
        Err(TreasuryProtocolError::QueryBindingMismatch)
    );
    Ok(())
}

fn average_rates_query(year: u16) -> Result<TreasuryFiscalQuery, Box<dyn std::error::Error>> {
    Ok(TreasuryFiscalQuery::average_interest_rates_v2(
        CalendarDate::new(year, 1, 1)?,
        CalendarDate::new(year, 12, 31)?,
        NonZeroU16::new(1).ok_or("page size must be non-zero")?,
    )?)
}
