use std::error::Error;

use market_squawk_adapter_sec::{
    CompanyFactsDocument, SecCompositeBounds, SecParserError, SecParserLimits, SubmissionsDocument,
    reconcile_submissions,
};
use tokio_util::sync::CancellationToken;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn official_json_shapes_preserve_accessions_amendments_periods_and_exact_values() -> TestResult {
    let limits = SecParserLimits::production_defaults();
    let recent = SubmissionsDocument::parse(
        include_bytes!("../fixtures/submissions-recent.json"),
        limits,
    )?;
    let archive = SubmissionsDocument::parse_archive(
        include_bytes!("../fixtures/submissions-archive.json"),
        limits,
    )?;
    let reconciled = reconcile_submissions(&recent, &[archive], limits)?;

    assert_eq!(reconciled.cik().as_str(), "0000320193");
    assert_eq!(reconciled.filings().len(), 3);
    assert_eq!(
        reconciled
            .filing("0000320193-25-000080")
            .ok_or("missing amendment")?
            .form()
            .as_str(),
        "10-Q/A"
    );
    assert!(
        reconciled
            .filing("0000320193-25-000080")
            .ok_or("missing amendment")?
            .is_amendment()
    );
    assert!(
        reconciled
            .filing("0000320193-25-000080")
            .ok_or("missing amendment")?
            .accepted_at()
            .is_none(),
        "an absent exact acceptance time must not be invented"
    );

    let facts =
        CompanyFactsDocument::parse(include_bytes!("../fixtures/company-facts.json"), limits)?;
    assert_eq!(facts.cik().as_str(), "0000320193");
    assert_eq!(facts.occurrences().len(), 3);
    let loss = facts
        .occurrences()
        .iter()
        .find(|fact| fact.concept().as_str() == "us-gaap:NetIncomeLoss")
        .ok_or("missing exact loss")?;
    assert_eq!(loss.value().to_string(), "-23434000000");
    assert_eq!(loss.unit().as_str(), "USD");
    assert_eq!(
        loss.period()
            .start()
            .ok_or("missing duration start")?
            .to_string(),
        "2025-03-30"
    );
    assert_eq!(loss.period().end().to_string(), "2025-06-28");
    assert_eq!(loss.accession().as_str(), "0000320193-25-000079");

    let high_precision = br#"{
        "cik":"0000320193",
        "facts":{"us-gaap":{"ExactRatio":{"units":{"pure":[{
            "val":0.1234567890123456789012345678,
            "accn":"0000320193-25-000079","form":"10-Q",
            "filed":"2025-08-01","end":"2025-06-28"
        }]}}}}
    }"#;
    let exact = CompanyFactsDocument::parse(high_precision, limits)?;
    assert_eq!(
        exact.occurrences()[0].value().to_string(),
        "0.1234567890123456789012345678"
    );
    Ok(())
}

#[test]
fn malformed_columnar_shapes_and_record_limits_fail_closed() -> TestResult {
    assert!(SecCompositeBounds::try_new(0, 1).is_err());
    let mismatched = br#"{
        "cik":"0000320193",
        "filings":{"recent":{"accessionNumber":["0000320193-25-000079"],"form":[]},"files":[]}
    }"#;
    assert!(
        SubmissionsDocument::parse(mismatched, SecParserLimits::production_defaults()).is_err()
    );
    let duplicate_identity = br#"{
        "cik":"0000320193",
        "cik":"0000789019",
        "filings":{"recent":{"accessionNumber":[],"form":[]},"files":[]}
    }"#;
    assert!(
        SubmissionsDocument::parse(duplicate_identity, SecParserLimits::production_defaults())
            .is_err(),
        "ambiguous duplicate JSON keys must fail closed"
    );

    let one_record = SecParserLimits::try_new(1024 * 1024, 1, 128, 16, 64 * 1024, 4 * 1024 * 1024)?;
    assert!(
        CompanyFactsDocument::parse(include_bytes!("../fixtures/company-facts.json"), one_record,)
            .is_err()
    );
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        SubmissionsDocument::parse_with_cancellation(
            include_bytes!("../fixtures/submissions-recent.json"),
            SecParserLimits::production_defaults(),
            &cancellation,
        ),
        Err(SecParserError::Cancelled)
    ));
    Ok(())
}
