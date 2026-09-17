use std::error::Error;

use cap_std::{ambient_authority, fs::Dir};
use market_squawk_adapter_sec::{
    CompanyFactsDocument, RawEvidenceStore, RetrievedSubmissions, SecCompositeBounds,
    SecParserError, SecParserLimits, SubmissionsDocument, reconcile_submissions,
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
    let temporary = tempfile::tempdir()?;
    let raw_store = RawEvidenceStore::new(Dir::open_ambient_dir(
        temporary.path(),
        ambient_authority(),
    )?);
    let retrieved = RetrievedSubmissions::import_exact_bytes(
        include_bytes!("../fixtures/submissions-recent.json"),
        &[include_bytes!("../fixtures/submissions-archive.json")],
        &raw_store,
        limits,
    )?;
    assert_eq!(
        retrieved.current_component().bytes().as_ref(),
        include_bytes!("../fixtures/submissions-recent.json")
    );

    assert_eq!(reconciled.cik().as_str(), "0000320193");
    let metadata = reconciled.company_metadata();
    assert_eq!(metadata.conformed_name(), "APPLE INC");
    assert_eq!(metadata.entity_type(), Some("operating"));
    assert_eq!(metadata.sic(), Some("3571"));
    assert_eq!(metadata.sic_description(), Some("Electronic Computers"));
    assert_eq!(metadata.ticker_exchange_pairs().len(), 1);
    assert_eq!(metadata.ticker_exchange_pairs()[0].ticker(), "AAPL");
    assert_eq!(metadata.ticker_exchange_pairs()[0].exchange(), "Nasdaq");
    assert_eq!(reconciled.companions().len(), 1);
    assert_eq!(
        reconciled.companions()[0].name().as_str(),
        "CIK0000320193-submissions-001.json"
    );
    assert_eq!(reconciled.companions()[0].filing_count(), 2);
    assert_eq!(
        reconciled.companions()[0].filing_from().to_string(),
        "2020-01-01"
    );
    assert_eq!(
        reconciled.companions()[0].filing_to().to_string(),
        "2025-07-31"
    );
    let former_name_document = br#"{
        "cik":"0000320193","name":"Apple Inc.",
        "formerNames":[{"name":"APPLE COMPUTER INC","from":"1994-01-26T05:00:00.000Z","to":"2007-01-04T05:00:00.000Z"}],
        "tickers":["AAPL"],"exchanges":["Nasdaq"],
        "filings":{"recent":{"accessionNumber":[],"filingDate":[],"reportDate":[],"acceptanceDateTime":[],"form":[]},"files":[]}
    }"#;
    let former_name = SubmissionsDocument::parse(former_name_document, limits)?;
    assert_eq!(
        former_name.company_metadata().former_names()[0].name(),
        "APPLE COMPUTER INC"
    );
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
    assert_eq!(facts.entity_name(), "APPLE INC");
    assert_eq!(facts.occurrences().len(), 3);
    let assets: Vec<_> = facts
        .occurrences()
        .iter()
        .filter(|fact| fact.concept().as_str() == "us-gaap:Assets")
        .collect();
    assert_eq!(assets[0].source_ordinal(), 0);
    assert_eq!(assets[1].source_ordinal(), 1);
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
        "entityName":"APPLE INC",
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
        "name":"APPLE INC","tickers":[],"exchanges":[],
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
    let mismatched_associations = br#"{
        "cik":"0000320193","name":"APPLE INC",
        "tickers":["AAPL"],"exchanges":[]
    }"#;
    assert!(matches!(
        SubmissionsDocument::parse(
            mismatched_associations,
            SecParserLimits::production_defaults()
        ),
        Err(SecParserError::MetadataAssociationLengthMismatch)
    ));
    let duplicate_association = br#"{
        "cik":"0000320193","name":"APPLE INC",
        "tickers":["AAPL","AAPL"],"exchanges":["Nasdaq","Nasdaq"]
    }"#;
    assert!(matches!(
        SubmissionsDocument::parse(
            duplicate_association,
            SecParserLimits::production_defaults()
        ),
        Err(SecParserError::DuplicateMetadataAssociation)
    ));

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
    let false_companion_coverage = br#"{
        "cik":"0000320193","name":"APPLE INC","tickers":[],"exchanges":[],
        "filings":{
            "recent":{"accessionNumber":[],"filingDate":[],"reportDate":[],"acceptanceDateTime":[],"form":[]},
            "files":[{"name":"CIK0000320193-submissions-001.json","filingCount":2,"filingFrom":"2020-01-01","filingTo":"2020-12-31"}]
        }
    }"#;
    let recent = SubmissionsDocument::parse(
        false_companion_coverage,
        SecParserLimits::production_defaults(),
    )?;
    let archive = SubmissionsDocument::parse_archive(
        include_bytes!("../fixtures/submissions-archive.json"),
        SecParserLimits::production_defaults(),
    )?;
    assert!(matches!(
        reconcile_submissions(&recent, &[archive], SecParserLimits::production_defaults()),
        Err(SecParserError::InvalidCompanionCoverage)
    ));
    let no_retained_output =
        SecParserLimits::try_new(1024 * 1024, 10, 128, 256 * 1024, 512 * 1024, 1)?;
    assert!(matches!(
        CompanyFactsDocument::parse(
            include_bytes!("../fixtures/company-facts.json"),
            no_retained_output
        ),
        Err(SecParserError::RetainedOutputLimitExceeded)
    ));
    Ok(())
}
