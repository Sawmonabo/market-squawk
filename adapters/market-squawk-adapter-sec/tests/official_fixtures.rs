use std::error::Error;

use cap_std::{ambient_authority, fs::Dir};
use market_squawk_adapter_sec::{
    CompanyFactsDocument, RawEvidenceStore, RetrievedSubmissions, SecCompositeBounds,
    SecParserError, SecParserLimits, SubmissionsDocument, normalize_filings, reconcile_submissions,
};
use market_squawk_domain::{ResearchObservation, SourceId};
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
    // Reduced from the retained official Apple submissions response retrieved 2026-10-02.
    // The accession's submitting account differs from the response's subject company.
    let agent_submission = serde_json::json!({
        "cik":"0000320193", "name":"Apple Inc.",
        "tickers":["AAPL"], "exchanges":["Nasdaq"],
        "filings":{"recent":{
            "accessionNumber":["0001628280-17-004790"], "form":["10-Q"],
            "filingDate":["2017-05-03"], "reportDate":["2017-04-01"],
            "acceptanceDateTime":["2017-05-04T00:32:23.000Z"],
            "primaryDocument":["a10-qq22017412017.htm"]
        }, "files":[]}
    });
    // One exact retained row for each spaced form in the same official response.
    let spaced_submissions = serde_json::json!({
        "cik":"0000320193", "name":"Apple Inc.",
        "tickers":["AAPL"], "exchanges":["Nasdaq"],
        "filings":{"recent":{
            "accessionNumber":[
                "0002100119-26-000139", "0000102909-26-000630", "0001308179-26-000008",
                "0001193125-24-036431", "0001193125-22-128361", "0001193125-19-041014",
                "9999999997-17-000002"
            ],
            "form":["SCHEDULE 13G", "SCHEDULE 13G/A", "DEF 14A", "SC 13G/A", "S-8 POS", "SC 13G", "NO ACT"],
            "filingDate":["2026-04-29", "2026-03-26", "2026-01-08", "2024-02-14", "2022-04-29", "2019-02-14", "2016-12-05"],
            "reportDate":["", "", "2026-02-24", "", "", "", "2016-10-07"],
            "acceptanceDateTime":[
                "2026-04-30T00:40:10.000Z", "2026-03-26T23:43:19.000Z", "2026-01-09T02:31:36.000Z",
                "2024-02-15T02:47:05.000Z", "2022-04-29T02:59:32.000Z", "2019-02-15T02:42:33.000Z",
                "2017-01-03T19:49:40.000Z"
            ],
            "primaryDocument":[
                "xslSCHEDULE_13G_X02/primary_doc.xml", "xslSCHEDULE_13G_X02/primary_doc.xml",
                "aapl014016-def14a.htm", "d751537dsc13ga.htm", "d279200ds8pos.htm",
                "d667611dsc13g.htm", "9999999997-17-000002.paper"
            ]
        }, "files":[]}
    });
    let spaced_bytes = serde_json::to_vec(&spaced_submissions)?;
    let spaced = RetrievedSubmissions::import_exact_bytes(&spaced_bytes, &[], &raw_store, limits)?;
    let spaced_archive = SubmissionsDocument::parse_archive(
        &serde_json::to_vec(&spaced_submissions["filings"]["recent"])?,
        limits,
    )?;
    assert_eq!(
        spaced_archive.filings().len(),
        spaced.document().filings().len()
    );
    for (index, archived) in spaced_archive.filings().iter().enumerate() {
        assert_eq!(
            Some(archived),
            spaced.document().filing(archived.accession().as_str()),
        );
        assert_eq!(
            archived.form().as_str(),
            spaced_submissions["filings"]["recent"]["form"][index]
                .as_str()
                .ok_or("missing exact spaced form")?,
        );
    }
    let spaced_observations = normalize_filings(
        &SourceId::try_from("sec-edgar")?,
        &spaced,
        spaced.raw().received_at().checked_add_nanos(1)?,
    )?;
    assert_eq!(spaced_observations.len(), 7);
    for observation in &spaced_observations {
        let ResearchObservation::Filing(filing) = observation else {
            return Err("expected spaced-form filing".into());
        };
        let source = spaced
            .document()
            .filing(filing.accession().as_str())
            .ok_or("missing spaced-form source filing")?;
        assert_eq!(filing.form_type(), source.form());
        assert_eq!(filing.subject().issuer_id(), Some(spaced.document().cik()));
        let reopened: ResearchObservation =
            serde_json::from_slice(&serde_json::to_vec(observation)?)?;
        assert_eq!(&reopened, observation);
    }
    let agent_recent = SubmissionsDocument::parse(&serde_json::to_vec(&agent_submission)?, limits)?;
    assert_eq!(agent_recent.cik().as_str(), "0000320193");
    let agent_filing = agent_recent
        .filing("0001628280-17-004790")
        .ok_or("missing agent-submitted filing")?;
    assert_eq!(
        agent_filing
            .accepted_at()
            .ok_or("missing acceptance time")?
            .unix_nanos(),
        1_493_857_943_000_000_000
    );
    let agent_archive = SubmissionsDocument::parse_archive(
        &serde_json::to_vec(&agent_submission["filings"]["recent"])?,
        limits,
    )?;
    let mut archive_context = agent_submission.clone();
    for column in archive_context["filings"]["recent"]
        .as_object_mut()
        .ok_or("missing columns")?
        .values_mut()
    {
        *column = serde_json::json!([]);
    }
    archive_context["filings"]["files"] = serde_json::json!([{
        "name":"CIK0000320193-submissions-001.json", "filingCount":1,
        "filingFrom":"2017-05-03", "filingTo":"2017-05-03"
    }]);
    let recent_context =
        SubmissionsDocument::parse(&serde_json::to_vec(&archive_context)?, limits)?;
    let joined = reconcile_submissions(
        &recent_context,
        std::slice::from_ref(&agent_archive),
        limits,
    )?;
    assert_eq!(joined.cik(), agent_recent.cik());
    assert_eq!(joined.filings(), agent_recent.filings());
    archive_context["filings"]["files"][0]["name"] =
        serde_json::json!("CIK0000789019-submissions-001.json");
    let wrong_context = SubmissionsDocument::parse(&serde_json::to_vec(&archive_context)?, limits)?;
    assert!(matches!(
        reconcile_submissions(&wrong_context, &[agent_archive], limits),
        Err(SecParserError::InvalidCompanionCoverage)
    ));
    let mut missing_zone = agent_submission.clone();
    missing_zone["filings"]["recent"]["acceptanceDateTime"][0] =
        serde_json::json!("2017-05-04T00:32:23");
    assert!(matches!(
        SubmissionsDocument::parse(&serde_json::to_vec(&missing_zone)?, limits),
        Err(SecParserError::InvalidTimestamp)
    ));
    let mut malformed_accession = agent_submission;
    malformed_accession["filings"]["recent"]["accessionNumber"][0] =
        serde_json::json!("0001628280-17-bad");
    assert!(matches!(
        SubmissionsDocument::parse(&serde_json::to_vec(&malformed_accession)?, limits),
        Err(SecParserError::InvalidAccession)
    ));
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
            "accn":"0001628280-25-000079","form":"SC 13G/A",
            "filed":"2025-08-01","end":"2025-06-28"
        }]}}}}
    }"#;
    let exact = CompanyFactsDocument::parse(high_precision, limits)?;
    assert_eq!(exact.cik().as_str(), "0000320193");
    assert_eq!(exact.occurrences()[0].form().as_str(), "SC 13G/A");
    assert_eq!(
        exact.occurrences()[0].accession().as_str(),
        "0001628280-25-000079"
    );
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
    let rejected = CompanyFactsDocument::parse(
        include_bytes!("../fixtures/company-facts.json"),
        no_retained_output,
    );
    assert!(
        matches!(rejected, Err(SecParserError::RetainedOutputLimitExceeded)),
        "{rejected:?}"
    );
    Ok(())
}
