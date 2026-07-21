use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::Path;

use bytes::Bytes;
use market_squawk_adapter_portfolio::{
    BasisResolution, ImportDisposition, LotMethod, PortfolioExtractionSource,
    PortfolioImportLimits, TransactionKind,
};
use market_squawk_data::{ResearchArrowBatch, extraction_batch_digest};
use market_squawk_domain::{
    AccountId, DataQuality, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, InstrumentId, MetadataRevision, ResearchObservation, RevisionNumber,
    SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{LocalAuthorityStateStore, SecretReference};
use market_squawk_sources::{
    AvailabilityEvidence, DiscoveryRequest, ExtractionBatch, ExtractionRecord, ExtractionRequest,
    SourceObject,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

type TestResult = Result<(), Box<dyn Error>>;

const FIXTURE: &[u8] = include_bytes!("../fixtures/manifest.json");
const SOURCE_ID: &str = "portfolio-local-fixture";
const METADATA_REVISION: &str = "portfolio-manifest-v1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureManifest {
    records: Vec<FixtureRecord>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureRecord {
    revision: String,
    payload: String,
}

#[test]
fn import_preserves_exact_records_normalizes_typed_portfolio_and_replays_for_data() -> TestResult {
    let fixture: FixtureManifest = serde_json::from_slice(FIXTURE)?;
    let batch = batch(&fixture.records, "portfolio-statement")?;
    let credential = SecretReference::try_from("keyring:brokerage-account-token")?;
    let archive = tempfile::tempdir()?;
    let mut source = PortfolioExtractionSource::try_new(
        SourceId::try_from(SOURCE_ID)?,
        MetadataRevision::new(SourceIdentifier::try_from(METADATA_REVISION)?),
        DataQuality::DirectUnverified,
        LocalAuthorityStateStore::try_open(archive.path())?,
        Some(credential),
        PortfolioImportLimits::standard(),
    )?;

    let imported = source.import_batch(&batch)?;
    assert_eq!(imported.disposition(), ImportDisposition::Applied);
    assert_eq!(imported.raw_records().len(), fixture.records.len());
    assert_eq!(source.raw_records().len(), fixture.records.len());
    assert_eq!(imported.accounts().len(), 2);
    assert_eq!(imported.holdings().len(), 4);
    assert_eq!(imported.transactions().len(), 5);
    assert_eq!(imported.cash_flows().len(), 3);
    assert_eq!(imported.cost_bases().len(), 2);
    assert_eq!(imported.supplied_totals().len(), 2);
    assert!(imported.discrepancies().is_empty());

    let first = &imported.raw_records()[0];
    assert_eq!(
        first.bytes().as_ref(),
        fixture.records[0].payload.as_bytes()
    );
    assert_eq!(
        first.payload_hash(),
        EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(fixture.records[0].payload.as_bytes()).into(),
        )
    );

    assert_eq!(
        imported.accounts()[0].account_id(),
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse::<AccountId>()?
    );
    assert_eq!(imported.accounts()[0].currency().as_str(), "USD");
    assert_eq!(imported.accounts()[0].as_of().unix_nanos(), 100);
    assert_eq!(
        imported.holdings()[0].instrument_id(),
        "11111111-1111-4111-8111-111111111111".parse::<InstrumentId>()?
    );
    assert_eq!(imported.holdings()[0].quantity().to_string(), "2.5");
    assert_eq!(imported.holdings()[1].quantity().to_string(), "-1");
    assert!(matches!(
        imported.holdings()[2].basis(),
        BasisResolution::Missing
    ));
    assert!(matches!(
        imported.holdings()[3].basis(),
        BasisResolution::Ambiguous { candidates, lot_method }
            if candidates.len() == 2 && *lot_method == LotMethod::SpecificIdentification
    ));
    assert_eq!(imported.cost_bases()[0].lot_method(), LotMethod::Fifo);
    assert_eq!(imported.cost_bases()[1].lot_method(), LotMethod::Lifo);
    assert_eq!(
        imported
            .transactions()
            .iter()
            .map(|transaction| transaction.kind())
            .collect::<Vec<_>>(),
        [
            TransactionKind::Trade,
            TransactionKind::CashTransfer,
            TransactionKind::Income,
            TransactionKind::Fee,
            TransactionKind::CorporateAction,
        ]
    );
    assert_eq!(
        imported.transactions()[0]
            .quantity()
            .ok_or("trade quantity absent")?
            .to_string(),
        "-0.5"
    );
    assert_eq!(
        imported.transactions()[0].lot_method(),
        Some(LotMethod::Fifo)
    );

    let data_batch = ResearchArrowBatch::try_from_extraction_batch(imported.normalized_batch())?;
    assert_eq!(
        data_batch.record_batch().num_rows(),
        imported.normalized_batch().records().len()
    );
    let normalized_digest = extraction_batch_digest(imported.normalized_batch())?;
    drop(imported);

    let replayed = source.import_batch(&batch)?;
    assert_eq!(replayed.disposition(), ImportDisposition::Replay);
    assert_eq!(source.raw_records().len(), fixture.records.len());
    assert_eq!(
        extraction_batch_digest(replayed.normalized_batch())?,
        normalized_digest
    );

    let debug = format!("{source:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("brokerage-account-token"));
    assert!(!debug.contains("account-taxable"));

    drop(replayed);
    drop(source);
    let mut restarted = PortfolioExtractionSource::try_new(
        SourceId::try_from(SOURCE_ID)?,
        MetadataRevision::new(SourceIdentifier::try_from(METADATA_REVISION)?),
        DataQuality::DirectUnverified,
        LocalAuthorityStateStore::try_open(archive.path())?,
        None,
        PortfolioImportLimits::standard(),
    )?;
    assert_eq!(restarted.raw_records().len(), fixture.records.len());
    let restarted_replay = restarted.import_batch(&batch)?;
    assert_eq!(restarted_replay.disposition(), ImportDisposition::Replay);
    assert_eq!(
        extraction_batch_digest(restarted_replay.normalized_batch())?,
        normalized_digest
    );
    Ok(())
}

#[test]
fn duplicate_broker_ids_fail_after_raw_archive_and_corrections_supersede_without_deletion()
-> TestResult {
    let unbound_archive = tempfile::tempdir()?;
    let mut source = open_source(unbound_archive.path())?;
    let unbound = [raw_transaction(
        "unbound-record",
        None,
        "unbound-fitid",
        "9.00",
    )];
    let unbound = batch(&unbound, "unbound-statement")?;
    assert!(matches!(
        source.import_batch(&unbound),
        Err(market_squawk_adapter_portfolio::PortfolioImportError::AccountMismatch)
    ));
    assert_eq!(source.raw_records().len(), 1);

    let duplicate_archive = tempfile::tempdir()?;
    let mut source = open_source(duplicate_archive.path())?;
    let duplicate = [
        raw_account(),
        raw_transaction("record-a", None, "duplicate-fitid", "10.00"),
        raw_transaction("record-b", None, "duplicate-fitid", "11.00"),
    ];
    let duplicate = batch(&duplicate, "duplicate-statement")?;
    assert!(matches!(
        source.import_batch(&duplicate),
        Err(market_squawk_adapter_portfolio::PortfolioImportError::DuplicateBrokerTransactionId)
    ));
    assert_eq!(source.raw_records().len(), 3);

    let correction_archive = tempfile::tempdir()?;
    let mut source = open_source(correction_archive.path())?;
    let original = [
        raw_account(),
        raw_transaction("corrected-record", None, "correct-fitid", "10.00"),
    ];
    let original_batch = batch(&original, "corrected-statement")?;
    let imported = source.import_batch(&original_batch)?;
    let original_reference = imported.raw_records()[1].source_reference().clone();
    let stable_record_id = SourceIdentifier::try_from("corrected-record")?;
    let revision_one = RevisionNumber::new(1)?;
    let original_lineage = transaction_lineage(imported.normalized_batch())?;
    assert_eq!(original_lineage.len(), 2);
    assert!(
        original_lineage
            .iter()
            .all(|(source_identifier, revision)| {
                source_identifier == &stable_record_id && *revision == revision_one
            })
    );
    drop(imported);

    let correction = [FixtureRecord {
        revision: "statement-2".to_owned(),
        payload: raw_transaction_payload(
            "corrected-record",
            Some("statement-1"),
            "correct-fitid",
            "10.25",
            2,
        ),
    }];
    let correction_batch = batch(&correction, "corrected-statement")?;
    let corrected = source.import_batch(&correction_batch)?;
    let revision_two = RevisionNumber::new(2)?;
    assert_eq!(corrected.disposition(), ImportDisposition::Applied);
    assert_eq!(source.raw_records().len(), 3);
    assert!(source.is_superseded(&original_reference));
    let active = source
        .active_record(&SourceIdentifier::try_from("corrected-record")?)
        .ok_or("corrected record absent")?;
    assert_ne!(active.source_reference(), &original_reference);
    assert_eq!(active.revision_number(), revision_two);
    assert_eq!(
        corrected.transactions()[0].amount().amount().to_string(),
        "10.25"
    );
    let corrected_lineage = transaction_lineage(corrected.normalized_batch())?;
    assert_eq!(corrected_lineage.len(), 2);
    assert!(
        corrected_lineage
            .iter()
            .all(|(source_identifier, revision)| {
                source_identifier == &stable_record_id && *revision == revision_two
            })
    );
    drop(corrected);

    let non_increasing = [FixtureRecord {
        revision: "statement-3".to_owned(),
        payload: raw_transaction_payload(
            "corrected-record",
            Some("statement-2"),
            "correct-fitid",
            "10.50",
            2,
        ),
    }];
    let non_increasing_batch = batch(&non_increasing, "non-increasing-correction")?;
    assert!(matches!(
        source.import_batch(&non_increasing_batch),
        Err(market_squawk_adapter_portfolio::PortfolioImportError::NonIncreasingRevision)
    ));
    assert_eq!(source.raw_records().len(), 4);

    let incompatible_account_correction = [FixtureRecord {
        revision: "statement-2".to_owned(),
        payload: raw_account_payload(Some("statement-1"), "EUR", 2),
    }];
    let incompatible_account_batch = batch(
        &incompatible_account_correction,
        "incompatible-account-correction",
    )?;
    assert!(matches!(
        source.import_batch(&incompatible_account_batch),
        Err(market_squawk_adapter_portfolio::PortfolioImportError::CurrencyMismatch)
    ));
    assert_eq!(source.raw_records().len(), 5);
    Ok(())
}

fn observation_revision(observation: &ResearchObservation) -> RevisionNumber {
    match observation {
        ResearchObservation::Filing(value) => value.context().time().revision(),
        ResearchObservation::Fundamental(value) => value.context().time().revision(),
        ResearchObservation::Macro(value) => value.context().time().revision(),
        ResearchObservation::PortfolioPosition(value) => value.context().time().revision(),
        ResearchObservation::Transaction(value) => value.context().time().revision(),
        ResearchObservation::CorporateAction(value) => value.context().time().revision(),
        ResearchObservation::AlternativeData(value) => value.context().time().revision(),
    }
}

fn transaction_lineage(
    batch: &ExtractionBatch,
) -> Result<Vec<(SourceIdentifier, RevisionNumber)>, Box<dyn Error>> {
    let observations = batch
        .records()
        .iter()
        .map(|record| serde_json::from_slice::<ResearchObservation>(record.payload()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(observations
        .into_iter()
        .filter_map(|observation| match &observation {
            ResearchObservation::Transaction(value) => Some((
                value.context().provenance().source_identifier().clone(),
                observation_revision(&observation),
            )),
            ResearchObservation::AlternativeData(value)
                if value.dataset().as_str() == "portfolio-transactions" =>
            {
                Some((
                    value.context().provenance().source_identifier().clone(),
                    observation_revision(&observation),
                ))
            }
            _ => None,
        })
        .collect())
}

fn open_source(archive: &Path) -> Result<PortfolioExtractionSource, Box<dyn Error>> {
    Ok(PortfolioExtractionSource::try_new(
        SourceId::try_from(SOURCE_ID)?,
        MetadataRevision::new(SourceIdentifier::try_from(METADATA_REVISION)?),
        DataQuality::DirectUnverified,
        LocalAuthorityStateStore::try_open(archive)?,
        None,
        PortfolioImportLimits::standard(),
    )?)
}

fn raw_transaction(
    record_id: &str,
    supersedes_revision: Option<&str>,
    broker_id: &str,
    amount: &str,
) -> FixtureRecord {
    FixtureRecord {
        revision: "statement-1".to_owned(),
        payload: raw_transaction_payload(record_id, supersedes_revision, broker_id, amount, 1),
    }
}

fn raw_account() -> FixtureRecord {
    FixtureRecord {
        revision: "statement-1".to_owned(),
        payload: raw_account_payload(None, "USD", 1),
    }
}

fn raw_account_payload(
    supersedes_revision: Option<&str>,
    currency: &str,
    revision_number: u32,
) -> String {
    let supersedes = supersedes_revision.map_or_else(String::new, |revision| {
        format!("\"supersedes_revision\":\"{revision}\",")
    });
    format!(
        "{{\"record_id\":\"account-authority\",{supersedes}\"revision_number\":{revision_number},\"received_at_unix_nanos\":\"103\",\"ingested_at_unix_nanos\":\"104\",\"record\":{{\"kind\":\"account\",\"account_id\":\"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\",\"currency\":\"{currency}\",\"cash_balance\":\"1000.00\",\"as_of_unix_nanos\":\"100\"}}}}"
    )
}

fn raw_transaction_payload(
    record_id: &str,
    supersedes_revision: Option<&str>,
    broker_id: &str,
    amount: &str,
    revision_number: u32,
) -> String {
    let supersedes =
        supersedes_revision.map_or_else(|| "null".to_owned(), |revision| format!("\"{revision}\""));
    format!(
        "{{\"record_id\":\"{record_id}\",\"supersedes_revision\":{supersedes},\"revision_number\":{revision_number},\"received_at_unix_nanos\":\"103\",\"ingested_at_unix_nanos\":\"104\",\"record\":{{\"kind\":\"transaction\",\"broker_transaction_id\":\"{broker_id}\",\"account_id\":\"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\",\"instrument_id\":null,\"currency\":\"USD\",\"transaction_type\":\"cash_transfer\",\"amount\":\"{amount}\",\"quantity\":null,\"occurred_at_unix_nanos\":\"99\",\"lot_method\":null}}}}"
    )
}

fn batch(records: &[FixtureRecord], object_id: &str) -> Result<ExtractionBatch, Box<dyn Error>> {
    let source_id = SourceId::try_from(SOURCE_ID)?;
    let metadata_revision = MetadataRevision::new(SourceIdentifier::try_from(METADATA_REVISION)?);
    let dataset = SourceIdentifier::try_from("portfolio-records")?;
    let discovery = DiscoveryRequest::try_new(
        dataset,
        None,
        NonZeroU16::new(1).ok_or("zero discovery count")?,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let object_bytes = records
        .iter()
        .flat_map(|record| record.payload.as_bytes())
        .copied()
        .collect::<Vec<_>>();
    let object_evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(&object_bytes).into(),
    ));
    let object = SourceObject::try_new_with_availability(
        source_id,
        metadata_revision,
        &discovery,
        SourceIdentifier::try_from(object_id)?,
        SourceIdentifier::try_from("application-market-squawk-portfolio-records-json")?,
        object_evidence,
        EffectiveInterval::new(Timestamp::from_unix_nanos(100), None)?,
        Some(Timestamp::from_unix_nanos(101)),
        AvailabilityEvidence::Observed {
            available_at: Timestamp::from_unix_nanos(102),
            evidence: SourceIdentifier::try_from("local-file-first-observed")?,
        },
        Some(u64::try_from(object_bytes.len())?),
    )?;
    let request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(256).ok_or("zero record count")?,
        NonZeroU64::new(8 * 1024 * 1024).ok_or("zero byte count")?,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let extracted = records
        .iter()
        .map(|record| {
            let payload = Bytes::copy_from_slice(record.payload.as_bytes());
            let evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(&payload).into(),
            ));
            ExtractionRecord::try_new(
                &request,
                SourceIdentifier::try_from("market-squawk-portfolio-raw-v1")?,
                evidence,
                Timestamp::from_unix_nanos(100),
                Some(Timestamp::from_unix_nanos(101)),
                AvailabilityEvidence::Observed {
                    available_at: Timestamp::from_unix_nanos(102),
                    evidence: SourceIdentifier::try_from("local-file-first-observed")?,
                },
                SourceIdentifier::try_from(record.revision.clone())?,
                None,
                payload,
            )
            .map_err(Into::into)
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(ExtractionBatch::try_new(&request, extracted)?)
}
