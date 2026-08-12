use std::error::Error;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_domain::{
    CalendarDate, Currency, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence,
    FundNavCompleteness, FundNavDisposition, FundNavMissingState, FundNavValue, InstrumentId,
    MetadataRevision, ProviderInstrumentId, ResearchObservation, RevisionNumber, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_platform::{LocalPaths, RawCaptureRecord, SealedResearchJournalStore};
use market_squawk_sources::{
    ProviderCaptureMaterial, ProviderCapturePageReceipt, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{
    TIINGO_APPLICATION_BYTES_PER_MONTH, TiingoAdapterError, TiingoDecoder, TiingoFundContext,
    TiingoFundNavContractEvidence, TiingoFundNavMappingInput, TiingoFundNavRevisionLinks,
    TiingoFundSupport, TiingoNavValueState, TiingoQuotaAdmission, TiingoQuotaLedger,
    TiingoQuotaWindows, TiingoRequestSpec, TiingoResponseEvidence, TiingoTicker,
    classify_fund_support, map_fund_nav, normalize_mutual_fund_row, unavailable_nav_candidate,
};

fn identifier(value: &str) -> Result<SourceIdentifier, Box<dyn Error>> {
    Ok(SourceIdentifier::try_from(value)?)
}

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("market-squawk-tiingo-{}", Uuid::new_v4())))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ignored = std::fs::remove_dir_all(&self.0);
    }
}

fn date(year: u16, month: u8, day: u8) -> Result<CalendarDate, Box<dyn Error>> {
    Ok(CalendarDate::new(year, month, day)?)
}

fn context(ticker: &TiingoTicker) -> Result<TiingoFundContext, Box<dyn Error>> {
    Ok(TiingoFundContext::try_new(
        "06dd06da-ef2d-44dd-bf28-b006da06b24b".parse::<InstrumentId>()?,
        ProviderInstrumentId::try_from(ticker.as_str())?,
        ticker.clone(),
        identifier("instrument-revision-7")?,
        identifier("mutual-fund-share-class-revision-3")?,
        identifier("tiingo-entitlement-generation-11")?,
        identifier("tiingo-daily-native-v1")?,
        Currency::try_from("USD")?,
    )?)
}

fn digest(value: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
}

fn contract() -> Result<TiingoFundNavContractEvidence, Box<dyn Error>> {
    Ok(TiingoFundNavContractEvidence::new(
        SourceId::try_from("tiingo-starter")?,
        MetadataRevision::new(identifier("tiingo-source-metadata-v1")?),
        ExactPayloadEvidence::from_content_digest(digest(b"tiingo-source-contract-v1")),
        ExactPayloadEvidence::from_content_digest(digest(b"tiingo-daily-native-v1")),
        NonZeroU64::new(11).ok_or("nonzero entitlement fixture")?,
        digest(b"tiingo-entitlement-generation-11"),
    ))
}

fn seal_response(
    body: &[u8],
    evidence: &TiingoResponseEvidence,
    contract: &TiingoFundNavContractEvidence,
    store: &SealedResearchJournalStore,
) -> Result<SealedProviderCaptureSetReceipt, Box<dyn Error>> {
    let request_identity = evidence.request().request_identity();
    let page = ProviderCapturePageReceipt::try_new(
        0,
        request_identity,
        None,
        None,
        evidence.status(),
        u64::try_from(body.len())?,
        evidence.body_digest(),
        evidence.received_at(),
    )?;
    let receipt = ProviderCaptureSetReceipt::try_new(
        contract.source_id().clone(),
        contract.source_contract_revision().clone(),
        identifier("tiingo.daily-prices")?,
        request_identity,
        ProviderCaptureTerminalDisposition::StandaloneResponse,
        vec![page],
    )?;
    let connection_id = Uuid::from_u128(2);
    let record = RawCaptureRecord::try_new_live(
        Uuid::new_v5(&connection_id, &evidence.body_digest().bytes()),
        Arc::from(contract.source_id().as_str()),
        connection_id,
        Some(0),
        None,
        DateTime::<Utc>::from_timestamp_nanos(evidence.received_at().unix_nanos()),
        Bytes::copy_from_slice(body),
    )?;
    Ok(ProviderCaptureMaterial::try_new(receipt, vec![record])?.seal(store)?)
}

#[test]
fn mutual_fund_nav_maps_exactly_and_defers_revision_authority() -> Result<(), Box<dyn Error>> {
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path())?;
    let store = paths.sealed_research_journal_store()?;
    let contract = contract()?;
    let ticker = TiingoTicker::try_new("VTSAX")?;
    let mut decoder = TiingoDecoder::new(identifier("tiingo-daily-native-v1")?);
    let metadata = decoder.decode_metadata(
        TiingoRequestSpec::metadata(ticker.clone())?,
        200,
        br#"{"ticker":"VTSAX","name":"Vanguard Total Stock Market Index Fund Admiral Shares","exchangeCode":"MF","description":"Mutual fund","startDate":"2000-11-13","endDate":"2026-08-10"}"#,
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(101),
    )?;
    let body = br#"[{"date":"2026-08-10T00:00:00.000Z","open":151.2300,"high":151.2300,"low":151.2300,"close":151.2300,"volume":0,"adjOpen":150.00,"adjHigh":150.00,"adjLow":150.00,"adjClose":150.00,"adjVolume":0,"divCash":0.01,"splitFactor":1}]"#;
    let response = decoder.decode_eod(
        TiingoRequestSpec::latest(ticker.clone())?,
        200,
        body,
        Timestamp::from_unix_nanos(200),
        Timestamp::from_unix_nanos(201),
    )?;

    let candidate =
        normalize_mutual_fund_row(context(&ticker)?, &metadata, &response, &response.rows()[0])?;
    let TiingoNavValueState::Observed(nav) = candidate.value() else {
        return Err("expected an observed NAV".into());
    };
    assert_eq!(nav.amount().to_string(), "151.23");
    assert_eq!(nav.currency().as_str(), "USD");
    assert_eq!(candidate.nav_date(), date(2026, 8, 10)?);
    assert_ne!(response.rows()[0].adjusted_ohlc().3, Some(nav.amount()));
    let sealed = seal_response(body, response.evidence(), &contract, &store)?;
    let mapped = map_fund_nav(TiingoFundNavMappingInput {
        candidate: &candidate,
        sealed_capture: &sealed,
        contract: &contract,
        authority_seed_revision: RevisionNumber::new(1)?,
        ingested_at: Timestamp::from_unix_nanos(202),
        canonical_published_at: Timestamp::from_unix_nanos(203),
        revision_links: TiingoFundNavRevisionLinks::default(),
    })?;
    assert_eq!(mapped.observation().value(), FundNavValue::Observed(nav));
    assert_eq!(
        mapped.observation().lineage().completeness(),
        FundNavCompleteness::Complete
    );
    assert_eq!(
        mapped.observation().lineage().disposition(),
        FundNavDisposition::Returned
    );
    assert!(mapped.observation().context().time().published().is_none());
    let (observations, revisions) = mapped.into_revision_authority_input()?;
    assert!(revisions.is_locally_observed());
    assert!(matches!(
        observations.as_slice(),
        [ResearchObservation::FundNav(_)]
    ));
    Ok(())
}

#[test]
fn unsupported_or_missing_fund_stays_explicit_and_quota_is_conjunctive()
-> Result<(), Box<dyn Error>> {
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path())?;
    let store = paths.sealed_research_journal_store()?;
    let contract = contract()?;
    let ticker = TiingoTicker::try_new("NOHISTORYX")?;
    let mut decoder = TiingoDecoder::new(identifier("tiingo-daily-native-v1")?);
    let metadata = decoder.decode_metadata(
        TiingoRequestSpec::metadata(ticker.clone())?,
        200,
        br#"{"ticker":"NOHISTORYX","name":"Reserved symbol","exchangeCode":"N/A","description":null,"startDate":null,"endDate":null}"#,
        Timestamp::from_unix_nanos(20),
        Timestamp::from_unix_nanos(21),
    )?;
    assert_eq!(
        classify_fund_support(&metadata),
        TiingoFundSupport::Unsupported
    );
    let empty_body = b"[]";
    let response = decoder.decode_eod(
        TiingoRequestSpec::latest(ticker.clone())?,
        200,
        empty_body,
        Timestamp::from_unix_nanos(30),
        Timestamp::from_unix_nanos(31),
    )?;
    assert_eq!(response.disposition().returned_symbols(), 0);
    assert_eq!(response.disposition().missing_symbols(), 1);
    let unavailable = unavailable_nav_candidate(
        context(&ticker)?,
        date(2026, 8, 10)?,
        TiingoNavValueState::Unsupported,
        response.evidence(),
        response.disposition(),
    )?;
    assert_eq!(unavailable.value(), TiingoNavValueState::Unsupported);
    assert!(unavailable.provider_row_digest().is_none());
    let sealed = seal_response(empty_body, response.evidence(), &contract, &store)?;
    let mapped = map_fund_nav(TiingoFundNavMappingInput {
        candidate: &unavailable,
        sealed_capture: &sealed,
        contract: &contract,
        authority_seed_revision: RevisionNumber::new(1)?,
        ingested_at: Timestamp::from_unix_nanos(32),
        canonical_published_at: Timestamp::from_unix_nanos(33),
        revision_links: TiingoFundNavRevisionLinks::default(),
    })?;
    assert_eq!(
        mapped.observation().value(),
        FundNavValue::Missing(FundNavMissingState::Unsupported)
    );

    let incomplete = unavailable_nav_candidate(
        context(&ticker)?,
        date(2026, 8, 10)?,
        TiingoNavValueState::Unavailable,
        response.evidence(),
        response.disposition(),
    )?;
    let mapped = map_fund_nav(TiingoFundNavMappingInput {
        candidate: &incomplete,
        sealed_capture: &sealed,
        contract: &contract,
        authority_seed_revision: RevisionNumber::new(1)?,
        ingested_at: Timestamp::from_unix_nanos(32),
        canonical_published_at: Timestamp::from_unix_nanos(33),
        revision_links: TiingoFundNavRevisionLinks::default(),
    })?;
    assert_eq!(
        mapped.observation().value(),
        FundNavValue::Missing(FundNavMissingState::Unavailable)
    );
    assert_eq!(
        mapped.observation().lineage().completeness(),
        FundNavCompleteness::Incomplete
    );

    let provider_refusal = decoder.decode_eod(
        TiingoRequestSpec::latest(ticker.clone())?,
        429,
        br#"{"detail":"rate limit"}"#,
        Timestamp::from_unix_nanos(40),
        Timestamp::from_unix_nanos(41),
    );
    assert!(matches!(
        provider_refusal,
        Err(TiingoAdapterError::Provider(_))
    ));

    let equity_ticker = TiingoTicker::try_new("AAPL")?;
    let equity_metadata = decoder.decode_metadata(
        TiingoRequestSpec::metadata(equity_ticker.clone())?,
        200,
        br#"{"ticker":"AAPL","name":"Apple Inc.","exchangeCode":"NASDAQ","description":"Equity","startDate":"1980-12-12","endDate":"2026-08-10"}"#,
        Timestamp::from_unix_nanos(50),
        Timestamp::from_unix_nanos(51),
    )?;
    let equity_body = br#"[{"date":"2026-08-10T00:00:00.000Z","open":200,"high":201,"low":199,"close":200,"volume":100,"adjOpen":200,"adjHigh":201,"adjLow":199,"adjClose":200,"adjVolume":100,"divCash":0,"splitFactor":1}]"#;
    let equity_response = decoder.decode_eod(
        TiingoRequestSpec::latest(equity_ticker.clone())?,
        200,
        equity_body,
        Timestamp::from_unix_nanos(52),
        Timestamp::from_unix_nanos(53),
    )?;
    assert!(matches!(
        normalize_mutual_fund_row(
            context(&equity_ticker)?,
            &equity_metadata,
            &equity_response,
            &equity_response.rows()[0],
        ),
        Err(TiingoAdapterError::InvalidFundContext)
    ));

    let windows = TiingoQuotaWindows::try_new(
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(200),
        Timestamp::from_unix_nanos(300),
        Timestamp::from_unix_nanos(400),
    )?;
    let mut ledger = TiingoQuotaLedger::new(windows);
    let oversized = NonZeroU64::new(TIINGO_APPLICATION_BYTES_PER_MONTH + 1)
        .ok_or("expected nonzero bandwidth bound")?;
    assert_eq!(
        ledger.classify(&ticker, oversized)?,
        TiingoQuotaAdmission::MonthlyBandwidthExhausted
    );
    let reservation = NonZeroU64::new(64).ok_or("expected nonzero reservation")?;
    let Ok(permit) = ledger.reserve(ticker.clone(), reservation)? else {
        return Err("unexpected quota denial".into());
    };
    ledger.commit_response(&permit, &ticker, 32)?;
    assert_eq!(ledger.snapshot().requests_this_hour(), 1);
    assert_eq!(ledger.snapshot().requests_this_day(), 1);
    assert_eq!(ledger.snapshot().response_bytes_this_month(), 32);
    assert!(
        ledger
            .snapshot()
            .unique_symbols_this_month()
            .contains(&ticker)
    );
    Ok(())
}
