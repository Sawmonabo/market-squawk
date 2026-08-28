use std::error::Error;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_domain::{
    BarTimeSemantics, BarTimestampBasis, CalendarDate, Currency, DigestAlgorithm, EvidenceDigest,
    ExactPayloadEvidence, FundNavCompleteness, FundNavDisposition, FundNavMissingState,
    FundNavValue, InstrumentId, MarketBarAdjustment, MarketBarSessionEvidence,
    MarketBarSessionKind, MetadataRevision, ProviderInstrumentId, RevisionBoundPayloadEvidence,
    SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_platform::{LocalPaths, RawCaptureRecord, SealedResearchJournalStore};
use market_squawk_sources::{
    ProviderCaptureMaterial, ProviderCapturePageReceipt, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{
    TIINGO_APPLICATION_BYTES_PER_MONTH, TiingoAdapterError, TiingoCompletedEodHistoryCandidate,
    TiingoCompletedFundNavHistoryCandidate, TiingoCompletedHistoryCapture, TiingoDecoder,
    TiingoEodBarTimeAuthority, TiingoEodBarTimeRequest, TiingoEodContractEvidence,
    TiingoEodExpectedSessionAuthority, TiingoEodExpectedSessionEvidence,
    TiingoEodExpectedSessionRequest, TiingoEodExpectedSessionValidationReceipt,
    TiingoEodFinancialCoverageDisposition, TiingoEodInstrumentAuthority, TiingoEodInstrumentKind,
    TiingoEodMapError, TiingoEodMappingInput, TiingoEodPagePublicationRoute, TiingoFundContext,
    TiingoFundNavContractEvidence, TiingoFundNavHistoryFinancialCoverage,
    TiingoFundNavMappingInput, TiingoFundSupport, TiingoHistoryCheckpointReceipt,
    TiingoHistoryPlan, TiingoHistoryTerminalDisposition, TiingoNavValueState,
    TiingoProviderAuthorityInstallation, TiingoProviderAuthorityRequirements,
    TiingoProviderRevisionEvidence, TiingoQuotaAdmission, TiingoQuotaLedger, TiingoQuotaWindows,
    TiingoRequestSpec, TiingoResponseEvidence, TiingoSealedHistoryPage,
    TiingoSourcePublicationEvidence, TiingoTicker, classify_fund_support, map_eod_page_candidate,
    map_fund_nav_candidate, missing_nav_candidate, normalize_mutual_fund_row,
    tiingo_provider_rate_declaration,
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
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(identifier("instrument-revision-7")?),
            ExactPayloadEvidence::from_content_digest(digest(b"instrument-revision-7")),
        ),
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(identifier("mutual-fund-share-class-revision-3")?),
            ExactPayloadEvidence::from_content_digest(digest(
                b"mutual-fund-share-class-revision-3",
            )),
        ),
        identifier("tiingo-entitlement-generation-11")?,
        identifier("tiingo-daily-native-v1")?,
        Timestamp::from_unix_nanos(19),
        Currency::try_from("USD")?,
    )?)
}

fn digest(value: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
}

fn contract() -> Result<TiingoFundNavContractEvidence, Box<dyn Error>> {
    Ok(TiingoFundNavContractEvidence::try_new(
        SourceId::try_from("tiingo-starter")?,
        MetadataRevision::new(identifier("tiingo-source-metadata-v1")?),
        ExactPayloadEvidence::from_content_digest(digest(b"tiingo-source-contract-v1")),
        identifier("tiingo-daily-native-v1")?,
        ExactPayloadEvidence::from_content_digest(digest(b"tiingo-daily-native-v1")),
        NonZeroU64::new(11).ok_or("nonzero entitlement fixture")?,
        identifier("tiingo-entitlement-generation-11")?,
        digest(b"tiingo-entitlement-generation-11"),
    )?)
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
    let dataset = match evidence.request().endpoint() {
        crate::TiingoEndpointFamily::Metadata => "tiingo-daily-metadata",
        crate::TiingoEndpointFamily::LatestDailyPrices => "tiingo-daily-latest",
        crate::TiingoEndpointFamily::HistoricalDailyPrices => "tiingo-daily-history-window",
    };
    let receipt = ProviderCaptureSetReceipt::try_new(
        contract.source_id().clone(),
        contract.source_contract_revision().clone(),
        identifier(dataset)?,
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

fn completed_single_page_history(
    plan: TiingoHistoryPlan,
    response: &crate::TiingoEodReceipt,
    sealed_capture: &SealedProviderCaptureSetReceipt,
    contract: &TiingoFundNavContractEvidence,
) -> Result<TiingoCompletedHistoryCapture, Box<dyn Error>> {
    if plan.pages().len() != 1 {
        return Err("expected one Tiingo history page in the focused fixture".into());
    }
    let page = TiingoSealedHistoryPage::try_new(&plan.pages()[0], response, sealed_capture)?;
    let requirements = TiingoProviderAuthorityRequirements::new(
        tiingo_provider_rate_declaration()?,
        contract.source_id().clone(),
        contract.source_contract_revision().clone(),
        contract.native_schema_revision().clone(),
        contract.entitlement_generation_identity().clone(),
    );
    let authority_generation = identifier("fixture-tiingo-history-authority-1")?;
    let installation = TiingoProviderAuthorityInstallation::try_new(
        &requirements,
        authority_generation.clone(),
        identifier("fixture-tiingo-history-store-1")?,
        digest(b"fixture-tiingo-history-installation"),
        Timestamp::from_unix_nanos(1),
    )?;
    let checkpoint = TiingoHistoryCheckpointReceipt::try_new(
        &plan,
        1,
        Some(page.page_identity()),
        authority_generation,
        installation.installation_identity(),
        digest(b"fixture-tiingo-terminal-history-checkpoint"),
        Timestamp::from_unix_nanos(1_000),
    )?;
    Ok(TiingoCompletedHistoryCapture::try_new(
        plan,
        vec![page],
        &checkpoint,
        &installation,
    )?)
}

struct FixedEodTimeAuthority;

impl TiingoEodBarTimeAuthority for FixedEodTimeAuthority {
    fn validate_current(&self) -> Result<(), TiingoEodMapError> {
        Ok(())
    }

    fn resolve(
        &self,
        request: &TiingoEodBarTimeRequest,
    ) -> Result<BarTimeSemantics, TiingoEodMapError> {
        if request.provider_date()
            != CalendarDate::new(2026, 8, 10)
                .map_err(|_| TiingoEodMapError::InvalidTimeAuthority)?
        {
            return Err(TiingoEodMapError::InvalidTimeAuthority);
        }
        let session = MarketBarSessionEvidence::try_new(
            MarketBarSessionKind::Regular,
            SourceIdentifier::try_from("xnas-session-calendar-v1")
                .map_err(|_| TiingoEodMapError::InvalidTimeAuthority)?,
            digest(b"xnas-session-calendar-v1"),
        )
        .map_err(|_| TiingoEodMapError::InvalidTimeAuthority)?;
        BarTimeSemantics::try_new(
            Timestamp::from_unix_nanos(40),
            Timestamp::from_unix_nanos(50),
            BarTimestampBasis::PeriodEnd,
            session,
        )
        .map_err(|_| TiingoEodMapError::InvalidTimeAuthority)
    }
}

struct FixedEodExpectedSessionAuthority;

impl TiingoEodExpectedSessionAuthority for FixedEodExpectedSessionAuthority {
    fn resolve_expected_sessions(
        &self,
        request: &TiingoEodExpectedSessionRequest,
    ) -> Result<TiingoEodExpectedSessionEvidence, TiingoEodMapError> {
        let expected_date = CalendarDate::new(2026, 8, 10)
            .map_err(|_| TiingoEodMapError::InvalidExpectedSessionEvidence)?;
        if request.start_date() != expected_date
            || request.end_date() != expected_date
            || request.venue_id().as_str() != "xnas"
            || request.ticker().as_str() != "AAPL"
        {
            return Err(TiingoEodMapError::InvalidExpectedSessionEvidence);
        }
        TiingoEodExpectedSessionEvidence::try_new(
            request,
            SourceIdentifier::try_from("xnas-expected-sessions")
                .map_err(|_| TiingoEodMapError::InvalidExpectedSessionEvidence)?,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(
                    SourceIdentifier::try_from("xnas-session-calendar-v1")
                        .map_err(|_| TiingoEodMapError::InvalidExpectedSessionEvidence)?,
                ),
                ExactPayloadEvidence::from_content_digest(digest(b"xnas-session-calendar-v1")),
            ),
            SourceIdentifier::try_from("xnas-calendar-authority-generation-7")
                .map_err(|_| TiingoEodMapError::InvalidExpectedSessionEvidence)?,
            Timestamp::from_unix_nanos(54),
            Timestamp::from_unix_nanos(55),
            digest(b"xnas-session-resolution-receipt-7"),
            vec![expected_date],
        )
    }

    fn validate_current(
        &self,
        evidence: &TiingoEodExpectedSessionEvidence,
    ) -> Result<TiingoEodExpectedSessionValidationReceipt, TiingoEodMapError> {
        if evidence.calendar_id().as_str() == "xnas-expected-sessions"
            && evidence.expected_sessions()
                == [CalendarDate::new(2026, 8, 10)
                    .map_err(|_| TiingoEodMapError::InvalidExpectedSessionEvidence)?]
        {
            TiingoEodExpectedSessionValidationReceipt::try_new(
                evidence,
                SourceIdentifier::try_from("xnas-calendar-authority-generation-7")
                    .map_err(|_| TiingoEodMapError::InvalidExpectedSessionEvidence)?,
                Timestamp::from_unix_nanos(56),
                digest(b"xnas-session-validation-receipt-7"),
            )
        } else {
            Err(TiingoEodMapError::InvalidExpectedSessionEvidence)
        }
    }
}

#[test]
fn mutual_fund_nav_maps_exactly_and_defers_revision_authority() -> Result<(), Box<dyn Error>> {
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path())?;
    let store = paths.sealed_research_journal_store()?;
    let contract = contract()?;
    let ticker = TiingoTicker::try_new("VTSAX")?;
    let decoder = TiingoDecoder::new(
        identifier("tiingo-daily-native-v1")?,
        identifier("tiingo-entitlement-generation-11")?,
    );
    let metadata_body = br#"{"ticker":"VTSAX","name":"Vanguard Total Stock Market Index Fund Admiral Shares","exchangeCode":"MF","description":"Mutual fund","startDate":"2000-11-13","endDate":"2026-08-10"}"#;
    let metadata = decoder.decode_metadata(
        TiingoRequestSpec::metadata(ticker.clone())?,
        200,
        metadata_body,
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(101),
    )?;
    let body = br#"[{"date":"2026-08-10T00:00:00.000Z","open":151.2300,"high":151.2300,"low":151.2300,"close":151.2300,"volume":0,"adjOpen":150.00,"adjHigh":150.00,"adjLow":150.00,"adjClose":150.00,"adjVolume":0,"divCash":0.01,"splitFactor":1}]"#;
    let latest_response = decoder.decode_eod(
        TiingoRequestSpec::latest(ticker.clone())?,
        200,
        body,
        Timestamp::from_unix_nanos(200),
        Timestamp::from_unix_nanos(201),
    )?;

    let candidate = normalize_mutual_fund_row(context(&ticker)?, &metadata, &latest_response, 0)?;
    let TiingoNavValueState::Observed(nav) = candidate.value() else {
        return Err("expected an observed NAV".into());
    };
    assert_eq!(nav.amount().to_string(), "151.23");
    assert_eq!(nav.currency().as_str(), "USD");
    assert_eq!(candidate.nav_date(), date(2026, 8, 10)?);
    assert_ne!(
        latest_response.rows()[0].adjusted_ohlc().3,
        Some(nav.amount())
    );
    let sealed = seal_response(body, latest_response.evidence(), &contract, &store)?;
    let sealed_metadata = seal_response(metadata_body, metadata.evidence(), &contract, &store)?;
    let mapped = map_fund_nav_candidate(TiingoFundNavMappingInput {
        candidate: &candidate,
        sealed_capture: &sealed,
        completed_history: None,
        sealed_metadata_capture: &sealed_metadata,
        contract: &contract,
        ingested_at: Timestamp::from_unix_nanos(202),
    })?;
    let latest_pending = match mapped.try_into_latest_pending_publication() {
        Ok(pending) => pending,
        Err(_) => return Err("latest Tiingo NAV candidate retained history coordinates".into()),
    };
    let mapped = latest_pending.into_candidate();
    assert_eq!(
        (
            mapped.value(),
            mapped.lineage().completeness(),
            mapped.lineage().disposition(),
            mapped.nav_date(),
            mapped.sealed_capture_receipt(),
            mapped.sealed_metadata_capture_receipt(),
            mapped.response_request_identity(),
            mapped.provider_row_index(),
            mapped.provider_row_digest(),
            mapped.history_page_identity(),
            mapped.history_completion_identity(),
        ),
        (
            FundNavValue::Observed(nav),
            FundNavCompleteness::Complete,
            FundNavDisposition::Returned,
            date(2026, 8, 10)?,
            sealed.receipt_digest(),
            sealed_metadata.receipt_digest(),
            latest_response.evidence().request().request_identity(),
            Some(0),
            candidate.provider_row_digest(),
            None,
            None,
        )
    );
    assert_ne!(mapped.handoff_identity().bytes(), [0; 32]);

    let history_body = br#"[{"date":"2026-08-07T00:00:00.000Z","open":150.7500,"high":150.7500,"low":150.7500,"close":150.7500,"volume":0,"adjOpen":149.50,"adjHigh":149.50,"adjLow":149.50,"adjClose":149.50,"adjVolume":0,"divCash":0,"splitFactor":1},{"date":"2026-08-10T00:00:00.000Z","open":151.2300,"high":151.2300,"low":151.2300,"close":151.2300,"volume":0,"adjOpen":150.00,"adjHigh":150.00,"adjLow":150.00,"adjClose":150.00,"adjVolume":0,"divCash":0.01,"splitFactor":1}]"#;
    let history_plan =
        TiingoHistoryPlan::try_new(ticker.clone(), date(2026, 8, 7)?, date(2026, 8, 10)?)?;
    let history_response = decoder.decode_eod(
        history_plan.pages()[0].clone(),
        200,
        history_body,
        Timestamp::from_unix_nanos(203),
        Timestamp::from_unix_nanos(204),
    )?;
    let historical_first =
        normalize_mutual_fund_row(context(&ticker)?, &metadata, &history_response, 0)?;
    let historical_second =
        normalize_mutual_fund_row(context(&ticker)?, &metadata, &history_response, 1)?;
    let historical_sealed =
        seal_response(history_body, history_response.evidence(), &contract, &store)?;
    let completed_history = completed_single_page_history(
        history_plan,
        &history_response,
        &historical_sealed,
        &contract,
    )?;
    let expected_history_request_set_identity = completed_history.plan().request_set_identity();
    let expected_history_page_identity = completed_history.pages()[0].page_identity();
    let expected_history_completion_identity = completed_history.completion_identity();
    let historical_first_mapped = map_fund_nav_candidate(TiingoFundNavMappingInput {
        candidate: &historical_first,
        sealed_capture: &historical_sealed,
        completed_history: Some(&completed_history),
        sealed_metadata_capture: &sealed_metadata,
        contract: &contract,
        ingested_at: Timestamp::from_unix_nanos(205),
    })?;
    let historical_first_mapped = match historical_first_mapped
        .try_into_latest_pending_publication()
    {
        Ok(_) => return Err("historical Tiingo NAV entered the latest publication handoff".into()),
        Err(candidate) => candidate,
    };
    let historical_second_mapped = map_fund_nav_candidate(TiingoFundNavMappingInput {
        candidate: &historical_second,
        sealed_capture: &historical_sealed,
        completed_history: Some(&completed_history),
        sealed_metadata_capture: &sealed_metadata,
        contract: &contract,
        ingested_at: Timestamp::from_unix_nanos(205),
    })?;
    let expected_first_handoff_identity = historical_first_mapped.handoff_identity();
    let expected_second_handoff_identity = historical_second_mapped.handoff_identity();
    let completed_nav_history = TiingoCompletedFundNavHistoryCandidate::try_new(
        completed_history,
        vec![historical_second_mapped, historical_first_mapped],
    )?;
    let expected_completed_handoff_identity = completed_nav_history.handoff_identity();
    let (
        completed_capture,
        ordered_rows,
        returned_provider_rows,
        financial_coverage,
        completed_handoff_identity,
    ) = completed_nav_history
        .into_pending_publication()
        .into_parts();
    let [first_row, second_row] = ordered_rows.as_ref() else {
        return Err("expected exactly two consumed historical Tiingo NAV rows".into());
    };
    assert_eq!(
        (
            returned_provider_rows,
            financial_coverage,
            completed_handoff_identity,
            completed_capture.plan().request_set_identity(),
            completed_capture.completion_identity(),
            [first_row, second_row].into_iter().all(|row| {
                row.sealed_capture_receipt() == historical_sealed.receipt_digest()
                    && row.sealed_metadata_capture_receipt() == sealed_metadata.receipt_digest()
                    && row.response_request_identity()
                        == history_response.evidence().request().request_identity()
                    && row.history_page_identity() == Some(expected_history_page_identity)
                    && row.history_completion_identity()
                        == Some(expected_history_completion_identity)
            }),
            [
                (
                    first_row.provider_row_index(),
                    first_row.provider_row_digest(),
                    first_row.nav_date(),
                    matches!(first_row.value(), FundNavValue::Observed(_)),
                    first_row.lineage().completeness(),
                    first_row.lineage().disposition(),
                    first_row.handoff_identity(),
                ),
                (
                    second_row.provider_row_index(),
                    second_row.provider_row_digest(),
                    second_row.nav_date(),
                    matches!(second_row.value(), FundNavValue::Observed(_)),
                    second_row.lineage().completeness(),
                    second_row.lineage().disposition(),
                    second_row.handoff_identity(),
                ),
            ],
        ),
        (
            2,
            TiingoFundNavHistoryFinancialCoverage::ExpectedFinancialDatesUnavailable,
            expected_completed_handoff_identity,
            expected_history_request_set_identity,
            expected_history_completion_identity,
            true,
            [
                (
                    Some(0),
                    historical_first.provider_row_digest(),
                    date(2026, 8, 7)?,
                    true,
                    FundNavCompleteness::Complete,
                    FundNavDisposition::Returned,
                    expected_first_handoff_identity,
                ),
                (
                    Some(1),
                    historical_second.provider_row_digest(),
                    date(2026, 8, 10)?,
                    true,
                    FundNavCompleteness::Complete,
                    FundNavDisposition::Returned,
                    expected_second_handoff_identity,
                ),
            ],
        )
    );
    assert_distinct_eod_missing_nav_and_quota_contracts()?;
    Ok(())
}

fn assert_distinct_eod_missing_nav_and_quota_contracts() -> Result<(), Box<dyn Error>> {
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path())?;
    let store = paths.sealed_research_journal_store()?;
    let contract = contract()?;
    let ticker = TiingoTicker::try_new("NOHISTORYX")?;
    let decoder = TiingoDecoder::new(
        identifier("tiingo-daily-native-v1")?,
        identifier("tiingo-entitlement-generation-11")?,
    );
    let metadata_body = br#"{"ticker":"NOHISTORYX","name":"Reserved symbol","exchangeCode":"N/A","description":null,"startDate":null,"endDate":null}"#;
    let metadata = decoder.decode_metadata(
        TiingoRequestSpec::metadata(ticker.clone())?,
        200,
        metadata_body,
        Timestamp::from_unix_nanos(20),
        Timestamp::from_unix_nanos(21),
    )?;
    assert_eq!(
        classify_fund_support(&metadata),
        TiingoFundSupport::Unsupported
    );
    let empty_body = b"[]";
    let latest_response = decoder.decode_eod(
        TiingoRequestSpec::latest(ticker.clone())?,
        200,
        empty_body,
        Timestamp::from_unix_nanos(30),
        Timestamp::from_unix_nanos(31),
    )?;
    assert_eq!(latest_response.disposition().returned_symbols(), 0);
    assert_eq!(latest_response.disposition().missing_symbols(), 1);
    assert!(matches!(
        missing_nav_candidate(
            context(&ticker)?,
            date(2026, 8, 10)?,
            TiingoNavValueState::Unsupported,
            &metadata,
            &latest_response,
        ),
        Err(TiingoAdapterError::InvalidResponseSelection)
    ));
    let unsupported_plan =
        TiingoHistoryPlan::try_new(ticker.clone(), date(2026, 8, 10)?, date(2026, 8, 10)?)?;
    let response = decoder.decode_eod(
        unsupported_plan.pages()[0].clone(),
        200,
        empty_body,
        Timestamp::from_unix_nanos(32),
        Timestamp::from_unix_nanos(33),
    )?;
    let unsupported = missing_nav_candidate(
        context(&ticker)?,
        date(2026, 8, 10)?,
        TiingoNavValueState::Unsupported,
        &metadata,
        &response,
    )?;
    assert_eq!(unsupported.value(), TiingoNavValueState::Unsupported);
    assert!(unsupported.provider_row_digest().is_none());
    let sealed = seal_response(empty_body, response.evidence(), &contract, &store)?;
    let sealed_metadata = seal_response(metadata_body, metadata.evidence(), &contract, &store)?;
    let unsupported_completed_history =
        completed_single_page_history(unsupported_plan, &response, &sealed, &contract)?;
    let mapped = map_fund_nav_candidate(TiingoFundNavMappingInput {
        candidate: &unsupported,
        sealed_capture: &sealed,
        completed_history: Some(&unsupported_completed_history),
        sealed_metadata_capture: &sealed_metadata,
        contract: &contract,
        ingested_at: Timestamp::from_unix_nanos(35),
    })?;
    assert_eq!(
        mapped.value(),
        FundNavValue::Missing(FundNavMissingState::Unsupported)
    );

    assert!(matches!(
        missing_nav_candidate(
            context(&ticker)?,
            date(2026, 8, 10)?,
            TiingoNavValueState::Unavailable,
            &metadata,
            &response,
        ),
        Err(TiingoAdapterError::UnprovenNavState)
    ));
    let supported_ticker = TiingoTicker::try_new("VTSAX")?;
    let supported_metadata_body = br#"{"ticker":"VTSAX","name":"Vanguard Total Stock Market Index Fund Admiral Shares","exchangeCode":"MF","description":"Mutual fund","startDate":"2000-11-13","endDate":"2026-08-10"}"#;
    let supported_metadata = decoder.decode_metadata(
        TiingoRequestSpec::metadata(supported_ticker.clone())?,
        200,
        supported_metadata_body,
        Timestamp::from_unix_nanos(32),
        Timestamp::from_unix_nanos(33),
    )?;
    let supported_missing_plan = TiingoHistoryPlan::try_new(
        supported_ticker.clone(),
        date(2026, 8, 10)?,
        date(2026, 8, 10)?,
    )?;
    let supported_empty = decoder.decode_eod(
        supported_missing_plan.pages()[0].clone(),
        200,
        empty_body,
        Timestamp::from_unix_nanos(34),
        Timestamp::from_unix_nanos(35),
    )?;
    let source_missing = missing_nav_candidate(
        context(&supported_ticker)?,
        date(2026, 8, 10)?,
        TiingoNavValueState::SourceMissing,
        &supported_metadata,
        &supported_empty,
    )?;
    let supported_sealed =
        seal_response(empty_body, supported_empty.evidence(), &contract, &store)?;
    let supported_metadata_sealed = seal_response(
        supported_metadata_body,
        supported_metadata.evidence(),
        &contract,
        &store,
    )?;
    let supported_completed_history = completed_single_page_history(
        supported_missing_plan,
        &supported_empty,
        &supported_sealed,
        &contract,
    )?;
    let mapped = map_fund_nav_candidate(TiingoFundNavMappingInput {
        candidate: &source_missing,
        sealed_capture: &supported_sealed,
        completed_history: Some(&supported_completed_history),
        sealed_metadata_capture: &supported_metadata_sealed,
        contract: &contract,
        ingested_at: Timestamp::from_unix_nanos(36),
    })?;
    assert_eq!(
        mapped.value(),
        FundNavValue::Missing(FundNavMissingState::SourceMissing)
    );
    assert_eq!(
        mapped.lineage().completeness(),
        FundNavCompleteness::Complete
    );
    let completed_nav_history =
        TiingoCompletedFundNavHistoryCandidate::try_new(supported_completed_history, vec![mapped])?;
    assert_eq!(completed_nav_history.returned_provider_rows(), 0);
    assert_eq!(
        completed_nav_history.financial_coverage(),
        TiingoFundNavHistoryFinancialCoverage::ExpectedFinancialDatesUnavailable
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
    let equity_metadata_body = br#"{"ticker":"AAPL","name":"Apple Inc.","exchangeCode":"NASDAQ","description":"Equity","startDate":"1980-12-12","endDate":"2026-08-10"}"#;
    let equity_metadata = decoder.decode_metadata(
        TiingoRequestSpec::metadata(equity_ticker.clone())?,
        200,
        equity_metadata_body,
        Timestamp::from_unix_nanos(50),
        Timestamp::from_unix_nanos(51),
    )?;
    let equity_body = br#"[{"date":"2026-08-10T00:00:00.000Z","open":200,"high":201,"low":199,"close":200,"volume":100,"adjOpen":200,"adjHigh":201,"adjLow":199,"adjClose":200,"adjVolume":100,"divCash":0,"splitFactor":1}]"#;
    let equity_history_plan = TiingoHistoryPlan::try_new(
        equity_ticker.clone(),
        date(2026, 8, 10)?,
        date(2026, 8, 10)?,
    )?;
    let equity_response = decoder.decode_eod(
        equity_history_plan.pages()[0].clone(),
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
            0,
        ),
        Err(TiingoAdapterError::InvalidFundContext)
    ));

    let venue_id = VenueId::try_from("xnas")?;
    let eod_contract = TiingoEodContractEvidence::try_new(
        contract.source_contract_revision().clone(),
        contract.source_contract_evidence().clone(),
        contract.native_schema_revision().clone(),
        contract.native_schema_evidence().clone(),
        contract.entitlement_generation(),
        contract.entitlement_generation_identity().clone(),
        contract.entitlement_evidence(),
        ExactPayloadEvidence::from_content_digest(digest(b"tiingo-adjusted-eod-surface-v1")),
    )?;
    let raw_feed = eod_contract.raw_feed().clone();
    let adjusted_feed = eod_contract.adjusted_feed().clone();
    let eod_instrument = TiingoEodInstrumentAuthority::try_new(
        "06dd06da-ef2d-44dd-bf28-b006da06b24b".parse::<InstrumentId>()?,
        venue_id.clone(),
        ProviderInstrumentId::try_from(equity_ticker.as_str())?,
        equity_ticker.clone(),
        identifier("NASDAQ")?,
        TiingoEodInstrumentKind::Equity,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(identifier("instrument-revision-8")?),
            ExactPayloadEvidence::from_content_digest(digest(b"instrument-revision-8")),
        ),
        ExactPayloadEvidence::from_content_digest(digest(
            b"tiingo-aapl-nasdaq-provider-mapping-v1",
        )),
        Timestamp::from_unix_nanos(51),
        Currency::try_from("USD")?,
    )?;
    let equity_sealed = seal_response(equity_body, equity_response.evidence(), &contract, &store)?;
    let equity_metadata_sealed = seal_response(
        equity_metadata_body,
        equity_metadata.evidence(),
        &contract,
        &store,
    )?;
    let eod_page = map_eod_page_candidate(TiingoEodMappingInput {
        response: &equity_response,
        metadata: &equity_metadata,
        sealed_capture: &equity_sealed,
        sealed_metadata_capture: &equity_metadata_sealed,
        instrument: &eod_instrument,
        contract: &eod_contract,
        bar_time_authority: &FixedEodTimeAuthority,
        ingested_at: Timestamp::from_unix_nanos(54),
    })?;
    let eod_page = match eod_page.into_publication_route() {
        TiingoEodPagePublicationRoute::Latest(_) => {
            return Err("historical Tiingo EOD entered the latest publication handoff".into());
        }
        TiingoEodPagePublicationRoute::Historical(page) => page,
    };
    let [raw, adjusted] = eod_page.bars() else {
        return Err("expected separate raw and adjusted Tiingo EOD candidates".into());
    };
    assert_eq!(
        (
            raw.adjustment(),
            adjusted.adjustment(),
            raw.feed(),
            adjusted.feed(),
            raw.provider_row_index(),
            adjusted.provider_row_index(),
            raw.provider_row_digest(),
            adjusted.provider_row_digest(),
        ),
        (
            MarketBarAdjustment::Raw,
            MarketBarAdjustment::All,
            &raw_feed,
            &adjusted_feed,
            0,
            0,
            equity_response.rows()[0].row_digest(),
            equity_response.rows()[0].row_digest(),
        )
    );
    assert_eq!(
        (
            raw.source_publication(),
            raw.provider_revision(),
            raw.received_at(),
            raw.decoded_at(),
            raw.ingested_at(),
            eod_page.gaps().len(),
            eod_page.provider_actions().len(),
            eod_page.sealed_metadata_capture_receipt(),
        ),
        (
            TiingoSourcePublicationEvidence::NotSupplied,
            TiingoProviderRevisionEvidence::NotSupplied,
            Timestamp::from_unix_nanos(52),
            Timestamp::from_unix_nanos(53),
            Timestamp::from_unix_nanos(54),
            0,
            1,
            equity_metadata_sealed.receipt_digest(),
        )
    );
    let eod_disposition = eod_page.eod_request_disposition();
    let metadata_disposition = eod_page.metadata_request_disposition();
    assert_eq!(
        (
            eod_disposition.requested_symbols(),
            eod_disposition.returned_symbols(),
            eod_disposition.missing_symbols(),
            eod_disposition.returned_rows(),
            eod_disposition.response_bytes(),
            metadata_disposition.requested_symbols(),
            metadata_disposition.returned_symbols(),
            metadata_disposition.missing_symbols(),
            metadata_disposition.returned_rows(),
            metadata_disposition.response_bytes(),
        ),
        (
            1,
            1,
            0,
            1,
            u64::try_from(equity_body.len())?,
            1,
            1,
            0,
            1,
            u64::try_from(equity_metadata_body.len())?,
        )
    );
    assert_ne!(raw.feed(), adjusted.feed());
    assert_ne!(raw.semantic_identity(), adjusted.semantic_identity());

    let completed_capture = completed_single_page_history(
        equity_history_plan,
        &equity_response,
        &equity_sealed,
        &contract,
    )?;
    let completed_history = TiingoCompletedEodHistoryCandidate::try_new(
        completed_capture,
        vec![eod_page],
        &eod_instrument,
        &FixedEodExpectedSessionAuthority,
    )?;
    let expected_completion_identity = completed_history.completion_identity();
    let pending_history = completed_history.into_pending_publication();
    assert_eq!(
        (
            pending_history.capture().terminal(),
            pending_history.pages().len(),
            pending_history.total_bars(),
            pending_history.total_gaps(),
            pending_history.total_provider_actions(),
            pending_history.financial_coverage(),
            pending_history.returned_sessions(),
            pending_history.missing_expected_sessions().is_empty(),
            pending_history
                .expected_session_validation()
                .authority_generation()
                .as_str(),
            pending_history.completion_identity(),
        ),
        (
            TiingoHistoryTerminalDisposition::ApplicationDateWindowsExhaustedWithoutProviderCursor,
            1,
            2,
            0,
            1,
            TiingoEodFinancialCoverageDisposition::Complete,
            [date(2026, 8, 10)?].as_slice(),
            true,
            "xnas-calendar-authority-generation-7",
            expected_completion_identity,
        )
    );

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
    let Ok(cancelled) = ledger.reserve(ticker.clone(), reservation)? else {
        return Err("unexpected pre-dispatch quota denial".into());
    };
    ledger.cancel_before_dispatch(&cancelled, &ticker)?;
    assert_eq!(ledger.snapshot().requests_this_hour(), 0);
    assert_eq!(ledger.snapshot().requests_this_day(), 0);
    assert!(ledger.snapshot().pending_response().is_none());
    assert!(
        !ledger
            .snapshot()
            .unique_symbols_this_month()
            .contains(&ticker)
    );
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
