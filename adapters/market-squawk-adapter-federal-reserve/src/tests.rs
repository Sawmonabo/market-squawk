//! Focused parser and immutable-publication acceptance tests.

use std::collections::VecDeque;
use std::error::Error;
use std::io::{Cursor, Write as _};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
    ResearchTemporalCoordinate, RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability,
    SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    BackoffPolicy, BudgetScope, CoverageDomain, DiscoveryRequest, EndpointPolicy,
    ExtractionAuthority, ExtractionRequest, ExtractionSource, FreshnessPolicy,
    HistoricalCapability, NetworkAccessPolicy, PathScope, ProviderBudgetPolicy, QueryParameterRule,
    QuerySensitivity, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataInput, SourceProtocolProfile,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

use crate::contract::test_h15_one_series_contract;
use crate::digest::sha256;
use crate::transport::{BoardHttpRequest, BoardHttpResponse, BoardTransport, system_timestamp};
use crate::*;

const H15_URL: &str = "https://www.federalreserve.gov/datadownload/Output.aspx?rel=H15&series=0123456789abcdef0123456789abcdef&lastobs=&from=&to=&filetype=csv&label=include&layout=seriescolumn";
const DESCRIPTION: &str = "Market yield on U.S. Treasury securities at 1-month constant maturity, quoted on investment basis";
const STRUCTURAL: [(&str, &[u8]); 3] = [
    ("frb-common.xsd", b"common"),
    ("h15-structure.xml", b"structure"),
    ("h15-dataset.xsd", b"dataset"),
];

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug)]
struct ScriptedBoardTransport {
    responses: Mutex<VecDeque<ScriptedBoardResponse>>,
    requests: Mutex<Vec<BoardHttpRequest>>,
}

#[derive(Debug)]
struct ScriptedBoardResponse {
    status: u16,
    content_type: Option<Vec<u8>>,
    etag: Option<Vec<u8>>,
    last_modified: Option<Vec<u8>>,
    body: Bytes,
}

impl BoardTransport for ScriptedBoardTransport {
    fn execute(
        &self,
        request: BoardHttpRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<BoardHttpResponse, BoardSourceError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(BoardSourceError::Cancelled);
            }
            if timeout.is_zero() {
                return Err(BoardSourceError::DeadlineExceeded);
            }
            self.requests
                .lock()
                .map_err(|_| BoardSourceError::Network)?
                .push(request);
            let response = self
                .responses
                .lock()
                .map_err(|_| BoardSourceError::Network)?
                .pop_front()
                .ok_or(BoardSourceError::Network)?;
            if response.body.len() > max_bytes {
                return Err(BoardSourceError::BodyTooLarge);
            }
            Ok(BoardHttpResponse {
                status: response.status,
                retry_after: None,
                content_encoding: None,
                content_type: response.content_type,
                etag: response.etag,
                last_modified: response.last_modified,
                declared_body_bytes: Some(response.body.len() as u64),
                body: response.body,
                received_at: system_timestamp()?,
                latency: Duration::from_millis(1),
            })
        })
    }
}

struct BoardSourceHarness {
    source: BoardSource,
    authority: ExtractionAuthority,
    _registry: AuthoritativeSourceRegistry,
    transport: Arc<ScriptedBoardTransport>,
}

#[test]
fn exact_h15_publication_retains_correction_and_replacement_evidence() -> Result<(), Box<dyn Error>>
{
    let contract = test_h15_one_series_contract(H15_URL)?;
    let first = parse_csv(
        &contract,
        h15_csv("5.125", "ND").as_bytes(),
        BoardParseLimits::default(),
    )?;
    assert_eq!(first.observation_count(), 2);
    assert_eq!(first.missing_observation_count(), 1);
    assert_eq!(
        first.series()[0].observations()[0].period().raw(),
        "2026-08-07"
    );

    let initial_timing = timing(5, None, 10, 20, 30)?;
    let initial_event = BoardPublicationEvent::try_new(
        BoardPublicationEventKind::ScheduledRelease,
        "h15-2026-08-10",
        sha256(b"scheduled release evidence"),
    )?;
    let first_receipt =
        match BoardPublisher::publish(&first, initial_event, initial_timing, None, Vec::new())? {
            BoardPublicationOutcome::Published { receipt } => receipt,
            BoardPublicationOutcome::ExactDuplicate { .. } => {
                return Err(BoardAdapterError::InvalidRevisionEvidence.into());
            }
        };
    assert_eq!(first_receipt.revision().get(), 1);

    let corrected = parse_csv(
        &contract,
        h15_csv("5.100", "ND").as_bytes(),
        BoardParseLimits::default(),
    )?;
    let correction_timing = timing(5, Some(35), 40, 50, 60)?;
    let correction_event = BoardPublicationEvent::try_new(
        BoardPublicationEventKind::OffScheduleCorrection,
        "h15-correction-2026-08-11",
        sha256(b"correction notice evidence"),
    )?;
    let correction_receipt = match BoardPublisher::publish(
        &corrected,
        correction_event,
        correction_timing,
        Some((&first, &first_receipt)),
        Vec::new(),
    )? {
        BoardPublicationOutcome::Published { receipt } => receipt,
        BoardPublicationOutcome::ExactDuplicate { .. } => {
            return Err(BoardAdapterError::InvalidRevisionEvidence.into());
        }
    };
    assert_eq!(correction_receipt.revision().get(), 2);
    assert_eq!(
        correction_receipt
            .revision_evidence()
            .changed_observations(),
        1
    );
    assert_eq!(
        correction_receipt
            .revision_evidence()
            .unchanged_observations(),
        1
    );
    assert_eq!(
        correction_receipt.predecessor_receipt_digest(),
        Some(first_receipt.receipt_digest())
    );
    assert_eq!(
        correction_receipt.vintage_capability(),
        BoardVintageCapability::LocallyRetainedAcquisitionsOnly
    );

    let xml_contract = BoardDatasetContract::try_sdmx(
        BoardDatasetFamily::H15CompleteRelease,
        BoardFileFormat::SdmxCompactXmlV1,
        "https://www.federalreserve.gov/releases/h15/h15-data.xml",
        BoardFrequency::BusinessDaily,
        contract.series_scope().clone(),
        sdmx_package()?,
    )?;
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><frb:CompactData xmlns:frb="http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message" xmlns:h15="urn:frb:h15"><frb:Header><frb:ID>H15-test</frb:ID><frb:Test>false</frb:Test><frb:Prepared>2026-08-11T12:00:00Z</frb:Prepared><frb:Sender id="FRB"/></frb:Header><h15:DataSet><h15:Series SERIES_NAME="RIFLGFCM01_N.B" FREQ="D" UNIT="Percent:_Per_Year" UNIT_MULT="1" CURRENCY="NA" SERIES_DESCRIPTION="{DESCRIPTION}"><h15:Obs TIME_PERIOD="2026-08-07" OBS_VALUE="5.125" OBS_STATUS="A"/></h15:Series></h15:DataSet></frb:CompactData>"#
    );
    let parsed_xml = parse_sdmx_xml(
        &xml_contract,
        xml.as_bytes(),
        &STRUCTURAL,
        BoardParseLimits::default(),
    )?;
    assert_eq!(parsed_xml.observation_count(), 1);
    assert_eq!(
        parsed_xml.sdmx_header().map(BoardSdmxHeader::id),
        Some("H15-test")
    );

    assert!(matches!(
        BoardRelease::G17IndustrialProduction.documented_route_lifecycle()?,
        BoardRouteLifecycle::DdpTransitionAnnounced {
            board_release_xml_remains_candidate: true,
            fred_is_separate_provenance: true,
            ..
        }
    ));
    Ok(())
}

#[test]
fn malformed_csv_schema_and_excessive_zip_expansion_fail_closed() -> Result<(), Box<dyn Error>> {
    let contract = test_h15_one_series_contract(H15_URL)?;
    let malformed = h15_csv("5.125", "ND").replace("Time Period", "Date");
    assert!(matches!(
        parse_csv(&contract, malformed.as_bytes(), BoardParseLimits::default()),
        Err(BoardAdapterError::CsvSchemaDrift)
    ));

    let zip_contract = BoardDatasetContract::try_sdmx(
        BoardDatasetFamily::H15CompleteRelease,
        BoardFileFormat::SdmxCompactZipV1,
        "https://www.federalreserve.gov/releases/h15/h15.zip",
        BoardFrequency::BusinessDaily,
        BoardSeriesScope::StructureBoundCompleteRelease { max_series: 32 },
        sdmx_package()?,
    )?;
    let bomb = zip_fixture(&STRUCTURAL)?;
    let strict = BoardParseLimits::try_new(
        2 * 1024 * 1024,
        8,
        2 * 1024 * 1024,
        1024 * 1024,
        2,
        32,
        1_000,
        32,
        32,
        4096,
    )?;
    assert!(matches!(
        parse_sdmx_zip(&zip_contract, &bomb, strict),
        Err(BoardAdapterError::CompressionRatioExceeded)
    ));
    Ok(())
}

#[tokio::test]
async fn authority_transport_preserves_exact_repost_and_conditional_evidence() -> TestResult {
    let first_body = Bytes::from(h15_csv("5.125", "ND"));
    let repost_body = Bytes::from(h15_csv("5.125", "ND").replace('\n', "\r\n"));
    let first_harness = source_harness(ScriptedBoardResponse {
        status: 200,
        content_type: Some(b"text/csv; charset=utf-8".to_vec()),
        etag: Some(b"\"h15-v1\"".to_vec()),
        last_modified: Some(b"Tue, 11 Aug 2026 16:00:00 GMT".to_vec()),
        body: first_body.clone(),
    })?;
    let first = extract_once(&first_harness).await?;
    assert_eq!(first.batch().records().len(), 2);
    assert_eq!(first.receipt().body_bytes(), first_body.len() as u64);
    assert_eq!(first.receipt().body_digest(), sha256(&first_body));
    assert_ne!(
        first.receipt().request_digest(),
        first.receipt().contract_request_digest()
    );
    assert_eq!(first_harness.source.health()?.requests_total(), 1);
    assert_eq!(
        first_harness
            .transport
            .requests
            .lock()
            .map_err(|_| "request log poisoned")?
            .len(),
        1
    );

    assert_eq!(first.capture().records().len(), 1);
    assert_eq!(first.capture().records()[0].payload(), first_body.as_ref());
    assert_eq!(
        first.capture().receipt().pages()[0].body_digest().bytes(),
        first.receipt().body_digest()
    );
    let repost = parse_csv(
        first_harness.source.profile().contract(),
        &repost_body,
        BoardParseLimits::default(),
    )?;
    assert_ne!(
        first.parsed().source_payload_digest(),
        repost.source_payload_digest()
    );
    assert_eq!(
        first.parsed().normalized_content_digest(),
        repost.normalized_content_digest()
    );

    let first_receipt = publish_initial(first.parsed(), first.receipt().received_at())?;
    let repost_event = BoardPublicationEvent::try_new(
        BoardPublicationEventKind::Repost,
        "h15-byte-repost-v2",
        sha256(b"locally observed exact-byte repost"),
    )?;
    let repost_timing = BoardPublicationTiming::try_new(
        None,
        None,
        None,
        first.receipt().received_at(),
        first.receipt().received_at(),
        first.receipt().received_at().checked_add_nanos(1)?,
    )?;
    let repost_receipt = match BoardPublisher::publish(
        &repost,
        repost_event,
        repost_timing,
        Some((first.parsed(), &first_receipt)),
        Vec::new(),
    )? {
        BoardPublicationOutcome::Published { receipt } => receipt,
        BoardPublicationOutcome::ExactDuplicate { .. } => {
            return Err("repost collapsed into an exact retry".into());
        }
    };
    assert_eq!(repost_receipt.revision().get(), 2);
    assert_eq!(repost_receipt.revision_evidence().changed_observations(), 0);
    assert_eq!(
        repost_receipt.predecessor_receipt_digest(),
        Some(first_receipt.receipt_digest())
    );

    let conditional = BoardConditionalRequest::try_new(
        first.receipt().validators().clone(),
        first.receipt().body_digest(),
    )?;
    assert_eq!(
        conditional.prior_payload_digest(),
        first.receipt().body_digest()
    );
    Ok(())
}

async fn extract_once(harness: &BoardSourceHarness) -> TestResult<BoardExtractionOutput> {
    let deadline = system_timestamp()?.checked_add_nanos(60_000_000_000)?;
    let discovery = harness
        .source
        .discover(
            harness.authority.clone(),
            DiscoveryRequest::try_new(
                harness.source.profile().dataset().clone(),
                None,
                NonZeroU16::MIN,
                deadline,
            )?,
            CancellationToken::new(),
        )
        .await?;
    let object = discovery
        .objects()
        .first()
        .ok_or("missing discovered Board object")?
        .clone();
    let extraction_request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(100).ok_or("record bound")?,
        NonZeroU64::new(4 * 1024 * 1024).ok_or("byte bound")?,
        deadline,
    )?;
    assert!(matches!(
        harness
            .source
            .extract(
                harness.authority.clone(),
                extraction_request.clone(),
                CancellationToken::new(),
            )
            .await,
        Err(market_squawk_sources::ExtractionSourceError::Source(
            market_squawk_sources::SourceError::InvalidProtocolState
        ))
    ));
    let output = harness
        .source
        .extract_with_evidence(
            harness.authority.clone(),
            extraction_request,
            CancellationToken::new(),
        )
        .await?;
    assert!(
        harness
            .source
            .revision_plan(output.batch())?
            .is_locally_observed()
    );
    Ok(output)
}

fn source_harness(response: ScriptedBoardResponse) -> TestResult<BoardSourceHarness> {
    let now = system_timestamp()?;
    let contract = test_h15_one_series_contract(H15_URL)?;
    let profile = BoardDatasetProfile::try_new(contract, BoardParseLimits::default(), Vec::new())?;
    let metadata = board_metadata(now)?;
    let transport = Arc::new(ScriptedBoardTransport {
        responses: Mutex::new(VecDeque::from([response])),
        requests: Mutex::new(Vec::new()),
    });
    let source = BoardSource::try_new_with_transport(metadata.clone(), profile, transport.clone())?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(metadata, now)?;
    let authority = registry.extraction_authority(&registered, &source)?;
    Ok(BoardSourceHarness {
        source,
        authority,
        _registry: registry,
        transport,
    })
}

fn board_metadata(now: Timestamp) -> TestResult<SourceMetadata> {
    let effective = EffectiveInterval::new(now.checked_sub_nanos(1)?, None)?;
    let provider = SourceIdentifier::try_from("federal-reserve-board")?;
    let evidence = exact_evidence(b"federal-reserve-board-h15-mock-metadata-v1");
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(SourceIdentifier::try_from("official-public-interface")?),
        evidence.clone(),
        effective,
    );
    let endpoint = ApiEndpointRule::try_new(
        "https://www.federalreserve.gov/datadownload/Output.aspx",
        PathScope::Exact,
        query_rules(&[
            ("rel", 8),
            ("series", 256),
            ("lastobs", 16),
            ("from", 16),
            ("to", 16),
            ("filetype", 16),
            ("label", 16),
            ("layout", 32),
        ])?,
        8,
        1_024,
    )?;
    let network = EndpointPolicy::try_from_api_rules(
        vec![endpoint],
        market_squawk_sources::HttpRequestBounds::default(),
    )?;
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::new(provider.clone()),
        NonZeroU32::MIN,
        NonZeroU64::new(60_000_000_000).ok_or("budget window")?,
        NonZeroU16::MIN,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000).ok_or("initial backoff")?,
            NonZeroU64::new(60_000_000_000).ok_or("maximum backoff")?,
            0,
        )?,
    )?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("federal-reserve-board-h15-test")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from(
                "federal-reserve-board-h15-test-v1",
            )?),
            evidence.clone(),
        ),
        SourceClass::OfficialAgency,
        provider,
        authorization,
        SourceCoverage::try_non_instrument(
            evidence,
            effective,
            CoverageDomain::Macroeconomic,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Unknown,
        )?,
        DataQuality::OfficialDelayed,
        NetworkAccessPolicy::Allowlisted(network),
        FreshnessPolicy::try_new(60, 60, 60, 60, 1)?,
        Some(budget),
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::Historical,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))?)
}

fn query_rules(values: &[(&str, u16)]) -> TestResult<Vec<QueryParameterRule>> {
    values
        .iter()
        .map(|(key, max)| {
            QueryParameterRule::try_new(
                SourceIdentifier::try_from(*key)?,
                *max,
                false,
                QuerySensitivity::Public,
            )
            .map_err(Into::into)
        })
        .collect()
}

fn exact_evidence(bytes: &[u8]) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(bytes).into(),
    ))
}

fn publish_initial(
    parsed: &ParsedBoardDataset,
    received_at: Timestamp,
) -> Result<BoardPublicationReceipt, BoardAdapterError> {
    let scheduled = received_at
        .checked_sub_nanos(1)
        .map_err(|_| BoardAdapterError::InvalidChronology)?;
    let timing = BoardPublicationTiming::try_new(
        Some(ResearchTemporalCoordinate::exact(scheduled)),
        None,
        None,
        received_at,
        received_at,
        received_at
            .checked_add_nanos(1)
            .map_err(|_| BoardAdapterError::InvalidChronology)?,
    )?;
    let event = BoardPublicationEvent::try_new(
        BoardPublicationEventKind::ScheduledRelease,
        "h15-initial-local-acquisition",
        sha256(b"selected scheduled-release evidence"),
    )?;
    match BoardPublisher::publish(parsed, event, timing, None, Vec::new())? {
        BoardPublicationOutcome::Published { receipt } => Ok(receipt),
        BoardPublicationOutcome::ExactDuplicate { .. } => {
            Err(BoardAdapterError::InvalidRevisionEvidence)
        }
    }
}

fn h15_csv(first: &str, second: &str) -> String {
    format!(
        "Series Description,\"{DESCRIPTION}\"\nUnit:,Percent:_Per_Year\nMultiplier:,1\nCurrency:,NA\nUnique Identifier: ,H15/H15/RIFLGFCM01_N.B\nTime Period,RIFLGFCM01_N.B\n2026-08-07,{first}\n2026-08-10,{second}\n"
    )
}

fn timing(
    scheduled: i64,
    correction: Option<i64>,
    route: i64,
    received: i64,
    parsed: i64,
) -> Result<BoardPublicationTiming, BoardAdapterError> {
    BoardPublicationTiming::try_new(
        Some(ResearchTemporalCoordinate::exact(
            Timestamp::from_unix_nanos(scheduled),
        )),
        Some(ResearchTemporalCoordinate::exact(
            Timestamp::from_unix_nanos(scheduled),
        )),
        correction
            .map(|value| ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(value))),
        Timestamp::from_unix_nanos(route),
        Timestamp::from_unix_nanos(received),
        Timestamp::from_unix_nanos(parsed),
    )
}

fn zip_fixture(structural: &[(&str, &[u8])]) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let compressed = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    writer.start_file("h15-data.xml", compressed)?;
    writer.write_all(&vec![b'A'; 64 * 1024])?;
    for (name, bytes) in structural {
        writer.start_file(*name, SimpleFileOptions::default())?;
        writer.write_all(bytes)?;
    }
    Ok(writer.finish()?.into_inner())
}

fn sdmx_package() -> Result<SdmxPackageContract, BoardAdapterError> {
    SdmxPackageContract::try_new(
        "urn:frb:h15",
        "H15",
        vec![
            BoardArtifactContract::try_new("h15-data.xml", BoardArtifactKind::DataXml, None)?,
            BoardArtifactContract::try_new(
                "frb-common.xsd",
                BoardArtifactKind::FrbCommonSchema,
                Some(sha256(STRUCTURAL[0].1)),
            )?,
            BoardArtifactContract::try_new(
                "h15-structure.xml",
                BoardArtifactKind::ReleaseStructure,
                Some(sha256(STRUCTURAL[1].1)),
            )?,
            BoardArtifactContract::try_new(
                "h15-dataset.xsd",
                BoardArtifactKind::DatasetSchema,
                Some(sha256(STRUCTURAL[2].1)),
            )?,
        ],
    )
}
