use std::collections::VecDeque;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AuthorizationBasis, CalendarDate, ChecksumCapability, CoverageDelay, DataQuality,
    DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    MetadataRevision, ResearchObservation, RevisionBoundPayloadEvidence, SchemaVersion,
    SequenceCapability, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::LocalPaths;
use market_squawk_sources::{
    ApiEndpointRule, AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    BackoffPolicy, BudgetScope, BudgetWindowSemantics, CoverageDomain, DiscoveryRequest,
    EndpointPolicy, ExtractionRequest, ExtractionSource, FreshnessPolicy, HistoricalCapability,
    NetworkAccessPolicy, PathScope, ProviderBudgetPolicy, ProviderBudgetWindow,
    ProviderNativeLineageImplementation, QueryParameterRule, QuerySensitivity, SourceCapabilities,
    SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput, SourceProtocolProfile,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::client::{
    TreasuryHttpRequest, TreasuryHttpResponse, TreasuryTransport, system_timestamp,
};
use crate::{
    TreasuryDailyRateFamily, TreasuryDailyRateQuery, TreasuryDailyRatesConfig, TreasuryFiscalQuery,
    TreasurySourceConfig,
};

use super::TreasurySource;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

static TEST_PROVIDER_RATE_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const TEST_PROVIDER_RATE_SETTLE: Duration = Duration::from_millis(1_050);

#[derive(Debug)]
struct ScriptedTransport {
    responses: Mutex<VecDeque<TreasuryHttpResponse>>,
    requested_urls: Mutex<Vec<String>>,
}

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("market-squawk-treasury-{}", Uuid::new_v4())))
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

impl TreasuryTransport for ScriptedTransport {
    fn execute(
        &self,
        request: TreasuryHttpRequest,
        _max_bytes: usize,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<TreasuryHttpResponse, super::TreasurySourceError>> {
        Box::pin(async move {
            if !request.accept.starts_with("application/") {
                return Err(super::TreasurySourceError::InvalidProtocol);
            }
            self.requested_urls
                .lock()
                .map_err(|_| super::TreasurySourceError::InvalidProtocol)?
                .push(request.url);
            let mut response = self
                .responses
                .lock()
                .map_err(|_| super::TreasurySourceError::InvalidProtocol)?
                .pop_front()
                .ok_or(super::TreasurySourceError::InvalidProtocol)?;
            response.received_at = system_timestamp()?;
            Ok(response)
        })
    }
}

#[tokio::test]
async fn authority_bound_sources_emit_canonical_fiscal_and_daily_rate_records() -> TestResult {
    let _provider_rate_guard = TEST_PROVIDER_RATE_SERIAL.lock().await;
    let now = system_timestamp()?;
    let fiscal_query = TreasuryFiscalQuery::average_interest_rates_v2(
        CalendarDate::new(2026, 1, 1)?,
        CalendarDate::new(2026, 12, 31)?,
        NonZeroU16::new(1).ok_or("nonzero page size")?,
    )?;
    let fiscal_config = TreasurySourceConfig::average_interest_rates(fiscal_query);
    assert!(
        TreasurySource::try_new(
            metadata(now, &fiscal_config, DataQuality::OfficialDelayed)?,
            fiscal_config.clone(),
        )
        .is_ok()
    );
    assert!(matches!(
        TreasurySource::try_new(
            metadata(now, &fiscal_config, DataQuality::DirectUnverified)?,
            fiscal_config.clone(),
        ),
        Err(super::TreasurySourceError::InvalidMetadata)
    ));
    assert!(matches!(
        TreasurySource::try_new(
            metadata_with_rate(
                now,
                &fiscal_config,
                DataQuality::OfficialDelayed,
                100,
                60_000_000_000,
                2
            )?,
            fiscal_config.clone(),
        ),
        Err(super::TreasurySourceError::InvalidMetadata)
    ));
    let mut fiscal_page_one: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../fixtures/average_interest_rates.json"))?;
    fiscal_page_one["meta"]["total-count"] = serde_json::json!(2);
    fiscal_page_one["meta"]["total-pages"] = serde_json::json!(2);
    fiscal_page_one["links"]["next"] = serde_json::json!("&page%5Bnumber%5D=2&page%5Bsize%5D=1");
    fiscal_page_one["links"]["last"] = serde_json::json!("&page%5Bnumber%5D=2&page%5Bsize%5D=1");
    let mut fiscal_page_two = fiscal_page_one.clone();
    fiscal_page_two["links"]["self"] = serde_json::json!("&page%5Bnumber%5D=2&page%5Bsize%5D=1");
    fiscal_page_two["links"]["prev"] = serde_json::json!("&page%5Bnumber%5D=1&page%5Bsize%5D=1");
    fiscal_page_two["links"]["next"] = serde_json::Value::Null;
    fiscal_page_two["data"][0]["record_date"] = serde_json::json!("2026-07-01");
    fiscal_page_two["data"][0]["src_line_nbr"] = serde_json::json!("2");
    fiscal_page_two["data"][0]["record_fiscal_quarter"] = serde_json::json!("4");
    fiscal_page_two["data"][0]["record_calendar_quarter"] = serde_json::json!("3");
    fiscal_page_two["data"][0]["record_calendar_month"] = serde_json::json!("07");
    fiscal_page_two["data"][0]["record_calendar_day"] = serde_json::json!("01");
    let fiscal_page_one = serde_json::to_vec(&fiscal_page_one)?;
    let fiscal_page_two = serde_json::to_vec(&fiscal_page_two)?;
    let fiscal =
        exercise_fiscal_source(now, fiscal_config, &fiscal_page_one, &fiscal_page_two).await?;
    assert_eq!(fiscal.len(), 2);
    assert_macro_record(
        &fiscal[0],
        "treasury:average-interest-rate:v2:Marketable:Treasury%20Bills",
        "3.706",
        DataQuality::OfficialDelayed,
        None,
    )?;

    let all_family_queries = TreasuryDailyRateFamily::ALL
        .into_iter()
        .map(|family| TreasuryDailyRateQuery::year(family, 2026))
        .collect::<Result<Vec<_>, _>>()?;
    let all_family_config =
        TreasurySourceConfig::daily_rates(TreasuryDailyRatesConfig::try_new(all_family_queries)?);
    let all_family_metadata = metadata(now, &all_family_config, DataQuality::OfficialDelayed)?;
    let all_family_transport = Arc::new(ScriptedTransport {
        responses: Mutex::new(VecDeque::from(
            [
                include_bytes!("../../fixtures/daily_par_yield_curve.xml").as_slice(),
                include_bytes!("../../fixtures/daily_bill_rates.xml").as_slice(),
                include_bytes!("../../fixtures/daily_long_term_rates.xml").as_slice(),
                include_bytes!("../../fixtures/daily_real_par_yield_curve.xml").as_slice(),
                include_bytes!("../../fixtures/daily_real_long_term_rates.xml").as_slice(),
            ]
            .into_iter()
            .map(|payload| TreasuryHttpResponse {
                status: 200,
                retry_after: None,
                content_encoding: None,
                content_type: Some(b"application/atom+xml".to_vec()),
                body: Bytes::copy_from_slice(payload),
                received_at: now,
            })
            .collect::<Vec<_>>(),
        )),
        requested_urls: Mutex::new(Vec::new()),
    });
    let all_family_source = TreasurySource::try_new_with_transport(
        all_family_metadata.clone(),
        all_family_config,
        all_family_transport,
    )?;
    let mut all_family_registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let all_family_registered = all_family_registry.register(all_family_metadata, now)?;
    let all_family_authority =
        all_family_registry.extraction_authority(&all_family_registered, &all_family_source)?;
    let all_family_doctor = all_family_source
        .run_doctor(
            all_family_authority,
            now.checked_add_nanos(60_000_000_000)?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        all_family_doctor.probe_count(),
        TreasuryDailyRateFamily::ALL.len()
    );
    let all_family_temporary = TemporaryDirectory::new();
    let all_family_store =
        LocalPaths::prepare(all_family_temporary.path())?.sealed_research_journal_store()?;
    let all_family_doctor = all_family_doctor.seal(&all_family_store)?;
    assert!(all_family_doctor.activation_ready());
    assert_eq!(
        all_family_doctor.receipt().observations().len(),
        TreasuryDailyRateFamily::ALL.len()
    );
    assert!(
        all_family_doctor
            .receipt()
            .observations()
            .iter()
            .all(|observation| observation.observed_numeric_points() > 0
                && observation
                    .observed_numeric_points()
                    .checked_add(observation.explicit_missing_points())
                    == Some(observation.canonical_points()))
    );
    assert_eq!(
        all_family_doctor
            .receipt()
            .observed_numeric_points()
            .checked_add(all_family_doctor.receipt().explicit_missing_points()),
        Some(all_family_doctor.receipt().canonical_points())
    );

    Ok(())
}

#[tokio::test]
async fn all_history_requires_each_raw_page_seal_and_restores_before_terminal() -> TestResult {
    let _provider_rate_guard = TEST_PROVIDER_RATE_SERIAL.lock().await;
    let now = system_timestamp()?;
    let family = TreasuryDailyRateFamily::NominalParYieldCurve;
    let query = TreasuryDailyRateQuery::all_history(family)?;
    let config =
        TreasurySourceConfig::daily_rates(TreasuryDailyRatesConfig::try_new([query.clone()])?);
    let source_metadata = metadata(now, &config, DataQuality::OfficialDelayed)?;
    let terminal_receipt_clock_floor = system_timestamp()?;
    let future_terminal_feed_at = terminal_receipt_clock_floor.checked_add_nanos(60_000_000_000)?;
    let terminal_feed = |feed_published_at: Timestamp| {
        format!(
            r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>{}</title>
  <id>{}</id>
  <updated>{}</updated>
</feed>"#,
            family.feed_title(),
            family.feed_identity(),
            DateTime::<Utc>::from_timestamp_nanos(feed_published_at.unix_nanos())
                .to_rfc3339_opts(SecondsFormat::Nanos, true),
        )
    };
    let terminal = terminal_feed(terminal_receipt_clock_floor);
    let future_terminal = terminal_feed(future_terminal_feed_at);
    let transport = Arc::new(ScriptedTransport {
        responses: Mutex::new(VecDeque::from([
            TreasuryHttpResponse {
                status: 200,
                retry_after: None,
                content_encoding: None,
                content_type: Some(b"application/atom+xml".to_vec()),
                body: Bytes::from_static(include_bytes!(
                    "../../fixtures/daily_par_yield_curve.xml"
                )),
                received_at: now,
            },
            TreasuryHttpResponse {
                status: 200,
                retry_after: None,
                content_encoding: None,
                content_type: Some(b"application/atom+xml".to_vec()),
                body: Bytes::from(future_terminal),
                received_at: now,
            },
            TreasuryHttpResponse {
                status: 200,
                retry_after: None,
                content_encoding: None,
                content_type: Some(b"application/atom+xml".to_vec()),
                body: Bytes::from(terminal),
                received_at: now,
            },
        ])),
        requested_urls: Mutex::new(Vec::new()),
    });
    let source =
        TreasurySource::try_new_with_transport(source_metadata.clone(), config, transport)?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(source_metadata, now)?;
    let authority = registry.extraction_authority(&registered, &source)?;
    let deadline = now.checked_add_nanos(60_000_000_000)?;
    let temporary = TemporaryDirectory::new();
    let store = LocalPaths::prepare(temporary.path())?.sealed_research_journal_store()?;

    let mut backfill = source.start_all_history_backfill(query.dataset())?;
    tokio::time::sleep(TEST_PROVIDER_RATE_SETTLE).await;
    let first = source
        .fetch_next_all_history_page(
            &backfill,
            authority.clone(),
            DiscoveryRequest::try_new(
                query.dataset().clone(),
                None,
                NonZeroU16::new(1).ok_or("nonzero result count")?,
                deadline,
            )?,
            CancellationToken::new(),
        )
        .await?;
    assert!(!first.terminal());
    let canonical = first.canonical().ok_or("missing canonical data page")?;
    assert_eq!(
        canonical.accounting().aggregate_canonical_points(),
        u64::try_from(canonical.batch().records().len())?
    );
    let (_, capture, admission) = first.into_parts();
    let (expectation, seal_request) = capture.into_whole_seal_parts();
    let sealed = expectation
        .try_rejoin(seal_request.seal(&store)?)?
        .try_into_whole()?
        .persisted_receipt()
        .clone();
    backfill.accept_sealed_page(admission, sealed)?;
    assert_eq!(backfill.checkpoint().next_page(), 1);
    assert!(backfill.acquisition_completion().is_err());

    let encoded = backfill.checkpoint().to_json()?;
    let mut restored = source.restore_all_history_backfill(&encoded, &store)?;
    tokio::time::sleep(TEST_PROVIDER_RATE_SETTLE).await;
    let terminal_request = DiscoveryRequest::try_new(
        query.dataset().clone(),
        None,
        NonZeroU16::new(1).ok_or("nonzero result count")?,
        deadline,
    )?;
    assert!(matches!(
        source
            .fetch_next_all_history_page(
                &restored,
                authority.clone(),
                terminal_request.clone(),
                CancellationToken::new(),
            )
            .await,
        Err(market_squawk_sources::ExtractionSourceError::Source(
            market_squawk_sources::SourceError::InvalidProtocolState
        ))
    ));
    let terminal = source
        .fetch_next_all_history_page(
            &restored,
            authority,
            terminal_request,
            CancellationToken::new(),
        )
        .await?;
    assert!(terminal.terminal());
    assert!(terminal.canonical().is_none());
    let (_, capture, admission) = terminal.into_parts();
    let (expectation, seal_request) = capture.into_whole_seal_parts();
    let sealed = expectation
        .try_rejoin(seal_request.seal(&store)?)?
        .try_into_whole()?
        .persisted_receipt()
        .clone();
    restored.accept_sealed_page(admission, sealed)?;
    let completion = restored.acquisition_completion()?;
    assert_eq!(completion.response_count(), 2);
    assert_eq!(completion.sealed_pages().len(), 2);
    assert!(completion.source_rows() > 0);
    assert!(completion.canonical_points() > completion.source_rows());
    assert!(!completion.provider_snapshot_isolation_claimed());
    assert_eq!(
        completion.canonical_content_digests().count(),
        completion.sealed_pages().len() - 1
    );

    let completed_checkpoint = restored.checkpoint().to_json()?;
    let completed = source.restore_all_history_backfill(&completed_checkpoint, &store)?;
    assert_eq!(
        completed.acquisition_completion()?.completion_digest(),
        completion.completion_digest()
    );
    Ok(())
}

async fn exercise_fiscal_source(
    now: Timestamp,
    config: TreasurySourceConfig,
    first_payload: &[u8],
    second_payload: &[u8],
) -> TestResult<Vec<market_squawk_sources::ExtractionRecord>> {
    let metadata = metadata(now, &config, DataQuality::OfficialDelayed)?;
    let response = |payload: &[u8]| TreasuryHttpResponse {
        status: 200,
        retry_after: None,
        content_encoding: None,
        content_type: Some(b"application/json".to_vec()),
        body: Bytes::copy_from_slice(payload),
        received_at: now,
    };
    let transport = Arc::new(ScriptedTransport {
        responses: Mutex::new(VecDeque::from([
            response(first_payload),
            response(second_payload),
            response(first_payload),
            response(second_payload),
        ])),
        requested_urls: Mutex::new(Vec::new()),
    });
    let source =
        TreasurySource::try_new_with_transport(metadata.clone(), config, transport.clone())?;
    let catalog = source.dataset_catalog()?;
    let activation = source.activation_intent();
    assert_eq!(activation.catalog(), &catalog);
    assert_eq!(catalog.datasets().len(), 1);
    assert!(!catalog.surface().requires_credential());
    let provider_dataset = catalog.datasets()[0].provider_dataset().clone();
    let analytical_dataset = source.analytical_dataset_identifier(&provider_dataset)?;
    assert_ne!(provider_dataset, analytical_dataset);
    assert!(!analytical_dataset.as_str().contains(':'));
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(metadata.clone(), now)?;
    let authority = registry.extraction_authority(&registered, &source)?;
    let deadline = now.checked_add_nanos(60_000_000_000)?;
    let discovery = source
        .discover_with_accounting(
            authority.clone(),
            DiscoveryRequest::try_new(
                provider_dataset.clone(),
                None,
                NonZeroU16::new(1).ok_or("nonzero result count")?,
                deadline,
            )?,
            CancellationToken::new(),
        )
        .await?;
    assert!(discovery.accounting().extraction_ready());
    assert_eq!(discovery.accounting().source_object_count(), 1);
    assert_eq!(
        (
            discovery.accounting().canonical_points(),
            discovery.accounting().observed_numeric_points(),
            discovery.accounting().explicit_missing_points(),
        ),
        (2, 2, 0)
    );
    let object = discovery
        .batch()
        .objects()
        .first()
        .ok_or("missing discovered object")?
        .clone();
    let extraction_request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(10_000).ok_or("nonzero record count")?,
        NonZeroU64::new(16 * 1024 * 1024).ok_or("nonzero byte count")?,
        deadline,
    )?;
    assert!(matches!(
        source
            .extract(
                authority.clone(),
                extraction_request.clone(),
                CancellationToken::new(),
            )
            .await,
        Err(market_squawk_sources::ExtractionSourceError::Source(
            market_squawk_sources::SourceError::InvalidProtocolState
        ))
    ));
    let output = source
        .extract_with_capture(
            authority.clone(),
            extraction_request,
            CancellationToken::new(),
        )
        .await?;
    let payloads = [first_payload, second_payload];
    assert_eq!(output.capture_material().records().len(), payloads.len());
    for (record, payload) in output.capture_material().records().iter().zip(payloads) {
        assert_eq!(record.payload(), payload);
    }
    assert_eq!(
        output.capture_material().receipt().pages().len(),
        payloads.len()
    );
    assert_eq!(
        output.capture_material().receipt().terminal(),
        market_squawk_sources::ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
    );
    let canonical_points = u64::try_from(output.batch().records().len())?;
    assert_eq!(
        (
            output.accounting().aggregate_canonical_points(),
            output.accounting().aggregate_observed_numeric_points(),
            output.accounting().aggregate_explicit_missing_points(),
        ),
        (canonical_points, canonical_points, 0)
    );
    assert!(output.accounting().terminal_for_query());
    let distinct_receive_time = output
        .accounting()
        .terminal_received_at()
        .checked_add_nanos(1)?;
    let retry_capture = super::capture_material(
        &metadata,
        provider_dataset.clone(),
        output
            .capture_material()
            .receipt()
            .request_set_identity()
            .bytes(),
        distinct_receive_time,
        Bytes::copy_from_slice(payloads[0]),
    )?;
    assert_ne!(
        retry_capture.receipt().observation_digest(),
        output.capture_material().receipt().observation_digest()
    );
    let mismatched_batch = output
        .batch()
        .clone()
        .try_bind_provider_capture(retry_capture.receipt())?;
    assert!(matches!(
        output
            .accounting()
            .validate_common_publication(&mismatched_batch, &retry_capture),
        Err(crate::TreasuryVerticalError::InvalidExtractionHandoff)
    ));
    let (extraction, capture_material, native_lineage, row_capture_page_ordinals) =
        output.try_into_common_publication()?;
    assert_eq!(row_capture_page_ordinals, [0, 1]);
    assert_eq!(
        native_lineage.schema().implementation(),
        ProviderNativeLineageImplementation::UsTreasuryMacroV1
    );
    native_lineage.validate(&extraction)?;
    let native_batch: serde_json::Value = serde_json::from_slice(
        native_lineage
            .batch_sidecar()
            .ok_or("missing Treasury Fiscal native sidecar")?
            .semantic_payload(),
    )?;
    assert_eq!(native_batch["surface"], "fiscal_data");
    assert_eq!(native_batch["profile"], "average_interest_rates_v2");
    assert_eq!(native_batch["page_size"], 1);
    assert!(
        native_batch["schema"]["labels"]
            .as_array()
            .ok_or("missing Treasury Fiscal native labels")?
            .contains(&serde_json::json!(["record_date", "Record Date"]))
    );
    assert!(
        native_batch["schema"]["data_types"]
            .as_array()
            .ok_or("missing Treasury Fiscal native data types")?
            .contains(&serde_json::json!(["avg_interest_rate_amt", "PERCENTAGE"]))
    );
    assert!(
        native_batch["schema"]["data_formats"]
            .as_array()
            .ok_or("missing Treasury Fiscal native data formats")?
            .contains(&serde_json::json!(["avg_interest_rate_amt", "10.2%"]))
    );
    assert_eq!(native_batch["pages"][0]["page_number"], 1);
    assert_eq!(native_batch["pages"][0]["returned"], 1);
    assert_eq!(native_batch["pages"][1]["page_number"], 2);
    assert_eq!(native_batch["pages"][1]["returned"], 1);
    let first_native: serde_json::Value =
        serde_json::from_slice(native_lineage.rows()[0].semantic_payload())?;
    let second_native: serde_json::Value =
        serde_json::from_slice(native_lineage.rows()[1].semantic_payload())?;
    assert!(
        first_native["fields"]
            .as_array()
            .ok_or("missing first Treasury Fiscal native fields")?
            .contains(&serde_json::json!(["record_date", "2026-06-30"]))
    );
    assert!(
        second_native["fields"]
            .as_array()
            .ok_or("missing second Treasury Fiscal native fields")?
            .contains(&serde_json::json!(["record_date", "2026-07-01"]))
    );
    assert_ne!(first_native["row_identity"], second_native["row_identity"]);
    assert_eq!(
        market_squawk_sources::SourceObjectCaptureIdentity::try_from_capture(
            capture_material.receipt()
        )?,
        extraction.request().object().capture_identity()
    );
    assert_eq!(capture_material.records()[0].payload(), payloads[0]);
    let revisions = source.revision_plan(&extraction)?;
    assert!(revisions.is_locally_observed());
    assert!(revisions.native_lineage_required());

    let urls = transport
        .requested_urls
        .lock()
        .map_err(|_| "request log poisoned")?;
    assert_eq!(urls.len(), 4);

    Ok(extraction.records().to_vec())
}

fn assert_macro_record(
    record: &market_squawk_sources::ExtractionRecord,
    series: &str,
    value: &str,
    quality: DataQuality,
    expected_published: Option<&str>,
) -> TestResult {
    assert_eq!(record.schema().as_str(), "market-squawk-research-v3");
    let observation: ResearchObservation = serde_json::from_slice(record.payload())?;
    let ResearchObservation::Macro(observation) = observation else {
        return Err("expected macro observation".into());
    };
    assert_eq!(observation.series().as_str(), series);
    assert_eq!(observation.unit().as_str(), "percent");
    assert_eq!(
        observation
            .value()
            .observed_value()
            .map(|decimal| decimal.to_string())
            .as_deref(),
        Some(value)
    );
    assert_eq!(observation.context().provenance().quality(), quality);
    assert_eq!(
        observation
            .context()
            .provenance()
            .source_timestamp()
            .map(Timestamp::unix_nanos),
        expected_published
            .map(parse_expected_timestamp)
            .transpose()?,
    );
    assert_eq!(
        observation
            .context()
            .time()
            .published()
            .and_then(|coordinate| coordinate.exact_timestamp())
            .map(Timestamp::unix_nanos),
        expected_published
            .map(parse_expected_timestamp)
            .transpose()?,
    );
    assert_eq!(
        record
            .published_time()
            .and_then(|coordinate| coordinate.exact_timestamp())
            .map(Timestamp::unix_nanos),
        expected_published
            .map(parse_expected_timestamp)
            .transpose()?,
    );
    Ok(())
}

fn parse_expected_timestamp(value: &str) -> TestResult<i64> {
    let parsed = chrono::DateTime::parse_from_rfc3339(value)?;
    parsed
        .timestamp()
        .checked_mul(1_000_000_000)
        .and_then(|nanos| nanos.checked_add(i64::from(parsed.timestamp_subsec_nanos())))
        .ok_or_else(|| "expected timestamp overflow".into())
}

fn metadata(
    now: Timestamp,
    config: &TreasurySourceConfig,
    quality: DataQuality,
) -> TestResult<SourceMetadata> {
    metadata_with_rate(now, config, quality, 1, 1_000_000_000, 1)
}

fn metadata_with_rate(
    now: Timestamp,
    config: &TreasurySourceConfig,
    quality: DataQuality,
    requests_per_window: u32,
    window_nanos: u64,
    max_concurrent: u16,
) -> TestResult<SourceMetadata> {
    let effective = EffectiveInterval::new(now.checked_sub_nanos(1)?, None)?;
    let evidence = exact_evidence(b"treasury-test-metadata");
    let provider = SourceIdentifier::try_from("us-treasury")?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(SourceIdentifier::try_from("official-public-interface")?),
        evidence.clone(),
        effective,
    );
    let endpoint = match config {
        TreasurySourceConfig::AverageInterestRates(query) => ApiEndpointRule::try_new(
            query
                .page(1)?
                .url()
                .split('?')
                .next()
                .ok_or("missing path")?,
            PathScope::Exact,
            query_rules(&[
                ("fields", 1_024),
                ("filter", 512),
                ("sort", 128),
                ("page[number]", 20),
                ("page[size]", 5),
            ])?,
            5,
            4_096,
        )?,
        TreasurySourceConfig::DailyRates(config) => ApiEndpointRule::try_new(
            config
                .queries()
                .first()
                .ok_or("daily-rate query missing")?
                .page(0)?
                .url()
                .split('?')
                .next()
                .ok_or("missing path")?,
            PathScope::Exact,
            query_rules(&[
                ("data", 64),
                ("field_tdr_date_value", 4),
                ("field_tdr_date_value_month", 6),
                ("page", 20),
            ])?,
            3,
            512,
        )?,
    };
    let network = EndpointPolicy::try_from_api_rules(
        vec![endpoint],
        market_squawk_sources::HttpRequestBounds::default(),
    )?;
    let budget = ProviderBudgetPolicy::try_new_conjunctive(
        BudgetScope::new(provider.clone()),
        &[ProviderBudgetWindow::try_new(
            NonZeroU32::new(requests_per_window).ok_or("nonzero request budget")?,
            NonZeroU64::new(window_nanos).ok_or("nonzero request window")?,
            BudgetWindowSemantics::Sliding,
        )?],
        NonZeroU16::new(max_concurrent).ok_or("nonzero concurrency")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000).ok_or("nonzero backoff")?,
            NonZeroU64::new(60_000_000_000).ok_or("nonzero max backoff")?,
            0,
        )?,
    )?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from(format!("treasury-{}", quality_token(quality)))?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from(format!(
                "treasury-{}-test-v1",
                quality_token(quality)
            ))?),
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
        quality,
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

const fn quality_token(quality: DataQuality) -> &'static str {
    match quality {
        DataQuality::OfficialDelayed => "fiscal",
        _ => "invalid",
    }
}
