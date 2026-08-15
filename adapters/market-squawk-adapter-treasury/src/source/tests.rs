use std::collections::VecDeque;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
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
    NetworkAccessPolicy, PathScope, ProviderBudgetPolicy, ProviderBudgetWindow, QueryParameterRule,
    QuerySensitivity, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataInput, SourceProtocolProfile,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::client::{
    TreasuryHttpRequest, TreasuryHttpResponse, TreasuryTransport, system_timestamp,
};
use crate::{
    TreasuryDailyRateFamily, TreasuryDailyRateQuery, TreasuryDailyRatesConfig,
    TreasuryExtractionCommitment, TreasuryFiscalQuery, TreasuryOwnerUseAttestation,
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
            self.responses
                .lock()
                .map_err(|_| super::TreasurySourceError::InvalidProtocol)?
                .pop_front()
                .ok_or(super::TreasurySourceError::InvalidProtocol)
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
    let mut fiscal_payload: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../fixtures/average_interest_rates.json"))?;
    fiscal_payload["meta"]["total-count"] = serde_json::json!(1);
    fiscal_payload["meta"]["total-pages"] = serde_json::json!(1);
    fiscal_payload["links"]["next"] = serde_json::Value::Null;
    fiscal_payload["links"]["last"] = serde_json::json!("&page%5Bnumber%5D=1&page%5Bsize%5D=1");
    let fiscal_payload = serde_json::to_vec(&fiscal_payload)?;
    let fiscal = exercise_source(
        now,
        fiscal_config,
        DataQuality::OfficialDelayed,
        true,
        &fiscal_payload,
        b"application/json",
    )
    .await?;
    assert_eq!(fiscal.len(), 1);
    assert_macro_record(
        &fiscal[0],
        "treasury:average-interest-rate:v2:Marketable:Treasury%20Bills",
        "3.706",
        DataQuality::OfficialDelayed,
        None,
    )?;

    let yield_config = TreasurySourceConfig::daily_par_yield_curve(2026)?;
    let yield_records = exercise_source(
        now,
        yield_config,
        DataQuality::OfficialDelayed,
        false,
        include_bytes!("../../fixtures/daily_par_yield_curve.xml"),
        b"application/atom+xml",
    )
    .await?;
    assert!(yield_records.len() >= 2);
    let one_month = yield_records
        .iter()
        .find(|record| {
            serde_json::from_slice::<ResearchObservation>(record.payload())
                .is_ok_and(|observation| {
                    matches!(observation, ResearchObservation::Macro(value) if value.series().as_str() == "treasury:daily-par-yield-curve:1m")
                })
        })
        .ok_or("missing one-month canonical yield")?;
    assert_macro_record(
        one_month,
        "treasury:daily-par-yield-curve:1m",
        "3.72",
        DataQuality::OfficialDelayed,
        Some("2026-07-21T06:54:08+00:00"),
    )?;
    Ok(())
}

#[tokio::test]
async fn fiscal_discovery_rejects_a_result_limit_before_the_provider_terminal_page() -> TestResult {
    let _provider_rate_guard = TEST_PROVIDER_RATE_SERIAL.lock().await;
    let now = system_timestamp()?;
    let query = TreasuryFiscalQuery::average_interest_rates_v2(
        CalendarDate::new(2026, 1, 1)?,
        CalendarDate::new(2026, 12, 31)?,
        NonZeroU16::new(1).ok_or("nonzero page size")?,
    )?;
    let config = TreasurySourceConfig::average_interest_rates(query.clone());
    let source_metadata = metadata(now, &config, DataQuality::OfficialDelayed)?;
    let transport = Arc::new(ScriptedTransport {
        responses: Mutex::new(VecDeque::from([TreasuryHttpResponse {
            status: 200,
            retry_after: None,
            content_encoding: None,
            content_type: Some(b"application/json".to_vec()),
            body: Bytes::from_static(include_bytes!("../../fixtures/average_interest_rates.json")),
            received_at: now,
        }])),
        requested_urls: Mutex::new(Vec::new()),
    });
    let source = TreasurySource::try_new_with_transport(
        source_metadata.clone(),
        config,
        owner_use_attestation(now)?,
        transport.clone(),
    )?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(source_metadata, now)?;
    let authority = registry.extraction_authority(&registered, &source)?;
    let deadline = now.checked_add_nanos(60_000_000_000)?;
    tokio::time::sleep(TEST_PROVIDER_RATE_SETTLE).await;
    assert!(
        source
            .discover_with_accounting(
                authority,
                DiscoveryRequest::try_new(
                    query.dataset()?,
                    None,
                    NonZeroU16::new(1).ok_or("nonzero result count")?,
                    deadline,
                )?,
                CancellationToken::new(),
            )
            .await
            .is_err()
    );
    assert_eq!(
        transport
            .requested_urls
            .lock()
            .map_err(|_| "request log poisoned")?
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn all_history_requires_each_raw_page_seal_and_restores_before_terminal() -> TestResult {
    let _provider_rate_guard = TEST_PROVIDER_RATE_SERIAL.lock().await;
    let now = system_timestamp()?;
    let terminal_at = now.checked_add_nanos(1)?;
    let family = TreasuryDailyRateFamily::NominalParYieldCurve;
    let query = TreasuryDailyRateQuery::all_history(family)?;
    let config =
        TreasurySourceConfig::daily_rates(TreasuryDailyRatesConfig::try_new([query.clone()])?);
    let source_metadata = metadata(now, &config, DataQuality::OfficialDelayed)?;
    let terminal = format!(
        r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>{}</title>
  <id>{}</id>
  <updated>2026-07-26T16:21:25Z</updated>
</feed>"#,
        family.feed_title(),
        family.feed_identity(),
    );
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
                body: Bytes::from(terminal),
                received_at: terminal_at,
            },
        ])),
        requested_urls: Mutex::new(Vec::new()),
    });
    let source = TreasurySource::try_new_with_transport(
        source_metadata.clone(),
        config,
        owner_use_attestation(now)?,
        transport,
    )?;
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
        canonical.accounting().canonical_points(),
        u64::try_from(canonical.batch().records().len())?
    );
    let (_, capture, admission) = first.into_parts();
    let sealed = capture.seal(&store)?;
    backfill.accept_sealed_page(admission, sealed)?;
    assert_eq!(backfill.checkpoint().next_page(), 1);
    assert!(backfill.acquisition_completion().is_err());

    let encoded = backfill.checkpoint().to_json()?;
    let mut restored = source.restore_all_history_backfill(&encoded, &store)?;
    tokio::time::sleep(TEST_PROVIDER_RATE_SETTLE).await;
    let terminal = source
        .fetch_next_all_history_page(
            &restored,
            authority,
            DiscoveryRequest::try_new(
                query.dataset().clone(),
                None,
                NonZeroU16::new(1).ok_or("nonzero result count")?,
                deadline,
            )?,
            CancellationToken::new(),
        )
        .await?;
    assert!(terminal.terminal());
    assert!(terminal.canonical().is_none());
    let (_, capture, admission) = terminal.into_parts();
    let sealed = capture.seal(&store)?;
    restored.accept_sealed_page(admission, sealed)?;
    let completion = restored.acquisition_completion()?;
    assert_eq!(completion.response_count(), 2);
    assert_eq!(completion.sealed_pages().len(), 2);
    assert!(completion.source_rows() > 0);
    assert!(completion.canonical_points() > completion.source_rows());
    assert!(!completion.provider_snapshot_isolation_claimed());
    let expectation = source.all_history_publication_expectation(&completion)?;
    assert_eq!(
        expectation.expected_append_rows(),
        completion.canonical_points()
    );
    assert_eq!(
        expectation.expected_extraction_contents().len(),
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

async fn exercise_source(
    now: Timestamp,
    config: TreasurySourceConfig,
    quality: DataQuality,
    expected_locally_observed_revisions: bool,
    payload: &[u8],
    content_type: &[u8],
) -> TestResult<Vec<market_squawk_sources::ExtractionRecord>> {
    tokio::time::sleep(TEST_PROVIDER_RATE_SETTLE).await;
    let metadata = metadata(now, &config, quality)?;
    let response_body = Bytes::copy_from_slice(payload);
    let response_content_type = content_type.to_vec();
    let response = || TreasuryHttpResponse {
        status: 200,
        retry_after: None,
        content_encoding: None,
        content_type: Some(response_content_type.clone()),
        body: response_body.clone(),
        received_at: now,
    };
    let transport = Arc::new(ScriptedTransport {
        responses: Mutex::new(VecDeque::from([response(), response(), response()])),
        requested_urls: Mutex::new(Vec::new()),
    });
    let source = TreasurySource::try_new_with_transport(
        metadata.clone(),
        config,
        owner_use_attestation(now)?,
        transport.clone(),
    )?;
    let catalog = source.dataset_catalog()?;
    let activation = source.activation_intent();
    assert_eq!(activation.catalog(), &catalog);
    assert_eq!(catalog.datasets().len(), 1);
    assert!(!catalog.surface().requires_credential());
    let provider_dataset = source.dataset()?;
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
    assert!(discovery.accounting().publication_expectation_ready());
    assert_eq!(discovery.accounting().source_object_count(), 1);
    assert!(discovery.accounting().canonical_points() >= 1);
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
    tokio::time::sleep(TEST_PROVIDER_RATE_SETTLE).await;
    let output = source
        .extract_with_capture(
            authority.clone(),
            extraction_request,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(output.capture_material().records().len(), 1);
    assert_eq!(output.capture_material().records()[0].payload(), payload);
    assert_eq!(
        output.accounting().canonical_points(),
        u64::try_from(output.batch().records().len())?
    );
    assert!(output.accounting().terminal_for_query());
    let temporary = TemporaryDirectory::new();
    let store = LocalPaths::prepare(temporary.path())?.sealed_research_journal_store()?;
    let distinct_receive_time = output.accounting().received_at().checked_add_nanos(1)?;
    let retry_capture = super::capture_material(
        &metadata,
        provider_dataset.clone(),
        output
            .capture_material()
            .receipt()
            .request_set_identity()
            .bytes(),
        distinct_receive_time,
        Bytes::copy_from_slice(payload),
    )?;
    assert_eq!(
        retry_capture.receipt().content_digest(),
        output.capture_material().receipt().content_digest()
    );
    assert_ne!(
        retry_capture.receipt().observation_digest(),
        output.capture_material().receipt().observation_digest()
    );
    let mismatched_batch = output
        .batch()
        .clone()
        .try_bind_provider_capture(output.capture_material().receipt())?;
    let mismatched_accounting = output.accounting().clone();
    let mismatched_seal = retry_capture.seal(&store)?;
    assert!(
        TreasuryExtractionCommitment::try_from_output(
            &mismatched_batch,
            mismatched_accounting,
            mismatched_seal,
        )
        .is_err()
    );
    let sealed_output = output.seal_for_publication(&store)?;
    let (extraction, commitment) = sealed_output.into_parts();
    let expectation = source.publication_expectation(discovery.accounting(), [commitment])?;
    assert_eq!(
        expectation.expected_append_rows(),
        u64::try_from(extraction.records().len())?
    );
    assert_eq!(expectation.expected_extraction_contents().len(), 1);
    let revisions = source.revision_plan(&extraction)?;
    assert_eq!(
        revisions.is_locally_observed(),
        expected_locally_observed_revisions
    );
    tokio::time::sleep(TEST_PROVIDER_RATE_SETTLE).await;
    let doctor = source
        .run_doctor(authority, deadline, CancellationToken::new())
        .await?;
    assert_eq!(doctor.probe_count(), 1);
    let doctor = doctor.seal(&store)?;
    assert_eq!(
        doctor.activation_ready(),
        expected_locally_observed_revisions
    );
    assert_eq!(doctor.sealed_captures().len(), 1);

    let urls = transport
        .requested_urls
        .lock()
        .map_err(|_| "request log poisoned")?;
    assert_eq!(urls.len(), 3);
    assert_eq!(urls[0], urls[1]);
    assert_eq!(urls[1], urls[2]);
    if !expected_locally_observed_revisions {
        assert!(!urls[0].contains("page="));
        assert!(!urls[0].contains("page%5B"));
    }

    Ok(extraction.records().to_vec())
}

fn owner_use_attestation(now: Timestamp) -> TestResult<TreasuryOwnerUseAttestation> {
    Ok(TreasuryOwnerUseAttestation::try_private_personal_research(
        EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(b"owner-authorized-private-treasury-research").into(),
        ),
        now,
    )?)
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
            NonZeroU32::new(1).ok_or("nonzero request budget")?,
            NonZeroU64::new(1_000_000_000).ok_or("nonzero request window")?,
            BudgetWindowSemantics::Sliding,
        )?],
        NonZeroU16::new(1).ok_or("nonzero concurrency")?,
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
