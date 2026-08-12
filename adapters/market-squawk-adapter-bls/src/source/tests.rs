use std::collections::VecDeque;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    EffectiveInterval, MetadataRevision, ResearchObservation, RevisionBoundPayloadEvidence,
    SchemaVersion, SequenceCapability, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    BackoffPolicy, BudgetScope, BudgetWindowSemantics, CoverageDomain, DiscoveryRequest,
    EndpointPolicy, ExtractionAuthority, ExtractionAuthorityError, ExtractionRequest,
    ExtractionSource, ExtractionSourceError, FreshnessPolicy, HistoricalCapability,
    NetworkAccessPolicy, PathScope, ProviderBudgetPolicy, ProviderBudgetWindow,
    ProviderCaptureTerminalDisposition, SourceCapabilities, SourceClass, SourceCoverage,
    SourceMetadata, SourceMetadataInput, SourceProtocolProfile,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::{BlsSource, PageCache, RetrievedBlsPage, exact_evidence, parse_object_id};
use crate::client::{BlsHttpRequest, BlsHttpResponse, BlsTransport, system_timestamp};
use crate::{
    BlsAccessTier, BlsAuthorization, BlsResponse, BlsSeriesMetadata, BlsSourceConfig,
    BlsSourceError,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const COMPLETE_RESPONSE: &[u8] = br#"{
  "status":"REQUEST_SUCCEEDED",
  "responseTime":1,
  "message":[],
  "Results":{"series":[{"seriesID":"LNS14000000","data":[{
    "year":"2026","period":"M06","periodName":"June","latest":"true",
    "value":"4.2","footnotes":[]
  }]}]}
}"#;

#[derive(Debug)]
struct ScriptedTransport {
    responses: Mutex<VecDeque<BlsHttpResponse>>,
    request_count: Mutex<u32>,
}

impl BlsTransport for ScriptedTransport {
    fn execute(
        &self,
        request: BlsHttpRequest,
        _max_bytes: usize,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<BlsHttpResponse, market_squawk_sources::ExtractionSourceError>> {
        Box::pin(async move {
            let body: serde_json::Value = serde_json::from_slice(&request.body)
                .map_err(|_| market_squawk_sources::SourceError::InvalidProtocolState)?;
            if request.url != BlsAuthorization::PublicV1.endpoint()
                || body["seriesid"][0] != "LNS14000000"
                || body["startyear"] != "2026"
                || body["endyear"] != "2026"
            {
                return Err(market_squawk_sources::SourceError::InvalidProtocolState.into());
            }
            let mut request_count = self
                .request_count
                .lock()
                .map_err(|_| market_squawk_sources::SourceError::InvalidProtocolState)?;
            *request_count = request_count.saturating_add(1);
            self.responses
                .lock()
                .map_err(|_| market_squawk_sources::SourceError::InvalidProtocolState)?
                .pop_front()
                .ok_or(market_squawk_sources::SourceError::InvalidProtocolState.into())
        })
    }
}

#[tokio::test]
async fn authority_bound_source_emits_canonical_period_precision() -> TestResult {
    let now = system_timestamp()?;
    let config = source_config()?;
    let transport = Arc::new(ScriptedTransport {
        responses: Mutex::new(VecDeque::from([
            complete_http_response(now),
            complete_http_response(now),
        ])),
        request_count: Mutex::new(0),
    });
    let invalid_metadata = source_metadata(now, &config, false)?;
    assert!(
        BlsSource::try_new_with_transport(invalid_metadata, config.clone(), transport.clone(),)
            .is_err()
    );

    let metadata = source_metadata(now, &config, true)?;
    let source = BlsSource::try_new_with_transport(metadata.clone(), config, transport.clone())?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(metadata, now)?;
    let authority = registry.extraction_authority(&registered, &source)?;
    let deadline = now.checked_add_nanos(60_000_000_000)?;
    let discovery = source
        .discover(
            authority.clone(),
            DiscoveryRequest::try_new(
                source.dataset().clone(),
                None,
                NonZeroU16::new(1).ok_or("nonzero discovery bound")?,
                deadline,
            )?,
            CancellationToken::new(),
        )
        .await?;
    let object = discovery
        .objects()
        .first()
        .ok_or("missing BLS source object")?
        .clone();
    let extraction_request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(10).ok_or("nonzero record bound")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero byte bound")?,
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
        Err(ExtractionSourceError::Source(
            market_squawk_sources::SourceError::InvalidProtocolState
        ))
    ));
    let normalized = source
        .normalized_page(&authority, &extraction_request, CancellationToken::new())
        .await?;
    assert_eq!(
        normalized.capture_material().records()[0].payload(),
        COMPLETE_RESPONSE
    );
    let output = source
        .extract_with_capture(authority, extraction_request, CancellationToken::new())
        .await?;
    let capture = output.capture_material();
    let capture_receipt = capture.receipt();
    assert_eq!(capture_receipt.source_id().as_str(), "bls-public-test");
    assert_eq!(capture_receipt.dataset(), source.dataset());
    assert_eq!(
        capture_receipt.terminal(),
        ProviderCaptureTerminalDisposition::StandaloneResponse
    );
    assert_eq!(
        capture_receipt.total_body_bytes(),
        u64::try_from(COMPLETE_RESPONSE.len())?
    );
    assert_eq!(capture_receipt.pages().len(), 1);
    let page_receipt = &capture_receipt.pages()[0];
    assert_eq!(page_receipt.ordinal(), 0);
    assert_eq!(page_receipt.http_status(), 200);
    assert_eq!(page_receipt.received_at(), now);
    let expected_body_digest: [u8; 32] = Sha256::digest(COMPLETE_RESPONSE).into();
    assert_eq!(page_receipt.body_digest().bytes(), expected_body_digest);
    assert_eq!(capture.records().len(), 1);
    let raw = &capture.records()[0];
    assert_eq!(raw.source(), "bls-public-test");
    assert_eq!(raw.source_sequence(), Some(0));
    assert!(raw.exchange_at().is_none());
    assert_eq!(
        raw.received_at().timestamp_nanos_opt(),
        Some(now.unix_nanos())
    );
    assert_eq!(raw.payload(), COMPLETE_RESPONSE);
    assert!(!raw.event_id().is_nil());
    assert!(!raw.connection_id().is_nil());

    let extraction = output.batch();
    let revisions = source.revision_plan(extraction)?;
    assert_eq!(extraction.records().len(), 1);
    assert!(revisions.is_locally_observed());
    let record = &extraction.records()[0];
    assert_eq!(record.schema().as_str(), "market-squawk-research-v3");
    assert!(record.published_time().is_none());
    assert_eq!(record.available_at(), Some(now));
    let period = record
        .effective_time()
        .source_period_value()
        .ok_or("BLS effective time lost source-period precision")?;
    assert_eq!(period.scheme().as_str(), "bls-monthly");
    assert_eq!(
        (
            period.year(),
            period.ordinal().get(),
            period.code().as_str()
        ),
        (2026, 6, "M06")
    );

    let ResearchObservation::Macro(observation) = serde_json::from_slice(record.payload())? else {
        return Err("expected canonical macro observation".into());
    };
    assert_eq!(observation.series().as_str(), "LNS14000000");
    assert_eq!(observation.unit().as_str(), "percent");
    assert_eq!(
        observation.context().provenance().quality(),
        DataQuality::OfficialDelayed
    );
    assert!(
        observation
            .context()
            .provenance()
            .source_timestamp()
            .is_none()
    );
    assert_eq!(
        observation
            .value()
            .observed_value()
            .map(|value| value.to_string()),
        Some("4.2".to_owned())
    );
    assert_eq!(
        *transport
            .request_count
            .lock()
            .map_err(|_| "request log poisoned")?,
        2
    );
    let health = source.health()?;
    assert!(health.last_attempt_at().is_some());
    assert_eq!(health.last_success_at(), Some(now));
    assert!(health.last_payload_digest().is_some());
    assert_eq!(health.consecutive_failures(), 0);
    Ok(())
}

#[tokio::test]
async fn authority_loss_during_completed_work_prevents_every_publication() -> TestResult {
    let now = system_timestamp()?;

    let (source, registry, authority, deadline) = source_harness(now)?;
    source.queue_test_publication_action(Some(registry))?;
    let discovery = source
        .discover(
            authority,
            discovery_request(&source, deadline)?,
            CancellationToken::new(),
        )
        .await;
    assert_not_current(discovery)?;

    let (source, registry, authority, deadline) = source_harness(now)?;
    let object = discover_object(&source, authority.clone(), deadline).await?;
    let request = extraction_request(object, deadline)?;
    source.queue_test_publication_action(Some(registry))?;
    let normalized = source
        .normalized_page(&authority, &request, CancellationToken::new())
        .await;
    assert_not_current(normalized)?;

    let (source, registry, authority, deadline) = source_harness(now)?;
    let object = discover_object(&source, authority.clone(), deadline).await?;
    source.queue_test_publication_action(None)?;
    source.queue_test_publication_action(Some(registry))?;
    let extraction = source
        .extract_with_capture(
            authority,
            extraction_request(object, deadline)?,
            CancellationToken::new(),
        )
        .await;
    assert_not_current(extraction)?;
    Ok(())
}

fn source_harness(
    now: Timestamp,
) -> TestResult<(
    BlsSource,
    AuthoritativeSourceRegistry,
    ExtractionAuthority,
    Timestamp,
)> {
    let config = source_config()?;
    let metadata = source_metadata(now, &config, true)?;
    let transport = Arc::new(ScriptedTransport {
        responses: Mutex::new(VecDeque::from([
            complete_http_response(now),
            complete_http_response(now),
        ])),
        request_count: Mutex::new(0),
    });
    let source = BlsSource::try_new_with_transport(metadata.clone(), config, transport)?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(metadata, now)?;
    let authority = registry.extraction_authority(&registered, &source)?;
    Ok((
        source,
        registry,
        authority,
        now.checked_add_nanos(60_000_000_000)?,
    ))
}

fn complete_http_response(received_at: Timestamp) -> BlsHttpResponse {
    BlsHttpResponse {
        status: 200,
        retry_after: None,
        content_encoding: None,
        content_type: Some(b"application/json".to_vec()),
        body: Bytes::from_static(COMPLETE_RESPONSE),
        received_at,
    }
}

fn discovery_request(source: &BlsSource, deadline: Timestamp) -> TestResult<DiscoveryRequest> {
    Ok(DiscoveryRequest::try_new(
        source.dataset().clone(),
        None,
        NonZeroU16::new(1).ok_or("nonzero discovery bound")?,
        deadline,
    )?)
}

async fn discover_object(
    source: &BlsSource,
    authority: ExtractionAuthority,
    deadline: Timestamp,
) -> TestResult<market_squawk_sources::SourceObject> {
    source
        .discover(
            authority,
            discovery_request(source, deadline)?,
            CancellationToken::new(),
        )
        .await?
        .objects()
        .first()
        .cloned()
        .ok_or_else(|| "missing BLS source object".into())
}

fn extraction_request(
    object: market_squawk_sources::SourceObject,
    deadline: Timestamp,
) -> TestResult<ExtractionRequest> {
    Ok(ExtractionRequest::try_new(
        object,
        NonZeroU32::new(10).ok_or("nonzero record bound")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero byte bound")?,
        deadline,
    )?)
}

fn assert_not_current<T>(result: Result<T, ExtractionSourceError>) -> TestResult {
    if matches!(
        result,
        Err(ExtractionSourceError::Authority(
            ExtractionAuthorityError::NotCurrent
        ))
    ) {
        Ok(())
    } else {
        Err("stale extraction authority published a BLS result".into())
    }
}

fn source_config() -> Result<BlsSourceConfig, BlsSourceError> {
    const METADATA: &[u8] = br#"{
      "schema_version":1,
      "series_id":"LNS14000000",
      "title":"Unemployment Rate",
      "unit":"percent",
      "frequency":"monthly",
      "seasonal_adjustment":"seasonally-adjusted",
      "measure":"rate"
    }"#;
    let series = BlsSeriesMetadata::parse_exact(
        Bytes::from_static(METADATA),
        exact_evidence(METADATA),
        SourceIdentifier::try_from("user-approved:bls-series-metadata:2026-07-21")
            .map_err(|_| BlsSourceError::InvalidSeriesMetadata)?,
    )?;
    BlsSourceConfig::try_new(BlsAuthorization::PublicV1, vec![series], 2026, 2026)
}

fn source_metadata(
    now: Timestamp,
    config: &BlsSourceConfig,
    exact_conjunctive_budget: bool,
) -> TestResult<SourceMetadata> {
    let effective = EffectiveInterval::new(now.checked_sub_nanos(1)?, None)?;
    let evidence = exact_evidence(b"bls-test-metadata");
    let provider = SourceIdentifier::try_from("us-bls")?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(SourceIdentifier::try_from("official-public-interface")?),
        evidence.clone(),
        effective,
    );
    let backoff = BackoffPolicy::try_new(
        NonZeroU64::new(1_000_000).ok_or("nonzero backoff")?,
        NonZeroU64::new(60_000_000_000).ok_or("nonzero max backoff")?,
        0,
    )?;
    let scope = BudgetScope::for_authorization(provider.clone(), &authorization)?;
    let budget = if exact_conjunctive_budget {
        ProviderBudgetPolicy::try_new_conjunctive(
            scope,
            &[
                ProviderBudgetWindow::try_new(
                    NonZeroU32::new(50).ok_or("nonzero short budget")?,
                    NonZeroU64::new(10_000_000_000).ok_or("nonzero short window")?,
                    BudgetWindowSemantics::Sliding,
                )?,
                ProviderBudgetWindow::try_new(
                    NonZeroU32::new(u32::from(config.limits().daily_queries()))
                        .ok_or("nonzero daily budget")?,
                    NonZeroU64::new(86_400_000_000_000).ok_or("nonzero daily window")?,
                    BudgetWindowSemantics::Sliding,
                )?,
            ],
            NonZeroU16::new(2).ok_or("nonzero concurrency")?,
            backoff,
        )?
    } else {
        ProviderBudgetPolicy::try_new(
            scope,
            NonZeroU32::new(u32::from(config.limits().daily_queries()))
                .ok_or("nonzero daily budget")?,
            NonZeroU64::new(86_400_000_000_000).ok_or("nonzero daily window")?,
            NonZeroU16::new(2).ok_or("nonzero concurrency")?,
            backoff,
        )?
    };
    let endpoint = ApiEndpointRule::try_new(
        BlsAuthorization::PublicV1.endpoint(),
        PathScope::Exact,
        Vec::new(),
        1,
        1,
    )?;
    let network = EndpointPolicy::try_from_api_rules(
        vec![endpoint],
        market_squawk_sources::HttpRequestBounds::default(),
    )?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("bls-public-test")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("bls-public-test-v1")?),
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

#[test]
fn object_id_requires_exact_lowercase_sha256() -> TestResult {
    let lowercase = SourceIdentifier::try_from(format!("bls:0:{}", "a".repeat(64)))?;
    assert_eq!(parse_object_id(&lowercase)?.0, 0);

    let uppercase = SourceIdentifier::try_from(format!("bls:0:{}", "A".repeat(64)))?;
    assert!(parse_object_id(&uppercase).is_err());
    Ok(())
}

fn page(bytes: &'static [u8], digest: &str) -> TestResult<RetrievedBlsPage> {
    Ok(RetrievedBlsPage {
        bytes: Bytes::from_static(bytes),
        response: BlsResponse::parse(
            include_bytes!("../../fixtures/series.json"),
            BlsAccessTier::PublicV1,
        )?,
        received_at: Timestamp::from_unix_nanos(1),
        sha256_hex: digest.to_owned(),
    })
}

#[test]
fn full_cache_skips_new_pages_without_crossing_its_bound() -> TestResult {
    let first_id = SourceIdentifier::try_from("bls:first")?;
    let second_id = SourceIdentifier::try_from("bls:second")?;

    let first = page(b"1234", "first")?;
    let first_charge = PageCache::retained_charge(&first_id, &first)?;
    let mut cache = PageCache::with_limit(first_charge);

    assert!(cache.insert(&first_id, &first)?);
    assert!(!cache.insert(&second_id, &page(b"5", "second")?)?);
    assert_eq!(cache.retained_bytes, first_charge);
    assert!(cache.pages.contains_key(first_id.as_str()));
    assert!(!cache.pages.contains_key(second_id.as_str()));
    Ok(())
}
