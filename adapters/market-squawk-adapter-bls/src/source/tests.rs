use std::collections::VecDeque;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    EffectiveInterval, MetadataRevision, ResearchObservation, RevisionBoundPayloadEvidence,
    SchemaVersion, SequenceCapability, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::LocalPaths;
use market_squawk_sources::{
    ApiEndpointRule, AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    AvailabilityEvidence, BackoffPolicy, BudgetScope, CoverageDomain, DiscoveryRequest,
    EndpointPolicy, ExtractionAuthority, ExtractionAuthorityError, ExtractionRequest,
    ExtractionSource, ExtractionSourceError, FreshnessPolicy, HistoricalCapability,
    NetworkAccessPolicy, PathScope, ProviderBudgetPolicy,
    ProviderCaptureTerminalDisposition, SourceCapabilities, SourceClass, SourceCoverage,
    SourceMetadata, SourceMetadataInput, SourceObjectCaptureIdentity, SourceProtocolProfile,
    CURRENT_RESEARCH_RECORD_SCHEMA,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{BlsSource, PageCache, RetrievedBlsPage, exact_evidence, parse_object_id};
use crate::client::{BlsHttpRequest, BlsHttpResponse, BlsTransport, system_timestamp};
use crate::{
    BlsAccessTier, BlsAuthorization, BlsDoctorReadiness, BlsResponse, BlsSeriesMetadata,
    BlsRootRightsRejoin, BlsSourceConfig, BlsSourceError, BlsUsagePolicy,
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

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("market-squawk-bls-{}", Uuid::new_v4())))
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
            if request.url != BlsAuthorization::public_v1().endpoint()
                || body["seriesid"][0] != "LNS14000000"
                || body["startyear"].as_str().is_none()
                || body["endyear"].as_str().is_none()
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
    let invalid_config = source_config_for_years(2007, 2026)?;
    let transport = Arc::new(ScriptedTransport {
        responses: Mutex::new(VecDeque::from([
            complete_http_response(now),
            complete_http_response_for_year(now, 2016),
            complete_http_response(now),
        ])),
        request_count: Mutex::new(0),
    });
    let invalid_metadata = source_metadata(now, &invalid_config, false)?;
    assert!(
        BlsSource::try_new_with_transport(invalid_metadata, invalid_config, transport.clone(),)
            .is_err()
    );

    let config = source_config_for_years(2007, 2026)?;
    let metadata = source_metadata(now, &config, true)?;
    let source = BlsSource::try_new_with_transport(metadata.clone(), config, transport.clone())?;
    let activation_plan = source.activation_plan()?;
    assert_eq!(activation_plan.source_id(), metadata.source_id());
    assert_eq!(activation_plan.metadata_revision(), metadata.revision());
    assert_eq!(activation_plan.provider_dataset(), source.dataset());
    let presentation = BlsUsagePolicy::private_personal_research_no_distribution()?
        .presentation_obligation()?;
    presentation.validate()?;
    assert_eq!(presentation.source_attribution(), crate::BLS_SOURCE_ATTRIBUTION);
    assert!(presentation.retrieval_date_required());
    assert!(presentation.truthful_representation_required());
    assert!(presentation.provider_limit_compliance_required());
    assert_eq!(
        activation_plan.presentation_obligation_digest(),
        presentation.obligation_digest()
    );
    assert!(activation_plan.rate().persistent_shared_authority_required());
    assert!(activation_plan.rate().counts_all_started_attempts());
    assert_eq!(activation_plan.rate().documented_requests_per_ten_seconds(), 50);
    assert_eq!(activation_plan.rate().documented_requests_per_day(), 25);
    assert_eq!(activation_plan.rate().application_requests_per_second(), 1);
    assert_eq!(activation_plan.rate().application_requests_per_day(), 25);
    assert_eq!(activation_plan.rate().maximum_in_flight(), 1);
    assert_eq!(activation_plan.rate().maximum_backoff_nanos(), 60_000_000_000);
    assert_eq!(
        activation_plan.rate().declaration_digest(),
        activation_plan
            .rate()
            .shared_rate_declaration()
            .declaration_digest()
    );
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(metadata, now)?;
    let authority = registry.extraction_authority(&registered, &source)?;
    let deadline = now.checked_add_nanos(60_000_000_000)?;
    let doctor = source
        .doctor(authority.clone(), deadline, CancellationToken::new())
        .await?;
    let doctor_report = doctor.report();
    assert_eq!(doctor_report.readiness(), BlsDoctorReadiness::Available);
    assert_eq!(doctor_report.series_id().as_str(), "LNS14000000");
    assert_eq!(doctor_report.year(), 2026);
    assert_eq!(doctor_report.returned_series(), 1);
    assert_eq!(doctor_report.returned_observations(), 1);
    assert_eq!(doctor_report.observed_values(), 1);
    assert_eq!(doctor_report.missing_values(), 0);
    assert_eq!(doctor_report.preliminary_values(), 0);
    assert_eq!(doctor_report.provider_messages(), 0);
    assert_eq!(
        doctor_report.response_bytes(),
        u64::try_from(COMPLETE_RESPONSE.len())?
    );
    assert_eq!(doctor_report.limits().enforced_requests_per_second(), 1);
    assert_eq!(
        doctor_report.presentation_obligation_digest(),
        presentation.obligation_digest()
    );
    assert_eq!(
        doctor.capture_material().records()[0].payload(),
        COMPLETE_RESPONSE
    );
    assert_eq!(
        doctor.capture_material().receipt().content_digest(),
        doctor_report.capture_content_digest()
    );
    assert_ne!(doctor_report.report_digest().bytes(), [0; 32]);
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path())?;
    let store = paths.sealed_research_journal_store()?;
    let (doctor_report, doctor_capture) = doctor.into_parts();
    let sealed_doctor_capture = doctor_capture.seal(&store)?;
    let activation = source.activation_candidate(doctor_report, sealed_doctor_capture)?;
    source.validate_activation_candidate(&activation)?;
    assert!(
        activation
            .validate(
                &activation_plan,
                activation.expires_at(),
                &source.runtime_instance,
            )
            .is_err()
    );
    assert_eq!(activation.plan(), &activation_plan);
    assert_eq!(
        activation.sealed_doctor_capture().capture().dataset(),
        source.dataset()
    );
    let discovery_request = DiscoveryRequest::try_new(
        source.dataset().clone(),
        None,
        NonZeroU16::new(2).ok_or("nonzero discovery bound")?,
        deadline,
    )?;
    assert!(matches!(
        source
            .discover(
                authority.clone(),
                discovery_request.clone(),
                CancellationToken::new(),
            )
            .await,
        Err(ExtractionSourceError::Source(
            market_squawk_sources::SourceError::InvalidProtocolState
        ))
    ));
    let overlong_discovery = DiscoveryRequest::try_new(
        source.dataset().clone(),
        None,
        NonZeroU16::new(2).ok_or("nonzero discovery bound")?,
        activation.expires_at(),
    )?;
    assert!(matches!(
        source
            .discover_with_activation(
                authority.clone(),
                overlong_discovery,
                &activation,
                CancellationToken::new(),
            )
            .await,
        Err(ExtractionSourceError::Source(
            market_squawk_sources::SourceError::InvalidProtocolState
        ))
    ));
    let discovery = source
        .discover_with_activation(
            authority.clone(),
            discovery_request,
            &activation,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        discovery.capture_material().receipt().terminal(),
        ProviderCaptureTerminalDisposition::CompleteRequestGraph
    );
    assert_eq!(discovery.capture_material().receipt().pages().len(), 2);
    assert_eq!(
        discovery
            .capture_material()
            .receipt()
            .request_graph_components()
            .len(),
        2
    );
    assert_eq!(discovery.capture_material().records().len(), 2);
    assert_eq!(discovery.capture_material().records()[1].payload(), COMPLETE_RESPONSE);
    let object = discovery
        .batch()
        .objects()
        .get(1)
        .ok_or("missing BLS source object")?
        .clone();
    let SourceObjectCaptureIdentity::Paged {
        content_digest,
        page_count,
        terminal,
    } = object.capture_identity()
    else {
        return Err("BLS discovery object lost exact capture lineage".into());
    };
    let discovery_component = &discovery
        .capture_material()
        .receipt()
        .request_graph_components()[1];
    assert_eq!(content_digest, discovery_component.content_digest());
    assert_eq!(page_count, discovery_component.page_count());
    assert_eq!(terminal, discovery_component.terminal());
    assert_eq!(
        object.availability(),
        &AvailabilityEvidence::LocalFirstObserved { observed_at: now }
    );
    let (_discovery_batch, discovery_capture) = discovery.into_parts();
    let sealed_discovery_capture = discovery_capture.seal(&store)?;
    assert_ne!(sealed_discovery_capture.receipt_digest().bytes(), [0; 32]);
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
        .normalized_page(
            &authority,
            &extraction_request,
            &activation,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        normalized.capture_material().records()[0].payload(),
        COMPLETE_RESPONSE
    );
    let output = source
        .extract_with_capture(
            authority,
            extraction_request,
            &activation,
            CancellationToken::new(),
        )
        .await?;
    let (extraction, capture) = output.into_parts();
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
    assert_eq!(page_receipt.received_at(), extraction_received_at);
    let expected_body_digest: [u8; 32] = Sha256::digest(COMPLETE_RESPONSE).into();
    assert_eq!(page_receipt.body_digest().bytes(), expected_body_digest);
    assert_eq!(capture.records().len(), 1);
    let raw = &capture.records()[0];
    assert_eq!(raw.source(), "bls-public-test");
    assert_eq!(raw.source_sequence(), Some(0));
    assert!(raw.exchange_at().is_none());
    assert_eq!(
        raw.received_at().timestamp_nanos_opt(),
        Some(extraction_received_at.unix_nanos())
    );
    assert_eq!(raw.payload(), COMPLETE_RESPONSE);
    assert!(!raw.event_id().is_nil());
    assert!(!raw.connection_id().is_nil());

    let sealed_capture = capture.seal(&store)?;
    let publication = source.publication_candidate(&extraction, &sealed_capture, &activation)?;
    source.validate_publication_candidate(&publication, &extraction, &activation)?;
    assert_eq!(publication.provider_dataset(), source.dataset());
    assert_eq!(
        publication.analytical_dataset(),
        activation_plan.analytical_dataset()
    );
    assert_eq!(publication.chunk_index(), 1);
    assert_eq!(publication.total_chunks(), 2);
    assert_eq!(publication.canonical_record_count(), 1);
    assert_eq!(publication.first_observed_at(), now);
    assert_eq!(publication.response_received_at(), extraction_received_at);
    assert!(publication.canonical_ingested_at() >= extraction_received_at);
    assert_eq!(
        publication.activation_candidate_digest(),
        activation.candidate_digest()
    );
    assert_eq!(
        publication.doctor_report_digest(),
        activation.doctor_report().report_digest()
    );
    assert_eq!(
        publication.sealed_doctor_capture_receipt_digest(),
        activation.sealed_doctor_capture().receipt_digest()
    );
    assert_eq!(publication.activation_expires_at(), activation.expires_at());
    let candidate_revisions = publication.revision_plan()?;
    assert_eq!(candidate_revisions.len(), 1);
    assert!(candidate_revisions.is_locally_observed());
    assert_eq!(
        publication.presentation_obligation_digest(),
        presentation.obligation_digest()
    );
    assert_eq!(
        publication.sealed_capture_receipt_digest(),
        sealed_capture.receipt_digest()
    );
    assert_eq!(publication.sealed_capture(), &sealed_capture);
    assert_ne!(publication.capture_content_digest().bytes(), [0; 32]);
    assert_ne!(publication.canonical_content_digest().bytes(), [0; 32]);
    let candidate_wire = serde_json::to_string(&publication)?;
    assert!(!candidate_wire.contains("\"generation\""));
    assert!(!candidate_wire.contains("\"manifest"));
    assert!(!candidate_wire.contains("\"published_at\""));

    let revisions = source.revision_plan(&extraction)?;
    assert_eq!(extraction.records().len(), 1);
    assert!(revisions.is_locally_observed());
    assert_eq!(revisions, candidate_revisions);
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
        observation.context().provenance().received_at(),
        extraction_received_at
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
        4
    );
    let health = source.health()?;
    assert!(health.last_attempt_at().is_some());
    assert_eq!(health.last_success_at(), Some(extraction_received_at));
    assert!(health.last_payload_digest().is_some());
    assert_eq!(health.consecutive_failures(), 0);
    Ok(())
}

#[tokio::test]
async fn authority_loss_during_completed_work_prevents_every_publication() -> TestResult {
    let now = system_timestamp()?;
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path())?;
    let store = paths.sealed_research_journal_store()?;

    let (source, registry, authority, activation, deadline) = source_harness(now, &store).await?;
    source.queue_test_publication_action(Some(registry))?;
    let discovery = source
        .discover_with_activation(
            authority,
            discovery_request(&source, deadline)?,
            &activation,
            CancellationToken::new(),
        )
        .await;
    assert_not_current(discovery)?;

    let (source, registry, authority, activation, deadline) = source_harness(now, &store).await?;
    let object = discover_object(&source, authority.clone(), &activation, deadline).await?;
    let request = extraction_request(object, deadline)?;
    source.queue_test_publication_action(Some(registry))?;
    let normalized = source
        .normalized_page(
            &authority,
            &request,
            &activation,
            CancellationToken::new(),
        )
        .await;
    assert_not_current(normalized)?;

    let (source, registry, authority, activation, deadline) = source_harness(now, &store).await?;
    let object = discover_object(&source, authority.clone(), &activation, deadline).await?;
    source.queue_test_publication_action(None)?;
    source.queue_test_publication_action(Some(registry))?;
    let extraction = source
        .extract_with_capture(
            authority,
            extraction_request(object, deadline)?,
            &activation,
            CancellationToken::new(),
        )
        .await;
    assert_not_current(extraction)?;
    Ok(())
}

async fn source_harness(
    now: Timestamp,
    store: &market_squawk_platform::SealedResearchJournalStore,
) -> TestResult<(
    BlsSource,
    AuthoritativeSourceRegistry,
    ExtractionAuthority,
    crate::BlsActivationCandidate,
    Timestamp,
)> {
    let config = source_config()?;
    let metadata = source_metadata(now, &config, true)?;
    let transport = Arc::new(ScriptedTransport {
        responses: Mutex::new(VecDeque::from([
            complete_http_response(now),
            complete_http_response(now),
            complete_http_response(now),
        ])),
        request_count: Mutex::new(0),
    });
    let source = BlsSource::try_new_with_transport(metadata.clone(), config, transport)?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(metadata, now)?;
    let authority = registry.extraction_authority(&registered, &source)?;
    let deadline = now.checked_add_nanos(60_000_000_000)?;
    let doctor = source
        .doctor(authority.clone(), deadline, CancellationToken::new())
        .await?;
    let (doctor_report, doctor_capture) = doctor.into_parts();
    let activation = source.activation_candidate(doctor_report, doctor_capture.seal(store)?)?;
    Ok((
        source,
        registry,
        authority,
        activation,
        deadline,
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

fn complete_http_response_for_year(received_at: Timestamp, year: u16) -> BlsHttpResponse {
    let body = format!(
        r#"{{
  "status":"REQUEST_SUCCEEDED",
  "responseTime":1,
  "message":[],
  "Results":{{"series":[{{"seriesID":"LNS14000000","data":[{{
    "year":"{year}","period":"M06","periodName":"June","latest":"false",
    "value":"4.0","footnotes":[]
  }}]}}]}}
}}"#,
    );
    BlsHttpResponse {
        status: 200,
        retry_after: None,
        content_encoding: None,
        content_type: Some(b"application/json".to_vec()),
        body: Bytes::from(body),
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
    activation: &crate::BlsActivationCandidate,
    deadline: Timestamp,
) -> TestResult<market_squawk_sources::SourceObject> {
    source
        .discover_with_activation(
            authority,
            discovery_request(source, deadline)?,
            activation,
            CancellationToken::new(),
        )
        .await?
        .batch()
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
    source_config_for_years(2026, 2026)
}

fn source_config_for_years(
    start_year: u16,
    end_year: u16,
) -> Result<BlsSourceConfig, BlsSourceError> {
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
    BlsSourceConfig::try_new(
        BlsAuthorization::PublicV1,
        owner_usage_policy()?,
        vec![series],
        start_year,
        end_year,
    )
}

fn owner_usage_policy() -> Result<BlsUsagePolicy, BlsSourceError> {
    BlsUsagePolicy::try_owner_authorized(
        exact_evidence(b"owner-approved-private-research").content_digest(),
    )
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
        NonZeroU64::new(1_000_000_000).ok_or("nonzero backoff")?,
        NonZeroU64::new(60_000_000_000).ok_or("nonzero max backoff")?,
        0,
    )?;
    let scope = BudgetScope::for_authorization(provider.clone(), &authorization)?;
    let budget = if exact_conjunctive_budget {
        crate::bls_application_provider_budget(config.tier())?
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
