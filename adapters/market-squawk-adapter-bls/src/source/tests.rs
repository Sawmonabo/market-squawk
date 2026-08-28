use std::collections::VecDeque;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, MetadataRevision, ResearchObservation,
    RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_platform::LocalPaths;
use market_squawk_sources::{
    ApiEndpointRule, AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    AuthorizationSubjectResolutionError, AuthorizationSubjectResolver, BackoffPolicy, BudgetScope,
    CoverageDomain, DiscoveryRequest, EndpointPolicy, ExtractionAuthority,
    ExtractionAuthorityError, ExtractionRequest, ExtractionSource, ExtractionSourceError,
    FreshnessPolicy, HistoricalCapability, NetworkAccessPolicy, PathScope, ProviderBudgetPolicy,
    SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput,
    SourceProtocolProfile,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{BlsSource, PageCache, RetrievedBlsPage, exact_evidence, parse_object_id};
use crate::client::{BlsHttpRequest, BlsHttpResponse, BlsTransport, system_timestamp};
use crate::{
    BlsAccessTier, BlsAuthorization, BlsDoctorReadiness, BlsResponse, BlsSeriesMetadata,
    BlsSourceConfig, BlsSourceError,
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

#[derive(Debug)]
struct TestSubjectResolver {
    subject: SourceIdentifier,
}

impl AuthorizationSubjectResolver for TestSubjectResolver {
    fn resolve_subject_record(
        &self,
        mode: AuthorizationMode,
        _evidence: EvidenceDigest,
    ) -> Result<SourceIdentifier, AuthorizationSubjectResolutionError> {
        if mode != AuthorizationMode::UserAuthorized {
            return Err(AuthorizationSubjectResolutionError::UnsupportedMode);
        }
        Ok(self.subject.clone())
    }
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
            let authorization_matches = if request.url == BlsAuthorization::public_v1().endpoint() {
                body.get("registrationkey").is_none()
            } else {
                request.url == "https://api.bls.gov/publicAPI/v2/timeseries/data/"
                    && body["registrationkey"]
                        .as_str()
                        .is_some_and(|value| !value.is_empty())
            };
            if !authorization_matches
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
    let first_received_at = now.checked_add_nanos(1)?;
    let second_received_at = now.checked_add_nanos(2)?;
    let restarted_doctor_received_at = now.checked_add_nanos(3)?;
    let credential_generation = EvidenceDigest::new(DigestAlgorithm::Sha256, [7; 32]);
    let secret = "fake-fake-fake-fake-fake-fake-fake-fake";
    let transport = Arc::new(ScriptedTransport {
        responses: Mutex::new(VecDeque::from([
            complete_http_response(now),
            complete_http_response(first_received_at),
            complete_http_response(second_received_at),
            complete_http_response(restarted_doctor_received_at),
        ])),
        request_count: Mutex::new(0),
    });
    let config = registered_source_config(credential_generation, secret)?;
    let metadata = source_metadata(now, &config, true)?;
    let source = BlsSource::try_new_with_transport(metadata.clone(), config, transport.clone())?;
    let activation_plan = source.activation_plan()?;
    assert_eq!(activation_plan.source_id(), metadata.source_id());
    assert_eq!(activation_plan.metadata_revision(), metadata.revision());
    assert_eq!(activation_plan.provider_dataset(), source.dataset());
    assert_eq!(
        activation_plan.credential_rejoin(),
        crate::BlsCredentialRejoin::RegisteredGeneration(credential_generation)
    );
    assert!(
        activation_plan
            .rate()
            .persistent_shared_authority_required()
    );
    assert!(activation_plan.rate().counts_all_started_attempts());
    assert_eq!(
        activation_plan.rate().documented_requests_per_ten_seconds(),
        50
    );
    assert_eq!(activation_plan.rate().documented_requests_per_day(), 500);
    assert_eq!(activation_plan.rate().application_requests_per_second(), 1);
    assert_eq!(activation_plan.rate().application_requests_per_day(), 400);
    assert_eq!(activation_plan.rate().maximum_in_flight(), 1);
    assert_eq!(
        activation_plan.rate().maximum_backoff_nanos(),
        60_000_000_000
    );
    assert_eq!(
        activation_plan.rate().declaration_digest(),
        activation_plan
            .rate()
            .shared_rate_declaration()
            .declaration_digest()
    );
    let rate_subject = activation_plan
        .rate()
        .authorization_subject()
        .ok_or("missing registered BLS rate subject")?
        .clone();
    let mut registry = AuthoritativeSourceRegistry::
        try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(Arc::new(
            TestSubjectResolver {
                subject: rate_subject.clone(),
            },
        ))?;
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
    let (pending_doctor, doctor_seal) = doctor.into_sealing_parts();
    let sealed_doctor_capture = doctor_seal.seal(&store)?;
    let activation = source.activation_candidate(pending_doctor, sealed_doctor_capture)?;
    source.validate_activation_candidate(&activation)?;
    assert_eq!(activation.plan(), &activation_plan);
    assert_eq!(
        activation.sealed_doctor_capture().capture().dataset(),
        source.dataset()
    );
    let discovery_request = discovery_request(&source, deadline)?;
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
        NonZeroU16::new(1).ok_or("nonzero discovery bound")?,
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
    let first_discovery = source
        .discover_with_activation(
            authority.clone(),
            discovery_request.clone(),
            &activation,
            CancellationToken::new(),
        )
        .await?;
    let second_discovery = source
        .discover_with_activation(
            authority,
            discovery_request,
            &activation,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(first_discovery.batch().objects().len(), 1);
    assert_eq!(second_discovery.batch().objects().len(), 1);
    assert_eq!(
        first_discovery.batch().objects()[0].object_id(),
        second_discovery.batch().objects()[0].object_id()
    );
    assert_eq!(
        first_discovery
            .capture_material()
            .receipt()
            .content_digest(),
        second_discovery
            .capture_material()
            .receipt()
            .content_digest()
    );
    assert_ne!(
        first_discovery
            .capture_material()
            .receipt()
            .observation_digest(),
        second_discovery
            .capture_material()
            .receipt()
            .observation_digest()
    );
    let first_object = first_discovery.batch().objects()[0].clone();
    let second_object = second_discovery.batch().objects()[0].clone();
    let (first_pending, first_capture) = first_discovery.into_sealing_parts()?;
    let (second_pending, second_capture) = second_discovery.into_sealing_parts()?;
    let first_sealed = first_capture.seal(&store)?;
    let second_sealed = second_capture.seal(&store)?;

    drop(activation);
    drop(source);
    drop(registry);

    let restarted_config = registered_source_config(credential_generation, secret)?;
    let restarted_metadata = source_metadata(now, &restarted_config, true)?;
    let restarted_source = BlsSource::try_new_with_transport(
        restarted_metadata.clone(),
        restarted_config,
        transport.clone(),
    )?;
    assert_eq!(
        restarted_source.dataset(),
        activation_plan.provider_dataset()
    );
    assert_eq!(
        restarted_source.activation_plan()?.plan_digest(),
        activation_plan.plan_digest()
    );
    let different_generation = registered_source_config(
        EvidenceDigest::new(DigestAlgorithm::Sha256, [8; 32]),
        secret,
    )?;
    assert_ne!(different_generation.dataset(), restarted_source.dataset());

    let mut restarted_registry = AuthoritativeSourceRegistry::
        try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(Arc::new(
            TestSubjectResolver {
                subject: rate_subject,
            },
        ))?;
    let restarted_registered = restarted_registry.register(restarted_metadata, now)?;
    let restarted_authority =
        restarted_registry.extraction_authority(&restarted_registered, &restarted_source)?;
    let restarted_doctor = restarted_source
        .doctor(
            restarted_authority.clone(),
            deadline,
            CancellationToken::new(),
        )
        .await?;
    let (restarted_pending, restarted_seal) = restarted_doctor.into_sealing_parts();
    let restarted_activation =
        restarted_source.activation_candidate(restarted_pending, restarted_seal.seal(&store)?)?;

    let first_admission = restarted_source
        .admit_sealed_discovery(first_pending, first_sealed, &restarted_activation)?
        .into_objects()
        .into_vec()
        .pop()
        .ok_or("missing first BLS receipt admission")?;
    let second_admission = restarted_source
        .admit_sealed_discovery(second_pending, second_sealed, &restarted_activation)?
        .into_objects()
        .into_vec()
        .pop()
        .ok_or("missing second BLS receipt admission")?;
    let first_output = restarted_source
        .extract_sealed_discovery(
            restarted_authority.clone(),
            extraction_request(first_object, deadline)?,
            first_admission,
            &restarted_activation,
            CancellationToken::new(),
        )
        .await?;
    let second_output = restarted_source
        .extract_sealed_discovery(
            restarted_authority,
            extraction_request(second_object, deadline)?,
            second_admission,
            &restarted_activation,
            CancellationToken::new(),
        )
        .await?;
    let first_publication =
        restarted_source.publication_candidate(first_output, &restarted_activation)?;
    let second_publication =
        restarted_source.publication_candidate(second_output, &restarted_activation)?;
    restarted_source.validate_publication_candidate(&first_publication, &restarted_activation)?;
    restarted_source.validate_publication_candidate(&second_publication, &restarted_activation)?;
    assert_eq!(
        first_publication.provider_dataset(),
        restarted_source.dataset()
    );
    assert_eq!(first_publication.total_chunks(), 1);
    assert_eq!(first_publication.canonical_record_count(), 1);
    assert_eq!(first_publication.first_observed_at(), first_received_at);
    assert_eq!(second_publication.first_observed_at(), second_received_at);
    assert_eq!(
        first_publication.capture_content_digest(),
        second_publication.capture_content_digest()
    );
    assert_ne!(
        first_publication.capture_observation_digest(),
        second_publication.capture_observation_digest()
    );
    assert_ne!(
        first_publication.sealed_discovery_capture_receipt_digest(),
        second_publication.sealed_discovery_capture_receipt_digest()
    );
    assert_eq!(
        first_publication.source_generation_digest(),
        activation_plan.plan_digest()
    );
    assert_eq!(
        first_publication.source_generation_digest(),
        second_publication.source_generation_digest()
    );
    assert_eq!(
        first_publication.batch().records()[0].revision(),
        second_publication.batch().records()[0].revision()
    );
    assert_ne!(
        first_publication.candidate_digest(),
        second_publication.candidate_digest()
    );
    assert!(!format!("{restarted_source:?}").contains(secret));

    let native_lineage = first_publication.native_lineage();
    let native_row = crate::BlsTimeseriesNativeLineageRowV1::try_decode_persisted(
        native_lineage.schema().version(),
        crate::BLS_TIMESERIES_NATIVE_LINEAGE_IMPLEMENTATION,
        native_lineage.rows()[0].semantic_payload(),
    )?;
    assert_eq!(native_row.series().series_id().as_str(), "LNS14000000");
    assert_eq!(native_row.series().unit().as_str(), "percent");
    assert_eq!(native_row.observation().period().as_str(), "M06");
    assert_eq!(native_row.observation().raw_value(), "4.2");

    let record = &first_publication.batch().records()[0];
    assert_eq!(record.schema().as_str(), "market-squawk-research-v3");
    assert!(record.published_time().is_none());
    assert_eq!(record.available_at(), Some(first_received_at));
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
        first_received_at
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
    let health = restarted_source.health()?;
    assert!(health.last_attempt_at().is_some());
    assert_eq!(health.last_success_at(), Some(restarted_doctor_received_at));
    assert!(health.last_payload_digest().is_some());
    assert_eq!(health.consecutive_failures(), 0);

    let complete_plan = crate::BlsCompletePublicationPlanHandoff::try_new(vec![first_publication])?;
    assert_eq!(complete_plan.total_chunks(), 1);
    assert_eq!(complete_plan.canonical_record_count(), 1);
    assert_eq!(
        complete_plan.request_set_identity(),
        complete_plan.candidates()[0].request_set_identity()
    );
    assert_eq!(
        complete_plan.source_generation_digest(),
        activation_plan.plan_digest()
    );
    assert_ne!(complete_plan.completion_digest().bytes(), [0; 32]);
    assert_eq!(complete_plan.into_candidates().len(), 1);
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
    let (object, admission) =
        discover_object(&source, authority.clone(), &activation, deadline, &store).await?;
    source.queue_test_publication_action(Some(registry))?;
    let extraction = source
        .extract_sealed_discovery(
            authority,
            extraction_request(object, deadline)?,
            admission,
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
    let (pending_doctor, doctor_seal) = doctor.into_sealing_parts();
    let activation = source.activation_candidate(pending_doctor, doctor_seal.seal(store)?)?;
    Ok((source, registry, authority, activation, deadline))
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
    activation: &crate::BlsActivationCandidate,
    deadline: Timestamp,
    store: &market_squawk_platform::SealedResearchJournalStore,
) -> TestResult<(
    market_squawk_sources::SourceObject,
    crate::BlsDiscoveryObjectAdmission,
)> {
    let discovery = source
        .discover_with_activation(
            authority,
            discovery_request(source, deadline)?,
            activation,
            CancellationToken::new(),
        )
        .await?;
    let object = discovery
        .batch()
        .objects()
        .first()
        .cloned()
        .ok_or("missing BLS source object")?;
    let (pending, capture) = discovery.into_sealing_parts()?;
    let admission = source
        .admit_sealed_discovery(pending, capture.seal(store)?, activation)?
        .into_objects()
        .into_vec()
        .pop()
        .ok_or("missing BLS discovery admission")?;
    Ok((object, admission))
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
        BlsAuthorization::public_v1(),
        vec![series],
        start_year,
        end_year,
    )
}

fn registered_source_config(
    credential_generation: EvidenceDigest,
    secret: &str,
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
        BlsAuthorization::registered_v2(
            crate::BlsRegistrationKey::try_new(secret.to_owned())?,
            credential_generation,
        )?,
        vec![series],
        2026,
        2026,
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
    let authorization_mode = match config.tier() {
        BlsAccessTier::PublicV1 => AuthorizationMode::PublicInterface,
        BlsAccessTier::RegisteredV2 => AuthorizationMode::UserAuthorized,
    };
    let authorization_basis = match config.tier() {
        BlsAccessTier::PublicV1 => SourceIdentifier::try_from("official-public-interface")?,
        BlsAccessTier::RegisteredV2 => {
            market_squawk_sources::ProviderRateDeclaration::governed_provider_subject(&provider)?
        }
    };
    let authorization = AuthorizationGrant::new(
        authorization_mode,
        AuthorizationBasis::new(authorization_basis),
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
        config.authorization().endpoint(),
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
    let request_identity = EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]);
    let observation_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, [2; 32]);
    let source_generation = EvidenceDigest::new(DigestAlgorithm::Sha256, [3; 32]);
    let first_cache_key = "first-receipt";
    let second_cache_key = "second-receipt";

    let first = page(b"1234", "first")?;
    let first_charge = PageCache::retained_charge(first_cache_key, &first_id, &first, true)?;
    let mut cache = PageCache::with_limit(first_charge);

    assert!(cache.insert(
        first_cache_key,
        &first_id,
        request_identity,
        observation_digest,
        source_generation,
        &first,
    )?);
    assert!(!cache.insert(
        second_cache_key,
        &second_id,
        request_identity,
        observation_digest,
        source_generation,
        &page(b"5", "second")?,
    )?);
    assert_eq!(cache.retained_bytes, first_charge);
    assert!(cache.pages.contains_key(first_cache_key));
    assert!(!cache.pages.contains_key(second_cache_key));
    Ok(())
}
