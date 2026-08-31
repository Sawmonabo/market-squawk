use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DeliveryEvidence, EffectiveInterval,
    MetadataRevision, ResearchObservation, ResearchTemporalPrecision, RevisionBoundPayloadEvidence,
    SchemaVersion, SequenceCapability, SourceId, Timestamp,
};
use market_squawk_platform::LocalPaths;
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationSubjectResolutionError,
    AuthorizationSubjectResolver, CoverageDomain, DiscoveryRequest, EndpointPolicy,
    ExtractionRequest, FreshnessPolicy, HttpRequestBounds, NetworkAccessPolicy,
    ProviderCaptureTerminalDisposition, SourceCapabilities, SourceCoverage, SourceMetadataInput,
};
use uuid::Uuid;

use super::*;
use crate::{
    CENSUS_OPERATION_MEMORY_LIMIT_BYTES, CensusActivationCandidate, CensusDataset,
    CensusGeographyClause, CensusGeographyCode, CensusGeographyScope, CensusPredicate,
    CensusPublicationCandidate, CensusReportedTime, CensusTimePoint, CensusTimePredicate,
    census_provider_rate_declaration,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("market-squawk-census-{}", Uuid::new_v4())))
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

const DATA_RESPONSE: &[u8] = br#"[
  ["CENSUS_VALUE", "time", "us", "NAME", "GEO_ID"],
  ["42", "2024-Q1", "1", "United States", "0100000US"]
]"#;

const DOCTOR_RESPONSE: &[u8] = br#"[
  ["NAME", "B01001_001E", "us"],
  ["United States", "340110988", "1"]
]"#;

const DATASET_RESPONSE: &[u8] = br#"{
  "dataset": [{
    "c_vintage": "timeseries",
    "c_dataset": ["timeseries", "economic", "fixture"],
    "title": "Quarterly aggregate fixture",
    "description": "Exact local quarterly transport fixture",
    "c_isAggregate": true,
    "c_isTimeseries": true
  }]
}"#;

const GROUPS_RESPONSE: &[u8] = br#"{"groups": []}"#;

const VARIABLES_RESPONSE: &[u8] = br#"{
  "variables": {
    "CENSUS_VALUE": {
      "label": "Quarterly value",
      "concept": "Quarterly economic fixture",
      "predicateType": "int",
      "group": "FIXTURE",
      "attributes": "",
      "required": true,
      "limit": 0
    },
    "time": {
      "label": "Time",
      "concept": "Census API Time Specification",
      "predicateType": "time",
      "group": "N/A",
      "attributes": "",
      "required": "required, predicate-only"
    },
    "us": {
      "label": "United States",
      "concept": "Census API Geography Specification",
      "predicateType": "fips-for",
      "group": "N/A",
      "attributes": "",
      "required": "predicate-only"
    }
  }
}"#;

const GEOGRAPHIES_RESPONSE: &[u8] = br#"{
  "fips": [{
    "name": "us",
    "geoLevelDisplay": "010",
    "referenceDate": "2024-01-01"
  }]
}"#;

#[derive(Debug)]
struct ScriptedTransport {
    expected_url: String,
    response: Mutex<Option<CensusHttpResponse>>,
    attempts: AtomicU64,
}

impl CensusTransport for ScriptedTransport {
    fn execute<'a>(
        &'a self,
        request: CensusHttpRequest<'a>,
        in_flight: &'a market_squawk_sources::InFlightExtractionRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<CensusHttpResponse, CensusSourceError>> {
        Box::pin(async move {
            in_flight
                .validate_current()
                .map_err(|_| CensusSourceError::Authority)?;
            if cancellation.is_cancelled()
                || timeout.is_zero()
                || request.authorized.redacted_url() != self.expected_url
                || request.authorized.key_query_value() != Some("test-census-key")
                || request
                    .authorized
                    .transport_url()
                    .query_pairs()
                    .any(|(key, _value)| key == "key")
                || DATA_RESPONSE.len() > max_bytes
            {
                return Err(CensusSourceError::Protocol);
            }
            self.attempts.fetch_add(1, Ordering::Relaxed);
            self.response
                .lock()
                .map_err(|_| CensusSourceError::Protocol)?
                .take()
                .ok_or(CensusSourceError::Protocol)
        })
    }
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedCensusClockInput {
    receipt: Timestamp,
    geography_digest: [u8; 32],
    row_digest: [u8; 32],
    family_digest: [u8; 32],
    content_digest: [u8; 32],
}

#[derive(Debug)]
struct DeterministicProcessingClock {
    decoded_at: Timestamp,
    ingested_at: Timestamp,
    parsed: Mutex<Option<ParsedCensusClockInput>>,
}

impl DeterministicProcessingClock {
    fn parsed_input(&self) -> TestResult<ParsedCensusClockInput> {
        self.parsed
            .lock()
            .map_err(|_| "processing clock mutex poisoned")?
            .clone()
            .ok_or_else(|| "processing clock was not sampled".into())
    }
}

impl CensusProcessingClock for DeterministicProcessingClock {
    fn sample_after_complete_parse(
        &self,
        page: &CensusDataPage,
    ) -> Result<(Timestamp, Timestamp), CensusSourceError> {
        let [observation] = page.observations() else {
            return Err(CensusSourceError::Protocol);
        };
        if !page.completeness().is_complete()
            || page.clocks().received_at() != page.clocks().decoded_at()
            || page.clocks().decoded_at() != page.clocks().ingested_at()
            || observation.reported_time()
                != Some(&CensusReportedTime::Quarter {
                    year: 2024,
                    quarter: 1,
                })
            || observation.geography().scope() != CensusGeographyScope::Aggregate
        {
            return Err(CensusSourceError::Protocol);
        }
        let input = ParsedCensusClockInput {
            receipt: page.clocks().received_at(),
            geography_digest: observation.geography().identity_digest(),
            row_digest: observation.row_digest(),
            family_digest: observation.revision_candidate().family_digest(),
            content_digest: observation.revision_candidate().content_digest(),
        };
        let mut parsed = self
            .parsed
            .lock()
            .map_err(|_| CensusSourceError::Protocol)?;
        if parsed.replace(input).is_some() {
            return Err(CensusSourceError::Protocol);
        }
        Ok((self.decoded_at, self.ingested_at))
    }
}

#[tokio::test]
async fn authorized_transport_samples_processing_clock_after_complete_parse_and_preserves_canonical_evidence()
-> TestResult {
    let now = system_timestamp()?;
    let decoded_at = now.checked_add_nanos(1)?;
    let ingested_at = now.checked_add_nanos(2)?;
    let contract = contract()?;
    let config = CensusSourceConfig::try_new(
        [contract.clone()],
        CensusParseLimits::try_new(1024 * 1024, 100, 100, 10_000, 1_000, 4_096)?,
    )?;
    let subject = SourceIdentifier::try_from("census-test-key-record")?;
    let metadata = source_metadata(now, subject.clone(), &config)?;
    let transport = Arc::new(ScriptedTransport {
        expected_url: contract.query().redacted_url().to_owned(),
        response: Mutex::new(Some(CensusHttpResponse {
            status: 200,
            key_error: false,
            retry_after: None,
            content_encoding: None,
            content_type: Some(b"application/json; charset=utf-8".to_vec()),
            rate_headers: CensusRateLimitHeaders::default(),
            body: Bytes::from_static(DATA_RESPONSE),
            received_at: now,
            latency: Duration::from_millis(7),
        })),
        attempts: AtomicU64::new(0),
    });
    let processing_clock = Arc::new(DeterministicProcessingClock {
        decoded_at,
        ingested_at,
        parsed: Mutex::new(None),
    });
    let source = CensusSource::try_new_with_transport_and_processing_clock(
        metadata.clone(),
        CensusApiKey::try_new("test-census-key".to_owned())?,
        config,
        transport.clone(),
        processing_clock.clone(),
    )?;
    let public_metadata = contract.metadata_requests()[0].public_request()?;
    assert!(!public_metadata.is_credentialed());
    assert_eq!(public_metadata.key_query_value(), None);
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(
        Arc::new(TestSubjectResolver { subject }),
    )?;
    let registered = registry.register(metadata.clone(), now)?;
    let authority = registry.extraction_authority(&registered, &source)?;
    let deadline = now.checked_add_nanos(60_000_000_000)?;
    let metadata_bundle = metadata_bundle(&metadata, &contract, now)?;
    let mut diagnostic = CensusDiagnosticJourney::new();
    let data = source
        .acquire_data(
            &authority,
            &metadata_bundle,
            deadline,
            CancellationToken::new(),
            &mut diagnostic,
        )
        .await?;
    assert!(data.page().completeness().is_complete());
    assert_eq!(data.page().accounting().returned_rows(), 1);
    assert_eq!(data.page().accounting().usable_rows(), 1);
    assert_eq!(data.page().accounting().skipped_rows(), 0);
    assert_eq!(data.page().accounting().missing_requested_variables(), 0);
    assert_eq!(data.page().accounting().requested_geographies(), Some(1));
    assert_eq!(data.page().accounting().returned_geographies(), 1);
    assert_eq!(data.page().clocks().received_at(), now);
    assert_eq!(data.page().clocks().decoded_at(), decoded_at);
    assert_eq!(data.page().clocks().ingested_at(), ingested_at);
    assert_eq!(
        data.page().observations()[0].reported_time(),
        Some(&CensusReportedTime::Quarter {
            year: 2024,
            quarter: 1,
        })
    );
    let geography = data.page().observations()[0].geography();
    assert_eq!(geography.scope(), CensusGeographyScope::Aggregate);
    assert_ne!(geography.identity_digest(), [0; 32]);
    let parsed_clock_input = processing_clock.parsed_input()?;
    assert_eq!(parsed_clock_input.receipt, now);
    assert_eq!(
        parsed_clock_input.geography_digest,
        geography.identity_digest()
    );
    assert_eq!(
        parsed_clock_input.row_digest,
        data.page().observations()[0].row_digest()
    );
    assert_eq!(
        parsed_clock_input.family_digest,
        data.page().observations()[0]
            .revision_candidate()
            .family_digest()
    );
    assert_eq!(
        parsed_clock_input.content_digest,
        data.page().observations()[0]
            .revision_candidate()
            .content_digest()
    );
    let retained_bytes = data.page().conservative_retained_bytes()?;
    assert!(retained_bytes > DATA_RESPONSE.len());
    assert!(retained_bytes <= CENSUS_OPERATION_MEMORY_LIMIT_BYTES);

    let acquisition_telemetry = metadata_bundle.telemetry().checked_add(data.telemetry())?;
    let acquisition = CensusDatasetAcquisition {
        metadata: metadata_bundle,
        data,
        telemetry: acquisition_telemetry,
    };
    let discovery_request = DiscoveryRequest::try_new(
        contract.dataset_id().clone(),
        None,
        NonZeroU16::MIN,
        deadline,
    )?;
    let discovery_capture_material = combined_capture_material(&metadata, &contract, &acquisition)?;
    let object = source_object(
        &metadata,
        &discovery_request,
        &contract,
        &acquisition,
        discovery_capture_material.receipt(),
    )?;
    let extraction_request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(10).ok_or("nonzero record bound")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero byte bound")?,
        deadline,
    )?;
    let output = extraction_output(
        &metadata,
        source.config(),
        &extraction_request,
        &contract,
        acquisition,
        discovery_capture_material.receipt(),
    )?;

    assert_eq!(output.batch().records().len(), 1);
    assert_eq!(output.publication_plan().observations().len(), 1);
    assert_eq!(output.publication_plan().captures().len(), 1);
    assert_eq!(output.publication_plan().captures()[0].component_count(), 5);
    assert_eq!(
        output.publication_plan().data_response_digest().bytes(),
        sha256(DATA_RESPONSE)
    );
    let binding = &output.publication_plan().observations()[0];
    assert_eq!(
        binding.geography().identity_digest(),
        parsed_clock_input.geography_digest
    );
    assert_eq!(binding.row_digest().bytes(), parsed_clock_input.row_digest);
    assert_eq!(
        binding.family_digest().bytes(),
        parsed_clock_input.family_digest
    );
    assert_eq!(
        binding.content_digest().bytes(),
        parsed_clock_input.content_digest
    );
    assert_eq!(binding.clocks().received_at(), now);
    assert_eq!(binding.clocks().decoded_at(), decoded_at);
    assert_eq!(binding.clocks().ingested_at(), ingested_at);
    assert_eq!(
        binding.reported_time(),
        Some(&CensusReportedTime::Quarter {
            year: 2024,
            quarter: 1,
        })
    );
    assert_eq!(
        binding.effective_time().precision(),
        ResearchTemporalPrecision::SourcePeriod
    );
    let period = binding
        .effective_time()
        .source_period_value()
        .ok_or("expected provider-qualified quarter")?;
    assert_eq!(period.scheme().as_str(), "census-quarter");
    assert_eq!(period.year(), 2024);
    assert_eq!(period.ordinal().get(), 1);
    assert_eq!(period.code().as_str(), "2024-Q1");
    assert!(binding.published_time().is_none());
    assert_eq!(
        binding.clocks().availability().conservative_available_at(),
        Some(now)
    );
    output.publication_plan().validate()?;
    let (batch, acquisition, publication_plan, telemetry) = output.into_parts();
    assert_eq!(
        discovery_capture_material.receipt().terminal(),
        ProviderCaptureTerminalDisposition::CompleteRequestGraph
    );
    assert_eq!(
        discovery_capture_material
            .receipt()
            .request_graph_components()
            .len(),
        5
    );
    assert_eq!(discovery_capture_material.records().len(), 5);
    assert_eq!(
        discovery_capture_material.records()[4].payload(),
        DATA_RESPONSE
    );
    assert_eq!(
        batch
            .request()
            .object()
            .capture_identity()
            .paged_content_digest(),
        Some(discovery_capture_material.receipt().content_digest())
    );
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path())?;
    let store = paths.sealed_research_journal_store()?;
    let (capture_expectation, seal_request) = discovery_capture_material.into_whole_seal_parts();
    let sealed_capture = seal_request.seal(&store)?;
    let capture_token = capture_expectation
        .try_rejoin(sealed_capture)?
        .try_into_whole()?;
    let doctor_query = crate::doctor::doctor_query()?;
    let doctor_capture = capture_receipt(
        &metadata,
        crate::doctor::doctor_dataset_identity()?,
        doctor_query.request_digest(),
        DOCTOR_RESPONSE,
        now,
    )?;
    let doctor_report = crate::doctor::build_doctor_report(
        &metadata,
        source.config(),
        &doctor_query,
        &Bytes::from_static(DOCTOR_RESPONSE),
        &doctor_capture,
        &CensusRateLimitHeaders::default(),
        now,
        Duration::from_millis(1),
    )?;
    let doctor_material = super::capture_material(
        &metadata,
        &doctor_capture,
        Bytes::from_static(DOCTOR_RESPONSE),
    )?;
    let (doctor_expectation, doctor_seal_request) = doctor_material.into_whole_seal_parts();
    let sealed_doctor_material = doctor_seal_request.seal(&store)?;
    let doctor_token = doctor_expectation
        .try_rejoin(sealed_doctor_material)?
        .try_into_whole()?;
    let activation = CensusActivationCandidate::try_new(
        source.activation_plan()?,
        doctor_report,
        doctor_token,
        now,
    )?;
    activation.validate(now)?;
    let expected_extraction_content =
        market_squawk_sources::ExtractionContentIdentity::try_from_batch(&batch)?.digest();
    let expected_sealed_capture_receipt = capture_token.persisted_receipt().receipt_digest();
    let native_lineage = census_native_lineage(&publication_plan, &batch)?;
    let mut row_capture_page_ordinals = Vec::new();
    row_capture_page_ordinals.try_reserve_exact(batch.records().len())?;
    row_capture_page_ordinals.extend(std::iter::repeat(4).take(batch.records().len()));
    let sealed_capture_binding = SealedProviderCaptureBinding::try_whole(
        capture_token,
        batch,
        native_lineage,
        row_capture_page_ordinals,
    )?;
    let publication_candidate =
        CensusPublicationCandidate::try_new(publication_plan, sealed_capture_binding, activation)?;
    let batch_semantics = publication_candidate
        .native_lineage()
        .batch_sidecar()
        .ok_or("missing Census response-wide semantics")?;
    let batch_semantics: serde_json::Value =
        serde_json::from_slice(batch_semantics.semantic_payload())?;
    assert_eq!(batch_semantics["schema_version"], 4);
    assert_eq!(
        batch_semantics["observations"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(
        batch_semantics["captures"].as_array().map(Vec::len),
        Some(1)
    );
    let native_semantics: serde_json::Value = serde_json::from_slice(
        publication_candidate.native_lineage().rows()[0].semantic_payload(),
    )?;
    assert_eq!(native_semantics["provider_variable"], "CENSUS_VALUE");
    assert_eq!(native_semantics["label"], "Quarterly value");
    assert_eq!(native_semantics["concept"], "Quarterly economic fixture");
    assert_eq!(native_semantics["group"], "FIXTURE");
    for forbidden in [
        "family_digest",
        "content_digest",
        "row_digest",
        "metadata_digest",
        "canonical_series",
        "canonical_source_identifier",
        "canonical_unit",
    ] {
        assert!(native_semantics.get(forbidden).is_none());
    }
    assert_eq!(publication_candidate.canonical_record_count(), 1);
    assert_eq!(
        publication_candidate.canonical_schema().as_str(),
        market_squawk_sources::CURRENT_RESEARCH_RECORD_SCHEMA
    );
    assert_eq!(
        publication_candidate.sealed_capture_receipt_digest(),
        expected_sealed_capture_receipt
    );
    let (sealed_capture_binding, revisions, activation) =
        publication_candidate.try_into_root_publication_parts()?;
    activation.validate(crate::http::system_timestamp()?)?;
    let batch = sealed_capture_binding.batch();
    let native_lineage = sealed_capture_binding.native_lineage();
    assert_eq!(
        market_squawk_sources::ExtractionContentIdentity::try_from_batch(&batch)?.digest(),
        expected_extraction_content
    );
    assert_eq!(revisions.len(), batch.records().len());
    assert!(revisions.is_locally_observed());
    assert_eq!(
        sealed_capture_binding.sealed_capture_receipt_digest(),
        expected_sealed_capture_receipt
    );
    assert_eq!(
        batch
            .request()
            .object()
            .capture_identity()
            .paged_content_digest(),
        Some(sealed_capture_binding.capture_evidence().content_digest())
    );
    assert_eq!(
        sealed_capture_binding
            .capture_evidence()
            .request_graph_components()[4]
            .observation_digest(),
        acquisition.data().capture().observation_digest()
    );
    assert_eq!(native_lineage.rows().len(), batch.records().len());
    let decoded = [serde_json::from_slice::<ResearchObservation>(
        batch.records()[0].payload(),
    )?];
    let revision_batch = revisions.into_observed_batch_with_native_lineage(
        metadata.source_id().clone(),
        batch,
        &decoded,
        native_lineage,
    )?;
    assert_eq!(revision_batch.input_len(), 1);
    assert_eq!(telemetry.requests(), 1);
    assert_eq!(telemetry.successful_responses(), 1);
    assert_eq!(source.telemetry().requests(), 1);
    assert_eq!(
        source.telemetry().response_bytes(),
        DATA_RESPONSE.len() as u64
    );
    assert_eq!(transport.attempts.load(Ordering::Relaxed), 1);
    assert!(CensusPredicate::try_new("NAME", CensusPredicateType::String, ["A:B"]).is_err());
    assert!(
        CensusDataQuery::try_new(
            CensusDataset::try_new(2024, "acs/acs1")?,
            CensusSelection::variables(["B01001_001E"])?,
            Vec::new(),
            CensusGeography::standard(
                CensusGeographyClause::try_new("state", [CensusGeographyCode::try_new("*")?],)?,
                Vec::new(),
            )?,
            Some(CensusTimePredicate::At {
                point: CensusTimePoint::year(2024)?,
            }),
        )
        .is_err()
    );

    let annotation_variable = SourceIdentifier::try_from("B01001_001EA")?;
    let exact_annotation = CensusAnnotationMatch::try_new(annotation_variable.clone(), "(X)")?;
    let exact_missing = MacroMissingValue::new(
        SourceIdentifier::try_from("(X)")?,
        Some(annotation_variable.clone()),
    );
    let annotation_rule =
        CensusAnnotatedMissingRule::try_new([exact_annotation], exact_missing.clone())?;
    assert_eq!(annotation_rule.missing(), &exact_missing);
    assert!(
        CensusAnnotatedMissingRule::try_new(
            [CensusAnnotationMatch::try_new(annotation_variable, "(X)")?],
            MacroMissingValue::new(
                SourceIdentifier::try_from("generic-missing")?,
                Some(SourceIdentifier::try_from("B01001_001EA")?),
            ),
        )
        .is_err()
    );

    let ResearchObservation::Macro(observation) =
        serde_json::from_slice(batch.records()[0].payload())?
    else {
        return Err("expected canonical macro observation".into());
    };
    assert_eq!(observation.unit().as_str(), "index_points");
    assert_eq!(
        observation
            .value()
            .observed_value()
            .map(|value| value.to_string()),
        Some("42".to_owned())
    );
    Ok(())
}

fn contract() -> TestResult<CensusDatasetContract> {
    let dataset = CensusDataset::try_time_series("economic/fixture")?;
    let query = CensusDataQuery::try_new(
        dataset,
        CensusSelection::variables(["CENSUS_VALUE"])?,
        Vec::new(),
        CensusGeography::standard(
            CensusGeographyClause::try_new("us", [CensusGeographyCode::try_new("1")?])?,
            Vec::new(),
        )?,
        Some(CensusTimePredicate::At {
            point: CensusTimePoint::quarter(2024, 1)?,
        }),
    )?;
    Ok(CensusDatasetContract::try_new(
        query,
        [CensusVariableMapping::try_new(
            SourceIdentifier::try_from("CENSUS_VALUE")?,
            SourceIdentifier::try_from("census.economic.quarterly")?,
            SourceIdentifier::try_from("index_points")?,
        )?],
        CensusEffectiveTimePolicy::RequireReportedTime,
    )?)
}

fn metadata_bundle(
    metadata: &SourceMetadata,
    contract: &CensusDatasetContract,
    received_at: Timestamp,
) -> TestResult<CensusMetadataBundle> {
    let bodies = [
        DATASET_RESPONSE,
        GROUPS_RESPONSE,
        VARIABLES_RESPONSE,
        GEOGRAPHIES_RESPONSE,
    ];
    let mut documents = Vec::new();
    for (request, body) in contract.metadata_requests().iter().zip(bodies) {
        let document = CensusDiscoveryDocument::parse(request, body, CensusParseLimits::default())?;
        documents.push(CensusCapturedDiscovery {
            request: request.clone(),
            body: Bytes::from_static(body),
            document,
            capture: capture_receipt(
                metadata,
                contract.dataset_id().clone(),
                request.request_digest(),
                body,
                received_at,
            )?,
            latency: Duration::from_millis(1),
        });
    }
    validate_metadata_bundle(contract, &documents)?;
    Ok(CensusMetadataBundle {
        dataset_id: contract.dataset_id().clone(),
        query_digest: contract.query().request_digest(),
        content_digest: metadata_bundle_digest(contract, &documents),
        documents,
        telemetry: CensusSourceTelemetry::default(),
    })
}

fn capture_receipt(
    metadata: &SourceMetadata,
    dataset: SourceIdentifier,
    request_digest: [u8; 32],
    body: &[u8],
    received_at: Timestamp,
) -> TestResult<ProviderCaptureSetReceipt> {
    let request = evidence_digest(request_digest);
    Ok(ProviderCaptureSetReceipt::try_new(
        metadata.source_id().clone(),
        metadata.revision().clone(),
        dataset,
        request,
        ProviderCaptureTerminalDisposition::StandaloneResponse,
        vec![ProviderCapturePageReceipt::try_new(
            0,
            request,
            None,
            None,
            200,
            u64::try_from(body.len())?,
            evidence_digest(sha256(body)),
            received_at,
        )?],
    )?)
}

fn source_metadata(
    now: Timestamp,
    subject: SourceIdentifier,
    config: &CensusSourceConfig,
) -> TestResult<SourceMetadata> {
    let evidence =
        ExactPayloadEvidence::from_content_digest(evidence_digest(sha256(b"census-test-metadata")));
    let effective = EffectiveInterval::new(now.checked_sub_nanos(1)?, None)?;
    let provider = SourceIdentifier::try_from("us-census")?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        AuthorizationBasis::new(subject.clone()),
        evidence.clone(),
        effective,
    );
    let budget = census_provider_rate_declaration(&subject)?.policy().clone();
    let network = EndpointPolicy::try_from_api_rules(
        census_api_endpoint_rules(config)?,
        HttpRequestBounds::default(),
    )?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("census-data-test")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("census-data-test-v1")?),
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
