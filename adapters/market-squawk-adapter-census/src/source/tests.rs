use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_platform::LocalPaths;
use market_squawk_domain::{
    AuthorizationBasis, CalendarDate, ChecksumCapability, CoverageDelay, DeliveryEvidence,
    EffectiveInterval, MetadataRevision, ResearchObservation, RevisionBoundPayloadEvidence,
    SchemaVersion, SequenceCapability, SourceId,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationSubjectResolutionError,
    AuthorizationSubjectResolver, CoverageDomain, DiscoveryRequest, EndpointPolicy,
    ExtractionRequest, FreshnessPolicy, HttpRequestBounds, NetworkAccessPolicy,
    ProviderCaptureTerminalDisposition,
    SourceCapabilities, SourceCoverage, SourceMetadataInput,
};
use uuid::Uuid;

use super::*;

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
  ["B01001_001E", "state"],
  ["42", "06"]
]"#;

const DOCTOR_RESPONSE: &[u8] = br#"[
  ["NAME", "B01001_001E", "us"],
  ["United States", "340110988", "1"]
]"#;

const DATASET_RESPONSE: &[u8] = br#"{
  "dataset": [{
    "c_vintage": 2024,
    "c_dataset": ["acs", "acs1"],
    "title": "ACS one-year fixture",
    "description": "Exact local transport fixture"
  }]
}"#;

const GROUPS_RESPONSE: &[u8] = br#"{"groups": []}"#;

const VARIABLES_RESPONSE: &[u8] = br#"{
  "variables": {
    "B01001_001E": {
      "label": "Estimate total",
      "concept": "Sex by Age",
      "predicateType": "int",
      "group": "B01001",
      "attributes": "",
      "required": true,
      "limit": 0
    },
    "state": {
      "label": "State",
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
    "name": "state",
    "geoLevelDisplay": "040",
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
        request: CensusHttpRequest,
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

#[tokio::test]
async fn authorized_transport_preserves_typed_rows_accounting_and_raw_capture() -> TestResult {
    let now = system_timestamp()?;
    let contract = contract()?;
    let config = CensusSourceConfig::try_new(
        [contract.clone()],
        CensusParseLimits::try_new(1024 * 1024, 100, 100, 10_000, 1_000, 4_096)?,
        CensusOwnerAuthorization::try_private_personal_research(
            SourceIdentifier::try_from("census-owner-attestation-test")?,
            evidence_digest(sha256(b"census-owner-attestation-test")),
            now,
        )?,
        evidence_digest(sha256(b"census-test-credential-generation")),
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
    let source = CensusSource::try_new_with_transport(
        metadata.clone(),
        CensusApiKey::try_new("test-census-key".to_owned())?,
        config,
        transport.clone(),
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
    let data = source
        .acquire_data(
            &authority,
            &metadata_bundle,
            deadline,
            CancellationToken::new(),
        )
        .await?;
    assert!(data.page().completeness().is_complete());
    assert_eq!(data.page().accounting().returned_rows(), 1);
    assert_eq!(data.page().accounting().missing_requested_variables(), 0);

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
    let object = source_object(&metadata, &discovery_request, &contract, &acquisition)?;
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
    )?;

    assert_eq!(output.batch().records().len(), 1);
    assert_eq!(output.publication_plan().observations().len(), 1);
    assert_eq!(output.publication_plan().captures().len(), 1);
    assert_eq!(output.publication_plan().captures()[0].component_count(), 5);
    output.publication_plan().validate()?;
    let (batch, acquisition, capture_material, publication_plan, telemetry) = output.into_parts();
    assert_eq!(
        capture_material.receipt().terminal(),
        ProviderCaptureTerminalDisposition::CompleteRequestGraph
    );
    assert_eq!(capture_material.receipt().request_graph_components().len(), 5);
    assert_eq!(capture_material.records().len(), 5);
    assert_eq!(capture_material.records()[4].payload(), DATA_RESPONSE);
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path())?;
    let store = paths.sealed_research_journal_store()?;
    let sealed_capture = capture_material.seal(&store)?;
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
    let doctor_material = capture_material(
        &metadata,
        &doctor_capture,
        Bytes::from_static(DOCTOR_RESPONSE),
    )?;
    let sealed_doctor_capture = doctor_material.seal(&store)?;
    let activation = CensusActivationCandidate::try_new(
        source.activation_plan()?,
        &doctor_report,
        &sealed_doctor_capture,
        now,
    )?;
    activation.validate(&doctor_report, &sealed_doctor_capture, now)?;
    let publication_candidate = CensusPublicationCandidate::try_new(
        publication_plan,
        &sealed_capture,
        &activation,
    )?;
    publication_candidate.validate(&activation, now)?;
    publication_candidate.validate_batch(&batch)?;
    assert_eq!(publication_candidate.revision_plan(&batch)?.len(), 1);
    assert_eq!(publication_candidate.canonical_record_count(), 1);
    assert_eq!(
        publication_candidate.canonical_schema().as_str(),
        market_squawk_sources::CURRENT_RESEARCH_RECORD_SCHEMA
    );
    assert_ne!(
        publication_candidate.canonical_schema_fingerprint().bytes(),
        [0; 32]
    );
    assert_eq!(
        publication_candidate.sealed_capture_receipt_digest(),
        sealed_capture.receipt_digest()
    );
    let candidate_wire = serde_json::to_string(&publication_candidate)?;
    assert!(!candidate_wire.contains("\"generation\""));
    assert!(!candidate_wire.contains("\"manifest_digest\""));
    assert!(!candidate_wire.contains("\"published_at\""));
    let presentation = source
        .config()
        .owner_authorization()
        .presentation_obligation()?;
    presentation.validate()?;
    assert_eq!(
        publication_candidate.presentation_obligation_digest(),
        presentation.obligation_digest()
    );
    assert!(presentation.prominent_display_required());
    assert!(presentation.reidentification_prohibited());
    assert_eq!(
        source
            .config()
            .owner_authorization()
            .authorize(CensusIntendedUse::Sale),
        Err(CensusPolicyError::ProhibitedUse)
    );
    assert_eq!(
        sealed_capture.capture().request_graph_components()[4].observation_digest(),
        acquisition.data().capture().observation_digest()
    );
    assert_eq!(telemetry.requests(), 1);
    assert_eq!(telemetry.successful_responses(), 1);
    assert_eq!(source.telemetry().requests(), 1);
    assert_eq!(
        source.telemetry().response_bytes(),
        DATA_RESPONSE.len() as u64
    );
    assert_eq!(transport.attempts.load(Ordering::Relaxed), 1);
    assert!(
        CensusPredicate::try_new("NAME", CensusPredicateType::String, ["A:B"]).is_err()
    );
    assert!(CensusDataQuery::try_new(
        CensusDataset::try_new(2024, "acs/acs1")?,
        CensusSelection::variables(["B01001_001E"])?,
        Vec::new(),
        CensusGeography::standard(
            CensusGeographyClause::try_new(
                "state",
                [CensusGeographyCode::try_new("*")?],
            )?,
            Vec::new(),
        )?,
        Some(CensusTimePredicate::At {
            point: CensusTimePoint::year(2024)?,
        }),
    )
    .is_err());

    let annotation_variable = SourceIdentifier::try_from("B01001_001EA")?;
    let exact_annotation = CensusAnnotationMatch::try_new(annotation_variable.clone(), "(X)")?;
    let exact_missing = MacroMissingValue::new(
        SourceIdentifier::try_from("(X)")?,
        Some(annotation_variable.clone()),
    );
    let annotation_rule = CensusAnnotatedMissingRule::try_new(
        [exact_annotation],
        exact_missing.clone(),
    )?;
    assert_eq!(annotation_rule.missing(), &exact_missing);
    assert!(CensusAnnotatedMissingRule::try_new(
        [CensusAnnotationMatch::try_new(annotation_variable, "(X)")?],
        MacroMissingValue::new(
            SourceIdentifier::try_from("generic-missing")?,
            Some(SourceIdentifier::try_from("B01001_001EA")?),
        ),
    )
    .is_err());

    let ResearchObservation::Macro(observation) =
        serde_json::from_slice(batch.records()[0].payload())?
    else {
        return Err("expected canonical macro observation".into());
    };
    assert_eq!(observation.unit().as_str(), "people");
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
    let dataset = crate::CensusDataset::try_new(2024, "acs/acs1")?;
    let query = CensusDataQuery::try_new(
        dataset,
        CensusSelection::variables(["B01001_001E"])?,
        Vec::new(),
        CensusGeography::standard(
            crate::CensusGeographyClause::try_new(
                "state",
                [crate::CensusGeographyCode::try_new("*")?],
            )?,
            Vec::new(),
        )?,
        None,
    )?;
    Ok(CensusDatasetContract::try_new(
        query,
        [CensusVariableMapping::try_new(
            SourceIdentifier::try_from("B01001_001E")?,
            SourceIdentifier::try_from("census.acs.population")?,
            SourceIdentifier::try_from("people")?,
        )?],
        CensusEffectiveTimePolicy::Fixed(ResearchTemporalCoordinate::calendar_date(
            CalendarDate::new(2024, 1, 1)?,
        )),
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
