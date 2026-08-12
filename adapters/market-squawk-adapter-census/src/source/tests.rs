use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AuthorizationBasis, CalendarDate, ChecksumCapability, CoverageDelay, DeliveryEvidence,
    EffectiveInterval, MetadataRevision, ResearchObservation, RevisionBoundPayloadEvidence,
    SchemaVersion, SequenceCapability, SourceId,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationSubjectResolutionError,
    AuthorizationSubjectResolver, BackoffPolicy, BudgetScope, BudgetWindowSemantics,
    CoverageDomain, DiscoveryRequest, EndpointPolicy, ExtractionRequest, FreshnessPolicy,
    HttpRequestBounds, NetworkAccessPolicy, ProviderBudgetPolicy, ProviderBudgetWindow,
    SourceCapabilities, SourceCoverage, SourceMetadataInput,
};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const DATA_RESPONSE: &[u8] = br#"[
  ["B01001_001E", "state"],
  ["42", "06"]
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
      "required": false
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
                || request.authorized.key_query_value() != "test-census-key"
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
    )?;
    let subject = SourceIdentifier::try_from("census-test-key-record")?;
    let metadata = source_metadata(now, subject.clone(), &config)?;
    let transport = Arc::new(ScriptedTransport {
        expected_url: contract.query().redacted_url().to_owned(),
        response: Mutex::new(Some(CensusHttpResponse {
            status: 200,
            retry_after: None,
            content_encoding: None,
            content_type: Some(b"application/json; charset=utf-8".to_vec()),
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
    let output = extraction_output(&metadata, &extraction_request, &contract, acquisition)?;

    assert_eq!(output.batch().records().len(), 1);
    assert_eq!(output.metadata_captures().len(), 4);
    assert_eq!(output.captures().len(), 5);
    let data_capture = output.data_capture().ok_or("missing data capture")?;
    assert_eq!(
        data_capture.receipt(),
        output.acquisition().data().capture()
    );
    assert_eq!(data_capture.records().len(), 1);
    assert_eq!(data_capture.records()[0].payload(), DATA_RESPONSE);
    assert_eq!(output.telemetry().requests(), 1);
    assert_eq!(output.telemetry().successful_responses(), 1);
    assert_eq!(source.telemetry().requests(), 1);
    assert_eq!(
        source.telemetry().response_bytes(),
        DATA_RESPONSE.len() as u64
    );
    assert_eq!(transport.attempts.load(Ordering::Relaxed), 1);

    let ResearchObservation::Macro(observation) =
        serde_json::from_slice(output.batch().records()[0].payload())?
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
    let windows = [
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(1).ok_or("nonzero second budget")?,
            NonZeroU64::new(ONE_SECOND_NANOS).ok_or("nonzero second window")?,
            BudgetWindowSemantics::Sliding,
        )?,
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(400).ok_or("nonzero daily budget")?,
            NonZeroU64::new(ONE_DAY_NANOS).ok_or("nonzero daily window")?,
            BudgetWindowSemantics::Sliding,
        )?,
    ];
    let budget = ProviderBudgetPolicy::try_new_conjunctive(
        BudgetScope::with_authorization_account(provider.clone(), subject),
        &windows,
        NonZeroU16::MIN,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000).ok_or("nonzero initial backoff")?,
            NonZeroU64::new(60_000_000_000).ok_or("nonzero maximum backoff")?,
            0,
        )?,
    )?;
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
