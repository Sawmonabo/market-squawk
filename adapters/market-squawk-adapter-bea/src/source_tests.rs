use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
    RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_platform::LocalPaths;
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    AuthorizationSubjectResolutionError, AuthorizationSubjectResolver, CoverageDomain,
    EndpointPolicy, FreshnessPolicy, HistoricalCapability, HttpRequestBounds, NetworkAccessPolicy,
    ProviderCaptureTerminalDisposition, SourceCapabilities, SourceClass, SourceCoverage,
    SourceMetadata, SourceMetadataInput, SourceProtocolProfile,
};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::auth::BeaSensitiveBody;
use crate::source::bea_api_endpoint_rule;
use crate::transport::{BeaHttpResponse, BeaSensitiveHeader, BeaTransport, system_timestamp};
use crate::{
    BeaAuthorizedRequest, BeaDatasetContract, BeaDatasetIdentity, BeaObservationValue,
    BeaParseLimits, BeaPublicationCandidate, BeaRequiredSharedSettlement,
    BeaSealedAcquisitionReceipt, BeaSource, BeaSourceConfig, BeaSourceError, BeaUserId,
};

const USER_ID: &str = "11111111-2222-3333-4444-555555555555";

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("market-squawk-bea-{}", Uuid::new_v4())))
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
    responses: Mutex<VecDeque<Bytes>>,
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

impl BeaTransport for ScriptedTransport {
    fn execute<'a>(
        &'a self,
        request: BeaAuthorizedRequest,
        in_flight: &'a market_squawk_sources::InFlightExtractionRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<BeaHttpResponse, BeaSourceError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(BeaSourceError::Cancelled);
            }
            if timeout.is_zero() {
                return Err(BeaSourceError::DeadlineExceeded);
            }
            in_flight
                .validate_current()
                .map_err(|_| BeaSourceError::Authority)?;
            if !request.expose_url().contains("UserID=")
                || !request.expose_url().contains("Method=")
            {
                return Err(BeaSourceError::Protocol);
            }
            let body = self
                .responses
                .lock()
                .map_err(|_| BeaSourceError::Network)?
                .pop_front()
                .ok_or(BeaSourceError::Network)?;
            if body.is_empty() || body.len() > max_bytes {
                return Err(BeaSourceError::BodyTooLarge);
            }
            Ok(BeaHttpResponse {
                status: 200,
                retry_after: None,
                content_encoding: None,
                content_type: Some(BeaSensitiveHeader::try_from_vec(
                    b"application/json; charset=utf-8".to_vec(),
                )?),
                body: BeaSensitiveBody::from_vec(body.to_vec()),
                received_at: system_timestamp()?,
                latency: Duration::from_millis(1),
            })
        })
    }
}

#[tokio::test]
async fn metadata_first_transport_retains_exact_capture_material_and_completeness() -> TestResult {
    let dataset = BeaDatasetIdentity::try_new("Regional")?;
    let mut parameters = BTreeMap::new();
    parameters.insert(
        crate::BeaParameterIdentity::try_new("TableName")?,
        "SAINC1".to_owned(),
    );
    let contract = BeaDatasetContract::try_new(dataset, parameters, Some(1))?;
    let provider_dataset = contract.dataset_id().clone();
    let config = BeaSourceConfig::try_new(vec![contract], BeaParseLimits::production_defaults())?;
    let upstream_responses = responses()?;
    let user_id = BeaUserId::try_new(USER_ID.to_owned())?;
    let malicious_header = format!("{}?UserID={USER_ID}", crate::BEA_API_ENDPOINT);
    let mut malicious_response = BeaHttpResponse {
        status: 200,
        retry_after: None,
        content_encoding: None,
        content_type: Some(BeaSensitiveHeader::try_from_vec(
            malicious_header.into_bytes(),
        )?),
        body: BeaSensitiveBody::from_vec(upstream_responses[0].to_vec()),
        received_at: system_timestamp()?,
        latency: Duration::from_millis(1),
    };
    let malicious_debug = format!("{malicious_response:?}");
    assert!(!malicious_debug.contains(USER_ID));
    assert!(!malicious_debug.contains(crate::BEA_API_ENDPOINT));
    assert!(!malicious_debug.contains("UserID"));
    assert!(
        malicious_response
            .retain_secret_free_headers(&user_id)
            .is_err()
    );
    assert!(
        malicious_response
            .content_type
            .as_ref()
            .is_some_and(BeaSensitiveHeader::is_zeroized)
    );
    let transport = Arc::new(ScriptedTransport {
        responses: Mutex::new(VecDeque::from(upstream_responses.clone())),
    });
    let now = system_timestamp()?;
    let metadata = source_metadata(now, &config)?;
    let source = BeaSource::try_new_with_transport(
        metadata.clone(),
        user_id,
        config,
        digest_evidence(b"bea-test-credential-generation-v1"),
        transport.clone(),
    )?;
    let mut registry =
        AuthoritativeSourceRegistry::try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(
            Arc::new(TestSubjectResolver {
                subject: market_squawk_sources::ProviderRateDeclaration::governed_provider_subject(
                    &SourceIdentifier::try_from("bea")?,
                )?,
            }),
        )?;
    let registered = registry.register(metadata, now)?;
    let authority = registry.extraction_authority(&registered, &source)?;
    let deadline = now.checked_add_nanos(30_000_000_000)?;

    let run = source
        .doctor(
            &authority,
            &provider_dataset,
            deadline,
            CancellationToken::new(),
        )
        .await?;
    let acquisition = run.acquisition();

    assert_eq!(acquisition.metadata().pages().len(), 3);
    assert_eq!(acquisition.data().page().observations().len(), 1);
    assert_eq!(
        acquisition.data().request().query().supplied_parameters(),
        &BTreeMap::from([(
            crate::BeaParameterIdentity::try_new("TableName")?,
            "SAINC1".to_owned(),
        )])
    );
    assert_eq!(
        acquisition.data().page().observations()[0]
            .identity()
            .table(),
        Some("SAINC1")
    );
    assert!(matches!(
        acquisition.data().page().observations()[0].value(),
        BeaObservationValue::Observed { .. }
    ));
    for (captured, upstream) in acquisition
        .metadata()
        .pages()
        .iter()
        .zip(upstream_responses.iter())
    {
        assert_secret_free_capture(captured.page().receipt(), captured.material(), upstream)?;
    }
    let data_material = acquisition.data().material();
    assert_secret_free_capture(
        acquisition.data().page().receipt(),
        data_material,
        upstream_responses.last().ok_or("missing data response")?,
    )?;
    assert_eq!(data_material.receipt().pages().len(), 1);
    assert_eq!(data_material.records().len(), 1);
    assert_eq!(
        u64::try_from(data_material.records()[0].payload().len())?,
        data_material.receipt().total_body_bytes()
    );
    assert_eq!(source.telemetry().requests(), 4);
    assert_eq!(source.telemetry().successful_responses(), 4);
    assert_eq!(source.telemetry().returned_rows(), 1);
    assert_eq!(run.receipt().request_count(), 4);
    assert_eq!(run.receipt().returned_rows(), 1);
    let provider_production_time = acquisition
        .data()
        .page()
        .production_time()
        .ok_or("missing provider production time")?
        .timestamp();
    let received_at = data_material.receipt().pages()[0].received_at();
    assert!(provider_production_time <= received_at);
    assert_eq!(
        run.receipt().source_production_time(),
        Some(provider_production_time)
    );
    assert!(!format!("{source:?}").contains(USER_ID));
    assert!(!format!("{run:?}").contains(USER_ID));
    assert!(!format!("{run:?}").contains(crate::BEA_API_ENDPOINT));
    let (doctor_receipt, evidence, graph) = run.into_sealing_parts()?;
    assert_eq!(graph.receipt().pages().len(), 4);
    assert_eq!(graph.receipt().request_graph_components().len(), 4);
    assert_eq!(
        graph.receipt().terminal(),
        ProviderCaptureTerminalDisposition::CompleteRequestGraph
    );
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path())?;
    let store = paths.sealed_research_journal_store()?;
    let sealed = BeaSealedAcquisitionReceipt::try_new(evidence, graph.seal(&store)?)?;
    let admission = doctor_receipt.bind_sealed(source.source_binding(), &sealed)?;
    source.activate_doctor(&admission)?;
    assert_eq!(
        source.source_binding().source_id(),
        authority.metadata().source_id()
    );
    assert_eq!(
        source.source_binding().metadata_revision(),
        authority.metadata().revision()
    );
    assert_eq!(
        source.quota_declaration().required_shared_settlements(),
        &[
            BeaRequiredSharedSettlement::ResponseBytes,
            BeaRequiredSharedSettlement::ProviderErrors,
        ]
    );
    let candidate = BeaPublicationCandidate::try_new(
        source.source_binding(),
        &admission,
        sealed,
        NonZeroU32::new(1).ok_or("canonical record bound")?,
    )?;
    candidate.validate()?;
    assert_eq!(candidate.observations().len(), 1);
    assert_eq!(
        candidate.observations()[0]
            .observation()
            .value()
            .observed_value(),
        Some(rust_decimal::Decimal::from(65_000))
    );
    assert_eq!(candidate.rejoin_coordinates().row_count(), 1);
    assert_eq!(candidate.rejoin_coordinates().data_component_ordinal(), 3);
    assert_eq!(
        candidate.rejoin_coordinates().dataset_id(),
        &provider_dataset
    );
    assert_eq!(
        candidate.rejoin_coordinates().provider_dataset().as_str(),
        "Regional"
    );
    assert_eq!(
        candidate.rejoin_coordinates().metadata_generation(),
        admission.metadata_generation()
    );
    let expected_data_upstream_digest = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(upstream_responses.last().ok_or("missing data response")?).into(),
    );
    assert_eq!(
        candidate
            .rejoin_coordinates()
            .data_upstream_response_digest(),
        expected_data_upstream_digest
    );
    assert_eq!(
        candidate.observations()[0].upstream_response_digest(),
        expected_data_upstream_digest
    );
    assert_eq!(
        candidate.rejoin_coordinates().candidate_digest(),
        candidate.candidate_digest()
    );
    assert_eq!(candidate.revision_plan().len(), 1);
    assert!(candidate.revision_plan().is_locally_observed());
    let research_observations = candidate.research_observations()?;
    let revision_batch = candidate.revision_plan().clone().into_observed_batch(
        candidate.rejoin_coordinates().source_id().clone(),
        &research_observations,
    )?;
    assert_eq!(revision_batch.input_len(), 1);
    assert_eq!(revision_batch.unique_records().len(), 1);
    let handoff = candidate.into_shared_publication_parts();
    assert_eq!(handoff.observations().len(), 1);
    assert_eq!(handoff.revision_plan().len(), handoff.observations().len());
    assert_eq!(
        handoff.coordinates().acquisition_capture_receipt_digest(),
        handoff
            .sealed_acquisition()
            .sealed_capture()
            .receipt_digest()
    );
    assert!(
        transport
            .responses
            .lock()
            .map_err(|_| "poisoned scripted response queue")?
            .is_empty()
    );
    Ok(())
}

fn assert_secret_free_capture(
    receipt: &crate::BeaPageReceipt,
    material: &market_squawk_sources::ProviderCaptureMaterial,
    upstream: &Bytes,
) -> TestResult {
    let retained = material
        .records()
        .first()
        .ok_or("missing retained response")?
        .payload();
    assert_eq!(material.records().len(), 1);
    assert_eq!(
        receipt.upstream_response_digest(),
        <[u8; 32]>::from(Sha256::digest(upstream))
    );
    assert_eq!(
        receipt.response_digest(),
        material.receipt().pages()[0].body_digest().bytes()
    );
    assert_ne!(
        receipt.upstream_response_digest(),
        receipt.response_digest()
    );
    assert!(
        !retained
            .windows(USER_ID.len())
            .any(|value| value == USER_ID.as_bytes())
    );
    assert!(
        retained
            .windows(36)
            .any(|value| value == b"************************************")
    );
    assert!(
        !retained
            .windows(crate::BEA_API_ENDPOINT.len())
            .any(|value| value == crate::BEA_API_ENDPOINT.as_bytes())
    );
    Ok(())
}

fn source_metadata(now: Timestamp, config: &BeaSourceConfig) -> TestResult<SourceMetadata> {
    let effective = EffectiveInterval::new(now.checked_sub_nanos(1)?, None)?;
    let provider = SourceIdentifier::try_from("bea")?;
    let evidence = exact_evidence(b"bea-local-transport-contract-metadata-v1");
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        AuthorizationBasis::new(
            market_squawk_sources::ProviderRateDeclaration::governed_provider_subject(&provider)?,
        ),
        evidence.clone(),
        effective,
    );
    let network = EndpointPolicy::try_from_api_rules(
        vec![bea_api_endpoint_rule(config)?],
        HttpRequestBounds::default(),
    )?;
    let budget = crate::bea_provider_rate_declaration()?.policy().clone();
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("bea-local-transport-contract")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("bea-local-contract-v1")?),
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

fn exact_evidence(bytes: &[u8]) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(digest_evidence(bytes))
}

fn digest_evidence(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn responses() -> Result<Vec<Bytes>, serde_json::Error> {
    [
        json!({
            "BEAAPI": {
                "Request": {"RequestParam": [
                    {"ParameterName": "USERID", "ParameterValue": USER_ID},
                    {"ParameterName": "METHOD", "ParameterValue": "GETDATASETLIST"},
                    {"ParameterName": "RESULTFORMAT", "ParameterValue": "JSON"}
                ]},
                "Results": {"Dataset": [{
                    "DatasetName": "Regional",
                    "DatasetDescription": "Regional data"
                }]}
            }
        }),
        json!({
            "BEAAPI": {
                "Request": {"RequestParam": [
                    {"ParameterName": "USERID", "ParameterValue": USER_ID},
                    {"ParameterName": "METHOD", "ParameterValue": "GETPARAMETERLIST"},
                    {"ParameterName": "DATASETNAME", "ParameterValue": "REGIONAL"},
                    {"ParameterName": "RESULTFORMAT", "ParameterValue": "JSON"}
                ]},
                "Results": {"Parameter": [{
                    "ParameterName": "TableName",
                    "ParameterDataType": "string",
                    "ParameterDescription": "Regional table",
                    "ParameterIsRequiredFlag": "1",
                    "MultipleAcceptedFlag": "0"
                }]}
            }
        }),
        json!({
            "BEAAPI": {
                "Request": {"RequestParam": [
                    {"ParameterName": "USERID", "ParameterValue": USER_ID},
                    {"ParameterName": "METHOD", "ParameterValue": "GETPARAMETERVALUES"},
                    {"ParameterName": "DATASETNAME", "ParameterValue": "REGIONAL"},
                    {"ParameterName": "PARAMETERNAME", "ParameterValue": "TABLENAME"},
                    {"ParameterName": "RESULTFORMAT", "ParameterValue": "JSON"}
                ]},
                "Results": {"ParamValue": [{"Key": "SAINC1", "Desc": "Income"}]}
            }
        }),
        json!({
            "BEAAPI": {
                "Request": {"RequestParam": [
                    {"ParameterName": "USERID", "ParameterValue": USER_ID},
                    {"ParameterName": "METHOD", "ParameterValue": "GETDATA"},
                    {"ParameterName": "DATASETNAME", "ParameterValue": "REGIONAL"},
                    {"ParameterName": "TABLENAME", "ParameterValue": "SAINC1"},
                    {"ParameterName": "RESULTFORMAT", "ParameterValue": "JSON"}
                ]},
                "Results": {
                    "UTCProductionTime": "2026-03-25T19:25:39.113",
                    "Dimensions": [
                        {"Ordinal": "1", "Name": "TimePeriod", "DataType": "string", "IsValue": "0"},
                        {"Ordinal": "2", "Name": "DataValue", "DataType": "numeric", "IsValue": "1"},
                        {"Ordinal": "3", "Name": "CL_UNIT", "DataType": "string", "IsValue": "0"},
                        {"Ordinal": "4", "Name": "UNIT_MULT", "DataType": "numeric", "IsValue": "0"}
                    ],
                    "Data": [{
                        "TimePeriod": "2025",
                        "DataValue": "65",
                        "CL_UNIT": "Dollars",
                        "UNIT_MULT": "3"
                    }]
                }
            }
        }),
    ]
    .into_iter()
    .map(|value| serde_json::to_vec(&value).map(Bytes::from))
    .collect()
}
