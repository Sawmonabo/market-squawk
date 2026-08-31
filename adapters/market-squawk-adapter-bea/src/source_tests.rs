use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::Write as _;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_data::SqliteProviderRateStore;
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
    ResearchObservation, RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_platform::{LocalAuthorityStateStore, LocalPaths};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    AuthorizationSubjectResolutionError, AuthorizationSubjectResolver, CoverageDomain,
    DiscoveryRequest, EndpointPolicy, ExtractionAuthorityError, ExtractionRequest, FreshnessPolicy,
    HistoricalCapability, HttpRequestBounds, NetworkAccessPolicy, ProviderRateAuthority,
    ProviderRateResponseClass, ProviderRateRetryAfterDisposition, SourceCapabilities, SourceClass,
    SourceCoverage, SourceMetadata, SourceMetadataInput, SourceProtocolProfile,
};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::auth::BeaSensitiveBody;
use crate::source::bea_api_endpoint_rule;
use crate::transport::{BeaHttpResponse, BeaSensitiveHeader, BeaTransport, system_timestamp};
use crate::{
    BeaAuthorizedRequest, BeaDatasetContract, BeaDatasetIdentity, BeaDoctorRefreshDisposition,
    BeaObservationValue, BeaParseLimits, BeaRequiredSharedSettlement, BeaSource, BeaSourceConfig,
    BeaSourceError, BeaUserId,
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
    let mut escaped_user_id = String::new();
    for byte in USER_ID.bytes() {
        write!(&mut escaped_user_id, "\\u{byte:04x}")?;
    }
    let malformed = format!(
        r#"{{"BEAAPI":{{"Request":{{"RequestParam":[{{"ParameterName":"USERID","ParameterValue":"{USER_ID}"}},{{"ParameterName":"METHOD","ParameterValue":"GETDATASETLIST"}},{{"ParameterName":"RESULTFORMAT","ParameterValue":"JSON"}}]}},"Results":{{"Dataset":[]}}}},"{escaped_user_id}":true}}"#
    );
    let malformed_request = crate::BeaQuery::dataset_list()?.single_page(None)?;
    let malformed_error = match crate::parse_metadata_page(
        malformed.as_bytes(),
        &malformed_request,
        &user_id,
        BeaParseLimits::production_defaults(),
    ) {
        Ok(_) => return Err("credential-bearing malformed fields must fail closed".into()),
        Err(error) => error,
    };
    assert!(matches!(
        malformed_error,
        crate::BeaError::RequestEchoMismatch
    ));
    assert!(!format!("{malformed_error}").contains(USER_ID));
    assert!(!format!("{malformed_error:?}").contains(USER_ID));
    let mut adversarial = malformed[..malformed.len() - 1].as_bytes().to_vec();
    adversarial.extend_from_slice(b",\"padding\":\"");
    adversarial.resize(
        BeaParseLimits::production_defaults()
            .max_bytes()
            .saturating_sub(2),
        b'x',
    );
    adversarial.extend_from_slice(b"\"}");
    let mut sanitization_observations = 0_u8;
    let adversarial_error = match crate::parser::sanitize_response_body_with_control(
        BeaSensitiveBody::from_vec(adversarial),
        &malformed_request,
        &user_id,
        BeaParseLimits::production_defaults(),
        || {
            sanitization_observations = sanitization_observations.saturating_add(1);
            if sanitization_observations > 1 {
                Err(crate::BeaError::SanitizationCancelled)
            } else {
                Ok(())
            }
        },
    ) {
        Ok(_) => return Err("a second decoded credential must fail before a long suffix".into()),
        Err(error) => error,
    };
    assert!(matches!(
        adversarial_error,
        crate::BeaError::RequestEchoMismatch
    ));
    assert_eq!(sanitization_observations, 1);
    let escaped_echo = String::from_utf8(upstream_responses[0].to_vec())?
        .replace(USER_ID, escaped_user_id.as_str());
    let escaped_page = crate::parse_metadata_page(
        escaped_echo.as_bytes(),
        &malformed_request,
        &user_id,
        BeaParseLimits::production_defaults(),
    )?;
    assert_ne!(
        escaped_page.receipt().upstream_response_digest(),
        escaped_page.receipt().response_digest()
    );
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
    let mut scripted_responses = upstream_responses.clone();
    scripted_responses.extend(upstream_responses.iter().take(3).cloned());
    let transport = Arc::new(ScriptedTransport {
        responses: Mutex::new(VecDeque::from(scripted_responses)),
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
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path())?;
    let authority_store = LocalAuthorityStateStore::try_open(
        paths.control_root()?.root().join("bea-source-authority"),
    )?;
    let provider_rate =
        ProviderRateAuthority::try_new(Arc::new(SqliteProviderRateStore::try_open(
            paths
                .control_root()?
                .root()
                .join("provider-rate-authority.sqlite3"),
        )?))?;
    let mut registry =
        AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
            authority_store,
            Arc::new(TestSubjectResolver {
                subject: market_squawk_sources::ProviderRateDeclaration::governed_provider_subject(
                    &SourceIdentifier::try_from("bea")?,
                )?,
            }),
            provider_rate,
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
    assert_eq!(run.metadata().pages().len(), 3);
    for (captured, upstream) in run.metadata().pages().iter().zip(&upstream_responses) {
        assert_secret_free_capture(captured.page().receipt(), captured.material(), upstream)?;
    }
    assert_eq!(source.telemetry().requests(), 3);
    assert_eq!(source.telemetry().successful_responses(), 3);
    assert_eq!(source.telemetry().returned_rows(), 0);
    assert_eq!(run.receipt().request_count(), 3);
    assert_eq!(run.receipt().metadata_rows(), 3);
    assert_eq!(
        run.receipt().metadata_completeness(),
        BeaCompleteness::Complete
    );
    assert!(!format!("{source:?}").contains(USER_ID));
    assert!(!format!("{run:?}").contains(USER_ID));
    assert!(!format!("{run:?}").contains(crate::BEA_API_ENDPOINT));
    let (pending_doctor, doctor_seal_request) = run.into_sealing_parts()?;
    let store = paths.sealed_research_journal_store()?;
    let admission = Arc::new(
        pending_doctor.try_rejoin(source.source_binding(), doctor_seal_request.seal(&store)?)?,
    );
    source.activate_doctor(Arc::clone(&admission))?;
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
    let discovery_request = DiscoveryRequest::try_new(
        provider_dataset.clone(),
        None,
        NonZeroU16::new(1).ok_or("discovery result bound")?,
        deadline,
    )?;
    let discovery = source
        .discover_captured(
            authority.clone(),
            discovery_request,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(discovery.data().page().observations().len(), 1);
    assert_eq!(
        discovery.data().request().query().supplied_parameters(),
        &BTreeMap::from([(
            crate::BeaParameterIdentity::try_new("TableName")?,
            "SAINC1".to_owned(),
        )])
    );
    assert_eq!(
        discovery.data().page().observations()[0].identity().table(),
        Some("SAINC1")
    );
    assert!(matches!(
        discovery.data().page().observations()[0].value(),
        BeaObservationValue::Missing(crate::BeaMissingValue::SuppressedRegional)
    ));
    let data_material = discovery.data().material();
    assert_secret_free_capture(
        discovery.data().page().receipt(),
        data_material,
        upstream_responses.last().ok_or("missing data response")?,
    )?;
    assert_eq!(data_material.receipt().pages().len(), 1);
    assert_eq!(data_material.records().len(), 1);
    assert_eq!(
        u64::try_from(data_material.records()[0].payload().len())?,
        data_material.receipt().total_body_bytes()
    );
    let provider_production_time = discovery
        .data()
        .page()
        .production_time()
        .ok_or("missing provider production time")?
        .timestamp();
    assert!(provider_production_time <= data_material.receipt().pages()[0].received_at());
    assert_eq!(source.telemetry().requests(), 4);
    assert_eq!(source.telemetry().successful_responses(), 4);
    assert_eq!(source.telemetry().returned_rows(), 1);
    let (pending_discovery, discovery_seal_request) = discovery.into_sealing_parts()?;
    let sealed_discovery = pending_discovery.try_rejoin(discovery_seal_request.seal(&store)?)?;
    let discovered_object = sealed_discovery
        .batch()
        .objects()
        .first()
        .ok_or("missing discovered object")?
        .clone();
    let extraction_request = ExtractionRequest::try_new(
        discovered_object,
        NonZeroU32::new(1).ok_or("extraction record bound")?,
        NonZeroU64::new(2 * 1024 * 1024).ok_or("extraction byte bound")?,
        deadline,
    )?;
    let expected_object_id = extraction_request.object().object_id().clone();
    let discovery_capture_identity = extraction_request.object().capture_identity();
    let requests_before_extraction = source.telemetry().requests();
    assert_eq!(requests_before_extraction, 4);
    let candidate = source.extract_sealed_discovery(
        authority.clone(),
        extraction_request,
        sealed_discovery,
        CancellationToken::new(),
    )?;
    assert_eq!(source.telemetry().requests(), requests_before_extraction);
    assert!(
        candidate.observations()[0]
            .observation()
            .value()
            .missing_value()
            .is_some_and(|missing| missing.marker().as_str() == "bea-regional-suppression-l")
    );
    let handoff = candidate.into_shared_publication_parts();
    let source_batch = handoff.batch();
    assert_eq!(
        source_batch.request().object().object_id(),
        &expected_object_id
    );
    assert_ne!(
        source_batch.request().object().capture_identity(),
        discovery_capture_identity
    );
    let provider_content_evidence = source_batch.request().object().evidence().content_digest();
    let native_sidecar: serde_json::Value = serde_json::from_slice(
        handoff
            .native_lineage()
            .batch_sidecar()
            .ok_or("missing BEA native batch sidecar")?
            .semantic_payload(),
    )?;
    assert_eq!(native_sidecar["family"], "bea.dataset-observations");
    assert_eq!(native_sidecar["dataset"], "Regional");
    assert_eq!(native_sidecar["provider_method"], "GetData");
    assert_eq!(native_sidecar["parameters"][0]["name"], "TableName");
    assert_eq!(native_sidecar["parameters"][0]["value"], "SAINC1");
    assert_eq!(
        native_sidecar["dimensions"].as_array().map(Vec::len),
        Some(4)
    );
    assert_eq!(native_sidecar["completeness"], "complete");
    assert_eq!(native_sidecar["returned_rows"], 1);
    assert!(native_sidecar["production_time_unix_nanos"].is_i64());
    let native_semantics: serde_json::Value =
        serde_json::from_slice(handoff.native_lineage().rows()[0].semantic_payload())?;
    assert_eq!(native_semantics["dataset"], "Regional");
    assert_eq!(native_semantics["period"], "2025Q1");
    assert_eq!(native_semantics["missing"], "suppressed_regional");
    assert!(native_semantics.get("production_time").is_none());
    for forbidden in [
        "parameters",
        "request_identity",
        "metadata_generation",
        "completeness",
        "result_attributes",
        "observation_digest",
    ] {
        assert!(native_semantics.get(forbidden).is_none());
    }
    assert_eq!(
        handoff
            .batch()
            .request()
            .object()
            .evidence()
            .content_digest(),
        provider_content_evidence
    );
    let (coordinates, revision_plan, sealed_capture_binding) = handoff.into_parts();
    let batch = sealed_capture_binding.batch();
    let native_lineage = sealed_capture_binding.native_lineage();
    let data_capture = sealed_capture_binding
        .persisted_segment_receipt(0)
        .ok_or("missing sealed BEA observation response")?
        .capture();
    assert!(data_capture.request_graph_components().is_empty());
    assert_eq!(data_capture.pages().len(), 1);
    assert_eq!(
        coordinates.acquisition_capture_receipt_digest(),
        sealed_capture_binding.sealed_capture_receipt_digest()
    );
    assert_eq!(native_lineage.rows().len(), 1);
    let research_observation =
        serde_json::from_slice::<ResearchObservation>(batch.records()[0].payload())?;
    let research_observations = [research_observation];
    let revision_batch = revision_plan.into_observed_batch_with_native_lineage(
        coordinates.source_id().clone(),
        batch,
        &research_observations,
        native_lineage,
    )?;
    assert_eq!(revision_batch.input_len(), 1);
    let refreshed_run = source
        .doctor(
            &authority,
            &provider_dataset,
            deadline,
            CancellationToken::new(),
        )
        .await?;
    let (pending_refreshed, refreshed_seal_request) = refreshed_run.into_sealing_parts()?;
    let refreshed = Arc::new(pending_refreshed.try_rejoin(
        source.source_binding(),
        refreshed_seal_request.seal(&store)?,
    )?);
    assert!(refreshed.verified_at() > admission.verified_at());
    assert!(refreshed.expires_at() > admission.expires_at());
    assert_eq!(
        source.activate_doctor(Arc::clone(&refreshed))?,
        BeaDoctorRefreshDisposition::RefreshedEvidence
    );
    assert!(matches!(
        source.activate_doctor(Arc::clone(&admission)),
        Err(BeaSourceError::StaleDoctorAdmission)
    ));
    assert_eq!(
        source
            .current_doctor_admission(&provider_dataset, refreshed.verified_at())?
            .ok_or("missing monotonic BEA doctor admission")?
            .admission_digest(),
        refreshed.admission_digest()
    );
    assert!(
        transport
            .responses
            .lock()
            .map_err(|_| "poisoned scripted response queue")?
            .is_empty()
    );
    let local_abort_target = crate::BeaQuery::dataset_list()?
        .single_page(None)?
        .authorize(&BeaUserId::try_new(USER_ID.to_owned())?)?
        .expose_url()
        .to_owned();
    for local_error in [
        crate::BeaError::SanitizationCancelled,
        crate::BeaError::SanitizationDeadlineExceeded,
        crate::BeaError::SanitizationClockUnavailable,
    ] {
        let failure = crate::source::BeaCompleteResponseFailure::from_sanitization(local_error);
        assert_eq!(
            failure.response_class(),
            ProviderRateResponseClass::KnownCompleteLocalAbort
        );
        let permit = loop {
            match authority.try_network_request(&local_abort_target) {
                Ok(permit) => break permit,
                Err(ExtractionAuthorityError::BudgetWaitUntil {
                    deadline: budget_deadline,
                }) => {
                    let wait = authority.remaining_budget_wait(budget_deadline)?;
                    let remaining = deadline
                        .unix_nanos()
                        .checked_sub(system_timestamp()?.unix_nanos())
                        .and_then(|nanos| u64::try_from(nanos).ok())
                        .map(Duration::from_nanos)
                        .ok_or("local-abort settlement proof exceeded its deadline")?;
                    if wait > remaining {
                        return Err("local-abort settlement proof exceeded its deadline".into());
                    }
                    tokio::time::sleep(wait).await;
                }
                Err(error) => return Err(error.into()),
            }
        };
        let receipt = crate::source::settle_complete_response(
            permit.authorize_send(&local_abort_target)?,
            137,
            failure.response_class(),
            ProviderRateRetryAfterDisposition::Absent,
            0,
        )?;
        assert_eq!(
            receipt.settlement().response_class(),
            ProviderRateResponseClass::KnownCompleteLocalAbort
        );
        assert_eq!(receipt.settlement().provider_error_units(), 0);
        assert_eq!(receipt.charged_response_bytes(), 137);
        assert_eq!(receipt.consecutive_refusals(), 0);
    }
    registry.shutdown()?;
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
                        "TimePeriod": "2025Q1",
                        "DataValue": "L",
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
