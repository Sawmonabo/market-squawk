use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream;
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
    RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    AuthorizationSubjectResolutionError, AuthorizationSubjectResolver, BackoffPolicy, BudgetScope,
    CoverageDomain, EndpointPolicy, ExtractionRequest, ExtractionSource, FreshnessPolicy,
    HistoricalCapability, NetworkAccessPolicy, PathScope, ProviderBudgetPolicy, QueryParameterRule,
    QuerySensitivity, SourceCapabilities, SourceClass, SourceCoverage, SourceError, SourceMetadata,
    SourceMetadataInput, SourceObject, SourceProtocolProfile,
};
use tokio_util::sync::CancellationToken;

use crate::{
    FredObservationPage, FredParseLimits, FredRightsArtifact, FredRightsPolicy,
    FredTermsDocumentBytes, FredTermsDocumentRole,
};

use super::http::collect_bounded_stream;
use super::{
    FredApiKey, FredDataset, FredHttpRequest, FredHttpResponse, FredSource, FredSourceError,
    FredTransport, canonical_payloads, system_timestamp,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn api_key_is_exactly_validated_and_never_debugged() -> TestResult {
    let secret = "abcdefghijklmnopqrstuvwxyz123456";
    let key = FredApiKey::try_new(secret.to_owned())?;
    assert!(!format!("{key:?}").contains(secret));
    assert!(FredApiKey::try_new("ABCDEF0123456789ABCDEF0123456789".to_owned()).is_err());
    assert!(FredApiKey::try_new("too-short".to_owned()).is_err());
    Ok(())
}

#[test]
fn dataset_identity_binds_series_and_closed_realtime_interval() -> TestResult {
    let dataset = FredDataset::parse(&SourceIdentifier::try_from(
        "alfred:series-observations:CPIAUCSL:2024-01-01:2024-02-01",
    )?)?;
    assert_eq!(dataset.series_id(), "CPIAUCSL");
    assert_eq!(dataset.realtime_start().to_string(), "2024-01-01");
    assert_eq!(dataset.realtime_end().to_string(), "2024-02-01");
    assert!(
        FredDataset::parse(&SourceIdentifier::try_from(
            "alfred:series-observations:CPIAUCSL:2024-02-01:2024-01-01",
        )?)
        .is_err()
    );
    assert!(FredDataset::parse(&SourceIdentifier::try_from("fred:search:unbounded")?).is_err());
    Ok(())
}

#[tokio::test]
async fn response_body_is_bounded_across_streamed_chunks() {
    let body = stream::iter([
        Ok::<_, std::io::Error>(Bytes::from_static(b"1234")),
        Ok(Bytes::from_static(b"5678")),
    ]);
    assert!(collect_bounded_stream(body, 7).await.is_err());
}

#[test]
fn canonical_payload_preserves_civil_dates_without_a_fabricated_publication_time() -> TestResult {
    let dataset = FredDataset::parse(&SourceIdentifier::try_from(
        "alfred:series-observations:CPIAUCSL:2024-01-01:2024-02-01",
    )?)?;
    let page = FredObservationPage::parse(
        include_bytes!("../../fixtures/observations.json"),
        FredParseLimits::production_defaults(),
    )?;
    let payloads = canonical_payloads(&dataset, &page, Timestamp::from_unix_nanos(77))?;
    let first: serde_json::Value = serde_json::from_slice(&payloads[0])?;
    assert_eq!(first["observation_date"], "2023-01-01");
    assert_eq!(first["realtime_start"], "2024-01-01");
    assert_eq!(first["received_at_unix_nanos"], 77);
    assert!(first.get("published_at").is_none());
    assert!(first.get("effective_at").is_none());
    Ok(())
}

#[test]
fn source_implements_the_sync_extraction_contract() {
    fn assert_contract<T: ExtractionSource + Sync>() {}
    assert_contract::<FredSource>();
}

#[tokio::test]
async fn discovery_and_ephemeral_extraction_are_exact_source_request_and_payload_bound()
-> TestResult {
    let now = system_timestamp()?;
    let response = FredHttpResponse {
        status: 200,
        retry_after: None,
        content_encoding: None,
        body: Bytes::from_static(include_bytes!("../../fixtures/observations.json")),
        received_at: now,
    };
    let source = source(now, response, "fred-discovery-test-user")?;
    let deadline = now.checked_add_nanos(10_000_000_000)?;
    let instant_request = market_squawk_sources::DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alfred:series-observations:CPIAUCSL:2024-01-01:2024-01-31")?,
        Some(now),
        NonZeroU16::new(1).ok_or("nonzero result limit")?,
        deadline,
    )?;
    assert!(matches!(
        source
            .discover(instant_request, CancellationToken::new())
            .await,
        Err(market_squawk_sources::ExtractionSourceError::Source(
            SourceError::InvalidProtocolState
        ))
    ));
    let discovery = market_squawk_sources::DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alfred:series-observations:CPIAUCSL:2024-01-01:2024-01-31")?,
        None,
        NonZeroU16::new(1).ok_or("nonzero result limit")?,
        deadline,
    )?;
    let discovered = source.discover(discovery, CancellationToken::new()).await?;
    assert_eq!(discovered.objects().len(), 1);
    let object = discovered.objects()[0].clone();
    assert!(
        object
            .evidence()
            .version_pinned_locator()
            .is_some_and(|locator| !locator
                .reference()
                .as_str()
                .contains("abcdefghijklmnopqrstuvwxyz123456"))
    );
    let transplanted = SourceObject::try_new(
        SourceId::try_from("another-fred-source")?,
        object.metadata_revision().clone(),
        discovered.request(),
        object.object_id().clone(),
        object.media_type().clone(),
        object.evidence().clone(),
        object.effective_interval(),
        object.published_at(),
        object.expected_bytes(),
    )?;
    let transplanted_request = ExtractionRequest::try_new(
        transplanted,
        NonZeroU32::new(2).ok_or("nonzero record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero byte limit")?,
        deadline,
    )?;
    assert!(matches!(
        source
            .extract_page_ephemeral(&transplanted_request, CancellationToken::new())
            .await,
        Err(market_squawk_sources::ExtractionSourceError::Source(
            SourceError::InvalidProtocolState
        ))
    ));
    let extraction = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(2).ok_or("nonzero record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero byte limit")?,
        deadline,
    )?;
    let page = source
        .extract_page_ephemeral(&extraction, CancellationToken::new())
        .await?;
    assert_eq!(page.canonical_payloads().len(), 2);
    assert_eq!(page.received_at(), now);
    Ok(())
}

#[tokio::test]
async fn provider_refusals_apply_retry_after_to_the_shared_budget() -> TestResult {
    let now = system_timestamp()?;
    for (status, subject) in [
        (429, "fred-rate-limit-test-user"),
        (503, "fred-unavailable-test-user"),
    ] {
        let source = source(
            now,
            FredHttpResponse {
                status,
                retry_after: Some(b"1".to_vec()),
                content_encoding: None,
                body: Bytes::from_static(b"{}"),
                received_at: now,
            },
            subject,
        )?;
        let request = market_squawk_sources::DiscoveryRequest::try_new(
            SourceIdentifier::try_from("fred:series-observations:CPIAUCSL:2024-01-01:2024-01-31")?,
            None,
            NonZeroU16::new(1).ok_or("nonzero result limit")?,
            now.checked_add_nanos(10_000_000_000)?,
        )?;
        let error = source
            .discover(request, CancellationToken::new())
            .await
            .err()
            .ok_or("provider refusal must stop discovery")?;
        assert!(matches!(
            error,
            market_squawk_sources::ExtractionSourceError::Source(
                SourceError::BudgetWaitUntil { .. }
            )
        ));
    }
    Ok(())
}

#[tokio::test]
async fn non_identity_content_encoding_is_rejected() -> TestResult {
    let now = system_timestamp()?;
    let source = source(
        now,
        FredHttpResponse {
            status: 200,
            retry_after: None,
            content_encoding: Some(b"gzip".to_vec()),
            body: Bytes::from_static(include_bytes!("../../fixtures/observations.json")),
            received_at: now,
        },
        "fred-content-encoding-test-user",
    )?;
    let request = market_squawk_sources::DiscoveryRequest::try_new(
        SourceIdentifier::try_from("fred:series-observations:CPIAUCSL:2024-01-01:2024-01-31")?,
        None,
        NonZeroU16::new(1).ok_or("nonzero result limit")?,
        now.checked_add_nanos(10_000_000_000)?,
    )?;
    assert!(matches!(
        source.discover(request, CancellationToken::new()).await,
        Err(market_squawk_sources::ExtractionSourceError::Source(
            SourceError::InvalidProtocolState
        ))
    ));
    Ok(())
}

#[derive(Debug)]
struct ScriptedTransport {
    response: FredHttpResponse,
}

impl FredTransport for ScriptedTransport {
    fn execute(
        &self,
        request: FredHttpRequest,
        _max_bytes: usize,
        _timeout: std::time::Duration,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<FredHttpResponse, FredSourceError>> {
        Box::pin(async move {
            if cancellation.is_cancelled() || request.public_url.as_str().contains("api_key") {
                return Err(FredSourceError::Cancelled);
            }
            Ok(self.response.clone())
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

fn source(
    now: Timestamp,
    response: FredHttpResponse,
    authorization_subject: &'static str,
) -> TestResult<FredSource> {
    let subject = SourceIdentifier::try_from(authorization_subject)?;
    let metadata = metadata(now, subject.clone())?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(
        Arc::new(TestSubjectResolver { subject }),
    )?;
    let registered = registry.register(metadata.clone(), now)?;
    Ok(FredSource::try_new_with_transport(
        metadata,
        &registered,
        FredApiKey::try_new("abcdefghijklmnopqrstuvwxyz123456".to_owned())?,
        rights(now)?,
        Arc::new(ScriptedTransport { response }),
        2,
    )?)
}

fn metadata(now: Timestamp, authorization_subject: SourceIdentifier) -> TestResult<SourceMetadata> {
    let effective = EffectiveInterval::new(now.checked_sub_nanos(1)?, None)?;
    let evidence = exact_evidence(b"fred-metadata");
    let provider = SourceIdentifier::try_from("fred")?;
    let basis = AuthorizationBasis::new(authorization_subject);
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        basis.clone(),
        evidence.clone(),
        effective,
    );
    let query_rules = [
        ("api_key", QuerySensitivity::Secret, 32),
        ("series_id", QuerySensitivity::Public, 120),
        ("realtime_start", QuerySensitivity::Public, 10),
        ("realtime_end", QuerySensitivity::Public, 10),
        ("limit", QuerySensitivity::Public, 6),
        ("offset", QuerySensitivity::Public, 20),
        ("sort_order", QuerySensitivity::Public, 4),
        ("order_by", QuerySensitivity::Public, 32),
        ("output_type", QuerySensitivity::Public, 1),
        ("file_type", QuerySensitivity::Public, 4),
    ]
    .into_iter()
    .map(|(key, sensitivity, max)| {
        QueryParameterRule::try_new(SourceIdentifier::try_from(key)?, max, false, sensitivity)
            .map_err(Into::into)
    })
    .collect::<TestResult<Vec<_>>>()?;
    let endpoint = ApiEndpointRule::try_new(
        "https://api.stlouisfed.org/fred/series/observations",
        PathScope::Exact,
        query_rules,
        10,
        1024,
    )?;
    let network = EndpointPolicy::try_from_api_rules(
        vec![endpoint],
        market_squawk_sources::HttpRequestBounds::default(),
    )?;
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::with_authorization_account(
            provider.clone(),
            basis.as_source_identifier().clone(),
        ),
        NonZeroU32::new(120).ok_or("nonzero request budget")?,
        NonZeroU64::new(60_000_000_000).ok_or("nonzero request window")?,
        NonZeroU16::new(2).ok_or("nonzero concurrency")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000).ok_or("nonzero backoff")?,
            NonZeroU64::new(60_000_000_000).ok_or("nonzero max backoff")?,
            0,
        )?,
    )?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("fred-official-api")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("fred-api-v1-test")?),
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
        FreshnessPolicy::try_new(
            60_000_000_000,
            60_000_000_000,
            60_000_000_000,
            60_000_000_000,
            1_000_000_000,
        )?,
        Some(budget),
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::RevisionPreserving,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))?)
}

fn rights(now: Timestamp) -> TestResult<FredRightsPolicy> {
    let assessed = now.checked_sub_nanos(1_000_000_000)?;
    let review = now.checked_add_nanos(60_000_000_000)?;
    let artifact = format!(
        r#"{{
            "schema_version":2,
            "series_scope":"unresolved",
            "terms_bundle_digest":"06323e093a0db740245f21c0cc89682998bfbfa0d1c02bd02691d72780659ce7",
            "terms_documents":[
                {{
                    "role":"api_terms",
                    "url":"https://fred.stlouisfed.org/docs/api/terms_of_use.html",
                    "sha256":"27d66951a524848e3777300299a69ef16f868ab2dbc9ca04a00ddea0b4db13bd",
                    "byte_length":20
                }},
                {{
                    "role":"fred_services_legal_terms",
                    "url":"https://fred.stlouisfed.org/legal/",
                    "sha256":"97da0ed4fc87909604e691990b7344467c66e7b1bc9424a2bfbcf41dcf25b9e5",
                    "byte_length":25
                }},
                {{
                    "role":"privacy_policy",
                    "url":"https://www.stlouisfed.org/about-us/privacy-policy/online-notice",
                    "sha256":"2b4d39194871cb7e47314173f79cf2491ac46edb364c4bf17e7ab98749ebe722",
                    "byte_length":41
                }}
            ],
            "assessed_at_unix_nanos":{},
            "review_required_by_unix_nanos":{},
            "operations":["persist"],
            "disposition":"blocked_unknown_rights",
            "confirmed_facts":["test"],
            "engineering_inferences":["test"],
            "sources":[
                {{
                    "url":"https://fred.stlouisfed.org/docs/api/terms_of_use.html",
                    "accessed_on":"2026-07-21",
                    "sha256":"27d66951a524848e3777300299a69ef16f868ab2dbc9ca04a00ddea0b4db13bd",
                    "byte_length":20,
                    "evidence_class":"confirmed"
                }},
                {{
                    "url":"https://fred.stlouisfed.org/legal/",
                    "accessed_on":"2026-07-21",
                    "sha256":"97da0ed4fc87909604e691990b7344467c66e7b1bc9424a2bfbcf41dcf25b9e5",
                    "byte_length":25,
                    "evidence_class":"confirmed"
                }},
                {{
                    "url":"https://www.stlouisfed.org/about-us/privacy-policy/online-notice",
                    "accessed_on":"2026-07-21",
                    "sha256":"2b4d39194871cb7e47314173f79cf2491ac46edb364c4bf17e7ab98749ebe722",
                    "byte_length":41,
                    "evidence_class":"confirmed"
                }}
            ]
        }}"#,
        assessed.unix_nanos(),
        review.unix_nanos()
    );
    let terms_bytes = [
        FredTermsDocumentBytes::try_new(FredTermsDocumentRole::ApiTerms, b"exact FRED API terms")?,
        FredTermsDocumentBytes::try_new(
            FredTermsDocumentRole::FredServicesLegalTerms,
            b"exact FRED services terms",
        )?,
        FredTermsDocumentBytes::try_new(
            FredTermsDocumentRole::PrivacyPolicy,
            b"exact St. Louis Fed online privacy notice",
        )?,
    ];
    let artifact = FredRightsArtifact::parse(artifact.as_bytes(), &terms_bytes)?;
    Ok(FredRightsPolicy::try_new(
        artifact.terms_evidence().clone(),
        Vec::new(),
    )?)
}

fn exact_evidence(bytes: &[u8]) -> ExactPayloadEvidence {
    use sha2::Digest;

    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        sha2::Sha256::digest(bytes).into(),
    ))
}
