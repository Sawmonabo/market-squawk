//! Consolidated critical adapter-boundary proofs.

use std::collections::VecDeque;
use std::error::Error;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
    RevisionBoundPayloadEvidence, RevisionNumber, SchemaVersion, SequenceCapability, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    AuthorizationSubjectResolutionError, AuthorizationSubjectResolver, BackoffPolicy, BudgetScope,
    CoverageDomain, EndpointPolicy, FreshnessPolicy, HistoricalCapability, NetworkAccessPolicy,
    ProviderCaptureTerminalDisposition, SourceCapabilities, SourceClass, SourceCoverage,
    SourceMetadata, SourceMetadataInput, SourceProtocolProfile,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::transport::{EiaHttpResponseFixture, EiaMockTransport};
use crate::{
    EiaAcquisition, EiaApiKey, EiaApplicationBudget, EiaCanonicalContext, EiaCanonicalObservation,
    EiaCapacityGuidance, EiaDataFieldContract, EiaDataFieldContractInput, EiaDataPage,
    EiaDataQuery, EiaDataQueryInput, EiaDatasetContract, EiaDatasetContractInput, EiaError,
    EiaFacetFilter, EiaFacetValue, EiaFieldId, EiaMetadataRequest, EiaMissingPolicy,
    EiaNativeValue, EiaParseLimits, EiaRevisionDisposition, EiaRevisionHead, EiaRoute, EiaSort,
    EiaSortDirection, EiaSourceTransport, EiaTransportLimits, EiaUnitSource, EiaValueKind,
    eia_api_endpoint_rules, eia_application_provider_budget, parse_facet_metadata,
    parse_route_metadata, plan_revisions,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[test]
fn metadata_multi_page_data_and_revisions_preserve_exact_evidence() -> TestResult {
    let limits = EiaParseLimits::production_defaults();
    let route = EiaRoute::try_from("electricity/retail-sales")?;
    let metadata_request = EiaMetadataRequest::route(route.clone());
    let received_at = Timestamp::from_unix_nanos(1_800_000_000_000_000_000);
    let metadata_bytes = route_metadata_bytes()?;
    let metadata = parse_route_metadata(&metadata_bytes, &metadata_request, received_at, limits)?;
    assert_eq!(metadata.frequencies().len(), 1);
    assert_eq!(metadata.facets().len(), 1);
    assert_eq!(metadata.data_columns().len(), 2);
    assert_eq!(
        metadata.unmapped_response_fields(),
        &["futureMetadataField".to_owned()]
    );
    assert_eq!(metadata.receipt().redacted_secret_fields(), 1);
    assert!(!String::from_utf8(metadata.retained_payload().to_vec())?.contains("fixture-secret"));

    let facet_request = EiaMetadataRequest::facet(route.clone(), field("region")?);
    let facet_bytes = serde_json::to_vec(&json!({
        "response": {
            "totalFacets": "1",
            "facets": [{"id": "US", "name": "United States", "alias": "US"}]
        },
        "request": {
            "command": "/v2/electricity/retail-sales/facet/region/",
            "params": {"api_key": "fixture-secret"}
        },
        "apiVersion": "2.1.12"
    }))?;
    let facet = parse_facet_metadata(&facet_bytes, &facet_request, received_at, limits)?;
    assert!(facet.contains(&EiaFacetValue::try_from("US")?));

    let query = EiaDataQuery::try_new(EiaDataQueryInput {
        route,
        data_fields: vec![field("price")?, field("grade")?],
        facets: vec![EiaFacetFilter::try_new(
            field("region")?,
            vec![EiaFacetValue::try_from("US")?],
        )?],
        frequency: field("monthly")?,
        start: Some("2024-01".to_owned()),
        end: Some("2024-03".to_owned()),
        sorts: vec![EiaSort::new(field("period")?, EiaSortDirection::Ascending)],
        length: 2,
    })?;
    let api_key = EiaApiKey::try_new("fixture-secret")?;
    let authenticated = query.page(0).authenticate(&api_key)?;
    assert!(
        authenticated
            .authenticated_url()
            .ok_or(EiaError::RequestConstruction)?
            .as_str()
            .contains("api_key=fixture-secret")
    );
    assert!(!authenticated.secret_free_url().as_str().contains("api_key"));
    assert!(!format!("{authenticated:?}").contains("fixture-secret"));

    let contract = EiaDatasetContract::try_new(EiaDatasetContractInput {
        metadata,
        query: query.clone(),
        fields: vec![
            EiaDataFieldContract::new(EiaDataFieldContractInput {
                field: field("price")?,
                value_kind: EiaValueKind::Decimal,
                unit_source: EiaUnitSource::RowField,
                missing_policy: EiaMissingPolicy::try_new(["NA".to_owned()], false)?,
            }),
            EiaDataFieldContract::new(EiaDataFieldContractInput {
                field: field("grade")?,
                value_kind: EiaValueKind::String,
                unit_source: EiaUnitSource::RowField,
                missing_policy: EiaMissingPolicy::try_new(Vec::<String>::new(), false)?,
            }),
        ],
        descriptor_fields: vec![field("region-name")?],
        clock_fields: vec![
            crate::EiaClockField::new(field("released")?, crate::EiaClockKind::Released),
            crate::EiaClockField::new(field("updated")?, crate::EiaClockKind::Updated),
            crate::EiaClockField::new(field("available")?, crate::EiaClockKind::Available),
        ],
    })?;
    let page_one_bytes = data_page_bytes(
        3,
        vec![
            data_row("2024-01", "10.00", "A", "2024-02-01T15:00:00Z"),
            data_row("2024-02", "NA", "B", "2024-03-01T15:00:00Z"),
        ],
        0,
    )?;
    let page_two_bytes = data_page_bytes(
        3,
        vec![data_row("2024-03", "12.50", "A", "2024-04-01T15:00:00Z")],
        2,
    )?;
    let page_one = EiaDataPage::parse(
        &page_one_bytes,
        query.page(0),
        &contract,
        received_at,
        limits,
    )?;
    let page_two = EiaDataPage::parse(
        &page_two_bytes,
        query.page(2),
        &contract,
        received_at,
        limits,
    )?;
    let acquisition = EiaAcquisition::try_from_pages(vec![page_one.clone(), page_two])?;
    assert_eq!(acquisition.receipt().page_count(), 2);
    assert_eq!(acquisition.receipt().returned_rows(), 3);
    assert_eq!(acquisition.receipt().observation_count(), 6);
    assert_eq!(acquisition.receipt().missing_observation_count(), 1);
    assert!(matches!(
        acquisition.observations()[0].value(),
        EiaNativeValue::String(value) if value == "A"
    ));
    let first_decimal = acquisition
        .observations()
        .iter()
        .find(|observation| matches!(observation.value(), EiaNativeValue::Decimal { .. }))
        .ok_or(EiaError::InvalidValue)?;
    assert_eq!(first_decimal.series().unit(), "USD per unit");
    let canonical = EiaCanonicalObservation::try_from_native(
        first_decimal,
        EiaCanonicalContext::new(
            SourceId::try_from("us-eia-api-v2")?,
            received_at,
            RevisionNumber::new(1)?,
            None,
        ),
    )?;
    assert_eq!(
        canonical.observation().value().observed_value(),
        Some(rust_decimal::Decimal::from_str_exact("10.00")?)
    );

    let changed_bytes = data_page_bytes(
        1,
        vec![data_row("2024-01", "11.00", "A", "2024-02-01T15:00:00Z")],
        0,
    )?;
    let changed = EiaDataPage::parse(
        &changed_bytes,
        query.page(0),
        &contract,
        received_at,
        limits,
    )?;
    let previous: Vec<_> = page_one
        .observations()
        .iter()
        .filter(|observation| observation.period().raw() == "2024-01")
        .map(|observation| {
            Ok(EiaRevisionHead::new(
                observation.family().clone(),
                RevisionNumber::new(1)?,
                observation.semantic_digest(),
            ))
        })
        .collect::<Result<_, Box<dyn Error>>>()?;
    let revisions = plan_revisions(changed.observations(), &previous)?;
    assert_eq!(revisions.len(), 2);
    assert!(
        revisions
            .iter()
            .any(|entry| entry.disposition() == EiaRevisionDisposition::Revised)
    );
    assert!(
        revisions
            .iter()
            .any(|entry| entry.disposition() == EiaRevisionDisposition::Duplicate)
    );
    Ok(())
}

#[tokio::test]
async fn authority_bound_transport_redacts_and_terminally_closes_paged_capture() -> TestResult {
    let now = current_timestamp()?;
    let route = EiaRoute::try_from("electricity/retail-sales")?;
    let query = EiaDataQuery::try_new(EiaDataQueryInput {
        route: route.clone(),
        data_fields: vec![field("price")?, field("grade")?],
        facets: vec![EiaFacetFilter::try_new(
            field("region")?,
            vec![EiaFacetValue::try_from("US")?],
        )?],
        frequency: field("monthly")?,
        start: Some("2024-01".to_owned()),
        end: Some("2024-03".to_owned()),
        sorts: vec![EiaSort::new(field("period")?, EiaSortDirection::Ascending)],
        length: 2,
    })?;
    let first = data_page_bytes(
        3,
        vec![
            data_row("2024-01", "10.00", "A", "2024-02-01T15:00:00Z"),
            data_row("2024-02", "NA", "B", "2024-03-01T15:00:00Z"),
        ],
        0,
    )?;
    let second = data_page_bytes(
        3,
        vec![data_row("2024-03", "12.50", "A", "2024-04-01T15:00:00Z")],
        2,
    )?;
    let transport = Arc::new(EiaMockTransport {
        responses: Mutex::new(VecDeque::from([
            response_fixture(route_metadata_bytes()?, now),
            response_fixture(first.clone(), now),
            response_fixture(second.clone(), now),
        ])),
        safe_urls: Mutex::new(Vec::new()),
        expected_api_key: "fixture-secret".into(),
    });
    let (metadata, subject) = source_metadata(now, &query)?;
    let source = EiaSourceTransport::try_new_with_transport(
        metadata.clone(),
        EiaApiKey::try_new("fixture-secret")?,
        EiaTransportLimits::production_defaults(),
        transport.clone(),
    )?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(
        Arc::new(TestSubjectResolver { subject }),
    )?;
    let registered = registry.register(metadata, now)?;
    let authority = registry.extraction_authority(&registered, &source)?;
    let deadline = now.checked_add_nanos(10_000_000_000)?;
    let metadata_result = source
        .discover_route_metadata(
            &authority,
            &EiaMetadataRequest::route(route),
            deadline,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        metadata_result.capture_receipt().terminal(),
        ProviderCaptureTerminalDisposition::StandaloneResponse
    );
    assert!(
        !String::from_utf8_lossy(metadata_result.raw_page().payload()).contains("fixture-secret")
    );
    assert_eq!(metadata_result.capture_material().records().len(), 1);
    assert_eq!(
        metadata_result.capture_material().records()[0].payload(),
        metadata_result.raw_page().payload()
    );

    let contract = fixture_contract(metadata_result.metadata().clone(), query)?;
    let retrieval = source
        .retrieve_data(&authority, &contract, deadline, CancellationToken::new())
        .await?;
    let receipt = retrieval.transport_receipt();
    assert_eq!((receipt.requests(), receipt.requested_rows()), (2, 4));
    assert_eq!((receipt.returned_rows(), receipt.observations()), (3, 6));
    assert_eq!(receipt.missing_observations(), 1);
    assert_eq!(
        receipt.response_bytes(),
        (first.len() + second.len()) as u64
    );
    assert_eq!(receipt.latency(), Duration::from_millis(10));
    assert_eq!(
        retrieval.capture_receipt().terminal(),
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
    );
    assert_eq!(retrieval.capture_receipt().pages().len(), 2);
    assert_eq!(retrieval.capture_material().records().len(), 2);
    assert_eq!(retrieval.pages().len(), 2);
    assert!(retrieval.pages().iter().all(|page| {
        let payload = String::from_utf8_lossy(page.raw_page().payload());
        payload.contains("[REDACTED]") && !payload.contains("fixture-secret")
    }));
    for (ordinal, (page, record)) in retrieval
        .pages()
        .iter()
        .zip(retrieval.capture_material().records())
        .enumerate()
    {
        assert_eq!(record.source_sequence(), Some(ordinal as u64));
        assert_eq!(record.payload(), page.raw_page().payload());
    }
    let safe_urls = transport
        .safe_urls
        .lock()
        .map_err(|_| "mock request log poisoned")?;
    assert_eq!(safe_urls.len(), 3);
    assert!(safe_urls.iter().all(|url| !url.contains("fixture-secret")));
    Ok(())
}

#[test]
fn provider_facts_and_separate_application_limits_fail_closed() -> TestResult {
    let guidance = EiaCapacityGuidance::current();
    assert_eq!(guidance.max_json_page_rows(), 5_000);
    assert_eq!(guidance.sustained_requests_per_hour(), None);
    assert_eq!(guidance.burst_requests_per_second(), None);
    assert_eq!(
        EiaApplicationBudget::try_new(Duration::from_millis(999), 1, 5_000),
        Err(EiaError::InvalidLimit)
    );
    assert_eq!(
        EiaApplicationBudget::try_new(Duration::from_secs(1), 2, 5_000),
        Err(EiaError::InvalidLimit)
    );
    assert_eq!(
        EiaApplicationBudget::try_new(Duration::from_secs(1), 1, 5_001),
        Err(EiaError::InvalidLimit)
    );
    assert_eq!(
        EiaParseLimits::try_new(1_024, 5_001, 1, 1, 1, 1, 1),
        Err(EiaError::InvalidLimit)
    );
    assert!(matches!(
        EiaApiKey::try_new(""),
        Err(EiaError::InvalidApiKey)
    ));
    assert_eq!(
        EiaRoute::try_from("electricity/../secret"),
        Err(EiaError::InvalidRoute)
    );
    Ok(())
}

fn field(value: &str) -> Result<EiaFieldId, EiaError> {
    EiaFieldId::try_from(value)
}

fn route_metadata_bytes() -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&json!({
        "response": {
            "id": "retail-sales",
            "name": "Retail sales",
            "description": "fixture route",
            "frequency": [{
                "id": "monthly",
                "description": "one point per month",
                "query": "M",
                "format": "YYYY-MM"
            }],
            "facets": [{"id": "region", "description": "region"}],
            "data": {
                "price": {"alias": "Price", "units": "USD per unit"},
                "grade": {"alias": "Grade", "units": "code"}
            },
            "startPeriod": "2024-01",
            "endPeriod": "2024-03",
            "defaultDateFormat": "YYYY-MM",
            "defaultFrequency": "monthly",
            "futureMetadataField": {"retained": true}
        },
        "request": {
            "command": "/v2/electricity/retail-sales/",
            "params": {"api_key": "fixture-secret"}
        },
        "apiVersion": "2.1.12"
    }))
}

fn fixture_contract(
    metadata: crate::EiaRouteMetadata,
    query: EiaDataQuery,
) -> Result<EiaDatasetContract, EiaError> {
    EiaDatasetContract::try_new(EiaDatasetContractInput {
        metadata,
        query,
        fields: vec![
            EiaDataFieldContract::new(EiaDataFieldContractInput {
                field: field("price")?,
                value_kind: EiaValueKind::Decimal,
                unit_source: EiaUnitSource::RowField,
                missing_policy: EiaMissingPolicy::try_new(["NA".to_owned()], false)?,
            }),
            EiaDataFieldContract::new(EiaDataFieldContractInput {
                field: field("grade")?,
                value_kind: EiaValueKind::String,
                unit_source: EiaUnitSource::RowField,
                missing_policy: EiaMissingPolicy::try_new(Vec::<String>::new(), false)?,
            }),
        ],
        descriptor_fields: vec![field("region-name")?],
        clock_fields: vec![
            crate::EiaClockField::new(field("released")?, crate::EiaClockKind::Released),
            crate::EiaClockField::new(field("updated")?, crate::EiaClockKind::Updated),
            crate::EiaClockField::new(field("available")?, crate::EiaClockKind::Available),
        ],
    })
}

fn response_fixture(body: Vec<u8>, received_at: Timestamp) -> EiaHttpResponseFixture {
    EiaHttpResponseFixture {
        status: 200,
        retry_after: None,
        content_encoding: None,
        content_type: Some(Box::from(b"application/json".as_slice())),
        body: Bytes::from(body),
        received_at,
        latency: Duration::from_millis(5),
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

fn source_metadata(
    now: Timestamp,
    query: &EiaDataQuery,
) -> TestResult<(SourceMetadata, SourceIdentifier)> {
    let effective = EffectiveInterval::new(now.checked_sub_nanos(1_000_000_000)?, None)?;
    let digest: [u8; 32] = Sha256::digest(b"eia-test-metadata").into();
    let evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest,
    ));
    let provider = SourceIdentifier::try_from("us-eia")?;
    let subject = SourceIdentifier::try_from("eia-key-fixture")?;
    let basis = AuthorizationBasis::new(subject.clone());
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        basis.clone(),
        evidence.clone(),
        effective,
    );
    let endpoint = EndpointPolicy::try_from_api_rules(
        eia_api_endpoint_rules(query)?,
        market_squawk_sources::HttpRequestBounds::default(),
    )?;
    let budget = eia_application_provider_budget(
        BudgetScope::with_authorization_account(
            provider.clone(),
            basis.as_source_identifier().clone(),
        ),
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000_000).ok_or("nonzero backoff")?,
            NonZeroU64::new(3_600_000_000_000).ok_or("nonzero max backoff")?,
            0,
        )?,
    )?;
    let metadata = SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("us-eia-api-v2")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("eia-api-v2-test")?),
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
        NetworkAccessPolicy::Allowlisted(endpoint),
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
    ))?;
    Ok((metadata, subject))
}

fn current_timestamp() -> TestResult<Timestamp> {
    let duration = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    let nanos = u128::from(duration.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(duration.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or("test clock overflow")?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn data_row(period: &str, price: &str, grade: &str, available: &str) -> serde_json::Value {
    json!({
        "period": period,
        "region": "US",
        "region-name": "United States",
        "price": price,
        "price-units": "USD per unit",
        "grade": grade,
        "grade-units": "code",
        "released": "2024-01-01T15:00:00Z",
        "updated": available,
        "available": available
    })
}

fn data_page_bytes(
    total: u64,
    rows: Vec<serde_json::Value>,
    offset: u64,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&json!({
        "response": {
            "total": total.to_string(),
            "dateFormat": "YYYY-MM",
            "frequency": "monthly",
            "data": rows
        },
        "request": {
            "command": "/v2/electricity/retail-sales/data/",
            "params": {"api_key": "fixture-secret", "offset": offset.to_string()}
        },
        "apiVersion": "2.1.12"
    }))
}
