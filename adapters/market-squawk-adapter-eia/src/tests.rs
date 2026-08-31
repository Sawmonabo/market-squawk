//! Consolidated critical adapter-boundary proofs.

use std::collections::VecDeque;
use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
    ResearchObservation, RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_platform::LocalPaths;
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    AuthorizationSubjectResolutionError, AuthorizationSubjectResolver, BackoffPolicy, BudgetScope,
    CoverageDomain, DiscoveryRequest, EndpointPolicy, ExtractionContentIdentity, ExtractionRequest,
    FreshnessPolicy, HistoricalCapability, NetworkAccessPolicy, ProviderCaptureTerminalDisposition,
    SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput,
    SourceObject, SourceObjectCaptureIdentity, SourceProtocolProfile,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::transport::{EiaHttpResponseFixture, EiaMockTransport, await_capture_completion};
use crate::{
    EiaAcquisition, EiaApiKey, EiaApplicationBudget, EiaCapacityGuidance, EiaDataFieldContract,
    EiaDataFieldContractInput, EiaDataPage, EiaDataPageTransition, EiaDataQuery, EiaDataQueryInput,
    EiaDatasetContract, EiaDatasetContractInput, EiaDatasetProfile, EiaError, EiaFacetFilter,
    EiaFacetValue, EiaFieldId, EiaMetadataRequest, EiaMissingPolicy, EiaNativeValue,
    EiaParseLimits, EiaRoute, EiaSort, EiaSortDirection, EiaSourceTransport, EiaTransportLimits,
    EiaUnitSource, EiaValueKind, eia_api_endpoint_rules, eia_application_provider_budget,
    parse_facet_metadata, parse_route_metadata, run_eia_doctor,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("market-squawk-eia-{}", Uuid::new_v4())))
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

#[test]
fn metadata_multi_page_data_and_revisions_preserve_exact_evidence() -> TestResult {
    let limits = EiaParseLimits::production_defaults();
    let route = EiaRoute::try_from("electricity/retail-sales")?;
    let metadata_request = EiaMetadataRequest::route(route.clone());
    let received_at = Timestamp::from_unix_nanos(1_800_000_000_000_000_000);
    let metadata_bytes = route_metadata_bytes()?;
    let metadata = parse_route_metadata(&metadata_bytes, &metadata_request, received_at, limits)?;
    assert_eq!(metadata.frequencies().len(), 2);
    assert_eq!(metadata.facets().len(), 1);
    assert_eq!(metadata.data_columns().len(), 2);
    assert_eq!(
        metadata.unmapped_response_fields(),
        &["futureMetadataField".to_owned()]
    );
    assert_eq!(metadata.receipt().redacted_secret_fields(), 1);
    assert!(!String::from_utf8(metadata.retained_payload().to_vec())?.contains("fixture-secret"));
    let quarterly_metadata = metadata.clone();

    let facet_request = EiaMetadataRequest::facet(route.clone(), field("region")?);
    let facet_bytes = facet_metadata_bytes()?;
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
        sorts: vec![
            EiaSort::new(field("period")?, EiaSortDirection::Ascending),
            EiaSort::new(field("region")?, EiaSortDirection::Ascending),
            EiaSort::new(field("region-name")?, EiaSortDirection::Ascending),
        ],
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
                missing_policy: EiaMissingPolicy::try_new(["NA".to_owned()], true)?,
            }),
            EiaDataFieldContract::new(EiaDataFieldContractInput {
                field: field("grade")?,
                value_kind: EiaValueKind::String,
                unit_source: EiaUnitSource::RowField,
                missing_policy: EiaMissingPolicy::try_new(Vec::<String>::new(), false)?,
            }),
        ],
        facet_catalogs: vec![facet.clone()],
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
    let mut null_missing_page: serde_json::Value = serde_json::from_slice(&page_two_bytes)?;
    null_missing_page["response"]["data"][0]["price"] = serde_json::Value::Null;
    let null_missing_page = EiaDataPage::parse(
        &serde_json::to_vec(&null_missing_page)?,
        query.page(2),
        &contract,
        received_at,
        limits,
    )?;
    assert_ne!(
        page_one.receipt().envelope_schema_digest(),
        null_missing_page.receipt().envelope_schema_digest()
    );
    assert_eq!(
        EiaAcquisition::try_from_pages(vec![page_one.clone(), null_missing_page])?
            .receipt()
            .missing_observation_count(),
        2
    );
    assert_eq!(
        EiaDataPage::parse(
            &data_page_bytes(
                2,
                vec![
                    data_row("2024-02", "11.00", "A", "2024-03-01T15:00:00Z"),
                    data_row("2024-01", "10.00", "A", "2024-02-01T15:00:00Z"),
                ],
                0,
            )?,
            query.page(0),
            &contract,
            received_at,
            limits,
        ),
        Err(EiaError::NonTotalSort)
    );
    let cross_page_sort_drift = EiaDataPage::parse(
        &data_page_bytes(
            3,
            vec![data_row("2023-12", "9.00", "A", "2024-01-01T15:00:00Z")],
            2,
        )?,
        query.page(2),
        &contract,
        received_at,
        limits,
    )?;
    assert_eq!(
        EiaAcquisition::try_from_pages(vec![page_one.clone(), cross_page_sort_drift]),
        Err(EiaError::NonTotalSort)
    );
    let regressed_receipt_page = EiaDataPage::parse(
        &page_two_bytes,
        query.page(2),
        &contract,
        received_at.checked_sub_nanos(1)?,
        limits,
    )?;
    assert_eq!(
        EiaAcquisition::try_from_pages(vec![page_one.clone(), regressed_receipt_page]),
        Err(EiaError::Pagination)
    );
    assert_eq!(page_one.description(), Some("fixture route"));
    let mut mismatched_echo: serde_json::Value = serde_json::from_slice(&page_one_bytes)?;
    mismatched_echo["request"]["params"]["frequency"] = json!("quarterly");
    assert_eq!(
        EiaDataPage::parse(
            &serde_json::to_vec(&mismatched_echo)?,
            query.page(0),
            &contract,
            received_at,
            limits,
        ),
        Err(EiaError::RequestEchoMismatch)
    );
    let replayed_row = data_row("2024-01", "10.00", "A", "2024-02-01T15:00:00Z");
    assert_eq!(
        EiaDataPage::parse(
            &data_page_bytes(2, vec![replayed_row.clone(), replayed_row], 0)?,
            query.page(0),
            &contract,
            received_at,
            limits,
        ),
        Err(EiaError::ObservationReplay)
    );
    let page_two_digest = page_two.receipt().retained_payload_digest();
    let acquisition = EiaAcquisition::try_from_pages(vec![page_one.clone(), page_two])?;
    assert_eq!(acquisition.receipt().page_count(), 2);
    assert_eq!(acquisition.receipt().returned_rows(), 3);
    assert_eq!(acquisition.receipt().observation_count(), 6);
    assert_eq!(acquisition.receipt().missing_observation_count(), 1);
    assert_eq!(
        acquisition.receipt().page_digests(),
        &[
            page_one.receipt().retained_payload_digest(),
            page_two_digest,
        ]
    );
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
    assert!(matches!(
        first_decimal.value(),
        EiaNativeValue::Decimal { value, .. }
            if *value == rust_decimal::Decimal::from_str_exact("10.00")?
    ));

    let quarterly_query = EiaDataQuery::try_new(EiaDataQueryInput {
        route: contract.query().route().clone(),
        data_fields: vec![field("price")?],
        facets: vec![EiaFacetFilter::try_new(
            field("region")?,
            vec![EiaFacetValue::try_from("US")?],
        )?],
        frequency: field("quarterly")?,
        start: Some("2024-Q1".to_owned()),
        end: Some("2024-Q1".to_owned()),
        sorts: vec![
            EiaSort::new(field("period")?, EiaSortDirection::Ascending),
            EiaSort::new(field("region")?, EiaSortDirection::Ascending),
            EiaSort::new(field("region-name")?, EiaSortDirection::Ascending),
        ],
        length: 1,
    })?;
    let quarterly_contract = EiaDatasetContract::try_new(EiaDatasetContractInput {
        metadata: quarterly_metadata,
        query: quarterly_query.clone(),
        fields: vec![EiaDataFieldContract::new(EiaDataFieldContractInput {
            field: field("price")?,
            value_kind: EiaValueKind::Decimal,
            unit_source: EiaUnitSource::RowField,
            missing_policy: EiaMissingPolicy::try_new(Vec::<String>::new(), false)?,
        })],
        facet_catalogs: vec![facet],
        descriptor_fields: vec![field("region-name")?],
        clock_fields: Vec::new(),
    })?;
    let quarterly_bytes = serde_json::to_vec(&json!({
        "response": {
            "total": "1",
            "dateFormat": "YYYY-\"Q\"Q",
            "frequency": "quarterly",
            "description": "fixture route",
            "data": [{
                "period": "2024-Q1",
                "region": "US",
                "region-name": "United States",
                "price": "10.25",
                "price-units": "USD per unit"
            }]
        },
        "request": {
            "command": "/v2/electricity/retail-sales/data/",
            "params": {
                "api_key": "fixture-secret",
                "data": ["price"],
                "facets": {"region": ["US"]},
                "frequency": "quarterly",
                "start": "2024-Q1",
                "end": "2024-Q1",
                "sort": [
                    {"column": "period", "direction": "asc"},
                    {"column": "region", "direction": "asc"},
                    {"column": "region-name", "direction": "asc"}
                ],
                "offset": "0",
                "length": "1",
                "out": "json"
            }
        },
        "apiVersion": "2.1.12"
    }))?;
    let quarterly = EiaDataPage::parse(
        &quarterly_bytes,
        quarterly_query.page(0),
        &quarterly_contract,
        received_at,
        limits,
    )?;
    assert!(matches!(
        quarterly.observations()[0].period().kind(),
        crate::EiaPeriodKind::Quarter {
            year: 2024,
            quarter: 1
        }
    ));
    Ok(())
}

#[tokio::test]
async fn authority_bound_transport_redacts_and_terminally_closes_paged_capture() -> TestResult {
    let now = current_timestamp()?;
    let route = EiaRoute::try_from("electricity/retail-sales")?;
    let query = EiaDataQuery::try_new(EiaDataQueryInput {
        route: route.clone(),
        data_fields: vec![field("price")?],
        facets: vec![EiaFacetFilter::try_new(
            field("region")?,
            vec![EiaFacetValue::try_from("US")?],
        )?],
        frequency: field("monthly")?,
        start: Some("2024-01".to_owned()),
        end: Some("2024-03".to_owned()),
        sorts: vec![
            EiaSort::new(field("period")?, EiaSortDirection::Ascending),
            EiaSort::new(field("region")?, EiaSortDirection::Ascending),
            EiaSort::new(field("region-name")?, EiaSortDirection::Ascending),
        ],
        length: 2,
    })?;
    let first = price_data_page_bytes(
        3,
        vec![
            price_row("2024-01", "10.00", "2024-02-01T15:00:00Z"),
            price_row("2024-02", "NA", "2024-03-01T15:00:00Z"),
        ],
        0,
    )?;
    let second = price_data_page_bytes(
        3,
        vec![price_row("2024-03", "12.50", "2024-04-01T15:00:00Z")],
        2,
    )?;
    let completed_body = Bytes::from(first.clone());
    let completion_cancellation = CancellationToken::new();
    completion_cancellation.cancel();
    assert_eq!(
        await_capture_completion(
            Duration::from_secs(1),
            completion_cancellation,
            std::future::ready(Ok(completed_body.clone())),
        )
        .await?,
        completed_body
    );
    let transport = Arc::new(EiaMockTransport {
        responses: Mutex::new(VecDeque::from([
            response_fixture(route_metadata_bytes()?, now),
            response_fixture(facet_metadata_bytes()?, now),
            response_fixture(first.clone(), now),
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
    let profile = EiaDatasetProfile::try_for_macro(
        query,
        vec![EiaDataFieldContract::new(EiaDataFieldContractInput {
            field: field("price")?,
            value_kind: EiaValueKind::Decimal,
            unit_source: EiaUnitSource::RowField,
            missing_policy: EiaMissingPolicy::try_new(["NA".to_owned()], false)?,
        })],
        vec![field("region-name")?],
        vec![
            crate::EiaClockField::new(field("released")?, crate::EiaClockKind::Released),
            crate::EiaClockField::new(field("updated")?, crate::EiaClockKind::Updated),
            crate::EiaClockField::new(field("available")?, crate::EiaClockKind::Available),
        ],
    )?;
    let doctor = run_eia_doctor(
        source,
        &authority,
        profile,
        deadline,
        CancellationToken::new(),
    )
    .await?;
    assert_eq!(doctor.report().provider_total(), 3);
    assert!(
        doctor
            .report()
            .requirements()
            .shared_provider_rate_authority_required()
    );
    assert!(
        doctor
            .report()
            .requirements()
            .root_rights_decision_rejoin_required()
    );
    let (pending_activation, doctor_seal_requests) = doctor.into_sealing_parts()?;
    assert_eq!(doctor_seal_requests.len(), 3);
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path())?;
    let store = paths.sealed_research_journal_store()?;
    let mut sealed_doctor = Vec::new();
    sealed_doctor.try_reserve_exact(doctor_seal_requests.len())?;
    for request in doctor_seal_requests {
        let sealed = request.seal(&store)?;
        sealed_doctor.push(sealed);
    }
    let provider = crate::EiaActivatedProvider::try_activate(pending_activation, sealed_doctor)?;
    let cancellation = CancellationToken::new();
    let mut cursor = provider.begin_retrieval(&authority, deadline)?;
    let retrieval = loop {
        let pending = provider
            .fetch_next_retrieval_page(&authority, cursor, deadline, CancellationToken::new())
            .await?;
        pending.page_material().root_journal_rejoin().validate(
            provider.source_metadata(),
            provider.contract(),
            pending.page_material(),
        )?;
        let (page_rejoin, page_seal_request) = pending.into_parts();
        let sealed_page = page_seal_request.seal(&store)?;
        match provider.rejoin_retrieval_page(
            &authority,
            page_rejoin,
            sealed_page,
            deadline,
            &cancellation,
        )? {
            EiaDataPageTransition::More(next) => cursor = next,
            EiaDataPageTransition::Complete(retrieval) => break retrieval,
        }
    };
    let receipt = retrieval.transport_receipt();
    assert_eq!((receipt.requests(), receipt.requested_rows()), (2, 4));
    assert_eq!((receipt.returned_rows(), receipt.observations()), (3, 3));
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
    assert_eq!(retrieval.pages().len(), 2);
    assert_eq!(retrieval.sealed_page_count(), 2);
    assert!((0..retrieval.sealed_page_count()).all(|ordinal| {
        retrieval
            .sealed_page_receipt(ordinal)
            .is_some_and(|sealed| {
                sealed.receipt_digest().bytes() != [0; 32]
                    && sealed.segment().physical_receipt_digest().bytes() != [0; 32]
            })
    }));
    for page in retrieval.pages() {
        page.root_journal_rejoin().validate(
            provider.source_metadata(),
            provider.contract(),
            page,
        )?;
    }
    assert!(retrieval.pages().iter().all(|page| {
        let payload = String::from_utf8_lossy(page.raw_page().payload());
        payload.contains("[REDACTED]") && !payload.contains("fixture-secret")
    }));
    let duplicate_raw_floor = usize::try_from(receipt.retained_bytes())?
        .checked_mul(2)
        .ok_or(EiaError::InvalidLimit)?;
    assert!(
        retrieval
            .acquisition()
            .receipt()
            .publication_retained_bytes()
            >= duplicate_raw_floor
    );
    for (ordinal, page) in retrieval.pages().iter().enumerate() {
        let sealed = retrieval
            .sealed_page_receipt(ordinal)
            .ok_or(EiaError::CaptureBinding)?;
        let frame = sealed
            .segment()
            .frames()
            .first()
            .ok_or(EiaError::CaptureBinding)?;
        assert_eq!(frame.source_sequence(), Some(0));
        assert_eq!(
            frame.provider_payload_digest(),
            page.raw_page().capture_receipt().body_digest()
        );
    }
    let retrieval_rejoin = retrieval.into_publication_rejoin();
    let publication_candidate =
        provider.publication_candidate(&authority, retrieval_rejoin, deadline, &cancellation)?;
    assert_eq!(publication_candidate.observations().len(), 3);
    assert_eq!(publication_candidate.series().len(), 1);
    assert_eq!(publication_candidate.revision_plan().len(), 3);
    assert!(publication_candidate.revision_plan().is_locally_observed());
    assert!(
        publication_candidate.rejoin().publication_retained_bytes()
            >= publication_candidate
                .rejoin()
                .acquisition_receipt()
                .publication_retained_bytes()
    );
    let missing = publication_candidate
        .observations()
        .iter()
        .find(|observation| matches!(observation.native_value(), EiaNativeValue::Missing(_)))
        .ok_or(EiaError::InvalidValue)?;
    let native_clocks = missing.native_clocks();
    assert!(native_clocks.released_at().is_some());
    assert!(native_clocks.updated_at().is_some());
    assert!(native_clocks.available_at().is_some());
    assert_eq!(native_clocks.received_at(), now);
    assert!(matches!(
        missing.native_value(),
        EiaNativeValue::Missing(value) if value.lexical() == Some("NA")
    ));
    assert!(
        missing
            .observation()
            .value()
            .missing_value()
            .is_some_and(|value| value.marker().as_str().starts_with("eia-missing:"))
    );
    assert_ne!(
        publication_candidate
            .rejoin()
            .ordered_capture_receipt_digest()
            .bytes(),
        [0; 32]
    );
    assert_eq!(
        publication_candidate
            .rejoin()
            .sealed_doctor_captures()
            .len(),
        3
    );
    assert_eq!(publication_candidate.rejoin().root_page_rejoins().len(), 2);
    assert_eq!(
        publication_candidate.rejoin().sealed_page_capture_count(),
        2
    );
    assert!(
        (0..publication_candidate.rejoin().sealed_page_capture_count()).all(|ordinal| {
            publication_candidate
                .rejoin()
                .sealed_page_capture(ordinal)
                .is_some_and(|sealed| {
                    sealed.receipt_digest().bytes() != [0; 32]
                        && sealed.segment().physical_receipt_digest().bytes() != [0; 32]
                })
        })
    );
    publication_candidate
        .rejoin()
        .validate(provider.source_metadata())?;

    let expected_observations = publication_candidate
        .research_observations()
        .collect::<Vec<_>>();
    let source_object_id = publication_candidate.rejoin().source_object_id()?;
    let discovery_request = DiscoveryRequest::try_new(
        publication_candidate.rejoin().provider_dataset().clone(),
        None,
        NonZeroU16::new(1).ok_or("nonzero discovery bound")?,
        deadline,
    )?;
    let source_object = SourceObject::try_new_with_availability(
        publication_candidate
            .rejoin()
            .source_metadata()
            .source_id()
            .clone(),
        publication_candidate
            .rejoin()
            .source_metadata()
            .revision()
            .clone(),
        &discovery_request,
        source_object_id,
        SourceIdentifier::try_from("application/json")?,
        ExactPayloadEvidence::from_content_digest(
            publication_candidate.rejoin().capture_content_digest(),
        ),
        EffectiveInterval::new(
            publication_candidate
                .rejoin()
                .acquisition_receipt()
                .first_received_at(),
            None,
        )?,
        None,
        market_squawk_sources::AvailabilityEvidence::LocalFirstObserved {
            observed_at: publication_candidate
                .rejoin()
                .acquisition_receipt()
                .last_received_at(),
        },
        Some(
            publication_candidate
                .rejoin()
                .capture_receipt()
                .total_body_bytes(),
        ),
    )?;
    assert_eq!(
        source_object.capture_identity(),
        SourceObjectCaptureIdentity::Standalone
    );
    let extraction_request = ExtractionRequest::try_new(
        source_object,
        NonZeroU32::new(4).ok_or("nonzero record bound")?,
        NonZeroU64::new(64 * 1024 * 1024).ok_or("nonzero byte bound")?,
        publication_candidate.rejoin().normalization_admitted_at(),
    )?;
    let shared = publication_candidate.try_into_shared_publication(extraction_request)?;
    assert_eq!(shared.batch().records().len(), 3);
    assert_eq!(shared.revision_plan().len(), shared.batch().records().len());
    assert!(shared.revision_plan().is_locally_observed());
    assert_eq!(
        shared.batch().request().object().capture_identity(),
        SourceObjectCaptureIdentity::try_from_capture(
            shared.sealed_capture_binding().capture_evidence()
        )?
    );
    assert_eq!(
        shared.batch().request().object().expected_bytes(),
        Some(
            shared
                .sealed_capture_binding()
                .capture_evidence()
                .total_body_bytes()
        )
    );
    let extraction_content_identity = ExtractionContentIdentity::try_from_batch(shared.batch())?;
    assert_eq!(
        shared.extraction_content_identity(),
        extraction_content_identity
    );
    assert_eq!(extraction_content_identity.record_count(), 3);
    assert_eq!(
        shared
            .sealed_capture_binding()
            .row_frames()
            .iter()
            .map(market_squawk_sources::ProviderCaptureRowFrame::capture_page_ordinal)
            .collect::<Vec<_>>(),
        [0, 0, 1]
    );
    assert!(shared.native_lineage().rows().iter().all(|row| {
        serde_json::from_slice::<serde_json::Value>(row.semantic_payload()).is_ok_and(|value| {
            value.get("page_ordinal").is_none()
                && value.get("native_semantic_digest").is_none()
                && value.get("native_schema_digest").is_none()
        })
    }));
    assert_eq!(
        shared.policy_evidence().doctor_report().report_digest(),
        provider.doctor_report().report_digest()
    );
    shared
        .policy_evidence()
        .validate(provider.source_metadata())?;
    let (policy_evidence, revision_plan, sealed_capture_binding) = shared.into_parts();
    let batch = sealed_capture_binding.batch();
    assert_eq!(batch.request().max_records(), 4);
    assert_eq!(
        sealed_capture_binding.content_identity(),
        extraction_content_identity
    );
    assert_eq!(sealed_capture_binding.record_count(), batch.records().len());
    assert_eq!(
        sealed_capture_binding.layout(),
        market_squawk_sources::ProviderCaptureBindingLayout::OrderedSegments
    );
    assert_eq!(revision_plan.len(), batch.records().len());
    assert!(revision_plan.is_locally_observed());
    let decoded = batch
        .records()
        .iter()
        .map(|record| serde_json::from_slice(record.payload()))
        .collect::<Result<Vec<ResearchObservation>, _>>()?;
    assert_eq!(decoded, expected_observations);
    let revision_batch = revision_plan.into_observed_batch_with_native_lineage(
        policy_evidence.source_metadata().source_id().clone(),
        batch,
        &decoded,
        sealed_capture_binding.native_lineage(),
    )?;
    assert_eq!(revision_batch.input_len(), 3);

    let safe_urls = transport
        .safe_urls
        .lock()
        .map_err(|_| "mock request log poisoned")?;
    assert_eq!(safe_urls.len(), 5);
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
    assert!(matches!(
        crate::transport::validate_provider_page_total(3, 2, 1),
        Err(crate::EiaSourceTransportError::PageLimitExceeded { max: 1 })
    ));
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
            "frequency": [
                {
                    "id": "monthly",
                    "description": "one point per month",
                    "query": "M",
                    "format": "YYYY-MM"
                },
                {
                    "id": "quarterly",
                    "description": "one point per quarter",
                    "query": "Q",
                    "format": "YYYY-\"Q\"Q"
                }
            ],
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

fn facet_metadata_bytes() -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&json!({
        "response": {
            "totalFacets": "1",
            "facets": [{"id": "US", "name": "United States", "alias": "US"}]
        },
        "request": {
            "command": "/v2/electricity/retail-sales/facet/region/",
            "params": {"api_key": "fixture-secret"}
        },
        "apiVersion": "2.1.12"
    }))
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
            "description": "fixture route",
            "data": rows
        },
        "request": {
            "command": "/v2/electricity/retail-sales/data/",
            "params": {
                "api_key": "fixture-secret",
                "data": ["grade", "price"],
                "facets": {"region": ["US"]},
                "frequency": "monthly",
                "start": "2024-01",
                "end": "2024-03",
                "sort": [
                    {"column": "period", "direction": "asc"},
                    {"column": "region", "direction": "asc"},
                    {"column": "region-name", "direction": "asc"}
                ],
                "offset": offset.to_string(),
                "length": "2",
                "out": "json"
            }
        },
        "apiVersion": "2.1.12"
    }))
}

fn price_row(period: &str, price: &str, available: &str) -> serde_json::Value {
    json!({
        "period": period,
        "region": "US",
        "region-name": "United States",
        "price": price,
        "price-units": "USD per unit",
        "released": "2024-01-01T15:00:00Z",
        "updated": available,
        "available": available
    })
}

fn price_data_page_bytes(
    total: u64,
    rows: Vec<serde_json::Value>,
    offset: u64,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&json!({
        "response": {
            "total": total.to_string(),
            "dateFormat": "YYYY-MM",
            "frequency": "monthly",
            "description": "fixture route",
            "data": rows
        },
        "request": {
            "command": "/v2/electricity/retail-sales/data/",
            "params": {
                "api_key": "fixture-secret",
                "data": ["price"],
                "facets": {"region": ["US"]},
                "frequency": "monthly",
                "start": "2024-01",
                "end": "2024-03",
                "sort": [
                    {"column": "period", "direction": "asc"},
                    {"column": "region", "direction": "asc"},
                    {"column": "region-name", "direction": "asc"}
                ],
                "offset": offset.to_string(),
                "length": "2",
                "out": "json"
            }
        },
        "apiVersion": "2.1.12"
    }))
}
