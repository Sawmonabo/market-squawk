use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream;
use market_squawk_domain::{
    AuthorizationBasis, CalendarDate, ChecksumCapability, CoverageDelay, DataQuality,
    DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    MetadataRevision, ResearchObservation, RevisionBoundPayloadEvidence, SchemaVersion,
    SequenceCapability, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    AuthorizationSubjectResolutionError, AuthorizationSubjectResolver, BackoffPolicy, BudgetScope,
    CURRENT_RESEARCH_RECORD_SCHEMA, CoverageDomain, EndpointPolicy, ExtractionAuthority,
    ExtractionRequest, ExtractionSource, FreshnessPolicy, HistoricalCapability,
    NetworkAccessPolicy, ProviderBudgetPolicy, ProviderNativeLineageImplementation,
    SourceCapabilities, SourceClass, SourceCoverage, SourceError, SourceMetadata,
    SourceMetadataInput, SourceMetadataProvider, SourceObject, SourceProtocolProfile,
    payload_matches_exact_evidence,
};
use sha2::Digest;
use tokio_util::sync::CancellationToken;

use crate::{
    FredOperation, FredOwnerAuthorizationEvidence, FredRightsArtifact, FredRightsPolicy,
    FredSeriesRightsGrant, FredServicePermissionChannel, FredServicePermissionEvidence,
    FredServicePermissionReview, FredTermsDocumentBytes, FredTermsDocumentRole, Sha256Digest,
};

use super::http::collect_bounded_stream;
use super::{
    FredApiKey, FredDataset, FredHttpRequest, FredHttpResponse, FredSource, FredSourceError,
    FredTransport, fred_observations_endpoint_rule, fred_release_observations_v2_endpoint_rule,
    fred_series_endpoint_rule, fred_vintage_dates_endpoint_rule, system_timestamp,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const EXACT_SERIES_RESPONSE: &[u8] = br#"{
    "realtime_start":"2024-01-01",
    "realtime_end":"2024-01-31",
    "seriess":[{
        "id":"CPIAUCSL",
        "realtime_start":"2024-01-01",
        "realtime_end":"2024-01-31",
        "title":"Consumer Price Index for All Urban Consumers: All Items in U.S. City Average",
        "observation_start":"1947-01-01",
        "observation_end":"2023-12-01",
        "frequency":"Monthly",
        "frequency_short":"M",
        "units":"Index 1982-1984=100",
        "units_short":"Index 1982-1984=100",
        "seasonal_adjustment":"Seasonally Adjusted",
        "seasonal_adjustment_short":"SA",
        "last_updated":"2024-01-11 07:42:02-06",
        "popularity":95,
        "notes":"Exact provider notes"
    }]
}"#;

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
    let provider_dataset =
        SourceIdentifier::try_from("alfred:series-observations:CPIAUCSL:2024-01-01:2024-02-01")?;
    let dataset = FredDataset::parse(&provider_dataset)?;
    assert_eq!(dataset.series_id(), "CPIAUCSL");
    assert_eq!(dataset.realtime_start().to_string(), "2024-01-01");
    assert_eq!(dataset.realtime_end().to_string(), "2024-02-01");
    assert_eq!(
        FredSource::analytical_dataset_identifier(&provider_dataset)?.as_str(),
        "alfred.series-observations.CPIAUCSL.2024-01-01.2024-02-01"
    );
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
fn source_implements_the_sync_extraction_contract() {
    fn assert_contract<T: ExtractionSource + Sync>() {}
    assert_contract::<FredSource>();
}

#[tokio::test]
async fn discovery_and_ephemeral_extraction_are_exact_source_request_and_payload_bound()
-> TestResult {
    let now = system_timestamp()?;
    let incomplete_response = FredHttpResponse {
        status: 200,
        retry_after: None,
        content_encoding: None,
        body: Bytes::from_static(include_bytes!("../../fixtures/observations.json")),
        received_at: now,
    };
    let deadline = now.checked_add_nanos(10_000_000_000)?;
    let incomplete = source(
        now,
        incomplete_response,
        "fred-incomplete-discovery-test-user",
    )?;
    let incomplete_request = market_squawk_sources::DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alfred:series-observations:CPIAUCSL:2024-01-01:2024-01-31")?,
        None,
        NonZeroU16::new(1).ok_or("nonzero result limit")?,
        deadline,
    )?;
    assert!(matches!(
        incomplete
            .source
            .discover(
                incomplete.authority,
                incomplete_request,
                CancellationToken::new(),
            )
            .await,
        Err(market_squawk_sources::ExtractionSourceError::Contract(
            market_squawk_sources::ExtractionError::DiscoveryLimitExceeded { requested: 1 }
        ))
    ));

    let mut complete_body: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../fixtures/observations.json"))?;
    complete_body["count"] = serde_json::json!(2);
    let complete_body = Bytes::from(serde_json::to_vec(&complete_body)?);
    let source = source(
        now,
        FredHttpResponse {
            status: 200,
            retry_after: None,
            content_encoding: None,
            body: complete_body.clone(),
            received_at: now,
        },
        "fred-discovery-test-user",
    )?;
    let instant_request = market_squawk_sources::DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alfred:series-observations:CPIAUCSL:2024-01-01:2024-01-31")?,
        Some(now),
        NonZeroU16::new(1).ok_or("nonzero result limit")?,
        deadline,
    )?;
    assert!(matches!(
        source
            .source
            .discover(
                source.authority.clone(),
                instant_request,
                CancellationToken::new(),
            )
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
    let discovered = source
        .source
        .discover(
            source.authority.clone(),
            discovery,
            CancellationToken::new(),
        )
        .await
        .map_err(|error| format!("discovery contract failed: {error:?}"))?;
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
            .source
            .extract_page_ephemeral(
                &source.authority,
                &transplanted_request,
                CancellationToken::new(),
            )
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
        .source
        .extract_page_ephemeral(&source.authority, &extraction, CancellationToken::new())
        .await
        .map_err(|error| format!("ephemeral extraction failed: {error:?}"))?;
    assert_eq!(page.canonical_payloads().len(), 2);
    assert_eq!(page.received_at(), now);
    assert_eq!(page.captures().len(), 2);
    assert_eq!(
        page.captures()[0].records()[0].payload(),
        EXACT_SERIES_RESPONSE
    );
    assert_eq!(
        page.captures()[1].records()[0].payload(),
        complete_body.as_ref()
    );
    assert!(matches!(
        source
            .source
            .extract_with_capture(
                source.authority.clone(),
                extraction,
                CancellationToken::new(),
            )
            .await,
        Err(market_squawk_sources::ExtractionSourceError::Source(
            SourceError::Unauthorized
        ))
    ));
    Ok(())
}

#[tokio::test]
async fn durable_extraction_emits_canonical_schema_v3_macro_observations() -> TestResult {
    let now = system_timestamp()?;
    let mut complete_body: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../fixtures/observations.json"))?;
    complete_body["count"] = serde_json::json!(2);
    let observation_body = Bytes::from(serde_json::to_vec(&complete_body)?);
    let observations_response = FredHttpResponse {
        status: 200,
        retry_after: None,
        content_encoding: None,
        body: observation_body.clone(),
        received_at: now,
    };
    let source = source_with_options(
        now,
        observations_response,
        FredHttpResponse {
            status: 200,
            retry_after: None,
            content_encoding: None,
            body: Bytes::from_static(EXACT_SERIES_RESPONSE),
            received_at: now,
        },
        "fred-durable-extraction-test-user",
        true,
    )?;
    let deadline = now.checked_add_nanos(10_000_000_000)?;
    let discovery = market_squawk_sources::DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alfred:series-observations:CPIAUCSL:2024-01-01:2024-01-31")?,
        None,
        NonZeroU16::new(1).ok_or("nonzero result limit")?,
        deadline,
    )?;
    let discovered = source
        .source
        .discover(
            source.authority.clone(),
            discovery,
            CancellationToken::new(),
        )
        .await?;
    let extraction = ExtractionRequest::try_new(
        discovered.objects()[0].clone(),
        NonZeroU32::new(2).ok_or("nonzero record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero byte limit")?,
        deadline,
    )?;
    assert!(matches!(
        source
            .source
            .extract(
                source.authority.clone(),
                extraction.clone(),
                CancellationToken::new(),
            )
            .await,
        Err(market_squawk_sources::ExtractionSourceError::Source(
            SourceError::InvalidProtocolState
        ))
    ));
    let output = source
        .source
        .extract_with_capture(
            source.authority.clone(),
            extraction,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(output.captures().len(), 2);
    assert_eq!(
        output.captures()[0].records()[0].payload(),
        EXACT_SERIES_RESPONSE
    );
    assert_eq!(
        output.captures()[1].records()[0].payload(),
        observation_body.as_ref()
    );
    let (batch, capture, native_lineage, row_capture_page_ordinals) =
        output.try_into_common_publication()?;
    assert_eq!(capture.receipt().pages().len(), 2);
    assert_eq!(row_capture_page_ordinals, [1, 1]);
    assert_eq!(
        native_lineage.schema().implementation(),
        ProviderNativeLineageImplementation::FredAlfredSeriesObservationsV1
    );
    native_lineage.validate(&batch)?;
    let native_batch: serde_json::Value = serde_json::from_slice(
        native_lineage
            .batch_sidecar()
            .ok_or("missing FRED native batch sidecar")?
            .semantic_payload(),
    )?;
    assert_eq!(native_batch["family"], "fred_alfred_series_observations");
    assert_eq!(native_batch["namespace"], "alfred");
    assert_eq!(native_batch["series"]["id"], "CPIAUCSL");
    assert_eq!(native_batch["series"]["observation_start"]["year"], 1947);
    assert_eq!(native_batch["series"]["observation_end"]["year"], 2023);
    assert_eq!(native_batch["series"]["units"], "Index 1982-1984=100");
    assert_eq!(native_batch["page"]["units"], "lin");
    assert_eq!(native_batch["page"]["offset"], 0);
    assert_eq!(native_batch["page"]["returned"], 2);
    assert_eq!(native_batch["page"]["terminal"], true);
    let first_native: serde_json::Value =
        serde_json::from_slice(native_lineage.rows()[0].semantic_payload())?;
    let second_native: serde_json::Value =
        serde_json::from_slice(native_lineage.rows()[1].semantic_payload())?;
    assert_eq!(first_native["raw_value"], "101.25");
    assert!(!first_native["value"].is_null());
    assert!(first_native["missing_marker"].is_null());
    assert_eq!(second_native["raw_value"], ".");
    assert!(second_native["value"].is_null());
    assert_eq!(second_native["missing_marker"], ".");
    let revisions = source.source.revision_plan(&batch)?;

    assert_eq!(batch.records().len(), 2);
    assert_eq!(revisions.len(), batch.records().len());
    assert!(!revisions.is_locally_observed());
    assert!(revisions.native_lineage_required());
    for record in batch.records() {
        assert_eq!(record.schema().as_str(), CURRENT_RESEARCH_RECORD_SCHEMA);
        assert!(payload_matches_exact_evidence(
            record.payload(),
            record.evidence()
        ));
    }
    assert_eq!(
        batch.records()[0]
            .published_time()
            .and_then(|coordinate| coordinate.calendar_date_value()),
        Some(CalendarDate::new(2024, 1, 1)?)
    );
    assert_eq!(
        batch.records()[0]
            .superseded_time()
            .and_then(|coordinate| coordinate.calendar_date_value()),
        Some(CalendarDate::new(2024, 2, 1)?)
    );
    let first: ResearchObservation = serde_json::from_slice(batch.records()[0].payload())?;
    let second: ResearchObservation = serde_json::from_slice(batch.records()[1].payload())?;
    let ResearchObservation::Macro(first) = first else {
        return Err("expected canonical macro observation".into());
    };
    let ResearchObservation::Macro(second) = second else {
        return Err("expected canonical macro observation".into());
    };
    assert_eq!(first.series().as_str(), "CPIAUCSL");
    assert_eq!(
        first.unit().as_str(),
        "fred-unit:v1:Index%201982-1984%3D100"
    );
    assert_eq!(
        first
            .value()
            .observed_value()
            .map(|value| value.to_string())
            .as_deref(),
        Some("101.25")
    );
    assert_eq!(
        first
            .context()
            .time()
            .effective()
            .calendar_date_value()
            .map(|date| date.to_string())
            .as_deref(),
        Some("2023-01-01")
    );
    assert_eq!(
        first
            .context()
            .time()
            .published()
            .and_then(|coordinate| coordinate.calendar_date_value())
            .map(|date| date.to_string())
            .as_deref(),
        Some("2024-01-01")
    );
    assert_eq!(
        first
            .context()
            .time()
            .superseded()
            .and_then(|coordinate| coordinate.calendar_date_value())
            .map(|date| date.to_string())
            .as_deref(),
        Some("2024-02-01")
    );
    assert!(first.context().provenance().source_timestamp().is_none());
    assert_eq!(
        first.context().provenance().quality(),
        DataQuality::OfficialDelayed
    );
    assert_eq!(
        second
            .value()
            .missing_value()
            .map(|missing| missing.marker().as_str()),
        Some(".")
    );
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
            .source
            .discover(source.authority.clone(), request, CancellationToken::new())
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
        source
            .source
            .discover(source.authority.clone(), request, CancellationToken::new(),)
            .await,
        Err(market_squawk_sources::ExtractionSourceError::Source(
            SourceError::InvalidProtocolState
        ))
    ));
    Ok(())
}

#[tokio::test]
async fn series_metadata_is_exact_request_bound_and_rejects_non_unique_identity() -> TestResult {
    let now = system_timestamp()?;
    let fred_source = source(
        now,
        FredHttpResponse {
            status: 200,
            retry_after: None,
            content_encoding: None,
            body: Bytes::from_static(include_bytes!("../../fixtures/observations.json")),
            received_at: now,
        },
        "fred-series-metadata-test-user",
    )?;
    let dataset =
        SourceIdentifier::try_from("alfred:series-observations:CPIAUCSL:2024-01-01:2024-01-31")?;
    let document = fred_source
        .source
        .acquire_series_metadata(
            &fred_source.authority,
            &dataset,
            now.checked_add_nanos(10_000_000_000)?,
            CancellationToken::new(),
            FredOperation::RetrieveEphemeral,
        )
        .await?;

    assert_eq!(
        document.source_id(),
        fred_source.source.metadata().source_id()
    );
    assert_eq!(
        document.metadata_revision(),
        fred_source.source.metadata().revision()
    );
    assert_eq!(document.dataset(), &dataset);
    assert_eq!(document.response_bytes().as_ref(), EXACT_SERIES_RESPONSE);
    assert_eq!(
        document.response_length(),
        EXACT_SERIES_RESPONSE.len() as u64
    );
    assert_eq!(document.received_at(), now);
    assert!(payload_matches_exact_evidence(
        document.response_bytes(),
        document.evidence()
    ));
    assert_eq!(document.capture_material().records().len(), 1);
    assert_eq!(
        document.capture_material().records()[0].payload(),
        EXACT_SERIES_RESPONSE
    );
    let locator = document
        .evidence()
        .version_pinned_locator()
        .ok_or("series metadata must retain its secret-free exact request locator")?;
    assert_eq!(
        locator.reference().as_str(),
        "https://api.stlouisfed.org/fred/series?series_id=CPIAUCSL&realtime_start=2024-01-01&realtime_end=2024-01-31&file_type=json"
    );
    assert!(!locator.reference().as_str().contains("api_key"));

    let series = document.series();
    assert_eq!(series.series_id().as_str(), "CPIAUCSL");
    assert_eq!(series.realtime_start().to_string(), "2024-01-01");
    assert_eq!(series.realtime_end().to_string(), "2024-01-31");
    assert_eq!(
        series.title(),
        "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average"
    );
    assert_eq!(series.observation_start().to_string(), "1947-01-01");
    assert_eq!(series.observation_end().to_string(), "2023-12-01");
    assert_eq!(series.frequency(), "Monthly");
    assert_eq!(series.frequency_short(), "M");
    assert_eq!(series.units(), "Index 1982-1984=100");
    assert_eq!(series.units_short(), "Index 1982-1984=100");
    assert_eq!(series.seasonal_adjustment(), "Seasonally Adjusted");
    assert_eq!(series.seasonal_adjustment_short(), "SA");
    assert_eq!(series.last_updated(), "2024-01-11 07:42:02-06");
    assert_eq!(series.popularity(), 95);
    assert_eq!(series.notes(), Some("Exact provider notes"));

    let exact_value: serde_json::Value = serde_json::from_slice(EXACT_SERIES_RESPONSE)?;
    let mut absent_value = exact_value.clone();
    absent_value["seriess"] = serde_json::json!([]);
    let mut mismatch_value = exact_value.clone();
    mismatch_value["seriess"][0]["id"] = serde_json::json!("GDPDEF");
    let mut malformed_value = exact_value.clone();
    malformed_value["seriess"][0]["last_updated"] = serde_json::json!("not-a-provider-timestamp");
    let mut extra_value = exact_value;
    let duplicate = extra_value["seriess"][0].clone();
    extra_value["seriess"]
        .as_array_mut()
        .ok_or("test series list")?
        .push(duplicate);
    for (subject, body) in [
        (
            "fred-series-metadata-absent-user",
            serde_json::to_vec(&absent_value)?,
        ),
        (
            "fred-series-metadata-mismatch-user",
            serde_json::to_vec(&mismatch_value)?,
        ),
        (
            "fred-series-metadata-malformed-user",
            serde_json::to_vec(&malformed_value)?,
        ),
        (
            "fred-series-metadata-extra-user",
            serde_json::to_vec(&extra_value)?,
        ),
    ] {
        let rejecting_source = source_with_options(
            now,
            FredHttpResponse {
                status: 200,
                retry_after: None,
                content_encoding: None,
                body: Bytes::from_static(include_bytes!("../../fixtures/observations.json")),
                received_at: now,
            },
            FredHttpResponse {
                status: 200,
                retry_after: None,
                content_encoding: None,
                body: Bytes::from(body),
                received_at: now,
            },
            subject,
            false,
        )?;
        assert!(matches!(
            rejecting_source
                .source
                .acquire_series_metadata(
                    &rejecting_source.authority,
                    &dataset,
                    now.checked_add_nanos(10_000_000_000)?,
                    CancellationToken::new(),
                    FredOperation::RetrieveEphemeral,
                )
                .await,
            Err(market_squawk_sources::ExtractionSourceError::Source(
                SourceError::InvalidProtocolState
            ))
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct ScriptedTransport {
    observations_response: FredHttpResponse,
    series_response: FredHttpResponse,
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
            if request.public_url.path() == "/fred/series" {
                Ok(self.series_response.clone())
            } else {
                Ok(self.observations_response.clone())
            }
        })
    }
}

struct TestFredSource {
    source: FredSource,
    authority: ExtractionAuthority,
    _registry: AuthoritativeSourceRegistry,
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
    observations_response: FredHttpResponse,
    authorization_subject: &'static str,
) -> TestResult<TestFredSource> {
    source_with_options(
        now,
        observations_response,
        FredHttpResponse {
            status: 200,
            retry_after: None,
            content_encoding: None,
            body: Bytes::from_static(EXACT_SERIES_RESPONSE),
            received_at: now,
        },
        authorization_subject,
        false,
    )
}

fn source_with_options(
    now: Timestamp,
    observations_response: FredHttpResponse,
    series_response: FredHttpResponse,
    authorization_subject: &'static str,
    permit_persistence: bool,
) -> TestResult<TestFredSource> {
    let subject = SourceIdentifier::try_from(authorization_subject)?;
    let metadata = metadata(now, subject.clone())?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(
        Arc::new(TestSubjectResolver { subject }),
    )?;
    let registered = registry.register(metadata.clone(), now)?;
    let source = FredSource::try_new_with_transport(
        metadata.clone(),
        FredApiKey::try_new("abcdefghijklmnopqrstuvwxyz123456".to_owned())?,
        rights(now, permit_persistence)?,
        Arc::new(ScriptedTransport {
            observations_response,
            series_response,
        }),
        2,
    )?;
    let authority = registry.extraction_authority(&registered, &source)?;
    Ok(TestFredSource {
        source,
        authority,
        _registry: registry,
    })
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
    let network = EndpointPolicy::try_from_api_rules(
        vec![
            fred_observations_endpoint_rule()?,
            fred_series_endpoint_rule()?,
            fred_vintage_dates_endpoint_rule()?,
            fred_release_observations_v2_endpoint_rule()?,
        ],
        market_squawk_sources::HttpRequestBounds::default(),
    )?;
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::with_authorization_account(
            provider.clone(),
            basis.as_source_identifier().clone(),
        ),
        NonZeroU32::new(1).ok_or("nonzero request budget")?,
        NonZeroU64::new(1_000_000_000).ok_or("nonzero request window")?,
        NonZeroU16::new(1).ok_or("nonzero concurrency")?,
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

fn rights(now: Timestamp, permit_persistence: bool) -> TestResult<FredRightsPolicy> {
    let assessed = now.checked_sub_nanos(1_000_000_000)?;
    let review = now.checked_add_nanos(60_000_000_000)?;
    let privacy_policy = concat!(
        r#"<div class="component search-box col-12" id="a" data-properties='"#,
        r#"{"endpoint":"//sxa/search/results/","#,
        r#""suggestionEndpoint":"//sxa/search/suggestions/","suggestionsMode":"","#,
        r#""resultPage":"/search","targetSignature":"siteResults","#,
        r#""v":"{E22FB38C-3672-49E1-B145-563EEAEC4951}","#,
        r#""s":"{A10D94E2-3F41-4100-A3BA-24E58460A483}","#,
        r#""p":0,"l":"","languageSource":"AllLanguages","#,
        r#""searchResultsSignature":"","itemid":"{679A23BE-3C34-4F6E-A98E-9A9246CFF1B5}"#,
        r#"","minSuggestionsTriggerCharacterCount":2}'>"#
    )
    .as_bytes();
    let artifact = format!(
        r#"{{
            "schema_version":5,
            "series_scope":"exact_service_and_series_grants",
            "terms_bundle_digest":"b3c33fd45878caee3c51ea1fafed95a1bed829432b49cd2a5c420e76df7aae3f",
            "terms_documents":[
                {{
                    "role":"api_terms",
                    "representation":"exact_raw",
                    "url":"https://fred.stlouisfed.org/docs/api/terms_of_use.html",
                    "sha256":"27d66951a524848e3777300299a69ef16f868ab2dbc9ca04a00ddea0b4db13bd",
                    "byte_length":20
                }},
                {{
                    "role":"fred_services_legal_terms",
                    "representation":"exact_raw",
                    "url":"https://fred.stlouisfed.org/legal/",
                    "sha256":"97da0ed4fc87909604e691990b7344467c66e7b1bc9424a2bfbcf41dcf25b9e5",
                    "byte_length":25
                }},
                {{
                    "role":"privacy_policy",
                    "representation":"privacy_sxa_search_item_canonical_v1",
                    "url":"https://www.stlouisfed.org/about-us/privacy-policy/online-notice",
                    "sha256":"6cd3afb454b4a8b7e6cfd026d43cede4ea7936cf081cd5a83bf803f846af7743",
                    "byte_length":473
                }}
            ],
            "assessed_at_unix_nanos":{},
            "review_required_by_unix_nanos":{},
            "operations":["persist"],
            "disposition":"service_permission_required",
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
                    "sha256":"8209e409687a552bea39fc7d01e7fe10e59b25ca03d0e8ffad59bafd12744749",
                    "byte_length":481,
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
        FredTermsDocumentBytes::try_new(FredTermsDocumentRole::PrivacyPolicy, privacy_policy)?,
    ];
    let artifact = FredRightsArtifact::parse(artifact.as_bytes(), &terms_bytes)?;
    let (service_permission, grants) = if permit_persistence {
        let authorization_bytes = b"exact CPI series owner authorization";
        let authorization = FredOwnerAuthorizationEvidence::try_new(
            "https://owner.example.test/cpiaucsl-authorization".to_owned(),
            Sha256Digest::from_bytes(sha2::Sha256::digest(authorization_bytes).into()),
            authorization_bytes.len(),
            authorization_bytes,
        )?;
        let grant = FredSeriesRightsGrant::try_new(
            SourceIdentifier::try_from("CPIAUCSL")?,
            SourceIdentifier::try_from("test-series-owner")?,
            authorization,
            artifact.terms_evidence().bundle_digest(),
            vec![FredOperation::Persist],
            assessed,
            review,
        )?;
        let permission_bytes = b"exact St. Louis Fed persistence permission";
        let channel = FredServicePermissionChannel::try_official_https(
            "https://fred.stlouisfed.org/contactus/client-test-permission".to_owned(),
            "https://fred.stlouisfed.org/contactus/".to_owned(),
        )?;
        let decision = FredServicePermissionReview::try_new(
            SourceIdentifier::try_from("client-test-reviewer")?,
            now,
            SourceIdentifier::try_from("federal-reserve-bank-of-st-louis")?,
            SourceIdentifier::try_from("market-squawk")?,
            SourceIdentifier::try_from("fred-api")?,
            vec![SourceIdentifier::try_from("CPIAUCSL")?],
            vec![FredOperation::Persist],
            Vec::new(),
            assessed,
            None,
            review,
        )?;
        let permission = FredServicePermissionEvidence::try_new(
            channel,
            decision,
            artifact.terms_evidence().bundle_digest(),
            Sha256Digest::from_bytes(sha2::Sha256::digest(permission_bytes).into()),
            permission_bytes.len(),
            permission_bytes,
        )?;
        (Some(permission), vec![grant])
    } else {
        (None, Vec::new())
    };
    Ok(FredRightsPolicy::try_new(
        artifact,
        service_permission,
        grants,
    )?)
}

fn exact_evidence(bytes: &[u8]) -> ExactPayloadEvidence {
    use sha2::Digest;

    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        sha2::Sha256::digest(bytes).into(),
    ))
}
