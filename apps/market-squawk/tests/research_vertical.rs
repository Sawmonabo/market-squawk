use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use futures_util::future::BoxFuture;
use market_squawk::application::{
    ApplicationDomainService, EphemeralSourceInspectionAuthority, EphemeralSourceInspectionRequest,
    EphemeralSourceInspectionResult, ManagedResearchExtractionSource,
    PrepublishedResearchSourceRegistration, ProductionResearchIngestCoordinator,
    ResearchExtractionLimits, ResearchIngestCoordinator, ResearchRevisionPlanError,
    ResearchRightsAuthority, ResearchSourceDiscoveryCoordinator, SourceDomainService,
    SourceRuntimeRequest, SourceRuntimeSnapshotBatch, SourceRuntimeView, SourceRuntimeViewError,
    application_capabilities,
};
use market_squawk::{
    LocalProduct, ProviderOnboardingPortal, ProviderOnboardingService,
    ProviderPortalActivationAuthority, ProviderPortalActivationError,
    ProviderPortalActivationRequest, ProviderPortalActivationView, ProviderPortalConfig,
    ProviderProfileRegistrationOutcome, ResearchService, ResearchServiceError,
    StartOnboardingRequest,
    cli::{Cli, Command, IngestCommand, QueryCommand},
    local_product::execute_cli_command,
};
use market_squawk_data::{
    CatalogConfig, CatalogLimit, CatalogResultLimits, ObjectStoreConfig, RightsBasis,
    SqliteProviderRateStore,
};
use market_squawk_domain::{
    AuthorizationBasis, AvailabilityEvidence, CalendarDate, ChecksumCapability, CoverageDelay,
    DataQuality, DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, MacroObservation, MetadataRevision, PayloadHash, PayloadReference,
    ResearchContext, ResearchObservation, ResearchProvenance, ResearchProvenanceInput,
    ResearchTemporalCoordinate, ResearchTime, RevisionBoundPayloadEvidence, RevisionNumber,
    SchemaVersion, SequenceCapability, SourceId, SourceIdentifier, Timestamp,
    VersionPinnedSourceLocator,
};
use market_squawk_platform::{
    AppConfig, ConfigOverrides, ConfigSources, EncryptedFileFallbackStatus,
    EncryptedFileSecretStore, LocalAuthorityStateStore, LocalPaths, PreferredSecretStore,
    SecretValue,
};
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ServiceError, ServiceLimits,
    SourceEvidencePolicy, ToolArtifactPolicy, ToolAuthorization, TypedToolRequest,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, AvailabilityEvidence as SourceAvailabilityEvidence,
    CoverageDomain, DiscoveryBatch, DiscoveryRequest, ExtractionAuthority, ExtractionBatch,
    ExtractionRecord, ExtractionRequest, ExtractionRevisionPlan, ExtractionSource,
    ExtractionSourceError, FRED_ALFRED_API_SURFACE_ID, FreshnessPolicy, HistoricalCapability,
    MAX_DISCOVERY_OBJECTS, NetworkAccessPolicy, ProviderRateAuthority, SourceCapabilities,
    SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput, SourceMetadataProvider,
    SourceObject, SourceProtocolProfile,
};
use reqwest::header::{CONTENT_TYPE, COOKIE, ORIGIN, SET_COOKIE};
use rust_decimal::Decimal;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug)]
struct UnusedAdapterActivation;

#[async_trait]
impl ProviderPortalActivationAuthority for UnusedAdapterActivation {
    async fn activate(
        &self,
        _session_id: Uuid,
        _request: ProviderPortalActivationRequest,
        _cancellation: CancellationToken,
    ) -> Result<ProviderPortalActivationView, ProviderPortalActivationError> {
        Err(ProviderPortalActivationError::Unavailable)
    }

    async fn cancel(
        &self,
        _session_id: Uuid,
        _cancellation: CancellationToken,
    ) -> Result<market_squawk::OnboardingSessionView, ProviderPortalActivationError> {
        Err(ProviderPortalActivationError::Unavailable)
    }
}

#[async_trait]
impl EphemeralSourceInspectionAuthority for UnusedAdapterActivation {
    async fn inspect(
        &self,
        _request: EphemeralSourceInspectionRequest,
    ) -> Result<EphemeralSourceInspectionResult, ServiceError> {
        Err(ServiceError::Unavailable)
    }
}

#[derive(Debug)]
struct CanonicalFredInspection;

#[async_trait]
impl EphemeralSourceInspectionAuthority for CanonicalFredInspection {
    async fn inspect(
        &self,
        request: EphemeralSourceInspectionRequest,
    ) -> Result<EphemeralSourceInspectionResult, ServiceError> {
        let received_at = Timestamp::from_unix_nanos(1_000);
        let ingested_at = Timestamp::from_unix_nanos(1_001);
        let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("fred-fred-alfred.api-v1-v2")
                .map_err(|_error| ServiceError::InvalidResult)?,
            instrument_id: None,
            venue_id: None,
            source_identifier: SourceIdentifier::try_from("fred:UNRATE:2026-06-01:2026-07-03")
                .map_err(|_error| ServiceError::InvalidResult)?,
            source_timestamp: None,
            received_at,
            ingested_at,
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                DigestAlgorithm::Sha256,
                [9; 32],
            )),
            availability: AvailabilityEvidence::local_first_observed(received_at),
        })
        .map_err(|_error| ServiceError::InvalidResult)?;
        let effective = ResearchTemporalCoordinate::calendar_date(
            CalendarDate::new(2026, 6, 1).map_err(|_error| ServiceError::InvalidResult)?,
        );
        let published = ResearchTemporalCoordinate::calendar_date(
            CalendarDate::new(2026, 7, 3).map_err(|_error| ServiceError::InvalidResult)?,
        );
        let time = ResearchTime::try_new_with_coordinates(
            effective,
            Some(published),
            RevisionNumber::new(1).map_err(|_error| ServiceError::InvalidResult)?,
            None,
        )
        .map_err(|_error| ServiceError::InvalidResult)?;
        let context =
            ResearchContext::new(provenance, time).map_err(|_error| ServiceError::InvalidResult)?;
        let observation = serde_json::to_value(ResearchObservation::Macro(MacroObservation::new(
            context,
            SourceIdentifier::try_from("UNRATE").map_err(|_error| ServiceError::InvalidResult)?,
            Decimal::new(41, 1),
            SourceIdentifier::try_from("fred-unit:v1:Percent")
                .map_err(|_error| ServiceError::InvalidResult)?,
        )))
        .map_err(|_error| ServiceError::InvalidResult)?;
        let page_evidence = ExactPayloadEvidence::with_version_pinned_locator(
            EvidenceDigest::new(DigestAlgorithm::Sha256, [7; 32]),
            VersionPinnedSourceLocator::new(
                SourceIdentifier::try_from("https://api.stlouisfed.org/fred/series/observations")
                    .map_err(|_error| ServiceError::InvalidResult)?,
                SourceIdentifier::try_from(
                    "0707070707070707070707070707070707070707070707070707070707070707",
                )
                .map_err(|_error| ServiceError::InvalidResult)?,
            ),
        );
        Ok(EphemeralSourceInspectionResult::new(
            request.provider().clone(),
            request.onboarding_session_id(),
            request.dataset_identifier().clone(),
            SourceIdentifier::try_from("fred-page-v2:0:1:1:1:1:fixture")
                .map_err(|_error| ServiceError::InvalidResult)?,
            request.page_index(),
            page_evidence,
            received_at,
            vec![observation],
        ))
    }
}

#[derive(Debug)]
struct EmptySourceRuntime;

#[async_trait]
impl SourceRuntimeView for EmptySourceRuntime {
    async fn current(
        &self,
        _request: SourceRuntimeRequest,
    ) -> Result<SourceRuntimeSnapshotBatch, SourceRuntimeViewError> {
        SourceRuntimeSnapshotBatch::try_new(Vec::new())
    }
}

#[test]
fn research_service_reopens_the_exact_local_catalog_and_artifact_authority()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let catalog = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let objects = ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?;

    drop(ResearchService::initialize(
        &paths,
        catalog.clone(),
        8,
        objects,
    )?);
    let reopened = ResearchService::open(&paths, catalog, 8, objects);
    assert!(!matches!(
        reopened,
        Err(ResearchServiceError::Ingest(
            market_squawk_data::IngestError::CatalogCompositionMismatch
        ))
    ));
    drop(reopened?);
    Ok(())
}

#[tokio::test]
async fn registered_provider_discovery_returns_exact_ingestible_object_and_rights_evidence()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let catalog = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let objects = ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?;
    let research = Arc::new(ResearchService::initialize(&paths, catalog, 8, objects)?);
    let registry = market_squawk_sources::AuthoritativeSourceRegistry::try_new_durable(
        LocalAuthorityStateStore::try_open(
            paths
                .control_root()?
                .root()
                .join("discovery-source-authority"),
        )?,
    )?;
    let profile = SourceIdentifier::try_from("embedded.treasury-discovery")?;
    let dataset = SourceIdentifier::try_from("average-interest-rates")?;
    let source = DiscoveryFixtureSource::try_new(
        "treasury-discovery-fixture",
        "average-interest-rates:sha256:fixture",
        FixtureDiscovery::Once,
        FixtureExtraction::Observation,
    )?;
    let source_id = source.metadata().source_id().clone();
    let primary_registration = PrepublishedResearchSourceRegistration::try_new(
        profile.clone(),
        source,
        ResearchRightsAuthority::try_new(
            source_id,
            RightsBasis::reviewed_terms(
                "https://fiscaldata.treasury.gov/api-documentation/",
                evidence(41),
            )?,
            evidence(42),
            None,
        )?,
    )?;
    let capacity_profile = SourceIdentifier::try_from("treasury.receipt-capacity")?;
    let capacity_dataset = SourceIdentifier::try_from("receipt-capacity-dataset")?;
    let capacity_source = DiscoveryFixtureSource::try_new(
        "receipt-capacity-fixture",
        "receipt-capacity-object",
        FixtureDiscovery::Repeated,
        FixtureExtraction::Observation,
    )?;
    let capacity_source_id = capacity_source.metadata().source_id().clone();
    let capacity_registration = PrepublishedResearchSourceRegistration::try_new(
        capacity_profile.clone(),
        capacity_source,
        fixture_rights(capacity_source_id, 71)?,
    )?;
    let expiry_profile = SourceIdentifier::try_from("treasury.receipt-expiry")?;
    let expiry_dataset = SourceIdentifier::try_from("receipt-expiry-dataset")?;
    let expiry_source = DiscoveryFixtureSource::try_new(
        "receipt-expiry-fixture",
        "receipt-expiry-object",
        FixtureDiscovery::Once,
        FixtureExtraction::Observation,
    )?;
    let expiry_source_id = expiry_source.metadata().source_id().clone();
    let rights_expiry = current_timestamp()?.checked_add_nanos(2_000_000_000)?;
    let expiry_registration = PrepublishedResearchSourceRegistration::try_new(
        expiry_profile.clone(),
        expiry_source,
        ResearchRightsAuthority::try_new(
            expiry_source_id,
            RightsBasis::reviewed_terms(
                "https://fiscaldata.treasury.gov/api-documentation/",
                evidence(81),
            )?,
            evidence(82),
            Some(rights_expiry),
        )?,
    )?;
    let coordinator = Arc::new(
        ProductionResearchIngestCoordinator::try_new_with_prepublished_sources(
            registry,
            Arc::clone(&research),
            ResearchExtractionLimits::try_new(
                NonZeroU16::new(8).ok_or("discovery bound is zero")?,
                NonZeroU32::new(16).ok_or("record bound is zero")?,
                NonZeroU64::new(64 * 1024).ok_or("byte bound is zero")?,
                Duration::from_secs(60),
                Duration::from_secs(5 * 60),
            )?,
            [
                primary_registration,
                capacity_registration,
                expiry_registration,
            ],
        )?,
    );
    let expiry_context = long_context("receipt-expiry", Duration::from_secs(60))?;
    let expiry_discovery = coordinator
        .discover_registered_objects(
            &expiry_profile,
            &expiry_dataset,
            None,
            NonZeroU16::MIN,
            &expiry_context,
        )
        .await?;
    let expiry_selection = expiry_discovery
        .objects()
        .first()
        .ok_or("expiry discovery returned no object")?;
    assert_eq!(
        expiry_selection.discovery_receipt_expires_at(),
        rights_expiry
    );
    let expiry_ingest = admitted_ingest(
        &expiry_profile,
        &expiry_dataset,
        expiry_selection.source_object().object_id(),
        expiry_selection.discovery_receipt(),
    )?;
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    assert!(matches!(
        ResearchIngestCoordinator::ingest(
            coordinator.as_ref(),
            &expiry_ingest,
            &expiry_context,
            expiry_context.limits(),
        )
        .await,
        Err(ServiceError::NotFound)
    ));
    let onboarding = Arc::new(ProviderOnboardingService::try_new_with_provider_rate(
        research.onboarding_catalog(),
        Arc::new(EncryptedFileSecretStore::try_open(
            directory.path().join("discovery-provider-secrets"),
            SecretValue::new("discovery test unlock".to_owned())?,
        )?),
        provider_rate_authority(&directory.path().join("discovery-provider-rate.sqlite3"))?,
    )?);
    let discovery: Arc<dyn ResearchSourceDiscoveryCoordinator> = Arc::clone(&coordinator) as Arc<_>;
    let source_service = SourceDomainService::try_new(
        onboarding,
        Arc::new(EmptySourceRuntime),
        discovery,
        Arc::new(UnusedAdapterActivation),
        Arc::new(CanonicalFredInspection),
    )?;
    let capabilities = application_capabilities()?;
    let inspect = capabilities
        .find("Source.Inspect")
        .ok_or("Source.Inspect is not registered")?;
    let inspection_session_id = Uuid::new_v4();
    let inspected = source_service
        .call(
            inspect.admit(
                json!({
                    "provider": FRED_ALFRED_API_SURFACE_ID,
                    "onboardingSessionId": inspection_session_id,
                    "datasetIdentifier": "fred:series:UNRATE",
                    "pageIndex": 0,
                    "maxRecords": 1,
                    "sourceCoverage": [FRED_ALFRED_API_SURFACE_ID],
                    "resultLimits": {"maximumItems": 1, "maximumBytes": 1024 * 1024},
                })
                .as_object()
                .cloned()
                .ok_or("inspection arguments must be an object")?,
            )?,
            discovery_context()?,
        )
        .await?;
    inspected.validate_for(inspect)?;
    assert!(
        inspected.structured_content()["provider"] == FRED_ALFRED_API_SURFACE_ID
            && inspected.structured_content()["onboardingSessionId"]
                == inspection_session_id.to_string()
            && inspected.structured_content()["observations"]
                .as_array()
                .is_some_and(|observations| observations.len() == 1)
    );
    let discover = capabilities
        .find("Source.Discover")
        .ok_or("Source.Discover is not registered")?;
    let list_objects = capabilities
        .find("Source.ListObjects")
        .ok_or("Source.ListObjects is not registered")?;
    let list_effects = list_objects.effects();
    assert!(
        list_objects.contract().authorization() == ToolAuthorization::ReadOnly
            && list_objects.contract().result().source_evidence() == SourceEvidencePolicy::Required
            && list_objects.contract().result().artifact() == ToolArtifactPolicy::InlineOnly
            && list_effects.read_only()
            && !list_effects.destructive()
            && list_effects.idempotent()
            && list_effects.open_world()
    );
    let effects = discover.effects();
    assert!(
        discover.contract().authorization() == ToolAuthorization::LocalConfirmation
            && discover.contract().result().source_evidence() == SourceEvidencePolicy::Required
            && discover.contract().result().artifact() == ToolArtifactPolicy::InlineOnly
            && !effects.read_only()
            && !effects.destructive()
            && !effects.idempotent()
            && effects.open_world()
    );
    let ingest = capabilities
        .find("Research.IngestSource")
        .ok_or("Research.IngestSource is not registered")?;
    let effects = ingest.effects();
    assert!(
        ingest.contract().authorization() == ToolAuthorization::LocalConfirmation
            && ingest.contract().result().source_evidence() == SourceEvidencePolicy::Required
            && ingest.contract().result().artifact() == ToolArtifactPolicy::InlineOnly
            && !effects.read_only()
            && !effects.destructive()
            && effects.idempotent()
            && effects.open_world()
    );
    assert!(
        discover
            .admit(
                json!({
                    "provider": profile,
                    "dataset": dataset,
                    "sourceCoverage": [profile],
                    "resultLimits": {"maximumItems": 64, "maximumBytes": 1024 * 1024},
                })
                .as_object()
                .cloned()
                .ok_or("unconfirmed discovery arguments must be an object")?,
            )
            .is_err()
    );
    let mismatched_scope = discover.admit(
        json!({
            "provider": profile,
            "dataset": dataset,
            "confirm": true,
            "sourceCoverage": ["substituted-provider"],
            "resultLimits": {"maximumItems": 64, "maximumBytes": 1024 * 1024},
        })
        .as_object()
        .cloned()
        .ok_or("discovery arguments must be an object")?,
    )?;
    assert!(matches!(
        source_service
            .call(mismatched_scope, discovery_context()?)
            .await,
        Err(ServiceError::InvalidRequest)
    ));
    let discovered = source_service
        .call(
            discover.admit(
                json!({
                    "provider": profile,
                    "dataset": dataset,
                    "confirm": true,
                    "sourceCoverage": [profile],
                    "resultLimits": {"maximumItems": 64, "maximumBytes": 1024 * 1024},
                })
                .as_object()
                .cloned()
                .ok_or("discovery arguments must be an object")?,
            )?,
            discovery_context()?,
        )
        .await?;
    let wire = discovered.structured_content();
    let selected = wire["objects"]
        .as_array()
        .and_then(|objects| objects.first())
        .ok_or("fixture discovery returned no object")?;
    let exact_object = SourceIdentifier::try_from(
        selected["object_id"]
            .as_str()
            .ok_or("fixture discovery omitted exact object identity")?,
    )?;
    let receipt = selected["discovery_receipt"]
        .as_str()
        .ok_or("fixture discovery omitted its receipt")?
        .to_owned();
    let context = discovery_context()?;
    let mismatched = admitted_ingest(
        &profile,
        &dataset,
        &SourceIdentifier::try_from("substituted-object")?,
        &receipt,
    )?;
    assert!(matches!(
        ResearchIngestCoordinator::ingest(
            coordinator.as_ref(),
            &mismatched,
            &context,
            context.limits(),
        )
        .await,
        Err(ServiceError::InvalidRequest)
    ));
    let request = admitted_ingest(&profile, &dataset, &exact_object, &receipt)?;
    let ingested = ResearchIngestCoordinator::ingest(
        coordinator.as_ref(),
        &request,
        &context,
        context.limits(),
    )
    .await?;
    assert!(matches!(
        ResearchIngestCoordinator::ingest(
            coordinator.as_ref(),
            &request,
            &context,
            context.limits(),
        )
        .await,
        Err(ServiceError::NotFound)
    ));

    assert!(
        discovered.item_count() == 1
            && wire["profile"] == profile.as_str()
            && wire["metadata"]["provider"] == "treasury"
            && wire["metadata"]["coverage"]["domain"] == "macroeconomic"
            && wire["metadata"]["quality_ceiling"] == "official_delayed"
            && wire["rights"]["persistence_operation_admitted"] == true
            && wire["objects"]
                .as_array()
                .is_some_and(|objects| objects.len() == 1)
            && wire["request"]["max_results"] == 8
            && exact_object.as_str() == "average-interest-rates:sha256:fixture"
            && wire["receipts_survive_restart"] == false
            && selected["discovery_receipt_expires_at"]
                .as_i64()
                .is_some_and(|expires_at| expires_at > 0)
            && discovered.metadata().source_coverage()["provider"] == profile.as_str()
            && discovered.metadata().source_coverage()["coverageDomain"] == "macroeconomic"
            && discovered.metadata().source_coverage()["coverageEvidence"]
                == wire["metadata"]["coverage"]["evidence"]
            && discovered.metadata().data_quality()["qualityCeiling"] == "official_delayed"
            && ingested.structured_content()["rowCount"] == 1
    );
    assert!(
        wire["objects"][0]["object_id"] == exact_object.as_str()
            && wire["objects"][0]["discovery_receipt"] == receipt
            && wire["rights"]["persistence_operation_admitted"] == true
            && wire["receipts_survive_restart"] == false
            && wire.get("response_body").is_none()
    );

    let capacity_context = long_context("receipt-capacity", Duration::from_secs(60))?;
    let first_capacity_discovery = coordinator
        .discover_registered_objects(
            &capacity_profile,
            &capacity_dataset,
            None,
            NonZeroU16::MIN,
            &capacity_context,
        )
        .await?;
    let first_capacity_selection = first_capacity_discovery
        .objects()
        .first()
        .map(|selection| {
            (
                selection.source_object().object_id().clone(),
                selection.discovery_receipt().to_owned(),
            )
        })
        .ok_or("capacity discovery returned no object")?;
    let rejected_publication = discover.admit(
        json!({
            "provider": capacity_profile,
            "dataset": capacity_dataset,
            "confirm": true,
            "sourceCoverage": [capacity_profile],
            "resultLimits": {"maximumItems": 1, "maximumBytes": 1},
        })
        .as_object()
        .cloned()
        .ok_or("constrained discovery arguments must be an object")?,
    )?;
    assert!(matches!(
        source_service
            .call(rejected_publication, discovery_context()?)
            .await,
        Err(ServiceError::ResourceExhausted)
    ));
    let outer_publication = discover.admit(
        json!({
            "provider": capacity_profile,
            "dataset": capacity_dataset,
            "confirm": true,
            "sourceCoverage": [capacity_profile],
            "resultLimits": {"maximumItems": 1, "maximumBytes": 1024 * 1024},
        })
        .as_object()
        .cloned()
        .ok_or("outer publication arguments must be an object")?,
    )?;
    let unpublished = source_service
        .call(outer_publication.clone(), discovery_context()?)
        .await?;
    source_service.rollback_unpublished_result(&outer_publication, &unpublished)?;
    for _index in 1..MAX_DISCOVERY_OBJECTS {
        coordinator
            .discover_registered_objects(
                &capacity_profile,
                &capacity_dataset,
                None,
                NonZeroU16::MIN,
                &capacity_context,
            )
            .await?;
    }
    assert!(matches!(
        coordinator
            .discover_registered_objects(
                &capacity_profile,
                &capacity_dataset,
                None,
                NonZeroU16::MIN,
                &capacity_context,
            )
            .await,
        Err(ServiceError::ResourceExhausted)
    ));
    let listed = source_service
        .call(
            list_objects.admit(
                json!({
                    "provider": capacity_profile,
                    "dataset": capacity_dataset,
                    "sourceCoverage": [capacity_profile],
                    "resultLimits": {"maximumItems": 1, "maximumBytes": 1024 * 1024},
                })
                .as_object()
                .cloned()
                .ok_or("listing arguments must be an object")?,
            )?,
            capacity_context.clone(),
        )
        .await?;
    let listed_wire = listed.structured_content();
    assert!(
        listed.item_count() == 1
            && listed_wire["objects"][0]["object_id"] == "receipt-capacity-object"
            && listed_wire["objects"][0].get("discovery_receipt").is_none()
            && listed_wire.get("rights").is_none()
            && listed_wire.get("receipts_survive_restart").is_none()
    );
    let (capacity_object, capacity_receipt) = first_capacity_selection;
    let capacity_ingest = admitted_ingest(
        &capacity_profile,
        &capacity_dataset,
        &capacity_object,
        &capacity_receipt,
    )?;
    assert!(
        ResearchIngestCoordinator::ingest(
            coordinator.as_ref(),
            &capacity_ingest,
            &capacity_context,
            capacity_context.limits(),
        )
        .await?
        .structured_content()["rowCount"]
            == 1
    );
    source_service.begin_shutdown();
    source_service
        .finish_shutdown(Instant::now() + Duration::from_secs(5))
        .await?;
    ResearchIngestCoordinator::begin_shutdown(coordinator.as_ref());
    ResearchIngestCoordinator::finish_shutdown(
        coordinator.as_ref(),
        Instant::now() + Duration::from_secs(5),
    )
    .await?;

    assert!(
        Cli::try_parse_from([
            "market-squawk",
            "source",
            "discover",
            "treasury.fiscal-data",
            "--dataset",
            "average-interest-rates",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "market-squawk",
            "ingest",
            "source",
            "treasury.fiscal-data",
            "average-interest-rates:sha256:fixture",
            "--dataset",
            "average-interest-rates",
            "--confirm",
        ])
        .is_ok()
    );

    let local_input = directory.path().join("local-input");
    fs::create_dir_all(&local_input)?;
    fs::write(local_input.join("prices.csv"), b"id,value\nrow-1,123.45\n")?;
    let local_input = fs::canonicalize(local_input)?;
    let manifest_path = local_input.join("manifest.json");
    fs::write(
        &manifest_path,
        serde_json::to_vec(&json!({
            "schema_version": 3,
            "objects": [{
                "dataset": "local-alternative-prices",
                "object_id": "local-price-object",
                "path": "prices.csv",
                "format": {"kind": "csv", "delimiter": 44},
                "effective_at": 100,
                "published_at": 150,
                "revision": "local-price-revision-1",
                "revision_number": 1,
                "superseded_at": null,
                "record_time": {
                    "effective": {
                        "schema_version": 2,
                        "coordinate": {"precision": "exact_timestamp", "value": 100}
                    },
                    "published": {
                        "schema_version": 2,
                        "coordinate": {"precision": "exact_timestamp", "value": 150}
                    },
                    "superseded": null
                },
                "instrument_binding": {"kind": "unscoped"},
                "row_policy": {
                    "identity_field": "id",
                    "fields": [{
                        "source": "value",
                        "field": "price",
                        "decimal_scale": 2,
                        "unit": "USD"
                    }]
                }
            }]
        }))?,
    )?;
    let environment = BTreeMap::<OsString, OsString>::new();
    let config = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides {
            data_dir: Some(directory.path().join("local-product")),
            ..ConfigOverrides::default()
        },
    ))?;
    let product = LocalProduct::try_new(config)?;
    assert_eq!(
        product
            .provider_onboarding()
            .encrypted_file_fallback_status()?,
        EncryptedFileFallbackStatus::Locked
    );
    let local_ingest = execute_cli_command(
        &product,
        Command::Ingest {
            command: IngestCommand::File {
                manifest: manifest_path,
                object: "local-price-object".to_owned(),
                dataset: "local-alternative-prices".to_owned(),
                confirm: true,
            },
        },
    )
    .await?;
    assert_eq!(local_ingest.value()["data"]["rowCount"], 1);
    assert!(
        product
            .application()
            .shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .is_complete()
    );
    Ok(())
}

#[tokio::test]
async fn one_shot_source_cli_mints_and_consumes_its_receipt_in_one_product_lifetime()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let environment = BTreeMap::<OsString, OsString>::new();
    let config = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides {
            data_dir: Some(directory.path().join("one-shot-product")),
            ..ConfigOverrides::default()
        },
    ))?;
    let profile = SourceIdentifier::try_from("treasury.one-shot-cli")?;
    let dataset = SourceIdentifier::try_from("one-shot-dataset")?;
    let source = DiscoveryFixtureSource::try_new(
        "one-shot-cli-fixture",
        "one-shot-object",
        FixtureDiscovery::Once,
        FixtureExtraction::Observation,
    )?;
    let source_id = source.metadata().source_id().clone();
    let registration = PrepublishedResearchSourceRegistration::try_new(
        profile.clone(),
        source,
        fixture_rights(source_id, 101)?,
    )?;
    let product = LocalProduct::try_new_with_prepublished_research_sources(config, [registration])?;
    let cli = Cli::try_parse_from([
        "market-squawk",
        "ingest",
        "source",
        profile.as_str(),
        "one-shot-object",
        "--dataset",
        dataset.as_str(),
        "--confirm",
    ])?;
    let ingested = execute_cli_command(&product, cli.command).await?;
    assert_eq!(ingested.value()["data"]["rowCount"], 1);
    assert!(
        product
            .application()
            .shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .is_complete()
    );
    Ok(())
}

#[tokio::test]
async fn oversized_datafusion_result_returns_one_retrievable_opaque_parquet_reference()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("overflow-input");
    fs::create_dir_all(&input)?;
    let mut csv = String::from("id,value\n");
    for row in 0..1_000 {
        csv.push_str(&format!("observation-{row:05},123.45\n"));
    }
    fs::write(input.join("prices.csv"), csv)?;
    let input = fs::canonicalize(input)?;
    let manifest = input.join("manifest.json");
    fs::write(
        &manifest,
        serde_json::to_vec(&json!({
            "schema_version": 3,
            "objects": [{
                "dataset": "overflow-prices",
                "object_id": "overflow-price-object",
                "path": "prices.csv",
                "format": {"kind": "csv", "delimiter": 44},
                "effective_at": 100,
                "published_at": 150,
                "revision": "overflow-price-revision-1",
                "revision_number": 1,
                "superseded_at": null,
                "record_time": {
                    "effective": {
                        "schema_version": 2,
                        "coordinate": {"precision": "exact_timestamp", "value": 100}
                    },
                    "published": {
                        "schema_version": 2,
                        "coordinate": {"precision": "exact_timestamp", "value": 150}
                    },
                    "superseded": null
                },
                "instrument_binding": {"kind": "unscoped"},
                "row_policy": {
                    "identity_field": "id",
                    "fields": [{
                        "source": "value",
                        "field": "price",
                        "decimal_scale": 2,
                        "unit": "USD"
                    }]
                }
            }]
        }))?,
    )?;
    let environment = BTreeMap::<OsString, OsString>::new();
    let config = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides {
            data_dir: Some(directory.path().join("overflow-product")),
            ..ConfigOverrides::default()
        },
    ))?;
    let product = LocalProduct::try_new(config)?;
    let ingested = execute_cli_command(
        &product,
        Command::Ingest {
            command: IngestCommand::File {
                manifest,
                object: "overflow-price-object".to_owned(),
                dataset: "overflow-prices".to_owned(),
                confirm: true,
            },
        },
    )
    .await?;
    assert_eq!(ingested.value()["data"]["rowCount"], 1_000);

    let query = execute_cli_command(
        &product,
        Command::Query {
            command: QueryCommand::Sql {
                dataset: "overflow-prices".to_owned(),
                statement: "SELECT * FROM dataset ORDER BY source_identifier".to_owned(),
                maximum_rows: 1_000,
            },
        },
    )
    .await?;
    let artifact = query.value()["data"]["artifact"]
        .as_object()
        .ok_or("overflow query omitted its terminal artifact reference")?;
    assert_eq!(artifact.len(), 5);
    assert!(!artifact.contains_key("owner"));
    assert!(!artifact.contains_key("expiresAt"));
    assert_eq!(artifact["rowCount"], 1_000);
    let artifact_id = artifact["artifactId"]
        .as_str()
        .ok_or("overflow query omitted its opaque artifact identifier")?;
    let sha256 = artifact["sha256"]
        .as_str()
        .ok_or("overflow query omitted its artifact digest")?;
    let byte_count = artifact["byteCount"]
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .ok_or("overflow query omitted its artifact byte count")?;
    assert_eq!(artifact["mediaType"], "application/vnd.apache.parquet");
    assert!(!artifact_id.contains('/') && !artifact_id.contains('.'));
    assert_eq!(sha256.len(), 64);
    assert!(byte_count > 256 * 1024);

    let read = execute_cli_command(
        &product,
        Command::Query {
            command: QueryCommand::Artifact {
                artifact_id: artifact_id.to_owned(),
                sha256: sha256.to_owned(),
                byte_count,
                media_type: "application/vnd.apache.parquet".to_owned(),
                offset: 0,
                maximum_bytes: 32 * 1024,
            },
        },
    )
    .await?;
    assert_eq!(read.value()["data"]["artifact"]["artifactId"], artifact_id);
    assert!(
        read.value()["data"]["contentBase64"]
            .as_str()
            .is_some_and(|content| content.starts_with("UEFSM"))
    );
    assert_eq!(read.value()["data"]["complete"], false);
    assert!(
        product
            .application()
            .shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .is_complete()
    );
    Ok(())
}

#[tokio::test]
async fn coordinator_duration_bounds_discovery_and_receipt_extraction_before_context_deadline()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("deadline-market-squawk"))?;
    let catalog = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let objects = ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?;
    let research = Arc::new(ResearchService::initialize(&paths, catalog, 8, objects)?);
    let registry = market_squawk_sources::AuthoritativeSourceRegistry::try_new_durable(
        LocalAuthorityStateStore::try_open(
            paths
                .control_root()?
                .root()
                .join("deadline-source-authority"),
        )?,
    )?;
    let dataset = SourceIdentifier::try_from("deadline-dataset")?;
    let discovery_profile = SourceIdentifier::try_from("deadline.discovery")?;
    let discovery_source = DiscoveryFixtureSource::try_new(
        "deadline-discovery-fixture",
        "deadline-discovery-object",
        FixtureDiscovery::Pending,
        FixtureExtraction::Observation,
    )?;
    let discovery_source_id = discovery_source.metadata().source_id().clone();
    let discovery_registration = PrepublishedResearchSourceRegistration::try_new(
        discovery_profile.clone(),
        discovery_source,
        fixture_rights(discovery_source_id, 51)?,
    )?;
    let extraction_profile = SourceIdentifier::try_from("deadline.extraction")?;
    let extraction_source = DiscoveryFixtureSource::try_new(
        "deadline-extraction-fixture",
        "deadline-extraction-object",
        FixtureDiscovery::Once,
        FixtureExtraction::Pending,
    )?;
    let extraction_source_id = extraction_source.metadata().source_id().clone();
    let extraction_registration = PrepublishedResearchSourceRegistration::try_new(
        extraction_profile.clone(),
        extraction_source,
        fixture_rights(extraction_source_id, 61)?,
    )?;
    let coordinator = ProductionResearchIngestCoordinator::try_new_with_prepublished_sources(
        registry,
        research,
        ResearchExtractionLimits::try_new(
            NonZeroU16::new(8).ok_or("discovery bound is zero")?,
            NonZeroU32::new(16).ok_or("record bound is zero")?,
            NonZeroU64::new(64 * 1024).ok_or("byte bound is zero")?,
            Duration::from_millis(50),
            Duration::from_secs(5),
        )?,
        [discovery_registration, extraction_registration],
    )?;
    let context = deadline_context("bounded-discovery")?;
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            coordinator.discover_registered_objects(
                &discovery_profile,
                &dataset,
                None,
                NonZeroU16::MIN,
                &context,
            ),
        )
        .await?,
        Err(ServiceError::DeadlineExceeded)
    ));

    let context = deadline_context("bounded-extraction")?;
    let discovery = coordinator
        .discover_registered_objects(
            &extraction_profile,
            &dataset,
            None,
            NonZeroU16::MIN,
            &context,
        )
        .await?;
    let selected = discovery
        .objects()
        .first()
        .ok_or("deadline fixture discovery returned no object")?;
    let request = admitted_ingest(
        &extraction_profile,
        &dataset,
        selected.source_object().object_id(),
        selected.discovery_receipt(),
    )?;
    let context = deadline_context("bounded-extraction-ingest")?;
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            ResearchIngestCoordinator::ingest(&coordinator, &request, &context, context.limits()),
        )
        .await?,
        Err(ServiceError::DeadlineExceeded)
    ));

    ResearchIngestCoordinator::begin_shutdown(&coordinator);
    ResearchIngestCoordinator::finish_shutdown(
        &coordinator,
        Instant::now() + Duration::from_secs(5),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn provider_portal_rejects_csrf_and_keeps_imported_secrets_write_only()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let catalog = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let objects = ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?;
    let research = ResearchService::initialize(&paths, catalog, 8, objects)?;
    let provider_rate =
        provider_rate_authority(&directory.path().join("portal-provider-rate.sqlite3"))?;
    let fallback_service = Arc::new(ProviderOnboardingService::try_new_with_provider_rate(
        research.onboarding_catalog(),
        Arc::new(
            PreferredSecretStore::try_new_with_locked_encrypted_file_fallback(
                "market-squawk-test",
                directory.path().join("preferred-provider-secrets"),
            )?,
        ),
        provider_rate.clone(),
    )?);
    assert_eq!(
        fallback_service.encrypted_file_fallback_status()?,
        EncryptedFileFallbackStatus::Locked
    );
    let fallback_portal = ProviderOnboardingPortal::start(
        Arc::clone(&fallback_service),
        Arc::new(UnusedAdapterActivation),
        ProviderPortalConfig::default(),
    )
    .await?;
    let fallback_base_url = fallback_portal.base_url().to_owned();
    let client = reqwest::Client::new();
    let fallback_bootstrap_response = client
        .get(format!("{fallback_base_url}/api/v1/bootstrap"))
        .send()
        .await?;
    let fallback_cookie = fallback_bootstrap_response
        .headers()
        .get(SET_COOKIE)
        .ok_or("fallback portal did not issue a session cookie")?
        .to_str()?
        .split(';')
        .next()
        .ok_or("fallback portal session cookie was empty")?
        .to_owned();
    let fallback_bootstrap: serde_json::Value = fallback_bootstrap_response.json().await?;
    let fallback_csrf = fallback_bootstrap["csrf_token"]
        .as_str()
        .ok_or("fallback portal did not issue a CSRF token")?;
    assert_eq!(fallback_bootstrap["encrypted_file_fallback"], "locked");
    let fallback_unlock = "portal unlock phrase must stay write-only";
    let unlocked = client
        .post(format!(
            "{fallback_base_url}/api/v1/secrets/fallback/unlock"
        ))
        .header(COOKIE, &fallback_cookie)
        .header(ORIGIN, &fallback_base_url)
        .header("x-csrf-token", fallback_csrf)
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(fallback_unlock.to_owned())
        .send()
        .await?;
    let unlocked_status = unlocked.status();
    let unlocked_body = unlocked.text().await?;
    assert!(
        unlocked_status == reqwest::StatusCode::OK
            && !unlocked_body.contains(fallback_unlock)
            && serde_json::from_str::<serde_json::Value>(&unlocked_body)?["encrypted_file_fallback"]
                == "ready"
    );
    fallback_portal.shutdown().await?;

    let secrets = Arc::new(EncryptedFileSecretStore::try_open(
        directory.path().join("provider-secrets"),
        SecretValue::new("test vault unlock".to_owned())?,
    )?);
    let service = Arc::new(ProviderOnboardingService::try_new_with_provider_rate(
        research.onboarding_catalog(),
        secrets,
        provider_rate,
    )?);
    let registered = service.register_profile("bls.v2-registered")?;
    let replayed = service.register_profile("bls.v2-registered")?;
    let portal = ProviderOnboardingPortal::start(
        Arc::clone(&service),
        Arc::new(UnusedAdapterActivation),
        ProviderPortalConfig::default(),
    )
    .await?;
    let base_url = portal.base_url().to_owned();
    let stylesheet_response = client.get(format!("{base_url}/portal.css")).send().await?;
    assert_eq!(stylesheet_response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        stylesheet_response
            .headers()
            .get(CONTENT_TYPE)
            .ok_or("portal stylesheet did not declare a content type")?
            .to_str()?,
        "text/css; charset=utf-8"
    );
    assert_eq!(
        stylesheet_response
            .headers()
            .get("cache-control")
            .ok_or("portal stylesheet did not declare a cache policy")?
            .to_str()?,
        "no-store"
    );
    let content_security_policy = stylesheet_response
        .headers()
        .get("content-security-policy")
        .ok_or("portal stylesheet did not declare a content security policy")?
        .to_str()?;
    assert!(content_security_policy.contains("style-src 'self'"));
    assert!(!content_security_policy.contains("'unsafe-inline'"));
    let bootstrap_response = client
        .get(format!("{base_url}/api/v1/bootstrap"))
        .send()
        .await?;
    let cookie = bootstrap_response
        .headers()
        .get(SET_COOKIE)
        .ok_or("portal did not issue a session cookie")?
        .to_str()?
        .split(';')
        .next()
        .ok_or("portal session cookie was empty")?
        .to_owned();
    let bootstrap: serde_json::Value = bootstrap_response.json().await?;
    let csrf = bootstrap["csrf_token"]
        .as_str()
        .ok_or("portal did not issue a CSRF token")?;
    assert_eq!(bootstrap["encrypted_file_fallback"], "disabled");
    let start_response = client
        .post(format!("{base_url}/api/v1/sessions"))
        .header(COOKIE, &cookie)
        .header(ORIGIN, &base_url)
        .header("x-csrf-token", csrf)
        .json(&serde_json::json!({
            "surface_id": "bls.v2-registered",
            "organization": "Market Squawk",
            "administrative_email": "operations@example.test"
        }))
        .send()
        .await?;
    let started: serde_json::Value = start_response.json().await?;
    let session_id = Uuid::parse_str(
        started["session_id"]
            .as_str()
            .ok_or("portal did not return a session identity")?,
    )?;
    let secret = "sentinel-registration-key-never-echo";
    let rejected = client
        .post(format!("{base_url}/api/v1/sessions/{session_id}/secret"))
        .header(COOKIE, &cookie)
        .header(ORIGIN, &base_url)
        .header("x-csrf-token", "wrong-token")
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(secret.to_owned())
        .send()
        .await?;
    let accepted = client
        .post(format!("{base_url}/api/v1/sessions/{session_id}/secret"))
        .header(COOKIE, &cookie)
        .header(ORIGIN, &base_url)
        .header("x-csrf-token", csrf)
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(secret.to_owned())
        .send()
        .await?;
    let accepted_status = accepted.status();
    let accepted_body = accepted.text().await?;
    let resumed = service.resume(session_id)?;
    let sec = service
        .start(
            StartOnboardingRequest::try_new(
                "sec.edgar-public",
                Some("Market Squawk".to_owned()),
                Some("operations@example.test".to_owned()),
            )?,
            CancellationToken::new(),
        )
        .await?;
    let recovered_sec = service.resume(sec.session_id())?;
    let sessions = service.sessions(CatalogLimit::new(8)?)?;
    let current = service.current_sessions(CatalogLimit::new(8)?)?;
    portal.shutdown().await?;

    assert!(
        registered.outcome() == ProviderProfileRegistrationOutcome::Replay
            && registered.profile().id() == "bls.v2-registered"
            && replayed.outcome() == ProviderProfileRegistrationOutcome::Replay
            && rejected.status() == reqwest::StatusCode::FORBIDDEN
            && accepted_status == reqwest::StatusCode::OK
            && !accepted_body.contains(secret)
            && resumed.credential_stored()
            && resumed.state() == market_squawk_sources::OnboardingState::StoredUnverified
            && resumed.public_configuration().get("registration_mode") == Some("registered_v2")
            && recovered_sec.public_configuration().get("organization") == Some("Market Squawk")
            && recovered_sec
                .public_configuration()
                .get("administrative_email")
                == Some("operations@example.test")
            && sessions.len() == 2
            && sessions[0].surface_id() == "sec.edgar-public"
            && current.len() == 2
            && current[0].surface_id() == "bls.v2-registered"
            && current[1].surface_id() == "sec.edgar-public"
    );
    Ok(())
}

#[derive(Debug)]
struct DiscoveryFixtureSource {
    metadata: SourceMetadata,
    object_id: SourceIdentifier,
    media_type: SourceIdentifier,
    effective: EffectiveInterval,
    payload: Bytes,
    payload_evidence: ExactPayloadEvidence,
    record_schema: SourceIdentifier,
    record_revision: SourceIdentifier,
    availability_evidence: SourceIdentifier,
    discovery: FixtureDiscovery,
    extraction: FixtureExtraction,
    discovery_attempts: AtomicUsize,
}

#[derive(Clone, Copy, Debug)]
enum FixtureDiscovery {
    Once,
    Repeated,
    Pending,
}

#[derive(Clone, Copy, Debug)]
enum FixtureExtraction {
    Observation,
    Pending,
}

impl DiscoveryFixtureSource {
    fn try_new(
        source_id: &str,
        object_id: &str,
        discovery: FixtureDiscovery,
        extraction: FixtureExtraction,
    ) -> Result<Self, Box<dyn Error>> {
        let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
        let source_id = SourceId::try_from(source_id)?;
        let payload = Bytes::from(serde_json::to_vec(&fixture_observation(
            source_id.clone(),
        )?)?);
        let payload_evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&payload).into(),
        ));
        Ok(Self {
            metadata: SourceMetadata::try_new(SourceMetadataInput::new(
                SchemaVersion::CURRENT,
                source_id,
                RevisionBoundPayloadEvidence::new(
                    MetadataRevision::new(SourceIdentifier::try_from("fixture-v1")?),
                    ExactPayloadEvidence::from_content_digest(evidence(31)),
                ),
                SourceClass::LocalFile,
                SourceIdentifier::try_from("treasury")?,
                AuthorizationGrant::new(
                    AuthorizationMode::UserOwnedLocal,
                    AuthorizationBasis::new(SourceIdentifier::try_from("fixture-owned")?),
                    ExactPayloadEvidence::from_content_digest(evidence(32)),
                    effective,
                ),
                SourceCoverage::try_non_instrument(
                    ExactPayloadEvidence::from_content_digest(evidence(33)),
                    effective,
                    CoverageDomain::Macroeconomic,
                    CoverageDelay::Delayed(1),
                    DeliveryEvidence::Unknown,
                )?,
                DataQuality::OfficialDelayed,
                NetworkAccessPolicy::Denied,
                FreshnessPolicy::try_new(1, 1, 1, 1, 0)?,
                None,
                SourceCapabilities::new(
                    false,
                    true,
                    SequenceCapability::Unsupported,
                    ChecksumCapability::Unsupported,
                    HistoricalCapability::RevisionPreserving,
                    false,
                ),
                SourceProtocolProfile::NotLive,
            ))?,
            object_id: SourceIdentifier::try_from(object_id)?,
            media_type: SourceIdentifier::try_from("application-json")?,
            effective,
            payload,
            payload_evidence,
            record_schema: SourceIdentifier::try_from("market-squawk-research-v3")?,
            record_revision: SourceIdentifier::try_from("fixture-record-v1")?,
            availability_evidence: SourceIdentifier::try_from("fixture-publication")?,
            discovery,
            extraction,
            discovery_attempts: AtomicUsize::new(0),
        })
    }
}

impl SourceMetadataProvider for DiscoveryFixtureSource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl ExtractionSource for DiscoveryFixtureSource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        let metadata = self.metadata.clone();
        let object_id = self.object_id.clone();
        let media_type = self.media_type.clone();
        let effective = self.effective;
        let expected_bytes = u64::try_from(self.payload.len()).unwrap_or(u64::MAX);
        let behavior = self.discovery;
        let attempt = self.discovery_attempts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(ExtractionSourceError::Cancelled);
            }
            authority.validate_current()?;
            match behavior {
                FixtureDiscovery::Once if attempt > 0 => {
                    return Err(market_squawk_sources::SourceError::ProviderUnavailable.into());
                }
                FixtureDiscovery::Pending => std::future::pending::<()>().await,
                FixtureDiscovery::Once | FixtureDiscovery::Repeated => {}
            }
            let object = SourceObject::try_new(
                metadata.source_id().clone(),
                metadata.revision().clone(),
                &request,
                object_id,
                media_type,
                ExactPayloadEvidence::from_content_digest(evidence(34)),
                effective,
                None,
                Some(expected_bytes),
            )?;
            DiscoveryBatch::try_new(&request, vec![object]).map_err(Into::into)
        })
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        let behavior = self.extraction;
        let payload = self.payload.clone();
        let payload_evidence = self.payload_evidence.clone();
        let record_schema = self.record_schema.clone();
        let record_revision = self.record_revision.clone();
        let availability_evidence = self.availability_evidence.clone();
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(ExtractionSourceError::Cancelled);
            }
            authority.validate_current()?;
            if matches!(behavior, FixtureExtraction::Pending) {
                std::future::pending::<()>().await;
            }
            let record = ExtractionRecord::try_new(
                &request,
                record_schema,
                payload_evidence,
                Timestamp::from_unix_nanos(1),
                Some(Timestamp::from_unix_nanos(2)),
                SourceAvailabilityEvidence::Observed {
                    available_at: Timestamp::from_unix_nanos(3),
                    evidence: availability_evidence,
                },
                record_revision,
                None,
                payload,
            )?;
            ExtractionBatch::try_new(&request, vec![record]).map_err(Into::into)
        })
    }
}

impl ManagedResearchExtractionSource for DiscoveryFixtureSource {
    fn revision_plan(
        &self,
        _batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        Ok(None)
    }
}

fn discovery_context() -> Result<RequestContext, Box<dyn Error>> {
    Ok(RequestContext::new(
        RequestId::try_string("provider-discovery-test")?,
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(10),
        ServiceLimits::try_new(
            1024 * 1024,
            64,
            8 * 1024 * 1024,
            4096,
            JsonStructureLimits::try_new(32, 1024 * 1024, 4096, 4096)?,
        )?,
    ))
}

fn deadline_context(request_id: &str) -> Result<RequestContext, Box<dyn Error>> {
    long_context(request_id, Duration::from_secs(10))
}

fn long_context(request_id: &str, duration: Duration) -> Result<RequestContext, Box<dyn Error>> {
    Ok(RequestContext::new(
        RequestId::try_string(request_id)?,
        CancellationToken::new(),
        Instant::now() + duration,
        ServiceLimits::try_new(
            1024 * 1024,
            64,
            8 * 1024 * 1024,
            4096,
            JsonStructureLimits::try_new(32, 1024 * 1024, 4096, 4096)?,
        )?,
    ))
}

fn admitted_ingest(
    profile: &SourceIdentifier,
    dataset: &SourceIdentifier,
    object: &SourceIdentifier,
    receipt: &str,
) -> Result<TypedToolRequest, Box<dyn Error>> {
    let arguments = json!({
        "provider": profile,
        "dataset": dataset,
        "object": object,
        "discoveryReceipt": receipt,
        "sourceCoverage": [profile],
        "resultLimits": {"maximumItems": 16, "maximumBytes": 1024 * 1024},
        "confirm": true,
    })
    .as_object()
    .cloned()
    .ok_or("ingest arguments must be an object")?;
    Ok(application_capabilities()?
        .find("Research.IngestSource")
        .ok_or("Research.IngestSource is not registered")?
        .admit(arguments)?)
}

fn fixture_rights(
    source_id: SourceId,
    seed: u8,
) -> Result<ResearchRightsAuthority, Box<dyn Error>> {
    Ok(ResearchRightsAuthority::try_new(
        source_id,
        RightsBasis::reviewed_terms(
            "https://fiscaldata.treasury.gov/api-documentation/",
            evidence(seed),
        )?,
        evidence(seed.saturating_add(1)),
        None,
    )?)
}

fn fixture_observation(source_id: SourceId) -> Result<ResearchObservation, Box<dyn Error>> {
    let available_at = Timestamp::from_unix_nanos(3);
    let source_identifier = SourceIdentifier::try_from("fixture-macro-observation")?;
    Ok(ResearchObservation::Macro(MacroObservation::new(
        ResearchContext::new(
            ResearchProvenance::try_new(ResearchProvenanceInput {
                source_id,
                instrument_id: None,
                venue_id: None,
                source_identifier: source_identifier.clone(),
                source_timestamp: Some(Timestamp::from_unix_nanos(1)),
                received_at: available_at,
                ingested_at: available_at,
                quality: DataQuality::OfficialDelayed,
                payload_reference: PayloadReference::SourceReference(source_identifier),
                availability: AvailabilityEvidence::evidenced(
                    available_at,
                    SourceIdentifier::try_from("fixture-publication")?,
                ),
            })?,
            ResearchTime::new(
                Timestamp::from_unix_nanos(1),
                Some(Timestamp::from_unix_nanos(2)),
                RevisionNumber::new(1)?,
                None,
            )?,
        )?,
        SourceIdentifier::try_from("average-interest-rates")?,
        Decimal::ONE,
        SourceIdentifier::try_from("percent")?,
    )))
}

fn provider_rate_authority(path: &Path) -> Result<ProviderRateAuthority, Box<dyn Error>> {
    Ok(ProviderRateAuthority::try_new(Arc::new(
        SqliteProviderRateStore::try_open(path.to_path_buf())?,
    ))?)
}

fn current_timestamp() -> Result<Timestamp, Box<dyn Error>> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let nanos = i64::try_from(elapsed.as_nanos())?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn evidence(seed: u8) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest([seed]).into())
}
