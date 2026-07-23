use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::future::BoxFuture;
use market_squawk::application::{
    ManagedResearchExtractionSource, ProductionResearchIngestCoordinator, ResearchExtractionLimits,
    ResearchIngestCoordinator, ResearchRevisionPlanError, ResearchRightsAuthority,
    ResearchSourceDiscoveryCoordinator,
};
use market_squawk::{
    ProviderOnboardingPortal, ProviderOnboardingService, ProviderPortalActivationAuthority,
    ProviderPortalActivationError, ProviderPortalActivationRequest, ProviderPortalActivationView,
    ProviderPortalConfig, ProviderProfileRegistrationOutcome, ResearchService,
    ResearchServiceError, StartOnboardingRequest,
};
use market_squawk_data::{
    CatalogConfig, CatalogLimit, CatalogResultLimits, ObjectStoreConfig, RightsBasis,
};
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
    RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_platform::{
    EncryptedFileFallbackStatus, EncryptedFileSecretStore, LocalAuthorityStateStore, LocalPaths,
    PreferredSecretStore, SecretValue,
};
use market_squawk_services::{JsonStructureLimits, RequestContext, RequestId, ServiceLimits};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, CoverageDomain, DiscoveryBatch, DiscoveryRequest,
    ExtractionAuthority, ExtractionBatch, ExtractionRequest, ExtractionRevisionPlan,
    ExtractionSource, ExtractionSourceError, FreshnessPolicy, HistoricalCapability,
    NetworkAccessPolicy, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataInput, SourceMetadataProvider, SourceObject, SourceProtocolProfile,
};
use reqwest::header::{CONTENT_TYPE, COOKIE, ORIGIN, SET_COOKIE};
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
    let coordinator = ProductionResearchIngestCoordinator::new(
        registry,
        research,
        ResearchExtractionLimits::try_new(
            NonZeroU16::new(8).ok_or("discovery bound is zero")?,
            NonZeroU32::new(16).ok_or("record bound is zero")?,
            NonZeroU64::new(64 * 1024).ok_or("byte bound is zero")?,
            Duration::from_secs(5),
        )?,
    );
    let profile = SourceIdentifier::try_from("treasury.fiscal-data")?;
    let dataset = SourceIdentifier::try_from("average-interest-rates")?;
    let source = DiscoveryFixtureSource::try_new()?;
    let source_id = source.metadata().source_id().clone();
    coordinator.register_source(
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
    let context = discovery_context()?;

    let discovery_authority: &dyn ResearchSourceDiscoveryCoordinator = &coordinator;
    let discovered = discovery_authority
        .discover_registered_objects(
            &profile,
            &dataset,
            None,
            NonZeroU16::new(4).ok_or("discovery bound is zero")?,
            &context,
        )
        .await?;
    let exact_object = discovered
        .objects()
        .first()
        .ok_or("fixture discovery returned no object")?
        .object_id()
        .clone();
    let extracted = coordinator
        .extract_registered_batch(&profile, &dataset, &exact_object, &context)
        .await?;

    assert!(
        discovered.profile() == &profile
            && discovered.metadata().provider().as_str() == "treasury"
            && discovered.metadata().coverage().domain() == CoverageDomain::Macroeconomic
            && discovered.rights().persistence_operation_admitted()
            && discovered.rights().basis_digest() == evidence(41)
            && discovered.rights().authorization_evidence() == evidence(42)
            && discovered.objects().len() == 1
            && exact_object.as_str() == "average-interest-rates:sha256:fixture"
            && extracted.request().object().object_id() == &exact_object
    );
    let wire = serde_json::to_value(&discovered)?;
    assert!(
        wire["objects"][0]["object_id"] == exact_object.as_str()
            && wire["rights"]["persistence_operation_admitted"] == true
            && wire.get("response_body").is_none()
    );

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
    let fallback_service = Arc::new(ProviderOnboardingService::try_new(
        research.onboarding_catalog(),
        Arc::new(
            PreferredSecretStore::try_new_with_locked_encrypted_file_fallback(
                "market-squawk-test",
                directory.path().join("preferred-provider-secrets"),
            )?,
        ),
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
    let service = Arc::new(ProviderOnboardingService::try_new(
        research.onboarding_catalog(),
        secrets,
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
        registered.outcome() == ProviderProfileRegistrationOutcome::Inserted
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
}

impl DiscoveryFixtureSource {
    fn try_new() -> Result<Self, Box<dyn Error>> {
        let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
        Ok(Self {
            metadata: SourceMetadata::try_new(SourceMetadataInput::new(
                SchemaVersion::CURRENT,
                SourceId::try_from("treasury-discovery-fixture")?,
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
            object_id: SourceIdentifier::try_from("average-interest-rates:sha256:fixture")?,
            media_type: SourceIdentifier::try_from("application-json")?,
            effective,
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
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(ExtractionSourceError::Cancelled);
            }
            authority.validate_current()?;
            let object = SourceObject::try_new(
                metadata.source_id().clone(),
                metadata.revision().clone(),
                &request,
                object_id,
                media_type,
                ExactPayloadEvidence::from_content_digest(evidence(34)),
                effective,
                None,
                Some(0),
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
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(ExtractionSourceError::Cancelled);
            }
            authority.validate_current()?;
            ExtractionBatch::try_new(&request, Vec::new()).map_err(Into::into)
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

fn evidence(seed: u8) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest([seed]).into())
}
