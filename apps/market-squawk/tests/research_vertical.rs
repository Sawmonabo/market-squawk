use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use market_squawk::{
    ProviderOnboardingPortal, ProviderOnboardingService, ProviderPortalActivationAuthority,
    ProviderPortalActivationError, ProviderPortalActivationRequest, ProviderPortalActivationView,
    ProviderPortalConfig, ProviderProfileRegistrationOutcome, ResearchService,
    ResearchServiceError, StartOnboardingRequest,
};
use market_squawk_data::{CatalogConfig, CatalogLimit, CatalogResultLimits, ObjectStoreConfig};
use market_squawk_platform::{EncryptedFileSecretStore, LocalPaths, SecretValue};
use reqwest::header::{CONTENT_TYPE, COOKIE, ORIGIN, SET_COOKIE};
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
    let client = reqwest::Client::new();
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
