use std::sync::atomic::{AtomicUsize, Ordering};

use market_squawk_adapter_schwab::{
    CallbackOutcome, OAuthCallback, ProtectedSchwabOAuthAuthority, ProviderIdentifier, QuoteField,
    QuoteRequest, RequestAdmission, RestExecutionOutcome, RestTransportBounds, SchwabHttpWire,
    SchwabHttpWireRequest, SchwabHttpWireResponse, SchwabOAuthAuthorityStatus,
    SchwabOAuthInteraction, SchwabOAuthWireError, SchwabOAuthWireRequest, SchwabOAuthWireResponse,
    SchwabRestExecutor, SchwabRestFamily, SchwabTransportError, SchwabTransportTelemetry,
};
use market_squawk_data::{
    CatalogConfig, CatalogResultLimits, ObjectStoreConfig, SqliteProviderRateStore,
};
use market_squawk_platform::{
    AppConfig, ConfigOverrides, ConfigSources, EncryptedFileSecretStore, LocalPaths,
};
use market_squawk_sources::AuthoritativeSourceRegistry;

use super::*;
use crate::ResearchService;
use crate::application::company_security_resolution::CompanySecurityResolutionAuthority;
use crate::application::{ProductionResearchIngestCoordinator, ResearchExtractionLimits};
use crate::provider_activation::ProviderAdapterActivation;
use crate::provider_activation::nasdaq_reference::NasdaqReferenceUniverseService;
use crate::provider_onboarding::SchwabOAuthMarketAuthority;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn configured_schwab_without_doctor_refreshes_and_survives_restart() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let catalog_config = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(64)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let (research, catalog, publisher) =
        ResearchService::open_or_initialize_with_provider_onboarding(
            &paths,
            catalog_config.clone(),
            8,
            ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
        )?;
    let research = Arc::new(research);
    let provider_rate = ProviderRateAuthority::try_new(Arc::new(
        SqliteProviderRateStore::try_open(directory.path().join("provider-rate.sqlite3"))?,
    ))?;
    let secrets = Arc::new(EncryptedFileSecretStore::try_open(
        directory.path().join("secrets"),
        SecretValue::new("Schwab recovery fixture unlock".to_owned())?,
    )?);
    let service = Arc::new(ProviderOnboardingService::try_new_with_provider_rate(
        catalog,
        Arc::clone(&secrets),
        provider_rate.clone(),
    )?);
    let profiles = built_in_provider_profiles()?;
    let profile = profiles
        .get(SCHWAB_MARKET_DATA_SURFACE_ID)
        .ok_or("missing Schwab profile")?;
    let request = OnboardingReservationRequest::try_new(
        profile.capability(),
        ProviderPublicConfiguration::default(),
        profile.capability().maximum_authority().clone(),
        SourceIdentifier::try_from("schwab-recovery-test")?,
        SourceIdentifier::try_from("configured-without-doctor")?,
        wall_deadline(Duration::from_secs(3))?,
        0,
    )?;
    let session_id = service
        .catalog
        .reserve_provider_onboarding(&request)?
        .session_id();
    service.submit_secret_blocking(
        session_id,
        SecretValue::new(
            r#"{"version":1,"app_key":"fixture-key","app_secret":"fixture-secret"}"#.to_owned(),
        )?,
        SecretCancellation::new(),
    )?;
    let bootstrap = service
        .prepare_schwab_oauth_bootstrap(session_id, CancellationToken::new())
        .await?;
    let configured = service
        .prepare_runtime_activation_target(session_id, CancellationToken::new())
        .await?;
    assert_no_doctor(&configured);
    let credential = service.retained_credential_coordinate(session_id)?;
    let configured_sequence = service
        .catalog
        .resume_provider_onboarding(session_id)?
        .next_sequence();
    let wire = Arc::new(RecoveryOAuthWire(AtomicUsize::new(0)));
    let oauth_root = directory.path().join("oauth");
    let protected = Arc::new(
        ProtectedSchwabOAuthAuthority::try_open(
            &oauth_root,
            service
                .schwab_oauth_authority_factory(&bootstrap)?
                .configuration(wire.clone())?,
        )
        .await?,
    );
    let callback = match OAuthCallback::parse(
        "https://127.0.0.1:8182/?code=one-time&state=recovery",
        "recovery",
        RequestAdmission::new(
            NonZeroUsize::new(4096).ok_or("request bound")?,
            NonZeroUsize::MIN,
        ),
    )? {
        CallbackOutcome::Authorized(callback) => callback,
        CallbackOutcome::Denied { .. } => return Err("fixture callback denied".into()),
    };
    let initial = protected
        .complete_authorization(
            &callback,
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            SchwabOAuthInteraction::Background,
        )
        .await?;
    let oauth =
        SchwabOAuthMarketAuthority::from_test_authority(session_id, initial, protected.clone());
    let config = AppConfig::load(ConfigSources::new(
        None,
        &std::collections::BTreeMap::<std::ffi::OsString, std::ffi::OsString>::new(),
        ConfigOverrides {
            data_dir: Some(directory.path().join("account-runtime")),
            ..Default::default()
        },
    ))?;
    let adapters = recovery_activation(
        Arc::clone(&service),
        Arc::clone(&research),
        &paths,
        &config,
        provider_rate.clone(),
    )?;
    // This is the production account constructor, with an actual protected OAuth owner.
    let account = adapters
        .activate_schwab_market_data_account(
            configured.clone(),
            oauth.clone(),
            CancellationToken::new(),
        )
        .await?;
    let (token, epoch) = account.acquire_runtime_publication_attempt().await?;
    let refreshed = epoch.receipt();
    assert_eq!(refreshed.generation().get(), 2);
    assert_eq!(
        refreshed.authorization_generation(),
        initial.authorization_generation()
    );
    assert_eq!(
        refreshed.authorization_scope_sha256(),
        initial.authorization_scope_sha256()
    );
    epoch.validate_current(refreshed)?;
    // The real REST executor accepts the requested family without any saved family probes.
    let response = requested_quote(token).await?;
    assert_eq!(response.payload().family(), SchwabRestFamily::Quotes);
    assert_eq!(response.payload().record_count(), 1);
    epoch.validate_current(refreshed)?;
    assert!(
        oauth
            .receipt_currentness()
            .validate_current_receipt(initial)
            .is_err()
    );
    assert_eq!(wire.0.load(Ordering::SeqCst), 2);
    assert_eq!(
        service
            .catalog
            .resume_provider_onboarding(session_id)?
            .next_sequence(),
        configured_sequence
    );
    assert!(configured.same_authority_as(&service.prepared_activation_lease(session_id)?));
    drop(epoch);
    let active = service.commit_prepared_activation(&configured).await?;
    account.seal_runtime_preparation();
    account.require_current().await?;
    assert_no_doctor(&active);
    let active_sequence = service
        .catalog
        .resume_provider_onboarding(session_id)?
        .next_sequence();
    let (_, stopped_epoch) = account.acquire_publication_attempt().await?;
    oauth.revoke_test_authority();
    assert!(
        stopped_epoch
            .validate_current(stopped_epoch.receipt())
            .is_err()
    );
    drop(stopped_epoch);
    assert!(account.require_current().await.is_err());
    drop(account);
    drop(adapters);
    drop(oauth);
    drop(protected);
    drop(service);
    drop(publisher);
    drop(research);

    let (research, catalog, _publisher) =
        ResearchService::open_or_initialize_with_provider_onboarding(
            &paths,
            catalog_config,
            8,
            ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
        )?;
    let recovered = Arc::new(ProviderOnboardingService::try_new_with_provider_rate(
        catalog,
        secrets,
        provider_rate.clone(),
    )?);
    let resumed = recovered.catalog.resume_provider_onboarding(session_id)?;
    assert_eq!(resumed.lifecycle().state(), OnboardingState::ActiveScoped);
    assert_eq!(resumed.next_sequence(), active_sequence);
    assert_eq!(
        resumed
            .lifecycle()
            .generation_runtime_evidence(credential.0),
        None
    );
    let recovered_credential = recovered.retained_credential_coordinate(session_id)?;
    assert_eq!(recovered_credential.0, credential.0);
    assert_eq!(recovered_credential.1, credential.1);
    assert_eq!(recovered_credential.2, CredentialGenerationState::ActiveScoped);
    let restored = recovered.activation_lease(session_id)?;
    assert_no_doctor(&restored);
    assert!(restored.same_authority_as(&active));
    let bootstrap = recovered
        .prepare_schwab_oauth_bootstrap(session_id, CancellationToken::new())
        .await?;
    let protected = Arc::new(
        ProtectedSchwabOAuthAuthority::try_open(
            &oauth_root,
            recovered
                .schwab_oauth_authority_factory(&bootstrap)?
                .configuration(wire.clone())?,
        )
        .await?,
    );
    let SchwabOAuthAuthorityStatus::Active(receipt) = protected.status().await? else {
        return Err("saved OAuth grant was not active after restart".into());
    };
    let oauth = SchwabOAuthMarketAuthority::from_test_authority(session_id, receipt, protected);
    let adapters = recovery_activation(
        recovered.clone(),
        Arc::new(research),
        &paths,
        &config,
        provider_rate,
    )?;
    let account = adapters
        .activate_schwab_market_data_account(restored, oauth, CancellationToken::new())
        .await?;
    account.require_current().await?;
    let (token, epoch) = account.acquire_publication_attempt().await?;
    assert_eq!(epoch.receipt(), refreshed);
    epoch.validate_current(refreshed)?;
    assert_eq!(requested_quote(token).await?.payload().record_count(), 1);
    assert_eq!(wire.0.load(Ordering::SeqCst), 2);
    assert_eq!(
        recovered
            .catalog
            .resume_provider_onboarding(session_id)?
            .next_sequence(),
        active_sequence
    );
    Ok(())
}

fn assert_no_doctor(lease: &ProviderActivationLease) {
    assert!(lease.runtime_verification_evidence().is_none());
    assert!(lease.runtime_evidence_digest().is_none());
    assert!(lease.verification_expires_at().is_none());
    assert!(lease.account_digest().is_none());
    assert!(lease.verification_evidence_digest().is_some());
}

fn recovery_activation(
    onboarding: Arc<ProviderOnboardingService>,
    research: Arc<ResearchService>,
    paths: &LocalPaths,
    config: &AppConfig,
    provider_rate: ProviderRateAuthority,
) -> TestResult<ProviderAdapterActivation> {
    let registry = AuthoritativeSourceRegistry::try_new_in_memory_for_bounded_extraction(
        Arc::new(provider_rate.clone()),
        provider_rate.clone(),
    )?;
    let (ingest, mutation, _) =
        ProductionResearchIngestCoordinator::try_new_with_runtime_authorities(
            registry,
            Arc::clone(&research),
            ResearchExtractionLimits::standard(),
            std::iter::empty(),
        )?;
    let nasdaq = NasdaqReferenceUniverseService::try_new_durable(
        provider_rate.clone(),
        research.analytical(),
        research.provider_capture_store(),
    )?;
    let resolution = Arc::new(CompanySecurityResolutionAuthority::new(
        research.company_identities(),
        research.market_data_instruments(),
        research.company_security_link_publication(),
        nasdaq.listing_reference_reader().ok_or("listing reader")?,
    ));
    Ok(ProviderAdapterActivation::new(
        onboarding,
        ingest,
        mutation,
        config.clone(),
        provider_rate,
        paths.control_root()?.root().to_path_buf(),
        resolution,
    ))
}

#[derive(Debug)]
struct RecoveryOAuthWire(AtomicUsize);

impl SchwabOAuthWire for RecoveryOAuthWire {
    fn exchange(
        &self,
        _request: SchwabOAuthWireRequest,
    ) -> Pin<
        Box<dyn Future<Output = Result<SchwabOAuthWireResponse, SchwabOAuthWireError>> + Send + '_>,
    > {
        Box::pin(async move {
            let attempt = self.0.fetch_add(1, Ordering::SeqCst);
            let body = match attempt {
                0 => br#"{"access_token":"initial-access","refresh_token":"initial-refresh","token_type":"Bearer","expires_in":30,"scope":"market-data"}"#.to_vec(),
                1 => br#"{"access_token":"refreshed-access","refresh_token":"refreshed-refresh","token_type":"Bearer","expires_in":1800,"scope":"market-data"}"#.to_vec(),
                _ => return Err(SchwabOAuthWireError::Protocol),
            };
            SchwabOAuthWireResponse::try_new(
                200,
                body,
                NonZeroUsize::new(4096).ok_or(SchwabOAuthWireError::Protocol)?,
            )
        })
    }
}

async fn requested_quote(
    token: market_squawk_adapter_schwab::TransientAccessToken,
) -> TestResult<market_squawk_adapter_schwab::ExecutedRestResponse> {
    let bound = NonZeroUsize::new(4096).ok_or("request bound")?;
    let request = QuoteRequest::try_new(
        vec![ProviderIdentifier::try_new("AAPL".to_owned())?],
        vec![QuoteField::Quote],
        None,
        RequestAdmission::new(bound, NonZeroUsize::MIN),
    )?;
    let bounds = RestTransportBounds::try_new(
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(2),
        NonZeroUsize::new(65536).ok_or("response bound")?,
        NonZeroUsize::MIN,
        bound,
    )?;
    let body = bytes::Bytes::from_static(br#"{"AAPL":{"assetMainType":"EQUITY","realtime":true,"quote":{"bidPrice":100.12,"askPrice":100.13,"bidSize":2,"askSize":3,"quoteTime":1780000000000}}}"#);
    let response = SchwabHttpWireResponse::try_new(
        200,
        request.request().url().to_owned(),
        Some(u64::try_from(body.len())?),
        vec![
            market_squawk_adapter_schwab::ResponseHeaderEvidence::try_new(
                "content-type".to_owned(),
                b"application/json".to_vec(),
            )?,
        ],
        body,
        bounds,
    )?;
    let executor = SchwabRestExecutor::try_new(
        Arc::new(RecoveryHttpWire(std::sync::Mutex::new(Some(response)))),
        bounds,
        ParseBounds::new(
            NonZeroUsize::new(65536).ok_or("parse bound")?,
            NonZeroUsize::new(64).ok_or("parse bound")?,
            bound,
            NonZeroUsize::new(16).ok_or("parse bound")?,
            32,
            8192,
        ),
        AccessTokenAdmission::new(bound, Duration::from_secs(1)),
        SchwabTransportTelemetry::default(),
    )?;
    match executor
        .execute(request.request(), &token, CancellationToken::new())
        .await?
    {
        RestExecutionOutcome::Accepted(response) => Ok(response),
        RestExecutionOutcome::InvalidPayload { error, .. } => {
            Err(format!("scripted quote payload rejected: {error}").into())
        }
        _ => Err("requested quote was not accepted".into()),
    }
}

#[derive(Debug)]
struct RecoveryHttpWire(std::sync::Mutex<Option<SchwabHttpWireResponse>>);

impl SchwabHttpWire for RecoveryHttpWire {
    fn get<'a>(
        &'a self,
        _request: SchwabHttpWireRequest<'a>,
    ) -> Pin<
        Box<dyn Future<Output = Result<SchwabHttpWireResponse, SchwabTransportError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.0
                .lock()
                .map_err(|_| SchwabTransportError::Protocol)?
                .take()
                .ok_or(SchwabTransportError::Protocol)
        })
    }
}
