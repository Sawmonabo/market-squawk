use market_squawk_data::{
    CatalogConfig, CatalogResultLimits, ObjectStoreConfig, SqliteProviderRateStore,
};
use market_squawk_domain::CoverageDelay;
use market_squawk_platform::{EncryptedFileSecretStore, LocalPaths};
use market_squawk_sources::{
    RuntimeCapabilityDisposition, SchwabMarketDataDoctorReceiptV1, SchwabMarketDataFamily,
    SchwabMarketDataFamilyEvidence, SchwabUserPreferenceDoctorEvidence,
};

use super::*;
use crate::ResearchService;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn expired_pending_schwab_doctor_renews_same_candidate_and_survives_restart() -> TestResult {
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
    let provider_rate = ProviderRateAuthority::try_new(Arc::new(
        SqliteProviderRateStore::try_open(directory.path().join("provider-rate.sqlite3"))?,
    ))?;
    let secrets = Arc::new(EncryptedFileSecretStore::try_open(
        directory.path().join("secrets"),
        SecretValue::new("Schwab recovery fixture unlock".to_owned())?,
    )?);
    let service = ProviderOnboardingService::try_new_with_provider_rate(
        catalog,
        Arc::clone(&secrets),
        provider_rate.clone(),
    )?;
    let profiles = built_in_provider_profiles()?;
    let profile = profiles
        .get(SCHWAB_MARKET_DATA_SURFACE_ID)
        .ok_or("missing Schwab profile")?;
    let setup_deadline = wall_deadline(Duration::from_secs(3))?;
    let request = OnboardingReservationRequest::try_new(
        profile.capability(),
        ProviderPublicConfiguration::default(),
        profile.capability().maximum_authority().clone(),
        SourceIdentifier::try_from("schwab-recovery-test")?,
        SourceIdentifier::try_from("pending-doctor-renewal")?,
        setup_deadline,
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
    service
        .prepare_schwab_oauth_bootstrap(session_id, CancellationToken::new())
        .await?;
    let credential = service.retained_credential_coordinate(session_id)?;
    let initial_scope = EvidenceDigest::new(DigestAlgorithm::Sha256, [31; 32]);
    let SchwabMarketDoctorRunPreparation::Ready(initial_lease) = service
        .prepare_schwab_market_doctor_run(session_id, 1, initial_scope, CancellationToken::new())
        .await?
    else {
        return Err("initial Schwab doctor was not ready".into());
    };
    let initial_binding = service.schwab_market_doctor_authority_binding(&initial_lease)?;
    let initial_observation = doctor_observation(
        1,
        1,
        initial_scope,
        Duration::from_secs(1),
        wall_deadline(Duration::from_secs(2))?,
        &initial_binding,
    )?;
    let initial_expiry = initial_observation.refresh_expires_at;
    let initial_access_expiry = initial_observation.access_expires_at;
    service
        .record_schwab_market_data_doctor_observation(
            &initial_lease,
            initial_observation,
            CancellationToken::new(),
        )
        .await?;
    let prior = retained_receipt(&service, session_id, credential.0)?;
    assert_eq!(prior.exclusive_expires_at(), initial_expiry);
    assert!(prior.exclusive_expires_at() > initial_access_expiry);
    assert_eq!(prior.predecessor_digest(), None);
    assert_eq!(initial_binding, receipt_binding(&prior, session_id, None)?);
    assert!(prior.admits_source_start());
    let unselected = prior
        .observation()
        .families
        .iter()
        .find(|family| family.family == SchwabMarketDataFamily::LevelOneFuturesOptions)
        .ok_or("missing futures-options disposition")?;
    assert_eq!(
        unselected.disposition,
        RuntimeCapabilityDisposition::NotProbed
    );
    assert_eq!(unselected.observation_sha256, None);
    assert_eq!(unselected.observed_at, None);
    assert!(matches!(
        service
            .prepare_schwab_market_doctor_run(
                session_id,
                1,
                initial_scope,
                CancellationToken::new()
            )
            .await?,
        SchwabMarketDoctorRunPreparation::Current
    ));
    // Access-token expiry does not expire capability evidence for the same authorization grant.
    let remaining = initial_access_expiry
        .unix_nanos()
        .saturating_sub(system_timestamp()?.unix_nanos());
    if remaining >= 0 {
        tokio::time::sleep(Duration::from_nanos(u64::try_from(remaining)? + 1_000_000)).await;
    }
    let now = system_timestamp()?;
    assert!(now >= initial_access_expiry && prior.is_current_at(now));
    assert!(matches!(
        service
            .prepare_schwab_market_doctor_run(
                session_id,
                1,
                initial_scope,
                CancellationToken::new()
            )
            .await?,
        SchwabMarketDoctorRunPreparation::Current
    ));

    // Expire both the receipt and operation reservation; retained setup intent must survive.
    let expiry = setup_deadline.max(prior.exclusive_expires_at());
    let remaining = expiry
        .unix_nanos()
        .saturating_sub(system_timestamp()?.unix_nanos());
    if remaining >= 0 {
        tokio::time::sleep(Duration::from_nanos(u64::try_from(remaining)? + 1_000_000)).await;
    }
    let now = system_timestamp()?;
    assert!(now >= setup_deadline && !prior.is_current_at(now));
    let pending = service.catalog.resume_provider_onboarding(session_id)?;
    let sequence_before_renewal = pending.next_sequence();
    assert_eq!(
        pending.lifecycle().state(),
        OnboardingState::RuntimeVerificationPending
    );
    assert_eq!(
        pending.lifecycle().candidate_generation(),
        Some(credential.0)
    );
    assert!(pending.lifecycle().active_generation().is_none());

    let SchwabMarketDoctorRunPreparation::Ready(renewal_lease) = service
        .prepare_schwab_market_doctor_run(session_id, 2, initial_scope, CancellationToken::new())
        .await?
    else {
        return Err("expired pending Schwab doctor was not ready for renewal".into());
    };
    assert_eq!(renewal_lease.session_id(), session_id);
    assert_eq!(renewal_lease.generation(), credential.0);
    assert_eq!(renewal_lease.application_secret_reference(), &credential.1);
    assert_eq!(
        service.schwab_market_doctor_authority_binding(&renewal_lease)?,
        receipt_binding(&prior, session_id, Some(prior.receipt_sha256()))?
    );
    service
        .record_schwab_market_data_doctor_observation(
            &renewal_lease,
            doctor_observation(
                2,
                2,
                initial_scope,
                Duration::from_secs(30 * 60),
                wall_deadline(Duration::from_secs(30 * 60))?,
                &service.schwab_market_doctor_authority_binding(&renewal_lease)?,
            )?,
            CancellationToken::new(),
        )
        .await?;
    let renewed = retained_receipt(&service, session_id, credential.0)?;
    assert_eq!(renewed.predecessor_digest(), Some(prior.receipt_sha256()));
    assert_ne!(renewed.receipt_sha256(), prior.receipt_sha256());
    assert_eq!(renewed.access_token_generation(), 2);
    assert_eq!(renewed.authorization_generation(), 2);
    assert!(renewed.verified_at() >= prior.exclusive_expires_at());

    // A changed effective scope renews immediately while the prior grant receipt is current.
    let replacement_scope = EvidenceDigest::new(DigestAlgorithm::Sha256, [32; 32]);
    let SchwabMarketDoctorRunPreparation::Ready(scope_lease) = service
        .prepare_schwab_market_doctor_run(
            session_id,
            2,
            replacement_scope,
            CancellationToken::new(),
        )
        .await?
    else {
        return Err("changed scope did not renew the current pending doctor immediately".into());
    };
    let scope_binding = service.schwab_market_doctor_authority_binding(&scope_lease)?;
    assert_eq!(
        scope_binding,
        receipt_binding(&renewed, session_id, Some(renewed.receipt_sha256()))?
    );
    service
        .record_schwab_market_data_doctor_observation(
            &scope_lease,
            doctor_observation(
                3,
                2,
                replacement_scope,
                Duration::from_secs(30 * 60),
                renewed.exclusive_expires_at(),
                &scope_binding,
            )?,
            CancellationToken::new(),
        )
        .await?;
    let scope_renewed = retained_receipt(&service, session_id, credential.0)?;
    assert_eq!(
        scope_renewed.predecessor_digest(),
        Some(renewed.receipt_sha256())
    );
    assert_eq!(
        scope_renewed.authorization_scope_sha256(),
        replacement_scope
    );
    assert!(scope_renewed.verified_at() < renewed.exclusive_expires_at());

    // A fresh consent grant also renews immediately without waiting for the old grant deadline.
    let SchwabMarketDoctorRunPreparation::Ready(grant_lease) = service
        .prepare_schwab_market_doctor_run(
            session_id,
            4,
            replacement_scope,
            CancellationToken::new(),
        )
        .await?
    else {
        return Err("changed grant did not renew the current pending doctor immediately".into());
    };
    let grant_binding = service.schwab_market_doctor_authority_binding(&grant_lease)?;
    assert_eq!(
        grant_binding,
        receipt_binding(
            &scope_renewed,
            session_id,
            Some(scope_renewed.receipt_sha256())
        )?
    );
    service
        .record_schwab_market_data_doctor_observation(
            &grant_lease,
            doctor_observation(
                4,
                4,
                replacement_scope,
                Duration::from_secs(30 * 60),
                wall_deadline(Duration::from_secs(30 * 60))?,
                &grant_binding,
            )?,
            CancellationToken::new(),
        )
        .await?;
    let grant_renewed = retained_receipt(&service, session_id, credential.0)?;
    assert_eq!(
        grant_renewed.predecessor_digest(),
        Some(scope_renewed.receipt_sha256())
    );
    assert_eq!(grant_renewed.authorization_generation(), 4);
    assert!(grant_renewed.verified_at() < scope_renewed.exclusive_expires_at());

    drop(service);
    drop(publisher);
    drop(research);
    let (_research, catalog, _publisher) =
        ResearchService::open_or_initialize_with_provider_onboarding(
            &paths,
            catalog_config,
            8,
            ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
        )?;
    let recovered = ProviderOnboardingService::try_new_with_provider_rate(
        catalog,
        secrets,
        provider_rate.clone(),
    )?;
    let resumed = recovered.catalog.resume_provider_onboarding(session_id)?;
    let lifecycle = resumed.lifecycle();
    assert_eq!(resumed.reservation().session_id(), session_id);
    assert_eq!(resumed.reservation().deadline_at(), setup_deadline);
    assert_eq!(
        lifecycle.state(),
        OnboardingState::RuntimeVerificationPending
    );
    assert_eq!(lifecycle.candidate_generation(), Some(credential.0));
    assert_eq!(
        lifecycle.generation_state(credential.0),
        Some(CredentialGenerationState::VerifiedLeastPrivilege)
    );
    assert!(lifecycle.active_generation().is_none());
    assert_eq!(resumed.next_sequence(), sequence_before_renewal + 3);
    assert_eq!(
        recovered.retained_credential_coordinate(session_id)?,
        credential
    );
    assert_eq!(
        retained_receipt(&recovered, session_id, credential.0)?,
        grant_renewed
    );
    assert_eq!(
        recovered.resume(session_id)?.next_action(),
        crate::provider_onboarding::OnboardingNextAction::VerifyAndActivate
    );
    assert!(matches!(
        recovered
            .prepare_schwab_market_doctor_run(
                session_id,
                4,
                replacement_scope,
                CancellationToken::new(),
            )
            .await?,
        SchwabMarketDoctorRunPreparation::Current
    ));
    let config =
        market_squawk_platform::AppConfig::load(market_squawk_platform::ConfigSources::new(
            None,
            &std::collections::BTreeMap::<std::ffi::OsString, std::ffi::OsString>::new(),
            market_squawk_platform::ConfigOverrides {
                data_dir: Some(directory.path().join("account-runtime")),
                ..Default::default()
            },
        ))?;
    let lease = recovered.prepared_activation_lease(session_id)?;
    crate::provider_activation::assert_schwab_prepared_publication_transition(
        Arc::new(recovered),
        lease,
        &config,
        provider_rate,
    )
    .await?;
    Ok(())
}

fn retained_receipt(
    service: &ProviderOnboardingService,
    session_id: Uuid,
    generation: SecretGeneration,
) -> TestResult<SchwabMarketDataDoctorReceiptV1> {
    service
        .catalog
        .resume_provider_onboarding(session_id)?
        .lifecycle()
        .generation_runtime_evidence(generation)
        .and_then(RuntimeVerificationEvidence::schwab_market_data_receipt)
        .cloned()
        .ok_or_else(|| "missing retained Schwab doctor receipt".into())
}

fn receipt_binding(
    receipt: &SchwabMarketDataDoctorReceiptV1,
    session_id: Uuid,
    predecessor: Option<EvidenceDigest>,
) -> TestResult<SchwabMarketDoctorAuthorityBinding> {
    Ok(SchwabMarketDoctorAuthorityBinding::try_new(
        receipt.surface_id().clone(),
        session_id,
        receipt.application_credential_generation(),
        receipt.application_credential_reference_sha256(),
        receipt.capability_revision(),
        receipt.capability_digest(),
        receipt.public_configuration_digest(),
        receipt.rights_decision_digest(),
        receipt.rate_policy_digest(),
        predecessor,
    )?)
}

fn doctor_observation(
    token_generation: u64,
    authorization_generation: u64,
    authorization_scope_sha256: EvidenceDigest,
    access_lifetime: Duration,
    refresh_expires_at: Timestamp,
    binding: &SchwabMarketDoctorAuthorityBinding,
) -> TestResult<SchwabMarketDataDoctorObservation> {
    use SchwabMarketDataFamily::*;

    let completed_at = system_timestamp()?;
    let digest = |value| EvidenceDigest::new(DigestAlgorithm::Sha256, [value; 32]);
    let mut families = [
        Quotes,
        PriceHistory,
        OptionChains,
        ExpirationChains,
        Movers,
        MarketHours,
        Instruments,
        LevelOneEquities,
        LevelOneOptions,
        LevelOneFutures,
        LevelOneFuturesOptions,
        LevelOneForex,
        NyseBook,
        NasdaqBook,
        OptionsBook,
        ChartEquity,
        ChartFutures,
        ScreenerEquity,
        ScreenerOption,
    ]
    .into_iter()
    .map(|family| {
        let available = family == Quotes;
        SchwabMarketDataFamilyEvidence {
            family,
            disposition: if available {
                RuntimeCapabilityDisposition::Available
            } else {
                RuntimeCapabilityDisposition::NotProbed
            },
            disposition_evidence_sha256: digest(1),
            observation_sha256: available.then(|| digest(2)),
            observed_at: available.then_some(completed_at),
        }
    })
    .collect::<Vec<_>>()
    .into_boxed_slice();
    // Use the real disposition constructor: unselected is neither a fabricated response nor
    // an unavailable entitlement, and must coexist with verified quote access after reopen.
    *families
        .iter_mut()
        .find(|family| family.family == LevelOneFuturesOptions)
        .ok_or("missing futures-options fixture")? =
        crate::provider_onboarding::schwab_market_doctor::family_receipt_evidence(
            binding,
            digest(21),
            LevelOneFuturesOptions,
            None,
        )?;
    Ok(SchwabMarketDataDoctorObservation {
        provider_observation_origin: SchwabMarketDataDoctorObservation::provider_observed_origin()?,
        access_token_generation: token_generation,
        authorization_generation,
        authorization_scope_sha256,
        access_issued_at: completed_at,
        access_expires_at: Timestamp::from_unix_nanos(
            completed_at.unix_nanos() + i64::try_from(access_lifetime.as_nanos())?,
        ),
        refresh_authorized_at: Timestamp::from_unix_nanos(
            refresh_expires_at.unix_nanos() - 7 * 24 * 60 * 60 * 1_000_000_000,
        ),
        refresh_expires_at,
        user_preference: SchwabUserPreferenceDoctorEvidence {
            endpoint_contract_sha256: crate::provider_onboarding::schwab_market_doctor::user_preference_endpoint_contract_sha256(),
            request_sha256: digest(3),
            response_sha256: digest(4),
            status_code: 200,
            response_bytes: 1,
            received_at: completed_at,
            latency_nanos: 1,
            market_data_principal_sha256: digest(5),
            streamer_bootstrap_sha256: digest(6),
            market_data_offer_sha256: None,
        },
        quote_delay: Some(CoverageDelay::RealTime),
        families,
        completed_at,
    })
}
