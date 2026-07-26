//! Transport-neutral provider onboarding application service.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};
use std::thread::JoinHandle as ThreadJoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt as _;
use market_squawk_adapter_coinbase::CoinbaseDirectHmacSigner;
use market_squawk_adapter_treasury::{
    DailyParYieldCurvePage, FiscalDataParseLimits, TreasuryYieldCurveProfile,
};
use market_squawk_data::{
    CatalogError, CatalogLimit, OnboardingCatalogCapability, OnboardingReservation,
    OnboardingReservationRequest, ResumedProviderOnboarding,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::{
    EncryptedFileFallbackStatus, EncryptedFileUnlockCapability, LocalSecretStoreError,
    SecretCancellation, SecretDeletionDisposition, SecretGeneration, SecretInteractionPolicy,
    SecretKey, SecretMutationEffect, SecretOperationControl, SecretReconciliationObservation,
    SecretStore, SecretValue,
};
use market_squawk_sources::{
    AuthorityBindings, AuthorityVerification, AuthorityVerificationInput,
    CapabilityRegistrationOutcome, CredentialGenerationState, OnboardingEvent, OnboardingState,
    ProbeTransport, ProfileReleaseState, ProviderOnboardingProfile, ProviderProfileError,
    ProviderProfileRegistry, ProviderPublicConfiguration, ProviderRateAuthority,
    ProviderRateDeclaration, SecretStoreClearOutcome, TREASURY_DAILY_RATES_PROBE_YEAR,
    built_in_provider_profiles, install_ring_tls_provider,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{
    Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard, OwnedSemaphorePermit, Semaphore, oneshot,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::contracts::{
    OnboardingSessionView, ProviderActivationLease, ProviderActivationLeaseInput,
    ProviderProfileRegistration, ProviderProfileView, session_view,
};

const SESSION_DURATION: Duration = Duration::from_secs(15 * 60);
const SECRET_OPERATION_DURATION: Duration = Duration::from_secs(30);
const MAXIMUM_CONCURRENT_SECRET_OPERATIONS: usize = 1;
const MAXIMUM_PENDING_SECRET_REAPS: usize = 64;
const _: () = assert!(MAXIMUM_PENDING_SECRET_REAPS <= Semaphore::MAX_PERMITS);
const MAX_PROBE_BODY_BYTES: usize = 1024 * 1024;
const MAX_CONTACT_BYTES: usize = 128;
const BLS_REGISTRATION_VALIDITY_NANOS: i64 = 365 * 86_400 * 1_000_000_000;
const COINBASE_DIRECT_VERIFICATION_VALIDITY_NANOS: i64 = 15 * 60 * 1_000_000_000;
const COINBASE_ACCOUNT_BINDING_DOMAIN: &[u8] =
    b"market-squawk/coinbase-exchange-account-binding/v1\0";
static SECRET_OPERATION_REAPER: LazyLock<SecretOperationReaper> =
    LazyLock::new(SecretOperationReaper::start);

mod lifecycle_runtime;
mod rate_runtime;

use rate_runtime::{ProbeRateAuthority, ProbeRatePermit};

struct SecretOperationTask<T: Send + 'static> {
    cancellation: SecretCancellation,
    command: Option<SecretReapCommand>,
    result: oneshot::Receiver<Result<T, ProviderOnboardingError>>,
}

struct SecretReapCommand {
    worker: JoinHandle<()>,
    _capacity: OwnedSemaphorePermit,
}

struct SecretOperationReaper {
    sender: Option<SyncSender<SecretReapCommand>>,
    capacity: Arc<Semaphore>,
    _thread: Option<ThreadJoinHandle<()>>,
}

/// Bounded request to start one exact code-owned profile.
#[derive(Clone, Debug)]
pub struct StartOnboardingRequest {
    surface_id: String,
    organization: Option<String>,
    administrative_email: Option<String>,
}

impl StartOnboardingRequest {
    /// Validates a profile identity and optional SEC declared-contact fields.
    pub fn try_new(
        surface_id: impl Into<String>,
        organization: Option<String>,
        administrative_email: Option<String>,
    ) -> Result<Self, ProviderOnboardingError> {
        let request = Self {
            surface_id: surface_id.into(),
            organization,
            administrative_email,
        };
        if request.surface_id.is_empty()
            || request.surface_id.len() > 128
            || request.surface_id.chars().any(char::is_control)
            || !valid_optional_contact(request.organization.as_deref(), false)
            || !valid_optional_contact(request.administrative_email.as_deref(), true)
            || request.organization.is_some() != request.administrative_email.is_some()
        {
            return Err(ProviderOnboardingError::InvalidRequest);
        }
        Ok(request)
    }

    fn declared_user_agent(&self) -> Option<String> {
        self.organization
            .as_ref()
            .zip(self.administrative_email.as_ref())
            .map(|(organization, email)| format!("{organization} {email}"))
    }
}

/// Provider onboarding composition over catalog and exact-generation secret authority.
pub struct ProviderOnboardingService {
    profiles: ProviderProfileRegistry,
    catalog: OnboardingCatalogCapability,
    secrets: Arc<dyn SecretStore>,
    client: reqwest::Client,
    probe_rates: ProbeRateAuthority,
    activation: AsyncMutex<()>,
    secret_operations: Arc<Semaphore>,
}

/// Borrowed serialization authority for exact onboarding/runtime mutation.
///
/// Construction is private to [`ProviderOnboardingService`]. The adapter activation boundary
/// retains this guard across currentness validation and the corresponding sealed runtime mutation,
/// preventing a lease transition from racing the source-map change.
pub(crate) struct ProviderOnboardingMutationAuthority<'a> {
    service: &'a ProviderOnboardingService,
    _guard: AsyncMutexGuard<'a, ()>,
}

/// Exact durable runtime-session authority admitted during startup reconciliation.
#[derive(Debug, Default)]
pub(crate) struct ProviderRuntimeStartupAdmissions {
    sessions: BTreeMap<SourceIdentifier, BTreeSet<Uuid>>,
}

impl ProviderRuntimeStartupAdmissions {
    pub(crate) fn try_new(
        entries: impl IntoIterator<Item = (SourceIdentifier, Uuid)>,
    ) -> Result<Self, ProviderOnboardingError> {
        let mut sessions: BTreeMap<SourceIdentifier, BTreeSet<Uuid>> = BTreeMap::new();
        for (surface_id, session_id) in entries {
            if session_id.is_nil() || !sessions.entry(surface_id).or_default().insert(session_id) {
                return Err(ProviderOnboardingError::InvalidSessionState);
            }
        }
        Ok(Self { sessions })
    }

    fn admits(&self, surface_id: &SourceIdentifier, session_id: Uuid) -> bool {
        self.sessions
            .get(surface_id)
            .is_some_and(|sessions| sessions.contains(&session_id))
    }
}

impl ProviderOnboardingService {
    pub(crate) async fn acquire_runtime_mutation_authority(
        &self,
    ) -> ProviderOnboardingMutationAuthority<'_> {
        ProviderOnboardingMutationAuthority {
            service: self,
            _guard: self.activation.lock().await,
        }
    }

    pub(crate) fn try_acquire_runtime_mutation_authority(
        &self,
    ) -> Result<ProviderOnboardingMutationAuthority<'_>, ProviderOnboardingError> {
        Ok(ProviderOnboardingMutationAuthority {
            service: self,
            _guard: self
                .activation
                .try_lock()
                .map_err(|_error| ProviderOnboardingError::ActivationUnavailable)?,
        })
    }

    pub(crate) fn prepared_activation_lease(
        &self,
        session_id: Uuid,
    ) -> Result<ProviderActivationLease, ProviderOnboardingError> {
        let resumed = self.catalog.resume_provider_onboarding(session_id)?;
        let profile = self.current_profile_for(&resumed)?;
        self.prepared_lease_from_resumed(&resumed, profile)
    }

    pub(crate) fn discard_prepared_activation_at_startup(
        &self,
        prepared: &ProviderActivationLease,
        evidence_digest: EvidenceDigest,
    ) -> Result<(), ProviderOnboardingError> {
        self.try_acquire_runtime_mutation_authority()?
            .discard_prepared_activation_at_startup(prepared, evidence_digest)
    }

    /// Constructs the production service with one product-wide durable provider-rate authority.
    ///
    /// # Errors
    ///
    /// Fails closed when profiles, TLS, durable rate admission, or startup reconciliation cannot
    /// be established.
    pub fn try_new_with_provider_rate<S>(
        catalog: OnboardingCatalogCapability,
        secrets: Arc<S>,
        provider_rate: ProviderRateAuthority,
    ) -> Result<Self, ProviderOnboardingError>
    where
        S: SecretStore + 'static,
    {
        Self::try_new_inner(
            catalog,
            secrets,
            provider_rate,
            ProviderRuntimeStartupAdmissions::default(),
        )
    }

    pub(crate) fn try_new_with_provider_rate_and_runtime_admissions<S>(
        catalog: OnboardingCatalogCapability,
        secrets: Arc<S>,
        provider_rate: ProviderRateAuthority,
        runtime_admissions: ProviderRuntimeStartupAdmissions,
    ) -> Result<Self, ProviderOnboardingError>
    where
        S: SecretStore + 'static,
    {
        Self::try_new_inner(catalog, secrets, provider_rate, runtime_admissions)
    }

    fn try_new_inner<S>(
        catalog: OnboardingCatalogCapability,
        secrets: Arc<S>,
        provider_rate: ProviderRateAuthority,
        runtime_admissions: ProviderRuntimeStartupAdmissions,
    ) -> Result<Self, ProviderOnboardingError>
    where
        S: SecretStore + 'static,
    {
        let _tls = install_ring_tls_provider()?;
        let client = reqwest::Client::builder()
            .https_only(true)
            .tls_backend_rustls()
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .retry(reqwest::retry::never())
            .referer(false)
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(Duration::from_secs(5))
            .read_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(10))
            .user_agent("market-squawk/0.1 provider-onboarding")
            .build()
            .map_err(|_| ProviderOnboardingError::ClientConfiguration)?;
        let profiles = built_in_provider_profiles()?;
        let probe_rates = ProbeRateAuthority::try_new_with_provider_rate(&profiles, provider_rate)?;
        let service = Self {
            profiles,
            catalog,
            secrets,
            client,
            probe_rates,
            activation: AsyncMutex::new(()),
            secret_operations: Arc::new(Semaphore::new(MAXIMUM_CONCURRENT_SECRET_OPERATIONS)),
        };
        for profile in service.profiles.iter() {
            let _registration = service.register_profile_capabilities(profile)?;
        }
        service.reconcile_startup(CatalogLimit::new(32)?, &runtime_admissions)?;
        Ok(service)
    }

    /// Returns every built-in profile in stable identity order.
    pub fn profiles(&self) -> Vec<ProviderProfileView> {
        self.profiles.iter().map(Into::into).collect()
    }

    /// Returns non-secret encrypted-fallback readiness without probing or mutating a backend.
    pub fn encrypted_file_fallback_status(
        &self,
    ) -> Result<EncryptedFileFallbackStatus, ProviderOnboardingError> {
        self.secrets
            .encrypted_file_fallback_status()
            .map_err(Into::into)
    }

    /// Consumes an explicit foreground unlock through the single-flight secret executor.
    pub async fn unlock_encrypted_file_fallback(
        &self,
        unlock: SecretValue,
        cancellation: CancellationToken,
    ) -> Result<EncryptedFileFallbackStatus, ProviderOnboardingError> {
        let secrets = Arc::clone(&self.secrets);
        await_blocking_secret_operation(
            Arc::clone(&self.secret_operations),
            cancellation,
            move |operation| {
                let control = secret_fallback_control("provider-fallback-unlock", operation)?;
                secrets
                    .unlock_encrypted_file_fallback(
                        EncryptedFileUnlockCapability::new(unlock),
                        &control,
                    )
                    .map_err(Into::into)
            },
        )
        .await
    }

    /// Drops the process-held fallback unlock through the single-flight secret executor.
    pub async fn lock_encrypted_file_fallback(
        &self,
        cancellation: CancellationToken,
    ) -> Result<EncryptedFileFallbackStatus, ProviderOnboardingError> {
        let secrets = Arc::clone(&self.secrets);
        await_blocking_secret_operation(
            Arc::clone(&self.secret_operations),
            cancellation,
            move |operation| {
                let control = secret_fallback_control("provider-fallback-lock", operation)?;
                secrets
                    .lock_encrypted_file_fallback(&control)
                    .map_err(Into::into)
            },
        )
        .await
    }

    /// Idempotently registers one exact code-owned capability without starting setup.
    pub fn register_profile(
        &self,
        surface_id: &str,
    ) -> Result<ProviderProfileRegistration, ProviderOnboardingError> {
        let profile = self
            .profiles
            .get(surface_id)
            .ok_or(ProviderOnboardingError::UnknownProfile)?;
        let outcome = self.register_profile_capabilities(profile)?;
        Ok(ProviderProfileRegistration::new(profile.into(), outcome))
    }

    /// Returns newest-first secret-free durable sessions within the requested catalog bound.
    pub fn sessions(
        &self,
        limit: CatalogLimit,
    ) -> Result<Vec<OnboardingSessionView>, ProviderOnboardingError> {
        let sessions = self.catalog.provider_onboarding_sessions(limit)?;
        self.session_views(sessions)
    }

    /// Returns the latest secret-free durable session for each surface in canonical order.
    pub fn current_sessions(
        &self,
        limit: CatalogLimit,
    ) -> Result<Vec<OnboardingSessionView>, ProviderOnboardingError> {
        let sessions = self.catalog.current_provider_onboarding_sessions(limit)?;
        sessions
            .into_iter()
            .map(|session| self.resume(session.reservation().session_id()))
            .collect()
    }

    /// Starts a durable session and completes every safe automatic step.
    pub async fn start(
        &self,
        request: StartOnboardingRequest,
        cancellation: CancellationToken,
    ) -> Result<OnboardingSessionView, ProviderOnboardingError> {
        let profile = self
            .profiles
            .get(&request.surface_id)
            .ok_or(ProviderOnboardingError::UnknownProfile)?;
        validate_declared_contact(profile, &request)?;
        let public_configuration = provider_public_configuration(profile, &request)?;
        self.register_profile_capabilities(profile)?;
        let deadline_at = wall_deadline(SESSION_DURATION)?;
        let operation_id = Uuid::new_v4();
        let reservation_request = OnboardingReservationRequest::try_new(
            profile.capability(),
            public_configuration,
            profile.capability().maximum_authority().clone(),
            SourceIdentifier::try_from("local-portal-user")?,
            SourceIdentifier::try_from(format!("provider-onboarding-{operation_id}"))?,
            deadline_at,
            0,
        )?;
        let reservation = self
            .catalog
            .reserve_provider_onboarding(&reservation_request)?;

        match profile.release_state() {
            ProfileReleaseState::RightsBlocked => {}
            ProfileReleaseState::RefreshRequired
                if profile.capability().setup_mode()
                    == market_squawk_sources::SetupMode::NoCredential =>
            {
                self.append(
                    &reservation,
                    1,
                    OnboardingEvent::RefreshRequired {
                        evidence_digest: event_digest(
                            b"refresh-required",
                            reservation.session_id(),
                            None,
                        ),
                    },
                )?;
            }
            ProfileReleaseState::RefreshRequired => {}
            ProfileReleaseState::Available | ProfileReleaseState::RightsLimited
                if profile.capability().setup_mode()
                    == market_squawk_sources::SetupMode::NoCredential =>
            {
                let declared_user_agent = if profile.id() == "sec.edgar-public" {
                    request.declared_user_agent()
                } else {
                    None
                };
                self.activate_anonymous(
                    profile,
                    &reservation,
                    declared_user_agent.as_deref(),
                    cancellation,
                )
                .await?;
            }
            ProfileReleaseState::Available | ProfileReleaseState::RightsLimited => {}
        }
        self.resume(reservation.session_id())
    }

    /// Imports one secret through the bounded blocking-operation executor.
    pub async fn submit_secret(
        self: &Arc<Self>,
        session_id: Uuid,
        secret: SecretValue,
        cancellation: CancellationToken,
    ) -> Result<OnboardingSessionView, ProviderOnboardingError> {
        let service = Arc::clone(self);
        await_blocking_secret_operation(
            Arc::clone(&self.secret_operations),
            cancellation,
            move |operation| service.submit_secret_blocking(session_id, secret, operation),
        )
        .await
    }

    fn submit_secret_blocking(
        &self,
        session_id: Uuid,
        secret: SecretValue,
        cancellation: SecretCancellation,
    ) -> Result<OnboardingSessionView, ProviderOnboardingError> {
        let mut resumed = self.catalog.resume_provider_onboarding(session_id)?;
        let profile = self.current_profile_for(&resumed)?;
        if profile.release_state() == ProfileReleaseState::RightsBlocked
            || !matches!(
                resumed.lifecycle().state(),
                OnboardingState::UserActionRequired
                    | OnboardingState::CredentialImportedUnverified
                    | OnboardingState::RotationPending
                    | OnboardingState::SecretReconciliationRequired
                    | OnboardingState::CleanupRequired
            )
        {
            return Err(ProviderOnboardingError::SecretImportUnavailable);
        }
        validate_secret_shape(profile, &secret)?;
        let generation = resumed
            .lifecycle()
            .candidate_generation()
            .ok_or(ProviderOnboardingError::InvalidSessionState)?;
        let secret_key = SecretKey::try_new(
            "provider-onboarding",
            &format!("{}.{}", profile.id(), session_id.simple()),
        )?;
        let deadline = Instant::now()
            .checked_add(SECRET_OPERATION_DURATION)
            .ok_or(ProviderOnboardingError::Clock)?;
        let control = SecretOperationControl::try_new(
            format!("provider-onboarding-{session_id}"),
            deadline,
            0,
            SecretInteractionPolicy::AllowPlatformPrompt,
            cancellation,
        )?;
        if let Some(plan) = resumed
            .lifecycle()
            .generation_store_plan(generation)
            .cloned()
        {
            match self
                .secrets
                .matches_planned(&secret_key, &plan, &secret, &control)?
            {
                SecretReconciliationObservation::Matches => {
                    self.append(
                        resumed.reservation(),
                        resumed.next_sequence(),
                        OnboardingEvent::CredentialStored {
                            reference: plan.target().clone(),
                        },
                    )?;
                    return self.resume(session_id);
                }
                SecretReconciliationObservation::Absent => {
                    self.append(
                        resumed.reservation(),
                        resumed.next_sequence(),
                        OnboardingEvent::SecretStoreCleared {
                            generation,
                            reference: plan.target().clone(),
                            outcome: SecretStoreClearOutcome::Absent,
                        },
                    )?;
                }
                SecretReconciliationObservation::Mismatch
                | SecretReconciliationObservation::PresentUnverified => {
                    let deletion = self.secrets.delete_planned(&secret_key, &plan, &control);
                    match deletion {
                        Ok(disposition) => {
                            self.append(
                                resumed.reservation(),
                                resumed.next_sequence(),
                                OnboardingEvent::SecretStoreCleared {
                                    generation,
                                    reference: plan.target().clone(),
                                    outcome: match disposition {
                                        SecretDeletionDisposition::Deleted => {
                                            SecretStoreClearOutcome::Deleted
                                        }
                                        SecretDeletionDisposition::AlreadyAbsent => {
                                            SecretStoreClearOutcome::Absent
                                        }
                                    },
                                },
                            )?;
                        }
                        Err(failure) => {
                            let error = failure.into_error();
                            self.append(
                                resumed.reservation(),
                                resumed.next_sequence(),
                                OnboardingEvent::CleanupRequired {
                                    generation: Some(generation),
                                    evidence_digest: event_digest(
                                        b"planned-secret-delete-indeterminate",
                                        session_id,
                                        Some(generation),
                                    ),
                                },
                            )?;
                            return Err(error.into());
                        }
                    }
                }
            }
            resumed = self.catalog.resume_provider_onboarding(session_id)?;
        }
        if resumed.lifecycle().generation_state(generation)
            != Some(CredentialGenerationState::Reserved)
        {
            return Err(ProviderOnboardingError::SecretImportUnavailable);
        }
        let plan = if let Some(active_generation) = resumed.lifecycle().active_generation() {
            let current = resumed
                .lifecycle()
                .generation_reference(active_generation)
                .ok_or(ProviderOnboardingError::InvalidSessionState)?;
            self.secrets
                .plan_replace(&secret_key, current, generation, &control)?
        } else {
            self.secrets
                .plan_create(&secret_key, generation, &control)?
        };
        self.append(
            resumed.reservation(),
            resumed.next_sequence(),
            OnboardingEvent::SecretStorePlanned {
                plan: plan.clone(),
                evidence_digest: event_digest(
                    b"secret-store-planned",
                    session_id,
                    Some(generation),
                ),
            },
        )?;
        let stored = self
            .secrets
            .execute_planned(&secret_key, &plan, secret, &control);
        match stored {
            Ok(_disposition) => {
                let resumed = self.catalog.resume_provider_onboarding(session_id)?;
                self.append(
                    resumed.reservation(),
                    resumed.next_sequence(),
                    OnboardingEvent::CredentialStored {
                        reference: plan.target().clone(),
                    },
                )?;
                self.resume(session_id)
            }
            Err(failure) => {
                let effect = failure.effect();
                let error = failure.into_error();
                let resumed = self.catalog.resume_provider_onboarding(session_id)?;
                let event = if effect == SecretMutationEffect::MayHaveApplied
                    || !matches!(
                        self.secrets.inspect_planned(&secret_key, &plan, &control),
                        Ok(SecretReconciliationObservation::Absent)
                    ) {
                    OnboardingEvent::SecretStoreReconciliationRequired {
                        generation,
                        evidence_digest: event_digest(
                            b"secret-store-reconciliation-required",
                            session_id,
                            Some(generation),
                        ),
                    }
                } else {
                    OnboardingEvent::SecretStoreCleared {
                        generation,
                        reference: plan.target().clone(),
                        outcome: SecretStoreClearOutcome::Absent,
                    }
                };
                self.append(resumed.reservation(), resumed.next_sequence(), event)?;
                Err(error.into())
            }
        }
    }

    /// Selects and verifies the exact lease that a new runtime publication must target.
    ///
    /// A prepared replacement generation always takes precedence over the still-active predecessor
    /// in the same session. The active lease is returned only when no replacement candidate
    /// exists, which keeps predecessor authority available for rollback without readmitting it as
    /// the requested successor. Repeated calls after activation recover the same durable
    /// generation without repeating the provider request.
    pub(crate) async fn prepare_runtime_activation_target(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<ProviderActivationLease, ProviderOnboardingError> {
        let _activation = self.activation.lock().await;
        loop {
            if cancellation.is_cancelled() {
                return Err(ProviderOnboardingError::OperationCancelled);
            }
            let resumed = self.catalog.resume_provider_onboarding(session_id)?;
            let profile = self.current_profile_for(&resumed)?;
            if resumed
                .lifecycle()
                .active_generation()
                .is_some_and(|generation| {
                    resumed.lifecycle().generation_is_active_scoped(generation)
                })
                && resumed.lifecycle().candidate_generation().is_none()
            {
                return self.lease_from_resumed(&resumed, profile);
            }
            match profile.release_state() {
                ProfileReleaseState::RightsBlocked => {
                    return Err(ProviderOnboardingError::RightsBlocked);
                }
                ProfileReleaseState::RefreshRequired => {
                    return Err(ProviderOnboardingError::EvidenceRefreshRequired);
                }
                ProfileReleaseState::Available | ProfileReleaseState::RightsLimited => {}
            }
            if resumed.lifecycle().state() == OnboardingState::RuntimeVerificationPending
                && resumed.lifecycle().candidate_generation().is_none()
                && resumed.lifecycle().anonymous_rights_digest()
                    == Some(profile.rights_decision_digest())
                && resumed.lifecycle().anonymous_rate_policy_digest()
                    == Some(profile.capability().rate_policy().evidence_digest())
                && resumed.lifecycle().anonymous_runtime_digest().is_some()
            {
                return self.prepared_lease_from_resumed(&resumed, profile);
            }
            let generation = resumed
                .lifecycle()
                .candidate_generation()
                .ok_or(ProviderOnboardingError::InvalidSessionState)?;
            match resumed.lifecycle().generation_state(generation) {
                Some(CredentialGenerationState::StoredUnverified) => {
                    let reference = resumed
                        .lifecycle()
                        .generation_reference(generation)
                        .cloned()
                        .ok_or(ProviderOnboardingError::InvalidSessionState)?;
                    let secrets = Arc::clone(&self.secrets);
                    let secret = await_blocking_secret_operation(
                        Arc::clone(&self.secret_operations),
                        cancellation.clone(),
                        move |operation| {
                            read_secret_reference(
                                secrets.as_ref(),
                                session_id,
                                &reference,
                                operation,
                                SecretInteractionPolicy::AllowPlatformPrompt,
                            )
                        },
                    )
                    .await?;
                    let probe_evidence = self
                        .run_credential_probe(profile, &secret, cancellation.clone())
                        .await?;
                    let verified_at = system_timestamp()?;
                    let verification_validity_nanos = match profile.id() {
                        "bls.v2-registered" => Some(BLS_REGISTRATION_VALIDITY_NANOS),
                        "coinbase.exchange-direct-market-data" => {
                            Some(COINBASE_DIRECT_VERIFICATION_VALIDITY_NANOS)
                        }
                        _ => None,
                    };
                    let verification_expires_at = verification_validity_nanos
                        .map(|validity| {
                            verified_at
                                .unix_nanos()
                                .checked_add(validity)
                                .map(Timestamp::from_unix_nanos)
                                .ok_or(ProviderOnboardingError::Clock)
                        })
                        .transpose()?;
                    let requested = resumed.lifecycle().requested_authority().clone();
                    let verification = AuthorityVerification::try_new(
                        profile.capability(),
                        AuthorityVerificationInput {
                            requested: requested.clone(),
                            observed: requested,
                            restrictions_digest: profile.rights_decision_digest(),
                            bindings: AuthorityBindings::new(
                                None,
                                None,
                                None,
                                probe_evidence
                                    .account_digest
                                    .or(Some(resumed.reservation().public_configuration_digest())),
                            ),
                            verified_at,
                            expires_at: verification_expires_at,
                            verifier_revision: profile.capability().verifier_revision().clone(),
                            assurance_limitation: credential_assurance(profile)?,
                            evidence_digest: probe_evidence.response_digest,
                        },
                    )
                    .map_err(|_| ProviderOnboardingError::InvalidSessionState)?;
                    self.append(
                        resumed.reservation(),
                        resumed.next_sequence(),
                        OnboardingEvent::AuthorityVerified {
                            verification: Box::new(verification),
                        },
                    )?;
                }
                Some(CredentialGenerationState::VerifiedLeastPrivilege) => {
                    let verification = resumed
                        .lifecycle()
                        .generation_verification(generation)
                        .ok_or(ProviderOnboardingError::InvalidSessionState)?;
                    if resumed
                        .lifecycle()
                        .generation_rights_digest(generation)
                        .is_none()
                    {
                        self.append(
                            resumed.reservation(),
                            resumed.next_sequence(),
                            OnboardingEvent::RightsAdmitted {
                                generation: Some(generation),
                                decision_digest: profile.rights_decision_digest(),
                            },
                        )?;
                    } else if resumed
                        .lifecycle()
                        .generation_rate_policy_digest(generation)
                        .is_none()
                    {
                        self.append(
                            resumed.reservation(),
                            resumed.next_sequence(),
                            OnboardingEvent::RatePolicyAdmitted {
                                generation: Some(generation),
                                policy_digest: profile.capability().rate_policy().evidence_digest(),
                            },
                        )?;
                    } else if resumed
                        .lifecycle()
                        .generation_runtime_digest(generation)
                        .is_none()
                    {
                        self.append(
                            resumed.reservation(),
                            resumed.next_sequence(),
                            OnboardingEvent::RuntimeVerified {
                                generation: Some(generation),
                                evidence_digest: derived_evidence_digest(
                                    b"credential-runtime",
                                    session_id,
                                    generation,
                                    verification.evidence_digest(),
                                ),
                            },
                        )?;
                    } else {
                        return self.prepared_lease_from_resumed(&resumed, profile);
                    }
                }
                Some(
                    CredentialGenerationState::Reserved
                    | CredentialGenerationState::StorePlanned
                    | CredentialGenerationState::StoreReconciliationRequired
                    | CredentialGenerationState::ActiveScoped
                    | CredentialGenerationState::SupersededRetained
                    | CredentialGenerationState::Retired
                    | CredentialGenerationState::Tombstoned
                    | CredentialGenerationState::AbandonedNoEffect
                    | CredentialGenerationState::CleanupRequired,
                )
                | None => return Err(ProviderOnboardingError::ActivationUnavailable),
            }
        }
    }

    /// Commits only the exact prepared lease after application-owned runtime staging succeeds.
    pub(crate) async fn commit_prepared_activation(
        &self,
        prepared: &ProviderActivationLease,
    ) -> Result<ProviderActivationLease, ProviderOnboardingError> {
        self.acquire_runtime_mutation_authority()
            .await
            .commit_prepared_activation(prepared)
    }

    fn commit_prepared_activation_locked(
        &self,
        prepared: &ProviderActivationLease,
    ) -> Result<ProviderActivationLease, ProviderOnboardingError> {
        let resumed = self
            .catalog
            .resume_provider_onboarding(prepared.session_id())?;
        let profile = self.current_profile_for(&resumed)?;
        match self.lease_from_resumed(&resumed, profile) {
            Ok(active) if require_same_active_lease(&active, prepared).is_ok() => {
                return Ok(active);
            }
            Ok(_) | Err(ProviderOnboardingError::ActivationUnavailable) => {}
            Err(error) => return Err(error),
        }
        let current = self.prepared_lease_from_resumed(&resumed, profile)?;
        require_same_active_lease(&current, prepared)?;
        let event = prepared_activation_event(
            resumed.lifecycle().active_generation(),
            prepared.generation(),
        )?;
        self.append(resumed.reservation(), resumed.next_sequence(), event)?;
        self.activation_lease(prepared.session_id())
    }

    /// Compatibility composition for non-research callers that do not stage a callable runtime.
    pub(crate) async fn activate(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<ProviderActivationLease, ProviderOnboardingError> {
        let prepared = self
            .prepare_runtime_activation_target(session_id, cancellation)
            .await?;
        self.commit_prepared_activation(&prepared).await
    }

    /// Recovers immutable adapter-construction authority for an active durable session.
    pub fn activation_lease(
        &self,
        session_id: Uuid,
    ) -> Result<ProviderActivationLease, ProviderOnboardingError> {
        let resumed = self.catalog.resume_provider_onboarding(session_id)?;
        let profile = self.current_profile_for(&resumed)?;
        self.lease_from_resumed(&resumed, profile)
    }

    /// Disables one retained adapter recipe whose authority can no longer be reconstructed.
    ///
    /// The evidence digest names the exact quarantined durable state. Other provider sessions and
    /// product domains remain available, while this session requires a new onboarding activation.
    pub(crate) fn invalidate_activation_recipe(
        &self,
        session_id: Uuid,
        evidence_digest: EvidenceDigest,
    ) -> Result<OnboardingSessionView, ProviderOnboardingError> {
        if evidence_digest.bytes() == [0; 32] {
            return Err(ProviderOnboardingError::InvalidSessionState);
        }
        let mut resumed = self.catalog.resume_provider_onboarding(session_id)?;
        let profile = self.profile_for(&resumed)?;
        let lifecycle = resumed.lifecycle();
        let quarantine_established = matches!(
            lifecycle.state(),
            OnboardingState::Blocked | OnboardingState::CleanupRequired
        ) && lifecycle.active_generation().is_none()
            && lifecycle.candidate_generation().is_none()
            && lifecycle.generation_states().all(|(_generation, state)| {
                matches!(
                    state,
                    CredentialGenerationState::CleanupRequired
                        | CredentialGenerationState::Retired
                        | CredentialGenerationState::Tombstoned
                        | CredentialGenerationState::AbandonedNoEffect
                )
            });
        if !quarantine_established {
            self.append(
                resumed.reservation(),
                resumed.next_sequence(),
                OnboardingEvent::ActivationQuarantined { evidence_digest },
            )?;
            resumed = self.catalog.resume_provider_onboarding(session_id)?;
        }
        Ok(session_view(profile, &resumed))
    }

    /// Returns whether one exact session has durably lost all activation authority.
    #[cfg(test)]
    pub(crate) fn activation_recipe_is_invalidated(
        &self,
        session_id: Uuid,
    ) -> Result<bool, ProviderOnboardingError> {
        let resumed = self.catalog.resume_provider_onboarding(session_id)?;
        let _profile = self.profile_for(&resumed)?;
        let lifecycle = resumed.lifecycle();
        Ok(matches!(
            lifecycle.state(),
            OnboardingState::Blocked | OnboardingState::CleanupRequired
        ) && lifecycle.active_generation().is_none()
            && lifecycle.candidate_generation().is_none()
            && lifecycle.generation_states().all(|(_generation, state)| {
                matches!(
                    state,
                    CredentialGenerationState::CleanupRequired
                        | CredentialGenerationState::Retired
                        | CredentialGenerationState::Tombstoned
                        | CredentialGenerationState::AbandonedNoEffect
                )
            }))
    }

    /// Revokes durable authority and completes deterministic local credential cleanup.
    pub(crate) async fn cancel(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<OnboardingSessionView, ProviderOnboardingError> {
        let _activation = self.activation.lock().await;
        loop {
            if cancellation.is_cancelled() {
                return Err(ProviderOnboardingError::OperationCancelled);
            }
            let resumed = self.catalog.resume_provider_onboarding(session_id)?;
            let lifecycle = resumed.lifecycle();
            let target = lifecycle.generation_states().find(|(_generation, state)| {
                !matches!(
                    state,
                    CredentialGenerationState::Tombstoned
                        | CredentialGenerationState::AbandonedNoEffect
                )
            });
            let Some((generation, state)) = target else {
                if lifecycle.cancellation_recorded() {
                    return self.resume(session_id);
                }
                self.append(
                    resumed.reservation(),
                    resumed.next_sequence(),
                    OnboardingEvent::Cancelled {
                        evidence_digest: event_digest(b"user-cancelled", session_id, None),
                    },
                )?;
                return self.resume(session_id);
            };
            match state {
                CredentialGenerationState::Reserved => {
                    if lifecycle.candidate_generation() != Some(generation) {
                        return Err(ProviderOnboardingError::InvalidSessionState);
                    }
                    self.append(
                        resumed.reservation(),
                        resumed.next_sequence(),
                        OnboardingEvent::CandidateCancelledNoEffect {
                            generation,
                            evidence_digest: event_digest(
                                b"user-cancelled-no-effect",
                                session_id,
                                Some(generation),
                            ),
                        },
                    )?;
                }
                CredentialGenerationState::StorePlanned
                | CredentialGenerationState::StoreReconciliationRequired
                | CredentialGenerationState::StoredUnverified
                | CredentialGenerationState::VerifiedLeastPrivilege
                | CredentialGenerationState::ActiveScoped => {
                    self.append(
                        resumed.reservation(),
                        resumed.next_sequence(),
                        OnboardingEvent::CleanupRequired {
                            generation: Some(generation),
                            evidence_digest: event_digest(
                                b"user-cancelled-cleanup-required",
                                session_id,
                                Some(generation),
                            ),
                        },
                    )?;
                }
                CredentialGenerationState::SupersededRetained
                | CredentialGenerationState::CleanupRequired
                | CredentialGenerationState::Retired => {
                    self.cleanup_superseded_unlocked(session_id, cancellation.clone())
                        .await?;
                }
                CredentialGenerationState::Tombstoned
                | CredentialGenerationState::AbandonedNoEffect => {}
            }
        }
    }

    async fn activate_anonymous(
        &self,
        profile: &ProviderOnboardingProfile,
        reservation: &OnboardingReservation,
        declared_user_agent: Option<&str>,
        cancellation: CancellationToken,
    ) -> Result<(), ProviderOnboardingError> {
        self.append(
            reservation,
            1,
            OnboardingEvent::RightsAdmitted {
                generation: None,
                decision_digest: profile.rights_decision_digest(),
            },
        )?;
        self.append(
            reservation,
            2,
            OnboardingEvent::RatePolicyAdmitted {
                generation: None,
                policy_digest: profile.capability().rate_policy().evidence_digest(),
            },
        )?;
        match self
            .run_probe(profile, declared_user_agent, cancellation)
            .await
        {
            Ok(evidence_digest) => self.append(
                reservation,
                3,
                OnboardingEvent::RuntimeVerified {
                    generation: None,
                    evidence_digest,
                },
            ),
            Err(ProviderOnboardingError::OperationCancelled) => self.append(
                reservation,
                3,
                OnboardingEvent::Cancelled {
                    evidence_digest: event_digest(
                        b"probe-cancelled",
                        reservation.session_id(),
                        None,
                    ),
                },
            ),
            Err(_) => self.append(
                reservation,
                3,
                OnboardingEvent::Unavailable {
                    evidence_digest: event_digest(
                        b"probe-unavailable",
                        reservation.session_id(),
                        None,
                    ),
                },
            ),
        }
    }

    async fn run_probe(
        &self,
        profile: &ProviderOnboardingProfile,
        declared_user_agent: Option<&str>,
        cancellation: CancellationToken,
    ) -> Result<EvidenceDigest, ProviderOnboardingError> {
        let probe = profile.probe();
        if probe.transport() == ProbeTransport::Local {
            return Ok(profile.rights_decision_digest());
        }
        let endpoint = probe
            .endpoint()
            .ok_or(ProviderOnboardingError::InvalidProfile)?;
        let policy = probe
            .endpoint_policy()
            .ok_or(ProviderOnboardingError::InvalidProfile)?;
        policy.authorize_request(endpoint)?;
        let rate_permit = self
            .probe_rates
            .acquire(
                profile,
                profile.capability().rate_policy(),
                None,
                cancellation.clone(),
            )
            .await?;
        let request = match probe.transport() {
            ProbeTransport::HttpGet => self.client.get(endpoint),
            ProbeTransport::HttpPostJson => self
                .client
                .post(endpoint)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(
                    probe
                        .body()
                        .ok_or(ProviderOnboardingError::InvalidProfile)?,
                ),
            ProbeTransport::Local => return Err(ProviderOnboardingError::InvalidProfile),
        };
        let request = if let Some(user_agent) = declared_user_agent {
            request.header(reqwest::header::USER_AGENT, user_agent)
        } else {
            request
        };
        let body = self
            .collect_probe_response(request, policy, &rate_permit, cancellation)
            .await?;
        validate_probe_semantics(profile.id(), &body)?;
        rate_permit.record_success()?;
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&body).into(),
        ))
    }

    async fn run_credential_probe(
        &self,
        profile: &ProviderOnboardingProfile,
        secret: &SecretValue,
        cancellation: CancellationToken,
    ) -> Result<CredentialProbeEvidence, ProviderOnboardingError> {
        let expected_transport = match profile.id() {
            "bls.v2-registered" => ProbeTransport::HttpPostJson,
            "coinbase.exchange-direct-market-data" => ProbeTransport::HttpGet,
            _ => return Err(ProviderOnboardingError::InvalidProfile),
        };
        if profile.probe().transport() != expected_transport {
            return Err(ProviderOnboardingError::InvalidProfile);
        }
        let probe = profile.probe();
        let endpoint = probe
            .endpoint()
            .ok_or(ProviderOnboardingError::InvalidProfile)?;
        let policy = probe
            .endpoint_policy()
            .ok_or(ProviderOnboardingError::InvalidProfile)?;
        policy.authorize_request(endpoint)?;
        let authorization_subject = ProviderRateDeclaration::governed_provider_subject(
            profile
                .capability()
                .rate_policy()
                .enforcement_policy()
                .ok_or(ProviderOnboardingError::InvalidProfile)?
                .scope()
                .as_source_identifier(),
        )
        .map_err(|_| ProviderOnboardingError::InvalidProfile)?;
        let rate_permit = self
            .probe_rates
            .acquire(
                profile,
                profile.capability().rate_policy(),
                Some(&authorization_subject),
                cancellation.clone(),
            )
            .await?;
        let request = match profile.id() {
            "bls.v2-registered" => {
                let mut body: serde_json::Value = serde_json::from_str(
                    probe
                        .body()
                        .ok_or(ProviderOnboardingError::InvalidProfile)?,
                )
                .map_err(|_| ProviderOnboardingError::InvalidProfile)?;
                let object = body
                    .as_object_mut()
                    .ok_or(ProviderOnboardingError::InvalidProfile)?;
                if object
                    .insert(
                        "registrationkey".to_owned(),
                        serde_json::Value::String(secret.expose_secret().to_owned()),
                    )
                    .is_some()
                {
                    return Err(ProviderOnboardingError::InvalidProfile);
                }
                let body = serde_json::to_vec(&body)
                    .map_err(|_| ProviderOnboardingError::InvalidProfile)?;
                self.client
                    .post(endpoint)
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body)
            }
            "coinbase.exchange-direct-market-data" => {
                let signer =
                    CoinbaseDirectHmacSigner::try_from_secret_envelope(secret.expose_secret())
                        .map_err(|_error| ProviderOnboardingError::InvalidSecretShape)?;
                signer
                    .verification_request(&self.client, unix_seconds_now()?)
                    .map_err(|_error| ProviderOnboardingError::InvalidSecretShape)?
            }
            _ => return Err(ProviderOnboardingError::InvalidProfile),
        };
        let response = self
            .collect_probe_response(request, policy, &rate_permit, cancellation)
            .await?;
        validate_probe_semantics(profile.id(), &response)?;
        let account_digest = match profile.id() {
            "coinbase.exchange-direct-market-data" => Some(coinbase_account_digest(&response)?),
            _ => None,
        };
        rate_permit.record_success()?;
        Ok(CredentialProbeEvidence {
            response_digest: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(&response).into(),
            ),
            account_digest,
        })
    }

    async fn collect_probe_response(
        &self,
        request: reqwest::RequestBuilder,
        policy: &market_squawk_sources::EndpointPolicy,
        rate_permit: &ProbeRatePermit,
        cancellation: CancellationToken,
    ) -> Result<Vec<u8>, ProviderOnboardingError> {
        let request_deadline = tokio::time::Instant::from_std(rate_permit.deadline);
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(ProviderOnboardingError::OperationCancelled);
            }
            () = tokio::time::sleep_until(request_deadline) => {
                return Err(ProviderOnboardingError::ProbeDeadlineExceeded);
            }
            response = request.send() => response.map_err(|_| ProviderOnboardingError::ProbeUnavailable)?,
        };
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            rate_permit
                .observe_http_429(
                    response
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .map(reqwest::header::HeaderValue::as_bytes),
                )
                .await?;
            return Err(ProviderOnboardingError::ProbeRateLimited);
        }
        if !response.status().is_success() {
            return Err(ProviderOnboardingError::ProbeUnavailable);
        }
        if let Some(length) = response.content_length() {
            policy.validate_response_size(length)?;
            if length > MAX_PROBE_BODY_BYTES as u64 {
                return Err(ProviderOnboardingError::ProbeUnavailable);
            }
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        loop {
            let next = tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Err(ProviderOnboardingError::OperationCancelled);
                }
                () = tokio::time::sleep_until(request_deadline) => {
                    return Err(ProviderOnboardingError::ProbeDeadlineExceeded);
                }
                next = stream.next() => next,
            };
            let Some(chunk) = next else {
                break;
            };
            let chunk = chunk.map_err(|_| ProviderOnboardingError::ProbeUnavailable)?;
            let next_len = body
                .len()
                .checked_add(chunk.len())
                .ok_or(ProviderOnboardingError::ProbeUnavailable)?;
            if next_len > MAX_PROBE_BODY_BYTES {
                return Err(ProviderOnboardingError::ProbeUnavailable);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    /// Reads the exact active or application-staged secret without blocking the async executor.
    pub(crate) async fn read_secret_for_activation_request(
        &self,
        lease: &ProviderActivationLease,
        cancellation: CancellationToken,
    ) -> Result<SecretValue, ProviderOnboardingError> {
        let (session_id, reference) = self.activation_secret_reference(lease)?;
        let secrets = Arc::clone(&self.secrets);
        await_blocking_secret_operation(
            Arc::clone(&self.secret_operations),
            cancellation,
            move |operation| {
                read_secret_reference(
                    secrets.as_ref(),
                    session_id,
                    &reference,
                    operation,
                    SecretInteractionPolicy::AllowPlatformPrompt,
                )
            },
        )
        .await
    }

    fn activation_secret_reference(
        &self,
        lease: &ProviderActivationLease,
    ) -> Result<(Uuid, market_squawk_platform::SecretRef), ProviderOnboardingError> {
        let resumed = self
            .catalog
            .resume_provider_onboarding(lease.session_id())?;
        let profile = self.current_profile_for(&resumed)?;
        let current = match self.lease_from_resumed(&resumed, profile) {
            Ok(current) if require_same_active_lease(&current, lease).is_ok() => current,
            Ok(_) | Err(ProviderOnboardingError::ActivationUnavailable) => {
                let prepared = self.prepared_lease_from_resumed(&resumed, profile)?;
                require_same_active_lease(&prepared, lease)?;
                prepared
            }
            Err(error) => return Err(error),
        };
        let reference = current
            .secret_reference()
            .cloned()
            .ok_or(ProviderOnboardingError::ActivationUnavailable)?;
        Ok((current.session_id(), reference))
    }

    fn prepared_lease_from_resumed(
        &self,
        resumed: &ResumedProviderOnboarding,
        profile: &ProviderOnboardingProfile,
    ) -> Result<ProviderActivationLease, ProviderOnboardingError> {
        let lifecycle = resumed.lifecycle();
        match profile.release_state() {
            ProfileReleaseState::RightsBlocked => {
                return Err(ProviderOnboardingError::RightsBlocked);
            }
            ProfileReleaseState::RefreshRequired => {
                return Err(ProviderOnboardingError::EvidenceRefreshRequired);
            }
            ProfileReleaseState::Available | ProfileReleaseState::RightsLimited => {}
        }
        let rights_decision_digest = profile.rights_decision_digest();
        let rate_policy_digest = profile.capability().rate_policy().evidence_digest();
        let issued_at = system_timestamp()?;
        let (generation, secret_reference, verification_expires_at, authority_effective_at) =
            if let Some(generation) = lifecycle.candidate_generation() {
                if lifecycle.generation_state(generation)
                    != Some(CredentialGenerationState::VerifiedLeastPrivilege)
                    || lifecycle.generation_rights_digest(generation)
                        != Some(rights_decision_digest)
                    || lifecycle.generation_rate_policy_digest(generation)
                        != Some(rate_policy_digest)
                    || lifecycle.generation_runtime_digest(generation).is_none()
                {
                    return Err(ProviderOnboardingError::ActivationUnavailable);
                }
                let reference = lifecycle
                    .generation_reference(generation)
                    .cloned()
                    .ok_or(ProviderOnboardingError::InvalidSessionState)?;
                let verification = lifecycle
                    .generation_verification(generation)
                    .ok_or(ProviderOnboardingError::InvalidSessionState)?;
                if verification.restrictions_digest() != rights_decision_digest
                    || verification
                        .expires_at()
                        .is_some_and(|expires_at| expires_at <= issued_at)
                {
                    return Err(ProviderOnboardingError::ActivationExpired);
                }
                (
                    Some(generation),
                    Some(reference),
                    verification.expires_at(),
                    verification.verified_at(),
                )
            } else {
                if lifecycle.active_generation().is_some()
                    || lifecycle.state() != OnboardingState::RuntimeVerificationPending
                    || lifecycle.anonymous_rights_digest() != Some(rights_decision_digest)
                    || lifecycle.anonymous_rate_policy_digest() != Some(rate_policy_digest)
                    || lifecycle.anonymous_runtime_digest().is_none()
                {
                    return Err(ProviderOnboardingError::ActivationUnavailable);
                }
                (None, None, None, resumed.reservation().created_at())
            };
        if authority_effective_at > issued_at {
            return Err(ProviderOnboardingError::InvalidSessionState);
        }
        Ok(ProviderActivationLease::new(ProviderActivationLeaseInput {
            session_id: resumed.reservation().session_id(),
            surface_id: lifecycle.surface_id().clone(),
            capability_revision: lifecycle.capability_revision(),
            capability_digest: lifecycle.capability_digest(),
            rights_decision_digest,
            rights: profile.rights().0.to_vec(),
            persistence_evidence: profile.persistence_evidence(),
            public_configuration_digest: resumed.reservation().public_configuration_digest(),
            public_configuration: resumed.public_configuration().clone(),
            account_digest: generation
                .and_then(|generation| lifecycle.generation_verification(generation))
                .and_then(|verification| verification.bindings().account_digest()),
            verification_evidence_digest: generation
                .and_then(|generation| lifecycle.generation_verification(generation))
                .map(AuthorityVerification::evidence_digest),
            runtime_evidence_digest: generation
                .and_then(|generation| lifecycle.generation_runtime_digest(generation))
                .or_else(|| lifecycle.anonymous_runtime_digest())
                .ok_or(ProviderOnboardingError::InvalidSessionState)?,
            provider_budget_policy: profile
                .capability()
                .rate_policy()
                .enforcement_policy()
                .cloned(),
            generation,
            secret_reference,
            verification_expires_at,
            authority_effective_at,
            issued_at,
        }))
    }

    fn lease_from_resumed(
        &self,
        resumed: &ResumedProviderOnboarding,
        profile: &ProviderOnboardingProfile,
    ) -> Result<ProviderActivationLease, ProviderOnboardingError> {
        let lifecycle = resumed.lifecycle();
        if lifecycle.active_generation().is_none()
            && (lifecycle.state() != OnboardingState::ActiveScoped
                || lifecycle.candidate_generation().is_some()
                || lifecycle.anonymous_rights_digest().is_none()
                || lifecycle.anonymous_rate_policy_digest().is_none()
                || lifecycle.anonymous_runtime_digest().is_none())
        {
            return Err(ProviderOnboardingError::ActivationUnavailable);
        }
        match profile.release_state() {
            ProfileReleaseState::RightsBlocked => {
                return Err(ProviderOnboardingError::RightsBlocked);
            }
            ProfileReleaseState::RefreshRequired => {
                return Err(ProviderOnboardingError::EvidenceRefreshRequired);
            }
            ProfileReleaseState::Available | ProfileReleaseState::RightsLimited => {}
        }
        let rights_decision_digest = profile.rights_decision_digest();
        if lifecycle.admitted_rights_digest() != Some(rights_decision_digest) {
            return Err(ProviderOnboardingError::InvalidSessionState);
        }
        let issued_at = system_timestamp()?;
        let (generation, secret_reference, verification_expires_at, authority_effective_at) =
            if let Some(generation) = lifecycle.active_generation() {
                if !lifecycle.generation_is_active_scoped(generation) {
                    return Err(ProviderOnboardingError::InvalidSessionState);
                }
                let reference = lifecycle
                    .generation_reference(generation)
                    .cloned()
                    .ok_or(ProviderOnboardingError::InvalidSessionState)?;
                let verification = lifecycle
                    .generation_verification(generation)
                    .ok_or(ProviderOnboardingError::InvalidSessionState)?;
                if verification.restrictions_digest() != rights_decision_digest
                    || verification
                        .expires_at()
                        .is_some_and(|expires_at| expires_at <= issued_at)
                {
                    return Err(ProviderOnboardingError::ActivationExpired);
                }
                (
                    Some(generation),
                    Some(reference),
                    verification.expires_at(),
                    verification.verified_at(),
                )
            } else {
                (None, None, None, resumed.reservation().created_at())
            };
        if authority_effective_at > issued_at {
            return Err(ProviderOnboardingError::InvalidSessionState);
        }
        Ok(ProviderActivationLease::new(ProviderActivationLeaseInput {
            session_id: resumed.reservation().session_id(),
            surface_id: lifecycle.surface_id().clone(),
            capability_revision: lifecycle.capability_revision(),
            capability_digest: lifecycle.capability_digest(),
            rights_decision_digest,
            rights: profile.rights().0.to_vec(),
            persistence_evidence: profile.persistence_evidence(),
            public_configuration_digest: resumed.reservation().public_configuration_digest(),
            public_configuration: resumed.public_configuration().clone(),
            account_digest: generation
                .and_then(|generation| lifecycle.generation_verification(generation))
                .and_then(|verification| verification.bindings().account_digest()),
            verification_evidence_digest: generation
                .and_then(|generation| lifecycle.generation_verification(generation))
                .map(AuthorityVerification::evidence_digest),
            runtime_evidence_digest: generation
                .and_then(|generation| lifecycle.generation_runtime_digest(generation))
                .or_else(|| lifecycle.anonymous_runtime_digest())
                .ok_or(ProviderOnboardingError::InvalidSessionState)?,
            provider_budget_policy: profile
                .capability()
                .rate_policy()
                .enforcement_policy()
                .cloned(),
            generation,
            secret_reference,
            verification_expires_at,
            authority_effective_at,
            issued_at,
        }))
    }

    fn append(
        &self,
        reservation: &OnboardingReservation,
        sequence: u64,
        event: OnboardingEvent,
    ) -> Result<(), ProviderOnboardingError> {
        self.catalog
            .append_provider_onboarding_event(reservation, sequence, event)?;
        Ok(())
    }

    fn register_profile_capabilities(
        &self,
        profile: &ProviderOnboardingProfile,
    ) -> Result<CapabilityRegistrationOutcome, ProviderOnboardingError> {
        let mut current = None;
        for capability in profile.capability_history() {
            current = Some(self.catalog.register_provider_capability(capability)?);
        }
        current.ok_or(ProviderOnboardingError::InvalidProfile)
    }

    fn profile_for<'a>(
        &'a self,
        resumed: &ResumedProviderOnboarding,
    ) -> Result<&'a ProviderOnboardingProfile, ProviderOnboardingError> {
        let profile = self
            .profiles
            .get(resumed.lifecycle().surface_id().as_str())
            .ok_or(ProviderOnboardingError::UnknownProfile)?;
        if profile
            .capability_at(
                resumed.lifecycle().capability_revision(),
                resumed.lifecycle().capability_digest(),
            )
            .is_none()
        {
            return Err(ProviderOnboardingError::InvalidSessionState);
        }
        validate_recovered_public_configuration(profile, resumed.public_configuration())?;
        Ok(profile)
    }

    fn current_profile_for<'a>(
        &'a self,
        resumed: &ResumedProviderOnboarding,
    ) -> Result<&'a ProviderOnboardingProfile, ProviderOnboardingError> {
        let profile = self.profile_for(resumed)?;
        if profile.capability().revision() != resumed.lifecycle().capability_revision()
            || profile.capability().content_digest() != resumed.lifecycle().capability_digest()
        {
            return Err(ProviderOnboardingError::EvidenceRefreshRequired);
        }
        Ok(profile)
    }

    fn session_views(
        &self,
        sessions: Vec<ResumedProviderOnboarding>,
    ) -> Result<Vec<OnboardingSessionView>, ProviderOnboardingError> {
        sessions
            .iter()
            .map(|resumed| {
                self.profile_for(resumed)
                    .map(|profile| session_view(profile, resumed))
            })
            .collect()
    }
}

impl ProviderOnboardingMutationAuthority<'_> {
    pub(crate) fn active_lease(
        &self,
        session_id: Uuid,
    ) -> Result<ProviderActivationLease, ProviderOnboardingError> {
        self.service.activation_lease(session_id)
    }

    pub(crate) fn require_active(
        &self,
        expected: &ProviderActivationLease,
    ) -> Result<(), ProviderOnboardingError> {
        let current = self.service.activation_lease(expected.session_id())?;
        require_same_active_lease(&current, expected)
    }

    pub(crate) fn require_prepared_or_active(
        &self,
        expected: &ProviderActivationLease,
    ) -> Result<(), ProviderOnboardingError> {
        let resumed = self
            .service
            .catalog
            .resume_provider_onboarding(expected.session_id())?;
        let profile = self.service.current_profile_for(&resumed)?;
        match self.service.lease_from_resumed(&resumed, profile) {
            Ok(current) if require_same_active_lease(&current, expected).is_ok() => return Ok(()),
            Ok(_) | Err(ProviderOnboardingError::ActivationUnavailable) => {}
            Err(error) => return Err(error),
        }
        let prepared = self
            .service
            .prepared_lease_from_resumed(&resumed, profile)?;
        require_same_active_lease(&prepared, expected)
    }

    pub(crate) fn commit_prepared_activation(
        &self,
        prepared: &ProviderActivationLease,
    ) -> Result<ProviderActivationLease, ProviderOnboardingError> {
        self.service.commit_prepared_activation_locked(prepared)
    }

    pub(crate) fn invalidate_activation_recipe(
        &self,
        session_id: Uuid,
        evidence_digest: EvidenceDigest,
    ) -> Result<OnboardingSessionView, ProviderOnboardingError> {
        self.service
            .invalidate_activation_recipe(session_id, evidence_digest)
    }

    pub(crate) async fn rollback_prepared_activation(
        &self,
        prepared: &ProviderActivationLease,
        evidence_digest: EvidenceDigest,
        cancellation: CancellationToken,
    ) -> Result<(), ProviderOnboardingError> {
        self.require_prepared_or_active(prepared)?;
        let generation = prepared
            .generation()
            .ok_or(ProviderOnboardingError::InvalidSessionState)?;
        let resumed = self
            .service
            .catalog
            .resume_provider_onboarding(prepared.session_id())?;
        if resumed.lifecycle().candidate_generation() != Some(generation) {
            return Err(ProviderOnboardingError::InvalidSessionState);
        }
        if resumed.lifecycle().active_generation().is_some() {
            self.service.append(
                resumed.reservation(),
                resumed.next_sequence(),
                OnboardingEvent::CleanupRequired {
                    generation: Some(generation),
                    evidence_digest,
                },
            )?;
        } else {
            self.invalidate_activation_recipe(prepared.session_id(), evidence_digest)?;
        }
        self.service
            .cleanup_superseded_unlocked(prepared.session_id(), cancellation)
            .await
    }

    fn discard_prepared_activation_at_startup(
        &self,
        prepared: &ProviderActivationLease,
        evidence_digest: EvidenceDigest,
    ) -> Result<(), ProviderOnboardingError> {
        let resumed = self
            .service
            .catalog
            .resume_provider_onboarding(prepared.session_id())?;
        let profile = self.service.current_profile_for(&resumed)?;
        let exact = self
            .service
            .prepared_lease_from_resumed(&resumed, profile)?;
        require_same_active_lease(&exact, prepared)?;
        let Some(generation) = prepared.generation() else {
            self.invalidate_activation_recipe(prepared.session_id(), evidence_digest)?;
            return Ok(());
        };
        if resumed.lifecycle().candidate_generation() != Some(generation) {
            return Err(ProviderOnboardingError::InvalidSessionState);
        }
        if resumed.lifecycle().active_generation().is_none() {
            self.invalidate_activation_recipe(prepared.session_id(), evidence_digest)?;
            return Ok(());
        }
        self.service.append(
            resumed.reservation(),
            resumed.next_sequence(),
            OnboardingEvent::CleanupRequired {
                generation: Some(generation),
                evidence_digest,
            },
        )?;
        Ok(())
    }
}

async fn await_blocking_secret_operation<T, F>(
    admission: Arc<Semaphore>,
    cancellation: CancellationToken,
    operation: F,
) -> Result<T, ProviderOnboardingError>
where
    T: Send + 'static,
    F: FnOnce(SecretCancellation) -> Result<T, ProviderOnboardingError> + Send + 'static,
{
    if cancellation.is_cancelled() {
        return Err(ProviderOnboardingError::OperationCancelled);
    }
    let permit = tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            return Err(ProviderOnboardingError::OperationCancelled);
        }
        permit = admission.acquire_owned() => {
            permit.map_err(|_| ProviderOnboardingError::SecretOperationUnavailable)?
        }
    };
    let mut task = SecretOperationTask::spawn(permit, operation)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            task.cancel();
            match task.await {
                Err(ProviderOnboardingError::SecretStore(
                    LocalSecretStoreError::OperationCancelled,
                )) => Err(ProviderOnboardingError::OperationCancelled),
                result => result,
            }
        }
        result = &mut task => result,
    }
}

impl<T: Send + 'static> SecretOperationTask<T> {
    fn spawn<F>(
        operation_permit: OwnedSemaphorePermit,
        operation: F,
    ) -> Result<Self, ProviderOnboardingError>
    where
        F: FnOnce(SecretCancellation) -> Result<T, ProviderOnboardingError> + Send + 'static,
    {
        let reaper_capacity = SECRET_OPERATION_REAPER.try_reserve()?;
        let cancellation = SecretCancellation::new();
        let operation_cancellation = cancellation.clone();
        let (result_sender, result) = oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let _operation_permit = operation_permit;
            let outcome = operation(operation_cancellation);
            let _ignored = result_sender.send(outcome);
        });
        Ok(Self {
            cancellation,
            command: Some(SecretReapCommand {
                worker,
                _capacity: reaper_capacity,
            }),
            result,
        })
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }

    fn handoff_pending_worker(&mut self) {
        let Some(command) = self.command.take() else {
            return;
        };
        if command.worker.is_finished() {
            return;
        }
        SECRET_OPERATION_REAPER.reap(command);
    }
}

impl<T: Send + 'static> Future for SecretOperationTask<T> {
    type Output = Result<T, ProviderOnboardingError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.result).poll(context) {
            Poll::Ready(Ok(result)) => {
                self.handoff_pending_worker();
                Poll::Ready(result)
            }
            Poll::Ready(Err(_closed)) => {
                let Some(command) = self.command.as_mut() else {
                    return Poll::Ready(Err(ProviderOnboardingError::SecretOperationUnavailable));
                };
                match Pin::new(&mut command.worker).poll(context) {
                    Poll::Ready(result) => {
                        self.command = None;
                        if result.is_err() {
                            tracing::error!("blocking secret worker failed before returning");
                        }
                        Poll::Ready(Err(ProviderOnboardingError::SecretOperationUnavailable))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: Send + 'static> Drop for SecretOperationTask<T> {
    fn drop(&mut self) {
        self.cancel();
        self.handoff_pending_worker();
    }
}

impl SecretOperationReaper {
    fn start() -> Self {
        let capacity = Arc::new(Semaphore::new(MAXIMUM_PENDING_SECRET_REAPS));
        let (sender, receiver) = sync_channel::<SecretReapCommand>(MAXIMUM_PENDING_SECRET_REAPS);
        let thread = std::thread::Builder::new()
            .name("market-squawk-secret-reaper".to_owned())
            .spawn(move || {
                while let Ok(command) = receiver.recv() {
                    Self::reap_worker(command);
                }
            });
        match thread {
            Ok(thread) => Self {
                sender: Some(sender),
                capacity,
                _thread: Some(thread),
            },
            Err(_error) => Self {
                sender: None,
                capacity,
                _thread: None,
            },
        }
    }

    fn try_reserve(&self) -> Result<OwnedSemaphorePermit, ProviderOnboardingError> {
        if self.sender.is_none() {
            return Err(ProviderOnboardingError::SecretOperationUnavailable);
        }
        Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_| ProviderOnboardingError::SecretOperationUnavailable)
    }

    fn reap(&self, command: SecretReapCommand) {
        let Some(sender) = self.sender.as_ref() else {
            self.reap_without_worker_thread(command);
            return;
        };
        // The semaphore and channel have equal capacity, and every queued command retains one
        // permit. An admitted command therefore always owns a channel slot: this send can fail
        // after a reaper disconnect, but it cannot wait for channel capacity.
        if let Err(error) = sender.send(command) {
            self.reap_without_worker_thread(error.0);
        }
    }

    fn reap_without_worker_thread(&self, command: SecretReapCommand) {
        Self::reap_worker(command);
    }

    fn reap_worker(mut command: SecretReapCommand) {
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        loop {
            match Pin::new(&mut command.worker).poll(&mut context) {
                Poll::Ready(result) => {
                    if result.is_err() {
                        tracing::error!("blocking secret worker failed while being reaped");
                    }
                    drop(command._capacity);
                    return;
                }
                Poll::Pending => std::thread::sleep(Duration::from_millis(1)),
            }
        }
    }
}

fn read_secret_reference(
    secrets: &dyn SecretStore,
    session_id: Uuid,
    reference: &market_squawk_platform::SecretRef,
    cancellation: SecretCancellation,
    interaction: SecretInteractionPolicy,
) -> Result<SecretValue, ProviderOnboardingError> {
    let deadline = Instant::now()
        .checked_add(SECRET_OPERATION_DURATION)
        .ok_or(ProviderOnboardingError::Clock)?;
    let control = SecretOperationControl::try_new(
        format!("provider-activation-{session_id}"),
        deadline,
        0,
        interaction,
        cancellation,
    )?;
    secrets.read(reference, &control).map_err(Into::into)
}

fn secret_fallback_control(
    owner: &'static str,
    cancellation: SecretCancellation,
) -> Result<SecretOperationControl, ProviderOnboardingError> {
    let deadline = Instant::now()
        .checked_add(SECRET_OPERATION_DURATION)
        .ok_or(ProviderOnboardingError::Clock)?;
    SecretOperationControl::try_new(
        owner,
        deadline,
        0,
        SecretInteractionPolicy::Forbid,
        cancellation,
    )
    .map_err(Into::into)
}

fn prepared_activation_event(
    active_generation: Option<SecretGeneration>,
    candidate_generation: Option<SecretGeneration>,
) -> Result<OnboardingEvent, ProviderOnboardingError> {
    match (active_generation, candidate_generation) {
        (Some(prior_generation), Some(candidate_generation)) => Ok(OnboardingEvent::Cutover {
            prior_generation,
            candidate_generation,
        }),
        (None, generation) => Ok(OnboardingEvent::Activate { generation }),
        (Some(_), None) => Err(ProviderOnboardingError::InvalidSessionState),
    }
}

fn require_same_active_lease(
    current: &ProviderActivationLease,
    expected: &ProviderActivationLease,
) -> Result<(), ProviderOnboardingError> {
    if current.session_id() == expected.session_id()
        && current.surface_id() == expected.surface_id()
        && current.capability_revision() == expected.capability_revision()
        && current.capability_digest() == expected.capability_digest()
        && current.rights_decision_digest() == expected.rights_decision_digest()
        && current.public_configuration_digest() == expected.public_configuration_digest()
        && current.account_digest() == expected.account_digest()
        && current.verification_evidence_digest() == expected.verification_evidence_digest()
        && current.provider_budget_policy() == expected.provider_budget_policy()
        && current.generation() == expected.generation()
        && current.secret_reference() == expected.secret_reference()
    {
        Ok(())
    } else {
        Err(ProviderOnboardingError::InvalidSessionState)
    }
}

impl fmt::Debug for ProviderOnboardingService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderOnboardingService")
            .field("profiles", &self.profiles)
            .field("catalog", &self.catalog)
            .field("secrets", &"[SEALED SECRET AUTHORITY]")
            .finish_non_exhaustive()
    }
}

fn validate_declared_contact(
    profile: &ProviderOnboardingProfile,
    request: &StartOnboardingRequest,
) -> Result<(), ProviderOnboardingError> {
    let (_, _, contact_requirement) = profile.requirements();
    match contact_requirement {
        market_squawk_sources::Requirement::RequiredNonSecret
            if request.organization.is_none() || request.administrative_email.is_none() =>
        {
            Err(ProviderOnboardingError::AdministrativeContactRequired)
        }
        market_squawk_sources::Requirement::NotRequired
        | market_squawk_sources::Requirement::RequiredProviderControlled
            if request.organization.is_some() || request.administrative_email.is_some() =>
        {
            Err(ProviderOnboardingError::InvalidRequest)
        }
        _ => Ok(()),
    }
}

fn provider_public_configuration(
    profile: &ProviderOnboardingProfile,
    request: &StartOnboardingRequest,
) -> Result<ProviderPublicConfiguration, ProviderOnboardingError> {
    let fields = match profile.id() {
        "sec.edgar-public" => BTreeMap::from([
            (
                "administrative_email".to_owned(),
                request
                    .administrative_email
                    .clone()
                    .ok_or(ProviderOnboardingError::AdministrativeContactRequired)?,
            ),
            (
                "organization".to_owned(),
                request
                    .organization
                    .clone()
                    .ok_or(ProviderOnboardingError::AdministrativeContactRequired)?,
            ),
        ]),
        "bls.v1-unregistered" => {
            BTreeMap::from([("registration_mode".to_owned(), "unregistered_v1".to_owned())])
        }
        "bls.v2-registered" => BTreeMap::from([
            (
                "administrative_email".to_owned(),
                request
                    .administrative_email
                    .clone()
                    .ok_or(ProviderOnboardingError::AdministrativeContactRequired)?,
            ),
            (
                "organization".to_owned(),
                request
                    .organization
                    .clone()
                    .ok_or(ProviderOnboardingError::AdministrativeContactRequired)?,
            ),
            ("registration_mode".to_owned(), "registered_v2".to_owned()),
        ]),
        _ => BTreeMap::new(),
    };
    ProviderPublicConfiguration::try_new(fields)
        .map_err(|_| ProviderOnboardingError::InvalidRequest)
}

fn validate_recovered_public_configuration(
    profile: &ProviderOnboardingProfile,
    configuration: &ProviderPublicConfiguration,
) -> Result<(), ProviderOnboardingError> {
    let exact = match profile.id() {
        "sec.edgar-public" => {
            configuration.iter().len() == 2
                && configuration
                    .get("organization")
                    .is_some_and(|value| valid_optional_contact(Some(value), false))
                && configuration
                    .get("administrative_email")
                    .is_some_and(|value| valid_optional_contact(Some(value), true))
        }
        "bls.v1-unregistered" => {
            configuration.iter().len() == 1
                && configuration.get("registration_mode") == Some("unregistered_v1")
        }
        "bls.v2-registered" => {
            configuration.iter().len() == 3
                && configuration.get("registration_mode") == Some("registered_v2")
                && configuration
                    .get("organization")
                    .is_some_and(|value| valid_optional_contact(Some(value), false))
                && configuration
                    .get("administrative_email")
                    .is_some_and(|value| valid_optional_contact(Some(value), true))
        }
        _ => configuration.is_empty(),
    };
    if exact {
        Ok(())
    } else {
        Err(ProviderOnboardingError::InvalidSessionState)
    }
}

fn validate_secret_shape(
    profile: &ProviderOnboardingProfile,
    secret: &SecretValue,
) -> Result<(), ProviderOnboardingError> {
    let value = secret.expose_secret();
    let valid = match profile.id() {
        "bls.v2-registered" => {
            value.len() <= 256 && value.bytes().all(|byte| byte.is_ascii_graphic())
        }
        "fred-alfred.api-v1-v2" => {
            value.len() == 32
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        }
        "coinbase.exchange-direct-market-data" => {
            CoinbaseDirectHmacSigner::try_from_secret_envelope(value).is_ok()
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(ProviderOnboardingError::InvalidSecretShape)
    }
}

fn validate_probe_semantics(profile_id: &str, body: &[u8]) -> Result<(), ProviderOnboardingError> {
    if body.is_empty() {
        return Err(ProviderOnboardingError::ProbeUnavailable);
    }
    if profile_id == "treasury.daily-rates-xml" {
        let request = TreasuryYieldCurveProfile::daily_par_yield_curve()
            .page(TREASURY_DAILY_RATES_PROBE_YEAR, 0)
            .map_err(|_| ProviderOnboardingError::InvalidProfile)?;
        let page = DailyParYieldCurvePage::parse(
            body,
            &request,
            FiscalDataParseLimits::production_defaults(),
        )
        .map_err(|_| ProviderOnboardingError::ProbeUnavailable)?;
        return (!page.is_terminal())
            .then_some(())
            .ok_or(ProviderOnboardingError::ProbeUnavailable);
    }
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| ProviderOnboardingError::ProbeUnavailable)?;
    let valid = match profile_id {
        "coinbase.public-market-data" => value.get("product_id").is_some(),
        "coinbase.exchange-direct-market-data" => coinbase_account_id(&value).is_some(),
        "kraken.spot-public-market-data" => {
            value
                .get("error")
                .and_then(serde_json::Value::as_array)
                .is_some()
                && value.get("result").is_some()
        }
        "sec.edgar-public" => value.get("cik").is_some() && value.get("filings").is_some(),
        "bls.v1-unregistered" | "bls.v2-registered" => {
            value.get("status").and_then(serde_json::Value::as_str) == Some("REQUEST_SUCCEEDED")
                && value.get("Results").is_some()
        }
        "treasury.fiscal-data" => {
            value.get("data").is_some()
                && value.get("meta").is_some()
                && value.get("links").is_some()
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(ProviderOnboardingError::ProbeUnavailable)
    }
}

#[derive(Clone, Copy, Debug)]
struct CredentialProbeEvidence {
    response_digest: EvidenceDigest,
    account_digest: Option<EvidenceDigest>,
}

fn coinbase_account_digest(body: &[u8]) -> Result<EvidenceDigest, ProviderOnboardingError> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| ProviderOnboardingError::ProbeUnavailable)?;
    let account = coinbase_account_id(&value).ok_or(ProviderOnboardingError::ProbeUnavailable)?;
    let length =
        u64::try_from(account.len()).map_err(|_| ProviderOnboardingError::ProbeUnavailable)?;
    let mut hasher = Sha256::new();
    hasher.update(COINBASE_ACCOUNT_BINDING_DOMAIN);
    hasher.update(length.to_be_bytes());
    hasher.update(account.as_bytes());
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

fn coinbase_account_id(value: &serde_json::Value) -> Option<&str> {
    let account = value.as_object()?.get("id")?.as_str()?;
    market_squawk_domain::SourceIdentifier::try_from(account)
        .ok()
        .map(|_validated| account)
}

fn event_digest(
    domain: &[u8],
    session_id: Uuid,
    generation: Option<market_squawk_platform::SecretGeneration>,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk-onboarding-event-v1\0");
    hasher.update(domain);
    hasher.update(session_id.as_bytes());
    if let Some(generation) = generation {
        hasher.update(generation.get().to_be_bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn derived_evidence_digest(
    domain: &[u8],
    session_id: Uuid,
    generation: market_squawk_platform::SecretGeneration,
    evidence: EvidenceDigest,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk-onboarding-derived-evidence-v1\0");
    hasher.update(domain);
    hasher.update(session_id.as_bytes());
    hasher.update(generation.get().to_be_bytes());
    hasher.update(evidence.bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn credential_assurance(
    profile: &ProviderOnboardingProfile,
) -> Result<SourceIdentifier, ProviderOnboardingError> {
    match profile.id() {
        "bls.v2-registered" => {
            SourceIdentifier::try_from("bls-timeseries-read-only-verification").map_err(Into::into)
        }
        "coinbase.exchange-direct-market-data" => SourceIdentifier::try_from(
            "coinbase-exchange-view-key-verified-live-entitlement-pending",
        )
        .map_err(Into::into),
        _ => Err(ProviderOnboardingError::InvalidProfile),
    }
}

fn unix_seconds_now() -> Result<u64, ProviderOnboardingError> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ProviderOnboardingError::Clock)?
        .as_secs();
    if seconds == 0 {
        Err(ProviderOnboardingError::Clock)
    } else {
        Ok(seconds)
    }
}

fn system_timestamp() -> Result<Timestamp, ProviderOnboardingError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ProviderOnboardingError::Clock)?;
    let nanos = i64::try_from(now.as_nanos()).map_err(|_| ProviderOnboardingError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn wall_deadline(duration: Duration) -> Result<Timestamp, ProviderOnboardingError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ProviderOnboardingError::Clock)?;
    let deadline = now
        .checked_add(duration)
        .ok_or(ProviderOnboardingError::Clock)?;
    let nanos = i64::try_from(deadline.as_nanos()).map_err(|_| ProviderOnboardingError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn valid_optional_contact(value: Option<&str>, email: bool) -> bool {
    value.is_none_or(|value| {
        !value.is_empty()
            && value.len() <= MAX_CONTACT_BYTES
            && value.is_ascii()
            && !value.chars().any(char::is_control)
            && (!email
                || (value
                    .split_once('@')
                    .is_some_and(|(local, domain)| !local.is_empty() && domain.contains('.'))))
    })
}

/// Provider onboarding application failure without secret or response-body content.
#[derive(Debug, Error)]
pub enum ProviderOnboardingError {
    /// A code-owned profile violated an internal probe or activation invariant.
    #[error("provider onboarding profile contract is invalid")]
    InvalidProfile,
    /// The selected profile identity is not code-owned.
    #[error("provider onboarding profile is unknown")]
    UnknownProfile,
    /// Input exceeded a bound or violated a profile field contract.
    #[error("provider onboarding request is invalid")]
    InvalidRequest,
    /// The exact profile requires a declared non-secret administrative contact.
    #[error("provider onboarding requires an administrative contact")]
    AdministrativeContactRequired,
    /// The current session/profile gate does not accept a secret.
    #[error("provider onboarding secret import is unavailable")]
    SecretImportUnavailable,
    /// The current generation or capability does not admit a replacement operation.
    #[error("provider onboarding renewal is unavailable")]
    RenewalUnavailable,
    /// Code-owned official evidence must be refreshed before activation can be attempted.
    #[error("provider onboarding evidence must be refreshed before activation")]
    EvidenceRefreshRequired,
    /// The retained provider rights decision blocks activation.
    #[error("provider onboarding rights block activation")]
    RightsBlocked,
    /// The durable session has not completed every activation precondition.
    #[error("provider onboarding activation is unavailable")]
    ActivationUnavailable,
    /// The exact provider verification expired before a lease could be recovered.
    #[error("provider onboarding activation verification expired")]
    ActivationExpired,
    /// Exact secure-store readback did not reproduce the submitted value.
    #[error("provider onboarding secret-store verification failed")]
    SecretVerificationFailed,
    /// Exact local deletion or retirement remains unresolved.
    #[error("provider onboarding secret cleanup is unavailable")]
    SecretCleanupUnavailable,
    /// A supported provider-side revocation requires adapter reconciliation.
    #[error("provider onboarding remote revocation requires reconciliation")]
    RemoteReconciliationRequired,
    /// Submitted secret material did not match the provider's documented shape.
    #[error("provider onboarding secret shape is invalid")]
    InvalidSecretShape,
    /// Durable state did not match the exact code-owned profile revision.
    #[error("provider onboarding session state is invalid")]
    InvalidSessionState,
    /// The hardened provider client could not be constructed.
    #[error("provider onboarding client configuration failed")]
    ClientConfiguration,
    /// The bounded verification request failed or returned an unexpected schema.
    #[error("provider onboarding verification is unavailable")]
    ProbeUnavailable,
    /// The shared provider/account budget cannot admit this verification within its fixed bound.
    #[error("provider onboarding verification is rate limited")]
    ProbeRateLimited,
    /// The admitted verification exceeded its fixed end-to-end deadline.
    #[error("provider onboarding verification deadline elapsed")]
    ProbeDeadlineExceeded,
    /// Cooperative cancellation completed before activation.
    #[error("provider onboarding operation was cancelled")]
    OperationCancelled,
    /// The isolated blocking secret operation could not be joined safely.
    #[error("provider onboarding secret operation is unavailable")]
    SecretOperationUnavailable,
    /// Wall-clock conversion failed.
    #[error("provider onboarding clock is unavailable")]
    Clock,
    /// A built-in profile was invalid.
    #[error(transparent)]
    Profile(#[from] ProviderProfileError),
    /// The non-secret catalog transition failed.
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    /// Exact-generation secret storage failed.
    #[error(transparent)]
    SecretStore(#[from] LocalSecretStoreError),
    /// A bounded identity was invalid.
    #[error(transparent)]
    Identity(#[from] market_squawk_domain::IdentityError),
    /// A fixed endpoint violated its code-owned policy.
    #[error(transparent)]
    Network(#[from] market_squawk_sources::NetworkPolicyError),
    /// The process TLS provider was unavailable.
    #[error(transparent)]
    Tls(#[from] market_squawk_sources::TlsProviderError),
}

#[cfg(test)]
mod tests {
    use market_squawk_data::{
        CatalogConfig, CatalogResultLimits, ObjectStoreConfig, SqliteProviderRateStore,
    };
    use market_squawk_platform::{EncryptedFileSecretStore, LocalPaths};

    use super::*;
    use crate::ResearchService;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn coinbase_direct_credential_shape_and_probe_semantics_fail_closed() -> TestResult {
        let profiles = built_in_provider_profiles()?;
        let profile = profiles
            .get("coinbase.exchange-direct-market-data")
            .ok_or("Coinbase Direct onboarding profile is missing")?;
        let secret = SecretValue::new(
            r#"{"version":1,"api_key":"fixture-key","passphrase":"fixture-passphrase","signing_secret":"dGVzdC1zZWNyZXQ="}"#
                .to_owned(),
        )?;
        validate_secret_shape(profile, &secret)?;
        let signer =
            market_squawk_adapter_coinbase::CoinbaseDirectHmacSigner::try_from_secret_envelope(
                secret.expose_secret(),
            )?;
        assert_eq!(
            format!("{signer:?}"),
            "CoinbaseDirectHmacSigner([REDACTED])"
        );
        validate_probe_semantics(profile.id(), br#"{"id":"fixture-user"}"#)?;
        assert_eq!(
            coinbase_account_digest(br#"{"id":"fixture-user"}"#)?,
            coinbase_account_digest(br#"{"id":"fixture-user","profile_id":"ignored"}"#)?
        );
        assert!(validate_probe_semantics(profile.id(), b"{}").is_err());
        assert!(validate_probe_semantics(profile.id(), b"[]").is_err());
        assert!(validate_probe_semantics(profile.id(), br#"{"id":"fixture user"}"#).is_err());
        assert!(validate_probe_semantics(profile.id(), br#"{"id":7}"#).is_err());
        Ok(())
    }

    #[test]
    fn startup_runtime_admission_is_exact_to_surface_and_session() -> TestResult {
        let surface = SourceIdentifier::try_from("bls.v2-registered")?;
        let predecessor = Uuid::new_v4();
        let desired = Uuid::new_v4();
        let admissions = ProviderRuntimeStartupAdmissions::try_new([(surface.clone(), desired)])?;
        assert!(!admissions.admits(&surface, predecessor));
        assert!(admissions.admits(&surface, desired));
        Ok(())
    }

    #[test]
    fn prepared_activation_event_selects_exact_cutover() -> TestResult {
        let predecessor = market_squawk_platform::SecretGeneration::new(1)?;
        let candidate = market_squawk_platform::SecretGeneration::new(2)?;
        assert!(matches!(
            prepared_activation_event(Some(predecessor), Some(candidate))?,
            OnboardingEvent::Cutover {
                prior_generation,
                candidate_generation,
            } if prior_generation == predecessor && candidate_generation == candidate
        ));
        Ok(())
    }

    #[test]
    fn startup_reconciles_every_page_of_recognized_historical_sessions() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
        let research = ResearchService::initialize(
            &paths,
            CatalogConfig::try_new(
                paths.catalog()?.clone(),
                Duration::from_millis(750),
                CatalogLimit::new(64)?,
                CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
            )?,
            8,
            ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
        )?;
        let catalog = research.onboarding_catalog();
        let profiles = built_in_provider_profiles()?;
        let sec = profiles
            .get("sec.edgar-public")
            .ok_or("SEC onboarding profile is missing")?;
        let historical = sec
            .capability_history()
            .next()
            .ok_or("SEC historical capability is missing")?;
        catalog.register_provider_capability(historical)?;
        let request = StartOnboardingRequest::try_new(
            sec.id(),
            Some("Market Squawk".to_owned()),
            Some("operations@example.test".to_owned()),
        )?;
        let public_configuration = provider_public_configuration(sec, &request)?;
        for index in 0..33_u8 {
            let reservation = OnboardingReservationRequest::try_new(
                historical,
                public_configuration.clone(),
                historical.maximum_authority().clone(),
                SourceIdentifier::try_from("startup-recovery-test")?,
                SourceIdentifier::try_from(format!("startup-recovery-{index}"))?,
                wall_deadline(SESSION_DURATION)?,
                0,
            )?;
            let _reservation = catalog.reserve_provider_onboarding(&reservation)?;
        }

        let provider_rate =
            ProviderRateAuthority::try_new(Arc::new(SqliteProviderRateStore::try_open(
                directory.path().join("startup-provider-rate.sqlite3"),
            )?))?;
        let secrets = Arc::new(EncryptedFileSecretStore::try_open(
            directory.path().join("startup-secrets"),
            SecretValue::new("startup reconciliation unlock".to_owned())?,
        )?);
        let service =
            ProviderOnboardingService::try_new_with_provider_rate(catalog, secrets, provider_rate)?;
        let session_ids = service
            .catalog
            .provider_onboarding_session_ids_after(None, CatalogLimit::new(64)?)?;
        assert_eq!(session_ids.len(), 33);
        for session_id in session_ids {
            assert_eq!(
                service
                    .catalog
                    .resume_provider_onboarding(session_id)?
                    .lifecycle()
                    .state(),
                OnboardingState::RefreshRequired
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn simultaneous_secret_completion_and_cancellation_returns_worker_outcome() -> TestResult
    {
        let admission = Arc::new(Semaphore::new(1));
        let cancellation = CancellationToken::new();
        let trigger = cancellation.clone();
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let operation = tokio::spawn(await_blocking_secret_operation(
            admission,
            cancellation,
            move |_secret_cancellation| {
                started_tx
                    .send(())
                    .map_err(|_| ProviderOnboardingError::SecretOperationUnavailable)?;
                release_rx
                    .recv()
                    .map_err(|_| ProviderOnboardingError::SecretOperationUnavailable)?;
                Ok(EncryptedFileFallbackStatus::Ready)
            },
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(250), started_rx.recv()).await,
            Ok(Some(()))
        ));
        trigger.cancel();
        release_tx.send(())?;

        let result = tokio::time::timeout(Duration::from_millis(250), operation).await???;
        assert_eq!(result, EncryptedFileFallbackStatus::Ready);
        Ok(())
    }

    #[tokio::test]
    async fn dropped_secret_waiter_cancels_and_reaps_worker_before_releasing_admission()
    -> TestResult {
        let admission = Arc::new(Semaphore::new(1));
        let (first_started_tx, mut first_started_rx) = tokio::sync::mpsc::unbounded_channel();
        let (first_cancelled_tx, mut first_cancelled_rx) = tokio::sync::mpsc::unbounded_channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let first = tokio::spawn(await_blocking_secret_operation(
            Arc::clone(&admission),
            CancellationToken::new(),
            move |secret_cancellation| {
                first_started_tx
                    .send(())
                    .map_err(|_| ProviderOnboardingError::SecretOperationUnavailable)?;
                release_rx
                    .recv()
                    .map_err(|_| ProviderOnboardingError::SecretOperationUnavailable)?;
                first_cancelled_tx
                    .send(secret_cancellation.is_cancelled())
                    .map_err(|_| ProviderOnboardingError::SecretOperationUnavailable)?;
                Ok(())
            },
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(250), first_started_rx.recv()).await,
            Ok(Some(()))
        ));
        first.abort();
        assert!(first.await.is_err());

        let (second_started_tx, mut second_started_rx) = tokio::sync::mpsc::unbounded_channel();
        let second = tokio::spawn(await_blocking_secret_operation(
            Arc::clone(&admission),
            CancellationToken::new(),
            move |_secret_cancellation| {
                second_started_tx
                    .send(())
                    .map_err(|_| ProviderOnboardingError::SecretOperationUnavailable)?;
                Ok(())
            },
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(25), second_started_rx.recv())
                .await
                .is_err()
        );
        release_tx.send(())?;
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(250), first_cancelled_rx.recv()).await?,
            Some(true)
        );
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(250), second_started_rx.recv()).await,
            Ok(Some(()))
        ));
        tokio::time::timeout(Duration::from_millis(250), second).await???;
        Ok(())
    }
}
