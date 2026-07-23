//! Transport-neutral provider onboarding application service.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt as _;
use market_squawk_data::{
    CatalogError, CatalogLimit, OnboardingCatalogCapability, OnboardingReservation,
    OnboardingReservationRequest, ResumedProviderOnboarding,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::{
    LocalSecretStoreError, SecretCancellation, SecretInteractionPolicy, SecretKey,
    SecretOperationControl, SecretStore, SecretValue,
};
use market_squawk_sources::{
    OnboardingEvent, OnboardingState, ProbeTransport, ProfileReleaseState,
    ProviderOnboardingProfile, ProviderProfileError, ProviderProfileRegistry,
    ProviderPublicConfiguration, built_in_provider_profiles, install_ring_tls_provider,
};
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::contracts::{
    OnboardingSessionView, ProviderProfileRegistration, ProviderProfileView, session_view,
};

const SESSION_DURATION: Duration = Duration::from_secs(15 * 60);
const SECRET_OPERATION_DURATION: Duration = Duration::from_secs(30);
const MAX_PROBE_BODY_BYTES: usize = 1024 * 1024;
const MAX_CONTACT_BYTES: usize = 128;

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
}

impl ProviderOnboardingService {
    /// Constructs the service from code-owned profiles and an already selected secret backend.
    pub fn try_new<S>(
        catalog: OnboardingCatalogCapability,
        secrets: Arc<S>,
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
        Ok(Self {
            profiles: built_in_provider_profiles()?,
            catalog,
            secrets,
            client,
        })
    }

    /// Returns every built-in profile in stable identity order.
    pub fn profiles(&self) -> Vec<ProviderProfileView> {
        self.profiles.iter().map(Into::into).collect()
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
        let outcome = self
            .catalog
            .register_provider_capability(profile.capability())?;
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
        self.session_views(sessions)
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
        self.catalog
            .register_provider_capability(profile.capability())?;
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

    /// Imports one secret directly into the selected store and records only opaque evidence.
    pub fn submit_secret(
        &self,
        session_id: Uuid,
        secret: SecretValue,
        cancellation: SecretCancellation,
    ) -> Result<OnboardingSessionView, ProviderOnboardingError> {
        let resumed = self.catalog.resume_provider_onboarding(session_id)?;
        let profile = self.profile_for(&resumed)?;
        if profile.release_state() == ProfileReleaseState::RightsBlocked
            || resumed.lifecycle().state() != OnboardingState::UserActionRequired
        {
            return Err(ProviderOnboardingError::SecretImportUnavailable);
        }
        validate_secret_shape(profile, &secret)?;
        let generation = resumed
            .lifecycle()
            .candidate_generation()
            .ok_or(ProviderOnboardingError::InvalidSessionState)?;
        let reservation = resumed.reservation().clone();
        let mut sequence = resumed.next_sequence();
        self.append(
            &reservation,
            sequence,
            OnboardingEvent::CredentialImported {
                generation,
                evidence_digest: event_digest(b"credential-imported", session_id, Some(generation)),
            },
        )?;
        sequence = checked_next(sequence)?;
        self.append(
            &reservation,
            sequence,
            OnboardingEvent::ProtocolValidated {
                generation,
                evidence_digest: event_digest(b"protocol-validated", session_id, Some(generation)),
            },
        )?;
        sequence = checked_next(sequence)?;

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
            SecretInteractionPolicy::Forbid,
            cancellation,
        )?;
        let expected = SecretValue::new(secret.expose_secret().to_owned())
            .map_err(|_| ProviderOnboardingError::InvalidSecretShape)?;
        let reference = match self
            .secrets
            .create(&secret_key, generation, secret, &control)
        {
            Ok(reference) => reference,
            Err(LocalSecretStoreError::IndeterminateCompletion) => {
                self.append(
                    &reservation,
                    sequence,
                    OnboardingEvent::CleanupRequired {
                        generation: Some(generation),
                        evidence_digest: event_digest(
                            b"secret-store-indeterminate",
                            session_id,
                            Some(generation),
                        ),
                    },
                )?;
                return self.resume(session_id);
            }
            Err(error) => {
                self.append(
                    &reservation,
                    sequence,
                    OnboardingEvent::Cancelled {
                        evidence_digest: event_digest(
                            b"secret-store-rejected",
                            session_id,
                            Some(generation),
                        ),
                    },
                )?;
                return Err(error.into());
            }
        };
        let readback_matches =
            self.secrets
                .read(&reference, &control)
                .ok()
                .is_some_and(|readback| {
                    let left = readback.expose_secret().as_bytes();
                    let right = expected.expose_secret().as_bytes();
                    left.len() == right.len() && bool::from(left.ct_eq(right))
                });
        if !readback_matches {
            let cleanup_event = if self.secrets.delete(&reference, &control).is_ok() {
                OnboardingEvent::Cancelled {
                    evidence_digest: event_digest(
                        b"secret-readback-failed-clean",
                        session_id,
                        Some(generation),
                    ),
                }
            } else {
                OnboardingEvent::CleanupRequired {
                    generation: Some(generation),
                    evidence_digest: event_digest(
                        b"secret-readback-failed-cleanup",
                        session_id,
                        Some(generation),
                    ),
                }
            };
            self.append(&reservation, sequence, cleanup_event)?;
            return Err(ProviderOnboardingError::SecretVerificationFailed);
        }
        let stored_event = OnboardingEvent::CredentialStored {
            reference: reference.clone(),
        };
        if let Err(catalog_error) = self.append(&reservation, sequence, stored_event) {
            let recorded = self
                .catalog
                .resume_provider_onboarding(session_id)
                .ok()
                .is_some_and(|resumed_after_error| {
                    resumed_after_error
                        .lifecycle()
                        .generation_reference(generation)
                        == Some(&reference)
                });
            if !recorded {
                let deletion_failed = self.secrets.delete(&reference, &control).is_err();
                if let Ok(resumed_after_error) = self.catalog.resume_provider_onboarding(session_id)
                {
                    let event = if deletion_failed {
                        OnboardingEvent::CleanupRequired {
                            generation: Some(generation),
                            evidence_digest: event_digest(
                                b"catalog-store-reconciliation",
                                session_id,
                                Some(generation),
                            ),
                        }
                    } else {
                        OnboardingEvent::Cancelled {
                            evidence_digest: event_digest(
                                b"catalog-store-rolled-back",
                                session_id,
                                Some(generation),
                            ),
                        }
                    };
                    let _ignored = self.append(
                        resumed_after_error.reservation(),
                        resumed_after_error.next_sequence(),
                        event,
                    );
                }
                return Err(catalog_error);
            }
        }
        if profile.release_state() == ProfileReleaseState::RefreshRequired {
            let resumed = self.catalog.resume_provider_onboarding(session_id)?;
            self.append(
                resumed.reservation(),
                resumed.next_sequence(),
                OnboardingEvent::RefreshRequired {
                    evidence_digest: event_digest(
                        b"refresh-required",
                        session_id,
                        Some(generation),
                    ),
                },
            )?;
        }
        self.resume(session_id)
    }

    /// Replays one exact durable session, closes safe refresh recovery, and returns status.
    pub fn resume(
        &self,
        session_id: Uuid,
    ) -> Result<OnboardingSessionView, ProviderOnboardingError> {
        let mut resumed = self.catalog.resume_provider_onboarding(session_id)?;
        let mut profile = self.profile_for(&resumed)?;
        if profile.release_state() == ProfileReleaseState::RefreshRequired
            && matches!(
                resumed.lifecycle().state(),
                OnboardingState::AnonymousAvailable | OnboardingState::StoredUnverified
            )
        {
            self.append(
                resumed.reservation(),
                resumed.next_sequence(),
                OnboardingEvent::RefreshRequired {
                    evidence_digest: event_digest(
                        b"refresh-required",
                        session_id,
                        resumed.lifecycle().candidate_generation(),
                    ),
                },
            )?;
            resumed = self.catalog.resume_provider_onboarding(session_id)?;
            profile = self.profile_for(&resumed)?;
        }
        Ok(session_view(profile, &resumed))
    }

    /// Permanently blocks later activation for one session.
    pub fn cancel(
        &self,
        session_id: Uuid,
    ) -> Result<OnboardingSessionView, ProviderOnboardingError> {
        let resumed = self.catalog.resume_provider_onboarding(session_id)?;
        self.append(
            resumed.reservation(),
            resumed.next_sequence(),
            OnboardingEvent::Cancelled {
                evidence_digest: event_digest(b"user-cancelled", session_id, None),
            },
        )?;
        self.resume(session_id)
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
                decision_digest: profile_rights_digest(profile),
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
            Ok(evidence_digest) => {
                self.append(
                    reservation,
                    3,
                    OnboardingEvent::RuntimeVerified {
                        generation: None,
                        evidence_digest,
                    },
                )?;
                self.append(
                    reservation,
                    4,
                    OnboardingEvent::Activate { generation: None },
                )
            }
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
            return Ok(profile_rights_digest(profile));
        }
        let endpoint = probe
            .endpoint()
            .ok_or(ProviderOnboardingError::InvalidProfile)?;
        let policy = probe
            .endpoint_policy()
            .ok_or(ProviderOnboardingError::InvalidProfile)?;
        policy.authorize_request(endpoint)?;
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
        let response = tokio::select! {
            () = cancellation.cancelled() => {
                return Err(ProviderOnboardingError::OperationCancelled);
            }
            response = request.send() => response.map_err(|_| ProviderOnboardingError::ProbeUnavailable)?,
        };
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
                () = cancellation.cancelled() => {
                    return Err(ProviderOnboardingError::OperationCancelled);
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
        validate_probe_semantics(profile.id(), &body)?;
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&body).into(),
        ))
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

    fn profile_for<'a>(
        &'a self,
        resumed: &ResumedProviderOnboarding,
    ) -> Result<&'a ProviderOnboardingProfile, ProviderOnboardingError> {
        let profile = self
            .profiles
            .get(resumed.lifecycle().surface_id().as_str())
            .ok_or(ProviderOnboardingError::UnknownProfile)?;
        if profile.capability().revision() != resumed.lifecycle().capability_revision()
            || profile.capability().content_digest() != resumed.lifecycle().capability_digest()
        {
            return Err(ProviderOnboardingError::InvalidSessionState);
        }
        validate_recovered_public_configuration(profile, resumed.public_configuration())?;
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
        let prefix = &body[..body.len().min(512)];
        return if prefix.windows(4).any(|window| window == b"<feed")
            || prefix.windows(5).any(|window| window == b"<?xml")
        {
            Ok(())
        } else {
            Err(ProviderOnboardingError::ProbeUnavailable)
        };
    }
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| ProviderOnboardingError::ProbeUnavailable)?;
    let valid = match profile_id {
        "coinbase.public-market-data" => value.get("product_id").is_some(),
        "kraken.spot-public-market-data" => {
            value
                .get("error")
                .and_then(serde_json::Value::as_array)
                .is_some()
                && value.get("result").is_some()
        }
        "sec.edgar-public" => value.get("cik").is_some() && value.get("filings").is_some(),
        "bls.v1-unregistered" | "bls.v2-registered" => {
            value.get("status").is_some() && value.get("Results").is_some()
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

fn profile_rights_digest(profile: &ProviderOnboardingProfile) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk-provider-rights-v1\0");
    hasher.update(profile.id().as_bytes());
    for right in profile.rights().0 {
        hasher.update(right.operation().evidence_name().as_bytes());
        hasher.update([0]);
        hasher.update(right.admission().evidence_name().as_bytes());
        hasher.update([0]);
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
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

fn checked_next(sequence: u64) -> Result<u64, ProviderOnboardingError> {
    sequence
        .checked_add(1)
        .ok_or(ProviderOnboardingError::InvalidSessionState)
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
    /// Exact secure-store readback did not reproduce the submitted value.
    #[error("provider onboarding secret-store verification failed")]
    SecretVerificationFailed,
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
    /// Cooperative cancellation completed before activation.
    #[error("provider onboarding operation was cancelled")]
    OperationCancelled,
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
