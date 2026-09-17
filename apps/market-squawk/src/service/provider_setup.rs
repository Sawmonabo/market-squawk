//! Native connection setup over the sole installed provider and credential authorities.

use std::{fmt, sync::Arc, time::Instant};

use market_squawk_data::CatalogLimit;
use market_squawk_domain::SourceIdentifier;
use market_squawk_platform::SecretValue;
use market_squawk_runtime::{
    ClientId, InputStager, InputTicketId, OperationEffect, RuntimeIdentity,
};
use market_squawk_services::{RequestContext, ServiceError, ToolResultMetadata, TypedToolResult};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use zeroize::Zeroizing;

use crate::application::source::{
    SourceLifecycleAction, SourceLifecycleAuthority, SourceLifecycleCommand,
    SourceLifecycleCommandInput, SourceLifecycleDisposition,
};
use crate::provider_onboarding::{
    OnboardingNextAction, ProviderOnboardingError, ProviderOnboardingRequest,
    ProviderOnboardingService, StartOnboardingRequest,
};
use crate::{LocalProduct, ProviderPortalActivationAuthority, ProviderPortalActivationError};

const GET_STATE: &str = "Source.Onboarding.GetState";
const APPLY: &str = "Source.Onboarding.Apply";
const APPLY_STAGED: &str = "Source.Onboarding.ApplyStaged";
const STAGED_SCHEMA: &str = "market-squawk.provider-setup.v1";
const MAXIMUM_SETUP_BYTES: u64 = 1024 * 1024;
const MAXIMUM_SESSIONS: usize = 32;

pub(super) struct InstalledProviderSetup {
    onboarding: Arc<ProviderOnboardingService>,
    activation: Arc<dyn ProviderPortalActivationAuthority>,
    lifecycle: Arc<dyn SourceLifecycleAuthority>,
    runtime: RuntimeIdentity,
    desktop_client: ClientId,
    cli_client: ClientId,
    inputs: Arc<InputStager>,
    start_gate: Arc<tokio::sync::Mutex<()>>,
}

impl InstalledProviderSetup {
    pub(super) fn new(
        product: &LocalProduct,
        runtime: RuntimeIdentity,
        desktop_client: ClientId,
        cli_client: ClientId,
        inputs: Arc<InputStager>,
        start_gate: Arc<tokio::sync::Mutex<()>>,
    ) -> Self {
        Self {
            onboarding: product.provider_onboarding(),
            activation: product.provider_portal_activation(),
            lifecycle: product.source_lifecycle_authority(),
            runtime,
            desktop_client,
            cli_client,
            inputs,
            start_gate,
        }
    }

    pub(super) fn effect(operation: &str) -> Option<OperationEffect> {
        match operation {
            GET_STATE => Some(OperationEffect::Read),
            APPLY | APPLY_STAGED => Some(OperationEffect::Mutation),
            _ => None,
        }
    }

    // These native-only actions are intentionally absent from the MCP/tool registry. The shared
    // runtime still authenticates client/generation, limits requests, and fences mutation replay.
    pub(super) fn desktop_capabilities() -> Vec<Value> {
        [(GET_STATE, true), (APPLY, false)]
            .into_iter()
            .map(|(name, read_only)| {
                json!({
                    "name": name,
                    "version": "1.0.0",
                    "description": "Native connection setup using the installed source authority.",
                    "inputSchema": if read_only {
                json!({"type": "object", "additionalProperties": false, "properties": {"sessionId": {"type": "string", "format": "uuid"}}})
            } else {
                json!({"type": "object", "additionalProperties": false,
                    "required": ["request", "confirm"],
                    "properties": {
                        "confirm": {"const": true},
                        "request": {"type": "object", "required": ["action"],
                            "properties": {"action": {"enum": ["start", "resume", "unlockFallback", "lockFallback", "submitSecret", "activate", "verifySaved", "restoreSaved", "resumePublication", "schwabOAuth", "renew", "cleanup", "cancel"]}}
                        }
                    }
                })
            },
                    "outputSchema": {"type": "object"},
                    "contract": {
                        "domain": "Source",
                        "authorization": if read_only { "read_only" } else { "local_confirmation" },
                    },
                    "metadata": {"privateInstalledClient": true},
                    "effects": {"readOnly": read_only, "destructive": !read_only,
                        "idempotent": read_only, "openWorld": !read_only},
                })
            })
            .collect()
    }

    pub(super) async fn call(
        &self,
        operation: &str,
        arguments: &Map<String, Value>,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let required_client = if operation == APPLY_STAGED {
            self.cli_client
        } else {
            self.desktop_client
        };
        if origin.workspace_id() != self.runtime.workspace_id().as_uuid()
            || origin.client_id() != required_client.as_uuid()
        {
            return Err(ServiceError::Unauthorized);
        }
        ensure_live(context)?;
        let data = if operation == GET_STATE {
            if !arguments.is_empty() {
                let input: InspectInput = serde_json::from_value(Value::Object(arguments.clone()))
                    .map_err(|_| ServiceError::InvalidRequest)?;
                let session = self
                    .onboarding
                    .retained_session_view(input.session_id)
                    .map_err(|_| ServiceError::Unavailable)?;
                let pending = tokio::select! {
                    biased;
                    () = context.cancellation().cancelled() => return Err(ServiceError::Cancelled),
                    () = tokio::time::sleep_until(context.deadline().into()) => return Err(ServiceError::DeadlineExceeded),
                    result = self.activation.setup_publication_pending(input.session_id) => result,
                };
                ensure_live(context)?;
                return TypedToolResult::try_new(
                    json!({"session":session,"publicationPending":pending}),
                    1,
                    ToolResultMetadata::complete_not_applicable(),
                    context.limits(),
                )
                .map(|result| {
                    result.into_envelope(
                        market_squawk_services::ResultEnvelopeProjection::NativeEvidenceV1,
                    )
                })
                .map_err(Into::into);
            }
            let profiles = self.onboarding.profiles();
            let mut sessions = self
                .onboarding
                .current_sessions(session_limit()?)
                .map_err(|_| ServiceError::Unavailable)?;
            let setup = profiles
                .iter()
                .map(|profile| {
                    let surface = SourceIdentifier::try_from(profile.id())
                        .map_err(|_| ServiceError::Unavailable)?;
                    let saved_session = self
                        .activation
                        .retained_setup_session(&surface)
                        .map_err(|_| ServiceError::Unavailable)?;
                    if let Some(session_id) = saved_session {
                        if !sessions
                            .iter()
                            .any(|session| session.session_id() == session_id)
                        {
                            let saved = self
                                .onboarding
                                .retained_session_view(session_id)
                                .map_err(|_| ServiceError::Unavailable)?;
                            if saved.surface_id() != profile.id() {
                                return Err(ServiceError::Unavailable);
                            }
                            sessions.retain(|session| session.surface_id() != profile.id());
                            sessions.push(saved);
                        }
                    }
                    Ok(json!({
                        "surfaceId": profile.id(), "activationKind": activation_kind(profile.id()),
                        "savedConfigurationSessionId": saved_session,
                    }))
                })
                .collect::<Result<Vec<_>, ServiceError>>()?;
            json!({
                "profiles": profiles, "sessions": sessions, "setup": setup,
                "encryptedFileFallback": self.onboarding.encrypted_file_fallback_status()
                    .map_err(|_| ServiceError::Unavailable)?,
            })
        } else if operation == APPLY || operation == APPLY_STAGED {
            let input: ApplyInput = if operation == APPLY_STAGED {
                let staged: StagedInput = serde_json::from_value(Value::Object(arguments.clone()))
                    .map_err(|_| ServiceError::InvalidRequest)?;
                if !staged.confirm {
                    return Err(ServiceError::Unauthorized);
                }
                let ticket = InputTicketId::try_from_uuid(staged.input_ticket_id)
                    .map_err(|_| ServiceError::InvalidRequest)?;
                let media_type = SourceIdentifier::try_from(STAGED_SCHEMA)
                    .map_err(|_| ServiceError::Unavailable)?;
                let claimed = self
                    .inputs
                    .claim(
                        ticket,
                        self.cli_client,
                        &media_type,
                        super::runtime::current_timestamp()
                            .map_err(|_| ServiceError::Unavailable)?,
                    )
                    .map_err(|_| ServiceError::Unauthorized)?;
                let bytes = claimed
                    .read_verified(MAXIMUM_SETUP_BYTES)
                    .map(Zeroizing::new)
                    .map_err(|_| ServiceError::InvalidRequest)?;
                let request: StagedSetup =
                    serde_json::from_slice(&bytes).map_err(|_| ServiceError::InvalidRequest)?;
                drop(bytes);
                if request.schema != STAGED_SCHEMA
                    || !matches!(
                        request.request,
                        ProviderOnboardingRequest::UnlockFallback { .. }
                            | ProviderOnboardingRequest::Activate { .. }
                            | ProviderOnboardingRequest::VerifySaved { .. }
                            | ProviderOnboardingRequest::RestoreSaved { .. }
                            | ProviderOnboardingRequest::ResumePublication { .. }
                    )
                {
                    return Err(ServiceError::InvalidRequest);
                }
                ApplyInput {
                    request: request.request,
                    confirm: true,
                }
            } else {
                serde_json::from_value(Value::Object(arguments.clone()))
                    .map_err(|_| ServiceError::InvalidRequest)?
            };
            if !input.confirm
                || matches!(
                    input.request,
                    ProviderOnboardingRequest::Bootstrap
                        | ProviderOnboardingRequest::Inspect { .. }
                )
            {
                return Err(ServiceError::Unauthorized);
            }
            let cancellation = context.cancellation().child_token();
            let _cancel_on_exit = cancellation.clone().drop_guard();
            let outcome = tokio::select! {
                biased;
                () = context.cancellation().cancelled() => return Err(ServiceError::Cancelled),
                () = tokio::time::sleep_until(context.deadline().into()) => return Err(ServiceError::DeadlineExceeded),
                result = self.apply(input.request, cancellation, context.deadline()) => result,
            };
            match outcome {
                Ok(value) => json!({"outcome": "completed", "value": value}),
                Err(error) => {
                    json!({"outcome": "rejected", "code": error.code, "message": error.message})
                }
            }
        } else {
            return Err(ServiceError::InvalidRequest);
        };
        ensure_live(context)?;
        TypedToolResult::try_new(
            data,
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map(|result| {
            result.into_envelope(market_squawk_services::ResultEnvelopeProjection::NativeEvidenceV1)
        })
        .map_err(Into::into)
    }

    async fn apply(
        &self,
        request: ProviderOnboardingRequest,
        cancellation: tokio_util::sync::CancellationToken,
        deadline: Instant,
    ) -> Result<Value, SetupFailure> {
        match request {
            ProviderOnboardingRequest::Bootstrap | ProviderOnboardingRequest::Inspect { .. } => {
                Err(SetupFailure::invalid())
            }
            ProviderOnboardingRequest::Start {
                surface_id,
                organization,
                administrative_email,
            } => {
                // The importer and every client share this catalog. Selecting an existing setup
                // must keep its stored secret generation and durable identity rather than import again.
                let _guard = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Err(SetupFailure::cancelled()),
                    guard = self.start_gate.lock() => guard,
                };
                let surface = SourceIdentifier::try_from(surface_id.as_str())
                    .map_err(|_| SetupFailure::invalid())?;
                if let Some(session_id) = self.activation.retained_setup_session(&surface)? {
                    return serialize(self.onboarding.resume(session_id)?);
                }
                let sessions = self.onboarding.current_sessions(
                    CatalogLimit::new(MAXIMUM_SESSIONS).map_err(|_| SetupFailure::unavailable())?,
                )?;
                if let Some(session) = sessions
                    .iter()
                    .find(|session| session.surface_id() == surface_id)
                {
                    let current = self.onboarding.resume(session.session_id())?;
                    if current.credential_stored()
                        || !matches!(
                            current.next_action(),
                            OnboardingNextAction::StartNewSession | OnboardingNextAction::None
                        )
                    {
                        return serialize(current);
                    }
                }
                let request = StartOnboardingRequest::try_new(
                    surface_id,
                    organization,
                    administrative_email,
                )?;
                serialize(self.onboarding.start_deferred(request)?)
            }
            ProviderOnboardingRequest::Resume { session_id } => {
                serialize(self.onboarding.resume(session_id)?)
            }
            ProviderOnboardingRequest::UnlockFallback { mut secret } => serialize(
                self.onboarding
                    .unlock_encrypted_file_fallback(
                        SecretValue::new(std::mem::take(&mut *secret))
                            .map_err(|_| SetupFailure::invalid())?,
                        cancellation,
                    )
                    .await?,
            ),
            ProviderOnboardingRequest::LockFallback => serialize(
                self.onboarding
                    .lock_encrypted_file_fallback(cancellation)
                    .await?,
            ),
            ProviderOnboardingRequest::SubmitSecret {
                session_id,
                mut secret,
            } => serialize(
                self.onboarding
                    .submit_secret(
                        session_id,
                        SecretValue::new(std::mem::take(&mut *secret))
                            .map_err(|_| SetupFailure::invalid())?,
                        cancellation,
                    )
                    .await?,
            ),
            ProviderOnboardingRequest::Activate {
                session_id,
                request,
            } => {
                let view = self
                    .activation
                    .activate(session_id, request, cancellation.child_token())
                    .await?;
                self.connect_configuration(session_id, false, &cancellation, deadline)
                    .await?;
                serialize(view)
            }
            ProviderOnboardingRequest::VerifySaved { session_id } => {
                let view = self
                    .activation
                    .verify_saved_setup(session_id, cancellation.child_token())
                    .await?;
                self.connect_configuration(session_id, false, &cancellation, deadline)
                    .await?;
                serialize(view)
            }
            ProviderOnboardingRequest::RestoreSaved { session_id } => {
                self.connect_configuration(session_id, true, &cancellation, deadline)
                    .await?;
                serialize(self.onboarding.resume(session_id)?)
            }
            ProviderOnboardingRequest::ResumePublication { session_id } => serialize(
                self.activation
                    .resume_research_publication(session_id, cancellation)
                    .await?,
            ),
            ProviderOnboardingRequest::SchwabOAuth {
                session_id,
                lifecycle_action,
            } => serialize(
                self.activation
                    .schwab_oauth(session_id, lifecycle_action, cancellation)
                    .await?,
            ),
            ProviderOnboardingRequest::Renew { session_id } => {
                serialize(self.onboarding.begin_renewal(session_id).await?)
            }
            ProviderOnboardingRequest::Cleanup { session_id } => serialize(
                self.onboarding
                    .reconcile_cleanup(session_id, cancellation)
                    .await?,
            ),
            ProviderOnboardingRequest::Cancel { session_id } => {
                serialize(self.activation.cancel(session_id, cancellation).await?)
            }
        }
    }

    async fn connect_configuration(
        &self,
        session_id: uuid::Uuid,
        restore_saved: bool,
        cancellation: &tokio_util::sync::CancellationToken,
        deadline: Instant,
    ) -> Result<(), SetupFailure> {
        let session = self.onboarding.resume(session_id)?;
        let provider = SourceIdentifier::try_from(session.surface_id())
            .map_err(|_| SetupFailure::invalid())?;
        if restore_saved && self.activation.retained_setup_session(&provider)? != Some(session_id) {
            return Err(SetupFailure::invalid());
        }
        let status = self
            .lifecycle
            .status(&provider, cancellation, deadline)
            .await
            .map_err(|_| SetupFailure::unavailable())?;
        let lease = if restore_saved {
            None
        } else {
            Some(self.onboarding.activation_lease(session_id)?)
        };
        let command = SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
            provider,
            action: if restore_saved {
                SourceLifecycleAction::Retry
            } else {
                SourceLifecycleAction::Reconfigure
            },
            expected_state_revision: status.fields().state_revision,
            expected_generation: None,
            expected_runtime_generation_digest: None,
            onboarding_session_id: lease.as_ref().map(|lease| lease.session_id()),
            public_configuration_digest: lease
                .as_ref()
                .map(|lease| lease.public_configuration_digest()),
            reason: if restore_saved {
                Some(
                    SourceIdentifier::try_from("console-saved-setup")
                        .map_err(|_| SetupFailure::invalid())?,
                )
            } else {
                None
            },
            cancellation: cancellation.child_token(),
            deadline,
        })
        .map_err(|_| SetupFailure::invalid())?;
        let receipt = self
            .lifecycle
            .execute(command)
            .await
            .map_err(|_| SetupFailure::unavailable())?;
        if !matches!(
            receipt.fields().disposition,
            SourceLifecycleDisposition::Applied | SourceLifecycleDisposition::Replay
        ) {
            return Err(SetupFailure::unavailable());
        }
        Ok(())
    }
}

impl fmt::Debug for InstalledProviderSetup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstalledProviderSetup")
            .field("runtime", &self.runtime)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InspectInput {
    session_id: uuid::Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplyInput {
    request: ProviderOnboardingRequest,
    confirm: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StagedInput {
    input_ticket_id: uuid::Uuid,
    confirm: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StagedSetup {
    schema: String,
    request: ProviderOnboardingRequest,
}

fn session_limit() -> Result<CatalogLimit, ServiceError> {
    CatalogLimit::new(MAXIMUM_SESSIONS).map_err(|_| ServiceError::Unavailable)
}

fn activation_kind(surface: &str) -> Option<&'static str> {
    match surface {
        "coinbase.public-market-data"
        | "coinbase.exchange-direct-market-data"
        | "kraken.spot-public-market-data" => Some("source"),
        market_squawk_sources::SEC_EDGAR_PROFILE_ID => Some("sec"),
        "bls.v1-unregistered" | "bls.v2-registered" => Some("bls"),
        "bea.api-data" => Some("bea"),
        "census.data-api" => Some("census"),
        "treasury.fiscal-data" => Some("treasury_fiscal"),
        "treasury.daily-rates-xml" => Some("treasury_daily_rates"),
        "fred-alfred.api-v1-v2" => Some("fred_alfred"),
        "eia.api-v2" => Some("eia_electricity_price"),
        "federal-reserve-board.data-download-program" => Some("federal_reserve_board_h15"),
        "yahoo-finance.experimental-enrichment" => Some("yahoo_enrichment"),
        "tiingo.starter-eod-nav" => Some("tiingo_starter_eod_nav"),
        _ => None,
    }
}

fn serialize(value: impl serde::Serialize) -> Result<Value, SetupFailure> {
    serde_json::to_value(value).map_err(|_| SetupFailure::unavailable())
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

struct SetupFailure {
    code: &'static str,
    message: &'static str,
}
impl SetupFailure {
    const fn invalid() -> Self {
        Self {
            code: "invalid_request",
            message: "Check the selected provider and the setup fields, then try again.",
        }
    }
    const fn unavailable() -> Self {
        Self {
            code: "provider_unavailable",
            message: "This connection could not be completed. Refresh its saved state and use the recovery action shown.",
        }
    }
    const fn cancelled() -> Self {
        Self {
            code: "cancelled",
            message: "Setup was interrupted. Refresh to resume its saved progress.",
        }
    }
}
impl From<ProviderOnboardingError> for SetupFailure {
    fn from(error: ProviderOnboardingError) -> Self {
        match error {
            ProviderOnboardingError::CredentialRejected => Self {
                code: "credential_rejected",
                message: "The provider rejected the saved credential. Replace it with a current credential and verify again.",
            },
            ProviderOnboardingError::ProbeRateLimited => Self {
                code: "rate_limited",
                message: "The provider asked Market Squawk to wait. Saved setup is retained; try verification later.",
            },
            ProviderOnboardingError::AdministrativeContactRequired => Self {
                code: "contact_required",
                message: "This provider needs your organization or name and a contact email.",
            },
            ProviderOnboardingError::SecretCleanupUnavailable
            | ProviderOnboardingError::RemoteReconciliationRequired => Self {
                code: "cleanup_required",
                message: "This connection needs cleanup before another attempt. Use Reconcile saved setup.",
            },
            ProviderOnboardingError::InvalidRequest
            | ProviderOnboardingError::InvalidSecretShape => Self::invalid(),
            ProviderOnboardingError::OperationCancelled
            | ProviderOnboardingError::ProbeDeadlineExceeded => Self::cancelled(),
            _ => Self::unavailable(),
        }
    }
}
impl From<ProviderPortalActivationError> for SetupFailure {
    fn from(error: ProviderPortalActivationError) -> Self {
        match error {
            ProviderPortalActivationError::InvalidRequest => Self::invalid(),
            ProviderPortalActivationError::Cancelled => Self::cancelled(),
            ProviderPortalActivationError::Unavailable
            | ProviderPortalActivationError::StateUnavailable => Self::unavailable(),
        }
    }
}
