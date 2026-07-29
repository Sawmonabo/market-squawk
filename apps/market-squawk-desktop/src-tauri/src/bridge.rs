//! Least-privilege Tauri bridge over the existing local application authorities.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk::{
    AppConfig, LocalProduct, OnboardingNextAction, OnboardingSessionView, ProviderOnboardingError,
    ProviderPortalActivationAuthority, ProviderPortalActivationError, StartOnboardingRequest,
};
use market_squawk_data::CatalogLimit;
use market_squawk_platform::SecretValue;
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ServiceError, ServiceLimits,
    validate_json_contract,
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use tauri::State;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::contracts::{
    ApplicationInvocation, DesktopBootstrap, DesktopCommandError, OperationSummary,
    ProviderOnboardingCommand, Readiness, ReadinessState, SetupStep, SetupStepAction,
    SetupStepState,
};

const MAXIMUM_APPLICATION_ARGUMENT_BYTES: usize = 256 * 1024;
const MAXIMUM_DESKTOP_RESULT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_DESKTOP_RESULT_ITEMS: u64 = 1_000;
const MAXIMUM_OPERATION_BYTES: usize = 128;
const MAXIMUM_PROVIDER_SESSIONS: usize = 32;
const APPLICATION_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const PROVIDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const SOURCE_SETUP_OPERATION: &str = "Source.Setup";

#[derive(Clone, Copy, Debug)]
enum InvocationAuthority {
    ReadOnly,
    ExactConfirmed(&'static str),
}

pub(crate) struct DesktopState {
    product: LocalProduct,
    config: AppConfig,
    portal_activation: Arc<dyn ProviderPortalActivationAuthority>,
    cancellation: CancellationToken,
}

impl DesktopState {
    pub(crate) fn new(product: LocalProduct, config: AppConfig) -> Self {
        let portal_activation = product.provider_portal_activation();
        Self {
            product,
            config,
            portal_activation,
            cancellation: CancellationToken::new(),
        }
    }

    fn bootstrap(&self) -> Result<DesktopBootstrap, DesktopCommandError> {
        let onboarding = self.product.provider_onboarding();
        let session_limit = CatalogLimit::new(MAXIMUM_PROVIDER_SESSIONS)
            .map_err(|_error| DesktopCommandError::internal())?;
        let profiles = serialize(onboarding.profiles())?;
        let session_views = onboarding
            .current_sessions(session_limit)
            .map_err(map_onboarding_error)?;
        let sessions = serialize(&session_views)?;
        let fallback = serialize(
            onboarding
                .encrypted_file_fallback_status()
                .map_err(map_onboarding_error)?,
        )?;
        let operations = self
            .product
            .application()
            .capabilities()
            .tools()
            .iter()
            .map(|tool| {
                let contract = tool.contract();
                let effects = tool.effects();
                OperationSummary::new(
                    tool.name().to_owned(),
                    tool.description().to_owned(),
                    contract.domain().as_str(),
                    contract.authorization().as_str(),
                    effects.read_only(),
                    effects.destructive(),
                    Value::Object(tool.input_schema().clone()),
                )
            })
            .collect();
        let model_runtime = if self.product.model_runtime().is_some() {
            Readiness::new(
                ReadinessState::Ready,
                "Verified",
                "The configured local training release and native inference runtime were admitted.",
            )
        } else {
            Readiness::new(
                ReadinessState::NotConfigured,
                "Not configured",
                "No verified local training release is configured for this workspace.",
            )
        };
        let paper_mode_enabled = self.config.paper_bot_enabled();
        let installation_verified = false;
        let model_runtime_ready = self.product.model_runtime().is_some();
        let setup_steps = setup_steps(
            &session_views,
            installation_verified,
            model_runtime_ready,
            paper_mode_enabled,
        );
        Ok(DesktopBootstrap::new(
            env!("CARGO_PKG_VERSION"),
            if cfg!(debug_assertions) {
                "development"
            } else {
                "release"
            },
            self.product.paths().root().display().to_string(),
            Readiness::new(
                ReadinessState::Ready,
                "Ready",
                "The controlled local workspace and catalogs opened successfully.",
            ),
            Readiness::new(
                ReadinessState::Unverified,
                "Not verified",
                "No signed installation receipt was admitted for this running build.",
            ),
            model_runtime,
            Readiness::new(
                ReadinessState::Available,
                "Available",
                "The bounded local stdio MCP service is available when explicitly started.",
            ),
            paper_mode_enabled,
            fallback,
            profiles,
            sessions,
            setup_steps,
            operations,
        ))
    }

    pub(crate) fn begin_shutdown(&self) {
        self.cancellation.cancel();
        self.product.application().begin_shutdown();
    }

    pub(crate) async fn finish_shutdown(&self) {
        self.begin_shutdown();
        let application = self.product.application();
        let Some(deadline) = Instant::now().checked_add(application.shutdown_timeout()) else {
            return;
        };
        let _report = application.shutdown(deadline).await;
    }
}

fn setup_steps(
    sessions: &[OnboardingSessionView],
    installation_verified: bool,
    model_runtime_ready: bool,
    paper_mode_enabled: bool,
) -> Vec<SetupStep> {
    let sources_ready = sessions.iter().any(|session| {
        !session.surface_id().starts_with("local.")
            && session.next_action() == OnboardingNextAction::Active
    });
    let files_ready = has_active_session(sessions, "local.files");
    let portfolio_ready = has_active_session(sessions, "local.portfolio-imports");
    let paper_profile_ready = has_active_session(sessions, "local.paper-execution");
    let research_ready = files_ready && model_runtime_ready;
    let paper_ready = paper_profile_ready && paper_mode_enabled;
    let review_ready =
        installation_verified && sources_ready && research_ready && portfolio_ready && paper_ready;

    vec![
        SetupStep::new(
            "system",
            "System",
            if installation_verified {
                SetupStepState::Complete
            } else {
                SetupStepState::Blocked
            },
            installation_verified,
            "Verify the exact installed Market Squawk release before relying on its identity.",
            (!installation_verified).then_some("This running build has no admitted installation receipt."),
            (!installation_verified).then_some(
                "Install an approved signed package, then reopen Market Squawk from that installation.",
            ),
            Some(SetupStepAction::ReviewInstallation),
        ),
        SetupStep::new(
            "storage",
            "Storage",
            SetupStepState::Complete,
            true,
            "The controlled workspace and catalog are open in the effective data directory.",
            None,
            None,
            None,
        ),
        SetupStep::new(
            "sources",
            "Sources",
            if sources_ready {
                SetupStepState::Complete
            } else {
                SetupStepState::ActionRequired
            },
            sources_ready,
            "Activate at least one supported external market or research source.",
            (!sources_ready).then_some("No external provider session currently holds active authority."),
            (!sources_ready).then_some(
                "Connect a supported zero-fee source and complete its provider-specific verification.",
            ),
            Some(SetupStepAction::ConfigureSources),
        ),
        SetupStep::new(
            "research",
            "Research",
            if research_ready {
                SetupStepState::Complete
            } else {
                SetupStepState::ActionRequired
            },
            research_ready,
            "Research readiness requires the local-file authority and an admitted local model runtime.",
            (!research_ready)
                .then_some("The local-file authority or verified training release is not ready."),
            (!research_ready).then_some(
                "Activate Local files and configure an approved absolute training-release root.",
            ),
            Some(SetupStepAction::ConfigureResearch),
        ),
        SetupStep::new(
            "portfolio",
            "Portfolio",
            if portfolio_ready {
                SetupStepState::Complete
            } else {
                SetupStepState::ActionRequired
            },
            portfolio_ready,
            "Portfolio imports become available only after their local authority is active.",
            (!portfolio_ready).then_some("The portfolio-import authority is not active."),
            (!portfolio_ready)
                .then_some("Activate Portfolio holdings and transactions imports."),
            Some(SetupStepAction::ConfigurePortfolio),
        ),
        SetupStep::new(
            "paper",
            "Paper",
            if paper_ready {
                SetupStepState::Complete
            } else {
                SetupStepState::ActionRequired
            },
            paper_ready,
            "Paper execution requires its local provider authority and explicit paper-mode configuration.",
            (!paper_ready)
                .then_some("Paper authority is inactive or paper mode is not enabled."),
            (!paper_ready).then_some(
                "Activate Local paper execution and restart with paper mode enabled in validated configuration.",
            ),
            Some(SetupStepAction::ConfigurePaper),
        ),
        SetupStep::new(
            "mcp",
            "MCP",
            SetupStepState::Available,
            true,
            "The bounded local stdio MCP server is installed and starts only on explicit request.",
            None,
            None,
            Some(SetupStepAction::ReviewMcp),
        ),
        SetupStep::new(
            "review",
            "Review",
            if review_ready {
                SetupStepState::Complete
            } else {
                SetupStepState::Blocked
            },
            review_ready,
            "Final readiness is derived from every required owning authority above.",
            (!review_ready).then_some("One or more required setup authorities remain incomplete."),
            (!review_ready)
                .then_some("Resolve each named blocker, refresh status, and review again."),
            Some(SetupStepAction::ReviewStatus),
        ),
    ]
}

fn has_active_session(sessions: &[OnboardingSessionView], surface_id: &str) -> bool {
    sessions.iter().any(|session| {
        session.surface_id() == surface_id && session.next_action() == OnboardingNextAction::Active
    })
}

#[tauri::command]
pub(crate) fn desktop_bootstrap(
    state: State<'_, DesktopState>,
) -> Result<DesktopBootstrap, DesktopCommandError> {
    state.bootstrap()
}

#[tauri::command]
pub(crate) async fn application_invoke(
    request: ApplicationInvocation,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    invoke_application(request, &state, InvocationAuthority::ReadOnly).await
}

async fn invoke_application(
    mut request: ApplicationInvocation,
    state: &DesktopState,
    authority: InvocationAuthority,
) -> Result<Value, DesktopCommandError> {
    if request.operation.is_empty()
        || request.operation.len() > MAXIMUM_OPERATION_BYTES
        || request.operation.chars().any(char::is_control)
    {
        return Err(DesktopCommandError::invalid_request(
            "The selected Market Squawk operation is invalid.",
        ));
    }
    let application = state.product.application();
    let capabilities = application.capabilities();
    let descriptor = capabilities
        .find(&request.operation)
        .ok_or_else(|| map_service_error(ServiceError::NotFound))?;
    let authorized = match authority {
        InvocationAuthority::ReadOnly => descriptor.effects().read_only(),
        InvocationAuthority::ExactConfirmed(operation) => request.operation == operation,
    };
    if !authorized {
        return Err(map_service_error(ServiceError::Unauthorized));
    }
    request.arguments.insert(
        "resultLimits".to_owned(),
        json!({
            "maximumItems": MAXIMUM_DESKTOP_RESULT_ITEMS,
            "maximumBytes": MAXIMUM_DESKTOP_RESULT_BYTES,
        }),
    );
    let input = Value::Object(request.arguments.clone());
    let input_structure = JsonStructureLimits::try_new(24, 64 * 1024, 4_096, 1_024)
        .map_err(|_error| DesktopCommandError::internal())?;
    validate_json_contract(&input, input_structure, MAXIMUM_APPLICATION_ARGUMENT_BYTES).map_err(
        |_error| {
            DesktopCommandError::invalid_request(
                "The operation input exceeds the desktop safety limits.",
            )
        },
    )?;

    let result_structure = JsonStructureLimits::try_new(32, 1024 * 1024, 10_000, 10_000)
        .map_err(|_error| DesktopCommandError::internal())?;
    let limits = ServiceLimits::try_new(
        1024 * 1024,
        1_000,
        4 * 1024 * 1024,
        10_000,
        result_structure,
    )
    .map_err(|_error| DesktopCommandError::internal())?;
    let deadline = Instant::now()
        .checked_add(APPLICATION_REQUEST_TIMEOUT)
        .ok_or_else(DesktopCommandError::internal)?;
    let cancellation = state.cancellation.child_token();
    let request_id = RequestId::try_string(format!("desktop-{}", Uuid::new_v4()))
        .map_err(|_error| DesktopCommandError::internal())?;
    let context = RequestContext::new(request_id, cancellation.clone(), deadline, limits);
    let operation = request.operation;
    let arguments = request.arguments;
    tokio::select! {
        biased;
        () = state.cancellation.cancelled() => {
            cancellation.cancel();
            Err(map_service_error(ServiceError::Cancelled))
        }
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            cancellation.cancel();
            Err(map_service_error(ServiceError::DeadlineExceeded))
        }
        result = application.invoke(&operation, arguments, context) => {
            result
                .map(|result| result.into_envelope())
                .map_err(map_service_error)
        }
    }
}

#[tauri::command]
pub(crate) async fn provider_onboarding(
    request: ProviderOnboardingCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    if request.requires_confirmation() && !confirmed {
        return Err(DesktopCommandError::new(
            "confirmation_required",
            "Confirm the provider change before continuing.",
        ));
    }
    let cancellation = state.cancellation.child_token();
    let operation_cancellation = cancellation.clone();
    let onboarding = state.product.provider_onboarding();
    let activation = Arc::clone(&state.portal_activation);
    let operation = async move {
        match request {
            ProviderOnboardingCommand::Bootstrap => provider_bootstrap(&onboarding),
            ProviderOnboardingCommand::Start {
                surface_id,
                organization,
                administrative_email,
            } => {
                let request =
                    StartOnboardingRequest::try_new(surface_id, organization, administrative_email)
                        .map_err(map_onboarding_error)?;
                serialize(
                    onboarding
                        .start(request, operation_cancellation)
                        .await
                        .map_err(map_onboarding_error)?,
                )
            }
            ProviderOnboardingCommand::Resume { session_id } => serialize(
                onboarding
                    .resume(session_id)
                    .map_err(map_onboarding_error)?,
            ),
            ProviderOnboardingCommand::UnlockFallback { secret } => {
                let secret = SecretValue::new(secret).map_err(|_error| {
                    DesktopCommandError::invalid_request(
                        "The encrypted-storage unlock value is invalid.",
                    )
                })?;
                serialize(
                    onboarding
                        .unlock_encrypted_file_fallback(secret, operation_cancellation)
                        .await
                        .map_err(map_onboarding_error)?,
                )
            }
            ProviderOnboardingCommand::LockFallback => serialize(
                onboarding
                    .lock_encrypted_file_fallback(operation_cancellation)
                    .await
                    .map_err(map_onboarding_error)?,
            ),
            ProviderOnboardingCommand::SubmitSecret { session_id, secret } => {
                let secret = SecretValue::new(secret).map_err(|_error| {
                    DesktopCommandError::invalid_request(
                        "The provider credential is empty or too large.",
                    )
                })?;
                serialize(
                    onboarding
                        .submit_secret(session_id, secret, operation_cancellation)
                        .await
                        .map_err(map_onboarding_error)?,
                )
            }
            ProviderOnboardingCommand::Activate {
                session_id,
                request,
            } => serialize(
                activation
                    .activate(session_id, request, operation_cancellation)
                    .await
                    .map_err(map_activation_error)?,
            ),
            ProviderOnboardingCommand::Renew { session_id } => serialize(
                onboarding
                    .begin_renewal(session_id)
                    .await
                    .map_err(map_onboarding_error)?,
            ),
            ProviderOnboardingCommand::Cleanup { session_id } => serialize(
                onboarding
                    .reconcile_cleanup(session_id, operation_cancellation)
                    .await
                    .map_err(map_onboarding_error)?,
            ),
            ProviderOnboardingCommand::Cancel { session_id } => serialize(
                activation
                    .cancel(session_id, operation_cancellation)
                    .await
                    .map_err(map_activation_error)?,
            ),
        }
    };
    tokio::select! {
        biased;
        () = state.cancellation.cancelled() => {
            cancellation.cancel();
            Err(DesktopCommandError::new(
                "cancelled",
                "The desktop is shutting down.",
            ))
        }
        () = tokio::time::sleep(PROVIDER_REQUEST_TIMEOUT) => {
            cancellation.cancel();
            Err(DesktopCommandError::new(
                "deadline_exceeded",
                "The provider operation exceeded its local deadline.",
            ))
        }
        result = operation => result,
    }
}

#[tauri::command]
pub(crate) fn open_official_provider_page(
    provider_id: String,
    state: State<'_, DesktopState>,
) -> Result<(), DesktopCommandError> {
    let profile = state
        .product
        .provider_onboarding()
        .profiles()
        .into_iter()
        .find(|profile| profile.id() == provider_id)
        .ok_or_else(|| {
            DesktopCommandError::invalid_request("The selected provider is not supported.")
        })?;
    tauri_plugin_opener::open_url(profile.official_handoff_url(), None::<&str>).map_err(|_error| {
        DesktopCommandError::new(
            "open_failed",
            "The official provider page could not be opened in the system browser.",
        )
    })
}

#[tauri::command]
pub(crate) async fn open_protected_provider_setup(
    provider_id: String,
    state: State<'_, DesktopState>,
) -> Result<(), DesktopCommandError> {
    let supported = state
        .product
        .provider_onboarding()
        .profiles()
        .into_iter()
        .any(|profile| profile.id() == provider_id);
    if !supported {
        return Err(DesktopCommandError::invalid_request(
            "The selected provider is not supported.",
        ));
    }
    let mut arguments = Map::new();
    arguments.insert("provider".to_owned(), Value::String(provider_id));
    arguments.insert("confirm".to_owned(), Value::Bool(true));
    let result = invoke_application(
        ApplicationInvocation {
            operation: SOURCE_SETUP_OPERATION.to_owned(),
            arguments,
        },
        &state,
        InvocationAuthority::ExactConfirmed(SOURCE_SETUP_OPERATION),
    )
    .await?;
    let portal_url = result
        .pointer("/data/portal/url")
        .and_then(Value::as_str)
        .ok_or_else(DesktopCommandError::internal)?;
    let parsed = Url::parse(portal_url).map_err(|_error| DesktopCommandError::internal())?;
    if parsed.scheme() != "http"
        || parsed.host_str() != Some("127.0.0.1")
        || parsed.port().is_none()
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(DesktopCommandError::internal());
    }
    tauri_plugin_opener::open_url(parsed.as_str(), None::<&str>).map_err(|_error| {
        DesktopCommandError::new(
            "open_failed",
            "The protected provider setup could not be opened in the system browser.",
        )
    })
}

fn provider_bootstrap(
    onboarding: &market_squawk::ProviderOnboardingService,
) -> Result<Value, DesktopCommandError> {
    let limit = CatalogLimit::new(MAXIMUM_PROVIDER_SESSIONS)
        .map_err(|_error| DesktopCommandError::internal())?;
    Ok(json!({
        "profiles": onboarding.profiles(),
        "sessions": onboarding
            .current_sessions(limit)
            .map_err(map_onboarding_error)?,
        "encryptedFileFallback": onboarding
            .encrypted_file_fallback_status()
            .map_err(map_onboarding_error)?,
    }))
}

fn serialize(value: impl Serialize) -> Result<Value, DesktopCommandError> {
    serde_json::to_value(value).map_err(|_error| DesktopCommandError::internal())
}

fn map_service_error(error: ServiceError) -> DesktopCommandError {
    DesktopCommandError::new(
        match error {
            ServiceError::InvalidRequest => "invalid_request",
            ServiceError::NotFound => "not_found",
            ServiceError::Unauthorized => "unauthorized",
            ServiceError::ResourceExhausted => "resource_exhausted",
            ServiceError::Cancelled => "cancelled",
            ServiceError::DeadlineExceeded => "deadline_exceeded",
            ServiceError::Unavailable => "unavailable",
            ServiceError::InvalidResult => "invalid_result",
            ServiceError::Internal => "internal",
        },
        error.to_string(),
    )
}

fn map_onboarding_error(error: ProviderOnboardingError) -> DesktopCommandError {
    let code = match error {
        ProviderOnboardingError::UnknownProfile => "unknown_provider",
        ProviderOnboardingError::InvalidRequest | ProviderOnboardingError::InvalidSecretShape => {
            "invalid_request"
        }
        ProviderOnboardingError::AdministrativeContactRequired => "contact_required",
        ProviderOnboardingError::CredentialRejected => "credential_rejected",
        ProviderOnboardingError::ProbeRateLimited => "rate_limited",
        ProviderOnboardingError::ProbeDeadlineExceeded => "deadline_exceeded",
        ProviderOnboardingError::OperationCancelled => "cancelled",
        ProviderOnboardingError::EvidenceRefreshRequired => "evidence_refresh_required",
        ProviderOnboardingError::RightsBlocked => "rights_blocked",
        ProviderOnboardingError::ActivationExpired => "activation_expired",
        ProviderOnboardingError::SecretCleanupUnavailable
        | ProviderOnboardingError::RemoteReconciliationRequired => "reconciliation_required",
        ProviderOnboardingError::InvalidProfile
        | ProviderOnboardingError::SecretImportUnavailable
        | ProviderOnboardingError::RenewalUnavailable
        | ProviderOnboardingError::ActivationUnavailable
        | ProviderOnboardingError::SecretVerificationFailed
        | ProviderOnboardingError::InvalidSessionState
        | ProviderOnboardingError::ClientConfiguration
        | ProviderOnboardingError::ProbeUnavailable
        | ProviderOnboardingError::OfficialDocumentUnavailable
        | ProviderOnboardingError::SecretOperationUnavailable
        | ProviderOnboardingError::Clock
        | ProviderOnboardingError::Profile(_)
        | ProviderOnboardingError::Catalog(_)
        | ProviderOnboardingError::SecretStore(_)
        | ProviderOnboardingError::Identity(_)
        | ProviderOnboardingError::Network(_)
        | ProviderOnboardingError::Tls(_) => "provider_unavailable",
    };
    DesktopCommandError::new(code, error.to_string())
}

fn map_activation_error(error: ProviderPortalActivationError) -> DesktopCommandError {
    DesktopCommandError::new(
        match error {
            ProviderPortalActivationError::InvalidRequest => "invalid_request",
            ProviderPortalActivationError::Unavailable => "provider_unavailable",
            ProviderPortalActivationError::StateUnavailable => "state_unavailable",
            ProviderPortalActivationError::Cancelled => "cancelled",
        },
        error.to_string(),
    )
}
