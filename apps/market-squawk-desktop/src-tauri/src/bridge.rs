//! Least-privilege Tauri bridge over the existing local application authorities.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk::{
    AppConfig, LocalProduct, ProviderOnboardingError, ProviderPortalActivationAuthority,
    ProviderPortalActivationError, StartOnboardingRequest,
};
use market_squawk_data::CatalogLimit;
use market_squawk_platform::SecretValue;
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ServiceError, ServiceLimits,
    validate_json_contract,
};
use serde::Serialize;
use serde_json::{Value, json};
use tauri::State;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::contracts::{
    ApplicationInvocation, DesktopBootstrap, DesktopCommandError, OperationSummary,
    ProviderOnboardingCommand, Readiness, ReadinessState,
};

const MAXIMUM_APPLICATION_ARGUMENT_BYTES: usize = 256 * 1024;
const MAXIMUM_OPERATION_BYTES: usize = 128;
const MAXIMUM_PROVIDER_SESSIONS: usize = 32;
const APPLICATION_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const PROVIDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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
        let sessions = serialize(
            onboarding
                .current_sessions(session_limit)
                .map_err(map_onboarding_error)?,
        )?;
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
            self.config.paper_bot_enabled(),
            fallback,
            profiles,
            sessions,
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
    if request.operation.is_empty()
        || request.operation.len() > MAXIMUM_OPERATION_BYTES
        || request.operation.chars().any(char::is_control)
    {
        return Err(DesktopCommandError::invalid_request(
            "The selected Market Squawk operation is invalid.",
        ));
    }
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
    let application = state.product.application();
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
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
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
