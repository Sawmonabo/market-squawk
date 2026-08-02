//! Bounded, secret-free presentation contracts for the desktop WebView.

use std::{collections::BTreeMap, fmt};

use market_squawk::ProviderPortalActivationRequest;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

/// Version of the desktop presentation contract.
pub(crate) const DESKTOP_CONTRACT_VERSION: &str = "market-squawk-desktop-v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReadinessState {
    Ready,
    Available,
    NotConfigured,
    Unverified,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SetupStepState {
    Complete,
    ActionRequired,
    Blocked,
    Available,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SetupStepAction {
    ConfigureSources,
    ConfigureResearch,
    ConfigurePortfolio,
    ConfigurePaper,
    ReviewMcp,
    ReviewStatus,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SetupStep {
    id: &'static str,
    label: &'static str,
    state: SetupStepState,
    complete: bool,
    detail: &'static str,
    blocking_reason: Option<&'static str>,
    recovery: Option<&'static str>,
    action: Option<SetupStepAction>,
}

impl SetupStep {
    #[allow(
        clippy::too_many_arguments,
        reason = "the presentation contract keeps every authority-derived setup fact explicit"
    )]
    pub(crate) const fn new(
        id: &'static str,
        label: &'static str,
        state: SetupStepState,
        complete: bool,
        detail: &'static str,
        blocking_reason: Option<&'static str>,
        recovery: Option<&'static str>,
        action: Option<SetupStepAction>,
    ) -> Self {
        Self {
            id,
            label,
            state,
            complete,
            detail,
            blocking_reason,
            recovery,
            action,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Readiness {
    state: ReadinessState,
    label: &'static str,
    detail: &'static str,
}

impl Readiness {
    pub(crate) const fn new(
        state: ReadinessState,
        label: &'static str,
        detail: &'static str,
    ) -> Self {
        Self {
            state,
            label,
            detail,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct McpClientInstruction {
    program: String,
    arguments: Vec<String>,
    environment: BTreeMap<String, String>,
    requires_desktop_exit: bool,
}

impl McpClientInstruction {
    pub(crate) const fn new(
        program: String,
        arguments: Vec<String>,
        environment: BTreeMap<String, String>,
    ) -> Self {
        Self {
            program,
            arguments,
            environment,
            requires_desktop_exit: true,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OperationSummary {
    name: String,
    description: String,
    domain: &'static str,
    authorization: &'static str,
    read_only: bool,
    destructive: bool,
    input_schema: Value,
}

impl OperationSummary {
    pub(crate) fn new(
        name: String,
        description: String,
        domain: &'static str,
        authorization: &'static str,
        read_only: bool,
        destructive: bool,
        input_schema: Value,
    ) -> Self {
        Self {
            name,
            description,
            domain,
            authorization,
            read_only,
            destructive,
            input_schema,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopBootstrap {
    contract_version: &'static str,
    application_version: &'static str,
    build_profile: &'static str,
    platform: &'static str,
    data_root: String,
    storage: Readiness,
    installation: Readiness,
    model_runtime: Readiness,
    mcp: Readiness,
    mcp_client: Option<McpClientInstruction>,
    telemetry_enabled: bool,
    encrypted_file_fallback: Value,
    provider_profiles: Value,
    provider_sessions: Value,
    setup_steps: Vec<SetupStep>,
    operations: Vec<OperationSummary>,
}

impl DesktopBootstrap {
    #[allow(
        clippy::too_many_arguments,
        reason = "each independently sourced readiness fact remains explicit at the presentation boundary"
    )]
    pub(crate) fn new(
        application_version: &'static str,
        build_profile: &'static str,
        data_root: String,
        storage: Readiness,
        installation: Readiness,
        model_runtime: Readiness,
        mcp: Readiness,
        mcp_client: Option<McpClientInstruction>,
        encrypted_file_fallback: Value,
        provider_profiles: Value,
        provider_sessions: Value,
        setup_steps: Vec<SetupStep>,
        operations: Vec<OperationSummary>,
    ) -> Self {
        Self {
            contract_version: DESKTOP_CONTRACT_VERSION,
            application_version,
            build_profile,
            platform: std::env::consts::OS,
            data_root,
            storage,
            installation,
            model_runtime,
            mcp,
            mcp_client,
            telemetry_enabled: false,
            encrypted_file_fallback,
            provider_profiles,
            provider_sessions,
            setup_steps,
            operations,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ApplicationInvocation {
    pub(crate) operation: String,
    #[serde(default)]
    pub(crate) arguments: Map<String, Value>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "action")]
pub(crate) enum InstallationControlCommand {
    Status,
    Update,
    Repair,
    Rollback,
    Uninstall,
}

impl InstallationControlCommand {
    pub(crate) const fn requires_confirmation(self) -> bool {
        !matches!(self, Self::Status)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "action")]
pub(crate) enum ProviderOnboardingCommand {
    Bootstrap,
    Start {
        surface_id: String,
        organization: Option<String>,
        administrative_email: Option<String>,
    },
    Resume {
        session_id: Uuid,
    },
    UnlockFallback {
        secret: String,
    },
    LockFallback,
    SubmitSecret {
        session_id: Uuid,
        secret: String,
    },
    Activate {
        session_id: Uuid,
        request: ProviderPortalActivationRequest,
    },
    Renew {
        session_id: Uuid,
    },
    Cleanup {
        session_id: Uuid,
    },
    Cancel {
        session_id: Uuid,
    },
}

impl ProviderOnboardingCommand {
    pub(crate) const fn requires_confirmation(&self) -> bool {
        !matches!(self, Self::Bootstrap | Self::Resume { .. })
    }
}

impl fmt::Debug for ProviderOnboardingCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let action = match self {
            Self::Bootstrap => "bootstrap",
            Self::Start { .. } => "start",
            Self::Resume { .. } => "resume",
            Self::UnlockFallback { .. } => "unlock_fallback",
            Self::LockFallback => "lock_fallback",
            Self::SubmitSecret { .. } => "submit_secret",
            Self::Activate { .. } => "activate",
            Self::Renew { .. } => "renew",
            Self::Cleanup { .. } => "cleanup",
            Self::Cancel { .. } => "cancel",
        };
        formatter
            .debug_struct("ProviderOnboardingCommand")
            .field("action", &action)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopCommandError {
    code: &'static str,
    message: String,
}

impl DesktopCommandError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn invalid_request(message: &'static str) -> Self {
        Self::new("invalid_request", message)
    }

    pub(crate) fn internal() -> Self {
        Self::new(
            "internal",
            "Market Squawk could not complete this local request.",
        )
    }
}

impl fmt::Display for DesktopCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for DesktopCommandError {}
