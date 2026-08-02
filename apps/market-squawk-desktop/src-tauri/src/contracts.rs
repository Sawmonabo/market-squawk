//! Bounded, secret-free presentation contracts for the desktop WebView.

use std::fmt;

use market_squawk::ProviderPortalActivationRequest;
use market_squawk_runtime::RuntimeIdentity;
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

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OperationSummary {
    name: String,
    description: String,
    domain: String,
    authorization: String,
    read_only: bool,
    destructive: bool,
    input_schema: Value,
}

impl OperationSummary {
    pub(crate) fn new(
        name: String,
        description: String,
        domain: String,
        authorization: String,
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
    runtime: RuntimeIdentity,
    storage: Readiness,
    installation: Readiness,
    model_runtime: Readiness,
    mcp: Readiness,
    mcp_client: Option<Value>,
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
        runtime: RuntimeIdentity,
        storage: Readiness,
        installation: Readiness,
        model_runtime: Readiness,
        mcp: Readiness,
        mcp_client: Option<Value>,
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
            runtime,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "query")]
pub(crate) enum DashboardQueryCommand {
    Overview,
    Lookup {
        text: String,
        categories: Option<Vec<String>>,
    },
    MarketSnapshot,
    MarketQuality,
    SourceStatus {
        source_ids: Option<Vec<String>>,
    },
    SourceCoverage {
        source_ids: Option<Vec<String>>,
    },
    SourceHealth {
        source_ids: Option<Vec<String>>,
    },
    ResearchDatasets {
        after_dataset: Option<String>,
    },
    ResearchManifest {
        dataset: String,
    },
    ResearchHistory {
        dataset: String,
    },
    ResearchAlternativeData {
        dataset: String,
    },
    PortfolioAccounts {
        after_account_id: Option<String>,
    },
    PortfolioHoldings {
        account_id: String,
    },
    PortfolioTransactions {
        account_id: String,
    },
    PortfolioPerformance {
        account_id: String,
    },
    PortfolioExposure {
        account_id: String,
    },
    PortfolioRisk {
        account_id: String,
    },
    PortfolioRevisions {
        account_id: String,
        after_revision_id: Option<String>,
    },
    PortfolioAttribution {
        account_id: String,
        baseline_revision_id: String,
    },
    PortfolioScenario {
        account_id: String,
        scenario: Map<String, Value>,
    },
    PortfolioScenarioBatch {
        account_id: String,
        scenarios: Vec<Value>,
    },
    PortfolioRebalance {
        account_id: String,
        proposal: Map<String, Value>,
    },
    PortfolioCandidateImpact {
        account_id: String,
        candidate: Map<String, Value>,
    },
    ModelBundles,
    Forecasts,
    ModelMetadata {
        model_id: String,
    },
    ModelPrediction {
        model_id: String,
        input: Map<String, Value>,
    },
    Forecast {
        vintage_id: String,
    },
    ForecastOutcomes {
        vintage_id: String,
    },
    DecisionScreens {
        limit: u16,
    },
    DecisionCandidates {
        run_id: String,
    },
    DecisionDossier {
        dossier_id: String,
    },
    DecisionTarget {
        target_id: String,
        revision: u32,
    },
    DecisionTargets {
        target_id: String,
    },
    DecisionTargetStatus {
        target_id: String,
        revision: u32,
    },
    Backtest {
        run_id: String,
    },
    AnalysisArtifact {
        artifact_id: String,
        sha256: String,
        byte_count: u64,
        media_type: String,
        offset: u64,
        maximum_bytes: u64,
    },
    PaperStatus,
    PaperOrders,
    PaperFills,
    FairValueMeasurements,
    FairValueClassification {
        measurement_id: String,
    },
    FairValueExplanation {
        measurement_id: String,
    },
    FairValueEvidence {
        measurement_id: String,
    },
    FairValueApprovalStatus {
        measurement_id: String,
        at: String,
    },
    FairValueAudit {
        after: Option<Map<String, Value>>,
        limit: u16,
    },
    FairValueMarketAccess {
        assessment_id: String,
    },
    Jobs {
        after_job_id: Option<String>,
        limit: u16,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "action")]
pub(crate) enum ResearchControlCommand {
    StartIngest {
        provider: String,
        object: String,
        dataset: String,
        discovery_receipt: String,
    },
    StartDatasetBuild {
        registration: Map<String, Value>,
    },
    StartExport {
        dataset: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "action")]
pub(crate) enum DecisionControlCommand {
    SaveScreen {
        expected_revision: Option<u32>,
        screen: Map<String, Value>,
    },
    RunScreen {
        run: Map<String, Value>,
        candidates: Vec<Value>,
        selected_at: String,
    },
    CreateTarget {
        target: Map<String, Value>,
    },
    ReviewTarget {
        review: Map<String, Value>,
    },
    ReevaluateTarget {
        expected_revision: u32,
        successor: Map<String, Value>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "action")]
pub(crate) enum ModelControlCommand {
    Evaluate {
        model_id: String,
        input: Map<String, Value>,
    },
    StartTraining {
        config_ticket_id: Uuid,
        authority_ticket_id: Uuid,
    },
    StartForecast {
        model_id: String,
        request: Map<String, Value>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "action")]
pub(crate) enum FairValueControlCommand {
    Measure {
        measurement: Map<String, Value>,
    },
    Classify {
        measurement_id: String,
    },
    Approve {
        measurement_id: String,
        decision_id: String,
        approved_by: String,
        approved_at: String,
        expires_at: String,
    },
    ApproveMarketAccess {
        account_id: String,
        venue_id: String,
        instrument_id: String,
        conclusion: String,
        effective_from: String,
        effective_until: String,
        rationale: String,
        prepared_by: String,
        prepared_at: String,
        approved_by: String,
        approved_at: String,
    },
    ProposeOverride {
        measurement_id: String,
        decision_id: String,
        requested_hierarchy: String,
        justification: String,
        prepared_by: String,
        prepared_at: String,
        expires_at: String,
    },
    RevokeApproval {
        approval_id: String,
        revoked_by: String,
        revoked_at: String,
        reason: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "action")]
pub(crate) enum PaperControlCommand {
    Start {
        provider: String,
        provider_session_id: Option<String>,
        initial_cash: String,
        fee_basis_points: u16,
    },
    Stop {
        reason: String,
    },
    Cancel {
        order_id: String,
    },
    Reconcile,
    TriggerKillSwitch {
        reason: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "action")]
pub(crate) enum PortfolioControlCommand {
    Import {
        account_id: String,
        artifact_id: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TrainingInputKind {
    Configuration,
    ModelAuthority,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "action")]
pub(crate) enum JobControlCommand {
    List {
        after_job_id: Option<String>,
        limit: u16,
    },
    Get {
        job_id: Uuid,
    },
    Watch {
        job_id: Uuid,
        generation: u64,
        after_sequence: u64,
        limit: u16,
    },
    Cancel {
        job_id: Uuid,
        generation: u64,
        expected_sequence: u64,
    },
    Confirm {
        job_id: Uuid,
        generation: u64,
        expected_sequence: u64,
        identity: String,
        digest: String,
    },
    Retry {
        job_id: Uuid,
        generation: u64,
        expected_sequence: u64,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SourceLifecycleInput {
    pub(crate) provider: String,
    pub(crate) expected_state_revision: u64,
    pub(crate) expected_generation: Option<u64>,
    pub(crate) onboarding_session_id: Option<Uuid>,
    pub(crate) public_configuration_sha256: Option<String>,
    pub(crate) reason: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceLifecycleAction {
    Start,
    Stop,
    Retry,
    Resynchronize,
    Verify,
    Reconfigure,
    Remove,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct McpStatus {
    service_ready: bool,
    shared_endpoint_ready: bool,
    claude_code: &'static str,
    codex: &'static str,
}

impl McpStatus {
    pub(crate) const fn service_ready(shared_endpoint_ready: bool) -> Self {
        Self {
            service_ready: true,
            shared_endpoint_ready,
            claude_code: "registration_pending",
            codex: "registration_pending",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopEvent {
    runtime: RuntimeIdentity,
    sequence: u64,
    body: DesktopEventBody,
}

impl DesktopEvent {
    pub(crate) const fn authority_changed(
        runtime: RuntimeIdentity,
        sequence: u64,
        domain: String,
        operation: String,
        request_id: String,
    ) -> Self {
        Self {
            runtime,
            sequence,
            body: DesktopEventBody::AuthorityChanged {
                domain,
                operation,
                request_id,
            },
        }
    }

    pub(crate) const fn resync_required(
        runtime: RuntimeIdentity,
        sequence: u64,
        reason: &'static str,
    ) -> Self {
        Self {
            runtime,
            sequence,
            body: DesktopEventBody::ResyncRequired { reason },
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum DesktopEventBody {
    AuthorityChanged {
        domain: String,
        operation: String,
        request_id: String,
    },
    ResyncRequired {
        reason: &'static str,
    },
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
