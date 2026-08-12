//! Bounded, secret-free presentation contracts for the desktop WebView.

use std::fmt;

use market_squawk::{ProviderPortalActivationRequest, application::setup::SetupPlanSelection};
use market_squawk_runtime::RuntimeIdentity;
use serde::{Deserialize, Serialize, Serializer};
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
    telemetry_enabled: bool,
    encrypted_file_fallback: Value,
    provider_profiles: Value,
    provider_sessions: Value,
    operations: Vec<OperationSummary>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DesktopServiceBootstrapRequirement {
    EncryptedFallbackLocked,
    ForegroundKeyringRetry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DesktopServiceBootstrapStatusName {
    BootstrapRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopServiceBootstrapStatus {
    status: DesktopServiceBootstrapStatusName,
    requirement: DesktopServiceBootstrapRequirement,
}

impl DesktopServiceBootstrapStatus {
    pub(crate) const fn required(requirement: DesktopServiceBootstrapRequirement) -> Self {
        Self {
            status: DesktopServiceBootstrapStatusName::BootstrapRequired,
            requirement,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum DesktopStartup {
    Ready(Box<DesktopBootstrap>),
    BootstrapRequired(DesktopServiceBootstrapStatus),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "action")]
pub(crate) enum DesktopServiceBootstrapCommand {
    UnlockEncryptedFallback { unlock: String },
    RetryAfterForegroundKeyring,
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
        encrypted_file_fallback: Value,
        provider_profiles: Value,
        provider_sessions: Value,
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
            telemetry_enabled: false,
            encrypted_file_fallback,
            provider_profiles,
            provider_sessions,
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
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "query"
)]
pub(crate) enum DashboardQueryCommand {
    Overview,
    MacroDashboard {
        provider: String,
        release: String,
    },
    Lookup {
        text: String,
        categories: Option<Vec<String>>,
    },
    MarketSnapshot,
    MarketQuality,
    MarketUnifiedFeed,
    MarketUniverse {
        text: Option<String>,
    },
    MarketTrades {
        instrument_id: Uuid,
    },
    MarketQuotes {
        instrument_id: Uuid,
    },
    MarketBooks {
        instrument_id: Uuid,
    },
    MarketComparisons {
        instrument_id: Uuid,
    },
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
    ResearchSourceObjects {
        provider: String,
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
        instrument_id: Uuid,
        proposed_quantity: String,
        scenario_shock: String,
    },
    ModelBundles,
    Forecasts,
    LatestValidForecast {
        instrument_id: Uuid,
        as_of: String,
    },
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
    AnalysisFeatureDatasets {
        dataset: Option<String>,
        after_dataset: Option<String>,
    },
    DecisionScreenRuns {
        after_run_id: Option<String>,
        limit: u16,
    },
    DecisionCandidates {
        run_id: String,
    },
    DecisionCandidateDossiers {
        candidate_id: String,
        after_dossier_id: Option<String>,
        limit: u16,
    },
    DecisionDossier {
        dossier_id: String,
    },
    DecisionDossierPreparation {
        candidate_id: String,
    },
    DecisionInvestmentAnalysis {
        analysis_id: String,
    },
    DecisionInvestmentAnalyses {
        after_analysis_id: Option<String>,
        limit: u16,
    },
    DecisionRecommendationTrackRecord {
        profile_id: String,
        profile_revision: u32,
        profile_digest: String,
        horizon_nanos: String,
        evaluated_at_unix_nanos: String,
    },
    DecisionTargetPreparation {
        dossier_id: String,
    },
    DecisionTarget {
        target_id: String,
        revision: u32,
    },
    DecisionTargets {
        target_id: String,
    },
    DecisionTargetIndex {
        after_target_id: Option<String>,
        limit: u16,
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
    OperationRuntimeStatus,
    OperationBackups {
        after_backup_id: Option<String>,
        limit: u16,
    },
    OperationBackup {
        backup_id: String,
    },
    OperationBackupRetentionPreview {
        keep_latest: u16,
    },
    OperationRestorePreview {
        backup_id: String,
    },
    OperationWorkspaces {
        after_workspace_id: Option<Uuid>,
        limit: u16,
    },
    OperationWorkspaceSwitchPreview {
        workspace_id: Uuid,
    },
    OperationUpdateStatus,
    OperationUpdatePreview,
    OperationProgramRollbackPreview,
    OperationLogs {
        from_unix_nanos: Option<String>,
        through_unix_nanos: Option<String>,
        minimum_severity: Option<OperationLogSeverity>,
        domain: Option<OperationLogDomain>,
        source_id: Option<String>,
        job_id: Option<String>,
        correlation_id: Option<String>,
        search: Option<String>,
        after_sequence: Option<String>,
        limit: u16,
    },
    OperationSettings,
    OperationSettingsChangePreview {
        expected_revision: String,
        changes: Vec<OperationSettingValue>,
    },
    OperationSettingsRollbackPreview {
        expected_revision: String,
        target_revision: String,
    },
    SetupPlanStatus,
    SetupPlanPreview {
        expected_revision: String,
        selection: SetupPlanSelection,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperationLogSeverity {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperationLogDomain {
    Application,
    Source,
    Market,
    Research,
    Portfolio,
    Model,
    Backtest,
    Execution,
    Risk,
    FairValue,
    Mcp,
    Lifecycle,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperationUpdateChannel {
    Stable,
    Preview,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub(crate) enum OperationSettingValue {
    LogRetentionDays(u16),
    LogMinimumSeverity(OperationLogSeverity),
    UpdateChannel(OperationUpdateChannel),
    AutomaticUpdateChecks(bool),
    StorageSoftLimitBytes(String),
    DefaultQueryRowLimit(u32),
    MaximumConcurrentJobs(u16),
    MarketFreshnessMillis(u64),
    BackupRetentionCount(u16),
}

#[derive(Debug, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "action"
)]
pub(crate) enum OperationsControlCommand {
    CheckForUpdates,
    ExportLogs {
        from_unix_nanos: Option<String>,
        through_unix_nanos: Option<String>,
        minimum_severity: Option<OperationLogSeverity>,
        domain: Option<OperationLogDomain>,
        source_id: Option<String>,
        job_id: Option<String>,
        correlation_id: Option<String>,
        search: Option<String>,
        after_sequence: Option<String>,
        limit: u16,
    },
    StartBackup,
    StartBackupVerification {
        backup_id: String,
    },
    StartBackupRetention {
        preview_id: Uuid,
        preview_digest: String,
    },
    StartRestore {
        preview_id: Uuid,
        preview_digest: String,
    },
    StartWorkspaceSwitch {
        preview_id: Uuid,
        preview_digest: String,
    },
    StartUpdate {
        preview_id: Uuid,
        preview_digest: String,
    },
    StartProgramRollback {
        preview_id: Uuid,
        preview_digest: String,
    },
    ApplySettingsChange {
        preview_id: Uuid,
        preview_digest: String,
    },
    RollbackSettings {
        preview_id: Uuid,
        preview_digest: String,
    },
    ApplySetupPlan {
        preview_id: Uuid,
        preview_sha256: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "action"
)]
pub(crate) enum ResearchControlCommand {
    DiscoverSourceObjects {
        provider: String,
        dataset: String,
    },
    StartIngestSource {
        provider: String,
        object: String,
        dataset: String,
        discovery_receipt: Uuid,
    },
    StartExport {
        dataset: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "action"
)]
pub(crate) enum AnalysisControlCommand {
    FeatureDatasetOptions,
    PreviewFeatureDataset { selection: Map<String, Value> },
    StartPreparedFeatureDataset { receipt: Map<String, Value> },
    BacktestOptions,
    PreviewBacktest { selection: Map<String, Value> },
    StartPreparedBacktest { receipt: Map<String, Value> },
}

#[derive(Debug, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "action"
)]
pub(crate) enum ModelControlCommand {
    Evaluate {
        model_id: String,
        input: Map<String, Value>,
    },
    StartTraining {
        config_ticket_id: Uuid,
        authority_ticket_id: Uuid,
    },
    ForecastPreparationOptions,
    PrepareForecast {
        selection: Map<String, Value>,
    },
    StartPreparedForecast {
        receipt: Map<String, Value>,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "action"
)]
pub(crate) enum FairValueControlCommand {
    Measure {
        measurement: Map<String, Value>,
    },
    Classify {
        measurement_id: String,
    },
    PreviewGovernanceAction {
        proposal: FairValueGovernanceProposal,
    },
    CommitGovernanceAction {
        preview_id: Uuid,
        authorization_handles: Vec<Uuid>,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "kind"
)]
pub(crate) enum FairValueGovernanceProposal {
    Approve {
        measurement_id: String,
        decision_id: String,
        expires_at: String,
    },
    Override {
        measurement_id: String,
        decision_id: String,
        requested_hierarchy: FairValueHierarchyInput,
        justification: String,
        expires_at: String,
    },
    Revoke {
        approval_id: String,
        reason: String,
    },
    MarketAccess {
        account_id: String,
        venue_id: String,
        instrument_id: String,
        conclusion: MarketAccessConclusionInput,
        effective_from: String,
        effective_until: String,
        rationale: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FairValueHierarchyInput {
    Level2,
    Level3,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MarketAccessConclusionInput {
    Accessible,
    Inaccessible,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "action"
)]
pub(crate) enum DecisionControlCommand {
    SaveScreen {
        expected_revision: Option<u32>,
        screen: Map<String, Value>,
    },
    RunScreen {
        screen_id: String,
        screen_revision: u32,
        dataset_manifest: Map<String, Value>,
        as_of: String,
    },
    PrepareDossier {
        draft: Map<String, Value>,
    },
    CreateDossier {
        receipt_id: Uuid,
    },
    PrepareTargetSet {
        draft: Map<String, Value>,
    },
    CreateTargetSet {
        receipt_id: Uuid,
    },
    ReevaluateTargetSet {
        receipt_id: Uuid,
    },
    PreviewGovernanceAction {
        proposal: DecisionGovernanceProposal,
    },
    CommitGovernanceAction {
        preview_id: Uuid,
        authorization_handles: Vec<Uuid>,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "kind"
)]
pub(crate) enum DecisionGovernanceProposal {
    Review {
        target_id: String,
        target_revision: u64,
        disposition: DecisionReviewDispositionInput,
        note: String,
    },
    Invalidation {
        target_id: String,
        target_revision: u64,
        invalidation_kind: DecisionInvalidationKindInput,
        note: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DecisionReviewDispositionInput {
    Activate,
    Reject,
    NeedsChanges,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DecisionInvalidationKindInput {
    CorporateAction,
    Model,
    Data,
    ReferenceMark,
    Assumption,
}

#[derive(Debug, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "query"
)]
pub(crate) enum GovernanceQueryCommand {
    ProvisioningStatus,
    Principals {
        after: Option<Uuid>,
        limit: Option<u16>,
    },
}

#[derive(Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "action"
)]
pub(crate) enum GovernanceControlCommand {
    ProvisionPrincipalSet {
        primary_display_name: String,
        primary_credential: String,
        reviewer_display_name: String,
        reviewer_credential: String,
    },
    AuthenticateAction {
        preview_id: Uuid,
        principal_id: Uuid,
        credential: String,
    },
}

impl fmt::Debug for GovernanceControlCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProvisionPrincipalSet {
                primary_display_name,
                primary_credential: _,
                reviewer_display_name,
                reviewer_credential: _,
            } => formatter
                .debug_struct("ProvisionPrincipalSet")
                .field("primary_display_name", primary_display_name)
                .field("primary_credential", &"[REDACTED]")
                .field("reviewer_display_name", reviewer_display_name)
                .field("reviewer_credential", &"[REDACTED]")
                .finish(),
            Self::AuthenticateAction {
                preview_id,
                principal_id,
                credential: _,
            } => formatter
                .debug_struct("AuthenticateAction")
                .field("preview_id", preview_id)
                .field("principal_id", principal_id)
                .field("credential", &"[REDACTED]")
                .finish(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "action"
)]
pub(crate) enum PaperControlCommand {
    Targets,
    Submit {
        target_id: String,
        target_revision: u64,
        side: String,
        order_type: String,
        quantity_lots: String,
        limit_target_level: Option<String>,
        stop_target_level: Option<String>,
        time_in_force: String,
    },
    Start {
        provider: String,
        provider_session_id: Option<String>,
        strategy_mode: String,
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

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TrainingInputKind {
    Configuration,
    ModelAuthority,
}

#[derive(Debug, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "action"
)]
pub(crate) enum JobControlCommand {
    List {
        after_job_id: Option<String>,
        limit: u16,
    },
    Get {
        job_id: Uuid,
        generation: String,
    },
    Watch {
        job_id: Uuid,
        generation: String,
        after_sequence: String,
        limit: u16,
    },
    Cancel {
        job_id: Uuid,
        generation: String,
        expected_sequence: String,
    },
    Confirm {
        job_id: Uuid,
        generation: String,
        expected_sequence: String,
        identity: String,
        digest: String,
    },
    Retry {
        job_id: Uuid,
        generation: String,
        expected_sequence: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SourceLifecycleInput {
    pub(crate) provider: String,
    pub(crate) expected_state_revision: u64,
    pub(crate) expected_generation: Option<u64>,
    pub(crate) expected_runtime_generation_sha256: Option<String>,
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
pub(crate) struct DesktopEvent {
    runtime: RuntimeIdentity,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    sequence: u64,
    body: DesktopEventBody,
}

fn serialize_u64_as_decimal<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.collect_str(value)
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

    pub(crate) const fn stream_disconnected(
        runtime: RuntimeIdentity,
        sequence: u64,
        reason: &'static str,
    ) -> Self {
        Self {
            runtime,
            sequence,
            body: DesktopEventBody::StreamDisconnected { reason },
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    tag = "type"
)]
enum DesktopEventBody {
    AuthorityChanged {
        domain: String,
        operation: String,
        request_id: String,
    },
    ResyncRequired {
        reason: &'static str,
    },
    StreamDisconnected {
        reason: &'static str,
    },
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct DesktopEventSubscriptionRequest {
    runtime: RuntimeIdentity,
    #[serde(deserialize_with = "deserialize_u64_from_decimal")]
    after_sequence: u64,
}

impl DesktopEventSubscriptionRequest {
    pub(crate) const fn runtime(self) -> RuntimeIdentity {
        self.runtime
    }

    pub(crate) const fn after_sequence(self) -> u64 {
        self.after_sequence
    }
}

fn deserialize_u64_from_decimal<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(serde::de::Error::custom(
            "expected a canonical unsigned 64-bit decimal",
        ));
    }
    value.parse().map_err(serde::de::Error::custom)
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopEventSubscriptionReceipt {
    subscription_id: Uuid,
    runtime: RuntimeIdentity,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    sequence: u64,
    resumed: bool,
}

impl DesktopEventSubscriptionReceipt {
    pub(crate) const fn new(
        subscription_id: Uuid,
        runtime: RuntimeIdentity,
        sequence: u64,
        resumed: bool,
    ) -> Self {
        Self {
            subscription_id,
            runtime,
            sequence,
            resumed,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "action"
)]
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
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "action"
)]
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
