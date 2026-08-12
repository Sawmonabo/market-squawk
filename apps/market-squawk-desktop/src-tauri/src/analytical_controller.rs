//! Desktop-owned analytical profiles and restart-safe workflow checkpoint references.
//!
//! This module owns only the opinionated Desktop composition boundary. Financial calculations,
//! evidence admission, recommendation generation, and immutable domain results remain owned by
//! the installed application service. Until those pure capabilities are composed end to end, this
//! controller exposes a truthful blocked state and no Find/Analyze start command.

use std::{
    collections::HashSet,
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use market_squawk_platform::{LocalAuthorityStateStore, LocalPaths};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tauri::State;
use uuid::Uuid;

use crate::contracts::DesktopCommandError;

const CONTROLLER_FORMAT_VERSION: u32 = 1;
const PROFILE_VERSION: u32 = 1;
const DEFAULT_PROFILE_NAME: &str = "Market Squawk Default V1";
const CONTROLLER_DIRECTORY: &str = "desktop-analytical-controller-v1";
const MAXIMUM_PROFILES: usize = 32;
const MAXIMUM_PROFILE_HISTORY: usize = 256;
const MAXIMUM_WORKFLOW_RUNS: usize = 256;
const MAXIMUM_HISTORY_PAGE: usize = 100;
const MAXIMUM_CHECKPOINTS_PER_RUN: usize = 64;
const MAXIMUM_CHILD_REFERENCES_PER_RUN: usize = 64;
const MAXIMUM_RESULT_REFERENCES_PER_RUN: usize = 128;
const DEFAULT_PROFILE_NAMESPACE: Uuid = Uuid::from_u128(0xd446_3dc7_70cf_56de_a09c_682b_e6d3_4691);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    tag = "kind"
)]
pub(crate) enum ProfileComponentBinding {
    /// Resolve the exact admitted component through the owning backend capability at run time.
    DefaultRequired,
    /// Bind one exact backend-owned component after its owning validator admits it.
    Exact {
        identity: String,
        version: String,
        digest: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct AnalyticalProfileConfig {
    supported_investment_policy: ProfileComponentBinding,
    point_in_time_dataset_policy: ProfileComponentBinding,
    required_feature_set: ProfileComponentBinding,
    model_bundle_policy: ProfileComponentBinding,
    training_calibration_policy: ProfileComponentBinding,
    forecast_horizon_policy: ProfileComponentBinding,
    valuation_policy: ProfileComponentBinding,
    backtest_cost_policy: ProfileComponentBinding,
    recommendation_policy: ProfileComponentBinding,
    risk_freshness_abstention_policy: ProfileComponentBinding,
}

impl AnalyticalProfileConfig {
    fn market_squawk_default_v1() -> Self {
        Self {
            supported_investment_policy: ProfileComponentBinding::DefaultRequired,
            point_in_time_dataset_policy: ProfileComponentBinding::DefaultRequired,
            required_feature_set: ProfileComponentBinding::DefaultRequired,
            model_bundle_policy: ProfileComponentBinding::DefaultRequired,
            training_calibration_policy: ProfileComponentBinding::DefaultRequired,
            forecast_horizon_policy: ProfileComponentBinding::DefaultRequired,
            valuation_policy: ProfileComponentBinding::DefaultRequired,
            backtest_cost_policy: ProfileComponentBinding::DefaultRequired,
            recommendation_policy: ProfileComponentBinding::DefaultRequired,
            risk_freshness_abstention_policy: ProfileComponentBinding::DefaultRequired,
        }
    }

    fn digest(&self) -> Result<String, DesktopCommandError> {
        let canonical =
            serde_json::to_vec(self).map_err(|_error| DesktopCommandError::internal())?;
        Ok(hex_digest(Sha256::digest(canonical)))
    }

    fn validate_shape(&self) -> Result<(), DesktopCommandError> {
        for binding in self.bindings() {
            if let ProfileComponentBinding::Exact {
                identity,
                version,
                digest,
            } = binding
                && (!valid_identifier(identity, 128)
                    || !valid_identifier(version, 64)
                    || !valid_digest(digest))
            {
                return Err(DesktopCommandError::invalid_request(
                    "The analytical profile contains an invalid exact component binding.",
                ));
            }
        }
        Ok(())
    }

    fn bindings(&self) -> [&ProfileComponentBinding; 10] {
        [
            &self.supported_investment_policy,
            &self.point_in_time_dataset_policy,
            &self.required_feature_set,
            &self.model_bundle_policy,
            &self.training_calibration_policy,
            &self.forecast_horizon_policy,
            &self.valuation_policy,
            &self.backtest_cost_policy,
            &self.recommendation_policy,
            &self.risk_freshness_abstention_policy,
        ]
    }

    fn differences_from(&self, default: &Self) -> Vec<AnalyticalProfileComponent> {
        let candidates = [
            (
                AnalyticalProfileComponent::SupportedInvestmentPolicy,
                &self.supported_investment_policy,
                &default.supported_investment_policy,
            ),
            (
                AnalyticalProfileComponent::PointInTimeDatasetPolicy,
                &self.point_in_time_dataset_policy,
                &default.point_in_time_dataset_policy,
            ),
            (
                AnalyticalProfileComponent::RequiredFeatureSet,
                &self.required_feature_set,
                &default.required_feature_set,
            ),
            (
                AnalyticalProfileComponent::ModelBundlePolicy,
                &self.model_bundle_policy,
                &default.model_bundle_policy,
            ),
            (
                AnalyticalProfileComponent::TrainingCalibrationPolicy,
                &self.training_calibration_policy,
                &default.training_calibration_policy,
            ),
            (
                AnalyticalProfileComponent::ForecastHorizonPolicy,
                &self.forecast_horizon_policy,
                &default.forecast_horizon_policy,
            ),
            (
                AnalyticalProfileComponent::ValuationPolicy,
                &self.valuation_policy,
                &default.valuation_policy,
            ),
            (
                AnalyticalProfileComponent::BacktestCostPolicy,
                &self.backtest_cost_policy,
                &default.backtest_cost_policy,
            ),
            (
                AnalyticalProfileComponent::RecommendationPolicy,
                &self.recommendation_policy,
                &default.recommendation_policy,
            ),
            (
                AnalyticalProfileComponent::RiskFreshnessAbstentionPolicy,
                &self.risk_freshness_abstention_policy,
                &default.risk_freshness_abstention_policy,
            ),
        ];
        candidates
            .into_iter()
            .filter_map(|(component, selected, baseline)| {
                (selected != baseline).then_some(component)
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum AnalyticalProfileComponent {
    SupportedInvestmentPolicy,
    PointInTimeDatasetPolicy,
    RequiredFeatureSet,
    ModelBundlePolicy,
    TrainingCalibrationPolicy,
    ForecastHorizonPolicy,
    ValuationPolicy,
    BacktestCostPolicy,
    RecommendationPolicy,
    RiskFreshnessAbstentionPolicy,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum AnalyticalProfileKind {
    Default,
    Custom,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProfileValidationState {
    DefaultImmutable,
    NotValidated,
    Blocked,
    Validated,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProfileValidationBasis {
    IdenticalToImmutableDefault,
    BackendComponentReceipts,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ServiceResultReference {
    operation: String,
    result_id: String,
    content_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ProfileValidationReceipt {
    receipt_id: Uuid,
    profile_id: Uuid,
    #[serde(with = "decimal_u64")]
    profile_revision: u64,
    config_digest: String,
    validated_at: String,
    basis: ProfileValidationBasis,
    backend_receipts: Vec<ServiceResultReference>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct AnalyticalProfile {
    profile_id: Uuid,
    owner_workspace_id: Uuid,
    display_name: String,
    kind: AnalyticalProfileKind,
    version: u32,
    #[serde(with = "decimal_u64")]
    revision: u64,
    config_digest: String,
    config: AnalyticalProfileConfig,
    validation_state: ProfileValidationState,
    last_validation: Option<ProfileValidationReceipt>,
    created_at: String,
    updated_at: String,
}

impl AnalyticalProfile {
    fn immutable_default(
        owner_workspace_id: Uuid,
        now: String,
    ) -> Result<Self, DesktopCommandError> {
        let config = AnalyticalProfileConfig::market_squawk_default_v1();
        let config_digest = config.digest()?;
        Ok(Self {
            profile_id: default_profile_id(),
            owner_workspace_id,
            display_name: DEFAULT_PROFILE_NAME.to_owned(),
            kind: AnalyticalProfileKind::Default,
            version: PROFILE_VERSION,
            revision: 1,
            config_digest,
            config,
            validation_state: ProfileValidationState::DefaultImmutable,
            last_validation: None,
            created_at: now.clone(),
            updated_at: now,
        })
    }

    fn binding(&self, activation_revision: u64, activated_at: String) -> ActiveProfileBinding {
        ActiveProfileBinding {
            profile_id: self.profile_id,
            owner_workspace_id: self.owner_workspace_id,
            display_name: self.display_name.clone(),
            kind: self.kind,
            version: self.version,
            profile_revision: self.revision,
            config_digest: self.config_digest.clone(),
            activation_revision,
            activated_at,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ActiveProfileBinding {
    profile_id: Uuid,
    owner_workspace_id: Uuid,
    display_name: String,
    kind: AnalyticalProfileKind,
    version: u32,
    #[serde(with = "decimal_u64")]
    profile_revision: u64,
    config_digest: String,
    #[serde(with = "decimal_u64")]
    activation_revision: u64,
    activated_at: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProfileHistoryAction {
    InitializedDefault,
    CopiedDefault,
    UpdatedCustom,
    ValidationBlocked,
    ValidatedCustom,
    ActivatedCustom,
    RestoredDefault,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ProfileHistoryEntry {
    event_id: Uuid,
    owner_workspace_id: Uuid,
    #[serde(with = "decimal_u64")]
    controller_revision: u64,
    action: ProfileHistoryAction,
    profile_id: Uuid,
    profile_version: u32,
    #[serde(with = "decimal_u64")]
    profile_revision: u64,
    config_digest: String,
    config: AnalyticalProfileConfig,
    validation_receipt: Option<ProfileValidationReceipt>,
    recorded_at: String,
    supersedes_profile_id: Option<Uuid>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum WorkflowKind {
    FindOpportunities,
    AnalyzeInvestment,
    OutcomeRefresh,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum WorkflowRunState {
    Blocked,
    Queued,
    Running,
    WaitingForServiceJob,
    Completed,
    Cancelled,
    Failed,
    Stale,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionEligibility {
    ExecutionIneligible,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct WorkflowProfileBinding {
    active: ActiveProfileBinding,
    resolved_component_receipt_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ServiceJobReference {
    job_id: Uuid,
    generation: String,
    terminal_sequence: Option<String>,
    result: Option<ServiceResultReference>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CoverageCompleteness {
    Complete,
    Truncated,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CoverageCounts {
    searched: u32,
    complete_evidence: u32,
    excluded: u32,
    deeply_analyzed: u32,
    generated: u32,
    no_action: u32,
    unavailable: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CoverageReceipt {
    receipt_id: Uuid,
    completeness: CoverageCompleteness,
    counts: CoverageCounts,
    content_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ExclusionReceipt {
    receipt_id: Uuid,
    excluded_count: u32,
    reasons_result: ServiceResultReference,
    content_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RankingReceipt {
    receipt_id: Uuid,
    ordered_result_ids: Vec<String>,
    policy_result: ServiceResultReference,
    content_sha256: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum WorkflowCheckpointStage {
    Created,
    CapabilityCompleted,
    WaitingForServiceJob,
    ResultsRetained,
    CoverageClosed,
    RankingClosed,
    Terminal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct WorkflowCheckpoint {
    #[serde(with = "decimal_u64")]
    sequence: u64,
    stage: WorkflowCheckpointStage,
    recorded_at: String,
    child_job: Option<ServiceJobReference>,
    result: Option<ServiceResultReference>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DesktopWorkflowRun {
    run_id: Uuid,
    schema_version: u32,
    owner_workspace_id: Uuid,
    kind: WorkflowKind,
    state: WorkflowRunState,
    target_instrument_id: Option<Uuid>,
    profile: WorkflowProfileBinding,
    created_at: String,
    updated_at: String,
    checkpoint_journal: Vec<WorkflowCheckpoint>,
    child_jobs: Vec<ServiceJobReference>,
    result_references: Vec<ServiceResultReference>,
    coverage_receipt: Option<CoverageReceipt>,
    exclusion_receipt: Option<ExclusionReceipt>,
    ranking_receipt: Option<RankingReceipt>,
    execution_eligibility: ExecutionEligibility,
    last_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ControllerDocument {
    format_version: u32,
    owner_workspace_id: Uuid,
    #[serde(with = "decimal_u64")]
    revision: u64,
    active_profile: ActiveProfileBinding,
    profiles: Vec<AnalyticalProfile>,
    profile_history: Vec<ProfileHistoryEntry>,
    workflow_runs: Vec<DesktopWorkflowRun>,
}

impl ControllerDocument {
    fn initial(owner_workspace_id: Uuid) -> Result<Self, DesktopCommandError> {
        let now = unix_nanos_now()?;
        let default = AnalyticalProfile::immutable_default(owner_workspace_id, now.clone())?;
        let active_profile = default.binding(1, now.clone());
        Ok(Self {
            format_version: CONTROLLER_FORMAT_VERSION,
            owner_workspace_id,
            revision: 1,
            active_profile,
            profiles: vec![default.clone()],
            profile_history: vec![ProfileHistoryEntry {
                event_id: Uuid::new_v4(),
                owner_workspace_id,
                controller_revision: 1,
                action: ProfileHistoryAction::InitializedDefault,
                profile_id: default.profile_id,
                profile_version: default.version,
                profile_revision: default.revision,
                config_digest: default.config_digest,
                config: default.config,
                validation_receipt: None,
                recorded_at: now,
                supersedes_profile_id: None,
            }],
            workflow_runs: Vec::new(),
        })
    }

    fn validate(&self, expected_workspace_id: Uuid) -> Result<(), DesktopCommandError> {
        if self.format_version != CONTROLLER_FORMAT_VERSION
            || self.owner_workspace_id != expected_workspace_id
            || self.revision == 0
            || self.profiles.is_empty()
            || self.profiles.len() > MAXIMUM_PROFILES
            || self.profile_history.is_empty()
            || self.profile_history.len() > MAXIMUM_PROFILE_HISTORY
            || self.workflow_runs.len() > MAXIMUM_WORKFLOW_RUNS
        {
            return Err(DesktopCommandError::new(
                "analytical_controller_invalid",
                "The retained Desktop analytical controller does not belong to this workspace or has an unsupported shape.",
            ));
        }

        let expected_default = AnalyticalProfile::immutable_default(
            expected_workspace_id,
            self.profiles[0].created_at.clone(),
        )?;
        let mut profile_ids = HashSet::with_capacity(self.profiles.len());
        let mut default_count = 0_usize;
        for profile in &self.profiles {
            profile.config.validate_shape()?;
            if profile.owner_workspace_id != expected_workspace_id
                || profile.profile_id.is_nil()
                || profile.version != PROFILE_VERSION
                || profile.revision == 0
                || !valid_display_name(&profile.display_name)
                || profile.config.digest()? != profile.config_digest
                || !valid_digest(&profile.config_digest)
                || !valid_timestamp(&profile.created_at)
                || !valid_timestamp(&profile.updated_at)
                || !profile_ids.insert(profile.profile_id)
                || (profile.validation_state == ProfileValidationState::Validated)
                    != profile.last_validation.is_some()
            {
                return Err(DesktopCommandError::new(
                    "analytical_controller_invalid",
                    "The retained Desktop analytical profile is invalid.",
                ));
            }
            if profile.kind == AnalyticalProfileKind::Default {
                default_count += 1;
                if profile.profile_id != expected_default.profile_id
                    || profile.display_name != expected_default.display_name
                    || profile.config != expected_default.config
                    || profile.config_digest != expected_default.config_digest
                    || profile.validation_state != ProfileValidationState::DefaultImmutable
                    || profile.last_validation.is_some()
                {
                    return Err(DesktopCommandError::new(
                        "analytical_controller_invalid",
                        "The immutable Market Squawk Default V1 profile does not match this release.",
                    ));
                }
            }
            if let Some(receipt) = &profile.last_validation
                && (profile.kind != AnalyticalProfileKind::Custom
                    || profile.validation_state != ProfileValidationState::Validated
                    || receipt.profile_id != profile.profile_id
                    || receipt.profile_revision != profile.revision
                    || receipt.config_digest != profile.config_digest
                    || !valid_validation_receipt(receipt)
                    || receipt.backend_receipts.len() > MAXIMUM_RESULT_REFERENCES_PER_RUN)
            {
                return Err(DesktopCommandError::new(
                    "analytical_controller_invalid",
                    "The retained Desktop analytical profile validation receipt is invalid.",
                ));
            }
        }
        if default_count != 1 {
            return Err(DesktopCommandError::new(
                "analytical_controller_invalid",
                "The Desktop analytical controller must retain exactly one immutable default profile.",
            ));
        }
        let active = self
            .profiles
            .iter()
            .find(|profile| profile.profile_id == self.active_profile.profile_id)
            .ok_or_else(|| {
                DesktopCommandError::new(
                    "analytical_controller_invalid",
                    "The active Desktop analytical profile is missing.",
                )
            })?;
        let active_validation_matches = active.last_validation.as_ref().is_some_and(|receipt| {
            receipt.profile_revision == self.active_profile.profile_revision
                && receipt.config_digest == self.active_profile.config_digest
        });
        if self.active_profile.owner_workspace_id != expected_workspace_id
            || self.active_profile.display_name != active.display_name
            || self.active_profile.version != active.version
            || self.active_profile.profile_revision != active.revision
            || self.active_profile.config_digest != active.config_digest
            || self.active_profile.kind != active.kind
            || self.active_profile.activation_revision == 0
            || !valid_timestamp(&self.active_profile.activated_at)
            || (active.kind == AnalyticalProfileKind::Custom
                && (active.validation_state != ProfileValidationState::Validated
                    || !active_validation_matches))
        {
            return Err(DesktopCommandError::new(
                "analytical_controller_invalid",
                "The active Desktop analytical profile binding is inconsistent.",
            ));
        }
        self.validate_history()?;
        self.validate_runs()?;
        Ok(())
    }

    fn validate_history(&self) -> Result<(), DesktopCommandError> {
        let mut previous = 0_u64;
        let mut ids = HashSet::with_capacity(self.profile_history.len());
        for event in &self.profile_history {
            event.config.validate_shape()?;
            if event.owner_workspace_id != self.owner_workspace_id
                || event.event_id.is_nil()
                || event.profile_id.is_nil()
                || event.controller_revision <= previous
                || event.controller_revision > self.revision
                || !ids.insert(event.event_id)
                || !valid_digest(&event.config_digest)
                || event.config.digest()? != event.config_digest
                || event.profile_version != PROFILE_VERSION
                || event.profile_revision == 0
                || !valid_timestamp(&event.recorded_at)
                || event.supersedes_profile_id.is_some_and(|id| id.is_nil())
                || event.validation_receipt.as_ref().is_some_and(|receipt| {
                    receipt.profile_id != event.profile_id
                        || receipt.profile_revision != event.profile_revision
                        || receipt.config_digest != event.config_digest
                        || !valid_validation_receipt(receipt)
                })
                || matches!(
                    event.action,
                    ProfileHistoryAction::ValidatedCustom | ProfileHistoryAction::ActivatedCustom
                ) != event.validation_receipt.is_some()
            {
                return Err(DesktopCommandError::new(
                    "analytical_controller_invalid",
                    "The Desktop profile history journal is invalid.",
                ));
            }
            previous = event.controller_revision;
        }
        if previous != self.revision {
            return Err(DesktopCommandError::new(
                "analytical_controller_invalid",
                "The Desktop profile history journal does not close the controller revision.",
            ));
        }
        Ok(())
    }

    fn validate_runs(&self) -> Result<(), DesktopCommandError> {
        let mut run_ids = HashSet::with_capacity(self.workflow_runs.len());
        for run in &self.workflow_runs {
            if run.schema_version != CONTROLLER_FORMAT_VERSION
                || run.run_id.is_nil()
                || run.owner_workspace_id != self.owner_workspace_id
                || run.profile.active.owner_workspace_id != self.owner_workspace_id
                || run.profile.active.profile_id.is_nil()
                || !valid_identifier(&run.profile.active.display_name, 64)
                || run.profile.active.version != PROFILE_VERSION
                || run.profile.active.profile_revision == 0
                || !valid_digest(&run.profile.active.config_digest)
                || run.profile.active.activation_revision == 0
                || !valid_timestamp(&run.profile.active.activated_at)
                || !valid_digest(&run.profile.resolved_component_receipt_sha256)
                || !run_ids.insert(run.run_id)
                || !valid_timestamp(&run.created_at)
                || !valid_timestamp(&run.updated_at)
                || run.target_instrument_id.is_some_and(|id| id.is_nil())
                || run.checkpoint_journal.len() > MAXIMUM_CHECKPOINTS_PER_RUN
                || run.child_jobs.len() > MAXIMUM_CHILD_REFERENCES_PER_RUN
                || run.result_references.len() > MAXIMUM_RESULT_REFERENCES_PER_RUN
                || run.execution_eligibility != ExecutionEligibility::ExecutionIneligible
                || run
                    .last_error
                    .as_ref()
                    .is_some_and(|error| error.is_empty() || error.len() > 1_024)
                || !valid_checkpoint_journal(&run.checkpoint_journal)
                || !run.child_jobs.iter().all(valid_service_job_reference)
                || !run
                    .result_references
                    .iter()
                    .all(valid_service_result_reference)
                || !valid_coverage_receipt(run.coverage_receipt.as_ref())
                || !valid_exclusion_receipt(run.exclusion_receipt.as_ref())
                || !valid_ranking_receipt(run.ranking_receipt.as_ref())
            {
                return Err(DesktopCommandError::new(
                    "analytical_controller_invalid",
                    "A retained Desktop analytical workflow checkpoint is invalid.",
                ));
            }
        }
        Ok(())
    }

    fn default_profile(&self) -> Result<&AnalyticalProfile, DesktopCommandError> {
        self.profiles
            .iter()
            .find(|profile| profile.kind == AnalyticalProfileKind::Default)
            .ok_or_else(DesktopCommandError::internal)
    }

    fn profile(&self, profile_id: Uuid) -> Result<&AnalyticalProfile, DesktopCommandError> {
        self.profiles
            .iter()
            .find(|profile| profile.profile_id == profile_id)
            .ok_or_else(|| {
                DesktopCommandError::new(
                    "profile_not_found",
                    "The selected Desktop analytical profile was not found.",
                )
            })
    }

    fn next_revision(&self) -> Result<u64, DesktopCommandError> {
        self.revision.checked_add(1).ok_or_else(|| {
            DesktopCommandError::new(
                "profile_revision_exhausted",
                "The Desktop analytical profile revision cannot advance.",
            )
        })
    }

    fn append_history(
        &mut self,
        action: ProfileHistoryAction,
        profile: &AnalyticalProfile,
        supersedes_profile_id: Option<Uuid>,
        now: String,
    ) -> Result<(), DesktopCommandError> {
        if self.profile_history.len() >= MAXIMUM_PROFILE_HISTORY {
            return Err(DesktopCommandError::new(
                "profile_history_full",
                "The bounded Desktop profile history is full. Preserve or reset the workspace before making another profile change.",
            ));
        }
        self.profile_history.push(ProfileHistoryEntry {
            event_id: Uuid::new_v4(),
            owner_workspace_id: self.owner_workspace_id,
            controller_revision: self.revision,
            action,
            profile_id: profile.profile_id,
            profile_version: profile.version,
            profile_revision: profile.revision,
            config_digest: profile.config_digest.clone(),
            config: profile.config.clone(),
            validation_receipt: profile.last_validation.clone(),
            recorded_at: now,
            supersedes_profile_id,
        });
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct DesktopAnalyticalController {
    owner_workspace_id: Uuid,
    store: LocalAuthorityStateStore,
    document: Mutex<ControllerDocument>,
}

impl DesktopAnalyticalController {
    pub(crate) fn try_open(
        paths: &LocalPaths,
        owner_workspace_id: Uuid,
    ) -> Result<Self, DesktopCommandError> {
        let root = paths
            .control_root()
            .map_err(|_error| DesktopCommandError::internal())?
            .root()
            .join(CONTROLLER_DIRECTORY);
        Self::try_open_at(&root, owner_workspace_id)
    }

    fn try_open_at(root: &Path, owner_workspace_id: Uuid) -> Result<Self, DesktopCommandError> {
        let store = LocalAuthorityStateStore::try_open(root)
            .map_err(|_error| DesktopCommandError::internal())?;
        let document = match store
            .load()
            .map_err(|_error| DesktopCommandError::internal())?
        {
            Some(payload) => serde_json::from_slice::<ControllerDocument>(&payload)
                .map_err(|_error| DesktopCommandError::internal())?,
            None => {
                let initial = ControllerDocument::initial(owner_workspace_id)?;
                let payload = serde_json::to_vec(&initial)
                    .map_err(|_error| DesktopCommandError::internal())?;
                store
                    .store(&payload)
                    .map_err(|_error| DesktopCommandError::internal())?;
                initial
            }
        };
        document.validate(owner_workspace_id)?;
        Ok(Self {
            owner_workspace_id,
            store,
            document: Mutex::new(document),
        })
    }

    fn dispatch(
        &self,
        request: AnalyticalControllerCommand,
        confirmed: bool,
    ) -> Result<AnalyticalControllerResponse, DesktopCommandError> {
        if request.requires_confirmation() && !confirmed {
            return Err(DesktopCommandError::new(
                "confirmation_required",
                "Confirm the Desktop analytical profile change before continuing.",
            ));
        }
        match request {
            AnalyticalControllerCommand::Status => self.status(),
            AnalyticalControllerCommand::CopyDefault { display_name } => {
                self.copy_default(display_name)
            }
            AnalyticalControllerCommand::UpdateCustom {
                profile_id,
                expected_revision,
                config,
            } => self.update_custom(profile_id, &expected_revision, config),
            AnalyticalControllerCommand::ValidateCustom {
                profile_id,
                expected_revision,
            } => self.validate_custom(profile_id, &expected_revision),
            AnalyticalControllerCommand::CompareWithDefault { profile_id } => {
                self.compare(profile_id)
            }
            AnalyticalControllerCommand::ActivateCustom {
                profile_id,
                expected_revision,
                validation_receipt_id,
            } => self.activate_custom(profile_id, &expected_revision, validation_receipt_id),
            AnalyticalControllerCommand::RestoreDefault {
                expected_activation_revision,
            } => self.restore_default(&expected_activation_revision),
            AnalyticalControllerCommand::History {
                after_revision,
                limit,
            } => self.history(after_revision.as_deref(), limit),
        }
    }

    fn status(&self) -> Result<AnalyticalControllerResponse, DesktopCommandError> {
        let document = self.lock_document()?;
        Ok(AnalyticalControllerResponse::Status {
            controller_schema_version: CONTROLLER_FORMAT_VERSION,
            owner_workspace_id: self.owner_workspace_id,
            controller_revision: document.revision,
            active_profile: document.active_profile.clone(),
            profiles: document.profiles.clone(),
            workflow_runs: document.workflow_runs.clone(),
            workflow_readiness: WorkflowReadiness::blocked(),
        })
    }

    fn copy_default(
        &self,
        display_name: String,
    ) -> Result<AnalyticalControllerResponse, DesktopCommandError> {
        if !valid_display_name(&display_name) || display_name == DEFAULT_PROFILE_NAME {
            return Err(DesktopCommandError::invalid_request(
                "Choose a distinct profile name between 1 and 64 visible characters.",
            ));
        }
        self.mutate(|document, now| {
            if document.profiles.len() >= MAXIMUM_PROFILES {
                return Err(DesktopCommandError::new(
                    "profile_limit_reached",
                    "The bounded Desktop analytical profile limit has been reached.",
                ));
            }
            if document
                .profiles
                .iter()
                .any(|profile| profile.display_name == display_name)
            {
                return Err(DesktopCommandError::invalid_request(
                    "A Desktop analytical profile already uses that name.",
                ));
            }
            let default = document.default_profile()?.clone();
            document.revision = document.next_revision()?;
            let custom = AnalyticalProfile {
                profile_id: Uuid::new_v4(),
                owner_workspace_id: document.owner_workspace_id,
                display_name,
                kind: AnalyticalProfileKind::Custom,
                version: PROFILE_VERSION,
                revision: 1,
                config_digest: default.config_digest,
                config: default.config,
                validation_state: ProfileValidationState::NotValidated,
                last_validation: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            };
            document.append_history(
                ProfileHistoryAction::CopiedDefault,
                &custom,
                Some(default.profile_id),
                now,
            )?;
            document.profiles.push(custom.clone());
            Ok(AnalyticalControllerResponse::Profile { profile: custom })
        })
    }

    fn update_custom(
        &self,
        profile_id: Uuid,
        expected_revision: &str,
        config: AnalyticalProfileConfig,
    ) -> Result<AnalyticalControllerResponse, DesktopCommandError> {
        let expected_revision = parse_revision(expected_revision)?;
        config.validate_shape()?;
        self.mutate(|document, now| {
            let index = document
                .profiles
                .iter()
                .position(|profile| profile.profile_id == profile_id)
                .ok_or_else(|| {
                    DesktopCommandError::new(
                        "profile_not_found",
                        "The selected Desktop analytical profile was not found.",
                    )
                })?;
            let current = document.profiles[index].clone();
            if current.kind != AnalyticalProfileKind::Custom {
                return Err(DesktopCommandError::new(
                    "default_profile_immutable",
                    "Market Squawk Default V1 is immutable. Create a custom copy before changing it.",
                ));
            }
            if document.active_profile.profile_id == profile_id {
                return Err(DesktopCommandError::new(
                    "active_profile_revision_bound",
                    "The active analytical profile revision is immutable. Create another custom copy before changing it.",
                ));
            }
            if current.revision != expected_revision {
                return Err(profile_conflict());
            }
            let next_profile_revision = current.revision.checked_add(1).ok_or_else(|| {
                DesktopCommandError::new(
                    "profile_revision_exhausted",
                    "The Desktop analytical profile revision cannot advance.",
                )
            })?;
            let config_digest = config.digest()?;
            document.revision = document.next_revision()?;
            let updated = AnalyticalProfile {
                revision: next_profile_revision,
                config,
                config_digest,
                validation_state: ProfileValidationState::NotValidated,
                last_validation: None,
                updated_at: now.clone(),
                ..current
            };
            document.profiles[index] = updated.clone();
            document.append_history(
                ProfileHistoryAction::UpdatedCustom,
                &updated,
                Some(profile_id),
                now,
            )?;
            Ok(AnalyticalControllerResponse::Profile { profile: updated })
        })
    }

    fn validate_custom(
        &self,
        profile_id: Uuid,
        expected_revision: &str,
    ) -> Result<AnalyticalControllerResponse, DesktopCommandError> {
        let expected_revision = parse_revision(expected_revision)?;
        self.mutate(|document, now| {
            let default = document.default_profile()?.clone();
            let index = document
                .profiles
                .iter()
                .position(|profile| profile.profile_id == profile_id)
                .ok_or_else(|| {
                    DesktopCommandError::new(
                        "profile_not_found",
                        "The selected Desktop analytical profile was not found.",
                    )
                })?;
            let current = document.profiles[index].clone();
            if current.kind != AnalyticalProfileKind::Custom {
                return Err(DesktopCommandError::new(
                    "default_profile_immutable",
                    "Market Squawk Default V1 is already immutable and does not need custom validation.",
                ));
            }
            if current.revision != expected_revision {
                return Err(profile_conflict());
            }
            document.revision = document.next_revision()?;
            let (validation_state, receipt, blockers, action) =
                if current.config_digest == default.config_digest && current.config == default.config
                {
                    (
                        ProfileValidationState::Validated,
                        Some(ProfileValidationReceipt {
                            receipt_id: Uuid::new_v4(),
                            profile_id,
                            profile_revision: current.revision,
                            config_digest: current.config_digest.clone(),
                            validated_at: now.clone(),
                            basis: ProfileValidationBasis::IdenticalToImmutableDefault,
                            backend_receipts: Vec::new(),
                        }),
                        Vec::new(),
                        ProfileHistoryAction::ValidatedCustom,
                    )
                } else {
                    (
                        ProfileValidationState::Blocked,
                        None,
                        vec![ProfileValidationBlocker {
                            code: "backend_component_validation_required",
                            detail: "One or more custom component bindings differ from Market Squawk Default V1. Their owning backend validators are not yet composed into this Desktop generation.",
                        }],
                        ProfileHistoryAction::ValidationBlocked,
                    )
                };
            let updated = AnalyticalProfile {
                validation_state,
                last_validation: receipt.clone(),
                updated_at: now.clone(),
                ..current
            };
            document.profiles[index] = updated.clone();
            document.append_history(action, &updated, Some(default.profile_id), now)?;
            Ok(AnalyticalControllerResponse::Validation {
                profile: updated,
                receipt,
                blockers,
            })
        })
    }

    fn compare(
        &self,
        profile_id: Uuid,
    ) -> Result<AnalyticalControllerResponse, DesktopCommandError> {
        let document = self.lock_document()?;
        let default = document.default_profile()?;
        let selected = document.profile(profile_id)?;
        let different_components = selected.config.differences_from(&default.config);
        Ok(AnalyticalControllerResponse::Comparison {
            default_profile: default.clone(),
            selected_profile: selected.clone(),
            equivalent: different_components.is_empty(),
            different_components,
        })
    }

    fn activate_custom(
        &self,
        profile_id: Uuid,
        expected_revision: &str,
        validation_receipt_id: Uuid,
    ) -> Result<AnalyticalControllerResponse, DesktopCommandError> {
        let expected_revision = parse_revision(expected_revision)?;
        self.mutate(|document, now| {
            let selected = document.profile(profile_id)?.clone();
            if selected.kind != AnalyticalProfileKind::Custom
                || selected.revision != expected_revision
            {
                return Err(profile_conflict());
            }
            let validated = selected.last_validation.as_ref().is_some_and(|receipt| {
                selected.validation_state == ProfileValidationState::Validated
                    && receipt.receipt_id == validation_receipt_id
                    && receipt.profile_revision == selected.revision
                    && receipt.config_digest == selected.config_digest
            });
            if !validated {
                return Err(DesktopCommandError::new(
                    "profile_validation_required",
                    "Validate this exact custom profile revision before activating it.",
                ));
            }
            let previous = document.active_profile.profile_id;
            let activation_revision = document
                .active_profile
                .activation_revision
                .checked_add(1)
                .ok_or_else(|| {
                    DesktopCommandError::new(
                        "profile_revision_exhausted",
                        "The Desktop analytical activation revision cannot advance.",
                    )
                })?;
            document.revision = document.next_revision()?;
            document.active_profile = selected.binding(activation_revision, now.clone());
            document.append_history(
                ProfileHistoryAction::ActivatedCustom,
                &selected,
                Some(previous),
                now,
            )?;
            Ok(AnalyticalControllerResponse::Activation {
                active_profile: document.active_profile.clone(),
            })
        })
    }

    fn restore_default(
        &self,
        expected_activation_revision: &str,
    ) -> Result<AnalyticalControllerResponse, DesktopCommandError> {
        let expected_activation_revision = parse_revision(expected_activation_revision)?;
        self.mutate(|document, now| {
            if document.active_profile.activation_revision != expected_activation_revision {
                return Err(profile_conflict());
            }
            if document.active_profile.kind == AnalyticalProfileKind::Default {
                return Ok(AnalyticalControllerResponse::Activation {
                    active_profile: document.active_profile.clone(),
                });
            }
            let previous = document.active_profile.profile_id;
            let default = document.default_profile()?.clone();
            let activation_revision = document
                .active_profile
                .activation_revision
                .checked_add(1)
                .ok_or_else(|| {
                    DesktopCommandError::new(
                        "profile_revision_exhausted",
                        "The Desktop analytical activation revision cannot advance.",
                    )
                })?;
            document.revision = document.next_revision()?;
            document.active_profile = default.binding(activation_revision, now.clone());
            document.append_history(
                ProfileHistoryAction::RestoredDefault,
                &default,
                Some(previous),
                now,
            )?;
            Ok(AnalyticalControllerResponse::Activation {
                active_profile: document.active_profile.clone(),
            })
        })
    }

    fn history(
        &self,
        after_revision: Option<&str>,
        limit: u16,
    ) -> Result<AnalyticalControllerResponse, DesktopCommandError> {
        let after_revision = after_revision.map(parse_revision).transpose()?.unwrap_or(0);
        let limit = usize::from(limit);
        if limit == 0 || limit > MAXIMUM_HISTORY_PAGE {
            return Err(DesktopCommandError::invalid_request(
                "The Desktop profile-history page limit is invalid.",
            ));
        }
        let document = self.lock_document()?;
        let available = document
            .profile_history
            .iter()
            .filter(|entry| entry.controller_revision > after_revision)
            .count();
        let entries: Vec<_> = document
            .profile_history
            .iter()
            .filter(|entry| entry.controller_revision > after_revision)
            .take(limit)
            .cloned()
            .collect();
        let next_after_revision = (available > entries.len())
            .then(|| entries.last().map(|entry| entry.controller_revision))
            .flatten();
        Ok(AnalyticalControllerResponse::History {
            completeness: if next_after_revision.is_some() {
                HistoryCompleteness::Truncated
            } else {
                HistoryCompleteness::Complete
            },
            returned_count: entries.len(),
            available_count: available,
            next_after_revision,
            entries,
        })
    }

    fn mutate(
        &self,
        change: impl FnOnce(
            &mut ControllerDocument,
            String,
        ) -> Result<AnalyticalControllerResponse, DesktopCommandError>,
    ) -> Result<AnalyticalControllerResponse, DesktopCommandError> {
        let mut guard = self.lock_document()?;
        let mut candidate = guard.clone();
        let response = change(&mut candidate, unix_nanos_now()?)?;
        candidate.validate(self.owner_workspace_id)?;
        let payload =
            serde_json::to_vec(&candidate).map_err(|_error| DesktopCommandError::internal())?;
        self.store
            .store(&payload)
            .map_err(|_error| DesktopCommandError::internal())?;
        *guard = candidate;
        Ok(response)
    }

    fn lock_document(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, ControllerDocument>, DesktopCommandError> {
        self.document.lock().map_err(|_error| {
            DesktopCommandError::new(
                "analytical_controller_unavailable",
                "The Desktop analytical controller is unavailable.",
            )
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum WorkflowReadinessState {
    Blocked,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowBlocker {
    code: &'static str,
    detail: &'static str,
    owner: &'static str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowReadiness {
    state: WorkflowReadinessState,
    blockers: Vec<WorkflowBlocker>,
}

impl WorkflowReadiness {
    fn blocked() -> Self {
        Self {
            state: WorkflowReadinessState::Blocked,
            blockers: vec![
                WorkflowBlocker {
                    code: "canonical_data_and_backend_composition_required",
                    detail: "The complete canonical market, feature, forecast, valuation, recommendation-ready backtest, portfolio/risk, proposal, and immutable-result path is not yet composed for this Desktop workflow.",
                    owner: "installed_application",
                },
                WorkflowBlocker {
                    code: "desktop_start_resume_not_registered",
                    detail: "This Desktop generation intentionally exposes no Find opportunities or Analyze this investment start command until the required pure capabilities and restart journey exist.",
                    owner: "desktop",
                },
            ],
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProfileValidationBlocker {
    code: &'static str,
    detail: &'static str,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum HistoryCompleteness {
    Complete,
    Truncated,
}

#[derive(Debug, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "action"
)]
pub(crate) enum AnalyticalControllerCommand {
    Status,
    CopyDefault {
        display_name: String,
    },
    UpdateCustom {
        profile_id: Uuid,
        expected_revision: String,
        config: AnalyticalProfileConfig,
    },
    ValidateCustom {
        profile_id: Uuid,
        expected_revision: String,
    },
    CompareWithDefault {
        profile_id: Uuid,
    },
    ActivateCustom {
        profile_id: Uuid,
        expected_revision: String,
        validation_receipt_id: Uuid,
    },
    RestoreDefault {
        expected_activation_revision: String,
    },
    History {
        after_revision: Option<String>,
        limit: u16,
    },
}

impl AnalyticalControllerCommand {
    const fn requires_confirmation(&self) -> bool {
        !matches!(
            self,
            Self::Status | Self::CompareWithDefault { .. } | Self::History { .. }
        )
    }
}

#[derive(Debug, Serialize)]
#[serde(
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    tag = "kind"
)]
enum AnalyticalControllerResponse {
    Status {
        controller_schema_version: u32,
        owner_workspace_id: Uuid,
        #[serde(with = "decimal_u64")]
        controller_revision: u64,
        active_profile: ActiveProfileBinding,
        profiles: Vec<AnalyticalProfile>,
        workflow_runs: Vec<DesktopWorkflowRun>,
        workflow_readiness: WorkflowReadiness,
    },
    Profile {
        profile: AnalyticalProfile,
    },
    Validation {
        profile: AnalyticalProfile,
        receipt: Option<ProfileValidationReceipt>,
        blockers: Vec<ProfileValidationBlocker>,
    },
    Comparison {
        default_profile: AnalyticalProfile,
        selected_profile: AnalyticalProfile,
        equivalent: bool,
        different_components: Vec<AnalyticalProfileComponent>,
    },
    Activation {
        active_profile: ActiveProfileBinding,
    },
    History {
        completeness: HistoryCompleteness,
        returned_count: usize,
        available_count: usize,
        #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
        next_after_revision: Option<u64>,
        entries: Vec<ProfileHistoryEntry>,
    },
}

#[tauri::command]
pub(crate) async fn analytical_controller(
    request: AnalyticalControllerCommand,
    confirmed: bool,
    state: State<'_, crate::bridge::DesktopState>,
) -> Result<serde_json::Value, DesktopCommandError> {
    let generation = state.generation()?;
    let _fence = generation.analytical_retirement_fence().await;
    state.admit_current(&generation)?;
    let response = generation
        .analytical_controller()
        .dispatch(request, confirmed)?;
    state.admit_current(&generation)?;
    serde_json::to_value(response).map_err(|_error| DesktopCommandError::internal())
}

fn valid_validation_receipt(receipt: &ProfileValidationReceipt) -> bool {
    !receipt.receipt_id.is_nil()
        && !receipt.profile_id.is_nil()
        && receipt.profile_revision > 0
        && valid_digest(&receipt.config_digest)
        && valid_timestamp(&receipt.validated_at)
        && receipt.backend_receipts.len() <= MAXIMUM_RESULT_REFERENCES_PER_RUN
        && receipt
            .backend_receipts
            .iter()
            .all(valid_service_result_reference)
        && match receipt.basis {
            ProfileValidationBasis::IdenticalToImmutableDefault => {
                receipt.backend_receipts.is_empty()
            }
            ProfileValidationBasis::BackendComponentReceipts => {
                !receipt.backend_receipts.is_empty()
            }
        }
}

fn valid_service_result_reference(reference: &ServiceResultReference) -> bool {
    valid_identifier(&reference.operation, 128)
        && valid_identifier(&reference.result_id, 256)
        && valid_digest(&reference.content_sha256)
}

fn valid_service_job_reference(reference: &ServiceJobReference) -> bool {
    !reference.job_id.is_nil()
        && valid_positive_decimal(&reference.generation)
        && reference
            .terminal_sequence
            .as_deref()
            .is_none_or(valid_unsigned_decimal)
        && reference
            .result
            .as_ref()
            .is_none_or(valid_service_result_reference)
}

fn valid_checkpoint_journal(checkpoints: &[WorkflowCheckpoint]) -> bool {
    let mut previous = 0_u64;
    checkpoints.iter().all(|checkpoint| {
        if checkpoint.sequence == 0
            || checkpoint.sequence <= previous
            || !valid_timestamp(&checkpoint.recorded_at)
            || checkpoint
                .child_job
                .as_ref()
                .is_some_and(|reference| !valid_service_job_reference(reference))
            || checkpoint
                .result
                .as_ref()
                .is_some_and(|reference| !valid_service_result_reference(reference))
        {
            return false;
        }
        previous = checkpoint.sequence;
        true
    })
}

fn valid_coverage_receipt(receipt: Option<&CoverageReceipt>) -> bool {
    receipt.is_none_or(|receipt| {
        let counts = &receipt.counts;
        let accounted = counts
            .generated
            .checked_add(counts.no_action)
            .and_then(|count| count.checked_add(counts.unavailable));
        !receipt.receipt_id.is_nil()
            && valid_digest(&receipt.content_sha256)
            && counts.complete_evidence <= counts.searched
            && counts.excluded <= counts.searched
            && counts.deeply_analyzed <= counts.complete_evidence
            && accounted.is_some_and(|count| count <= counts.deeply_analyzed)
    })
}

fn valid_exclusion_receipt(receipt: Option<&ExclusionReceipt>) -> bool {
    receipt.is_none_or(|receipt| {
        !receipt.receipt_id.is_nil()
            && valid_service_result_reference(&receipt.reasons_result)
            && valid_digest(&receipt.content_sha256)
    })
}

fn valid_ranking_receipt(receipt: Option<&RankingReceipt>) -> bool {
    receipt.is_none_or(|receipt| {
        let mut result_ids = HashSet::with_capacity(receipt.ordered_result_ids.len());
        !receipt.receipt_id.is_nil()
            && !receipt.ordered_result_ids.is_empty()
            && receipt.ordered_result_ids.len() <= MAXIMUM_RESULT_REFERENCES_PER_RUN
            && receipt
                .ordered_result_ids
                .iter()
                .all(|result_id| valid_identifier(result_id, 256) && result_ids.insert(result_id))
            && valid_service_result_reference(&receipt.policy_result)
            && valid_digest(&receipt.content_sha256)
    })
}

fn profile_conflict() -> DesktopCommandError {
    DesktopCommandError::new(
        "profile_conflict",
        "The Desktop analytical profile changed. Refresh it before continuing.",
    )
}

fn default_profile_id() -> Uuid {
    Uuid::new_v5(&DEFAULT_PROFILE_NAMESPACE, DEFAULT_PROFILE_NAME.as_bytes())
}

fn valid_display_name(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed == value
        && !value.is_empty()
        && value.chars().count() <= 64
        && !value.chars().any(char::is_control)
}

fn valid_identifier(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.is_ascii()
        && !value.chars().any(char::is_control)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        && value.bytes().any(|byte| byte != b'0')
}

fn valid_unsigned_decimal(value: &str) -> bool {
    !value.is_empty()
        && (value == "0" || !value.starts_with('0'))
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.parse::<u64>().is_ok()
}

fn valid_positive_decimal(value: &str) -> bool {
    valid_unsigned_decimal(value) && value != "0"
}

fn valid_timestamp(value: &str) -> bool {
    valid_positive_decimal(value)
}

fn parse_revision(value: &str) -> Result<u64, DesktopCommandError> {
    if !valid_unsigned_decimal(value) {
        return Err(DesktopCommandError::invalid_request(
            "The Desktop analytical revision is invalid.",
        ));
    }
    value.parse().map_err(|_error| {
        DesktopCommandError::invalid_request("The Desktop analytical revision is invalid.")
    })
}

fn unix_nanos_now() -> Result<String, DesktopCommandError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| DesktopCommandError::internal())?;
    Ok(elapsed.as_nanos().to_string())
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    use std::fmt::Write as _;

    let bytes = bytes.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _infallible = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

fn serialize_optional_u64_as_decimal<S>(
    value: &Option<u64>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}

mod decimal_u64 {
    use serde::{Deserialize as _, Deserializer, Serializer};

    pub(super) fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(value)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.is_empty()
            || (value.starts_with('0') && value != "0")
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(serde::de::Error::custom(
                "expected a canonical unsigned decimal string",
            ));
        }
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        AnalyticalControllerCommand, AnalyticalControllerResponse, AnalyticalProfileKind,
        DesktopAnalyticalController,
    };

    #[test]
    fn default_profile_survives_restart_and_restores_after_validated_custom_activation()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "market-squawk-desktop-profile-{}",
            uuid::Uuid::new_v4()
        ));
        let workspace_id = uuid::Uuid::new_v4();
        let paths = market_squawk_platform::LocalPaths::prepare(&root)?;

        let controller = DesktopAnalyticalController::try_open(&paths, workspace_id)?;
        let custom = match controller.dispatch(
            AnalyticalControllerCommand::CopyDefault {
                display_name: "My validated copy".to_owned(),
            },
            true,
        )? {
            AnalyticalControllerResponse::Profile { profile } => profile,
            _ => return Err("unexpected copy response".into()),
        };
        let receipt = match controller.dispatch(
            AnalyticalControllerCommand::ValidateCustom {
                profile_id: custom.profile_id,
                expected_revision: custom.revision.to_string(),
            },
            true,
        )? {
            AnalyticalControllerResponse::Validation {
                receipt: Some(receipt),
                ..
            } => receipt,
            _ => return Err("unexpected validation response".into()),
        };
        let active = match controller.dispatch(
            AnalyticalControllerCommand::ActivateCustom {
                profile_id: custom.profile_id,
                expected_revision: custom.revision.to_string(),
                validation_receipt_id: receipt.receipt_id,
            },
            true,
        )? {
            AnalyticalControllerResponse::Activation { active_profile } => active_profile,
            _ => return Err("unexpected activation response".into()),
        };
        assert_eq!(active.kind, AnalyticalProfileKind::Custom);
        drop(controller);

        let restarted = DesktopAnalyticalController::try_open(&paths, workspace_id)?;
        let active_after_restart = match restarted.status()? {
            AnalyticalControllerResponse::Status { active_profile, .. } => active_profile,
            _ => return Err("unexpected status response".into()),
        };
        assert_eq!(active_after_restart.profile_id, custom.profile_id);
        restarted.dispatch(
            AnalyticalControllerCommand::RestoreDefault {
                expected_activation_revision: active_after_restart.activation_revision.to_string(),
            },
            true,
        )?;
        drop(restarted);

        let restored = DesktopAnalyticalController::try_open(&paths, workspace_id)?;
        match restored.status()? {
            AnalyticalControllerResponse::Status { active_profile, .. } => {
                assert_eq!(active_profile.kind, AnalyticalProfileKind::Default);
                assert_eq!(active_profile.activation_revision, 3);
            }
            _ => return Err("unexpected restored status response".into()),
        }
        drop(restored);
        drop(paths);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
