use market_squawk_runtime::WorkspaceId;
use serde::{Deserialize, Serialize};

use super::{SETUP_PLAN_FORMAT_VERSION, SetupPlanError};

pub(super) const WORKSPACE_SOFT_LIMIT_BYTES: u64 = 50 * 1024_u64.pow(3);
const LOG_RETENTION_DAYS: u16 = 30;
const BACKUP_RETENTION_COUNT: u16 = 8;
const FIRST_USE_TARGET_MINUTES: u16 = 5;

/// Closed goals supported by the durable V1 setup planner.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupGoal {
    /// Configure the complete recommended local product.
    EverythingRecommended,
    /// Reach a verified public-market result first.
    ExplorePublicMarkets,
    /// Build point-in-time investment research.
    ResearchInvestments,
    /// Import and analyze an owned portfolio.
    ManagePortfolio,
    /// Use the managed model runtimes and research workflow.
    BuildAndEvaluateModels,
    /// Configure safe paper-only practice under central risk.
    PracticePaperExecution,
    /// Connect the separately owned Claude Code MCP client.
    UseClaudeCode,
    /// Connect the separately owned Codex MCP client.
    UseCodex,
}

impl SetupGoal {
    pub(super) const ALL: [Self; 8] = [
        Self::EverythingRecommended,
        Self::ExplorePublicMarkets,
        Self::ResearchInvestments,
        Self::ManagePortfolio,
        Self::BuildAndEvaluateModels,
        Self::PracticePaperExecution,
        Self::UseClaudeCode,
        Self::UseCodex,
    ];
}

/// Code-owned V1 starter-plan choices; callers cannot supply arbitrary actions.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupStarterPlan {
    /// Complete every recommended configuration outcome.
    EverythingRecommended,
    /// Prioritize a public market snapshot and quality evidence.
    PublicMarkets,
    /// Prioritize point-in-time research and a research result.
    Research,
    /// Prioritize portfolio import, reconciliation, risk, and backup.
    Portfolio,
    /// Prioritize managed model readiness and a forecast result.
    Models,
    /// Prioritize stopped paper-only configuration and central risk.
    PaperPractice,
    /// Prioritize separately detected and registered local MCP clients.
    AiClients,
}

impl SetupStarterPlan {
    pub(super) const ALL: [Self; 7] = [
        Self::EverythingRecommended,
        Self::PublicMarkets,
        Self::Research,
        Self::Portfolio,
        Self::Models,
        Self::PaperPractice,
        Self::AiClients,
    ];

    fn matches_any(self, goals: &[SetupGoal]) -> bool {
        match self {
            Self::EverythingRecommended => goals == [SetupGoal::EverythingRecommended],
            Self::PublicMarkets => goals.contains(&SetupGoal::ExplorePublicMarkets),
            Self::Research => goals.contains(&SetupGoal::ResearchInvestments),
            Self::Portfolio => goals.contains(&SetupGoal::ManagePortfolio),
            Self::Models => goals.contains(&SetupGoal::BuildAndEvaluateModels),
            Self::PaperPractice => goals.contains(&SetupGoal::PracticePaperExecution),
            Self::AiClients => {
                goals.contains(&SetupGoal::UseClaudeCode) || goals.contains(&SetupGoal::UseCodex)
            }
        }
    }
}

/// Versioned catalog returned to setup clients before a plan is selected.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupPlanCatalog {
    format_version: u16,
    goals: [SetupGoal; 8],
    starter_plans: [SetupStarterPlan; 7],
    recommended_starter_plan: SetupStarterPlan,
}

impl SetupPlanCatalog {
    pub(super) const fn current() -> Self {
        Self {
            format_version: SETUP_PLAN_FORMAT_VERSION,
            goals: SetupGoal::ALL,
            starter_plans: SetupStarterPlan::ALL,
            recommended_starter_plan: SetupStarterPlan::EverythingRecommended,
        }
    }

    /// Returns the closed setup-plan format revision.
    #[must_use]
    pub const fn format_version(self) -> u16 {
        self.format_version
    }

    /// Returns every admitted setup goal in stable display order.
    #[must_use]
    pub const fn goals(&self) -> &[SetupGoal; 8] {
        &self.goals
    }

    /// Returns every code-owned starter plan in stable display order.
    #[must_use]
    pub const fn starter_plans(&self) -> &[SetupStarterPlan; 7] {
        &self.starter_plans
    }

    /// Returns the dominant default-complete starter plan.
    #[must_use]
    pub const fn recommended_starter_plan(self) -> SetupStarterPlan {
        self.recommended_starter_plan
    }
}

/// Validated goal selection and one compatible code-owned starter plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetupPlanSelection {
    goals: Vec<SetupGoal>,
    starter_plan: SetupStarterPlan,
}

impl SetupPlanSelection {
    /// Selects the complete recommended plan for the special all-inclusive goal, or the
    /// deterministic most-specific starter for a bounded explicit goal set.
    pub fn recommended(goals: Vec<SetupGoal>) -> Result<Self, SetupPlanError> {
        let goals = canonicalize_goals(goals)?;
        let starter_plan = if goals == [SetupGoal::EverythingRecommended] {
            SetupStarterPlan::EverythingRecommended
        } else if goals.contains(&SetupGoal::ManagePortfolio) {
            SetupStarterPlan::Portfolio
        } else if goals.contains(&SetupGoal::BuildAndEvaluateModels) {
            SetupStarterPlan::Models
        } else if goals.contains(&SetupGoal::PracticePaperExecution) {
            SetupStarterPlan::PaperPractice
        } else if goals.contains(&SetupGoal::ResearchInvestments) {
            SetupStarterPlan::Research
        } else if goals.contains(&SetupGoal::ExplorePublicMarkets) {
            SetupStarterPlan::PublicMarkets
        } else {
            SetupStarterPlan::AiClients
        };
        Self::try_new(goals, starter_plan)
    }

    /// Admits one explicit starter only when it is a code-owned choice compatible with a selected
    /// goal. The special all-inclusive goal cannot be mixed with narrower goals or plans.
    pub fn try_new(
        goals: Vec<SetupGoal>,
        starter_plan: SetupStarterPlan,
    ) -> Result<Self, SetupPlanError> {
        let goals = canonicalize_goals(goals)?;
        if !starter_plan.matches_any(&goals) {
            return Err(SetupPlanError::InvalidSelection);
        }
        Ok(Self {
            goals,
            starter_plan,
        })
    }

    /// Returns the selected goals in canonical order.
    #[must_use]
    pub fn goals(&self) -> &[SetupGoal] {
        &self.goals
    }

    /// Returns the selected code-owned starter plan.
    #[must_use]
    pub const fn starter_plan(&self) -> SetupStarterPlan {
        self.starter_plan
    }

    pub(super) fn validate(&self) -> Result<(), SetupPlanError> {
        validate_goals(&self.goals)?;
        if !self.starter_plan.matches_any(&self.goals) {
            return Err(SetupPlanError::InvalidSelection);
        }
        Ok(())
    }
}

fn canonicalize_goals(mut goals: Vec<SetupGoal>) -> Result<Vec<SetupGoal>, SetupPlanError> {
    if goals.is_empty() || goals.len() > SetupGoal::ALL.len() {
        return Err(SetupPlanError::InvalidSelection);
    }
    goals.sort_unstable();
    validate_goals(&goals)?;
    Ok(goals)
}

fn validate_goals(goals: &[SetupGoal]) -> Result<(), SetupPlanError> {
    if goals.is_empty() || goals.len() > SetupGoal::ALL.len() {
        return Err(SetupPlanError::InvalidSelection);
    }
    if goals.windows(2).any(|pair| {
        pair.first()
            .zip(pair.get(1))
            .is_some_and(|(left, right)| left >= right)
    }) {
        return Err(SetupPlanError::InvalidSelection);
    }
    if goals.contains(&SetupGoal::EverythingRecommended)
        && goals != [SetupGoal::EverythingRecommended]
    {
        return Err(SetupPlanError::InvalidSelection);
    }
    Ok(())
}

/// Stable ordered identity for every durable setup-plan step.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupStepId {
    /// Goal selection and starter-plan confirmation.
    GoalsAndStarterPlan,
    /// Managed workspace, retention, time, and disk policy.
    StorageRetentionTimeAndDisk,
    /// Public and zero-fee provider outcomes.
    PublicAndZeroFeeProviders,
    /// Controlled owned-file and portfolio imports.
    FileAndPortfolioImport,
    /// Managed Python, native, and ONNX runtime readiness.
    ModelRuntime,
    /// Stopped paper-only defaults and central risk.
    PaperAndRisk,
    /// Separately detected Claude Code client.
    ClaudeCode,
    /// Separately detected Codex client.
    Codex,
    /// Verified backup and retention policy.
    Backup,
    /// Exact capability, gap, and change review.
    Review,
    /// Real owner-derived first useful result.
    FirstUsefulResult,
}

/// Closed user outcome unlocked by one planned setup step.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupOutcome {
    /// A durable resumable workspace plan exists.
    DurableResumablePlan,
    /// Workspace retention, time, and disk policies are explicit and governed.
    GovernedWorkspaceBudget,
    /// Provider results carry exact coverage, freshness, and quality evidence.
    QualityLabeledProviderEvidence,
    /// Owned imports produce durable lineage and reconciliation receipts.
    ReceiptBoundLocalData,
    /// Managed Python, native, and ONNX runtime identities are verified.
    VerifiedLocalModelRuntime,
    /// Paper operation remains stopped and subject to central risk.
    StoppedPaperUnderCentralRisk,
    /// Claude Code has a separate owned registration and real safe-read result.
    VerifiedClaudeCodeMcp,
    /// Codex has a separate owned registration and real safe-read result.
    VerifiedCodexMcp,
    /// A coherent recovery point has passed verification.
    VerifiedRecoveryPoint,
    /// Actual capability gaps, safe skips, contacts, and reversible changes are reviewed.
    CapabilityGapReview,
    /// One real result or an honest owner-derived blocker opens from Overview.
    FirstUsefulResult,
}

/// Whether the selected starter performs a step now or leaves its installed capability available.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupStepDisposition {
    /// The selected starter performs this outcome now.
    Included,
    /// The installed capability stays available for an explicit later resume.
    AvailableToFinishLater,
}

/// Explicit skip semantics; skipping never satisfies the step's later owner-derived predicate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupSafeSkip {
    /// The plan cannot advance past this setup contract without retaining it as incomplete.
    NotSkippable,
    /// An explicit skip preserves installed capability and never records owner success.
    CapabilityRemainsInstalledAndAvailable,
}

/// Closed input category disclosed before a setup step begins.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupRequiredInput {
    /// The step needs only existing product authority facts.
    None,
    /// The step requires an explicit local user confirmation.
    LocalConfirmation,
    /// The step requires a user-owned local storage destination or budget.
    LocalDisk,
    /// The step may require a zero-fee account or provider-issued key.
    ZeroFeeAccountOrProviderKey,
    /// The step requires a user-selected owned file through controlled staging.
    OwnedFile,
    /// The step requires a supported local client discovered by its owning adapter.
    DetectedLocalClient,
}

/// Official external systems a selected plan may contact. No arbitrary URL is represented.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupExternalContact {
    /// Coinbase's official public market-data API.
    CoinbasePublicApi,
    /// Kraken's official public market-data API.
    KrakenPublicApi,
    /// The official U.S. Securities and Exchange Commission service.
    SecuritiesAndExchangeCommission,
    /// The official U.S. Bureau of Labor Statistics service.
    BureauOfLaborStatistics,
    /// The official U.S. Treasury service.
    UnitedStatesTreasury,
    /// The official FRED/ALFRED service at the Federal Reserve Bank of St. Louis.
    FederalReserveBankOfStLouis,
    /// The installed Claude Code official CLI.
    ClaudeCodeOfficialCli,
    /// The installed Codex official CLI.
    CodexOfficialCli,
}

/// Reversible local changes disclosed by the immutable plan preview.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupReversibleLocalChange {
    /// Retain a replaceable workspace setup plan.
    AcceptWorkspacePlan,
    /// Apply typed workspace retention and disk-budget settings with rollback.
    ConfigureWorkspaceRetentionAndBudget,
    /// Activate provider sessions that can later be stopped or removed.
    ActivateOrRemoveProviderSessions,
    /// Publish controlled imports whose derived local data can later be removed.
    ImportOrRemoveDerivedLocalData,
    /// Configure the installed model runtime through its typed settings authority.
    ConfigureOrResetModelRuntime,
    /// Configure stopped paper-only defaults that can be reset without starting execution.
    ConfigureStoppedPaperAccountAndRiskDefaults,
    /// Register one owned Claude Code entry that can be disconnected.
    RegisterOrDisconnectClaudeCode,
    /// Register one owned Codex entry that can be disconnected.
    RegisterOrDisconnectCodex,
    /// Create a verified backup policy that can later be changed or removed.
    CreateOrRemoveBackupPolicy,
}

/// Honest disk-impact class; variable content remains fenced by the selected workspace budget.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupDiskImpact {
    /// The complete product is already installed; this step adds no product bytes.
    NoAdditionalProductBytes,
    /// User/provider content varies but counts against the workspace soft limit.
    VariableWithinWorkspaceSoftLimit,
    /// Backup bytes vary with selected content and the separately approved destination.
    VariableBackupDestination,
}

/// Time semantics applied to setup-created research and evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupTimePolicy {
    /// Research uses point-in-time decisions and calls observation provenance first observed locally.
    PointInTimeWithFirstObservedLocallyProvenance,
}

/// Public or zero-fee provider result the plan seeks without claiming it already exists.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupProviderOutcome {
    /// A quality-labelled Coinbase public market snapshot.
    CoinbasePublicMarketSnapshot,
    /// A quality-labelled Kraken public market snapshot.
    KrakenPublicMarketSnapshot,
    /// SEC filing research under its exact provider authority.
    SecFilingResearch,
    /// BLS macro research under its exact public dataset authority.
    BlsMacroResearch,
    /// Treasury rates research under its exact public dataset authority.
    TreasuryRatesResearch,
    /// FRED/ALFRED research after its protected key and exact dataset, series, and vintage range are verified.
    FredAlfredAuthorizedResearch,
}

/// Controlled local formats admitted by the import step.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupImportFormat {
    /// Comma-separated values.
    Csv,
    /// One bounded JSON document.
    Json,
    /// Newline-delimited JSON records.
    Ndjson,
    /// A bounded Parquet input.
    Parquet,
    /// A supported portfolio-specific input.
    PortfolioFile,
}

/// Planned first result; live owner facts later prove or reject its availability.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupFirstResult {
    /// A verified public market snapshot with time and quality evidence.
    VerifiedPublicMarketSnapshot,
    /// A point-in-time research result with source lineage.
    PointInTimeResearchResult,
    /// An imported and reconciled portfolio summary.
    ReconciledPortfolioSummary,
    /// A forecast produced by an admitted model bundle.
    AdmittedModelForecast,
    /// A stopped paper-only account and central-risk review.
    StoppedPaperAndRiskReview,
    /// A real safe read through a separately verified MCP client.
    VerifiedMcpSafeRead,
}

/// Closed choice payload for one plan step.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SetupStepChoice {
    /// Selected goals and the compatible code-owned starter.
    Goals {
        starter_plan: SetupStarterPlan,
        goals: Vec<SetupGoal>,
    },
    /// Managed local storage, retention, and point-in-time policy.
    Storage {
        retention_days: u16,
        workspace_soft_limit_bytes: u64,
        time_policy: SetupTimePolicy,
    },
    /// Provider outcomes sought without claiming current readiness.
    Providers {
        outcomes: Box<[SetupProviderOutcome]>,
    },
    /// Controlled input formats and receipt requirements.
    Imports {
        formats: Box<[SetupImportFormat]>,
        preserve_source_identity: bool,
        require_reconciliation_receipt: bool,
    },
    /// Installed managed model-runtime components to verify.
    ModelRuntime {
        managed_python: bool,
        native_inference: bool,
        onnx_inference: bool,
    },
    /// Safety invariants for the paper starter.
    PaperRisk {
        starts_stopped: bool,
        paper_only: bool,
        central_risk_required: bool,
    },
    /// Separate Claude Code registration and verification requirements.
    ClaudeCode {
        separate_client_credential: bool,
        require_real_safe_read: bool,
    },
    /// Separate Codex registration and verification requirements.
    Codex {
        separate_client_credential: bool,
        require_real_safe_read: bool,
    },
    /// Verified backup and bounded retention choice.
    Backup {
        retention_count: u16,
        verify_after_create: bool,
    },
    /// Review disclosure requirements.
    Review {
        show_gaps_and_reversible_changes: bool,
    },
    /// Preferred owner-derived first result and normal-condition target.
    FirstUsefulResult {
        result: SetupFirstResult,
        target_minutes: u16,
    },
}

/// One immutable, closed, redacted plan step. Live completion does not belong to this value.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetupPlanStep {
    id: SetupStepId,
    outcome: SetupOutcome,
    disposition: SetupStepDisposition,
    required_input: SetupRequiredInput,
    external_contacts: Box<[SetupExternalContact]>,
    reversible_local_change: Option<SetupReversibleLocalChange>,
    expected_active_minutes: u16,
    disk_impact: SetupDiskImpact,
    safe_skip: SetupSafeSkip,
    choice: SetupStepChoice,
}

impl SetupPlanStep {
    /// Returns the stable step identity.
    #[must_use]
    pub const fn id(&self) -> SetupStepId {
        self.id
    }

    /// Returns the exact user outcome this step is intended to unlock.
    #[must_use]
    pub const fn outcome(&self) -> SetupOutcome {
        self.outcome
    }

    /// Returns whether this starter performs the step or keeps it available for later.
    #[must_use]
    pub const fn disposition(&self) -> SetupStepDisposition {
        self.disposition
    }

    /// Returns the closed input class disclosed before the step begins.
    #[must_use]
    pub const fn required_input(&self) -> SetupRequiredInput {
        self.required_input
    }

    /// Returns only official external contacts admitted for the step.
    #[must_use]
    pub fn external_contacts(&self) -> &[SetupExternalContact] {
        &self.external_contacts
    }

    /// Returns the disclosed reversible local change, if the step changes local state.
    #[must_use]
    pub const fn reversible_local_change(&self) -> Option<SetupReversibleLocalChange> {
        self.reversible_local_change
    }

    /// Returns expected active minutes, excluding provider-controlled external waits.
    #[must_use]
    pub const fn expected_active_minutes(&self) -> u16 {
        self.expected_active_minutes
    }

    /// Returns the honest disk-impact class.
    #[must_use]
    pub const fn disk_impact(&self) -> SetupDiskImpact {
        self.disk_impact
    }

    /// Returns the explicit safe-skip rule.
    #[must_use]
    pub const fn safe_skip(&self) -> SetupSafeSkip {
        self.safe_skip
    }

    /// Returns the step's closed choice payload.
    #[must_use]
    pub const fn choice(&self) -> &SetupStepChoice {
        &self.choice
    }
}

/// Immutable code-owned plan retained by the workspace setup authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetupPlan {
    format_version: u16,
    revision: u64,
    selection: SetupPlanSelection,
    steps: [SetupPlanStep; 11],
}

impl SetupPlan {
    pub(super) fn try_build(
        revision: u64,
        selection: SetupPlanSelection,
    ) -> Result<Self, SetupPlanError> {
        if revision == 0 {
            return Err(SetupPlanError::InvalidRevision);
        }
        selection.validate()?;
        let starter = selection.starter_plan;
        let result = first_result(starter);
        let steps = [
            step(
                SetupStepId::GoalsAndStarterPlan,
                SetupOutcome::DurableResumablePlan,
                SetupStepDisposition::Included,
                SetupRequiredInput::LocalConfirmation,
                &[],
                Some(SetupReversibleLocalChange::AcceptWorkspacePlan),
                2,
                SetupDiskImpact::NoAdditionalProductBytes,
                SetupSafeSkip::NotSkippable,
                SetupStepChoice::Goals {
                    starter_plan: starter,
                    goals: selection.goals.clone(),
                },
            ),
            step(
                SetupStepId::StorageRetentionTimeAndDisk,
                SetupOutcome::GovernedWorkspaceBudget,
                SetupStepDisposition::Included,
                SetupRequiredInput::LocalDisk,
                &[],
                Some(SetupReversibleLocalChange::ConfigureWorkspaceRetentionAndBudget),
                1,
                SetupDiskImpact::NoAdditionalProductBytes,
                SetupSafeSkip::NotSkippable,
                SetupStepChoice::Storage {
                    retention_days: LOG_RETENTION_DAYS,
                    workspace_soft_limit_bytes: WORKSPACE_SOFT_LIMIT_BYTES,
                    time_policy: SetupTimePolicy::PointInTimeWithFirstObservedLocallyProvenance,
                },
            ),
            step(
                SetupStepId::PublicAndZeroFeeProviders,
                SetupOutcome::QualityLabeledProviderEvidence,
                SetupStepDisposition::Included,
                SetupRequiredInput::ZeroFeeAccountOrProviderKey,
                &[
                    SetupExternalContact::CoinbasePublicApi,
                    SetupExternalContact::KrakenPublicApi,
                    SetupExternalContact::SecuritiesAndExchangeCommission,
                    SetupExternalContact::BureauOfLaborStatistics,
                    SetupExternalContact::UnitedStatesTreasury,
                    SetupExternalContact::FederalReserveBankOfStLouis,
                ],
                Some(SetupReversibleLocalChange::ActivateOrRemoveProviderSessions),
                5,
                SetupDiskImpact::VariableWithinWorkspaceSoftLimit,
                SetupSafeSkip::CapabilityRemainsInstalledAndAvailable,
                SetupStepChoice::Providers {
                    outcomes: Box::from([
                        SetupProviderOutcome::CoinbasePublicMarketSnapshot,
                        SetupProviderOutcome::KrakenPublicMarketSnapshot,
                        SetupProviderOutcome::SecFilingResearch,
                        SetupProviderOutcome::BlsMacroResearch,
                        SetupProviderOutcome::TreasuryRatesResearch,
                        SetupProviderOutcome::FredAlfredAuthorizedResearch,
                    ]),
                },
            ),
            step(
                SetupStepId::FileAndPortfolioImport,
                SetupOutcome::ReceiptBoundLocalData,
                disposition(starter, SetupStepId::FileAndPortfolioImport),
                SetupRequiredInput::OwnedFile,
                &[],
                Some(SetupReversibleLocalChange::ImportOrRemoveDerivedLocalData),
                5,
                SetupDiskImpact::VariableWithinWorkspaceSoftLimit,
                SetupSafeSkip::CapabilityRemainsInstalledAndAvailable,
                SetupStepChoice::Imports {
                    formats: Box::from([
                        SetupImportFormat::Csv,
                        SetupImportFormat::Json,
                        SetupImportFormat::Ndjson,
                        SetupImportFormat::Parquet,
                        SetupImportFormat::PortfolioFile,
                    ]),
                    preserve_source_identity: true,
                    require_reconciliation_receipt: true,
                },
            ),
            step(
                SetupStepId::ModelRuntime,
                SetupOutcome::VerifiedLocalModelRuntime,
                disposition(starter, SetupStepId::ModelRuntime),
                SetupRequiredInput::None,
                &[],
                Some(SetupReversibleLocalChange::ConfigureOrResetModelRuntime),
                2,
                SetupDiskImpact::VariableWithinWorkspaceSoftLimit,
                SetupSafeSkip::CapabilityRemainsInstalledAndAvailable,
                SetupStepChoice::ModelRuntime {
                    managed_python: true,
                    native_inference: true,
                    onnx_inference: true,
                },
            ),
            step(
                SetupStepId::PaperAndRisk,
                SetupOutcome::StoppedPaperUnderCentralRisk,
                disposition(starter, SetupStepId::PaperAndRisk),
                SetupRequiredInput::LocalConfirmation,
                &[],
                Some(SetupReversibleLocalChange::ConfigureStoppedPaperAccountAndRiskDefaults),
                2,
                SetupDiskImpact::NoAdditionalProductBytes,
                SetupSafeSkip::CapabilityRemainsInstalledAndAvailable,
                SetupStepChoice::PaperRisk {
                    starts_stopped: true,
                    paper_only: true,
                    central_risk_required: true,
                },
            ),
            step(
                SetupStepId::ClaudeCode,
                SetupOutcome::VerifiedClaudeCodeMcp,
                disposition_for_client(&selection, SetupGoal::UseClaudeCode),
                SetupRequiredInput::DetectedLocalClient,
                &[SetupExternalContact::ClaudeCodeOfficialCli],
                Some(SetupReversibleLocalChange::RegisterOrDisconnectClaudeCode),
                2,
                SetupDiskImpact::NoAdditionalProductBytes,
                SetupSafeSkip::CapabilityRemainsInstalledAndAvailable,
                SetupStepChoice::ClaudeCode {
                    separate_client_credential: true,
                    require_real_safe_read: true,
                },
            ),
            step(
                SetupStepId::Codex,
                SetupOutcome::VerifiedCodexMcp,
                disposition_for_client(&selection, SetupGoal::UseCodex),
                SetupRequiredInput::DetectedLocalClient,
                &[SetupExternalContact::CodexOfficialCli],
                Some(SetupReversibleLocalChange::RegisterOrDisconnectCodex),
                2,
                SetupDiskImpact::NoAdditionalProductBytes,
                SetupSafeSkip::CapabilityRemainsInstalledAndAvailable,
                SetupStepChoice::Codex {
                    separate_client_credential: true,
                    require_real_safe_read: true,
                },
            ),
            step(
                SetupStepId::Backup,
                SetupOutcome::VerifiedRecoveryPoint,
                SetupStepDisposition::Included,
                SetupRequiredInput::LocalDisk,
                &[],
                Some(SetupReversibleLocalChange::CreateOrRemoveBackupPolicy),
                5,
                SetupDiskImpact::VariableBackupDestination,
                SetupSafeSkip::CapabilityRemainsInstalledAndAvailable,
                SetupStepChoice::Backup {
                    retention_count: BACKUP_RETENTION_COUNT,
                    verify_after_create: true,
                },
            ),
            step(
                SetupStepId::Review,
                SetupOutcome::CapabilityGapReview,
                SetupStepDisposition::Included,
                SetupRequiredInput::None,
                &[],
                None,
                2,
                SetupDiskImpact::NoAdditionalProductBytes,
                SetupSafeSkip::NotSkippable,
                SetupStepChoice::Review {
                    show_gaps_and_reversible_changes: true,
                },
            ),
            step(
                SetupStepId::FirstUsefulResult,
                SetupOutcome::FirstUsefulResult,
                SetupStepDisposition::Included,
                SetupRequiredInput::None,
                &[],
                None,
                1,
                SetupDiskImpact::VariableWithinWorkspaceSoftLimit,
                SetupSafeSkip::NotSkippable,
                SetupStepChoice::FirstUsefulResult {
                    result,
                    target_minutes: FIRST_USE_TARGET_MINUTES,
                },
            ),
        ];
        Ok(Self {
            format_version: SETUP_PLAN_FORMAT_VERSION,
            revision,
            selection,
            steps,
        })
    }

    pub(super) fn validate(&self) -> Result<(), SetupPlanError> {
        let rebuilt = Self::try_build(self.revision, self.selection.clone())?;
        if self != &rebuilt {
            return Err(SetupPlanError::CorruptState);
        }
        Ok(())
    }

    /// Returns the plan format revision.
    #[must_use]
    pub const fn format_version(&self) -> u16 {
        self.format_version
    }

    /// Returns the monotonic revision this plan becomes if accepted.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the exact validated goal and starter selection.
    #[must_use]
    pub const fn selection(&self) -> &SetupPlanSelection {
        &self.selection
    }

    /// Returns every plan step in the approved durable setup order.
    #[must_use]
    pub const fn steps(&self) -> &[SetupPlanStep; 11] {
        &self.steps
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "every disclosed step dimension is independently authority-relevant"
)]
fn step(
    id: SetupStepId,
    outcome: SetupOutcome,
    disposition: SetupStepDisposition,
    required_input: SetupRequiredInput,
    external_contacts: &[SetupExternalContact],
    reversible_local_change: Option<SetupReversibleLocalChange>,
    expected_active_minutes: u16,
    disk_impact: SetupDiskImpact,
    safe_skip: SetupSafeSkip,
    choice: SetupStepChoice,
) -> SetupPlanStep {
    SetupPlanStep {
        id,
        outcome,
        disposition,
        required_input,
        external_contacts: external_contacts.into(),
        reversible_local_change,
        expected_active_minutes,
        disk_impact,
        safe_skip,
        choice,
    }
}

fn disposition(starter: SetupStarterPlan, step: SetupStepId) -> SetupStepDisposition {
    let included = match step {
        SetupStepId::FileAndPortfolioImport => matches!(
            starter,
            SetupStarterPlan::EverythingRecommended
                | SetupStarterPlan::Research
                | SetupStarterPlan::Portfolio
                | SetupStarterPlan::Models
        ),
        SetupStepId::ModelRuntime => !matches!(starter, SetupStarterPlan::PublicMarkets),
        SetupStepId::PaperAndRisk => matches!(
            starter,
            SetupStarterPlan::EverythingRecommended
                | SetupStarterPlan::Portfolio
                | SetupStarterPlan::PaperPractice
        ),
        _ => true,
    };
    if included {
        SetupStepDisposition::Included
    } else {
        SetupStepDisposition::AvailableToFinishLater
    }
}

fn disposition_for_client(
    selection: &SetupPlanSelection,
    client_goal: SetupGoal,
) -> SetupStepDisposition {
    if selection.starter_plan == SetupStarterPlan::EverythingRecommended
        || selection.goals.contains(&client_goal)
    {
        SetupStepDisposition::Included
    } else {
        SetupStepDisposition::AvailableToFinishLater
    }
}

fn first_result(starter: SetupStarterPlan) -> SetupFirstResult {
    match starter {
        SetupStarterPlan::EverythingRecommended | SetupStarterPlan::PublicMarkets => {
            SetupFirstResult::VerifiedPublicMarketSnapshot
        }
        SetupStarterPlan::Research => SetupFirstResult::PointInTimeResearchResult,
        SetupStarterPlan::Portfolio => SetupFirstResult::ReconciledPortfolioSummary,
        SetupStarterPlan::Models => SetupFirstResult::AdmittedModelForecast,
        SetupStarterPlan::PaperPractice => SetupFirstResult::StoppedPaperAndRiskReview,
        SetupStarterPlan::AiClients => SetupFirstResult::VerifiedMcpSafeRead,
    }
}

/// Capability promised by an included setup step, subject to later owner-derived readiness facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupCapability {
    /// Managed workspace placement and plan authority.
    ManagedWorkspace,
    /// Typed retention and disk-budget authority.
    RetentionAndDiskBudget,
    /// Public market data capability.
    PublicMarketData,
    /// Filing research capability.
    FilingResearch,
    /// Macro research capability.
    MacroResearch,
    /// Controlled owned-file import capability.
    ControlledFileImport,
    /// Portfolio import and reconciliation capability.
    PortfolioImport,
    /// Managed Python research runtime.
    ManagedPythonRuntime,
    /// Native model inference runtime.
    NativeModelInference,
    /// ONNX model inference runtime.
    OnnxModelInference,
    /// Paper-only execution capability, initially stopped.
    PaperOnlyExecution,
    /// Central risk authority.
    CentralRisk,
    /// Separately registered Claude Code MCP capability.
    ClaudeCodeMcp,
    /// Separately registered Codex MCP capability.
    CodexMcp,
    /// Verified backup capability.
    VerifiedBackup,
    /// Capability and gap review.
    CapabilityReview,
    /// Owner-derived first useful result.
    FirstUsefulResult,
}

pub(super) fn included_capabilities(
    plan: &SetupPlan,
) -> Result<Vec<SetupCapability>, SetupPlanError> {
    let mut capabilities = Vec::new();
    capabilities
        .try_reserve_exact(17)
        .map_err(|_| SetupPlanError::CapacityExceeded)?;
    for step in &plan.steps {
        if step.disposition != SetupStepDisposition::Included {
            continue;
        }
        let additions: &[SetupCapability] = match step.id {
            SetupStepId::GoalsAndStarterPlan => &[SetupCapability::ManagedWorkspace],
            SetupStepId::StorageRetentionTimeAndDisk => &[SetupCapability::RetentionAndDiskBudget],
            SetupStepId::PublicAndZeroFeeProviders => &[
                SetupCapability::PublicMarketData,
                SetupCapability::FilingResearch,
                SetupCapability::MacroResearch,
            ],
            SetupStepId::FileAndPortfolioImport => &[
                SetupCapability::ControlledFileImport,
                SetupCapability::PortfolioImport,
            ],
            SetupStepId::ModelRuntime => &[
                SetupCapability::ManagedPythonRuntime,
                SetupCapability::NativeModelInference,
                SetupCapability::OnnxModelInference,
            ],
            SetupStepId::PaperAndRisk => &[
                SetupCapability::PaperOnlyExecution,
                SetupCapability::CentralRisk,
            ],
            SetupStepId::ClaudeCode => &[SetupCapability::ClaudeCodeMcp],
            SetupStepId::Codex => &[SetupCapability::CodexMcp],
            SetupStepId::Backup => &[SetupCapability::VerifiedBackup],
            SetupStepId::Review => &[SetupCapability::CapabilityReview],
            SetupStepId::FirstUsefulResult => &[SetupCapability::FirstUsefulResult],
        };
        capabilities.extend_from_slice(additions);
    }
    Ok(capabilities)
}

pub(super) fn external_contacts(
    plan: &SetupPlan,
) -> Result<Vec<SetupExternalContact>, SetupPlanError> {
    let mut contacts = Vec::new();
    contacts
        .try_reserve_exact(8)
        .map_err(|_| SetupPlanError::CapacityExceeded)?;
    for step in &plan.steps {
        if step.disposition == SetupStepDisposition::Included {
            for contact in &step.external_contacts {
                if !contacts.contains(contact) {
                    contacts.push(*contact);
                }
            }
        }
    }
    Ok(contacts)
}

pub(super) fn reversible_changes(
    plan: &SetupPlan,
) -> Result<Vec<SetupReversibleLocalChange>, SetupPlanError> {
    let mut changes = Vec::new();
    changes
        .try_reserve_exact(9)
        .map_err(|_| SetupPlanError::CapacityExceeded)?;
    for step in &plan.steps {
        if step.disposition == SetupStepDisposition::Included
            && let Some(change) = step.reversible_local_change
        {
            changes.push(change);
        }
    }
    Ok(changes)
}

pub(super) fn safe_skip_steps(plan: &SetupPlan) -> Result<Vec<SetupStepId>, SetupPlanError> {
    let mut steps = Vec::new();
    steps
        .try_reserve_exact(7)
        .map_err(|_| SetupPlanError::CapacityExceeded)?;
    for step in &plan.steps {
        if step.safe_skip == SetupSafeSkip::CapabilityRemainsInstalledAndAvailable {
            steps.push(step.id);
        }
    }
    Ok(steps)
}

/// Checked aggregate of expected active setup time and the first-value target.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupTimeEstimate {
    expected_active_minutes: u16,
    first_use_target_minutes: u16,
    includes_external_wait: bool,
}

impl SetupTimeEstimate {
    /// Returns the sum of active minutes for included steps, excluding provider-controlled waits.
    #[must_use]
    pub const fn expected_active_minutes(self) -> u16 {
        self.expected_active_minutes
    }

    /// Returns the supported normal-condition first-use target.
    #[must_use]
    pub const fn first_use_target_minutes(self) -> u16 {
        self.first_use_target_minutes
    }

    /// Returns whether included steps may contain provider-controlled external waiting time.
    #[must_use]
    pub const fn includes_external_wait(self) -> bool {
        self.includes_external_wait
    }
}

pub(super) fn time_estimate(plan: &SetupPlan) -> Result<SetupTimeEstimate, SetupPlanError> {
    let mut expected_active_minutes = 0_u16;
    let mut includes_external_wait = false;
    for step in &plan.steps {
        if step.disposition == SetupStepDisposition::Included {
            expected_active_minutes = expected_active_minutes
                .checked_add(step.expected_active_minutes)
                .ok_or(SetupPlanError::CapacityExceeded)?;
            includes_external_wait |= !step.external_contacts.is_empty();
        }
    }
    Ok(SetupTimeEstimate {
        expected_active_minutes,
        first_use_target_minutes: FIRST_USE_TARGET_MINUTES,
        includes_external_wait,
    })
}

/// Honest setup disk forecast: installed product bytes are already present and variable content is
/// constrained by the workspace soft limit or a separately selected backup destination.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupDiskEstimate {
    workspace_soft_limit_bytes: u64,
    included_impacts: Box<[SetupDiskImpact]>,
}

impl SetupDiskEstimate {
    /// Returns the selected workspace soft limit applied to variable local content.
    #[must_use]
    pub const fn workspace_soft_limit_bytes(&self) -> u64 {
        self.workspace_soft_limit_bytes
    }

    /// Returns the distinct honest impact classes represented by included steps.
    #[must_use]
    pub fn included_impacts(&self) -> &[SetupDiskImpact] {
        &self.included_impacts
    }
}

pub(super) fn disk_estimate(plan: &SetupPlan) -> Result<SetupDiskEstimate, SetupPlanError> {
    let mut impacts = Vec::new();
    impacts
        .try_reserve_exact(3)
        .map_err(|_| SetupPlanError::CapacityExceeded)?;
    for step in &plan.steps {
        if step.disposition == SetupStepDisposition::Included
            && !impacts.contains(&step.disk_impact)
        {
            impacts.push(step.disk_impact);
        }
    }
    Ok(SetupDiskEstimate {
        workspace_soft_limit_bytes: WORKSPACE_SOFT_LIMIT_BYTES,
        included_impacts: impacts.into_boxed_slice(),
    })
}

pub(super) fn plan_digest(
    workspace: WorkspaceId,
    plan: &SetupPlan,
) -> Result<[u8; 32], SetupPlanError> {
    use sha2::{Digest as _, Sha256};

    serde_json::to_vec(&("market-squawk-setup-plan-v1", workspace, plan))
        .map(|encoded| Sha256::digest(encoded).into())
        .map_err(|_| SetupPlanError::Encoding)
}
