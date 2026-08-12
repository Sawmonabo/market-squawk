//! Narrow desktop controls over the shared application service.

use serde_json::{Map, Value, json};
use tauri::State;

use crate::{
    bridge::{DesktopState, InvocationAuthority, invoke_application, invoke_private_application},
    contracts::{
        AnalysisControlCommand, ApplicationInvocation, DashboardQueryCommand,
        DecisionControlCommand, DesktopCommandError, FairValueControlCommand,
        GovernanceControlCommand, GovernanceQueryCommand, JobControlCommand, ModelControlCommand,
        OperationLogDomain, OperationLogSeverity, OperationSettingValue, OperationsControlCommand,
        PaperControlCommand, ResearchControlCommand, SourceLifecycleAction, SourceLifecycleInput,
    },
};

// Canonical string conversion happens after the shared bridge's size check, so retain its cap.
const MAXIMUM_CANONICAL_JOB_RESULT_BYTES: usize = 1024 * 1024;
const FEDERAL_RESERVE_BOARD_DDP_PROVIDER: &str = "federal-reserve-board.data-download-program";
const H15_RELEASE: &str = "h15";

#[tauri::command]
pub(crate) async fn dashboard_query(
    request: DashboardQueryCommand,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let (operation, arguments) = match request {
        DashboardQueryCommand::Overview => ("Analysis.GetDecisionOverview", Map::new()),
        DashboardQueryCommand::MacroDashboard { provider, release } => {
            if provider != FEDERAL_RESERVE_BOARD_DDP_PROVIDER || release != H15_RELEASE {
                return Err(DesktopCommandError::invalid_request(
                    "The selected macro dashboard is unsupported.",
                ));
            }
            let mut arguments = Map::new();
            arguments.insert("provider".to_owned(), json!(provider));
            arguments.insert("release".to_owned(), json!(release));
            ("Macro.GetDashboard", arguments)
        }
        DashboardQueryCommand::Lookup { text, categories } => {
            let mut arguments = Map::new();
            arguments.insert("query".to_owned(), json!(text));
            insert_optional(&mut arguments, "categories", categories);
            ("Analysis.Lookup", arguments)
        }
        DashboardQueryCommand::MarketSnapshot => ("Market.GetSnapshot", Map::new()),
        DashboardQueryCommand::MarketQuality => ("Market.GetQuality", Map::new()),
        DashboardQueryCommand::MarketUnifiedFeed => ("Market.GetUnifiedFeed", Map::new()),
        DashboardQueryCommand::MarketUniverse { text } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "query", text);
            ("Market.SearchUniverse", arguments)
        }
        DashboardQueryCommand::MarketTrades { instrument_id } => {
            ("Market.GetTrades", instrument_arguments(instrument_id))
        }
        DashboardQueryCommand::MarketQuotes { instrument_id } => {
            ("Market.GetQuotes", instrument_arguments(instrument_id))
        }
        DashboardQueryCommand::MarketBooks { instrument_id } => {
            ("Market.GetBooks", instrument_arguments(instrument_id))
        }
        DashboardQueryCommand::MarketComparisons { instrument_id } => {
            ("Market.GetComparisons", instrument_arguments(instrument_id))
        }
        DashboardQueryCommand::SourceStatus { source_ids } => {
            ("Source.GetStatus", source_arguments(source_ids))
        }
        DashboardQueryCommand::SourceCoverage { source_ids } => {
            ("Source.GetCoverage", source_arguments(source_ids))
        }
        DashboardQueryCommand::SourceHealth { source_ids } => {
            ("Source.GetHealth", source_arguments(source_ids))
        }
        DashboardQueryCommand::ResearchDatasets { after_dataset } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "afterDataset", after_dataset);
            ("Research.ListDatasets", arguments)
        }
        DashboardQueryCommand::ResearchManifest { dataset } => {
            ("Research.GetManifest", dataset_arguments(dataset))
        }
        DashboardQueryCommand::ResearchHistory { dataset } => {
            ("Research.GetHistory", dataset_arguments(dataset))
        }
        DashboardQueryCommand::ResearchAlternativeData { dataset } => {
            ("Research.GetAlternativeData", dataset_arguments(dataset))
        }
        DashboardQueryCommand::ResearchSourceObjects { provider, dataset } => (
            "Source.ListObjects",
            source_discovery_arguments(provider, dataset),
        ),
        DashboardQueryCommand::PortfolioAccounts { after_account_id } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "afterAccountId", after_account_id);
            ("Portfolio.ListAccounts", arguments)
        }
        DashboardQueryCommand::PortfolioHoldings { account_id } => {
            ("Portfolio.GetHoldings", account_arguments(account_id))
        }
        DashboardQueryCommand::PortfolioTransactions { account_id } => {
            ("Portfolio.GetTransactions", account_arguments(account_id))
        }
        DashboardQueryCommand::PortfolioPerformance { account_id } => {
            ("Portfolio.GetPerformance", account_arguments(account_id))
        }
        DashboardQueryCommand::PortfolioExposure { account_id } => {
            ("Portfolio.GetExposure", account_arguments(account_id))
        }
        DashboardQueryCommand::PortfolioRisk { account_id } => {
            ("Portfolio.GetRisk", account_arguments(account_id))
        }
        DashboardQueryCommand::PortfolioRevisions {
            account_id,
            after_revision_id,
        } => {
            let mut arguments = account_arguments(account_id);
            insert_optional(&mut arguments, "afterRevisionId", after_revision_id);
            ("Portfolio.ListRevisions", arguments)
        }
        DashboardQueryCommand::PortfolioAttribution {
            account_id,
            baseline_revision_id,
        } => {
            let mut arguments = account_arguments(account_id);
            arguments.insert("baselineRevisionId".to_owned(), json!(baseline_revision_id));
            ("Portfolio.GetAttribution", arguments)
        }
        DashboardQueryCommand::PortfolioScenario {
            account_id,
            scenario,
        } => {
            let mut arguments = account_arguments(account_id);
            arguments.insert("scenario".to_owned(), Value::Object(scenario));
            ("Portfolio.EvaluateScenario", arguments)
        }
        DashboardQueryCommand::PortfolioScenarioBatch {
            account_id,
            scenarios,
        } => {
            let mut arguments = account_arguments(account_id);
            arguments.insert("scenarios".to_owned(), Value::Array(scenarios));
            ("Portfolio.EvaluateScenarioBatch", arguments)
        }
        DashboardQueryCommand::PortfolioRebalance {
            account_id,
            proposal,
        } => {
            let mut arguments = account_arguments(account_id);
            arguments.insert("proposal".to_owned(), Value::Object(proposal));
            ("Portfolio.ProposeRebalance", arguments)
        }
        DashboardQueryCommand::PortfolioCandidateImpact {
            account_id,
            candidate,
        } => {
            let mut arguments = account_arguments(account_id);
            arguments.insert("candidate".to_owned(), Value::Object(candidate));
            ("Portfolio.EvaluateCandidateImpact", arguments)
        }
        DashboardQueryCommand::ModelBundles => ("Model.ListBundles", Map::new()),
        DashboardQueryCommand::Forecasts => ("Model.ListForecasts", Map::new()),
        DashboardQueryCommand::LatestValidForecast {
            instrument_id,
            as_of,
        } => {
            let mut arguments = Map::new();
            arguments.insert("instrumentId".to_owned(), json!(instrument_id));
            arguments.insert("asOf".to_owned(), json!(as_of));
            ("Model.SelectLatestValidForecast", arguments)
        }
        DashboardQueryCommand::ModelMetadata { model_id } => {
            ("Model.GetMetadata", model_arguments(model_id))
        }
        DashboardQueryCommand::ModelPrediction { model_id, input } => {
            let mut arguments = model_arguments(model_id);
            arguments.insert("input".to_owned(), Value::Object(input));
            ("Model.Predict", arguments)
        }
        DashboardQueryCommand::Forecast { vintage_id } => {
            ("Model.GetForecast", vintage_arguments(vintage_id))
        }
        DashboardQueryCommand::ForecastOutcomes { vintage_id } => {
            ("Model.GetForecastOutcomes", vintage_arguments(vintage_id))
        }
        DashboardQueryCommand::DecisionScreens { limit } => {
            let mut arguments = Map::new();
            arguments.insert("limit".to_owned(), json!(limit));
            ("Decision.ListScreens", arguments)
        }
        DashboardQueryCommand::AnalysisFeatureDatasets {
            dataset,
            after_dataset,
        } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "dataset", dataset);
            insert_optional(&mut arguments, "afterDataset", after_dataset);
            ("Analysis.GetFeatureDatasets", arguments)
        }
        DashboardQueryCommand::DecisionScreenRuns {
            after_run_id,
            limit,
        } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "afterRunId", after_run_id);
            arguments.insert("limit".to_owned(), json!(limit));
            ("Decision.ListScreenRuns", arguments)
        }
        DashboardQueryCommand::DecisionCandidates { run_id } => {
            let mut arguments = Map::new();
            arguments.insert("runId".to_owned(), json!(run_id));
            ("Decision.GetCandidates", arguments)
        }
        DashboardQueryCommand::DecisionDossier { dossier_id } => {
            let mut arguments = Map::new();
            arguments.insert("dossierId".to_owned(), json!(dossier_id));
            ("Decision.GetDossier", arguments)
        }
        DashboardQueryCommand::DecisionDossierPreparation { candidate_id } => {
            let mut arguments = Map::new();
            arguments.insert("candidateId".to_owned(), json!(candidate_id));
            ("Decision.GetDossierPreparation", arguments)
        }
        DashboardQueryCommand::DecisionInvestmentAnalysis { analysis_id } => {
            let mut arguments = Map::new();
            arguments.insert("analysisId".to_owned(), json!(analysis_id));
            ("Decision.GetInvestmentAnalysis", arguments)
        }
        DashboardQueryCommand::DecisionInvestmentAnalyses {
            after_analysis_id,
            limit,
        } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "afterAnalysisId", after_analysis_id);
            arguments.insert("limit".to_owned(), json!(limit));
            ("Decision.ListInvestmentAnalyses", arguments)
        }
        DashboardQueryCommand::DecisionTargetPreparation { dossier_id } => {
            let mut arguments = Map::new();
            arguments.insert("dossierId".to_owned(), json!(dossier_id));
            ("Decision.GetTargetPreparation", arguments)
        }
        DashboardQueryCommand::DecisionCandidateDossiers {
            candidate_id,
            after_dossier_id,
            limit,
        } => {
            let mut arguments = Map::new();
            arguments.insert("candidateId".to_owned(), json!(candidate_id));
            insert_optional(&mut arguments, "afterDossierId", after_dossier_id);
            arguments.insert("limit".to_owned(), json!(limit));
            ("Decision.ListCandidateDossiers", arguments)
        }
        DashboardQueryCommand::DecisionTarget {
            target_id,
            revision,
        } => (
            "Decision.GetTargetSet",
            target_arguments(target_id, revision),
        ),
        DashboardQueryCommand::DecisionTargets { target_id } => {
            let mut arguments = Map::new();
            arguments.insert("targetId".to_owned(), json!(target_id));
            ("Decision.ListTargetSets", arguments)
        }
        DashboardQueryCommand::DecisionTargetIndex {
            after_target_id,
            limit,
        } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "afterTargetId", after_target_id);
            arguments.insert("limit".to_owned(), json!(limit));
            ("Decision.ListTargetIndex", arguments)
        }
        DashboardQueryCommand::DecisionTargetStatus {
            target_id,
            revision,
        } => (
            "Decision.GetTargetSetStatus",
            target_arguments(target_id, revision),
        ),
        DashboardQueryCommand::Backtest { run_id } => {
            let mut arguments = Map::new();
            arguments.insert("runId".to_owned(), json!(run_id));
            ("Analysis.GetBacktests", arguments)
        }
        DashboardQueryCommand::AnalysisArtifact {
            artifact_id,
            sha256,
            byte_count,
            media_type,
            offset,
            maximum_bytes,
        } => {
            let mut arguments = Map::new();
            arguments.insert("artifactId".to_owned(), json!(artifact_id));
            arguments.insert("sha256".to_owned(), json!(sha256));
            arguments.insert("byteCount".to_owned(), json!(byte_count));
            arguments.insert("mediaType".to_owned(), json!(media_type));
            arguments.insert("offset".to_owned(), json!(offset));
            arguments.insert("maximumBytes".to_owned(), json!(maximum_bytes));
            ("Analysis.ReadArtifact", arguments)
        }
        DashboardQueryCommand::PaperStatus => ("Bot.GetStatus", Map::new()),
        DashboardQueryCommand::PaperOrders => ("Execution.GetOrders", Map::new()),
        DashboardQueryCommand::PaperFills => ("Execution.GetFills", Map::new()),
        DashboardQueryCommand::FairValueMeasurements => ("FairValue.ListMeasurements", Map::new()),
        DashboardQueryCommand::FairValueClassification { measurement_id } => (
            "FairValue.GetClassification",
            measurement_arguments(measurement_id),
        ),
        DashboardQueryCommand::FairValueExplanation { measurement_id } => {
            ("FairValue.Explain", measurement_arguments(measurement_id))
        }
        DashboardQueryCommand::FairValueEvidence { measurement_id } => (
            "FairValue.GetEvidence",
            measurement_arguments(measurement_id),
        ),
        DashboardQueryCommand::FairValueApprovalStatus { measurement_id, at } => {
            let mut arguments = measurement_arguments(measurement_id);
            arguments.insert("at".to_owned(), json!(at));
            ("FairValue.GetApprovalStatus", arguments)
        }
        DashboardQueryCommand::FairValueAudit { after, limit } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "after", after);
            arguments.insert("limit".to_owned(), json!(limit));
            ("FairValue.ListAuditEvents", arguments)
        }
        DashboardQueryCommand::FairValueMarketAccess { assessment_id } => {
            let mut arguments = Map::new();
            arguments.insert("assessmentId".to_owned(), json!(assessment_id));
            ("FairValue.GetMarketAccess", arguments)
        }
        DashboardQueryCommand::Jobs {
            after_job_id,
            limit,
        } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "afterJobId", after_job_id);
            arguments.insert("limit".to_owned(), json!(limit));
            ("Job.List", arguments)
        }
        DashboardQueryCommand::OperationRuntimeStatus => {
            ("Operations.GetRuntimeStatus", Map::new())
        }
        DashboardQueryCommand::OperationBackups {
            after_backup_id,
            limit,
        } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "afterBackupId", after_backup_id);
            arguments.insert("limit".to_owned(), json!(limit));
            ("Operations.ListBackups", arguments)
        }
        DashboardQueryCommand::OperationBackup { backup_id } => {
            ("Operations.GetBackup", backup_arguments(backup_id))
        }
        DashboardQueryCommand::OperationBackupRetentionPreview { keep_latest } => {
            let mut arguments = Map::new();
            arguments.insert("keepLatest".to_owned(), json!(keep_latest));
            ("Operations.PreviewBackupRetention", arguments)
        }
        DashboardQueryCommand::OperationRestorePreview { backup_id } => {
            ("Operations.PreviewRestore", backup_arguments(backup_id))
        }
        DashboardQueryCommand::OperationWorkspaces {
            after_workspace_id,
            limit,
        } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "afterWorkspaceId", after_workspace_id);
            arguments.insert("limit".to_owned(), json!(limit));
            ("Operations.ListWorkspaces", arguments)
        }
        DashboardQueryCommand::OperationWorkspaceSwitchPreview { workspace_id } => {
            let mut arguments = Map::new();
            arguments.insert("workspaceId".to_owned(), json!(workspace_id));
            ("Operations.PreviewWorkspaceSwitch", arguments)
        }
        DashboardQueryCommand::OperationUpdateStatus => ("Operations.GetUpdateStatus", Map::new()),
        DashboardQueryCommand::OperationUpdatePreview => ("Operations.PreviewUpdate", Map::new()),
        DashboardQueryCommand::OperationProgramRollbackPreview => {
            ("Operations.PreviewProgramRollback", Map::new())
        }
        DashboardQueryCommand::OperationLogs {
            from_unix_nanos,
            through_unix_nanos,
            minimum_severity,
            domain,
            source_id,
            job_id,
            correlation_id,
            search,
            after_sequence,
            limit,
        } => (
            "Operations.QueryLogs",
            operation_log_arguments(OperationLogArguments {
                from_unix_nanos,
                through_unix_nanos,
                minimum_severity,
                domain,
                source_id,
                job_id,
                correlation_id,
                search,
                after_sequence,
                limit,
            })?,
        ),
        DashboardQueryCommand::OperationSettings => ("Operations.GetSettings", Map::new()),
        DashboardQueryCommand::OperationSettingsChangePreview {
            expected_revision,
            changes,
        } => {
            let mut arguments = Map::new();
            arguments.insert(
                "expectedRevision".to_owned(),
                json!(parse_unsigned_decimal(
                    expected_revision,
                    "The settings revision must be an unsigned decimal.",
                )?),
            );
            arguments.insert("changes".to_owned(), operation_setting_values(changes)?);
            ("Operations.PreviewSettingsChange", arguments)
        }
        DashboardQueryCommand::OperationSettingsRollbackPreview {
            expected_revision,
            target_revision,
        } => {
            let mut arguments = Map::new();
            arguments.insert(
                "expectedRevision".to_owned(),
                json!(parse_unsigned_decimal(
                    expected_revision,
                    "The settings revision must be an unsigned decimal.",
                )?),
            );
            arguments.insert(
                "targetRevision".to_owned(),
                json!(parse_unsigned_decimal(
                    target_revision,
                    "The settings rollback revision must be an unsigned decimal.",
                )?),
            );
            ("Operations.PreviewSettingsRollback", arguments)
        }
        DashboardQueryCommand::SetupPlanStatus => ("Setup.GetStatus", Map::new()),
        DashboardQueryCommand::SetupPlanPreview {
            expected_revision,
            selection,
        } => {
            let mut arguments = Map::new();
            arguments.insert(
                "expectedRevision".to_owned(),
                json!(parse_unsigned_decimal(
                    expected_revision,
                    "The setup-plan revision must be an unsigned decimal.",
                )?),
            );
            arguments.insert("selection".to_owned(), json!(selection));
            ("Setup.PreviewPlan", arguments)
        }
    };
    let mut result = invoke_application(
        ApplicationInvocation {
            operation: operation.to_owned(),
            arguments,
        },
        &state,
        InvocationAuthority::ReadOnly,
    )
    .await?;
    if operation == "Job.List" {
        canonicalize_job_result(operation, &mut result)?;
    }
    Ok(result)
}

#[tauri::command]
pub(crate) async fn operations_control(
    request: OperationsControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    require_confirmation(confirmed)?;
    let (operation, arguments) = match request {
        OperationsControlCommand::CheckForUpdates => ("Operations.CheckForUpdates", Map::new()),
        OperationsControlCommand::ExportLogs {
            from_unix_nanos,
            through_unix_nanos,
            minimum_severity,
            domain,
            source_id,
            job_id,
            correlation_id,
            search,
            after_sequence,
            limit,
        } => (
            "Operations.ExportLogs",
            operation_log_arguments(OperationLogArguments {
                from_unix_nanos,
                through_unix_nanos,
                minimum_severity,
                domain,
                source_id,
                job_id,
                correlation_id,
                search,
                after_sequence,
                limit,
            })?,
        ),
        OperationsControlCommand::StartBackup => ("Operations.StartBackup", Map::new()),
        OperationsControlCommand::StartBackupVerification { backup_id } => (
            "Operations.StartBackupVerification",
            backup_arguments(backup_id),
        ),
        OperationsControlCommand::StartBackupRetention {
            preview_id,
            preview_digest,
        } => (
            "Operations.StartBackupRetention",
            preview_arguments(preview_id, preview_digest),
        ),
        OperationsControlCommand::StartRestore {
            preview_id,
            preview_digest,
        } => (
            "Operations.StartRestore",
            preview_arguments(preview_id, preview_digest),
        ),
        OperationsControlCommand::StartWorkspaceSwitch {
            preview_id,
            preview_digest,
        } => (
            "Operations.StartWorkspaceSwitch",
            preview_arguments(preview_id, preview_digest),
        ),
        OperationsControlCommand::StartUpdate {
            preview_id,
            preview_digest,
        } => (
            "Operations.StartUpdate",
            preview_arguments(preview_id, preview_digest),
        ),
        OperationsControlCommand::StartProgramRollback {
            preview_id,
            preview_digest,
        } => (
            "Operations.StartProgramRollback",
            preview_arguments(preview_id, preview_digest),
        ),
        OperationsControlCommand::ApplySettingsChange {
            preview_id,
            preview_digest,
        } => (
            "Operations.ApplySettingsChange",
            preview_arguments(preview_id, preview_digest),
        ),
        OperationsControlCommand::RollbackSettings {
            preview_id,
            preview_digest,
        } => (
            "Operations.RollbackSettings",
            preview_arguments(preview_id, preview_digest),
        ),
        OperationsControlCommand::ApplySetupPlan {
            preview_id,
            preview_sha256,
        } => {
            let mut arguments = Map::new();
            arguments.insert("previewId".to_owned(), json!(preview_id));
            arguments.insert("previewSha256".to_owned(), json!(preview_sha256));
            ("Setup.ApplyPlan", arguments)
        }
    };
    invoke_application(
        ApplicationInvocation {
            operation: operation.to_owned(),
            arguments,
        },
        &state,
        InvocationAuthority::ExactConfirmed(operation),
    )
    .await
}

#[tauri::command]
pub(crate) async fn fair_value_control(
    request: FairValueControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
    window: tauri::Window,
) -> Result<Value, DesktopCommandError> {
    match request {
        FairValueControlCommand::Measure { measurement } => {
            let mut arguments = Map::new();
            arguments.insert("measurement".to_owned(), Value::Object(measurement));
            invoke_narrow("FairValue.Measure", arguments, true, confirmed, &state).await
        }
        FairValueControlCommand::Classify { measurement_id } => {
            invoke_narrow(
                "FairValue.Classify",
                measurement_arguments(measurement_id),
                true,
                confirmed,
                &state,
            )
            .await
        }
        FairValueControlCommand::PreviewGovernanceAction { proposal } => {
            require_confirmation(confirmed)?;
            let mut arguments = Map::new();
            arguments.insert("proposal".to_owned(), json!(proposal));
            invoke_private_application(
                "FairValue.PreviewGovernanceAction",
                arguments,
                &state,
                InvocationAuthority::ExactConfirmed("FairValue.PreviewGovernanceAction"),
            )
            .await
        }
        FairValueControlCommand::CommitGovernanceAction {
            preview_id,
            authorization_handles,
        } => {
            require_confirmation(confirmed)?;
            let ticket_ids = state.consume_governance_authorizations(
                window.label(),
                preview_id,
                authorization_handles,
            )?;
            let mut arguments = Map::new();
            arguments.insert("previewId".to_owned(), json!(preview_id));
            arguments.insert("ticketIds".to_owned(), json!(ticket_ids));
            invoke_private_application(
                "FairValue.CommitGovernanceAction",
                arguments,
                &state,
                InvocationAuthority::ExactConfirmed("FairValue.CommitGovernanceAction"),
            )
            .await
        }
    }
}

#[tauri::command]
pub(crate) async fn decision_control(
    request: DecisionControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
    window: tauri::Window,
) -> Result<Value, DesktopCommandError> {
    match request {
        DecisionControlCommand::SaveScreen {
            expected_revision,
            screen,
        } => {
            require_confirmation(confirmed)?;
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "expectedRevision", expected_revision);
            arguments.insert("screen".to_owned(), Value::Object(screen));
            invoke_private_application(
                "Decision.SaveScreen",
                arguments,
                &state,
                InvocationAuthority::ExactConfirmed("Decision.SaveScreen"),
            )
            .await
        }
        DecisionControlCommand::RunScreen {
            screen_id,
            screen_revision,
            dataset_manifest,
            as_of,
        } => {
            require_confirmation(confirmed)?;
            let mut arguments = Map::new();
            arguments.insert("screenId".to_owned(), json!(screen_id));
            arguments.insert("screenRevision".to_owned(), json!(screen_revision));
            arguments.insert(
                "datasetManifest".to_owned(),
                Value::Object(dataset_manifest),
            );
            arguments.insert("asOf".to_owned(), json!(as_of));
            invoke_private_application(
                "Decision.RunScreen",
                arguments,
                &state,
                InvocationAuthority::ExactConfirmed("Decision.RunScreen"),
            )
            .await
        }
        DecisionControlCommand::PrepareDossier { draft } => {
            let mut arguments = Map::new();
            arguments.insert("draft".to_owned(), Value::Object(draft));
            invoke_private_application(
                "Decision.PrepareDossier",
                arguments,
                &state,
                InvocationAuthority::ReadOnly,
            )
            .await
        }
        DecisionControlCommand::CreateDossier { receipt_id } => {
            require_confirmation(confirmed)?;
            let mut arguments = Map::new();
            arguments.insert("receiptId".to_owned(), json!(receipt_id));
            invoke_private_application(
                "Decision.CreateDossier",
                arguments,
                &state,
                InvocationAuthority::ExactConfirmed("Decision.CreateDossier"),
            )
            .await
        }
        DecisionControlCommand::PrepareTargetSet { draft } => {
            let mut arguments = Map::new();
            arguments.insert("draft".to_owned(), Value::Object(draft));
            invoke_private_application(
                "Decision.PrepareTargetSet",
                arguments,
                &state,
                InvocationAuthority::ReadOnly,
            )
            .await
        }
        DecisionControlCommand::CreateTargetSet { receipt_id } => {
            require_confirmation(confirmed)?;
            let mut arguments = Map::new();
            arguments.insert("receiptId".to_owned(), json!(receipt_id));
            invoke_private_application(
                "Decision.CreateTargetSet",
                arguments,
                &state,
                InvocationAuthority::ExactConfirmed("Decision.CreateTargetSet"),
            )
            .await
        }
        DecisionControlCommand::ReevaluateTargetSet { receipt_id } => {
            require_confirmation(confirmed)?;
            let mut arguments = Map::new();
            arguments.insert("receiptId".to_owned(), json!(receipt_id));
            invoke_private_application(
                "Decision.ReevaluateTargetSet",
                arguments,
                &state,
                InvocationAuthority::ExactConfirmed("Decision.ReevaluateTargetSet"),
            )
            .await
        }
        DecisionControlCommand::PreviewGovernanceAction { proposal } => {
            require_confirmation(confirmed)?;
            let mut arguments = Map::new();
            arguments.insert("proposal".to_owned(), json!(proposal));
            invoke_private_application(
                "Decision.PreviewGovernanceAction",
                arguments,
                &state,
                InvocationAuthority::ExactConfirmed("Decision.PreviewGovernanceAction"),
            )
            .await
        }
        DecisionControlCommand::CommitGovernanceAction {
            preview_id,
            authorization_handles,
        } => {
            require_confirmation(confirmed)?;
            let ticket_ids = state.consume_governance_authorizations(
                window.label(),
                preview_id,
                authorization_handles,
            )?;
            let mut arguments = Map::new();
            arguments.insert("previewId".to_owned(), json!(preview_id));
            arguments.insert("ticketIds".to_owned(), json!(ticket_ids));
            invoke_private_application(
                "Decision.CommitGovernanceAction",
                arguments,
                &state,
                InvocationAuthority::ExactConfirmed("Decision.CommitGovernanceAction"),
            )
            .await
        }
    }
}

#[tauri::command]
pub(crate) async fn governance_query(
    request: GovernanceQueryCommand,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    match request {
        GovernanceQueryCommand::ProvisioningStatus => {
            invoke_private_application(
                "Governance.ProvisioningStatus",
                Map::new(),
                &state,
                InvocationAuthority::ReadOnly,
            )
            .await
        }
        GovernanceQueryCommand::Principals { after, limit } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "after", after);
            insert_optional(&mut arguments, "limit", limit);
            invoke_private_application(
                "Governance.ListPrincipals",
                arguments,
                &state,
                InvocationAuthority::ReadOnly,
            )
            .await
        }
    }
}

#[tauri::command]
pub(crate) async fn governance_control(
    request: GovernanceControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
    window: tauri::Window,
) -> Result<Value, DesktopCommandError> {
    require_confirmation(confirmed)?;
    match request {
        GovernanceControlCommand::ProvisionPrincipalSet {
            primary_display_name,
            primary_credential,
            reviewer_display_name,
            reviewer_credential,
        } => {
            let mut arguments = Map::new();
            arguments.insert(
                "primaryDisplayName".to_owned(),
                Value::String(primary_display_name),
            );
            arguments.insert(
                "primaryCredential".to_owned(),
                Value::String(primary_credential),
            );
            arguments.insert(
                "reviewerDisplayName".to_owned(),
                Value::String(reviewer_display_name),
            );
            arguments.insert(
                "reviewerCredential".to_owned(),
                Value::String(reviewer_credential),
            );
            invoke_private_application(
                "Governance.ProvisionPrincipalSet",
                arguments,
                &state,
                InvocationAuthority::ExactConfirmed("Governance.ProvisionPrincipalSet"),
            )
            .await
        }
        GovernanceControlCommand::AuthenticateAction {
            preview_id,
            principal_id,
            credential,
        } => {
            let mut arguments = Map::new();
            arguments.insert("previewId".to_owned(), json!(preview_id));
            arguments.insert("principalId".to_owned(), json!(principal_id));
            arguments.insert("credential".to_owned(), Value::String(credential));
            let result = invoke_private_application(
                "Governance.AuthenticateAction",
                arguments,
                &state,
                InvocationAuthority::ExactConfirmed("Governance.AuthenticateAction"),
            )
            .await?;
            state.retain_governance_authorization(window.label(), result)
        }
    }
}

#[tauri::command]
pub(crate) async fn paper_control(
    request: PaperControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let (operation, arguments, authority) = match request {
        PaperControlCommand::Targets => (
            "Execution.GetManualPaperTargets",
            Map::new(),
            InvocationAuthority::ReadOnly,
        ),
        PaperControlCommand::Submit {
            target_id,
            target_revision,
            side,
            order_type,
            quantity_lots,
            limit_target_level,
            stop_target_level,
            time_in_force,
        } => {
            require_confirmation(confirmed)?;
            let mut arguments = Map::new();
            arguments.insert("targetId".to_owned(), json!(target_id));
            arguments.insert("targetRevision".to_owned(), json!(target_revision));
            arguments.insert("side".to_owned(), json!(side));
            arguments.insert("orderType".to_owned(), json!(order_type));
            arguments.insert("quantityLots".to_owned(), json!(quantity_lots));
            insert_optional(&mut arguments, "limitTargetLevel", limit_target_level);
            insert_optional(&mut arguments, "stopTargetLevel", stop_target_level);
            arguments.insert("timeInForce".to_owned(), json!(time_in_force));
            (
                "Execution.SubmitManualPaperDraft",
                arguments,
                InvocationAuthority::RiskMediated("Execution.SubmitManualPaperDraft"),
            )
        }
        PaperControlCommand::Start {
            provider,
            provider_session_id,
            strategy_mode,
            initial_cash,
            fee_basis_points,
        } => {
            require_confirmation(confirmed)?;
            let mut arguments = Map::new();
            arguments.insert("provider".to_owned(), json!(provider));
            insert_optional(&mut arguments, "providerSessionId", provider_session_id);
            arguments.insert("strategyMode".to_owned(), json!(strategy_mode));
            arguments.insert("initialCash".to_owned(), json!(initial_cash));
            arguments.insert("feeBasisPoints".to_owned(), json!(fee_basis_points));
            (
                "Bot.Start",
                arguments,
                InvocationAuthority::ExactConfirmed("Bot.Start"),
            )
        }
        PaperControlCommand::Stop { reason } => {
            require_confirmation(confirmed)?;
            (
                "Bot.Stop",
                reason_arguments(reason),
                InvocationAuthority::ExactConfirmed("Bot.Stop"),
            )
        }
        PaperControlCommand::Cancel { order_id } => {
            require_confirmation(confirmed)?;
            let mut arguments = Map::new();
            arguments.insert("orderId".to_owned(), json!(order_id));
            (
                "Execution.Cancel",
                arguments,
                InvocationAuthority::RiskMediated("Execution.Cancel"),
            )
        }
        PaperControlCommand::Reconcile => {
            require_confirmation(confirmed)?;
            (
                "Execution.Reconcile",
                Map::new(),
                InvocationAuthority::RiskMediated("Execution.Reconcile"),
            )
        }
        PaperControlCommand::TriggerKillSwitch { reason } => {
            require_confirmation(confirmed)?;
            (
                "Risk.TriggerKillSwitch",
                reason_arguments(reason),
                InvocationAuthority::ExactConfirmed("Risk.TriggerKillSwitch"),
            )
        }
    };
    invoke_application(
        ApplicationInvocation {
            operation: operation.to_owned(),
            arguments,
        },
        &state,
        authority,
    )
    .await
}

#[tauri::command]
pub(crate) async fn analysis_control(
    request: AnalysisControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let (operation, arguments, mutation) = match request {
        AnalysisControlCommand::FeatureDatasetOptions => (
            "Analysis.GetFeatureDatasetPreparationOptions",
            Map::new(),
            false,
        ),
        AnalysisControlCommand::PreviewFeatureDataset { selection } => {
            ("Analysis.PreviewFeatureDatasetBuild", selection, false)
        }
        AnalysisControlCommand::StartPreparedFeatureDataset { receipt } => {
            let mut arguments = Map::new();
            arguments.insert("receipt".to_owned(), Value::Object(receipt));
            ("Analysis.StartPreparedFeatureDatasetBuild", arguments, true)
        }
        AnalysisControlCommand::BacktestOptions => {
            ("Analysis.GetBacktestPreparation", Map::new(), false)
        }
        AnalysisControlCommand::PreviewBacktest { selection } => {
            let mut arguments = Map::new();
            arguments.insert("selection".to_owned(), Value::Object(selection));
            ("Analysis.PreviewBacktest", arguments, false)
        }
        AnalysisControlCommand::StartPreparedBacktest { receipt } => {
            let mut arguments = Map::new();
            arguments.insert("receipt".to_owned(), Value::Object(receipt));
            ("Analysis.StartPreparedBacktest", arguments, true)
        }
    };
    invoke_narrow(operation, arguments, mutation, confirmed, &state).await
}

#[tauri::command]
pub(crate) async fn model_control(
    request: ModelControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let (operation, arguments, mutation) = match request {
        ModelControlCommand::Evaluate { model_id, input } => {
            let mut arguments = model_arguments(model_id);
            arguments.insert("input".to_owned(), Value::Object(input));
            ("Model.Evaluate", arguments, true)
        }
        ModelControlCommand::StartTraining {
            config_ticket_id,
            authority_ticket_id,
        } => {
            let mut arguments = Map::new();
            arguments.insert("configTicketId".to_owned(), json!(config_ticket_id));
            arguments.insert("authorityTicketId".to_owned(), json!(authority_ticket_id));
            ("Model.StartTraining", arguments, true)
        }
        ModelControlCommand::ForecastPreparationOptions => {
            ("Model.GetForecastPreparation", Map::new(), false)
        }
        ModelControlCommand::PrepareForecast { selection } => {
            let mut arguments = Map::new();
            arguments.insert("selection".to_owned(), Value::Object(selection));
            ("Model.PrepareForecast", arguments, false)
        }
        ModelControlCommand::StartPreparedForecast { receipt } => {
            let mut arguments = Map::new();
            arguments.insert("receipt".to_owned(), Value::Object(receipt));
            ("Model.StartPreparedForecast", arguments, true)
        }
    };
    invoke_narrow(operation, arguments, mutation, confirmed, &state).await
}

#[tauri::command]
pub(crate) async fn research_control(
    request: ResearchControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let (operation, arguments) = match request {
        ResearchControlCommand::DiscoverSourceObjects { provider, dataset } => (
            "Source.Discover",
            source_discovery_arguments(provider, dataset),
        ),
        ResearchControlCommand::StartIngestSource {
            provider,
            object,
            dataset,
            discovery_receipt,
        } => {
            let mut arguments = Map::new();
            arguments.insert("provider".to_owned(), json!(provider));
            arguments.insert("object".to_owned(), json!(object));
            arguments.insert("dataset".to_owned(), json!(dataset));
            arguments.insert("discoveryReceipt".to_owned(), json!(discovery_receipt));
            ("Research.StartIngestSource", arguments)
        }
        ResearchControlCommand::StartExport { dataset } => {
            ("Research.StartExport", dataset_arguments(dataset))
        }
    };
    invoke_narrow(operation, arguments, true, confirmed, &state).await
}

#[tauri::command]
pub(crate) async fn job_control(
    request: JobControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let (operation, arguments, mutation) = match request {
        JobControlCommand::List {
            after_job_id,
            limit,
        } => {
            let mut arguments = Map::new();
            if let Some(after_job_id) = after_job_id {
                arguments.insert("afterJobId".to_owned(), json!(after_job_id));
            }
            arguments.insert("limit".to_owned(), json!(limit));
            ("Job.List", arguments, false)
        }
        JobControlCommand::Get { job_id, generation } => {
            let mut arguments = map_with_job_id(job_id);
            arguments.insert(
                "generation".to_owned(),
                json!(parse_job_generation(generation)?),
            );
            ("Job.Get", arguments, false)
        }
        JobControlCommand::Watch {
            job_id,
            generation,
            after_sequence,
            limit,
        } => {
            let mut arguments = map_with_job_id(job_id);
            arguments.insert(
                "generation".to_owned(),
                json!(parse_job_generation(generation)?),
            );
            arguments.insert(
                "afterSequence".to_owned(),
                json!(parse_job_sequence(after_sequence)?),
            );
            arguments.insert("limit".to_owned(), json!(limit));
            ("Job.Watch", arguments, false)
        }
        JobControlCommand::Cancel {
            job_id,
            generation,
            expected_sequence,
        } => {
            let arguments = job_mutation_arguments(job_id, generation, expected_sequence)?;
            ("Job.Cancel", arguments, true)
        }
        JobControlCommand::Confirm {
            job_id,
            generation,
            expected_sequence,
            identity,
            digest,
        } => {
            let mut arguments = job_mutation_arguments(job_id, generation, expected_sequence)?;
            arguments.insert("identity".to_owned(), json!(identity));
            arguments.insert("digest".to_owned(), json!(digest));
            ("Job.Confirm", arguments, true)
        }
        JobControlCommand::Retry {
            job_id,
            generation,
            expected_sequence,
        } => {
            let arguments = job_mutation_arguments(job_id, generation, expected_sequence)?;
            ("Job.Retry", arguments, true)
        }
    };
    invoke_narrow(operation, arguments, mutation, confirmed, &state).await
}

#[tauri::command]
pub(crate) async fn source_control(
    action: SourceLifecycleAction,
    request: SourceLifecycleInput,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let operation = match action {
        SourceLifecycleAction::Start => "Source.Start",
        SourceLifecycleAction::Stop => "Source.Stop",
        SourceLifecycleAction::Retry => "Source.Retry",
        SourceLifecycleAction::Resynchronize => "Source.Resynchronize",
        SourceLifecycleAction::Verify => "Source.Verify",
        SourceLifecycleAction::Reconfigure => "Source.Reconfigure",
        SourceLifecycleAction::Remove => "Source.Remove",
    };
    let mut arguments = Map::new();
    arguments.insert("provider".to_owned(), json!(&request.provider));
    arguments.insert("sourceCoverage".to_owned(), json!([&request.provider]));
    arguments.insert(
        "expectedStateRevision".to_owned(),
        json!(request.expected_state_revision),
    );
    insert_optional(
        &mut arguments,
        "expectedGeneration",
        request.expected_generation,
    );
    insert_optional(
        &mut arguments,
        "expectedRuntimeGenerationSha256",
        request.expected_runtime_generation_sha256,
    );
    insert_optional(
        &mut arguments,
        "onboardingSessionId",
        request.onboarding_session_id,
    );
    insert_optional(
        &mut arguments,
        "publicConfigurationSha256",
        request.public_configuration_sha256,
    );
    insert_optional(&mut arguments, "reason", request.reason);
    invoke_narrow(operation, arguments, true, confirmed, &state).await
}

async fn invoke_narrow(
    operation: &'static str,
    arguments: Map<String, Value>,
    mutation: bool,
    confirmed: bool,
    state: &DesktopState,
) -> Result<Value, DesktopCommandError> {
    if mutation && !confirmed {
        return Err(DesktopCommandError::new(
            "confirmation_required",
            "Confirm the requested change before continuing.",
        ));
    }
    let authority = if mutation {
        InvocationAuthority::ExactConfirmed(operation)
    } else {
        InvocationAuthority::ReadOnly
    };
    let mut result = invoke_application(
        ApplicationInvocation {
            operation: operation.to_owned(),
            arguments,
        },
        state,
        authority,
    )
    .await?;
    if matches!(
        operation,
        "Job.List" | "Job.Get" | "Job.Watch" | "Job.Cancel" | "Job.Confirm" | "Job.Retry"
    ) {
        canonicalize_job_result(operation, &mut result)?;
    }
    Ok(result)
}

fn require_confirmation(confirmed: bool) -> Result<(), DesktopCommandError> {
    if confirmed {
        Ok(())
    } else {
        Err(DesktopCommandError::new(
            "confirmation_required",
            "Confirm the governance step before continuing.",
        ))
    }
}

fn map_with_job_id(job_id: uuid::Uuid) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("jobId".to_owned(), json!(job_id));
    arguments
}

fn account_arguments(account_id: String) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("accountId".to_owned(), json!(account_id));
    arguments
}

fn instrument_arguments(instrument_id: uuid::Uuid) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("instrumentIds".to_owned(), json!([instrument_id]));
    arguments
}

fn source_discovery_arguments(provider: String, dataset: String) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("provider".to_owned(), json!(provider.clone()));
    arguments.insert("dataset".to_owned(), json!(dataset));
    arguments.insert("sourceCoverage".to_owned(), json!([provider]));
    arguments
}

fn dataset_arguments(dataset: String) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("dataset".to_owned(), json!(dataset));
    arguments
}

fn target_arguments(target_id: String, revision: u32) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("targetId".to_owned(), json!(target_id));
    arguments.insert("revision".to_owned(), json!(revision));
    arguments
}

fn model_arguments(model_id: String) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("modelId".to_owned(), json!(model_id));
    arguments
}

fn vintage_arguments(vintage_id: String) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("vintageId".to_owned(), json!(vintage_id));
    arguments
}

fn measurement_arguments(measurement_id: String) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("measurementId".to_owned(), json!(measurement_id));
    arguments
}

fn backup_arguments(backup_id: String) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("backupId".to_owned(), json!(backup_id));
    arguments
}

fn preview_arguments(preview_id: uuid::Uuid, preview_digest: String) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("previewId".to_owned(), json!(preview_id));
    arguments.insert("previewDigest".to_owned(), json!(preview_digest));
    arguments
}

struct OperationLogArguments {
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
}

fn operation_log_arguments(
    input: OperationLogArguments,
) -> Result<Map<String, Value>, DesktopCommandError> {
    let mut arguments = Map::new();
    if let Some(value) = input.from_unix_nanos {
        arguments.insert("from".to_owned(), json!(parse_unix_nanos(value)?));
    }
    if let Some(value) = input.through_unix_nanos {
        arguments.insert("through".to_owned(), json!(parse_unix_nanos(value)?));
    }
    insert_optional(&mut arguments, "minimumSeverity", input.minimum_severity);
    insert_optional(&mut arguments, "domain", input.domain);
    insert_optional(&mut arguments, "sourceId", input.source_id);
    insert_optional(&mut arguments, "jobId", input.job_id);
    insert_optional(&mut arguments, "correlationId", input.correlation_id);
    insert_optional(&mut arguments, "search", input.search);
    if let Some(value) = input.after_sequence {
        arguments.insert(
            "afterSequence".to_owned(),
            json!(parse_unsigned_decimal(
                value,
                "The log sequence must be an unsigned decimal.",
            )?),
        );
    }
    arguments.insert("limit".to_owned(), json!(input.limit));
    Ok(arguments)
}

fn parse_unix_nanos(value: String) -> Result<i64, DesktopCommandError> {
    value.parse::<i64>().map_err(|_error| {
        DesktopCommandError::invalid_request("Log time filters must be signed Unix nanoseconds.")
    })
}

fn parse_job_generation(value: String) -> Result<u64, DesktopCommandError> {
    parse_canonical_u64(&value)
        .filter(|generation| *generation > 0)
        .ok_or_else(|| {
            DesktopCommandError::invalid_request(
                "The job generation must be a positive canonical unsigned decimal.",
            )
        })
}

fn parse_job_sequence(value: String) -> Result<u64, DesktopCommandError> {
    parse_canonical_u64(&value).ok_or_else(|| {
        DesktopCommandError::invalid_request(
            "The job sequence must be a canonical unsigned decimal.",
        )
    })
}

fn parse_canonical_u64(value: &str) -> Option<u64> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || (bytes.len() > 1 && bytes[0] == b'0')
        || !bytes.iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    value.parse::<u64>().ok()
}

fn parse_unsigned_decimal(
    value: String,
    message: &'static str,
) -> Result<u64, DesktopCommandError> {
    value
        .parse::<u64>()
        .map_err(|_error| DesktopCommandError::invalid_request(message))
}

fn operation_setting_values(
    values: Vec<OperationSettingValue>,
) -> Result<Value, DesktopCommandError> {
    values
        .into_iter()
        .map(|value| match value {
            OperationSettingValue::StorageSoftLimitBytes(value) => Ok(json!({
                "kind": "storage_soft_limit_bytes",
                "value": parse_unsigned_decimal(
                    value,
                    "The storage limit must be an unsigned decimal.",
                )?,
            })),
            OperationSettingValue::LogRetentionDays(value) => {
                Ok(json!({ "kind": "log_retention_days", "value": value }))
            }
            OperationSettingValue::LogMinimumSeverity(value) => {
                Ok(json!({ "kind": "log_minimum_severity", "value": value }))
            }
            OperationSettingValue::UpdateChannel(value) => {
                Ok(json!({ "kind": "update_channel", "value": value }))
            }
            OperationSettingValue::AutomaticUpdateChecks(value) => {
                Ok(json!({ "kind": "automatic_update_checks", "value": value }))
            }
            OperationSettingValue::DefaultQueryRowLimit(value) => {
                Ok(json!({ "kind": "default_query_row_limit", "value": value }))
            }
            OperationSettingValue::MaximumConcurrentJobs(value) => {
                Ok(json!({ "kind": "maximum_concurrent_jobs", "value": value }))
            }
            OperationSettingValue::MarketFreshnessMillis(value) => {
                Ok(json!({ "kind": "market_freshness_millis", "value": value }))
            }
            OperationSettingValue::BackupRetentionCount(value) => {
                Ok(json!({ "kind": "backup_retention_count", "value": value }))
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn reason_arguments(reason: String) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("reason".to_owned(), json!(reason));
    arguments
}

fn source_arguments(source_ids: Option<Vec<String>>) -> Map<String, Value> {
    let mut arguments = Map::new();
    insert_optional(&mut arguments, "sourceCoverage", source_ids);
    arguments
}

fn job_mutation_arguments(
    job_id: uuid::Uuid,
    generation: String,
    expected_sequence: String,
) -> Result<Map<String, Value>, DesktopCommandError> {
    let mut arguments = map_with_job_id(job_id);
    arguments.insert(
        "generation".to_owned(),
        json!(parse_job_generation(generation)?),
    );
    arguments.insert(
        "expectedSequence".to_owned(),
        json!(parse_job_sequence(expected_sequence)?),
    );
    Ok(arguments)
}

fn canonicalize_job_result(
    operation: &'static str,
    result: &mut Value,
) -> Result<(), DesktopCommandError> {
    let data = result
        .get_mut("data")
        .ok_or_else(DesktopCommandError::internal)?;
    match operation {
        "Job.List" => {
            let jobs = data
                .get_mut("jobs")
                .and_then(Value::as_array_mut)
                .ok_or_else(DesktopCommandError::internal)?;
            for job in jobs {
                canonicalize_job_view(job)?;
            }
        }
        "Job.Get" | "Job.Cancel" | "Job.Confirm" => canonicalize_job_view(data)?,
        "Job.Retry" => canonicalize_job_receipt(data)?,
        "Job.Watch" => canonicalize_job_event_page(data)?,
        _ => return Err(DesktopCommandError::internal()),
    }
    let bytes = serde_json::to_vec(result).map_err(|_error| DesktopCommandError::internal())?;
    if bytes.len() > MAXIMUM_CANONICAL_JOB_RESULT_BYTES {
        return Err(DesktopCommandError::new(
            "resource_exhausted",
            "The operation result exceeds the dashboard safety limit.",
        ));
    }
    Ok(())
}

fn canonicalize_job_view(value: &mut Value) -> Result<(), DesktopCommandError> {
    let object = value
        .as_object_mut()
        .ok_or_else(DesktopCommandError::internal)?;
    canonicalize_unsigned_field(object, "generation", true)?;
    canonicalize_unsigned_field(object, "sequence", false)
}

fn canonicalize_job_receipt(value: &mut Value) -> Result<(), DesktopCommandError> {
    canonicalize_job_view(value)
}

fn canonicalize_job_event_page(value: &mut Value) -> Result<(), DesktopCommandError> {
    let object = value
        .as_object_mut()
        .ok_or_else(DesktopCommandError::internal)?;
    let events = object
        .get_mut("events")
        .and_then(Value::as_array_mut)
        .ok_or_else(DesktopCommandError::internal)?;
    for event in events {
        let tuple = event
            .as_array_mut()
            .filter(|tuple| tuple.len() == 2)
            .ok_or_else(DesktopCommandError::internal)?;
        canonicalize_unsigned_value(&mut tuple[0], false)?;
    }
    let next = object
        .get_mut("next")
        .ok_or_else(DesktopCommandError::internal)?;
    if !next.is_null() {
        canonicalize_unsigned_value(next, false)?;
    }
    Ok(())
}

fn canonicalize_unsigned_field(
    object: &mut Map<String, Value>,
    field: &'static str,
    positive: bool,
) -> Result<(), DesktopCommandError> {
    let value = object
        .get_mut(field)
        .ok_or_else(DesktopCommandError::internal)?;
    canonicalize_unsigned_value(value, positive)
}

fn canonicalize_unsigned_value(
    value: &mut Value,
    positive: bool,
) -> Result<(), DesktopCommandError> {
    // The shared bridge has already converted values outside JavaScript's safe range to strings.
    // Normalize both possible internal Serde representations to one WebView contract.
    let parsed = match value {
        Value::Number(number) => number.as_u64(),
        Value::String(decimal) => parse_canonical_u64(decimal),
        _ => None,
    }
    .filter(|value| !positive || *value > 0)
    .ok_or_else(DesktopCommandError::internal)?;
    *value = Value::String(parsed.to_string());
    Ok(())
}

fn insert_optional<T: serde::Serialize>(
    arguments: &mut Map<String, Value>,
    key: &'static str,
    value: Option<T>,
) {
    if let Some(value) = value {
        arguments.insert(key.to_owned(), json!(value));
    }
}
