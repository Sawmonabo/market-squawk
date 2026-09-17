//! Narrow desktop controls over the shared application service.

use serde_json::{Map, Value, json};
use tauri::State;

use crate::{
    bridge::{DesktopState, InvocationAuthority, invoke_application, invoke_private_application},
    contracts::{
        AnalysisControlCommand, ApplicationInvocation, BacktestProductCommand,
        DashboardQueryCommand, DecisionControlCommand, DesktopCommandError,
        FairValueControlCommand, GovernanceControlCommand, GovernanceQueryCommand,
        JobControlCommand, ModelControlCommand, ModelProductCommand, OperationLogDomain,
        OperationLogSeverity, OperationSettingValue, OperationsControlCommand, PaperControlCommand,
        ResearchControlCommand, SourceLifecycleAction, SourceLifecycleInput,
    },
};

// Canonical string conversion happens after the shared bridge's size check, so retain its cap.
const MAXIMUM_CANONICAL_JOB_RESULT_BYTES: usize = 1024 * 1024;
const MAXIMUM_SAFE_WEB_NUMBER: u64 = 9_007_199_254_740_991;

#[derive(Clone, Copy)]
enum DashboardProjection {
    None,
    ResearchCollections,
    ResearchCollection,
    ResearchObservations,
    ResearchActivities,
}

#[tauri::command]
pub(crate) async fn dashboard_query(
    request: DashboardQueryCommand,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    let mut projection = DashboardProjection::None;
    let (operation, arguments) = match request {
        DashboardQueryCommand::MacroContext {
            knowledge_cutoff,
            effective_date_cutoff,
        } => {
            if knowledge_cutoff.is_some() != effective_date_cutoff.is_some() {
                return Err(DesktopCommandError::invalid_request(
                    "Economic context dates must be supplied together.",
                ));
            }
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "knowledgeCutoff", knowledge_cutoff);
            insert_optional(&mut arguments, "effectiveDateCutoff", effective_date_cutoff);
            ("Macro.GetContext", arguments)
        }
        DashboardQueryCommand::Lookup { text, categories } => {
            let mut arguments = Map::new();
            arguments.insert("query".to_owned(), json!(text));
            insert_optional(&mut arguments, "categories", categories);
            ("Analysis.Lookup", arguments)
        }
        DashboardQueryCommand::MarketOverview { page_token } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "pageToken", page_token);
            ("Market.GetOverview", arguments)
        }
        DashboardQueryCommand::MarketUniverse { text, page_token } => {
            let mut arguments = Map::new();
            arguments.insert("query".to_owned(), json!(text));
            insert_optional(&mut arguments, "pageToken", page_token);
            ("Market.SearchUniverse", arguments)
        }
        DashboardQueryCommand::MarketInstrument { selection_token } => {
            let mut arguments = Map::new();
            arguments.insert("selectionToken".to_owned(), json!(selection_token));
            ("Market.GetInstrument", arguments)
        }
        DashboardQueryCommand::MarketHistory { history_token } => {
            let mut arguments = Map::new();
            arguments.insert("historyToken".to_owned(), json!(history_token));
            ("Market.GetHistory", arguments)
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
        DashboardQueryCommand::ResearchCollections { after_collection } => {
            projection = DashboardProjection::ResearchCollections;
            let mut arguments = Map::new();
            let after_dataset = after_collection
                .map(|collection| generation.resolve_research_collection(collection))
                .transpose()?;
            insert_optional(&mut arguments, "afterDataset", after_dataset);
            ("Research.ListDatasets", arguments)
        }
        DashboardQueryCommand::ResearchCollection { collection } => {
            projection = DashboardProjection::ResearchCollection;
            let dataset = generation.resolve_research_collection(collection)?;
            ("Research.GetManifest", dataset_arguments(dataset))
        }
        DashboardQueryCommand::ResearchCollectionHistory { collection } => {
            projection = DashboardProjection::ResearchObservations;
            let dataset = generation.resolve_research_collection(collection)?;
            ("Research.GetHistory", dataset_arguments(dataset))
        }
        DashboardQueryCommand::ResearchCollectionAlternativeData { collection } => {
            projection = DashboardProjection::ResearchObservations;
            let dataset = generation.resolve_research_collection(collection)?;
            ("Research.GetAlternativeData", dataset_arguments(dataset))
        }
        DashboardQueryCommand::ResearchActivities => {
            projection = DashboardProjection::ResearchActivities;
            let mut arguments = Map::new();
            arguments.insert("limit".to_owned(), json!(25_u16));
            ("Job.List", arguments)
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
        DashboardQueryCommand::PortfolioAccounts {
            after_account_token,
        } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "afterAccountToken", after_account_token);
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
        DashboardQueryCommand::PortfolioRisk { account_token } => {
            let mut arguments = Map::new();
            arguments.insert("accountToken".to_owned(), json!(account_token));
            ("Portfolio.GetRisk", arguments)
        }
        DashboardQueryCommand::PortfolioRevisions {
            account_id,
            after_snapshot_token,
        } => {
            let mut arguments = account_arguments(account_id);
            insert_optional(&mut arguments, "afterSnapshotToken", after_snapshot_token);
            ("Portfolio.ListRevisions", arguments)
        }
        DashboardQueryCommand::PortfolioAttribution {
            account_id,
            baseline_snapshot_token,
        } => {
            let mut arguments = account_arguments(account_id);
            arguments.insert(
                "baselineSnapshotToken".to_owned(),
                json!(baseline_snapshot_token),
            );
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
            instrument_id,
            proposed_quantity,
            scenario_shock,
        } => {
            let mut arguments = Map::new();
            arguments.insert("instrumentId".to_owned(), json!(instrument_id));
            arguments.insert("proposedQuantity".to_owned(), json!(proposed_quantity));
            arguments.insert("scenarioShock".to_owned(), json!(scenario_shock));
            ("Portfolio.EvaluateCandidateImpact", arguments)
        }
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
        DashboardQueryCommand::Forecast { forecast_token } => {
            ("Model.GetForecast", forecast_arguments(forecast_token))
        }
        DashboardQueryCommand::ForecastOutcomes { forecast_token } => (
            "Model.GetForecastOutcomes",
            forecast_arguments(forecast_token),
        ),
        DashboardQueryCommand::DecisionScreens { limit } => {
            let mut arguments = Map::new();
            arguments.insert("limit".to_owned(), json!(limit));
            ("Decision.ListScreens", arguments)
        }
        DashboardQueryCommand::DecisionScreen { screen_id } => {
            let mut arguments = Map::new();
            arguments.insert("screenId".to_owned(), json!(screen_id));
            ("Decision.GetScreen", arguments)
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
        DashboardQueryCommand::DecisionInvestmentAnalysis { action_token } => {
            let mut arguments = Map::new();
            arguments.insert("actionToken".to_owned(), json!(action_token));
            ("Decision.GetInvestmentAnalysis", arguments)
        }
        DashboardQueryCommand::DecisionInvestmentAnalyses {
            after_action_token,
            limit,
        } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "afterActionToken", after_action_token);
            arguments.insert("limit".to_owned(), json!(limit));
            ("Decision.ListInvestmentAnalyses", arguments)
        }
        DashboardQueryCommand::DecisionRecommendationTrackRecord { action_token } => {
            let mut arguments = Map::new();
            arguments.insert("actionToken".to_owned(), json!(action_token));
            ("Decision.GetRecommendationTrackRecord", arguments)
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
        DashboardQueryCommand::FairValueWorkspace {
            measurement_token,
            at,
        } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "measurementToken", measurement_token);
            arguments.insert("at".to_owned(), json!(at));
            ("FairValue.GetWorkspace", arguments)
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
    };
    let mut result = invoke_application(
        ApplicationInvocation {
            operation: operation.to_owned(),
            arguments,
        },
        &state,
        &generation,
        InvocationAuthority::ReadOnly,
    )
    .await?;
    if operation == "Job.List" {
        canonicalize_job_result(operation, &mut result)?;
    }
    match projection {
        DashboardProjection::None => {}
        DashboardProjection::ResearchCollections => {
            project_research_collections(&mut result, &generation)?;
        }
        DashboardProjection::ResearchCollection => {
            project_research_collection(&mut result, &generation)?;
        }
        DashboardProjection::ResearchObservations => {
            project_research_observations(&mut result)?;
        }
        DashboardProjection::ResearchActivities => {
            project_research_activities(&mut result, &generation)?;
        }
    }
    Ok(result)
}

fn project_research_collections(
    result: &mut Value,
    generation: &crate::bridge::DesktopGeneration,
) -> Result<(), DesktopCommandError> {
    let data = application_result_data(result)?.clone();
    if data.is_null() {
        return project_product_metadata(result);
    }
    let page = data.as_object().ok_or_else(DesktopCommandError::internal)?;
    let items = page
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(DesktopCommandError::internal)?;
    let mut collections = Vec::new();
    collections
        .try_reserve_exact(items.len())
        .map_err(|_error| DesktopCommandError::internal())?;
    for item in items {
        collections.push(project_research_collection_value(item, generation)?);
    }
    let has_more = page
        .get("hasMore")
        .and_then(Value::as_bool)
        .ok_or_else(DesktopCommandError::internal)?;
    let next_collection = if has_more {
        let dataset = page
            .get("nextAfterDataset")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(DesktopCommandError::internal)?;
        Value::String(
            generation
                .register_research_collection(dataset)?
                .to_string(),
        )
    } else {
        Value::Null
    };
    *application_result_data_mut(result)? = json!({
        "items": collections,
        "hasMore": has_more,
        "nextCollection": next_collection,
    });
    project_product_metadata(result)
}

fn project_research_collection(
    result: &mut Value,
    generation: &crate::bridge::DesktopGeneration,
) -> Result<(), DesktopCommandError> {
    let data = application_result_data(result)?.clone();
    if data.is_null() {
        return Err(DesktopCommandError::internal());
    }
    *application_result_data_mut(result)? = project_research_collection_value(&data, generation)?;
    project_product_metadata(result)
}

fn project_research_collection_value(
    generation_value: &Value,
    generation: &crate::bridge::DesktopGeneration,
) -> Result<Value, DesktopCommandError> {
    let dataset = generation_value
        .pointer("/manifest/datasetId")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(DesktopCommandError::internal)?;
    let schema = generation_value
        .pointer("/manifest/schema/name")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(DesktopCommandError::internal)?;
    let row_count = safe_web_count(
        generation_value
            .get("rowCount")
            .and_then(Value::as_u64)
            .ok_or_else(DesktopCommandError::internal)?,
    )?;
    let collection = generation.register_research_collection(dataset)?;
    Ok(json!({
        "collectionToken": collection,
        "title": research_collection_title(schema),
        "rowCount": row_count,
    }))
}

fn project_research_observations(result: &mut Value) -> Result<(), DesktopCommandError> {
    let data = application_result_data(result)?.clone();
    if data.is_null() {
        return project_product_metadata(result);
    }
    let observations = data.as_object().ok_or_else(DesktopCommandError::internal)?;
    let projected = if let Some(rows) = observations.get("rows").and_then(Value::as_array) {
        let mut projected_rows = Vec::new();
        projected_rows
            .try_reserve_exact(rows.len())
            .map_err(|_error| DesktopCommandError::internal())?;
        for row in rows {
            projected_rows.push(project_research_observation_row(row)?);
        }
        json!({
            "kind": "inline",
            "rows": projected_rows,
        })
    } else {
        let row_count = observations
            .get("artifact")
            .and_then(Value::as_object)
            .and_then(|artifact| artifact.get("rowCount"))
            .and_then(Value::as_u64)
            .ok_or_else(DesktopCommandError::internal)?;
        json!({
            "kind": "artifact",
            "rowCount": safe_web_count(row_count)?,
        })
    };
    *application_result_data_mut(result)? = projected;
    project_product_metadata(result)
}

fn project_feature_dataset_options(
    result: &mut Value,
    generation: &crate::bridge::DesktopGeneration,
) -> Result<(), DesktopCommandError> {
    let data = application_result_data(result)?
        .as_object()
        .ok_or_else(DesktopCommandError::internal)?;
    let catalog_generation = data
        .get("catalogGeneration")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(DesktopCommandError::internal)?;
    let datasets = data
        .get("datasets")
        .and_then(Value::as_array)
        .ok_or_else(DesktopCommandError::internal)?;
    let mut choices = Vec::new();
    choices
        .try_reserve_exact(datasets.len())
        .map_err(|_error| DesktopCommandError::internal())?;
    for dataset in datasets {
        let dataset = dataset
            .as_object()
            .ok_or_else(DesktopCommandError::internal)?;
        let id = dataset
            .get("id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(DesktopCommandError::internal)?;
        let label = dataset
            .get("label")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(DesktopCommandError::internal)?;
        let examples = safe_web_count(
            dataset
                .get("examples")
                .and_then(Value::as_u64)
                .ok_or_else(DesktopCommandError::internal)?,
        )?;
        let observed_from = required_scalar(dataset, "observedFrom")?;
        let observed_through = required_scalar(dataset, "observedThrough")?;
        let available_uses = dataset
            .get("availableUses")
            .and_then(Value::as_array)
            .filter(|uses| {
                !uses.is_empty()
                    && uses.iter().all(|use_case| {
                        matches!(use_case.as_str(), Some("local_analysis" | "train"))
                    })
            })
            .cloned()
            .ok_or_else(DesktopCommandError::internal)?;
        let choice = generation.register_research_preparation_choice(
            format!("{catalog_generation}:{id}"),
            json!({
                "catalogGeneration": catalog_generation,
                "dataset": id,
            }),
        )?;
        choices.push(json!({
            "choiceToken": choice,
            "title": research_collection_title(label),
            "examples": examples,
            "observedFrom": observed_from,
            "observedThrough": observed_through,
            "availableUses": available_uses,
        }));
    }
    *application_result_data_mut(result)? = json!({ "choices": choices });
    project_product_metadata(result)
}

fn project_feature_dataset_preview(
    result: &mut Value,
    generation: &crate::bridge::DesktopGeneration,
) -> Result<(), DesktopCommandError> {
    let data = application_result_data(result)?
        .as_object()
        .ok_or_else(DesktopCommandError::internal)?;
    let receipt = data
        .get("receipt")
        .cloned()
        .filter(Value::is_object)
        .ok_or_else(DesktopCommandError::internal)?;
    let expires_at = receipt
        .get("expiresAt")
        .filter(|value| value.is_string() || value.is_number())
        .cloned()
        .ok_or_else(DesktopCommandError::internal)?;
    let confirmation_token = generation.register_research_preparation_receipt(receipt)?;
    let projected = json!({
        "confirmationToken": confirmation_token,
        "intendedUse": required_string_value(data, "intendedUse")?,
        "examples": required_safe_web_count(data, "examples")?,
        "trainExamples": required_safe_web_count(data, "trainExamples")?,
        "validationExamples": required_safe_web_count(data, "validationExamples")?,
        "testExamples": required_safe_web_count(data, "testExamples")?,
        "observedFrom": required_scalar(data, "observedFrom")?,
        "observedThrough": required_scalar(data, "observedThrough")?,
        "expiresAt": expires_at,
    });
    *application_result_data_mut(result)? = projected;
    project_product_metadata(result)
}

fn project_research_action_accepted(result: &mut Value) -> Result<(), DesktopCommandError> {
    *application_result_data_mut(result)? = json!({ "accepted": true });
    set_product_item_counts(result, 1)?;
    project_product_metadata(result)
}

fn required_string_value(
    object: &Map<String, Value>,
    field: &'static str,
) -> Result<String, DesktopCommandError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(DesktopCommandError::internal)
}

fn required_scalar(
    object: &Map<String, Value>,
    field: &'static str,
) -> Result<Value, DesktopCommandError> {
    object
        .get(field)
        .filter(|value| value.is_string() || value.is_number())
        .cloned()
        .ok_or_else(DesktopCommandError::internal)
}

fn required_safe_web_count(
    object: &Map<String, Value>,
    field: &'static str,
) -> Result<u64, DesktopCommandError> {
    object
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(DesktopCommandError::internal)
        .and_then(safe_web_count)
}

fn project_research_activities(
    result: &mut Value,
    generation: &crate::bridge::DesktopGeneration,
) -> Result<(), DesktopCommandError> {
    let jobs = application_result_data(result)?
        .get("jobs")
        .and_then(Value::as_array)
        .ok_or_else(DesktopCommandError::internal)?;
    let mut activities = Vec::new();
    activities
        .try_reserve_exact(jobs.len())
        .map_err(|_error| DesktopCommandError::internal())?;
    for job in jobs {
        let Some(activity) = project_research_activity(job, generation)? else {
            continue;
        };
        activities.push(activity);
    }
    let count = safe_web_count(
        u64::try_from(activities.len()).map_err(|_error| DesktopCommandError::internal())?,
    )?;
    *application_result_data_mut(result)? = json!({ "activities": activities });
    set_product_item_counts(result, count)?;
    project_product_metadata(result)
}

fn project_research_activity(
    value: &Value,
    generation: &crate::bridge::DesktopGeneration,
) -> Result<Option<Value>, DesktopCommandError> {
    let job = value
        .as_object()
        .ok_or_else(DesktopCommandError::internal)?;
    let kind = job
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(DesktopCommandError::internal)?;
    if !kind.starts_with("research.")
        && kind != "analysis.phase-one-feature-derived-generation-job.v1"
    {
        return Ok(None);
    }
    let job_id = job
        .get("jobId")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(DesktopCommandError::internal)?;
    let generation_value = job
        .get("generation")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(DesktopCommandError::internal)?;
    let sequence = job
        .get("sequence")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(DesktopCommandError::internal)?;
    let activity = generation.register_research_activity(
        format!("{job_id}:{generation_value}"),
        json!({
            "jobId": job_id,
            "generation": generation_value,
            "expectedSequence": sequence,
        }),
    )?;
    Ok(Some(project_research_activity_payload(job, activity)?))
}

fn project_research_activity_payload(
    job: &Map<String, Value>,
    activity: uuid::Uuid,
) -> Result<Value, DesktopCommandError> {
    let kind = job
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(DesktopCommandError::internal)?;
    let state = job
        .get("state")
        .and_then(Value::as_str)
        .ok_or_else(DesktopCommandError::internal)?;
    let completed_units = optional_safe_web_count(job.get("completedUnits"))?;
    let total_units = optional_safe_web_count(job.get("totalUnits"))?;
    let cancellation_requested = job
        .get("cancellationRequested")
        .and_then(Value::as_bool)
        .ok_or_else(DesktopCommandError::internal)?;
    let retryable = state == "failed"
        && job
            .get("failure")
            .and_then(Value::as_object)
            .and_then(|failure| failure.get("retryable"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
    Ok(json!({
        "activityToken": activity,
        "label": research_activity_label(kind),
        "state": state,
        "completedUnits": completed_units,
        "totalUnits": total_units,
        "cancellationRequested": cancellation_requested,
        "updatedAt": job
            .get("updatedAt")
            .filter(|value| value.is_string() || value.is_number())
            .cloned()
            .ok_or_else(DesktopCommandError::internal)?,
        "canCancel": matches!(
            state,
            "queued" | "preparing" | "running" | "awaiting_confirmation" | "recovering"
        ) && !cancellation_requested,
        "canRetry": retryable,
    }))
}

fn research_activity_label(kind: &str) -> &'static str {
    match kind {
        "research.ingest-source.v1" => "Load research information",
        "research.phase-one-derived-generation-job.v1" => "Prepare derived research data",
        "analysis.phase-one-feature-derived-generation-job.v1" => "Prepare model features",
        "research.dataset-export.v1" => "Export research history",
        "research.sec-fund-publication.v1" => "Prepare company and fund reports",
        _ => "Research activity",
    }
}

fn optional_safe_web_count(value: Option<&Value>) -> Result<Option<u64>, DesktopCommandError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .ok_or_else(DesktopCommandError::internal)
            .and_then(safe_web_count)
            .map(Some),
    }
}

fn project_research_observation_row(row: &Value) -> Result<Value, DesktopCommandError> {
    let row = row.as_object().ok_or_else(DesktopCommandError::internal)?;
    let mut projected = Map::new();
    copy_first_scalar(row, &mut projected, "revision", &["revision"]);
    copy_first_scalar(row, &mut projected, "quality", &["quality"]);
    copy_first_scalar(
        row,
        &mut projected,
        "effectiveAt",
        &[
            "effective_at",
            "effectiveAt",
            "effective_date",
            "effectiveDate",
        ],
    );
    copy_first_scalar(
        row,
        &mut projected,
        "publishedAt",
        &[
            "published_at",
            "publishedAt",
            "published_date",
            "publishedDate",
        ],
    );
    copy_first_scalar(
        row,
        &mut projected,
        "availableAt",
        &["available_at", "availableAt"],
    );
    copy_first_scalar(
        row,
        &mut projected,
        "supersededAt",
        &[
            "superseded_at",
            "supersededAt",
            "superseded_date",
            "supersededDate",
        ],
    );
    Ok(Value::Object(projected))
}

fn copy_first_scalar(
    source: &Map<String, Value>,
    destination: &mut Map<String, Value>,
    output: &str,
    candidates: &[&str],
) {
    if let Some(value) = candidates
        .iter()
        .filter_map(|candidate| source.get(*candidate))
        .find(|value| value.is_null() || value.is_string() || value.is_number())
    {
        destination.insert(output.to_owned(), value.clone());
    }
}

fn research_collection_title(schema: &str) -> &'static str {
    let name = schema.to_ascii_lowercase();
    if name.contains("fund_nav") || name.contains("fund-nav") || name.contains("fund nav") {
        "Mutual fund NAV history"
    } else if name.contains("option") {
        "Options history"
    } else if name.contains("macro") || name.contains("economic") || name.contains("rate") {
        "Economic indicators"
    } else if name.contains("filing") || name.contains("fundamental") {
        "Company and fund reports"
    } else if name.contains("feature") {
        "Model inputs"
    } else if name.contains("label") || name.contains("outcome") {
        "Model outcomes"
    } else if name.contains("bar") || name.contains("price") || name.contains("eod") {
        "Market price history"
    } else {
        "Research collection"
    }
}

fn project_product_metadata(result: &mut Value) -> Result<(), DesktopCommandError> {
    let metadata = result
        .get_mut("metadata")
        .and_then(Value::as_object_mut)
        .ok_or_else(DesktopCommandError::internal)?;
    let completeness = metadata
        .get("completeness")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(DesktopCommandError::internal)?
        .to_owned();
    let returned_items = safe_web_count(
        metadata
            .get("returnedItems")
            .and_then(Value::as_u64)
            .ok_or_else(DesktopCommandError::internal)?,
    )?;
    let available_items = safe_web_count(
        metadata
            .get("availableItems")
            .and_then(Value::as_u64)
            .ok_or_else(DesktopCommandError::internal)?,
    )?;
    *metadata = Map::from_iter([
        ("completeness".to_owned(), json!(completeness)),
        ("returnedItems".to_owned(), json!(returned_items)),
        ("availableItems".to_owned(), json!(available_items)),
    ]);
    Ok(())
}

fn set_product_item_counts(result: &mut Value, count: u64) -> Result<(), DesktopCommandError> {
    let metadata = result
        .get_mut("metadata")
        .and_then(Value::as_object_mut)
        .ok_or_else(DesktopCommandError::internal)?;
    metadata.insert("returnedItems".to_owned(), json!(count));
    metadata.insert("availableItems".to_owned(), json!(count));
    Ok(())
}

fn application_result_data(result: &Value) -> Result<&Value, DesktopCommandError> {
    result
        .as_object()
        .and_then(|result| result.get("data"))
        .ok_or_else(DesktopCommandError::internal)
}

fn application_result_data_mut(result: &mut Value) -> Result<&mut Value, DesktopCommandError> {
    result
        .as_object_mut()
        .and_then(|result| result.get_mut("data"))
        .ok_or_else(DesktopCommandError::internal)
}

fn safe_web_count(value: u64) -> Result<u64, DesktopCommandError> {
    if value <= MAXIMUM_SAFE_WEB_NUMBER {
        Ok(value)
    } else {
        Err(DesktopCommandError::internal())
    }
}

#[tauri::command]
pub(crate) async fn operations_control(
    request: OperationsControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
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
    };
    invoke_application(
        ApplicationInvocation {
            operation: operation.to_owned(),
            arguments,
        },
        &state,
        &generation,
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
    let generation = state.generation()?;
    match request {
        FairValueControlCommand::Measure { measurement } => {
            let mut arguments = Map::new();
            arguments.insert("measurement".to_owned(), Value::Object(measurement));
            invoke_narrow(
                "FairValue.Measure",
                arguments,
                true,
                confirmed,
                &state,
                &generation,
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
                &generation,
                InvocationAuthority::ExactConfirmed("FairValue.PreviewGovernanceAction"),
            )
            .await
        }
        FairValueControlCommand::CommitGovernanceAction {
            preview_id,
            authorization_handles,
        } => {
            require_confirmation(confirmed)?;
            let ticket_ids = generation.consume_governance_authorizations(
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
                &generation,
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
    let generation = state.generation()?;
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
                &generation,
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
                &generation,
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
                &generation,
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
                &generation,
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
                &generation,
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
                &generation,
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
                &generation,
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
                &generation,
                InvocationAuthority::ExactConfirmed("Decision.PreviewGovernanceAction"),
            )
            .await
        }
        DecisionControlCommand::CommitGovernanceAction {
            preview_id,
            authorization_handles,
        } => {
            require_confirmation(confirmed)?;
            let ticket_ids = generation.consume_governance_authorizations(
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
                &generation,
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
    let generation = state.generation()?;
    match request {
        GovernanceQueryCommand::ProvisioningStatus => {
            invoke_private_application(
                "Governance.ProvisioningStatus",
                Map::new(),
                &state,
                &generation,
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
                &generation,
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
    let generation = state.generation()?;
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
                &generation,
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
                &generation,
                InvocationAuthority::ExactConfirmed("Governance.AuthenticateAction"),
            )
            .await?;
            let result = generation.retain_governance_authorization(window.label(), result)?;
            state.admit_current(&generation)?;
            Ok(result)
        }
    }
}

#[tauri::command]
pub(crate) async fn paper_control(
    request: PaperControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    let (operation, arguments, authority) = match request {
        PaperControlCommand::StartPreparation => (
            "Bot.GetStartPreparation",
            Map::new(),
            InvocationAuthority::ReadOnly,
        ),
        PaperControlCommand::PrepareStart {
            cash_choice,
            cost_choice,
            mode_choice,
        } => {
            let mut arguments = Map::new();
            arguments.insert("cashChoice".to_owned(), json!(cash_choice));
            arguments.insert("costChoice".to_owned(), json!(cost_choice));
            arguments.insert("modeChoice".to_owned(), json!(mode_choice));
            ("Bot.PrepareStart", arguments, InvocationAuthority::ReadOnly)
        }
        PaperControlCommand::Start { confirmation_token } => {
            require_confirmation(confirmed)?;
            let mut arguments = Map::new();
            arguments.insert("confirmationToken".to_owned(), json!(confirmation_token));
            (
                "Bot.Start",
                arguments,
                InvocationAuthority::ExactConfirmed("Bot.Start"),
            )
        }
        PaperControlCommand::Targets => (
            "Execution.GetManualPaperTargets",
            Map::new(),
            InvocationAuthority::ReadOnly,
        ),
        PaperControlCommand::PrepareManual {
            target_token,
            side,
            order_type,
            quantity_lots,
            limit_target_level,
            stop_target_level,
            time_in_force,
        } => {
            let mut arguments = Map::new();
            arguments.insert("targetToken".to_owned(), json!(target_token));
            arguments.insert("side".to_owned(), json!(side));
            arguments.insert("orderType".to_owned(), json!(order_type));
            arguments.insert("quantityLots".to_owned(), json!(quantity_lots));
            insert_optional(&mut arguments, "limitTargetLevel", limit_target_level);
            insert_optional(&mut arguments, "stopTargetLevel", stop_target_level);
            arguments.insert("timeInForce".to_owned(), json!(time_in_force));
            (
                "Execution.PrepareManualPaperDraft",
                arguments,
                InvocationAuthority::ReadOnly,
            )
        }
        PaperControlCommand::SubmitManual { confirmation_token } => {
            require_confirmation(confirmed)?;
            let mut arguments = Map::new();
            arguments.insert("confirmationToken".to_owned(), json!(confirmation_token));
            (
                "Execution.SubmitManualPaperDraft",
                arguments,
                InvocationAuthority::RiskMediated("Execution.SubmitManualPaperDraft"),
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
        PaperControlCommand::Cancel { action_token } => {
            require_confirmation(confirmed)?;
            let mut arguments = Map::new();
            arguments.insert("actionToken".to_owned(), json!(action_token));
            (
                "Execution.Cancel",
                arguments,
                InvocationAuthority::RiskMediated("Execution.Cancel"),
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
        &generation,
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
    let generation = state.generation()?;
    match request {
        AnalysisControlCommand::FeatureDatasetOptions => {
            let mut result = invoke_narrow(
                "Analysis.GetFeatureDatasetPreparationOptions",
                Map::new(),
                false,
                confirmed,
                &state,
                &generation,
            )
            .await?;
            project_feature_dataset_options(&mut result, &generation)?;
            return Ok(result);
        }
        AnalysisControlCommand::PreviewFeatureDataset {
            choice,
            intended_use,
        } => {
            let authority = generation.resolve_research_preparation_choice(choice)?;
            let mut arguments = authority
                .as_object()
                .cloned()
                .ok_or_else(DesktopCommandError::internal)?;
            arguments.insert("intendedUse".to_owned(), json!(intended_use));
            let mut result = invoke_narrow(
                "Analysis.PreviewFeatureDatasetBuild",
                arguments,
                false,
                confirmed,
                &state,
                &generation,
            )
            .await?;
            project_feature_dataset_preview(&mut result, &generation)?;
            return Ok(result);
        }
        AnalysisControlCommand::StartPreparedFeatureDataset { confirmation_token } => {
            require_confirmation(confirmed)?;
            let authority = generation.consume_research_preparation_receipt(confirmation_token)?;
            let mut arguments = Map::new();
            arguments.insert("receipt".to_owned(), authority);
            let mut result = invoke_narrow(
                "Analysis.StartPreparedFeatureDatasetBuild",
                arguments,
                true,
                confirmed,
                &state,
                &generation,
            )
            .await?;
            project_research_action_accepted(&mut result)?;
            return Ok(result);
        }
        _ => {}
    }
    let (operation, arguments, mutation) = match request {
        AnalysisControlCommand::BacktestOptions => {
            ("Analysis.GetBacktestPreparation", Map::new(), false)
        }
        AnalysisControlCommand::PreviewBacktest { selection } => {
            let mut arguments = Map::new();
            arguments.insert("selection".to_owned(), Value::Object(selection));
            ("Analysis.PreviewBacktest", arguments, false)
        }
        AnalysisControlCommand::StartPreparedBacktest { confirmation_token } => {
            let mut arguments = Map::new();
            arguments.insert("confirmationToken".to_owned(), json!(confirmation_token));
            ("Analysis.StartPreparedBacktest", arguments, true)
        }
        AnalysisControlCommand::FeatureDatasetOptions
        | AnalysisControlCommand::PreviewFeatureDataset { .. }
        | AnalysisControlCommand::StartPreparedFeatureDataset { .. } => unreachable!(),
    };
    invoke_narrow(
        operation,
        arguments,
        mutation,
        confirmed,
        &state,
        &generation,
    )
    .await
}

#[tauri::command]
pub(crate) async fn backtest_products(
    request: BacktestProductCommand,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    let (operation, arguments) = match request {
        BacktestProductCommand::List => ("Analysis.ListProductBacktests", Map::new()),
        BacktestProductCommand::Get { backtest_token } => {
            let mut arguments = Map::new();
            arguments.insert("backtestToken".to_owned(), json!(backtest_token));
            ("Analysis.GetProductBacktest", arguments)
        }
    };
    invoke_narrow(operation, arguments, false, false, &state, &generation).await
}

#[tauri::command]
pub(crate) async fn model_control(
    request: ModelControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    let (operation, arguments, mutation) = match request {
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
        ModelControlCommand::StartPreparedForecast { confirmation_token } => {
            let mut arguments = Map::new();
            arguments.insert("confirmationToken".to_owned(), json!(confirmation_token));
            ("Model.StartPreparedForecast", arguments, true)
        }
    };
    invoke_narrow(
        operation,
        arguments,
        mutation,
        confirmed,
        &state,
        &generation,
    )
    .await
}

#[tauri::command]
pub(crate) async fn model_products(
    request: ModelProductCommand,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    let operation = match request {
        ModelProductCommand::List => "Model.ListBundles",
        ModelProductCommand::Activity => "Model.ListProductActivity",
    };
    invoke_narrow(operation, Map::new(), false, false, &state, &generation).await
}

#[tauri::command]
pub(crate) async fn research_control(
    request: ResearchControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    let mut product_projection = false;
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
        ResearchControlCommand::StartCollectionExport { collection } => {
            product_projection = true;
            let dataset = generation.resolve_research_collection(collection)?;
            ("Research.StartExport", dataset_arguments(dataset))
        }
        ResearchControlCommand::CancelActivity { activity } => {
            require_confirmation(confirmed)?;
            product_projection = true;
            let authority = generation.resolve_research_activity(activity)?;
            ("Job.Cancel", research_activity_arguments(authority)?)
        }
        ResearchControlCommand::RetryActivity { activity } => {
            require_confirmation(confirmed)?;
            product_projection = true;
            let authority = generation.resolve_research_activity(activity)?;
            ("Job.Retry", research_activity_arguments(authority)?)
        }
    };
    let mut result =
        invoke_narrow(operation, arguments, true, confirmed, &state, &generation).await?;
    if product_projection {
        project_research_action_accepted(&mut result)?;
    }
    Ok(result)
}

#[tauri::command]
pub(crate) async fn job_control(
    request: JobControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
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
    invoke_narrow(
        operation,
        arguments,
        mutation,
        confirmed,
        &state,
        &generation,
    )
    .await
}

#[tauri::command]
pub(crate) async fn source_control(
    action: SourceLifecycleAction,
    request: SourceLifecycleInput,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
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
    let expected_state_revision = parse_positive_u64(
        request.expected_state_revision,
        "The source state revision must be a positive canonical unsigned decimal.",
    )?;
    arguments.insert(
        "expectedStateRevision".to_owned(),
        json!(expected_state_revision.to_string()),
    );
    if let Some(expected_generation) = request.expected_generation {
        let expected_generation = parse_positive_u64(
            expected_generation,
            "The source generation must be a positive canonical unsigned decimal.",
        )?;
        arguments.insert(
            "expectedGeneration".to_owned(),
            Value::String(expected_generation.to_string()),
        );
    }
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
    invoke_narrow(operation, arguments, true, confirmed, &state, &generation).await
}

async fn invoke_narrow(
    operation: &'static str,
    arguments: Map<String, Value>,
    mutation: bool,
    confirmed: bool,
    state: &DesktopState,
    generation: &std::sync::Arc<crate::bridge::DesktopGeneration>,
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
        generation,
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

fn forecast_arguments(forecast_token: uuid::Uuid) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("forecastToken".to_owned(), json!(forecast_token));
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

fn parse_positive_u64(value: String, message: &'static str) -> Result<u64, DesktopCommandError> {
    parse_canonical_u64(&value)
        .filter(|parsed| *parsed > 0)
        .ok_or_else(|| DesktopCommandError::invalid_request(message))
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

fn research_activity_arguments(
    authority: Value,
) -> Result<Map<String, Value>, DesktopCommandError> {
    let authority = authority
        .as_object()
        .ok_or_else(DesktopCommandError::internal)?;
    let job_id = authority
        .get("jobId")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<uuid::Uuid>().ok())
        .ok_or_else(DesktopCommandError::internal)?;
    let generation = required_string_value(authority, "generation")?;
    let expected_sequence = required_string_value(authority, "expectedSequence")?;
    job_mutation_arguments(job_id, generation, expected_sequence)
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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::project_research_activity_payload;

    #[test]
    fn research_activity_projection_keeps_internal_authority_native() {
        let sentinel = "raw-provider-manifest-job-authority";
        let raw = json!({
            "jobId": "2e59a77e-5115-4860-92f2-61a18dd4136c",
            "generation": "91",
            "sequence": "7",
            "kind": "research.ingest-source.v1",
            "state": "running",
            "completedUnits": 3,
            "totalUnits": 10,
            "cancellationRequested": false,
            "updatedAt": "1800000000000000001",
            "result": {"manifest": sentinel},
            "failure": {"provider": sentinel},
            "sourceCoverage": [sentinel],
        });
        let token = Uuid::from_u128(7);
        let projected =
            project_research_activity_payload(raw.as_object().expect("raw activity object"), token)
                .expect("product activity projection");

        assert_eq!(projected.get("activityToken"), Some(&json!(token)));
        assert!(!projected.to_string().contains(sentinel));
        assert_eq!(projected.as_object().map(serde_json::Map::len), Some(9));
    }
}
