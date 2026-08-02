//! Narrow desktop controls over the shared application service.

use serde_json::{Map, Value, json};
use tauri::State;

use crate::{
    bridge::{DesktopState, InvocationAuthority, invoke_application},
    contracts::{
        ApplicationInvocation, DashboardQueryCommand, DecisionControlCommand, DesktopCommandError,
        FairValueControlCommand, JobControlCommand, ModelControlCommand, PaperControlCommand,
        PortfolioControlCommand, ResearchControlCommand, SourceLifecycleAction,
        SourceLifecycleInput,
    },
};

#[tauri::command]
pub(crate) async fn dashboard_query(
    request: DashboardQueryCommand,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let (operation, arguments) = match request {
        DashboardQueryCommand::Overview => ("Analysis.GetDecisionOverview", Map::new()),
        DashboardQueryCommand::Lookup { text, categories } => {
            let mut arguments = Map::new();
            arguments.insert("query".to_owned(), json!(text));
            insert_optional(&mut arguments, "categories", categories);
            ("Analysis.Lookup", arguments)
        }
        DashboardQueryCommand::MarketSnapshot => ("Market.GetSnapshot", Map::new()),
        DashboardQueryCommand::MarketQuality => ("Market.GetQuality", Map::new()),
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
    };
    invoke_application(
        ApplicationInvocation {
            operation: operation.to_owned(),
            arguments,
        },
        &state,
        InvocationAuthority::ReadOnly,
    )
    .await
}

#[tauri::command]
pub(crate) async fn fair_value_control(
    request: FairValueControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let (operation, arguments) = fair_value_request(request);
    invoke_narrow(operation, arguments, true, confirmed, &state).await
}

#[tauri::command]
pub(crate) async fn paper_control(
    request: PaperControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    if !confirmed {
        return Err(DesktopCommandError::new(
            "confirmation_required",
            "Confirm the requested paper-operation change before continuing.",
        ));
    }
    let (operation, arguments, risk_mediated) = match request {
        PaperControlCommand::Start {
            provider,
            provider_session_id,
            initial_cash,
            fee_basis_points,
        } => {
            let mut arguments = Map::new();
            arguments.insert("provider".to_owned(), json!(provider));
            insert_optional(&mut arguments, "providerSessionId", provider_session_id);
            arguments.insert("initialCash".to_owned(), json!(initial_cash));
            arguments.insert("feeBasisPoints".to_owned(), json!(fee_basis_points));
            ("Bot.Start", arguments, false)
        }
        PaperControlCommand::Stop { reason } => ("Bot.Stop", reason_arguments(reason), false),
        PaperControlCommand::Cancel { order_id } => {
            let mut arguments = Map::new();
            arguments.insert("orderId".to_owned(), json!(order_id));
            ("Execution.Cancel", arguments, true)
        }
        PaperControlCommand::Reconcile => ("Execution.Reconcile", Map::new(), true),
        PaperControlCommand::TriggerKillSwitch { reason } => {
            ("Risk.TriggerKillSwitch", reason_arguments(reason), false)
        }
    };
    let authority = if risk_mediated {
        InvocationAuthority::RiskMediated(operation)
    } else {
        InvocationAuthority::ExactConfirmed(operation)
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
pub(crate) async fn portfolio_control(
    request: PortfolioControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let PortfolioControlCommand::Import {
        account_id,
        artifact_id,
    } = request;
    let mut arguments = account_arguments(account_id);
    arguments.insert("artifactId".to_owned(), json!(artifact_id));
    invoke_narrow("Portfolio.Import", arguments, true, confirmed, &state).await
}

fn fair_value_request(request: FairValueControlCommand) -> (&'static str, Map<String, Value>) {
    match request {
        FairValueControlCommand::Measure { measurement } => {
            let mut arguments = Map::new();
            arguments.insert("measurement".to_owned(), Value::Object(measurement));
            ("FairValue.Measure", arguments)
        }
        FairValueControlCommand::Classify { measurement_id } => {
            ("FairValue.Classify", measurement_arguments(measurement_id))
        }
        FairValueControlCommand::Approve {
            measurement_id,
            decision_id,
            approved_by,
            approved_at,
            expires_at,
        } => {
            let mut arguments = measurement_arguments(measurement_id);
            arguments.insert("decisionId".to_owned(), json!(decision_id));
            arguments.insert("approvedBy".to_owned(), json!(approved_by));
            arguments.insert("approvedAt".to_owned(), json!(approved_at));
            arguments.insert("expiresAt".to_owned(), json!(expires_at));
            ("FairValue.Approve", arguments)
        }
        FairValueControlCommand::ProposeOverride {
            measurement_id,
            decision_id,
            requested_hierarchy,
            justification,
            prepared_by,
            prepared_at,
            expires_at,
        } => {
            let mut arguments = measurement_arguments(measurement_id);
            arguments.insert("decisionId".to_owned(), json!(decision_id));
            arguments.insert("requestedHierarchy".to_owned(), json!(requested_hierarchy));
            arguments.insert("justification".to_owned(), json!(justification));
            arguments.insert("preparedBy".to_owned(), json!(prepared_by));
            arguments.insert("preparedAt".to_owned(), json!(prepared_at));
            arguments.insert("expiresAt".to_owned(), json!(expires_at));
            ("FairValue.ProposeOverride", arguments)
        }
        FairValueControlCommand::RevokeApproval {
            approval_id,
            revoked_by,
            revoked_at,
            reason,
        } => {
            let mut arguments = Map::new();
            arguments.insert("approvalId".to_owned(), json!(approval_id));
            arguments.insert("revokedBy".to_owned(), json!(revoked_by));
            arguments.insert("revokedAt".to_owned(), json!(revoked_at));
            arguments.insert("reason".to_owned(), json!(reason));
            ("FairValue.RevokeApproval", arguments)
        }
        FairValueControlCommand::ApproveMarketAccess {
            account_id,
            venue_id,
            instrument_id,
            conclusion,
            effective_from,
            effective_until,
            rationale,
            prepared_by,
            prepared_at,
            approved_by,
            approved_at,
        } => {
            let mut arguments = Map::new();
            arguments.insert("accountId".to_owned(), json!(account_id));
            arguments.insert("venueId".to_owned(), json!(venue_id));
            arguments.insert("instrumentId".to_owned(), json!(instrument_id));
            arguments.insert("conclusion".to_owned(), json!(conclusion));
            arguments.insert("effectiveFrom".to_owned(), json!(effective_from));
            arguments.insert("effectiveUntil".to_owned(), json!(effective_until));
            arguments.insert("rationale".to_owned(), json!(rationale));
            arguments.insert("preparedBy".to_owned(), json!(prepared_by));
            arguments.insert("preparedAt".to_owned(), json!(prepared_at));
            arguments.insert("approvedBy".to_owned(), json!(approved_by));
            arguments.insert("approvedAt".to_owned(), json!(approved_at));
            ("FairValue.ApproveMarketAccess", arguments)
        }
    }
}

#[tauri::command]
pub(crate) async fn model_control(
    request: ModelControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let (operation, arguments) = match request {
        ModelControlCommand::Evaluate { model_id, input } => {
            let mut arguments = model_arguments(model_id);
            arguments.insert("input".to_owned(), Value::Object(input));
            ("Model.Evaluate", arguments)
        }
        ModelControlCommand::StartTraining {
            config_ticket_id,
            authority_ticket_id,
        } => {
            let mut arguments = Map::new();
            arguments.insert("configTicketId".to_owned(), json!(config_ticket_id));
            arguments.insert("authorityTicketId".to_owned(), json!(authority_ticket_id));
            ("Model.StartTraining", arguments)
        }
        ModelControlCommand::StartForecast { model_id, request } => {
            let mut arguments = model_arguments(model_id);
            arguments.insert("request".to_owned(), Value::Object(request));
            ("Model.StartForecast", arguments)
        }
    };
    invoke_narrow(operation, arguments, true, confirmed, &state).await
}

#[tauri::command]
pub(crate) async fn decision_control(
    request: DecisionControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let (operation, arguments) = match request {
        DecisionControlCommand::SaveScreen {
            expected_revision,
            screen,
        } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "expectedRevision", expected_revision);
            arguments.insert("screen".to_owned(), Value::Object(screen));
            ("Decision.SaveScreen", arguments)
        }
        DecisionControlCommand::RunScreen {
            run,
            candidates,
            selected_at,
        } => {
            let mut arguments = Map::new();
            arguments.insert("run".to_owned(), Value::Object(run));
            arguments.insert("candidates".to_owned(), Value::Array(candidates));
            arguments.insert("selectedAt".to_owned(), json!(selected_at));
            ("Decision.RunScreen", arguments)
        }
        DecisionControlCommand::CreateTarget { target } => {
            let mut arguments = Map::new();
            arguments.insert("target".to_owned(), Value::Object(target));
            ("Decision.CreateTargetSet", arguments)
        }
        DecisionControlCommand::ReviewTarget { review } => {
            let mut arguments = Map::new();
            arguments.insert("review".to_owned(), Value::Object(review));
            ("Decision.ReviewTargetSet", arguments)
        }
        DecisionControlCommand::ReevaluateTarget {
            expected_revision,
            successor,
        } => {
            let mut arguments = Map::new();
            arguments.insert("expectedRevision".to_owned(), json!(expected_revision));
            arguments.insert("successor".to_owned(), Value::Object(successor));
            ("Decision.ReevaluateTargetSet", arguments)
        }
    };
    invoke_narrow(operation, arguments, true, confirmed, &state).await
}

#[tauri::command]
pub(crate) async fn research_control(
    request: ResearchControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let (operation, arguments) = match request {
        ResearchControlCommand::StartIngest {
            provider,
            object,
            dataset,
            discovery_receipt,
        } => {
            let mut arguments = dataset_arguments(dataset);
            arguments.insert("provider".to_owned(), json!(provider));
            arguments.insert("object".to_owned(), json!(object));
            arguments.insert("discoveryReceipt".to_owned(), json!(discovery_receipt));
            ("Research.StartIngestSource", arguments)
        }
        ResearchControlCommand::StartDatasetBuild { registration } => {
            let mut arguments = Map::new();
            arguments.insert("registration".to_owned(), Value::Object(registration));
            ("Research.StartDatasetBuild", arguments)
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
        JobControlCommand::Get { job_id } => ("Job.Get", map_with_job_id(job_id), false),
        JobControlCommand::Watch {
            job_id,
            generation,
            after_sequence,
            limit,
        } => {
            let mut arguments = map_with_job_id(job_id);
            arguments.insert("generation".to_owned(), json!(generation));
            arguments.insert("afterSequence".to_owned(), json!(after_sequence));
            arguments.insert("limit".to_owned(), json!(limit));
            ("Job.Watch", arguments, false)
        }
        JobControlCommand::Cancel {
            job_id,
            generation,
            expected_sequence,
        } => (
            "Job.Cancel",
            job_mutation_arguments(job_id, generation, expected_sequence),
            true,
        ),
        JobControlCommand::Confirm {
            job_id,
            generation,
            expected_sequence,
            identity,
            digest,
        } => {
            let mut arguments = job_mutation_arguments(job_id, generation, expected_sequence);
            arguments.insert("identity".to_owned(), json!(identity));
            arguments.insert("digest".to_owned(), json!(digest));
            ("Job.Confirm", arguments, true)
        }
        JobControlCommand::Retry {
            job_id,
            generation,
            expected_sequence,
        } => (
            "Job.Retry",
            job_mutation_arguments(job_id, generation, expected_sequence),
            true,
        ),
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
    arguments.insert("provider".to_owned(), json!(request.provider));
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
    invoke_application(
        ApplicationInvocation {
            operation: operation.to_owned(),
            arguments,
        },
        state,
        authority,
    )
    .await
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
    generation: u64,
    expected_sequence: u64,
) -> Map<String, Value> {
    let mut arguments = map_with_job_id(job_id);
    arguments.insert("generation".to_owned(), json!(generation));
    arguments.insert("expectedSequence".to_owned(), json!(expected_sequence));
    arguments
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
