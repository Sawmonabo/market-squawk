//! Narrow desktop controls over the shared application service.

use serde_json::{Map, Value, json};
use tauri::State;

use crate::{
    bridge::{DesktopState, InvocationAuthority, invoke_application},
    contracts::{
        ApplicationInvocation, DashboardQueryCommand, DesktopCommandError, JobControlCommand,
        SourceLifecycleAction, SourceLifecycleInput,
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
        DashboardQueryCommand::SourceStatus => ("Source.GetStatus", Map::new()),
        DashboardQueryCommand::SourceCoverage => ("Source.GetCoverage", Map::new()),
        DashboardQueryCommand::SourceHealth => ("Source.GetHealth", Map::new()),
        DashboardQueryCommand::ResearchDatasets { after_dataset } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "afterDataset", after_dataset);
            ("Research.ListDatasets", arguments)
        }
        DashboardQueryCommand::PortfolioAccounts { after_account_id } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "afterAccountId", after_account_id);
            ("Portfolio.ListAccounts", arguments)
        }
        DashboardQueryCommand::PortfolioHoldings { account_id } => {
            ("Portfolio.GetHoldings", account_arguments(account_id))
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
        DashboardQueryCommand::ModelBundles => ("Model.ListBundles", Map::new()),
        DashboardQueryCommand::Forecasts => ("Model.ListForecasts", Map::new()),
        DashboardQueryCommand::Backtests { dataset } => {
            let mut arguments = Map::new();
            insert_optional(&mut arguments, "dataset", dataset);
            ("Analysis.GetBacktests", arguments)
        }
        DashboardQueryCommand::PaperStatus => ("Bot.GetStatus", Map::new()),
        DashboardQueryCommand::PaperOrders => ("Execution.GetOrders", Map::new()),
        DashboardQueryCommand::PaperFills => ("Execution.GetFills", Map::new()),
        DashboardQueryCommand::FairValueMeasurements => ("FairValue.ListMeasurements", Map::new()),
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
