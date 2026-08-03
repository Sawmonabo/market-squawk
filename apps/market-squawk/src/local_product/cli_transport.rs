//! Bounded CLI transport over the same application operations exposed through MCP.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use market_squawk_data::{AnalyticalReadError, QueryError};
use market_squawk_platform::UserAuthorizedInputRoot;
use market_squawk_runtime::{ApplicationClientError, LoopbackApplicationClient};
use market_squawk_services::{
    ArtifactError, JsonStructureLimits, RequestContext, RequestId, ServiceLimits,
    ToolResultMetadata, TypedToolResult,
};
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{LocalProduct, cli_backtest, cli_dataset, cli_model, cli_portfolio, cli_provider};
use crate::application::{
    logs::{LogDomain, LogSeverity},
    settings::{SettingValue, UpdateChannel},
    setup::{SetupGoal, SetupPlanSelection, SetupStarterPlan},
};
use crate::cli::{
    BacktestCommand, BackupOperationsCommand, BackupRetentionCommand, BotCommand, Command,
    DatasetCommand, ExecutionCommand, FairValueCommand, FeatureCommand, IngestCommand, JobCommand,
    LogDomainArgument, LogOperationsCommand, LogQueryArguments, LogSeverityArgument, ModelCommand,
    OperationsCommand, OperationsPreviewConfirmationArguments, PortfolioCommand,
    ProgramRollbackCommand, QueryCommand, RestoreCommand, SettingsChangeArguments,
    SettingsChangeCommand, SettingsOperationsCommand, SettingsRollbackCommand, SetupApplyArguments,
    SetupCommand, SetupGoalArgument, SetupPreviewArguments, SetupStarterPlanArgument,
    SourceCommand, UpdateChannelArgument, UpdateOperationsCommand, WorkspaceOperationsCommand,
    WorkspaceSwitchCommand,
};

mod files;
mod query;

const CLI_REQUEST_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const CLI_INSTALLED_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CLI_JSON_MAXIMUM_BYTES: u64 = 8 * 1024 * 1024;
const CLI_DEFAULT_MAXIMUM_ITEMS: usize = 10_000;
const CLI_DEFAULT_MAXIMUM_BYTES: usize = 16 * 1024 * 1024;
const CLI_HARD_MAXIMUM_BYTES: usize = 64 * 1024 * 1024;

/// Structured result returned by one product CLI command.
#[derive(Debug)]
pub struct CliProductResult {
    summary: &'static str,
    value: Value,
}

impl CliProductResult {
    /// Concise operator-facing disposition.
    pub const fn summary(&self) -> &'static str {
        self.summary
    }

    /// Stable JSON result envelope.
    pub const fn value(&self) -> &Value {
        &self.value
    }
}

/// CLI admission, filesystem-boundary, or application-operation failure.
#[derive(Debug, Error)]
pub enum CliProductError {
    /// The selected command belongs to another process composition.
    #[error("command is not a local product operation")]
    WrongCommand,
    /// A request file path cannot be safely confined and read.
    #[error("CLI request file is not an admitted bounded regular file")]
    RequestFile,
    /// A request file is not a JSON object.
    #[error("CLI request file must contain one JSON object")]
    RequestShape,
    /// An immutable analytical generation could not be resolved.
    #[error("CLI analytical generation resolution failed: {0}")]
    AnalyticalRead(#[from] AnalyticalReadError),
    /// A bounded read-only DataFusion query failed.
    #[error("CLI analytical query failed: {0}")]
    Query(#[from] QueryError),
    /// Opaque analytical result publication or retrieval failed.
    #[error("CLI analytical artifact failed: {0}")]
    Artifact(#[from] ArtifactError),
    /// A point-in-time dataset build failed admission or publication.
    #[error("{0}")]
    Dataset(#[from] cli_dataset::CliDatasetError),
    /// A controlled portfolio manifest import failed.
    #[error("{0}")]
    Portfolio(#[from] cli_portfolio::CliPortfolioImportError),
    /// A production model bundle failed closed admission.
    #[error("{0}")]
    ModelAdmission(#[from] cli_model::CliModelAdmissionError),
    /// A governed backtest input failed closed registration.
    #[error("{0}")]
    BacktestRegistration(#[from] cli_backtest::CliBacktestRegistrationError),
    /// A verified provider activation request failed closed.
    #[error("{0}")]
    ProviderActivation(#[from] cli_provider::CliProviderActivationError),
    /// CLI-owned request limits are invalid.
    #[error("CLI request limits are invalid")]
    Limits,
    /// The shared application rejected or failed the operation.
    #[error("application operation failed: {0}")]
    Application(#[from] market_squawk_services::ServiceError),
    /// The installed service client rejected or could not complete the operation.
    #[error(transparent)]
    Client(#[from] ApplicationClientError),
    /// The installed-service request contract could not be constructed.
    #[error("CLI installed-service request is invalid")]
    RuntimeRequest,
    /// A mutation command omitted its explicit operator confirmation.
    #[error("CLI mutation requires --confirm")]
    ConfirmationRequired,
    /// A typed settings preview omitted every closed setting value.
    #[error("CLI settings change requires at least one typed setting option")]
    SettingsChangeRequired,
    /// A guided setup goal/starter selection is incompatible.
    #[error("CLI setup selection is invalid: {0}")]
    SetupPlan(#[from] crate::application::setup::SetupPlanError),
    /// This operation still requires a service-owned staged-input consumer.
    #[error(
        "CLI operation `{operation}` requires a service-owned staged-input or specialized workflow"
    )]
    StagedInputRequired { operation: &'static str },
    /// The operation is owned only by the shared installed service.
    #[error("CLI operation `{operation}` requires the installed application service")]
    InstalledServiceRequired { operation: &'static str },
    /// The paper command could not observe its requested stop signal.
    #[error("failed to wait for the paper-operation stop condition")]
    Signal,
}

/// Executes one product command through shared application authority.
pub async fn execute_cli_command(
    product: &LocalProduct,
    command: Command,
) -> Result<CliProductResult, CliProductError> {
    execute(CliAuthority::Local(product), command).await
}

/// Executes one product command through the authenticated installed-service client.
pub async fn execute_installed_cli_command(
    client: &LoopbackApplicationClient,
    command: Command,
) -> Result<CliProductResult, CliProductError> {
    execute(CliAuthority::Installed(client), command).await
}

#[derive(Clone, Copy)]
enum CliAuthority<'a> {
    Local(&'a LocalProduct),
    Installed(&'a LoopbackApplicationClient),
}

impl<'a> CliAuthority<'a> {
    fn local_for(self, operation: &'static str) -> Result<&'a LocalProduct, CliProductError> {
        match self {
            Self::Local(product) => Ok(product),
            Self::Installed(_) => Err(CliProductError::StagedInputRequired { operation }),
        }
    }
}

async fn execute(
    authority: CliAuthority<'_>,
    command: Command,
) -> Result<CliProductResult, CliProductError> {
    match command {
        Command::Source { command } => source(authority, command).await,
        Command::Ingest { command } => ingest(authority, command).await,
        Command::Dataset { command } => dataset(authority, command).await,
        Command::Query { command } => query(authority, command).await,
        Command::Feature { command } => feature(authority, command).await,
        Command::Model { command } => model(authority, command).await,
        Command::Portfolio { command } => portfolio(authority, command).await,
        Command::Backtest { command } => backtest(authority, command).await,
        Command::Bot { command } => bot(authority, command).await,
        Command::Execution { command } => execution(authority, command).await,
        Command::FairValue { command } => fair_value(authority, command).await,
        Command::Job { command } => job(authority, command).await,
        Command::Operations { command } => operations(authority, command).await,
        Command::Setup { command } => setup(authority, command).await,
        Command::Init
        | Command::Config { .. }
        | Command::Capture(_)
        | Command::Release { .. }
        | Command::Service { .. }
        | Command::Mcp { .. }
        | Command::Doctor
        | Command::Mock(_)
        | Command::PaperBot(_)
        | Command::Replay(_) => Err(CliProductError::WrongCommand),
    }
}

async fn source(
    authority: CliAuthority<'_>,
    command: SourceCommand,
) -> Result<CliProductResult, CliProductError> {
    let (operation, mut arguments, summary) = match command {
        SourceCommand::Register { provider, confirm } => (
            "Source.Register",
            json_object(json!({"provider": provider, "confirm": confirm}))?,
            "source profile registered",
        ),
        SourceCommand::Status { provider } => (
            "Source.GetStatus",
            source_filter(provider),
            "source status read",
        ),
        SourceCommand::Coverage { provider } => (
            "Source.GetCoverage",
            source_filter(provider),
            "source coverage read",
        ),
        SourceCommand::Health { provider } => (
            "Source.GetHealth",
            source_filter(provider),
            "source health read",
        ),
        SourceCommand::Setup { provider, confirm } => (
            "Source.Setup",
            json_object(json!({"provider": provider, "confirm": confirm}))?,
            "source setup opened",
        ),
        SourceCommand::Discover { provider, dataset } => (
            "Source.ListObjects",
            json_object(json!({
                "provider": provider,
                "dataset": dataset,
                "sourceCoverage": [provider],
            }))?,
            "source objects discovered",
        ),
        SourceCommand::Inspect {
            provider,
            onboarding_session_id,
            dataset_identifier,
            page_index,
            max_records,
        } => {
            let maximum_items = usize::from(max_records);
            let mut arguments = json_object(json!({
                "provider": provider,
                "onboardingSessionId": onboarding_session_id,
                "datasetIdentifier": dataset_identifier,
                "pageIndex": page_index,
                "maxRecords": max_records,
                "sourceCoverage": [provider],
            }))?;
            return invoke(
                authority,
                "Source.Inspect",
                &mut arguments,
                Some(maximum_items),
                "source page inspected",
            )
            .await;
        }
        SourceCommand::Activate { request, confirm } => {
            let value = cli_provider::activate_research_provider(
                authority.local_for("Source.Activate")?,
                &request,
                confirm,
                CancellationToken::new(),
            )
            .await?;
            return direct_result(value, "source adapter activated");
        }
    };
    invoke(authority, operation, &mut arguments, None, summary).await
}

async fn ingest(
    authority: CliAuthority<'_>,
    command: IngestCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        IngestCommand::Source {
            provider,
            object,
            dataset,
            confirm,
        } => {
            let mut discovery_arguments = json_object(json!({
                "provider": provider,
                "dataset": dataset,
                "confirm": confirm,
                "sourceCoverage": [provider],
            }))?;
            let discovery = invoke(
                authority,
                "Source.Discover",
                &mut discovery_arguments,
                None,
                "source ingestion authority minted",
            )
            .await?;
            let discovery_receipt =
                exact_discovery_receipt(&discovery, &provider, &dataset, &object)?;
            let mut arguments = json_object(json!({
                "provider": provider,
                "object": object,
                "dataset": dataset,
                "discoveryReceipt": discovery_receipt,
                "confirm": confirm,
                "sourceCoverage": [provider],
            }))?;
            invoke(
                authority,
                "Research.IngestSource",
                &mut arguments,
                None,
                "source object ingested",
            )
            .await
        }
        IngestCommand::File {
            manifest,
            object,
            dataset,
            confirm,
        } => {
            files::ingest_local_file(
                authority.local_for("Research.IngestFile")?,
                &manifest,
                object,
                dataset,
                confirm,
            )
            .await
        }
    }
}

fn exact_discovery_receipt(
    discovery: &CliProductResult,
    provider: &str,
    dataset: &str,
    object: &str,
) -> Result<String, CliProductError> {
    let data = discovery
        .value()
        .get("data")
        .and_then(Value::as_object)
        .ok_or(CliProductError::Application(
            market_squawk_services::ServiceError::InvalidResult,
        ))?;
    if data.get("profile").and_then(Value::as_str) != Some(provider)
        || data
            .get("request")
            .and_then(Value::as_object)
            .and_then(|request| request.get("dataset"))
            .and_then(Value::as_str)
            != Some(dataset)
    {
        return Err(CliProductError::Application(
            market_squawk_services::ServiceError::InvalidResult,
        ));
    }
    let objects =
        data.get("objects")
            .and_then(Value::as_array)
            .ok_or(CliProductError::Application(
                market_squawk_services::ServiceError::InvalidResult,
            ))?;
    let mut matches = objects.iter().filter(|candidate| {
        candidate.get("object_id").and_then(Value::as_str) == Some(object)
            && candidate.get("dataset").and_then(Value::as_str) == Some(dataset)
    });
    let selected = matches.next().ok_or(CliProductError::Application(
        market_squawk_services::ServiceError::NotFound,
    ))?;
    if matches.next().is_some() {
        return Err(CliProductError::Application(
            market_squawk_services::ServiceError::InvalidResult,
        ));
    }
    selected
        .get("discovery_receipt")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or(CliProductError::Application(
            market_squawk_services::ServiceError::InvalidResult,
        ))
}

async fn dataset(
    authority: CliAuthority<'_>,
    command: DatasetCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        DatasetCommand::List { after_dataset } => {
            let mut arguments = Map::new();
            if let Some(after_dataset) = after_dataset {
                arguments.insert("afterDataset".to_owned(), Value::String(after_dataset));
            }
            invoke(
                authority,
                "Research.ListDatasets",
                &mut arguments,
                None,
                "datasets listed",
            )
            .await
        }
        DatasetCommand::Manifest { dataset } => {
            let mut arguments = json_object(json!({"dataset": dataset}))?;
            invoke(
                authority,
                "Research.GetManifest",
                &mut arguments,
                None,
                "dataset manifest read",
            )
            .await
        }
        DatasetCommand::Build { request, confirm } => {
            let value = cli_dataset::build_point_in_time_dataset(
                authority.local_for("Research.BuildDataset")?,
                &request,
                confirm,
            )
            .await?;
            direct_result(value, "point-in-time dataset published")
        }
    }
}

async fn query(
    authority: CliAuthority<'_>,
    command: QueryCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        QueryCommand::Artifact {
            artifact_id,
            sha256,
            byte_count,
            media_type,
            offset,
            maximum_bytes,
        } => {
            let mut arguments = json_object(json!({
                "artifactId": artifact_id,
                "sha256": sha256,
                "byteCount": byte_count,
                "mediaType": media_type,
                "offset": offset,
                "maximumBytes": maximum_bytes,
            }))?;
            invoke(
                authority,
                "Analysis.ReadArtifact",
                &mut arguments,
                Some(1),
                "artifact chunk read",
            )
            .await
        }
        QueryCommand::Dataset {
            dataset,
            maximum_rows,
        } => {
            let mut arguments = json_object(json!({"dataset": dataset}))?;
            invoke(
                authority,
                "Research.GetHistory",
                &mut arguments,
                Some(maximum_rows),
                "dataset observations read",
            )
            .await
        }
        QueryCommand::Sql {
            dataset,
            statement,
            maximum_rows,
        } => {
            query::query_sql(
                authority.local_for("Research.QuerySql")?,
                &dataset,
                statement,
                maximum_rows,
            )
            .await
        }
    }
}

async fn feature(
    authority: CliAuthority<'_>,
    command: FeatureCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        FeatureCommand::List { after_dataset } => {
            let mut arguments = Map::new();
            if let Some(after_dataset) = after_dataset {
                arguments.insert("afterDataset".to_owned(), Value::String(after_dataset));
            }
            invoke(
                authority,
                "Analysis.GetFeatureDatasets",
                &mut arguments,
                None,
                "feature registry read",
            )
            .await
        }
        FeatureCommand::Build { request, confirm } => {
            let value = cli_dataset::build_point_in_time_dataset(
                authority.local_for("Analysis.BuildFeatureDataset")?,
                &request,
                confirm,
            )
            .await?;
            direct_result(value, "point-in-time feature dataset published")
        }
    }
}

async fn model(
    authority: CliAuthority<'_>,
    command: ModelCommand,
) -> Result<CliProductResult, CliProductError> {
    let (operation, mut arguments, summary) = match command {
        ModelCommand::List => ("Model.ListBundles", Map::new(), "model bundles listed"),
        ModelCommand::Admit { request, confirm } => {
            let value = cli_model::admit_model_bundle(
                authority.local_for("Model.Admit")?,
                &request,
                confirm,
            )?;
            return direct_result(value, "model bundle admitted");
        }
        ModelCommand::Metadata { model } => (
            "Model.GetMetadata",
            json_object(json!({"modelId": model}))?,
            "model metadata read",
        ),
        ModelCommand::Evaluate { request, confirm } => {
            let mut arguments = read_json_object(&request)?;
            arguments.insert("confirm".to_owned(), Value::Bool(confirm));
            ("Model.Evaluate", arguments, "model evaluation completed")
        }
        ModelCommand::Predict { request } => (
            "Model.Predict",
            read_json_object(&request)?,
            "model prediction completed",
        ),
    };
    invoke(authority, operation, &mut arguments, None, summary).await
}

async fn portfolio(
    authority: CliAuthority<'_>,
    command: PortfolioCommand,
) -> Result<CliProductResult, CliProductError> {
    let (operation, mut arguments, summary) = match command {
        PortfolioCommand::Import {
            path,
            account,
            confirm,
        } => {
            let value = cli_portfolio::import_portfolio_manifest(
                authority.local_for("Portfolio.Import")?,
                &path,
                account,
                confirm,
            )
            .await?;
            return Ok(CliProductResult {
                summary: "portfolio manifest imported",
                value,
            });
        }
        PortfolioCommand::Holdings { account } => (
            "Portfolio.GetHoldings",
            json_object(json!({"accountId": account}))?,
            "portfolio holdings read",
        ),
        PortfolioCommand::Transactions { account } => (
            "Portfolio.GetTransactions",
            json_object(json!({"accountId": account}))?,
            "portfolio transactions read",
        ),
        PortfolioCommand::Performance { request } => (
            "Portfolio.GetPerformance",
            read_json_object(&request)?,
            "portfolio performance calculated",
        ),
        PortfolioCommand::Exposure { request } => (
            "Portfolio.GetExposure",
            read_json_object(&request)?,
            "portfolio exposure calculated",
        ),
        PortfolioCommand::Risk { request } => (
            "Portfolio.GetRisk",
            read_json_object(&request)?,
            "portfolio risk calculated",
        ),
    };
    invoke(authority, operation, &mut arguments, None, summary).await
}

async fn backtest(
    authority: CliAuthority<'_>,
    command: BacktestCommand,
) -> Result<CliProductResult, CliProductError> {
    let (operation, mut arguments, summary) = match command {
        BacktestCommand::Run { request, confirm } => {
            authority.local_for("Analysis.RunBacktest")?;
            let value = cli_backtest::register_backtest_input(&request, confirm).await?;
            let arguments = json_object(value)?;
            ("Analysis.RunBacktest", arguments, "backtest completed")
        }
        BacktestCommand::Show { run } => (
            "Analysis.GetBacktests",
            json_object(json!({"runId": run}))?,
            "backtest result read",
        ),
    };
    invoke(authority, operation, &mut arguments, None, summary).await
}

async fn bot(
    authority: CliAuthority<'_>,
    command: BotCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        BotCommand::Status => {
            let mut arguments = Map::new();
            invoke(
                authority,
                "Bot.GetStatus",
                &mut arguments,
                None,
                "paper operation status read",
            )
            .await
        }
        BotCommand::Start { paper, confirm } => {
            let mut start_arguments = json_object(json!({
                "provider": match paper.provider {
                    crate::cli::ProductionSourceArgument::Coinbase => "coinbase",
                    crate::cli::ProductionSourceArgument::CoinbaseDirect => "coinbase-direct",
                    crate::cli::ProductionSourceArgument::Kraken => "kraken",
                },
                "initialCash": paper.initial_cash.to_string(),
                "feeBasisPoints": paper.fee_basis_points,
                "confirm": confirm,
            }))?;
            if let Some(provider_session_id) = paper.provider_session_id {
                start_arguments.insert(
                    "providerSessionId".to_owned(),
                    Value::String(provider_session_id.to_string()),
                );
            }
            let started = invoke(
                authority,
                "Bot.Start",
                &mut start_arguments,
                None,
                "paper operation started",
            )
            .await?;
            match paper.seconds {
                Some(seconds) => tokio::time::sleep(Duration::from_secs(seconds)).await,
                None => tokio::signal::ctrl_c()
                    .await
                    .map_err(|_| CliProductError::Signal)?,
            }
            let mut stop_arguments = json_object(json!({
                "reason": "CLI stop condition reached",
                "confirm": true,
            }))?;
            let stopped = invoke(
                authority,
                "Bot.Stop",
                &mut stop_arguments,
                None,
                "paper operation stopped",
            )
            .await?;
            Ok(CliProductResult {
                summary: "paper operation completed",
                value: json!({"start": started.value, "stop": stopped.value}),
            })
        }
        BotCommand::Stop { reason, confirm } => {
            let mut arguments = json_object(json!({"reason": reason, "confirm": confirm}))?;
            invoke(
                authority,
                "Bot.Stop",
                &mut arguments,
                None,
                "paper operation stopped",
            )
            .await
        }
    }
}

async fn execution(
    authority: CliAuthority<'_>,
    command: ExecutionCommand,
) -> Result<CliProductResult, CliProductError> {
    let (operation, mut arguments, summary) = match command {
        ExecutionCommand::Orders => ("Execution.GetOrders", Map::new(), "paper orders listed"),
        ExecutionCommand::Fills => ("Execution.GetFills", Map::new(), "paper fills listed"),
        ExecutionCommand::Cancel { order, confirm } => (
            "Execution.Cancel",
            json_object(json!({"orderId": order, "confirm": confirm}))?,
            "paper order cancellation processed",
        ),
        ExecutionCommand::Reconcile { confirm } => (
            "Execution.Reconcile",
            json_object(json!({"confirm": confirm}))?,
            "paper execution reconciled",
        ),
    };
    invoke(authority, operation, &mut arguments, None, summary).await
}

async fn fair_value(
    authority: CliAuthority<'_>,
    command: FairValueCommand,
) -> Result<CliProductResult, CliProductError> {
    let (operation, mut arguments, summary) = match command {
        FairValueCommand::List => (
            "FairValue.ListMeasurements",
            Map::new(),
            "fair-value measurements listed",
        ),
        FairValueCommand::Measure { request, confirm } => {
            let mut arguments = read_json_object(&request)?;
            arguments.insert("confirm".to_owned(), Value::Bool(confirm));
            (
                "FairValue.Measure",
                arguments,
                "fair-value measurement created",
            )
        }
        FairValueCommand::Classify {
            measurement,
            confirm,
        } => (
            "FairValue.Classify",
            json_object(json!({"measurementId": measurement, "confirm": confirm}))?,
            "fair-value measurement classified",
        ),
        FairValueCommand::Explain { measurement } => (
            "FairValue.Explain",
            json_object(json!({"measurementId": measurement}))?,
            "fair-value classification explained",
        ),
        FairValueCommand::Evidence { measurement } => (
            "FairValue.GetEvidence",
            json_object(json!({"measurementId": measurement}))?,
            "fair-value evidence read",
        ),
        FairValueCommand::ApprovalStatus { measurement, at } => (
            "FairValue.GetApprovalStatus",
            json_object(json!({"measurementId": measurement, "at": at}))?,
            "fair-value approval status read",
        ),
        FairValueCommand::Approve {
            measurement,
            decision,
            reviewer,
            approved_at,
            expires_at,
            confirm,
        } => (
            "FairValue.Approve",
            json_object(json!({
                "measurementId": measurement,
                "decisionId": decision,
                "approvedBy": reviewer,
                "approvedAt": approved_at,
                "expiresAt": expires_at,
                "confirm": confirm,
            }))?,
            "fair-value measurement approved",
        ),
    };
    invoke(authority, operation, &mut arguments, None, summary).await
}

async fn job(
    authority: CliAuthority<'_>,
    command: JobCommand,
) -> Result<CliProductResult, CliProductError> {
    if matches!(authority, CliAuthority::Local(_)) {
        return Err(CliProductError::InstalledServiceRequired { operation: "Job" });
    }
    let (operation, arguments, summary) = match command {
        JobCommand::List {
            after_job_id,
            limit,
        } => (
            "Job.List",
            json!({
                "afterJobId": after_job_id.map(|value| value.to_string()),
                "limit": limit,
            }),
            "durable jobs listed",
        ),
        JobCommand::Get { job_id } => (
            "Job.Get",
            json!({"jobId": job_id.to_string()}),
            "durable job read",
        ),
        JobCommand::Watch {
            job_id,
            generation,
            after_sequence,
            limit,
        } => (
            "Job.Watch",
            json!({
                "jobId": job_id.to_string(),
                "generation": generation,
                "afterSequence": after_sequence,
                "limit": limit,
            }),
            "durable job events read",
        ),
        JobCommand::Cancel {
            job_id,
            generation,
            expected_sequence,
            confirm,
        } => {
            require_confirmation(confirm)?;
            (
                "Job.Cancel",
                json!({
                    "jobId": job_id.to_string(),
                    "generation": generation,
                    "expectedSequence": expected_sequence,
                    "confirm": true,
                }),
                "durable job cancellation requested",
            )
        }
        JobCommand::Confirm {
            job_id,
            generation,
            expected_sequence,
            confirmation_identity,
            evidence_sha256,
            confirm,
        } => {
            require_confirmation(confirm)?;
            (
                "Job.Confirm",
                json!({
                    "jobId": job_id.to_string(),
                    "generation": generation,
                    "expectedSequence": expected_sequence,
                    "identity": confirmation_identity,
                    "digest": lowercase_sha256(&evidence_sha256)?,
                    "confirm": true,
                }),
                "durable job confirmation recorded",
            )
        }
        JobCommand::Retry {
            job_id,
            generation,
            expected_sequence,
            confirm,
        } => {
            require_confirmation(confirm)?;
            (
                "Job.Retry",
                json!({
                    "jobId": job_id.to_string(),
                    "generation": generation,
                    "expectedSequence": expected_sequence,
                    "confirm": true,
                }),
                "durable job retry requested",
            )
        }
    };
    invoke_without_result_limits(authority, operation, arguments, summary).await
}

async fn operations(
    authority: CliAuthority<'_>,
    command: OperationsCommand,
) -> Result<CliProductResult, CliProductError> {
    require_installed(authority, "Operations")?;
    match command {
        OperationsCommand::Backup { command } => backup_operations(authority, command).await,
        OperationsCommand::Workspace { command } => workspace_operations(authority, command).await,
        OperationsCommand::Update { command } => update_operations(authority, command).await,
        OperationsCommand::Logs { command } => log_operations(authority, command).await,
        OperationsCommand::Settings { command } => settings_operations(authority, command).await,
    }
}

async fn backup_operations(
    authority: CliAuthority<'_>,
    command: BackupOperationsCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        BackupOperationsCommand::List {
            after_backup_id,
            limit,
        } => {
            let mut arguments = json_object(json!({"limit": limit}))?;
            if let Some(after_backup_id) = after_backup_id {
                arguments.insert(
                    "afterBackupId".to_owned(),
                    Value::String(lowercase_sha256(&after_backup_id)?.to_owned()),
                );
            }
            invoke(
                authority,
                "Operations.ListBackups",
                &mut arguments,
                Some(usize::from(limit)),
                "product backups listed",
            )
            .await
        }
        BackupOperationsCommand::Get { backup_id } => {
            let mut arguments = backup_identity_arguments(backup_id)?;
            invoke(
                authority,
                "Operations.GetBackup",
                &mut arguments,
                Some(1),
                "product backup manifest read",
            )
            .await
        }
        BackupOperationsCommand::Create { confirm } => {
            require_confirmation(confirm)?;
            let mut arguments = json_object(json!({"confirm": true}))?;
            invoke(
                authority,
                "Operations.StartBackup",
                &mut arguments,
                Some(1),
                "product backup job started",
            )
            .await
        }
        BackupOperationsCommand::Verify { backup_id, confirm } => {
            require_confirmation(confirm)?;
            let mut arguments = backup_identity_arguments(backup_id)?;
            arguments.insert("confirm".to_owned(), Value::Bool(true));
            invoke(
                authority,
                "Operations.StartBackupVerification",
                &mut arguments,
                Some(1),
                "product backup verification job started",
            )
            .await
        }
        BackupOperationsCommand::Retention { command } => match command {
            BackupRetentionCommand::Preview { keep_latest } => {
                let mut arguments = json_object(json!({"keepLatest": keep_latest}))?;
                invoke(
                    authority,
                    "Operations.PreviewBackupRetention",
                    &mut arguments,
                    None,
                    "backup retention previewed",
                )
                .await
            }
            BackupRetentionCommand::Apply(preview) => {
                let mut arguments = operations_preview_arguments(preview)?;
                invoke(
                    authority,
                    "Operations.StartBackupRetention",
                    &mut arguments,
                    Some(1),
                    "backup retention job started",
                )
                .await
            }
        },
        BackupOperationsCommand::Restore { command } => match command {
            RestoreCommand::Preview { backup_id } => {
                let mut arguments = backup_identity_arguments(backup_id)?;
                invoke(
                    authority,
                    "Operations.PreviewRestore",
                    &mut arguments,
                    None,
                    "product restore previewed",
                )
                .await
            }
            RestoreCommand::Start(preview) => {
                let mut arguments = operations_preview_arguments(preview)?;
                invoke(
                    authority,
                    "Operations.StartRestore",
                    &mut arguments,
                    Some(1),
                    "product restore job started",
                )
                .await
            }
        },
    }
}

async fn workspace_operations(
    authority: CliAuthority<'_>,
    command: WorkspaceOperationsCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        WorkspaceOperationsCommand::List {
            after_workspace_id,
            limit,
        } => {
            let mut arguments = json_object(json!({"limit": limit}))?;
            if let Some(after_workspace_id) = after_workspace_id {
                arguments.insert(
                    "afterWorkspaceId".to_owned(),
                    Value::String(after_workspace_id.to_string()),
                );
            }
            invoke(
                authority,
                "Operations.ListWorkspaces",
                &mut arguments,
                Some(usize::from(limit)),
                "local workspaces listed",
            )
            .await
        }
        WorkspaceOperationsCommand::Switch { command } => match command {
            WorkspaceSwitchCommand::Preview { workspace_id } => {
                let mut arguments = json_object(json!({"workspaceId": workspace_id.to_string()}))?;
                invoke(
                    authority,
                    "Operations.PreviewWorkspaceSwitch",
                    &mut arguments,
                    None,
                    "workspace switch previewed",
                )
                .await
            }
            WorkspaceSwitchCommand::Start(preview) => {
                let mut arguments = operations_preview_arguments(preview)?;
                invoke(
                    authority,
                    "Operations.StartWorkspaceSwitch",
                    &mut arguments,
                    Some(1),
                    "workspace switch job started",
                )
                .await
            }
        },
    }
}

async fn update_operations(
    authority: CliAuthority<'_>,
    command: UpdateOperationsCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        UpdateOperationsCommand::Status => {
            let mut arguments = Map::new();
            invoke(
                authority,
                "Operations.GetUpdateStatus",
                &mut arguments,
                Some(1),
                "trusted update status read",
            )
            .await
        }
        UpdateOperationsCommand::Check { confirm } => {
            require_confirmation(confirm)?;
            let mut arguments = json_object(json!({"confirm": true}))?;
            invoke(
                authority,
                "Operations.CheckForUpdates",
                &mut arguments,
                None,
                "trusted update candidate checked and staged",
            )
            .await
        }
        UpdateOperationsCommand::Preview => {
            let mut arguments = Map::new();
            invoke(
                authority,
                "Operations.PreviewUpdate",
                &mut arguments,
                None,
                "trusted update activation previewed",
            )
            .await
        }
        UpdateOperationsCommand::Start(preview) => {
            let mut arguments = operations_preview_arguments(preview)?;
            invoke(
                authority,
                "Operations.StartUpdate",
                &mut arguments,
                Some(1),
                "trusted update job started",
            )
            .await
        }
        UpdateOperationsCommand::ProgramRollback { command } => match command {
            ProgramRollbackCommand::Preview => {
                let mut arguments = Map::new();
                invoke(
                    authority,
                    "Operations.PreviewProgramRollback",
                    &mut arguments,
                    None,
                    "program rollback previewed",
                )
                .await
            }
            ProgramRollbackCommand::Start(preview) => {
                let mut arguments = operations_preview_arguments(preview)?;
                invoke(
                    authority,
                    "Operations.StartProgramRollback",
                    &mut arguments,
                    Some(1),
                    "program rollback job started",
                )
                .await
            }
        },
    }
}

async fn log_operations(
    authority: CliAuthority<'_>,
    command: LogOperationsCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        LogOperationsCommand::Query(query) => {
            let maximum_items = usize::from(query.limit);
            let mut arguments = log_query_arguments(query)?;
            invoke(
                authority,
                "Operations.QueryLogs",
                &mut arguments,
                Some(maximum_items),
                "structured logs queried",
            )
            .await
        }
        LogOperationsCommand::Export { query, confirm } => {
            require_confirmation(confirm)?;
            let mut arguments = log_query_arguments(query)?;
            arguments.insert("confirm".to_owned(), Value::Bool(true));
            invoke(
                authority,
                "Operations.ExportLogs",
                &mut arguments,
                Some(1),
                "redacted log export published",
            )
            .await
        }
    }
}

async fn settings_operations(
    authority: CliAuthority<'_>,
    command: SettingsOperationsCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        SettingsOperationsCommand::Get => {
            let mut arguments = Map::new();
            invoke(
                authority,
                "Operations.GetSettings",
                &mut arguments,
                None,
                "typed product settings read",
            )
            .await
        }
        SettingsOperationsCommand::Change { command } => match command {
            SettingsChangeCommand::Preview(change) => {
                let expected_revision = change.expected_revision;
                let changes = setting_values(change)?;
                let mut arguments = json_object(json!({
                    "expectedRevision": expected_revision,
                    "changes": changes,
                }))?;
                invoke(
                    authority,
                    "Operations.PreviewSettingsChange",
                    &mut arguments,
                    None,
                    "typed settings change previewed",
                )
                .await
            }
            SettingsChangeCommand::Apply(preview) => {
                let mut arguments = operations_preview_arguments(preview)?;
                invoke(
                    authority,
                    "Operations.ApplySettingsChange",
                    &mut arguments,
                    Some(1),
                    "typed settings change applied",
                )
                .await
            }
        },
        SettingsOperationsCommand::Rollback { command } => match command {
            SettingsRollbackCommand::Preview {
                expected_revision,
                target_revision,
            } => {
                let mut arguments = json_object(json!({
                    "expectedRevision": expected_revision,
                    "targetRevision": target_revision,
                }))?;
                invoke(
                    authority,
                    "Operations.PreviewSettingsRollback",
                    &mut arguments,
                    None,
                    "typed settings rollback previewed",
                )
                .await
            }
            SettingsRollbackCommand::Apply(preview) => {
                let mut arguments = operations_preview_arguments(preview)?;
                invoke(
                    authority,
                    "Operations.RollbackSettings",
                    &mut arguments,
                    Some(1),
                    "typed settings rollback applied",
                )
                .await
            }
        },
    }
}

async fn setup(
    authority: CliAuthority<'_>,
    command: SetupCommand,
) -> Result<CliProductResult, CliProductError> {
    require_installed(authority, "Setup")?;
    match command {
        SetupCommand::Status => {
            let mut arguments = Map::new();
            invoke(
                authority,
                "Setup.GetStatus",
                &mut arguments,
                None,
                "guided setup status read",
            )
            .await
        }
        SetupCommand::Preview(arguments) => setup_preview(authority, arguments).await,
        SetupCommand::Apply(arguments) => setup_apply(authority, arguments).await,
    }
}

async fn setup_preview(
    authority: CliAuthority<'_>,
    arguments: SetupPreviewArguments,
) -> Result<CliProductResult, CliProductError> {
    let goals = arguments
        .goals
        .into_iter()
        .map(setup_goal)
        .collect::<Vec<_>>();
    let selection = SetupPlanSelection::try_new(goals, setup_starter_plan(arguments.starter_plan))?;
    let mut operation_arguments = json_object(json!({
        "expectedRevision": arguments.expected_revision,
        "selection": selection,
    }))?;
    invoke(
        authority,
        "Setup.PreviewPlan",
        &mut operation_arguments,
        None,
        "guided setup plan previewed; no setup step completed",
    )
    .await
}

async fn setup_apply(
    authority: CliAuthority<'_>,
    arguments: SetupApplyArguments,
) -> Result<CliProductResult, CliProductError> {
    require_confirmation(arguments.confirm)?;
    if arguments.preview_id.is_nil() {
        return Err(CliProductError::RequestShape);
    }
    let preview_sha256 = lowercase_sha256(&arguments.preview_sha256)?;
    let mut operation_arguments = json_object(json!({
        "previewId": arguments.preview_id.to_string(),
        "previewSha256": preview_sha256,
        "confirm": true,
    }))?;
    invoke(
        authority,
        "Setup.ApplyPlan",
        &mut operation_arguments,
        Some(1),
        "guided setup plan accepted; capability steps remain evidence-driven",
    )
    .await
}

fn require_installed(
    authority: CliAuthority<'_>,
    operation: &'static str,
) -> Result<(), CliProductError> {
    if matches!(authority, CliAuthority::Installed(_)) {
        Ok(())
    } else {
        Err(CliProductError::InstalledServiceRequired { operation })
    }
}

fn backup_identity_arguments(backup_id: String) -> Result<Map<String, Value>, CliProductError> {
    json_object(json!({"backupId": lowercase_sha256(&backup_id)?}))
}

fn operations_preview_arguments(
    arguments: OperationsPreviewConfirmationArguments,
) -> Result<Map<String, Value>, CliProductError> {
    require_confirmation(arguments.confirm)?;
    if arguments.preview_id.is_nil() {
        return Err(CliProductError::RequestShape);
    }
    json_object(json!({
        "previewId": arguments.preview_id.to_string(),
        "previewDigest": lowercase_sha256(&arguments.preview_digest)?,
        "confirm": true,
    }))
}

fn log_query_arguments(
    arguments: LogQueryArguments,
) -> Result<Map<String, Value>, CliProductError> {
    let mut result = json_object(json!({"limit": arguments.limit}))?;
    insert_optional_string(&mut result, "from", arguments.from);
    insert_optional_string(&mut result, "through", arguments.through);
    if let Some(severity) = arguments.minimum_severity {
        result.insert(
            "minimumSeverity".to_owned(),
            serde_json::to_value(log_severity(severity))
                .map_err(|_| CliProductError::RequestShape)?,
        );
    }
    if let Some(domain) = arguments.domain {
        result.insert(
            "domain".to_owned(),
            serde_json::to_value(log_domain(domain)).map_err(|_| CliProductError::RequestShape)?,
        );
    }
    insert_optional_string(&mut result, "sourceId", arguments.source_id);
    insert_optional_string(&mut result, "jobId", arguments.job_id);
    insert_optional_string(&mut result, "correlationId", arguments.correlation_id);
    insert_optional_string(&mut result, "search", arguments.search);
    if let Some(after_sequence) = arguments.after_sequence {
        result.insert("afterSequence".to_owned(), json!(after_sequence));
    }
    Ok(result)
}

fn insert_optional_string(
    arguments: &mut Map<String, Value>,
    name: &'static str,
    value: Option<String>,
) {
    if let Some(value) = value {
        arguments.insert(name.to_owned(), Value::String(value));
    }
}

fn log_severity(value: LogSeverityArgument) -> LogSeverity {
    match value {
        LogSeverityArgument::Trace => LogSeverity::Trace,
        LogSeverityArgument::Debug => LogSeverity::Debug,
        LogSeverityArgument::Info => LogSeverity::Info,
        LogSeverityArgument::Warn => LogSeverity::Warn,
        LogSeverityArgument::Error => LogSeverity::Error,
    }
}

fn log_domain(value: LogDomainArgument) -> LogDomain {
    match value {
        LogDomainArgument::Application => LogDomain::Application,
        LogDomainArgument::Source => LogDomain::Source,
        LogDomainArgument::Market => LogDomain::Market,
        LogDomainArgument::Research => LogDomain::Research,
        LogDomainArgument::Portfolio => LogDomain::Portfolio,
        LogDomainArgument::Model => LogDomain::Model,
        LogDomainArgument::Backtest => LogDomain::Backtest,
        LogDomainArgument::Execution => LogDomain::Execution,
        LogDomainArgument::Risk => LogDomain::Risk,
        LogDomainArgument::FairValue => LogDomain::FairValue,
        LogDomainArgument::Mcp => LogDomain::Mcp,
        LogDomainArgument::Lifecycle => LogDomain::Lifecycle,
    }
}

fn setting_values(
    arguments: SettingsChangeArguments,
) -> Result<Vec<SettingValue>, CliProductError> {
    let mut values = Vec::new();
    if let Some(value) = arguments.log_retention_days {
        values.push(SettingValue::LogRetentionDays(value));
    }
    if let Some(value) = arguments.log_minimum_severity {
        values.push(SettingValue::LogMinimumSeverity(log_severity(value)));
    }
    if let Some(value) = arguments.update_channel {
        values.push(SettingValue::UpdateChannel(update_channel(value)));
    }
    if let Some(value) = arguments.automatic_update_checks {
        values.push(SettingValue::AutomaticUpdateChecks(value));
    }
    if let Some(value) = arguments.storage_soft_limit_bytes {
        values.push(SettingValue::StorageSoftLimitBytes(value));
    }
    if let Some(value) = arguments.default_query_row_limit {
        values.push(SettingValue::DefaultQueryRowLimit(value));
    }
    if let Some(value) = arguments.maximum_concurrent_jobs {
        values.push(SettingValue::MaximumConcurrentJobs(value));
    }
    if let Some(value) = arguments.market_freshness_millis {
        values.push(SettingValue::MarketFreshnessMillis(value));
    }
    if let Some(value) = arguments.backup_retention_count {
        values.push(SettingValue::BackupRetentionCount(value));
    }
    if values.is_empty() {
        Err(CliProductError::SettingsChangeRequired)
    } else {
        Ok(values)
    }
}

fn update_channel(value: UpdateChannelArgument) -> UpdateChannel {
    match value {
        UpdateChannelArgument::Stable => UpdateChannel::Stable,
        UpdateChannelArgument::Preview => UpdateChannel::Preview,
    }
}

fn setup_goal(value: SetupGoalArgument) -> SetupGoal {
    match value {
        SetupGoalArgument::EverythingRecommended => SetupGoal::EverythingRecommended,
        SetupGoalArgument::ExplorePublicMarkets => SetupGoal::ExplorePublicMarkets,
        SetupGoalArgument::ResearchInvestments => SetupGoal::ResearchInvestments,
        SetupGoalArgument::ManagePortfolio => SetupGoal::ManagePortfolio,
        SetupGoalArgument::BuildAndEvaluateModels => SetupGoal::BuildAndEvaluateModels,
        SetupGoalArgument::PracticePaperExecution => SetupGoal::PracticePaperExecution,
        SetupGoalArgument::UseClaudeCode => SetupGoal::UseClaudeCode,
        SetupGoalArgument::UseCodex => SetupGoal::UseCodex,
    }
}

fn setup_starter_plan(value: SetupStarterPlanArgument) -> SetupStarterPlan {
    match value {
        SetupStarterPlanArgument::EverythingRecommended => SetupStarterPlan::EverythingRecommended,
        SetupStarterPlanArgument::PublicMarkets => SetupStarterPlan::PublicMarkets,
        SetupStarterPlanArgument::Research => SetupStarterPlan::Research,
        SetupStarterPlanArgument::Portfolio => SetupStarterPlan::Portfolio,
        SetupStarterPlanArgument::Models => SetupStarterPlan::Models,
        SetupStarterPlanArgument::PaperPractice => SetupStarterPlan::PaperPractice,
        SetupStarterPlanArgument::AiClients => SetupStarterPlan::AiClients,
    }
}

fn require_confirmation(confirm: bool) -> Result<(), CliProductError> {
    if confirm {
        Ok(())
    } else {
        Err(CliProductError::ConfirmationRequired)
    }
}

fn lowercase_sha256(value: &str) -> Result<&str, CliProductError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(value)
    } else {
        Err(CliProductError::RequestShape)
    }
}

async fn invoke(
    authority: CliAuthority<'_>,
    operation: &str,
    arguments: &mut Map<String, Value>,
    maximum_items: Option<usize>,
    summary: &'static str,
) -> Result<CliProductResult, CliProductError> {
    let maximum_items = maximum_items.unwrap_or(CLI_DEFAULT_MAXIMUM_ITEMS);
    arguments.insert(
        "resultLimits".to_owned(),
        json!({
            "maximumItems": maximum_items,
            "maximumBytes": CLI_DEFAULT_MAXIMUM_BYTES,
        }),
    );
    invoke_without_result_limits(
        authority,
        operation,
        Value::Object(std::mem::take(arguments)),
        summary,
    )
    .await
}

async fn invoke_without_result_limits(
    authority: CliAuthority<'_>,
    operation: &str,
    arguments: Value,
    summary: &'static str,
) -> Result<CliProductResult, CliProductError> {
    let value = match authority {
        CliAuthority::Local(product) => {
            let Value::Object(arguments) = arguments else {
                return Err(CliProductError::RequestShape);
            };
            let result = product
                .application()
                .invoke(operation, arguments, request_context()?)
                .await?;
            result_envelope(&result)
        }
        CliAuthority::Installed(client) => {
            let context = request_context()?;
            let response = client
                .invoke_operation(
                    context.request_id().clone(),
                    operation,
                    arguments,
                    CLI_INSTALLED_REQUEST_TIMEOUT,
                    CancellationToken::new(),
                )
                .await?;
            unwrap_application_result(response.result())?
        }
    };
    Ok(CliProductResult { summary, value })
}

fn unwrap_application_result(result: &Value) -> Result<Value, CliProductError> {
    let object = result.as_object().ok_or(CliProductError::RuntimeRequest)?;
    match object.get("ok").and_then(Value::as_bool) {
        Some(true) => object
            .get("value")
            .cloned()
            .ok_or(CliProductError::RuntimeRequest),
        Some(false) => match object.get("error").and_then(Value::as_str) {
            Some("rejected") => Err(CliProductError::Client(ApplicationClientError::Rejected)),
            Some("interrupted") => {
                Err(CliProductError::Client(ApplicationClientError::Interrupted))
            }
            Some("unavailable") => {
                Err(CliProductError::Client(ApplicationClientError::Unavailable))
            }
            _ => Err(CliProductError::RuntimeRequest),
        },
        None => Err(CliProductError::RuntimeRequest),
    }
}

fn direct_result(value: Value, summary: &'static str) -> Result<CliProductResult, CliProductError> {
    let context = request_context()?;
    let result = TypedToolResult::try_new(
        value,
        1,
        ToolResultMetadata::complete_not_applicable(),
        context.limits(),
    )
    .map_err(|_| CliProductError::Limits)?;
    Ok(CliProductResult {
        summary,
        value: result_envelope(&result),
    })
}

fn request_context() -> Result<RequestContext, CliProductError> {
    let structure = JsonStructureLimits::try_new(32, 1024 * 1024, 100_000, 10_000)
        .map_err(|_| CliProductError::Limits)?;
    let limits = ServiceLimits::try_new(
        CLI_DEFAULT_MAXIMUM_BYTES,
        CLI_DEFAULT_MAXIMUM_ITEMS,
        CLI_HARD_MAXIMUM_BYTES,
        100_000,
        structure,
    )
    .map_err(|_| CliProductError::Limits)?;
    let request_id = RequestId::try_string(format!("cli-{}", uuid::Uuid::new_v4()))
        .map_err(|_| CliProductError::Limits)?;
    let deadline = Instant::now()
        .checked_add(CLI_REQUEST_TIMEOUT)
        .ok_or(CliProductError::Limits)?;
    Ok(RequestContext::new(
        request_id,
        CancellationToken::new(),
        deadline,
        limits,
    ))
}

fn source_filter(provider: Option<String>) -> Map<String, Value> {
    let mut arguments = Map::new();
    if let Some(provider) = provider {
        arguments.insert("sourceCoverage".to_owned(), json!([provider]));
    }
    arguments
}

fn result_envelope(result: &TypedToolResult) -> Value {
    let metadata = result.metadata();
    json!({
        "data": result.structured_content(),
        "metadata": {
            "completeness": metadata.completeness(),
            "returnedItems": result.item_count(),
            "availableItems": metadata.available_items().unwrap_or(result.item_count()),
            "sourceCoverage": metadata.source_coverage(),
            "dataQuality": metadata.data_quality(),
            "sourceEvidence": metadata.source_evidence(),
        },
        "encodedBytes": result.encoded_bytes(),
    })
}

fn read_json_object(path: &Path) -> Result<Map<String, Value>, CliProductError> {
    let input = read_bounded_input(path)?;
    serde_json::from_slice::<Value>(input.as_bytes())
        .map_err(|_| CliProductError::RequestShape)?
        .as_object()
        .cloned()
        .ok_or(CliProductError::RequestShape)
}

fn read_bounded_input(
    path: &Path,
) -> Result<market_squawk_platform::BoundedInput, CliProductError> {
    let absolute = admitted_absolute_path(path)?;
    let parent = absolute.parent().ok_or(CliProductError::RequestFile)?;
    let name = absolute.file_name().ok_or(CliProductError::RequestFile)?;
    UserAuthorizedInputRoot::open(parent)
        .and_then(|root| root.resolve(PathBuf::from(name)))
        .and_then(|input| input.open_bounded(CLI_JSON_MAXIMUM_BYTES))
        .and_then(|input| input.read_bounded())
        .map_err(|_| CliProductError::RequestFile)
}

fn admitted_absolute_path(path: &Path) -> Result<PathBuf, CliProductError> {
    let absolute = std::path::absolute(path).map_err(|_| CliProductError::RequestFile)?;
    if absolute
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(CliProductError::RequestFile);
    }
    Ok(absolute)
}

fn json_object(value: Value) -> Result<Map<String, Value>, CliProductError> {
    value
        .as_object()
        .cloned()
        .ok_or(CliProductError::RequestShape)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}
