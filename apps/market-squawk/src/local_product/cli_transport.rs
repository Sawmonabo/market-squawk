//! Bounded CLI transport over the same application operations exposed through MCP.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use market_squawk_data::{AnalyticalReadError, QueryError};
use market_squawk_platform::UserAuthorizedInputRoot;
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ServiceLimits, ToolResultMetadata,
    TypedToolResult,
};
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{LocalProduct, cli_dataset, cli_model, cli_portfolio};
use crate::cli::{
    BacktestCommand, BotCommand, Command, DatasetCommand, ExecutionCommand, FairValueCommand,
    FeatureCommand, IngestCommand, ModelCommand, PortfolioCommand, QueryCommand, SourceCommand,
};

mod files;
mod query;

const CLI_REQUEST_TIMEOUT: Duration = Duration::from_secs(5 * 60);
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
    /// A point-in-time dataset build failed admission or publication.
    #[error("{0}")]
    Dataset(#[from] cli_dataset::CliDatasetError),
    /// A controlled portfolio manifest import failed.
    #[error("{0}")]
    Portfolio(#[from] cli_portfolio::CliPortfolioImportError),
    /// A production model bundle failed closed admission.
    #[error("{0}")]
    ModelAdmission(#[from] cli_model::CliModelAdmissionError),
    /// CLI-owned request limits are invalid.
    #[error("CLI request limits are invalid")]
    Limits,
    /// The shared application rejected or failed the operation.
    #[error("application operation failed: {0}")]
    Application(#[from] market_squawk_services::ServiceError),
    /// The paper command could not observe its requested stop signal.
    #[error("failed to wait for the paper-operation stop condition")]
    Signal,
}

/// Executes one product command through shared application authority.
pub async fn execute_cli_command(
    product: &LocalProduct,
    command: Command,
) -> Result<CliProductResult, CliProductError> {
    match command {
        Command::Source { command } => source(product, command).await,
        Command::Ingest { command } => ingest(product, command).await,
        Command::Dataset { command } => dataset(product, command).await,
        Command::Query { command } => query(product, command).await,
        Command::Feature { command } => feature(product, command).await,
        Command::Model { command } => model(product, command).await,
        Command::Portfolio { command } => portfolio(product, command).await,
        Command::Backtest { command } => backtest(product, command).await,
        Command::Bot { command } => bot(product, command).await,
        Command::Execution { command } => execution(product, command).await,
        Command::FairValue { command } => fair_value(product, command).await,
        Command::Init
        | Command::Config { .. }
        | Command::Capture(_)
        | Command::Mcp { .. }
        | Command::Doctor
        | Command::Mock(_)
        | Command::PaperBot(_)
        | Command::Replay(_) => Err(CliProductError::WrongCommand),
    }
}

async fn source(
    product: &LocalProduct,
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
    };
    invoke(product, operation, &mut arguments, None, summary).await
}

async fn ingest(
    product: &LocalProduct,
    command: IngestCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        IngestCommand::Source {
            provider,
            object,
            dataset,
            confirm,
        } => {
            let mut arguments = json_object(json!({
                "provider": provider,
                "object": object,
                "dataset": dataset,
                "confirm": confirm,
                "sourceCoverage": [provider],
            }))?;
            invoke(
                product,
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
        } => files::ingest_local_file(product, &manifest, object, dataset, confirm).await,
    }
}

async fn dataset(
    product: &LocalProduct,
    command: DatasetCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        DatasetCommand::List => {
            let mut arguments = Map::new();
            invoke(
                product,
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
                product,
                "Research.GetManifest",
                &mut arguments,
                None,
                "dataset manifest read",
            )
            .await
        }
        DatasetCommand::Build { request, confirm } => {
            let value =
                cli_dataset::build_point_in_time_dataset(product, &request, confirm).await?;
            direct_result(value, "point-in-time dataset published")
        }
    }
}

async fn query(
    product: &LocalProduct,
    command: QueryCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        QueryCommand::Dataset {
            dataset,
            maximum_rows,
        } => {
            let mut arguments = json_object(json!({"dataset": dataset}))?;
            invoke(
                product,
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
        } => query::query_sql(product, &dataset, statement, maximum_rows).await,
    }
}

async fn feature(
    product: &LocalProduct,
    command: FeatureCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        FeatureCommand::List => {
            let mut arguments = Map::new();
            invoke(
                product,
                "Analysis.GetFeatureDatasets",
                &mut arguments,
                None,
                "feature registry read",
            )
            .await
        }
        FeatureCommand::Build { request, confirm } => {
            let value =
                cli_dataset::build_point_in_time_dataset(product, &request, confirm).await?;
            direct_result(value, "point-in-time feature dataset published")
        }
    }
}

async fn model(
    product: &LocalProduct,
    command: ModelCommand,
) -> Result<CliProductResult, CliProductError> {
    let (operation, mut arguments, summary) = match command {
        ModelCommand::List => ("Model.ListBundles", Map::new(), "model bundles listed"),
        ModelCommand::Admit { request, confirm } => {
            let value = cli_model::admit_model_bundle(product, &request, confirm)?;
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
    invoke(product, operation, &mut arguments, None, summary).await
}

async fn portfolio(
    product: &LocalProduct,
    command: PortfolioCommand,
) -> Result<CliProductResult, CliProductError> {
    let (operation, mut arguments, summary) = match command {
        PortfolioCommand::Import {
            path,
            account,
            confirm,
        } => {
            let value =
                cli_portfolio::import_portfolio_manifest(product, &path, account, confirm).await?;
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
    invoke(product, operation, &mut arguments, None, summary).await
}

async fn backtest(
    product: &LocalProduct,
    command: BacktestCommand,
) -> Result<CliProductResult, CliProductError> {
    let (operation, mut arguments, summary) = match command {
        BacktestCommand::Run { request, confirm } => {
            let mut arguments = read_json_object(&request)?;
            arguments.insert("confirm".to_owned(), Value::Bool(confirm));
            ("Analysis.RunBacktest", arguments, "backtest completed")
        }
        BacktestCommand::Show { run } => (
            "Analysis.GetBacktests",
            json_object(json!({"runId": run}))?,
            "backtest result read",
        ),
    };
    invoke(product, operation, &mut arguments, None, summary).await
}

async fn bot(
    product: &LocalProduct,
    command: BotCommand,
) -> Result<CliProductResult, CliProductError> {
    match command {
        BotCommand::Status => {
            let mut arguments = Map::new();
            invoke(
                product,
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
                    crate::cli::ProductionSourceArgument::Kraken => "kraken",
                },
                "initialCash": paper.initial_cash.to_string(),
                "feeBasisPoints": paper.fee_basis_points,
                "confirm": confirm,
            }))?;
            let started = invoke(
                product,
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
                product,
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
                product,
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
    product: &LocalProduct,
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
    invoke(product, operation, &mut arguments, None, summary).await
}

async fn fair_value(
    product: &LocalProduct,
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
    invoke(product, operation, &mut arguments, None, summary).await
}

async fn invoke(
    product: &LocalProduct,
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
    let application = product.application();
    let result = application
        .invoke(operation, std::mem::take(arguments), request_context()?)
        .await?;
    Ok(CliProductResult {
        summary,
        value: result_envelope(&result),
    })
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
