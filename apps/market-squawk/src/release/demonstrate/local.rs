//! Complete local CLI/application/MCP release demonstration.

#[path = "mcp.rs"]
mod mcp;

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use market_squawk_modeling::verify_application_training_environment;
use market_squawk_platform::{ConfigOverrides, ConfigSources};
use market_squawk_services::ServiceError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::application::application_capabilities;
use crate::cli::{
    BotCommand, Command, DatasetCommand, ExecutionCommand, FairValueCommand, IngestCommand,
    ModelCommand, PortfolioCommand, QueryCommand, SourceCommand,
};
use crate::doctor;
use crate::local_product::{CliProductError, execute_cli_command};
use crate::{AppConfig, LocalProduct};

const ACCOUNT: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const INSTRUMENT: &str = "11111111-1111-4111-8111-111111111111";
const PORTFOLIO_FIXTURE: &[u8] = include_bytes!(
    "../../../../../adapters/market-squawk-adapter-portfolio/fixtures/manifest.json"
);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LocalApplicationEvidence {
    pub(super) capabilities: CapabilityEvidence,
    pub(super) training_environment_admitted: bool,
    pub(super) model_runtime_composed: bool,
    pub(super) cli: CliEvidence,
    pub(super) doctor: DoctorEvidence,
    pub(super) mcp: mcp::McpEvidence,
    pub(super) completed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CapabilityEvidence {
    descriptor_contract_valid: bool,
    domains: Vec<String>,
    tool_count: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CliEvidence {
    local_file_ingest: bool,
    dataset_manifest: bool,
    datafusion_query: bool,
    source_reads: bool,
    model_registry: bool,
    portfolio_import: bool,
    portfolio_analytics: bool,
    fair_value_no_level1_promotion: bool,
    bot_status: bool,
    stopped_execution_fail_closed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DoctorEvidence {
    local_storage_unmodified: bool,
    remote_exporter_disabled: bool,
    arbitrary_artifact_path_access_disabled: bool,
    application_descriptor_valid: bool,
    required_domains_complete: bool,
    mcp_descriptor_valid: bool,
}

pub(super) async fn run(
    config: AppConfig,
    scratch: &Path,
    python_directory: &Path,
) -> Result<LocalApplicationEvidence> {
    let (training_root, training_environment_admitted) = verify_training_matrix(python_directory)?;
    let isolated = isolated_config(config, scratch.join("product"), training_root)?;
    let product = LocalProduct::try_new(isolated.clone())
        .context("complete local product composition failed")?;
    let model_runtime_composed = product.model_runtime().is_some();
    if !model_runtime_composed {
        bail!("signed training release did not compose the production model runtime");
    }

    let capabilities =
        application_capabilities().context("application capability descriptor is invalid")?;
    let expected_names = capabilities
        .tools()
        .iter()
        .map(|tool| tool.name().to_owned())
        .collect::<Vec<_>>();
    let domains = capabilities
        .tools()
        .iter()
        .map(|tool| tool.contract().domain().as_str().to_owned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let capability_evidence = CapabilityEvidence {
        descriptor_contract_valid: true,
        domains,
        tool_count: expected_names.len(),
    };

    let cli = run_cli_vertical(&product, scratch).await?;
    let doctor = doctor_evidence(&isolated).await?;
    let mcp = mcp::run(&product, &expected_names).await?;
    Ok(LocalApplicationEvidence {
        capabilities: capability_evidence,
        training_environment_admitted,
        model_runtime_composed,
        cli,
        doctor,
        mcp,
        completed: true,
    })
}

async fn run_cli_vertical(product: &LocalProduct, scratch: &Path) -> Result<CliEvidence> {
    let local_manifest = write_local_manifest(&scratch.join("local-input"))?;
    let ingested = execute_cli_command(
        product,
        Command::Ingest {
            command: IngestCommand::File {
                manifest: local_manifest,
                object: "release-price-object".to_owned(),
                dataset: "release-alternative-prices".to_owned(),
                confirm: true,
            },
        },
    )
    .await
    .context("release local-file ingestion failed")?;
    let local_file_ingest = ingested
        .value()
        .pointer("/data/rowCount")
        .and_then(Value::as_u64)
        == Some(1);
    if !local_file_ingest {
        bail!("release local-file ingestion did not publish exactly one row");
    }
    execute_cli_command(
        product,
        Command::Dataset {
            command: DatasetCommand::List {
                after_dataset: None,
            },
        },
    )
    .await
    .context("release dataset listing failed")?;
    let manifest = execute_cli_command(
        product,
        Command::Dataset {
            command: DatasetCommand::Manifest {
                dataset: "release-alternative-prices".to_owned(),
            },
        },
    )
    .await
    .context("release dataset manifest read failed")?;
    let dataset_manifest = contains_string(manifest.value(), "release-alternative-prices");
    let query = execute_cli_command(
        product,
        Command::Query {
            command: QueryCommand::Sql {
                dataset: "release-alternative-prices".to_owned(),
                statement: "SELECT * FROM dataset ORDER BY source_identifier".to_owned(),
                maximum_rows: 16,
            },
        },
    )
    .await
    .context("release DataFusion query failed")?;
    let datafusion_query = query
        .value()
        .pointer("/data/rows")
        .and_then(Value::as_array)
        .is_some_and(|rows| rows.len() == 1);
    if !dataset_manifest || !datafusion_query {
        bail!("release dataset manifest or DataFusion query evidence is incomplete");
    }

    for command in [
        SourceCommand::Status { provider: None },
        SourceCommand::Coverage { provider: None },
        SourceCommand::Health { provider: None },
    ] {
        execute_cli_command(product, Command::Source { command })
            .await
            .context("release source read failed")?;
    }
    let model = execute_cli_command(
        product,
        Command::Model {
            command: ModelCommand::List,
        },
    )
    .await
    .context("release model registry read failed")?;
    let model_registry = model.value().get("data").is_some();

    let portfolio_manifest = write_portfolio_manifest(&scratch.join("portfolio-input"))?;
    let imported = execute_cli_command(
        product,
        Command::Portfolio {
            command: PortfolioCommand::Import {
                path: portfolio_manifest,
                account: ACCOUNT.to_owned(),
                confirm: true,
            },
        },
    )
    .await
    .context("release portfolio import failed")?;
    let portfolio_import = imported
        .value()
        .pointer("/data/accountId")
        .and_then(Value::as_str)
        == Some(ACCOUNT)
        && imported
            .value()
            .pointer("/data/rawEvidenceRetained")
            .and_then(Value::as_bool)
            == Some(true)
        && imported
            .value()
            .pointer("/data/reconciliationDiscrepancies")
            .and_then(Value::as_u64)
            == Some(0);
    let holdings = execute_cli_command(
        product,
        Command::Portfolio {
            command: PortfolioCommand::Holdings {
                account: ACCOUNT.to_owned(),
            },
        },
    )
    .await
    .context("release portfolio holdings read failed")?;
    let transactions = execute_cli_command(
        product,
        Command::Portfolio {
            command: PortfolioCommand::Transactions {
                account: ACCOUNT.to_owned(),
            },
        },
    )
    .await
    .context("release portfolio transactions read failed")?;
    let account_request = write_json(
        &scratch.join("portfolio-request.json"),
        &json!({"accountId": ACCOUNT}),
    )?;
    let mut portfolio_analytics = holdings
        .value()
        .pointer("/data")
        .and_then(Value::as_array)
        .is_some_and(|rows| !rows.is_empty())
        && transactions
            .value()
            .pointer("/data")
            .and_then(Value::as_array)
            .is_some_and(|rows| !rows.is_empty());
    for command in [
        PortfolioCommand::Performance {
            request: account_request.clone(),
        },
        PortfolioCommand::Exposure {
            request: account_request.clone(),
        },
        PortfolioCommand::Risk {
            request: account_request.clone(),
        },
    ] {
        let result = execute_cli_command(product, Command::Portfolio { command })
            .await
            .context("release portfolio analytical read failed")?;
        portfolio_analytics &= result
            .value()
            .pointer("/data")
            .is_some_and(|value| !value.is_null());
    }
    if !portfolio_import || !portfolio_analytics {
        bail!("release portfolio import or analytics evidence is incomplete");
    }

    let fair_value = fair_value_vertical(product, scratch).await?;
    let bot = execute_cli_command(
        product,
        Command::Bot {
            command: BotCommand::Status,
        },
    )
    .await
    .context("release bot status read failed")?;
    let bot_status = bot.value().pointer("/data/state").and_then(Value::as_str) == Some("stopped");
    let mut stopped_execution_fail_closed = true;
    for command in [
        ExecutionCommand::Orders,
        ExecutionCommand::Fills,
        ExecutionCommand::Reconcile { confirm: true },
    ] {
        if !matches!(
            execute_cli_command(product, Command::Execution { command }).await,
            Err(CliProductError::Application(ServiceError::Unavailable))
        ) {
            stopped_execution_fail_closed = false;
            break;
        }
    }
    if !bot_status || !stopped_execution_fail_closed {
        bail!("stopped paper execution authority did not fail closed");
    }
    Ok(CliEvidence {
        local_file_ingest,
        dataset_manifest,
        datafusion_query,
        source_reads: true,
        model_registry,
        portfolio_import,
        portfolio_analytics,
        fair_value_no_level1_promotion: fair_value,
        bot_status,
        stopped_execution_fail_closed,
    })
}

async fn fair_value_vertical(product: &LocalProduct, scratch: &Path) -> Result<bool> {
    let request = write_json(
        &scratch.join("fair-value-request.json"),
        &json!({
            "measurement": {
                "accountId": ACCOUNT,
                "instrumentId": INSTRUMENT,
                "amount": "250.005",
                "currency": "USD",
                "scale": 3,
                "measurementAt": "1970-01-01T00:00:00.000000100Z",
                "preparedAt": "1970-01-01T00:00:00.000000104Z",
                "preparedBy": "release-demo",
                "method": "market_approach",
                "producerSelections": [
                    {"producer": "portfolio", "significance": "significant"}
                ]
            }
        }),
    )?;
    let measured = execute_cli_command(
        product,
        Command::FairValue {
            command: FairValueCommand::Measure {
                request,
                confirm: true,
            },
        },
    )
    .await
    .context("release fair-value measurement failed")?;
    let measurement = measured
        .value()
        .pointer("/data/measurement/measurementId")
        .and_then(Value::as_str)
        .context("release fair-value measurement omitted its identity")?
        .to_owned();
    let hierarchy = measured
        .value()
        .pointer("/data/classification/hierarchy")
        .and_then(Value::as_str)
        .context("release fair-value measurement omitted its hierarchy")?;
    match hierarchy {
        "level_2" | "level_3" | "unclassified" => {}
        "level_1" => {
            bail!("portfolio-derived fair value was incorrectly promoted to Level 1");
        }
        _ => bail!("release fair-value measurement returned an unknown hierarchy"),
    }
    execute_cli_command(
        product,
        Command::FairValue {
            command: FairValueCommand::Classify {
                measurement: measurement.clone(),
                confirm: true,
            },
        },
    )
    .await
    .context("release fair-value classification failed")?;
    execute_cli_command(
        product,
        Command::FairValue {
            command: FairValueCommand::Explain {
                measurement: measurement.clone(),
            },
        },
    )
    .await
    .context("release fair-value explanation failed")?;
    execute_cli_command(
        product,
        Command::FairValue {
            command: FairValueCommand::Evidence { measurement },
        },
    )
    .await
    .context("release fair-value evidence read failed")?;
    Ok(true)
}

async fn doctor_evidence(config: &AppConfig) -> Result<DoctorEvidence> {
    let value = serde_json::to_value(
        doctor::inspect(config)
            .await
            .context("release doctor inspection failed")?,
    )
    .context("release doctor report serialization failed")?;
    let evidence = DoctorEvidence {
        local_storage_unmodified: value
            .pointer("/localStorage/modifiedByInspection")
            .and_then(Value::as_bool)
            == Some(false),
        remote_exporter_disabled: value
            .pointer("/tracing/remoteExporter")
            .and_then(Value::as_bool)
            == Some(false),
        arbitrary_artifact_path_access_disabled: value
            .pointer("/artifacts/arbitraryPathAccess")
            .and_then(Value::as_bool)
            == Some(false),
        application_descriptor_valid: value
            .pointer("/application/descriptorContractValid")
            .and_then(Value::as_bool)
            == Some(true),
        required_domains_complete: value
            .pointer("/application/requiredDomainsComplete")
            .and_then(Value::as_bool)
            == Some(true),
        mcp_descriptor_valid: value
            .pointer("/mcp/descriptorContractValid")
            .and_then(Value::as_bool)
            == Some(true),
    };
    if !evidence.local_storage_unmodified
        || !evidence.remote_exporter_disabled
        || !evidence.arbitrary_artifact_path_access_disabled
        || !evidence.application_descriptor_valid
        || !evidence.required_domains_complete
        || !evidence.mcp_descriptor_valid
    {
        bail!("release doctor report violated a local safety or descriptor predicate");
    }
    Ok(evidence)
}

fn verify_training_matrix(python_directory: &Path) -> Result<(PathBuf, bool)> {
    let application = std::env::current_exe().context("release executable path is unavailable")?;
    let worker = application.with_file_name(if cfg!(windows) {
        "market-squawk-onnx-worker.exe"
    } else {
        "market-squawk-onnx-worker"
    });
    let mut roots = Vec::new();
    for name in ["release-cp312", "release-cp313"] {
        let root = python_directory.join(name);
        let metadata = fs::symlink_metadata(&root)
            .with_context(|| format!("signed Python training root {name} is unavailable"))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("signed Python training root is not a real directory");
        }
        verify_application_training_environment(&root, &application, &worker)
            .with_context(|| format!("signed Python training root {name} failed admission"))?;
        roots.push(root);
    }
    let selected = roots
        .into_iter()
        .next()
        .context("signed Python training matrix is empty")?;
    Ok((selected, true))
}

pub(super) fn revalidate_training_matrix(python_directory: &Path) -> Result<()> {
    let (_selected, admitted) = verify_training_matrix(python_directory)?;
    if !admitted {
        bail!("signed Python training matrix failed revalidation");
    }
    Ok(())
}

fn isolated_config(
    config: AppConfig,
    data_dir: PathBuf,
    training_root: PathBuf,
) -> Result<AppConfig> {
    let mut overrides = ConfigOverrides::from(config);
    overrides.data_dir = Some(data_dir);
    overrides.training_release_root = Some(training_root);
    overrides.source_secret = None;
    overrides.coinbase = None;
    overrides.kraken = None;
    overrides.paper_bot_enabled = Some(false);
    AppConfig::load(ConfigSources::new(
        None,
        &BTreeMap::<OsString, OsString>::new(),
        overrides,
    ))
    .context("offline release application configuration is invalid")
}

fn write_local_manifest(root: &Path) -> Result<PathBuf> {
    fs::create_dir_all(root).context("release local input directory could not be created")?;
    fs::write(root.join("prices.csv"), b"id,value\nrow-1,123.45\n")
        .context("release local CSV could not be written")?;
    let root = fs::canonicalize(root).context("release local input root is unavailable")?;
    write_json(
        &root.join("manifest.json"),
        &json!({
            "schema_version": 3,
            "objects": [{
                "dataset": "release-alternative-prices",
                "object_id": "release-price-object",
                "path": "prices.csv",
                "format": {"kind": "csv", "delimiter": 44},
                "effective_at": 100,
                "published_at": 150,
                "revision": "release-price-revision-1",
                "revision_number": 1,
                "superseded_at": null,
                "record_time": {
                    "effective": {
                        "schema_version": 2,
                        "coordinate": {"precision": "exact_timestamp", "value": 100}
                    },
                    "published": {
                        "schema_version": 2,
                        "coordinate": {"precision": "exact_timestamp", "value": 150}
                    },
                    "superseded": null
                },
                "instrument_binding": {"kind": "unscoped"},
                "row_policy": {
                    "identity_field": "id",
                    "fields": [{
                        "source": "value",
                        "field": "price",
                        "decimal_scale": 2,
                        "unit": "USD"
                    }]
                }
            }]
        }),
    )
}

fn write_portfolio_manifest(root: &Path) -> Result<PathBuf> {
    fs::create_dir_all(root).context("release portfolio input directory could not be created")?;
    let fixture: Value =
        serde_json::from_slice(PORTFOLIO_FIXTURE).context("sealed portfolio fixture is invalid")?;
    let records = fixture
        .pointer("/records")
        .and_then(Value::as_array)
        .context("sealed portfolio fixture omitted records")?;
    write_json(
        &root.join("manifest.json"),
        &json!({
            "schema_version": 1,
            "dataset": "release-portfolio",
            "object_id": "release-portfolio-object",
            "effective_at_unix_nanos": 100,
            "effective_until_unix_nanos": null,
            "published_at_unix_nanos": 101,
            "available_at_unix_nanos": 102,
            "records": records,
        }),
    )
}

fn write_json(path: &Path, value: &Value) -> Result<PathBuf> {
    fs::write(path, serde_json::to_vec(value)?)
        .with_context(|| format!("release input {} could not be written", path.display()))?;
    fs::canonicalize(path).context("release input could not be canonicalized")
}

fn contains_string(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Array(values) => values.iter().any(|value| contains_string(value, expected)),
        Value::Object(values) => values
            .values()
            .any(|value| contains_string(value, expected)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}
