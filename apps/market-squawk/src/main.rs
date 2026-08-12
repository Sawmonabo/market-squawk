// Rust #159105: this macOS-only dev/test-link diagnostic is caused by the measured
// `__eh_frame` exceeding arm64 compact-unwind's 24-bit offset range. Release diagnostics remain
// enabled because this allowance is restricted to debug-assertion builds.
#![cfg_attr(all(target_os = "macos", debug_assertions), allow(linker_messages))]

use std::{
    ffi::OsString,
    io::{IsTerminal as _, Read as _},
    path::Path,
    process::{Command as ProcessCommand, Stdio},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use market_squawk::{
    AppConfig, AppPaths, DiagnosticEngine, DiagnosticEngineSnapshot, LocalProduct,
    ProductionSourceProvider,
    cli::{
        Cli, Command, ConfigCommand, McpCommand, OutputFormat, ProductionSourceArgument,
        ReleaseCommand, ReleaseEvidenceCommand, ServiceCommand, SourceCommand,
    },
    doctor,
    local_product::{execute_installed_cli_command, verified_installed_service_program},
    paper_bot::local_paper_bot,
    release::execute_release_command,
    replay::replay_coinbase_journal,
    service::InstalledServiceConnector,
    source::{MarketSource, coinbase::CoinbaseSource, mock::MockSource},
    source_supervisor::{
        SourceShutdownError, SourceShutdownOutcome, SourceSupervisor, SupervisedSourceTask,
    },
    termination::TerminationSignals,
};
use market_squawk_domain::{
    CaptureAuthorityIdentity, ConnectionGeneration, MetadataRevision, SourceId, SourceIdentifier,
};
use market_squawk_installer::{ProgramName, active_release_root_for_installed_program};
use market_squawk_mcp::{McpLimitSpec, McpLimits, McpStdioRelay};
use market_squawk_platform::{
    CaptureChannelLimits, CaptureProcessInfrastructureLimits, CaptureShutdownStatus,
    CaptureWorkerReapError, CaptureWorkerTermination, CaptureWriterPolicy, ConfigOverrides,
    ConfigSources, DiagnosticCaptureBundle, PendingCaptureWriter, SecretValue,
    initialize_capture_process_infrastructure, raw_capture_channel, spawn_capture_writer,
};
use market_squawk_runtime::NamedClient;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const APPLICATION_MAIN_STACK_BYTES: usize = 8 * 1024 * 1024;
const MAXIMUM_BOOTSTRAP_UNLOCK_BYTES: u64 = 4 * 1024;

fn main() -> Result<()> {
    let application = std::thread::Builder::new()
        .name("market-squawk-main".to_owned())
        .stack_size(APPLICATION_MAIN_STACK_BYTES)
        .spawn(run_application)
        .context("failed to start the Market Squawk application thread")?;
    application
        .join()
        .map_err(|_| anyhow!("the Market Squawk application thread terminated unexpectedly"))?
}

fn run_application() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(Box::pin(run()))
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    initialize_logging(&cli.log, cli.json_logs)?;
    let output = cli.output;
    let config_file = cli.config.clone();
    let installation_data_root = cli.installation_data_root.clone();
    let training_release_root = match cli.training_release_root {
        Some(root) => Some(root),
        None => {
            let executable =
                std::env::current_exe().context("failed to resolve the running CLI executable")?;
            active_release_root_for_installed_program(&executable, ProgramName::Cli)
                .context("failed to resolve the installed training runtime")?
        }
    };
    let cli_overrides = ConfigOverrides {
        data_dir: cli.data_dir,
        capture_queue_capacity: cli.capture_queue_capacity,
        capture_memory_ceiling_bytes: cli.capture_memory_ceiling_bytes,
        capture_destination_registry_memory_ceiling_bytes: cli
            .capture_destination_registry_memory_ceiling_bytes,
        source_shutdown_ms: cli.source_shutdown_ms,
        training_release_root,
        ..ConfigOverrides::default()
    };

    match cli.command {
        Command::Init => {
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            let product = LocalProduct::try_new(config)?;
            let initialization = (|| -> Result<()> {
                if let Some(path) = product
                    .paths()
                    .journal_initialization_file("coinbase-exchange")?
                    && !path.exists()
                {
                    product
                        .paths()
                        .open_journal_writer("coinbase-exchange")?
                        .flush()?;
                }
                Ok(())
            })();
            let application = product.application();
            let deadline = std::time::Instant::now()
                .checked_add(application.shutdown_timeout())
                .ok_or_else(|| anyhow!("application shutdown deadline overflow"))?;
            let shutdown = application.shutdown(deadline).await;
            match (initialization, shutdown.is_complete()) {
                (Ok(()), true) => {
                    println!("initialized {}", product.paths().root().display());
                }
                (Err(error), true) => return Err(error),
                (Ok(()), false) => {
                    anyhow::bail!("local application initialization shutdown was incomplete");
                }
                (Err(error), false) => {
                    return Err(error.context(
                        "local application initialization failed and shutdown was incomplete",
                    ));
                }
            }
        }
        Command::Config { command } => {
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            run_config_command(command, &config, output)?;
        }
        command @ (Command::Source { .. }
        | Command::Market { .. }
        | Command::Ingest { .. }
        | Command::Dataset { .. }
        | Command::Query { .. }
        | Command::Feature { .. }
        | Command::Model { .. }
        | Command::Portfolio { .. }
        | Command::Backtest { .. }
        | Command::Bot { .. }
        | Command::Execution { .. }
        | Command::FairValue { .. }
        | Command::Job { .. }
        | Command::Operations { .. }
        | Command::Setup { .. }) => {
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            run_product_command(config, command, output, installation_data_root.as_deref()).await?;
        }
        Command::Service { command } => {
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            run_service_command(
                command,
                &config,
                config_file.as_deref(),
                &cli.log,
                cli.json_logs,
                output,
                installation_data_root.as_deref(),
            )
            .await?;
        }
        Command::Doctor => {
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            run_doctor(config, output).await?;
        }
        Command::Mock(arguments) => {
            let product = arguments.product;
            let events = arguments.events;
            let paper_bot = arguments.paper_bot;
            let config = load_config(
                config_file.as_deref(),
                ConfigOverrides {
                    products: Some(vec![product.clone()]),
                    paper_bot_enabled: Some(paper_bot),
                    ..cli_overrides
                },
            )?;
            let source: Box<dyn MarketSource> = Box::new(MockSource::new(product, events));
            let disposition = run_source(config, source, RunMode::UntilSourceStops).await?;
            let snapshot = finish_run_source(disposition).await?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        Command::Capture(arguments) => {
            let products = arguments.products;
            let seconds = arguments.seconds;
            let paper_bot = arguments.paper_bot;
            let config = load_config(
                config_file.as_deref(),
                ConfigOverrides {
                    products: Some(products.clone()),
                    paper_bot_enabled: Some(paper_bot),
                    ..cli_overrides
                },
            )?;
            let source: Box<dyn MarketSource> = Box::new(CoinbaseSource::new(products));
            let mode = seconds.map_or(RunMode::UntilInterrupted, RunMode::ForDuration);
            let disposition = run_source(config, source, mode).await?;
            let snapshot = finish_run_source(disposition).await?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        Command::PaperBot(arguments) => {
            let provider = match arguments.provider {
                ProductionSourceArgument::Coinbase => ProductionSourceProvider::Coinbase,
                ProductionSourceArgument::Kraken => ProductionSourceProvider::Kraken,
                ProductionSourceArgument::CoinbaseDirect => {
                    return Err(anyhow!(
                        "Coinbase Direct requires `market-squawk bot start` so the exact \
                         provider-onboarding session and application authority are retained"
                    ));
                }
            };
            let seconds = arguments.seconds;
            let initial_cash = arguments.initial_cash;
            let fee_basis_points = arguments.fee_basis_points;
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            let composition = local_paper_bot(config, provider, initial_cash, fee_basis_points)?;
            let cancellation = CancellationToken::new();
            let runtime = composition.start(cancellation.clone()).await?;
            let primary = match seconds {
                Some(seconds) => {
                    tokio::time::sleep(Duration::from_secs(seconds)).await;
                    None
                }
                None => tokio::signal::ctrl_c()
                    .await
                    .err()
                    .map(|error| anyhow!(error).context("failed to listen for Ctrl-C")),
            };
            let shutdown = cancel_and_shutdown_paper_bot(
                &cancellation,
                primary,
                runtime.shutdown(),
                |shutdown| shutdown.is_complete(),
            )
            .await?;
            let paper = shutdown
                .paper()
                .as_ref()
                .map_err(|error| anyhow!(error.to_string()))?;
            println!(
                "paper bot stopped cleanly: sequence={}, orders={}, fills={}",
                paper.sequence(),
                paper.orders().len(),
                paper.fills().len()
            );
        }
        Command::Mcp { command } => {
            let Some(McpCommand::Serve { client }) = command else {
                anyhow::bail!("mcp serve requires --client claude-code or --client codex");
            };
            let termination = TerminationSignals::install()?;
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            let client = NamedClient::from(client);
            let transport =
                installed_service_connector(&config, installation_data_root.as_deref())?
                    .connect_mcp_relay(client)?;
            let relay = McpStdioRelay::try_new(
                client,
                transport,
                McpLimits::try_from(McpLimitSpec::default())?,
            )?;
            run_mcp_until_termination(relay, termination).await?;
        }
        Command::Release { command } => {
            let benchmark_worker = matches!(
                &command,
                ReleaseCommand::Evidence {
                    command: ReleaseEvidenceCommand::BenchmarkWorker(_)
                }
            );
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            let result = execute_release_command(config, command).await?;
            if benchmark_worker {
                println!("{}", serde_json::to_string(&result)?);
            } else {
                emit_result(output, "release operation completed", &result)?;
            }
        }
        Command::Replay(arguments) => {
            let source = arguments.source;
            let journal_format = arguments.journal_format;
            if source != "coinbase-exchange" {
                anyhow::bail!("decoded replay currently supports source=coinbase-exchange");
            }
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            let paths = AppPaths::for_read(config.data_dir().to_path_buf());
            let journal_path =
                paths.select_journal_for_read(&source, journal_format.map(Into::into))?;
            let replay = replay_coinbase_journal(
                journal_path,
                duration_millis_i64(config.stale_after())?,
                false,
            )?;
            println!("{}", serde_json::to_string_pretty(&replay)?);
        }
    }

    Ok(())
}

async fn cancel_and_shutdown_paper_bot<T, Shutdown, Complete>(
    cancellation: &CancellationToken,
    primary: Option<anyhow::Error>,
    shutdown: Shutdown,
    is_complete: Complete,
) -> Result<T>
where
    T: std::fmt::Debug,
    Shutdown: std::future::Future<Output = T>,
    Complete: FnOnce(&T) -> bool,
{
    cancellation.cancel();
    let outcome = shutdown.await;
    let cleanup_failure = (!is_complete(&outcome))
        .then(|| format!("production paper-bot shutdown was incomplete: {outcome:?}"));
    match (primary, cleanup_failure) {
        (Some(error), Some(cleanup)) => Err(error.context(cleanup)),
        (Some(error), None) => Err(error),
        (None, Some(cleanup)) => Err(anyhow!(cleanup)),
        (None, None) => Ok(outcome),
    }
}

fn finish_with_cleanup<T>(primary: Result<T>, cleanup_failure: Option<anyhow::Error>) -> Result<T> {
    match (primary, cleanup_failure) {
        (Ok(value), None) => Ok(value),
        (Ok(_value), Some(cleanup)) => Err(cleanup),
        (Err(error), None) => Err(error),
        (Err(error), Some(cleanup)) => {
            Err(error.context(format!("cleanup also failed: {cleanup:#}")))
        }
    }
}

async fn run_mcp_until_termination(
    relay: McpStdioRelay,
    mut termination: TerminationSignals,
) -> Result<()> {
    let cancellation = CancellationToken::new();
    let mut serving = Box::pin(relay.serve_stdio(cancellation.clone()));
    tokio::select! {
        result = &mut serving => {
            result?;
            Ok(())
        }
        signal = termination.wait() => {
            cancellation.cancel();
            let completion = serving.await.map(|_exit| ()).map_err(anyhow::Error::from);
            match signal {
                Ok(()) => completion,
                Err(error) => finish_with_cleanup(Err(error.into()), completion.err()),
            }
        }
    }
}

fn load_config(
    config_file: Option<&std::path::Path>,
    cli_overrides: ConfigOverrides,
) -> Result<AppConfig> {
    let mut environment = ConfigSources::process_environment();
    environment.remove(&OsString::from("MARKET_SQUAWK_LOG"));
    environment.remove(&OsString::from("MARKET_SQUAWK_EXTERNAL_NETWORK"));
    environment.remove(&OsString::from("MARKET_SQUAWK_PROVIDER_TERMS_ACCEPTED"));
    Ok(AppConfig::load(ConfigSources::new(
        config_file,
        &environment,
        cli_overrides,
    ))?)
}

fn run_config_command(
    command: ConfigCommand,
    config: &AppConfig,
    output: OutputFormat,
) -> Result<()> {
    let value = serde_json::to_value(config.redacted_view())?;
    match command {
        ConfigCommand::Show => emit_result(output, "effective configuration", &value),
        ConfigCommand::Validate => {
            let validated = serde_json::json!({
                "valid": true,
                "effective": value,
            });
            emit_result(output, "configuration is valid", &validated)
        }
    }
}

async fn run_product_command(
    config: AppConfig,
    command: Command,
    output: OutputFormat,
    installation_data_root: Option<&Path>,
) -> Result<()> {
    let opens_onboarding_portal = matches!(
        &command,
        Command::Source {
            command: SourceCommand::Setup { .. }
        }
    );
    let connector = installed_service_connector(&config, installation_data_root)?;
    let client = connector.connect(NamedClient::Cli, None)?;
    let result = execute_installed_cli_command(&client, command).await;
    let portal_outcome = match &result {
        Ok(result) if opens_onboarding_portal => {
            match emit_result(output, result.summary(), result.value()) {
                Ok(()) => hold_onboarding_portal(result.value()).await,
                Err(error) => Err(error),
            }
        }
        Ok(_) | Err(_) => Ok(()),
    };
    let result = match result {
        Ok(result) => portal_outcome.map(|()| result),
        Err(error) => Err(anyhow::Error::from(error)),
    };
    let result = result?;
    if opens_onboarding_portal {
        Ok(())
    } else {
        emit_result(output, result.summary(), result.value())
    }
}

async fn run_service_command(
    command: ServiceCommand,
    config: &AppConfig,
    config_file: Option<&std::path::Path>,
    log: &str,
    json_logs: bool,
    output: OutputFormat,
    installation_data_root: Option<&Path>,
) -> Result<()> {
    let (summary, value) = match command {
        ServiceCommand::Status => service_status(config, installation_data_root).await?,
        ServiceCommand::Start => {
            start_installed_service(
                config,
                config_file,
                log,
                json_logs,
                Duration::from_secs(15),
                installation_data_root,
            )
            .await?
        }
        ServiceCommand::Bootstrap {
            stdin,
            retry_after_foreground_keyring,
        } => {
            let connector = installed_service_connector(config, installation_data_root)?;
            let status = if retry_after_foreground_keyring {
                connector.bootstrap_retry_after_foreground_keyring().await?
            } else {
                connector
                    .bootstrap_unlock(read_bootstrap_unlock(stdin)?)
                    .await?
            };
            (
                "installed service bootstrap was accepted",
                serde_json::to_value(status)?,
            )
        }
    };
    emit_result(output, summary, &value)
}

fn installed_service_connector(
    config: &AppConfig,
    installation_data_root: Option<&Path>,
) -> Result<InstalledServiceConnector> {
    installation_data_root
        .map_or_else(
            || InstalledServiceConnector::try_new(config),
            |root| InstalledServiceConnector::try_new_at_installation_root(config, root),
        )
        .map_err(Into::into)
}

async fn service_status(
    config: &AppConfig,
    installation_data_root: Option<&Path>,
) -> Result<(&'static str, serde_json::Value)> {
    if let Ok(snapshot) = installed_service_snapshot(config, installation_data_root).await {
        return Ok(("installed service is ready", snapshot));
    }
    let connector = installed_service_connector(config, installation_data_root)?;
    let status = connector.bootstrap_status().await?;
    Ok((
        "installed service requires credential bootstrap",
        serde_json::json!({"status": "bootstrap_required", "bootstrap": status}),
    ))
}

fn read_bootstrap_unlock(explicit_stdin: bool) -> Result<SecretValue> {
    if explicit_stdin {
        let mut bytes = Vec::new();
        std::io::stdin()
            .take(MAXIMUM_BOOTSTRAP_UNLOCK_BYTES + 1)
            .read_to_end(&mut bytes)
            .context("failed to read the bounded bootstrap unlock from standard input")?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAXIMUM_BOOTSTRAP_UNLOCK_BYTES {
            anyhow::bail!("bootstrap unlock exceeds its input bound");
        }
        if bytes.last() == Some(&b'\n') {
            bytes.pop();
            if bytes.last() == Some(&b'\r') {
                bytes.pop();
            }
        }
        return SecretValue::from_utf8_bytes(bytes)
            .context("bootstrap unlock is empty, invalid UTF-8, or outside its secret bound");
    }
    if !std::io::stdin().is_terminal() {
        anyhow::bail!("bootstrap unlock requires a terminal or explicit --stdin");
    }
    let unlock = rpassword::prompt_password("Encrypted fallback unlock: ")
        .context("failed to read the no-echo bootstrap unlock")?;
    SecretValue::new(unlock).context("bootstrap unlock is empty or outside its secret bound")
}

async fn installed_service_snapshot(
    config: &AppConfig,
    installation_data_root: Option<&Path>,
) -> Result<serde_json::Value> {
    let connector = installed_service_connector(config, installation_data_root)?;
    let client = connector.connect(NamedClient::Cli, None)?;
    client.probe_ready(CancellationToken::new()).await?;
    let bootstrap = client.bootstrap(CancellationToken::new()).await?;
    Ok(serde_json::json!({
        "status": "ready",
        "bootstrap": bootstrap,
    }))
}

async fn start_installed_service(
    config: &AppConfig,
    config_file: Option<&std::path::Path>,
    log: &str,
    json_logs: bool,
    readiness_timeout: Duration,
    installation_data_root: Option<&Path>,
) -> Result<(&'static str, serde_json::Value)> {
    if let Ok(snapshot) = installed_service_snapshot(config, installation_data_root).await {
        return Ok(("installed service was already ready", snapshot));
    }

    let program = verified_installed_service_program()?;
    let mut command = ProcessCommand::new(program);
    command
        .arg("--data-dir")
        .arg(config.data_dir())
        .arg("--log")
        .arg(log)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(config_file) = config_file {
        command.arg("--config").arg(config_file);
    }
    if let Some(training_release_root) = config.training_release_root() {
        command
            .arg("--training-release-root")
            .arg(training_release_root);
    }
    if let Some(root) = installation_data_root {
        command.arg("--installation-data-root").arg(root);
    }
    if json_logs {
        command.arg("--json-logs");
    }
    let mut child = command
        .spawn()
        .context("failed to start the verified installed Market Squawk service")?;
    let deadline = tokio::time::Instant::now()
        .checked_add(readiness_timeout)
        .ok_or_else(|| anyhow!("installed-service readiness deadline overflow"))?;
    loop {
        if let Ok(snapshot) = installed_service_snapshot(config, installation_data_root).await {
            return Ok(("installed service started", snapshot));
        }
        let connector = installed_service_connector(config, installation_data_root)?;
        if let Ok(status) = connector.bootstrap_status().await {
            return Ok((
                "installed service started and requires credential bootstrap",
                serde_json::json!({"status": "bootstrap_required", "bootstrap": status}),
            ));
        }
        if let Some(status) = child
            .try_wait()
            .context("failed to inspect the installed-service child")?
        {
            anyhow::bail!("installed service exited before readiness with status {status}");
        }
        if tokio::time::Instant::now() >= deadline {
            child
                .kill()
                .context("installed service missed readiness and could not be terminated")?;
            reap_service_child(&mut child, Duration::from_secs(2)).await?;
            anyhow::bail!("installed service did not reach authenticated readiness in time");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn reap_service_child(child: &mut std::process::Child, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| anyhow!("installed-service reap deadline overflow"))?;
    loop {
        if child
            .try_wait()
            .context("failed to inspect the terminating installed service")?
            .is_some()
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("terminated installed service could not be reaped within its deadline");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn hold_onboarding_portal(result: &serde_json::Value) -> Result<()> {
    let portal_url = result
        .pointer("/data/portal/url")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("source setup omitted the local portal URL"))?;
    let parsed =
        url::Url::parse(portal_url).context("source setup returned an invalid portal URL")?;
    if parsed.scheme() != "http"
        || parsed.host_str() != Some("127.0.0.1")
        || parsed.port().is_none()
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        anyhow::bail!("source setup returned a portal URL outside the loopback trust boundary");
    }
    let lifetime_seconds = result
        .pointer("/data/portal/expiresInSeconds")
        .and_then(serde_json::Value::as_u64)
        .filter(|seconds| (30..=60 * 60).contains(seconds))
        .ok_or_else(|| anyhow!("source setup returned an invalid portal lifetime"))?;

    if let Err(error) = webbrowser::open(portal_url) {
        warn!(
            error = %error,
            portal_url,
            "could not launch the system browser; the bounded local portal remains available at the emitted URL"
        );
    }
    info!(
        portal_url,
        lifetime_seconds,
        "provider onboarding portal is active; press Ctrl-C after setup or wait for expiry"
    );
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.context("failed to observe the provider-onboarding stop signal")
        }
        () = tokio::time::sleep(Duration::from_secs(lifetime_seconds)) => Ok(()),
    }
}

async fn run_doctor(config: AppConfig, output: OutputFormat) -> Result<()> {
    let report = doctor::inspect(&config).await?;
    let summary = if report.is_ready() {
        "local readiness checks passed"
    } else {
        "local readiness is blocked"
    };
    let value = serde_json::to_value(report)?;
    emit_result(output, summary, &value)
}

fn emit_result(output: OutputFormat, human_summary: &str, value: &serde_json::Value) -> Result<()> {
    match output {
        OutputFormat::Human => {
            println!("{human_summary}");
            println!("{}", serde_json::to_string_pretty(value)?);
        }
        OutputFormat::Json => println!("{}", serde_json::to_string(value)?),
    }
    Ok(())
}

fn capture_identity(source: &str) -> Result<(CaptureAuthorityIdentity, uuid::Uuid)> {
    let source_id = SourceId::try_from(source)?;
    let revision = MetadataRevision::new(SourceIdentifier::try_from("app-stage1-v1")?);
    let connection_id = uuid::Uuid::new_v4();
    let session_text = format!("{source}-{connection_id}");
    let session = SourceIdentifier::try_from(session_text.as_str())?;
    let generation = ConnectionGeneration::new(1)?;
    Ok((
        CaptureAuthorityIdentity::new(source_id, revision, session, generation),
        connection_id,
    ))
}

fn duration_millis_i64(duration: Duration) -> Result<i64> {
    Ok(i64::try_from(duration.as_millis())?)
}

fn initialize_logging(filter: &str, json_logs: bool) -> Result<()> {
    let env_filter = EnvFilter::try_new(filter).context("invalid log filter")?;
    if json_logs {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::stderr)
            .json()
            .try_init()
            .map_err(|error| anyhow!("failed to initialize logging: {error}"))?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::stderr)
            .try_init()
            .map_err(|error| anyhow!("failed to initialize logging: {error}"))?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum RunMode {
    UntilSourceStops,
    UntilInterrupted,
    ForDuration(u64),
}

#[derive(Debug)]
enum RunSourceDisposition {
    Complete(DiagnosticEngineSnapshot),
    CapturePending {
        pending: PendingCaptureWriter<DiagnosticCaptureBundle>,
        shutdown: PipelineShutdownReport,
    },
}

#[derive(Debug)]
struct SourceEventShutdownReport {
    source: std::result::Result<SourceShutdownOutcome, SourceShutdownError>,
    event_join_failed: bool,
}

#[derive(Debug)]
struct PipelineShutdownReport {
    primary: Option<anyhow::Error>,
    source_event: SourceEventShutdownReport,
}

async fn finish_run_source(disposition: RunSourceDisposition) -> Result<DiagnosticEngineSnapshot> {
    match disposition {
        RunSourceDisposition::Complete(snapshot) => Ok(snapshot),
        RunSourceDisposition::CapturePending {
            mut pending,
            mut shutdown,
        } => {
            pending.wait_until_terminated().await;
            let capture_error = compose_deferred_capture_error(pending.try_reap());
            match compose_pipeline_error(&mut shutdown, Some(capture_error)) {
                Some(error) => Err(error),
                None => Err(anyhow!("raw capture shutdown deadline elapsed")),
            }
        }
    }
}

fn compose_deferred_capture_error(
    reaped: std::result::Result<Option<&CaptureWorkerTermination>, CaptureWorkerReapError>,
) -> anyhow::Error {
    match reaped {
        Ok(Some(termination)) => {
            anyhow!("raw capture shutdown deadline elapsed; final worker report: {termination:?}")
        }
        Ok(None) => anyhow!(
            "raw capture shutdown deadline elapsed; terminated worker retained no final report"
        ),
        Err(error) => anyhow!(error).context("failed to reap deferred capture worker"),
    }
}

async fn run_source(
    config: AppConfig,
    source: Box<dyn MarketSource>,
    mode: RunMode,
) -> Result<RunSourceDisposition> {
    let paths = AppPaths::prepare(config.data_dir())?;
    let source_name = match mode {
        RunMode::UntilSourceStops => "mock",
        RunMode::UntilInterrupted | RunMode::ForDuration(_) => "coinbase-exchange",
    };
    let (capture_identity, connection_id) = capture_identity(source_name)?;
    let process =
        initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
            config.capture_destination_registry_memory_ceiling_bytes(),
        ))?;
    let (publisher, mut capture_control, capture_writer) = raw_capture_channel(
        &process,
        CaptureChannelLimits::new(
            config.capture_queue_capacity(),
            config.capture_memory_ceiling_bytes(),
        ),
        DiagnosticCaptureBundle::new(capture_identity.clone()),
    )?;
    let journal = paths.open_journal_writer(source_name)?;
    let flush_batch = std::num::NonZeroUsize::new(256)
        .ok_or_else(|| anyhow!("capture flush batch invariant failed"))?;
    let writer_policy = CaptureWriterPolicy::try_new(flush_batch, config.capture_flush_interval())?;
    let stale_after_ms = duration_millis_i64(config.stale_after())?;
    let diagnostic_engine = Arc::new(RwLock::new(DiagnosticEngine::new(
        stale_after_ms,
        config.paper_bot_enabled(),
    )));
    let capture_handle = spawn_capture_writer(capture_writer, journal, writer_policy)?;
    if let Err(error) = capture_control.activate_initial() {
        let mut pending_capture = capture_handle.shutdown(config.capture_shutdown());
        pending_capture.wait_until_terminated().await;
        let reaped = pending_capture.try_reap();
        return match reaped {
            Ok(Some(_termination)) => Err(error.into()),
            Ok(None) => Err(anyhow!(error).context(
                "capture activation failed and its terminated writer retained no final report",
            )),
            Err(reap_error) => Err(anyhow!(error).context(format!(
                "capture activation failed and writer reap also failed: {reap_error}"
            ))),
        };
    }
    let (event_sender, mut event_receiver) = mpsc::channel(16_384);
    let diagnostic_engine_for_events = Arc::clone(&diagnostic_engine);
    let event_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(
            u64::try_from(stale_after_ms.max(250)).unwrap_or(1_000) / 2,
        ));
        loop {
            tokio::select! {
                event = event_receiver.recv() => {
                    match event {
                        Some(event) => diagnostic_engine_for_events.write().handle(event),
                        None => break,
                    }
                }
                _ = interval.tick() => {
                    diagnostic_engine_for_events
                        .write()
                        .refresh_staleness(chrono::Utc::now());
                }
            }
        }
    });

    let supervisor =
        SourceSupervisor::new(publisher, capture_control, capture_identity, connection_id);
    let mut source_task = SupervisedSourceTask::spawn(supervisor, source, event_sender);
    let mut source_outcome = None;
    let mut primary_error = None;

    match mode {
        RunMode::UntilSourceStops => source_outcome = Some(source_task.wait().await),
        RunMode::UntilInterrupted => {
            tokio::select! {
                result = source_task.wait() => source_outcome = Some(result),
                signal = tokio::signal::ctrl_c() => {
                    if let Err(error) = signal {
                        primary_error = Some(anyhow!(error).context("failed to listen for Ctrl-C"));
                    }
                }
            }
        }
        RunMode::ForDuration(seconds) => {
            tokio::select! {
                result = source_task.wait() => source_outcome = Some(result),
                _ = tokio::time::sleep(Duration::from_secs(seconds)) => {}
                signal = tokio::signal::ctrl_c() => {
                    if let Err(error) = signal {
                        primary_error = Some(anyhow!(error).context("failed to listen for Ctrl-C"));
                    }
                }
            }
        }
    }

    let mut pipeline_shutdown = shutdown_source_then_event(
        &mut source_task,
        source_outcome,
        primary_error,
        config.source_shutdown(),
        event_task,
    )
    .await;
    let mut pending_capture = capture_handle.shutdown(config.capture_shutdown());
    let shutdown_status = pending_capture.wait_until_deadline().await;
    if shutdown_status == CaptureShutdownStatus::DeadlineElapsed {
        return Ok(RunSourceDisposition::CapturePending {
            pending: pending_capture,
            shutdown: pipeline_shutdown,
        });
    }
    let capture_error = match pending_capture.try_reap() {
        Ok(Some(termination)) if termination.outcome().is_incomplete() => Some(anyhow!(
            "raw capture shutdown was incomplete: {termination:?}"
        )),
        Ok(Some(_termination)) => None,
        Ok(None) => Some(anyhow!(
            "terminated capture worker had no final report after its join handle was reaped"
        )),
        Err(error) => Some(anyhow!(error).context("failed to reap terminated capture worker")),
    };
    if let Some(error) = compose_pipeline_error(&mut pipeline_shutdown, capture_error) {
        return Err(error);
    }
    match pipeline_shutdown.source_event.source {
        Ok(SourceShutdownOutcome::Graceful) => {}
        Ok(SourceShutdownOutcome::AbortedAtDeadline) => {
            warn!(
                deadline_ms = config.source_shutdown().as_millis(),
                "source task was aborted and reaped at the shutdown deadline"
            );
        }
        Ok(SourceShutdownOutcome::TaskFailed(_)) | Err(_) => {
            return Err(anyhow!(
                "pipeline error composition omitted a source shutdown failure"
            ));
        }
    }

    let snapshot = diagnostic_engine.read().snapshot();
    info!(
        processed_events = snapshot.processed_events,
        "run completed"
    );
    Ok(RunSourceDisposition::Complete(snapshot))
}

fn compose_pipeline_error(
    shutdown: &mut PipelineShutdownReport,
    capture: Option<anyhow::Error>,
) -> Option<anyhow::Error> {
    let mut secondary = Vec::with_capacity(3);
    match &shutdown.source_event.source {
        Ok(SourceShutdownOutcome::Graceful | SourceShutdownOutcome::AbortedAtDeadline) => {}
        Ok(SourceShutdownOutcome::TaskFailed(failure)) => {
            secondary.push(format!(
                "source task failed ({:?}): {}",
                failure.kind(),
                failure.detail()
            ));
        }
        Err(error) => secondary.push(error.to_string()),
    }
    if shutdown.source_event.event_join_failed {
        secondary.push("event processor task failed while being joined".to_owned());
    }
    if let Some(error) = capture {
        secondary.push(format!("{error:#}"));
    }
    match (shutdown.primary.take(), secondary.is_empty()) {
        (Some(error), true) => Some(error),
        (Some(error), false) => Some(error.context(format!(
            "pipeline cleanup also failed: {}",
            secondary.join("; ")
        ))),
        (None, true) => None,
        (None, false) => Some(anyhow!(secondary.join("; "))),
    }
}

async fn shutdown_source_then_event(
    source_task: &mut SupervisedSourceTask,
    observed_source_outcome: Option<SourceShutdownOutcome>,
    primary: Option<anyhow::Error>,
    source_deadline: Duration,
    event_task: tokio::task::JoinHandle<()>,
) -> PipelineShutdownReport {
    let source_outcome = match observed_source_outcome {
        Some(outcome) => Ok(outcome),
        None => source_task.shutdown(source_deadline).await,
    };
    let event_join_failed = event_task.await.is_err();
    PipelineShutdownReport {
        primary,
        source_event: SourceEventShutdownReport {
            source: source_outcome,
            event_join_failed,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use async_trait::async_trait;
    use clap::Parser;
    use market_squawk::source::{CaptureContext, MarketSource, SourceRunOutcome};
    use market_squawk::source_supervisor::{
        SourceShutdownOutcome, SourceSupervisor, SupervisedSourceTask,
    };
    use market_squawk_platform::{
        AppConfig, CaptureChannelLimits, CaptureProcessInfrastructureLimits, CaptureShutdownStatus,
        CaptureWorkerReapError, CaptureWriterPolicy, ConfigOverrides, ConfigSources,
        DiagnosticCaptureBundle, MemoryCaptureSink, PendingCaptureWriter,
        initialize_capture_process_infrastructure, raw_capture_channel, spawn_capture_writer,
    };
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::{
        Cli, PipelineShutdownReport, RunMode, RunSourceDisposition, SourceEventShutdownReport,
        cancel_and_shutdown_paper_bot, capture_identity, compose_deferred_capture_error,
        compose_pipeline_error, finish_with_cleanup, run_source, shutdown_source_then_event,
    };

    const TEST_MEMORY_SINK_MAX_RECORDS: usize = 4_096;
    const TEST_MEMORY_SINK_RETAINED_CEILING_BYTES: usize = 64 * 1024 * 1024;

    fn test_memory_capture_sink() -> Result<MemoryCaptureSink, Box<dyn std::error::Error>> {
        Ok(MemoryCaptureSink::try_new(
            NonZeroUsize::new(TEST_MEMORY_SINK_MAX_RECORDS)
                .ok_or("invalid test sink record limit")?,
            NonZeroUsize::new(TEST_MEMORY_SINK_RETAINED_CEILING_BYTES)
                .ok_or("invalid test sink retained-byte ceiling")?,
        )?)
    }

    fn retain_pending_owner(
        disposition: RunSourceDisposition,
    ) -> Option<PendingCaptureWriter<DiagnosticCaptureBundle>> {
        match disposition {
            RunSourceDisposition::Complete(_snapshot) => None,
            RunSourceDisposition::CapturePending { pending, .. } => Some(pending),
        }
    }

    #[test]
    fn pending_disposition_retains_the_concrete_capture_owner_type() {
        let _type_check: fn(
            RunSourceDisposition,
        ) -> Option<PendingCaptureWriter<DiagnosticCaptureBundle>> = retain_pending_owner;
    }

    #[test]
    fn deferred_capture_reap_error_preserves_the_primary_pipeline_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        const PRIMARY_FAILURE: &str = "injected primary pipeline failure";
        let mut shutdown = PipelineShutdownReport {
            primary: Some(anyhow::anyhow!(PRIMARY_FAILURE)),
            source_event: SourceEventShutdownReport {
                source: Ok(SourceShutdownOutcome::Graceful),
                event_join_failed: false,
            },
        };

        let capture_error =
            compose_deferred_capture_error(Err(CaptureWorkerReapError::WorkerStillRunning));
        let composed = compose_pipeline_error(&mut shutdown, Some(capture_error))
            .ok_or("the injected primary failure must be retained")?;
        let rendered = format!("{composed:#}");

        assert!(rendered.contains(PRIMARY_FAILURE));
        assert!(rendered.contains("failed to reap deferred capture worker"));
        assert!(rendered.contains("capture worker is still running"));
        Ok(())
    }

    #[tokio::test]
    async fn paper_bot_primary_wait_failure_still_cancels_and_awaits_shutdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let shutdown_awaited = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&shutdown_awaited);
        let result = cancel_and_shutdown_paper_bot(
            &cancellation,
            Some(anyhow::anyhow!("injected signal-listener failure")),
            async move {
                observed.store(true, Ordering::Release);
                false
            },
            |complete| *complete,
        )
        .await;

        let error = match result {
            Err(error) => error,
            Ok(_) => return Err("the primary wait failure must remain fatal".into()),
        };
        let rendered = format!("{error:#}");
        assert!(cancellation.is_cancelled());
        assert!(shutdown_awaited.load(Ordering::Acquire));
        assert!(rendered.contains("injected signal-listener failure"));
        assert!(rendered.contains("shutdown was incomplete"));
        Ok(())
    }

    #[test]
    fn command_failure_preserves_incomplete_application_shutdown()
    -> Result<(), Box<dyn std::error::Error>> {
        const COMMAND_FAILURE: &str = "injected command failure";
        const SHUTDOWN_FAILURE: &str = "injected shutdown failure";

        let result = finish_with_cleanup::<()>(
            Err(anyhow::anyhow!(COMMAND_FAILURE)),
            Some(anyhow::anyhow!(SHUTDOWN_FAILURE)),
        );
        let error = match result {
            Ok(()) => {
                return Err("the primary command and cleanup failures must remain terminal".into());
            }
            Err(error) => error,
        };
        let rendered = format!("{error:#}");

        assert!(rendered.contains(COMMAND_FAILURE));
        assert!(rendered.contains(SHUTDOWN_FAILURE));
        Ok(())
    }

    #[test]
    fn source_shutdown_cli_override_is_explicit_and_bounded_by_configuration()
    -> Result<(), Box<dyn std::error::Error>> {
        let cli = Cli::try_parse_from(["market-squawk", "--source-shutdown-ms", "2500", "mock"])?;

        assert_eq!(cli.source_shutdown_ms, Some(2_500));
        Ok(())
    }

    #[test]
    fn capture_memory_cli_overrides_use_only_the_v01_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let cli = Cli::try_parse_from([
            "market-squawk",
            "--capture-queue-capacity",
            "32",
            "--capture-memory-ceiling-bytes",
            "67108864",
            "--capture-destination-registry-memory-ceiling-bytes",
            "1048576",
            "mock",
        ])?;
        assert_eq!(cli.capture_queue_capacity, Some(32));
        assert_eq!(cli.capture_memory_ceiling_bytes, Some(67_108_864));
        assert_eq!(
            cli.capture_destination_registry_memory_ceiling_bytes,
            Some(1_048_576)
        );
        assert!(
            Cli::try_parse_from(["market-squawk", "--journal-queue-capacity", "32", "mock",])
                .is_err()
        );
        Ok(())
    }
    #[derive(Debug)]
    struct NonCooperativeSource {
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for NonCooperativeSource {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    #[async_trait]
    impl MarketSource for NonCooperativeSource {
        async fn run_session(
            &mut self,
            _capture: CaptureContext,
            _events: mpsc::Sender<market_squawk::DiagnosticMarketEvent>,
            _cancellation: CancellationToken,
        ) -> anyhow::Result<SourceRunOutcome> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn application_deadline_reaps_source_then_event_and_capture_workers()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let config = AppConfig::load(ConfigSources::new(
            None,
            &BTreeMap::new(),
            ConfigOverrides {
                data_dir: Some(directory.path().join("data")),
                capture_flush_interval_ms: Some(10),
                capture_shutdown_ms: Some(100),
                source_shutdown_ms: Some(1_200),
                ..ConfigOverrides::default()
            },
        ))?;
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let source: Box<dyn MarketSource> = Box::new(NonCooperativeSource {
            dropped: Arc::clone(&dropped),
        });

        let disposition = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            run_source(config, source, RunMode::ForDuration(0)),
        )
        .await??;

        assert!(matches!(disposition, RunSourceDisposition::Complete(_)));
        assert!(dropped.load(std::sync::atomic::Ordering::Acquire));
        Ok(())
    }

    #[tokio::test]
    async fn primary_branch_error_still_reaps_source_event_and_capture_owners_in_order()
    -> Result<(), Box<dyn std::error::Error>> {
        const PRIMARY_FAILURE: &str = "injected MCP branch failure";
        let (identity, connection_id) = capture_identity("mock")?;
        let process =
            initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
                NonZeroUsize::new(1024 * 1024).ok_or("invalid registry memory ceiling")?,
            ))?;
        let (publisher, mut control, writer) = raw_capture_channel(
            &process,
            CaptureChannelLimits::new(
                NonZeroUsize::new(8).ok_or("invalid fixed queue capacity")?,
                NonZeroUsize::new(64 * 1024 * 1024).ok_or("invalid capture memory ceiling")?,
            ),
            DiagnosticCaptureBundle::new(identity.clone()),
        )?;
        let capture_handle = spawn_capture_writer(
            writer,
            test_memory_capture_sink()?,
            CaptureWriterPolicy::default(),
        )?;
        control.activate_initial()?;
        let (event_sender, mut event_receiver) = mpsc::channel(1);
        let event_reaped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let event_reaped_by_task = Arc::clone(&event_reaped);
        let event_task = tokio::spawn(async move {
            while event_receiver.recv().await.is_some() {}
            event_reaped_by_task.store(true, std::sync::atomic::Ordering::Release);
        });
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let source: Box<dyn MarketSource> = Box::new(NonCooperativeSource {
            dropped: Arc::clone(&dropped),
        });
        let supervisor =
            SourceSupervisor::new(publisher.try_clone()?, control, identity, connection_id);
        let mut source_task = SupervisedSourceTask::spawn(supervisor, source, event_sender);

        let mut shutdown = shutdown_source_then_event(
            &mut source_task,
            None,
            Some(anyhow::anyhow!(PRIMARY_FAILURE)),
            std::time::Duration::from_millis(10),
            event_task,
        )
        .await;

        assert_eq!(
            shutdown.source_event.source,
            Ok(SourceShutdownOutcome::Graceful)
        );
        assert!(!shutdown.source_event.event_join_failed);
        assert!(source_task.is_reaped());
        assert!(dropped.load(std::sync::atomic::Ordering::Acquire));
        assert!(event_reaped.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            publisher.integrity(),
            market_squawk_domain::CaptureIntegrityState::Incomplete
        );
        let mut pending = capture_handle.shutdown(std::time::Duration::from_secs(1));
        assert_eq!(
            pending.wait_until_deadline().await,
            CaptureShutdownStatus::WorkerTerminated
        );
        let report = pending
            .try_reap()?
            .ok_or("capture worker did not retain a termination report")?;
        assert!(!report.outcome().is_incomplete());
        let composed = compose_pipeline_error(&mut shutdown, None)
            .ok_or("injected primary failure was lost during cleanup")?;
        assert!(format!("{composed:#}").contains(PRIMARY_FAILURE));
        Ok(())
    }
}
