use std::{ffi::OsString, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use market_squawk::{
    AppConfig, AppPaths, DiagnosticEngine, DiagnosticEngineSnapshot, LocalProduct,
    cli::{Cli, Command, ConfigCommand, McpCommand, OutputFormat},
    local_product::execute_cli_command,
    mcp::LocalMcpComposition,
    paper_bot::local_paper_bot,
    replay::replay_coinbase_journal,
    source::{MarketSource, coinbase::CoinbaseSource, mock::MockSource},
    source_supervisor::{
        SourceShutdownError, SourceShutdownOutcome, SourceSupervisor, SupervisedSourceTask,
    },
};
use market_squawk_domain::{
    CaptureAuthorityIdentity, ConnectionGeneration, MetadataRevision, SourceId, SourceIdentifier,
};
use market_squawk_platform::{
    CaptureChannelLimits, CaptureProcessInfrastructureLimits, CaptureShutdownStatus,
    CaptureWorkerReapError, CaptureWorkerTermination, CaptureWriterPolicy, ConfigOverrides,
    ConfigSources, DiagnosticCaptureBundle, PendingCaptureWriter,
    initialize_capture_process_infrastructure, raw_capture_channel, spawn_capture_writer,
};
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    initialize_logging(&cli.log, cli.json_logs)?;
    let output = cli.output;
    let config_file = cli.config.clone();
    let cli_overrides = ConfigOverrides {
        data_dir: cli.data_dir,
        capture_queue_capacity: cli.capture_queue_capacity,
        capture_memory_ceiling_bytes: cli.capture_memory_ceiling_bytes,
        capture_destination_registry_memory_ceiling_bytes: cli
            .capture_destination_registry_memory_ceiling_bytes,
        source_shutdown_ms: cli.source_shutdown_ms,
        training_release_root: cli.training_release_root,
        ..ConfigOverrides::default()
    };

    match cli.command {
        Command::Init => {
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            let paths = AppPaths::prepare(config.data_dir())?;
            if let Some(path) = paths.journal_initialization_file("coinbase-exchange")?
                && !path.exists()
            {
                paths.open_journal_writer("coinbase-exchange")?.flush()?;
            }
            println!("initialized {}", paths.root().display());
        }
        Command::Config { command } => {
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            run_config_command(command, &config, output)?;
        }
        command @ (Command::Source { .. }
        | Command::Ingest { .. }
        | Command::Dataset { .. }
        | Command::Query { .. }
        | Command::Feature { .. }
        | Command::Model { .. }
        | Command::Portfolio { .. }
        | Command::Backtest { .. }
        | Command::Bot { .. }
        | Command::Execution { .. }
        | Command::FairValue { .. }) => {
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            run_product_command(config, command, output).await?;
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
            let provider = arguments.provider;
            let seconds = arguments.seconds;
            let initial_cash = arguments.initial_cash;
            let fee_basis_points = arguments.fee_basis_points;
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            let composition =
                local_paper_bot(config, provider.into(), initial_cash, fee_basis_points)?;
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
            if !matches!(command, None | Some(McpCommand::Serve)) {
                anyhow::bail!("unsupported MCP operation");
            }
            let config = load_config(config_file.as_deref(), cli_overrides)?;
            let product = LocalProduct::try_new(config)?;
            let composition = LocalMcpComposition::try_new(product.paths(), product.application())?;
            let _exit = composition.serve_stdio(CancellationToken::new()).await?;
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

fn load_config(
    config_file: Option<&std::path::Path>,
    cli_overrides: ConfigOverrides,
) -> Result<AppConfig> {
    let mut environment = ConfigSources::process_environment();
    environment.remove(&OsString::from("MARKET_SQUAWK_LOG"));
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
    let value = serde_json::json!({
        "data_root_configured": !config.data_dir().as_os_str().is_empty(),
        "products": config.products(),
        "stale_after_ms": config.stale_after().as_millis(),
        "capture_queue_capacity": config.capture_queue_capacity().get(),
        "capture_memory_ceiling_bytes": config.capture_memory_ceiling_bytes().get(),
        "capture_destination_registry_memory_ceiling_bytes": config
            .capture_destination_registry_memory_ceiling_bytes()
            .get(),
        "paper_bot_enabled": config.paper_bot_enabled(),
        "capture_flush_interval_ms": config.capture_flush_interval().as_millis(),
        "capture_shutdown_ms": config.capture_shutdown().as_millis(),
        "source_shutdown_ms": config.source_shutdown().as_millis(),
        "training_release_configured": config.training_release_root().is_some(),
        "source_secret_configured": config.source_secret().is_some(),
        "coinbase_configured": config.coinbase().is_some(),
        "kraken_configured": config.kraken().is_some(),
    });
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
) -> Result<()> {
    let product = LocalProduct::try_new(config)?;
    let application = product.application();
    let result = execute_cli_command(&product, command).await;
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(10))
        .ok_or_else(|| anyhow!("application shutdown deadline overflow"))?;
    let shutdown = application.shutdown(deadline).await;
    let result = result.map_err(anyhow::Error::from)?;
    if !shutdown.is_complete() {
        anyhow::bail!("local application shutdown was incomplete: {shutdown:?}");
    }
    emit_result(output, result.summary(), result.value())
}

async fn run_doctor(config: AppConfig, output: OutputFormat) -> Result<()> {
    let storage_exists = config
        .data_dir()
        .try_exists()
        .context("failed to inspect the configured local storage root")?;
    let product = LocalProduct::try_new(config.clone());
    let (application_composed, shutdown_complete, composition_error) = match product {
        Ok(product) => {
            let application = product.application();
            let deadline = std::time::Instant::now()
                .checked_add(Duration::from_secs(10))
                .ok_or_else(|| anyhow!("application shutdown deadline overflow"))?;
            let report = application.shutdown(deadline).await;
            (true, report.is_complete(), None)
        }
        Err(error) => (false, false, Some(error.to_string())),
    };
    let value = serde_json::json!({
        "status": "blocked",
        "local_storage": {
            "configured": !config.data_dir().as_os_str().is_empty(),
            "exists": storage_exists,
        },
        "tracing": {
            "local_only": true,
            "remote_exporter": false,
        },
        "providers": {
            "coinbase_configured": config.coinbase().is_some(),
            "kraken_configured": config.kraken().is_some(),
        },
        "application": {
            "composed": application_composed,
            "shutdown_complete": shutdown_complete,
            "error": composition_error,
        },
        "release_blockers": [
            "typed activation requests for SEC, BLS, Treasury, and permitted FRED/ALFRED research adapters",
            "governed backtest input registration through CLI and MCP",
            "automatic research feature/example materialization over persisted point-in-time observations",
            "producer-issued fair-value input publication and account-specific market-access assessment",
            "complete MCP coverage for dataset, model-admission, portfolio-import, and fair-value producer workflows",
            "Python financial analytics and model-training release interface",
            "release security, fuzz, benchmark, documentation, and full-workspace verification evidence",
        ],
    });
    emit_result(output, "release readiness is blocked", &value)
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
        compose_pipeline_error, run_source, shutdown_source_then_event,
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
                source_shutdown_ms: Some(1),
                ..ConfigOverrides::default()
            },
        ))?;
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let source: Box<dyn MarketSource> = Box::new(NonCooperativeSource {
            dropped: Arc::clone(&dropped),
        });

        let disposition = tokio::time::timeout(
            std::time::Duration::from_secs(2),
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
