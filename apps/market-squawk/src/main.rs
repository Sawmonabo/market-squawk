use std::{ffi::OsString, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use market_squawk::{
    AppConfig, AppPaths, DiagnosticEngine, DiagnosticEngineSnapshot, JournalFileFormat,
    mcp::McpServer,
    replay::replay_coinbase_journal,
    source::{MarketSource, coinbase::CoinbaseSource, mock::MockSource},
    source_supervisor::SourceSupervisor,
};
use market_squawk_domain::{
    CaptureAuthorityIdentity, ConnectionGeneration, MetadataRevision, SourceId, SourceIdentifier,
};
use market_squawk_platform::{
    CaptureShutdownStatus, CaptureWriterPolicy, ConfigOverrides, ConfigSources,
    DiagnosticCaptureBundle, PendingCaptureWriter, raw_capture_channel, spawn_capture_writer,
};
use parking_lot::RwLock;
use tokio::sync::{mpsc, watch};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "market-squawk")]
#[command(about = "Local-first market capture, diagnostic replay/simulation, and MCP")]
#[command(version)]
struct Cli {
    #[arg(long)]
    data_dir: Option<PathBuf>,

    #[arg(long)]
    config: Option<PathBuf>,

    #[arg(long, env = "MARKET_SQUAWK_LOG", default_value = "info")]
    log: String,

    #[arg(long)]
    json_logs: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create the local data directory and an empty Coinbase journal.
    Init,

    /// Run a deterministic mock feed to verify journaling and in-memory analytics locally.
    Mock {
        #[arg(long, default_value = "TEST-USD")]
        product: String,
        #[arg(long, default_value_t = 100)]
        events: usize,
        #[arg(long)]
        paper_bot: bool,
    },

    /// Capture public Coinbase Exchange Level 2, heartbeat, and match messages.
    Capture {
        #[arg(long, value_delimiter = ',', default_value = "BTC-USD")]
        products: Vec<String>,
        /// Stop after this many seconds. Omit to run until Ctrl-C.
        #[arg(long)]
        seconds: Option<u64>,
        #[arg(long)]
        paper_bot: bool,
    },

    /// Run the local MCP stdio server, optionally with a live Coinbase feed.
    Mcp {
        #[arg(long, value_delimiter = ',', default_value = "BTC-USD")]
        products: Vec<String>,
        /// Do not open a network connection; expose only current empty/local state and journal tools.
        #[arg(long)]
        offline: bool,
        /// Select a journal when both the current and legacy formats exist.
        #[arg(long, value_enum, requires = "offline")]
        journal_format: Option<JournalFormatArgument>,
        #[arg(long)]
        paper_bot: bool,
    },

    /// Validate and summarize an immutable journal.
    Replay {
        #[arg(long, default_value = "coinbase-exchange")]
        source: String,
        /// Select a journal when both the current and legacy formats exist.
        #[arg(long, value_enum)]
        journal_format: Option<JournalFormatArgument>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum JournalFormatArgument {
    Current,
    Legacy,
}

impl From<JournalFormatArgument> for JournalFileFormat {
    fn from(value: JournalFormatArgument) -> Self {
        match value {
            JournalFormatArgument::Current => Self::Current,
            JournalFormatArgument::Legacy => Self::Legacy,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    initialize_logging(&cli.log, cli.json_logs)?;
    let config_file = cli.config.clone();
    let cli_data_dir = cli.data_dir.clone();

    match cli.command {
        Command::Init => {
            let config = load_config(config_file.as_deref(), cli_data_dir, None, None)?;
            let paths = AppPaths::prepare(config.data_dir())?;
            if let Some(path) = paths.journal_initialization_file("coinbase-exchange")?
                && !path.exists()
            {
                paths.open_journal_writer("coinbase-exchange")?.flush()?;
            }
            println!("initialized {}", paths.root().display());
        }
        Command::Mock {
            product,
            events,
            paper_bot,
        } => {
            let config = load_config(
                config_file.as_deref(),
                cli_data_dir,
                Some(vec![product.clone()]),
                Some(paper_bot),
            )?;
            let source: Box<dyn MarketSource> = Box::new(MockSource::new(product, events));
            let disposition = run_source(config, source, RunMode::UntilSourceStops).await?;
            let snapshot = finish_run_source(disposition).await?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        Command::Capture {
            products,
            seconds,
            paper_bot,
        } => {
            let config = load_config(
                config_file.as_deref(),
                cli_data_dir,
                Some(products.clone()),
                Some(paper_bot),
            )?;
            let source: Box<dyn MarketSource> = Box::new(CoinbaseSource::new(products));
            let mode = seconds.map_or(RunMode::UntilInterrupted, RunMode::ForDuration);
            let disposition = run_source(config, source, mode).await?;
            let snapshot = finish_run_source(disposition).await?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        Command::Mcp {
            products,
            offline,
            journal_format,
            paper_bot,
        } => {
            let config = load_config(
                config_file.as_deref(),
                cli_data_dir,
                Some(products.clone()),
                Some(paper_bot),
            )?;
            if offline {
                run_offline_mcp(config, journal_format.map(Into::into)).await?;
            } else {
                let source: Box<dyn MarketSource> = Box::new(CoinbaseSource::new(products));
                let disposition = run_source(config, source, RunMode::Mcp).await?;
                let _snapshot = finish_run_source(disposition).await?;
            }
        }
        Command::Replay {
            source,
            journal_format,
        } => {
            if source != "coinbase-exchange" {
                anyhow::bail!("decoded replay currently supports source=coinbase-exchange");
            }
            let config = load_config(config_file.as_deref(), cli_data_dir, None, None)?;
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

fn load_config(
    config_file: Option<&std::path::Path>,
    data_dir: Option<PathBuf>,
    products: Option<Vec<String>>,
    paper_bot_enabled: Option<bool>,
) -> Result<AppConfig> {
    let mut environment = ConfigSources::process_environment();
    environment.remove(&OsString::from("MARKET_SQUAWK_LOG"));
    Ok(AppConfig::load(ConfigSources::new(
        config_file,
        &environment,
        ConfigOverrides {
            data_dir,
            products,
            paper_bot_enabled,
            ..ConfigOverrides::default()
        },
    ))?)
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
    Mcp,
}

#[derive(Debug)]
enum RunSourceDisposition {
    Complete(DiagnosticEngineSnapshot),
    CapturePending(PendingCaptureWriter<DiagnosticCaptureBundle>),
}

async fn finish_run_source(disposition: RunSourceDisposition) -> Result<DiagnosticEngineSnapshot> {
    match disposition {
        RunSourceDisposition::Complete(snapshot) => Ok(snapshot),
        RunSourceDisposition::CapturePending(mut pending) => {
            pending.wait_until_terminated().await;
            let termination = pending
                .try_reap()?
                .cloned()
                .ok_or_else(|| anyhow!("terminated capture worker had no final report"))?;
            Err(anyhow!(
                "raw capture shutdown deadline elapsed; final worker report: {termination:?}"
            ))
        }
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
        RunMode::UntilInterrupted | RunMode::ForDuration(_) | RunMode::Mcp => "coinbase-exchange",
    };
    let journal_path = paths.journal_write_file(source_name)?;
    let (capture_identity, connection_id) = capture_identity(source_name)?;
    let (publisher, mut capture_control, capture_writer) = raw_capture_channel(
        config.journal_queue_capacity(),
        DiagnosticCaptureBundle::new(capture_identity.clone()),
    );
    let journal = paths.open_journal_writer(source_name)?;
    let flush_batch = std::num::NonZeroUsize::new(256)
        .ok_or_else(|| anyhow!("capture flush batch invariant failed"))?;
    let writer_policy = CaptureWriterPolicy::try_new(flush_batch, config.capture_flush_interval())?;
    let capture_handle = spawn_capture_writer(capture_writer, journal, writer_policy)?;
    capture_control.activate_initial()?;
    let diagnostic_engine = Arc::new(RwLock::new(DiagnosticEngine::new(
        duration_millis_i64(config.stale_after())?,
        config.paper_bot_enabled(),
    )));
    let (event_sender, mut event_receiver) = mpsc::channel(16_384);
    let (cancel_sender, cancel_receiver) = watch::channel(false);

    let diagnostic_engine_for_events = Arc::clone(&diagnostic_engine);
    let stale_after_ms = duration_millis_i64(config.stale_after())?;
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
    let mut source_task = tokio::spawn(supervisor.run(source, event_sender, cancel_receiver));

    let mut source_completed = false;
    let mut source_error: Option<anyhow::Error> = None;

    match mode {
        RunMode::UntilSourceStops => match (&mut source_task).await {
            Ok(Ok(())) => source_completed = true,
            Ok(Err(error)) => {
                source_completed = true;
                source_error = Some(error);
            }
            Err(error) => {
                source_completed = true;
                source_error = Some(error.into());
            }
        },
        RunMode::UntilInterrupted => {
            tokio::select! {
                result = &mut source_task => {
                    source_completed = true;
                    source_error = flatten_source_result(result);
                }
                signal = tokio::signal::ctrl_c() => {
                    signal.context("failed to listen for Ctrl-C")?;
                }
            }
        }
        RunMode::ForDuration(seconds) => {
            tokio::select! {
                result = &mut source_task => {
                    source_completed = true;
                    source_error = flatten_source_result(result);
                }
                _ = tokio::time::sleep(Duration::from_secs(seconds)) => {}
                signal = tokio::signal::ctrl_c() => {
                    signal.context("failed to listen for Ctrl-C")?;
                }
            }
        }
        RunMode::Mcp => {
            let mcp = McpServer::new(Arc::clone(&diagnostic_engine), journal_path.clone());
            tokio::select! {
                result = &mut source_task => {
                    source_completed = true;
                    source_error = flatten_source_result(result);
                }
                result = mcp.serve_stdio() => result?,
                signal = tokio::signal::ctrl_c() => {
                    signal.context("failed to listen for Ctrl-C")?;
                }
            }
        }
    }

    let _ = cancel_sender.send(true);
    if !source_completed {
        source_error = flatten_source_result(source_task.await);
    }

    let event_result = event_task.await.context("event processor task panicked");
    let mut pending_capture = capture_handle.shutdown(config.capture_shutdown());
    let shutdown_status = pending_capture.wait_until_deadline().await;
    if shutdown_status == CaptureShutdownStatus::DeadlineElapsed {
        return Ok(RunSourceDisposition::CapturePending(pending_capture));
    }
    let capture_termination = pending_capture
        .try_reap()?
        .cloned()
        .ok_or_else(|| anyhow!("terminated capture worker had no final report"))?;
    if capture_termination.outcome().is_incomplete() {
        return Err(anyhow!(
            "raw capture shutdown was incomplete: {capture_termination:?}"
        ));
    }
    event_result?;

    if let Some(error) = source_error {
        error!(error = %format!("{error:#}"), "source stopped with an error");
        return Err(error);
    }

    let snapshot = diagnostic_engine.read().snapshot();
    info!(
        processed_events = snapshot.processed_events,
        "run completed"
    );
    Ok(RunSourceDisposition::Complete(snapshot))
}

fn flatten_source_result(
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Option<anyhow::Error> {
    match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(error) => Some(error.into()),
    }
}

async fn run_offline_mcp(
    config: AppConfig,
    journal_format: Option<JournalFileFormat>,
) -> Result<()> {
    let paths = AppPaths::for_read(config.data_dir().to_path_buf());
    let journal_path = paths.select_journal_for_read("coinbase-exchange", journal_format)?;
    let diagnostic_engine = Arc::new(RwLock::new(DiagnosticEngine::new(
        duration_millis_i64(config.stale_after())?,
        config.paper_bot_enabled(),
    )));
    McpServer::new(diagnostic_engine, journal_path)
        .serve_stdio()
        .await
}

#[cfg(test)]
mod tests {
    use super::{DiagnosticCaptureBundle, PendingCaptureWriter, RunSourceDisposition};

    fn retain_pending_owner(
        disposition: RunSourceDisposition,
    ) -> Option<PendingCaptureWriter<DiagnosticCaptureBundle>> {
        match disposition {
            RunSourceDisposition::Complete(_snapshot) => None,
            RunSourceDisposition::CapturePending(pending) => Some(pending),
        }
    }

    #[test]
    fn pending_disposition_retains_the_concrete_capture_owner_type() {
        let _type_check: fn(
            RunSourceDisposition,
        ) -> Option<PendingCaptureWriter<DiagnosticCaptureBundle>> = retain_pending_owner;
    }
}
