use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use market_squawk::{
    AppPaths, Engine, EngineConfig, JournalFileFormat,
    journal::{JournalSink, JournalWriter},
    mcp::McpServer,
    replay::replay_coinbase_journal,
    source::{MarketSource, coinbase::CoinbaseSource, mock::MockSource},
};
use parking_lot::RwLock;
use tokio::sync::{mpsc, watch};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "market-squawk")]
#[command(about = "Local-first live market-data capture, replay, paper bots, and MCP")]
#[command(version)]
struct Cli {
    #[arg(long, env = "MARKET_SQUAWK_DATA_DIR", default_value = ".market-squawk")]
    data_dir: PathBuf,

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
    let paths = AppPaths::new(&cli.data_dir);

    match cli.command {
        Command::Init => {
            paths.initialize()?;
            if let Some(path) = paths.journal_initialization_file("coinbase-exchange")? {
                JournalWriter::open(path)?.flush()?;
            }
            println!("initialized {}", paths.root().display());
        }
        Command::Mock {
            product,
            events,
            paper_bot,
        } => {
            let config = EngineConfig {
                data_dir: cli.data_dir,
                products: vec![product.clone()],
                paper_bot_enabled: paper_bot,
                ..EngineConfig::default()
            };
            let source: Box<dyn MarketSource> = Box::new(MockSource::new(product, events));
            let snapshot = run_source(config, source, RunMode::UntilSourceStops).await?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        Command::Capture {
            products,
            seconds,
            paper_bot,
        } => {
            let config = EngineConfig {
                data_dir: cli.data_dir,
                products: products.clone(),
                paper_bot_enabled: paper_bot,
                ..EngineConfig::default()
            };
            let source: Box<dyn MarketSource> = Box::new(CoinbaseSource::new(products));
            let mode = seconds.map_or(RunMode::UntilInterrupted, RunMode::ForDuration);
            let snapshot = run_source(config, source, mode).await?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        Command::Mcp {
            products,
            offline,
            journal_format,
            paper_bot,
        } => {
            let config = EngineConfig {
                data_dir: cli.data_dir,
                products: products.clone(),
                paper_bot_enabled: paper_bot,
                ..EngineConfig::default()
            };
            if offline {
                run_offline_mcp(config, journal_format.map(Into::into)).await?;
            } else {
                let source: Box<dyn MarketSource> = Box::new(CoinbaseSource::new(products));
                let _ = run_source(config, source, RunMode::Mcp).await?;
            }
        }
        Command::Replay {
            source,
            journal_format,
        } => {
            if source != "coinbase-exchange" {
                anyhow::bail!("decoded replay currently supports source=coinbase-exchange");
            }
            let journal_path =
                paths.select_journal_for_read(&source, journal_format.map(Into::into))?;
            let replay = replay_coinbase_journal(
                journal_path,
                EngineConfig::default().stale_after_ms,
                false,
            )?;
            println!("{}", serde_json::to_string_pretty(&replay)?);
        }
    }

    Ok(())
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

async fn run_source(
    config: EngineConfig,
    source: Box<dyn MarketSource>,
    mode: RunMode,
) -> Result<market_squawk::EngineSnapshot> {
    let paths = AppPaths::new(&config.data_dir);
    paths.initialize()?;
    let journal_path = paths.journal_write_file(match mode {
        RunMode::UntilSourceStops => "mock",
        RunMode::UntilInterrupted | RunMode::ForDuration(_) | RunMode::Mcp => "coinbase-exchange",
    });
    let (journal, journal_task) = JournalSink::spawn(&journal_path, config.journal_queue_capacity)?;
    let engine = Arc::new(RwLock::new(Engine::new(
        config.stale_after_ms,
        config.paper_bot_enabled,
    )));
    let (event_sender, mut event_receiver) = mpsc::channel(16_384);
    let (cancel_sender, cancel_receiver) = watch::channel(false);

    let engine_for_events = Arc::clone(&engine);
    let stale_after_ms = config.stale_after_ms;
    let event_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(
            u64::try_from(stale_after_ms.max(250)).unwrap_or(1_000) / 2,
        ));
        loop {
            tokio::select! {
                event = event_receiver.recv() => {
                    match event {
                        Some(event) => engine_for_events.write().handle(event),
                        None => break,
                    }
                }
                _ = interval.tick() => {
                    engine_for_events.write().refresh_staleness(chrono::Utc::now());
                }
            }
        }
    });

    let source_journal = journal.clone();
    let mut source_task = tokio::spawn(async move {
        source
            .run(source_journal, event_sender, cancel_receiver)
            .await
    });

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
            let mcp = McpServer::new(Arc::clone(&engine), journal_path.clone());
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

    journal.flush().await?;
    journal.shutdown().await?;
    journal_task.await.context("journal task panicked")??;
    event_task.await.context("event processor task panicked")?;

    if let Some(error) = source_error {
        error!(error = %format!("{error:#}"), "source stopped with an error");
        return Err(error);
    }

    let snapshot = engine.read().snapshot();
    info!(
        processed_events = snapshot.processed_events,
        "run completed"
    );
    Ok(snapshot)
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
    config: EngineConfig,
    journal_format: Option<JournalFileFormat>,
) -> Result<()> {
    let paths = AppPaths::new(&config.data_dir);
    let journal_path = paths.select_journal_for_read("coinbase-exchange", journal_format)?;
    let engine = Arc::new(RwLock::new(Engine::new(
        config.stale_after_ms,
        config.paper_bot_enabled,
    )));
    McpServer::new(engine, journal_path).serve_stdio().await
}
