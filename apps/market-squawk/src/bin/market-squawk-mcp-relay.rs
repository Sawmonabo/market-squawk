// Rust #159105: see the service binary for the measured macOS debug-link limitation.
#![cfg_attr(all(target_os = "macos", debug_assertions), allow(linker_messages))]

use std::{ffi::OsString, path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::{Parser, ValueEnum};
use market_squawk::{service::InstalledServiceConnector, termination::TerminationSignals};
use market_squawk_mcp::{McpLimitSpec, McpLimits, McpStdioRelay};
use market_squawk_platform::{AppConfig, ConfigOverrides, ConfigSources};
use market_squawk_runtime::NamedClient;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum RelayClient {
    Claude,
    Codex,
}

impl From<RelayClient> for NamedClient {
    fn from(value: RelayClient) -> Self {
        match value {
            RelayClient::Claude => Self::ClaudeCode,
            RelayClient::Codex => Self::Codex,
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "market-squawk-mcp-relay", version)]
struct RelayArguments {
    /// Installer-owned MCP client registration.
    #[arg(long, value_enum)]
    client: RelayClient,
    /// Local Market Squawk data root.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Explicit local configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
}

fn main() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<()> {
    let arguments = RelayArguments::parse();
    let config = load_config(arguments.config.as_deref(), arguments.data_dir)?;
    let client = NamedClient::from(arguments.client);
    let transport = InstalledServiceConnector::try_new(&config)?.connect_mcp_relay(client)?;
    let relay = McpStdioRelay::try_new(
        client,
        Arc::clone(&transport),
        McpLimits::try_from(McpLimitSpec::default())?,
    )?;
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    let signal = tokio::spawn(async move {
        if let Ok(mut signals) = TerminationSignals::install()
            && signals.wait().await.is_ok()
        {
            signal_cancellation.cancel();
        }
    });
    let result = relay.serve_stdio(cancellation).await;
    signal.abort();
    let _ = signal.await;
    result?;
    Ok(())
}

fn load_config(
    config_file: Option<&std::path::Path>,
    data_dir: Option<PathBuf>,
) -> Result<AppConfig> {
    let mut environment = ConfigSources::process_environment();
    environment.remove(&OsString::from("MARKET_SQUAWK_LOG"));
    environment.remove(&OsString::from("MARKET_SQUAWK_EXTERNAL_NETWORK"));
    environment.remove(&OsString::from("MARKET_SQUAWK_PROVIDER_TERMS_ACCEPTED"));
    Ok(AppConfig::load(ConfigSources::new(
        config_file,
        &environment,
        ConfigOverrides {
            data_dir,
            ..ConfigOverrides::default()
        },
    ))?)
}
