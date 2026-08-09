// Rust #159105: see the service binary for the measured macOS debug-link limitation.
#![cfg_attr(all(target_os = "macos", debug_assertions), allow(linker_messages))]

use std::{ffi::OsString, path::PathBuf, sync::Arc};

use anyhow::{Context as _, Result, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
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
    /// Installer-owned encoded override for a non-default service authority root.
    #[arg(long, hide = true)]
    installation_data_root_encoded: Option<String>,
}

const MAXIMUM_INSTALLATION_ROOT_BYTES: usize = 4 * 1024;
const MAXIMUM_INSTALLATION_ROOT_ARGUMENT_BYTES: usize = 6 * 1024;

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
    let connector = arguments
        .installation_data_root_encoded
        .as_deref()
        .map(decode_installation_data_root)
        .transpose()?
        .map_or_else(
            || InstalledServiceConnector::try_new(&config),
            |root| InstalledServiceConnector::try_new_at_installation_root(&config, root),
        )?;
    let transport = connector.connect_mcp_relay(client)?;
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

fn decode_installation_data_root(encoded: &str) -> Result<PathBuf> {
    if encoded.is_empty() || encoded.len() > MAXIMUM_INSTALLATION_ROOT_ARGUMENT_BYTES {
        bail!("the installed service root argument is invalid");
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded)
        .context("the installed service root argument is invalid")?;
    if decoded.is_empty() || decoded.len() > MAXIMUM_INSTALLATION_ROOT_BYTES {
        bail!("the installed service root argument is invalid");
    }
    let value =
        String::from_utf8(decoded).context("the installed service root argument is invalid")?;
    if value.contains('\0') {
        bail!("the installed service root argument is invalid");
    }
    let root = PathBuf::from(value);
    if !root.is_absolute() {
        bail!("the installed service root argument is invalid");
    }
    Ok(root)
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
