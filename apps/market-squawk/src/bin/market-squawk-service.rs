// Rust #159105: this macOS-only dev/test-link diagnostic is caused by the measured
// `__eh_frame` exceeding arm64 compact-unwind's 24-bit offset range. Release diagnostics remain
// enabled because this allowance is restricted to debug-assertion builds.
#![cfg_attr(all(target_os = "macos", debug_assertions), allow(linker_messages))]

use std::{ffi::OsString, path::PathBuf};

use anyhow::{Context as _, Result, anyhow};
use clap::Parser;
use market_squawk::{AppConfig, service::InstalledService, termination::TerminationSignals};
use market_squawk_platform::{ConfigOverrides, ConfigSources};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

const APPLICATION_MAIN_STACK_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "market-squawk-service", version)]
struct ServiceArguments {
    /// Local Market Squawk data root.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Explicit local configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Absolute installed Python training-release root used to verify admitted models.
    #[arg(long)]
    training_release_root: Option<PathBuf>,
    /// Local tracing filter.
    #[arg(long, env = "MARKET_SQUAWK_LOG", default_value = "info")]
    log: String,
    /// Render local tracing as JSON.
    #[arg(long)]
    json_logs: bool,
}

fn main() -> Result<()> {
    let service = std::thread::Builder::new()
        .name("market-squawk-service-main".to_owned())
        .stack_size(APPLICATION_MAIN_STACK_BYTES)
        .spawn(run_service)
        .context("failed to start the Market Squawk service thread")?;
    service
        .join()
        .map_err(|_| anyhow!("the Market Squawk service thread terminated unexpectedly"))?
}

fn run_service() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(Box::pin(run()))
}

async fn run() -> Result<()> {
    let arguments = ServiceArguments::parse();
    initialize_logging(&arguments.log, arguments.json_logs)?;
    let config = load_config(
        arguments.config.as_deref(),
        arguments.data_dir,
        arguments.training_release_root,
    )?;
    let mut termination = TerminationSignals::install()?;
    let service = InstalledService::start(config).await?;
    let cancellation = CancellationToken::new();
    let mut serving = Box::pin(service.run(cancellation.clone()));
    tokio::select! {
        result = &mut serving => result.map_err(Into::into),
        signal = termination.wait() => {
            signal?;
            cancellation.cancel();
            serving.await.map_err(Into::into)
        }
    }
}

fn load_config(
    config_file: Option<&std::path::Path>,
    data_dir: Option<PathBuf>,
    training_release_root: Option<PathBuf>,
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
            training_release_root,
            ..ConfigOverrides::default()
        },
    ))?)
}

fn initialize_logging(filter: &str, json: bool) -> Result<()> {
    let environment = EnvFilter::try_new(filter).context("invalid tracing filter")?;
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(environment)
            .json()
            .try_init()
            .map_err(|error| anyhow!("failed to initialize JSON tracing: {error}"))?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(environment)
            .try_init()
            .map_err(|error| anyhow!("failed to initialize tracing: {error}"))?;
    }
    Ok(())
}
