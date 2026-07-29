//! Market Squawk's Tauri 2 composition root.

use std::{ffi::OsString, path::PathBuf};

use clap::Parser;
use market_squawk::{LocalProduct, LocalProductError};
use market_squawk_platform::{AppConfig, ConfigError, ConfigOverrides, ConfigSources};
use tauri::Manager;
use thiserror::Error;

mod bridge;
mod contracts;

use bridge::{
    DesktopState, application_invoke, desktop_bootstrap, open_official_provider_page,
    provider_onboarding,
};

#[derive(Debug, Parser)]
#[command(name = "market-squawk-desktop")]
#[command(about = "Market Squawk Obsidian Signal desktop application")]
#[command(version)]
struct DesktopArgs {
    /// Explicit local Market Squawk TOML configuration.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Local Market Squawk data root.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Absolute verified Python training-release root.
    #[arg(long)]
    training_release_root: Option<PathBuf>,
    /// Enable paper-only bot behavior.
    #[arg(long)]
    paper_mode: bool,
}

#[derive(Debug, Error)]
enum DesktopStartupError {
    #[error("desktop arguments are invalid")]
    Arguments(#[source] clap::Error),
    #[error("desktop configuration is invalid")]
    Configuration(#[from] ConfigError),
    #[error("local product initialization failed")]
    Product(#[from] LocalProductError),
    #[error("desktop runtime initialization failed")]
    Tauri(#[from] tauri::Error),
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let code = match try_run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error}");
            1
        }
    };
    std::process::exit(code);
}

fn try_run() -> Result<i32, DesktopStartupError> {
    let args = DesktopArgs::try_parse().map_err(DesktopStartupError::Arguments)?;
    let mut environment = ConfigSources::process_environment();
    environment.remove(&OsString::from("MARKET_SQUAWK_LOG"));
    environment.remove(&OsString::from("MARKET_SQUAWK_EXTERNAL_NETWORK"));
    environment.remove(&OsString::from("MARKET_SQUAWK_PROVIDER_TERMS_ACCEPTED"));
    let config = AppConfig::load(ConfigSources::new(
        args.config.as_deref(),
        &environment,
        ConfigOverrides {
            data_dir: args.data_dir,
            paper_bot_enabled: args.paper_mode.then_some(true),
            training_release_root: args.training_release_root,
            ..ConfigOverrides::default()
        },
    ))?;
    let product_config = config.clone();
    let product =
        tauri::async_runtime::block_on(async move { LocalProduct::try_new(product_config) })?;
    let state = DesktopState::new(product, config);
    let app = tauri::Builder::default()
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            application_invoke,
            desktop_bootstrap,
            open_official_provider_page,
            provider_onboarding
        ])
        .build(tauri::generate_context!())?;
    let exit_code = app.run_return(|handle, event| match event {
        tauri::RunEvent::ExitRequested { .. } => {
            handle.state::<DesktopState>().begin_shutdown();
        }
        tauri::RunEvent::Exit => {
            let state = handle.state::<DesktopState>();
            tauri::async_runtime::block_on(state.finish_shutdown());
        }
        _ => {}
    });
    Ok(exit_code)
}
