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
    open_protected_provider_setup, provider_onboarding,
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
    #[error("desktop configuration is invalid")]
    Configuration(#[from] ConfigError),
    #[error("local product initialization failed")]
    Product(#[from] LocalProductError),
    #[error("desktop runtime initialization failed")]
    Tauri(#[from] tauri::Error),
    #[error("desktop state was already installed")]
    DuplicateState,
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let args = DesktopArgs::try_parse().unwrap_or_else(|error| error.exit());
    let code = match try_run(args) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error}");
            1
        }
    };
    std::process::exit(code);
}

fn try_run(args: DesktopArgs) -> Result<i32, DesktopStartupError> {
    let app = tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            application_invoke,
            desktop_bootstrap,
            open_official_provider_page,
            open_protected_provider_setup,
            provider_onboarding
        ])
        .build(tauri::generate_context!())?;
    let desktop_data_directory = app.path().app_local_data_dir()?;
    let mut environment = ConfigSources::process_environment();
    environment.remove(&OsString::from("MARKET_SQUAWK_LOG"));
    environment.remove(&OsString::from("MARKET_SQUAWK_EXTERNAL_NETWORK"));
    environment.remove(&OsString::from("MARKET_SQUAWK_PROVIDER_TERMS_ACCEPTED"));
    let config = AppConfig::load(
        ConfigSources::new(
            args.config.as_deref(),
            &environment,
            ConfigOverrides {
                data_dir: args.data_dir,
                paper_bot_enabled: args.paper_mode.then_some(true),
                training_release_root: args.training_release_root,
                ..ConfigOverrides::default()
            },
        )
        .with_data_directory_default(desktop_data_directory),
    )?;
    let product_config = config.clone();
    let product =
        tauri::async_runtime::block_on(async move { LocalProduct::try_new(product_config) })?;
    let state = DesktopState::new(product, config);
    if !app.manage(state) {
        return Err(DesktopStartupError::DuplicateState);
    }
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
