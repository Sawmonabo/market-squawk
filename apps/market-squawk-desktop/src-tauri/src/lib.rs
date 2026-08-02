//! Market Squawk's Tauri 2 composition root.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use clap::Parser;
#[cfg(target_os = "linux")]
use market_squawk::verified_installed_cli_program;
use market_squawk_installer::{
    InstallError, PlatformError, UninstallRequest, default_install_root, uninstall,
};
use market_squawk_platform::{AppConfig, ConfigError, ConfigOverrides, ConfigSources};
use tauri::Manager;
use thiserror::Error;

mod bridge;
mod contracts;
mod installation;
mod service;

use bridge::{
    DesktopState, application_invoke, desktop_bootstrap, installation_control,
    open_official_provider_page, open_protected_provider_setup, provider_onboarding,
};

#[cfg(target_os = "linux")]
const MAXIMUM_APPIMAGE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
#[cfg(target_os = "linux")]
const APPIMAGE_HEADER_BYTES: usize = 11;
#[cfg(target_os = "linux")]
const DESKTOP_EXECUTABLE_BASENAME: &str = "market-squawk-desktop";
#[cfg(target_os = "linux")]
const CLI_EXECUTABLE_BASENAME: &str = "market-squawk";

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
    /// Dispatch the packaged stdio MCP process from a portable Linux image.
    #[arg(long, hide = true)]
    stdio_mcp: bool,
    /// Remove the current user's managed program store before native package removal.
    #[arg(
        long,
        hide = true,
        conflicts_with_all = ["config", "data_dir", "training_release_root", "stdio_mcp"]
    )]
    native_uninstall: bool,
}

#[derive(Debug, Error)]
enum DesktopStartupError {
    #[error("installed MCP transport is unavailable")]
    McpTransportUnavailable,
    #[cfg(target_os = "linux")]
    #[error("installed MCP transport could not start")]
    McpTransportStart {
        #[source]
        source: std::io::Error,
    },
    #[error("desktop configuration path is invalid")]
    ConfigurationPath {
        #[source]
        source: std::io::Error,
    },
    #[error("selected managed desktop release could not start")]
    ManagedReleaseStart {
        #[source]
        source: std::io::Error,
    },
    #[error("desktop configuration is invalid")]
    Configuration(#[from] ConfigError),
    #[error("complete product installation failed")]
    Installation(#[from] installation::InstallationStartupError),
    #[error("installed service initialization failed")]
    Service(#[from] service::DesktopServiceError),
    #[error("installed service bootstrap is incompatible with this dashboard")]
    InvalidServiceBootstrap,
    #[error("desktop runtime initialization failed")]
    Tauri(#[from] tauri::Error),
    #[error("desktop state was already installed")]
    DuplicateState,
    #[error("native package cleanup could not determine the program root")]
    NativeUninstallRoot(#[from] PlatformError),
    #[error("native package cleanup failed")]
    NativeUninstall(#[from] InstallError),
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let args = DesktopArgs::try_parse().unwrap_or_else(|error| error.exit());
    let result = if args.native_uninstall {
        run_native_uninstall()
    } else if args.stdio_mcp {
        run_stdio_mcp(args)
    } else {
        try_run(args)
    };
    let code = match result {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error}");
            1
        }
    };
    std::process::exit(code);
}

fn run_native_uninstall() -> Result<i32, DesktopStartupError> {
    let root = default_install_root()?;
    uninstall(UninstallRequest::preserving_data(root))?;
    Ok(0)
}

#[cfg(target_os = "linux")]
fn run_stdio_mcp(args: DesktopArgs) -> Result<i32, DesktopStartupError> {
    use std::os::unix::process::CommandExt as _;

    let cli = verified_installed_cli_program()
        .map_err(|_error| DesktopStartupError::McpTransportUnavailable)?;
    let launcher = appimage_mcp_launcher(&cli)
        .map_err(|_error| DesktopStartupError::McpTransportUnavailable)?
        .ok_or(DesktopStartupError::McpTransportUnavailable)?;
    let data_directory = args
        .data_dir
        .filter(|path| path.is_absolute())
        .ok_or(DesktopStartupError::McpTransportUnavailable)?;
    let config_path = args
        .config
        .map(|path| {
            if !path.is_absolute() {
                return Err(DesktopStartupError::McpTransportUnavailable);
            }
            std::fs::canonicalize(path)
                .map_err(|_source| DesktopStartupError::McpTransportUnavailable)
        })
        .transpose()?;
    let training_release_root = args
        .training_release_root
        .map(|path| {
            if path.is_absolute() {
                Ok(path)
            } else {
                Err(DesktopStartupError::McpTransportUnavailable)
            }
        })
        .transpose()?;

    let mut command = std::process::Command::new(launcher.cli_program);
    if let Some(path) = config_path {
        command.arg("--config").arg(path);
    }
    command.arg("--data-dir").arg(data_directory);
    if let Some(path) = training_release_root {
        command.arg("--training-release-root").arg(path);
    }
    command.arg("mcp").arg("serve");
    let source = command.exec();
    Err(DesktopStartupError::McpTransportStart { source })
}

#[cfg(not(target_os = "linux"))]
fn run_stdio_mcp(_args: DesktopArgs) -> Result<i32, DesktopStartupError> {
    Err(DesktopStartupError::McpTransportUnavailable)
}

fn try_run(args: DesktopArgs) -> Result<i32, DesktopStartupError> {
    let app = tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .invoke_handler(tauri::generate_handler![
            application_invoke,
            desktop_bootstrap,
            installation_control,
            open_official_provider_page,
            open_protected_provider_setup,
            provider_onboarding
        ])
        .build(tauri::generate_context!())?;
    let installation = installation::prepare(app.handle())?;
    if let Some(program) = installation.handoff_program.as_ref() {
        return handoff_to_selected_release(program);
    }
    let desktop_data_directory = app.path().app_local_data_dir()?;
    let mut environment = ConfigSources::process_environment();
    environment.remove(&OsString::from("MARKET_SQUAWK_LOG"));
    environment.remove(&OsString::from("MARKET_SQUAWK_EXTERNAL_NETWORK"));
    environment.remove(&OsString::from("MARKET_SQUAWK_PROVIDER_TERMS_ACCEPTED"));
    let config_path = args
        .config
        .as_deref()
        .map(std::fs::canonicalize)
        .transpose()
        .map_err(|source| DesktopStartupError::ConfigurationPath { source })?;
    let config = AppConfig::load(
        ConfigSources::new(
            config_path.as_deref(),
            &environment,
            ConfigOverrides {
                data_dir: args.data_dir,
                training_release_root: args
                    .training_release_root
                    .or(installation.active_release_root),
                ..ConfigOverrides::default()
            },
        )
        .with_data_directory_default(desktop_data_directory),
    )?;
    let service =
        tauri::async_runtime::block_on(service::connect_or_start(&config, config_path.as_deref()))?;
    let state = DesktopState::try_new(
        service.application,
        service.bootstrap,
        installation.root,
        installation.status,
    )
    .map_err(|_error| DesktopStartupError::InvalidServiceBootstrap)?;
    if !app.manage(state) {
        return Err(DesktopStartupError::DuplicateState);
    }
    let exit_code = app.run_return(|handle, event| match event {
        tauri::RunEvent::ExitRequested { .. } => {
            handle.state::<DesktopState>().begin_shutdown();
        }
        tauri::RunEvent::Exit => {
            let state = handle.state::<DesktopState>();
            let restart_program = state.scheduled_restart_program();
            tauri::async_runtime::block_on(state.finish_shutdown());
            if let Some(program) = restart_program
                && std::process::Command::new(program)
                    .args(std::env::args_os().skip(1))
                    .spawn()
                    .is_err()
            {
                eprintln!("market-squawk-desktop: failed to restart the selected release");
            }
        }
        _ => {}
    });
    Ok(exit_code)
}

fn handoff_to_selected_release(program: &Path) -> Result<i32, DesktopStartupError> {
    let mut command = std::process::Command::new(program);
    command.args(std::env::args_os().skip(1));
    #[cfg(target_os = "linux")]
    command.env_remove("APPIMAGE").env_remove("APPDIR");
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;

        let source = command.exec();
        Err(DesktopStartupError::ManagedReleaseStart { source })
    }
    #[cfg(windows)]
    {
        command
            .spawn()
            .map_err(|source| DesktopStartupError::ManagedReleaseStart { source })?;
        Ok(0)
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct AppImageMcpLauncher {
    cli_program: PathBuf,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug)]
pub(crate) struct AppImageMcpLauncherError;

#[cfg(target_os = "linux")]
pub(crate) fn appimage_mcp_launcher(
    cli_program: &Path,
) -> Result<Option<AppImageMcpLauncher>, AppImageMcpLauncherError> {
    use std::io::Read as _;
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let (appimage, app_dir) = match (std::env::var_os("APPIMAGE"), std::env::var_os("APPDIR")) {
        (None, None) => return Ok(None),
        (Some(appimage), Some(app_dir)) => (appimage, app_dir),
        _ => return Err(AppImageMcpLauncherError),
    };
    let appimage = PathBuf::from(appimage);
    let app_dir = PathBuf::from(app_dir);
    if !appimage.is_absolute() || !app_dir.is_absolute() {
        return Err(AppImageMcpLauncherError);
    }
    let named = std::fs::symlink_metadata(&appimage).map_err(|_source| AppImageMcpLauncherError)?;
    let mode = named.permissions().mode();
    let process_owner = std::fs::metadata("/proc/self")
        .map_err(|_source| AppImageMcpLauncherError)?
        .uid();
    let owner = named.uid();
    if named.file_type().is_symlink()
        || !named.is_file()
        || named.len() == 0
        || named.len() > MAXIMUM_APPIMAGE_BYTES
        || mode & 0o111 == 0
        || mode & 0o022 != 0
        || (owner != 0 && owner != process_owner)
    {
        return Err(AppImageMcpLauncherError);
    }
    let mut header = [0_u8; APPIMAGE_HEADER_BYTES];
    std::fs::File::open(&appimage)
        .and_then(|mut file| file.read_exact(&mut header))
        .map_err(|_source| AppImageMcpLauncherError)?;
    if header[8..] != [b'A', b'I', 2] {
        return Err(AppImageMcpLauncherError);
    }
    let program = std::fs::canonicalize(appimage).map_err(|_source| AppImageMcpLauncherError)?;
    let app_dir = std::fs::canonicalize(app_dir).map_err(|_source| AppImageMcpLauncherError)?;
    if !std::fs::metadata(&app_dir)
        .map_err(|_source| AppImageMcpLauncherError)?
        .is_dir()
    {
        return Err(AppImageMcpLauncherError);
    }
    let current = std::env::current_exe()
        .and_then(std::fs::canonicalize)
        .map_err(|_source| AppImageMcpLauncherError)?;
    let expected_current =
        std::fs::canonicalize(app_dir.join("usr/bin").join(DESKTOP_EXECUTABLE_BASENAME))
            .map_err(|_source| AppImageMcpLauncherError)?;
    let expected_cli = std::fs::canonicalize(app_dir.join("usr/bin").join(CLI_EXECUTABLE_BASENAME))
        .map_err(|_source| AppImageMcpLauncherError)?;
    let cli_program =
        std::fs::canonicalize(cli_program).map_err(|_source| AppImageMcpLauncherError)?;
    if current != expected_current || cli_program != expected_cli || current == program {
        return Err(AppImageMcpLauncherError);
    }
    Ok(Some(AppImageMcpLauncher { cli_program }))
}
