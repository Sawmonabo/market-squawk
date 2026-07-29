//! Stable installer CLI and bounded HTTPS release retrieval.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use futures_util::StreamExt as _;
use reqwest::redirect::Policy;
use semver::Version;
use serde::Serialize;
use tempfile::NamedTempFile;
use thiserror::Error;
use url::Url;

use crate::contracts::{
    InstallRequest, MutableDataClass, RepairRequest, RollbackRequest, UninstallRequest,
    UpdateRequest,
};
use crate::lifecycle::{
    InstallError, install, repair, resolve_program, rollback, status, uninstall, update,
};
use crate::manifest::{MAXIMUM_ARCHIVE_BYTES, MAXIMUM_MANIFEST_BYTES, ReleaseManifest};
use crate::platform::{ProgramName, default_install_root};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(120);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MAXIMUM_REDIRECTS: usize = 10;

/// Parses and executes one installer command.
///
/// # Errors
///
/// Returns [`CommandError`] for invalid source selection, download, lifecycle, serialization, or
/// fixed-program launch failure.
pub async fn run_cli() -> Result<(), CommandError> {
    execute(Cli::parse()).await
}

#[derive(Debug, Parser)]
#[command(
    name = "market-squawk-installer",
    about = "Install and maintain a complete verified Market Squawk release"
)]
struct Cli {
    /// Override the platform-native per-user program root.
    #[arg(long, global = true)]
    root: Option<PathBuf>,
    /// Emit one JSON result.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: InstallerCommand,
}

#[derive(Debug, Subcommand)]
enum InstallerCommand {
    /// Install from a stable HTTPS manifest or exact local files.
    Install(InstallArguments),
    /// Download and activate the next release from the retained channel.
    Update,
    /// Verify and reconstruct the active version from its retained exact bundle.
    Repair,
    /// Reactivate the retained previous known-good version.
    Rollback,
    /// Remove programs while preserving data unless a class path is separately confirmed.
    Uninstall(UninstallArguments),
    /// Report active, previous, channel, and integrity state.
    Status,
    /// Launch one fixed code-owned installed program.
    Launch {
        /// Program identity; arbitrary executable paths are not accepted.
        #[arg(value_enum)]
        program: ProgramName,
    },
}

#[derive(Debug, Args)]
struct InstallArguments {
    /// Stable HTTPS release-manifest URL.
    #[arg(long, conflicts_with_all = ["manifest", "bundle"])]
    manifest_url: Option<String>,
    /// Exact local release manifest for offline installation.
    #[arg(long, requires = "bundle")]
    manifest: Option<PathBuf>,
    /// Exact local complete ZIP bundle for offline installation.
    #[arg(long, requires = "manifest")]
    bundle: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct UninstallArguments {
    /// Confirm deletion of one exact configuration directory.
    #[arg(long, value_name = "DIRECTORY")]
    confirm_delete_configuration: Option<PathBuf>,
    /// Confirm deletion of one exact credentials directory.
    #[arg(long, value_name = "DIRECTORY")]
    confirm_delete_credentials: Option<PathBuf>,
    /// Confirm deletion of one exact catalog directory.
    #[arg(long, value_name = "DIRECTORY")]
    confirm_delete_catalogs: Option<PathBuf>,
    /// Confirm deletion of one exact portfolio directory.
    #[arg(long, value_name = "DIRECTORY")]
    confirm_delete_portfolios: Option<PathBuf>,
    /// Confirm deletion of one exact dataset directory.
    #[arg(long, value_name = "DIRECTORY")]
    confirm_delete_datasets: Option<PathBuf>,
    /// Confirm deletion of one exact model directory.
    #[arg(long, value_name = "DIRECTORY")]
    confirm_delete_models: Option<PathBuf>,
    /// Confirm deletion of one exact log directory.
    #[arg(long, value_name = "DIRECTORY")]
    confirm_delete_logs: Option<PathBuf>,
    /// Confirm deletion of one exact artifact directory.
    #[arg(long, value_name = "DIRECTORY")]
    confirm_delete_artifacts: Option<PathBuf>,
}

async fn execute(cli: Cli) -> Result<(), CommandError> {
    let root = match cli.root {
        Some(root) => root,
        None => default_install_root()?,
    };
    match cli.command {
        InstallerCommand::Install(arguments) => {
            let receipt = match (arguments.manifest_url, arguments.manifest, arguments.bundle) {
                (Some(url), None, None) => {
                    let downloaded = download_release(&url, &root).await?;
                    install(
                        InstallRequest::from_local(
                            root,
                            &downloaded.manifest,
                            downloaded.bundle.path(),
                        )?
                        .with_channel_manifest_url(&url)?,
                    )?
                }
                (None, Some(manifest), Some(bundle)) => {
                    let bytes = read_manifest(&manifest)?;
                    install(InstallRequest::from_local(root, &bytes, &bundle)?)?
                }
                _ => return Err(CommandError::InstallSource),
            };
            output(cli.json, "installed", &receipt)?;
        }
        InstallerCommand::Update => {
            let current = status(&root)?;
            let url = current
                .channel_manifest_url()
                .ok_or(CommandError::UpdateChannel)?;
            let downloaded = download_release(url, &root).await?;
            let receipt = update(
                UpdateRequest::from_local(root, &downloaded.manifest, downloaded.bundle.path())?
                    .with_channel_manifest_url(url)?,
            )?;
            output(cli.json, "updated", &receipt)?;
        }
        InstallerCommand::Repair => {
            let receipt = repair(RepairRequest::new(root))?;
            output(cli.json, "verified", &receipt)?;
        }
        InstallerCommand::Rollback => {
            let receipt = rollback(RollbackRequest::new(root))?;
            output(cli.json, "rolled back", &receipt)?;
        }
        InstallerCommand::Uninstall(arguments) => {
            let receipt = uninstall(uninstall_request(root, arguments))?;
            output(cli.json, "uninstalled", &receipt)?;
        }
        InstallerCommand::Status => {
            let current = status(&root)?;
            output(cli.json, "status", &current)?;
        }
        InstallerCommand::Launch { program } => {
            let executable = resolve_program(&root, program)?;
            let exit = ProcessCommand::new(executable)
                .status()
                .map_err(CommandError::Launch)?;
            if !exit.success() {
                return Err(CommandError::ProgramExit(exit.code()));
            }
        }
    }
    Ok(())
}

fn uninstall_request(root: PathBuf, arguments: UninstallArguments) -> UninstallRequest {
    let mut request = UninstallRequest::preserving_data(root);
    for (class, path) in [
        (
            MutableDataClass::Configuration,
            arguments.confirm_delete_configuration,
        ),
        (
            MutableDataClass::Credentials,
            arguments.confirm_delete_credentials,
        ),
        (
            MutableDataClass::Catalogs,
            arguments.confirm_delete_catalogs,
        ),
        (
            MutableDataClass::Portfolios,
            arguments.confirm_delete_portfolios,
        ),
        (
            MutableDataClass::Datasets,
            arguments.confirm_delete_datasets,
        ),
        (MutableDataClass::Models, arguments.confirm_delete_models),
        (MutableDataClass::Logs, arguments.confirm_delete_logs),
        (
            MutableDataClass::Artifacts,
            arguments.confirm_delete_artifacts,
        ),
    ] {
        if let Some(path) = path {
            request = request.confirm_delete(class, path);
        }
    }
    request
}

#[derive(Debug)]
struct DownloadedRelease {
    manifest: Vec<u8>,
    bundle: NamedTempFile,
}

async fn download_release(
    manifest_url: &str,
    install_root: &Path,
) -> Result<DownloadedRelease, CommandError> {
    let manifest_url = admitted_manifest_url(manifest_url)?;
    install_tls_provider()?;
    let client = reqwest::Client::builder()
        .user_agent(concat!(
            "market-squawk-installer/",
            env!("CARGO_PKG_VERSION")
        ))
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_TIMEOUT)
        .timeout(TOTAL_TIMEOUT)
        .redirect(Policy::custom(|attempt| {
            let url = attempt.url();
            if attempt.previous().len() >= MAXIMUM_REDIRECTS
                || url.scheme() != "https"
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                attempt.error("unsafe or excessive release redirect")
            } else {
                attempt.follow()
            }
        }))
        .build()
        .map_err(CommandError::Network)?;

    let manifest_response = client
        .get(manifest_url)
        .send()
        .await
        .map_err(CommandError::Network)?
        .error_for_status()
        .map_err(CommandError::Network)?;
    let manifest = collect_bounded_response(manifest_response, MAXIMUM_MANIFEST_BYTES).await?;
    let release = ReleaseManifest::admit_current(&manifest)?;
    let archive_url = admitted_https_url(&release.target_release().archive.url)?;

    let parent = install_root.parent().ok_or(CommandError::DownloadRoot)?;
    fs::create_dir_all(parent).map_err(CommandError::Io)?;
    let mut bundle = tempfile::Builder::new()
        .prefix(".market-squawk-download-")
        .suffix(".zip")
        .tempfile_in(parent)
        .map_err(CommandError::Io)?;
    let response = client
        .get(archive_url)
        .send()
        .await
        .map_err(CommandError::Network)?
        .error_for_status()
        .map_err(CommandError::Network)?;
    if response
        .content_length()
        .is_some_and(|length| length != release.target_release().archive.size)
    {
        return Err(CommandError::DownloadIdentity);
    }
    let mut stream = response.bytes_stream();
    let mut total = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(CommandError::Network)?;
        total = total
            .checked_add(u64::try_from(chunk.len()).map_err(|_| CommandError::DownloadIdentity)?)
            .ok_or(CommandError::DownloadIdentity)?;
        if total > release.target_release().archive.size || total > MAXIMUM_ARCHIVE_BYTES {
            return Err(CommandError::DownloadIdentity);
        }
        bundle.write_all(&chunk).map_err(CommandError::Io)?;
    }
    if total != release.target_release().archive.size {
        return Err(CommandError::DownloadIdentity);
    }
    bundle.as_file_mut().sync_all().map_err(CommandError::Io)?;
    Ok(DownloadedRelease { manifest, bundle })
}

async fn collect_bounded_response(
    response: reqwest::Response,
    maximum: usize,
) -> Result<Vec<u8>, CommandError> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return Err(CommandError::DownloadIdentity);
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(CommandError::Network)?;
        if bytes.len().saturating_add(chunk.len()) > maximum {
            return Err(CommandError::DownloadIdentity);
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.is_empty() {
        return Err(CommandError::DownloadIdentity);
    }
    Ok(bytes)
}

fn admitted_https_url(value: &str) -> Result<Url, CommandError> {
    let url = Url::parse(value).map_err(|_| CommandError::DownloadUrl)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(CommandError::DownloadUrl);
    }
    Ok(url)
}

fn admitted_manifest_url(value: &str) -> Result<Url, CommandError> {
    let url = admitted_https_url(value)?;
    let latest = "/Sawmonabo/market-squawk/releases/latest/download/market-squawk-release.json";
    if url.host_str() != Some("github.com")
        || url.query().is_some()
        || (url.path() != latest && !is_versioned_manifest_path(url.path()))
    {
        return Err(CommandError::DownloadUrl);
    }
    Ok(url)
}

fn is_versioned_manifest_path(path: &str) -> bool {
    const PREFIX: &str = "/Sawmonabo/market-squawk/releases/download/";
    const MANIFEST: &str = "market-squawk-release.json";

    let Some(remainder) = path.strip_prefix(PREFIX) else {
        return false;
    };
    let Some((tag, asset)) = remainder.split_once('/') else {
        return false;
    };
    let Some(version_text) = tag.strip_prefix('v') else {
        return false;
    };
    asset == MANIFEST
        && Version::parse(version_text).is_ok_and(|version| version_text == version.to_string())
}

fn install_tls_provider() -> Result<(), CommandError> {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return Ok(());
    }
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| CommandError::TlsProvider)
}

fn read_manifest(path: &Path) -> Result<Vec<u8>, CommandError> {
    let metadata = fs::symlink_metadata(path).map_err(CommandError::Io)?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MAXIMUM_MANIFEST_BYTES as u64
    {
        return Err(CommandError::DownloadIdentity);
    }
    fs::read(path).map_err(CommandError::Io)
}

fn output<T: Serialize>(json: bool, action: &str, value: &T) -> Result<(), CommandError> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(value).map_err(CommandError::Json)?
        );
    } else {
        println!("Market Squawk {action}.");
    }
    Ok(())
}

/// Installer command, retrieval, or fixed-program launch failure.
#[derive(Debug, Error)]
pub enum CommandError {
    #[error("choose either --manifest-url or both --manifest and --bundle")]
    InstallSource,
    #[error("the installed release has no retained HTTPS update channel")]
    UpdateChannel,
    #[error("release URL must be an uncredentialed HTTPS URL without a fragment")]
    DownloadUrl,
    #[error("release download size or identity is invalid")]
    DownloadIdentity,
    #[error("the installation root has no usable parent directory")]
    DownloadRoot,
    #[error("failed to install the process TLS provider")]
    TlsProvider,
    #[error("release network request failed")]
    Network(#[source] reqwest::Error),
    #[error("installer filesystem operation failed")]
    Io(#[source] std::io::Error),
    #[error("failed to encode installer output")]
    Json(#[source] serde_json::Error),
    #[error("failed to launch the fixed installed program")]
    Launch(#[source] std::io::Error),
    #[error("installed program exited unsuccessfully: {0:?}")]
    ProgramExit(Option<i32>),
    #[error(transparent)]
    Lifecycle(#[from] InstallError),
    #[error(transparent)]
    Manifest(#[from] crate::manifest::ManifestError),
    #[error(transparent)]
    Platform(#[from] crate::platform::PlatformError),
}
