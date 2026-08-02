//! Stable installer CLI and bounded HTTPS release retrieval.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use futures_util::StreamExt as _;
use market_squawk_runtime::{InstallationId, RuntimeIdentity, ServiceGeneration, WorkspaceId};
use reqwest::redirect::Policy;
use semver::Version;
use serde::Serialize;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::contracts::{
    InstallReceipt, InstallRequest, MutableDataClass, RepairRequest, RollbackRequest,
    UninstallRequest, UpdateRequest,
};
use crate::lifecycle::{
    InstallError, active_program_path, install, repair, rollback, status, uninstall, update,
};
use crate::manifest::{
    ComponentIdentity, ComponentRole, MAXIMUM_ARCHIVE_BYTES, MAXIMUM_ARCHIVE_ENTRIES,
    MAXIMUM_ENTRY_BYTES, MAXIMUM_MANIFEST_BYTES, ReleaseManifest,
};
use crate::platform::{NativeTrustMode, ProgramName, SupportedTarget, default_install_root};
use crate::service_registration::{
    RestartInstalledServiceRequest, installed_service_status, restart_installed_service,
    verify_installed_service,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(120);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MAXIMUM_REDIRECTS: usize = 10;
const HASH_BUFFER_BYTES: usize = 64 * 1024;
const MAXIMUM_MANIFEST_TREE_DEPTH: usize = 64;

/// Parses and executes one installer command.
///
/// # Errors
///
/// Returns [`CommandError`] for invalid source selection, download, lifecycle, serialization, or
/// fixed-program launch failure.
pub async fn run_cli() -> Result<(), CommandError> {
    execute(Cli::parse()).await
}

/// Downloads and activates the next complete release from the retained update channel.
///
/// # Errors
///
/// Returns [`CommandError`] when no channel is retained or the remote manifest, bundle, or
/// installation lifecycle fails admission.
pub async fn update_from_channel(root: &Path) -> Result<InstallReceipt, CommandError> {
    let current = status(root)?;
    let url = current
        .channel_manifest_url()
        .ok_or(CommandError::UpdateChannel)?
        .to_owned();
    let downloaded = download_release(&url, root).await?;
    Ok(update(
        UpdateRequest::from_local(
            root.to_path_buf(),
            &downloaded.manifest,
            downloaded.bundle.path(),
        )?
        .with_channel_manifest_url(&url)?,
    )?)
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
    /// Install from an immutable HTTPS manifest or exact local files.
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
    /// Inspect or restart only the exact installer-owned per-user service registration.
    Service {
        #[command(subcommand)]
        command: ServiceControlCommand,
    },
    /// Build release metadata from one closed native staging tree.
    Manifest {
        #[command(subcommand)]
        command: ManifestCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ServiceControlCommand {
    /// Verify owned registration and authenticated current health.
    Status,
    /// Verify owned registration against one exact expected runtime generation.
    Verify(ExpectedRuntimeArguments),
    /// Restart the owned registration and require the exact next runtime generation.
    Restart(ExpectedRuntimeArguments),
}

#[derive(Debug, Args)]
struct ExpectedRuntimeArguments {
    /// Exact stable installation identity authenticated by the currently running service.
    #[arg(long)]
    installation_id: Uuid,
    /// Exact active workspace identity authenticated by the currently running service.
    #[arg(long)]
    workspace_id: Uuid,
    /// Exact current one-based service generation.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    service_generation: u64,
}

impl ExpectedRuntimeArguments {
    fn runtime(self) -> Result<RuntimeIdentity, CommandError> {
        RuntimeIdentity::try_new(
            InstallationId::try_from_uuid(self.installation_id)
                .map_err(|_| CommandError::ServiceIdentity)?,
            WorkspaceId::try_from_uuid(self.workspace_id)
                .map_err(|_| CommandError::ServiceIdentity)?,
            ServiceGeneration::try_new(self.service_generation)
                .map_err(|_| CommandError::ServiceIdentity)?,
        )
        .map_err(|_| CommandError::ServiceIdentity)
    }
}

#[derive(Debug, Subcommand)]
enum ManifestCommand {
    /// Hash every staged component and write one current-target manifest.
    Build(ManifestBuildArguments),
}

#[derive(Debug, Args)]
struct ManifestBuildArguments {
    /// Exact product version; must match this installer binary.
    #[arg(long)]
    version: String,
    /// Exact lowercase Git commit object ID.
    #[arg(long)]
    commit: String,
    /// Exact lowercase Git tree object ID.
    #[arg(long)]
    tree: String,
    /// Deterministic RFC 3339 release time.
    #[arg(long)]
    generated_at: String,
    /// Closed staging directory represented by the archive.
    #[arg(long)]
    staging_root: PathBuf,
    /// Complete ZIP archive produced from the staging directory.
    #[arg(long)]
    bundle: PathBuf,
    /// Immutable GitHub Release URL for the complete ZIP archive.
    #[arg(long)]
    archive_url: String,
    /// Native publisher-trust evidence verified for this target.
    #[arg(long, value_enum)]
    native_trust_mode: NativeTrustMode,
    /// New manifest file to create.
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, Args)]
struct InstallArguments {
    /// Immutable HTTPS release-manifest URL used for this installation.
    #[arg(long, conflicts_with_all = ["manifest", "bundle"])]
    manifest_url: Option<String>,
    /// Moving HTTPS manifest URL retained for later updates.
    #[arg(long, requires = "manifest_url")]
    channel_manifest_url: Option<String>,
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
    let Cli {
        root,
        json,
        command,
    } = cli;
    let command = match command {
        InstallerCommand::Manifest { command } => {
            match command {
                ManifestCommand::Build(arguments) => {
                    let receipt = build_release_manifest(arguments)?;
                    output(json, "built release manifest", &receipt)?;
                }
            }
            return Ok(());
        }
        command => command,
    };

    let root = match root {
        Some(root) => root,
        None => default_install_root()?,
    };
    match command {
        InstallerCommand::Install(arguments) => {
            let receipt = match (
                arguments.manifest_url,
                arguments.channel_manifest_url,
                arguments.manifest,
                arguments.bundle,
            ) {
                (Some(url), channel_url, None, None) => {
                    let downloaded = download_release(&url, &root).await?;
                    let channel_url = channel_url.as_deref().unwrap_or(&url);
                    install(
                        InstallRequest::from_local(
                            root.clone(),
                            &downloaded.manifest,
                            downloaded.bundle.path(),
                        )?
                        .with_channel_manifest_url(channel_url)?,
                    )?
                }
                (None, None, Some(manifest), Some(bundle)) => {
                    let bytes = read_manifest(&manifest)?;
                    install(InstallRequest::from_local(root.clone(), &bytes, &bundle)?)?
                }
                _ => return Err(CommandError::InstallSource),
            };
            output_install(json, &root, &receipt)?;
        }
        InstallerCommand::Update => {
            let receipt = update_from_channel(&root).await?;
            output(json, "updated", &receipt)?;
        }
        InstallerCommand::Repair => {
            let receipt = repair(RepairRequest::new(root))?;
            output(json, "verified", &receipt)?;
        }
        InstallerCommand::Rollback => {
            let receipt = rollback(RollbackRequest::new(root))?;
            output(json, "rolled back", &receipt)?;
        }
        InstallerCommand::Uninstall(arguments) => {
            let receipt = uninstall(uninstall_request(root, arguments))?;
            output(json, "uninstalled", &receipt)?;
        }
        InstallerCommand::Status => {
            let current = status(&root)?;
            output(json, "status", &current)?;
        }
        InstallerCommand::Launch { program } => {
            let executable = active_program_path(&root, program)?;
            let exit = ProcessCommand::new(executable)
                .status()
                .map_err(CommandError::Launch)?;
            if !exit.success() {
                return Err(CommandError::ProgramExit(exit.code()));
            }
        }
        InstallerCommand::Service { command } => match command {
            ServiceControlCommand::Status => {
                let status = installed_service_status(&root)?;
                output(json, "verified installed service", &status)?;
            }
            ServiceControlCommand::Verify(expected) => {
                let status = verify_installed_service(&root, expected.runtime()?)?;
                output(json, "verified expected installed service", &status)?;
            }
            ServiceControlCommand::Restart(expected) => {
                let request = RestartInstalledServiceRequest::new(root, expected.runtime()?);
                let status = restart_installed_service(request)?;
                output(json, "restarted installed service", &status)?;
            }
        },
        InstallerCommand::Manifest { .. } => return Err(CommandError::ManifestBuild),
    }
    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ManifestBuildReceipt {
    output: PathBuf,
    target: &'static str,
    component_count: usize,
    manifest_sha256: String,
}

fn build_release_manifest(
    arguments: ManifestBuildArguments,
) -> Result<ManifestBuildReceipt, CommandError> {
    if arguments.version != env!("CARGO_PKG_VERSION") {
        return Err(CommandError::ManifestBuild);
    }
    let target = SupportedTarget::current()?;
    if !arguments.native_trust_mode.supports(target) {
        return Err(CommandError::ManifestBuild);
    }
    let root = controlled_staging_root(&arguments.staging_root)?;
    let bundle = controlled_regular_file(&arguments.bundle, MAXIMUM_ARCHIVE_BYTES)?;
    let output = new_output_path(&arguments.output, &root)?;
    let components = staged_components(&root, target)?;
    let paths_before = components
        .iter()
        .map(|component| component.path.clone())
        .collect::<Vec<_>>();
    let paths_after = staged_paths(&root)?;
    if paths_before
        .iter()
        .map(|path| path.as_ref())
        .ne(paths_after.iter().map(String::as_str))
    {
        return Err(CommandError::ManifestBuild);
    }

    let bundle_size = fs::metadata(&bundle).map_err(CommandError::Io)?.len();
    let bundle_sha256 = stable_sha256_file(&bundle, MAXIMUM_ARCHIVE_BYTES)?;
    let value = json!({
        "schema_version": crate::manifest::MANIFEST_SCHEMA_VERSION,
        "product": "market-squawk",
        "version": arguments.version,
        "tag": format!("v{}", env!("CARGO_PKG_VERSION")),
        "repository": "Sawmonabo/market-squawk",
        "commit_sha": arguments.commit,
        "tree_sha": arguments.tree,
        "generated_at": arguments.generated_at,
        "targets": [{
            "target": target,
            "minimum_system": target.minimum_system(),
            "native_trust_mode": arguments.native_trust_mode,
            "archive": {
                "url": arguments.archive_url,
                "size": bundle_size,
                "sha256": bundle_sha256,
            },
            "components": components,
        }],
    });
    let mut encoded = serde_json::to_vec_pretty(&value).map_err(CommandError::Json)?;
    encoded.push(b'\n');
    ReleaseManifest::admit_current(&encoded)?;
    let manifest_sha256 = format!("{:x}", Sha256::digest(&encoded));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&output)
        .map_err(CommandError::Io)?;
    file.write_all(&encoded).map_err(CommandError::Io)?;
    file.sync_all().map_err(CommandError::Io)?;
    Ok(ManifestBuildReceipt {
        output,
        target: target.as_str(),
        component_count: paths_before.len(),
        manifest_sha256,
    })
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

fn controlled_staging_root(path: &Path) -> Result<PathBuf, CommandError> {
    let metadata = fs::symlink_metadata(path).map_err(CommandError::Io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(CommandError::ManifestBuild);
    }
    path.canonicalize().map_err(CommandError::Io)
}

fn controlled_regular_file(path: &Path, maximum: u64) -> Result<PathBuf, CommandError> {
    let metadata = fs::symlink_metadata(path).map_err(CommandError::Io)?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > maximum
    {
        return Err(CommandError::ManifestBuild);
    }
    path.canonicalize().map_err(CommandError::Io)
}

fn new_output_path(path: &Path, staging_root: &Path) -> Result<PathBuf, CommandError> {
    let name = path
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or(CommandError::ManifestBuild)?;
    let parent = path.parent().ok_or(CommandError::ManifestBuild)?;
    let parent = controlled_staging_root(parent)?;
    let output = parent.join(name);
    if output.starts_with(staging_root) {
        return Err(CommandError::ManifestBuild);
    }
    match fs::symlink_metadata(&output) {
        Ok(_) => return Err(CommandError::ManifestBuild),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(CommandError::Io(error)),
    }
    Ok(output)
}

fn staged_components(
    root: &Path,
    target: SupportedTarget,
) -> Result<Vec<ComponentIdentity>, CommandError> {
    staged_paths(root)?
        .into_iter()
        .map(|path| {
            let role = component_role(&path, target);
            let file = root.join(Path::new(&path));
            let metadata = fs::symlink_metadata(&file).map_err(CommandError::Io)?;
            let size = metadata.len();
            let sha256 = stable_sha256_file(&file, MAXIMUM_ENTRY_BYTES)?;
            Ok(ComponentIdentity {
                path: path.into(),
                role,
                size,
                sha256: sha256.into(),
                executable: file_is_executable(&metadata, role),
            })
        })
        .collect()
}

fn staged_paths(root: &Path) -> Result<Vec<String>, CommandError> {
    let mut directories = vec![root.to_path_buf()];
    let mut cursor = 0_usize;
    let mut paths = BTreeSet::new();
    while cursor < directories.len() {
        let directory = &directories[cursor];
        for entry in fs::read_dir(directory).map_err(CommandError::Io)? {
            let entry = entry.map_err(CommandError::Io)?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(CommandError::Io)?;
            if metadata.file_type().is_symlink() {
                return Err(CommandError::ManifestBuild);
            }
            let relative = path
                .strip_prefix(root)
                .map_err(|_| CommandError::ManifestBuild)?;
            if relative.components().count() > MAXIMUM_MANIFEST_TREE_DEPTH {
                return Err(CommandError::ManifestBuild);
            }
            if metadata.is_dir() {
                directories.push(path);
                continue;
            }
            if !metadata.file_type().is_file()
                || paths.len() >= MAXIMUM_ARCHIVE_ENTRIES
                || !paths.insert(portable_relative_path(relative)?)
            {
                return Err(CommandError::ManifestBuild);
            }
        }
        cursor = cursor.checked_add(1).ok_or(CommandError::ManifestBuild)?;
    }
    if paths.is_empty() {
        return Err(CommandError::ManifestBuild);
    }
    Ok(paths.into_iter().collect())
}

fn portable_relative_path(path: &Path) -> Result<String, CommandError> {
    let components = path
        .components()
        .map(|component| {
            let std::path::Component::Normal(value) = component else {
                return Err(CommandError::ManifestBuild);
            };
            value
                .to_str()
                .filter(|value| !value.is_empty())
                .ok_or(CommandError::ManifestBuild)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(components.join("/"))
}

fn component_role(path: &str, target: SupportedTarget) -> ComponentRole {
    if let Some(role) = ComponentRole::REQUIRED
        .into_iter()
        .find(|role| role.fixed_path(target).as_deref() == Some(path))
    {
        role
    } else if path.starts_with("desktop/") {
        ComponentRole::DesktopResource
    } else if path.starts_with("licenses/") {
        ComponentRole::License
    } else if path.starts_with("notices/") {
        ComponentRole::Notice
    } else {
        ComponentRole::PythonEnvironment
    }
}

#[cfg(unix)]
fn file_is_executable(metadata: &fs::Metadata, _role: ComponentRole) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(windows)]
fn file_is_executable(_metadata: &fs::Metadata, role: ComponentRole) -> bool {
    role.requires_executable()
}

fn stable_sha256_file(path: &Path, maximum: u64) -> Result<String, CommandError> {
    let named_before = fs::symlink_metadata(path).map_err(CommandError::Io)?;
    if !named_before.file_type().is_file()
        || named_before.file_type().is_symlink()
        || named_before.len() > maximum
    {
        return Err(CommandError::ManifestBuild);
    }
    let mut file = File::open(path).map_err(CommandError::Io)?;
    let opened_before = file.metadata().map_err(CommandError::Io)?;
    if !same_file_metadata(&named_before, &opened_before) {
        return Err(CommandError::ManifestBuild);
    }
    let mut digest = Sha256::new();
    let mut observed = 0_u64;
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer).map_err(CommandError::Io)?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(u64::try_from(read).map_err(|_| CommandError::ManifestBuild)?)
            .filter(|bytes| *bytes <= maximum)
            .ok_or(CommandError::ManifestBuild)?;
        digest.update(&buffer[..read]);
    }
    let opened_after = file.metadata().map_err(CommandError::Io)?;
    let named_after = fs::symlink_metadata(path).map_err(CommandError::Io)?;
    if observed != named_before.len()
        || !same_file_metadata(&named_before, &opened_after)
        || !same_file_metadata(&named_before, &named_after)
    {
        return Err(CommandError::ManifestBuild);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn same_file_metadata(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.file_type().is_file()
        && right.file_type().is_file()
        && left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
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
    let target = SupportedTarget::current()?;
    let latest = format!(
        "/Sawmonabo/market-squawk/releases/latest/download/market-squawk-release-{}.json",
        target.as_str()
    );
    if url.host_str() != Some("github.com")
        || url.query().is_some()
        || (url.path() != latest && !is_versioned_manifest_path(url.path(), target))
    {
        return Err(CommandError::DownloadUrl);
    }
    Ok(url)
}

fn is_versioned_manifest_path(path: &str, target: SupportedTarget) -> bool {
    const PREFIX: &str = "/Sawmonabo/market-squawk/releases/download/";

    let Some(remainder) = path.strip_prefix(PREFIX) else {
        return false;
    };
    let Some((tag, asset)) = remainder.split_once('/') else {
        return false;
    };
    let Some(version_text) = tag.strip_prefix('v') else {
        return false;
    };
    asset == format!("market-squawk-release-{}.json", target.as_str())
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

fn output_install(json: bool, _root: &Path, receipt: &InstallReceipt) -> Result<(), CommandError> {
    #[cfg(unix)]
    let entrypoints = if json {
        None
    } else {
        let cli = crate::lifecycle::stable_program_path(_root, ProgramName::Cli)?;
        let directory = cli.parent().ok_or(CommandError::DownloadRoot)?;
        let desktop = directory.join(
            ProgramName::Desktop
                .relative_path(SupportedTarget::current()?)
                .file_name()
                .ok_or(CommandError::DownloadRoot)?,
        );
        let installer = directory.join(
            ProgramName::Installer
                .relative_path(SupportedTarget::current()?)
                .file_name()
                .ok_or(CommandError::DownloadRoot)?,
        );
        Some((desktop, cli, installer))
    };
    output(json, "installed", receipt)?;
    #[cfg(unix)]
    if let Some((desktop, cli, installer)) = entrypoints {
        println!("Desktop: {}", desktop.display());
        println!("CLI: {}", cli.display());
        println!("Updates and repair: {}", installer.display());
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
    #[error("release manifest build input changed, escaped its boundary, or is inconsistent")]
    ManifestBuild,
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
    #[error("installed-service runtime identity is invalid")]
    ServiceIdentity,
    #[error(transparent)]
    ServiceRegistration(#[from] crate::service_registration::ServiceRegistrationError),
    #[error(transparent)]
    Lifecycle(#[from] InstallError),
    #[error(transparent)]
    Manifest(#[from] crate::manifest::ManifestError),
    #[error(transparent)]
    Platform(#[from] crate::platform::PlatformError),
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{SupportedTarget, admitted_manifest_url};

    #[test]
    fn manifest_download_uses_only_the_current_target_channel() -> Result<(), Box<dyn Error>> {
        let target = SupportedTarget::current()?.as_str();
        for url in [
            format!(
                "https://github.com/Sawmonabo/market-squawk/releases/latest/download/\
                 market-squawk-release-{target}.json"
            ),
            format!(
                "https://github.com/Sawmonabo/market-squawk/releases/download/v1.0.0/\
                 market-squawk-release-{target}.json"
            ),
        ] {
            admitted_manifest_url(&url)?;
        }
        assert!(
            admitted_manifest_url(
                "https://github.com/Sawmonabo/market-squawk/releases/latest/download/\
                 market-squawk-release.json"
            )
            .is_err()
        );
        Ok(())
    }
}
