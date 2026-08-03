//! Immutable installed-release update-channel admission.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Read as _;
use std::path::Path;

use cap_fs_ext::{DirExt as _, FollowSymlinks, MetadataExt as _, OpenOptionsFollowExt as _};
use cap_std::ambient_authority;
use cap_std::fs::{Dir, Metadata, OpenOptions};
use market_squawk_installer::{
    ProgramName, SupportedTarget, TrustedRoot, active_release_root_for_installed_program,
    installation_root_for_installed_program,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use url::Url;

const UPDATE_DIRECTORY: [&str; 3] = ["share", "market-squawk", "update"];
const CHANNEL_FILE: &str = "channel.json";
const PINNED_ROOT_FILE: &str = "1.root.json";
const CHANNEL_SCHEMA_VERSION: u16 = 1;
const MAXIMUM_UPDATE_METADATA_BYTES: u64 = 1024 * 1024;

/// Package-owned update input admitted from the active immutable release.
#[derive(Debug)]
pub(crate) enum InstalledUpdatePackage {
    /// A verified repository origin, public root, and platform target selection are available.
    Available(AvailableInstalledUpdatePackage),
    /// Updates are truthfully unavailable for this execution or package.
    Unavailable(InstalledUpdateUnavailable),
}

impl InstalledUpdatePackage {
    /// Loads only the fixed update contract belonging to the currently executing service.
    ///
    /// Source and development executions return a typed unavailable state. An installed release
    /// with a missing, partial, altered, or invalid contract fails closed.
    pub(crate) fn load() -> Result<Self, InstalledUpdatePackageError> {
        let executable = std::env::current_exe()
            .map_err(|source| InstalledUpdatePackageError::CurrentExecutable { source })?;
        let Some(release_root) =
            active_release_root_for_installed_program(&executable, ProgramName::Service)
                .map_err(|_source| InstalledUpdatePackageError::InstalledReleaseInvalid)?
        else {
            return Ok(Self::Unavailable(
                InstalledUpdateUnavailable::SourceOrDevelopmentExecution,
            ));
        };
        let target = SupportedTarget::current()
            .map_err(|_source| InstalledUpdatePackageError::UnsupportedPlatform)?;
        load_from_verified_release(&executable, &release_root, target)
    }
}

/// Fully validated material needed to compose the existing managed-update consumer.
pub(crate) struct AvailableInstalledUpdatePackage {
    install_root: std::path::PathBuf,
    repository: ValidatedUpdateRepositoryInput,
    minimum_workspace_schema_version: u64,
    maximum_workspace_schema_version: u64,
}

impl AvailableInstalledUpdatePackage {
    /// Returns the verified installation root owning the active immutable release.
    pub(crate) fn install_root(&self) -> &Path {
        &self.install_root
    }

    /// Returns the validated repository input without performing network access.
    pub(crate) const fn repository(&self) -> &ValidatedUpdateRepositoryInput {
        &self.repository
    }

    /// Returns the oldest workspace schema accepted by this update channel.
    pub(crate) const fn minimum_workspace_schema_version(&self) -> u64 {
        self.minimum_workspace_schema_version
    }

    /// Returns the newest workspace schema accepted by this update channel.
    pub(crate) const fn maximum_workspace_schema_version(&self) -> u64 {
        self.maximum_workspace_schema_version
    }
}

impl fmt::Debug for AvailableInstalledUpdatePackage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AvailableInstalledUpdatePackage")
            .field("repository", &self.repository)
            .field(
                "minimum_workspace_schema_version",
                &self.minimum_workspace_schema_version,
            )
            .field(
                "maximum_workspace_schema_version",
                &self.maximum_workspace_schema_version,
            )
            .finish()
    }
}

/// Closed, already-admitted arguments for `TrustedUpdateRepository` construction.
pub(crate) struct ValidatedUpdateRepositoryInput {
    base_url: Url,
    pinned_root: Box<[u8]>,
    manifest_target_path: Box<str>,
    archive_target_path: Box<str>,
}

impl ValidatedUpdateRepositoryInput {
    /// Returns the exact admitted HTTPS repository directory.
    pub(crate) const fn base_url(&self) -> &Url {
        &self.base_url
    }

    /// Returns the threshold-verified packaged public root bytes.
    pub(crate) fn pinned_root(&self) -> &[u8] {
        &self.pinned_root
    }

    /// Returns the fixed signed manifest target for this release platform.
    pub(crate) fn manifest_target_path(&self) -> &str {
        &self.manifest_target_path
    }

    /// Returns the fixed signed archive target for this release platform.
    pub(crate) fn archive_target_path(&self) -> &str {
        &self.archive_target_path
    }
}

impl fmt::Debug for ValidatedUpdateRepositoryInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedUpdateRepositoryInput")
            .field("base_url", &self.base_url)
            .field("pinned_root", &"[VERIFIED PUBLIC TRUST ROOT]")
            .field("manifest_target_path", &self.manifest_target_path)
            .field("archive_target_path", &self.archive_target_path)
            .finish()
    }
}

/// Why the immutable package does not provide update authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InstalledUpdateUnavailable {
    /// The service is running from a source or development build, not an installed release.
    SourceOrDevelopmentExecution,
    /// This valid release was built without production update-signing material.
    ProductionSigningMaterialUnavailable,
}

/// Installed update-package discovery or admission failure.
#[derive(Debug, Error)]
pub(crate) enum InstalledUpdatePackageError {
    /// The operating system did not expose the current service executable.
    #[error("the current service executable is unavailable")]
    CurrentExecutable {
        /// Path-redacted operating-system failure.
        #[source]
        source: std::io::Error,
    },
    /// A claimed installation or its immutable active release failed revalidation.
    #[error("the active installed release is invalid")]
    InstalledReleaseInvalid,
    /// The current operating system and architecture have no V1 package contract.
    #[error("the current release platform is unsupported")]
    UnsupportedPlatform,
    /// The fixed package path contained a symlink, reparse point, special file, or replacement.
    #[error("the installed update package path is unsafe")]
    UnsafePackagePath,
    /// A required package component was missing or could not be read under its fixed bound.
    #[error("the installed update package is incomplete")]
    IncompletePackage,
    /// The channel descriptor did not match the release builder's exact schema.
    #[error("the installed update channel descriptor is invalid")]
    InvalidChannelDescriptor,
    /// The pinned public root did not match its exact length, digest, or trust contract.
    #[error("the installed update public root is invalid")]
    InvalidPinnedRoot,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "availability")]
enum ChannelDocument {
    #[serde(rename = "available")]
    Available(AvailableChannelDocument),
    #[serde(rename = "unavailable")]
    Unavailable(UnavailableChannelDocument),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AvailableChannelDocument {
    schema_version: u16,
    minimum_workspace_schema_version: u64,
    maximum_workspace_schema_version: u64,
    pinned_root: PinnedRootDocument,
    repository_base_url: Box<str>,
    targets: BTreeMap<Box<str>, TargetSelectionDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UnavailableChannelDocument {
    schema_version: u16,
    reason: UnavailableReason,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum UnavailableReason {
    #[serde(rename = "production-signing-material-unavailable")]
    ProductionSigningMaterialUnavailable,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PinnedRootDocument {
    path: Box<str>,
    sha256: Box<str>,
    size: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TargetSelectionDocument {
    archive_target_path: Box<str>,
    manifest_target_path: Box<str>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootRoutingEnvelope {
    signed: RootRouting,
    signatures: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct RootRouting {
    version: u64,
    consistent_snapshot: bool,
}

fn load_from_verified_release(
    executable: &Path,
    release_root: &Path,
    target: SupportedTarget,
) -> Result<InstalledUpdatePackage, InstalledUpdatePackageError> {
    let update_directory = open_update_directory(release_root)?;
    let channel_bytes = read_bounded_required(&update_directory, CHANNEL_FILE)?;
    let channel: ChannelDocument = serde_json::from_slice(&channel_bytes)
        .map_err(|_source| InstalledUpdatePackageError::InvalidChannelDescriptor)?;
    let package = match channel {
        ChannelDocument::Unavailable(document) => {
            if document.schema_version != CHANNEL_SCHEMA_VERSION
                || document.reason != UnavailableReason::ProductionSigningMaterialUnavailable
                || fixed_file_exists(&update_directory, PINNED_ROOT_FILE)?
            {
                return Err(InstalledUpdatePackageError::InvalidChannelDescriptor);
            }
            InstalledUpdatePackage::Unavailable(
                InstalledUpdateUnavailable::ProductionSigningMaterialUnavailable,
            )
        }
        ChannelDocument::Available(document) => {
            let install_root =
                installation_root_for_installed_program(executable, ProgramName::Service)
                    .map_err(|_source| InstalledUpdatePackageError::InstalledReleaseInvalid)?
                    .ok_or(InstalledUpdatePackageError::InstalledReleaseInvalid)?;
            InstalledUpdatePackage::Available(admit_available(
                &update_directory,
                target,
                install_root,
                document,
            )?)
        }
    };

    let revalidated = active_release_root_for_installed_program(executable, ProgramName::Service)
        .map_err(|_source| InstalledUpdatePackageError::InstalledReleaseInvalid)?;
    if revalidated.as_deref() != Some(release_root) {
        return Err(InstalledUpdatePackageError::InstalledReleaseInvalid);
    }
    Ok(package)
}

fn admit_available(
    update_directory: &Dir,
    target: SupportedTarget,
    install_root: std::path::PathBuf,
    document: AvailableChannelDocument,
) -> Result<AvailableInstalledUpdatePackage, InstalledUpdatePackageError> {
    if document.schema_version != CHANNEL_SCHEMA_VERSION
        || document.minimum_workspace_schema_version == 0
        || document.minimum_workspace_schema_version > document.maximum_workspace_schema_version
        || document.pinned_root.path.as_ref() != PINNED_ROOT_FILE
        || document.pinned_root.size == 0
        || document.pinned_root.size > MAXIMUM_UPDATE_METADATA_BYTES
        || !is_lower_sha256(&document.pinned_root.sha256)
        || !exact_target_set(&document.targets)
    {
        return Err(InstalledUpdatePackageError::InvalidChannelDescriptor);
    }

    let root = read_bounded_required(update_directory, PINNED_ROOT_FILE)?;
    if u64::try_from(root.len()).ok() != Some(document.pinned_root.size)
        || sha256_hex(&root) != document.pinned_root.sha256.as_ref()
    {
        return Err(InstalledUpdatePackageError::InvalidPinnedRoot);
    }
    let routing: RootRoutingEnvelope = serde_json::from_slice(&root)
        .map_err(|_source| InstalledUpdatePackageError::InvalidPinnedRoot)?;
    if routing.signed.version != 1
        || !routing.signed.consistent_snapshot
        || routing.signatures.is_empty()
    {
        return Err(InstalledUpdatePackageError::InvalidPinnedRoot);
    }
    TrustedRoot::from_pinned(&root)
        .map_err(|_source| InstalledUpdatePackageError::InvalidPinnedRoot)?;

    let base_url = Url::parse(&document.repository_base_url)
        .map_err(|_source| InstalledUpdatePackageError::InvalidChannelDescriptor)?;
    if base_url.scheme() != "https"
        || base_url.host_str().is_none()
        || !base_url.username().is_empty()
        || base_url.password().is_some()
        || base_url.query().is_some()
        || base_url.fragment().is_some()
        || !base_url.path().ends_with('/')
        || base_url.path().starts_with("//")
    {
        return Err(InstalledUpdatePackageError::InvalidChannelDescriptor);
    }

    let selection = document
        .targets
        .get(target.as_str())
        .ok_or(InstalledUpdatePackageError::InvalidChannelDescriptor)?;
    let expected = expected_target_selection(target);
    if selection.archive_target_path.as_ref() != expected.0
        || selection.manifest_target_path.as_ref() != expected.1
    {
        return Err(InstalledUpdatePackageError::InvalidChannelDescriptor);
    }

    Ok(AvailableInstalledUpdatePackage {
        install_root,
        repository: ValidatedUpdateRepositoryInput {
            base_url,
            pinned_root: root,
            manifest_target_path: selection.manifest_target_path.clone(),
            archive_target_path: selection.archive_target_path.clone(),
        },
        minimum_workspace_schema_version: document.minimum_workspace_schema_version,
        maximum_workspace_schema_version: document.maximum_workspace_schema_version,
    })
}

fn exact_target_set(targets: &BTreeMap<Box<str>, TargetSelectionDocument>) -> bool {
    let expected = [
        SupportedTarget::Aarch64AppleDarwin,
        SupportedTarget::X86_64AppleDarwin,
        SupportedTarget::X86_64PcWindowsMsvc,
        SupportedTarget::X86_64UnknownLinuxGnu,
    ];
    targets.len() == expected.len()
        && expected.into_iter().all(|target| {
            let Some(selection) = targets.get(target.as_str()) else {
                return false;
            };
            let expected = expected_target_selection(target);
            selection.archive_target_path.as_ref() == expected.0
                && selection.manifest_target_path.as_ref() == expected.1
        })
}

fn expected_target_selection(target: SupportedTarget) -> (String, String) {
    (
        format!("channels/stable/{}/bundle.zip", target.as_str()),
        format!("channels/stable/{}/manifest.json", target.as_str()),
    )
}

fn open_update_directory(release_root: &Path) -> Result<Dir, InstalledUpdatePackageError> {
    if !release_root.is_absolute() {
        return Err(InstalledUpdatePackageError::UnsafePackagePath);
    }
    let named_root = std::fs::symlink_metadata(release_root)
        .map_err(|_source| InstalledUpdatePackageError::IncompletePackage)?;
    if !named_root.is_dir() || named_root.file_type().is_symlink() {
        return Err(InstalledUpdatePackageError::UnsafePackagePath);
    }
    let mut directory = Dir::open_ambient_dir(release_root, ambient_authority())
        .map_err(|_source| InstalledUpdatePackageError::IncompletePackage)?;
    let opened_root = directory
        .dir_metadata()
        .map_err(|_source| InstalledUpdatePackageError::IncompletePackage)?;
    if !opened_root.is_dir() || is_reparse_point(&opened_root) {
        return Err(InstalledUpdatePackageError::UnsafePackagePath);
    }

    for component in UPDATE_DIRECTORY {
        let named = directory
            .symlink_metadata(component)
            .map_err(|_source| InstalledUpdatePackageError::IncompletePackage)?;
        if !named.is_dir() || named.file_type().is_symlink() || is_reparse_point(&named) {
            return Err(InstalledUpdatePackageError::UnsafePackagePath);
        }
        let next = directory
            .open_dir_nofollow(component)
            .map_err(|_source| InstalledUpdatePackageError::UnsafePackagePath)?;
        let opened = next
            .dir_metadata()
            .map_err(|_source| InstalledUpdatePackageError::IncompletePackage)?;
        if !opened.is_dir()
            || is_reparse_point(&opened)
            || FileIdentity::from_metadata(&opened) != FileIdentity::from_metadata(&named)
        {
            return Err(InstalledUpdatePackageError::UnsafePackagePath);
        }
        directory = next;
    }
    Ok(directory)
}

fn read_bounded_required(
    directory: &Dir,
    name: &str,
) -> Result<Box<[u8]>, InstalledUpdatePackageError> {
    let named = directory
        .symlink_metadata(name)
        .map_err(|_source| InstalledUpdatePackageError::IncompletePackage)?;
    if !named.is_file()
        || named.file_type().is_symlink()
        || is_reparse_point(&named)
        || named.len() == 0
        || named.len() > MAXIMUM_UPDATE_METADATA_BYTES
    {
        return Err(InstalledUpdatePackageError::UnsafePackagePath);
    }
    let identity = FileIdentity::from_metadata(&named);
    let size = usize::try_from(named.len())
        .map_err(|_source| InstalledUpdatePackageError::IncompletePackage)?;
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    configure_windows_nofollow(&mut options);
    let mut file = directory
        .open_with(name, &options)
        .map_err(|_source| InstalledUpdatePackageError::UnsafePackagePath)?;
    let opened = file
        .metadata()
        .map_err(|_source| InstalledUpdatePackageError::IncompletePackage)?;
    if !opened.is_file()
        || is_reparse_point(&opened)
        || FileIdentity::from_metadata(&opened) != identity
    {
        return Err(InstalledUpdatePackageError::UnsafePackagePath);
    }

    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_source| InstalledUpdatePackageError::IncompletePackage)?;
    bytes.resize(size, 0);
    file.read_exact(&mut bytes)
        .map_err(|_source| InstalledUpdatePackageError::IncompletePackage)?;
    let mut trailing = [0_u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|_source| InstalledUpdatePackageError::IncompletePackage)?
        != 0
    {
        return Err(InstalledUpdatePackageError::UnsafePackagePath);
    }
    let revalidated = directory
        .symlink_metadata(name)
        .map_err(|_source| InstalledUpdatePackageError::IncompletePackage)?;
    if FileIdentity::from_metadata(&revalidated) != identity {
        return Err(InstalledUpdatePackageError::UnsafePackagePath);
    }
    Ok(bytes.into_boxed_slice())
}

fn fixed_file_exists(directory: &Dir, name: &str) -> Result<bool, InstalledUpdatePackageError> {
    match directory.symlink_metadata(name) {
        Ok(_metadata) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_source) => Err(InstalledUpdatePackageError::IncompletePackage),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    length: u64,
}

impl FileIdentity {
    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
        }
    }
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut value = String::with_capacity(64);
    for byte in digest {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

#[cfg(windows)]
fn is_reparse_point(metadata: &Metadata) -> bool {
    use cap_std::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
const fn is_reparse_point(_metadata: &Metadata) -> bool {
    false
}

#[cfg(windows)]
fn configure_windows_nofollow(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options
        .share_mode(FILE_SHARE_READ)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(windows))]
fn configure_windows_nofollow(_options: &mut OpenOptions) {}
