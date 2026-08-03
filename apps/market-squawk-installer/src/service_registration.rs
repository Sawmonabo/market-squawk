//! Per-user installed-service registration and readiness authority.

#![cfg_attr(
    test,
    expect(
        dead_code,
        reason = "platform command paths are intentionally inert in deterministic library tests"
    )
)]

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "windows")]
mod windows;

use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::io::{Read, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use atomicwrites::{AllowOverwrite, AtomicFile};
use market_squawk_runtime::{RuntimeIdentity, ServiceGeneration};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::platform::{ProgramName, SupportedTarget};
use crate::store::InstallStore;

const RECEIPT_FILE: &str = "service-registration.json";
const RECEIPT_SCHEMA_VERSION: u32 = 2;
const REGISTRATION_OWNER: &str = "market-squawk-installer-v1";
const MAXIMUM_EXECUTABLE_BYTES: u64 = 768 * 1024 * 1024;
const MAXIMUM_RECEIPT_BYTES: usize = 64 * 1024;
const MAXIMUM_NATIVE_DOCUMENT_BYTES: usize = 64 * 1024;
const MAXIMUM_COMMAND_OUTPUT_BYTES: u64 = 1024 * 1024;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(20);
const HEALTH_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
const HEALTH_STATUS_TIMEOUT: Duration = Duration::from_secs(5);
const HEALTH_RESTART_TIMEOUT: Duration = Duration::from_secs(30);
const HEALTH_RETRY_INTERVAL: Duration = Duration::from_millis(100);
#[cfg(test)]
const TEST_HEALTH_FAILURE_MARKER: &[u8] = b"market-squawk-test-service-health-failure";

/// Exact immutable release binding used for registration, repair, and verification.
#[derive(Debug)]
pub(crate) struct RegistrationSpec<'a> {
    install_root: &'a Path,
    version_root: &'a Path,
    target: SupportedTarget,
    version: &'a str,
    manifest_sha256: &'a str,
}

impl<'a> RegistrationSpec<'a> {
    /// Admits one candidate registration specification.
    pub(crate) fn new(
        install_root: &'a Path,
        version_root: &'a Path,
        target: SupportedTarget,
        version: &'a str,
        manifest_sha256: &'a str,
    ) -> Result<Self, ServiceRegistrationError> {
        let current = SupportedTarget::current().map_err(|_| ServiceRegistrationError::Target)?;
        let parsed = Version::parse(version).map_err(|_| ServiceRegistrationError::Identity)?;
        if current != target
            || parsed.to_string() != version
            || !install_root.is_absolute()
            || !version_root.is_absolute()
            || !is_lower_sha256(manifest_sha256)
        {
            return Err(ServiceRegistrationError::Identity);
        }
        Ok(Self {
            install_root,
            version_root,
            target,
            version,
            manifest_sha256,
        })
    }
}

/// Durable exact registration evidence used to authorize later repair and removal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ServiceRegistrationReceipt {
    schema_version: u32,
    owner: Box<str>,
    platform_registration: Box<str>,
    target: Box<str>,
    version: Box<str>,
    manifest_sha256: Box<str>,
    service: ProgramIdentity,
    mcp_relay: ProgramIdentity,
    cli: ProgramIdentity,
    configuration_sha256: Box<str>,
    pending_configuration_sha256: Option<Box<str>>,
}

/// Verified owned registration and authenticated running-service identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstalledServiceStatus {
    platform_registration: Box<str>,
    target: Box<str>,
    version: Box<str>,
    manifest_sha256: Box<str>,
    runtime: RuntimeIdentity,
}

impl InstalledServiceStatus {
    /// Returns the fixed platform registration identity owned by this installation.
    #[must_use]
    pub fn platform_registration(&self) -> &str {
        &self.platform_registration
    }

    /// Returns the exact installed release version executed by the service.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns the canonical target triple bound by the active registration receipt.
    #[must_use]
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Returns the active release-manifest digest bound by the registration receipt.
    #[must_use]
    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    /// Returns the exact authenticated installation, workspace, and service generation.
    #[must_use]
    pub const fn runtime(&self) -> RuntimeIdentity {
        self.runtime
    }
}

/// Exact current-generation authorization for one bounded owned-service restart.
#[derive(Debug)]
pub struct RestartInstalledServiceRequest {
    root: PathBuf,
    expected_current: RuntimeIdentity,
}

impl RestartInstalledServiceRequest {
    /// Creates a restart request without accepting a command, executable, registration, or URL.
    #[must_use]
    pub fn new(root: PathBuf, expected_current: RuntimeIdentity) -> Self {
        Self {
            root,
            expected_current,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProgramIdentity {
    path: PathBuf,
    size: u64,
    sha256: Box<str>,
}

#[derive(Debug)]
struct RegistrationMaterial {
    receipt: ServiceRegistrationReceipt,
    service_path: PathBuf,
    release_root: PathBuf,
}

#[derive(Clone, Debug)]
pub(super) struct PreparedRegistration {
    pub(super) identity: &'static str,
    pub(super) document: Vec<u8>,
    pub(super) configuration_sha256: Box<str>,
}

#[derive(Clone, Debug)]
pub(super) struct NativeRegistrationSnapshot {
    pub(super) document: Vec<u8>,
    pub(super) configuration_sha256: Box<str>,
    pub(super) owned: bool,
}

/// Registers, starts, and proves one exact candidate service before activation succeeds.
pub(crate) fn activate_and_verify(
    spec: &RegistrationSpec<'_>,
) -> Result<ServiceRegistrationReceipt, ServiceRegistrationError> {
    let material = registration_material(spec)?;
    #[cfg(test)]
    {
        if file_contains_marker(&material.service_path, TEST_HEALTH_FAILURE_MARKER)? {
            return Err(ServiceRegistrationError::Health);
        }
        write_receipt(spec.install_root, &material.receipt)?;
        Ok(material.receipt)
    }
    #[cfg(not(test))]
    {
        activate_native(spec, material)
    }
}

/// Revalidates the receipt, native registration, active state, and authenticated service health.
pub(crate) fn verify(
    spec: &RegistrationSpec<'_>,
) -> Result<ServiceRegistrationReceipt, ServiceRegistrationError> {
    let material = registration_material(spec)?;
    let receipt = verify_owned_material(spec, &material)?;
    #[cfg(not(test))]
    probe_health(&material, None, HEALTH_STATUS_TIMEOUT)?;
    Ok(receipt)
}

fn verify_owned_material(
    spec: &RegistrationSpec<'_>,
    material: &RegistrationMaterial,
) -> Result<ServiceRegistrationReceipt, ServiceRegistrationError> {
    let receipt = read_receipt(spec.install_root)?.ok_or(ServiceRegistrationError::Receipt)?;
    #[cfg(test)]
    {
        if receipt != material.receipt {
            return Err(ServiceRegistrationError::Receipt);
        }
        if file_contains_marker(&material.service_path, TEST_HEALTH_FAILURE_MARKER)? {
            return Err(ServiceRegistrationError::Health);
        }
    }
    #[cfg(not(test))]
    {
        let desired = prepare_native(material)?;
        let mut expected = material.receipt.clone();
        expected.configuration_sha256 = desired.configuration_sha256.clone();
        if receipt != expected || receipt.pending_configuration_sha256.is_some() {
            return Err(ServiceRegistrationError::Receipt);
        }
        let current = inspect_native()?.ok_or(ServiceRegistrationError::RegistrationMissing)?;
        if !current.owned
            || current.configuration_sha256.as_ref() != desired.configuration_sha256.as_ref()
            || receipt.configuration_sha256.as_ref() != desired.configuration_sha256.as_ref()
        {
            return Err(ServiceRegistrationError::Conflict);
        }
        ensure_native_active()?;
    }
    Ok(receipt)
}

/// Revalidates the exact active installation, owned native registration, and authenticated health.
///
/// # Errors
///
/// Fails closed when the installation is absent or transitioning, any installed component or
/// registration evidence differs from its receipt, the native entry is inactive, or authenticated
/// readiness cannot be established within the fixed status deadline.
pub fn installed_service_status(
    root: &Path,
) -> Result<InstalledServiceStatus, ServiceRegistrationError> {
    with_current_registration(root, |spec, material| {
        let receipt = verify_owned_material(spec, material)?;
        let runtime = probe_health(material, None, HEALTH_STATUS_TIMEOUT)?;
        Ok(service_status(&receipt, runtime))
    })
}

/// Proves that owned registration health is bound to one exact expected runtime generation.
///
/// # Errors
///
/// Fails under the same conditions as [`installed_service_status`] and when the authenticated
/// installation, workspace, or service generation differs from `expected`.
pub fn verify_installed_service(
    root: &Path,
    expected: RuntimeIdentity,
) -> Result<InstalledServiceStatus, ServiceRegistrationError> {
    with_current_registration(root, |spec, material| {
        let receipt = verify_owned_material(spec, material)?;
        let runtime = probe_health(material, Some(expected), HEALTH_STATUS_TIMEOUT)?;
        Ok(service_status(&receipt, runtime))
    })
}

/// Restarts only the exact owned native registration and proves its deterministic next generation.
///
/// The current service must first authenticate as `request.expected_current`. The post-restart
/// service must retain that installation and workspace identity and advance the service generation
/// by exactly one. No caller-selected command, executable, registration identity, URL, or argument
/// is accepted.
///
/// # Errors
///
/// Fails before restart if the installation, receipt, native configuration, active process, or
/// authenticated current generation is inconsistent. Fails after the bounded native restart when
/// the exact next authenticated generation does not become ready within the fixed deadline.
pub fn restart_installed_service(
    request: RestartInstalledServiceRequest,
) -> Result<InstalledServiceStatus, ServiceRegistrationError> {
    let RestartInstalledServiceRequest {
        root,
        expected_current,
    } = request;
    let next_generation = expected_current
        .service_generation()
        .get()
        .checked_add(1)
        .ok_or(ServiceRegistrationError::Identity)?;
    let expected_next = RuntimeIdentity::try_new(
        expected_current.installation_id(),
        expected_current.workspace_id(),
        ServiceGeneration::try_new(next_generation)
            .map_err(|_| ServiceRegistrationError::Identity)?,
    )
    .map_err(|_| ServiceRegistrationError::Identity)?;
    with_current_registration(&root, |spec, material| {
        let receipt = verify_owned_material(spec, material)?;
        probe_health(material, Some(expected_current), HEALTH_STATUS_TIMEOUT)?;
        restart_native()?;
        let runtime = probe_health(material, Some(expected_next), HEALTH_RESTART_TIMEOUT)?;
        Ok(service_status(&receipt, runtime))
    })
}

fn with_current_registration<T>(
    root: &Path,
    operation: impl FnOnce(
        &RegistrationSpec<'_>,
        &RegistrationMaterial,
    ) -> Result<T, ServiceRegistrationError>,
) -> Result<T, ServiceRegistrationError> {
    let mut store =
        InstallStore::open_existing(root)?.ok_or(ServiceRegistrationError::RegistrationMissing)?;
    if store.load_pending_activation()?.is_some() {
        // Lifecycle status is the sole crash-recovery owner for an interrupted activation. Drop
        // this guard before invoking it, then reacquire the store lock and recheck the barrier.
        drop(store);
        crate::lifecycle::status(root).map_err(|_| ServiceRegistrationError::Installation)?;
        store = InstallStore::open_existing(root)?
            .ok_or(ServiceRegistrationError::RegistrationMissing)?;
        if store.load_pending_activation()?.is_some() {
            return Err(ServiceRegistrationError::Transition);
        }
    }
    let state = store
        .load_state()?
        .ok_or(ServiceRegistrationError::RegistrationMissing)?;
    let version_root = store.version_path(&state.active);
    crate::archive::verify_installed_tree(&version_root, &state.active.components)
        .map_err(|_| ServiceRegistrationError::Receipt)?;
    let spec = RegistrationSpec::new(
        root,
        &version_root,
        state.active.target,
        &state.active.version,
        &state.active.manifest_sha256,
    )?;
    let material = registration_material(&spec)?;
    operation(&spec, &material)
}

fn service_status(
    receipt: &ServiceRegistrationReceipt,
    runtime: RuntimeIdentity,
) -> InstalledServiceStatus {
    InstalledServiceStatus {
        platform_registration: receipt.platform_registration.clone(),
        target: receipt.target.clone(),
        version: receipt.version.clone(),
        manifest_sha256: receipt.manifest_sha256.clone(),
        runtime,
    }
}

/// Stops and removes only the exact registration owned by the stored receipt.
pub(crate) fn remove_owned(
    install_root: &Path,
    target: SupportedTarget,
) -> Result<bool, ServiceRegistrationError> {
    let current = SupportedTarget::current().map_err(|_| ServiceRegistrationError::Target)?;
    if current != target || !install_root.is_absolute() {
        return Err(ServiceRegistrationError::Target);
    }
    let Some(receipt) = read_receipt(install_root)? else {
        return Ok(false);
    };
    validate_receipt_identity(&receipt, target)?;
    #[cfg(not(test))]
    {
        if let Some(native) = inspect_native()? {
            let matches_receipt = native.configuration_sha256.as_ref()
                == receipt.configuration_sha256.as_ref()
                || receipt.pending_configuration_sha256.as_deref()
                    == Some(native.configuration_sha256.as_ref());
            if !native.owned || !matches_receipt {
                return Err(ServiceRegistrationError::Conflict);
            }
            remove_native(&native)?;
        }
    }
    remove_receipt(install_root)?;
    Ok(true)
}

#[cfg(not(test))]
fn activate_native(
    spec: &RegistrationSpec<'_>,
    mut material: RegistrationMaterial,
) -> Result<ServiceRegistrationReceipt, ServiceRegistrationError> {
    let desired = prepare_native(&material)?;
    material.receipt.configuration_sha256 = desired.configuration_sha256.clone();
    let prior_native = inspect_native()?;
    let prior_receipt = read_receipt(spec.install_root)?;
    authorize_replacement(prior_native.as_ref(), prior_receipt.as_ref())?;

    let mut pending_receipt = material.receipt.clone();
    pending_receipt.configuration_sha256 = prior_native.as_ref().map_or_else(
        || desired.configuration_sha256.clone(),
        |native| native.configuration_sha256.clone(),
    );
    pending_receipt.pending_configuration_sha256 = Some(desired.configuration_sha256.clone());
    write_receipt(spec.install_root, &pending_receipt)?;

    let attempt = apply_native(&desired)
        .and_then(|()| start_native())
        .and_then(|()| probe_health(&material, None, HEALTH_RESTART_TIMEOUT).map(|_| ()));
    if let Err(error) = attempt {
        restore_native(prior_native.as_ref(), &desired)?;
        restore_receipt(spec.install_root, prior_receipt.as_ref())?;
        return Err(error);
    }
    material.receipt.pending_configuration_sha256 = None;
    if let Err(error) = write_receipt(spec.install_root, &material.receipt) {
        restore_native(prior_native.as_ref(), &desired)?;
        restore_receipt(spec.install_root, prior_receipt.as_ref())?;
        return Err(error);
    }
    Ok(material.receipt)
}

#[cfg(not(test))]
fn authorize_replacement(
    native: Option<&NativeRegistrationSnapshot>,
    receipt: Option<&ServiceRegistrationReceipt>,
) -> Result<(), ServiceRegistrationError> {
    let Some(native) = native else {
        return Ok(());
    };
    if !native.owned {
        return Err(ServiceRegistrationError::Conflict);
    }
    let receipt = receipt.ok_or(ServiceRegistrationError::Conflict)?;
    validate_receipt_identity(
        receipt,
        SupportedTarget::current().map_err(|_| ServiceRegistrationError::Target)?,
    )?;
    if native.configuration_sha256.as_ref() != receipt.configuration_sha256.as_ref()
        && receipt.pending_configuration_sha256.as_deref()
            != Some(native.configuration_sha256.as_ref())
    {
        return Err(ServiceRegistrationError::Conflict);
    }
    Ok(())
}

fn registration_material(
    spec: &RegistrationSpec<'_>,
) -> Result<RegistrationMaterial, ServiceRegistrationError> {
    let named_root = fs::symlink_metadata(spec.version_root)
        .map_err(|source| ServiceRegistrationError::io("inspect candidate release", source))?;
    if named_root.file_type().is_symlink() || !named_root.is_dir() {
        return Err(ServiceRegistrationError::UnsafePath);
    }
    let canonical_install_root = fs::canonicalize(spec.install_root)
        .map_err(|source| ServiceRegistrationError::io("resolve program store", source))?;
    let canonical_root = fs::canonicalize(spec.version_root)
        .map_err(|source| ServiceRegistrationError::io("resolve candidate release", source))?;
    if canonical_root == canonical_install_root
        || !canonical_root.starts_with(canonical_install_root)
    {
        return Err(ServiceRegistrationError::UnsafePath);
    }
    let service = program_identity(&canonical_root, ProgramName::Service, spec.target)?;
    let mcp_relay = program_identity(&canonical_root, ProgramName::McpRelay, spec.target)?;
    let cli = program_identity(&canonical_root, ProgramName::Cli, spec.target)?;
    let receipt = ServiceRegistrationReceipt {
        schema_version: RECEIPT_SCHEMA_VERSION,
        owner: REGISTRATION_OWNER.into(),
        platform_registration: platform_registration_identity(spec.target).into(),
        target: spec.target.as_str().into(),
        version: spec.version.into(),
        manifest_sha256: spec.manifest_sha256.into(),
        service: service.clone(),
        mcp_relay,
        cli,
        configuration_sha256: empty_sha256().into(),
        pending_configuration_sha256: None,
    };
    Ok(RegistrationMaterial {
        receipt,
        service_path: service.path,
        release_root: canonical_root,
    })
}

fn program_identity(
    release_root: &Path,
    program: ProgramName,
    target: SupportedTarget,
) -> Result<ProgramIdentity, ServiceRegistrationError> {
    let path = release_root.join(program.relative_path(target));
    let metadata = fs::symlink_metadata(&path)
        .map_err(|source| ServiceRegistrationError::io("inspect registered program", source))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > MAXIMUM_EXECUTABLE_BYTES
    {
        return Err(ServiceRegistrationError::UnsafePath);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        if metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.permissions().mode() & 0o111 == 0
            || metadata.permissions().mode() & 0o022 != 0
        {
            return Err(ServiceRegistrationError::UnsafePath);
        }
    }
    let canonical = fs::canonicalize(&path)
        .map_err(|source| ServiceRegistrationError::io("resolve registered program", source))?;
    if canonical != path || !canonical.starts_with(release_root) {
        return Err(ServiceRegistrationError::UnsafePath);
    }
    Ok(ProgramIdentity {
        size: metadata.len(),
        sha256: stable_file_sha256(&canonical)?.into(),
        path: canonical,
    })
}

fn stable_file_sha256(path: &Path) -> Result<String, ServiceRegistrationError> {
    let before = fs::metadata(path)
        .map_err(|source| ServiceRegistrationError::io("inspect registered program", source))?;
    let first = hash_file(path, before.len())?;
    let second = hash_file(path, before.len())?;
    let after = fs::metadata(path)
        .map_err(|source| ServiceRegistrationError::io("reinspect registered program", source))?;
    if first != second
        || before.len() != after.len()
        || before.modified().ok() != after.modified().ok()
    {
        return Err(ServiceRegistrationError::Changed);
    }
    Ok(first)
}

fn hash_file(path: &Path, expected: u64) -> Result<String, ServiceRegistrationError> {
    let mut file = File::open(path)
        .map_err(|source| ServiceRegistrationError::io("open registered program", source))?;
    let mut hasher = Sha256::new();
    let mut remaining = expected;
    let mut buffer = [0_u8; 64 * 1024];
    while remaining > 0 {
        let maximum = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| ServiceRegistrationError::Identity)?;
        let read = file
            .read(&mut buffer[..maximum])
            .map_err(|source| ServiceRegistrationError::io("hash registered program", source))?;
        if read == 0 {
            return Err(ServiceRegistrationError::Changed);
        }
        hasher.update(&buffer[..read]);
        remaining = remaining
            .checked_sub(u64::try_from(read).map_err(|_| ServiceRegistrationError::Identity)?)
            .ok_or(ServiceRegistrationError::Identity)?;
    }
    let mut trailing = [0_u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|source| ServiceRegistrationError::io("finish registered program hash", source))?
        != 0
    {
        return Err(ServiceRegistrationError::Changed);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn receipt_path(root: &Path) -> PathBuf {
    root.join(RECEIPT_FILE)
}

fn read_receipt(
    root: &Path,
) -> Result<Option<ServiceRegistrationReceipt>, ServiceRegistrationError> {
    let path = receipt_path(root);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(ServiceRegistrationError::io(
                "inspect service receipt",
                source,
            ));
        }
    };
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > MAXIMUM_RECEIPT_BYTES as u64
    {
        return Err(ServiceRegistrationError::Receipt);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        if metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(ServiceRegistrationError::Receipt);
        }
    }
    let bytes = fs::read(&path)
        .map_err(|source| ServiceRegistrationError::io("read service receipt", source))?;
    let receipt: ServiceRegistrationReceipt =
        serde_json::from_slice(&bytes).map_err(|_| ServiceRegistrationError::Receipt)?;
    validate_receipt_identity(
        &receipt,
        SupportedTarget::current().map_err(|_| ServiceRegistrationError::Target)?,
    )?;
    Ok(Some(receipt))
}

fn write_receipt(
    root: &Path,
    receipt: &ServiceRegistrationReceipt,
) -> Result<(), ServiceRegistrationError> {
    validate_receipt_identity(
        receipt,
        SupportedTarget::current().map_err(|_| ServiceRegistrationError::Target)?,
    )?;
    let mut bytes =
        serde_json::to_vec_pretty(receipt).map_err(|_| ServiceRegistrationError::Receipt)?;
    bytes.push(b'\n');
    if bytes.len() > MAXIMUM_RECEIPT_BYTES {
        return Err(ServiceRegistrationError::Receipt);
    }
    let atomic = AtomicFile::new(receipt_path(root), AllowOverwrite);
    atomic
        .write(|file| {
            file.write_all(&bytes)?;
            file.sync_all()?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                file.set_permissions(fs::Permissions::from_mode(0o600))?;
            }
            Ok(())
        })
        .map_err(|error| {
            let source: std::io::Error = error.into();
            ServiceRegistrationError::io("publish service receipt", source)
        })?;
    sync_receipt_parent(root)
}

fn remove_receipt(root: &Path) -> Result<(), ServiceRegistrationError> {
    let path = receipt_path(root);
    fs::remove_file(path)
        .map_err(|source| ServiceRegistrationError::io("remove service receipt", source))?;
    sync_receipt_parent(root)
}

#[cfg(not(test))]
fn restore_receipt(
    root: &Path,
    receipt: Option<&ServiceRegistrationReceipt>,
) -> Result<(), ServiceRegistrationError> {
    match receipt {
        Some(receipt) => write_receipt(root, receipt),
        None => match fs::remove_file(receipt_path(root)) {
            Ok(()) => sync_receipt_parent(root),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(ServiceRegistrationError::io(
                "remove failed activation receipt",
                source,
            )),
        },
    }
}

fn sync_receipt_parent(root: &Path) -> Result<(), ServiceRegistrationError> {
    #[cfg(unix)]
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| {
            ServiceRegistrationError::io("synchronize service receipt directory", source)
        })?;
    Ok(())
}

fn validate_receipt_identity(
    receipt: &ServiceRegistrationReceipt,
    target: SupportedTarget,
) -> Result<(), ServiceRegistrationError> {
    if receipt.schema_version != RECEIPT_SCHEMA_VERSION
        || receipt.owner.as_ref() != REGISTRATION_OWNER
        || receipt.platform_registration.as_ref() != platform_registration_identity(target)
        || receipt.target.as_ref() != target.as_str()
        || Version::parse(&receipt.version)
            .map(|version| version.to_string() != receipt.version.as_ref())
            .unwrap_or(true)
        || !is_lower_sha256(&receipt.manifest_sha256)
        || !is_lower_sha256(&receipt.configuration_sha256)
        || receipt
            .pending_configuration_sha256
            .as_deref()
            .is_some_and(|digest| !is_lower_sha256(digest))
        || [&receipt.service, &receipt.mcp_relay, &receipt.cli]
            .iter()
            .any(|identity| {
                !identity.path.is_absolute()
                    || identity.size == 0
                    || identity.size > MAXIMUM_EXECUTABLE_BYTES
                    || !is_lower_sha256(&identity.sha256)
            })
    {
        return Err(ServiceRegistrationError::Receipt);
    }
    Ok(())
}

fn probe_health(
    material: &RegistrationMaterial,
    expected: Option<RuntimeIdentity>,
    timeout: Duration,
) -> Result<RuntimeIdentity, ServiceRegistrationError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(ServiceRegistrationError::CommandTimeout)?;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(ServiceRegistrationError::Health);
        }
        let command_timeout = deadline
            .saturating_duration_since(now)
            .min(HEALTH_COMMAND_TIMEOUT);
        match probe_health_once(material, expected, command_timeout) {
            Ok(runtime) => return Ok(runtime),
            Err(_) if Instant::now() < deadline => thread::sleep(HEALTH_RETRY_INTERVAL),
            Err(_) => return Err(ServiceRegistrationError::Health),
        }
    }
}

fn probe_health_once(
    material: &RegistrationMaterial,
    expected: Option<RuntimeIdentity>,
    timeout: Duration,
) -> Result<RuntimeIdentity, ServiceRegistrationError> {
    let output = run_bounded_with_timeout(
        &material.receipt.cli.path,
        [
            OsString::from("--output"),
            OsString::from("json"),
            OsString::from("--training-release-root"),
            material.release_root.as_os_str().to_owned(),
            OsString::from("service"),
            OsString::from("status"),
        ],
        true,
        timeout,
    )?;
    let document: Value =
        serde_json::from_slice(&output.stdout).map_err(|_| ServiceRegistrationError::Health)?;
    validate_health_document_expected(&document, &material.receipt.version, expected)
}

#[cfg(test)]
fn validate_health_document(
    document: &Value,
    expected_version: &str,
) -> Result<RuntimeIdentity, ServiceRegistrationError> {
    validate_health_document_expected(document, expected_version, None)
}

fn validate_health_document_expected(
    document: &Value,
    expected_version: &str,
    expected: Option<RuntimeIdentity>,
) -> Result<RuntimeIdentity, ServiceRegistrationError> {
    let runtime_value = document
        .pointer("/bootstrap/runtime")
        .ok_or(ServiceRegistrationError::Health)?;
    let runtime: RuntimeIdentity = serde_json::from_value(runtime_value.clone())
        .map_err(|_| ServiceRegistrationError::Health)?;
    let readiness = document
        .pointer("/bootstrap/readiness")
        .ok_or(ServiceRegistrationError::Health)?;
    if document.get("status").and_then(Value::as_str) != Some("ready")
        || document
            .pointer("/bootstrap/schemaVersion")
            .and_then(Value::as_u64)
            != Some(1)
        || document
            .pointer("/bootstrap/product/version")
            .and_then(Value::as_str)
            != Some(expected_version)
        || expected.is_some_and(|expected| runtime != expected)
        || readiness.get("service").and_then(Value::as_bool) != Some(true)
        || readiness.get("nativeApplication").and_then(Value::as_bool) != Some(true)
        || readiness.get("cli").and_then(Value::as_bool) != Some(true)
        || readiness.get("mcp").and_then(Value::as_bool) != Some(true)
        || document
            .pointer("/bootstrap/application/contractVersion")
            .and_then(Value::as_u64)
            .is_none_or(|version| version == 0)
        || document
            .pointer("/bootstrap/application/operations")
            .and_then(Value::as_array)
            .is_none_or(|operations| operations.is_empty() || operations.len() > 1_024)
        || document
            .pointer("/bootstrap/mcpAuthority/endpointIdentity")
            .and_then(Value::as_str)
            .is_none_or(|identity| !is_lower_sha256(identity))
    {
        return Err(ServiceRegistrationError::Health);
    }
    Ok(runtime)
}

#[cfg(test)]
fn file_contains_marker(path: &Path, marker: &[u8]) -> Result<bool, ServiceRegistrationError> {
    let bytes = fs::read(path)
        .map_err(|source| ServiceRegistrationError::io("read test service fixture", source))?;
    Ok(bytes.windows(marker.len()).any(|window| window == marker))
}

#[derive(Debug)]
pub(super) struct CommandOutput {
    pub(super) status: ExitStatus,
    pub(super) stdout: Vec<u8>,
}

pub(super) fn run_bounded<I, S>(
    program: &Path,
    arguments: I,
    capture_stdout: bool,
) -> Result<CommandOutput, ServiceRegistrationError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_bounded_with_timeout(program, arguments, capture_stdout, COMMAND_TIMEOUT)
}

fn run_bounded_with_timeout<I, S>(
    program: &Path,
    arguments: I,
    capture_stdout: bool,
    timeout: Duration,
) -> Result<CommandOutput, ServiceRegistrationError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = run_bounded_raw_with_timeout(program, arguments, capture_stdout, timeout)?;
    if !output.status.success() {
        return Err(ServiceRegistrationError::CommandFailed(
            output.status.code(),
        ));
    }
    Ok(output)
}

#[cfg(windows)]
pub(super) fn run_bounded_raw<I, S>(
    program: &Path,
    arguments: I,
    capture_stdout: bool,
) -> Result<CommandOutput, ServiceRegistrationError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_bounded_raw_with_timeout(program, arguments, capture_stdout, COMMAND_TIMEOUT)
}

fn run_bounded_raw_with_timeout<I, S>(
    program: &Path,
    arguments: I,
    capture_stdout: bool,
    timeout: Duration,
) -> Result<CommandOutput, ServiceRegistrationError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    if timeout.is_zero() || timeout > COMMAND_TIMEOUT {
        return Err(ServiceRegistrationError::CommandTimeout);
    }
    let mut command = Command::new(program);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(if capture_stdout {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|source| {
        ServiceRegistrationError::io("start platform registration command", source)
    })?;
    let stdout = child
        .stdout
        .take()
        .map(|stdout| thread::spawn(move || read_bounded(stdout, MAXIMUM_COMMAND_OUTPUT_BYTES)));
    let stderr = child
        .stderr
        .take()
        .map(|stderr| thread::spawn(move || read_bounded(stderr, MAXIMUM_COMMAND_OUTPUT_BYTES)));
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(ServiceRegistrationError::CommandTimeout)?;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|source| {
            ServiceRegistrationError::io("inspect platform registration command", source)
        })? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ignored = child.kill();
            let _ignored = child.wait();
            return Err(ServiceRegistrationError::CommandTimeout);
        }
        thread::sleep(Duration::from_millis(20));
    };
    let stdout = join_reader(stdout)?;
    let _stderr = join_reader(stderr)?;
    Ok(CommandOutput { status, stdout })
}

fn read_bounded(mut reader: impl Read, maximum: u64) -> Result<Vec<u8>, ServiceRegistrationError> {
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| {
            ServiceRegistrationError::io("read platform registration command", source)
        })?;
    if bytes.len() as u64 > maximum {
        return Err(ServiceRegistrationError::CommandOutput);
    }
    Ok(bytes)
}

fn join_reader(
    reader: Option<thread::JoinHandle<Result<Vec<u8>, ServiceRegistrationError>>>,
) -> Result<Vec<u8>, ServiceRegistrationError> {
    match reader {
        Some(reader) => reader
            .join()
            .map_err(|_| ServiceRegistrationError::CommandOutput)?,
        None => Ok(Vec::new()),
    }
}

pub(super) fn sha256_bytes(bytes: &[u8]) -> Box<str> {
    format!("{:x}", Sha256::digest(bytes)).into()
}

#[cfg(any(target_os = "macos", target_os = "windows"))]
pub(super) fn xml_escape(value: &str) -> Result<String, ServiceRegistrationError> {
    if value.chars().any(|character| character.is_control()) {
        return Err(ServiceRegistrationError::Identity);
    }
    Ok(value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;"))
}

pub(super) fn native_document(bytes: Vec<u8>) -> Result<Vec<u8>, ServiceRegistrationError> {
    if bytes.is_empty() || bytes.len() > MAXIMUM_NATIVE_DOCUMENT_BYTES {
        return Err(ServiceRegistrationError::NativeDocument);
    }
    Ok(bytes)
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

fn empty_sha256() -> &'static str {
    "0000000000000000000000000000000000000000000000000000000000000000"
}

fn platform_registration_identity(target: SupportedTarget) -> &'static str {
    match target {
        SupportedTarget::Aarch64AppleDarwin | SupportedTarget::X86_64AppleDarwin => {
            "com.marketsquawk.service"
        }
        SupportedTarget::X86_64UnknownLinuxGnu => "market-squawk.service",
        SupportedTarget::X86_64PcWindowsMsvc => r"\MarketSquawk\Service",
    }
}

#[cfg(not(test))]
fn prepare_native(
    material: &RegistrationMaterial,
) -> Result<PreparedRegistration, ServiceRegistrationError> {
    #[cfg(target_os = "macos")]
    return macos::prepare(&material.service_path, &material.release_root);
    #[cfg(target_os = "linux")]
    return linux::prepare(&material.service_path, &material.release_root);
    #[cfg(target_os = "windows")]
    return windows::prepare(&material.service_path, &material.release_root);
    #[allow(unreachable_code)]
    Err(ServiceRegistrationError::Target)
}

#[cfg(not(test))]
fn inspect_native() -> Result<Option<NativeRegistrationSnapshot>, ServiceRegistrationError> {
    #[cfg(target_os = "macos")]
    return macos::inspect();
    #[cfg(target_os = "linux")]
    return linux::inspect();
    #[cfg(target_os = "windows")]
    return windows::inspect();
    #[allow(unreachable_code)]
    Err(ServiceRegistrationError::Target)
}

#[cfg(not(test))]
fn apply_native(prepared: &PreparedRegistration) -> Result<(), ServiceRegistrationError> {
    #[cfg(target_os = "macos")]
    return macos::apply(prepared);
    #[cfg(target_os = "linux")]
    return linux::apply(prepared);
    #[cfg(target_os = "windows")]
    return windows::apply(prepared);
    #[allow(unreachable_code)]
    Err(ServiceRegistrationError::Target)
}

#[cfg(not(test))]
fn start_native() -> Result<(), ServiceRegistrationError> {
    #[cfg(target_os = "macos")]
    return macos::start();
    #[cfg(target_os = "linux")]
    return linux::start();
    #[cfg(target_os = "windows")]
    return windows::start();
    #[allow(unreachable_code)]
    Err(ServiceRegistrationError::Target)
}

#[cfg(not(test))]
fn restart_native() -> Result<(), ServiceRegistrationError> {
    #[cfg(target_os = "macos")]
    return macos::restart();
    #[cfg(target_os = "linux")]
    return linux::restart();
    #[cfg(target_os = "windows")]
    return windows::restart();
    #[allow(unreachable_code)]
    Err(ServiceRegistrationError::Target)
}

#[cfg(test)]
fn restart_native() -> Result<(), ServiceRegistrationError> {
    Err(ServiceRegistrationError::NativeControlUnavailable)
}

#[cfg(not(test))]
fn ensure_native_active() -> Result<(), ServiceRegistrationError> {
    #[cfg(target_os = "macos")]
    return macos::ensure_active();
    #[cfg(target_os = "linux")]
    return linux::ensure_active();
    #[cfg(target_os = "windows")]
    return windows::ensure_active();
    #[allow(unreachable_code)]
    Err(ServiceRegistrationError::Target)
}

#[cfg(not(test))]
fn remove_native(native: &NativeRegistrationSnapshot) -> Result<(), ServiceRegistrationError> {
    #[cfg(target_os = "macos")]
    return macos::remove(native);
    #[cfg(target_os = "linux")]
    return linux::remove(native);
    #[cfg(target_os = "windows")]
    return windows::remove(native);
    #[allow(unreachable_code)]
    Err(ServiceRegistrationError::Target)
}

#[cfg(not(test))]
fn restore_native(
    prior: Option<&NativeRegistrationSnapshot>,
    attempted: &PreparedRegistration,
) -> Result<(), ServiceRegistrationError> {
    #[cfg(target_os = "macos")]
    return macos::restore(prior, attempted);
    #[cfg(target_os = "linux")]
    return linux::restore(prior, attempted);
    #[cfg(target_os = "windows")]
    return windows::restore(prior, attempted);
    #[allow(unreachable_code)]
    Err(ServiceRegistrationError::Target)
}

/// Per-user registration, ownership, command, or health failure.
#[derive(Debug, Error)]
pub enum ServiceRegistrationError {
    /// The target is not the running supported platform.
    #[error("service registration target does not match this platform")]
    Target,
    /// Version, digest, or path identity is malformed.
    #[error("service registration identity is invalid")]
    Identity,
    /// The installation could not complete lifecycle crash recovery before service control.
    #[error("installed program state could not be recovered for service control")]
    Installation,
    /// An installation activation remains pending and cannot be crossed by service control.
    #[error("installed program activation is still in progress")]
    Transition,
    /// A registered executable path is unsafe or replaceable.
    #[error("service registration path is unsafe")]
    UnsafePath,
    /// A registered executable changed during identity verification.
    #[error("service registration executable changed during verification")]
    Changed,
    /// The durable registration receipt is absent, unsafe, or invalid.
    #[error("service registration receipt is invalid")]
    Receipt,
    /// Another owner or an unexplained generation owns the stable platform entry.
    #[error("service registration conflicts with an entry not owned by this installation")]
    Conflict,
    /// The expected native entry is absent.
    #[error("owned service registration is missing")]
    RegistrationMissing,
    /// Native control is intentionally unavailable in an inert deterministic test build.
    #[error("native service control is unavailable in this build")]
    NativeControlUnavailable,
    /// The native configuration document is malformed or exceeds its bound.
    #[error("native service registration document is invalid")]
    NativeDocument,
    /// A bounded platform command exceeded its deadline.
    #[error("platform service-registration command exceeded its deadline")]
    CommandTimeout,
    /// A bounded platform command exceeded its output ceiling.
    #[error("platform service-registration command exceeded its output ceiling")]
    CommandOutput,
    /// A platform command returned failure.
    #[error("platform service-registration command failed with status {0:?}")]
    CommandFailed(Option<i32>),
    /// Authenticated service readiness did not bind the expected generation.
    #[error("installed service did not prove exact authenticated readiness")]
    Health,
    /// Filesystem or process I/O failed.
    #[error("service registration I/O failed while {operation}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    /// Exclusive installer-store validation failed.
    #[error(transparent)]
    Store(#[from] crate::store::StoreError),
}

impl ServiceRegistrationError {
    fn io(operation: &'static str, source: std::io::Error) -> Self {
        Self::Io { operation, source }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::path::Path;

    use serde_json::json;

    #[cfg(target_os = "linux")]
    use super::linux;
    #[cfg(target_os = "macos")]
    use super::macos;
    #[cfg(target_os = "windows")]
    use super::windows;
    use super::{
        RuntimeIdentity, ServiceGeneration, validate_health_document,
        validate_health_document_expected,
    };

    type TestResult = Result<(), Box<dyn Error>>;

    #[test]
    #[cfg(target_os = "macos")]
    fn macos_registration_binds_exact_argv_and_restart_policy() -> TestResult {
        let service = Path::new("/Users/Test & Co/Market Squawk/bin/market-squawk-service");
        let release = Path::new("/Users/Test & Co/Market Squawk/releases/0.2.0");

        let launch_agent = macos::render_launch_agent(service, release)?;
        assert!(launch_agent.contains("<key>ProgramArguments</key>"));
        assert!(launch_agent.contains(
            "<string>/Users/Test &amp; Co/Market Squawk/bin/market-squawk-service</string>"
        ));
        assert!(launch_agent.contains("<key>KeepAlive</key>"));
        assert!(!launch_agent.contains("/bin/sh"));

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_registration_binds_exact_argv_and_runtime_hardening() -> TestResult {
        let service = Path::new("/home/test/Market Squawk/bin/market-squawk-service");
        let release = Path::new("/home/test/Market Squawk/releases/0.2.0");
        let unit = linux::render_user_unit(service, release)?;
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("NoNewPrivileges=yes"));
        assert!(unit.contains("ProtectSystem=full"));
        assert!(!unit.contains("loginctl enable-linger"));
        assert!(!unit.contains("/bin/sh"));

        Ok(())
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_registration_binds_exact_argv_and_least_privilege() -> TestResult {
        let task = windows::render_task_xml(
            Path::new(r"C:\Users\Test & Co\Market Squawk\market-squawk-service.exe"),
            Path::new(r"C:\Users\Test & Co\Market Squawk\releases\0.2.0"),
            "S-1-5-21-1000",
        )?;
        assert!(task.contains("<LogonType>InteractiveToken</LogonType>"));
        assert!(task.contains("<RunLevel>LeastPrivilege</RunLevel>"));
        assert!(task.contains("<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>"));
        assert!(task.contains("Test &amp; Co"));
        assert!(!task.contains("powershell"));
        assert!(!task.contains("cmd.exe"));
        let digest = windows::task_configuration_digest(task.as_bytes())?;
        assert_eq!(digest.len(), 64);
        let elevated = task.replace("LeastPrivilege", "HighestAvailable");
        assert!(windows::task_configuration_digest(elevated.as_bytes()).is_err());
        Ok(())
    }

    #[test]
    fn service_health_rejects_wrong_or_incomplete_runtime_generation() -> TestResult {
        let ready = json!({
            "status": "ready",
            "bootstrap": {
                "schemaVersion": 1,
                "product": { "version": "0.2.0" },
                "application": {
                    "contractVersion": 1,
                    "operations": [{ "name": "Source.GetStatus" }]
                },
                "mcpAuthority": {
                    "endpointIdentity": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                },
                "runtime": {
                    "installationId": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "serviceGeneration": 7,
                    "workspaceId": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
                },
                "readiness": {
                    "service": true,
                    "nativeApplication": true,
                    "cli": true,
                    "mcp": true
                }
            }
        });
        let expected = validate_health_document(&ready, "0.2.0")?;
        validate_health_document_expected(&ready, "0.2.0", Some(expected))?;

        let wrong_generation = RuntimeIdentity::try_new(
            expected.installation_id(),
            expected.workspace_id(),
            ServiceGeneration::try_new(8)?,
        )?;
        assert!(
            validate_health_document_expected(&ready, "0.2.0", Some(wrong_generation)).is_err()
        );

        let mut invalid = ready;
        invalid["bootstrap"]["runtime"]["serviceGeneration"] = json!(0);
        assert!(validate_health_document(&invalid, "0.2.0").is_err());
        invalid["bootstrap"]["runtime"]["serviceGeneration"] = json!(7);
        invalid["bootstrap"]["product"]["version"] = json!("0.2.1");
        assert!(validate_health_document(&invalid, "0.2.0").is_err());
        invalid["bootstrap"]["product"]["version"] = json!("0.2.0");
        invalid["bootstrap"]["readiness"]["mcp"] = json!(false);
        assert!(validate_health_document(&invalid, "0.2.0").is_err());
        Ok(())
    }
}
