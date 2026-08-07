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
use std::fmt;
use std::fs::{self, File};
use std::io::{Read, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use atomicwrites::{AllowOverwrite, AtomicFile};
#[cfg(not(test))]
use market_squawk_runtime::ServiceStartupEvidenceWriter;
use market_squawk_runtime::{
    InstallationId, RuntimeIdentity, ServiceGeneration, ServiceStartupEvidenceError,
    ServiceStartupState, read_service_startup_evidence,
};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::platform::{ProgramName, SupportedTarget, default_workspace_data_root};
use crate::store::InstallStore;

const RECEIPT_FILE: &str = "service-registration.json";
const RECEIPT_SCHEMA_VERSION: u32 = 2;
const REGISTRATION_OWNER: &str = "market-squawk-installer-v1";
const MAXIMUM_EXECUTABLE_BYTES: u64 = 768 * 1024 * 1024;
const MAXIMUM_RECEIPT_BYTES: usize = 64 * 1024;
const MAXIMUM_NATIVE_DOCUMENT_BYTES: usize = 64 * 1024;
const MAXIMUM_COMMAND_OUTPUT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_COMMAND_DIAGNOSTIC_CHARS: usize = 8 * 1024;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(20);
const HEALTH_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
const HEALTH_STATUS_TIMEOUT: Duration = Duration::from_secs(5);
const HEALTH_RESTART_TIMEOUT: Duration = Duration::from_secs(30);
const HEALTH_ACTIVATION_TIMEOUT: Duration = Duration::from_secs(120);
const HEALTH_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const HEALTH_MISSING_EVIDENCE_GRACE: Duration = Duration::from_secs(5);
const HEALTH_STOPPED_GRACE: Duration = Duration::from_secs(5);
const HEALTH_READY_ENDPOINT_GRACE: Duration = Duration::from_secs(2);
const HEALTH_STARTING_PHASE_TIMEOUT: Duration = Duration::from_secs(60);
#[cfg(test)]
const TEST_HEALTH_FAILURE_MARKER: &[u8] = b"market-squawk-test-service-health-failure";
#[cfg(test)]
const TEST_BOOTSTRAP_REQUIRED_MARKER: &[u8] = b"market-squawk-test-service-bootstrap-required";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HealthProbe {
    Ready(RuntimeIdentity),
    BootstrapRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HealthRetryMode {
    Ordinary,
    StartupTransition,
}

#[derive(Debug)]
struct HealthRetryTracker {
    started_at: Instant,
    last_state: Option<Option<ServiceStartupState>>,
    state_changed_at: Instant,
}

impl HealthRetryTracker {
    fn new(now: Instant) -> Self {
        Self {
            started_at: now,
            last_state: None,
            state_changed_at: now,
        }
    }

    fn observe(&mut self, state: Option<ServiceStartupState>, now: Instant) -> Duration {
        if self.last_state != Some(state) {
            self.last_state = Some(state);
            self.state_changed_at = now;
        }
        now.saturating_duration_since(self.state_changed_at)
    }
}

/// Exact immutable release binding used for registration, repair, and verification.
#[derive(Debug)]
pub(crate) struct RegistrationSpec<'a> {
    installation_data_root: &'a Path,
    install_root: &'a Path,
    version_root: &'a Path,
    target: SupportedTarget,
    version: &'a str,
    manifest_sha256: &'a str,
}

impl<'a> RegistrationSpec<'a> {
    /// Admits one candidate registration specification.
    pub(crate) fn new(
        installation_data_root: &'a Path,
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
            || !installation_data_root.is_absolute()
            || !install_root.is_absolute()
            || !version_root.is_absolute()
            || !is_lower_sha256(manifest_sha256)
        {
            return Err(ServiceRegistrationError::Identity);
        }
        Ok(Self {
            installation_data_root,
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
    installation_data_root: PathBuf,
    workspace_data_root: PathBuf,
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
        if file_contains_marker(&material.service_path, TEST_HEALTH_FAILURE_MARKER)?
            || file_contains_marker(&material.service_path, TEST_BOOTSTRAP_REQUIRED_MARKER)?
        {
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
        #[cfg(not(test))]
        ServiceStartupEvidenceWriter::try_open(&material.installation_data_root)?.clear()?;
        restart_native()?;
        let runtime = probe_health_outcome(
            material,
            Some(expected_next),
            HEALTH_RESTART_TIMEOUT,
            HealthRetryMode::StartupTransition,
        )
        .and_then(|outcome| match outcome {
            HealthProbe::Ready(runtime) => Ok(runtime),
            HealthProbe::BootstrapRequired => Err(ServiceRegistrationError::Health),
        })?;
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
        store.installation_data_root()?,
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
        #[cfg(not(test))]
        prove_native_absent()?;
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
        } else {
            prove_native_absent()?;
        }
        prove_native_absent()?;
    }
    remove_receipt(install_root)?;
    Ok(true)
}

/// Removes the exact owned registration or proves that both its receipt and native entry are
/// absent. This is the first-install recovery boundary after an inner rollback may already have
/// restored the known-good empty state.
pub(crate) fn remove_owned_or_prove_absent(
    install_root: &Path,
    target: SupportedTarget,
) -> Result<(), ServiceRegistrationError> {
    remove_owned(install_root, target)?;
    Ok(())
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
    ServiceStartupEvidenceWriter::try_open(&material.installation_data_root)?.clear()?;
    write_receipt(spec.install_root, &pending_receipt)?;

    let replacing_existing = prior_native.is_some();
    let attempt = apply_native(&desired)
        .and_then(|()| start_native(replacing_existing))
        .and_then(|()| probe_activation_health(&material, HEALTH_ACTIVATION_TIMEOUT).map(|_| ()));
    if let Err(error) = attempt {
        return Err(activation_failure(
            error,
            prior_native.as_ref(),
            &desired,
            spec.install_root,
            prior_receipt.as_ref(),
        ));
    }
    material.receipt.pending_configuration_sha256 = None;
    if let Err(error) = write_receipt(spec.install_root, &material.receipt) {
        return Err(activation_failure(
            error,
            prior_native.as_ref(),
            &desired,
            spec.install_root,
            prior_receipt.as_ref(),
        ));
    }
    Ok(material.receipt)
}

#[cfg(not(test))]
fn activation_failure(
    primary: ServiceRegistrationError,
    prior_native: Option<&NativeRegistrationSnapshot>,
    attempted: &PreparedRegistration,
    install_root: &Path,
    prior_receipt: Option<&ServiceRegistrationReceipt>,
) -> ServiceRegistrationError {
    let native = restore_native(prior_native, attempted).err().map(Box::new);
    let receipt = if native.is_none() {
        match restore_receipt(install_root, prior_receipt) {
            Ok(()) => ReceiptRollback::Restored,
            Err(error) => ReceiptRollback::Failed(Box::new(error)),
        }
    } else {
        // The pending receipt admits both the previous and attempted native digests. Retaining it
        // is the only safe recovery authority when native rollback could not prove which
        // registration remains installed.
        ReceiptRollback::PreservedForRecovery
    };
    ServiceActivationFailure {
        primary: Box::new(primary),
        rollback: ActivationRollback { native, receipt },
    }
    .into()
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
    let canonical_installation_data_root = fs::canonicalize(spec.installation_data_root)
        .map_err(|source| ServiceRegistrationError::io("resolve installation data root", source))?;
    let canonical_root = fs::canonicalize(spec.version_root)
        .map_err(|source| ServiceRegistrationError::io("resolve candidate release", source))?;
    if canonical_root == canonical_install_root
        || !canonical_root.starts_with(&canonical_install_root)
    {
        return Err(ServiceRegistrationError::UnsafePath);
    }
    if canonical_install_root.parent() != Some(canonical_installation_data_root.as_path()) {
        return Err(ServiceRegistrationError::UnsafePath);
    }
    let workspace_data_root =
        default_workspace_data_root().map_err(|_| ServiceRegistrationError::UnsafePath)?;
    if !workspace_data_root.is_absolute() {
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
        installation_data_root: canonical_installation_data_root,
        workspace_data_root,
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
    match probe_health_outcome(material, expected, timeout, HealthRetryMode::Ordinary)? {
        HealthProbe::Ready(runtime) => Ok(runtime),
        HealthProbe::BootstrapRequired => Err(ServiceRegistrationError::Health),
    }
}

#[cfg(not(test))]
fn probe_activation_health(
    material: &RegistrationMaterial,
    timeout: Duration,
) -> Result<HealthProbe, ServiceRegistrationError> {
    probe_health_outcome(material, None, timeout, HealthRetryMode::StartupTransition)
}

fn probe_health_outcome(
    material: &RegistrationMaterial,
    expected: Option<RuntimeIdentity>,
    timeout: Duration,
    retry_mode: HealthRetryMode,
) -> Result<HealthProbe, ServiceRegistrationError> {
    let deadline =
        Instant::now()
            .checked_add(timeout)
            .ok_or(ServiceRegistrationError::CommandTimeout(
                PlatformServiceOperation::ProbeHealth,
            ))?;
    let now = Instant::now();
    let mut last_transient = None;
    let mut retry_tracker = HealthRetryTracker::new(now);
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(service_health_failure(
                last_transient.unwrap_or(ServiceRegistrationError::CommandTimeout(
                    PlatformServiceOperation::ProbeHealth,
                )),
                material,
            ));
        }
        let command_timeout = deadline
            .saturating_duration_since(now)
            .min(HEALTH_COMMAND_TIMEOUT);
        match probe_health_once(material, expected, command_timeout) {
            Ok(outcome) => return Ok(outcome),
            Err(error)
                if transient_health_error(&error, material, retry_mode, &mut retry_tracker)
                    && Instant::now() < deadline =>
            {
                last_transient = Some(error);
                thread::sleep(
                    HEALTH_RETRY_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
                );
            }
            Err(error) => return Err(service_health_failure(error, material)),
        }
    }
}

fn transient_health_error(
    error: &ServiceRegistrationError,
    material: &RegistrationMaterial,
    retry_mode: HealthRetryMode,
    tracker: &mut HealthRetryTracker,
) -> bool {
    let command_can_be_transient = match error {
        ServiceRegistrationError::CommandTimeout(PlatformServiceOperation::ProbeHealth) => true,
        ServiceRegistrationError::CommandFailed(failure) => {
            failure.operation == PlatformServiceOperation::ProbeHealth
        }
        _ => false,
    };
    if !command_can_be_transient {
        return false;
    }
    let native = match native_manager_state() {
        Ok(native) if native.may_be_starting() => native,
        Ok(_) | Err(_) => return false,
    };
    let now = Instant::now();
    let evidence = match read_service_startup_evidence(&material.installation_data_root) {
        Ok(evidence) => evidence.map(|evidence| evidence.state()),
        Err(_) => return false,
    };
    let unchanged_for = tracker.observe(evidence, now);
    match evidence {
        Some(ServiceStartupState::Failed { .. }) => false,
        Some(ServiceStartupState::Starting { .. }) => unchanged_for < HEALTH_STARTING_PHASE_TIMEOUT,
        Some(ServiceStartupState::Ready) => unchanged_for < HEALTH_READY_ENDPOINT_GRACE,
        Some(ServiceStartupState::Stopped) => {
            retry_mode == HealthRetryMode::StartupTransition && unchanged_for < HEALTH_STOPPED_GRACE
        }
        None => {
            now.saturating_duration_since(tracker.started_at) < HEALTH_MISSING_EVIDENCE_GRACE
                && native.may_be_starting()
        }
    }
}

fn service_health_failure(
    last: ServiceRegistrationError,
    material: &RegistrationMaterial,
) -> ServiceRegistrationError {
    let native = match native_manager_state() {
        Ok(state) => NativeManagerObservation::Observed(state),
        Err(error) => NativeManagerObservation::InspectionFailed(Box::new(error)),
    };
    let startup = match read_service_startup_evidence(&material.installation_data_root) {
        Ok(evidence) => StartupObservation::Observed(evidence.map(|evidence| evidence.state())),
        Err(error) => StartupObservation::InspectionFailed(Box::new(error)),
    };
    ServiceHealthFailure {
        last: Box::new(last),
        native,
        startup,
    }
    .into()
}

fn probe_health_once(
    material: &RegistrationMaterial,
    expected: Option<RuntimeIdentity>,
    timeout: Duration,
) -> Result<HealthProbe, ServiceRegistrationError> {
    let output = run_bounded_with_timeout(
        PlatformServiceOperation::ProbeHealth,
        &material.receipt.cli.path,
        [
            OsString::from("--output"),
            OsString::from("json"),
            OsString::from("--data-dir"),
            material.workspace_data_root.as_os_str().to_owned(),
            OsString::from("--installation-data-root"),
            material.installation_data_root.as_os_str().to_owned(),
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
    classify_health_document(&document, &material.receipt.version, expected)
}

#[cfg(test)]
fn validate_health_document(
    document: &Value,
    expected_version: &str,
) -> Result<RuntimeIdentity, ServiceRegistrationError> {
    validate_health_document_expected(document, expected_version, None)
}

#[cfg(test)]
fn validate_health_document_expected(
    document: &Value,
    expected_version: &str,
    expected: Option<RuntimeIdentity>,
) -> Result<RuntimeIdentity, ServiceRegistrationError> {
    match classify_health_document(document, expected_version, expected)? {
        HealthProbe::Ready(runtime) => Ok(runtime),
        HealthProbe::BootstrapRequired => Err(ServiceRegistrationError::Health),
    }
}

fn classify_health_document(
    document: &Value,
    expected_version: &str,
    expected: Option<RuntimeIdentity>,
) -> Result<HealthProbe, ServiceRegistrationError> {
    if document.get("status").and_then(Value::as_str) == Some("bootstrap_required") {
        return validate_bootstrap_required_document(document, expected);
    }
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
    Ok(HealthProbe::Ready(runtime))
}

fn validate_bootstrap_required_document(
    document: &Value,
    expected: Option<RuntimeIdentity>,
) -> Result<HealthProbe, ServiceRegistrationError> {
    let top = document
        .as_object()
        .filter(|top| top.len() == 2)
        .ok_or(ServiceRegistrationError::Health)?;
    let bootstrap = top
        .get("bootstrap")
        .and_then(Value::as_object)
        .filter(|bootstrap| bootstrap.len() == 4)
        .ok_or(ServiceRegistrationError::Health)?;
    let installation: InstallationId = serde_json::from_value(
        bootstrap
            .get("installationId")
            .cloned()
            .ok_or(ServiceRegistrationError::Health)?,
    )
    .map_err(|_| ServiceRegistrationError::Health)?;
    let generation = bootstrap
        .get("generation")
        .and_then(Value::as_u64)
        .and_then(|generation| ServiceGeneration::try_new(generation).ok())
        .ok_or(ServiceRegistrationError::Health)?;
    if expected.is_some()
        || top.get("status").and_then(Value::as_str) != Some("bootstrap_required")
        || bootstrap.get("state").and_then(Value::as_str) != Some("required")
        || !matches!(
            bootstrap.get("requirement").and_then(Value::as_str),
            Some("encrypted_fallback_locked" | "foreground_keyring_retry")
        )
    {
        return Err(ServiceRegistrationError::Health);
    }
    let _authenticated_process_identity = (installation, generation);
    Ok(HealthProbe::BootstrapRequired)
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
    pub(super) stderr: Vec<u8>,
}

pub(super) fn run_bounded<I, S>(
    operation: PlatformServiceOperation,
    program: &Path,
    arguments: I,
    capture_stdout: bool,
) -> Result<CommandOutput, ServiceRegistrationError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_bounded_with_timeout(
        operation,
        program,
        arguments,
        capture_stdout,
        COMMAND_TIMEOUT,
    )
}

fn run_bounded_with_timeout<I, S>(
    operation: PlatformServiceOperation,
    program: &Path,
    arguments: I,
    capture_stdout: bool,
    timeout: Duration,
) -> Result<CommandOutput, ServiceRegistrationError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = run_bounded_raw_with_timeout(
        operation,
        program,
        arguments,
        capture_stdout,
        false,
        timeout,
    )?;
    if !output.status.success() {
        return Err(ServiceRegistrationError::CommandFailed(
            PlatformCommandFailure::new(operation, &output),
        ));
    }
    Ok(output)
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
pub(super) fn run_bounded_raw<I, S>(
    operation: PlatformServiceOperation,
    program: &Path,
    arguments: I,
    capture_stdout: bool,
) -> Result<CommandOutput, ServiceRegistrationError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_bounded_raw_with_timeout(
        operation,
        program,
        arguments,
        capture_stdout,
        false,
        COMMAND_TIMEOUT,
    )
}

#[cfg(target_os = "macos")]
pub(super) fn run_bounded_raw_silent<I, S>(
    operation: PlatformServiceOperation,
    program: &Path,
    arguments: I,
) -> Result<CommandOutput, ServiceRegistrationError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_bounded_raw_with_timeout(operation, program, arguments, false, true, COMMAND_TIMEOUT)
}

fn run_bounded_raw_with_timeout<I, S>(
    operation: PlatformServiceOperation,
    program: &Path,
    arguments: I,
    capture_stdout: bool,
    suppress_stdout: bool,
    timeout: Duration,
) -> Result<CommandOutput, ServiceRegistrationError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    if timeout.is_zero() || timeout > COMMAND_TIMEOUT {
        return Err(ServiceRegistrationError::CommandTimeout(operation));
    }
    let mut command = Command::new(program);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stderr(Stdio::piped());
    if suppress_stdout {
        command.stdout(Stdio::null());
    } else {
        command.stdout(Stdio::piped());
    }
    let mut child = command.spawn().map_err(|source| {
        ServiceRegistrationError::io("start platform registration command", source)
    })?;
    let stdout = child.stdout.take().map(|stdout| {
        thread::spawn(move || read_bounded(stdout, MAXIMUM_COMMAND_OUTPUT_BYTES, operation))
    });
    let stderr = child.stderr.take().map(|stderr| {
        thread::spawn(move || read_bounded(stderr, MAXIMUM_COMMAND_OUTPUT_BYTES, operation))
    });
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(ServiceRegistrationError::CommandTimeout(operation))?;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|source| {
            ServiceRegistrationError::io("inspect platform registration command", source)
        })? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ignored = child.kill();
            let _ignored = child.wait();
            let _stdout = join_reader(stdout, operation)?;
            let _stderr = join_reader(stderr, operation)?;
            return Err(ServiceRegistrationError::CommandTimeout(operation));
        }
        thread::sleep(Duration::from_millis(20));
    };
    let mut stdout = join_reader(stdout, operation)?;
    let stderr = join_reader(stderr, operation)?;
    if status.success() && !capture_stdout {
        stdout.clear();
    }
    Ok(CommandOutput {
        status,
        stdout,
        stderr,
    })
}

fn read_bounded(
    mut reader: impl Read,
    maximum: u64,
    operation: PlatformServiceOperation,
) -> Result<Vec<u8>, ServiceRegistrationError> {
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| {
            ServiceRegistrationError::io("read platform registration command", source)
        })?;
    if bytes.len() as u64 > maximum {
        return Err(ServiceRegistrationError::CommandOutput(operation));
    }
    Ok(bytes)
}

fn join_reader(
    reader: Option<thread::JoinHandle<Result<Vec<u8>, ServiceRegistrationError>>>,
    operation: PlatformServiceOperation,
) -> Result<Vec<u8>, ServiceRegistrationError> {
    match reader {
        Some(reader) => reader
            .join()
            .map_err(|_| ServiceRegistrationError::CommandOutput(operation))?,
        None => Ok(Vec::new()),
    }
}

/// One closed native service-manager operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlatformServiceOperation {
    /// Reload native registration configuration.
    ReloadManager,
    /// Inspect one exact registration.
    InspectRegistration,
    /// List native registrations only to distinguish absence from an inspect failure.
    ListRegistrations,
    /// Inspect the native manager's closed process or task state.
    InspectProcessState,
    /// Apply one exact native registration.
    ApplyRegistration,
    /// Enable one exact native registration.
    EnableRegistration,
    /// Start one exact native registration.
    StartRegistration,
    /// Restart one exact native registration.
    RestartRegistration,
    /// Stop one exact native registration.
    StopRegistration,
    /// Disable one exact native registration.
    DisableRegistration,
    /// Remove one exact native registration.
    RemoveRegistration,
    /// Resolve the current user identity required by the native service manager.
    ResolveCurrentUser,
    /// Query the authenticated Market Squawk service endpoint.
    ProbeHealth,
}

impl fmt::Display for PlatformServiceOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::ReloadManager => "reload-manager",
            Self::InspectRegistration => "inspect-registration",
            Self::ListRegistrations => "list-registrations",
            Self::InspectProcessState => "inspect-process-state",
            Self::ApplyRegistration => "apply-registration",
            Self::EnableRegistration => "enable-registration",
            Self::StartRegistration => "start-registration",
            Self::RestartRegistration => "restart-registration",
            Self::StopRegistration => "stop-registration",
            Self::DisableRegistration => "disable-registration",
            Self::RemoveRegistration => "remove-registration",
            Self::ResolveCurrentUser => "resolve-current-user",
            Self::ProbeHealth => "probe-health",
        };
        formatter.write_str(name)
    }
}

/// Bounded failure evidence returned by a native service-manager command.
#[derive(Debug)]
pub struct PlatformCommandFailure {
    operation: PlatformServiceOperation,
    status: Option<i32>,
    diagnostic: Box<str>,
}

impl PlatformCommandFailure {
    fn new(operation: PlatformServiceOperation, output: &CommandOutput) -> Self {
        let diagnostic = if output.stderr.is_empty() {
            &output.stdout
        } else {
            &output.stderr
        };
        Self {
            operation,
            status: output.status.code(),
            diagnostic: bounded_command_diagnostic(diagnostic),
        }
    }
}

impl fmt::Display for PlatformCommandFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} failed with status {:?}",
            self.operation, self.status
        )?;
        if !self.diagnostic.is_empty() {
            write!(formatter, ": {}", self.diagnostic)?;
        }
        Ok(())
    }
}

impl std::error::Error for PlatformCommandFailure {}

fn bounded_command_diagnostic(bytes: &[u8]) -> Box<str> {
    String::from_utf8_lossy(bytes)
        .chars()
        .filter(|character| !character.is_control() || matches!(character, '\n' | '\r' | '\t'))
        .take(MAXIMUM_COMMAND_DIAGNOSTIC_CHARS)
        .collect::<String>()
        .trim()
        .into()
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum NativeManagerState {
    #[cfg(target_os = "windows")]
    Active,
    #[cfg(target_os = "windows")]
    Registered,
    Absent,
    #[cfg(target_os = "macos")]
    LoadedStateUnavailable,
    #[cfg(target_os = "linux")]
    Linux(LinuxManagerState),
}

impl NativeManagerState {
    fn may_be_starting(&self) -> bool {
        match self {
            #[cfg(target_os = "windows")]
            Self::Active => true,
            #[cfg(target_os = "windows")]
            Self::Registered => false,
            Self::Absent => false,
            #[cfg(target_os = "macos")]
            Self::LoadedStateUnavailable => true,
            #[cfg(target_os = "linux")]
            Self::Linux(state) => state.may_be_starting(),
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct LinuxManagerState {
    active_state: Box<str>,
    sub_state: Box<str>,
    result: Box<str>,
    exec_main_code: i32,
    exec_main_status: i32,
    restarts: u64,
}

#[cfg(target_os = "linux")]
impl LinuxManagerState {
    fn may_be_starting(&self) -> bool {
        matches!(
            self.active_state.as_ref(),
            "active" | "activating" | "reloading"
        )
    }
}

impl fmt::Display for NativeManagerState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            #[cfg(target_os = "windows")]
            Self::Active => "active",
            #[cfg(target_os = "windows")]
            Self::Registered => "registered-but-not-active",
            Self::Absent => "absent",
            #[cfg(target_os = "macos")]
            Self::LoadedStateUnavailable => "loaded-running-state-unavailable",
            #[cfg(target_os = "linux")]
            Self::Linux(state) => {
                return write!(
                    formatter,
                    "systemd active={} sub={} result={} exec-code={} exec-status={} restarts={}",
                    state.active_state,
                    state.sub_state,
                    state.result,
                    state.exec_main_code,
                    state.exec_main_status,
                    state.restarts
                );
            }
        })
    }
}

#[derive(Debug)]
enum NativeManagerObservation {
    Observed(NativeManagerState),
    InspectionFailed(Box<ServiceRegistrationError>),
}

impl fmt::Display for NativeManagerObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Observed(state) => state.fmt(formatter),
            Self::InspectionFailed(error) => write!(formatter, "inspection-failed ({error})"),
        }
    }
}

#[derive(Debug)]
enum StartupObservation {
    Observed(Option<ServiceStartupState>),
    InspectionFailed(Box<ServiceStartupEvidenceError>),
}

impl fmt::Display for StartupObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Observed(Some(state)) => write!(formatter, "{state:?}"),
            Self::Observed(None) => formatter.write_str("unavailable"),
            Self::InspectionFailed(error) => write!(formatter, "inspection-failed ({error})"),
        }
    }
}

/// Final authenticated-health failure with bounded native and application startup evidence.
#[derive(Debug, Error)]
#[error(
    "installed service health failed: {last}; native manager: {native}; application startup: \
     {startup}"
)]
pub struct ServiceHealthFailure {
    #[source]
    last: Box<ServiceRegistrationError>,
    native: NativeManagerObservation,
    startup: StartupObservation,
}

#[derive(Debug)]
struct ActivationRollback {
    native: Option<Box<ServiceRegistrationError>>,
    receipt: ReceiptRollback,
}

#[derive(Debug)]
enum ReceiptRollback {
    Restored,
    PreservedForRecovery,
    Failed(Box<ServiceRegistrationError>),
}

impl fmt::Display for ActivationRollback {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.native, &self.receipt) {
            (None, ReceiptRollback::Restored) => formatter.write_str("succeeded"),
            (None, ReceiptRollback::Failed(receipt)) => {
                write!(formatter, "failed restoring receipt: {receipt}")
            }
            (Some(native), ReceiptRollback::PreservedForRecovery) => write!(
                formatter,
                "failed restoring native state ({native}); pending recovery receipt preserved"
            ),
            (Some(_), ReceiptRollback::Restored | ReceiptRollback::Failed(_))
            | (None, ReceiptRollback::PreservedForRecovery) => {
                formatter.write_str("entered an invalid rollback state")
            }
        }
    }
}

/// Primary installed-service activation failure plus any independent rollback failure.
#[derive(Debug, Error)]
#[error("installed-service activation failed: {primary}; rollback {rollback}")]
pub struct ServiceActivationFailure {
    #[source]
    primary: Box<ServiceRegistrationError>,
    rollback: ActivationRollback,
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
        SupportedTarget::X86_64PcWindowsMsvc => r"\MarketSquawkService",
    }
}

#[cfg(not(test))]
fn prepare_native(
    material: &RegistrationMaterial,
) -> Result<PreparedRegistration, ServiceRegistrationError> {
    #[cfg(target_os = "macos")]
    return macos::prepare(
        &material.service_path,
        &material.workspace_data_root,
        &material.installation_data_root,
        &material.release_root,
    );
    #[cfg(target_os = "linux")]
    return linux::prepare(
        &material.service_path,
        &material.workspace_data_root,
        &material.installation_data_root,
        &material.release_root,
    );
    #[cfg(target_os = "windows")]
    return windows::prepare(
        &material.service_path,
        &material.workspace_data_root,
        &material.installation_data_root,
        &material.release_root,
    );
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
fn prove_native_absent() -> Result<(), ServiceRegistrationError> {
    #[cfg(target_os = "macos")]
    return macos::prove_absent();
    #[cfg(target_os = "linux")]
    return linux::prove_absent();
    #[cfg(target_os = "windows")]
    return windows::prove_absent();
    #[allow(unreachable_code)]
    Err(ServiceRegistrationError::Target)
}

#[cfg(not(test))]
fn native_manager_state() -> Result<NativeManagerState, ServiceRegistrationError> {
    #[cfg(target_os = "macos")]
    return macos::manager_state();
    #[cfg(target_os = "linux")]
    return linux::manager_state();
    #[cfg(target_os = "windows")]
    return windows::manager_state();
    #[allow(unreachable_code)]
    Err(ServiceRegistrationError::Target)
}

#[cfg(test)]
fn native_manager_state() -> Result<NativeManagerState, ServiceRegistrationError> {
    Err(ServiceRegistrationError::NativeControlUnavailable)
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
fn start_native(replacing_existing: bool) -> Result<(), ServiceRegistrationError> {
    #[cfg(target_os = "macos")]
    return macos::start(replacing_existing);
    #[cfg(target_os = "linux")]
    return linux::start(replacing_existing);
    #[cfg(target_os = "windows")]
    return windows::start(replacing_existing);
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
    /// Activation failed and preserves both the primary and any rollback failure.
    #[error(transparent)]
    ActivationFailed(#[from] ServiceActivationFailure),
    /// Health failed and preserves the last typed probe plus bounded native/application evidence.
    #[error(transparent)]
    HealthFailed(#[from] ServiceHealthFailure),
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
    #[error("platform service operation {0} exceeded its deadline")]
    CommandTimeout(PlatformServiceOperation),
    /// A bounded platform command exceeded its output ceiling.
    #[error("platform service operation {0} exceeded its output ceiling")]
    CommandOutput(PlatformServiceOperation),
    /// A platform command returned failure.
    #[error(transparent)]
    CommandFailed(PlatformCommandFailure),
    /// Authenticated service readiness did not bind the expected generation.
    #[error("installed service did not prove exact authenticated readiness")]
    Health,
    /// Application-owned startup evidence could not be prepared or inspected.
    #[error(transparent)]
    StartupEvidence(#[from] ServiceStartupEvidenceError),
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
        HealthProbe, RuntimeIdentity, ServiceGeneration, classify_health_document,
        validate_health_document, validate_health_document_expected,
    };

    type TestResult = Result<(), Box<dyn Error>>;

    #[test]
    #[cfg(target_os = "macos")]
    fn macos_registration_binds_exact_argv_and_restart_policy() -> TestResult {
        let service = Path::new("/Users/Test & Co/Market Squawk/bin/market-squawk-service");
        let release = Path::new("/Users/Test & Co/Market Squawk/releases/0.2.0");
        let workspace = Path::new("/Users/Test & Co/Library/Application Support/Market Squawk");
        let installation = Path::new("/Users/Test & Co/Market Squawk");

        let launch_agent = macos::render_launch_agent(service, workspace, installation, release)?;
        assert!(launch_agent.contains("<key>ProgramArguments</key>"));
        assert!(launch_agent.contains(
            "<string>/Users/Test &amp; Co/Market Squawk/bin/market-squawk-service</string>"
        ));
        assert!(launch_agent.contains("<string>--data-dir</string>"));
        assert!(launch_agent.contains("<string>--installation-data-root</string>"));
        assert!(launch_agent.contains("<key>WorkingDirectory</key>"));
        assert!(launch_agent.contains("<key>KeepAlive</key>"));
        assert!(launch_agent.contains("<key>AssociatedBundleIdentifiers</key>"));
        assert!(!launch_agent.contains("<key>ProcessType</key>"));
        assert!(!launch_agent.contains("/bin/sh"));

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_registration_binds_exact_argv_and_runtime_hardening() -> TestResult {
        let service = Path::new("/home/test/Market Squawk/bin/market-squawk-service");
        let release = Path::new("/home/test/Market Squawk/releases/0.2.0");
        let workspace = Path::new("/home/test/.local/share/com.marketsquawk.desktop");
        let installation = Path::new("/home/test/.local/share/Market Squawk");
        let unit = linux::render_user_unit(service, workspace, installation, release)?;
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("WorkingDirectory="));
        assert!(unit.contains("--data-dir"));
        assert!(unit.contains("--installation-data-root"));
        assert!(unit.contains("NoNewPrivileges=yes"));
        assert!(unit.contains("CapabilityBoundingSet="));
        assert!(unit.contains("RestrictNamespaces=yes"));
        assert!(!unit.contains("PrivateUsers="));
        assert!(!unit.contains("ProtectSystem="));
        assert!(!unit.contains("loginctl enable-linger"));
        assert!(!unit.contains("/bin/sh"));

        Ok(())
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_registration_binds_exact_argv_and_least_privilege() -> TestResult {
        let task = windows::render_task_xml(
            Path::new(r"C:\Users\Test & Co\Market Squawk\market-squawk-service.exe"),
            Path::new(r"C:\Users\Test & Co\AppData\Local\com.marketsquawk.desktop"),
            Path::new(r"C:\Users\Test & Co\AppData\Local\Market Squawk"),
            Path::new(r"C:\Users\Test & Co\Market Squawk\releases\0.2.0"),
            "S-1-5-21-1000",
        )?;
        assert!(task.contains("<LogonType>InteractiveToken</LogonType>"));
        assert!(task.contains("<RunLevel>LeastPrivilege</RunLevel>"));
        assert!(task.contains("<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>"));
        assert!(task.contains("<Interval>PT1M</Interval>"));
        assert!(task.contains("<WorkingDirectory>"));
        assert!(task.contains("--installation-data-root"));
        assert!(task.contains("Test &amp; Co"));
        assert!(!task.contains("powershell"));
        assert!(!task.contains("cmd.exe"));
        let digest = windows::task_configuration_digest(task.as_bytes())?;
        assert_eq!(digest.len(), 64);
        let elevated = task.replace("LeastPrivilege", "HighestAvailable");
        assert!(windows::task_configuration_digest(elevated.as_bytes()).is_err());
        let invalid_restart = task.replace("PT1M", "PT30S");
        assert!(windows::task_configuration_digest(invalid_restart.as_bytes()).is_err());
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

        let bootstrap_required = json!({
            "status": "bootstrap_required",
            "bootstrap": {
                "state": "required",
                "requirement": "encrypted_fallback_locked",
                "installationId": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "generation": 9
            }
        });
        assert!(matches!(
            classify_health_document(&bootstrap_required, "0.2.0", None)?,
            HealthProbe::BootstrapRequired
        ));
        let mut malformed_bootstrap = bootstrap_required;
        malformed_bootstrap["bootstrap"]["state"] = json!("retrying");
        assert!(classify_health_document(&malformed_bootstrap, "0.2.0", None).is_err());
        Ok(())
    }
}
