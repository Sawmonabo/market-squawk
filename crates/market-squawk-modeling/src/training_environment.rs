//! Independent verification of the installed Python training release authority.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, Metadata};
use std::io::Read as _;
use std::path::{Component, Path, PathBuf};

use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const EMBEDDED_FOUNDATION: Option<&str> = option_env!("MARKET_SQUAWK_TRAINING_FOUNDATION_RECEIPT");
const AUTHORITY_DIRECTORY: &str = "share/market-squawk";
const ENVIRONMENT_RECEIPT: &str = "share/market-squawk/training-environment.json";
const RELEASE_MANIFEST: &str = "share/market-squawk/market-squawk-release.json";
const MAX_AUTHORITY_BYTES: u64 = 16 * 1024;
const MAX_RECORD_BYTES: u64 = 2 * 1024 * 1024;
const MAX_DISTRIBUTION_FILE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_APPLICATION_EXECUTABLE_BYTES: u64 = 768 * 1024 * 1024;
const MAX_ONNX_WORKER_EXECUTABLE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_VALIDATOR_EXECUTABLE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_DISTRIBUTION_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_DISTRIBUTION_FILES: usize = 8_192;
const MAX_DISTRIBUTION_ROOTS: usize = 64;
const MAX_RUNTIME_DISTRIBUTIONS: usize = 32;
const TRAINING_DRIVER_RECORD_PATH: &str = "../../../bin/market-squawk-train";
const TRAINING_DRIVER_RELATIVE_PATH: &str = "bin/market-squawk-train";
const RECORD_SET_DOMAIN: &[u8] = b"market-squawk-record-set-v1\0";
const RELEASE_MANIFEST_DOMAIN: &[u8] = b"market-squawk-release-manifest-v1\0";
const ENVIRONMENT_RECEIPT_DOMAIN: &[u8] = b"market-squawk-training-environment-v1\0";

/// Installed training-release verification failed closed.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TrainingEnvironmentError {
    /// The build-time foundation is absent, malformed, or non-canonical.
    #[error("embedded training foundation is invalid")]
    EmbeddedFoundation,
    /// The fixed release root or its authority directory is not controlled.
    #[error("training release root is not controlled")]
    ControlledRoot,
    /// The external environment receipt is malformed or inconsistent.
    #[error("training environment receipt is invalid")]
    EnvironmentReceipt,
    /// The release manifest is malformed or inconsistent.
    #[error("training release manifest is invalid")]
    ReleaseManifest,
    /// The installed wheel RECORD or one of its files is invalid.
    #[error("installed training distribution is invalid")]
    InstalledDistribution,
    /// The active interpreter, extension, or validator is not the admitted release object.
    #[error("active training runtime differs from the admitted release")]
    RuntimeWitness,
}

/// Independently verified identity of one installed Python training environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedTrainingEnvironment {
    receipt_sha256: [u8; 32],
    release_manifest_sha256: [u8; 32],
    application_sha256: [u8; 32],
    onnx_worker_sha256: [u8; 32],
    validator_sha256: [u8; 32],
    python_tag: Box<str>,
    python_version: Box<str>,
    training_code_revision: Box<str>,
}

impl VerifiedTrainingEnvironment {
    /// Returns the exact external environment-receipt digest.
    #[must_use]
    pub const fn receipt_sha256(&self) -> [u8; 32] {
        self.receipt_sha256
    }

    /// Returns the exact signed release-manifest digest installed into this environment.
    #[must_use]
    pub const fn release_manifest_sha256(&self) -> [u8; 32] {
        self.release_manifest_sha256
    }

    /// Returns the signed exact application executable digest.
    #[must_use]
    pub const fn application_sha256(&self) -> [u8; 32] {
        self.application_sha256
    }

    /// Returns the signed exact ONNX worker executable digest.
    #[must_use]
    pub const fn onnx_worker_sha256(&self) -> [u8; 32] {
        self.onnx_worker_sha256
    }

    /// Returns the signed exact model-validator executable digest.
    #[must_use]
    pub const fn validator_sha256(&self) -> [u8; 32] {
        self.validator_sha256
    }

    /// Returns the signed CPython compatibility tag for this environment.
    #[must_use]
    pub fn python_tag(&self) -> &str {
        &self.python_tag
    }

    /// Returns the signed exact CPython version for this environment.
    #[must_use]
    pub fn python_version(&self) -> &str {
        &self.python_version
    }

    /// Returns the builder-derived source-closure revision.
    #[must_use]
    pub fn training_code_revision(&self) -> &str {
        &self.training_code_revision
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FoundationWire {
    build_python_sha256: String,
    build_python_version: String,
    cargo_lock_sha256: String,
    requirements_lock_sha256: String,
    release_public_key: String,
    release_signer_sha256: String,
    runtime_distributions: Vec<RuntimeRequirementWire>,
    schema_version: u32,
    source_closure_sha256: String,
    toolchain_sha256: String,
    training_code_revision: String,
    wheelhouse_lock_sha256: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SignedReleaseManifestWire {
    payload: ReleaseManifestWire,
    schema_version: u32,
    signature: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReleaseManifestWire {
    application: FileDigestWire,
    foundation_sha256: String,
    onnx_worker: FileDigestWire,
    project_wheel: ProjectWheelWire,
    schema_version: u32,
    validator: FileDigestWire,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProjectWheelWire {
    abi_tag: String,
    filename: String,
    macos_deployment_target: String,
    platform_tag: String,
    python_tag: String,
    sha256: String,
    size_bytes: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FileDigestWire {
    sha256: String,
    size_bytes: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SignedEnvironmentWire {
    payload: EnvironmentWire,
    schema_version: u32,
    signature: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EnvironmentWire {
    foundation_sha256: String,
    interpreter: InterpreterWire,
    native_extension: RelativeFileWire,
    project_distribution: DistributionWire,
    release_manifest_sha256: String,
    runtime_distributions: Vec<DistributionWire>,
    training_code_revision: String,
    validator_sha256: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InterpreterWire {
    executable_relative_path: String,
    implementation: String,
    python_tag: String,
    sha256: String,
    size_bytes: u64,
    version: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RelativeFileWire {
    relative_path: String,
    sha256: String,
    size_bytes: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DistributionWire {
    file_count: usize,
    file_set_sha256: String,
    name: String,
    record_relative_path: String,
    record_sha256: String,
    record_size_bytes: u64,
    roots: Vec<String>,
    version: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeRequirementWire {
    name: String,
    version: String,
}

struct VerifiedFiles {
    environment: EnvironmentWire,
    receipt_sha256: [u8; 32],
    release_manifest_sha256: [u8; 32],
    application_sha256: [u8; 32],
    application_size_bytes: u64,
    onnx_worker_sha256: [u8; 32],
    onnx_worker_size_bytes: u64,
    validator_sha256: [u8; 32],
    root: PathBuf,
}

struct FileIdentity {
    bytes: Vec<u8>,
    sha256: [u8; 32],
    size_bytes: u64,
}

/// Verifies the fixed installed receipt, wheel RECORD, active interpreter, and native extension.
///
/// # Errors
///
/// Returns an error when any embedded, external, filesystem, or runtime identity differs.
pub fn verify_python_training_environment(
    root: &Path,
    interpreter: &Path,
    implementation: &str,
    version: &str,
    python_tag: &str,
    native_extension: &Path,
) -> Result<VerifiedTrainingEnvironment, TrainingEnvironmentError> {
    let verified = verify_installed_files(root)?;
    let expected_interpreter = verified.root.join(relative_path(
        &verified.environment.interpreter.executable_relative_path,
    )?);
    let expected_native = verified.root.join(relative_path(
        &verified.environment.native_extension.relative_path,
    )?);
    if canonical(interpreter)? != canonical(&expected_interpreter)?
        || canonical(native_extension)? != canonical(&expected_native)?
        || implementation != verified.environment.interpreter.implementation
        || version != verified.environment.interpreter.version
        || python_tag != verified.environment.interpreter.python_tag
    {
        return Err(TrainingEnvironmentError::RuntimeWitness);
    }
    verified.into_public()
}

/// Verifies the fixed installed receipt and the currently executing Rust validator.
///
/// # Errors
///
/// Returns an error when the validator or any installed release identity differs.
pub fn verify_validator_training_environment(
    root: &Path,
    validator: &Path,
) -> Result<VerifiedTrainingEnvironment, TrainingEnvironmentError> {
    let verified = verify_installed_files(root)?;
    let expected = verified.root.join("bin/market-squawk-model-validator");
    if canonical(validator)? != canonical(&expected)? {
        return Err(TrainingEnvironmentError::RuntimeWitness);
    }
    verified.into_public()
}

/// Verifies the fixed installed receipt and the selected application and sibling ONNX worker.
///
/// The selected programs may be separately installed copies. Their stable sizes and SHA-256
/// identities must equal the executables bound by the signed release manifest.
///
/// # Errors
///
/// Returns an error when the application, worker, or any installed release identity differs.
pub fn verify_application_training_environment(
    root: &Path,
    application: &Path,
    onnx_worker: &Path,
) -> Result<VerifiedTrainingEnvironment, TrainingEnvironmentError> {
    let verified = verify_installed_files(root)?;
    verify_runtime_program_identity(
        application,
        verified.application_sha256,
        verified.application_size_bytes,
        MAX_APPLICATION_EXECUTABLE_BYTES,
    )?;
    verify_runtime_program_identity(
        onnx_worker,
        verified.onnx_worker_sha256,
        verified.onnx_worker_size_bytes,
        MAX_ONNX_WORKER_EXECUTABLE_BYTES,
    )?;
    verified.into_public()
}

impl VerifiedFiles {
    fn into_public(self) -> Result<VerifiedTrainingEnvironment, TrainingEnvironmentError> {
        Ok(VerifiedTrainingEnvironment {
            receipt_sha256: self.receipt_sha256,
            release_manifest_sha256: self.release_manifest_sha256,
            application_sha256: self.application_sha256,
            onnx_worker_sha256: self.onnx_worker_sha256,
            validator_sha256: self.validator_sha256,
            python_tag: self.environment.interpreter.python_tag.into(),
            python_version: self.environment.interpreter.version.into(),
            training_code_revision: self.environment.training_code_revision.into(),
        })
    }
}

fn verify_installed_files(root: &Path) -> Result<VerifiedFiles, TrainingEnvironmentError> {
    verify_directory(root).map_err(|_| TrainingEnvironmentError::ControlledRoot)?;
    verify_directory(&root.join(AUTHORITY_DIRECTORY))
        .map_err(|_| TrainingEnvironmentError::ControlledRoot)?;
    let canonical_root = canonical(root)?;

    let foundation = embedded_foundation()?;
    let foundation_bytes = EMBEDDED_FOUNDATION
        .ok_or(TrainingEnvironmentError::EmbeddedFoundation)?
        .as_bytes();
    let foundation_sha256 = hash(foundation_bytes);

    let receipt_file = read_controlled(
        &canonical_root,
        Path::new(ENVIRONMENT_RECEIPT),
        MAX_AUTHORITY_BYTES,
        false,
    )?;
    let signed_environment: SignedEnvironmentWire = canonical_wire(
        &receipt_file.bytes,
        TrainingEnvironmentError::EnvironmentReceipt,
    )?;
    let manifest_file = read_controlled(
        &canonical_root,
        Path::new(RELEASE_MANIFEST),
        MAX_AUTHORITY_BYTES,
        false,
    )?;
    let signed_manifest: SignedReleaseManifestWire = canonical_wire(
        &manifest_file.bytes,
        TrainingEnvironmentError::ReleaseManifest,
    )?;
    if signed_environment.schema_version != 1
        || signed_manifest.schema_version != 2
        || !verify_signature(
            &foundation.release_public_key,
            ENVIRONMENT_RECEIPT_DOMAIN,
            &signed_environment.payload,
            &signed_environment.signature,
        )
        || !verify_signature(
            &foundation.release_public_key,
            RELEASE_MANIFEST_DOMAIN,
            &signed_manifest.payload,
            &signed_manifest.signature,
        )
    {
        return Err(TrainingEnvironmentError::EnvironmentReceipt);
    }
    let environment = signed_environment.payload;
    let manifest = signed_manifest.payload;

    if manifest.schema_version != 2
        || parse_hex(&environment.foundation_sha256)? != foundation_sha256
        || parse_hex(&manifest.foundation_sha256)? != foundation_sha256
        || parse_hex(&environment.release_manifest_sha256)? != manifest_file.sha256
        || environment.training_code_revision != foundation.training_code_revision
        || environment.validator_sha256 != manifest.validator.sha256
        || !valid_interpreter(&environment.interpreter)
        || !valid_project_wheel(&manifest.project_wheel)
        || manifest.application.size_bytes == 0
        || manifest.onnx_worker.size_bytes == 0
        || manifest.validator.size_bytes == 0
    {
        return Err(TrainingEnvironmentError::EnvironmentReceipt);
    }

    let interpreter = read_controlled(
        &canonical_root,
        relative_path(&environment.interpreter.executable_relative_path)?,
        MAX_DISTRIBUTION_FILE_BYTES,
        true,
    )?;
    exact_file(
        &interpreter,
        &environment.interpreter.sha256,
        environment.interpreter.size_bytes,
        TrainingEnvironmentError::RuntimeWitness,
    )?;

    let validator = read_controlled(
        &canonical_root,
        Path::new("bin/market-squawk-model-validator"),
        MAX_VALIDATOR_EXECUTABLE_BYTES,
        false,
    )?;
    exact_file(
        &validator,
        &manifest.validator.sha256,
        manifest.validator.size_bytes,
        TrainingEnvironmentError::RuntimeWitness,
    )?;

    let application = read_controlled(
        &canonical_root,
        Path::new("bin/market-squawk"),
        MAX_APPLICATION_EXECUTABLE_BYTES,
        false,
    )?;
    exact_file(
        &application,
        &manifest.application.sha256,
        manifest.application.size_bytes,
        TrainingEnvironmentError::RuntimeWitness,
    )?;

    let onnx_worker = read_controlled(
        &canonical_root,
        Path::new("bin/market-squawk-onnx-worker"),
        MAX_ONNX_WORKER_EXECUTABLE_BYTES,
        false,
    )?;
    exact_file(
        &onnx_worker,
        &manifest.onnx_worker.sha256,
        manifest.onnx_worker.size_bytes,
        TrainingEnvironmentError::RuntimeWitness,
    )?;

    let wheel = read_controlled(
        &canonical_root,
        &Path::new(AUTHORITY_DIRECTORY).join(&manifest.project_wheel.filename),
        MAX_DISTRIBUTION_FILE_BYTES,
        false,
    )?;
    exact_file(
        &wheel,
        &manifest.project_wheel.sha256,
        manifest.project_wheel.size_bytes,
        TrainingEnvironmentError::ReleaseManifest,
    )?;

    verify_distributions(&canonical_root, &environment, &foundation)?;
    Ok(VerifiedFiles {
        environment,
        receipt_sha256: receipt_file.sha256,
        release_manifest_sha256: manifest_file.sha256,
        application_sha256: application.sha256,
        application_size_bytes: application.size_bytes,
        onnx_worker_sha256: onnx_worker.sha256,
        onnx_worker_size_bytes: onnx_worker.size_bytes,
        validator_sha256: validator.sha256,
        root: canonical_root,
    })
}

fn verify_runtime_program_identity(
    path: &Path,
    expected_sha256: [u8; 32],
    expected_size_bytes: u64,
    maximum_bytes: u64,
) -> Result<(), TrainingEnvironmentError> {
    if expected_size_bytes == 0 || expected_size_bytes > maximum_bytes {
        return Err(TrainingEnvironmentError::RuntimeWitness);
    }
    let named = fs::symlink_metadata(path).map_err(|_| TrainingEnvironmentError::RuntimeWitness)?;
    if named.file_type().is_symlink() || !named.is_file() {
        return Err(TrainingEnvironmentError::RuntimeWitness);
    }
    controlled_metadata(&named).map_err(|_| TrainingEnvironmentError::RuntimeWitness)?;
    let canonical_path =
        fs::canonicalize(path).map_err(|_| TrainingEnvironmentError::RuntimeWitness)?;
    if !canonical_path.is_absolute() {
        return Err(TrainingEnvironmentError::RuntimeWitness);
    }
    let mut file =
        File::open(&canonical_path).map_err(|_| TrainingEnvironmentError::RuntimeWitness)?;
    let before = file
        .metadata()
        .map_err(|_| TrainingEnvironmentError::RuntimeWitness)?;
    if !same_file(&named, &before)
        || controlled_metadata(&before).is_err()
        || before.len() != expected_size_bytes
    {
        return Err(TrainingEnvironmentError::RuntimeWitness);
    }

    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut observed = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| TrainingEnvironmentError::RuntimeWitness)?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(u64::try_from(read).map_err(|_| TrainingEnvironmentError::RuntimeWitness)?)
            .filter(|value| *value <= maximum_bytes)
            .ok_or(TrainingEnvironmentError::RuntimeWitness)?;
        digest.update(&buffer[..read]);
    }

    let after = file
        .metadata()
        .map_err(|_| TrainingEnvironmentError::RuntimeWitness)?;
    let named_after =
        fs::symlink_metadata(path).map_err(|_| TrainingEnvironmentError::RuntimeWitness)?;
    let canonical_after =
        fs::canonicalize(path).map_err(|_| TrainingEnvironmentError::RuntimeWitness)?;
    if named_after.file_type().is_symlink()
        || !named_after.is_file()
        || controlled_metadata(&after).is_err()
        || controlled_metadata(&named_after).is_err()
        || !same_file(&before, &after)
        || !same_file(&before, &named_after)
        || canonical_after != canonical_path
        || observed != expected_size_bytes
        || <[u8; 32]>::from(digest.finalize()) != expected_sha256
    {
        return Err(TrainingEnvironmentError::RuntimeWitness);
    }
    Ok(())
}

fn embedded_foundation() -> Result<FoundationWire, TrainingEnvironmentError> {
    let bytes = EMBEDDED_FOUNDATION
        .ok_or(TrainingEnvironmentError::EmbeddedFoundation)?
        .as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_AUTHORITY_BYTES as usize || !bytes.is_ascii() {
        return Err(TrainingEnvironmentError::EmbeddedFoundation);
    }
    let wire: FoundationWire = canonical_wire(bytes, TrainingEnvironmentError::EmbeddedFoundation)?;
    let digests = [
        &wire.build_python_sha256,
        &wire.cargo_lock_sha256,
        &wire.requirements_lock_sha256,
        &wire.release_signer_sha256,
        &wire.source_closure_sha256,
        &wire.toolchain_sha256,
        &wire.wheelhouse_lock_sha256,
    ];
    if wire.schema_version != 1
        || wire.training_code_revision != wire.source_closure_sha256
        || !valid_version(&wire.build_python_version)
        || parse_hex(&wire.release_public_key).is_err()
        || !valid_runtime_requirements(&wire.runtime_distributions)
        || digests.into_iter().any(|value| !valid_hex(value))
    {
        return Err(TrainingEnvironmentError::EmbeddedFoundation);
    }
    Ok(wire)
}

struct VerifiedDistribution {
    entries: BTreeMap<String, ([u8; 32], u64)>,
    roots: BTreeSet<String>,
    site_packages: PathBuf,
}

fn verify_distributions(
    root: &Path,
    environment: &EnvironmentWire,
    foundation: &FoundationWire,
) -> Result<(), TrainingEnvironmentError> {
    if environment.project_distribution.name != "market-squawk"
        || environment.project_distribution.version != "0.2.0"
        || environment.runtime_distributions.len() != foundation.runtime_distributions.len()
    {
        return Err(TrainingEnvironmentError::InstalledDistribution);
    }
    let project = verify_distribution(root, &environment.project_distribution)?;
    let expected_site_packages = expected_site_packages(root, &environment.interpreter.version)?;
    if canonical(&project.site_packages)? != canonical(&expected_site_packages)? {
        return Err(TrainingEnvironmentError::InstalledDistribution);
    }
    let mut owned_roots = project.roots.clone();
    for (wire, requirement) in environment
        .runtime_distributions
        .iter()
        .zip(&foundation.runtime_distributions)
    {
        if wire.name != requirement.name || wire.version != requirement.version {
            return Err(TrainingEnvironmentError::InstalledDistribution);
        }
        let verified = verify_distribution(root, wire)?;
        if canonical(&verified.site_packages)? != canonical(&project.site_packages)?
            || verified
                .roots
                .iter()
                .any(|value| !owned_roots.insert(value.clone()))
        {
            return Err(TrainingEnvironmentError::InstalledDistribution);
        }
    }

    let native_relative = relative_path(&environment.native_extension.relative_path)?;
    let native_path = root.join(native_relative);
    let native_entry = native_path
        .strip_prefix(&project.site_packages)
        .map_err(|_| TrainingEnvironmentError::InstalledDistribution)?
        .to_str()
        .ok_or(TrainingEnvironmentError::InstalledDistribution)?
        .replace('\\', "/");
    let (digest, size) = project
        .entries
        .get(&native_entry)
        .ok_or(TrainingEnvironmentError::InstalledDistribution)?;
    if *digest != parse_hex(&environment.native_extension.sha256)?
        || *size != environment.native_extension.size_bytes
    {
        return Err(TrainingEnvironmentError::InstalledDistribution);
    }
    Ok(())
}

fn verify_distribution(
    root: &Path,
    distribution: &DistributionWire,
) -> Result<VerifiedDistribution, TrainingEnvironmentError> {
    if !valid_distribution(distribution) {
        return Err(TrainingEnvironmentError::InstalledDistribution);
    }
    let record_relative = relative_path(&distribution.record_relative_path)?;
    let record = read_controlled(root, record_relative, MAX_RECORD_BYTES, false)?;
    exact_file(
        &record,
        &distribution.record_sha256,
        distribution.record_size_bytes,
        TrainingEnvironmentError::InstalledDistribution,
    )?;
    let record_path = root.join(record_relative);
    let site_packages = record_path
        .parent()
        .and_then(Path::parent)
        .ok_or(TrainingEnvironmentError::InstalledDistribution)?;
    verify_directory(site_packages).map_err(|_| TrainingEnvironmentError::InstalledDistribution)?;
    let record_entry = record_path
        .strip_prefix(site_packages)
        .map_err(|_| TrainingEnvironmentError::InstalledDistribution)?
        .to_str()
        .ok_or(TrainingEnvironmentError::InstalledDistribution)?
        .replace('\\', "/");
    let roots = distribution.roots.iter().cloned().collect::<BTreeSet<_>>();

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(false)
        .from_reader(record.bytes.as_slice());
    let mut entries = BTreeMap::new();
    let mut saw_record = false;
    let require_training_driver =
        distribution.name == "market-squawk" && distribution.version == "0.2.0";
    let mut saw_training_driver = false;
    let mut total_bytes = 0_u64;
    for row in reader.records() {
        let row = row.map_err(|_| TrainingEnvironmentError::InstalledDistribution)?;
        if row.len() != 3 || entries.len() >= MAX_DISTRIBUTION_FILES {
            return Err(TrainingEnvironmentError::InstalledDistribution);
        }
        let name = row
            .get(0)
            .ok_or(TrainingEnvironmentError::InstalledDistribution)?;
        let digest = row
            .get(1)
            .ok_or(TrainingEnvironmentError::InstalledDistribution)?;
        let size = row
            .get(2)
            .ok_or(TrainingEnvironmentError::InstalledDistribution)?;
        if name == record_entry {
            if saw_record || !digest.is_empty() || !size.is_empty() {
                return Err(TrainingEnvironmentError::InstalledDistribution);
            }
            saw_record = true;
            continue;
        }
        let is_training_driver = require_training_driver && name == TRAINING_DRIVER_RECORD_PATH;
        let file = if is_training_driver {
            if saw_training_driver || digest.is_empty() || size.is_empty() {
                return Err(TrainingEnvironmentError::InstalledDistribution);
            }
            saw_training_driver = true;
            read_controlled(
                root,
                Path::new(TRAINING_DRIVER_RELATIVE_PATH),
                MAX_DISTRIBUTION_FILE_BYTES,
                false,
            )?
        } else {
            let relative = relative_path(name)?;
            let first = relative
                .components()
                .next()
                .and_then(|value| match value {
                    Component::Normal(value) => value.to_str(),
                    _ => None,
                })
                .ok_or(TrainingEnvironmentError::InstalledDistribution)?;
            if !roots.contains(first) {
                return Err(TrainingEnvironmentError::InstalledDistribution);
            }
            read_controlled(site_packages, relative, MAX_DISTRIBUTION_FILE_BYTES, false)?
        };
        let size = if digest.is_empty() && size.is_empty() {
            file.size_bytes
        } else {
            let expected = digest
                .strip_prefix("sha256=")
                .ok_or(TrainingEnvironmentError::InstalledDistribution)?;
            let size = size
                .parse::<u64>()
                .map_err(|_| TrainingEnvironmentError::InstalledDistribution)?;
            if file.size_bytes != size || base64_url(&file.sha256) != expected {
                return Err(TrainingEnvironmentError::InstalledDistribution);
            }
            size
        };
        total_bytes = total_bytes
            .checked_add(size)
            .ok_or(TrainingEnvironmentError::InstalledDistribution)?;
        if total_bytes > MAX_DISTRIBUTION_BYTES {
            return Err(TrainingEnvironmentError::InstalledDistribution);
        }
        if entries
            .insert(name.to_owned(), (file.sha256, size))
            .is_some()
        {
            return Err(TrainingEnvironmentError::InstalledDistribution);
        }
    }
    if !saw_record
        || require_training_driver != saw_training_driver
        || entries.len() != distribution.file_count
        || record_set_digest(&entries) != parse_hex(&distribution.file_set_sha256)?
    {
        return Err(TrainingEnvironmentError::InstalledDistribution);
    }
    let mut expected_paths = entries
        .keys()
        .filter(|name| name.as_str() != TRAINING_DRIVER_RECORD_PATH)
        .cloned()
        .collect::<BTreeSet<_>>();
    expected_paths.insert(record_entry);
    if scan_distribution_paths(site_packages, &roots)? != expected_paths {
        return Err(TrainingEnvironmentError::InstalledDistribution);
    }
    Ok(VerifiedDistribution {
        entries,
        roots,
        site_packages: site_packages.to_path_buf(),
    })
}

fn expected_site_packages(root: &Path, version: &str) -> Result<PathBuf, TrainingEnvironmentError> {
    let parts = version.split('.').collect::<Vec<_>>();
    let [major, minor, _patch] = parts.as_slice() else {
        return Err(TrainingEnvironmentError::InstalledDistribution);
    };
    Ok(root.join(format!("lib/python{major}.{minor}/site-packages")))
}

fn scan_distribution_paths(
    site_packages: &Path,
    roots: &BTreeSet<String>,
) -> Result<BTreeSet<String>, TrainingEnvironmentError> {
    let mut files = BTreeSet::new();
    let mut pending = roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    let mut directories = 0_usize;
    let maximum_paths = MAX_DISTRIBUTION_FILES
        .checked_mul(3)
        .and_then(|value| value.checked_add(1))
        .ok_or(TrainingEnvironmentError::InstalledDistribution)?;
    let mut discovered = pending.len();
    while let Some(relative) = pending.pop() {
        let path = site_packages.join(&relative);
        let metadata = fs::symlink_metadata(&path)
            .map_err(|_| TrainingEnvironmentError::InstalledDistribution)?;
        if metadata.file_type().is_symlink() {
            return Err(TrainingEnvironmentError::InstalledDistribution);
        }
        if metadata.is_file() {
            controlled_metadata(&metadata)
                .map_err(|_| TrainingEnvironmentError::InstalledDistribution)?;
            let value = relative
                .to_str()
                .ok_or(TrainingEnvironmentError::InstalledDistribution)?
                .replace('\\', "/");
            if !files.insert(value) || files.len() > MAX_DISTRIBUTION_FILES + 1 {
                return Err(TrainingEnvironmentError::InstalledDistribution);
            }
            continue;
        }
        if !metadata.is_dir() {
            return Err(TrainingEnvironmentError::InstalledDistribution);
        }
        controlled_metadata(&metadata)
            .map_err(|_| TrainingEnvironmentError::InstalledDistribution)?;
        directories = directories
            .checked_add(1)
            .ok_or(TrainingEnvironmentError::InstalledDistribution)?;
        if directories > MAX_DISTRIBUTION_FILES * 2 {
            return Err(TrainingEnvironmentError::InstalledDistribution);
        }
        for entry in
            fs::read_dir(path).map_err(|_| TrainingEnvironmentError::InstalledDistribution)?
        {
            let entry = entry.map_err(|_| TrainingEnvironmentError::InstalledDistribution)?;
            let name = entry.file_name();
            if name.to_str().is_none() {
                return Err(TrainingEnvironmentError::InstalledDistribution);
            }
            discovered = discovered
                .checked_add(1)
                .filter(|value| *value <= maximum_paths)
                .ok_or(TrainingEnvironmentError::InstalledDistribution)?;
            pending.push(relative.join(name));
        }
    }
    Ok(files)
}

fn verify_signature<T: Serialize>(
    encoded_key: &str,
    domain: &[u8],
    payload: &T,
    encoded_signature: &str,
) -> bool {
    let Ok(key_bytes) = parse_hex(encoded_key) else {
        return false;
    };
    let Ok(key) = VerifyingKey::from_bytes(&key_bytes) else {
        return false;
    };
    let Ok(signature_bytes) = parse_signature(encoded_signature) else {
        return false;
    };
    let signature = Signature::from_bytes(&signature_bytes);
    let Ok(value) = serde_json::to_value(payload) else {
        return false;
    };
    let Ok(encoded_payload) = serde_json::to_vec(&value) else {
        return false;
    };
    let mut message = Vec::with_capacity(domain.len() + encoded_payload.len());
    message.extend_from_slice(domain);
    message.extend_from_slice(&encoded_payload);
    key.verify_strict(&message, &signature).is_ok()
}

fn canonical_wire<T>(
    bytes: &[u8],
    error: TrainingEnvironmentError,
) -> Result<T, TrainingEnvironmentError>
where
    T: for<'de> Deserialize<'de>,
{
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| error)?;
    if serde_json::to_vec(&value).map_err(|_| error)? != bytes {
        return Err(error);
    }
    serde_json::from_value(value).map_err(|_| error)
}

fn read_controlled(
    root: &Path,
    relative: &Path,
    maximum: u64,
    allow_file_symlink: bool,
) -> Result<FileIdentity, TrainingEnvironmentError> {
    if !relative
        .components()
        .all(|part| matches!(part, Component::Normal(_)))
    {
        return Err(TrainingEnvironmentError::ControlledRoot);
    }
    let path = root.join(relative);
    verify_parent_chain(root, &path)?;
    let link = fs::symlink_metadata(&path).map_err(|_| TrainingEnvironmentError::ControlledRoot)?;
    if (link.file_type().is_symlink() && !allow_file_symlink)
        || (!link.file_type().is_file() && !link.file_type().is_symlink())
    {
        return Err(TrainingEnvironmentError::ControlledRoot);
    }
    if !allow_file_symlink && !canonical(&path)?.starts_with(canonical(root)?) {
        return Err(TrainingEnvironmentError::ControlledRoot);
    }
    let mut file = File::open(&path).map_err(|_| TrainingEnvironmentError::ControlledRoot)?;
    let before = file
        .metadata()
        .map_err(|_| TrainingEnvironmentError::ControlledRoot)?;
    verify_file_metadata(&before, maximum)?;
    let capacity =
        usize::try_from(before.len()).map_err(|_| TrainingEnvironmentError::ControlledRoot)?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes)
        .map_err(|_| TrainingEnvironmentError::ControlledRoot)?;
    let after = file
        .metadata()
        .map_err(|_| TrainingEnvironmentError::ControlledRoot)?;
    if !same_file(&before, &after) || bytes.len() as u64 != before.len() {
        return Err(TrainingEnvironmentError::ControlledRoot);
    }
    Ok(FileIdentity {
        sha256: hash(&bytes),
        size_bytes: before.len(),
        bytes,
    })
}

fn verify_parent_chain(root: &Path, path: &Path) -> Result<(), TrainingEnvironmentError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| TrainingEnvironmentError::ControlledRoot)?;
    let mut current = root.to_path_buf();
    verify_directory(&current)?;
    let components = relative.components().collect::<Vec<_>>();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        current.push(component.as_os_str());
        verify_directory(&current)?;
    }
    Ok(())
}

fn verify_directory(path: &Path) -> Result<(), TrainingEnvironmentError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| TrainingEnvironmentError::ControlledRoot)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(TrainingEnvironmentError::ControlledRoot);
    }
    controlled_metadata(&metadata)
}

fn verify_file_metadata(metadata: &Metadata, maximum: u64) -> Result<(), TrainingEnvironmentError> {
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(TrainingEnvironmentError::ControlledRoot);
    }
    controlled_metadata(metadata)
}

#[cfg(unix)]
fn controlled_metadata(metadata: &Metadata) -> Result<(), TrainingEnvironmentError> {
    use std::os::unix::fs::MetadataExt as _;

    if metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o022 != 0 {
        return Err(TrainingEnvironmentError::ControlledRoot);
    }
    Ok(())
}

#[cfg(not(unix))]
fn controlled_metadata(_metadata: &Metadata) -> Result<(), TrainingEnvironmentError> {
    Ok(())
}

#[cfg(unix)]
fn same_file(left: &Metadata, right: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
}

#[cfg(not(unix))]
fn same_file(left: &Metadata, right: &Metadata) -> bool {
    left.len() == right.len() && left.modified().ok() == right.modified().ok()
}

fn exact_file(
    file: &FileIdentity,
    expected_sha256: &str,
    expected_size: u64,
    error: TrainingEnvironmentError,
) -> Result<(), TrainingEnvironmentError> {
    if file.sha256 != parse_hex(expected_sha256)? || file.size_bytes != expected_size {
        return Err(error);
    }
    Ok(())
}

fn relative_path(value: &str) -> Result<&Path, TrainingEnvironmentError> {
    let path = Path::new(value);
    if value.is_empty()
        || value.len() > 1_024
        || value.contains('\\')
        || path.is_absolute()
        || !path
            .components()
            .all(|part| matches!(part, Component::Normal(value) if !value.is_empty()))
    {
        return Err(TrainingEnvironmentError::EnvironmentReceipt);
    }
    Ok(path)
}

fn valid_interpreter(value: &InterpreterWire) -> bool {
    let expected_tag = match value.version.split('.').collect::<Vec<_>>().as_slice() {
        ["3", "12", patch] if patch.bytes().all(|byte| byte.is_ascii_digit()) => "cp312",
        ["3", "13", patch] if patch.bytes().all(|byte| byte.is_ascii_digit()) => "cp313",
        _ => return false,
    };
    value.implementation == "cpython"
        && value.python_tag == expected_tag
        && value.executable_relative_path == "bin/python"
        && value.size_bytes > 0
        && valid_hex(&value.sha256)
}

fn valid_project_wheel(value: &ProjectWheelWire) -> bool {
    !value.filename.is_empty()
        && value.filename.len() <= 255
        && !value.filename.contains(['/', '\\'])
        && value.filename.ends_with(".whl")
        && value.python_tag == "cp310"
        && value.abi_tag == "abi3"
        && value.platform_tag == "macosx_12_0_arm64"
        && value.macos_deployment_target == "12.0"
        && value.size_bytes > 0
        && valid_hex(&value.sha256)
}

fn valid_runtime_requirements(values: &[RuntimeRequirementWire]) -> bool {
    !values.is_empty()
        && values.len() <= MAX_RUNTIME_DISTRIBUTIONS
        && values.iter().all(|value| {
            valid_distribution_name(&value.name) && valid_release_version(&value.version)
        })
        && values
            .windows(2)
            .all(|pair| pair[0].name.as_str() < pair[1].name.as_str())
}

fn valid_distribution(value: &DistributionWire) -> bool {
    value.file_count > 0
        && value.file_count <= MAX_DISTRIBUTION_FILES
        && valid_hex(&value.file_set_sha256)
        && valid_distribution_name(&value.name)
        && valid_hex(&value.record_sha256)
        && value.record_size_bytes > 0
        && value.record_size_bytes <= MAX_RECORD_BYTES
        && !value.roots.is_empty()
        && value.roots.len() <= MAX_DISTRIBUTION_ROOTS
        && value.roots.iter().all(|root| {
            let path = Path::new(root);
            !root.is_empty()
                && root.len() <= 255
                && path.components().count() == 1
                && matches!(path.components().next(), Some(Component::Normal(_)))
        })
        && value
            .roots
            .windows(2)
            .all(|pair| pair[0].as_str() < pair[1].as_str())
        && valid_release_version(&value.version)
}

fn valid_distribution_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && !value.contains("--")
}

fn valid_release_version(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'+' | b'-' | b'_'))
}

fn valid_version(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<_>>();
    parts.len() == 3
        && parts
            .into_iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

fn valid_hex(value: &str) -> bool {
    value.len() == 64
        && value != "0000000000000000000000000000000000000000000000000000000000000000"
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn parse_hex(value: &str) -> Result<[u8; 32], TrainingEnvironmentError> {
    if !valid_hex(value) {
        return Err(TrainingEnvironmentError::EnvironmentReceipt);
    }
    let mut bytes = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (nibble(chunk[0])? << 4) | nibble(chunk[1])?;
    }
    Ok(bytes)
}

fn parse_signature(value: &str) -> Result<[u8; 64], TrainingEnvironmentError> {
    if value.len() != 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(TrainingEnvironmentError::EnvironmentReceipt);
    }
    let mut bytes = [0_u8; 64];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (nibble(chunk[0])? << 4) | nibble(chunk[1])?;
    }
    Ok(bytes)
}

fn nibble(value: u8) -> Result<u8, TrainingEnvironmentError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(TrainingEnvironmentError::EnvironmentReceipt),
    }
}

fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn canonical(path: &Path) -> Result<PathBuf, TrainingEnvironmentError> {
    fs::canonicalize(path).map_err(|_| TrainingEnvironmentError::ControlledRoot)
}

fn record_set_digest(entries: &BTreeMap<String, ([u8; 32], u64)>) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(RECORD_SET_DOMAIN);
    for (path, (sha256, size)) in entries {
        digest.update(u32::try_from(path.len()).unwrap_or(u32::MAX).to_be_bytes());
        digest.update(path.as_bytes());
        digest.update(size.to_be_bytes());
        digest.update(sha256);
    }
    digest.finalize().into()
}

fn base64_url(bytes: &[u8; 32]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut encoded = String::with_capacity(43);
    for chunk in bytes.chunks(3) {
        let value = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        encoded.push(char::from(ALPHABET[((value >> 18) & 0x3f) as usize]));
        encoded.push(char::from(ALPHABET[((value >> 12) & 0x3f) as usize]));
        if chunk.len() > 1 {
            encoded.push(char::from(ALPHABET[((value >> 6) & 0x3f) as usize]));
        }
        if chunk.len() > 2 {
            encoded.push(char::from(ALPHABET[(value & 0x3f) as usize]));
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{TrainingEnvironmentError, hash, verify_runtime_program_identity};

    #[test]
    fn runtime_program_identity_accepts_a_copy_and_rejects_a_substitution()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let installed = temporary.path().join("installed-program");
        let selected = temporary.path().join("selected-program");
        fs::write(&installed, b"signed program bytes")?;
        fs::copy(&installed, &selected)?;
        assert_ne!(fs::canonicalize(&installed)?, fs::canonicalize(&selected)?);
        let expected = hash(b"signed program bytes");

        verify_runtime_program_identity(
            &selected,
            expected,
            b"signed program bytes".len() as u64,
            1024,
        )?;

        fs::write(&selected, b"tamper program bytes")?;
        assert_eq!(
            verify_runtime_program_identity(
                &selected,
                expected,
                b"signed program bytes".len() as u64,
                1024,
            ),
            Err(TrainingEnvironmentError::RuntimeWitness)
        );
        Ok(())
    }
}
