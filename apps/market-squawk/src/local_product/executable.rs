//! Exact executable identity and sibling-helper admission at the process boundary.

use std::fs::{self, File};
use std::io::{self, Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};

use market_squawk_modeling::OnnxWorkerProgramError;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const HASH_CHUNK_BYTES: usize = 64 * 1024;
const APPLICATION_BASENAME: &str = "market-squawk";
const DESKTOP_APPLICATION_BASENAME: &str = "market-squawk-desktop";
const SERVICE_APPLICATION_BASENAME: &str = "market-squawk-service";
#[cfg(debug_assertions)]
const MCP_RELAY_APPLICATION_BASENAME: &str = "market-squawk-mcp-relay";
const ONNX_WORKER_BASENAME: &str = "market-squawk-onnx-worker";

/// Records build provenance without scanning or attesting the executable's contents.
pub(super) fn current_program_build_metadata() -> Result<Vec<u8>, ExecutableIdentityError> {
    #[derive(serde::Serialize)]
    struct BuildMetadata {
        schema_version: u32,
        package_version: &'static str,
        recorded_native_build_revision: Option<&'static str>,
        target_os: &'static str,
        target_arch: &'static str,
        executable_size: u64,
        executable_modified_unix_nanos: Option<String>,
    }

    let executable = std::env::current_exe()
        .map_err(|source| ExecutableIdentityError::CurrentExecutable { source })?;
    let metadata =
        fs::metadata(executable).map_err(|source| ExecutableIdentityError::Metadata { source })?;
    if !metadata.is_file() {
        return Err(ExecutableIdentityError::UnsafeFileType);
    }
    if metadata.len() == 0 {
        return Err(ExecutableIdentityError::InvalidSize);
    }
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos().to_string());
    serde_json::to_vec(&BuildMetadata {
        schema_version: 1,
        package_version: env!("CARGO_PKG_VERSION"),
        recorded_native_build_revision: option_env!("MARKET_SQUAWK_NATIVE_BUILD_REVISION"),
        target_os: std::env::consts::OS,
        target_arch: std::env::consts::ARCH,
        executable_size: metadata.len(),
        executable_modified_unix_nanos: modified,
    })
    .map_err(ExecutableIdentityError::BuildMetadata)
}

/// Returns the signed application identity and its fixed sibling ONNX worker path.
///
/// The CLI is the signed application identity. The desktop and installed service carry that exact
/// CLI plus the worker as sibling executables, so every presentation and the shared service remain
/// bound to the one release manifest produced by the training pipeline.
pub(super) fn installed_release_programs() -> Result<(PathBuf, PathBuf), ExecutableIdentityError> {
    let executable = std::env::current_exe()
        .map_err(|source| ExecutableIdentityError::CurrentExecutable { source })?;
    let directory = executable
        .parent()
        .ok_or(ExecutableIdentityError::InvalidExecutablePath)?
        .to_path_buf();
    let executable_name = executable.file_stem().and_then(|name| name.to_str());
    let application = if matches!(
        executable_name,
        Some(DESKTOP_APPLICATION_BASENAME | SERVICE_APPLICATION_BASENAME)
    ) {
        directory.join(format!(
            "{APPLICATION_BASENAME}{}",
            std::env::consts::EXE_SUFFIX
        ))
    } else {
        executable
    };
    let worker = directory.join(format!(
        "{ONNX_WORKER_BASENAME}{}",
        std::env::consts::EXE_SUFFIX
    ));
    Ok((application, worker))
}

/// Returns the exact installed CLI path after stable-file and permission verification.
pub(super) fn installed_application_program() -> Result<PathBuf, ExecutableIdentityError> {
    let (application, _worker) = installed_release_programs()?;
    validate_installed_application_permissions(&application)?;
    let _digest = hash_stable_regular_file(&application)?;
    Ok(application)
}

/// Returns the exact installed service sibling after stable-file and permission verification.
pub(super) fn installed_service_program() -> Result<PathBuf, ExecutableIdentityError> {
    let executable = std::env::current_exe()
        .map_err(|source| ExecutableIdentityError::CurrentExecutable { source })?;
    let directory = executable
        .parent()
        .ok_or(ExecutableIdentityError::InvalidExecutablePath)?;
    let service = directory.join(format!(
        "{SERVICE_APPLICATION_BASENAME}{}",
        std::env::consts::EXE_SUFFIX
    ));
    validate_installed_application_permissions(&service)?;
    let _digest = hash_stable_regular_file(&service)?;
    Ok(service)
}

/// Admits an explicit service from the verified development-runtime cache.
#[cfg(debug_assertions)]
pub(super) fn development_service_program(
    service: &Path,
) -> Result<PathBuf, ExecutableIdentityError> {
    development_program(service, SERVICE_APPLICATION_BASENAME)
}

/// Admits an explicit MCP relay from the verified development-runtime cache.
#[cfg(debug_assertions)]
pub(super) fn development_mcp_relay_program(
    relay: &Path,
) -> Result<PathBuf, ExecutableIdentityError> {
    development_program(relay, MCP_RELAY_APPLICATION_BASENAME)
}

#[cfg(debug_assertions)]
fn development_program(
    program: &Path,
    expected_basename: &str,
) -> Result<PathBuf, ExecutableIdentityError> {
    let expected_name = format!("{expected_basename}{}", std::env::consts::EXE_SUFFIX);
    if !program.is_absolute()
        || program.file_name().and_then(|name| name.to_str()) != Some(&expected_name)
    {
        return Err(ExecutableIdentityError::InvalidExecutablePath);
    }
    validate_installed_application_permissions(program)?;
    let _digest = hash_stable_regular_file(program)?;
    fs::canonicalize(program).map_err(|source| ExecutableIdentityError::Canonicalize { source })
}

#[cfg(unix)]
fn validate_installed_application_permissions(
    application: &Path,
) -> Result<(), ExecutableIdentityError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let current = std::env::current_exe()
        .map_err(|source| ExecutableIdentityError::CurrentExecutable { source })?;
    let current =
        fs::metadata(current).map_err(|source| ExecutableIdentityError::Metadata { source })?;
    let installed = fs::symlink_metadata(application)
        .map_err(|source| ExecutableIdentityError::Metadata { source })?;
    let mode = installed.permissions().mode();
    if current.uid() != installed.uid() || mode & 0o111 == 0 || mode & 0o022 != 0 {
        return Err(ExecutableIdentityError::UnsafePermissions);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_installed_application_permissions(
    _application: &Path,
) -> Result<(), ExecutableIdentityError> {
    Ok(())
}

fn hash_stable_regular_file(path: &Path) -> Result<[u8; 32], ExecutableIdentityError> {
    let named = fs::symlink_metadata(path)
        .map_err(|source| ExecutableIdentityError::Metadata { source })?;
    if named.file_type().is_symlink() || !named.is_file() {
        return Err(ExecutableIdentityError::UnsafeFileType);
    }
    let canonical = fs::canonicalize(path)
        .map_err(|source| ExecutableIdentityError::Canonicalize { source })?;
    if !canonical.is_absolute() {
        return Err(ExecutableIdentityError::InvalidExecutablePath);
    }
    let mut file =
        File::open(&canonical).map_err(|source| ExecutableIdentityError::Open { source })?;
    let before = file
        .metadata()
        .map_err(|source| ExecutableIdentityError::Metadata { source })?;
    if !before.is_file() {
        return Err(ExecutableIdentityError::UnsafeFileType);
    }
    if before.len() == 0 {
        return Err(ExecutableIdentityError::InvalidSize);
    }
    // Packaging owns executable-size limits. Each startup pass is bounded by the opened file
    // length, with fixed-size storage and growth rejected before another chunk is read.
    let first = hash_pass(&mut file, before.len())?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| ExecutableIdentityError::Read { source })?;
    let second = hash_pass(&mut file, before.len())?;
    let after = file
        .metadata()
        .map_err(|source| ExecutableIdentityError::Metadata { source })?;
    if first.digest != second.digest
        || first.bytes != before.len()
        || second.bytes != before.len()
        || after.len() != before.len()
        || after.modified().ok() != before.modified().ok()
    {
        return Err(ExecutableIdentityError::Changed);
    }
    Ok(first.digest)
}

fn hash_pass(file: &mut File, expected_bytes: u64) -> Result<HashPass, ExecutableIdentityError> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; HASH_CHUNK_BYTES];
    let mut bytes = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| ExecutableIdentityError::Read { source })?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(u64::try_from(read).map_err(|_| ExecutableIdentityError::InvalidSize)?)
            .ok_or(ExecutableIdentityError::InvalidSize)?;
        if bytes > expected_bytes {
            return Err(ExecutableIdentityError::Changed);
        }
        hasher.update(&buffer[..read]);
    }
    Ok(HashPass {
        digest: hasher.finalize().into(),
        bytes,
    })
}

struct HashPass {
    digest: [u8; 32],
    bytes: u64,
}

/// Startup executable or helper identity could not be established exactly.
#[derive(Debug, Error)]
pub enum ExecutableIdentityError {
    /// Observed build metadata could not be encoded.
    #[error("program build metadata could not be encoded")]
    BuildMetadata(#[source] serde_json::Error),
    /// The operating system did not report the running executable.
    #[error("current executable identity is unavailable")]
    CurrentExecutable {
        /// Path-redacted operating-system error.
        #[source]
        source: io::Error,
    },
    /// The executable did not have a usable absolute parent or canonical path.
    #[error("executable path identity is invalid")]
    InvalidExecutablePath,
    /// A named executable or helper was a symlink or non-regular file.
    #[error("executable identity names an unsafe file type")]
    UnsafeFileType,
    /// The installed application was not executable, owner-matched, and protected from writes.
    #[error("installed application permissions are unsafe")]
    UnsafePermissions,
    /// Executable metadata could not be read.
    #[error("executable metadata is unavailable")]
    Metadata {
        /// Path-redacted operating-system error.
        #[source]
        source: io::Error,
    },
    /// The executable could not be canonicalized.
    #[error("executable canonical identity is unavailable")]
    Canonicalize {
        /// Path-redacted operating-system error.
        #[source]
        source: io::Error,
    },
    /// The executable could not be opened.
    #[error("executable could not be opened")]
    Open {
        /// Path-redacted operating-system error.
        #[source]
        source: io::Error,
    },
    /// The executable is empty or its byte count cannot be represented.
    #[error("executable is empty or its byte count is invalid")]
    InvalidSize,
    /// A bounded executable read failed.
    #[error("executable identity read failed")]
    Read {
        /// Path-redacted operating-system error.
        #[source]
        source: io::Error,
    },
    /// Two passes or retained metadata disagreed.
    #[error("executable changed while its identity was established")]
    Changed,
    /// The installed helper differs from its signed release-manifest identity.
    #[error("executable differs from its signed release identity")]
    SignedDigestMismatch,
    /// The ONNX worker rejected the exact sibling executable.
    #[error("ONNX worker admission failed: {0}")]
    OnnxWorker(#[from] OnnxWorkerProgramError),
}

impl std::fmt::Debug for HashPass {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HashPass")
            .field("digest", &"[SHA-256]")
            .field("bytes", &self.bytes)
            .finish()
    }
}
