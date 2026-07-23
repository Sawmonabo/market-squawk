//! Exact executable identity and sibling-helper admission at the process boundary.

use std::fs::{self, File};
use std::io::{self, Read as _, Seek as _, SeekFrom};
use std::path::Path;

use market_squawk_modeling::{OnnxWorkerProgram, OnnxWorkerProgramError};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const HASH_CHUNK_BYTES: usize = 64 * 1024;
const MAXIMUM_APPLICATION_BYTES: u64 = 768 * 1024 * 1024;
const MAXIMUM_ONNX_WORKER_BYTES: u64 = 256 * 1024 * 1024;
const ONNX_WORKER_BASENAME: &str = "market-squawk-onnx-worker";

/// Returns the two-pass SHA-256 identity of the exact executable opened at startup.
pub(super) fn current_executable_sha256() -> Result<[u8; 32], ExecutableIdentityError> {
    let executable = std::env::current_exe()
        .map_err(|source| ExecutableIdentityError::CurrentExecutable { source })?;
    hash_stable_regular_file(&executable, MAXIMUM_APPLICATION_BYTES)
}

/// Admits the exact sibling ONNX worker when it is installed beside the application.
///
/// Absence is represented explicitly because a native-only or empty model runtime does not need
/// a worker. Persisted ONNX generations still fail closed later if this capability is absent.
pub(super) fn admit_installed_onnx_worker()
-> Result<Option<OnnxWorkerProgram>, ExecutableIdentityError> {
    let executable = std::env::current_exe()
        .map_err(|source| ExecutableIdentityError::CurrentExecutable { source })?;
    let directory = executable
        .parent()
        .ok_or(ExecutableIdentityError::InvalidExecutablePath)?;
    let candidate = directory.join(format!(
        "{ONNX_WORKER_BASENAME}{}",
        std::env::consts::EXE_SUFFIX
    ));
    match fs::symlink_metadata(&candidate) {
        Ok(_) => {}
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(ExecutableIdentityError::Metadata { source }),
    }
    let digest = hash_stable_regular_file(&candidate, MAXIMUM_ONNX_WORKER_BYTES)?;
    OnnxWorkerProgram::admit(candidate, digest)
        .map(Some)
        .map_err(Into::into)
}

fn hash_stable_regular_file(
    path: &Path,
    maximum_bytes: u64,
) -> Result<[u8; 32], ExecutableIdentityError> {
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
    if !before.is_file() || before.len() == 0 || before.len() > maximum_bytes {
        return Err(ExecutableIdentityError::InvalidSize);
    }
    let first = hash_pass(&mut file, maximum_bytes)?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| ExecutableIdentityError::Read { source })?;
    let second = hash_pass(&mut file, maximum_bytes)?;
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

fn hash_pass(file: &mut File, maximum_bytes: u64) -> Result<HashPass, ExecutableIdentityError> {
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
        if bytes > maximum_bytes {
            return Err(ExecutableIdentityError::InvalidSize);
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
    /// The executable is empty or exceeds its fixed startup ceiling.
    #[error("executable size is outside the admitted bound")]
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
