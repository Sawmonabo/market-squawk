//! Bounded cross-process evidence for installed-service startup diagnostics.

use std::fmt;
use std::fs::{self, File};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use atomicwrites::{AllowOverwrite, AtomicFile};
use market_squawk_platform::{LocalPaths, PathError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const EVIDENCE_DIRECTORY: &str = "installed-service-startup";
const EVIDENCE_FILE: &str = "state.json";
const EVIDENCE_SCHEMA_VERSION: u32 = 1;
const MAXIMUM_EVIDENCE_BYTES: u64 = 4 * 1024;

/// One closed application-owned phase in installed-service startup or shutdown.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ServiceStartupPhase {
    /// The service process parsed its fixed installed invocation.
    ProcessStarted,
    /// Local application configuration was loaded successfully.
    ConfigurationLoaded,
    /// Bounded structured logging was installed successfully.
    LoggingReady,
    /// The sole application runtime was being composed.
    RuntimeComposition,
    /// The authenticated local service was ready to serve.
    Serving,
    /// The service was completing a requested shutdown.
    Shutdown,
}

/// Closed startup state; no path, credential, provider response, or arbitrary error text is stored.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", tag = "state")]
pub enum ServiceStartupState {
    /// The service reached the named phase and is continuing.
    Starting { phase: ServiceStartupPhase },
    /// The authenticated local service reached its serving boundary.
    Ready,
    /// The service failed while completing the named phase.
    Failed { phase: ServiceStartupPhase },
    /// The service completed a controlled stop.
    Stopped,
}

/// Validated installed-service startup evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ServiceStartupEvidence {
    schema_version: u32,
    state: ServiceStartupState,
}

impl ServiceStartupEvidence {
    /// Returns the closed startup state.
    #[must_use]
    pub const fn state(self) -> ServiceStartupState {
        self.state
    }
}

/// Single-process publisher for one installed service's latest startup state.
pub struct ServiceStartupEvidenceWriter {
    file: PathBuf,
}

impl fmt::Debug for ServiceStartupEvidenceWriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceStartupEvidenceWriter")
            .field("file", &"[installation startup evidence]")
            .finish()
    }
}

impl ServiceStartupEvidenceWriter {
    /// Opens the fixed startup-evidence location beneath one absolute installation data root.
    ///
    /// # Errors
    ///
    /// Fails when the installation layout or fixed evidence directory is unsafe.
    pub fn try_open(
        installation_data_root: impl AsRef<Path>,
    ) -> Result<Self, ServiceStartupEvidenceError> {
        if !installation_data_root.as_ref().is_absolute() {
            return Err(ServiceStartupEvidenceError::UnsafeLocation);
        }
        let paths = LocalPaths::prepare(installation_data_root)?;
        let directory = paths.control_root()?.root().join(EVIDENCE_DIRECTORY);
        match fs::create_dir(&directory) {
            Ok(()) => set_private_directory_permissions(&directory)?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(source) => {
                return Err(ServiceStartupEvidenceError::io(
                    "create startup-evidence directory",
                    source,
                ));
            }
        }
        validate_directory(&directory, paths.control_root()?.root())?;
        Ok(Self {
            file: directory.join(EVIDENCE_FILE),
        })
    }

    /// Atomically publishes one closed startup state.
    ///
    /// # Errors
    ///
    /// Fails when the fixed evidence file is unsafe or cannot be written and synchronized.
    pub fn publish(&self, state: ServiceStartupState) -> Result<(), ServiceStartupEvidenceError> {
        validate_existing_file(&self.file)?;
        let bytes = serde_json::to_vec(&ServiceStartupEvidence {
            schema_version: EVIDENCE_SCHEMA_VERSION,
            state,
        })
        .map_err(|_| ServiceStartupEvidenceError::InvalidEvidence)?;
        if bytes.len() as u64 > MAXIMUM_EVIDENCE_BYTES {
            return Err(ServiceStartupEvidenceError::InvalidEvidence);
        }
        AtomicFile::new(&self.file, AllowOverwrite)
            .write(|file| {
                set_private_file_permissions(file)?;
                file.write_all(&bytes)?;
                file.sync_all()
            })
            .map_err(|error| {
                let source: std::io::Error = error.into();
                ServiceStartupEvidenceError::io("publish startup evidence", source)
            })?;
        validate_existing_file(&self.file)
    }

    /// Removes only the fixed regular evidence file before a new native activation attempt.
    ///
    /// # Errors
    ///
    /// Fails closed for an unsafe file type or filesystem error.
    pub fn clear(&self) -> Result<(), ServiceStartupEvidenceError> {
        validate_existing_file(&self.file)?;
        match fs::remove_file(&self.file) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(ServiceStartupEvidenceError::io(
                "clear startup evidence",
                source,
            )),
        }
    }
}

/// Reads one validated startup-evidence record without creating product state.
///
/// # Errors
///
/// Fails when the installation layout, fixed file type, bound, schema, or state is invalid.
pub fn read_service_startup_evidence(
    installation_data_root: impl AsRef<Path>,
) -> Result<Option<ServiceStartupEvidence>, ServiceStartupEvidenceError> {
    if !installation_data_root.as_ref().is_absolute() {
        return Err(ServiceStartupEvidenceError::UnsafeLocation);
    }
    let paths = LocalPaths::open_existing(installation_data_root)?;
    let directory = paths.control_root()?.root().join(EVIDENCE_DIRECTORY);
    match fs::symlink_metadata(&directory) {
        Ok(_) => validate_directory(&directory, paths.control_root()?.root())?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(ServiceStartupEvidenceError::io(
                "inspect startup-evidence directory",
                source,
            ));
        }
    }
    let file_path = directory.join(EVIDENCE_FILE);
    validate_existing_file(&file_path)?;
    let metadata = match fs::symlink_metadata(&file_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(ServiceStartupEvidenceError::io(
                "inspect startup evidence",
                source,
            ));
        }
    };
    if metadata.len() > MAXIMUM_EVIDENCE_BYTES {
        return Err(ServiceStartupEvidenceError::InvalidEvidence);
    }
    let file = File::open(&file_path)
        .map_err(|source| ServiceStartupEvidenceError::io("open startup evidence", source))?;
    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len())
            .map_err(|_| ServiceStartupEvidenceError::InvalidEvidence)?,
    );
    file.take(MAXIMUM_EVIDENCE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| ServiceStartupEvidenceError::io("read startup evidence", source))?;
    if bytes.len() as u64 > MAXIMUM_EVIDENCE_BYTES {
        return Err(ServiceStartupEvidenceError::InvalidEvidence);
    }
    let evidence: ServiceStartupEvidence =
        serde_json::from_slice(&bytes).map_err(|_| ServiceStartupEvidenceError::InvalidEvidence)?;
    if evidence.schema_version != EVIDENCE_SCHEMA_VERSION {
        return Err(ServiceStartupEvidenceError::InvalidEvidence);
    }
    Ok(Some(evidence))
}

fn validate_directory(path: &Path, control_root: &Path) -> Result<(), ServiceStartupEvidenceError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| {
        ServiceStartupEvidenceError::io("inspect startup-evidence directory", source)
    })?;
    let resolved = fs::canonicalize(path).map_err(|source| {
        ServiceStartupEvidenceError::io("resolve startup-evidence directory", source)
    })?;
    if !metadata.is_dir()
        || is_redirect(&metadata)
        || resolved.parent() != Some(control_root)
        || resolved.file_name().and_then(|name| name.to_str()) != Some(EVIDENCE_DIRECTORY)
    {
        return Err(ServiceStartupEvidenceError::UnsafeLocation);
    }
    validate_private_directory_permissions(&metadata)
}

fn validate_existing_file(path: &Path) -> Result<(), ServiceStartupEvidenceError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !is_redirect(&metadata) => {
            validate_private_file_permissions(&metadata)
        }
        Ok(_) => Err(ServiceStartupEvidenceError::UnsafeLocation),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ServiceStartupEvidenceError::io(
            "inspect startup-evidence file",
            source,
        )),
    }
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<(), ServiceStartupEvidenceError> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|source| {
        ServiceStartupEvidenceError::io("secure startup-evidence directory", source)
    })
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<(), ServiceStartupEvidenceError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(file: &File) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    file.set_permissions(fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_file_permissions(_file: &File) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn validate_private_directory_permissions(
    metadata: &fs::Metadata,
) -> Result<(), ServiceStartupEvidenceError> {
    use std::os::unix::fs::PermissionsExt as _;

    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(ServiceStartupEvidenceError::UnsafeLocation);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_directory_permissions(
    _metadata: &fs::Metadata,
) -> Result<(), ServiceStartupEvidenceError> {
    Ok(())
}

#[cfg(unix)]
fn validate_private_file_permissions(
    metadata: &fs::Metadata,
) -> Result<(), ServiceStartupEvidenceError> {
    use std::os::unix::fs::PermissionsExt as _;

    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(ServiceStartupEvidenceError::UnsafeLocation);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_file_permissions(
    _metadata: &fs::Metadata,
) -> Result<(), ServiceStartupEvidenceError> {
    Ok(())
}

#[cfg(unix)]
fn is_redirect(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(windows)]
fn is_redirect(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

/// Startup-evidence publication or validation failure.
#[derive(Debug, Error)]
pub enum ServiceStartupEvidenceError {
    /// The installation root or fixed evidence entry is unsafe.
    #[error("installed-service startup-evidence location is unsafe")]
    UnsafeLocation,
    /// The bounded schema or state is invalid.
    #[error("installed-service startup evidence is invalid")]
    InvalidEvidence,
    /// The controlled local layout is unavailable.
    #[error(transparent)]
    Path(#[from] PathError),
    /// A fixed filesystem operation failed.
    #[error("installed-service startup-evidence I/O failed while {operation}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
}

impl ServiceStartupEvidenceError {
    fn io(operation: &'static str, source: std::io::Error) -> Self {
        Self::Io { operation, source }
    }
}
