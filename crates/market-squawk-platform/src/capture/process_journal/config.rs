//! Validated helper launch and journal-destination configuration.

use std::path::{Path, PathBuf};
use std::time::Duration;

use thiserror::Error;

use crate::{JournalSelectionError, LocalPaths, PathError};

use super::super::writer::CaptureDestination;

const HELPER_BINARY_NAME: &str = "market-squawk-capture-helper";

/// Bounded configuration for one process-isolated journal capture writer.
#[derive(Clone, Debug)]
pub struct ProcessJournalCaptureConfig {
    root: PathBuf,
    source: String,
    destination: CaptureDestination,
    executable: PathBuf,
    startup_deadline: Duration,
    #[cfg(all(feature = "capture-test", debug_assertions))]
    test_behavior: Option<ProcessCaptureHelperTestBehavior>,
}

impl ProcessJournalCaptureConfig {
    /// Validates a prepared root, source, and same-installation helper binary.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the root or source is invalid, the startup deadline is zero, or
    /// the helper is not the exact validated sibling of the running executable.
    pub fn try_new(
        root: impl AsRef<Path>,
        source: impl Into<String>,
        startup_deadline: Duration,
    ) -> Result<Self, ProcessJournalCaptureConfigError> {
        let executable = validated_sibling_helper()?;
        Self::try_new_inner(root, source.into(), executable, startup_deadline, None)
    }

    #[cfg(all(feature = "capture-test", debug_assertions))]
    #[doc(hidden)]
    pub fn try_new_for_test(
        root: impl AsRef<Path>,
        source: impl Into<String>,
        executable: impl AsRef<Path>,
        behavior: ProcessCaptureHelperTestBehavior,
        startup_deadline: Duration,
    ) -> Result<Self, ProcessJournalCaptureConfigError> {
        let executable = validate_executable(executable.as_ref(), false)?;
        Self::try_new_inner(
            root,
            source.into(),
            executable,
            startup_deadline,
            Some(behavior),
        )
    }

    fn try_new_inner(
        root: impl AsRef<Path>,
        source: String,
        executable: PathBuf,
        startup_deadline: Duration,
        #[cfg(all(feature = "capture-test", debug_assertions))] test_behavior: Option<
            ProcessCaptureHelperTestBehavior,
        >,
        #[cfg(not(all(feature = "capture-test", debug_assertions)))] _test_behavior: Option<()>,
    ) -> Result<Self, ProcessJournalCaptureConfigError> {
        if startup_deadline.is_zero() {
            return Err(ProcessJournalCaptureConfigError::ZeroStartupDeadline);
        }
        let paths = LocalPaths::prepare(root)?;
        let destination_path = paths.journal_write_file(&source)?;
        Ok(Self {
            root: paths.root().to_path_buf(),
            source,
            destination: CaptureDestination::for_journal(&destination_path),
            executable,
            startup_deadline,
            #[cfg(all(feature = "capture-test", debug_assertions))]
            test_behavior,
        })
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    pub(super) fn source(&self) -> &str {
        &self.source
    }

    pub(super) fn destination(&self) -> &CaptureDestination {
        &self.destination
    }

    pub(super) fn executable(&self) -> &Path {
        &self.executable
    }

    pub(super) const fn startup_deadline(&self) -> Duration {
        self.startup_deadline
    }

    pub(super) fn post_handshake_cleanup_deadline(&self) -> Duration {
        #[cfg(all(feature = "capture-test", debug_assertions))]
        if let Some(ProcessCaptureHelperTestBehavior::DelayShutdownAfterPostHandshakeFailure {
            cleanup_deadline,
        }) = self.test_behavior
        {
            return cleanup_deadline;
        }
        #[cfg(all(feature = "capture-test", debug_assertions))]
        if let Some(ProcessCaptureHelperTestBehavior::FailAfterDestinationFence {
            cleanup_deadline,
            ..
        }) = self.test_behavior
        {
            return cleanup_deadline;
        }
        self.startup_deadline
    }

    #[cfg(all(feature = "capture-test", debug_assertions))]
    pub(super) const fn test_behavior(&self) -> Option<ProcessCaptureHelperTestBehavior> {
        self.test_behavior
    }

    #[cfg(all(feature = "capture-test", debug_assertions))]
    pub(super) fn inject_post_handshake_failure(&self) -> bool {
        matches!(
            self.test_behavior,
            Some(ProcessCaptureHelperTestBehavior::DelayShutdownAfterPostHandshakeFailure { .. })
        )
    }

    #[cfg(all(feature = "capture-test", debug_assertions))]
    pub(super) fn inject_post_fence_failure(&self) -> bool {
        matches!(
            self.test_behavior,
            Some(ProcessCaptureHelperTestBehavior::FailAfterDestinationFence { .. })
        )
    }

    pub(super) fn reap_observation_delay(&self) -> Duration {
        #[cfg(all(feature = "capture-test", debug_assertions))]
        if let Some(ProcessCaptureHelperTestBehavior::FailAfterDestinationFence {
            reap_observation_delay,
            ..
        }) = self.test_behavior
        {
            return reap_observation_delay;
        }
        Duration::ZERO
    }
}

#[cfg(all(feature = "capture-test", debug_assertions))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum ProcessCaptureHelperTestBehavior {
    StallAfterAppend,
    DelayShutdownAfterPostHandshakeFailure {
        cleanup_deadline: Duration,
    },
    FailAfterDestinationFence {
        cleanup_deadline: Duration,
        reap_observation_delay: Duration,
    },
}

fn validated_sibling_helper() -> Result<PathBuf, ProcessJournalCaptureConfigError> {
    let current =
        std::env::current_exe().map_err(ProcessJournalCaptureConfigError::CurrentExecutable)?;
    let current = std::fs::canonicalize(current)
        .map_err(ProcessJournalCaptureConfigError::CurrentExecutable)?;
    let parent = current
        .parent()
        .ok_or(ProcessJournalCaptureConfigError::HelperNotSibling)?;
    let expected_name = format!("{HELPER_BINARY_NAME}{}", std::env::consts::EXE_SUFFIX);
    let expected = parent.join(expected_name);
    match validate_executable(&expected, true) {
        Ok(executable) => Ok(executable),
        #[cfg(debug_assertions)]
        Err(_error)
            if parent
                .file_name()
                .is_some_and(|name| name == std::ffi::OsStr::new("deps")) =>
        {
            let harness_parent = parent
                .parent()
                .ok_or(ProcessJournalCaptureConfigError::HelperNotSibling)?;
            validate_executable(
                &harness_parent.join(format!(
                    "{HELPER_BINARY_NAME}{}",
                    std::env::consts::EXE_SUFFIX
                )),
                true,
            )
        }
        Err(error) => Err(error),
    }
}

fn validate_executable(
    candidate: &Path,
    require_exact_path: bool,
) -> Result<PathBuf, ProcessJournalCaptureConfigError> {
    let link_metadata = std::fs::symlink_metadata(candidate)
        .map_err(ProcessJournalCaptureConfigError::HelperMetadata)?;
    if link_metadata.file_type().is_symlink() {
        return Err(ProcessJournalCaptureConfigError::HelperSymlink);
    }
    let canonical = std::fs::canonicalize(candidate)
        .map_err(ProcessJournalCaptureConfigError::HelperMetadata)?;
    if require_exact_path && canonical != candidate {
        return Err(ProcessJournalCaptureConfigError::HelperNotSibling);
    }
    let metadata =
        std::fs::metadata(&canonical).map_err(ProcessJournalCaptureConfigError::HelperMetadata)?;
    if !metadata.is_file() {
        return Err(ProcessJournalCaptureConfigError::HelperNotFile);
    }
    validate_executable_permissions(&metadata)?;
    Ok(canonical)
}

#[cfg(unix)]
fn validate_executable_permissions(
    metadata: &std::fs::Metadata,
) -> Result<(), ProcessJournalCaptureConfigError> {
    use std::os::unix::fs::MetadataExt as _;

    if metadata.mode() & 0o111 == 0 {
        return Err(ProcessJournalCaptureConfigError::HelperNotExecutable);
    }
    if metadata.mode() & 0o022 != 0 {
        return Err(ProcessJournalCaptureConfigError::HelperUnsafePermissions);
    }
    let current = std::env::current_exe()
        .and_then(std::fs::metadata)
        .map_err(ProcessJournalCaptureConfigError::CurrentExecutable)?;
    if current.uid() != metadata.uid() {
        return Err(ProcessJournalCaptureConfigError::HelperOwnerMismatch);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_executable_permissions(
    _metadata: &std::fs::Metadata,
) -> Result<(), ProcessJournalCaptureConfigError> {
    Ok(())
}

/// Invalid process-isolated capture configuration.
#[derive(Debug, Error)]
pub enum ProcessJournalCaptureConfigError {
    #[error("capture helper startup deadline must be nonzero")]
    ZeroStartupDeadline,
    #[error("failed to resolve the running executable")]
    CurrentExecutable(#[source] std::io::Error),
    #[error("capture helper is not the exact sibling of the running executable")]
    HelperNotSibling,
    #[error("failed to inspect the capture helper")]
    HelperMetadata(#[source] std::io::Error),
    #[error("capture helper path must not be a symbolic link")]
    HelperSymlink,
    #[error("capture helper path does not name a regular file")]
    HelperNotFile,
    #[error("capture helper file is not executable")]
    HelperNotExecutable,
    #[error("capture helper file is writable by its group or other users")]
    HelperUnsafePermissions,
    #[error("capture helper owner does not match the running executable owner")]
    HelperOwnerMismatch,
    #[error(transparent)]
    Paths(#[from] PathError),
    #[error(transparent)]
    JournalSelection(#[from] JournalSelectionError),
}
