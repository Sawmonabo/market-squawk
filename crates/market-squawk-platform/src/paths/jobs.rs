//! Prepared placement for the durable background-job SQLite database.

use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cap_std::fs::Dir;

use super::sqlite::{
    PreparedSqliteLocation, acquire_writer_file, open_private_file,
    validate_for_open as validate_sqlite_root, validate_optional_sqlite_sidecar,
    validate_private_file_identity,
};
use super::{ControlRoot, PathError};

const DATABASE_FILE: &str = "jobs.sqlite3";
const DATABASE_WAL_FILE: &str = "jobs.sqlite3-wal";
const DATABASE_SHM_FILE: &str = "jobs.sqlite3-shm";
const WRITER_LOCK_FILE: &str = ".jobs.writer.lock";

/// Retained handle proving the fixed job database names one private, unique-link file.
pub struct JobDatabaseFileGuard {
    file: File,
    location: JobDatabaseLocation,
}

impl JobDatabaseFileGuard {
    /// Clones the retained exact file capability for the SQLite connection boundary.
    pub fn try_clone_file(&self) -> Result<File, PathError> {
        self.file
            .try_clone()
            .map_err(|source| PathError::io("failed to clone opened job database", source))
    }

    /// Revalidates the retained handle against the fixed capability-relative database name.
    pub fn validate_identity(&self) -> Result<(), PathError> {
        validate_private_file_identity(&self.location, DATABASE_FILE, &self.file)
    }
}

impl fmt::Debug for JobDatabaseFileGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JobDatabaseFileGuard([PRIVATE FILE CAPABILITY])")
    }
}

/// Lifetime guard for the private, capability-relative job-database writer lease.
pub struct JobDatabaseWriterGuard {
    file: File,
}

impl fmt::Debug for JobDatabaseWriterGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JobDatabaseWriterGuard([LOCKED CAPABILITY])")
    }
}

impl Drop for JobDatabaseWriterGuard {
    fn drop(&mut self) {
        let _ignored = self.file.unlock();
    }
}

/// Prepared capability for the fixed local background-job SQLite database.
#[derive(Clone)]
pub struct JobDatabaseLocation {
    path: PathBuf,
    root: PathBuf,
    root_capability: Arc<Dir>,
}

impl JobDatabaseLocation {
    fn from_control_root(root: PathBuf, root_capability: Arc<Dir>) -> Self {
        Self {
            path: root.join(DATABASE_FILE),
            root,
            root_capability,
        }
    }

    /// Returns the canonical display path; the retained directory remains filesystem authority.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Opens or creates the fixed database without following links.
    pub fn prepare_database_file(&self) -> Result<JobDatabaseFileGuard, PathError> {
        let file = open_private_file(
            self,
            DATABASE_FILE,
            true,
            true,
            "failed to open private job database",
        )?;
        Ok(JobDatabaseFileGuard {
            file,
            location: self.clone(),
        })
    }

    /// Opens the existing fixed database without following links.
    pub fn open_database_file(&self) -> Result<JobDatabaseFileGuard, PathError> {
        let file = open_private_file(
            self,
            DATABASE_FILE,
            false,
            false,
            "failed to open existing job database",
        )?;
        Ok(JobDatabaseFileGuard {
            file,
            location: self.clone(),
        })
    }

    /// Proves the display path still names the retained control-directory capability.
    ///
    /// SQLite callers must invoke this immediately before and after every path-based VFS open.
    pub fn validate_for_open(&self) -> Result<(), PathError> {
        validate_sqlite_root(self)
    }

    /// Acquires the exclusive no-follow cross-process writer lease.
    pub fn acquire_writer(&self) -> Result<JobDatabaseWriterGuard, PathError> {
        let file = acquire_writer_file(
            self,
            WRITER_LOCK_FILE,
            true,
            || PathError::JobDatabaseAlreadyLocked,
            "failed to open job database writer lock",
            "failed to acquire job database writer lock",
        )?;
        Ok(JobDatabaseWriterGuard { file })
    }

    /// Validates the optional SQLite WAL and shared-memory files as private unique-link files.
    pub fn validate_sqlite_sidecars(&self) -> Result<(), PathError> {
        self.validate_for_open()?;
        validate_optional_sqlite_sidecar(self, DATABASE_WAL_FILE, false)?;
        validate_optional_sqlite_sidecar(self, DATABASE_SHM_FILE, false)?;
        self.validate_for_open()
    }
}

impl PreparedSqliteLocation for JobDatabaseLocation {
    fn display_root(&self) -> &Path {
        &self.root
    }

    fn directory(&self) -> &Arc<Dir> {
        &self.root_capability
    }
}

impl fmt::Debug for JobDatabaseLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JobDatabaseLocation([PREPARED LOCAL CAPABILITY])")
    }
}

impl ControlRoot {
    /// Returns the fixed job-database capability under the retained control directory.
    pub fn job_database_location(&self) -> JobDatabaseLocation {
        JobDatabaseLocation::from_control_root(
            self.display_root.clone(),
            Arc::clone(&self.directory),
        )
    }
}
