//! Installed-service ownership of durable job persistence and bounded execution.

use std::{sync::Arc, time::Duration};

use market_squawk_domain::Timestamp;
use market_squawk_jobs::{
    JobAuthority, JobAuthorityError, JobRepositoryConfig, JobRepositoryError, JobRunner,
    JobShutdownOutcome, SchedulerError, SchedulerLimits, SqliteJobRepository,
};
use market_squawk_platform::LocalPaths;
use thiserror::Error;

const JOB_DATABASE_BUSY_TIMEOUT: Duration = Duration::from_millis(750);
const JOB_WRITER_QUEUE_CAPACITY: usize = 256;
const MAXIMUM_QUEUED_JOBS: usize = 256;
const MAXIMUM_RUNNING_JOBS: usize = 8;
const MAXIMUM_QUEUED_PER_KIND: usize = 64;
const MAXIMUM_RUNNING_PER_KIND: usize = 2;

/// Sole installed-process owner for durable job state and runner scheduling.
#[derive(Debug)]
pub struct InstalledJobAuthority {
    repository: Arc<SqliteJobRepository>,
    authority: Arc<JobAuthority<SqliteJobRepository>>,
}

impl InstalledJobAuthority {
    /// Opens the capability-confined job database, registers code-owned runners, and recovers
    /// every durable nonterminal generation before service publication.
    pub async fn open(
        paths: &LocalPaths,
        runners: Vec<Arc<dyn JobRunner>>,
        at: Timestamp,
    ) -> Result<Self, InstalledJobError> {
        let config =
            JobRepositoryConfig::try_new(JOB_DATABASE_BUSY_TIMEOUT, JOB_WRITER_QUEUE_CAPACITY)?;
        let limits = SchedulerLimits::try_new(
            MAXIMUM_QUEUED_JOBS,
            MAXIMUM_RUNNING_JOBS,
            MAXIMUM_QUEUED_PER_KIND,
            MAXIMUM_RUNNING_PER_KIND,
        )?;
        let repository = Arc::new(
            SqliteJobRepository::open(paths.control_root()?.job_database_location(), config)
                .await?,
        );
        let authority = match JobAuthority::try_new(Arc::clone(&repository), limits, runners) {
            Ok(authority) => Arc::new(authority),
            Err(error) => {
                return match repository.shutdown().await {
                    Ok(()) => Err(error.into()),
                    Err(repository_cleanup) => Err(InstalledJobError::StartupCleanup {
                        cause: error,
                        authority_cleanup: None,
                        repository_cleanup: Some(repository_cleanup),
                    }),
                };
            }
        };
        if let Err(startup) = authority.recover(at).await {
            let authority_cleanup = authority.shutdown(at, Duration::from_secs(15)).await.err();
            let repository_cleanup = repository.shutdown().await.err();
            if authority_cleanup.is_none() && repository_cleanup.is_none() {
                return Err(startup.into());
            }
            return Err(InstalledJobError::StartupCleanup {
                cause: startup,
                authority_cleanup,
                repository_cleanup,
            });
        }
        Ok(Self {
            repository,
            authority,
        })
    }

    /// Durable read authority shared with job resources and product clients.
    #[must_use]
    pub fn repository(&self) -> Arc<SqliteJobRepository> {
        Arc::clone(&self.repository)
    }

    /// Mutable job lifecycle authority shared by registered application job services.
    #[must_use]
    pub fn authority(&self) -> Arc<JobAuthority<SqliteJobRepository>> {
        Arc::clone(&self.authority)
    }

    /// Stops job admission and owned runners while leaving durable storage available.
    ///
    /// The installed service calls this before shutting down the domain authorities that runners
    /// may still need. It must call [`Self::shutdown_repository`] only after those authorities and
    /// their audit sinks have finished.
    pub async fn shutdown_authority(
        &self,
        at: Timestamp,
        runner_deadline: Duration,
    ) -> Result<JobShutdownOutcome, InstalledJobError> {
        self.authority
            .shutdown(at, runner_deadline)
            .await
            .map_err(Into::into)
    }

    /// Drains the sole SQLite writer after job runners and dependent domains have stopped.
    pub async fn shutdown_repository(&self) -> Result<(), InstalledJobError> {
        self.repository.shutdown().await.map_err(Into::into)
    }
}

/// Installed job composition, recovery, or shutdown failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InstalledJobError {
    /// The local path capability required for the job database is unavailable.
    #[error("installed job storage path is unavailable")]
    Path,
    /// Durable job storage is unavailable or invalid.
    #[error(transparent)]
    Repository(#[from] JobRepositoryError),
    /// Scheduler limits are invalid.
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
    /// Job scheduling, recovery, or shutdown failed.
    #[error(transparent)]
    Authority(#[from] JobAuthorityError),
    /// Startup failed and one or more owned cleanup steps also failed.
    #[error(
        "installed job startup failed ({cause}); authority cleanup: {authority_cleanup:?}; \
         repository cleanup: {repository_cleanup:?}"
    )]
    StartupCleanup {
        /// Original authority construction or recovery failure.
        cause: JobAuthorityError,
        /// Runner-authority cleanup failure, when an authority had been constructed.
        authority_cleanup: Option<JobAuthorityError>,
        /// Durable repository cleanup failure.
        repository_cleanup: Option<JobRepositoryError>,
    },
}

impl From<market_squawk_platform::PathError> for InstalledJobError {
    fn from(_error: market_squawk_platform::PathError) -> Self {
        Self::Path
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};

    use market_squawk_domain::Timestamp;
    use market_squawk_jobs::{JobRepository as _, RecoveryPageLimit};
    use market_squawk_platform::LocalPaths;

    use super::InstalledJobAuthority;

    type TestError = Box<dyn Error + Send + Sync>;

    #[tokio::test]
    async fn runner_authority_stops_before_durable_storage() -> Result<(), TestError> {
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path().join("market-squawk"))?;
        let at = Timestamp::from_unix_nanos(1);
        let jobs = InstalledJobAuthority::open(&paths, Vec::new(), at).await?;
        let repository = jobs.repository();

        jobs.shutdown_authority(at, Duration::from_secs(1)).await?;
        let page = repository
            .recover_nonterminal(None, RecoveryPageLimit::try_new(1)?)
            .await?;
        assert!(page.snapshots().is_empty());

        jobs.shutdown_repository().await?;
        Ok(())
    }
}
