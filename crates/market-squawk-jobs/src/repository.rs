use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_platform::JobDatabaseLocation;
use rusqlite::{Connection, params};
use tokio::sync::{mpsc, oneshot};
use tokio_util::task::TaskTracker;

mod codec;
mod engine;

use codec::{decode_event, decode_snapshot};
use engine::{
    decode_cursor, initialize, map_path, map_sql, open_reader, read_snapshot, sql_u64, sql_usize,
    writer_loop,
};

use crate::{
    AdmittedJobSpec, JobEvent, JobEventPage, JobEventPageLimit, JobEventSequence, JobGeneration,
    JobId, JobListCursor, JobListPage, JobListPageLimit, JobRecoveryPage, JobRepository,
    JobRepositoryError, JobSnapshot, JobState, RecoveryCursor, RecoveryPageLimit,
};

const SCHEMA_VERSION: i64 = 1;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounded SQLite writer configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobRepositoryConfig {
    busy_timeout: Duration,
    writer_queue_capacity: usize,
}

impl JobRepositoryConfig {
    /// Admits a positive busy timeout no longer than five seconds and a bounded writer queue.
    pub fn try_new(
        busy_timeout: Duration,
        writer_queue_capacity: usize,
    ) -> Result<Self, JobRepositoryError> {
        if busy_timeout.is_zero()
            || busy_timeout > Duration::from_secs(5)
            || writer_queue_capacity == 0
            || writer_queue_capacity > 4_096
        {
            return Err(JobRepositoryError::InvalidState);
        }
        Ok(Self {
            busy_timeout,
            writer_queue_capacity,
        })
    }
}

/// SQLite-backed durable job repository with one bounded writer task.
#[derive(Clone, Debug)]
pub struct SqliteJobRepository {
    inner: Arc<RepositoryInner>,
}

#[derive(Debug)]
struct RepositoryInner {
    location: JobDatabaseLocation,
    config: JobRepositoryConfig,
    writer: mpsc::Sender<WriteCommand>,
    tracker: TaskTracker,
    closing: AtomicBool,
}

#[derive(Debug)]
enum WriteCommand {
    Create {
        spec: AdmittedJobSpec,
        reply: oneshot::Sender<Result<JobSnapshot, JobRepositoryError>>,
    },
    Append {
        id: JobId,
        generation: JobGeneration,
        expected: JobEventSequence,
        event: JobEvent,
        reply: oneshot::Sender<Result<JobSnapshot, JobRepositoryError>>,
    },
    Recover {
        orphaned: Box<JobSnapshot>,
        at: Timestamp,
        reply: oneshot::Sender<Result<JobSnapshot, JobRepositoryError>>,
    },
    Retry {
        failed: Box<JobSnapshot>,
        at: Timestamp,
        reply: oneshot::Sender<Result<JobSnapshot, JobRepositoryError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

impl SqliteJobRepository {
    /// Opens or creates the owned database, verifies WAL/foreign-key mode, then starts one writer.
    pub async fn open(
        location: JobDatabaseLocation,
        config: JobRepositoryConfig,
    ) -> Result<Self, JobRepositoryError> {
        let database_file = location.prepare_database_file().map_err(map_path)?;
        let writer_guard = location.acquire_writer().map_err(map_path)?;
        let initialize_location = location.clone();
        tokio::task::spawn_blocking(move || initialize(&initialize_location, config))
            .await
            .map_err(|_| JobRepositoryError::Unavailable)??;
        database_file.validate_identity().map_err(map_path)?;
        location.validate_sqlite_sidecars().map_err(map_path)?;

        let (writer, receiver) = mpsc::channel(config.writer_queue_capacity);
        let tracker = TaskTracker::new();
        let writer_location = location.clone();
        tracker.spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                writer_loop(
                    writer_location,
                    config,
                    receiver,
                    database_file,
                    writer_guard,
                );
            })
            .await;
            if result.is_err() {
                // A closed writer channel makes every later mutation fail closed.
            }
        });
        Ok(Self {
            inner: Arc::new(RepositoryInner {
                location,
                config,
                writer,
                tracker,
                closing: AtomicBool::new(false),
            }),
        })
    }

    /// Stops admission, drains the one writer, and releases its cross-process lease only afterward.
    pub async fn shutdown(&self) -> Result<(), JobRepositoryError> {
        if !self.inner.closing.swap(true, Ordering::AcqRel) {
            let (reply, receiver) = oneshot::channel();
            self.inner
                .writer
                .send(WriteCommand::Shutdown { reply })
                .await
                .map_err(|_| JobRepositoryError::Unavailable)?;
            tokio::time::timeout(SHUTDOWN_TIMEOUT, receiver)
                .await
                .map_err(|_| JobRepositoryError::Unavailable)?
                .map_err(|_| JobRepositoryError::Unavailable)?;
        }
        self.inner.tracker.close();
        tokio::time::timeout(SHUTDOWN_TIMEOUT, self.inner.tracker.wait())
            .await
            .map_err(|_| JobRepositoryError::Unavailable)?;
        Ok(())
    }

    async fn send(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<JobSnapshot, JobRepositoryError>>) -> WriteCommand,
    ) -> Result<JobSnapshot, JobRepositoryError> {
        if self.inner.closing.load(Ordering::Acquire) {
            return Err(JobRepositoryError::Unavailable);
        }
        let (reply, receiver) = oneshot::channel();
        self.inner
            .writer
            .send(command(reply))
            .await
            .map_err(|_| JobRepositoryError::Unavailable)?;
        receiver
            .await
            .map_err(|_| JobRepositoryError::Unavailable)?
    }

    async fn read<T, F>(&self, operation: F) -> Result<T, JobRepositoryError>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, JobRepositoryError> + Send + 'static,
    {
        let location = self.inner.location.clone();
        let config = self.inner.config;
        tokio::task::spawn_blocking(move || {
            let connection = open_reader(&location, config)?;
            operation(&connection)
        })
        .await
        .map_err(|_| JobRepositoryError::Unavailable)?
    }
}

impl Drop for RepositoryInner {
    fn drop(&mut self) {
        self.tracker.close();
    }
}

#[async_trait]
impl JobRepository for SqliteJobRepository {
    async fn create(&self, spec: &AdmittedJobSpec) -> Result<JobSnapshot, JobRepositoryError> {
        let spec = spec.clone();
        self.send(|reply| WriteCommand::Create { spec, reply })
            .await
    }

    async fn append(
        &self,
        id: JobId,
        generation: JobGeneration,
        expected: JobEventSequence,
        event: JobEvent,
    ) -> Result<JobSnapshot, JobRepositoryError> {
        self.send(|reply| WriteCommand::Append {
            id,
            generation,
            expected,
            event,
            reply,
        })
        .await
    }

    async fn request_cancellation(
        &self,
        id: JobId,
        generation: JobGeneration,
        expected: JobEventSequence,
        at: Timestamp,
    ) -> Result<JobSnapshot, JobRepositoryError> {
        let event = JobEvent::try_new(JobState::Cancelling, at, None, None, None)
            .map_err(|_| JobRepositoryError::InvalidState)?;
        self.append(id, generation, expected, event).await
    }

    async fn begin_recovery(
        &self,
        orphaned: &JobSnapshot,
        at: Timestamp,
    ) -> Result<JobSnapshot, JobRepositoryError> {
        let orphaned = Box::new(orphaned.clone());
        self.send(|reply| WriteCommand::Recover {
            orphaned,
            at,
            reply,
        })
        .await
    }

    async fn begin_retry(
        &self,
        failed: &JobSnapshot,
        at: Timestamp,
    ) -> Result<JobSnapshot, JobRepositoryError> {
        let failed = Box::new(failed.clone());
        self.send(|reply| WriteCommand::Retry { failed, at, reply })
            .await
    }

    async fn get(
        &self,
        id: JobId,
        generation: JobGeneration,
    ) -> Result<JobSnapshot, JobRepositoryError> {
        self.read(move |connection| read_snapshot(connection, id, generation))
            .await
    }

    async fn list(
        &self,
        cursor: Option<&JobListCursor>,
        limit: JobListPageLimit,
    ) -> Result<JobListPage, JobRepositoryError> {
        let cursor = cursor
            .map(|value| value.as_source_identifier().as_str().to_owned())
            .map(|value| JobId::try_from_str(&value).map_err(|_| JobRepositoryError::InvalidState))
            .transpose()?;
        self.read(move |connection| {
            let cursor = cursor.map_or_else(Vec::new, |id| id.as_uuid().as_bytes().to_vec());
            let fetch = limit.get().saturating_add(1);
            let mut statement = connection
                .prepare(
                    "SELECT current.snapshot_json FROM jobs AS current
                     WHERE current.job_id > ?1
                       AND current.generation = (
                         SELECT MAX(candidate.generation) FROM jobs AS candidate
                         WHERE candidate.job_id = current.job_id
                       )
                     ORDER BY current.job_id LIMIT ?2",
                )
                .map_err(map_sql)?;
            let rows = statement
                .query_map(params![cursor, sql_usize(fetch)?], |row| {
                    row.get::<_, Vec<u8>>(0)
                })
                .map_err(map_sql)?;
            let mut snapshots = Vec::with_capacity(fetch);
            for row in rows {
                snapshots.push(decode_snapshot(&row.map_err(map_sql)?)?);
            }
            let next = if snapshots.len() > limit.get() {
                Some(JobListCursor::new(
                    SourceIdentifier::try_from(
                        snapshots[limit.get() - 1].id().as_uuid().to_string(),
                    )
                    .map_err(|_| JobRepositoryError::InvalidState)?,
                ))
            } else {
                None
            };
            snapshots.truncate(limit.get());
            JobListPage::try_new(snapshots, next, limit)
                .map_err(|_| JobRepositoryError::InvalidState)
        })
        .await
    }

    async fn events_after(
        &self,
        id: JobId,
        generation: JobGeneration,
        after: JobEventSequence,
        limit: JobEventPageLimit,
    ) -> Result<JobEventPage, JobRepositoryError> {
        self.read(move |connection| {
            let mut statement = connection
                .prepare(
                    "SELECT sequence, event_json FROM job_events
                     WHERE job_id = ?1 AND generation = ?2 AND sequence > ?3
                     ORDER BY sequence LIMIT ?4",
                )
                .map_err(map_sql)?;
            let fetch = limit.get().saturating_add(1);
            let generation_sql = sql_u64(generation.get())?;
            let after_sql = sql_u64(after.get())?;
            let fetch_sql = sql_usize(fetch)?;
            let rows = statement
                .query_map(
                    params![
                        id.as_uuid().as_bytes().as_slice(),
                        generation_sql,
                        after_sql,
                        fetch_sql
                    ],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
                )
                .map_err(map_sql)?;
            let mut events = Vec::with_capacity(fetch);
            for row in rows {
                let (sequence, bytes) = row.map_err(map_sql)?;
                let sequence =
                    u64::try_from(sequence).map_err(|_| JobRepositoryError::InvalidState)?;
                events.push((JobEventSequence::new(sequence), decode_event(&bytes)?));
            }
            let next = (events.len() > limit.get()).then(|| events[limit.get() - 1].0);
            events.truncate(limit.get());
            JobEventPage::try_new(events, next, limit).map_err(|_| JobRepositoryError::InvalidState)
        })
        .await
    }

    async fn recover_nonterminal(
        &self,
        cursor: Option<&RecoveryCursor>,
        limit: RecoveryPageLimit,
    ) -> Result<JobRecoveryPage, JobRepositoryError> {
        let cursor = cursor.map(|value| value.as_source_identifier().as_str().to_owned());
        self.read(move |connection| {
            let cursor_id = decode_cursor(cursor.as_deref())?;
            let mut statement = connection
                .prepare(
                    "SELECT current.snapshot_json FROM jobs AS current
                     WHERE current.state IN (0, 1, 2, 3, 4, 9)
                       AND current.job_id > ?1
                       AND current.generation = (
                         SELECT MAX(candidate.generation) FROM jobs AS candidate
                         WHERE candidate.job_id = current.job_id
                       )
                     ORDER BY current.job_id LIMIT ?2",
                )
                .map_err(map_sql)?;
            let fetch = limit.get().saturating_add(1);
            let fetch_sql = sql_usize(fetch)?;
            let rows = statement
                .query_map(params![cursor_id, fetch_sql], |row| {
                    row.get::<_, Vec<u8>>(0)
                })
                .map_err(map_sql)?;
            let mut snapshots = Vec::with_capacity(fetch);
            for row in rows {
                snapshots.push(decode_snapshot(&row.map_err(map_sql)?)?);
            }
            let next = if snapshots.len() > limit.get() {
                let last = &snapshots[limit.get() - 1];
                Some(RecoveryCursor::new(
                    SourceIdentifier::try_from(last.id().as_uuid().to_string())
                        .map_err(|_| JobRepositoryError::InvalidState)?,
                ))
            } else {
                None
            };
            snapshots.truncate(limit.get());
            JobRecoveryPage::try_new(snapshots, next, limit)
                .map_err(|_| JobRepositoryError::InvalidState)
        })
        .await
    }
}
