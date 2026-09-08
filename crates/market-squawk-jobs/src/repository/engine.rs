use market_squawk_domain::Timestamp;
use market_squawk_platform::{JobDatabaseFileGuard, JobDatabaseLocation, JobDatabaseWriterGuard};
use rusqlite::{Connection, OpenFlags, OptionalExtension as _, Transaction, params};
use tokio::sync::mpsc;

use super::backup::{capture, verify_database};
use super::codec::{decode_snapshot, encode_event, encode_snapshot, state_code};
use super::{JOB_DATABASE_APPLICATION_ID, JobRepositoryConfig, SCHEMA_VERSION, WriteCommand};
use crate::{
    AdmittedJobSpec, JobEvent, JobEventSequence, JobFailure, JobGeneration, JobId,
    JobRepositoryError, JobSnapshot, JobState, validate_transition,
};

pub(super) fn initialize(
    location: &JobDatabaseLocation,
    config: JobRepositoryConfig,
) -> Result<(), JobRepositoryError> {
    let connection = open_writer(location, config)?;
    initialize_schema(&connection)?;
    verify_database(&connection)
}

pub(super) fn initialize_schema(connection: &Connection) -> Result<(), JobRepositoryError> {
    let application_id = connection
        .query_row("PRAGMA application_id", [], |row| row.get::<_, i64>(0))
        .map_err(map_sql)?;
    let version = connection
        .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
        .map_err(map_sql)?;
    let objects = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(map_sql)?;
    if application_id == 0 && version == 0 && objects == 0 {
        connection
            .execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE jobs (
                    job_id BLOB NOT NULL,
                    generation INTEGER NOT NULL CHECK (generation > 0),
                    sequence INTEGER NOT NULL CHECK (sequence >= 0),
                    state INTEGER NOT NULL CHECK (state BETWEEN 0 AND 9),
                    snapshot_json BLOB NOT NULL,
                    PRIMARY KEY (job_id, generation)
                 ) WITHOUT ROWID;
                 CREATE TABLE job_events (
                    job_id BLOB NOT NULL,
                    generation INTEGER NOT NULL,
                    sequence INTEGER NOT NULL CHECK (sequence > 0),
                    event_json BLOB NOT NULL,
                    PRIMARY KEY (job_id, generation, sequence),
                    FOREIGN KEY (job_id, generation) REFERENCES jobs(job_id, generation)
                 ) WITHOUT ROWID;
                 PRAGMA application_id = 1297305930;
                 PRAGMA user_version = 1;
                 COMMIT;",
            )
            .map_err(map_sql)?;
    } else if application_id != JOB_DATABASE_APPLICATION_ID || version != SCHEMA_VERSION {
        return Err(JobRepositoryError::InvalidState);
    }
    Ok(())
}

pub(super) fn open_writer(
    location: &JobDatabaseLocation,
    config: JobRepositoryConfig,
) -> Result<Connection, JobRepositoryError> {
    let file = location.open_database_file().map_err(map_path)?;
    location.validate_for_open().map_err(map_path)?;
    file.validate_identity().map_err(map_path)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let connection = Connection::open_with_flags(location.path(), flags).map_err(map_sql)?;
    location.validate_for_open().map_err(map_path)?;
    file.validate_identity().map_err(map_path)?;
    connection
        .busy_timeout(config.busy_timeout)
        .map_err(map_sql)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(map_sql)?;
    let mode: String = connection
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
        .map_err(map_sql)?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(JobRepositoryError::Unavailable);
    }
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(map_sql)?;
    location.validate_sqlite_sidecars().map_err(map_path)?;
    Ok(connection)
}

pub(super) fn open_reader(
    location: &JobDatabaseLocation,
    config: JobRepositoryConfig,
) -> Result<Connection, JobRepositoryError> {
    let file = location.open_database_file().map_err(map_path)?;
    location.validate_for_open().map_err(map_path)?;
    file.validate_identity().map_err(map_path)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let connection = Connection::open_with_flags(location.path(), flags).map_err(map_sql)?;
    location.validate_for_open().map_err(map_path)?;
    file.validate_identity().map_err(map_path)?;
    connection
        .busy_timeout(config.busy_timeout)
        .map_err(map_sql)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(map_sql)?;
    location.validate_sqlite_sidecars().map_err(map_path)?;
    Ok(connection)
}

pub(super) fn writer_loop(
    location: JobDatabaseLocation,
    config: JobRepositoryConfig,
    mut receiver: mpsc::Receiver<WriteCommand>,
    database_file: JobDatabaseFileGuard,
    writer_guard: JobDatabaseWriterGuard,
) {
    let connection = open_writer(&location, config);
    let mut shutdown_reply = None;
    while let Some(command) = receiver.blocking_recv() {
        match command {
            WriteCommand::Create { spec, reply } => {
                let result = connection
                    .as_ref()
                    .map_err(|error| *error)
                    .and_then(|connection| create_snapshot(connection, &spec));
                let _ignored = reply.send(result);
            }
            WriteCommand::Append {
                id,
                generation,
                expected,
                event,
                reply,
            } => {
                let result = connection
                    .as_ref()
                    .map_err(|error| *error)
                    .and_then(|connection| {
                        append_event(connection, id, generation, expected, event)
                    });
                let _ignored = reply.send(result);
            }
            WriteCommand::Recover {
                orphaned,
                at,
                reply,
            } => {
                let result = connection
                    .as_ref()
                    .map_err(|error| *error)
                    .and_then(|connection| begin_recovery(connection, &orphaned, at));
                let _ignored = reply.send(result);
            }
            WriteCommand::Retry { failed, at, reply } => {
                let result = connection
                    .as_ref()
                    .map_err(|error| *error)
                    .and_then(|connection| begin_retry(connection, &failed, at));
                let _ignored = reply.send(result);
            }
            WriteCommand::Snapshot {
                binding,
                backup_id,
                backup_generation,
                backup_kind,
                reply,
                release,
            } => {
                let result = connection
                    .as_ref()
                    .map_err(|error| *error)
                    .and_then(|connection| {
                        capture(
                            connection,
                            binding,
                            backup_id,
                            backup_generation,
                            &backup_kind,
                        )
                    });
                let retained = result.is_ok();
                let _ignored = reply.send(result);
                if retained {
                    let _ignored = release.recv();
                }
            }
            WriteCommand::Shutdown { reply } => {
                receiver.close();
                shutdown_reply = Some(reply);
                break;
            }
        }
    }
    drop(connection);
    drop(database_file);
    drop(writer_guard);
    if let Some(reply) = shutdown_reply {
        let _ignored = reply.send(());
    }
}

fn create_snapshot(
    connection: &Connection,
    spec: &AdmittedJobSpec,
) -> Result<JobSnapshot, JobRepositoryError> {
    let snapshot = JobSnapshot::try_new(
        spec.clone(),
        JobEventSequence::new(0),
        JobState::Queued,
        None,
        None,
        None,
        None,
        spec.admitted_at(),
        false,
    )
    .map_err(|_| JobRepositoryError::InvalidState)?;
    let changed = connection
        .execute(
            "INSERT OR IGNORE INTO jobs
             (job_id, generation, sequence, state, snapshot_json) VALUES (?1, ?2, 0, 0, ?3)",
            params![
                spec.id().as_uuid().as_bytes().as_slice(),
                sql_u64(spec.generation().get())?,
                encode_snapshot(&snapshot)?
            ],
        )
        .map_err(map_sql)?;
    if changed == 1 {
        Ok(snapshot)
    } else {
        Err(JobRepositoryError::Conflict)
    }
}

fn append_event(
    connection: &Connection,
    id: JobId,
    generation: JobGeneration,
    expected: JobEventSequence,
    event: JobEvent,
) -> Result<JobSnapshot, JobRepositoryError> {
    let transaction = connection.unchecked_transaction().map_err(map_sql)?;
    let snapshot = read_snapshot_transaction(&transaction, id, generation)?;
    if snapshot.sequence() != expected {
        return Err(JobRepositoryError::Conflict);
    }
    validate_transition(&snapshot, &event)?;
    let sequence = expected
        .checked_next()
        .map_err(|_| JobRepositoryError::InvalidState)?;
    let next = apply_event(snapshot, sequence, &event)?;
    update_snapshot(&transaction, expected, &next)?;
    transaction
        .execute(
            "INSERT INTO job_events (job_id, generation, sequence, event_json)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                id.as_uuid().as_bytes().as_slice(),
                sql_u64(generation.get())?,
                sql_u64(sequence.get())?,
                encode_event(&event)?
            ],
        )
        .map_err(map_sql)?;
    transaction.commit().map_err(map_sql)?;
    Ok(next)
}

fn begin_recovery(
    connection: &Connection,
    orphaned: &JobSnapshot,
    at: Timestamp,
) -> Result<JobSnapshot, JobRepositoryError> {
    let transaction = connection.unchecked_transaction().map_err(map_sql)?;
    let current = read_snapshot_transaction(&transaction, orphaned.id(), orphaned.generation())?;
    if &current != orphaned {
        return Err(JobRepositoryError::Conflict);
    }
    let interruption = JobEvent::try_new(JobState::Interrupted, at, None, None, None)
        .map_err(|_| JobRepositoryError::InvalidState)?;
    validate_transition(&current, &interruption)?;
    let interrupted_sequence = current
        .sequence()
        .checked_next()
        .map_err(|_| JobRepositoryError::InvalidState)?;
    let interrupted = apply_event(current, interrupted_sequence, &interruption)?;
    update_snapshot(&transaction, orphaned.sequence(), &interrupted)?;
    transaction
        .execute(
            "INSERT INTO job_events (job_id, generation, sequence, event_json)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                orphaned.id().as_uuid().as_bytes().as_slice(),
                sql_u64(orphaned.generation().get())?,
                sql_u64(interrupted_sequence.get())?,
                encode_event(&interruption)?
            ],
        )
        .map_err(map_sql)?;

    let spec = orphaned
        .spec()
        .next_generation(at)
        .map_err(|_| JobRepositoryError::Terminal)?;
    let recovering = JobSnapshot::try_new(
        spec.clone(),
        JobEventSequence::new(0),
        JobState::Recovering,
        None,
        None,
        None,
        None,
        at,
        orphaned.cancellation_requested(),
    )
    .map_err(|_| JobRepositoryError::InvalidState)?;
    transaction
        .execute(
            "INSERT INTO jobs (job_id, generation, sequence, state, snapshot_json)
             VALUES (?1, ?2, 0, 9, ?3)",
            params![
                spec.id().as_uuid().as_bytes().as_slice(),
                sql_u64(spec.generation().get())?,
                encode_snapshot(&recovering)?
            ],
        )
        .map_err(map_sql)?;
    transaction.commit().map_err(map_sql)?;
    Ok(recovering)
}

fn begin_retry(
    connection: &Connection,
    failed: &JobSnapshot,
    at: Timestamp,
) -> Result<JobSnapshot, JobRepositoryError> {
    let transaction = connection.unchecked_transaction().map_err(map_sql)?;
    let current = read_snapshot_transaction(&transaction, failed.id(), failed.generation())?;
    if &current != failed {
        return Err(JobRepositoryError::Conflict);
    }
    if current.state() != JobState::Failed
        || !current
            .terminal_failure()
            .is_some_and(JobFailure::retryable)
        || at < current.updated_at()
    {
        return Err(JobRepositoryError::Terminal);
    }
    let spec = current
        .spec()
        .next_generation(at)
        .map_err(|_| JobRepositoryError::Terminal)?;
    let retrying = JobSnapshot::try_new(
        spec.clone(),
        JobEventSequence::new(0),
        JobState::Recovering,
        None,
        None,
        None,
        None,
        at,
        false,
    )
    .map_err(|_| JobRepositoryError::InvalidState)?;
    transaction
        .execute(
            "INSERT INTO jobs (job_id, generation, sequence, state, snapshot_json)
             VALUES (?1, ?2, 0, 9, ?3)",
            params![
                spec.id().as_uuid().as_bytes().as_slice(),
                sql_u64(spec.generation().get())?,
                encode_snapshot(&retrying)?
            ],
        )
        .map_err(map_sql)?;
    transaction.commit().map_err(map_sql)?;
    Ok(retrying)
}

fn update_snapshot(
    transaction: &Transaction<'_>,
    expected: JobEventSequence,
    snapshot: &JobSnapshot,
) -> Result<(), JobRepositoryError> {
    let changed = transaction
        .execute(
            "UPDATE jobs SET sequence = ?1, state = ?2, snapshot_json = ?3
             WHERE job_id = ?4 AND generation = ?5 AND sequence = ?6",
            params![
                sql_u64(snapshot.sequence().get())?,
                state_code(snapshot.state()),
                encode_snapshot(snapshot)?,
                snapshot.id().as_uuid().as_bytes().as_slice(),
                sql_u64(snapshot.generation().get())?,
                sql_u64(expected.get())?
            ],
        )
        .map_err(map_sql)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(JobRepositoryError::Conflict)
    }
}

pub(super) fn apply_event(
    snapshot: JobSnapshot,
    sequence: JobEventSequence,
    event: &JobEvent,
) -> Result<JobSnapshot, JobRepositoryError> {
    let cancellation_requested =
        snapshot.cancellation_requested() || event.state() == JobState::Cancelling;
    let progress = event
        .progress()
        .cloned()
        .or_else(|| snapshot.progress().cloned());
    JobSnapshot::try_new(
        snapshot.spec().clone(),
        sequence,
        event.state(),
        progress,
        event.confirmation().cloned(),
        event.result().cloned(),
        event.failure().cloned(),
        event.occurred_at(),
        cancellation_requested,
    )
    .map_err(|_| JobRepositoryError::InvalidState)
}

pub(super) fn read_snapshot(
    connection: &Connection,
    id: JobId,
    generation: JobGeneration,
) -> Result<JobSnapshot, JobRepositoryError> {
    let bytes = connection
        .query_row(
            "SELECT snapshot_json FROM jobs WHERE job_id = ?1 AND generation = ?2",
            params![
                id.as_uuid().as_bytes().as_slice(),
                sql_u64(generation.get())?
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(map_sql)?
        .ok_or(JobRepositoryError::NotFound)?;
    decode_snapshot(&bytes)
}

fn read_snapshot_transaction(
    transaction: &Transaction<'_>,
    id: JobId,
    generation: JobGeneration,
) -> Result<JobSnapshot, JobRepositoryError> {
    let bytes = transaction
        .query_row(
            "SELECT snapshot_json FROM jobs WHERE job_id = ?1 AND generation = ?2",
            params![
                id.as_uuid().as_bytes().as_slice(),
                sql_u64(generation.get())?
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(map_sql)?
        .ok_or(JobRepositoryError::NotFound)?;
    decode_snapshot(&bytes)
}

pub(super) fn decode_cursor(cursor: Option<&str>) -> Result<Vec<u8>, JobRepositoryError> {
    let Some(cursor) = cursor else {
        return Ok(Vec::new());
    };
    let id = JobId::try_from_str(cursor).map_err(|_| JobRepositoryError::InvalidState)?;
    Ok(id.as_uuid().as_bytes().to_vec())
}

pub(super) fn map_sql(_error: rusqlite::Error) -> JobRepositoryError {
    JobRepositoryError::Unavailable
}

pub(super) fn map_path(_error: market_squawk_platform::PathError) -> JobRepositoryError {
    JobRepositoryError::Unavailable
}

pub(super) fn sql_u64(value: u64) -> Result<i64, JobRepositoryError> {
    i64::try_from(value).map_err(|_| JobRepositoryError::InvalidState)
}

pub(super) fn sql_usize(value: usize) -> Result<i64, JobRepositoryError> {
    i64::try_from(value).map_err(|_| JobRepositoryError::InvalidState)
}
