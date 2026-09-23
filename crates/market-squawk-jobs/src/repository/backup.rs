use std::path::{Path, PathBuf};

use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_platform::JobDatabaseLocation;
use rusqlite::{Connection, OptionalExtension as _, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::codec::{
    StoredEvent, StoredSnapshot, decode_event, decode_snapshot, encode_event, encode_snapshot,
};
use super::engine::{apply_event, map_path, map_sql, open_writer, sql_u64};
use super::{
    JOB_DATABASE_APPLICATION_ID, JobRepositoryConfig, SCHEMA_VERSION, SqliteJobRepository,
};
use crate::{
    JobEvent, JobEventSequence, JobGeneration, JobId, JobRepositoryError, JobSnapshot, JobState,
    validate_transition,
};

/// Stable component schema identity for the protected logical jobs-and-receipts envelope.
pub const JOBS_AND_RECEIPTS_BACKUP_SCHEMA: &str = "market-squawk-jobs-and-receipts-v1";
const MAXIMUM_GENERATIONS: usize = 1_000_000;
const MAXIMUM_EVENTS: usize = 8_000_000;
const MAXIMUM_ENCODED_BYTES: usize = 1024 * 1024 * 1024;

/// Exact common product-snapshot identity bound into the logical jobs export.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct JobsAndReceiptsBackupBinding {
    cutoff: Timestamp,
    snapshot_id: [u8; 32],
}

impl JobsAndReceiptsBackupBinding {
    /// Admits a nonzero product snapshot identity at one exact cutoff.
    pub fn try_new(cutoff: Timestamp, snapshot_id: [u8; 32]) -> Result<Self, JobRepositoryError> {
        if snapshot_id == [0; 32] {
            return Err(JobRepositoryError::InvalidState);
        }
        Ok(Self {
            cutoff,
            snapshot_id,
        })
    }

    /// Exact common cutoff shared by all product component owners.
    #[must_use]
    pub const fn cutoff(self) -> Timestamp {
        self.cutoff
    }

    /// Exact common product snapshot identity.
    #[must_use]
    pub const fn snapshot_id(self) -> [u8; 32] {
        self.snapshot_id
    }
}

/// Owner-issued evidence for one canonical logical jobs export.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobsAndReceiptsBackupReceipt {
    binding: JobsAndReceiptsBackupBinding,
    authority_revision_sha256: [u8; 32],
    byte_length: u64,
    sha256: [u8; 32],
}

impl JobsAndReceiptsBackupReceipt {
    /// Exact product snapshot bound into this export.
    #[must_use]
    pub const fn binding(self) -> JobsAndReceiptsBackupBinding {
        self.binding
    }

    /// Digest of the canonical owner payload, excluding its protecting envelope field.
    #[must_use]
    pub const fn authority_revision_sha256(self) -> [u8; 32] {
        self.authority_revision_sha256
    }

    /// Exact canonical envelope byte length.
    #[must_use]
    pub const fn byte_length(self) -> u64 {
        self.byte_length
    }

    /// SHA-256 of the complete canonical envelope bytes.
    #[must_use]
    pub const fn sha256(self) -> [u8; 32] {
        self.sha256
    }
}

/// Canonical protected JobsAndReceipts component produced under the repository writer fence.
#[derive(Debug)]
pub struct JobsAndReceiptsBackupExport {
    encoded: Vec<u8>,
    receipt: JobsAndReceiptsBackupReceipt,
}

impl JobsAndReceiptsBackupExport {
    /// Owner evidence to retain across component materialization and revalidation.
    #[must_use]
    pub const fn receipt(&self) -> JobsAndReceiptsBackupReceipt {
        self.receipt
    }

    /// Borrows the exact canonical bytes for bounded component writing.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }

    /// Transfers the exact canonical bytes to the controlled bundle writer.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.encoded
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Protection {
    Protected,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Envelope {
    schema: String,
    protection: Protection,
    payload: Payload,
    digest: [u8; 32],
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct UnsignedEnvelope<'a> {
    schema: &'a str,
    protection: Protection,
    payload: &'a Payload,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Payload {
    binding: JobsAndReceiptsBackupBinding,
    database: DatabaseIdentity,
    limits: ExportLimits,
    counts: ExportCounts,
    bounds: ExportBounds,
    snapshots: Vec<SnapshotRecord>,
    events: Vec<EventRecord>,
    latest_generations: Vec<GenerationIdentity>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DatabaseIdentity {
    application_id: i64,
    user_version: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ExportLimits {
    maximum_generations: usize,
    maximum_events: usize,
    maximum_encoded_bytes: usize,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ExportCounts {
    jobs: usize,
    generations: usize,
    events: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ExportBounds {
    first_generation: Option<GenerationIdentity>,
    last_generation: Option<GenerationIdentity>,
    first_event: Option<EventIdentity>,
    last_event: Option<EventIdentity>,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct GenerationIdentity {
    job_id: Uuid,
    generation: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct EventIdentity {
    job_id: Uuid,
    generation: u64,
    sequence: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SnapshotRecord {
    identity: GenerationIdentity,
    snapshot: StoredSnapshot,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct EventRecord {
    identity: EventIdentity,
    event: StoredEvent,
}

pub(super) fn capture(
    connection: &Connection,
    binding: JobsAndReceiptsBackupBinding,
    backup_id: JobId,
    backup_generation: JobGeneration,
    backup_kind: &SourceIdentifier,
) -> Result<JobsAndReceiptsBackupExport, JobRepositoryError> {
    verify_database(connection)?;
    let transaction = connection.unchecked_transaction().map_err(map_sql)?;
    let mut snapshots = Vec::new();
    let mut statement = transaction
        .prepare("SELECT job_id, generation, snapshot_json FROM jobs ORDER BY job_id, generation")
        .map_err(map_sql)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })
        .map_err(map_sql)?;
    for row in rows {
        if snapshots.len() >= MAXIMUM_GENERATIONS {
            return Err(JobRepositoryError::InvalidState);
        }
        let (id_bytes, generation, encoded) = row.map_err(map_sql)?;
        let id = uuid_from_bytes(&id_bytes)?;
        let generation = u64::try_from(generation).map_err(|_| JobRepositoryError::InvalidState)?;
        let snapshot = decode_snapshot(&encoded)?;
        if snapshot.id().as_uuid() != id || snapshot.generation().get() != generation {
            return Err(JobRepositoryError::InvalidState);
        }
        snapshots.push(SnapshotRecord {
            identity: GenerationIdentity {
                job_id: id,
                generation,
            },
            snapshot: StoredSnapshot::from(&snapshot),
        });
    }
    drop(statement);

    let mut events = Vec::new();
    let mut statement = transaction
        .prepare(
            "SELECT job_id, generation, sequence, event_json FROM job_events \
             ORDER BY job_id, generation, sequence",
        )
        .map_err(map_sql)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        })
        .map_err(map_sql)?;
    for row in rows {
        if events.len() >= MAXIMUM_EVENTS {
            return Err(JobRepositoryError::InvalidState);
        }
        let (id_bytes, generation, sequence, encoded) = row.map_err(map_sql)?;
        events.push(EventRecord {
            identity: EventIdentity {
                job_id: uuid_from_bytes(&id_bytes)?,
                generation: u64::try_from(generation)
                    .map_err(|_| JobRepositoryError::InvalidState)?,
                sequence: u64::try_from(sequence).map_err(|_| JobRepositoryError::InvalidState)?,
            },
            event: StoredEvent::from(&decode_event(&encoded)?),
        });
    }
    drop(statement);
    transaction.commit().map_err(map_sql)?;

    let latest_generations = latest_generations(&snapshots)?;
    let counts = counts(&snapshots, &events, &latest_generations)?;
    let bounds = bounds(&snapshots, &events);
    let payload = Payload {
        binding,
        database: DatabaseIdentity {
            application_id: JOB_DATABASE_APPLICATION_ID,
            user_version: SCHEMA_VERSION,
        },
        limits: expected_limits(),
        counts,
        bounds,
        snapshots,
        events,
        latest_generations,
    };
    validate_payload(&payload, Some((backup_id, backup_generation, backup_kind)))?;
    encode_envelope(payload)
}

fn encode_envelope(payload: Payload) -> Result<JobsAndReceiptsBackupExport, JobRepositoryError> {
    let digest = payload_digest(&payload)?;
    let envelope = Envelope {
        schema: JOBS_AND_RECEIPTS_BACKUP_SCHEMA.to_owned(),
        protection: Protection::Protected,
        payload,
        digest,
    };
    let encoded = serde_json::to_vec(&envelope).map_err(|_| JobRepositoryError::InvalidState)?;
    if encoded.is_empty() || encoded.len() > MAXIMUM_ENCODED_BYTES {
        return Err(JobRepositoryError::InvalidState);
    }
    let byte_length = u64::try_from(encoded.len()).map_err(|_| JobRepositoryError::InvalidState)?;
    let receipt = JobsAndReceiptsBackupReceipt {
        binding: envelope.payload.binding,
        authority_revision_sha256: digest,
        byte_length,
        sha256: Sha256::digest(&encoded).into(),
    };
    Ok(JobsAndReceiptsBackupExport { encoded, receipt })
}

fn decode_envelope(encoded: &[u8]) -> Result<Envelope, JobRepositoryError> {
    if encoded.is_empty() || encoded.len() > MAXIMUM_ENCODED_BYTES {
        return Err(JobRepositoryError::InvalidState);
    }
    let envelope: Envelope =
        serde_json::from_slice(encoded).map_err(|_| JobRepositoryError::InvalidState)?;
    if envelope.schema != JOBS_AND_RECEIPTS_BACKUP_SCHEMA
        || envelope.protection != Protection::Protected
        || envelope.digest != payload_digest(&envelope.payload)?
        || serde_json::to_vec(&envelope).map_err(|_| JobRepositoryError::InvalidState)? != encoded
    {
        return Err(JobRepositoryError::InvalidState);
    }
    validate_payload(&envelope.payload, None)?;
    Ok(envelope)
}

fn payload_digest(payload: &Payload) -> Result<[u8; 32], JobRepositoryError> {
    let canonical = serde_json::to_vec(&UnsignedEnvelope {
        schema: JOBS_AND_RECEIPTS_BACKUP_SCHEMA,
        protection: Protection::Protected,
        payload,
    })
    .map_err(|_| JobRepositoryError::InvalidState)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/jobs-and-receipts/protected-envelope/v1");
    digest.update(canonical);
    Ok(digest.finalize().into())
}

fn validate_payload(
    payload: &Payload,
    active_backup: Option<(JobId, JobGeneration, &SourceIdentifier)>,
) -> Result<(), JobRepositoryError> {
    JobsAndReceiptsBackupBinding::try_new(payload.binding.cutoff, payload.binding.snapshot_id)?;
    if payload.database
        != (DatabaseIdentity {
            application_id: JOB_DATABASE_APPLICATION_ID,
            user_version: SCHEMA_VERSION,
        })
        || payload.limits != expected_limits()
        || payload.counts
            != counts(
                &payload.snapshots,
                &payload.events,
                &payload.latest_generations,
            )?
        || payload.bounds != bounds(&payload.snapshots, &payload.events)
        || payload.latest_generations != latest_generations(&payload.snapshots)?
    {
        return Err(JobRepositoryError::InvalidState);
    }
    replay(payload, active_backup)
}

fn replay(
    payload: &Payload,
    active_backup: Option<(JobId, JobGeneration, &SourceIdentifier)>,
) -> Result<(), JobRepositoryError> {
    let mut event_index = 0_usize;
    let mut previous: Option<JobSnapshot> = None;
    let mut nonterminal = Vec::new();
    for record in &payload.snapshots {
        let stored: JobSnapshot = record.snapshot.clone().try_into()?;
        if stored.id().as_uuid() != record.identity.job_id
            || stored.generation().get() != record.identity.generation
            || stored.spec().admitted_at() > payload.binding.cutoff
            || stored.spec().authority().captured_at() > payload.binding.cutoff
            || stored.updated_at() > payload.binding.cutoff
        {
            return Err(JobRepositoryError::InvalidState);
        }
        let initial_cancellation = match previous.as_ref() {
            None if stored.generation().get() == 1 => false,
            Some(prior)
                if prior.id() == stored.id()
                    && prior.generation().get().checked_add(1)
                        == Some(stored.generation().get()) =>
            {
                let expected = prior
                    .spec()
                    .next_generation(stored.spec().admitted_at())
                    .map_err(|_| JobRepositoryError::InvalidState)?;
                if expected != *stored.spec()
                    || !matches!(prior.state(), JobState::Failed | JobState::Interrupted)
                {
                    return Err(JobRepositoryError::InvalidState);
                }
                prior.state() == JobState::Interrupted && prior.cancellation_requested()
            }
            Some(prior) if prior.id() != stored.id() && stored.generation().get() == 1 => false,
            _ => return Err(JobRepositoryError::InvalidState),
        };
        let initial_state = if stored.generation().get() == 1 {
            JobState::Queued
        } else {
            JobState::Recovering
        };
        let mut replayed = JobSnapshot::try_new(
            stored.spec().clone(),
            JobEventSequence::new(0),
            initial_state,
            None,
            None,
            None,
            None,
            stored.spec().admitted_at(),
            initial_cancellation,
        )
        .map_err(|_| JobRepositoryError::InvalidState)?;
        for sequence in 1..=stored.sequence().get() {
            let event_record = payload
                .events
                .get(event_index)
                .ok_or(JobRepositoryError::InvalidState)?;
            let expected_identity = EventIdentity {
                job_id: stored.id().as_uuid(),
                generation: stored.generation().get(),
                sequence,
            };
            if event_record.identity != expected_identity {
                return Err(JobRepositoryError::InvalidState);
            }
            let event: JobEvent = event_record.event.clone().try_into()?;
            if event.occurred_at() > payload.binding.cutoff {
                return Err(JobRepositoryError::InvalidState);
            }
            validate_transition(&replayed, &event)?;
            replayed = apply_event(replayed, JobEventSequence::new(sequence), &event)?;
            event_index = event_index
                .checked_add(1)
                .ok_or(JobRepositoryError::InvalidState)?;
        }
        if replayed != stored {
            return Err(JobRepositoryError::InvalidState);
        }
        if !stored.state().is_terminal() {
            nonterminal.push((
                stored.id(),
                stored.generation(),
                stored.spec().kind().clone(),
            ));
        }
        previous = Some(stored);
    }
    if event_index != payload.events.len() {
        return Err(JobRepositoryError::InvalidState);
    }
    if let Some((id, generation, kind)) = active_backup
        && (nonterminal.as_slice() != [(id, generation, kind.clone())].as_slice()
            || previous_nonterminal_state(&payload.snapshots, id, generation)? != JobState::Running)
    {
        return Err(JobRepositoryError::InvalidState);
    }
    Ok(())
}

fn previous_nonterminal_state(
    snapshots: &[SnapshotRecord],
    id: JobId,
    generation: JobGeneration,
) -> Result<JobState, JobRepositoryError> {
    snapshots
        .iter()
        .find(|record| {
            record.identity.job_id == id.as_uuid() && record.identity.generation == generation.get()
        })
        .ok_or(JobRepositoryError::InvalidState)
        .and_then(|record| {
            record
                .snapshot
                .clone()
                .try_into()
                .map(|snapshot: JobSnapshot| snapshot.state())
        })
}

fn expected_limits() -> ExportLimits {
    ExportLimits {
        maximum_generations: MAXIMUM_GENERATIONS,
        maximum_events: MAXIMUM_EVENTS,
        maximum_encoded_bytes: MAXIMUM_ENCODED_BYTES,
    }
}

fn counts(
    snapshots: &[SnapshotRecord],
    events: &[EventRecord],
    latest: &[GenerationIdentity],
) -> Result<ExportCounts, JobRepositoryError> {
    if snapshots.len() > MAXIMUM_GENERATIONS || events.len() > MAXIMUM_EVENTS {
        return Err(JobRepositoryError::InvalidState);
    }
    Ok(ExportCounts {
        jobs: latest.len(),
        generations: snapshots.len(),
        events: events.len(),
    })
}

fn bounds(snapshots: &[SnapshotRecord], events: &[EventRecord]) -> ExportBounds {
    ExportBounds {
        first_generation: snapshots.first().map(|record| record.identity.clone()),
        last_generation: snapshots.last().map(|record| record.identity.clone()),
        first_event: events.first().map(|record| record.identity.clone()),
        last_event: events.last().map(|record| record.identity.clone()),
    }
}

fn latest_generations(
    snapshots: &[SnapshotRecord],
) -> Result<Vec<GenerationIdentity>, JobRepositoryError> {
    let mut latest = Vec::new();
    for record in snapshots {
        if latest
            .last()
            .is_some_and(|current: &GenerationIdentity| current.job_id > record.identity.job_id)
            || latest.last().is_some_and(|current| {
                current.job_id == record.identity.job_id
                    && current.generation >= record.identity.generation
            })
        {
            return Err(JobRepositoryError::InvalidState);
        }
        if latest
            .last()
            .is_some_and(|current| current.job_id == record.identity.job_id)
        {
            *latest.last_mut().ok_or(JobRepositoryError::InvalidState)? = record.identity.clone();
        } else {
            latest.push(record.identity.clone());
        }
    }
    Ok(latest)
}

pub(super) fn verify_database(connection: &Connection) -> Result<(), JobRepositoryError> {
    let application_id = connection
        .query_row("PRAGMA application_id", [], |row| row.get::<_, i64>(0))
        .map_err(map_sql)?;
    let user_version = connection
        .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
        .map_err(map_sql)?;
    let objects = connection
        .query_row(
            "SELECT group_concat(type || ':' || name, ',') FROM (\
             SELECT type, name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name)",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .map_err(map_sql)?;
    let integrity = connection
        .query_row("PRAGMA integrity_check(1)", [], |row| {
            row.get::<_, String>(0)
        })
        .map_err(map_sql)?;
    let foreign_key_violation = connection
        .query_row(
            "SELECT rowid FROM pragma_foreign_key_check LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(map_sql)?;
    if application_id != JOB_DATABASE_APPLICATION_ID
        || user_version != SCHEMA_VERSION
        || objects.as_deref() != Some("table:job_events,table:jobs")
        || integrity != "ok"
        || foreign_key_violation.is_some()
    {
        return Err(JobRepositoryError::InvalidState);
    }
    Ok(())
}

impl SqliteJobRepository {
    /// Restores one validated logical export only into an absent database and absent sidecars.
    pub async fn restore_fresh(
        location: JobDatabaseLocation,
        config: JobRepositoryConfig,
        encoded: &[u8],
    ) -> Result<(), JobRepositoryError> {
        if encoded.len() > MAXIMUM_ENCODED_BYTES {
            return Err(JobRepositoryError::InvalidState);
        }
        let encoded = encoded.to_vec();
        tokio::task::spawn_blocking(move || restore_fresh_blocking(location, config, &encoded))
            .await
            .map_err(|_| JobRepositoryError::Unavailable)?
    }
}

fn restore_fresh_blocking(
    location: JobDatabaseLocation,
    config: JobRepositoryConfig,
    encoded: &[u8],
) -> Result<(), JobRepositoryError> {
    let envelope = decode_envelope(encoded)?;
    require_absent(&location)?;
    let writer_guard = location.acquire_writer().map_err(map_path)?;
    require_absent(&location)?;
    let database_file = location.prepare_database_file().map_err(map_path)?;
    let connection = open_writer(&location, config)?;
    super::engine::initialize_schema(&connection)?;
    verify_database(&connection)?;
    let transaction = connection.unchecked_transaction().map_err(map_sql)?;
    for record in &envelope.payload.snapshots {
        let snapshot: JobSnapshot = record.snapshot.clone().try_into()?;
        transaction
            .execute(
                "INSERT INTO jobs (job_id, generation, sequence, state, snapshot_json) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    snapshot.id().as_uuid().as_bytes().as_slice(),
                    sql_u64(snapshot.generation().get())?,
                    sql_u64(snapshot.sequence().get())?,
                    super::codec::state_code(snapshot.state()),
                    encode_snapshot(&snapshot)?,
                ],
            )
            .map_err(map_sql)?;
    }
    for record in &envelope.payload.events {
        let event: JobEvent = record.event.clone().try_into()?;
        transaction
            .execute(
                "INSERT INTO job_events (job_id, generation, sequence, event_json) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    record.identity.job_id.as_bytes().as_slice(),
                    sql_u64(record.identity.generation)?,
                    sql_u64(record.identity.sequence)?,
                    encode_event(&event)?,
                ],
            )
            .map_err(map_sql)?;
    }
    transaction.commit().map_err(map_sql)?;
    verify_database(&connection)?;
    connection
        .close()
        .map_err(|_| JobRepositoryError::Unavailable)?;
    database_file.validate_identity().map_err(map_path)?;
    location.validate_sqlite_sidecars().map_err(map_path)?;
    drop(database_file);
    drop(writer_guard);
    Ok(())
}

fn require_absent(location: &JobDatabaseLocation) -> Result<(), JobRepositoryError> {
    for path in sqlite_paths(location.path()) {
        match std::fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return Err(JobRepositoryError::InvalidState),
        }
    }
    Ok(())
}

fn sqlite_paths(database: &Path) -> [PathBuf; 3] {
    let mut wal = database.as_os_str().to_owned();
    wal.push("-wal");
    let mut shm = database.as_os_str().to_owned();
    shm.push("-shm");
    [
        database.to_path_buf(),
        PathBuf::from(wal),
        PathBuf::from(shm),
    ]
}

fn uuid_from_bytes(bytes: &[u8]) -> Result<Uuid, JobRepositoryError> {
    Uuid::from_slice(bytes).map_err(|_| JobRepositoryError::InvalidState)
}
